"""V6 canonical Report normalizer.

Builds a canonical Report dict (matches schemas/report_v6.schema.json) from
the existing ProfileAnalysis output. Lossy by necessity — see
docs/v6/canonical_schema_inventory.md §7 for the strategy.

Usage:
    from core.v6_schema import build_canonical_report
    report = build_canonical_report(analysis, llm_text="...")

Output is a plain dict (JSON-serializable) — not a dataclass. This keeps the
schema as the single source of truth and avoids dataclass drift.

================================================================================
SCOPE NOTICE — Heuristic adapter, NOT the long-term quality path. (W2.5 #9)
================================================================================

This file implements *legacy compatibility*: it converts ActionCard / Alert
output (produced by the rule-based registry + 3-stage LLM v5.19 pipeline)
into the canonical Report shape so that scorers, golden cases, and external
consumers can use the schema today.

The heuristics in this module (issue_id inference, fix_type pattern matching,
target extraction, suppression rules) are intentionally lossy and conservative.
They WILL miss nuances that the LLM had in its head but did not put into a
structured field on ActionCard.

DO NOT extend the heuristics here to reach for higher quality scores. That
would create a pile of brittle regex that fights the LLM. Instead, the Week 3
plan migrates to **LLM canonical-direct output**:

    Week 3 (R6 + Q3):
        - Update llm_prompts/prompts.py so the LLM emits canonical
          Finding/Action/Evidence JSON directly.
        - Replace this normalizer's call sites with a thin parser that
          validates + admits the LLM output unchanged when valid.
        - Keep this file only for ActionCard inputs that pre-date v6
          (offline replay of historical analyses).

When you find yourself wanting to add another `_ISSUE_ID_BY_*` heuristic
here, that is a signal to file a Week 3 task to make the LLM emit it.

Tests guarding the legacy path live in dabs/app/tests/test_v6_normalizer.py.
================================================================================
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from core.models import (
    ActionCard,
    Alert,
    ProfileAnalysis,
    Severity,
)

from ._constants import SCHEMA_VERSION
from .aliases import (
    apply_category_alias as _normalize_finding_category,
    apply_fix_type_alias as _normalize_fix_type,
    apply_issue_id_alias as _normalize_issue_id,
)
from .enrich import enrich_llm_canonical
from .issue_registry import ALL_ISSUE_IDS, is_known
from .verification_reshape import (
    VALID_VERIFICATION_TYPES as _VALID_VERIFICATION_TYPES,
    reshape_verification_entry as _normalize_verification_entry,
)

if TYPE_CHECKING:
    from .alias_telemetry import AliasHitCounts  # noqa: F401

# ---------------------------------------------------------------------------
# Severity normalization
# ---------------------------------------------------------------------------

_SEVERITY_MAP = {
    Severity.CRITICAL: "critical",
    Severity.HIGH: "high",
    Severity.MEDIUM: "medium",
    Severity.LOW: "low" if hasattr(Severity, "LOW") else "low",
    Severity.INFO: "info",
    Severity.OK: "ok",
}


def _severity_str(s: Any) -> str:
    if isinstance(s, str):
        return s.lower() or "info"
    return _SEVERITY_MAP.get(s, "info")


# ---------------------------------------------------------------------------
# Issue ID inference
# ---------------------------------------------------------------------------

# Map from (category, metric_name or keyword) → canonical issue_id.
# See docs/v6/output_contract.md §5 for the controlled vocabulary.
_ISSUE_ID_BY_CATEGORY: dict[str, str] = {
    "spill": "spill_dominant",
    "memory": "spill_dominant",
    "shuffle": "shuffle_dominant",
    "skew": "data_skew",
    "photon": "photon_partial_fallback",
    "scan": "low_file_pruning",
    "io": "large_scan_volume",
    "cache": "low_cache_hit",
    "cloud_storage": "cold_node_possibility",
    "join": "merge_join_efficiency",
    "compilation": "compilation_overhead",
    "driver": "driver_overhead",
    "federation": "federation_detected",
    "streaming": "streaming_detected",
    "stats": "cardinality_estimate_off",
    "clustering": "missing_clustering",
    "cardinality": "cardinality_estimate_off",
    "sql_pattern": "row_count_explosion",
}

# More specific overrides by metric name keyword.
_ISSUE_ID_BY_METRIC: dict[str, str] = {
    "peak_memory": "spill_dominant",
    "spill": "spill_dominant",
    "shuffle_bytes": "shuffle_volume",
    "skew": "data_skew",
    "photon_ratio": "photon_partial_fallback",
    "files_pruned": "low_file_pruning",
    "cache_hit": "low_cache_hit",
    "result_from_cache": "result_from_cache_detected",
    "compilation_time": "compilation_overhead",
    "driver": "driver_overhead",
    "implicit_cast": "implicit_cast_on_join_key",
    "decimal_heavy_aggregate": "decimal_heavy_aggregate",
    "hash_resize": "hash_resize_dominant",
    "row_count": "row_count_explosion",
    "missing_join": "missing_join_predicate",
    "files_read": "full_scan_large_table",
}


def _infer_issue_id(category: str, metric_name: str, root_cause_group: str) -> str:
    """Best-effort issue_id from the existing fields.

    All emitted ids are checked against the registry (`ALL_ISSUE_IDS`).
    Unknown candidates fall through to `unknown_<category>` so downstream
    scorers can flag them rather than silently accept a typo.
    """
    cat = (category or "").lower()
    metric = (metric_name or "").lower()
    rcg = (root_cause_group or "").lower()

    for key, iid in _ISSUE_ID_BY_METRIC.items():
        if key in metric and is_known(iid):
            return iid

    # root_cause_group sometimes mirrors a registered issue_id
    if rcg and re.match(r"^[a-z][a-z0-9_]*$", rcg) and is_known(rcg):
        return rcg

    if cat in _ISSUE_ID_BY_CATEGORY:
        candidate = _ISSUE_ID_BY_CATEGORY[cat]
        if is_known(candidate):
            return candidate

    return f"unknown_{(cat or 'other')}"


# ---------------------------------------------------------------------------
# Category normalization (Alert.category → canonical enum)
# ---------------------------------------------------------------------------

_CATEGORY_NORMALIZE: dict[str, str] = {
    "spill": "memory",
    "memory": "memory",
    "shuffle": "shuffle",
    "skew": "skew",
    "photon": "photon",
    "scan": "scan",
    "io": "scan",
    "cache": "cache",
    "cloud_storage": "cache",
    "join": "join",
    "compilation": "compilation",
    "driver": "driver",
    "federation": "federation",
    "streaming": "streaming",
    "stats": "stats",
    "cardinality": "cardinality",
    "clustering": "clustering",
    "sql_pattern": "sql_pattern",
    "compute": "compute",
}


def _normalize_category(cat: str) -> str:
    return _CATEGORY_NORMALIZE.get((cat or "").lower(), "other")


# ---------------------------------------------------------------------------
# fix_type inference
# ---------------------------------------------------------------------------

_FIX_TYPE_HEURISTICS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bSET\s+spark\.", re.IGNORECASE), "configuration"),
    (re.compile(r"\bALTER\s+TABLE\b.*\bCLUSTER\s+BY\b", re.IGNORECASE), "clustering"),
    (re.compile(r"\bCLUSTER\s+BY\b", re.IGNORECASE), "clustering"),
    (re.compile(r"\bALTER\s+TABLE\b", re.IGNORECASE), "ddl"),
    (re.compile(r"\bCREATE\s+(OR\s+REPLACE\s+)?TABLE\b", re.IGNORECASE), "ddl"),
    (re.compile(r"\bCREATE\s+(OR\s+REPLACE\s+)?MATERIALIZED VIEW\b", re.IGNORECASE), "ddl"),
    (re.compile(r"\bOPTIMIZE\b", re.IGNORECASE), "maintenance"),
    (re.compile(r"\bANALYZE\s+TABLE\b", re.IGNORECASE), "maintenance"),
    (re.compile(r"\bVACUUM\b", re.IGNORECASE), "maintenance"),
]


def _infer_fix_type(card: ActionCard) -> str:
    text = f"{card.fix_sql} {card.fix} {card.problem}"
    for pat, ft in _FIX_TYPE_HEURISTICS:
        if pat.search(text):
            return ft
    if "EXPLAIN" in text.upper() or "DESCRIBE" in text.upper():
        return "investigation"
    if not card.fix_sql.strip() and card.fix:
        return "rewrite" if "rewrite" in card.fix.lower() or "書き換え" in card.fix else "investigation"
    return "rewrite"


# ---------------------------------------------------------------------------
# action_id derivation
# ---------------------------------------------------------------------------

_ACTION_ID_RE = re.compile(r"[^a-z0-9]+")


def _slugify_action(card: ActionCard, fallback: str) -> str:
    base = (card.fix or card.problem or fallback).lower()
    slug = _ACTION_ID_RE.sub("_", base).strip("_")
    slug = slug[:60] or fallback
    if not re.match(r"^[a-z]", slug):
        slug = f"a_{slug}"
    return slug


# ---------------------------------------------------------------------------
# Evidence parsing
# ---------------------------------------------------------------------------

# Match patterns like "peak_memory=12GB" / "spill_bytes=8GB" / "cache_hit_ratio=25%"
_EVIDENCE_KV_RE = re.compile(
    r"(?P<metric>[a-z_][a-z0-9_]*)\s*[=:]\s*(?P<value>[^,;\s]+)",
    re.IGNORECASE,
)


def _evidence_from_strings(
    items: list[str],
    *,
    known_metrics: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Parse the existing list[str] into structured Evidence entries.

    Best-effort: looks for `metric=value` shapes; otherwise stores the raw
    string as value_display with metric='evidence_text'.

    grounded=True only when:
      - the parsed metric name matches one in `known_metrics` (set built
        from profile QueryMetrics / NodeMetrics fields)
      - W2.5 #3: previously this defaulted to True for any kv-shaped string
        which let unverified claims through schema check. Now anything we
        cannot match against ground truth is grounded=False.
    """
    known = known_metrics or set()
    out: list[dict[str, Any]] = []
    for raw in items or []:
        s = (raw or "").strip()
        if not s:
            continue
        match = _EVIDENCE_KV_RE.search(s)
        if match:
            metric = match.group("metric").lower()
            out.append({
                "metric": metric,
                "value_display": match.group("value"),
                "source": "actioncard.evidence",
                "grounded": metric in known,
            })
        else:
            out.append({
                "metric": "evidence_text",
                "value_display": s[:200],
                "source": "actioncard.evidence",
                "grounded": False,  # raw text — cannot mechanically verify
            })
    return out


def _evidence_from_alert(
    alert: Alert,
    *,
    known_metrics: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Convert one Alert to Evidence. grounded=True only if its metric_name
    is a known profile metric (W2.5 #3)."""
    if not alert.metric_name and not alert.current_value:
        return []
    known = known_metrics or set()
    metric = (alert.metric_name or "alert_metric").lower()
    return [{
        "metric": metric,
        "value_display": alert.current_value or "(n/a)",
        "threshold": alert.threshold or "",
        "source": f"alert:{alert.category}",
        "grounded": metric in known,
    }]


# ---------------------------------------------------------------------------
# Verification mapping
# ---------------------------------------------------------------------------


def _verification_from_card(card: ActionCard) -> list[dict[str, Any]]:
    """Convert ActionCard.verification_steps (mix of dict and str) into the
    canonical Verification union. Strings are treated as informal "metric +
    expected" pairs that we tag as metric=description / expected="see step".
    """
    out: list[dict[str, Any]] = []
    for step in card.verification_steps or []:
        if isinstance(step, dict):
            if "metric" in step and "expected" in step:
                out.append({
                    "type": "metric",
                    "metric": str(step["metric"]),
                    "expected": str(step["expected"]),
                })
            elif "sql" in step and "expected" in step:
                out.append({
                    "type": "sql",
                    "sql": str(step["sql"]),
                    "expected": str(step["expected"]),
                })
        elif isinstance(step, str) and step.strip():
            # Best-effort: keep as informal metric verification step.
            out.append({
                "type": "metric",
                "metric": "verification_step",
                "expected": step.strip()[:300],
            })
    if card.validation_metric and not out:
        out.append({
            "type": "metric",
            "metric": card.validation_metric,
            "expected": "improves",
        })
    return out


# ---------------------------------------------------------------------------
# Confidence inference (priority_rank → confidence)
# ---------------------------------------------------------------------------


def _infer_confidence(card: ActionCard) -> str:
    rank = card.priority_rank or 0
    if rank >= 80:
        return "high"
    if rank >= 50:
        return "medium"
    if rank > 0:
        return "low"
    return "needs_verification"


# ---------------------------------------------------------------------------
# Suppression rules
# ---------------------------------------------------------------------------

_SUPPRESS_FEDERATION_CATS = {"clustering", "scan", "cache", "photon", "stats"}
_SUPPRESS_CACHE_CATS = {"memory", "shuffle", "compute"}
_SUPPRESS_STREAMING_FIX_HINT = {"OPTIMIZE", "VACUUM"}


def _should_suppress(
    finding_category: str,
    actions: list[dict[str, Any]],
    *,
    is_federation: bool,
    is_streaming: bool,
    result_from_cache: bool,
    is_serverless: bool,
) -> tuple[bool, str]:
    if is_federation and finding_category in _SUPPRESS_FEDERATION_CATS:
        return True, "federation_workload_irrelevant"

    if result_from_cache and finding_category in _SUPPRESS_CACHE_CATS:
        return True, "result_cache_skip"

    if is_streaming:
        for a in actions:
            sql = (a.get("fix_sql") or "") + " " + (a.get("what") or "")
            if any(kw in sql.upper() for kw in _SUPPRESS_STREAMING_FIX_HINT):
                return True, "streaming_inappropriate"

    return False, ""


# ---------------------------------------------------------------------------
# Action / Finding builders
# ---------------------------------------------------------------------------


def _infer_rollback(fix_type: str, fix_sql: str) -> dict[str, Any] | None:
    """Conservative rollback hints by fix_type. W2.5 #7.

    OPTIMIZE/VACUUM のような物理的に元に戻せないものは irreversible、
    AQE のような実行時自動制御は auto、SET/ALTER は config/sql で復元可。
    """
    sql_upper = (fix_sql or "").upper()
    if fix_type == "configuration" or sql_upper.startswith("SET "):
        # Most SET statements can be reverted by SET ... = (default value).
        return {
            "type": "config",
            "manual_steps": "SET 元の値に戻す (現状値を事前に SHOW で確認)",
        }
    if fix_type == "clustering":
        return {
            "type": "sql",
            "rollback_sql": "ALTER TABLE <target> CLUSTER BY NONE; -- LC を解除",
        }
    if fix_type == "ddl" and "ALTER TABLE" in sql_upper and "ALTER COLUMN" in sql_upper:
        return {
            "type": "sql",
            "manual_steps": "型変更後に元の型に戻す ALTER TABLE を発行 (要 downtime / 整合確認)",
        }
    if fix_type == "maintenance":
        # OPTIMIZE/VACUUM は元に戻せない
        return {"type": "irreversible"}
    if fix_type in {"investigation", "operational"}:
        return None  # 副作用なし
    return {"type": "manual"}


def _infer_preconditions(fix_type: str, fix_sql: str) -> list[str]:
    """Conservative preconditions list based on fix_type. W2.5 #7."""
    sql_upper = (fix_sql or "").upper()
    pre: list[str] = []
    if fix_type == "maintenance" and "OPTIMIZE" in sql_upper:
        pre.append("OPTIMIZE 実行可能なクラスタ容量と空き時間")
    if fix_type == "maintenance" and "VACUUM" in sql_upper:
        pre.append("VACUUM の retention 期間を業務側と合意済み")
    if fix_type == "ddl" and "ALTER COLUMN" in sql_upper:
        pre.append("型変更による既存クエリ/パイプラインへの影響確認済み")
    if fix_type == "clustering":
        pre.append("CLUSTER BY 後の OPTIMIZE FULL を実施できる時間帯")
    if fix_type == "configuration" and "spark.databricks" in (fix_sql or ""):
        pre.append("対象 warehouse / cluster でこの spark conf を SET できる権限")
    return pre


def _impact_confidence_from_priority(priority_rank: int, has_quant: bool) -> str:
    """Map priority_rank → impact_confidence. W2.5 #7."""
    if has_quant and priority_rank >= 80:
        return "high"
    if priority_rank >= 70:
        return "medium"
    if priority_rank >= 30:
        return "low"
    return "needs_verification"


def _action_from_card(card: ActionCard, idx: int) -> dict[str, Any]:
    fix_type = _infer_fix_type(card)
    target = _infer_action_target(card, fix_type)
    what = (card.fix or card.problem or "")[:300] or "Apply recommendation"

    action: dict[str, Any] = {
        "action_id": _slugify_action(card, f"action_{idx}"),
        "target": target,
        "fix_type": fix_type,
        "what": what,
    }
    if card.likely_cause:
        action["why"] = card.likely_cause
    if card.fix_sql:
        action["fix_sql"] = card.fix_sql
        action["fix_sql_dialect"] = "databricks"
        # W5 Day 3: attach skeleton metadata so scorers and (Week 5+) prompt
        # injection can use the structured form rather than the raw 86k-char
        # SQL. Only attach when SQL is non-trivial (skeleton method != "fullsql"
        # for short SQLs is still recorded for transparency).
        try:
            from core.sql_skeleton import build_sql_skeleton  # noqa: WPS433
            skel = build_sql_skeleton(card.fix_sql)
            action["fix_sql_skeleton"] = skel.skeleton
            action["fix_sql_skeleton_method"] = skel.method
            action["fix_sql_chars_original"] = skel.original_chars
            action["fix_sql_chars_in_prompt"] = skel.skeleton_chars
        except Exception:  # noqa: BLE001 — skeleton must never block normalizer
            pass
    if card.expected_impact:
        action["expected_effect"] = card.expected_impact
    verif = _verification_from_card(card)
    if verif:
        action["verification"] = verif
    if card.risk:
        action["risk"] = card.risk.lower()
    if card.risk_reason:
        action["risk_reason"] = card.risk_reason
    if card.effort:
        action["effort"] = card.effort.lower()
    if card.priority_rank:
        action["priority_rank"] = int(card.priority_rank)
    if card.selected_because:
        action["selected_because"] = card.selected_because
    if card.is_preserved:
        action["is_preserved"] = True

    # W2.5 #7: safety-related fields
    pre = _infer_preconditions(fix_type, card.fix_sql or "")
    if pre:
        action["preconditions"] = pre
    rollback = _infer_rollback(fix_type, card.fix_sql or "")
    if rollback:
        action["rollback"] = rollback
    action["impact_confidence"] = _impact_confidence_from_priority(
        card.priority_rank or 0,
        has_quant="%" in (card.expected_impact or "") or "倍" in (card.expected_impact or ""),
    )
    return action


_TARGET_PATTERNS_BY_FIX = {
    "configuration": re.compile(r"\b(spark\.\S+)", re.IGNORECASE),
    "ddl": re.compile(r"\b(?:ALTER|CREATE|DROP)\s+TABLE\s+([\w.`]+)", re.IGNORECASE),
    "clustering": re.compile(r"\b(?:ALTER|CREATE)\s+TABLE\s+([\w.`]+)", re.IGNORECASE),
    "maintenance": re.compile(r"\b(?:OPTIMIZE|ANALYZE|VACUUM)\s+(?:TABLE\s+)?([\w.`]+)", re.IGNORECASE),
}


def _infer_action_target(card: ActionCard, fix_type: str) -> str:
    pat = _TARGET_PATTERNS_BY_FIX.get(fix_type)
    if pat:
        text = card.fix_sql or card.fix or card.problem
        m = pat.search(text)
        if m:
            return m.group(1).strip("`").strip()
    # Fallback: use root_cause_group or first 60 chars of problem
    if card.root_cause_group:
        return card.root_cause_group
    return (card.problem or fix_type)[:60]


def _finding_from_alert_and_card(
    alert: Alert | None,
    card: ActionCard | None,
    *,
    seen_issue_ids: set[str],
    suppress_ctx: dict[str, bool],
    known_metrics: set[str] | None = None,
) -> dict[str, Any] | None:
    """Build one Finding dict. Returns None if there is no useful content."""

    category_raw = (alert.category if alert else "") or (card.coverage_category.lower() if card else "")
    category = _normalize_category(category_raw)

    severity = _severity_str(alert.severity) if alert else (card.severity.lower() if card and card.severity else "medium")

    metric_name = alert.metric_name if alert else ""
    issue_id = _infer_issue_id(category_raw or category, metric_name, card.root_cause_group if card else "")

    # Disambiguate duplicate issue_ids (rare but possible)
    base_iid = issue_id
    suffix = 2
    while issue_id in seen_issue_ids:
        issue_id = f"{base_iid}_{suffix}"
        suffix += 1
    seen_issue_ids.add(issue_id)

    title = (alert.message if alert else "") or (card.problem if card else "") or issue_id
    title = title[:200]

    description_parts = []
    if alert and alert.recommendation:
        description_parts.append(alert.recommendation)
    if card and card.likely_cause:
        description_parts.append(card.likely_cause)

    evidence: list[dict[str, Any]] = []
    if alert:
        evidence.extend(_evidence_from_alert(alert, known_metrics=known_metrics))
    if card:
        evidence.extend(_evidence_from_strings(card.evidence, known_metrics=known_metrics))

    if not evidence:
        # schema requires evidence >=1; synthetic entries are always
        # grounded=False so Q3 / hallucination scorers can detect them.
        evidence.append({
            "metric": "evidence_text",
            "value_display": title,
            "source": "synthetic",
            "grounded": False,
        })

    actions: list[dict[str, Any]] = []
    if card:
        actions.append(_action_from_card(card, idx=0))

    suppressed, reason = _should_suppress(
        category, actions,
        is_federation=suppress_ctx.get("is_federation", False),
        is_streaming=suppress_ctx.get("is_streaming", False),
        result_from_cache=suppress_ctx.get("result_from_cache", False),
        is_serverless=suppress_ctx.get("is_serverless", False),
    )

    finding: dict[str, Any] = {
        "issue_id": issue_id,
        "category": category,
        "severity": severity,
        "title": title,
        "evidence": evidence,
        "actions": actions,
    }
    if card:
        finding["confidence"] = _infer_confidence(card)
        if card.root_cause_group:
            finding["root_cause_group"] = card.root_cause_group
        if card.coverage_category:
            finding["coverage_category"] = card.coverage_category
    if description_parts:
        finding["description"] = "\n\n".join(description_parts)
    if alert:
        finding["alert_links"] = [alert.alert_id]
    if suppressed:
        finding["suppressed"] = True
        finding["suppression_reason"] = reason
    return finding


# ---------------------------------------------------------------------------
# Verdict logic
# ---------------------------------------------------------------------------


def _max_severity(findings: list[dict[str, Any]]) -> str:
    order = {"critical": 4, "high": 3, "medium": 2, "low": 1, "info": 0, "ok": 0}
    return max((f.get("severity", "info") for f in findings), key=lambda s: order.get(s, 0), default="info")


def _verdict_from_findings(findings: list[dict[str, Any]], result_from_cache: bool) -> str:
    if result_from_cache:
        return "skipped_cached"
    visible = [f for f in findings if not f.get("suppressed")]
    if not visible:
        return "healthy"
    sev = _max_severity(visible)
    if sev == "critical":
        return "critical"
    if sev in ("high", "medium"):
        return "needs_attention"
    return "informational"


# ---------------------------------------------------------------------------
# Top-level API
# ---------------------------------------------------------------------------


def _build_known_metric_names(analysis: ProfileAnalysis) -> set[str]:
    """Build the set of metric names that are mechanically present in the
    profile. Used by Evidence.grounded detection (W2.5 #3).

    Sources:
      - QueryMetrics dataclass field names + `extra_metrics` keys
      - BottleneckIndicators dataclass field names
      - NodeMetrics dataclass field names (any node)
    """
    names: set[str] = set()
    qm = analysis.query_metrics
    for f in qm.__dataclass_fields__.keys():  # type: ignore[attr-defined]
        names.add(f.lower())
    for k in (qm.extra_metrics or {}).keys():
        names.add(str(k).lower())
    bi = analysis.bottleneck_indicators
    for f in bi.__dataclass_fields__.keys():  # type: ignore[attr-defined]
        names.add(f.lower())
    for n in analysis.node_metrics or []:
        for f in n.__dataclass_fields__.keys():  # type: ignore[attr-defined]
            names.add(f.lower())
        break  # all NodeMetrics share schema
    # Common short aliases used in ActionCard evidence strings
    names.update({
        "spill", "skew", "shuffle", "cache", "cache_hit",
        "photon_ratio", "files_pruned", "duration_ms",
        "rows_read", "bytes_read",
    })
    return names


# NOTE: The alias maps and ``enrich_llm_canonical`` orchestrator moved
# to dedicated modules in v6.7.1 per the v6.6.5+ refactor plan
# (``docs/v6/alias-admission-rule.md``):
#   - ``aliases.py``                — fix_type / category / issue_id maps
#   - ``metadata_repair.py``        — schema_version / report_id / etc.
#   - ``context_rebuild.py``        — context object rebuild
#   - ``enum_canonicalize.py``      — applies the alias maps to findings
#   - ``verification_reshape.py``   — verification dict coercion
#   - ``enrich.py``                 — orchestrator (4-step pipeline)
# The legacy private names (``_normalize_fix_type``, ``_FIX_TYPE_ALIASES``,
# ``_normalize_verification_entry``, ``enrich_llm_canonical``) are still
# importable from this module via the re-exports at the top — existing
# tests and eval call sites keep working unchanged.


def build_canonical_report(
    analysis: ProfileAnalysis,
    *,
    llm_text: str = "",
    pipeline_version: str = "",
    prompt_version: str = "",
    language: str = "en",
) -> dict[str, Any]:
    """Convert ProfileAnalysis into a canonical Report dict (v6.0)."""
    qm = analysis.query_metrics
    bi = analysis.bottleneck_indicators

    is_serverless = bool(
        qm.query_typename == "LakehouseSqlQuery"
        or (analysis.warehouse_info and analysis.warehouse_info.is_serverless)
    )
    is_streaming = analysis.streaming_context is not None
    is_federation = bool(qm.is_federation_query)
    result_cache = bool(qm.result_from_cache)

    suppress_ctx = {
        "is_federation": is_federation,
        "is_streaming": is_streaming,
        "result_from_cache": result_cache,
        "is_serverless": is_serverless,
    }
    known_metrics = _build_known_metric_names(analysis)

    # Pair up alerts with action_cards by category/root_cause_group when possible.
    findings: list[dict[str, Any]] = []
    appendix: list[dict[str, Any]] = []
    seen_issue_ids: set[str] = set()

    used_card_indices: set[int] = set()
    alerts: list[Alert] = list(getattr(bi, "alerts", []) or [])
    cards: list[ActionCard] = list(analysis.action_cards or [])

    # Pass 1: emit a finding for each alert, attaching matching card if any
    for alert in alerts:
        match_card = None
        for i, c in enumerate(cards):
            if i in used_card_indices:
                continue
            cat_match = c.coverage_category and alert.category and c.coverage_category.upper().startswith(alert.category.upper()[:3])
            problem_match = alert.message and alert.message[:30] in (c.problem or "")
            metric_match = alert.metric_name and alert.metric_name in (c.problem or "") + " " + " ".join(c.evidence)
            if cat_match or problem_match or metric_match:
                match_card = c
                used_card_indices.add(i)
                break
        f = _finding_from_alert_and_card(alert, match_card, seen_issue_ids=seen_issue_ids, suppress_ctx=suppress_ctx, known_metrics=known_metrics)
        if not f:
            continue
        if f.get("suppressed"):
            appendix.append(f)
        else:
            findings.append(f)

    # Pass 2: cards that had no matching alert
    for i, card in enumerate(cards):
        if i in used_card_indices:
            continue
        f = _finding_from_alert_and_card(None, card, seen_issue_ids=seen_issue_ids, suppress_ctx=suppress_ctx, known_metrics=known_metrics)
        if not f:
            continue
        if f.get("suppressed"):
            appendix.append(f)
        else:
            findings.append(f)

    verdict = _verdict_from_findings(findings, result_cache)

    headline = (llm_text or "").strip().split("\n", 1)[0][:200] or _default_headline(verdict, findings)

    summary: dict[str, Any] = {
        "headline": headline,
        "verdict": verdict,
    }
    key_metrics = _build_key_metrics(qm, bi)
    if key_metrics:
        summary["key_metrics"] = key_metrics

    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_id": str(uuid.uuid4()),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "query_id": qm.query_id or "unknown",
        "context": _build_context(qm, analysis, language),
        "summary": summary,
        "findings": findings,
    }
    if pipeline_version:
        report["pipeline_version"] = pipeline_version
    if prompt_version:
        report["prompt_version"] = prompt_version
    if appendix:
        report["appendix_excluded_findings"] = appendix
    return report


def _default_headline(verdict: str, findings: list[dict[str, Any]]) -> str:
    if verdict == "healthy":
        return "問題は検出されませんでした"
    if verdict == "skipped_cached":
        return "結果キャッシュヒットのため分析を抑制しました"
    visible = [f for f in findings if not f.get("suppressed")]
    if not visible:
        return "情報レベルの所見のみ"
    return f"{visible[0].get('title', 'Issue detected')}"


# ``_build_context`` moved to ``context_rebuild.build_context`` in
# v6.7.1 — re-exported below so existing internal users (build_canonical_report
# above) keep importing it locally without churn.
from .context_rebuild import build_context as _build_context  # noqa: E402, F401


def _build_key_metrics(qm, bi) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    if qm.total_time_ms:
        metrics.append({
            "name": "duration_ms",
            "value_display": _format_ms(qm.total_time_ms),
            "value_raw": int(qm.total_time_ms),
        })
    if qm.read_bytes:
        metrics.append({
            "name": "read_bytes",
            "value_display": _format_bytes(qm.read_bytes),
            "value_raw": int(qm.read_bytes),
        })
    if qm.spill_to_disk_bytes:
        metrics.append({
            "name": "spill_bytes",
            "value_display": _format_bytes(qm.spill_to_disk_bytes),
            "value_raw": int(qm.spill_to_disk_bytes),
            "direction": "bad",
        })
    if getattr(bi, "cache_hit_ratio", 0):
        metrics.append({
            "name": "cache_hit_ratio",
            "value_display": f"{bi.cache_hit_ratio * 100:.1f}%",
            "value_raw": float(bi.cache_hit_ratio),
        })
    if getattr(bi, "photon_ratio", 0):
        metrics.append({
            "name": "photon_ratio",
            "value_display": f"{bi.photon_ratio * 100:.1f}%",
            "value_raw": float(bi.photon_ratio),
        })
    return metrics[:6]


def _format_ms(ms: int) -> str:
    if ms is None:
        return "0 ms"
    if ms < 1000:
        return f"{ms} ms"
    return f"{ms / 1000:.2f} s"


def _format_bytes(b: int) -> str:
    if b is None:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    val = float(b)
    for u in units:
        if val < 1024 or u == units[-1]:
            return f"{val:.1f} {u}".replace(".0 ", " ")
        val /= 1024
    return f"{b} B"
