"""V6 A/B runner — execute goldens_runner under multiple flag conditions.

Codex W3.5 review (2026-04-25) recommended W4 axis:
  baseline / canonical-direct / no-force-fill / both
on the same goldens manifest, capturing per-case metric diff, regression
verdicts, and canonical parse failure rate.

Implementation: each condition is run as a child process so feature_flags
caches don't bleed across conditions and runs can be parallelized later.

Output:
  eval/ab_summary/<run_name>.json
  eval/ab_summary/<run_name>.md

See: docs/eval/ab_runner_design.md
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Top-level import so tests can patch it via
# ``patch("eval.ab_runner.score_layer_b", ...)``. Lazy imports inside
# the dispatcher would force tests into using ``patch.dict(sys.modules)``.
from eval.scorers.r10_quality_judge import score_layer_b

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Conditions (Codex W3.5 review §3-1)
# ---------------------------------------------------------------------------

# Each condition is a dict of env var → value. Empty dict = baseline.
CONDITIONS: dict[str, dict[str, str]] = {
    # V6 standard: all V6_* default-on (since v6.6.4). Empty overrides
    # means "use the defaults", which is now the production behavior.
    "baseline": {},
    # V5 fallback: every V6 kill switch flipped off — the "legacy
    # full-off" pattern from docs/v6/why-default-on.md. Drives V5 vs
    # V6 comparison runs against the current sidecar-aware baseline.
    "v5_legacy": {
        "V6_CANONICAL_SCHEMA": "0",
        "V6_REVIEW_NO_KNOWLEDGE": "0",
        "V6_REFINE_MICRO_KNOWLEDGE": "0",
        "V6_ALWAYS_INCLUDE_MINIMUM": "0",
        "V6_SKIP_CONDENSED_KNOWLEDGE": "0",
        "V6_RECOMMENDATION_NO_FORCE_FILL": "0",
        "V6_SQL_SKELETON_EXTENDED": "0",
    },
    # Historical V6.0/V6.1 single-flag opt-ins. Pre-v6.6.4 these
    # exercised one V6 behavior at a time against a default-off
    # baseline. Now they are redundant with ``baseline`` (same env
    # state once defaults are on); kept for backward-compat with
    # existing ab_summary/* artifacts but no longer informative.
    "canonical-direct": {"V6_CANONICAL_SCHEMA": "1"},
    "no-force-fill": {"V6_RECOMMENDATION_NO_FORCE_FILL": "1"},
    "both": {
        "V6_CANONICAL_SCHEMA": "1",
        "V6_RECOMMENDATION_NO_FORCE_FILL": "1",
        "V6_REVIEW_NO_KNOWLEDGE": "1",
        "V6_REFINE_MICRO_KNOWLEDGE": "1",
        "V6_ALWAYS_INCLUDE_MINIMUM": "1",
        "V6_SKIP_CONDENSED_KNOWLEDGE": "1",
    },
}

# Primary metric fields tracked in the per-case diff (the keys we read out
# of the goldens_runner JSON output). Each maps to the human-readable
# verdict comparison key.
PRIMARY_METRICS = (
    "evidence_grounding_composite",
    "evidence_metric_grounding_ratio",
    "finding_support_ratio",
    "ungrounded_numeric_ratio",  # lower-is-better
    "recall_ratio",
    "recall_strict_ratio",
    "actionability_specific_ratio",
    "hallucination_score_avg",
)
LOWER_IS_BETTER = frozenset({"ungrounded_numeric_ratio"})

# verdict thresholds (W4 Day 1 §4)
_VERDICT_DELTA = 0.05  # ±5 pt


# ---------------------------------------------------------------------------
# Subprocess execution
# ---------------------------------------------------------------------------


def _run_condition(
    condition: str,
    env_overrides: dict[str, str],
    run_name: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Run goldens_runner as a child process under one condition.

    Returns the parsed JSON baseline dict.
    """
    sub_baseline_name = f"{run_name}__{condition}"
    out_json = REPO_ROOT / args.out_dir / f"{sub_baseline_name}.json"

    env = os.environ.copy()
    # Reset all V6 flags first (don't inherit from parent shell)
    for k in list(env.keys()):
        if k.startswith("V6_"):
            env.pop(k, None)
    env.update(env_overrides)

    cmd = [
        sys.executable, "-m", "eval.goldens_runner",
        "--manifest", args.manifest,
        "--baseline-name", sub_baseline_name,
        "--out-dir", str(REPO_ROOT / "eval" / "baselines"),
        "--report-dir", str(REPO_ROOT / "eval" / "reports"),
        "--lang", args.lang,
    ]
    if args.skip_judge:
        cmd.append("--skip-judge")
    if args.skip_llm:
        cmd.append("--skip-llm")
    if args.tag:
        cmd.extend(["--tag", args.tag])
    if args.limit is not None:
        cmd.extend(["--limit", str(args.limit)])
    # Pass Databricks credentials via env, not CLI args. ``ps aux``
    # would otherwise expose the PAT in the process listing on shared
    # hosts. ``goldens_runner`` already reads ``DATABRICKS_HOST`` /
    # ``DATABRICKS_TOKEN`` as argparse defaults via os.environ.get.
    if args.host:
        env["DATABRICKS_HOST"] = args.host
    if args.token:
        env["DATABRICKS_TOKEN"] = args.token

    pythonpath = env.get("PYTHONPATH", "")
    dabs_app = str(REPO_ROOT / "dabs" / "app")
    env["PYTHONPATH"] = (
        f"{dabs_app}:{pythonpath}" if pythonpath else dabs_app
    )

    logger.info("running condition=%s flags=%s", condition, list(env_overrides.keys()))
    proc = subprocess.run(
        cmd, env=env, cwd=str(REPO_ROOT), capture_output=True, text=True
    )
    if proc.returncode != 0:
        logger.error("condition=%s failed: %s", condition, proc.stderr[-500:])
        return {"condition": condition, "error": proc.stderr[-2000:]}

    json_path = REPO_ROOT / "eval" / "baselines" / f"{sub_baseline_name}.json"
    if not json_path.exists():
        return {"condition": condition, "error": f"baseline JSON missing: {json_path}"}
    # V6.2 Tier 1: load via the sidecar-aware helper so each case carries
    # ``canonical_report`` (or None when the sidecar is absent) for
    # downstream Layer B consumption.
    from eval.goldens_runner import _load_baseline_with_canonical

    return {
        "condition": condition,
        "baseline": _load_baseline_with_canonical(json_path),
    }


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def _avg(records: list[dict[str, Any]], field: str) -> float:
    vals = [r.get(field) for r in records if r.get(field) is not None and "skipped_reason" not in r]
    vals = [v for v in vals if isinstance(v, (int, float))]
    return sum(vals) / len(vals) if vals else 0.0


def _canonical_failure_rate(records: list[dict[str, Any]]) -> float:
    evaluated = [r for r in records if "skipped_reason" not in r]
    if not evaluated:
        return 0.0
    failures = sum(
        1 for r in evaluated if r.get("canonical_source") in (None, "normalizer_fallback", "missing")
    )
    return failures / len(evaluated)


def _canonical_source_breakdown(records: list[dict[str, Any]]) -> dict[str, float]:
    """Codex W4 review: split parse failure into normalizer_fallback vs
    missing so Week 5 can target the actual gap (not lump them together).
    """
    evaluated = [r for r in records if "skipped_reason" not in r]
    if not evaluated:
        return {"llm_direct": 0.0, "normalizer_fallback": 0.0, "missing": 0.0}
    counts = {"llm_direct": 0, "normalizer_fallback": 0, "missing": 0}
    for r in evaluated:
        src = r.get("canonical_source")
        if src in counts:
            counts[src] += 1
        else:
            counts["missing"] += 1
    n = len(evaluated)
    return {k: round(v / n, 4) for k, v in counts.items()}


def _fallback_case_composite_avg(records: list[dict[str, Any]]) -> float:
    """Average ``evidence_grounding_composite`` over cases that did NOT
    take the V6 LLM-direct canonical path (v6.7.0 telemetry).

    Codex framing: the raw ``canonical_parse_failure_rate`` reports how
    often we fell back, but does not say *how good* the fallback path
    actually was. ``fallback_case_composite_avg`` is the customer-impact
    counterpart — when V6 fell back, was the resulting analysis still
    grounded enough to be useful? A high value here means the fallback
    is a viable safety net; a low value means a fallback is effectively
    a degraded report.
    """
    fallback = [
        r for r in records
        if r.get("canonical_source") in ("normalizer_fallback", "missing")
        and "skipped_reason" not in r
        and isinstance(r.get("evidence_grounding_composite"), (int, float))
    ]
    if not fallback:
        return 0.0
    return round(
        sum(r["evidence_grounding_composite"] for r in fallback) / len(fallback),
        4,
    )


def _alias_hit_summary(records: list[dict[str, Any]]) -> dict[str, int | float]:
    """Aggregate per-case ``alias_hits`` dicts into a single summary
    block (v6.7.0 telemetry — leverages the shared aggregator from
    ``core.v6_schema.alias_telemetry``).
    """
    from core.v6_schema.alias_telemetry import AliasHitCounts, aggregate

    trackers: list[AliasHitCounts] = []
    for r in records:
        if "skipped_reason" in r:
            continue
        ah = r.get("alias_hits")
        if not isinstance(ah, dict):
            continue
        # Reconstitute the dataclass for the shared aggregator. Only
        # LLM-direct cases populate ``alias_hits``; everything else
        # contributes a zero tracker so the rate denominator stays
        # honest about what was eligible.
        trackers.append(
            AliasHitCounts(
                fix_type=int(ah.get("fix_type", 0) or 0),
                category=int(ah.get("category", 0) or 0),
                issue_id=int(ah.get("issue_id", 0) or 0),
            )
        )
    return aggregate(trackers)


def _classify_verdict(deltas: dict[str, float]) -> str:
    """Per-case verdict: regressed > improved > unchanged."""
    is_regressed = False
    is_improved = False
    for metric in ("evidence_grounding_composite", "evidence_metric_grounding_ratio", "finding_support_ratio"):
        d = deltas.get(metric)
        if d is None:
            continue
        if d <= -_VERDICT_DELTA:
            is_regressed = True
        elif d >= _VERDICT_DELTA:
            is_improved = True
    # ungrounded_numeric is lower-is-better → flip sign
    un = deltas.get("ungrounded_numeric_ratio")
    if un is not None:
        if un >= _VERDICT_DELTA:
            is_regressed = True
        elif un <= -_VERDICT_DELTA:
            is_improved = True
    if is_regressed:
        return "regressed"
    if is_improved:
        return "improved"
    return "unchanged"


def _schema_pass_rate(records: list[dict[str, Any]]) -> float:
    evaluated = [r for r in records if "skipped_reason" not in r]
    if not evaluated:
        return 1.0
    return sum(1 for r in evaluated if r.get("schema_valid") is True) / len(evaluated)


# ---------------------------------------------------------------------------
# Layer B dispatch (V6.2 Tier 1, Codex 2026-04-26)
# ---------------------------------------------------------------------------
#
# The per-case canonical Report now travels with each baseline record
# (sidecar attach via ``_load_baseline_with_canonical``), so the LLM
# judge can evaluate the actual structured output instead of the
# placeholder used in v6.1. Codex recommended:
#   - per-case judging with top_n=5 rather than a single representative
#   - deterministic ranking (recall miss / schema invalid weight first,
#     case_id ascending breaks ties)
#   - missing canonical → explicit skip counter, not silent placeholder
#   - schema_version mismatch → separate skip counter so old baselines
#     don't pollute the missing-canonical signal


# Currently-supported canonical schema_version. Mirror of
# ``goldens_runner._CANONICAL_SIDECAR_SCHEMA_VERSION``; kept here as a
# tiny constant to avoid an import cycle on the Layer B path.
_CURRENT_CANONICAL_SCHEMA_VERSION = 1


def _rank_for_layer_b(rec: dict[str, Any]) -> tuple[int, str]:
    """Return a sort key — lower sorts first → higher judge priority."""
    rank = 0
    if rec.get("recall_missed"):
        rank -= len(rec.get("recall_missed") or [])
    if rec.get("schema_valid") is False:
        rank -= 2
    return (rank, str(rec.get("case_id") or ""))


def _select_layer_b_cases(records: list[dict[str, Any]], top_n: int) -> list[dict]:
    """Pick the deterministically-ranked top_n evaluable records.

    ``top_n <= 0`` returns an empty list (judge nothing). Codex 2026-04-26
    review caught that the previous ``max(1, top_n)`` silently took 1
    case when callers passed 0, which is the wrong default.
    """
    if top_n <= 0:
        return []
    evaluable = [r for r in records if "skipped_reason" not in r]
    evaluable.sort(key=_rank_for_layer_b)
    return evaluable[:top_n]


def _run_layer_b_for_condition(
    records: list[dict[str, Any]],
    *,
    host: str,
    token: str,
    judge_model: str,
    top_n: int,
) -> tuple[float | None, list[str], dict[str, int]]:
    """Invoke the LLM judge across top_n cases for one condition.

    Returns ``(mean_score, reasons, skip_counts)``. ``mean_score`` is
    None when no judgeable case existed (every selected case was
    missing its canonical). ``skip_counts`` exposes
    ``missing_canonical`` so callers can surface the gap rather than
    treating "no canonical" as "judge cleanly returned 1.0".
    """
    skips = {"missing_canonical": 0, "schema_mismatch": 0}
    if not host or not token:
        return None, [], skips

    cases = _select_layer_b_cases(records, top_n=top_n)
    scores: list[float] = []
    reasons: list[str] = []
    for rec in cases:
        canonical = rec.get("canonical_report")
        if canonical is None:
            skips["missing_canonical"] += 1
            continue
        # Schema-version gate (V6.2-4). Missing version is treated as
        # mismatch since we have no way to validate the structure.
        canonical_version = canonical.get("schema_version")
        if canonical_version != _CURRENT_CANONICAL_SCHEMA_VERSION:
            skips["schema_mismatch"] += 1
            logger.warning(
                "Layer B skipping case=%s: canonical schema_version=%r != current %d",
                rec.get("case_id"),
                canonical_version,
                _CURRENT_CANONICAL_SCHEMA_VERSION,
            )
            continue
        try:
            score, rec_reasons = score_layer_b(
                canonical_report=canonical,
                databricks_host=host,
                databricks_token=token,
                judge_model=judge_model,
                top_n=top_n,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("Layer B failed for case=%s: %s", rec.get("case_id"), e)
            continue
        if score is not None:
            scores.append(score)
            reasons.extend(rec_reasons)

    if not scores:
        return None, [], skips
    return sum(scores) / len(scores), reasons, skips


def _build_summary(
    run_name: str,
    cond_results: dict[str, dict[str, Any]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    baseline_records = cond_results["baseline"]["baseline"]["cases"]
    baseline_by_case = {r["case_id"]: r for r in baseline_records}

    cases_count = len(baseline_records)

    # Per-condition averages
    metrics_per_condition: dict[str, dict[str, float]] = {}
    canonical_parse_failure_rate: dict[str, float] = {}
    canonical_source_breakdown: dict[str, dict[str, float]] = {}
    schema_pass_per_condition: dict[str, float] = {}
    fallback_case_composite_avg: dict[str, float] = {}
    alias_hit_summary_per_condition: dict[str, dict[str, int | float]] = {}
    for cond, payload in cond_results.items():
        records = payload.get("baseline", {}).get("cases", []) or []
        metrics_per_condition[cond] = {
            metric: round(_avg(records, metric), 4)
            for metric in PRIMARY_METRICS
        }
        canonical_parse_failure_rate[cond] = round(_canonical_failure_rate(records), 4)
        canonical_source_breakdown[cond] = _canonical_source_breakdown(records)
        schema_pass_per_condition[cond] = round(_schema_pass_rate(records), 4)
        # v6.7.0 telemetry
        fallback_case_composite_avg[cond] = _fallback_case_composite_avg(records)
        alias_hit_summary_per_condition[cond] = _alias_hit_summary(records)

    # R10 add-on: deterministic Layer A. V6.2: Layer B LLM judge now
    # consumes the actual canonical Report (sidecar persisted) per
    # selected case rather than a placeholder.
    from eval.scorers.r10_quality import score_r10, to_dict as _r10_to_dict

    use_judge = bool(getattr(args, "with_llm_judge", False))
    judge_top_n = int(getattr(args, "judge_top_n", 5))
    judge_model = getattr(args, "judge_model", "databricks-claude-sonnet-4")

    r10_per_condition: dict[str, dict[str, Any]] = {}
    layer_b_skip_per_condition: dict[str, dict[str, int]] = {}
    for cond in metrics_per_condition:
        m = metrics_per_condition[cond]

        layer_b_score: float | None = None
        layer_b_reasons: list[str] = []
        if use_judge:
            cond_records = (
                cond_results.get(cond, {}).get("baseline", {}).get("cases") or []
            )
            layer_b_score, layer_b_reasons, skips = _run_layer_b_for_condition(
                cond_records,
                host=getattr(args, "host", ""),
                token=getattr(args, "token", ""),
                judge_model=judge_model,
                top_n=judge_top_n,
            )
            layer_b_skip_per_condition[cond] = skips
            if skips.get("missing_canonical", 0) > 0:
                logger.info(
                    "Layer B condition=%s: skipped %d cases missing canonical",
                    cond, skips["missing_canonical"],
                )

        r10 = score_r10(
            schema_pass_ratio=schema_pass_per_condition.get(cond, 1.0),
            actionability_specific=m.get("actionability_specific_ratio", 0.0),
            recall_strict=m.get("recall_strict_ratio", 0.0),
            hallucination_clean=m.get("hallucination_score_avg", 0.0),
            q3_composite=m.get("evidence_grounding_composite", 0.0),
            q3_finding_support=m.get("finding_support_ratio", 0.0),
            q3_metric_grounded=m.get("evidence_metric_grounding_ratio", 0.0),
            q3_ungrounded_numeric=m.get("ungrounded_numeric_ratio", 0.0),
            canonical_parse_failure_rate=canonical_parse_failure_rate.get(cond, 1.0),
            layer_b_score=layer_b_score,
            layer_b_reasons=layer_b_reasons,
        )
        r10_per_condition[cond] = _r10_to_dict(r10)

    # Per-case diff (baseline vs each candidate)
    case_diff: dict[str, dict[str, Any]] = {}
    regression_summary: dict[str, dict[str, int]] = {}

    # Iterate only the conditions that actually ran. ``CONDITIONS`` is
    # a static catalogue (baseline + v5_legacy + historical opt-ins);
    # callers can pick a subset via ``--conditions a,b``, in which case
    # the dict lookup below would KeyError on the unrequested ones.
    for cond in cond_results:
        if cond == "baseline":
            continue
        regression_summary[cond] = {"improved": 0, "regressed": 0, "unchanged": 0}
        records = cond_results[cond].get("baseline", {}).get("cases", []) or []
        cand_by_case = {r["case_id"]: r for r in records}
        for case_id, base in baseline_by_case.items():
            cand = cand_by_case.get(case_id)
            if cand is None or "skipped_reason" in base or "skipped_reason" in cand:
                continue
            deltas = {}
            for metric in PRIMARY_METRICS:
                bv = base.get(metric)
                cv = cand.get(metric)
                if bv is None or cv is None:
                    continue
                deltas[metric] = round(float(cv) - float(bv), 4)
            verdict = _classify_verdict(deltas)
            regression_summary[cond][verdict] += 1
            entry = case_diff.setdefault(case_id, {})
            entry[cond] = {"deltas": deltas, "verdict": verdict}

    return {
        "run_name": run_name,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cases_count": cases_count,
        # Only the conditions that actually ran. Was previously
        # ``list(CONDITIONS.keys())`` which broke ``--conditions a,b``
        # (markdown writer would KeyError on the un-run conditions).
        "conditions": list(cond_results.keys()),
        "metrics_per_condition": metrics_per_condition,
        "schema_pass_per_condition": schema_pass_per_condition,
        "r10_per_condition": r10_per_condition,
        "case_diff": case_diff,
        "regression_summary": regression_summary,
        "canonical_parse_failure_rate": canonical_parse_failure_rate,
        "canonical_source_breakdown": canonical_source_breakdown,
        # v6.7.0 telemetry: customer-impact view of the fallback path
        # plus alias-hit telemetry per condition. Codex framing —
        # fallback_case_composite_avg is the metric to watch when
        # judging whether a normalizer_fallback is "OK enough", and
        # alias_hit_rate tells us whether the alias map is earning
        # its keep (admission rule decision support).
        "fallback_case_composite_avg": fallback_case_composite_avg,
        "alias_hit_summary": alias_hit_summary_per_condition,
        # V6.2 Tier 1: distinguishes "judge ran on N cases" from
        # "judge skipped N cases" so reviewers can spot the gap when
        # an old baseline lands without canonical or with a stale
        # schema_version. Empty dict when --with-llm-judge wasn't
        # requested.
        "layer_b_skip_per_condition": layer_b_skip_per_condition,
        "args": {
            "manifest": args.manifest,
            "skip_llm": args.skip_llm,
            "skip_judge": args.skip_judge,
            "tag": args.tag,
            "limit": args.limit,
        },
    }


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------


def _format_pct(v: float) -> str:
    return f"{v * 100:.2f}%"


def _signed(v: float | None) -> str:
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.2f}pt"


def _write_markdown(summary: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# {summary['run_name']} — V6 A/B Summary",
        "",
        f"Generated: {summary['generated_at']}",
        f"Cases: {summary['cases_count']}, Conditions: {len(summary['conditions'])}",
        "",
        "## Metrics per condition",
        "",
        "| Metric | " + " | ".join(summary["conditions"]) + " |",
        "|--------|" + "|".join(["-" * 12 for _ in summary["conditions"]]) + "|",
    ]
    for metric in PRIMARY_METRICS:
        row = [f"| {metric}"]
        for cond in summary["conditions"]:
            v = summary["metrics_per_condition"][cond].get(metric, 0.0) or 0.0
            row.append(_format_pct(v))
        lines.append(" | ".join(row) + " |")
    lines += [
        "",
        "## Canonical parse failure rate",
        "",
        "| Condition | failure_rate |",
        "|-----------|-------------:|",
    ]
    for cond, rate in summary["canonical_parse_failure_rate"].items():
        lines.append(f"| {cond} | {_format_pct(rate)} |")

    # ---- v6.7.0 telemetry: fallback composite + alias hits ----
    fallback_avg = summary.get("fallback_case_composite_avg", {})
    alias_summary = summary.get("alias_hit_summary", {})
    if fallback_avg or alias_summary:
        lines += [
            "",
            "## V6 telemetry (fallback + alias hits)",
            "",
            "| Condition | fallback_composite_avg | alias_hit_rate | hits_per_case_avg | fix_type | category | issue_id |",
            "|-----------|----------------------:|---------------:|------------------:|---------:|---------:|---------:|",
        ]
        for cond in summary["conditions"]:
            fb = fallback_avg.get(cond, 0.0) or 0.0
            ah = alias_summary.get(cond, {}) or {}
            lines.append(
                f"| {cond} | "
                f"{_format_pct(fb)} | "
                f"{_format_pct(ah.get('alias_hit_rate', 0.0))} | "
                f"{ah.get('hits_per_case_avg', 0.0):.2f} | "
                f"{ah.get('fix_type_total', 0)} | "
                f"{ah.get('category_total', 0)} | "
                f"{ah.get('issue_id_total', 0)} |"
            )
        lines += [
            "",
            "_`fallback_composite_avg` = avg evidence_grounding_composite over cases that did NOT take the LLM-direct canonical path. `alias_hit_rate` = fraction of LLM-direct cases that needed the alias map to validate. `hits_per_case_avg` = total alias rewrites ÷ cases (intensity)._ ",
        ]

    # ---- R10 add-on (Day 5) ----
    r10_per_condition = summary.get("r10_per_condition", {})
    if r10_per_condition:
        lines += [
            "",
            "## R10 quality add-on (Layer A — deterministic)",
            "",
            "| Condition | layer_a_score | overall_verdict | reasons |",
            "|-----------|--------------:|:----------------|---------|",
        ]
        for cond in summary["conditions"]:
            r10 = r10_per_condition.get(cond, {})
            la = r10.get("layer_a_score", 0.0) or 0.0
            verdict = r10.get("overall_verdict", "?")
            reasons = r10.get("layer_a_reasons", []) or []
            verdict_emoji = {"pass": "✅", "borderline": "🟡", "fail": "❌"}.get(verdict, "?")
            reasons_short = "; ".join(reasons[:3]) if reasons else "(all targets met)"
            lines.append(
                f"| {cond} | {_format_pct(la)} | {verdict_emoji} {verdict} | {reasons_short} |"
            )

    lines += [
        "",
        "## Regression summary (vs baseline)",
        "",
        "| Condition | improved | regressed | unchanged |",
        "|-----------|---------:|----------:|----------:|",
    ]
    for cond, counts in summary["regression_summary"].items():
        lines.append(
            f"| {cond} | {counts['improved']} | {counts['regressed']} | {counts['unchanged']} |"
        )

    # ---- per-case detail (W4 Day 3) ----
    case_diff = summary.get("case_diff", {})
    if case_diff:
        lines += [
            "",
            "## Per-case verdicts",
            "",
            "Showing all cases that have at least one non-baseline condition. "
            "Verdict is determined by ±5pt on the primary 4 metrics "
            "(q3_composite / metric_grounded / finding_support / ungrounded_numeric).",
            "",
        ]
        # one block per condition
        for cond in summary["conditions"]:
            if cond == "baseline":
                continue
            cases_in_cond = [
                (case_id, cond_data[cond])
                for case_id, cond_data in case_diff.items()
                if cond in cond_data
            ]
            if not cases_in_cond:
                continue
            # Sort: regressed first, then improved, then unchanged
            order = {"regressed": 0, "improved": 1, "unchanged": 2}
            cases_in_cond.sort(key=lambda x: (order.get(x[1].get("verdict"), 9), x[0]))
            lines += [
                f"### {cond}",
                "",
                "| case_id | verdict | Δ q3_composite | Δ metric_grounded | Δ finding_support | Δ ungrounded_numeric |",
                "|---------|---------|--------------:|------------------:|------------------:|---------------------:|",
            ]
            for case_id, entry in cases_in_cond:
                deltas = entry.get("deltas", {})
                verdict = entry.get("verdict", "?")
                emoji = {"improved": "✅", "regressed": "❌", "unchanged": "➖"}.get(verdict, "?")
                lines.append(
                    f"| {case_id} | {emoji} {verdict} | "
                    f"{_signed(deltas.get('evidence_grounding_composite'))} | "
                    f"{_signed(deltas.get('evidence_metric_grounding_ratio'))} | "
                    f"{_signed(deltas.get('finding_support_ratio'))} | "
                    f"{_signed(deltas.get('ungrounded_numeric_ratio'))} |"
                )
            lines.append("")

    lines += ["", "## Notes", ""]
    if summary["args"]["skip_llm"]:
        lines.append(
            "- `--skip-llm` mode: all conditions use the same rule-based pipeline. Differences "
            "between conditions only reflect prompt-string changes (no actual LLM behavior). "
            "Use `--enable-llm` once API access is configured."
        )
    lines.append("- Verdict thresholds: ±5pt on Q3 composite / metric_grounded / finding_support / ungrounded_numeric.")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ab_runner",
        description="V6 A/B runner: 4 conditions over goldens manifest.",
    )
    parser.add_argument("--manifest", default="eval/goldens/manifest.yaml")
    parser.add_argument("--run-name", default="v6_ab_smoke")
    parser.add_argument("--out-dir", default="eval/ab_summary")
    parser.add_argument("--lang", default="ja", choices=["en", "ja"])
    parser.add_argument("--skip-judge", action="store_true")
    parser.add_argument("--skip-llm", action="store_true",
                        help="skip analysis LLM (rule-based only)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--tag", default=None)
    parser.add_argument("--host", default=os.environ.get("DATABRICKS_HOST", ""))
    parser.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN", ""))
    parser.add_argument("--conditions", default=None,
                        help="comma-separated subset of conditions (default all 4)")
    # W4 Day 6: composite gates
    # Codex W4 review (2026-04-25): "--gate-w4-completion" was overloaded —
    # it requires LLM-only thresholds. Renamed to --gate-llm-quality, with
    # an alias kept for backward compatibility.
    parser.add_argument("--gate-llm-quality", "--gate-w4-completion",
                        dest="gate_llm_quality",
                        action="store_true",
                        help="LLM quality gate: Q3 ≥ 80%, metric_grounded ≥ 70%, "
                             "finding_support ≥ 80%, ungrounded_numeric ≤ 15%, "
                             "recall_strict ≥ 50%, hallucination ≥ 0.85, "
                             "schema = 100%, regression ≤ 1, parse_failure ≤ 5%. "
                             "Requires LLM API access — rule-based runs cannot pass.")
    parser.add_argument("--gate-w4-infra", action="store_true",
                        help="W4 infrastructure gate: 4 conditions ran, schema 100%, "
                             "rule-based regressions == 0, R10 score ≥ 0.55. "
                             "Passes on rule-based-only baseline.")
    parser.add_argument("--gate-condition", default="both",
                        help="which condition the LLM-quality gate applies to (default: both)")
    parser.add_argument("--gate-r10-verdict", default=None,
                        choices=["pass", "borderline", "fail"],
                        help="fail if R10 layer_a verdict is worse than this (default: no gate)")
    # V6.1: Layer B LLM judge integration
    parser.add_argument("--with-llm-judge", action="store_true",
                        help="invoke L3/L4 LLM judge per-condition to populate R10 layer_b_score (requires DATABRICKS_HOST/TOKEN)")
    parser.add_argument("--judge-top-n", type=int, default=5,
                        help="judge top-N actions per condition (default 5, used with --with-llm-judge)")
    parser.add_argument("--judge-model", default="databricks-claude-sonnet-4",
                        help="LLM model for the judge (default databricks-claude-sonnet-4)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    selected = (
        args.conditions.split(",") if args.conditions else list(CONDITIONS.keys())
    )
    selected = [c.strip() for c in selected if c.strip() in CONDITIONS]
    if "baseline" not in selected:
        selected.insert(0, "baseline")
    logger.info("conditions: %s", selected)

    cond_results: dict[str, dict[str, Any]] = {}
    for cond in selected:
        cond_results[cond] = _run_condition(
            cond, CONDITIONS[cond], args.run_name, args
        )
        if cond_results[cond].get("error"):
            logger.error("condition=%s aborted: %s", cond, cond_results[cond]["error"])
            return 2

    summary = _build_summary(args.run_name, cond_results, args)
    out_json = REPO_ROOT / args.out_dir / f"{args.run_name}.json"
    out_md = REPO_ROOT / args.out_dir / f"{args.run_name}.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    _write_markdown(summary, out_md)
    logger.info("A/B summary written: %s", out_json)
    logger.info("A/B markdown written: %s", out_md)

    # ---- W4 Day 6: composite gates ----
    gate_violations: list[str] = []
    if args.gate_llm_quality:
        gate_violations.extend(_llm_quality_violations(summary, args.gate_condition))
    if args.gate_w4_infra:
        gate_violations.extend(_w4_infra_violations(summary))
    if args.gate_r10_verdict is not None:
        gate_violations.extend(_r10_verdict_violations(summary, args.gate_r10_verdict))
    if gate_violations:
        logger.error("A/B gate FAILED:")
        for v in gate_violations:
            logger.error("  - %s", v)
        return 1
    if args.gate_llm_quality or args.gate_w4_infra or args.gate_r10_verdict:
        logger.info("A/B gate passed.")
    return 0


# ---------------------------------------------------------------------------
# Composite gate (Codex W3.5 §3-3)
# ---------------------------------------------------------------------------

# Week 4 completion targets (TODO.md V6 W4 引き継ぎ)
W4_TARGETS: dict[str, float] = {
    "q3_composite":          0.80,
    "metric_grounded":       0.70,
    "finding_support":       0.80,
    "ungrounded_numeric_max": 0.15,
    "recall_strict":         0.50,
    "hallucination":         0.85,
    "schema_pass":           1.00,
    "case_regressions_max":  1,
    "canonical_parse_failure_max": 0.05,
}


def _w4_infra_violations(summary: dict[str, Any]) -> list[str]:
    """W4 infra gate (Codex W4 review): rule-based runs should pass.

    Checks:
      - all 4 conditions executed
      - schema_pass = 100% in every condition
      - regression_summary regressed == 0 in every candidate condition
      - R10 layer_a_score >= 0.55 (i.e. infra alive, not LLM quality)
    """
    v: list[str] = []
    expected = {"baseline", "canonical-direct", "no-force-fill", "both"}
    actual = set(summary.get("conditions") or [])
    if not expected.issubset(actual):
        v.append(f"missing conditions: {sorted(expected - actual)}")
    for cond, schema in summary.get("schema_pass_per_condition", {}).items():
        if schema < 1.0:
            v.append(f"[infra/{cond}] schema_pass {schema:.2%} < 100%")
    for cond, counts in summary.get("regression_summary", {}).items():
        if counts.get("regressed", 0) > 0:
            v.append(f"[infra/{cond}] regressions {counts['regressed']} > 0")
    for cond, r10 in summary.get("r10_per_condition", {}).items():
        score = r10.get("layer_a_score", 0.0) or 0.0
        if score < 0.55:
            v.append(f"[infra/{cond}] R10 layer_a {score:.2%} < 0.55")
    return v


def _llm_quality_violations(summary: dict[str, Any], cond: str) -> list[str]:
    metrics = summary["metrics_per_condition"].get(cond) or {}
    schema = summary["schema_pass_per_condition"].get(cond, 0.0)
    parse_fail = summary["canonical_parse_failure_rate"].get(cond, 1.0)
    regressions = (summary["regression_summary"].get(cond) or {}).get("regressed", 0)

    v: list[str] = []
    if metrics.get("evidence_grounding_composite", 0.0) < W4_TARGETS["q3_composite"]:
        v.append(f"q3_composite {metrics.get('evidence_grounding_composite', 0):.2%} < target {W4_TARGETS['q3_composite']:.2%}")
    if metrics.get("evidence_metric_grounding_ratio", 0.0) < W4_TARGETS["metric_grounded"]:
        v.append(f"metric_grounded {metrics.get('evidence_metric_grounding_ratio', 0):.2%} < target {W4_TARGETS['metric_grounded']:.2%}")
    if metrics.get("finding_support_ratio", 0.0) < W4_TARGETS["finding_support"]:
        v.append(f"finding_support {metrics.get('finding_support_ratio', 0):.2%} < target {W4_TARGETS['finding_support']:.2%}")
    if metrics.get("ungrounded_numeric_ratio", 1.0) > W4_TARGETS["ungrounded_numeric_max"]:
        v.append(f"ungrounded_numeric {metrics.get('ungrounded_numeric_ratio', 1):.2%} > max {W4_TARGETS['ungrounded_numeric_max']:.2%}")
    if metrics.get("recall_strict_ratio", 0.0) < W4_TARGETS["recall_strict"]:
        v.append(f"recall_strict {metrics.get('recall_strict_ratio', 0):.2%} < target {W4_TARGETS['recall_strict']:.2%}")
    if metrics.get("hallucination_score_avg", 0.0) < W4_TARGETS["hallucination"]:
        v.append(f"hallucination {metrics.get('hallucination_score_avg', 0):.2f} < target {W4_TARGETS['hallucination']:.2f}")
    if schema < W4_TARGETS["schema_pass"]:
        v.append(f"schema_pass {schema:.2%} < target {W4_TARGETS['schema_pass']:.2%}")
    if regressions > W4_TARGETS["case_regressions_max"]:
        v.append(f"case_regressions {regressions} > max {W4_TARGETS['case_regressions_max']}")
    if parse_fail > W4_TARGETS["canonical_parse_failure_max"]:
        v.append(f"canonical_parse_failure {parse_fail:.2%} > max {W4_TARGETS['canonical_parse_failure_max']:.2%}")
    return [f"[llm-quality/{cond}] {m}" for m in v]


_VERDICT_RANK = {"pass": 2, "borderline": 1, "fail": 0}


def _r10_verdict_violations(summary: dict[str, Any], min_verdict: str) -> list[str]:
    threshold = _VERDICT_RANK[min_verdict]
    out = []
    for cond, r10 in summary.get("r10_per_condition", {}).items():
        v = r10.get("overall_verdict", "fail")
        if _VERDICT_RANK.get(v, 0) < threshold:
            out.append(f"R10 {cond} verdict={v} < required {min_verdict}")
    return out


if __name__ == "__main__":
    sys.exit(main())
