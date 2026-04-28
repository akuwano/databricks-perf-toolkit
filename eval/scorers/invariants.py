"""L2: profile-signature invariants scorer (Codex (b) recommendation, 2026-04-26).

Per-profile invariants: when a known signature appears in
ProfileEvidence, the canonical Report or LLM narrative MUST mention any
member of an acceptable remedy family (NOT a specific solution).

Why "remedy family", not "specific solution"?
  Codex flagged that pinning a specific fix (e.g., "must say CLUSTER BY")
  produces false positives when the user emits an equally valid
  alternative (ZORDER, repartition, AQE skew, "already clustered but
  ineffective"). The invariant should accept any remedy in the family.

Why is this distinct from L1 rule_echo_in_llm?
  L1 enforces "if a Finding emitted, narrative must echo it". L2
  enforces "if profile has signature X, the response (canonical OR
  narrative) must mention some remedy from family F" — even when no
  rule fired. So L2 catches gaps in rule coverage; L1 catches gaps
  between rule and narrative.

Initial invariants (3-5, expandable):
  - heavy_decimal_arithmetic        → DECIMAL/type_review family
  - dominant_shuffle_outside_lc     → clustering family
  - spill_heavy_warehouse_review    → warehouse_size/scale-up family
  - cte_recompute_materialization   → materialization family
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..profile_evidence import ProfileEvidence


# ---------------------------------------------------------------------------
# Remedy family vocabulary (mirrored from canonical_diff scorer to keep one
# source of truth for "what counts as that kind of remediation").
#
# Adding a family member only widens what counts as "covered" — a strict
# expansion. Removing one is a behavior change and requires test update.
# ---------------------------------------------------------------------------

REMEDY_FAMILY_KEYWORDS: dict[str, tuple[str, ...]] = {
    "type_review": (
        "DECIMAL", "decimal",
        "DESCRIBE TABLE",
        "BIGINT", "INT/BIGINT",
        "型最適化", "型確認", "数値型",
        "precision", "scale",
    ),
    "clustering": (
        "CLUSTER BY", "Liquid Clustering",
        "OPTIMIZE FULL", "OPTIMIZE",
        "クラスタリング",
        "ZORDER",  # alternative remedy in clustering family
    ),
    "materialization": (
        "CTAS", "CREATE OR REPLACE TABLE",
        "MATERIALIZED VIEW", "実体化", "物理化",
        "persist", "ReusedExchange",
    ),
    "warehouse_size": (
        "warehouse", "ウェアハウス", "scale up", "スケールアップ",
        "X-Large", "2X-Large", "DBU/h",
    ),
    "filter_early": (
        "Filter Early", "事前フィルタ", "JOIN 前", "predicate pushdown",
        "WHERE",
    ),
    "aqe_skew": (
        "AQE", "skewJoin", "skew join", "adaptive",
    ),
}


@dataclass
class InvariantViolation:
    invariant_id: str
    description: str
    expected_families: tuple[str, ...]
    evidence: tuple[str, ...] = ()


@dataclass
class InvariantsScore:
    fired_invariants: list[str] = field(default_factory=list)
    satisfied_invariants: list[str] = field(default_factory=list)
    violations: list[InvariantViolation] = field(default_factory=list)
    score: float = 1.0
    no_op: bool = False


# ---------------------------------------------------------------------------
# Invariant definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Invariant:
    invariant_id: str
    description: str
    # condition: (ProfileEvidence) → bool
    expected_families: tuple[str, ...]


_INVARIANTS: tuple[tuple[_Invariant, str], ...] = (
    (
        _Invariant(
            invariant_id="heavy_decimal_arithmetic",
            description=(
                "Heavy aggregate (peak >= 100 GB) with arithmetic on numeric "
                "columns — response should include type_review remedy."
            ),
            expected_families=("type_review",),
        ),
        "decimal_arithmetic_in_heavy_agg",
    ),
    (
        _Invariant(
            invariant_id="dominant_shuffle_outside_lc",
            description=(
                "Dominant shuffle key (>= 10 GB written or >= 256 MB/part) is "
                "a column of a scanned table but not in current LC keys — "
                "response should include clustering family remedy."
            ),
            expected_families=("clustering",),
        ),
        "dominant_shuffle_keys_outside_lc",
    ),
    (
        _Invariant(
            invariant_id="cte_recompute_materialization",
            description=(
                "CTE referenced 2+ times outside its WITH definition — "
                "response should discuss materialization family or "
                "ReusedExchange verification."
            ),
            expected_families=("materialization",),
        ),
        "cte_multi_reference",
    ),
    (
        _Invariant(
            invariant_id="spill_heavy_warehouse_review",
            description=(
                "Cumulative spill >= 100 GB — response should propose either "
                "memory remediation (warehouse_size) or upstream reduction "
                "(filter_early / clustering)."
            ),
            # Multiple acceptable families — any one satisfies.
            expected_families=("warehouse_size", "filter_early", "clustering"),
        ),
        "spill_dominant",
    ),
)


# ---------------------------------------------------------------------------
# Structural invariant: fix_sql is REQUIRED for SQL-action recommendations
# ---------------------------------------------------------------------------
#
# Codex 2026-04-26: when an Action's ``what`` describes a SQL action
# (ALTER TABLE / CLUSTER BY / SET TBLPROPERTIES / OPTIMIZE / ANALYZE /
# DML / SET), but ``fix_sql`` is empty, the customer-actionable SQL
# was dropped — a regression we must catch even when no profile-level
# invariant fires.
#
# Allowlist (case-insensitive substring match, treated as canonical
# SQL-action triggers). Generic "SET" alone is risky (false positives
# in prose like "set the warehouse to Large"), so SET appears only as
# "SET TBLPROPERTIES" / "SET TBLPROPERTIES (...)".

_SQL_ACTION_KEYWORDS: tuple[str, ...] = (
    "ALTER TABLE",
    "CLUSTER BY",
    "OPTIMIZE",
    "SET TBLPROPERTIES",
    "ANALYZE TABLE",
    "INSERT INTO",
    "MERGE INTO",
    "UPDATE ",
    "DELETE FROM",
    "VACUUM",
)


def _action_mentions_sql_action(text: str) -> bool:
    if not text:
        return False
    upper = text.upper()
    return any(kw in upper for kw in _SQL_ACTION_KEYWORDS)


def _check_fix_sql_required(
    canonical: dict[str, Any],
) -> tuple[bool, list[InvariantViolation]]:
    """Scan canonical Report actions for fix_sql contract violations.

    Returns ``(fired, violations)``. ``fired=True`` means at least one
    action mentioned a SQL keyword (so the invariant applies); empty
    canonical Reports / Reports with only non-SQL actions remain no-op.
    """
    fired = False
    violations: list[InvariantViolation] = []
    findings = (canonical.get("findings") or []) + (
        canonical.get("appendix_excluded_findings") or []
    )
    offending: list[str] = []
    for f in findings:
        for a in f.get("actions") or []:
            what = str(a.get("what") or "")
            fix_sql = str(a.get("fix_sql") or "").strip()
            if not _action_mentions_sql_action(what):
                continue
            fired = True
            if not fix_sql:
                offending.append(what[:160])
    if offending:
        violations.append(
            InvariantViolation(
                invariant_id="fix_sql_required_for_sql_actions",
                description=(
                    "Action 'what' mentions a SQL action (ALTER/CLUSTER BY/"
                    "OPTIMIZE/SET TBLPROPERTIES/ANALYZE/DML) but 'fix_sql' "
                    "is empty. Customers need the literal SQL command, "
                    "not prose-only advice."
                ),
                expected_families=("fix_sql_populated",),
                evidence=tuple(offending),
            )
        )
    return fired, violations


def _haystack_from(canonical: dict[str, Any], llm_narrative: str) -> str:
    """Combine canonical Report text + LLM narrative into one lowered blob.

    L2 invariants accept either source — a structured Action.what or a
    LLM exec summary mention both count as "the family was addressed".
    """
    parts: list[str] = [llm_narrative or ""]
    summary = canonical.get("summary") or {}
    parts.append(str(summary.get("headline", "")))
    for f in (canonical.get("findings") or []) + (
        canonical.get("appendix_excluded_findings") or []
    ):
        parts.append(str(f.get("title", "")))
        parts.append(str(f.get("description", "")))
        for a in (f.get("actions") or []):
            parts.append(str(a.get("what", "")))
            parts.append(str(a.get("why", "")))
            parts.append(str(a.get("expected_effect", "")))
            parts.append(str(a.get("fix_sql", "")))
    return " ".join(parts).lower()


def _family_satisfied(family: str, haystack_lc: str) -> bool:
    needles = REMEDY_FAMILY_KEYWORDS.get(family, ())
    return any(n.lower() in haystack_lc for n in needles)


def score_invariants(
    evidence: ProfileEvidence,
    canonical: dict[str, Any] | None,
    llm_narrative: str = "",
) -> InvariantsScore:
    """Run all invariants against the given evidence + report.

    A no-op result is returned when no invariant fires (the profile
    didn't trigger any signature). Empty narrative is OK — invariants
    can still be satisfied by structured Action.what / fix_sql text.

    Penalty model: each violation subtracts 0.25; clamped to [0, 1].
    """
    canonical = canonical or {}
    fired: list[str] = []
    satisfied: list[str] = []
    violations: list[InvariantViolation] = []

    haystack = _haystack_from(canonical, llm_narrative)

    for invariant, attr_name in _INVARIANTS:
        if not getattr(evidence, attr_name, False):
            continue
        fired.append(invariant.invariant_id)
        if any(_family_satisfied(fam, haystack) for fam in invariant.expected_families):
            satisfied.append(invariant.invariant_id)
        else:
            ev_tuple = ()
            if invariant.invariant_id == "heavy_decimal_arithmetic":
                ev_tuple = tuple(f"{n}: {e}" for n, e in evidence.decimal_arithmetic_examples)
            elif invariant.invariant_id == "dominant_shuffle_outside_lc":
                ev_tuple = tuple(f"{t}.{c}" for t, c in evidence.dominant_shuffle_outside_lc_columns)
            elif invariant.invariant_id == "cte_recompute_materialization":
                ev_tuple = tuple(f"{n} (refs={r})" for n, r in evidence.cte_multi_reference_names)
            elif invariant.invariant_id == "spill_heavy_warehouse_review":
                gb = evidence.spill_total_bytes / (1024**3)
                ev_tuple = (f"spill_total={gb:.1f} GB",)
            violations.append(
                InvariantViolation(
                    invariant_id=invariant.invariant_id,
                    description=invariant.description,
                    expected_families=invariant.expected_families,
                    evidence=ev_tuple,
                )
            )

    # Structural invariant: fix_sql required for SQL-action recommendations.
    # This fires even when no profile-signature invariant did, because
    # it scores the *output* contract, not a profile pattern.
    fix_sql_fired, fix_sql_violations = _check_fix_sql_required(canonical)
    if fix_sql_fired:
        fired.append("fix_sql_required_for_sql_actions")
        if fix_sql_violations:
            violations.extend(fix_sql_violations)
        else:
            satisfied.append("fix_sql_required_for_sql_actions")

    if not fired:
        return InvariantsScore(no_op=True)

    score = max(0.0, 1.0 - 0.25 * len(violations))
    return InvariantsScore(
        fired_invariants=fired,
        satisfied_invariants=satisfied,
        violations=violations,
        score=round(score, 4),
        no_op=False,
    )


def aggregate_invariants(scores: list[InvariantsScore]) -> float:
    real = [s for s in scores if not s.no_op]
    if not real:
        return 1.0
    return round(sum(s.score for s in real) / len(real), 4)
