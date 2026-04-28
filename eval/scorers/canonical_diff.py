"""Mechanical baseline-vs-current drop detection (Codex (e) recommendation).

Designed to detect the V5→V6 style regression where the new version drops
high-value recommendations the old version was emitting (DECIMAL type
review, dominant shuffle key LC add, etc.). The existing diff_judge.py
asks an LLM for a 1-5 coverage score; this module adds a deterministic
"did X disappear" signal that runs alongside it.

Scope: works on either raw ActionCard lists (current diff_runner shape)
or canonical Report dicts (forward-looking). Returns a structured
`CanonicalDiffScore` so the downstream verdict can weight individual
drop classes.

Design notes:
- Remedy family taxonomy is intentionally compact and additive — start
  with the families V5/V6 disagreed on, grow as new regressions surface.
- Drop = "baseline mentioned family F, current did not". A new family
  appearing in current is recorded as `new_*` but does not penalize.
- Issue-id drops use canonical Finding.issue_id when available; fall back
  to None (unknown) when the input is plain ActionCards.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Remedy family taxonomy
#
# Each entry: family_id → list of case-insensitive substrings.
# A card "matches" a family if any substring appears in its concatenated
# text fields (problem / fix / fix_sql / expected_impact / what / why).
#
# Designed to be additive — adding a family only narrows what counts as
# "covered". Removing one is a behavior change and requires test update.
# ---------------------------------------------------------------------------
REMEDY_FAMILIES: dict[str, tuple[str, ...]] = {
    # Numeric type optimization (V5 caught for Q23, V6 dropped)
    "type_review": (
        "DECIMAL",
        "DESCRIBE TABLE",
        "BIGINT",
        "型最適化",
        "型確認",
        "数値型",
    ),
    # Liquid Clustering DDL recommendations
    "clustering": (
        "CLUSTER BY",
        "Liquid Clustering",
        "OPTIMIZE FULL",
        "クラスタリング",
    ),
    # Hierarchical Clustering specifically (separate from generic clustering
    # because the canonical-vs-legacy property name matters)
    "hierarchical_clustering": (
        "delta.liquid.hierarchicalClusteringColumns",
        "Hierarchical Clustering",
        "階層化",
    ),
    # CTE / view materialization fixes
    "materialization": (
        "CTAS",
        "CREATE OR REPLACE TABLE",
        "MATERIALIZED VIEW",
        "実体化",
        "物理化",
        "persist",
    ),
    # Filter Early / predicate pushdown
    "filter_early": (
        "Filter Early",
        "事前フィルタ",
        "WHERE",
        "JOIN 前",
        "predicate pushdown",
    ),
    # AQE / skew handling
    "aqe_skew": (
        "AQE",
        "skewJoin",
        "skew join",
        "adaptive",
    ),
    # Broadcast / shuffle reduction
    "broadcast": (
        "BROADCAST",
        "autoBroadcastJoinThreshold",
        "ブロードキャスト",
    ),
    # Repartition / shuffle hint
    "repartition": (
        "REPARTITION",
        "shuffle.partitions",
    ),
    # Compression / file layout
    "compression": (
        "zstd",
        "snappy",
        "compression",
        "圧縮",
    ),
    # Warehouse sizing
    "warehouse_size": (
        "warehouse",
        "ウェアハウス",
        "ウェアハウスサイズ",
    ),
}

# Severity rank for prioritizing missed issue_ids
_SEVERITY_RANK = {"critical": 4, "high": 3, "medium": 2, "low": 1}


@dataclass
class CanonicalDiffScore:
    """Mechanical diff between baseline and current recommendation sets."""

    baseline_card_count: int = 0
    current_card_count: int = 0

    # Remedy family coverage
    baseline_families: set[str] = field(default_factory=set)
    current_families: set[str] = field(default_factory=set)
    dropped_families: list[str] = field(default_factory=list)
    new_families: list[str] = field(default_factory=list)

    # Issue-id coverage (when canonical Reports available)
    baseline_issue_ids: set[str] = field(default_factory=set)
    current_issue_ids: set[str] = field(default_factory=set)
    dropped_issue_ids: list[str] = field(default_factory=list)
    new_issue_ids: list[str] = field(default_factory=list)
    dropped_high_severity_issue_ids: list[str] = field(default_factory=list)

    # Aggregate score: 1.0 if no drops, penalized by each dropped family /
    # high-severity issue_id. New families do not penalize.
    score: float = 1.0
    summary: str = ""


def _card_text(card: Any) -> str:
    """Concatenate text fields from an ActionCard or canonical Action."""
    if isinstance(card, dict):
        # canonical Action shape
        parts = [
            card.get("what", ""),
            card.get("why", ""),
            card.get("expected_effect", ""),
            card.get("fix_sql", ""),
        ]
    else:
        parts = [
            getattr(card, "problem", ""),
            getattr(card, "fix", ""),
            getattr(card, "fix_sql", ""),
            getattr(card, "expected_impact", ""),
            getattr(card, "likely_cause", ""),
        ]
    return " ".join(p or "" for p in parts)


def _extract_families(cards: list[Any]) -> set[str]:
    """Return the set of remedy families covered by `cards`."""
    if not cards:
        return set()
    blob = " ".join(_card_text(c) for c in cards).lower()
    found: set[str] = set()
    for family, needles in REMEDY_FAMILIES.items():
        if any(n.lower() in blob for n in needles):
            found.add(family)
    return found


def _extract_issue_ids(canonical: dict[str, Any] | None) -> dict[str, str]:
    """Map issue_id → severity from a canonical Report dict.

    Returns empty when the input is None / not a canonical Report.
    """
    if not canonical:
        return {}
    out: dict[str, str] = {}
    for f in (canonical.get("findings") or []):
        iid = f.get("issue_id")
        if iid:
            out[str(iid)] = str(f.get("severity") or "").lower()
    for f in (canonical.get("appendix_excluded_findings") or []):
        iid = f.get("issue_id")
        if iid and iid not in out:
            out[str(iid)] = str(f.get("severity") or "").lower()
    return out


def score_canonical_diff(
    baseline_cards: list[Any],
    current_cards: list[Any],
    *,
    baseline_canonical: dict[str, Any] | None = None,
    current_canonical: dict[str, Any] | None = None,
    family_drop_penalty: float = 0.10,
    high_severity_drop_penalty: float = 0.15,
) -> CanonicalDiffScore:
    """Compute mechanical drop signals between baseline and current.

    Args:
        baseline_cards: ActionCards (or canonical Action dicts) from the
            baseline run.
        current_cards: same shape, current run.
        baseline_canonical / current_canonical: optional canonical Report
            dicts. When supplied, issue_id-level drops are also reported.
        family_drop_penalty: penalty subtracted per dropped remedy family.
        high_severity_drop_penalty: penalty per high/critical issue_id drop.

    Returns:
        CanonicalDiffScore. score is clamped to [0, 1].
    """
    base_fams = _extract_families(baseline_cards)
    curr_fams = _extract_families(current_cards)
    dropped_fams = sorted(base_fams - curr_fams)
    new_fams = sorted(curr_fams - base_fams)

    base_iids = _extract_issue_ids(baseline_canonical)
    curr_iids = _extract_issue_ids(current_canonical)
    dropped_iids = sorted(set(base_iids) - set(curr_iids))
    new_iids = sorted(set(curr_iids) - set(base_iids))
    dropped_hi = sorted(
        iid for iid in dropped_iids
        if _SEVERITY_RANK.get(base_iids.get(iid, ""), 0) >= 3
    )

    penalty = (
        family_drop_penalty * len(dropped_fams)
        + high_severity_drop_penalty * len(dropped_hi)
    )
    score = max(0.0, min(1.0, 1.0 - penalty))

    parts: list[str] = []
    if dropped_fams:
        parts.append(f"dropped_families={dropped_fams}")
    if dropped_hi:
        parts.append(f"dropped_high_severity_ids={dropped_hi}")
    if not parts:
        parts.append("no drops detected")
    summary = "; ".join(parts)

    return CanonicalDiffScore(
        baseline_card_count=len(baseline_cards or []),
        current_card_count=len(current_cards or []),
        baseline_families=base_fams,
        current_families=curr_fams,
        dropped_families=dropped_fams,
        new_families=new_fams,
        baseline_issue_ids=set(base_iids),
        current_issue_ids=set(curr_iids),
        dropped_issue_ids=dropped_iids,
        new_issue_ids=new_iids,
        dropped_high_severity_issue_ids=dropped_hi,
        score=round(score, 4),
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Optional helper: aggregate per-profile diff scores into one number
# (used by stage_gate / regression_detector if they want a single gate).
# ---------------------------------------------------------------------------
def aggregate_canonical_diff(scores: list[CanonicalDiffScore]) -> float:
    if not scores:
        return 1.0
    return round(sum(s.score for s in scores) / len(scores), 4)
