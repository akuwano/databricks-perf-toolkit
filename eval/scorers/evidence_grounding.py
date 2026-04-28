"""Q3: Evidence grounding scorer (Week 3 Day 5).

Operates on a canonical Report dict (matches schemas/report_v6.schema.json)
and computes 5 signals recommended by Codex (W3 kickoff 2026-04-25):

  1. evidence_metric_grounding   — Evidence.metric matches profile-known set
  2. ungrounded_numeric_claim    — numeric mentions not anchored in profile/grounded evidence
  3. valid_source_taxonomy       — Evidence.source ∈ allowed taxonomy
  4. valid_knowledge_section_id  — knowledge:<sid> references a real knowledge section_id
  5. finding_grounded_support    — every Finding has ≥1 grounded=true non-synthetic evidence

The scorer is mechanical (no LLM) so it can run in CI gates without API keys.

See: docs/knowledge/v6_knowledge_policy.md §11 + docs/v6/output_contract.md §8
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]

# ---------------------------------------------------------------------------
# Composite weights (W3.5 #2 — Codex review reflected)
#
# Rationale: equal-weight averaging let valid_source / valid_knowledge_id
# (currently 100% by construction in the rule-based baseline) inflate the
# composite. We weight the two "core grounding" signals heavier and demote
# the two taxonomy signals to auxiliary indicators.
# ---------------------------------------------------------------------------

COMPOSITE_WEIGHTS: dict[str, float] = {
    "finding_support":  0.35,
    "metric_grounded":  0.30,
    "ungrounded_numeric_inverse": 0.20,  # uses (1 - ungrounded_ratio)
    "valid_source":     0.075,
    "valid_knowledge_id": 0.075,
}
# Sanity check at import time
assert abs(sum(COMPOSITE_WEIGHTS.values()) - 1.0) < 1e-9, (
    f"COMPOSITE_WEIGHTS must sum to 1.0, got {sum(COMPOSITE_WEIGHTS.values())}"
)

# ---------------------------------------------------------------------------
# Source taxonomy (W3 policy §8)
# ---------------------------------------------------------------------------

_VALID_SOURCE_PREFIXES = (
    "profile.",
    "node[",
    "alert:",
    "actioncard.evidence",
    "knowledge:",
    "synthetic",
)


def _source_taxonomy_valid(source: str) -> bool:
    if not source:
        return False
    s = source.strip()
    return any(s.startswith(p) for p in _VALID_SOURCE_PREFIXES)


# ---------------------------------------------------------------------------
# Numeric anchor detection
# ---------------------------------------------------------------------------

_NUMERIC_UNIT_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(GB|MB|KB|TB|秒|seconds?|ms|ミリ秒|%|％|x|倍)",
    re.IGNORECASE,
)


def _normalize_numeric_token(s: str) -> str:
    """Normalize a numeric+unit token for cross-comparison.

    The LLM often writes "97ms" while structured evidence carries
    "97 ms" (with space). Without normalization the scorer treats them
    as different tokens and inflates ungrounded_numeric_ratio. Strip
    all whitespace inside the token so "97ms" / "97 ms" / "97  ms"
    collapse to one canonical form.
    """
    return re.sub(r"\s+", "", s.lower())

# Sources that count as legitimate numeric anchors. Narrative-derived
# sources (``actioncard.evidence`` copy-pastes the LLM's own prose,
# ``synthetic`` is explicitly LLM-derived/computed) are excluded so
# the scorer can't be gamed by an LLM citing a number it just emitted.
# V5 vs V6 smoke (2026-04-26) showed V5's normalizer self-anchored
# every narrative number this way, hiding real grounding gaps.
_ANCHOR_SOURCE_EXCLUDE = ("actioncard.evidence", "synthetic")


def _is_anchor_source(source: str) -> bool:
    s = (source or "").strip().lower()
    return not any(s.startswith(p) for p in _ANCHOR_SOURCE_EXCLUDE)


# ---------------------------------------------------------------------------
# Knowledge section-id loader (cached)
# ---------------------------------------------------------------------------

_KNOWLEDGE_SECTION_IDS: set[str] | None = None
_SECTION_MARKER_RE = re.compile(r"<!--\s*section_id:\s*([A-Za-z0-9_]+)\s*-->")


def _load_known_knowledge_section_ids() -> set[str]:
    """Scan core/knowledge/*.md for `<!-- section_id: ... -->` markers."""
    global _KNOWLEDGE_SECTION_IDS
    if _KNOWLEDGE_SECTION_IDS is not None:
        return _KNOWLEDGE_SECTION_IDS
    ids: set[str] = set()
    knowledge_dir = REPO_ROOT / "dabs" / "app" / "core" / "knowledge"
    if knowledge_dir.is_dir():
        for md in knowledge_dir.glob("*.md"):
            try:
                txt = md.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for m in _SECTION_MARKER_RE.finditer(txt):
                ids.add(m.group(1))
    _KNOWLEDGE_SECTION_IDS = ids
    return ids


def reset_knowledge_cache() -> None:
    """Used by tests when knowledge files mutate."""
    global _KNOWLEDGE_SECTION_IDS
    _KNOWLEDGE_SECTION_IDS = None


# ---------------------------------------------------------------------------
# Score model
# ---------------------------------------------------------------------------


@dataclass
class EvidenceGroundingScore:
    """Per-Report Q3 score with the 5 signals."""

    # 1. metric grounding
    evidence_total: int = 0
    evidence_grounded: int = 0  # honors Evidence.grounded
    metric_grounding_ratio: float = 1.0

    # 2. ungrounded numeric
    numeric_claim_total: int = 0
    numeric_claim_unsupported: int = 0
    ungrounded_numeric_ratio: float = 0.0

    # 3. valid source taxonomy
    source_total: int = 0
    source_invalid: list[str] = field(default_factory=list)
    valid_source_ratio: float = 1.0

    # 4. valid knowledge section ids
    knowledge_refs_total: int = 0
    knowledge_refs_unknown: list[str] = field(default_factory=list)
    valid_knowledge_section_ratio: float = 1.0

    # 5. finding-level support
    findings_total: int = 0
    findings_with_grounded_support: int = 0
    finding_support_ratio: float = 1.0

    # Composite: simple equal-weight average of the 4 ratios + (1 - ungrounded)
    composite_score: float = 1.0


# ---------------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------------


def score_evidence_grounding(
    report: dict[str, Any],
    *,
    profile_known_metrics: set[str] | None = None,
) -> EvidenceGroundingScore:
    """Compute Q3 evidence grounding signals on a canonical Report dict."""
    findings = (report.get("findings") or []) + (report.get("appendix_excluded_findings") or [])

    known_metrics = {m.lower() for m in (profile_known_metrics or set())}
    known_section_ids = _load_known_knowledge_section_ids()

    score = EvidenceGroundingScore()

    # ----- 1. evidence metric grounding -----
    # 2/3. source taxonomy + numeric collection done in same loop
    profile_anchored_numeric: set[str] = set()
    sources: list[str] = []
    knowledge_refs: list[str] = []

    findings_with_support = 0
    for f in findings:
        ev = f.get("evidence") or []
        has_real_grounded = False
        for e in ev:
            score.evidence_total += 1
            if e.get("grounded"):
                score.evidence_grounded += 1
                # value_display anchors numeric claims — but only when
                # the evidence comes from a profile-derived source.
                # Narrative-derived sources (actioncard.evidence /
                # synthetic) self-anchor and would inflate the score.
                if _is_anchor_source(str(e.get("source", ""))):
                    disp = str(e.get("value_display", "")).lower()
                    for m in _NUMERIC_UNIT_RE.finditer(disp):
                        profile_anchored_numeric.add(_normalize_numeric_token(m.group(0)))
                # synthetic / non-grounded evidence does not count for support
                if e.get("source") != "synthetic":
                    has_real_grounded = True

            src = str(e.get("source", "")).strip()
            sources.append(src)
            if src.startswith("knowledge:"):
                sid = src.split(":", 1)[1].strip()
                knowledge_refs.append(sid)

            # If a metric is also unknown, count it for the metric ratio check
            metric = str(e.get("metric", "")).lower()
            if metric and known_metrics and metric not in known_metrics and not e.get("grounded"):
                # already counted by evidence_grounded != evidence_total
                pass

        if has_real_grounded:
            findings_with_support += 1

    score.findings_total = len(findings)
    score.findings_with_grounded_support = findings_with_support
    score.finding_support_ratio = (
        findings_with_support / len(findings) if findings else 1.0
    )

    if score.evidence_total > 0:
        score.metric_grounding_ratio = score.evidence_grounded / score.evidence_total
    else:
        score.metric_grounding_ratio = 1.0

    # ----- 3. valid source taxonomy -----
    score.source_total = len(sources)
    if score.source_total > 0:
        invalid = [s for s in sources if not _source_taxonomy_valid(s)]
        score.source_invalid = invalid[:20]
        score.valid_source_ratio = 1.0 - (len(invalid) / score.source_total)

    # ----- 4. valid knowledge section ids -----
    score.knowledge_refs_total = len(knowledge_refs)
    if score.knowledge_refs_total > 0:
        unknown = [sid for sid in knowledge_refs if sid not in known_section_ids]
        score.knowledge_refs_unknown = unknown[:20]
        score.valid_knowledge_section_ratio = 1.0 - (
            len(unknown) / score.knowledge_refs_total
        )

    # ----- 2. ungrounded numeric claims (gather from text) -----
    haystack_parts: list[str] = []
    summary = report.get("summary") or {}
    haystack_parts.append(str(summary.get("headline", "")))
    for km in summary.get("key_metrics") or []:
        kv = str(km.get("value_display", ""))
        haystack_parts.append(kv)
        for m in _NUMERIC_UNIT_RE.finditer(kv.lower()):
            profile_anchored_numeric.add(_normalize_numeric_token(m.group(0)))
    for f in findings:
        haystack_parts.append(str(f.get("title", "")))
        haystack_parts.append(str(f.get("description", "")))
        for a in f.get("actions") or []:
            haystack_parts.append(str(a.get("what", "")))
            haystack_parts.append(str(a.get("why", "")))
            haystack_parts.append(str(a.get("expected_effect", "")))
            haystack_parts.append(str(a.get("fix_sql", "")))
    text = " ".join(haystack_parts).lower()

    seen: set[str] = set()
    unsupported: list[str] = []
    total_numeric = 0
    for m in _NUMERIC_UNIT_RE.finditer(text):
        token = _normalize_numeric_token(m.group(0))
        if token in seen:
            continue
        seen.add(token)
        total_numeric += 1
        if token not in profile_anchored_numeric:
            unsupported.append(token)
    score.numeric_claim_total = total_numeric
    score.numeric_claim_unsupported = len(unsupported)
    score.ungrounded_numeric_ratio = (
        len(unsupported) / total_numeric if total_numeric else 0.0
    )

    # ----- composite (W3.5 #2: weighted average) -----
    score.composite_score = round(
        score.finding_support_ratio * COMPOSITE_WEIGHTS["finding_support"]
        + score.metric_grounding_ratio * COMPOSITE_WEIGHTS["metric_grounded"]
        + (1.0 - score.ungrounded_numeric_ratio) * COMPOSITE_WEIGHTS["ungrounded_numeric_inverse"]
        + score.valid_source_ratio * COMPOSITE_WEIGHTS["valid_source"]
        + score.valid_knowledge_section_ratio * COMPOSITE_WEIGHTS["valid_knowledge_id"],
        4,
    )

    return score


def aggregate_evidence_grounding(scores: list[EvidenceGroundingScore]) -> float:
    """Mean composite score across cases."""
    if not scores:
        return 1.0
    return sum(s.composite_score for s in scores) / len(scores)
