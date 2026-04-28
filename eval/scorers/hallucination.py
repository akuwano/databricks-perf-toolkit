"""Hallucination detection scorer.

Mechanical first pass (W2.5 #4 expanded):
1. Check forbidden_claims keywords against report text.
2. Numeric claims with units (GB/MB/%) that don't appear in profile_evidence.
3. Penalty for findings whose evidence is mostly grounded=false.

Week 3+ will add LLM-as-judge for nuanced cases (DBSQL knowledge violations,
contextual claim verification).

See: docs/eval/report_quality_rubric.md section 3
"""

from __future__ import annotations

import re
from typing import Any

from ..models import HallucinationScore

# Patterns to extract numeric value claims from card text
# Example matches: "12.5GB", "3.2 seconds", "45%", "100x"
_NUMERIC_CLAIM_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?)\s*(GB|MB|KB|秒|seconds?|ms|ミリ秒|%|％|x|倍)",
    re.IGNORECASE,
)


def score_hallucination(
    card: Any,
    forbidden_claims: list[dict] | None = None,
    profile_evidence: dict | None = None,
) -> HallucinationScore:
    """Score one ActionCard for hallucination.

    Args:
        card: ActionCard with problem/fix_sql/rationale/evidence text fields.
        forbidden_claims: list of {id, description} from golden case yaml.
        profile_evidence: dict of allowed numeric values, table names, etc.
            Stub for Week 1; full impl in Week 3 (R6 evidence grounding).

    Returns:
        HallucinationScore with score=1.0 (clean) to 0.0 (severe).
    """
    forbidden_claims = forbidden_claims or []
    profile_evidence = profile_evidence or {}

    text = " ".join(
        [
            getattr(card, "problem", "") or "",
            getattr(card, "fix_sql", "") or "",
            getattr(card, "rationale", "") or "",
            getattr(card, "expected_impact", "") or "",
            getattr(card, "evidence", "") or "",
        ]
    )

    forbidden_hits: list[str] = []
    for claim in forbidden_claims:
        # forbidden_claim entries have id + description; check for description
        # keywords in the report. Week 1 stub uses simple keyword search.
        description = claim.get("description", "") if isinstance(claim, dict) else str(claim)
        claim_id = claim.get("id", "") if isinstance(claim, dict) else ""

        # Heuristic: if id contains "_recommendation" and the keyword
        # (e.g. "federation", "TEMP VIEW") appears in the report, flag.
        keyword = claim_id.replace("_recommendation", "").replace("_", " ").strip().lower()
        if keyword and keyword in text.lower():
            forbidden_hits.append(claim_id or description[:60])

    # Unsupported numeric claims (Week 1 stub: just collect, don't penalize yet
    # since profile_evidence integration comes in Week 3)
    unsupported_values: list[str] = []
    if profile_evidence.get("allowed_numeric_substrings"):
        allowed = profile_evidence["allowed_numeric_substrings"]
        for match in _NUMERIC_CLAIM_PATTERN.finditer(text):
            full = match.group(0)
            if not any(a in full for a in allowed):
                unsupported_values.append(full)

    # Score: 1.0 baseline; subtract 0.3 per forbidden hit, 0.1 per unsupported value
    penalty = 0.3 * len(forbidden_hits) + 0.1 * len(unsupported_values)
    score = max(0.0, 1.0 - penalty)

    return HallucinationScore(
        card_index=getattr(card, "card_index", 0) or 0,
        forbidden_claim_hits=forbidden_hits,
        unsupported_value_claims=unsupported_values,
        score=score,
    )


def aggregate_hallucination(scores: list[HallucinationScore]) -> float:
    """Query-level avg hallucination score (1.0 clean → 0.0 severe)."""
    if not scores:
        return 1.0
    return sum(s.score for s in scores) / len(scores)


# ---------------------------------------------------------------------------
# Canonical Report direct hallucination scoring (W2.5 #4)
#
# Combines 3 signals:
#   forbidden_claim hits (Week 1)        — penalty 0.3 each
#   ungrounded evidence ratio            — penalty (0..0.3) proportional
#   ungrounded numeric claim             — penalty 0.05 each
# ---------------------------------------------------------------------------

# Numeric claim units we expect to be grounded
_NUMERIC_UNIT_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(GB|MB|KB|TB|秒|seconds?|ms|ミリ秒|%|％)",
    re.IGNORECASE,
)


def _normalize_numeric_token(s: str) -> str:
    """Strip whitespace inside the numeric+unit token so "97ms" and
    "97 ms" compare equal. Mirrors evidence_grounding._normalize_numeric_token."""
    return re.sub(r"\s+", "", s.lower())


def score_canonical_report_hallucination(
    report: dict,
    forbidden_claims: list[dict] | None = None,
) -> HallucinationScore:
    """Score the entire canonical Report for hallucination signals."""
    forbidden_claims = forbidden_claims or []

    findings = report.get("findings") or []
    appendix = report.get("appendix_excluded_findings") or []
    all_findings = findings + appendix

    # Aggregate text and evidence list
    haystack_parts: list[str] = []
    evidence_total = 0
    evidence_grounded = 0
    profile_numeric_displays: set[str] = set()

    summary = report.get("summary") or {}
    haystack_parts.append(str(summary.get("headline", "")))
    for km in summary.get("key_metrics") or []:
        v = str(km.get("value_display", ""))
        haystack_parts.append(v)
        # treat key_metrics displays as ground-truth numeric anchors
        for m in _NUMERIC_UNIT_RE.finditer(v):
            profile_numeric_displays.add(_normalize_numeric_token(m.group(0)))

    for f in all_findings:
        for e in f.get("evidence") or []:
            evidence_total += 1
            if e.get("grounded"):
                evidence_grounded += 1
                disp = str(e.get("value_display", "")).lower()
                for m in _NUMERIC_UNIT_RE.finditer(disp):
                    profile_numeric_displays.add(_normalize_numeric_token(m.group(0)))
            haystack_parts.append(str(e.get("value_display", "")))
        haystack_parts.append(str(f.get("title", "")))
        haystack_parts.append(str(f.get("description", "")))
        for a in f.get("actions") or []:
            haystack_parts.append(str(a.get("what", "")))
            haystack_parts.append(str(a.get("why", "")))
            haystack_parts.append(str(a.get("expected_effect", "")))
            haystack_parts.append(str(a.get("fix_sql", "")))

    text = " ".join(haystack_parts)
    text_lc = text.lower()

    # 1. Forbidden claim hits
    forbidden_hits: list[str] = []
    for claim in forbidden_claims:
        if not isinstance(claim, dict):
            continue
        cid = claim.get("id", "")
        keyword = cid.replace("_recommendation", "").replace("_", " ").strip().lower()
        if keyword and keyword in text_lc:
            forbidden_hits.append(cid)

    # 2. Numeric claims that are not anchored in any grounded evidence
    unsupported_values: list[str] = []
    seen_numeric: set[str] = set()
    for m in _NUMERIC_UNIT_RE.finditer(text_lc):
        token = _normalize_numeric_token(m.group(0))
        if token in seen_numeric:
            continue
        seen_numeric.add(token)
        if token not in profile_numeric_displays:
            unsupported_values.append(token)

    # 3. Ungrounded evidence ratio
    ungrounded_ratio = 0.0
    if evidence_total > 0:
        ungrounded_ratio = 1.0 - (evidence_grounded / evidence_total)

    forbidden_penalty = 0.3 * len(forbidden_hits)
    numeric_penalty = 0.05 * len(unsupported_values)
    grounded_penalty = 0.3 * ungrounded_ratio

    score = max(0.0, 1.0 - (forbidden_penalty + numeric_penalty + grounded_penalty))

    return HallucinationScore(
        card_index=0,
        forbidden_claim_hits=forbidden_hits,
        unsupported_value_claims=unsupported_values[:20],
        score=round(score, 4),
    )
