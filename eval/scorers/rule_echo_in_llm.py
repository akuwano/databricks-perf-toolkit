"""L1: Rule-based emission echo check (Codex 5-layer model, 2026-04-26).

When a rule-based detector emits a canonical Finding (e.g.,
``decimal_heavy_aggregate`` from ``recommendations_registry``), the
LLM's narrative section should still acknowledge it. If the LLM
silently drops the rule's signal — through ``V6_CANONICAL_SCHEMA``
compression, knowledge minimization, or other pruning — the
recommendation reaches the structured Report but never the human
reader, defeating the rule.

This scorer compares:
  - Rule-emitted issue_ids (Findings with severity != 'low' that came
    from the registry, identified by their canonical issue_id).
  - LLM narrative text (executive_summary, top_alerts, action plan
    text — anything the user actually reads first).

If a rule_echo is missing, we record it. The aggregate score is
``covered / total_rule_findings`` (1.0 = clean).

Why not reuse score_recall?
  - ``recall`` measures "did we cover the goldens' must_cover_issues",
    which only fires for cases authored with golden files.
  - rule_echo is *case-independent*: it fires whenever ANY rule emits a
    Finding, regardless of whether the case has a golden.
  - Together they form a layered floor: rule_echo prevents LLM-side
    silent drop; recall ensures gold-mandated coverage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Findings whose severity is below this rank are not enforced. Rationale:
# 'low' Findings are typically advisory (e.g., result_from_cache_detected)
# and mentioning them in every narrative would clutter the output without
# improving recommendation quality.
_MIN_SEVERITY_RANK = 2  # medium and above

_SEVERITY_RANK = {"critical": 4, "high": 3, "medium": 2, "low": 1}


@dataclass
class RuleEchoScore:
    rule_finding_count: int = 0
    echoed_count: int = 0
    missed_issue_ids: list[str] = field(default_factory=list)
    score: float = 1.0  # 1.0 = clean, 0.0 = all rules dropped from narrative
    # When True, the canonical Report had no rule-emitted Findings to check
    # so the score is vacuously 1.0 (caller may choose to ignore).
    no_op: bool = False


def _collect_rule_finding_ids(canonical: dict[str, Any]) -> list[tuple[str, str]]:
    """Return ``[(issue_id, severity)]`` for Findings worth enforcing.

    "Worth enforcing" = severity at or above ``_MIN_SEVERITY_RANK`` AND
    issue_id is non-empty. We do not currently distinguish "this came
    from a rule" vs "the LLM emitted it directly under V6_CANONICAL_SCHEMA"
    because the canonical Report intentionally erases that provenance —
    however the contract is the same for both: a Finding deserves a
    narrative mention.
    """
    findings = (canonical.get("findings") or []) + (
        canonical.get("appendix_excluded_findings") or []
    )
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for f in findings:
        if not isinstance(f, dict):
            continue
        iid = str(f.get("issue_id") or "")
        sev = str(f.get("severity") or "").lower()
        if not iid or iid in seen:
            continue
        if _SEVERITY_RANK.get(sev, 0) < _MIN_SEVERITY_RANK:
            continue
        seen.add(iid)
        out.append((iid, sev))
    return out


def _issue_keywords(issue_id: str) -> tuple[str, ...]:
    """Pull canonical keywords for an issue from the registry."""
    try:
        from core.v6_schema.issue_registry import get_keywords  # noqa: WPS433
    except ImportError:
        return ()
    return tuple(get_keywords(issue_id))


def score_rule_echo(
    canonical: dict[str, Any],
    llm_narrative: str,
) -> RuleEchoScore:
    """Score whether each non-low canonical Finding is echoed in the LLM narrative.

    Args:
        canonical: full canonical Report dict (with findings[] and
            appendix_excluded_findings[]).
        llm_narrative: combined LLM-authored text. Pass everything the
            human will read first — typically:
                qr.llm_analysis_excerpt + canonical.summary.headline
            joined as a single string. The scorer is case-insensitive.

    Returns:
        RuleEchoScore with a ratio in [0, 1] and a list of dropped ids.
    """
    rule_finds = _collect_rule_finding_ids(canonical)
    if not rule_finds:
        return RuleEchoScore(no_op=True)

    # Without LLM narrative the scorer can't possibly find echoes — return
    # no_op so rule-based-only smoke runs (--skip-llm) don't pollute the
    # aggregate. Real evaluation always supplies narrative text.
    if not (llm_narrative or "").strip():
        return RuleEchoScore(rule_finding_count=len(rule_finds), no_op=True)

    haystack = (llm_narrative or "").lower()
    echoed: list[str] = []
    missed: list[str] = []
    for iid, _sev in rule_finds:
        keywords = _issue_keywords(iid) or (iid.replace("_", " "),)
        # Treat the issue_id itself as the strongest keyword — when the
        # canonical Report links by id, narrative often quotes it.
        candidate_terms = (iid.lower(), iid.replace("_", " ").lower(), *(k.lower() for k in keywords))
        if any(term in haystack for term in candidate_terms if term):
            echoed.append(iid)
        else:
            missed.append(iid)

    total = len(rule_finds)
    score = (len(echoed) / total) if total else 1.0
    return RuleEchoScore(
        rule_finding_count=total,
        echoed_count=len(echoed),
        missed_issue_ids=missed,
        score=round(score, 4),
        no_op=False,
    )


def aggregate_rule_echo(scores: list[RuleEchoScore]) -> float:
    """Average rule_echo score across cases, ignoring no_op entries."""
    real = [s for s in scores if not s.no_op]
    if not real:
        return 1.0
    return round(sum(s.score for s in real) / len(real), 4)
