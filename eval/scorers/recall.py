"""Critical issue recall scorer.

W2.5 #6 expansion:
- Pull keywords from `core.v6_schema.issue_registry` when the golden case
  doesn't carry its own (avoids duplication, single source of truth).
- An issue is "covered" if ANY of:
    a. canonical Report has a Finding whose issue_id matches (strong)
    b. any keyword appears in the searchable haystack (fallback)
- Adds a `score_canonical_recall` helper that consumes the canonical
  Report directly so we can detect false negatives like "spill_dominant
  was supposed to be reported but no Finding emitted it".

See: docs/eval/report_quality_rubric.md section 5
"""

from __future__ import annotations

from typing import Any

from ..models import RecallScore


def _issue_keywords(issue_id: str, fallback: list[str] | None) -> list[str]:
    """Return search keywords for an issue, preferring registry over yaml."""
    fallback = list(fallback or [])
    try:
        from core.v6_schema.issue_registry import get_keywords  # noqa: WPS433
    except ImportError:
        return fallback
    reg = list(get_keywords(issue_id))
    # Combine — yaml-provided keywords are still respected.
    seen: set[str] = set()
    out: list[str] = []
    for k in fallback + reg:
        kl = k.lower()
        if kl in seen:
            continue
        seen.add(kl)
        out.append(k)
    return out


def score_recall(
    report_text: str,
    cards: list,
    must_cover_issues: list[dict],
    *,
    canonical_report: dict | None = None,
) -> RecallScore:
    """Compute critical issue recall for one query.

    Args:
        report_text: Full report markdown (or empty if no markdown).
        cards: List of ActionCards (each with text fields).
        must_cover_issues: From golden case yaml. Each entry has
            {id, severity, keywords?, description?}.
        canonical_report: Optional canonical Report dict — when supplied,
            an emitted Finding.issue_id == required id counts as covered
            (true-positive recall on the structured layer, not just text).

    Returns:
        RecallScore with recall_ratio in [0.0, 1.0].
    """
    if not must_cover_issues:
        return RecallScore(must_cover_count=0, covered_count=0, recall_ratio=1.0)

    haystack = (report_text or "").lower()
    for card in cards:
        for attr in ("problem", "fix_sql", "rationale", "expected_impact", "evidence"):
            v = getattr(card, attr, "") or ""
            haystack += " " + str(v).lower()

    emitted_issue_ids: set[str] = set()
    if canonical_report:
        for f in (canonical_report.get("findings") or []):
            iid = f.get("issue_id")
            if iid:
                emitted_issue_ids.add(str(iid))
        for f in (canonical_report.get("appendix_excluded_findings") or []):
            iid = f.get("issue_id")
            if iid:
                emitted_issue_ids.add(str(iid))

    covered: list[str] = []
    missed: list[str] = []
    for issue in must_cover_issues:
        issue_id = issue.get("id", "")
        keywords = _issue_keywords(issue_id, issue.get("keywords"))

        # 1. Strong: canonical Finding with same issue_id
        if issue_id and issue_id in emitted_issue_ids:
            covered.append(issue_id)
            continue
        # 2. Fallback: keyword presence in haystack
        if keywords and any(kw.lower() in haystack for kw in keywords):
            covered.append(issue_id)
        else:
            missed.append(issue_id)

    total = len(must_cover_issues)
    return RecallScore(
        must_cover_count=total,
        covered_count=len(covered),
        missed_issues=missed,
        recall_ratio=(len(covered) / total) if total else 1.0,
    )


def score_canonical_recall(
    canonical_report: dict[str, Any],
    must_cover_issues: list[dict],
) -> RecallScore:
    """Recall purely from canonical Report.findings (no keyword fallback).

    Stricter than `score_recall`: only true-positive Finding emissions
    count. Useful for false-negative detection in Week 3+ regression gates.
    """
    if not must_cover_issues:
        return RecallScore(must_cover_count=0, covered_count=0, recall_ratio=1.0)
    emitted = set()
    for f in (canonical_report.get("findings") or []):
        iid = f.get("issue_id")
        if iid:
            emitted.add(str(iid))
    for f in (canonical_report.get("appendix_excluded_findings") or []):
        iid = f.get("issue_id")
        if iid:
            emitted.add(str(iid))

    covered = []
    missed = []
    for issue in must_cover_issues:
        iid = issue.get("id", "")
        if iid and iid in emitted:
            covered.append(iid)
        else:
            missed.append(iid)
    total = len(must_cover_issues)
    return RecallScore(
        must_cover_count=total,
        covered_count=len(covered),
        missed_issues=missed,
        recall_ratio=(len(covered) / total) if total else 1.0,
    )
