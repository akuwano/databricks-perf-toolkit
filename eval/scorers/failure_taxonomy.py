"""Q5: Failure taxonomy scorer (Week 5 Day 5).

Categorizes the failure modes that the deterministic Layer-A scorers
have already detected, so we can attribute *why* a case scored low
(rather than just *that* it scored low).

Codex W4 review §6:
> Q5 failure taxonomy は「parse 失敗・根拠不足・誤検知・過剰提案」
> を分離して回帰原因を潰せる形にする

Five categories (matching docs/v6/sql_skeleton_design.md §10):

| category              | trigger condition                                   |
|-----------------------|----------------------------------------------------|
| parse_failure         | SQL skeleton method ∈ {head_tail, truncate}         |
| evidence_unsupported  | Finding.evidence has zero grounded=true items       |
| false_positive        | suppression should apply but Finding emitted        |
| over_recommendation   | a single Finding has > 3 actions                    |
| missing_critical      | golden must_cover_issues[id] not present in report  |

This scorer takes a canonical Report dict + (optional) golden case
metadata and returns a FailureTaxonomyScore that downstream R10 can
read.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

CATEGORIES = (
    "parse_failure",
    "evidence_unsupported",
    "false_positive",
    "over_recommendation",
    "missing_critical",
)

# Default penalties per category (per-incident, not per-case).
# W6 Day 4 (Codex W5 #5): parse_failure was 0.10 in W5 — Codex flagged
# this as too light because skeleton becomes the primary action grounding.
# Bumped to 0.15 to align with the impact of evidence_unsupported.
DEFAULT_PENALTIES: dict[str, float] = {
    "parse_failure":         0.15,
    "evidence_unsupported":  0.20,
    "false_positive":        0.15,
    "over_recommendation":   0.05,
    "missing_critical":      0.30,
}


@dataclass
class FailureTaxonomyScore:
    """Per-report failure taxonomy result."""

    counts: dict[str, int] = field(default_factory=lambda: {c: 0 for c in CATEGORIES})
    incidents: list[dict[str, Any]] = field(default_factory=list)
    score: float = 1.0  # 1.0 clean, 0.0 catastrophic
    by_category_penalty: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Trigger-detection helpers
# ---------------------------------------------------------------------------


def _has_grounded_evidence(finding: dict[str, Any]) -> bool:
    for ev in finding.get("evidence") or []:
        if ev.get("grounded") and ev.get("source") != "synthetic":
            return True
    return False


def _is_suppressed(finding: dict[str, Any]) -> bool:
    return bool(finding.get("suppressed"))


def _action_count(finding: dict[str, Any]) -> int:
    return len(finding.get("actions") or [])


def _skeleton_method(action: dict[str, Any]) -> str:
    return str(action.get("fix_sql_skeleton_method") or "")


# ---------------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------------


def score_failure_taxonomy(
    report: dict[str, Any],
    *,
    must_cover_issues: list[dict[str, Any]] | None = None,
    suppression_expected: list[str] | None = None,
    penalties: dict[str, float] | None = None,
    over_recommendation_threshold: int = 3,
) -> FailureTaxonomyScore:
    """Categorize failure modes in a canonical Report.

    Args:
        report: canonical Report dict.
        must_cover_issues: list[{id, severity, ...}] from golden case yaml;
            used for `missing_critical` detection.
        suppression_expected: list of finding.issue_id values that the
            golden expects to be suppressed (in appendix_excluded_findings).
            Used for `false_positive` detection.
        penalties: per-category penalty override.
        over_recommendation_threshold: action count threshold (default 3).

    Returns:
        FailureTaxonomyScore with counts/incidents/score.
    """
    must_cover_issues = must_cover_issues or []
    suppression_expected = set(suppression_expected or [])
    penalties = penalties or DEFAULT_PENALTIES

    score_obj = FailureTaxonomyScore()
    counts: Counter = Counter()
    incidents: list[dict[str, Any]] = []

    findings_active = report.get("findings") or []
    findings_appendix = report.get("appendix_excluded_findings") or []
    all_findings = findings_active + findings_appendix

    # ----- 1. parse_failure (per Action) -----
    for f in all_findings:
        for a in f.get("actions") or []:
            method = _skeleton_method(a)
            if method in {"head_tail", "truncate"}:
                counts["parse_failure"] += 1
                incidents.append({
                    "category": "parse_failure",
                    "issue_id": f.get("issue_id"),
                    "method": method,
                    "action_id": a.get("action_id"),
                })

    # ----- 2. evidence_unsupported (per Finding in active list) -----
    for f in findings_active:
        if not _has_grounded_evidence(f):
            counts["evidence_unsupported"] += 1
            incidents.append({
                "category": "evidence_unsupported",
                "issue_id": f.get("issue_id"),
                "title": (f.get("title") or "")[:80],
            })

    # ----- 3. false_positive (per Finding in active list) -----
    for f in findings_active:
        iid = f.get("issue_id", "")
        if iid in suppression_expected:
            counts["false_positive"] += 1
            incidents.append({
                "category": "false_positive",
                "issue_id": iid,
                "should_be_suppressed_in": "appendix_excluded_findings",
            })

    # ----- 4. over_recommendation (per Finding) -----
    for f in findings_active:
        n = _action_count(f)
        if n > over_recommendation_threshold:
            counts["over_recommendation"] += 1
            incidents.append({
                "category": "over_recommendation",
                "issue_id": f.get("issue_id"),
                "action_count": n,
                "threshold": over_recommendation_threshold,
            })

    # ----- 5. missing_critical (per golden must_cover_issues) -----
    if must_cover_issues:
        emitted_ids = {
            f.get("issue_id") for f in all_findings if f.get("issue_id")
        }
        for issue in must_cover_issues:
            iid = issue.get("id", "")
            severity = (issue.get("severity") or "").lower()
            if not iid:
                continue
            if iid in emitted_ids:
                continue
            # Only critical/high count as "missing_critical"; lower severity
            # missing is captured by general recall metric.
            if severity in {"critical", "high"}:
                counts["missing_critical"] += 1
                incidents.append({
                    "category": "missing_critical",
                    "issue_id": iid,
                    "severity": severity,
                })

    # ----- aggregate score -----
    score = 1.0
    by_cat: dict[str, float] = {}
    for cat in CATEGORIES:
        n = counts.get(cat, 0)
        penalty = penalties.get(cat, 0.0) * n
        by_cat[cat] = round(penalty, 4)
        score -= penalty
    score = max(0.0, round(score, 4))

    score_obj.counts = {c: counts.get(c, 0) for c in CATEGORIES}
    score_obj.incidents = incidents
    score_obj.score = score
    score_obj.by_category_penalty = by_cat
    return score_obj


def aggregate_failure_taxonomy(scores: list[FailureTaxonomyScore]) -> dict[str, Any]:
    """Aggregate over a batch of cases."""
    if not scores:
        return {
            "avg_score": 1.0,
            "total_counts": {c: 0 for c in CATEGORIES},
            "cases_with_any_incident": 0,
        }
    total_counts = {c: 0 for c in CATEGORIES}
    total_score = 0.0
    cases_with_any = 0
    for s in scores:
        for c in CATEGORIES:
            total_counts[c] += s.counts.get(c, 0)
        total_score += s.score
        if any(s.counts.get(c, 0) > 0 for c in CATEGORIES):
            cases_with_any += 1
    return {
        "avg_score": round(total_score / len(scores), 4),
        "total_counts": total_counts,
        "cases_with_any_incident": cases_with_any,
    }
