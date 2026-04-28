"""Tests for Q5 failure taxonomy (Week 5 Day 5)."""

from __future__ import annotations

from eval.scorers.failure_taxonomy import (
    CATEGORIES,
    aggregate_failure_taxonomy,
    score_failure_taxonomy,
)


def _empty_report(active_findings=None, appendix=None):
    return {
        "schema_version": "v6.0",
        "summary": {"headline": "x", "verdict": "informational"},
        "findings": active_findings or [],
        "appendix_excluded_findings": appendix or [],
    }


# ----- parse_failure -----


def test_parse_failure_counted_per_action():
    report = _empty_report([
        {
            "issue_id": "x",
            "evidence": [{"metric": "m", "value_display": "1", "grounded": True, "source": "profile.x"}],
            "actions": [
                {"action_id": "a1", "fix_sql_skeleton_method": "head_tail"},
                {"action_id": "a2", "fix_sql_skeleton_method": "truncate"},
                {"action_id": "a3", "fix_sql_skeleton_method": "sqlglot"},
            ],
        }
    ])
    s = score_failure_taxonomy(report)
    assert s.counts["parse_failure"] == 2
    assert s.score < 1.0


# ----- evidence_unsupported -----


def test_evidence_unsupported_when_only_synthetic():
    report = _empty_report([
        {
            "issue_id": "x",
            "evidence": [{"metric": "et", "value_display": "y", "grounded": False, "source": "synthetic"}],
            "actions": [],
        }
    ])
    s = score_failure_taxonomy(report)
    assert s.counts["evidence_unsupported"] == 1


def test_evidence_supported_when_grounded():
    report = _empty_report([
        {
            "issue_id": "x",
            "evidence": [
                {"metric": "m", "value_display": "1", "grounded": True, "source": "profile.x"},
                {"metric": "et", "value_display": "y", "grounded": False, "source": "synthetic"},
            ],
            "actions": [],
        }
    ])
    s = score_failure_taxonomy(report)
    assert s.counts["evidence_unsupported"] == 0


# ----- false_positive -----


def test_false_positive_when_should_be_suppressed():
    report = _empty_report([
        {
            "issue_id": "low_file_pruning",  # appears in forbidden_claims of federation case
            "evidence": [{"metric": "m", "value_display": "1", "grounded": True, "source": "profile.x"}],
            "actions": [],
        }
    ])
    s = score_failure_taxonomy(
        report,
        suppression_expected=["low_file_pruning"],
    )
    assert s.counts["false_positive"] == 1


# ----- over_recommendation -----


def test_over_recommendation_per_finding():
    report = _empty_report([
        {
            "issue_id": "x",
            "evidence": [{"metric": "m", "value_display": "1", "grounded": True, "source": "profile.x"}],
            "actions": [{"action_id": f"a{i}"} for i in range(5)],  # 5 actions
        }
    ])
    s = score_failure_taxonomy(report, over_recommendation_threshold=3)
    assert s.counts["over_recommendation"] == 1


def test_over_recommendation_within_threshold():
    report = _empty_report([
        {
            "issue_id": "x",
            "evidence": [{"metric": "m", "value_display": "1", "grounded": True, "source": "profile.x"}],
            "actions": [{"action_id": "a1"}, {"action_id": "a2"}, {"action_id": "a3"}],
        }
    ])
    s = score_failure_taxonomy(report)
    assert s.counts["over_recommendation"] == 0


# ----- missing_critical -----


def test_missing_critical_high_severity():
    report = _empty_report([])  # no findings emitted
    s = score_failure_taxonomy(
        report,
        must_cover_issues=[
            {"id": "spill_dominant", "severity": "high"},
            {"id": "shuffle_dominant", "severity": "medium"},  # not counted
        ],
    )
    assert s.counts["missing_critical"] == 1


def test_no_missing_when_emitted():
    report = _empty_report([{"issue_id": "spill_dominant", "evidence": [], "actions": []}])
    s = score_failure_taxonomy(
        report,
        must_cover_issues=[{"id": "spill_dominant", "severity": "critical"}],
    )
    assert s.counts["missing_critical"] == 0


# ----- score aggregation -----


def test_clean_report_score_is_one():
    report = _empty_report([])
    s = score_failure_taxonomy(report)
    assert s.score == 1.0
    assert all(s.counts[c] == 0 for c in CATEGORIES)


def test_score_clamps_to_zero():
    """Many incidents shouldn't drive score below 0."""
    report = _empty_report([
        {
            "issue_id": f"x{i}",
            "evidence": [{"metric": "et", "source": "synthetic", "grounded": False}],
            "actions": [],
        }
        for i in range(20)
    ])
    s = score_failure_taxonomy(report)
    assert s.score >= 0.0
    assert s.score <= 1.0


# ----- aggregate -----


def test_aggregate_avg():
    s1 = score_failure_taxonomy(_empty_report([]))
    s2 = score_failure_taxonomy(
        _empty_report([
            {"issue_id": "x", "evidence": [{"source": "synthetic"}], "actions": []}
        ])
    )
    a = aggregate_failure_taxonomy([s1, s2])
    assert 0.5 <= a["avg_score"] <= 1.0
    assert a["cases_with_any_incident"] == 1


def test_aggregate_empty():
    a = aggregate_failure_taxonomy([])
    assert a["avg_score"] == 1.0
    assert a["cases_with_any_incident"] == 0
