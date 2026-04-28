"""Tests for L1 rule_echo_in_llm scorer.

Verifies the scorer detects the V6 silent-drop pattern: a rule emitted
a canonical Finding (e.g., decimal_heavy_aggregate) but the LLM
narrative compressed it away. Also pins the no_op behavior for cases
without enforceable Findings.
"""

from __future__ import annotations

from eval.scorers.rule_echo_in_llm import (
    RuleEchoScore,
    aggregate_rule_echo,
    score_rule_echo,
)


def test_no_op_when_no_findings():
    score = score_rule_echo({}, "anything")
    assert score.no_op is True
    assert score.score == 1.0


def test_no_op_when_narrative_empty_even_if_findings_exist():
    """--skip-llm produces empty narrative; no enforcement is possible."""
    canonical = {"findings": [{"issue_id": "shuffle_dominant", "severity": "high"}]}
    score = score_rule_echo(canonical, "")
    assert score.no_op is True
    assert score.rule_finding_count == 1  # findings counted, score not enforced
    score2 = score_rule_echo(canonical, "   ")
    assert score2.no_op is True


def test_no_op_when_only_low_severity_findings():
    canonical = {
        "findings": [
            {"issue_id": "result_from_cache_detected", "severity": "low"}
        ]
    }
    score = score_rule_echo(canonical, "")
    assert score.no_op is True


def test_clean_when_narrative_echoes_issue_id_directly():
    canonical = {
        "findings": [
            {"issue_id": "decimal_heavy_aggregate", "severity": "medium"}
        ]
    }
    narrative = "The query has a decimal_heavy_aggregate issue worth reviewing."
    score = score_rule_echo(canonical, narrative)
    assert score.no_op is False
    assert score.score == 1.0
    assert score.missed_issue_ids == []


def test_clean_when_narrative_uses_registry_keyword():
    """Even without echoing the id literally, registry keywords count.
    For decimal_heavy_aggregate the registry includes 'DECIMAL'."""
    canonical = {
        "findings": [
            {"issue_id": "decimal_heavy_aggregate", "severity": "medium"}
        ]
    }
    narrative = "Verify the DECIMAL precision before optimizing further."
    score = score_rule_echo(canonical, narrative)
    assert score.score == 1.0
    assert "decimal_heavy_aggregate" not in score.missed_issue_ids


def test_v5_to_v6_silent_drop_detected():
    """V6 emitted decimal_heavy_aggregate as a canonical Finding, but the
    LLM narrative made no mention of DECIMAL or type review."""
    canonical = {
        "findings": [
            {"issue_id": "shuffle_dominant", "severity": "high"},
            {"issue_id": "decimal_heavy_aggregate", "severity": "medium"},
        ]
    }
    narrative = (
        "Shuffle dominates the runtime. The shuffle key should be added to "
        "the table's clustering."
    )
    score = score_rule_echo(canonical, narrative)
    assert score.no_op is False
    assert "decimal_heavy_aggregate" in score.missed_issue_ids
    # 1 of 2 covered → 0.5
    assert abs(score.score - 0.5) < 0.001


def test_appendix_findings_count_in_enforcement():
    canonical = {
        "appendix_excluded_findings": [
            {"issue_id": "missing_clustering", "severity": "high"}
        ]
    }
    # Non-empty narrative that does NOT mention clustering → enforced miss
    narrative = "Just talking about photon utilization."
    score = score_rule_echo(canonical, narrative)
    assert score.no_op is False
    assert "missing_clustering" in score.missed_issue_ids


def test_duplicate_issue_ids_counted_once():
    canonical = {
        "findings": [
            {"issue_id": "shuffle_dominant", "severity": "high"},
            {"issue_id": "shuffle_dominant", "severity": "high"},  # dupe
        ]
    }
    score = score_rule_echo(canonical, "shuffle dominates")
    assert score.rule_finding_count == 1
    assert score.score == 1.0


def test_case_insensitive_matching():
    canonical = {"findings": [{"issue_id": "spill_dominant", "severity": "high"}]}
    narrative = "ディスクスピル is bad here."  # JA keyword from registry
    score = score_rule_echo(canonical, narrative)
    assert score.score == 1.0


def test_missing_id_with_no_keywords_falls_back_to_id_text():
    """If registry has no keywords for an id, the id (with underscores
    rendered as spaces) is itself searched."""
    canonical = {
        "findings": [{"issue_id": "totally_made_up_issue", "severity": "high"}]
    }
    score = score_rule_echo(canonical, "totally made up issue is found")
    assert score.score == 1.0


def test_aggregate_ignores_no_op():
    scores = [
        RuleEchoScore(no_op=True),
        RuleEchoScore(score=0.5, rule_finding_count=2, echoed_count=1),
        RuleEchoScore(score=1.0, rule_finding_count=1, echoed_count=1),
    ]
    # 0.5 and 1.0 → 0.75
    assert aggregate_rule_echo(scores) == 0.75


def test_aggregate_returns_one_when_all_no_op():
    scores = [RuleEchoScore(no_op=True), RuleEchoScore(no_op=True)]
    assert aggregate_rule_echo(scores) == 1.0


def test_aggregate_empty():
    assert aggregate_rule_echo([]) == 1.0
