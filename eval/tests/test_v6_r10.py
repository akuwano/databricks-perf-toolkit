"""Tests for V6 R10 quality add-on (Week 4 Day 5)."""

from __future__ import annotations

from eval.scorers.r10_quality import (
    BORDERLINE_THRESHOLD,
    LAYER_A_THRESHOLDS,
    LAYER_A_WEIGHTS,
    PASS_THRESHOLD,
    compute_layer_a,
    score_r10,
    score_r10_from_condition_metrics,
    to_dict,
)


def test_weights_sum_to_one():
    assert abs(sum(LAYER_A_WEIGHTS.values()) - 1.0) < 1e-9


def test_thresholds_present_for_all_weights():
    assert set(LAYER_A_THRESHOLDS.keys()) == set(LAYER_A_WEIGHTS.keys())


def test_layer_a_perfect_score():
    metrics = {k: 1.0 for k in LAYER_A_WEIGHTS}
    score, reasons = compute_layer_a(metrics)
    assert score == 1.0
    assert reasons == []


def test_layer_a_zero_metrics_drops_below_borderline():
    metrics = {k: 0.0 for k in LAYER_A_WEIGHTS}
    score, reasons = compute_layer_a(metrics)
    assert score == 0.0
    # All 6 metrics should appear in reasons
    assert len(reasons) == len(LAYER_A_WEIGHTS)


def test_layer_a_partial():
    """W3.5 baseline (rule-based): expect borderline-or-fail."""
    metrics = {
        "schema_pass":            1.00,
        "actionability_specific": 0.94,
        "recall_strict":          0.18,
        "hallucination_clean":    0.65,
        "q3_composite":           0.65,
        "canonical_parse_ok":     0.00,
    }
    score, reasons = compute_layer_a(metrics)
    # 1.0*0.10 + 0.94*0.15 + 0.18*0.20 + 0.65*0.20 + 0.65*0.30 + 0.0*0.05
    # = 0.10 + 0.141 + 0.036 + 0.13 + 0.195 + 0.0 = 0.602
    assert abs(score - 0.602) < 0.001
    assert any("recall_strict" in r for r in reasons)
    assert any("hallucination_clean" in r for r in reasons)
    assert any("q3_composite" in r for r in reasons)
    assert any("canonical_parse_ok" in r for r in reasons)


def test_score_r10_overall_verdict_fail_low():
    s = score_r10(
        schema_pass_ratio=0.5,
        actionability_specific=0.0,
        recall_strict=0.0,
        hallucination_clean=0.0,
        q3_composite=0.0,
        q3_finding_support=0.0,
        q3_metric_grounded=0.0,
        q3_ungrounded_numeric=1.0,
        canonical_parse_failure_rate=1.0,
    )
    assert s.overall_verdict == "fail"
    assert s.overall_score < BORDERLINE_THRESHOLD
    assert len(s.layer_a_reasons) >= 4


def test_score_r10_pass_when_all_above_threshold():
    s = score_r10(
        schema_pass_ratio=1.0,
        actionability_specific=0.95,
        recall_strict=0.60,
        hallucination_clean=0.90,
        q3_composite=0.85,
        q3_finding_support=0.85,
        q3_metric_grounded=0.75,
        q3_ungrounded_numeric=0.10,
        canonical_parse_failure_rate=0.0,
    )
    assert s.overall_verdict == "pass"
    assert s.overall_score >= PASS_THRESHOLD
    assert s.layer_a_reasons == []


def test_score_r10_borderline():
    s = score_r10(
        schema_pass_ratio=1.0,
        actionability_specific=0.80,
        recall_strict=0.40,  # below 0.50 threshold
        hallucination_clean=0.80,  # below 0.85
        q3_composite=0.70,  # below 0.80
        q3_finding_support=0.70,
        q3_metric_grounded=0.70,
        q3_ungrounded_numeric=0.20,
        canonical_parse_failure_rate=0.10,
    )
    # 1.0*0.10 + 0.80*0.15 + 0.40*0.20 + 0.80*0.20 + 0.70*0.30 + 0.90*0.05
    # = 0.10 + 0.12 + 0.08 + 0.16 + 0.21 + 0.045 = 0.715
    assert s.overall_verdict == "borderline"


def test_layer_b_optional_doesnt_break():
    s = score_r10(
        schema_pass_ratio=1.0,
        actionability_specific=0.90,
        recall_strict=0.55,
        hallucination_clean=0.90,
        q3_composite=0.85,
        q3_finding_support=0.85,
        q3_metric_grounded=0.80,
        q3_ungrounded_numeric=0.10,
        canonical_parse_failure_rate=0.0,
        layer_b_score=None,
    )
    assert s.layer_b_score is None
    assert s.overall_score == s.layer_a_score


def test_layer_b_when_present_aggregated():
    s = score_r10(
        schema_pass_ratio=1.0,
        actionability_specific=1.0,
        recall_strict=1.0,
        hallucination_clean=1.0,
        q3_composite=1.0,
        q3_finding_support=1.0,
        q3_metric_grounded=1.0,
        q3_ungrounded_numeric=0.0,
        canonical_parse_failure_rate=0.0,
        layer_b_score=0.40,
    )
    # layer_a = 1.0, layer_b = 0.40, mean = 0.70 → borderline
    assert s.overall_verdict == "borderline"
    assert abs(s.overall_score - 0.70) < 1e-9


def test_score_r10_from_condition_metrics():
    metrics = {
        "schema_pass_rate": 1.0,
        "actionability_specific_ratio": 0.94,
        "recall_strict_ratio": 0.18,
        "hallucination_score_avg": 0.65,
        "evidence_grounding_composite": 0.65,
        "finding_support_ratio": 0.58,
        "evidence_metric_grounding_ratio": 0.45,
        "ungrounded_numeric_ratio": 0.22,
    }
    s = score_r10_from_condition_metrics(metrics, parse_failure_rate=1.0)
    # canonical_parse_ok = 0
    assert "canonical_parse_ok" in " ".join(s.layer_a_reasons)
    d = to_dict(s)
    assert "overall_score" in d
    assert "inputs" in d
