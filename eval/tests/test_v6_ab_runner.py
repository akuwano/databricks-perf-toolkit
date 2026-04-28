"""Tests for V6 A/B runner aggregation logic (Week 4 Day 3)."""

from __future__ import annotations

from eval.ab_runner import (
    _alias_hit_summary,
    _avg,
    _build_summary,
    _canonical_failure_rate,
    _classify_verdict,
    _fallback_case_composite_avg,
)


# ----- verdict classification -----


def test_verdict_unchanged_small_deltas():
    assert _classify_verdict({}) == "unchanged"
    assert _classify_verdict({"evidence_grounding_composite": 0.02}) == "unchanged"
    assert _classify_verdict({"evidence_grounding_composite": -0.04}) == "unchanged"


def test_verdict_improved_on_composite():
    assert _classify_verdict({"evidence_grounding_composite": 0.05}) == "improved"
    assert _classify_verdict({"evidence_grounding_composite": 0.10}) == "improved"


def test_verdict_regressed_on_composite():
    assert _classify_verdict({"evidence_grounding_composite": -0.05}) == "regressed"
    assert _classify_verdict({"evidence_grounding_composite": -0.20}) == "regressed"


def test_verdict_regression_takes_precedence_over_improvement():
    """Even if some metric improved, any regression makes it regressed."""
    deltas = {
        "evidence_grounding_composite": 0.10,
        "finding_support_ratio": -0.06,
    }
    assert _classify_verdict(deltas) == "regressed"


def test_verdict_ungrounded_numeric_lower_is_better():
    """ungrounded_numeric が +5pt = 悪化 (lower-is-better)."""
    assert _classify_verdict({"ungrounded_numeric_ratio": 0.05}) == "regressed"
    assert _classify_verdict({"ungrounded_numeric_ratio": -0.05}) == "improved"


# ----- avg helper -----


def test_avg_skips_skipped_and_none():
    records = [
        {"x": 1.0},
        {"x": 3.0, "skipped_reason": "no_profile"},  # skipped
        {"x": None},  # None
        {"x": 2.0},
    ]
    assert _avg(records, "x") == 1.5


def test_avg_empty_returns_zero():
    assert _avg([], "x") == 0.0


# ----- canonical failure rate -----


def test_canonical_failure_rate_all_normalizer():
    records = [
        {"canonical_source": "normalizer_fallback"},
        {"canonical_source": "normalizer_fallback"},
    ]
    assert _canonical_failure_rate(records) == 1.0


def test_canonical_failure_rate_all_llm_direct():
    records = [
        {"canonical_source": "llm_direct"},
        {"canonical_source": "llm_direct"},
    ]
    assert _canonical_failure_rate(records) == 0.0


def test_canonical_failure_rate_mixed():
    records = [
        {"canonical_source": "llm_direct"},
        {"canonical_source": "normalizer_fallback"},
        {"canonical_source": "missing"},
        {"canonical_source": "llm_direct"},
    ]
    assert _canonical_failure_rate(records) == 0.5


def test_canonical_failure_rate_skips_unevaluated():
    records = [
        {"canonical_source": "llm_direct"},
        {"skipped_reason": "no_profile"},  # excluded
    ]
    assert _canonical_failure_rate(records) == 0.0


# ----- summary builder -----


def _stub_args():
    class A:
        manifest = "x"
        skip_llm = True
        skip_judge = True
        tag = None
        limit = None
    return A()


def test_build_summary_per_case_diff_emits():
    """case が baseline と candidate で値が違うとき、case_diff に entry が
    追加され、verdict が分類される。"""
    cond_results = {
        "baseline": {
            "baseline": {
                "cases": [
                    {
                        "case_id": "spill_q1",
                        "evidence_grounding_composite": 0.50,
                        "finding_support_ratio": 0.40,
                        "evidence_metric_grounding_ratio": 0.30,
                        "ungrounded_numeric_ratio": 0.20,
                        "canonical_source": "normalizer_fallback",
                    },
                ]
            }
        },
        "canonical-direct": {
            "baseline": {
                "cases": [
                    {
                        "case_id": "spill_q1",
                        "evidence_grounding_composite": 0.85,  # +35pt
                        "finding_support_ratio": 0.80,         # +40pt
                        "evidence_metric_grounding_ratio": 0.75,
                        "ungrounded_numeric_ratio": 0.10,
                        "canonical_source": "llm_direct",
                    },
                ]
            }
        },
        "no-force-fill": {
            "baseline": {"cases": []}
        },
        "both": {
            "baseline": {"cases": []}
        },
    }
    s = _build_summary("test", cond_results, _stub_args())
    assert "spill_q1" in s["case_diff"]
    assert s["case_diff"]["spill_q1"]["canonical-direct"]["verdict"] == "improved"
    deltas = s["case_diff"]["spill_q1"]["canonical-direct"]["deltas"]
    assert deltas["evidence_grounding_composite"] == 0.35
    assert deltas["finding_support_ratio"] == 0.40
    # canonical_parse_failure_rate
    assert s["canonical_parse_failure_rate"]["baseline"] == 1.0
    assert s["canonical_parse_failure_rate"]["canonical-direct"] == 0.0


# ----- v6.7.0 telemetry: fallback composite + alias hits -----


def test_fallback_case_composite_avg_only_counts_non_llm_direct():
    records = [
        {"case_id": "a", "canonical_source": "llm_direct", "evidence_grounding_composite": 0.90},
        {"case_id": "b", "canonical_source": "normalizer_fallback", "evidence_grounding_composite": 0.50},
        {"case_id": "c", "canonical_source": "missing", "evidence_grounding_composite": 0.30},
        {"case_id": "d", "canonical_source": "llm_direct", "evidence_grounding_composite": 0.95},
    ]
    avg = _fallback_case_composite_avg(records)
    # Only b and c qualify — (0.50 + 0.30) / 2 = 0.40
    assert avg == 0.4


def test_fallback_case_composite_avg_skips_skipped_cases():
    records = [
        {"case_id": "a", "canonical_source": "missing", "skipped_reason": "pipeline_error"},
        {"case_id": "b", "canonical_source": "normalizer_fallback", "evidence_grounding_composite": 0.40},
    ]
    assert _fallback_case_composite_avg(records) == 0.4


def test_fallback_case_composite_avg_zero_when_all_llm_direct():
    records = [
        {"case_id": "a", "canonical_source": "llm_direct", "evidence_grounding_composite": 0.80},
        {"case_id": "b", "canonical_source": "llm_direct", "evidence_grounding_composite": 0.85},
    ]
    assert _fallback_case_composite_avg(records) == 0.0


def test_alias_hit_summary_aggregates_per_case_dicts():
    records = [
        {"case_id": "a", "alias_hits": {"fix_type": 2, "category": 0, "issue_id": 1, "total": 3}},
        {"case_id": "b", "alias_hits": {"fix_type": 0, "category": 0, "issue_id": 0, "total": 0}},
        {"case_id": "c", "alias_hits": {"fix_type": 0, "category": 1, "issue_id": 0, "total": 1}},
        {"case_id": "d"},  # no alias_hits → ignored
    ]
    summary = _alias_hit_summary(records)
    # Only a, b, c contribute trackers (d skipped because no alias_hits key)
    assert summary["cases"] == 3
    assert summary["fix_type_total"] == 2
    assert summary["category_total"] == 1
    assert summary["issue_id_total"] == 1
    assert summary["hits_total"] == 4
    assert summary["cases_with_any_hit"] == 2
    assert summary["alias_hit_rate"] == 0.6667


def test_alias_hit_summary_zero_when_no_alias_hits():
    records = [
        {"case_id": "a"},  # no alias_hits at all
        {"case_id": "b", "alias_hits": None},
    ]
    summary = _alias_hit_summary(records)
    assert summary["cases"] == 0
    assert summary["alias_hit_rate"] == 0.0


def test_alias_hit_summary_skips_skipped_cases():
    records = [
        {"case_id": "a", "alias_hits": {"fix_type": 5, "total": 5}, "skipped_reason": "x"},
        {"case_id": "b", "alias_hits": {"fix_type": 1, "total": 1}},
    ]
    summary = _alias_hit_summary(records)
    # skipped record dropped → only b counts
    assert summary["cases"] == 1
    assert summary["fix_type_total"] == 1


def test_build_summary_regression_counts():
    cond_results = {
        "baseline": {
            "baseline": {"cases": [
                {"case_id": "a", "evidence_grounding_composite": 0.50},
                {"case_id": "b", "evidence_grounding_composite": 0.50},
            ]}
        },
        "canonical-direct": {
            "baseline": {"cases": [
                {"case_id": "a", "evidence_grounding_composite": 0.60},  # +10
                {"case_id": "b", "evidence_grounding_composite": 0.40},  # -10
            ]}
        },
        "no-force-fill": {"baseline": {"cases": []}},
        "both": {"baseline": {"cases": []}},
    }
    s = _build_summary("t", cond_results, _stub_args())
    counts = s["regression_summary"]["canonical-direct"]
    assert counts["improved"] == 1
    assert counts["regressed"] == 1
    assert counts["unchanged"] == 0
