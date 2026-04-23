"""Tests for core.comparison_llm module."""

from core.comparison_llm import (
    _build_comparison_fact_pack,
    _build_comparison_prompt,
    _build_comparison_system_prompt,
    _build_decision_drivers,
)
from core.models import ComparisonResult, MetricDiff


def _make_result():
    return ComparisonResult(
        comparison_id="cmp-1",
        baseline_variant="before",
        candidate_variant="after",
        experiment_id="exp-1",
        regression_detected=True,
        regression_severity="HIGH",
        metric_diffs=[
            MetricDiff(
                metric_name="total_time_ms",
                metric_group="latency",
                direction_when_increase="WORSENS",
                baseline_value=1000,
                candidate_value=1500,
                absolute_diff=500,
                relative_diff_ratio=0.5,
                changed_flag=True,
                regression_flag=True,
                severity="HIGH",
            ),
            MetricDiff(
                metric_name="photon_ratio",
                metric_group="engine",
                direction_when_increase="IMPROVES",
                baseline_value=0.5,
                candidate_value=0.8,
                absolute_diff=0.3,
                relative_diff_ratio=0.6,
                changed_flag=True,
                improvement_flag=True,
            ),
            MetricDiff(
                metric_name="spill_to_disk_bytes",
                metric_group="spill",
                direction_when_increase="WORSENS",
                baseline_value=0,
                candidate_value=0,
                changed_flag=False,
            ),
        ],
    )


def _make_result_with_suppression():
    """Result where IO-dependent metrics were suppressed."""
    return ComparisonResult(
        comparison_id="cmp-2",
        regression_detected=False,
        regression_severity="NONE",
        metric_diffs=[
            MetricDiff(
                metric_name="read_bytes",
                metric_group="io",
                direction_when_increase="WORSENS",
                baseline_value=1_000_000_000,
                candidate_value=500_000_000,
                absolute_diff=-500_000_000,
                relative_diff_ratio=-0.5,
                changed_flag=True,
                improvement_flag=True,
            ),
            MetricDiff(
                metric_name="remote_read_ratio",
                metric_group="io",
                direction_when_increase="WORSENS",
                baseline_value=0.3,
                candidate_value=0.5,
                absolute_diff=0.2,
                relative_diff_ratio=0.67,
                changed_flag=True,
                regression_flag=False,  # Suppressed
                severity="NONE",
            ),
        ],
    )


class TestBuildComparisonPrompt:
    def test_contains_variants(self):
        prompt = _build_comparison_prompt(_make_result())
        assert "before" in prompt
        assert "after" in prompt

    def test_contains_metric_changes(self):
        prompt = _build_comparison_prompt(_make_result())
        assert "total_time_ms" in prompt
        assert "photon_ratio" in prompt
        assert "REGRESSION" in prompt
        assert "IMPROVED" in prompt

    def test_contains_percentage(self):
        prompt = _build_comparison_prompt(_make_result())
        assert "50.0%" in prompt or "+50.0%" in prompt

    def test_japanese_mode(self):
        prompt = _build_comparison_prompt(_make_result(), lang="ja")
        assert "Fact Pack" in prompt

    def test_english_mode(self):
        prompt = _build_comparison_prompt(_make_result(), lang="en")
        assert "Fact Pack" in prompt

    def test_unchanged_metrics_in_separate_section(self):
        result = ComparisonResult(
            metric_diffs=[
                MetricDiff(metric_name="total_time_ms", changed_flag=False, baseline_value=1000),
            ],
        )
        prompt = _build_comparison_prompt(result)
        assert "## Unchanged Metrics" in prompt
        assert "total_time_ms" in prompt


class TestBuildComparisonSystemPrompt:
    def test_en_contains_4_sections(self):
        prompt = _build_comparison_system_prompt("en")
        assert "### 1. Change Summary" in prompt
        assert "### 2. Root Cause Hypotheses" in prompt
        assert "### 3. Recommended Actions" in prompt
        assert "### 4. Overall Verdict" in prompt

    def test_en_emphasizes_substantive_analysis(self):
        prompt = _build_comparison_system_prompt("en")
        assert "specific, substantive, actionable" in prompt

    def test_en_contains_rules(self):
        prompt = _build_comparison_system_prompt("en")
        assert "### Rules" in prompt
        assert "Quote numbers accurately" in prompt

    def test_en_contains_confidence_levels(self):
        prompt = _build_comparison_system_prompt("en")
        assert "high / medium / needs_verification" in prompt

    def test_en_verdict_options(self):
        prompt = _build_comparison_system_prompt("en")
        assert "**Go**" in prompt
        assert "**Hold**" in prompt
        assert "**Rollback**" in prompt

    def test_ja_contains_4_sections(self):
        prompt = _build_comparison_system_prompt("ja")
        assert "### 1. 変化の要約" in prompt
        assert "### 2. 原因の考察" in prompt
        assert "### 3. 推奨アクション" in prompt
        assert "### 4. 総合判定" in prompt

    def test_ja_emphasizes_substantive_analysis(self):
        prompt = _build_comparison_system_prompt("ja")
        assert "具体的で実用的な分析" in prompt

    def test_ja_contains_rules(self):
        prompt = _build_comparison_system_prompt("ja")
        assert "ルール" in prompt

    def test_en_mentions_noise(self):
        prompt = _build_comparison_system_prompt("en")
        assert "noise" in prompt.lower()

    def test_en_mentions_causal_links_guidance(self):
        prompt = _build_comparison_system_prompt("en")
        assert "Allowed Causal Links" in prompt


class TestBuildComparisonFactPack:
    def test_en_contains_decision_drivers(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "en")
        assert "## Decision Drivers" in fact_pack

    def test_en_contains_top_regressions(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "en")
        assert "### Top Regressions" in fact_pack
        assert "total_time_ms" in fact_pack

    def test_en_contains_top_improvements(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "en")
        assert "### Top Improvements" in fact_pack
        assert "photon_ratio" in fact_pack

    def test_en_contains_weight_info(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "en")
        assert "weight=" in fact_pack
        assert "impact_score=" in fact_pack

    def test_en_contains_causal_links(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "en")
        assert "## Known Causal Links" in fact_pack
        assert "read_bytes -> total_time_ms" in fact_pack

    def test_en_contains_analysis_context(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "en")
        assert "## Analysis Context" in fact_pack
        assert "Single-run comparison" in fact_pack

    def test_en_suppressed_regressions_shown(self):
        fact_pack = _build_comparison_fact_pack(_make_result_with_suppression(), "en")
        assert "## Suppressed Regressions" in fact_pack
        assert "remote_read_ratio" in fact_pack

    def test_en_absolute_diff_included(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "en")
        assert "abs_diff=" in fact_pack

    def test_ja_contains_decision_drivers(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "ja")
        assert "## 判定ドライバー" in fact_pack

    def test_ja_contains_top_regressions(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "ja")
        assert "### 主要な悪化" in fact_pack

    def test_ja_contains_top_improvements(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "ja")
        assert "### 主要な改善" in fact_pack

    def test_ja_contains_weight_info(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "ja")
        assert "重み=" in fact_pack
        assert "影響スコア=" in fact_pack

    def test_ja_labels(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "ja")
        assert "悪化" in fact_pack
        assert "改善" in fact_pack
        assert "変化なし" in fact_pack

    def test_ja_context(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "ja")
        assert "## 分析の前提条件" in fact_pack
        assert "単一実行の比較" in fact_pack

    def test_ja_causal_links(self):
        fact_pack = _build_comparison_fact_pack(_make_result(), "ja")
        assert "## 既知の因果関係" in fact_pack


class TestBuildDecisionDrivers:
    def test_top_regressions_sorted_by_impact(self):
        drivers = _build_decision_drivers(_make_result())
        assert len(drivers["top_regressions"]) == 1
        assert drivers["top_regressions"][0][0] == "total_time_ms"

    def test_top_improvements_sorted_by_impact(self):
        drivers = _build_decision_drivers(_make_result())
        assert len(drivers["top_improvements"]) == 1
        assert drivers["top_improvements"][0][0] == "photon_ratio"

    def test_max_5_per_category(self):
        diffs = [
            MetricDiff(
                metric_name=f"metric_{i}",
                baseline_value=100,
                candidate_value=200,
                relative_diff_ratio=0.5 + i * 0.1,
                changed_flag=True,
                regression_flag=True,
            )
            for i in range(10)
        ]
        result = ComparisonResult(metric_diffs=diffs)
        drivers = _build_decision_drivers(result)
        assert len(drivers["top_regressions"]) <= 5

    def test_suppressed_detection(self):
        drivers = _build_decision_drivers(_make_result_with_suppression())
        assert "remote_read_ratio" in drivers["suppressed_regressions"]

    def test_empty_result(self):
        result = ComparisonResult(metric_diffs=[])
        drivers = _build_decision_drivers(result)
        assert drivers["top_regressions"] == []
        assert drivers["top_improvements"] == []
        assert drivers["suppressed_regressions"] == []
