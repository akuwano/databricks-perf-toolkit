"""Tests for core.comparison_reporter module."""

from core.comparison_reporter import generate_comparison_report
from core.models import ComparisonResult, MetricDiff


def _make_result(
    regression: bool = False,
    diffs: list[MetricDiff] | None = None,
) -> ComparisonResult:
    if diffs is None:
        diffs = [
            MetricDiff(
                metric_name="total_time_ms",
                metric_group="latency",
                direction_when_increase="WORSENS",
                baseline_value=1000,
                candidate_value=1200 if regression else 800,
                absolute_diff=200 if regression else -200,
                relative_diff_ratio=0.2 if regression else -0.2,
                changed_flag=True,
                improvement_flag=not regression,
                regression_flag=regression,
                severity="HIGH" if regression else "NONE",
                summary_text="total_time_ms: 1000 -> 1200",
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
                regression_flag=False,
                severity="NONE",
                summary_text="photon_ratio: 0.5 -> 0.8",
            ),
        ]
    return ComparisonResult(
        comparison_id="cmp-test-001",
        baseline_analysis_id="baseline-id-abc123def456",
        candidate_analysis_id="candidate-id-abc123def456",
        query_fingerprint="fp123456789abcdef0",
        experiment_id="exp-broadcast-test",
        baseline_variant="before",
        candidate_variant="after",
        metric_diffs=diffs,
        regression_detected=regression,
        regression_severity="HIGH" if regression else "NONE",
        summary="Regressed: total_time_ms" if regression else "Improved: total_time_ms",
    )


class TestGenerateComparisonReport:
    def test_contains_header(self):
        report = generate_comparison_report(_make_result())
        assert "# Profile Comparison Report" in report

    def test_contains_comparison_id(self):
        report = generate_comparison_report(_make_result())
        assert "cmp-test-001" in report

    def test_contains_variants(self):
        report = generate_comparison_report(_make_result())
        assert "before" in report
        assert "after" in report

    def test_contains_experiment_id(self):
        report = generate_comparison_report(_make_result())
        assert "exp-broadcast-test" in report

    def test_contains_metric_table(self):
        report = generate_comparison_report(_make_result())
        assert "| Metric |" in report
        assert "Total Time Ms" in report
        assert "Photon Ratio" in report

    def test_regression_shows_verdict(self):
        report = generate_comparison_report(_make_result(regression=True))
        assert "**Regression detected**" in report
        assert "HIGH" in report

    def test_no_regression_shows_verdict(self):
        report = generate_comparison_report(_make_result(regression=False))
        assert "**No regression detected**" in report

    def test_regression_section_present(self):
        report = generate_comparison_report(_make_result(regression=True))
        assert "## Regressions" in report
        assert "REGRESSION" in report

    def test_improvement_section_present(self):
        report = generate_comparison_report(_make_result())
        assert "## Improvements" in report
        assert "IMPROVED" in report

    def test_percentage_change_shown(self):
        report = generate_comparison_report(_make_result())
        # Should contain percentage like +20.0% or -20.0%
        assert "%" in report

    def test_grouped_by_metric_group(self):
        report = generate_comparison_report(_make_result())
        assert "### Latency" in report
        assert "### Engine" in report

    def test_empty_diffs(self):
        result = _make_result(diffs=[])
        report = generate_comparison_report(result)
        assert "# Profile Comparison Report" in report
        assert "**No regression detected**" in report

    def test_bytes_formatting(self):
        diffs = [
            MetricDiff(
                metric_name="read_bytes",
                metric_group="io",
                baseline_value=1_073_741_824,  # 1 GB
                candidate_value=536_870_912,  # 0.5 GB
                absolute_diff=-536_870_912,
                relative_diff_ratio=-0.5,
                changed_flag=True,
                improvement_flag=True,
            ),
        ]
        report = generate_comparison_report(_make_result(diffs=diffs))
        assert "GB" in report

    def test_duration_formatting(self):
        diffs = [
            MetricDiff(
                metric_name="total_time_ms",
                metric_group="latency",
                baseline_value=65_000,  # 65 seconds
                candidate_value=30_000,
                absolute_diff=-35_000,
                relative_diff_ratio=-0.538,
                changed_flag=True,
                improvement_flag=True,
            ),
        ]
        report = generate_comparison_report(_make_result(diffs=diffs))
        assert "min" in report or "s" in report
