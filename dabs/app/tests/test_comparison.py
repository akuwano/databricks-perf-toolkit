"""Tests for core.comparison module."""

from core.comparison import COMPARABLE_METRICS, ComparisonService
from core.models import (
    AnalysisContext,
    BottleneckIndicators,
    ComparisonRequest,
    ProfileAnalysis,
    QueryMetrics,
)


def _make_analysis(
    total_time_ms: int = 1000,
    spill_bytes: int = 0,
    photon_ratio: float = 0.8,
    read_bytes: int = 1_000_000,
    variant: str = "baseline",
    fingerprint: str = "abc123",
    experiment_id: str = "exp-1",
) -> ProfileAnalysis:
    """Helper to create a ProfileAnalysis with specific metric values."""
    return ProfileAnalysis(
        query_metrics=QueryMetrics(
            total_time_ms=total_time_ms,
            read_bytes=read_bytes,
        ),
        bottleneck_indicators=BottleneckIndicators(
            spill_bytes=spill_bytes,
            photon_ratio=photon_ratio,
        ),
        analysis_context=AnalysisContext(
            query_fingerprint=fingerprint,
            experiment_id=experiment_id,
            variant=variant,
        ),
    )


class TestComparisonService:
    def setup_method(self):
        self.service = ComparisonService()
        self.request = ComparisonRequest(
            baseline_analysis_id="id-baseline",
            candidate_analysis_id="id-candidate",
        )

    def test_no_change_no_regression(self):
        baseline = _make_analysis(total_time_ms=1000, variant="before")
        candidate = _make_analysis(total_time_ms=1000, variant="after")
        result = self.service.compare_analyses(baseline, candidate, self.request)
        assert not result.regression_detected
        assert result.regression_severity == "NONE"

    def test_regression_detected_when_time_increases(self):
        baseline = _make_analysis(total_time_ms=1000, variant="before")
        candidate = _make_analysis(total_time_ms=1200, variant="after")  # +20%
        result = self.service.compare_analyses(baseline, candidate, self.request)
        assert result.regression_detected
        time_diff = next(m for m in result.metric_diffs if m.metric_name == "total_time_ms")
        assert time_diff.regression_flag
        assert time_diff.severity == "HIGH"

    def test_improvement_detected_when_time_decreases(self):
        baseline = _make_analysis(total_time_ms=1000, variant="before")
        candidate = _make_analysis(total_time_ms=800, variant="after")  # -20%
        result = self.service.compare_analyses(baseline, candidate, self.request)
        time_diff = next(m for m in result.metric_diffs if m.metric_name == "total_time_ms")
        assert time_diff.improvement_flag
        assert not time_diff.regression_flag

    def test_spill_regression_is_high_severity(self):
        baseline = _make_analysis(spill_bytes=1_000_000, variant="before")
        candidate = _make_analysis(spill_bytes=2_000_000, variant="after")  # +100%
        result = self.service.compare_analyses(baseline, candidate, self.request)
        spill_diff = next(m for m in result.metric_diffs if m.metric_name == "spill_bytes")
        assert spill_diff.regression_flag
        assert spill_diff.severity == "HIGH"

    def test_photon_improvement(self):
        baseline = _make_analysis(photon_ratio=0.5, variant="before")
        candidate = _make_analysis(photon_ratio=0.8, variant="after")  # +60%
        result = self.service.compare_analyses(baseline, candidate, self.request)
        photon_diff = next(m for m in result.metric_diffs if m.metric_name == "photon_ratio")
        assert photon_diff.improvement_flag
        assert not photon_diff.regression_flag

    def test_small_change_below_threshold_no_flag(self):
        baseline = _make_analysis(total_time_ms=1000, variant="before")
        candidate = _make_analysis(total_time_ms=1050, variant="after")  # +5%
        result = self.service.compare_analyses(baseline, candidate, self.request)
        time_diff = next(m for m in result.metric_diffs if m.metric_name == "total_time_ms")
        assert not time_diff.regression_flag
        assert not time_diff.improvement_flag

    def test_context_fields_propagated(self):
        baseline = _make_analysis(variant="before", fingerprint="fp1", experiment_id="exp-1")
        candidate = _make_analysis(variant="after")
        result = self.service.compare_analyses(baseline, candidate, self.request)
        assert result.query_fingerprint == "fp1"
        assert result.experiment_id == "exp-1"
        assert result.baseline_variant == "before"
        assert result.candidate_variant == "after"

    def test_comparison_id_generated(self):
        baseline = _make_analysis(variant="before")
        candidate = _make_analysis(variant="after")
        result = self.service.compare_analyses(baseline, candidate, self.request)
        assert result.comparison_id  # non-empty UUID

    def test_summary_contains_metric_names(self):
        baseline = _make_analysis(total_time_ms=1000, variant="before")
        candidate = _make_analysis(total_time_ms=1200, variant="after")
        result = self.service.compare_analyses(baseline, candidate, self.request)
        assert "total_time_ms" in result.summary

    def test_all_comparable_metrics_covered(self):
        baseline = _make_analysis(variant="before")
        candidate = _make_analysis(variant="after")
        result = self.service.compare_analyses(baseline, candidate, self.request)
        metric_names = {m.metric_name for m in result.metric_diffs}
        assert metric_names == set(COMPARABLE_METRICS.keys())

    def test_custom_threshold(self):
        service = ComparisonService(threshold=0.50)  # 50% threshold
        baseline = _make_analysis(total_time_ms=1000, variant="before")
        candidate = _make_analysis(total_time_ms=1200, variant="after")  # +20%
        result = service.compare_analyses(baseline, candidate, self.request)
        time_diff = next(m for m in result.metric_diffs if m.metric_name == "total_time_ms")
        assert not time_diff.regression_flag  # below 50% threshold

    def test_zero_baseline_no_crash(self):
        baseline = _make_analysis(total_time_ms=0, variant="before")
        candidate = _make_analysis(total_time_ms=100, variant="after")
        result = self.service.compare_analyses(baseline, candidate, self.request)
        time_diff = next(m for m in result.metric_diffs if m.metric_name == "total_time_ms")
        assert time_diff.relative_diff_ratio is None  # division by zero avoided

    def test_queue_time_regression(self):
        baseline = _make_analysis(variant="before")
        baseline.query_metrics.queued_provisioning_time_ms = 5000
        candidate = _make_analysis(variant="after")
        candidate.query_metrics.queued_provisioning_time_ms = 20000
        result = self.service.compare_analyses(baseline, candidate, self.request)
        diff = next(
            m for m in result.metric_diffs if m.metric_name == "queued_provisioning_time_ms"
        )
        assert diff.regression_flag

    def test_result_fetch_noise_floor(self):
        """Small fetch time values should not trigger regression."""
        baseline = _make_analysis(variant="before")
        baseline.query_metrics.result_fetch_time_ms = 100
        candidate = _make_analysis(variant="after")
        candidate.query_metrics.result_fetch_time_ms = 500  # +400% but both < 3s noise floor
        result = self.service.compare_analyses(baseline, candidate, self.request)
        diff = next(m for m in result.metric_diffs if m.metric_name == "result_fetch_time_ms")
        assert not diff.regression_flag

    def test_shuffle_bytes_written_regression(self):
        """Shuffle write volume increase should be detected as regression."""
        baseline = _make_analysis(variant="before")
        baseline.bottleneck_indicators.shuffle_bytes_written_total = 500_000_000
        candidate = _make_analysis(variant="after")
        candidate.bottleneck_indicators.shuffle_bytes_written_total = 1_000_000_000  # +100%
        result = self.service.compare_analyses(baseline, candidate, self.request)
        diff = next(
            m for m in result.metric_diffs if m.metric_name == "shuffle_bytes_written_total"
        )
        assert diff.regression_flag
        assert diff.severity == "MEDIUM"

    def test_shuffle_bytes_noise_floor(self):
        """Small shuffle values below noise floor should not trigger regression."""
        baseline = _make_analysis(variant="before")
        baseline.bottleneck_indicators.shuffle_bytes_written_total = 10_000_000  # 10MB
        candidate = _make_analysis(variant="after")
        candidate.bottleneck_indicators.shuffle_bytes_written_total = (
            50_000_000  # 50MB, both < 100MB
        )
        result = self.service.compare_analyses(baseline, candidate, self.request)
        diff = next(
            m for m in result.metric_diffs if m.metric_name == "shuffle_bytes_written_total"
        )
        assert not diff.regression_flag

    def test_write_remote_bytes_regression(self):
        """Write volume increase should be detected."""
        baseline = _make_analysis(variant="before")
        baseline.query_metrics.write_remote_bytes = 100_000_000
        candidate = _make_analysis(variant="after")
        candidate.query_metrics.write_remote_bytes = 200_000_000  # +100%
        result = self.service.compare_analyses(baseline, candidate, self.request)
        diff = next(m for m in result.metric_diffs if m.metric_name == "write_remote_bytes")
        assert diff.regression_flag

    def test_write_remote_files_noise_floor(self):
        """Small file count below noise floor should not trigger."""
        baseline = _make_analysis(variant="before")
        baseline.query_metrics.write_remote_files = 1
        candidate = _make_analysis(variant="after")
        candidate.query_metrics.write_remote_files = 4  # +300% but both < 5 files
        result = self.service.compare_analyses(baseline, candidate, self.request)
        diff = next(m for m in result.metric_diffs if m.metric_name == "write_remote_files")
        assert not diff.regression_flag

    def test_write_remote_rows_regression(self):
        """Write row count increase above noise floor should be regression."""
        baseline = _make_analysis(variant="before")
        baseline.query_metrics.write_remote_rows = 10_000
        candidate = _make_analysis(variant="after")
        candidate.query_metrics.write_remote_rows = 25_000  # +150%
        result = self.service.compare_analyses(baseline, candidate, self.request)
        diff = next(m for m in result.metric_diffs if m.metric_name == "write_remote_rows")
        assert diff.regression_flag
