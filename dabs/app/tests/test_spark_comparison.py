"""Tests for core.spark_comparison module."""

import pytest
from core.models import MetricDiff
from core.spark_comparison import (
    _STREAMING_AGG_METRICS,
    SPARK_COMPARABLE_METRICS,
    _aggregate_stages,
    _aggregate_streaming,
    _build_metric_diff,
    _build_summary,
    _summarize_severity,
    compare_spark_apps,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_summary(
    app_id: str = "app-baseline",
    duration_ms: float = 60_000,
    total_exec_run_ms: float = 50_000,
    total_gc_time_ms: float = 2_000,
    gc_overhead_pct: float = 4.0,
    total_input_gb: float = 10.0,
    total_shuffle_gb: float = 2.0,
    total_spill_gb: float = 0.0,
    stages_with_disk_spill: float = 0.0,
    job_success_rate: float = 1.0,
    total_tasks: float = 100.0,
    estimated_total_dbu: float = 5.0,
    estimated_dbu_per_hour: float = 5.0,
    estimated_total_usd: float = 3.50,
) -> dict:
    return {
        "app_id": app_id,
        "duration_ms": duration_ms,
        "total_exec_run_ms": total_exec_run_ms,
        "total_gc_time_ms": total_gc_time_ms,
        "gc_overhead_pct": gc_overhead_pct,
        "total_input_gb": total_input_gb,
        "total_shuffle_gb": total_shuffle_gb,
        "total_spill_gb": total_spill_gb,
        "stages_with_disk_spill": stages_with_disk_spill,
        "job_success_rate": job_success_rate,
        "total_tasks": total_tasks,
        "estimated_total_dbu": estimated_total_dbu,
        "estimated_dbu_per_hour": estimated_dbu_per_hour,
        "estimated_total_usd": estimated_total_usd,
    }


def _make_stages(
    count: int = 3,
    task_skew_ratio: float = 1.5,
    cpu_efficiency_pct: float = 80.0,
    disk_spill_mb: float = 0.0,
    shuffle_read_mb: float = 100.0,
    bottleneck_type: str = "",
) -> list[dict]:
    return [
        {
            "task_skew_ratio": task_skew_ratio,
            "cpu_efficiency_pct": cpu_efficiency_pct,
            "disk_spill_mb": disk_spill_mb,
            "shuffle_read_mb": shuffle_read_mb,
            "bottleneck_type": bottleneck_type,
        }
        for _ in range(count)
    ]


# ---------------------------------------------------------------------------
# _aggregate_stages
# ---------------------------------------------------------------------------


class TestAggregateStages:
    def test_empty_stages_returns_zeros(self):
        result = _aggregate_stages([])
        assert result["max_task_skew_ratio"] == 0.0
        assert result["avg_cpu_efficiency_pct"] == 0.0
        assert result["total_disk_spill_mb"] == 0.0
        assert result["total_shuffle_read_mb"] == 0.0
        assert result["bottleneck_count"] == 0.0

    def test_single_stage(self):
        stages = [
            {
                "task_skew_ratio": 2.5,
                "cpu_efficiency_pct": 75.0,
                "disk_spill_mb": 100.0,
                "shuffle_read_mb": 200.0,
                "bottleneck_type": "shuffle",
            }
        ]
        result = _aggregate_stages(stages)
        assert result["max_task_skew_ratio"] == 2.5
        assert result["avg_cpu_efficiency_pct"] == 75.0
        assert result["total_disk_spill_mb"] == 100.0
        assert result["total_shuffle_read_mb"] == 200.0
        assert result["bottleneck_count"] == 1.0

    def test_multiple_stages_aggregation(self):
        stages = [
            {
                "task_skew_ratio": 1.0,
                "cpu_efficiency_pct": 60.0,
                "disk_spill_mb": 50.0,
                "shuffle_read_mb": 100.0,
                "bottleneck_type": "",
            },
            {
                "task_skew_ratio": 3.0,
                "cpu_efficiency_pct": 80.0,
                "disk_spill_mb": 150.0,
                "shuffle_read_mb": 200.0,
                "bottleneck_type": "skew",
            },
        ]
        result = _aggregate_stages(stages)
        assert result["max_task_skew_ratio"] == 3.0
        assert result["avg_cpu_efficiency_pct"] == pytest.approx(70.0)
        assert result["total_disk_spill_mb"] == 200.0
        assert result["total_shuffle_read_mb"] == 300.0
        assert result["bottleneck_count"] == 1.0

    def test_bottleneck_none_not_counted(self):
        stages = [
            {"bottleneck_type": "none"},
            {"bottleneck_type": "None"},
            {"bottleneck_type": "null"},
            {"bottleneck_type": ""},
            {"bottleneck_type": "skew"},
        ]
        result = _aggregate_stages(stages)
        assert result["bottleneck_count"] == 1.0

    def test_alternative_key_names(self):
        """Stage dicts may use skew_ratio instead of task_skew_ratio."""
        stages = [{"skew_ratio": 4.0, "cpu_efficiency": 90.0, "spill_disk_mb": 500.0}]
        result = _aggregate_stages(stages)
        assert result["max_task_skew_ratio"] == 4.0
        assert result["avg_cpu_efficiency_pct"] == 90.0
        assert result["total_disk_spill_mb"] == 500.0


# ---------------------------------------------------------------------------
# _build_metric_diff
# ---------------------------------------------------------------------------


class TestBuildMetricDiff:
    def test_missing_baseline_returns_empty_diff(self):
        md = _build_metric_diff("duration_ms", "latency", "WORSENS", None, 100.0, 0.10)
        assert md.baseline_value is None
        assert md.candidate_value is None
        assert not md.regression_flag
        assert not md.improvement_flag

    def test_missing_candidate_returns_empty_diff(self):
        md = _build_metric_diff("duration_ms", "latency", "WORSENS", 100.0, None, 0.10)
        assert md.baseline_value is None

    def test_worsens_increase_is_regression(self):
        md = _build_metric_diff("duration_ms", "latency", "WORSENS", 100.0, 120.0, 0.10)
        assert md.regression_flag
        assert not md.improvement_flag
        assert md.severity == "HIGH"  # duration_ms is HIGH severity

    def test_worsens_decrease_is_improvement(self):
        md = _build_metric_diff("duration_ms", "latency", "WORSENS", 100.0, 80.0, 0.10)
        assert md.improvement_flag
        assert not md.regression_flag

    def test_improves_increase_is_improvement(self):
        md = _build_metric_diff("job_success_rate", "reliability", "IMPROVES", 0.8, 0.95, 0.10)
        assert md.improvement_flag
        assert not md.regression_flag

    def test_improves_decrease_is_regression(self):
        md = _build_metric_diff("job_success_rate", "reliability", "IMPROVES", 1.0, 0.8, 0.10)
        assert md.regression_flag
        assert not md.improvement_flag

    def test_neutral_never_flags(self):
        md = _build_metric_diff("total_tasks", "parallelism", "NEUTRAL", 100.0, 200.0, 0.10)
        assert not md.regression_flag
        assert not md.improvement_flag

    def test_below_threshold_no_flags(self):
        md = _build_metric_diff("duration_ms", "latency", "WORSENS", 100.0, 105.0, 0.10)
        assert not md.regression_flag
        assert not md.improvement_flag

    def test_zero_baseline_ratio_is_none(self):
        md = _build_metric_diff("duration_ms", "latency", "WORSENS", 0.0, 100.0, 0.10)
        assert md.relative_diff_ratio is None
        assert md.absolute_diff == 100.0

    def test_same_values_no_change(self):
        md = _build_metric_diff("duration_ms", "latency", "WORSENS", 100.0, 100.0, 0.10)
        assert not md.changed_flag
        assert not md.regression_flag
        assert not md.improvement_flag

    def test_high_severity_metric(self):
        md = _build_metric_diff("total_spill_gb", "spill", "WORSENS", 1.0, 2.0, 0.10)
        assert md.severity == "HIGH"

    def test_medium_severity_metric(self):
        md = _build_metric_diff("total_shuffle_gb", "shuffle", "WORSENS", 1.0, 2.0, 0.10)
        assert md.regression_flag
        assert md.severity == "MEDIUM"

    def test_noise_floor_gc_time(self):
        """GC time below 5000ms on both sides is noise and should not flag."""
        md = _build_metric_diff("total_gc_time_ms", "gc", "WORSENS", 1000.0, 3000.0, 0.10)
        assert not md.regression_flag

    def test_noise_floor_gc_overhead(self):
        """GC overhead below 1% on both sides is noise."""
        md = _build_metric_diff("gc_overhead_pct", "gc", "WORSENS", 0.3, 0.8, 0.10)
        assert not md.regression_flag

    def test_above_noise_floor_does_flag(self):
        md = _build_metric_diff("total_gc_time_ms", "gc", "WORSENS", 10_000.0, 15_000.0, 0.10)
        assert md.regression_flag

    def test_summary_text_format(self):
        md = _build_metric_diff("duration_ms", "latency", "WORSENS", 100.0, 120.0, 0.10)
        assert "duration_ms" in md.summary_text
        assert "100.0" in md.summary_text
        assert "120.0" in md.summary_text


# ---------------------------------------------------------------------------
# _summarize_severity
# ---------------------------------------------------------------------------


class TestSummarizeSeverity:
    def test_high_if_any_high(self):
        diffs = [
            MetricDiff(severity="MEDIUM", regression_flag=True),
            MetricDiff(severity="HIGH", regression_flag=True),
        ]
        assert _summarize_severity(diffs) == "HIGH"

    def test_medium_if_no_high(self):
        diffs = [MetricDiff(severity="MEDIUM", regression_flag=True)]
        assert _summarize_severity(diffs) == "MEDIUM"

    def test_none_if_empty(self):
        assert _summarize_severity([]) == "NONE"


# ---------------------------------------------------------------------------
# _build_summary
# ---------------------------------------------------------------------------


class TestBuildSummary:
    def test_no_changes(self):
        diffs = [MetricDiff(metric_name="duration_ms")]
        summary = _build_summary(diffs, 0.0)
        assert "No significant changes" in summary
        assert "Net score" in summary

    def test_regression_listed(self):
        diffs = [MetricDiff(metric_name="duration_ms", regression_flag=True)]
        summary = _build_summary(diffs, -1.0)
        assert "Regressed: duration_ms" in summary

    def test_improvement_listed(self):
        diffs = [MetricDiff(metric_name="total_spill_gb", improvement_flag=True)]
        summary = _build_summary(diffs, 1.0)
        assert "Improved: total_spill_gb" in summary

    def test_both_regressed_and_improved(self):
        diffs = [
            MetricDiff(metric_name="duration_ms", regression_flag=True),
            MetricDiff(metric_name="total_spill_gb", improvement_flag=True),
        ]
        summary = _build_summary(diffs, -0.5)
        assert "Regressed" in summary
        assert "Improved" in summary


# ---------------------------------------------------------------------------
# compare_spark_apps  (integration-level tests)
# ---------------------------------------------------------------------------


class TestCompareSparkApps:
    def test_no_change_returns_no_regression(self):
        summary = _make_summary()
        stages = _make_stages()
        result = compare_spark_apps(summary, summary, stages, stages)
        assert not result.regression_detected
        assert result.regression_severity == "NONE"
        assert "No significant changes" in result.summary

    def test_comparison_id_generated(self):
        summary = _make_summary()
        result = compare_spark_apps(summary, summary, [], [])
        assert result.comparison_id  # non-empty UUID

    def test_app_ids_propagated(self):
        base = _make_summary(app_id="app-A")
        cand = _make_summary(app_id="app-B")
        result = compare_spark_apps(base, cand, [], [])
        assert result.baseline_analysis_id == "app-A"
        assert result.candidate_analysis_id == "app-B"

    def test_all_comparable_metrics_covered(self):
        summary = _make_summary()
        stages = _make_stages()
        result = compare_spark_apps(summary, summary, stages, stages)
        metric_names = {m.metric_name for m in result.metric_diffs}
        # Without streaming data, streaming metrics are skipped
        expected = set(SPARK_COMPARABLE_METRICS.keys()) - _STREAMING_AGG_METRICS
        assert metric_names == expected

    def test_duration_regression_high_severity(self):
        base = _make_summary(duration_ms=60_000)
        cand = _make_summary(duration_ms=80_000)  # +33%
        result = compare_spark_apps(base, cand, [], [])
        assert result.regression_detected
        assert result.regression_severity == "HIGH"
        dm = next(m for m in result.metric_diffs if m.metric_name == "duration_ms")
        assert dm.regression_flag
        assert dm.severity == "HIGH"

    def test_spill_regression_high_severity(self):
        base = _make_summary(total_spill_gb=1.0)
        cand = _make_summary(total_spill_gb=5.0)  # +400%
        result = compare_spark_apps(base, cand, [], [])
        spill = next(m for m in result.metric_diffs if m.metric_name == "total_spill_gb")
        assert spill.regression_flag
        assert spill.severity == "HIGH"

    def test_spill_zero_baseline_no_ratio(self):
        """When baseline is 0, ratio is None so regression cannot be flagged."""
        base = _make_summary(total_spill_gb=0.0)
        cand = _make_summary(total_spill_gb=5.0)
        result = compare_spark_apps(base, cand, [], [])
        spill = next(m for m in result.metric_diffs if m.metric_name == "total_spill_gb")
        assert spill.relative_diff_ratio is None
        assert not spill.regression_flag  # can't detect ratio-based regression from 0

    def test_duration_improvement(self):
        base = _make_summary(duration_ms=100_000)
        cand = _make_summary(duration_ms=70_000)  # -30%
        result = compare_spark_apps(base, cand, [], [])
        dm = next(m for m in result.metric_diffs if m.metric_name == "duration_ms")
        assert dm.improvement_flag
        assert not dm.regression_flag

    def test_job_success_rate_improvement(self):
        base = _make_summary(job_success_rate=0.7)
        cand = _make_summary(job_success_rate=1.0)  # +43%
        result = compare_spark_apps(base, cand, [], [])
        jsr = next(m for m in result.metric_diffs if m.metric_name == "job_success_rate")
        assert jsr.improvement_flag

    def test_job_success_rate_regression(self):
        base = _make_summary(job_success_rate=1.0)
        cand = _make_summary(job_success_rate=0.8)  # -20%
        result = compare_spark_apps(base, cand, [], [])
        jsr = next(m for m in result.metric_diffs if m.metric_name == "job_success_rate")
        assert jsr.regression_flag

    def test_small_change_below_threshold(self):
        base = _make_summary(duration_ms=100_000)
        cand = _make_summary(duration_ms=105_000)  # +5%, below 10% threshold
        result = compare_spark_apps(base, cand, [], [])
        dm = next(m for m in result.metric_diffs if m.metric_name == "duration_ms")
        assert not dm.regression_flag
        assert not dm.improvement_flag

    def test_custom_threshold(self):
        base = _make_summary(duration_ms=100_000)
        cand = _make_summary(duration_ms=160_000)  # +60%
        result = compare_spark_apps(base, cand, [], [], threshold=0.70)
        dm = next(m for m in result.metric_diffs if m.metric_name == "duration_ms")
        assert not dm.regression_flag  # below 70% threshold

    def test_stage_metrics_compared(self):
        base_stages = _make_stages(count=2, task_skew_ratio=1.0, cpu_efficiency_pct=90.0)
        cand_stages = _make_stages(count=2, task_skew_ratio=5.0, cpu_efficiency_pct=50.0)
        summary = _make_summary()
        result = compare_spark_apps(summary, summary, base_stages, cand_stages)
        skew = next(m for m in result.metric_diffs if m.metric_name == "max_task_skew_ratio")
        assert skew.regression_flag  # skew increased (WORSENS)
        cpu = next(m for m in result.metric_diffs if m.metric_name == "avg_cpu_efficiency_pct")
        assert cpu.regression_flag  # cpu efficiency decreased (IMPROVES direction)

    def test_stage_metrics_improvement(self):
        base_stages = _make_stages(count=2, task_skew_ratio=5.0, disk_spill_mb=500.0)
        cand_stages = _make_stages(count=2, task_skew_ratio=1.0, disk_spill_mb=0.0)
        summary = _make_summary()
        result = compare_spark_apps(summary, summary, base_stages, cand_stages)
        skew = next(m for m in result.metric_diffs if m.metric_name == "max_task_skew_ratio")
        assert skew.improvement_flag
        spill = next(m for m in result.metric_diffs if m.metric_name == "total_disk_spill_mb")
        assert spill.improvement_flag

    def test_empty_stages_no_crash(self):
        summary = _make_summary()
        result = compare_spark_apps(summary, summary, [], [])
        assert not result.regression_detected

    def test_missing_summary_keys_no_crash(self):
        base = {"app_id": "a"}
        cand = {"app_id": "b"}
        result = compare_spark_apps(base, cand, [], [])
        # Metrics with missing values should have None baseline/candidate
        dm = next(m for m in result.metric_diffs if m.metric_name == "duration_ms")
        assert dm.baseline_value is None
        assert dm.candidate_value is None
        assert not dm.regression_flag

    def test_net_score_positive_for_improvements(self):
        base = _make_summary(duration_ms=100_000, total_spill_gb=10.0)
        cand = _make_summary(duration_ms=50_000, total_spill_gb=1.0)
        result = compare_spark_apps(base, cand, [], [])
        # Net score should be positive when things improve
        assert "Net score: +" in result.summary

    def test_net_score_negative_for_regressions(self):
        base = _make_summary(duration_ms=50_000, total_spill_gb=1.0)
        cand = _make_summary(duration_ms=100_000, total_spill_gb=10.0)
        result = compare_spark_apps(base, cand, [], [])
        assert "Net score: -" in result.summary

    def test_zero_baseline_values_no_crash(self):
        base = _make_summary(
            duration_ms=0,
            total_exec_run_ms=0,
            total_gc_time_ms=0,
            total_spill_gb=0,
            total_shuffle_gb=0,
            total_input_gb=0,
        )
        cand = _make_summary(duration_ms=100)
        result = compare_spark_apps(base, cand, [], [])
        dm = next(m for m in result.metric_diffs if m.metric_name == "duration_ms")
        assert dm.relative_diff_ratio is None  # division by zero avoided
        assert dm.absolute_diff == 100.0

    def test_neutral_metric_not_flagged(self):
        base = _make_summary(total_tasks=100)
        cand = _make_summary(total_tasks=500)  # +400%
        result = compare_spark_apps(base, cand, [], [])
        tasks = next(m for m in result.metric_diffs if m.metric_name == "total_tasks")
        assert not tasks.regression_flag
        assert not tasks.improvement_flag

    def test_regression_detected_with_high_regression(self):
        """HIGH severity regression always triggers regression_detected."""
        base = _make_summary(duration_ms=100_000)
        cand = _make_summary(duration_ms=200_000)
        result = compare_spark_apps(base, cand, [], [])
        assert result.regression_detected

    def test_regression_detected_with_negative_net_score_and_regressions(self):
        """regression_detected is True when net_score < 0 and regressions exist."""
        base = _make_summary(total_shuffle_gb=1.0)
        cand = _make_summary(total_shuffle_gb=5.0)  # MEDIUM severity regression
        result = compare_spark_apps(base, cand, [], [])
        shuffle = next(m for m in result.metric_diffs if m.metric_name == "total_shuffle_gb")
        assert shuffle.regression_flag
        assert shuffle.severity == "MEDIUM"

    def test_mixed_improvements_and_regressions(self):
        base = _make_summary(duration_ms=100_000, total_spill_gb=10.0)
        cand = _make_summary(duration_ms=120_000, total_spill_gb=2.0)
        result = compare_spark_apps(base, cand, [], [])
        dm = next(m for m in result.metric_diffs if m.metric_name == "duration_ms")
        assert dm.regression_flag
        spill = next(m for m in result.metric_diffs if m.metric_name == "total_spill_gb")
        assert spill.improvement_flag
        assert "Regressed" in result.summary
        assert "Improved" in result.summary

    def test_summary_contains_regressed_metric_names(self):
        base = _make_summary(duration_ms=100_000)
        cand = _make_summary(duration_ms=150_000)
        result = compare_spark_apps(base, cand, [], [])
        assert "duration_ms" in result.summary

    def test_bottleneck_count_regression(self):
        """Bottleneck count going from nonzero to higher is a regression."""
        base_stages = _make_stages(count=3, bottleneck_type="skew")  # 3 bottlenecks
        cand_stages = [
            *_make_stages(count=3, bottleneck_type="skew"),
            *_make_stages(count=3, bottleneck_type="spill"),
        ]  # 6 bottlenecks
        summary = _make_summary()
        result = compare_spark_apps(summary, summary, base_stages, cand_stages)
        bn = next(m for m in result.metric_diffs if m.metric_name == "bottleneck_count")
        assert bn.regression_flag  # went from 3 to 6

    def test_bottleneck_count_zero_baseline_no_ratio(self):
        """When baseline bottleneck count is 0, ratio is None."""
        base_stages = _make_stages(count=3, bottleneck_type="")
        cand_stages = _make_stages(count=3, bottleneck_type="skew")
        summary = _make_summary()
        result = compare_spark_apps(summary, summary, base_stages, cand_stages)
        bn = next(m for m in result.metric_diffs if m.metric_name == "bottleneck_count")
        assert bn.relative_diff_ratio is None
        assert not bn.regression_flag  # can't detect from zero baseline


# ---------------------------------------------------------------------------
# _aggregate_streaming
# ---------------------------------------------------------------------------


def _make_streaming_query(
    query_id: str = "q-1",
    avg_batch_duration_ms: float = 5000,
    max_batch_duration_ms: float = 8000,
    avg_processed_rows_per_sec: float = 1000.0,
    max_state_memory_bytes: int = 0,
    total_rows_dropped_by_watermark: int = 0,
    avg_commit_ms: float = 100.0,
    bottleneck_type: str = "STREAM_OK",
) -> dict:
    return {
        "query_id": query_id,
        "avg_batch_duration_ms": avg_batch_duration_ms,
        "max_batch_duration_ms": max_batch_duration_ms,
        "avg_processed_rows_per_sec": avg_processed_rows_per_sec,
        "max_state_memory_bytes": max_state_memory_bytes,
        "total_rows_dropped_by_watermark": total_rows_dropped_by_watermark,
        "avg_commit_ms": avg_commit_ms,
        "bottleneck_type": bottleneck_type,
    }


class TestAggregateStreaming:
    def test_empty_queries_returns_empty(self):
        result = _aggregate_streaming([])
        assert result == {}

    def test_single_query(self):
        queries = [
            _make_streaming_query(
                avg_batch_duration_ms=5000,
                max_batch_duration_ms=8000,
                avg_processed_rows_per_sec=1000.0,
                avg_commit_ms=100.0,
                bottleneck_type="STREAM_SLOW_BATCH",
            )
        ]
        result = _aggregate_streaming(queries)
        assert result["stream_avg_batch_duration_ms"] == 5000
        assert result["stream_max_batch_duration_ms"] == 8000
        assert result["stream_avg_processed_rows_per_sec"] == 1000.0
        assert result["stream_avg_commit_ms"] == 100.0
        assert result["stream_bottleneck_count"] == 1.0

    def test_ok_bottleneck_not_counted(self):
        queries = [_make_streaming_query(bottleneck_type="STREAM_OK")]
        result = _aggregate_streaming(queries)
        assert result["stream_bottleneck_count"] == 0.0

    def test_multiple_queries_aggregation(self):
        queries = [
            _make_streaming_query(
                avg_batch_duration_ms=4000,
                max_batch_duration_ms=6000,
                avg_processed_rows_per_sec=800.0,
                max_state_memory_bytes=1000,
                total_rows_dropped_by_watermark=10,
                avg_commit_ms=50.0,
                bottleneck_type="STREAM_BACKLOG",
            ),
            _make_streaming_query(
                avg_batch_duration_ms=6000,
                max_batch_duration_ms=10000,
                avg_processed_rows_per_sec=1200.0,
                max_state_memory_bytes=2000,
                total_rows_dropped_by_watermark=20,
                avg_commit_ms=150.0,
                bottleneck_type="STREAM_OK",
            ),
        ]
        result = _aggregate_streaming(queries)
        assert result["stream_avg_batch_duration_ms"] == pytest.approx(5000)
        assert result["stream_max_batch_duration_ms"] == 10000
        assert result["stream_avg_processed_rows_per_sec"] == pytest.approx(1000)
        assert result["stream_max_state_memory_bytes"] == 2000
        assert result["stream_total_rows_dropped_by_watermark"] == 30
        assert result["stream_avg_commit_ms"] == pytest.approx(100)
        assert result["stream_bottleneck_count"] == 1.0  # only BACKLOG counted

    def test_none_values_treated_as_zero(self):
        queries = [{"query_id": "q-1", "bottleneck_type": "STREAM_OK"}]
        result = _aggregate_streaming(queries)
        assert result["stream_avg_batch_duration_ms"] == 0
        assert result["stream_bottleneck_count"] == 0.0


# ---------------------------------------------------------------------------
# Streaming comparison (integration)
# ---------------------------------------------------------------------------


class TestStreamingComparison:
    def test_both_streaming_includes_metrics(self):
        summary = _make_summary()
        base_stream = [_make_streaming_query(avg_batch_duration_ms=5000)]
        cand_stream = [_make_streaming_query(avg_batch_duration_ms=5000)]
        result = compare_spark_apps(
            summary,
            summary,
            [],
            [],
            baseline_streaming=base_stream,
            candidate_streaming=cand_stream,
        )
        metric_names = {m.metric_name for m in result.metric_diffs}
        assert _STREAMING_AGG_METRICS.issubset(metric_names)

    def test_streaming_regression_detected(self):
        summary = _make_summary()
        base_stream = [_make_streaming_query(avg_batch_duration_ms=5000)]
        cand_stream = [_make_streaming_query(avg_batch_duration_ms=15000)]  # 3x increase
        result = compare_spark_apps(
            summary,
            summary,
            [],
            [],
            baseline_streaming=base_stream,
            candidate_streaming=cand_stream,
        )
        dm = next(m for m in result.metric_diffs if m.metric_name == "stream_avg_batch_duration_ms")
        assert dm.regression_flag  # duration increased (WORSENS)

    def test_streaming_throughput_improvement(self):
        summary = _make_summary()
        base_stream = [_make_streaming_query(avg_processed_rows_per_sec=500)]
        cand_stream = [_make_streaming_query(avg_processed_rows_per_sec=2000)]
        result = compare_spark_apps(
            summary,
            summary,
            [],
            [],
            baseline_streaming=base_stream,
            candidate_streaming=cand_stream,
        )
        tp = next(
            m for m in result.metric_diffs if m.metric_name == "stream_avg_processed_rows_per_sec"
        )
        assert tp.improvement_flag  # throughput increased (IMPROVES)

    def test_one_side_streaming_skips_metrics(self):
        summary = _make_summary()
        base_stream = [_make_streaming_query()]
        result = compare_spark_apps(
            summary,
            summary,
            [],
            [],
            baseline_streaming=base_stream,
            candidate_streaming=None,
        )
        metric_names = {m.metric_name for m in result.metric_diffs}
        assert not _STREAMING_AGG_METRICS.intersection(metric_names)
        assert "Streaming metrics skipped" in result.summary

    def test_neither_streaming_skips_metrics(self):
        summary = _make_summary()
        result = compare_spark_apps(
            summary,
            summary,
            [],
            [],
            baseline_streaming=None,
            candidate_streaming=None,
        )
        metric_names = {m.metric_name for m in result.metric_diffs}
        assert not _STREAMING_AGG_METRICS.intersection(metric_names)
        assert "Streaming metrics skipped" not in result.summary

    def test_no_streaming_params_backward_compatible(self):
        """Existing callers without streaming args still work."""
        summary = _make_summary()
        result = compare_spark_apps(summary, summary, [], [])
        assert not result.regression_detected

    def test_build_summary_with_streaming_skipped(self):
        diffs = [MetricDiff(metric_name="duration_ms")]
        summary = _build_summary(diffs, 0.0, streaming_skipped=True)
        assert "Streaming metrics skipped" in summary

    def test_build_summary_without_streaming_skipped(self):
        diffs = [MetricDiff(metric_name="duration_ms")]
        summary = _build_summary(diffs, 0.0, streaming_skipped=False)
        assert "Streaming metrics skipped" not in summary
