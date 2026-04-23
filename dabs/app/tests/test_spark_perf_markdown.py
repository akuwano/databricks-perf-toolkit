"""Tests for core.spark_perf_markdown module."""

import pytest
from core.spark_perf_markdown import (
    _fmt_num,
    _percentile,
    build_streaming_analysis_comment,
    build_streaming_section,
    compute_streaming_alerts,
    estimate_trigger_interval_ms,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_streaming_query(
    query_id: str = "query-abc-123",
    source_type: str = "CloudFiles",
    sink_type: str = "Delta",
    total_batches: int = 10,
    avg_batch_duration_ms: float = 5000,
    avg_processed_rows_per_sec: float = 1000.0,
    bottleneck_type: str = "STREAM_OK",
    severity: str = "NONE",
    recommendation: str = "",
) -> dict:
    return {
        "query_id": query_id,
        "source_type": source_type,
        "sink_type": sink_type,
        "total_batches": total_batches,
        "avg_batch_duration_ms": avg_batch_duration_ms,
        "avg_processed_rows_per_sec": avg_processed_rows_per_sec,
        "bottleneck_type": bottleneck_type,
        "severity": severity,
        "recommendation": recommendation,
    }


def _make_streaming_summary(
    query_count: int = 1,
    total_batches: int = 10,
    avg_batch_duration_ms: float = 5000,
    avg_throughput_rows_per_sec: float = 1000.0,
    max_state_memory_bytes: int = 0,
    stateful_query_count: int = 0,
    has_exceptions: bool = False,
    source_types: list | None = None,
    sink_types: list | None = None,
) -> dict:
    return {
        "query_count": query_count,
        "total_batches": total_batches,
        "avg_batch_duration_ms": avg_batch_duration_ms,
        "avg_throughput_rows_per_sec": avg_throughput_rows_per_sec,
        "max_state_memory_bytes": max_state_memory_bytes,
        "stateful_query_count": stateful_query_count,
        "has_exceptions": has_exceptions,
        "source_types": source_types or ["CloudFiles"],
        "sink_types": sink_types or ["Delta"],
    }


def _make_batch(
    batch_id: int = 0,
    batch_duration_ms: float = 5000,
    add_batch_ms: float = 4000,
    query_planning_ms: float = 200,
    latest_offset_ms: float = 300,
    commit_offsets_ms: float = 200,
    commit_batch_ms: float = 100,
    num_input_rows: int = 1000,
    processed_rows_per_sec: float = 200.0,
    query_id: str = "query-abc-123",
) -> dict:
    return {
        "batch_id": batch_id,
        "query_id": query_id,
        "batch_duration_ms": batch_duration_ms,
        "add_batch_ms": add_batch_ms,
        "query_planning_ms": query_planning_ms,
        "latest_offset_ms": latest_offset_ms,
        "commit_offsets_ms": commit_offsets_ms,
        "commit_batch_ms": commit_batch_ms,
        "num_input_rows": num_input_rows,
        "processed_rows_per_sec": processed_rows_per_sec,
    }


def _make_idle_events(timestamps: list[str]) -> list[dict]:
    return [{"event_timestamp": ts} for ts in timestamps]


# ---------------------------------------------------------------------------
# _percentile
# ---------------------------------------------------------------------------


class TestPercentile:
    def test_empty_list_returns_zero(self):
        assert _percentile([], 0.5) == 0.0

    def test_single_value(self):
        assert _percentile([42.0], 0.95) == 42.0

    def test_two_values_median(self):
        result = _percentile([10.0, 20.0], 0.5)
        assert result == pytest.approx(15.0)

    def test_p95_of_sorted_list(self):
        vals = list(range(1, 101))  # 1..100
        result = _percentile([float(v) for v in vals], 0.95)
        assert result == pytest.approx(95.05, rel=0.01)

    def test_p0_returns_min(self):
        assert _percentile([5.0, 10.0, 15.0], 0.0) == 5.0

    def test_p100_returns_max(self):
        assert _percentile([5.0, 10.0, 15.0], 1.0) == 15.0


# ---------------------------------------------------------------------------
# _fmt_num
# ---------------------------------------------------------------------------


class TestFmtNum:
    def test_none_returns_dash(self):
        assert _fmt_num(None) == "-"

    def test_zero_decimals(self):
        assert _fmt_num(1234.5) == "1,234"

    def test_two_decimals(self):
        assert _fmt_num(1234.567, decimals=2) == "1,234.57"

    def test_zero_value(self):
        assert _fmt_num(0.0) == "0"


# ---------------------------------------------------------------------------
# estimate_trigger_interval_ms
# ---------------------------------------------------------------------------


class TestEstimateTriggerInterval:
    def test_none_with_no_events(self):
        assert estimate_trigger_interval_ms(None, None) is None

    def test_none_with_empty_events(self):
        assert estimate_trigger_interval_ms([], []) is None

    def test_none_with_single_event(self):
        events = _make_idle_events(["2026-01-01T00:00:00"])
        assert estimate_trigger_interval_ms(events, None) is None

    def test_snaps_to_common_interval_10s(self):
        events = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:10",
                "2026-01-01T00:00:20",
            ]
        )
        result = estimate_trigger_interval_ms(events, None)
        assert result == 10000  # snaps to 10s

    def test_snaps_to_common_interval_30s(self):
        events = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:30",
                "2026-01-01T00:01:00",
            ]
        )
        result = estimate_trigger_interval_ms(events, None)
        assert result == 30000

    def test_snaps_to_common_interval_60s(self):
        events = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:01:00",
                "2026-01-01T00:02:00",
            ]
        )
        result = estimate_trigger_interval_ms(events, None)
        assert result == 60000

    def test_non_exact_interval_snaps_to_nearest_common(self):
        # 8 second intervals: closest common = 10s (diff=2000, ratio=2000/8000=0.25 < 0.5)
        # → snaps to 10s
        events = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:08",
                "2026-01-01T00:00:16",
            ]
        )
        result = estimate_trigger_interval_ms(events, None)
        assert result == 10000

    def test_handles_datetime_with_microseconds(self):
        events = _make_idle_events(
            [
                "2026-01-01T00:00:00.000000",
                "2026-01-01T00:00:10.000000",
                "2026-01-01T00:00:20.000000",
            ]
        )
        result = estimate_trigger_interval_ms(events, None)
        assert result == 10000

    def test_handles_space_separated_datetime(self):
        events = _make_idle_events(
            [
                "2026-01-01 00:00:00",
                "2026-01-01 00:00:10",
                "2026-01-01 00:00:20",
            ]
        )
        result = estimate_trigger_interval_ms(events, None)
        assert result == 10000

    def test_invalid_timestamps_returns_none(self):
        events = [{"event_timestamp": "not-a-date"}, {"event_timestamp": "also-bad"}]
        assert estimate_trigger_interval_ms(events, None) is None

    def test_missing_timestamp_key_skipped(self):
        events = [{"other_key": "val"}, {"other_key": "val2"}]
        assert estimate_trigger_interval_ms(events, None) is None


# ---------------------------------------------------------------------------
# compute_streaming_alerts
# ---------------------------------------------------------------------------


class TestComputeStreamingAlerts:
    def test_empty_batches_returns_empty(self):
        assert compute_streaming_alerts(None, None) == {}
        assert compute_streaming_alerts([], None) == {}

    def test_no_lag_when_all_within_trigger(self):
        batches = [_make_batch(batch_id=i, batch_duration_ms=3000) for i in range(5)]
        idle = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:10",
                "2026-01-01T00:00:20",
            ]
        )
        result = compute_streaming_alerts(batches, idle)
        assert result["trigger_interval_ms"] == 10000
        assert result["trigger_lag"]["count"] == 0
        assert result["trigger_lag"]["severity"] == "NONE"

    def test_lag_detected_when_duration_exceeds_trigger(self):
        batches = [
            _make_batch(batch_id=0, batch_duration_ms=3000),
            _make_batch(batch_id=1, batch_duration_ms=15000),  # exceeds 10s trigger
            _make_batch(batch_id=2, batch_duration_ms=20000),  # exceeds 10s trigger
        ]
        idle = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:10",
                "2026-01-01T00:00:20",
            ]
        )
        result = compute_streaming_alerts(batches, idle)
        lag = result["trigger_lag"]
        assert lag["count"] == 2
        assert lag["severity"] == "HIGH"  # >50% of batches

    def test_lag_medium_severity_when_minority(self):
        batches = [
            _make_batch(batch_id=0, batch_duration_ms=3000),
            _make_batch(batch_id=1, batch_duration_ms=3000),
            _make_batch(batch_id=2, batch_duration_ms=3000),
            _make_batch(batch_id=3, batch_duration_ms=15000),  # only 1 of 4 exceeds
        ]
        idle = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:10",
                "2026-01-01T00:00:20",
            ]
        )
        result = compute_streaming_alerts(batches, idle)
        assert result["trigger_lag"]["severity"] == "MEDIUM"

    def test_no_spike_when_all_similar(self):
        batches = [_make_batch(batch_id=i, batch_duration_ms=5000) for i in range(5)]
        result = compute_streaming_alerts(batches, None)
        assert result["duration_spike"]["count"] == 0
        assert result["duration_spike"]["severity"] == "NONE"

    def test_spike_detected(self):
        # avg = (1000+1000+1000+10000)/4 = 3250, threshold = 3250*3 = 9750
        # batch_id=3 (10000ms) exceeds threshold
        batches = [
            _make_batch(batch_id=0, batch_duration_ms=1000),
            _make_batch(batch_id=1, batch_duration_ms=1000),
            _make_batch(batch_id=2, batch_duration_ms=1000),
            _make_batch(batch_id=3, batch_duration_ms=10000),
        ]
        result = compute_streaming_alerts(batches, None)
        spike = result["duration_spike"]
        assert spike["count"] == 1
        assert spike["severity"] == "MEDIUM"
        assert spike["worst"][0]["batch_id"] == 3

    def test_no_spike_with_fewer_than_3_batches(self):
        batches = [_make_batch(batch_id=0, batch_duration_ms=1000)]
        result = compute_streaming_alerts(batches, None)
        assert "duration_spike" not in result

    def test_no_trigger_lag_without_idle_events(self):
        batches = [_make_batch(batch_id=i, batch_duration_ms=5000) for i in range(5)]
        result = compute_streaming_alerts(batches, None)
        assert result["trigger_interval_ms"] is None
        assert "trigger_lag" not in result


# ---------------------------------------------------------------------------
# build_streaming_section
# ---------------------------------------------------------------------------


class TestBuildStreamingSection:
    def test_empty_queries_returns_not_detected(self):
        md = build_streaming_section([], {})
        assert "## G." in md
        assert "detected" in md.lower() or "検出" in md

    def test_zero_query_count_returns_not_detected(self):
        summary = _make_streaming_summary(query_count=0)
        md = build_streaming_section([_make_streaming_query()], summary)
        assert "## G." in md
        assert "detected" in md.lower() or "検出" in md

    def test_basic_section_ja(self):
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary()
        md = build_streaming_section(queries, summary, lang="ja")
        assert "## G. ストリーミング分析" in md
        assert "ストリーミングクエリ数" in md
        assert "CloudFiles" in md
        assert "STREAM_OK" in md

    def test_basic_section_en(self):
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary()
        md = build_streaming_section(queries, summary, lang="en")
        assert "## G. Streaming Analysis" in md
        assert "Streaming Queries" in md
        assert "CloudFiles" in md

    def test_includes_batch_summary_for_problem_query(self):
        """Problem queries (non-OK bottleneck) should show batch summary detail."""
        queries = [_make_streaming_query(bottleneck_type="STREAM_SLOW_BATCH", severity="MEDIUM")]
        summary = _make_streaming_summary()
        batches = [_make_batch(batch_id=i) for i in range(5)]
        md = build_streaming_section(queries, summary, streaming_batches=batches, lang="en")
        assert "Problem Query Details" in md
        assert "Min" in md
        assert "P95" in md

    def test_no_batch_summary_without_batches(self):
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary()
        md = build_streaming_section(queries, summary, streaming_batches=None, lang="en")
        assert "Batch Summary Metrics" not in md

    def test_trigger_lag_shown_for_problem_query(self):
        """Query with lag batches should appear in problem details."""
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary()
        batches = [
            _make_batch(batch_id=0, batch_duration_ms=3000),
            _make_batch(batch_id=1, batch_duration_ms=15000),
            _make_batch(batch_id=2, batch_duration_ms=3000),
        ]
        idle = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:10",
                "2026-01-01T00:00:20",
            ]
        )
        md = build_streaming_section(
            queries, summary, streaming_batches=batches, idle_events=idle, lang="en"
        )
        assert "Problem Query Details" in md
        assert "Trigger Lag" in md
        assert "1/3" in md or "15,000" in md or "15000" in md

    def test_spike_shown_for_problem_query(self):
        """Query with spike batches should appear in problem details."""
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary()
        # avg = (1000*3 + 10000)/4 = 3250, threshold = 9750 → 10000 > 9750
        batches = [
            _make_batch(batch_id=0, batch_duration_ms=1000),
            _make_batch(batch_id=1, batch_duration_ms=1000),
            _make_batch(batch_id=2, batch_duration_ms=1000),
            _make_batch(batch_id=3, batch_duration_ms=10000),
        ]
        md = build_streaming_section(queries, summary, streaming_batches=batches, lang="en")
        assert "Problem Query Details" in md
        assert "Duration Spike" in md

    def test_bottleneck_evaluation_includes_all_queries(self):
        queries = [
            _make_streaming_query(query_id="q1", bottleneck_type="STREAM_BACKLOG", severity="HIGH"),
            _make_streaming_query(query_id="q2", bottleneck_type="STREAM_OK", severity="NONE"),
        ]
        summary = _make_streaming_summary(query_count=2)
        md = build_streaming_section(queries, summary, lang="en")
        assert "STREAM_BACKLOG" in md
        assert "STREAM_OK" in md

    def test_trigger_interval_shown_in_query_table(self):
        """Per-query trigger interval should appear in query list table."""
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary()
        idle = _make_idle_events(
            [
                "2026-01-01T00:00:00",
                "2026-01-01T00:00:30",
                "2026-01-01T00:01:00",
            ]
        )
        md = build_streaming_section(queries, summary, idle_events=idle, lang="en")
        assert "Trigger" in md
        assert "30s" in md

    def test_analysis_summary_present(self):
        """Analysis comment is now generated by build_streaming_analysis_comment()."""
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary()
        batches = [_make_batch(batch_id=i) for i in range(3)]
        md = build_streaming_analysis_comment(
            streaming_summary=summary,
            streaming_batches=batches,
            streaming_queries=queries,
            deep_analysis=None,
            trigger_interval_ms=None,
            lang="en",
        )
        assert "Analysis Summary" in md

    def test_multiple_queries_all_listed(self):
        queries = [_make_streaming_query(query_id=f"query-{i}") for i in range(3)]
        summary = _make_streaming_summary(query_count=3)
        md = build_streaming_section(queries, summary, lang="en")
        assert md.count("query-") >= 3

    def test_stateful_query_noted_in_overview_comment(self):
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary(stateful_query_count=2)
        md = build_streaming_section(queries, summary, lang="en")
        assert "stateful" in md.lower()

    def test_exception_noted_in_overview_comment(self):
        queries = [_make_streaming_query()]
        summary = _make_streaming_summary(has_exceptions=True)
        md = build_streaming_section(queries, summary, lang="en")
        assert "exception" in md.lower() or "Exception" in md
