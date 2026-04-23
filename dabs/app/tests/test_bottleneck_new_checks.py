"""Tests for new bottleneck checks: result cache, queue time, result fetch, compilation phases."""

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.models import QueryMetrics


def _make_qm(**overrides) -> QueryMetrics:
    """Build a QueryMetrics with sensible defaults."""
    defaults = dict(
        query_id="test-1",
        status="FINISHED",
        query_text="SELECT 1",
        total_time_ms=10000,
        compilation_time_ms=500,
        execution_time_ms=9000,
        read_bytes=1_000_000_000,
        read_cache_bytes=800_000_000,
        photon_total_time_ms=7000,
        task_total_time_ms=9000,
        read_files_count=100,
        pruned_files_count=900,
    )
    defaults.update(overrides)
    return QueryMetrics(**defaults)


def _calc(qm: QueryMetrics):
    return calculate_bottleneck_indicators(qm, [], [], [])


# ---------------------------------------------------------------------------
# Result Cache Hit
# ---------------------------------------------------------------------------
class TestResultCacheAnnotation:
    """When result_from_cache=True, analysis should add an INFO alert."""

    def test_cache_hit_adds_info_alert(self):
        qm = _make_qm(result_from_cache=True)
        bi = _calc(qm)
        cache_alerts = [a for a in bi.alerts if a.category == "result_cache"]
        assert len(cache_alerts) == 1
        assert cache_alerts[0].severity.name == "INFO"
        assert not cache_alerts[0].is_actionable

    def test_no_alert_when_not_cached(self):
        qm = _make_qm(result_from_cache=False)
        bi = _calc(qm)
        cache_alerts = [a for a in bi.alerts if a.category == "result_cache"]
        assert len(cache_alerts) == 0

    def test_signal_emitted_when_cached(self):
        qm = _make_qm(result_from_cache=True)
        bi = _calc(qm)
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "result_from_cache" in signal_ids


# ---------------------------------------------------------------------------
# Queue Time (Serverless)
# ---------------------------------------------------------------------------
class TestQueueTimeBottleneck:
    """Queue wait time detection for Serverless warehouses."""

    def test_high_provisioning_queue_alert(self):
        qm = _make_qm(queued_provisioning_time_ms=15000)  # 15s
        bi = _calc(qm)
        queue_alerts = [a for a in bi.alerts if a.category == "queue"]
        assert len(queue_alerts) >= 1
        assert any(a.metric_name == "queued_provisioning_time_ms" for a in queue_alerts)

    def test_high_overload_queue_alert(self):
        qm = _make_qm(queued_overload_time_ms=10000)  # 10s
        bi = _calc(qm)
        queue_alerts = [a for a in bi.alerts if a.category == "queue"]
        assert len(queue_alerts) >= 1
        assert any(a.metric_name == "queued_overload_time_ms" for a in queue_alerts)

    def test_no_alert_for_low_queue(self):
        qm = _make_qm(queued_provisioning_time_ms=500, queued_overload_time_ms=200)
        bi = _calc(qm)
        queue_alerts = [a for a in bi.alerts if a.category == "queue"]
        assert len(queue_alerts) == 0

    def test_signal_emitted_for_high_queue(self):
        qm = _make_qm(queued_provisioning_time_ms=20000)
        bi = _calc(qm)
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "high_queue_time" in signal_ids

    def test_no_signal_for_zero_queue(self):
        qm = _make_qm()
        bi = _calc(qm)
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "high_queue_time" not in signal_ids


# ---------------------------------------------------------------------------
# Result Fetch Time
# ---------------------------------------------------------------------------
class TestResultFetchTimeBottleneck:
    """Large result set detection via result_fetch_time_ms."""

    def test_high_fetch_time_alert(self):
        # fetch time > 10% of total = 1000ms, and > 5s absolute
        qm = _make_qm(total_time_ms=10000, result_fetch_time_ms=6000)
        bi = _calc(qm)
        fetch_alerts = [a for a in bi.alerts if a.metric_name == "result_fetch_time_ms"]
        assert len(fetch_alerts) == 1

    def test_no_alert_for_low_fetch_time(self):
        qm = _make_qm(total_time_ms=10000, result_fetch_time_ms=100)
        bi = _calc(qm)
        fetch_alerts = [a for a in bi.alerts if a.metric_name == "result_fetch_time_ms"]
        assert len(fetch_alerts) == 0

    def test_signal_emitted(self):
        qm = _make_qm(total_time_ms=10000, result_fetch_time_ms=8000)
        bi = _calc(qm)
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "high_result_fetch_time" in signal_ids


# ---------------------------------------------------------------------------
# Compilation Phase Breakdown
# ---------------------------------------------------------------------------
class TestCompilationPhaseBottleneck:
    """Slow compilation phase detection from planning_phases."""

    def test_slow_optimization_alert(self):
        qm = _make_qm(
            compilation_time_ms=30000,
            planning_phases=[
                {"phase": "ANALYSIS", "duration_ms": 2000},
                {"phase": "OPTIMIZATION", "duration_ms": 25000},
                {"phase": "PLANNING", "duration_ms": 3000},
            ],
        )
        bi = _calc(qm)
        phase_alerts = [a for a in bi.alerts if a.category == "compilation"]
        assert len(phase_alerts) >= 1
        # Should identify OPTIMIZATION as the dominant phase
        assert any("OPTIMIZATION" in a.message for a in phase_alerts)

    def test_no_alert_for_fast_compilation(self):
        qm = _make_qm(
            compilation_time_ms=500,
            planning_phases=[
                {"phase": "ANALYSIS", "duration_ms": 100},
                {"phase": "OPTIMIZATION", "duration_ms": 300},
                {"phase": "PLANNING", "duration_ms": 100},
            ],
        )
        bi = _calc(qm)
        phase_alerts = [a for a in bi.alerts if a.category == "compilation"]
        assert len(phase_alerts) == 0

    def test_no_alert_when_no_phases(self):
        qm = _make_qm(compilation_time_ms=30000, planning_phases=[])
        bi = _calc(qm)
        phase_alerts = [a for a in bi.alerts if a.category == "compilation"]
        assert len(phase_alerts) == 0

    def test_signal_emitted_for_slow_compilation(self):
        qm = _make_qm(
            compilation_time_ms=20000,
            planning_phases=[
                {"phase": "ANALYSIS", "duration_ms": 15000},
                {"phase": "OPTIMIZATION", "duration_ms": 4000},
                {"phase": "PLANNING", "duration_ms": 1000},
            ],
        )
        bi = _calc(qm)
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "slow_compilation" in signal_ids


# ---------------------------------------------------------------------------
# Metadata Time
# ---------------------------------------------------------------------------
class TestMetadataTimeBottleneck:
    """High metadata resolution time detection."""

    def test_high_metadata_time_alert(self):
        qm = _make_qm(metadata_time_ms=60000)  # 60s
        bi = _calc(qm)
        meta_alerts = [a for a in bi.alerts if a.metric_name == "metadata_time_ms"]
        assert len(meta_alerts) == 1

    def test_no_alert_for_normal_metadata_time(self):
        qm = _make_qm(metadata_time_ms=500)
        bi = _calc(qm)
        meta_alerts = [a for a in bi.alerts if a.metric_name == "metadata_time_ms"]
        assert len(meta_alerts) == 0
