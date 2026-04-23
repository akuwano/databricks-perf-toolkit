"""Regression tests for shuffle memory efficiency alert false-positives.

Node 212 in Q23 profile reported 269,550 MB/partition (≈ 263 GB) with
no spill and only 88 KB written. That value came from dividing the
cumulative node-level peakMemoryBytes by the coalesced output
partition count (1) — a formula that breaks down for final-coalesce
shuffles.

The fix uses Sink-side metrics (peak memory / tasks) as the primary
formula, and adds sanity gates in the bottleneck analyzer to drop
physically impossible readings.
"""

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.models import QueryMetrics, ShuffleMetrics


def _qm() -> QueryMetrics:
    return QueryMetrics(
        query_id="t",
        status="FINISHED",
        total_time_ms=1000,
        execution_time_ms=1000,
        read_bytes=1_000_000_000,
    )


class TestMemoryPerPartitionFormula:
    def test_uses_sink_peak_over_sink_tasks_when_available(self):
        # Simulates the Q23 Node 212 scenario.
        sm = ShuffleMetrics(
            node_id="212",
            partition_count=1,  # coalesced to single output
            peak_memory_bytes=282_643_862_064,  # cumulative node peak (misleading)
            sink_peak_memory_bytes=7_540_844_544,  # 7.5 GB actual sink working memory
            sink_tasks_total=1797,
            sink_bytes_written=88_069,
            sink_num_spills=0,
        )
        # 7.5 GB / 1797 tasks ≈ 4 MB per task — realistic
        assert 2 <= sm.memory_per_partition_mb <= 10

    def test_falls_back_to_old_formula_when_sink_absent(self):
        sm = ShuffleMetrics(
            node_id="1",
            partition_count=100,
            peak_memory_bytes=10 * 1024 * 1024 * 1024,  # 10 GB
            sink_peak_memory_bytes=0,
            sink_tasks_total=0,
        )
        # 10 GB / 100 = 100 MB/partition
        assert 95 <= sm.memory_per_partition_mb <= 105

    def test_returns_zero_when_partition_count_is_one_and_no_sink(self):
        """Single-partition without sink data cannot be meaningfully divided."""
        sm = ShuffleMetrics(
            node_id="1",
            partition_count=1,
            peak_memory_bytes=10 * 1024 * 1024 * 1024,
        )
        assert sm.memory_per_partition_mb == 0.0


class TestAlertSuppression:
    def test_lightweight_shuffle_suppressed(self):
        """Node 212 pattern must not trigger a memory inefficiency alert."""
        sm = ShuffleMetrics(
            node_id="212",
            partition_count=1,
            peak_memory_bytes=282_643_862_064,
            sink_peak_memory_bytes=7_540_844_544,
            sink_tasks_total=1797,
            sink_bytes_written=88_069,
            sink_num_spills=0,
        )
        bi = calculate_bottleneck_indicators(_qm(), [], [sm], [])
        mem_alerts = [a for a in bi.alerts if a.metric_name == "memory_per_partition"]
        assert not mem_alerts, f"unexpected alert: {[a.message for a in mem_alerts]}"

    def test_genuine_heavy_shuffle_still_alerts(self):
        """Actual 2 GB/partition with spill should still alert."""
        sm = ShuffleMetrics(
            node_id="1",
            partition_count=10,
            peak_memory_bytes=30 * 1024 * 1024 * 1024,  # 30 GB across 10 parts
            sink_peak_memory_bytes=30 * 1024 * 1024 * 1024,
            sink_tasks_total=10,
            sink_bytes_written=5 * 1024 * 1024 * 1024,  # 5 GB written
            sink_num_spills=100,
        )
        bi = calculate_bottleneck_indicators(_qm(), [], [sm], [])
        mem_alerts = [a for a in bi.alerts if a.metric_name == "memory_per_partition"]
        assert mem_alerts, "expected memory_per_partition alert for heavy shuffle"

    def test_impossible_value_without_spill_suppressed(self):
        """10+ GB/partition with zero spill is physically impossible.

        Such readings indicate the formula is breaking down, not a real
        memory problem.
        """
        sm = ShuffleMetrics(
            node_id="99",
            partition_count=2,
            peak_memory_bytes=50 * 1024 * 1024 * 1024,  # 25 GB/partition
            sink_num_spills=0,
            sink_bytes_written=100 * 1024 * 1024,  # 100 MB — small
        )
        bi = calculate_bottleneck_indicators(_qm(), [], [sm], [])
        mem_alerts = [a for a in bi.alerts if a.metric_name == "memory_per_partition"]
        assert not mem_alerts

    def test_moderate_heavy_alerts(self):
        """500 MB/partition with spill should alert (below impossible threshold)."""
        sm = ShuffleMetrics(
            node_id="5",
            partition_count=20,
            peak_memory_bytes=10 * 1024 * 1024 * 1024,  # 500 MB/part
            sink_bytes_written=2 * 1024 * 1024 * 1024,
            sink_num_spills=5,
        )
        bi = calculate_bottleneck_indicators(_qm(), [], [sm], [])
        mem_alerts = [a for a in bi.alerts if a.metric_name == "memory_per_partition"]
        assert mem_alerts
