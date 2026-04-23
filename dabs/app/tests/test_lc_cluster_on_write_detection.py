"""Detect Liquid Clustering ClusterOnWrite overhead.

Signature we look for:
- Target table is Delta with ``clustering_columns`` configured
- Spill-heavy hash-partition shuffle occurred before the write
- AT LEAST ONE of:
    - memory_per_partition_mb > 128 (the standard unhealthy threshold)
    - sink_num_spills > 0
    - peak_memory_bytes > 2 * sink_bytes_written (spill-prone ratio)

When all above hold → emit a HIGH alert telling the user the bottleneck is
the write-side LC re-shuffle, not the SQL logic.
"""

from core.analyzers.explain_analysis import (
    detect_lc_cluster_on_write_overhead,
)
from core.constants import Severity
from core.models import (
    BottleneckIndicators,
    ShuffleMetrics,
    TargetTableInfo,
)


def _delta_lc_target() -> TargetTableInfo:
    return TargetTableInfo(
        catalog="c",
        database="d",
        table="t",
        provider="delta",
        clustering_columns=[["COL1"], ["COL2"], ["COL3"]],
        hierarchical_clustering_columns=["col1", "col2"],
    )


def _heavy_spill_shuffle(node_id: str = "7388") -> ShuffleMetrics:
    return ShuffleMetrics(
        node_id=node_id,
        partition_count=3239,
        sink_bytes_written=1_154_000_000_000,  # 1154 GB
        peak_memory_bytes=11_246_000_000_000,  # 11.2 TB
        sink_num_spills=2534,
    )


class TestDetectLcClusterOnWriteOverhead:
    def test_fires_high_alert_on_lc_delta_write_with_spill(self):
        ind = BottleneckIndicators()
        sm = [_heavy_spill_shuffle()]
        target = _delta_lc_target()
        detect_lc_cluster_on_write_overhead(ind, sm, target)
        hits = [a for a in ind.alerts if a.metric_name == "lc_cluster_on_write_spill"]
        assert len(hits) == 1
        assert hits[0].severity == Severity.HIGH

    def test_alert_mentions_clustering_column_names(self):
        ind = BottleneckIndicators()
        detect_lc_cluster_on_write_overhead(ind, [_heavy_spill_shuffle()], _delta_lc_target())
        msg = next(a.message for a in ind.alerts if a.metric_name == "lc_cluster_on_write_spill")
        assert "COL1" in msg
        assert "COL2" in msg
        assert "COL3" in msg

    def test_no_alert_when_target_not_delta(self):
        ind = BottleneckIndicators()
        parquet_target = TargetTableInfo(provider="parquet")
        detect_lc_cluster_on_write_overhead(ind, [_heavy_spill_shuffle()], parquet_target)
        assert not any(a.metric_name == "lc_cluster_on_write_spill" for a in ind.alerts)

    def test_no_alert_when_no_clustering_columns(self):
        ind = BottleneckIndicators()
        delta_no_cluster = TargetTableInfo(provider="delta", clustering_columns=[])
        detect_lc_cluster_on_write_overhead(ind, [_heavy_spill_shuffle()], delta_no_cluster)
        assert not any(a.metric_name == "lc_cluster_on_write_spill" for a in ind.alerts)

    def test_no_alert_when_no_spill(self):
        """Healthy shuffle (<128MB/part, 0 spill) — no alert."""
        ind = BottleneckIndicators()
        healthy = ShuffleMetrics(
            node_id="1",
            partition_count=200,
            sink_bytes_written=10_000_000,
            peak_memory_bytes=20_000_000,  # 20 MB peak
            sink_num_spills=0,
        )
        detect_lc_cluster_on_write_overhead(ind, [healthy], _delta_lc_target())
        assert not any(a.metric_name == "lc_cluster_on_write_spill" for a in ind.alerts)

    def test_no_alert_when_target_info_missing(self):
        ind = BottleneckIndicators()
        detect_lc_cluster_on_write_overhead(ind, [_heavy_spill_shuffle()], None)
        assert not any(a.metric_name == "lc_cluster_on_write_spill" for a in ind.alerts)

    def test_no_alert_below_scale_gate(self):
        """Sub-GiB writes with spill events should NOT fire. The scale gate
        (sink_bytes_written >= 1 GiB) protects against false positives on
        small/dev-sized INSERTs into LC tables."""
        ind = BottleneckIndicators()
        tiny_but_spilly = ShuffleMetrics(
            node_id="1",
            partition_count=50,
            sink_bytes_written=50_000_000,  # 50 MB — below 1 GiB gate
            peak_memory_bytes=500_000_000,  # 500 MB peak (>3x ratio on 50 MB written)
            sink_num_spills=5,  # >=3 spills
        )
        detect_lc_cluster_on_write_overhead(ind, [tiny_but_spilly], _delta_lc_target())
        assert not any(a.metric_name == "lc_cluster_on_write_spill" for a in ind.alerts)

    def test_no_alert_for_single_spill_on_meaningful_write(self):
        """A single spill event on a meaningful-sized write should not fire —
        spill count gate requires >=3 and no other pressure signal present."""
        ind = BottleneckIndicators()
        borderline = ShuffleMetrics(
            node_id="1",
            partition_count=400,
            sink_tasks_total=400,
            sink_bytes_written=5_000_000_000,  # 5 GiB — passes scale gate
            # peak memory 7 GB / 400 tasks = ~17 MB/part, ratio <3x on 5GiB written
            peak_memory_bytes=7_000_000_000,
            sink_num_spills=1,  # <3
        )
        detect_lc_cluster_on_write_overhead(ind, [borderline], _delta_lc_target())
        assert not any(a.metric_name == "lc_cluster_on_write_spill" for a in ind.alerts)

    def test_hierarchical_clustering_surfaced_in_recommendation(self):
        ind = BottleneckIndicators()
        detect_lc_cluster_on_write_overhead(ind, [_heavy_spill_shuffle()], _delta_lc_target())
        rec = next(
            a.recommendation for a in ind.alerts if a.metric_name == "lc_cluster_on_write_spill"
        )
        # Recommendation should mention options: OPTIMIZE separately / reduce LC keys
        lower = rec.lower()
        assert "optimize" in lower or "cluster" in lower


class TestRealProfileEndToEnd:
    def test_real_profile_triggers_alert(self):
        """End-to-end: run analyze_from_dict on the captured profile and
        verify the new alert fires."""
        import json
        from pathlib import Path

        from core.analyzers import analyze_from_dict

        repo_root = Path(__file__).resolve().parents[3]
        path = repo_root / "json" / "Dey" / "Master_table_insertion_after_optimization_2XL.json"
        if not path.exists():
            import pytest

            pytest.skip(f"Fixture profile not present at {path}")
        with open(path) as f:
            data = json.load(f)
        analysis = analyze_from_dict(data)
        # target_table_info attached
        assert analysis.target_table_info is not None
        assert analysis.target_table_info.is_delta is True
        assert len(analysis.target_table_info.clustering_columns) == 3
        # Alert fires
        lc_alerts = [
            a
            for a in analysis.bottleneck_indicators.alerts
            if a.metric_name == "lc_cluster_on_write_spill"
        ]
        assert len(lc_alerts) >= 1, (
            f"Expected lc_cluster_on_write_spill alert; got metric_names="
            f"{sorted({a.metric_name for a in analysis.bottleneck_indicators.alerts})}"
        )
