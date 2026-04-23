"""Regression tests for AQE-layout vs skew disambiguation.

When AQE successfully self-repartitioned at runtime AND no shuffle
spilled, the workload is a data-volume / physical-layout issue, NOT
key skew. Previously our "hash resize + high probes" heuristic alone
labeled such workloads as "likely data skew", which was misleading.
"""

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.analyzers.recommendations import generate_action_cards
from core.constants import JoinType
from core.models import (
    JoinInfo,
    NodeMetrics,
    QueryMetrics,
    QueryStructure,
    ShuffleMetrics,
    SQLAnalysis,
)


def _qm() -> QueryMetrics:
    return QueryMetrics(
        query_id="t",
        status="FINISHED",
        total_time_ms=1000,
        execution_time_ms=1000,
        read_bytes=1_000_000_000,
    )


def _agg_node(node_id: str, *, resize: int, probes: float, key: str) -> NodeMetrics:
    return NodeMetrics(
        node_id=node_id,
        node_name="Grouping Aggregate",
        node_tag="PHOTON_GROUPING_AGG_EXEC",
        grouping_expressions=[key],
        extra_metrics={
            "Number of times hash table was resized": resize,
            "Avg hash probes per row": probes,
        },
    )


def _shuffle(
    *,
    node_id: str,
    bytes_written: int,
    spills: int,
    aqe_self_repartition: int = 0,
    original_parts: int = 0,
    intended_parts: int = 0,
) -> ShuffleMetrics:
    return ShuffleMetrics(
        node_id=node_id,
        partition_count=intended_parts or 100,
        peak_memory_bytes=bytes_written * 10,
        sink_peak_memory_bytes=bytes_written,
        sink_tasks_total=max(intended_parts, 100),
        sink_bytes_written=bytes_written,
        sink_num_spills=spills,
        aqe_self_repartition_count=aqe_self_repartition,
        aqe_original_num_partitions=original_parts,
        aqe_intended_num_partitions=intended_parts,
    )


class TestAqeLayoutDowngradesSkew:
    def test_severe_skew_thresholds_downgraded_when_aqe_self_repartition_no_spill(self):
        """Resize >= 1000 AND probes >= 50 would normally trigger HIGH skew.
        With AQE self-repartition + no spill, that diagnosis is wrong."""
        nodes = [_agg_node("1", resize=5000, probes=100, key="db.s.t.k")]
        shuffles = [
            _shuffle(
                node_id="s1",
                bytes_written=200 * 1024 * 1024 * 1024,  # 200 GB
                spills=0,
                aqe_self_repartition=1,
                original_parts=112,
                intended_parts=3436,
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, shuffles, [])
        # Skew-wording alert (likely/suspected data skew) should NOT fire
        alerts = [a for a in bi.alerts if a.metric_name == "hash_table_resize_count"]
        assert alerts
        msg = alerts[0].message
        assert "likely data skew" not in msg
        assert "suspected data skew" not in msg
        # Instead, the AQE-layout diagnosis should be present
        assert "AQE self-repartitioned" in msg
        assert "NOT key skew" in msg

    def test_skew_still_flagged_when_spill_present(self):
        """Real skew: high resize + high probes + AQE repartition + spill.
        Presence of spill means AQE did not fully absorb the volume."""
        nodes = [_agg_node("1", resize=5000, probes=100, key="db.s.t.k")]
        shuffles = [
            _shuffle(
                node_id="s1",
                bytes_written=200 * 1024 * 1024 * 1024,
                spills=50,  # spilled
                aqe_self_repartition=1,
                original_parts=112,
                intended_parts=3436,
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, shuffles, [])
        alerts = [a for a in bi.alerts if a.metric_name == "hash_table_resize_count"]
        assert alerts
        msg = alerts[0].message
        assert "likely data skew" in msg or "suspected data skew" in msg

    def test_skew_still_flagged_when_no_aqe_repartition(self):
        """Classic skew without AQE intervention."""
        nodes = [_agg_node("1", resize=5000, probes=100, key="db.s.t.k")]
        shuffles = [
            _shuffle(
                node_id="s1",
                bytes_written=200 * 1024 * 1024 * 1024,
                spills=0,
                aqe_self_repartition=0,  # no AQE intervention
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, shuffles, [])
        alerts = [a for a in bi.alerts if a.metric_name == "hash_table_resize_count"]
        assert alerts
        assert "likely data skew" in alerts[0].message


class TestLargeShuffleLayoutCard:
    def _make_cards(self, nodes, shuffles):
        bi = calculate_bottleneck_indicators(_qm(), nodes, shuffles, [])
        return generate_action_cards(
            bi,
            [],
            _qm(),
            shuffles,
            [JoinInfo(join_type=JoinType.SHUFFLE_HASH)],
            SQLAnalysis(structure=QueryStructure(join_count=1)),
            [],
        )

    def test_layout_card_generated_on_aqe_pattern(self):
        nodes = [_agg_node("1", resize=5000, probes=100, key="db.sch.sales.customer_sk")]
        shuffles = [
            _shuffle(
                node_id="s1",
                bytes_written=200 * 1024 * 1024 * 1024,
                spills=0,
                aqe_self_repartition=1,
                original_parts=112,
                intended_parts=3436,
            )
        ]
        cards = self._make_cards(nodes, shuffles)
        layout = [c for c in cards if "Large shuffle" in c.problem or "AQE" in c.problem]
        assert layout, f"expected layout card, got {[c.problem for c in cards]}"
        card = layout[0]
        # Evidence mentions AQE growth ratio and bytes
        assert any("AQE" in e or "GB" in e for e in card.evidence)
        # fix_sql guides DESCRIBE + type review + clustering
        assert "DESCRIBE" in card.fix_sql
        assert "BIGINT" in card.fix_sql
        assert "CLUSTER BY" in card.fix_sql
        # Target table/column extracted from hotspot keys
        assert "db.sch.sales" in card.fix_sql
        assert "customer_sk" in card.fix_sql

    def test_layout_card_absent_without_aqe_pattern(self):
        nodes = [_agg_node("1", resize=500, probes=20, key="db.sch.t.k")]
        shuffles = [
            _shuffle(
                node_id="s1",
                bytes_written=1 * 1024 * 1024 * 1024,
                spills=0,
                aqe_self_repartition=0,
            )
        ]
        cards = self._make_cards(nodes, shuffles)
        assert not [c for c in cards if "Large shuffle" in c.problem]

    def test_layout_card_absent_when_spill_present(self):
        nodes = [_agg_node("1", resize=5000, probes=100, key="db.sch.t.k")]
        shuffles = [
            _shuffle(
                node_id="s1",
                bytes_written=200 * 1024 * 1024 * 1024,
                spills=100,  # spilled — not the layout pattern
                aqe_self_repartition=1,
                original_parts=112,
                intended_parts=3436,
            )
        ]
        cards = self._make_cards(nodes, shuffles)
        assert not [c for c in cards if "Large shuffle" in c.problem]


class TestShuffleMetricsAqeExtraction:
    def test_indicators_capture_aqe_flag_and_ratio(self):
        nodes = [_agg_node("1", resize=5000, probes=100, key="db.s.t.k")]
        shuffles = [
            _shuffle(
                node_id="s1",
                bytes_written=200 * 1024 * 1024 * 1024,
                spills=0,
                aqe_self_repartition=1,
                original_parts=112,
                intended_parts=3436,
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, shuffles, [])
        assert bi.aqe_self_repartition_seen is True
        assert 30 <= bi.max_aqe_partition_growth_ratio <= 31  # 3436/112 ≈ 30.7
        assert bi.shuffle_bytes_written_total == 200 * 1024 * 1024 * 1024
