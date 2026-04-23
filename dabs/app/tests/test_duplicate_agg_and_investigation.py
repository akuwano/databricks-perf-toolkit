"""Tests for:
- Duplicate GROUP BY detection (Feature A extension)
- Investigation SQL action card (Feature D)
"""

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.analyzers.recommendations import generate_action_cards
from core.constants import JoinType
from core.models import JoinInfo, NodeMetrics, QueryMetrics, QueryStructure, SQLAnalysis


def _qm() -> QueryMetrics:
    return QueryMetrics(
        query_id="t",
        status="FINISHED",
        total_time_ms=1000,
        execution_time_ms=1000,
        read_bytes=1000,
    )


def _agg(
    node_id: str,
    *,
    resize: int,
    probes: float = 50.0,
    group_exprs: list[str] | None = None,
) -> NodeMetrics:
    return NodeMetrics(
        node_id=node_id,
        node_name="Grouping Aggregate",
        node_tag="PHOTON_GROUPING_AGG_EXEC",
        grouping_expressions=group_exprs or [],
        extra_metrics={
            "Number of times hash table was resized": resize,
            "Avg hash probes per row": probes,
        },
    )


class TestDuplicateGroupBy:
    def test_flags_duplicate_group_by(self):
        # Same grouping keys on 3 separate aggregation nodes
        nodes = [_agg(str(i), resize=500, group_exprs=["sales.customer_sk"]) for i in range(3)]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        dups = [a for a in bi.alerts if a.metric_name == "duplicate_aggregation"]
        assert len(dups) == 1
        assert "3" in dups[0].current_value or "3 nodes" in dups[0].current_value
        assert "sales.customer_sk" in dups[0].message
        assert "1,500" in dups[0].message  # 500*3 total resizes

    def test_single_occurrence_not_flagged(self):
        nodes = [_agg("1", resize=500, group_exprs=["sales.k"])]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        assert not [a for a in bi.alerts if a.metric_name == "duplicate_aggregation"]

    def test_different_keys_not_flagged(self):
        nodes = [
            _agg("1", resize=500, group_exprs=["t.col_a"]),
            _agg("2", resize=500, group_exprs=["t.col_b"]),
            _agg("3", resize=500, group_exprs=["t.col_c"]),
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        assert not [a for a in bi.alerts if a.metric_name == "duplicate_aggregation"]

    def test_small_duplicate_below_threshold_not_flagged(self):
        # 2 nodes × 30 resizes = 60 total, below threshold of 100
        nodes = [_agg(str(i), resize=30, group_exprs=["t.k"]) for i in range(2)]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        assert not [a for a in bi.alerts if a.metric_name == "duplicate_aggregation"]

    def test_multiple_duplicate_keys_each_flagged(self):
        # Two separate duplicate groups
        nodes = [
            _agg("1", resize=500, group_exprs=["t.a"]),
            _agg("2", resize=500, group_exprs=["t.a"]),
            _agg("3", resize=500, group_exprs=["t.b"]),
            _agg("4", resize=500, group_exprs=["t.b"]),
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        dups = [a for a in bi.alerts if a.metric_name == "duplicate_aggregation"]
        assert len(dups) == 2


class TestInvestigationActionCard:
    def _make(self, nodes: list[NodeMetrics]) -> list:
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        sql_an = SQLAnalysis(structure=QueryStructure(join_count=1))
        # Must have join_info for the hash card to fire; investigation card
        # does not depend on it but we wire it for realism.
        join_info = [JoinInfo(join_type=JoinType.SHUFFLE_HASH)]
        return generate_action_cards(bi, [], _qm(), [], join_info, sql_an, [])

    def test_card_present_when_hotspots_and_resize(self):
        nodes = [
            _agg("1", resize=5000, probes=100, group_exprs=["catalog.sch.tbl.customer_sk"]),
            _agg("2", resize=3000, probes=80, group_exprs=["catalog.sch.tbl.customer_sk"]),
        ]
        cards = self._make(nodes)
        inv = [c for c in cards if "Investigate" in c.problem]
        assert inv
        sql = inv[0].fix_sql
        assert "COUNT(DISTINCT" in sql
        assert "customer_sk" in sql
        assert "catalog.sch.tbl" in sql
        assert "ORDER BY n DESC" in sql
        assert "LIMIT 20" in sql

    def test_card_absent_when_no_hotspots(self):
        # No extra_metrics → no hotspots
        nm = NodeMetrics(node_id="1", node_name="x", node_tag="Scan")
        cards = self._make([nm])
        assert not [c for c in cards if "Investigate" in c.problem]

    def test_card_uses_top_hotspot_columns(self):
        nodes = [
            _agg("1", resize=10000, probes=100, group_exprs=["db.s.big_table.user_id"]),
            _agg("2", resize=100, probes=50, group_exprs=["db.s.small_table.sku"]),
        ]
        cards = self._make(nodes)
        inv = [c for c in cards if "Investigate" in c.problem]
        assert inv
        # Higher-resize node's column appears first in fix_sql
        sql = inv[0].fix_sql
        assert sql.index("user_id") < sql.index("sku")

    def test_card_priority_above_hash_fix(self):
        nodes = [
            _agg("1", resize=5000, probes=100, group_exprs=["db.s.t.k"]),
            _agg("2", resize=5000, probes=100, group_exprs=["db.s.t.k"]),
        ]
        cards = self._make(nodes)
        problems = [c.problem for c in cards]
        assert any("Investigate" in p for p in problems)
        # Investigation should rank at or above the Hash fix card to promote
        # root-cause analysis before speculative fixes.
        inv_idx = next((i for i, c in enumerate(cards) if "Investigate" in c.problem), -1)
        hash_idx = next((i for i, c in enumerate(cards) if "Hash" in c.problem), -1)
        if inv_idx >= 0 and hash_idx >= 0:
            assert inv_idx <= hash_idx
