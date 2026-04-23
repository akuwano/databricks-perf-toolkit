"""Tests for hash table resize hotspot attribution.

Verifies that the hash resize alert surfaces:
- Per-node breakdown of top contributors (not just aggregate count)
- Correct wording based on the operator type (join vs grouping)
- Specific column names from the hot nodes
"""

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.models import NodeMetrics, QueryMetrics


def _qm() -> QueryMetrics:
    return QueryMetrics(
        query_id="t",
        status="FINISHED",
        total_time_ms=1000,
        execution_time_ms=1000,
        read_bytes=1000,
    )


def _node(
    node_id: str,
    *,
    tag: str,
    resize: int = 0,
    probes: float = 0.0,
    join_left: list[str] | None = None,
    join_right: list[str] | None = None,
    group_exprs: list[str] | None = None,
) -> NodeMetrics:
    return NodeMetrics(
        node_id=node_id,
        node_name=tag.replace("PHOTON_", "").replace("_EXEC", "").replace("_", " ").title(),
        node_tag=tag,
        join_keys_left=join_left or [],
        join_keys_right=join_right or [],
        grouping_expressions=group_exprs or [],
        extra_metrics={
            "Number of times hash table was resized": resize,
            "Avg hash probes per row": probes,
        },
    )


class TestHotspotExtraction:
    def test_hotspots_ranked_by_resize_count(self):
        nodes = [
            _node(
                "1",
                tag="PHOTON_SHUFFLED_HASH_JOIN_EXEC",
                resize=100,
                probes=30,
                join_left=["a.k"],
                join_right=["b.k"],
            ),
            _node(
                "2", tag="PHOTON_GROUPING_AGG_EXEC", resize=500, probes=80, group_exprs=["t.col"]
            ),
            _node(
                "3",
                tag="PHOTON_SHUFFLED_HASH_JOIN_EXEC",
                resize=50,
                probes=20,
                join_left=["x.id"],
                join_right=["y.id"],
            ),
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        hs = bi.hash_resize_hotspots
        assert len(hs) == 3
        # Ranked by resize desc
        assert hs[0].node_id == "2"
        assert hs[0].resize == 500
        assert hs[0].key_kind == "group"
        assert hs[0].keys == ["t.col"]
        assert hs[1].node_id == "1"
        assert hs[1].key_kind == "join"
        assert hs[1].keys == ["a.k ↔ b.k"]

    def test_hotspot_ignores_zero_resize_nodes(self):
        nodes = [
            _node("1", tag="PHOTON_GROUPING_AGG_EXEC", resize=0, group_exprs=["x"]),
            _node("2", tag="PHOTON_GROUPING_AGG_EXEC", resize=200, group_exprs=["y"]),
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        assert len(bi.hash_resize_hotspots) == 1
        assert bi.hash_resize_hotspots[0].node_id == "2"


class TestAlertWording:
    def test_alert_mentions_grouping_when_top_is_agg(self):
        nodes = [
            _node(
                "1",
                tag="PHOTON_GROUPING_AGG_EXEC",
                resize=19137,
                probes=1587,
                group_exprs=["sales.customer_sk"],
            ),
            _node(
                "2",
                tag="PHOTON_GROUPING_AGG_EXEC",
                resize=15399,
                probes=1605,
                group_exprs=["sales.customer_sk"],
            ),
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        alerts = [a for a in bi.alerts if a.metric_name == "hash_table_resize_count"]
        assert alerts, "expected hash_table_resize_count alert"
        a = alerts[0]
        assert "grouping keys" in a.message
        assert "join keys" not in a.message
        assert "sales.customer_sk" in a.message
        # Two nodes on the same grouping key collapse into a single bullet
        # with aggregated resize count (19,137 + 15,399 = 34,536).
        assert "34,536 resizes" in a.message
        assert "× 2 nodes" in a.message
        # Recommendation should point to data/cardinality verification
        assert "verify" in a.recommendation.lower() or "cardinality" in a.recommendation.lower()

    def test_alert_mentions_join_when_top_is_join(self):
        nodes = [
            _node(
                "1",
                tag="PHOTON_SHUFFLED_HASH_JOIN_EXEC",
                resize=5000,
                probes=100,
                join_left=["sales.cust_sk"],
                join_right=["cust.cust_sk"],
            ),
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        alerts = [a for a in bi.alerts if a.metric_name == "hash_table_resize_count"]
        assert alerts
        a = alerts[0]
        assert "join keys" in a.message
        assert "grouping keys" not in a.message
        assert "sales.cust_sk ↔ cust.cust_sk" in a.message

    def test_alert_says_mixed_when_top_has_both(self):
        nodes = [
            _node(
                "1", tag="PHOTON_GROUPING_AGG_EXEC", resize=5000, probes=100, group_exprs=["g.col"]
            ),
            _node(
                "2",
                tag="PHOTON_SHUFFLED_HASH_JOIN_EXEC",
                resize=3000,
                probes=80,
                join_left=["a.k"],
                join_right=["b.k"],
            ),
            _node(
                "3", tag="PHOTON_GROUPING_AGG_EXEC", resize=2000, probes=60, group_exprs=["h.col"]
            ),
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        alerts = [a for a in bi.alerts if a.metric_name == "hash_table_resize_count"]
        assert alerts
        assert "join/grouping keys" in alerts[0].message


class TestHotspotInMessage:
    def test_message_shows_top_3_contributors(self):
        nodes = [
            _node(
                str(i),
                tag="PHOTON_GROUPING_AGG_EXEC",
                resize=1000 - i * 50,
                probes=100,
                group_exprs=[f"col_{i}"],
            )
            for i in range(10)
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        alerts = [a for a in bi.alerts if a.metric_name == "hash_table_resize_count"]
        assert alerts
        msg = alerts[0].message
        # Top 3 columns should appear, but not the 4th
        assert "col_0" in msg
        assert "col_1" in msg
        assert "col_2" in msg
        assert "col_3" not in msg

    def test_hotspots_stored_up_to_10(self):
        nodes = [
            _node(
                str(i),
                tag="PHOTON_GROUPING_AGG_EXEC",
                resize=1000 - i * 50,
                probes=100,
                group_exprs=[f"col_{i}"],
            )
            for i in range(15)
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        assert len(bi.hash_resize_hotspots) == 10
