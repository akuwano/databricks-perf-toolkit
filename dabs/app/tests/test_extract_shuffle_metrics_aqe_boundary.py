"""Boundary tests for extract_shuffle_metrics — AQE/AOS events without partition_count.

Ensures a node with only AQE/AOS runtime-optimization signals (and no
"Sink - Number of partitions" label) is still kept in the shuffle list
so downstream reporters can surface the event.
"""

from core.extractors import extract_shuffle_metrics


def _node(node_id: str, tag: str, extra: dict) -> dict:
    """Minimal shuffle-exchange node payload consumed by extract_shuffle_metrics."""
    return {
        "id": node_id,
        "tag": tag,
        "name": f"{tag} node {node_id}",
        "metrics": [{"label": k, "value": v, "metricType": "UNKNOWN"} for k, v in extra.items()],
    }


def _graph(nodes: list[dict]) -> dict:
    """Wrap a list of nodes in the top-level profile structure the extractor expects."""
    return {"graphs": [{"nodes": nodes, "edges": []}]}


class TestAqeAosEventsKeptWithoutPartitionCount:
    """A node with AQE/AOS signals but no partition metric must still be kept."""

    def test_aqe_self_repartition_only(self):
        # No "Sink - Number of partitions", only AQE self-repartition
        node = _node(
            "42",
            "PHOTON_SHUFFLE_EXCHANGE_SINK_EXEC",
            {"Exchange - Adp self-triggered repartition count": 3},
        )
        sms = extract_shuffle_metrics(_graph([node]))
        assert len(sms) == 1
        assert sms[0].node_id == "42"
        assert sms[0].aqe_self_repartition_count == 3
        assert sms[0].partition_count == 0  # deliberately absent

    def test_aqe_skew_split_only(self):
        node = _node(
            "9",
            "PHOTON_SHUFFLE_EXCHANGE_SINK_EXEC",
            {"AQEShuffleRead - Number of skewed partitions": 5},
        )
        sms = extract_shuffle_metrics(_graph([node]))
        assert len(sms) == 1
        assert sms[0].aqe_skewed_partitions == 5

    def test_aqe_cancellation_only(self):
        node = _node(
            "11",
            "PHOTON_SHUFFLE_EXCHANGE_SINK_EXEC",
            {"Exchange - Adp total cancellation count": 2},
        )
        sms = extract_shuffle_metrics(_graph([node]))
        assert len(sms) == 1
        assert sms[0].aqe_cancellation_count == 2

    def test_aos_coordinated_only(self):
        node = _node(
            "77",
            "PHOTON_SHUFFLE_EXCHANGE_SINK_EXEC",
            {
                "Exchange - Aos coordinated repartition count": 1,
                "Exchange - Aos old number of partitions": 400,
                "Exchange - Aos new number of partitions": 50,
            },
        )
        sms = extract_shuffle_metrics(_graph([node]))
        assert len(sms) == 1
        assert sms[0].aos_coordinated_repartition_count == 1
        assert sms[0].aos_old_num_partitions == 400
        assert sms[0].aos_new_num_partitions == 50


class TestNodeStillDroppedWhenNoSignal:
    """Sanity: nodes with neither partition_count nor AQE/AOS signal must be dropped."""

    def test_shuffle_node_without_any_useful_metric_is_dropped(self):
        node = _node(
            "0",
            "PHOTON_SHUFFLE_EXCHANGE_SINK_EXEC",
            {"Some - Unrelated metric": 123},
        )
        sms = extract_shuffle_metrics(_graph([node]))
        assert sms == []

    def test_empty_graph_returns_empty(self):
        assert extract_shuffle_metrics(_graph([])) == []
        assert extract_shuffle_metrics({"graphs": []}) == []
