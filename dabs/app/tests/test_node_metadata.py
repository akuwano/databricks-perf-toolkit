"""Tests for expanded node metadata extraction (FILTERS, JOIN keys, IS_DELTA, JOIN_TYPE, JOIN_ALGORITHM, PARTITION_FILTERS)."""

from core.extractors import extract_node_metrics


def _make_profile_with_node(metadata=None, node_name="Scan", node_tag="SCAN"):
    """Create a minimal profile JSON with one node containing given metadata."""
    node = {
        "id": "1",
        "name": node_name,
        "tag": node_tag,
        "keyMetrics": {"durationMs": 100, "peakMemoryBytes": 0, "rowsNum": 1000},
        "metrics": [],
        "metadata": metadata or [],
    }
    return {"graphs": [{"nodes": [node]}]}


class TestFilterConditionExtraction:
    """FILTERS metadata should be extracted into NodeMetrics."""

    def test_single_filter(self):
        data = _make_profile_with_node(metadata=[{"key": "FILTERS", "values": ["(col1 > 10)"]}])
        nodes = extract_node_metrics(data)
        assert len(nodes) == 1
        assert nodes[0].filter_conditions == ["(col1 > 10)"]

    def test_multiple_filters(self):
        data = _make_profile_with_node(
            metadata=[{"key": "FILTERS", "values": ["(dt >= '20240101')", "(status = 'ACTIVE')"]}]
        )
        nodes = extract_node_metrics(data)
        assert nodes[0].filter_conditions == ["(dt >= '20240101')", "(status = 'ACTIVE')"]

    def test_no_filters(self):
        data = _make_profile_with_node(metadata=[])
        nodes = extract_node_metrics(data)
        assert nodes[0].filter_conditions == []

    def test_condition_metadata(self):
        """CONDITION key (used by Filter/Join nodes) should also be extracted."""
        data = _make_profile_with_node(
            metadata=[{"key": "CONDITION", "value": "(a.id = b.id)"}],
            node_name="Filter",
        )
        nodes = extract_node_metrics(data)
        assert nodes[0].filter_conditions == ["(a.id = b.id)"]


class TestJoinKeysExtraction:
    """LEFT_KEYS and RIGHT_KEYS should be extracted from join nodes."""

    def test_join_keys(self):
        data = _make_profile_with_node(
            metadata=[
                {"key": "LEFT_KEYS", "values": ["customer_id"]},
                {"key": "RIGHT_KEYS", "values": ["id"]},
            ],
            node_name="BroadcastHashJoin",
            node_tag="JOIN",
        )
        nodes = extract_node_metrics(data)
        assert nodes[0].join_keys_left == ["customer_id"]
        assert nodes[0].join_keys_right == ["id"]

    def test_composite_join_keys(self):
        data = _make_profile_with_node(
            metadata=[
                {"key": "LEFT_KEYS", "values": ["region", "date"]},
                {"key": "RIGHT_KEYS", "values": ["region", "dt"]},
            ],
            node_name="SortMergeJoin",
            node_tag="JOIN",
        )
        nodes = extract_node_metrics(data)
        assert nodes[0].join_keys_left == ["region", "date"]
        assert nodes[0].join_keys_right == ["region", "dt"]

    def test_no_join_keys(self):
        data = _make_profile_with_node(metadata=[])
        nodes = extract_node_metrics(data)
        assert nodes[0].join_keys_left == []
        assert nodes[0].join_keys_right == []


class TestIsDeltaExtraction:
    """IS_DELTA metadata should be extracted."""

    def test_is_delta_true(self):
        data = _make_profile_with_node(metadata=[{"key": "IS_DELTA", "value": "true"}])
        nodes = extract_node_metrics(data)
        assert nodes[0].is_delta is True

    def test_is_delta_false(self):
        data = _make_profile_with_node(metadata=[{"key": "IS_DELTA", "value": "false"}])
        nodes = extract_node_metrics(data)
        assert nodes[0].is_delta is False

    def test_missing_is_delta(self):
        data = _make_profile_with_node(metadata=[])
        nodes = extract_node_metrics(data)
        assert nodes[0].is_delta is False

    def test_combined_metadata(self):
        """All metadata types extracted together from a single node."""
        data = _make_profile_with_node(
            metadata=[
                {"key": "IS_DELTA", "value": "true"},
                {"key": "IS_PHOTON", "value": "true"},
                {"key": "FILTERS", "values": ["(dt >= '2024')"]},
                {"key": "SCAN_CLUSTERS", "values": ["dt"]},
            ]
        )
        nodes = extract_node_metrics(data)
        n = nodes[0]
        assert n.is_delta is True
        assert n.is_photon is True
        assert n.filter_conditions == ["(dt >= '2024')"]
        assert n.clustering_keys == ["dt"]


class TestJoinTypeAlgorithmExtraction:
    """JOIN_TYPE and JOIN_ALGORITHM metadata extraction."""

    def test_join_type_extracted(self):
        data = _make_profile_with_node(
            metadata=[{"key": "JOIN_TYPE", "value": "Inner"}],
            node_name="SortMergeJoin",
            node_tag="JOIN",
        )
        nodes = extract_node_metrics(data)
        assert nodes[0].join_type == "Inner"

    def test_join_algorithm_extracted(self):
        data = _make_profile_with_node(
            metadata=[{"key": "JOIN_ALGORITHM", "value": "SortMerge"}],
            node_name="SortMergeJoin",
            node_tag="JOIN",
        )
        nodes = extract_node_metrics(data)
        assert nodes[0].join_algorithm == "SortMerge"

    def test_both_type_and_algorithm(self):
        data = _make_profile_with_node(
            metadata=[
                {"key": "JOIN_TYPE", "value": "Inner"},
                {"key": "JOIN_ALGORITHM", "value": "BroadcastHash"},
                {"key": "LEFT_KEYS", "values": ["id"]},
                {"key": "RIGHT_KEYS", "values": ["user_id"]},
            ],
            node_name="BroadcastHashJoin",
            node_tag="JOIN",
        )
        nodes = extract_node_metrics(data)
        n = nodes[0]
        assert n.join_type == "Inner"
        assert n.join_algorithm == "BroadcastHash"
        assert n.join_keys_left == ["id"]
        assert n.join_keys_right == ["user_id"]

    def test_missing_join_metadata(self):
        data = _make_profile_with_node(metadata=[])
        nodes = extract_node_metrics(data)
        assert nodes[0].join_type == ""
        assert nodes[0].join_algorithm == ""


class TestPartitionFiltersExtraction:
    """PARTITION_FILTERS metadata extraction."""

    def test_partition_filters_list(self):
        data = _make_profile_with_node(
            metadata=[
                {"key": "PARTITION_FILTERS", "values": ["(dt >= '2024-01-01')", "(region = 'us')"]}
            ]
        )
        nodes = extract_node_metrics(data)
        assert nodes[0].partition_filters == ["(dt >= '2024-01-01')", "(region = 'us')"]

    def test_partition_filters_single_value(self):
        data = _make_profile_with_node(
            metadata=[{"key": "PARTITION_FILTERS", "value": "(dt = '2024-01-01')"}]
        )
        nodes = extract_node_metrics(data)
        assert nodes[0].partition_filters == ["(dt = '2024-01-01')"]

    def test_no_partition_filters(self):
        data = _make_profile_with_node(metadata=[])
        nodes = extract_node_metrics(data)
        assert nodes[0].partition_filters == []

    def test_partition_filters_with_delta_scan(self):
        """Partition filters alongside IS_DELTA on a scan node."""
        data = _make_profile_with_node(
            metadata=[
                {"key": "IS_DELTA", "value": "true"},
                {"key": "PARTITION_FILTERS", "values": ["(year = 2024)"]},
                {"key": "FILTERS", "values": ["(status = 'active')"]},
            ]
        )
        nodes = extract_node_metrics(data)
        n = nodes[0]
        assert n.is_delta is True
        assert n.partition_filters == ["(year = 2024)"]
        assert n.filter_conditions == ["(status = 'active')"]
