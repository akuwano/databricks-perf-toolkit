"""Tests for data flow DAG extraction and visualization."""

from core.extractors import (
    _find_interesting_edges,
    extract_data_flow,
    extract_data_flow_dag,
)
from core.models import DataFlowDAG
from core.reporters import (
    generate_ascii_tree,
    generate_data_flow_section,
    generate_mermaid_flowchart,
)
from core.utils import format_rows_human

# Reuse SAMPLE_PROFILE from existing tests
SAMPLE_PROFILE = {
    "query": {"id": "test-query"},
    "graphs": [
        {"nodes": [{"id": "0", "name": "Summary"}]},
        {
            "nodes": [
                {
                    "id": "100",
                    "name": "Scan table_a",
                    "tag": "SCAN_EXEC",
                    "keyMetrics": {
                        "rowsNum": 1950176,
                        "durationMs": 1000,
                        "peakMemoryBytes": 16000000,
                    },
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "200",
                    "name": "Scan table_b",
                    "tag": "SCAN_EXEC",
                    "keyMetrics": {
                        "rowsNum": 24110484,
                        "durationMs": 6000,
                        "peakMemoryBytes": 184000000,
                    },
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "250",
                    "name": "Filter",
                    "tag": "FILTER_EXEC",
                    "keyMetrics": {"rowsNum": 100000},
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "300",
                    "name": "Inner Join",
                    "tag": "PHOTON_BROADCAST_HASH_JOIN_EXEC",
                    "keyMetrics": {
                        "rowsNum": 13200000000,
                        "durationMs": 279000,
                        "peakMemoryBytes": 1970000000,
                    },
                    "metadata": [
                        {
                            "key": "LEFT_KEYS",
                            "label": "Left keys",
                            "values": ["a.user_id"],
                            "metaValues": [{"value": "a.user_id"}],
                        },
                        {
                            "key": "RIGHT_KEYS",
                            "label": "Right keys",
                            "values": ["b.user_id"],
                            "metaValues": [{"value": "b.user_id"}],
                        },
                    ],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "400",
                    "name": "LEFT OUTER JOIN",
                    "tag": "PHOTON_SHUFFLED_HASH_JOIN_EXEC",
                    "keyMetrics": {
                        "rowsNum": 5520000000000,
                        "durationMs": 37500,
                        "peakMemoryBytes": 73000000000,
                    },
                    "metadata": [
                        {
                            "key": "LEFT_KEYS",
                            "label": "Left keys",
                            "values": ["user_id", "quest_name"],
                            "metaValues": [
                                {"value": "user_id"},
                                {"value": "quest_name"},
                            ],
                        },
                    ],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "500",
                    "name": "Shuffle",
                    "tag": "SHUFFLE_EXEC",
                    "keyMetrics": {"rowsNum": 1720000000},
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "999",
                    "name": "Write",
                    "tag": "WRITE_EXEC",
                    "keyMetrics": {"rowsNum": 0},
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
            ],
            "edges": [
                {"fromId": "100", "toId": "250"},
                {"fromId": "250", "toId": "300"},
                {"fromId": "200", "toId": "300"},
                {"fromId": "300", "toId": "400"},
                {"fromId": "400", "toId": "500"},
                {"fromId": "500", "toId": "999"},
            ],
            "stageData": [],
        },
    ],
}


# --- _find_interesting_edges tests ---


class TestFindInterestingEdges:
    def test_direct_edges(self):
        interesting = {"100", "300"}
        children = {"100": ["300"]}
        edges = _find_interesting_edges(interesting, children)
        assert ("100", "300") in edges

    def test_edges_through_intermediate_nodes(self):
        interesting = {"100", "400"}
        children = {"100": ["200"], "200": ["300"], "300": ["400"]}
        edges = _find_interesting_edges(interesting, children)
        assert ("100", "400") in edges

    def test_multiple_parents_to_one_child(self):
        interesting = {"100", "200", "300"}
        children = {"100": ["300"], "200": ["300"]}
        edges = _find_interesting_edges(interesting, children)
        assert ("100", "300") in edges
        assert ("200", "300") in edges

    def test_chain_of_joins(self):
        interesting = {"100", "200", "300", "400"}
        children = {"100": ["300"], "200": ["300"], "300": ["400"]}
        edges = _find_interesting_edges(interesting, children)
        assert ("100", "300") in edges
        assert ("200", "300") in edges
        assert ("300", "400") in edges

    def test_empty_graph(self):
        edges = _find_interesting_edges(set(), {})
        assert edges == []

    def test_no_edges_between_interesting(self):
        interesting = {"100", "200"}
        children = {"100": ["999"], "200": ["998"]}
        edges = _find_interesting_edges(interesting, children)
        assert edges == []


# --- extract_data_flow_dag tests ---


class TestExtractDataFlowDAG:
    def test_returns_dag_with_entries_and_edges(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        assert dag is not None
        assert len(dag.entries) >= 4
        assert len(dag.edges) >= 3

    def test_edges_connect_interesting_nodes_only(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        assert dag is not None
        interesting_ids = {e.node_id for e in dag.entries}
        for edge in dag.edges:
            assert edge.from_node_id in interesting_ids
            assert edge.to_node_id in interesting_ids

    def test_edges_through_intermediate_filter(self):
        """Scan 100 -> Filter 250 -> Join 300: edge should be (100, 300)."""
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        assert dag is not None
        edge_pairs = [(e.from_node_id, e.to_node_id) for e in dag.edges]
        assert ("100", "300") in edge_pairs

    def test_source_and_sink_nodes(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        assert dag is not None
        assert "100" in dag.source_node_ids
        assert "200" in dag.source_node_ids
        assert "400" in dag.sink_node_ids

    def test_adjacency_maps(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        assert dag is not None
        assert "300" in dag.children_map.get("100", [])
        assert "300" in dag.children_map.get("200", [])
        assert "400" in dag.children_map.get("300", [])
        assert "100" in dag.parents_map.get("300", [])
        assert "200" in dag.parents_map.get("300", [])

    def test_empty_graphs(self):
        dag = extract_data_flow_dag({"query": {}, "graphs": []})
        assert dag is None

    def test_backward_compatible_with_flat_list(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        flat = extract_data_flow(SAMPLE_PROFILE)
        assert dag is not None
        assert len(dag.entries) == len(flat)
        assert [e.node_id for e in dag.entries] == [e.node_id for e in flat]


# --- format_rows_human tests ---


class TestFormatRowsHuman:
    def test_small_numbers(self):
        assert format_rows_human(0) == "0"
        assert format_rows_human(999) == "999"

    def test_thousands(self):
        assert format_rows_human(1_500) == "1.5K"
        assert format_rows_human(999_999) == "1000.0K"

    def test_millions(self):
        assert format_rows_human(1_950_176) == "1.95M"
        assert format_rows_human(24_110_484) == "24.11M"

    def test_billions(self):
        assert format_rows_human(13_200_000_000) == "13.20B"

    def test_trillions(self):
        assert format_rows_human(5_520_000_000_000) == "5.52T"


# --- generate_mermaid_flowchart tests ---


class TestGenerateMermaidFlowchart:
    def test_contains_graph_td(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        mermaid = generate_mermaid_flowchart(dag)
        assert mermaid.startswith("graph TD")

    def test_contains_node_definitions(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        mermaid = generate_mermaid_flowchart(dag)
        assert "n100[" in mermaid
        assert "n200[" in mermaid
        assert "n300[" in mermaid
        assert "n400[" in mermaid

    def test_contains_edge_definitions(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        mermaid = generate_mermaid_flowchart(dag)
        assert "n100 --> n300" in mermaid
        assert "n200 --> n300" in mermaid
        assert "n300 --> n400" in mermaid

    def test_node_labels_contain_rows(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        mermaid = generate_mermaid_flowchart(dag)
        assert "rows" in mermaid

    def test_node_labels_contain_join_keys(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        mermaid = generate_mermaid_flowchart(dag)
        assert "user_id" in mermaid

    def test_contains_style_classes(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        mermaid = generate_mermaid_flowchart(dag)
        assert "classDef critical" in mermaid
        assert "classDef warning" in mermaid

    def test_problem_nodes_highlighted(self):
        """Nodes with data explosion or high duration should be colored."""
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        mermaid = generate_mermaid_flowchart(dag)
        # Inner Join has massive data explosion (1.95M + 24.1M → 13.2B)
        # and longest duration → should be critical
        assert "class" in mermaid
        # At least one node should be classified
        has_classification = "class n" in mermaid
        assert has_classification

    def test_empty_dag(self):
        dag = DataFlowDAG()
        assert generate_mermaid_flowchart(dag) == ""


# --- generate_ascii_tree tests ---


class TestGenerateAsciiTree:
    def test_root_is_sink_node(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        tree = generate_ascii_tree(dag)
        lines = tree.split("\n")
        assert "LEFT OUTER JOIN" in lines[0]

    def test_contains_all_operations(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        tree = generate_ascii_tree(dag)
        assert "Scan table_a" in tree
        assert "Scan table_b" in tree
        assert "Inner Join" in tree
        assert "LEFT OUTER JOIN" in tree

    def test_contains_row_counts(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        tree = generate_ascii_tree(dag)
        assert "rows" in tree

    def test_tree_structure_characters(self):
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        tree = generate_ascii_tree(dag)
        assert any(c in tree for c in ["\u251c", "\u2514", "\u2502"])

    def test_empty_dag(self):
        dag = DataFlowDAG()
        assert generate_ascii_tree(dag) == ""


# --- generate_data_flow_section tests ---


class TestGenerateDataFlowSectionWithDAG:
    def test_contains_mermaid_block(self):
        flow = extract_data_flow(SAMPLE_PROFILE)
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        section = generate_data_flow_section(flow, dag)
        assert "```mermaid" in section
        assert "graph TD" in section

    def test_contains_ascii_tree(self):
        flow = extract_data_flow(SAMPLE_PROFILE)
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        section = generate_data_flow_section(flow, dag)
        assert "LEFT OUTER JOIN" in section
        assert any(c in section for c in ["\u251c", "\u2514"])

    def test_still_contains_table(self):
        flow = extract_data_flow(SAMPLE_PROFILE)
        dag = extract_data_flow_dag(SAMPLE_PROFILE)
        section = generate_data_flow_section(flow, dag)
        assert "| " in section

    def test_backward_compatible_without_dag(self):
        flow = extract_data_flow(SAMPLE_PROFILE)
        section = generate_data_flow_section(flow)
        assert "```mermaid" not in section
        assert "| " in section
