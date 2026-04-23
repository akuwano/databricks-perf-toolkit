"""Tests for extract_scan_table_name — replaces brittle split()-based extraction."""

from core.explain_parser import ExplainNode, PlanKind, extract_scan_table_name


def _node(node_name: str) -> ExplainNode:
    return ExplainNode(raw_line=node_name, indent=0, kind=PlanKind.PHYSICAL, node_name=node_name)


class TestExtractScanTableName:
    def test_plain_scan(self):
        assert (
            extract_scan_table_name(_node("Scan parquet main.sales.orders")) == "main.sales.orders"
        )

    def test_scan_with_columns_bracket_is_stripped(self):
        # Trailing [cols] adornment must not leak into the identifier
        assert (
            extract_scan_table_name(_node("Scan parquet catalog.s.t[col1,col2]")) == "catalog.s.t"
        )

    def test_scan_with_parens_is_stripped(self):
        # Trailing (...) partition spec must not leak
        assert (
            extract_scan_table_name(_node("BatchScan main.db.tbl(partition=2024)")) == "main.db.tbl"
        )

    def test_photon_scan(self):
        assert extract_scan_table_name(_node("PhotonScan main.db.tbl")) == "main.db.tbl"

    def test_photon_scan_with_bracket_is_stripped(self):
        assert extract_scan_table_name(_node("PhotonScan catalog.s.t[id,name]")) == "catalog.s.t"

    def test_batch_scan(self):
        assert (
            extract_scan_table_name(_node("BatchScan catalog.schema.table"))
            == "catalog.schema.table"
        )

    def test_scan_with_backticks(self):
        assert (
            extract_scan_table_name(_node("Scan parquet `main`.`sales`.`orders`"))
            == "main.sales.orders"
        )

    def test_star_prefix(self):
        assert extract_scan_table_name(_node("* Scan parquet catalog.s.t")) == "catalog.s.t"

    def test_unrecognized_returns_empty(self):
        # Filter, Project, etc. are not scan nodes
        assert extract_scan_table_name(_node("Filter (id > 10)")) == ""
        assert extract_scan_table_name(_node("")) == ""
