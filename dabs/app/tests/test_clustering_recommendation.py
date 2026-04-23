"""Tests for clustering recommendation functionality."""

from core.analyzers import (
    _update_top_scanned_with_clustering,
    generate_action_cards,
)
from core.models import (
    BottleneckIndicators,
    ColumnReference,
    QueryMetrics,
    QueryStructure,
    SQLAnalysis,
    TableReference,
    TableScanMetrics,
)


class TestUpdateTopScannedWithClustering:
    """Tests for _update_top_scanned_with_clustering helper function."""

    def test_short_name_match(self):
        """Test matching by short table name."""
        tables = [
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.qtz_member", bytes_read=1000000),
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.id_pos", bytes_read=500000),
        ]
        _update_top_scanned_with_clustering(tables, "qtz_member", ["member_id", "created_at"])
        assert tables[0].recommended_clustering_keys == ["member_id", "created_at"]
        assert tables[1].recommended_clustering_keys == []

    def test_full_qualified_name_match(self):
        """Test matching by fully qualified table name."""
        tables = [
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.qtz_member", bytes_read=1000000),
        ]
        _update_top_scanned_with_clustering(
            tables, "prd_delta.qtz_s3_etl.qtz_member", ["member_id"]
        )
        assert tables[0].recommended_clustering_keys == ["member_id"]

    def test_fallback_to_first_table(self):
        """Test fallback to first table when no match found."""
        tables = [
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.qtz_member", bytes_read=1000000),
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.id_pos", bytes_read=500000),
        ]
        _update_top_scanned_with_clustering(tables, "unknown_table", ["col1", "col2"])
        assert tables[0].recommended_clustering_keys == ["col1", "col2"]
        assert tables[1].recommended_clustering_keys == []

    def test_no_update_if_already_populated(self):
        """Test that existing clustering keys are not overwritten."""
        tables = [
            TableScanMetrics(
                table_name="prd_delta.qtz_s3_etl.qtz_member",
                bytes_read=1000000,
                recommended_clustering_keys=["existing_key"],
            ),
        ]
        _update_top_scanned_with_clustering(tables, "qtz_member", ["new_key"])
        assert tables[0].recommended_clustering_keys == ["existing_key"]

    def test_empty_tables_list(self):
        """Test with empty tables list."""
        tables = []
        _update_top_scanned_with_clustering(tables, "qtz_member", ["member_id"])
        assert tables == []

    def test_empty_clustering_keys(self):
        """Test with empty clustering keys."""
        tables = [
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.qtz_member", bytes_read=1000000),
        ]
        _update_top_scanned_with_clustering(tables, "qtz_member", [])
        assert tables[0].recommended_clustering_keys == []

    def test_backtick_removal(self):
        """Test that backticks are properly removed during matching."""
        tables = [
            TableScanMetrics(
                table_name="`prd_delta`.`qtz_s3_etl`.`qtz_member`", bytes_read=1000000
            ),
        ]
        _update_top_scanned_with_clustering(tables, "qtz_member", ["member_id"])
        assert tables[0].recommended_clustering_keys == ["member_id"]


class TestGenerateActionCardsWithAliasMatching:
    """Tests for generate_action_cards with alias matching."""

    def test_alias_resolution_for_clustering(self):
        """Test that table aliases are resolved for clustering key matching."""
        sql_analysis = SQLAnalysis(
            raw_sql="SELECT * FROM prd_delta.qtz_s3_etl.qtz_member m WHERE m.member_id = 123",
            tables=[
                TableReference(
                    table="qtz_member",
                    full_name="prd_delta.qtz_s3_etl.qtz_member",
                    alias="m",
                ),
            ],
            columns=[
                ColumnReference(
                    column_name="member_id",
                    table_name="",
                    table_alias="m",
                    context="where",
                    operator="=",
                ),
            ],
            structure=QueryStructure(join_count=0, subquery_count=0, cte_count=0),
        )

        top_scanned_tables = [
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.qtz_member", bytes_read=5000000000),
        ]

        indicators = BottleneckIndicators(filter_rate=0.01)
        query_metrics = QueryMetrics(read_files_count=1000, pruned_files_count=50)

        generate_action_cards(
            indicators=indicators,
            hot_operators=[],
            query_metrics=query_metrics,
            shuffle_metrics=[],
            join_info=[],
            sql_analysis=sql_analysis,
            top_scanned_tables=top_scanned_tables,
            llm_clustering_config=None,
        )

        assert top_scanned_tables[0].recommended_clustering_keys == ["member_id"]

    def test_multiple_columns_with_aliases(self):
        """Test multiple columns from aliased tables."""
        sql_analysis = SQLAnalysis(
            raw_sql='SELECT * FROM prd_delta.qtz_s3_etl.qtz_member m WHERE m.member_id = 123 AND m.created_at > "2024-01-01"',
            tables=[
                TableReference(
                    table="qtz_member",
                    full_name="prd_delta.qtz_s3_etl.qtz_member",
                    alias="m",
                ),
            ],
            columns=[
                ColumnReference(
                    column_name="member_id",
                    table_name="",
                    table_alias="m",
                    context="where",
                    operator="=",
                ),
                ColumnReference(
                    column_name="created_at",
                    table_name="",
                    table_alias="m",
                    context="where",
                    operator=">",
                ),
            ],
            structure=QueryStructure(join_count=0, subquery_count=0, cte_count=0),
        )

        top_scanned_tables = [
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.qtz_member", bytes_read=5000000000),
        ]

        indicators = BottleneckIndicators(filter_rate=0.01)
        query_metrics = QueryMetrics(read_files_count=1000, pruned_files_count=50)

        generate_action_cards(
            indicators=indicators,
            hot_operators=[],
            query_metrics=query_metrics,
            shuffle_metrics=[],
            join_info=[],
            sql_analysis=sql_analysis,
            top_scanned_tables=top_scanned_tables,
            llm_clustering_config=None,
        )

        assert "member_id" in top_scanned_tables[0].recommended_clustering_keys
        assert "created_at" in top_scanned_tables[0].recommended_clustering_keys

    def test_join_columns_with_different_tables(self):
        """Test JOIN columns are attributed to correct tables."""
        sql_analysis = SQLAnalysis(
            raw_sql="SELECT * FROM qtz_member m JOIN id_pos p ON m.member_id = p.member_id",
            tables=[
                TableReference(
                    table="qtz_member",
                    full_name="prd_delta.qtz_s3_etl.qtz_member",
                    alias="m",
                ),
                TableReference(
                    table="id_pos",
                    full_name="prd_delta.qtz_s3_etl.id_pos",
                    alias="p",
                ),
            ],
            columns=[
                ColumnReference(
                    column_name="member_id",
                    table_name="",
                    table_alias="m",
                    context="join",
                    operator="=",
                ),
                ColumnReference(
                    column_name="member_id",
                    table_name="",
                    table_alias="p",
                    context="join",
                    operator="=",
                ),
            ],
            structure=QueryStructure(join_count=1, subquery_count=0, cte_count=0),
        )

        top_scanned_tables = [
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.qtz_member", bytes_read=5000000000),
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.id_pos", bytes_read=2000000000),
        ]

        indicators = BottleneckIndicators(filter_rate=0.01)
        query_metrics = QueryMetrics(read_files_count=1000, pruned_files_count=50)

        generate_action_cards(
            indicators=indicators,
            hot_operators=[],
            query_metrics=query_metrics,
            shuffle_metrics=[],
            join_info=[],
            sql_analysis=sql_analysis,
            top_scanned_tables=top_scanned_tables,
            llm_clustering_config=None,
        )

        # First table (highest bytes_read) should get the recommendation
        assert top_scanned_tables[0].recommended_clustering_keys == ["member_id"]

    def test_single_table_query_without_table_reference(self):
        """Test single table query where columns have no table reference."""
        sql_analysis = SQLAnalysis(
            raw_sql="SELECT * FROM qtz_member WHERE member_id = 123",
            tables=[
                TableReference(
                    table="qtz_member",
                    full_name="prd_delta.qtz_s3_etl.qtz_member",
                    alias="",
                ),
            ],
            columns=[
                ColumnReference(
                    column_name="member_id",
                    table_name="",
                    table_alias="",
                    context="where",
                    operator="=",
                ),
            ],
            structure=QueryStructure(join_count=0, subquery_count=0, cte_count=0),
        )

        top_scanned_tables = [
            TableScanMetrics(table_name="prd_delta.qtz_s3_etl.qtz_member", bytes_read=5000000000),
        ]

        indicators = BottleneckIndicators(filter_rate=0.01)
        query_metrics = QueryMetrics(read_files_count=1000, pruned_files_count=50)

        generate_action_cards(
            indicators=indicators,
            hot_operators=[],
            query_metrics=query_metrics,
            shuffle_metrics=[],
            join_info=[],
            sql_analysis=sql_analysis,
            top_scanned_tables=top_scanned_tables,
            llm_clustering_config=None,
        )

        # Single table query should attribute columns to that table
        assert top_scanned_tables[0].recommended_clustering_keys == ["member_id"]
