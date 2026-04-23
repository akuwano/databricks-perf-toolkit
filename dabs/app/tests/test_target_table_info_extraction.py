"""TargetTableInfo extraction from profile JSON.

For INSERT / CTAS / MERGE queries the profile stores a DESCRIBE-like
block inside ``graphs[*].photonExplain[*].params[*].paramValue`` that
starts with ``DeltaTableV2(...)`` or ``CatalogTable(...)``. This block
carries the target table's provider, clustering columns, hierarchical
clustering columns, and other delta.* table properties.

These tests lock in the extraction behaviour so downstream analyzers
and LLM prompts can reason about the target table's real DDL instead
of guessing from the write node's ``IS_DELTA`` flag (which reflects
the Photon parquet writer, not the logical format).
"""

from core.extractors import extract_target_table_info
from core.models import TargetTableInfo


def _profile_with_describe_block(paramValue: str) -> dict:
    return {
        "graphs": [
            {
                "photonExplain": [
                    {
                        "sparkPlanId": 0,
                        "exprId": "e1",
                        "reason": "AppendDataExecV1 is a Photon fallback.",
                        "params": [{"paramKey": None, "paramValue": paramValue}],
                    }
                ]
            }
        ]
    }


class TestBasicExtraction:
    def test_returns_none_when_no_photon_explain(self):
        info = extract_target_table_info({"graphs": [{}]})
        assert info is None

    def test_parses_catalog_database_table(self):
        pv = (
            "AppendDataExecV1, DeltaTableV2(...,Some(CatalogTable(\n"
            "Catalog: ck_db_ws\n"
            "Database: default\n"
            "Table: mycloudcur_incremental_2xl_sf\n"
            "Owner: someone@example.com\n"
            "Provider: delta\n"
            "Table Properties: []\n"
            ")))"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info is not None
        assert info.catalog == "ck_db_ws"
        assert info.database == "default"
        assert info.table == "mycloudcur_incremental_2xl_sf"
        assert info.full_name == "ck_db_ws.default.mycloudcur_incremental_2xl_sf"
        assert info.provider == "delta"

    def test_parses_parquet_provider(self):
        pv = (
            "CatalogTable(\n"
            "Catalog: c\n"
            "Database: d\n"
            "Table: t\n"
            "Provider: parquet\n"
            "Table Properties: []\n"
            ")"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info is not None
        assert info.provider == "parquet"


class TestClusteringColumnsParse:
    def test_single_column_list(self):
        pv = (
            "CatalogTable(\n"
            "Catalog: c\nDatabase: d\nTable: t\n"
            "Provider: delta\n"
            'Table Properties: [clusteringColumns=[["COL1"]], delta.lastCommitTimestamp=1]\n'
            ")"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info.clustering_columns == [["COL1"]]

    def test_multiple_columns(self):
        pv = (
            "CatalogTable(\n"
            "Catalog: c\nDatabase: d\nTable: t\n"
            "Provider: delta\n"
            'Table Properties: [clusteringColumns=[["A"],["B"],["C"]], '
            "delta.checkpointPolicy=v2]\n"
            ")"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info.clustering_columns == [["A"], ["B"], ["C"]]

    def test_hierarchical_clustering_columns(self):
        pv = (
            "CatalogTable(\n"
            "Catalog: c\nDatabase: d\nTable: t\n"
            "Provider: delta\n"
            'Table Properties: [clusteringColumns=[["A"],["B"]], '
            "delta.liquid.hierarchicalClusteringColumns=a_col, b_col, "
            "delta.minReaderVersion=3]\n"
            ")"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info.hierarchical_clustering_columns == ["a_col", "b_col"]

    def test_absence_of_hierarchical_means_empty(self):
        pv = (
            "CatalogTable(\n"
            "Catalog: c\nDatabase: d\nTable: t\n"
            "Provider: delta\n"
            'Table Properties: [clusteringColumns=[["A"]]]\n'
            ")"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info.hierarchical_clustering_columns == []

    def test_no_clustering_means_empty_list(self):
        pv = (
            "CatalogTable(\n"
            "Catalog: c\nDatabase: d\nTable: t\n"
            "Provider: delta\n"
            "Table Properties: [delta.minReaderVersion=3]\n"
            ")"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info.clustering_columns == []


class TestDeltaDetection:
    """TargetTableInfo.is_delta must reflect the Provider field, NOT the
    downstream Write node's IS_DELTA flag (which describes the file
    writer, not the logical format)."""

    def test_provider_delta_is_delta_true(self):
        pv = (
            "CatalogTable(\nCatalog: c\nDatabase: d\nTable: t\n"
            "Provider: delta\nTable Properties: []\n)"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info.is_delta is True

    def test_provider_parquet_is_delta_false(self):
        pv = (
            "CatalogTable(\nCatalog: c\nDatabase: d\nTable: t\n"
            "Provider: parquet\nTable Properties: []\n)"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info.is_delta is False

    def test_deltatablev2_marker_implies_delta(self):
        """Even when Provider field is missing, the DeltaTableV2(...) wrapper
        is itself a strong signal."""
        pv = (
            "AppendDataExecV1, DeltaTableV2(...,Some(CatalogTable(\n"
            "Catalog: c\nDatabase: d\nTable: t\n"
            "Table Properties: []\n"
            ")))"
        )
        info = extract_target_table_info(_profile_with_describe_block(pv))
        assert info.is_delta is True

    def test_explicit_parquet_provider_wins_over_stray_delta_properties(self):
        """Explicit non-Delta provider must win over a stray ``delta.*``
        TBLPROPERTY (which can persist after a migration or be injected by
        a catalog default). Guards against false-positive Delta detection."""
        from core.models import TargetTableInfo

        info = TargetTableInfo(
            provider="parquet",
            properties={"delta.autoOptimize.optimizeWrite": "true"},
        )
        assert info.is_delta is False

    def test_empty_provider_with_delta_property_is_delta(self):
        """With no explicit provider and no wrapper, a ``delta.*`` property
        is a valid last-resort Delta signal."""
        from core.models import TargetTableInfo

        info = TargetTableInfo(
            provider="",
            properties={"delta.checkpointPolicy": "v2"},
        )
        assert info.is_delta is True


class TestRealProfile:
    """Regression test against the real captured profile fixture."""

    def test_real_profile_extracts_three_clustering_columns(self):
        import json
        from pathlib import Path

        repo_root = Path(__file__).resolve().parents[3]
        path = repo_root / "json" / "Dey" / "Master_table_insertion_after_optimization_2XL.json"
        if not path.exists():
            import pytest

            pytest.skip(f"Fixture profile not present at {path}")
        with open(path) as f:
            p = json.load(f)
        info = extract_target_table_info(p)
        assert info is not None
        assert info.is_delta is True
        assert info.full_name == "ck_db_ws.default.mycloudcur_incremental_2xl_sf"
        assert info.clustering_columns == [
            ["MYCLOUD_STARTMONTH"],
            ["MYCLOUD_STARTYEAR"],
            ["LINEITEM_USAGEACCOUNTID"],
        ]
        assert info.hierarchical_clustering_columns == [
            "mycloud_startmonth",
            "mycloud_startyear",
        ]


class TestModelDefaults:
    def test_empty_info_has_safe_defaults(self):
        info = TargetTableInfo()
        assert info.full_name == ""
        assert info.provider == ""
        assert info.clustering_columns == []
        assert info.hierarchical_clustering_columns == []
        assert info.is_delta is False
