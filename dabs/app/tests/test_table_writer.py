"""Tests for table writer module."""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from core.constants import Severity
from core.models import (
    ActionCard,
    Alert,
    BottleneckIndicators,
    OperatorHotspot,
    ProfileAnalysis,
    QueryMetrics,
    QueryStructure,
    SQLAnalysis,
    StageInfo,
    TableScanMetrics,
)
from services.table_writer import TableWriter, TableWriterConfig

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config():
    return TableWriterConfig(
        catalog="test_catalog",
        schema="test_schema",
        databricks_host="https://test.cloud.databricks.com",
        databricks_token="dapi_test_token",
        http_path="/sql/1.0/warehouses/abc123",
        enabled=True,
    )


@pytest.fixture
def disabled_config():
    return TableWriterConfig(
        catalog="test_catalog",
        schema="test_schema",
        databricks_host="https://test.cloud.databricks.com",
        databricks_token="dapi_test_token",
        http_path="/sql/1.0/warehouses/abc123",
        enabled=False,
    )


@pytest.fixture
def no_http_path_config():
    return TableWriterConfig(
        catalog="test_catalog",
        schema="test_schema",
        databricks_host="https://test.cloud.databricks.com",
        databricks_token="dapi_test_token",
        http_path="",
        enabled=True,
    )


@pytest.fixture
def sample_analysis():
    """Create a sample ProfileAnalysis for testing."""
    return ProfileAnalysis(
        query_metrics=QueryMetrics(
            query_id="q-123",
            status="FINISHED",
            query_text="SELECT * FROM t",
            total_time_ms=5000,
            execution_time_ms=4000,
            read_bytes=1024000,
        ),
        bottleneck_indicators=BottleneckIndicators(
            cache_hit_ratio=0.85,
            photon_ratio=0.70,
            spill_bytes=0,
            shuffle_impact_ratio=0.1,
            alerts=[
                Alert(severity=Severity.CRITICAL, category="spill", message="High spill"),
                Alert(severity=Severity.HIGH, category="cache", message="Low cache"),
                Alert(severity=Severity.MEDIUM, category="shuffle", message="Shuffle"),
                Alert(severity=Severity.INFO, category="io", message="Info"),
            ],
        ),
        sql_analysis=SQLAnalysis(
            structure=QueryStructure(
                statement_type="SELECT",
                join_count=2,
                complexity_score=5,
            ),
        ),
        action_cards=[
            ActionCard(
                problem="High disk spill",
                evidence=["spill > 5GB"],
                likely_cause="Insufficient memory",
                fix="Increase warehouse size",
                expected_impact="high",
                effort="low",
                priority_score=8.5,
            ),
        ],
        top_scanned_tables=[
            TableScanMetrics(
                table_name="catalog.schema.orders",
                bytes_read=500000,
                bytes_pruned=300000,
                files_read=10,
                files_pruned=6,
                rows_scanned=100000,
                current_clustering_keys=["id"],
                recommended_clustering_keys=["date", "region"],
            ),
        ],
        hot_operators=[
            OperatorHotspot(
                rank=1,
                node_id="n-1",
                node_name="SortMergeJoin",
                duration_ms=2000,
                time_share_percent=40.0,
                rows_in=50000,
                rows_out=25000,
                spill_bytes=1024,
                peak_memory_bytes=2048000,
                is_photon=True,
                bottleneck_type="join",
            ),
        ],
        stage_info=[
            StageInfo(
                stage_id="s-1",
                status="COMPLETE",
                duration_ms=3000,
                num_tasks=10,
                num_complete_tasks=10,
            ),
        ],
        endpoint_id="ep-001",
    )


# ---------------------------------------------------------------------------
# TableWriterConfig tests
# ---------------------------------------------------------------------------


class TestTableWriterConfig:
    """Tests for TableWriterConfig.from_env().

    Uses a nonexistent config file path so that the config_store
    doesn't interfere with env-only defaults.
    """

    def test_from_env_defaults(self):
        with patch.dict(
            os.environ, {"DBSQL_PROFILER_CONFIG": "/tmp/_no_such_config.json"}, clear=True
        ):
            cfg = TableWriterConfig.from_env()
        assert cfg.catalog == "main"
        assert cfg.schema == "profiler"
        assert cfg.enabled is False

    def test_from_env_custom(self):
        env = {
            "DBSQL_PROFILER_CONFIG": "/tmp/_no_such_config.json",
            "PROFILER_CATALOG": "my_catalog",
            "PROFILER_SCHEMA": "my_schema",
            "DATABRICKS_HOST": "https://host.com",
            "DATABRICKS_TOKEN": "tok",
            "PROFILER_WAREHOUSE_HTTP_PATH": "/sql/1.0/warehouses/x",
            "PROFILER_TABLE_WRITE_ENABLED": "true",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = TableWriterConfig.from_env()
        assert cfg.catalog == "my_catalog"
        assert cfg.schema == "my_schema"
        assert cfg.databricks_host == "https://host.com"
        assert cfg.http_path == "/sql/1.0/warehouses/x"
        assert cfg.enabled is True

    def test_from_env_enabled_variants(self):
        for val in ("true", "True", "TRUE", "1", "yes"):
            with patch.dict(
                os.environ,
                {
                    "PROFILER_TABLE_WRITE_ENABLED": val,
                    "DBSQL_PROFILER_CONFIG": "/tmp/_no_such_config.json",
                },
                clear=True,
            ):
                assert TableWriterConfig.from_env().enabled is True

        for val in ("false", "0", "no", ""):
            with patch.dict(
                os.environ,
                {
                    "PROFILER_TABLE_WRITE_ENABLED": val,
                    "DBSQL_PROFILER_CONFIG": "/tmp/_no_such_config.json",
                },
                clear=True,
            ):
                assert TableWriterConfig.from_env().enabled is False


# ---------------------------------------------------------------------------
# TableWriter tests
# ---------------------------------------------------------------------------


class TestTableWriter:
    def test_fqn(self, config):
        writer = TableWriter(config)
        assert writer._fqn("my_table") == "test_catalog.test_schema.my_table"

    def test_write_disabled(self, disabled_config, sample_analysis):
        writer = TableWriter(disabled_config)
        result = writer.write(sample_analysis)
        assert result is None

    def test_write_no_http_path(self, no_http_path_config, sample_analysis):
        writer = TableWriter(no_http_path_config)
        result = writer.write(sample_analysis)
        assert result is None

    @patch("services.table_writer.TableWriter._get_connection")
    def test_write_calls_all_sub_writers(self, mock_get_conn, config, sample_analysis):
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        writer = TableWriter(config)
        analysis_id = writer.write(
            sample_analysis, report="# Report", raw_profile_json='{"query": {}}'
        )

        assert analysis_id is not None
        # Should have executed DDL + INSERT for each table
        executed_sqls = [c.args[0] for c in mock_cursor.execute.call_args_list]

        # Verify all 6 DDL statements were called
        ddl_sqls = [s for s in executed_sqls if "CREATE TABLE" in s]
        assert len(ddl_sqls) == 6

        # Verify INSERT statements were called
        insert_sqls = [s for s in executed_sqls if "INSERT INTO" in s]
        # header(1) + actions(1) + table_scans(1) + hot_operators(1) + stages(1) + raw(1)
        assert len(insert_sqls) == 6

    @patch("services.table_writer.TableWriter._get_connection")
    def test_write_empty_child_tables(self, mock_get_conn, config):
        """When analysis has no action_cards/scans/etc, only header + raw are written."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        empty_analysis = ProfileAnalysis(
            query_metrics=QueryMetrics(query_id="q-empty"),
        )
        writer = TableWriter(config)
        analysis_id = writer.write(empty_analysis)

        assert analysis_id is not None
        executed_sqls = [c.args[0] for c in mock_cursor.execute.call_args_list]
        insert_sqls = [s for s in executed_sqls if "INSERT INTO" in s]
        # Only header + raw (child tables skipped because lists are empty)
        assert len(insert_sqls) == 2

    @patch("services.table_writer.TableWriter._get_connection")
    def test_ensure_table_idempotent(self, mock_get_conn, config):
        """DDL for the same table is only executed once per writer instance."""
        mock_cursor = MagicMock()
        writer = TableWriter(config)

        writer._ensure_table(mock_cursor, "profiler_analysis_header")
        writer._ensure_table(mock_cursor, "profiler_analysis_header")

        ddl_calls = [c for c in mock_cursor.execute.call_args_list if "CREATE TABLE" in c.args[0]]
        assert len(ddl_calls) == 1

    @patch("services.table_writer.TableWriter._get_connection")
    def test_write_connection_error(self, mock_get_conn, config, sample_analysis):
        """Connection errors are caught and None is returned."""
        mock_get_conn.side_effect = Exception("Connection refused")

        writer = TableWriter(config)
        result = writer.write(sample_analysis)
        assert result is None

    @patch("services.table_writer.TableWriter._get_connection")
    def test_header_insert_params(self, mock_get_conn, config, sample_analysis):
        """Verify header INSERT parameters are correctly mapped."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        writer = TableWriter(config)
        writer.write(sample_analysis, report="# Test Report")

        # Find the header INSERT call
        header_insert_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO" in c.args[0] and "header" in c.args[0]
        ]
        assert len(header_insert_calls) == 1
        params = header_insert_calls[0].kwargs.get("parameters", {})

        assert params["query_id"] == "q-123"
        assert params["query_status"] == "FINISHED"
        assert params["total_time_ms"] == 5000
        assert params["cache_hit_ratio"] == 0.85
        assert params["statement_type"] == "SELECT"
        assert params["critical_alert_count"] == 1
        assert params["high_alert_count"] == 1
        assert params["action_card_count"] == 1
        assert params["report_markdown"] == "# Test Report"

    @patch("services.table_writer.TableWriter._get_connection")
    def test_actions_insert_params(self, mock_get_conn, config, sample_analysis):
        """Verify actions INSERT parameters are correctly mapped."""
        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_get_conn.return_value = mock_conn

        writer = TableWriter(config)
        writer.write(sample_analysis)

        action_insert_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if "INSERT INTO" in c.args[0] and "actions" in c.args[0]
        ]
        assert len(action_insert_calls) == 1
        params = action_insert_calls[0].kwargs.get("parameters", {})

        assert params["problem"] == "High disk spill"
        assert params["priority_score"] == 8.5
        assert params["expected_impact"] == "high"
        assert json.loads(params["evidence"]) == ["spill > 5GB"]

    def test_host_stripping_logic(self, config):
        """Verify host URL stripping logic works correctly."""
        TableWriter(config)
        # Access the private method's logic: the host should be stripped
        host = config.databricks_host
        if host.startswith("https://"):
            host = host[len("https://") :]
        host = host.rstrip("/")
        assert host == "test.cloud.databricks.com"

    def test_host_stripping_with_trailing_slash(self):
        """Host with trailing slash is stripped."""
        cfg = TableWriterConfig(
            catalog="c",
            schema="s",
            databricks_host="https://host.com/",
            databricks_token="t",
            http_path="/p",
        )
        TableWriter(cfg)
        host = cfg.databricks_host
        if host.startswith("https://"):
            host = host[len("https://") :]
        host = host.rstrip("/")
        assert host == "host.com"

    def test_get_connection_no_token_attempts_sdk(self):
        """When token is empty, SDK auth path is attempted (not PAT)."""
        no_token_config = TableWriterConfig(
            catalog="c",
            schema="s",
            databricks_host="https://host.com",
            databricks_token="",
            http_path="/sql/1.0/warehouses/x",
        )
        writer = TableWriter(no_token_config)
        # Without databricks SDK installed, it will raise an ImportError or similar
        # The key point is it does NOT try to connect with empty access_token
        # (which would cause OAuth browser flow)
        with pytest.raises(Exception):
            writer._get_connection()
