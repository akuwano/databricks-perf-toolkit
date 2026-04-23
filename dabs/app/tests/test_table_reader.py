"""Tests for services.table_reader module."""

from unittest.mock import MagicMock, patch

import pytest
from services.table_reader import AnalysisSummary, TableReader
from services.table_writer import TableWriterConfig


@pytest.fixture
def config():
    return TableWriterConfig(
        catalog="test_catalog",
        schema="test_schema",
        databricks_host="https://test.cloud.databricks.com",
        databricks_token="test-token",
        http_path="/sql/1.0/warehouses/abc123",
        enabled=True,
    )


@pytest.fixture
def reader(config):
    return TableReader(config)


class TestTableReader:
    def test_fqn(self, reader):
        assert reader._fqn("my_table") == "test_catalog.test_schema.my_table"

    def test_row_to_analysis_basic(self, reader):
        row = {
            "analysis_id": "id-1",
            "query_id": "q-1",
            "query_status": "FINISHED",
            "query_text": "SELECT 1",
            "total_time_ms": 500,
            "compilation_time_ms": 50,
            "execution_time_ms": 450,
            "read_bytes": 1000,
            "read_remote_bytes": 200,
            "read_cache_bytes": 800,
            "spill_to_disk_bytes": 0,
            "photon_total_time_ms": 400,
            "task_total_time_ms": 450,
            "read_files_count": 10,
            "pruned_files_count": 5,
            "pruned_bytes": 500,
            "rows_read_count": 100,
            "rows_produced_count": 50,
            "bytes_read_from_cache_percentage": 80,
            "write_remote_bytes": 0,
            "write_remote_files": 0,
            "network_sent_bytes": 0,
            "read_partitions_count": 2,
            "cache_hit_ratio": 0.8,
            "remote_read_ratio": 0.2,
            "photon_ratio": 0.9,
            "spill_bytes": 0,
            "filter_rate": 0.5,
            "bytes_pruning_ratio": 0.5,
            "shuffle_impact_ratio": 0.1,
            "cloud_storage_retry_ratio": 0.0,
            "has_data_skew": False,
            "skewed_partitions": 0,
            "rescheduled_scan_ratio": 0.0,
            "oom_fallback_count": 0,
            "endpoint_id": "ep-1",
            "query_fingerprint": "fp123",
            "query_fingerprint_version": "v1",
            "experiment_id": "exp-1",
            "variant": "baseline",
            "variant_group": "",
            "baseline_flag": True,
            "tags_json": '{"env":"prod"}',
            "source_run_id": "",
            "source_job_id": "",
            "source_job_run_id": "",
            "analysis_notes": "test note",
            "query_text_normalized": "select ?",
        }
        analysis = reader._row_to_analysis(row)
        assert analysis.query_metrics.query_id == "q-1"
        assert analysis.query_metrics.total_time_ms == 500
        assert analysis.bottleneck_indicators.photon_ratio == 0.9
        assert analysis.analysis_context.query_fingerprint == "fp123"
        assert analysis.analysis_context.experiment_id == "exp-1"
        assert analysis.analysis_context.variant == "baseline"
        assert analysis.analysis_context.baseline_flag is True
        assert analysis.analysis_context.tags == {"env": "prod"}

    def test_row_to_analysis_null_handling(self, reader):
        row = {
            "analysis_id": "id-2",
            "query_id": None,
            "query_status": None,
            "query_text": None,
            "total_time_ms": None,
            "compilation_time_ms": None,
            "execution_time_ms": None,
            "read_bytes": None,
            "read_remote_bytes": None,
            "read_cache_bytes": None,
            "spill_to_disk_bytes": None,
            "photon_total_time_ms": None,
            "task_total_time_ms": None,
            "read_files_count": None,
            "pruned_files_count": None,
            "pruned_bytes": None,
            "rows_read_count": None,
            "rows_produced_count": None,
            "bytes_read_from_cache_percentage": None,
            "write_remote_bytes": None,
            "write_remote_files": None,
            "network_sent_bytes": None,
            "read_partitions_count": None,
            "cache_hit_ratio": None,
            "remote_read_ratio": None,
            "photon_ratio": None,
            "spill_bytes": None,
            "filter_rate": None,
            "bytes_pruning_ratio": None,
            "shuffle_impact_ratio": None,
            "cloud_storage_retry_ratio": None,
            "has_data_skew": None,
            "skewed_partitions": None,
            "rescheduled_scan_ratio": None,
            "oom_fallback_count": None,
            "endpoint_id": None,
            "query_fingerprint": None,
            "query_fingerprint_version": None,
            "experiment_id": None,
            "variant": None,
            "variant_group": None,
            "baseline_flag": None,
            "tags_json": None,
            "source_run_id": None,
            "source_job_id": None,
            "source_job_run_id": None,
            "analysis_notes": None,
            "query_text_normalized": None,
        }
        analysis = reader._row_to_analysis(row)
        assert analysis.query_metrics.total_time_ms == 0
        assert analysis.bottleneck_indicators.photon_ratio == 0.0
        assert analysis.analysis_context.tags == {}

    def test_row_to_summary(self, reader):
        row = {
            "analysis_id": "id-1",
            "analyzed_at": "2026-03-19T00:00:00",
            "query_id": "q-1",
            "query_fingerprint": "fp123",
            "experiment_id": "exp-1",
            "variant": "baseline",
            "total_time_ms": 500,
            "read_bytes": 1000,
            "spill_bytes": 0,
            "warehouse_name": "wh-1",
            "warehouse_size": "Medium",
            "action_card_count": 3,
            "critical_alert_count": 1,
        }
        summary = reader._row_to_summary(row)
        assert isinstance(summary, AnalysisSummary)
        assert summary.analysis_id == "id-1"
        assert summary.total_time_ms == 500
        assert summary.warehouse_name == "wh-1"

    @patch.object(TableReader, "_get_connection")
    def test_get_analysis_by_id_not_found(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)
        result = reader.get_analysis_by_id("nonexistent")
        assert result is None

    @patch.object(TableReader, "_get_connection")
    def test_list_analyses_empty(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.description = []
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)
        results = reader.list_analyses()
        assert results == []


class TestFindBaseline:
    @patch.object(TableReader, "_get_connection")
    def test_find_baseline_by_family(self, mock_conn, reader):
        """Should find baseline with matching query_family_id."""
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("analysis_id",),
            ("query_id",),
            ("query_status",),
            ("query_text",),
            ("total_time_ms",),
            ("compilation_time_ms",),
            ("execution_time_ms",),
            ("read_bytes",),
            ("read_remote_bytes",),
            ("read_cache_bytes",),
            ("spill_to_disk_bytes",),
            ("photon_total_time_ms",),
            ("task_total_time_ms",),
            ("read_files_count",),
            ("pruned_files_count",),
            ("pruned_bytes",),
            ("rows_read_count",),
            ("rows_produced_count",),
            ("bytes_read_from_cache_percentage",),
            ("write_remote_bytes",),
            ("write_remote_files",),
            ("network_sent_bytes",),
            ("read_partitions_count",),
            ("cache_hit_ratio",),
            ("remote_read_ratio",),
            ("photon_ratio",),
            ("spill_bytes",),
            ("filter_rate",),
            ("bytes_pruning_ratio",),
            ("shuffle_impact_ratio",),
            ("cloud_storage_retry_ratio",),
            ("has_data_skew",),
            ("skewed_partitions",),
            ("rescheduled_scan_ratio",),
            ("oom_fallback_count",),
            ("endpoint_id",),
            ("query_fingerprint",),
            ("query_fingerprint_version",),
            ("experiment_id",),
            ("variant",),
            ("variant_group",),
            ("baseline_flag",),
            ("tags_json",),
            ("source_run_id",),
            ("source_job_id",),
            ("source_job_run_id",),
            ("analysis_notes",),
            ("query_text_normalized",),
            ("query_family_id",),
            ("purpose_signature",),
            ("variant_type",),
            ("feature_json",),
        ]
        mock_cursor.fetchone.return_value = (
            "baseline-id",
            "q-1",
            "FINISHED",
            "SELECT 1",
            1000,
            50,
            950,
            1000,
            200,
            800,
            0,
            400,
            950,
            10,
            5,
            500,
            100,
            50,
            80,
            0,
            0,
            0,
            2,
            0.8,
            0.2,
            0.9,
            0,
            0.5,
            0.5,
            0.1,
            0.0,
            False,
            0,
            0.0,
            0,
            "ep-1",
            "fp123",
            "v1",
            "exp-1",
            "baseline",
            "",
            True,
            None,
            "",
            "",
            "",
            "",
            "",
            "fam123",
            "ps123",
            "",
            "",
        )
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.find_baseline("fam123")
        assert result is not None
        assert result.analysis_context.baseline_flag is True


class TestGetAnalysisWithReport:
    @patch.object(TableReader, "_get_connection")
    def test_returns_tuple_with_report(self, mock_conn, reader):
        """get_analysis_with_report returns (ProfileAnalysis, report_markdown)."""
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("analysis_id",),
            ("query_id",),
            ("query_status",),
            ("query_text",),
            ("total_time_ms",),
            ("compilation_time_ms",),
            ("execution_time_ms",),
            ("read_bytes",),
            ("read_remote_bytes",),
            ("read_cache_bytes",),
            ("spill_to_disk_bytes",),
            ("photon_total_time_ms",),
            ("task_total_time_ms",),
            ("read_files_count",),
            ("pruned_files_count",),
            ("pruned_bytes",),
            ("rows_read_count",),
            ("rows_produced_count",),
            ("bytes_read_from_cache_percentage",),
            ("write_remote_bytes",),
            ("write_remote_files",),
            ("network_sent_bytes",),
            ("read_partitions_count",),
            ("cache_hit_ratio",),
            ("remote_read_ratio",),
            ("photon_ratio",),
            ("spill_bytes",),
            ("filter_rate",),
            ("bytes_pruning_ratio",),
            ("shuffle_impact_ratio",),
            ("cloud_storage_retry_ratio",),
            ("has_data_skew",),
            ("skewed_partitions",),
            ("rescheduled_scan_ratio",),
            ("oom_fallback_count",),
            ("endpoint_id",),
            ("query_fingerprint",),
            ("query_fingerprint_version",),
            ("experiment_id",),
            ("variant",),
            ("variant_group",),
            ("baseline_flag",),
            ("tags_json",),
            ("source_run_id",),
            ("source_job_id",),
            ("source_job_run_id",),
            ("analysis_notes",),
            ("query_text_normalized",),
            ("query_family_id",),
            ("purpose_signature",),
            ("variant_type",),
            ("feature_json",),
            ("report_markdown",),
            ("warehouse_name",),
            ("warehouse_size",),
            ("action_card_count",),
            ("critical_alert_count",),
        ]
        mock_cursor.fetchone.return_value = (
            "id-1",
            "q-1",
            "FINISHED",
            "SELECT 1",
            1000,
            50,
            950,
            1000,
            200,
            800,
            0,
            400,
            950,
            10,
            5,
            500,
            100,
            50,
            80,
            0,
            0,
            0,
            2,
            0.8,
            0.2,
            0.9,
            0,
            0.5,
            0.5,
            0.1,
            0.0,
            False,
            0,
            0.0,
            0,
            "ep-1",
            "fp123",
            "v1",
            "exp-1",
            "baseline",
            "",
            True,
            None,
            "",
            "",
            "",
            "",
            "",
            "fam123",
            "ps123",
            "",
            "",
            "# Report\nSome markdown",
            "Shared WH",
            "Medium",
            3,
            1,
        )
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_analysis_with_report("id-1")
        assert result is not None
        assert result.analysis.query_metrics.query_id == "q-1"
        assert result.report_markdown == "# Report\nSome markdown"
        assert result.warehouse_name == "Shared WH"
        assert result.warehouse_size == "Medium"
        assert result.action_card_count == 3
        assert result.critical_alert_count == 1

    @patch.object(TableReader, "_get_connection")
    def test_returns_none_when_not_found(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_analysis_with_report("nonexistent")
        assert result is None


class TestFindBaselineOrig:
    @patch.object(TableReader, "_get_connection")
    def test_find_baseline_returns_none_when_not_found(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.find_baseline("nonexistent")
        assert result is None
