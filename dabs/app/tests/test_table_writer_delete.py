"""Tests for TableWriter.delete_analysis()."""

from unittest.mock import MagicMock, patch

import pytest
from services.table_writer import TableWriter, TableWriterConfig


@pytest.fixture
def config():
    return TableWriterConfig(
        catalog="test_catalog",
        schema="test_schema",
        databricks_host="https://test.com",
        databricks_token="tok",
        http_path="/sql/1.0/warehouses/abc",
        enabled=True,
    )


@pytest.fixture
def writer(config):
    return TableWriter(config)


class TestDeleteAnalysis:
    @patch.object(TableWriter, "_get_connection")
    def test_deletes_from_all_tables(self, mock_conn, writer):
        mock_cursor = MagicMock()
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = writer.delete_analysis("id-123")
        assert result is True

        # Should delete from 6 analysis tables
        assert mock_cursor.execute.call_count == 6
        for c in mock_cursor.execute.call_args_list:
            sql = c[0][0]
            assert "DELETE FROM" in sql
            assert "analysis_id" in sql

    @patch.object(TableWriter, "_get_connection")
    def test_returns_false_on_error(self, mock_conn, writer):
        mock_conn.side_effect = Exception("connection failed")
        result = writer.delete_analysis("id-123")
        assert result is False

    def test_disabled_returns_false(self, config):
        config.enabled = False
        w = TableWriter(config)
        assert w.delete_analysis("id-123") is False

    def test_no_http_path_returns_false(self, config):
        config.http_path = ""
        w = TableWriter(config)
        assert w.delete_analysis("id-123") is False


class TestDeleteMultiple:
    @patch.object(TableWriter, "delete_analysis")
    def test_deletes_multiple(self, mock_del, writer):
        mock_del.return_value = True
        result = writer.delete_analyses(["id-1", "id-2", "id-3"])
        assert result == 3
        assert mock_del.call_count == 3

    @patch.object(TableWriter, "delete_analysis")
    def test_counts_only_successes(self, mock_del, writer):
        mock_del.side_effect = [True, False, True]
        result = writer.delete_analyses(["id-1", "id-2", "id-3"])
        assert result == 2

    @patch.object(TableWriter, "delete_analysis")
    def test_empty_list(self, mock_del, writer):
        result = writer.delete_analyses([])
        assert result == 0
        assert mock_del.call_count == 0
