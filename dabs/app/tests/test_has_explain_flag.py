"""Persisting and surfacing the has_explain flag.

Verifies:
- DDL lists has_explain with a migration fallback
- _write_header binds the flag from analysis.explain_analysis
- AnalysisSummary + _row_to_summary carry has_explain through
- /api/v1/history JSON exposes has_explain
"""

from unittest.mock import MagicMock

from core.explain_parser import ExplainExtended, ExplainSection
from core.models import ProfileAnalysis
from services.table_reader import AnalysisSummary, TableReader
from services.table_writer import (
    _HEADER_DDL,
    TableWriter,
    TableWriterConfig,
)


def _writer() -> TableWriter:
    config = TableWriterConfig(
        catalog="main",
        schema="prof",
        databricks_host="https://host",
        databricks_token="tok",
        http_path="/sql/1.0/warehouses/x",
        enabled=True,
    )
    return TableWriter(config)


class TestHeaderDDL:
    def test_ddl_declares_has_explain_column(self):
        assert "has_explain BOOLEAN" in _HEADER_DDL

    def test_migration_includes_has_explain(self):
        writer = _writer()
        cursor = MagicMock()
        cursor.fetchall.return_value = [("analysis_id",)]  # pretend column missing
        writer._migrate_header_columns(cursor, "`main`.`prof`.`profiler_analysis_header`")
        # Collect all ALTER TABLE calls
        alter_calls = [c for c in cursor.execute.call_args_list if "ADD COLUMNS" in str(c)]
        assert any("has_explain BOOLEAN" in str(c) for c in alter_calls)


class TestWriteHeaderBindsHasExplain:
    def _bind_value(self, analysis: ProfileAnalysis) -> bool | None:
        """Capture the has_explain parameter the writer would bind."""
        writer = _writer()
        cursor = MagicMock()
        # ensure _ensure_table short-circuits
        writer._tables_ensured.add("profiler_analysis_header")
        from datetime import UTC, datetime

        writer._write_header(
            cursor,
            analysis_id="aid",
            analyzed_at=datetime.now(UTC),
            query_id="qid",
            analysis=analysis,
            report="",
            lang="ja",
        )
        # Find the INSERT call and extract the bound params
        for call in cursor.execute.call_args_list:
            args, kwargs = call
            if kwargs.get("parameters"):
                return kwargs["parameters"].get("has_explain")
        return None

    def test_has_explain_true_when_sections_parsed(self):
        analysis = ProfileAnalysis()
        ex = ExplainExtended()
        ex.sections.append(ExplainSection(name="Physical Plan"))
        analysis.explain_analysis = ex
        assert self._bind_value(analysis) is True

    def test_has_explain_false_when_explain_analysis_is_none(self):
        analysis = ProfileAnalysis()
        analysis.explain_analysis = None
        assert self._bind_value(analysis) is False

    def test_has_explain_false_when_sections_empty(self):
        analysis = ProfileAnalysis()
        analysis.explain_analysis = ExplainExtended()  # empty sections
        assert self._bind_value(analysis) is False


class TestSummaryRoundTrip:
    def test_analysis_summary_has_has_explain_field(self):
        # AnalysisSummary must expose the flag so the history API can serialize it
        s = AnalysisSummary()
        assert hasattr(s, "has_explain")
        assert s.has_explain is None  # default

    def test_row_to_summary_reads_has_explain(self):
        config = TableWriterConfig(
            catalog="main",
            schema="prof",
            databricks_host="host",
            databricks_token="tok",
            http_path="/sql/1.0/warehouses/x",
            enabled=True,
        )
        reader = TableReader(config)
        summary = reader._row_to_summary({"analysis_id": "a", "has_explain": True})
        assert summary.has_explain is True

    def test_row_to_summary_missing_column_defaults_to_none(self):
        config = TableWriterConfig(
            catalog="main",
            schema="prof",
            databricks_host="host",
            databricks_token="tok",
            http_path="/sql/1.0/warehouses/x",
            enabled=True,
        )
        reader = TableReader(config)
        summary = reader._row_to_summary({"analysis_id": "a"})
        assert summary.has_explain is None


class TestHistoryApiResponse:
    def test_history_response_includes_has_explain(self, monkeypatch):
        # Build a minimal Flask test client by importing the app
        import os

        os.environ.setdefault("DATABRICKS_HOST", "https://example.test")
        os.environ.setdefault("DATABRICKS_TOKEN", "tok")

        from app import app
        from services import table_reader as tr_mod

        fake_summary = AnalysisSummary(
            analysis_id="aid",
            query_id="qid",
            total_time_ms=1000,
            has_explain=True,
        )

        class _FakeReader:
            def __init__(self, _config):
                pass

            def list_analyses(self, **_kw):
                return [fake_summary]

        monkeypatch.setattr(tr_mod, "TableReader", _FakeReader)

        # Force http_path so the /api/v1/history route does not early-return
        from services.table_writer import TableWriterConfig as Cfg

        original_from_env = Cfg.from_env

        def _cfg():
            cfg = original_from_env()
            cfg.http_path = "/sql/1.0/warehouses/stub"
            return cfg

        monkeypatch.setattr(Cfg, "from_env", _cfg)

        client = app.test_client()
        resp = client.get("/api/v1/history?limit=1")
        assert resp.status_code == 200
        body = resp.get_json()
        assert isinstance(body, list) and len(body) == 1
        assert body[0]["has_explain"] is True
