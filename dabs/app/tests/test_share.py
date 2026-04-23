"""Tests for routes.share — shared result pages and Slack summary API."""

from unittest.mock import patch

import pytest
from core.models import BottleneckIndicators, ProfileAnalysis, QueryMetrics


@pytest.fixture
def app():
    """Create Flask test app with share blueprint."""
    import os
    import sys

    # Ensure app module is importable
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    from app import app as flask_app

    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


def _make_analysis():
    return ProfileAnalysis(
        query_metrics=QueryMetrics(
            query_id="q-123456789012",
            status="FINISHED",
            total_time_ms=5000,
            read_bytes=1_000_000,
        ),
        bottleneck_indicators=BottleneckIndicators(
            cache_hit_ratio=0.85,
            photon_ratio=0.90,
            spill_bytes=0,
            critical_issues=["Data skew"],
            warnings=["Low filter rate"],
        ),
    )


class TestSharedResultPage:
    def test_from_memory(self, client, app):
        """Returns 200 when analysis is in memory."""
        from app import analysis_store

        analysis_store["test-id"] = {
            "status": "completed",
            "analysis": _make_analysis(),
            "llm_result": {"llm_enabled": False},
            "report": "# Test Report\nSome content",
            "filename": "test.json",
        }
        try:
            resp = client.get("/shared/test-id")
            assert resp.status_code == 200
            assert b"Test Report" in resp.data or b"test-id" in resp.data
        finally:
            del analysis_store["test-id"]

    @patch("routes.share._load_from_delta")
    def test_from_delta(self, mock_delta, client):
        """Returns 200 when loaded from Delta tables."""
        from services.table_reader import AnalysisWithReport

        mock_delta.return_value = AnalysisWithReport(
            analysis=_make_analysis(),
            report_markdown="# Delta Report",
            warehouse_name="Shared WH",
            warehouse_size="Medium",
            action_card_count=5,
            critical_alert_count=2,
        )
        resp = client.get("/shared/delta-id")
        assert resp.status_code == 200
        mock_delta.assert_called_once_with("delta-id")

    @patch("routes.share._load_from_delta")
    def test_not_found(self, mock_delta, client):
        """Returns 404 when not in memory or Delta."""
        mock_delta.return_value = None
        resp = client.get("/shared/nonexistent")
        assert resp.status_code == 404


class TestSlackSummaryAPI:
    def test_summary_from_memory(self, client, app):
        """Returns text/plain summary."""
        from app import analysis_store

        analysis_store["sum-id"] = {
            "status": "completed",
            "analysis": _make_analysis(),
            "llm_result": {"llm_enabled": False},
            "report": "# Report",
            "filename": "test.json",
        }
        try:
            resp = client.get("/api/v1/shared/sum-id/summary")
            assert resp.status_code == 200
            assert resp.content_type.startswith("text/plain")
            text = resp.data.decode()
            assert "q-123456789012"[:12] in text
            assert "85.0%" in text
        finally:
            del analysis_store["sum-id"]

    @patch("routes.share._load_from_delta")
    def test_summary_not_found(self, mock_delta, client):
        mock_delta.return_value = None
        resp = client.get("/api/v1/shared/nonexistent/summary")
        assert resp.status_code == 404
