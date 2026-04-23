"""Tests for /api/v1/workload/cross-analyze — LLM cross-analysis."""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture()
def app():
    os.environ.setdefault("DATABRICKS_HOST", "https://test.cloud.databricks.com")
    from app import app as flask_app

    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


class TestCrossAnalyzeAPI:
    """POST /api/v1/workload/cross-analyze generates LLM cross-analysis."""

    def test_requires_both_reports(self, client):
        resp = client.post(
            "/api/v1/workload/cross-analyze",
            json={"dbsql_report": "some report", "spark_report": ""},
        )
        assert resp.status_code in (400, 422)

    def test_requires_json_body(self, client):
        resp = client.post(
            "/api/v1/workload/cross-analyze",
            data="",
            content_type="application/json",
        )
        assert resp.status_code in (400, 422)

    @patch(
        "core.llm_client.call_llm_with_retry",
        return_value="## Cross Analysis\n\nBoth have shuffle issues.",
    )
    @patch("core.llm_client.create_openai_client")
    @patch("app.get_databricks_credentials", return_value=("https://test.db.com", "tok"))
    def test_returns_cross_analysis(self, mock_creds, mock_client, mock_llm, client):
        resp = client.post(
            "/api/v1/workload/cross-analyze",
            json={
                "dbsql_report": "# DBSQL Report\nShuffle impact: 45%",
                "spark_report": "# Spark Report\nDATA_SKEW on stage 20",
            },
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert "analysis" in data
        assert len(data["analysis"]) > 0

    @patch("app.get_databricks_credentials", return_value=("", ""))
    def test_no_credentials_returns_503(self, mock_creds, client):
        resp = client.post(
            "/api/v1/workload/cross-analyze",
            json={
                "dbsql_report": "report1",
                "spark_report": "report2",
            },
        )
        assert resp.status_code == 503
