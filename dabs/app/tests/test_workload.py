"""Tests for /workload — cross-analysis page and workload pairs API."""

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


class TestWorkloadPage:
    """GET /workload returns the cross-analysis page."""

    def test_returns_200(self, client):
        resp = client.get("/workload")
        assert resp.status_code == 200
        assert b"html" in resp.data.lower()

    def test_has_dbsql_and_spark_selectors(self, client):
        resp = client.get("/workload")
        html = resp.data.decode()
        assert "dbsqlSel" in html or "dbsql" in html.lower()
        assert "sparkSel" in html or "spark" in html.lower()


class TestWorkloadPairsAPI:
    """CRUD for workload pairs (DBSQL analysis ↔ Spark app)."""

    def test_list_pairs_empty(self, client, tmp_path):
        config_file = str(tmp_path / "test_config.json")
        with patch.dict(os.environ, {"DBSQL_PROFILER_CONFIG": config_file}):
            resp = client.get("/api/v1/workload/pairs")
            assert resp.status_code == 200
            data = resp.get_json()
            assert "pairs" in data
            assert isinstance(data["pairs"], list)

    def test_save_pair(self, client, tmp_path):
        config_file = str(tmp_path / "test_config.json")
        with patch.dict(os.environ, {"DBSQL_PROFILER_CONFIG": config_file}):
            resp = client.post(
                "/api/v1/workload/pairs",
                json={
                    "analysis_id": "abc-123",
                    "app_id": "app-20260322-0001",
                    "label": "ETL batch vs query",
                },
            )
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["status"] == "saved"

            # Verify it appears in the list
            resp = client.get("/api/v1/workload/pairs")
            pairs = resp.get_json()["pairs"]
            assert len(pairs) == 1
            assert pairs[0]["analysis_id"] == "abc-123"
            assert pairs[0]["app_id"] == "app-20260322-0001"

    def test_delete_pair(self, client, tmp_path):
        config_file = str(tmp_path / "test_config.json")
        with patch.dict(os.environ, {"DBSQL_PROFILER_CONFIG": config_file}):
            # Save a pair
            client.post(
                "/api/v1/workload/pairs",
                json={"analysis_id": "abc-123", "app_id": "app-001"},
            )

            # Delete it
            resp = client.delete(
                "/api/v1/workload/pairs",
                json={"analysis_id": "abc-123", "app_id": "app-001"},
            )
            assert resp.status_code == 200

            # Verify gone
            resp = client.get("/api/v1/workload/pairs")
            assert len(resp.get_json()["pairs"]) == 0


class TestWorkloadReport:
    """GET /api/v1/workload/report returns DBSQL report markdown by analysis_id."""

    def test_requires_analysis_id(self, client):
        resp = client.get("/api/v1/workload/report")
        assert resp.status_code in (400, 422)
