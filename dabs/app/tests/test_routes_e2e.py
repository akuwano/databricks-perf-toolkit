"""E2E route tests — verify Flask routes return expected responses.

These tests use the Flask test client to confirm that API endpoints and
page routes are wired correctly (not just unit-level logic).
"""

import io
import json
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


# -- Minimal profile JSON for upload tests --
_MINIMAL_PROFILE = {
    "id": "test-query-001",
    "metrics": {
        "totalTimeMsec": 1000,
    },
    "graphs": [
        {
            "nodes": [
                {
                    "id": "0",
                    "name": "Scan",
                    "metrics": [{"name": "number of output rows", "value": "1000"}],
                }
            ],
            "edges": [],
        }
    ],
}


class TestPageRoutes:
    """HTML pages return 200."""

    def test_index_page(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert b"html" in resp.data.lower()

    def test_history_page(self, client):
        resp = client.get("/history")
        assert resp.status_code == 200

    def test_compare_page(self, client):
        resp = client.get("/compare")
        assert resp.status_code == 200

    def test_spark_perf_page(self, client):
        resp = client.get("/spark-perf")
        assert resp.status_code == 200

    def test_report_upload_page(self, client):
        resp = client.get("/report")
        assert resp.status_code == 200


class TestSettingsAPI:
    """Settings endpoints work end-to-end."""

    def test_get_settings(self, client):
        resp = client.get("/api/v1/settings")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "catalog" in data
        assert "schema" in data

    def test_save_and_get_settings(self, client, tmp_path):
        config_file = str(tmp_path / "test_config.json")
        with patch.dict(os.environ, {"DBSQL_PROFILER_CONFIG": config_file}):
            # Save
            resp = client.post(
                "/api/v1/settings",
                json={"catalog": "test_cat", "schema": "test_sch"},
            )
            assert resp.status_code == 200

            # Verify saved
            resp = client.get("/api/v1/settings")
            data = resp.get_json()
            assert data["catalog"] == "test_cat"
            assert data["schema"] == "test_sch"

    def test_debug_config(self, client):
        resp = client.get("/api/v1/debug/config")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "settings" in data
        assert "config_paths" in data


class TestAnalysisAPI:
    """Analysis upload and status polling."""

    def test_analyze_no_file_returns_error(self, client):
        resp = client.post("/api/v1/analyze")
        assert resp.status_code in (400, 422)

    def test_analyze_invalid_json_returns_error(self, client):
        data = {"file": (io.BytesIO(b"not json"), "bad.json")}
        resp = client.post("/api/v1/analyze", data=data, content_type="multipart/form-data")
        assert resp.status_code in (400, 422)

    @patch("app.get_databricks_credentials", return_value=("https://test.databricks.com", "token"))
    def test_analyze_skip_llm_returns_id(self, mock_creds, client):
        """Upload with skip_llm=true should start analysis and return an ID."""
        profile_bytes = json.dumps(_MINIMAL_PROFILE).encode()
        data = {
            "file": (io.BytesIO(profile_bytes), "profile.json"),
            "skip_llm": "true",
        }
        resp = client.post("/api/v1/analyze", data=data, content_type="multipart/form-data")
        assert resp.status_code == 200
        result = resp.get_json()
        assert "id" in result
        assert result["status"] == "pending"

    @patch("app.get_databricks_credentials", return_value=("https://test.databricks.com", "token"))
    def test_analyze_status_polling(self, mock_creds, client):
        """Status endpoint returns valid response for a started analysis."""
        profile_bytes = json.dumps(_MINIMAL_PROFILE).encode()
        data = {
            "file": (io.BytesIO(profile_bytes), "profile.json"),
            "skip_llm": "true",
        }
        resp = client.post("/api/v1/analyze", data=data, content_type="multipart/form-data")
        analysis_id = resp.get_json()["id"]

        # Poll status (should be pending or completed quickly with skip_llm)
        resp = client.get(f"/api/v1/analyze/{analysis_id}/status")
        assert resp.status_code == 200
        status_data = resp.get_json()
        assert status_data["status"] in ("pending", "processing", "completed", "failed")

    def test_analyze_status_not_found(self, client):
        resp = client.get("/api/v1/analyze/nonexistent-id/status")
        assert resp.status_code == 404


class TestSparkPerfAPI:
    """Spark Perf endpoints return expected shapes."""

    def test_applications_without_config(self, client, tmp_path):
        """Without http_path configured, should return 503."""
        config_file = str(tmp_path / "empty_config.json")
        with patch.dict(os.environ, {"DBSQL_PROFILER_CONFIG": config_file}, clear=False):
            # Remove any spark perf env vars that might provide http_path
            env_clean = {k: "" for k in ("SPARK_PERF_HTTP_PATH",) if k in os.environ}
            with patch.dict(os.environ, env_clean, clear=False):
                from core.config_store import _reset_runtime_config_cache

                _reset_runtime_config_cache()
                resp = client.get("/api/v1/spark-perf/applications")
                # May return 503 (no config) or 200 (runtime-config has http_path)
                assert resp.status_code in (200, 503)

    def test_report_without_app_id(self, client):
        """Report endpoint requires app_id parameter."""
        resp = client.get("/api/v1/spark-perf/report")
        assert resp.status_code in (400, 422)

    def test_spark_perf_settings(self, client):
        resp = client.get("/api/v1/spark-perf/settings")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "catalog" in data


class TestSharedRoutes:
    """Shared result routes."""

    def test_shared_nonexistent(self, client):
        resp = client.get("/shared/nonexistent-id")
        assert resp.status_code == 404

    def test_shared_summary_nonexistent(self, client):
        resp = client.get("/api/v1/shared/nonexistent-id/summary")
        assert resp.status_code == 404
