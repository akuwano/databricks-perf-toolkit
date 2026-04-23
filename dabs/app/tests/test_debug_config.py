"""Tests for /api/v1/debug/config endpoint."""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture()
def app():
    """Create Flask test app."""
    os.environ.setdefault("DATABRICKS_HOST", "https://test.cloud.databricks.com")
    from app import app as flask_app

    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


class TestDebugConfig:
    """GET /api/v1/debug/config returns effective settings with source info."""

    def test_returns_json(self, client):
        resp = client.get("/api/v1/debug/config")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "settings" in data

    def test_each_setting_has_value_and_source(self, client):
        resp = client.get("/api/v1/debug/config")
        data = resp.get_json()
        for key, info in data["settings"].items():
            assert "value" in info, f"{key} missing 'value'"
            assert "source" in info, f"{key} missing 'source'"
            assert info["source"] in ("env", "user_config", "runtime_config", "default")

    def test_env_var_takes_priority(self, client):
        with patch.dict(os.environ, {"PROFILER_CATALOG": "env_catalog"}):
            resp = client.get("/api/v1/debug/config")
            data = resp.get_json()
            cat = data["settings"]["catalog"]
            assert cat["value"] == "env_catalog"
            assert cat["source"] == "env"

    def test_shows_all_expected_keys(self, client):
        resp = client.get("/api/v1/debug/config")
        data = resp.get_json()
        expected = {
            "catalog",
            "schema",
            "http_path",
            "table_write_enabled",
            "spark_perf_catalog",
            "spark_perf_schema",
            "spark_perf_table_prefix",
            "spark_perf_http_path",
        }
        assert expected.issubset(data["settings"].keys())

    def test_includes_config_paths(self, client):
        resp = client.get("/api/v1/debug/config")
        data = resp.get_json()
        assert "config_paths" in data
        assert "user_config" in data["config_paths"]
        assert "runtime_config" in data["config_paths"]
