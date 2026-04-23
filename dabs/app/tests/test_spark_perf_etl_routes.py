"""Tests for /api/v1/spark-perf/etl-runs — ETL job trigger and status."""

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


class TestETLTriggerAPI:
    """POST /api/v1/spark-perf/etl-runs."""

    def test_requires_json_body(self, client):
        resp = client.post(
            "/api/v1/spark-perf/etl-runs",
            data="",
            content_type="application/json",
        )
        assert resp.status_code in (400, 422)

    def test_requires_log_root(self, client):
        resp = client.post(
            "/api/v1/spark-perf/etl-runs",
            json={"cluster_id": "c1"},
        )
        assert resp.status_code in (400, 422)

    def test_requires_cluster_id(self, client):
        resp = client.post(
            "/api/v1/spark-perf/etl-runs",
            json={"log_root": "/Volumes/x"},
        )
        assert resp.status_code in (400, 422)

    def test_returns_503_when_job_id_not_configured(self, client):
        resp = client.post(
            "/api/v1/spark-perf/etl-runs",
            json={"log_root": "/Volumes/x", "cluster_id": "c1"},
        )
        assert resp.status_code == 503
        assert (
            "job_id" in resp.get_json()["error"].lower()
            or "configured" in resp.get_json()["error"].lower()
        )

    @patch("services.job_launcher.JobLauncherConfig.from_env")
    @patch("services.job_launcher.JobLauncher.trigger_etl")
    def test_trigger_success(self, mock_trigger, mock_config, client):
        from services.job_launcher import JobLauncherConfig

        mock_config.return_value = JobLauncherConfig(etl_job_id=999)
        mock_trigger.return_value = {"run_id": 42, "run_page_url": "https://db.com/run/42"}

        resp = client.post(
            "/api/v1/spark-perf/etl-runs",
            json={"log_root": "/Volumes/main/base/logs", "cluster_id": "cluster-01"},
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["run_id"] == 42
        assert data["status"] == "PENDING"


class TestETLStatusAPI:
    """GET /api/v1/spark-perf/etl-runs/<run_id>/status."""

    @patch("services.job_launcher.JobLauncher.get_run_status")
    @patch("services.job_launcher.JobLauncherConfig.from_env")
    def test_status_running(self, mock_config, mock_status, client):
        from services.job_launcher import JobLauncherConfig

        mock_config.return_value = JobLauncherConfig(etl_job_id=999)
        mock_status.return_value = {
            "run_id": 42,
            "state": "RUNNING",
            "result_state": None,
            "state_message": "In run",
            "run_page_url": "https://db.com/run/42",
        }

        resp = client.get("/api/v1/spark-perf/etl-runs/42/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["state"] == "RUNNING"

    @patch("services.job_launcher.JobLauncher.get_run_status")
    @patch("services.job_launcher.JobLauncherConfig.from_env")
    def test_status_completed(self, mock_config, mock_status, client):
        from services.job_launcher import JobLauncherConfig

        mock_config.return_value = JobLauncherConfig(etl_job_id=999)
        mock_status.return_value = {
            "run_id": 42,
            "state": "TERMINATED",
            "result_state": "SUCCESS",
            "state_message": "",
            "run_page_url": "https://db.com/run/42",
        }

        resp = client.get("/api/v1/spark-perf/etl-runs/42/status")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["state"] == "TERMINATED"
        assert data["result_state"] == "SUCCESS"
