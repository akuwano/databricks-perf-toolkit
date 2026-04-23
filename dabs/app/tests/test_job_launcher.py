"""Tests for services/job_launcher.py — ETL job triggering via Databricks SDK."""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


class TestJobLauncherConfig:
    """JobLauncherConfig loads from config_store."""

    def test_from_env_default(self):
        from services.job_launcher import JobLauncherConfig

        with patch.dict(os.environ, {}, clear=False):
            config = JobLauncherConfig.from_env()
            assert config.etl_job_id == 0
            assert config.summary_job_id == 0

    def test_from_env_with_job_ids(self, tmp_path):
        from services.job_launcher import JobLauncherConfig

        config_file = str(tmp_path / "config.json")
        with patch.dict(
            os.environ,
            {
                "SPARK_PERF_ETL_JOB_ID": "12345",
                "SPARK_PERF_SUMMARY_JOB_ID": "67890",
                "DBSQL_PROFILER_CONFIG": config_file,
            },
        ):
            config = JobLauncherConfig.from_env()
            assert config.etl_job_id == 12345
            assert config.summary_job_id == 67890


class TestJobLauncherTrigger:
    """JobLauncher.trigger_etl() calls SDK jobs.run_now()."""

    def test_trigger_etl_calls_run_now(self):
        from services.job_launcher import JobLauncher, JobLauncherConfig

        config = JobLauncherConfig(etl_job_id=999)
        launcher = JobLauncher(config)

        mock_ws = MagicMock()
        mock_run = MagicMock()
        mock_run.run_id = 42
        mock_run.run_page_url = "https://databricks.com/run/42"
        mock_ws.jobs.run_now.return_value = mock_run
        launcher._ws = mock_ws

        result = launcher.trigger_etl(
            log_root="/Volumes/main/base/logs",
            cluster_id="cluster-01",
        )

        assert result["run_id"] == 42
        mock_ws.jobs.run_now.assert_called_once()
        call_kwargs = mock_ws.jobs.run_now.call_args
        assert call_kwargs.kwargs["job_id"] in (999,)
        params = call_kwargs.kwargs["notebook_params"]
        assert params["log_root"] == "/Volumes/main/base/logs"
        assert params["cluster_id"] == "cluster-01"

    def test_trigger_etl_requires_job_id(self):
        from services.job_launcher import JobLauncher, JobLauncherConfig

        config = JobLauncherConfig(etl_job_id=0)
        launcher = JobLauncher(config)

        with pytest.raises(ValueError, match="job_id"):
            launcher.trigger_etl(log_root="/Volumes/x", cluster_id="c1")


class TestJobLauncherStatus:
    """JobLauncher.get_run_status() calls SDK jobs.get_run()."""

    def test_get_run_status_running(self):
        from services.job_launcher import JobLauncher, JobLauncherConfig

        config = JobLauncherConfig(etl_job_id=999)
        launcher = JobLauncher(config)

        mock_ws = MagicMock()
        mock_run = MagicMock()
        mock_run.state.life_cycle_state.value = "RUNNING"
        mock_run.state.result_state = None
        mock_run.state.state_message = "In run"
        mock_run.run_page_url = "https://databricks.com/run/42"
        mock_ws.jobs.get_run.return_value = mock_run
        launcher._ws = mock_ws

        result = launcher.get_run_status(42)

        assert result["run_id"] == 42
        assert result["state"] == "RUNNING"
        assert result["result_state"] is None
        mock_ws.jobs.get_run.assert_called_once_with(42)

    def test_get_run_status_completed(self):
        from services.job_launcher import JobLauncher, JobLauncherConfig

        config = JobLauncherConfig(etl_job_id=999)
        launcher = JobLauncher(config)

        mock_ws = MagicMock()
        mock_run = MagicMock()
        mock_run.state.life_cycle_state.value = "TERMINATED"
        mock_run.state.result_state.value = "SUCCESS"
        mock_run.state.state_message = ""
        mock_run.run_page_url = "https://databricks.com/run/42"
        mock_ws.jobs.get_run.return_value = mock_run
        launcher._ws = mock_ws

        result = launcher.get_run_status(42)

        assert result["state"] == "TERMINATED"
        assert result["result_state"] == "SUCCESS"
