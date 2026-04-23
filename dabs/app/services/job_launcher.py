"""Job launcher for triggering Spark Perf jobs via Databricks SDK.

Uses WorkspaceClient to call Jobs API (run_now / get_run).
Authentication uses SP credentials auto-detected from Databricks Apps context.
Supports two independent jobs: ETL Pipeline and LLM Summary.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class JobLauncherConfig:
    """Configuration for JobLauncher."""

    etl_job_id: int = 0  # Spark Perf ETL job ID
    summary_job_id: int = 0  # Spark Perf LLM Summary job ID

    @classmethod
    def from_env(cls) -> JobLauncherConfig:
        from core.config_store import get_setting

        etl_id = get_setting("spark_perf_etl_job_id", "0")
        summary_id = get_setting("spark_perf_summary_job_id", "0")
        return cls(
            etl_job_id=int(etl_id) if etl_id.isdigit() else 0,
            summary_job_id=int(summary_id) if summary_id.isdigit() else 0,
        )


class JobLauncher:
    """Launches Databricks Jobs and polls run status.

    Always uses SP (Service Principal) authentication.
    deploy.sh grants CAN_MANAGE_RUN on jobs to the app SP.
    """

    def __init__(self, config: JobLauncherConfig) -> None:
        self._config = config
        self._ws: Any = None

    def _get_client(self) -> Any:
        if self._ws is None:
            from databricks.sdk import WorkspaceClient

            self._ws = WorkspaceClient()
            logger.info("JobLauncher: using SP authentication")
        return self._ws

    def _run_job(self, job_id: int, params: dict) -> dict:
        """Trigger a job and return run info."""
        ws = self._get_client()
        run_resp = ws.jobs.run_now(job_id=job_id, notebook_params=params)
        run_id = run_resp.run_id
        logger.info("Job triggered: job_id=%s, run_id=%s, params=%s", job_id, run_id, params)

        # run_now returns a Wait object without run_page_url; fetch it via get_run
        run_page_url = ""
        try:
            run_detail = ws.jobs.get_run(run_id)
            run_page_url = getattr(run_detail, "run_page_url", "") or ""
        except Exception as e:
            logger.warning("Failed to get run_page_url for run %s: %s", run_id, e)

        return {
            "run_id": run_id,
            "run_page_url": run_page_url,
        }

    def trigger_etl(
        self,
        log_root: str,
        cluster_id: str,
        catalog: str = "",
        schema: str = "",
        table_prefix: str = "",
    ) -> dict:
        """Trigger the ETL pipeline job. Returns {run_id, run_page_url}."""
        if not self._config.etl_job_id:
            raise ValueError("spark_perf_etl_job_id is not configured")

        if not catalog or not schema:
            from core.config_store import get_setting

            catalog = catalog or get_setting("spark_perf_catalog", "main")
            schema = schema or get_setting("spark_perf_schema", "default")

        if not table_prefix:
            from core.config_store import get_setting

            table_prefix = get_setting("spark_perf_table_prefix", "PERF_")

        return self._run_job(
            self._config.etl_job_id,
            {
                "log_root": log_root,
                "cluster_id": cluster_id,
                "catalog": catalog,
                "schema": schema,
                "table_prefix": table_prefix,
            },
        )

    def trigger_summary(
        self,
        app_id: str = "",
        catalog: str = "",
        schema: str = "",
        table_prefix: str = "",
        model_endpoint: str = "",
        output_language: str = "ja",
        experiment_id: str = "",
        variant: str = "",
    ) -> dict:
        """Trigger the LLM summary generation job. Returns {run_id, run_page_url}."""
        if not self._config.summary_job_id:
            raise ValueError("spark_perf_summary_job_id is not configured")

        if not catalog or not schema:
            from core.config_store import get_setting

            catalog = catalog or get_setting("spark_perf_catalog", "main")
            schema = schema or get_setting("spark_perf_schema", "default")

        if not table_prefix:
            from core.config_store import get_setting

            table_prefix = get_setting("spark_perf_table_prefix", "PERF_")

        return self._run_job(
            self._config.summary_job_id,
            {
                "catalog": catalog,
                "schema": schema,
                "table_prefix": table_prefix,
                "app_id": app_id or "ALL",
                "model_endpoint": model_endpoint or "databricks-claude-sonnet-4-5",
                "output_lang": output_language,
                "experiment_id": experiment_id,
                "variant": variant,
            },
        )

    def get_run_status(self, run_id: int) -> dict:
        """Get the lifecycle state of a job run."""
        ws = self._get_client()
        run = ws.jobs.get_run(run_id)
        return {
            "run_id": run_id,
            "state": run.state.life_cycle_state.value,
            "result_state": run.state.result_state.value if run.state.result_state else None,
            "state_message": run.state.state_message or "",
            "run_page_url": run.run_page_url or "",
        }
