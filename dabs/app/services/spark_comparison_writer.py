"""Writer for Spark comparison results to Delta tables.

Persists spark_comparison_pairs and spark_comparison_metrics
tables using the same connection pattern as SparkPerfReader.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.models import ComparisonResult

from .spark_perf_reader import SparkPerfConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL templates (Liquid Clustering)
# ---------------------------------------------------------------------------

_SPARK_COMPARISON_PAIRS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  comparison_id STRING NOT NULL,
  baseline_app_id STRING NOT NULL,
  candidate_app_id STRING NOT NULL,
  cluster_id STRING,
  regression_detected BOOLEAN,
  regression_severity STRING,
  summary STRING,
  compared_at TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (cluster_id, compared_at)
"""

_SPARK_COMPARISON_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  comparison_id STRING NOT NULL,
  metric_name STRING NOT NULL,
  metric_group STRING,
  baseline_value DOUBLE,
  candidate_value DOUBLE,
  percent_diff DOUBLE,
  regression_flag BOOLEAN,
  improvement_flag BOOLEAN,
  severity STRING,
  created_at TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (comparison_id, metric_name)
"""


_SPARK_COMPARE_RESULT_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  comparison_id STRING NOT NULL,
  compared_at TIMESTAMP NOT NULL,
  baseline_analyzed_at TIMESTAMP,
  baseline_app_id STRING,
  baseline_experiment STRING,
  baseline_variant STRING,
  baseline_duration_ms BIGINT,
  baseline_alerts INT,
  candidate_analyzed_at TIMESTAMP,
  candidate_app_id STRING,
  candidate_experiment STRING,
  candidate_variant STRING,
  candidate_duration_ms BIGINT,
  candidate_alerts INT,
  regression_detected BOOLEAN,
  regression_severity STRING,
  net_score DOUBLE,
  report_markdown STRING
)
USING DELTA
CLUSTER BY (compared_at)
"""


class SparkComparisonWriter:
    """Writes Spark comparison results to Delta tables."""

    def __init__(self, config: SparkPerfConfig) -> None:
        self._config = config
        self._tables_ensured: set[str] = set()

    def _fqn(self, table_suffix: str) -> str:
        """Return fully-qualified table name with prefix (validated)."""
        from core.sql_safe import safe_fqn

        table_name = f"{self._config.table_prefix}{table_suffix}"
        return safe_fqn(self._config.catalog, self._config.schema, table_name)

    @staticmethod
    def _strip_host(raw: str) -> str:
        """Remove scheme and trailing slash from a hostname."""
        h = raw
        if h.startswith("https://"):
            h = h[len("https://") :]
        if h.startswith("http://"):
            h = h[len("http://") :]
        return h.rstrip("/")

    def _get_connection(self) -> Any:
        """Create a databricks-sql-connector connection."""
        from databricks import sql as dbsql

        from . import _sdk_credentials_provider

        host = self._strip_host(self._config.databricks_host)

        if self._config.databricks_token:
            logger.info("SQL connector: using PAT token, host=%s", host)
            return dbsql.connect(
                server_hostname=host,
                http_path=self._config.http_path,
                access_token=self._config.databricks_token,
            )

        # Databricks SDK OAuth (Apps / service principal)
        from databricks.sdk.core import Config

        cfg = Config()
        effective_host = host or self._strip_host(cfg.host or "")
        logger.info("SQL connector: using SDK credentials_provider, host=%s", effective_host)

        return dbsql.connect(
            server_hostname=effective_host,
            http_path=self._config.http_path,
            credentials_provider=_sdk_credentials_provider(cfg),
        )

    def _ensure_table(self, cursor: Any, table_suffix: str, ddl_template: str) -> None:
        """Execute CREATE TABLE IF NOT EXISTS (idempotent)."""
        if table_suffix in self._tables_ensured:
            return
        fqn = self._fqn(table_suffix)
        ddl = ddl_template.format(fqn=fqn)
        logger.info("Ensuring table exists: %s", fqn)
        cursor.execute(ddl)
        self._tables_ensured.add(table_suffix)

        # Migrate: add net_score column if missing
        if table_suffix == "spark_compare_result":
            try:
                cursor.execute(f"DESCRIBE TABLE {fqn}")
                existing = {row[0].lower() for row in cursor.fetchall()}
                if "net_score" not in existing:
                    logger.info("Adding column net_score to %s", fqn)
                    cursor.execute(f"ALTER TABLE {fqn} ADD COLUMNS (net_score DOUBLE)")
            except Exception:
                logger.debug("Column migration check skipped for %s", fqn, exc_info=True)

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def write_comparison(self, result: ComparisonResult, cluster_id: str = "") -> str | None:
        """Write a Spark comparison result. Returns comparison_id or None on error."""
        if not self._config.http_path:
            logger.warning("http_path not configured; skipping spark comparison write")
            return None

        now = datetime.now(UTC)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    self._write_pair(cursor, result, cluster_id, now)
                    self._write_metrics(cursor, result, now)

            logger.info("Spark comparison written: comparison_id=%s", result.comparison_id)
            return result.comparison_id
        except Exception:
            logger.exception("Failed to write spark comparison result")
            return None

    def write_compare_result(
        self,
        comparison_id: str,
        baseline: dict,
        candidate: dict,
        regression_detected: bool,
        regression_severity: str,
        report_markdown: str,
        net_score: float = 0.0,
    ) -> str | None:
        """Write a flat Spark comparison result row. Returns comparison_id or None."""
        if not self._config.http_path:
            logger.warning("http_path not configured; skipping spark compare result write")
            return None

        now = datetime.now(UTC)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    table_suffix = "spark_compare_result"
                    self._ensure_table(cursor, table_suffix, _SPARK_COMPARE_RESULT_DDL)
                    fqn = self._fqn(table_suffix)

                    params = {
                        "comparison_id": comparison_id,
                        "compared_at": now,
                        "baseline_analyzed_at": baseline.get("analyzed_at"),
                        "baseline_app_id": baseline.get("app_id"),
                        "baseline_experiment": baseline.get("experiment") or None,
                        "baseline_variant": baseline.get("variant") or None,
                        "baseline_duration_ms": baseline.get("duration_ms"),
                        "baseline_alerts": baseline.get("alerts", 0),
                        "candidate_analyzed_at": candidate.get("analyzed_at"),
                        "candidate_app_id": candidate.get("app_id"),
                        "candidate_experiment": candidate.get("experiment") or None,
                        "candidate_variant": candidate.get("variant") or None,
                        "candidate_duration_ms": candidate.get("duration_ms"),
                        "candidate_alerts": candidate.get("alerts", 0),
                        "regression_detected": regression_detected,
                        "regression_severity": regression_severity,
                        "net_score": net_score,
                        "report_markdown": report_markdown or None,
                    }
                    cols = ", ".join(params.keys())
                    placeholders = ", ".join(f":{k}" for k in params.keys())
                    sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
                    cursor.execute(sql, parameters=params)

            logger.info("Spark compare result written: comparison_id=%s", comparison_id)
            return comparison_id
        except Exception:
            logger.exception("Failed to write spark compare result")
            return None

    # -----------------------------------------------------------------------
    # Per-table writers
    # -----------------------------------------------------------------------

    def _write_pair(
        self,
        cursor: Any,
        result: ComparisonResult,
        cluster_id: str,
        now: datetime,
    ) -> None:
        table_suffix = "spark_comparison_pairs"
        self._ensure_table(cursor, table_suffix, _SPARK_COMPARISON_PAIRS_DDL)

        params = {
            "comparison_id": result.comparison_id,
            "baseline_app_id": result.baseline_analysis_id,
            "candidate_app_id": result.candidate_analysis_id,
            "cluster_id": cluster_id or None,
            "regression_detected": result.regression_detected,
            "regression_severity": result.regression_severity,
            "summary": result.summary or None,
            "compared_at": now,
        }
        cols = ", ".join(params.keys())
        placeholders = ", ".join(f":{k}" for k in params.keys())
        sql = f"INSERT INTO {self._fqn(table_suffix)} ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, parameters=params)

    def _write_metrics(
        self,
        cursor: Any,
        result: ComparisonResult,
        now: datetime,
    ) -> None:
        table_suffix = "spark_comparison_metrics"
        self._ensure_table(cursor, table_suffix, _SPARK_COMPARISON_METRICS_DDL)
        fqn = self._fqn(table_suffix)

        for md in result.metric_diffs:
            percent_diff = (
                md.relative_diff_ratio * 100 if md.relative_diff_ratio is not None else None
            )
            params = {
                "comparison_id": result.comparison_id,
                "metric_name": md.metric_name,
                "metric_group": md.metric_group or None,
                "baseline_value": md.baseline_value,
                "candidate_value": md.candidate_value,
                "percent_diff": percent_diff,
                "regression_flag": md.regression_flag,
                "improvement_flag": md.improvement_flag,
                "severity": md.severity,
                "created_at": now,
            }
            cols = ", ".join(params.keys())
            placeholders = ", ".join(f":{k}" for k in params.keys())
            sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, parameters=params)
