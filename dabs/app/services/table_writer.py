"""
Table writer for persisting ProfileAnalysis results to Databricks managed Delta tables.

Uses databricks-sql-connector to write analysis results via SQL Warehouse.
Tables are auto-created with Liquid Clustering if they don't exist.
"""

from __future__ import annotations

import base64
import gzip
import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from core.models import ComparisonResult, KnowledgeDocument, ProfileAnalysis

logger = logging.getLogger(__name__)


def _compress_json(text: str | None) -> str | None:
    """Gzip-compress and Base64-encode a JSON string to reduce INSERT payload size."""
    if not text:
        return None
    compressed = gzip.compress(text.encode("utf-8"))
    return base64.b64encode(compressed).decode("ascii")


# ---------------------------------------------------------------------------
# DDL templates (Liquid Clustering)
# ---------------------------------------------------------------------------

_HEADER_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  analysis_id STRING NOT NULL,
  analyzed_at TIMESTAMP NOT NULL,
  query_id STRING,
  query_status STRING,
  query_text STRING,

  -- v3: comparison / tracking context
  query_text_normalized STRING,
  query_fingerprint STRING,
  query_fingerprint_version STRING,
  experiment_id STRING,
  variant STRING,
  variant_group STRING,
  baseline_flag BOOLEAN,
  tags_json STRING,
  source_run_id STRING,
  source_job_id STRING,
  source_job_run_id STRING,
  analysis_notes STRING,
  metric_direction_version STRING,
  knowledge_version STRING,

  -- v3: Query family grouping
  query_family_id STRING,
  purpose_signature STRING,
  variant_type STRING,
  feature_json STRING,

  total_time_ms BIGINT,
  compilation_time_ms BIGINT,
  execution_time_ms BIGINT,
  photon_total_time_ms BIGINT,
  task_total_time_ms BIGINT,

  read_bytes BIGINT,
  read_remote_bytes BIGINT,
  read_cache_bytes BIGINT,
  spill_to_disk_bytes BIGINT,
  pruned_bytes BIGINT,
  rows_read_count BIGINT,
  rows_produced_count BIGINT,
  read_files_count BIGINT,
  pruned_files_count BIGINT,
  read_partitions_count BIGINT,
  bytes_read_from_cache_percentage INT,
  write_remote_bytes BIGINT,
  write_remote_files BIGINT,
  network_sent_bytes BIGINT,

  cache_hit_ratio DOUBLE,
  remote_read_ratio DOUBLE,
  photon_ratio DOUBLE,
  spill_bytes BIGINT,
  filter_rate DOUBLE,
  bytes_pruning_ratio DOUBLE,
  shuffle_impact_ratio DOUBLE,
  cloud_storage_retry_ratio DOUBLE,
  has_data_skew BOOLEAN,
  skewed_partitions INT,
  rescheduled_scan_ratio DOUBLE,
  oom_fallback_count INT,

  statement_type STRING,
  join_count INT,
  subquery_count INT,
  cte_count INT,
  complexity_score INT,
  has_distinct BOOLEAN,
  has_group_by BOOLEAN,
  has_order_by BOOLEAN,
  has_limit BOOLEAN,
  has_union BOOLEAN,

  critical_alert_count INT,
  high_alert_count INT,
  medium_alert_count INT,
  info_alert_count INT,
  action_card_count INT,
  hot_operator_count INT,
  stage_count INT,
  scanned_table_count INT,

  endpoint_id STRING,
  warehouse_id STRING,
  warehouse_name STRING,
  warehouse_size STRING,
  warehouse_type STRING,
  warehouse_is_serverless BOOLEAN,
  warehouse_state STRING,

  estimated_cost_usd DOUBLE,

  prompt_version STRING,
  report_markdown STRING,
  extra_metrics_json STRING,
  lang STRING,
  -- True when the user attached EXPLAIN text at analysis time. Used by the
  -- history UI to show whether EXPLAIN-derived insights are available.
  has_explain BOOLEAN,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (query_fingerprint, experiment_id, analyzed_at)
"""

_ACTIONS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  analysis_id STRING NOT NULL,
  query_id STRING,
  analyzed_at TIMESTAMP NOT NULL,
  action_rank INT,
  problem STRING,
  evidence STRING,
  likely_cause STRING,
  fix STRING,
  fix_sql STRING,
  expected_impact STRING,
  effort STRING,
  priority_score DOUBLE,
  validation_metric STRING,
  risk STRING,
  risk_reason STRING,
  verification_steps_json STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (analysis_id, action_rank)
"""

_TABLE_SCANS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  analysis_id STRING NOT NULL,
  query_id STRING,
  analyzed_at TIMESTAMP NOT NULL,
  table_name STRING,
  bytes_read BIGINT,
  bytes_pruned BIGINT,
  files_read BIGINT,
  files_pruned BIGINT,
  rows_scanned BIGINT,
  file_pruning_rate DOUBLE,
  bytes_pruning_rate DOUBLE,
  current_clustering_keys STRING,
  recommended_clustering_keys STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (analysis_id, table_name)
"""

_HOT_OPERATORS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  analysis_id STRING NOT NULL,
  query_id STRING,
  analyzed_at TIMESTAMP NOT NULL,
  operator_rank INT,
  node_id STRING,
  node_name STRING,
  duration_ms BIGINT,
  time_share_percent DOUBLE,
  rows_in BIGINT,
  rows_out BIGINT,
  spill_bytes BIGINT,
  peak_memory_bytes BIGINT,
  is_photon BOOLEAN,
  bottleneck_type STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (analysis_id, bottleneck_type, operator_rank)
"""

_STAGES_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  analysis_id STRING NOT NULL,
  query_id STRING,
  analyzed_at TIMESTAMP NOT NULL,
  stage_id STRING,
  status STRING,
  duration_ms BIGINT,
  num_tasks INT,
  num_complete_tasks INT,
  num_failed_tasks INT,
  num_killed_tasks INT,
  note STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (analysis_id, stage_id)
"""

_RAW_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  analysis_id STRING NOT NULL,
  query_id STRING,
  analyzed_at TIMESTAMP NOT NULL,
  profile_json STRING,
  analysis_json STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (analysis_id, analyzed_at)
"""

# ---------------------------------------------------------------------------
# v3: Comparison / Knowledge / Metric Direction DDLs
# ---------------------------------------------------------------------------

_COMPARISON_PAIRS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  comparison_id STRING NOT NULL,
  pair_status STRING,
  pair_type STRING,
  baseline_analysis_id STRING NOT NULL,
  candidate_analysis_id STRING NOT NULL,
  query_fingerprint STRING,
  experiment_id STRING,
  baseline_variant STRING,
  candidate_variant STRING,
  baseline_analyzed_at TIMESTAMP,
  candidate_analyzed_at TIMESTAMP,
  comparison_scope STRING,
  comparison_reason STRING,
  requested_by STRING,
  request_source STRING,
  tags_json STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (query_fingerprint, experiment_id, created_at)
"""

_COMPARISON_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  comparison_id STRING NOT NULL,
  baseline_analysis_id STRING NOT NULL,
  candidate_analysis_id STRING NOT NULL,
  query_fingerprint STRING,
  experiment_id STRING,
  baseline_variant STRING,
  candidate_variant STRING,
  metric_name STRING NOT NULL,
  metric_group STRING,
  direction_when_increase STRING,
  baseline_value DOUBLE,
  candidate_value DOUBLE,
  absolute_diff DOUBLE,
  relative_diff_ratio DOUBLE,
  percent_diff DOUBLE,
  changed_flag BOOLEAN,
  improvement_flag BOOLEAN,
  regression_flag BOOLEAN,
  severity STRING,
  summary_text STRING,
  created_at TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (query_fingerprint, metric_name, created_at)
"""

_KNOWLEDGE_DOCUMENTS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  document_id STRING NOT NULL,
  knowledge_type STRING,
  source_type STRING,
  source_analysis_id STRING,
  source_comparison_id STRING,
  query_fingerprint STRING,
  experiment_id STRING,
  variant STRING,
  title STRING NOT NULL,
  summary STRING,
  body_markdown STRING,
  problem_category STRING,
  root_cause STRING,
  recommendation STRING,
  expected_impact STRING,
  confidence_score DOUBLE,
  applicability_scope STRING,
  status STRING,
  tags_json STRING,
  created_by STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (knowledge_type, problem_category, created_at)
"""

_KNOWLEDGE_TAGS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  document_id STRING NOT NULL,
  tag_name STRING NOT NULL,
  tag_value STRING,
  created_at TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (tag_name, tag_value, created_at)
"""

_METRIC_DIRECTIONS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  metric_name STRING NOT NULL,
  metric_group STRING,
  display_name STRING,
  unit STRING,
  increase_effect STRING NOT NULL,
  decrease_effect STRING NOT NULL,
  preferred_trend STRING,
  regression_threshold_ratio DOUBLE,
  improvement_threshold_ratio DOUBLE,
  notes STRING,
  active_flag BOOLEAN,
  version STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (metric_group, metric_name)
"""

_COMPARE_RESULT_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  comparison_id STRING NOT NULL,
  compared_at TIMESTAMP NOT NULL,
  baseline_analyzed_at TIMESTAMP,
  baseline_query_id STRING,
  baseline_experiment STRING,
  baseline_variant STRING,
  baseline_duration_ms BIGINT,
  baseline_alerts INT,
  candidate_analyzed_at TIMESTAMP,
  candidate_query_id STRING,
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

# L5 (2026-04-26): user feedback (欠落申告) — Codex (d) recommended Delta
# over JSONL for analysis_id join + Genie usability. analysis_id is FK
# to profiler_analysis_header (loose; we don't enforce). target_type /
# sentiment / category are intentionally STRING with a small enum
# checked by the route, not a Delta CHECK constraint, so the schema
# stays flexible as new feedback kinds are added.
_FEEDBACK_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  feedback_id STRING,
  analysis_id STRING,
  target_type STRING,
  target_id STRING,
  sentiment STRING,
  category STRING,
  free_text STRING,
  user_email STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (created_at)
"""

# L5 Phase 1.5 (2026-04-26): bulk export audit log. Codex (a) requirement
# — every workspace_admin export gets a row so we can investigate later.
# user_email_hash mirrors the bundle file's hashing scheme so per-user
# attribution survives even after the email column policy changes.
_FEEDBACK_EXPORT_LOG_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  export_id STRING,
  exported_at TIMESTAMP,
  workspace_slug STRING,
  user_email_hash STRING,
  user_email_domain STRING,
  scope STRING,
  since_ts TIMESTAMP,
  until_ts TIMESTAMP,
  feedback_count BIGINT,
  bundle_count BIGINT,
  size_bytes BIGINT,
  profile_included BOOLEAN,
  success BOOLEAN,
  error_reason STRING
)
USING DELTA
CLUSTER BY (exported_at)
"""

# Phase 3 (v6.7.5) of docs/v6/query-rewrite-extraction.md: append-only
# log of every rewrite attempt. ``source_sql_hash`` (sha256 of the
# normalised source SQL) is the grouping key for multi-model compare;
# ``parent_id`` walks the refine chain. ``source_sql_hash_version``
# (Codex Q1) lets us evolve the normalisation contract later — a v2
# scheme can be backfilled side-by-side without invalidating existing
# group/compare queries that pinned to v1. validation_* are nullable
# because the user may copy a rewrite without ever running validate.
# ``output_format`` (v6.7.9, Codex follow-up) discriminates executable
# SQL from diff patches so downstream consumers (history, validate,
# refine) don't treat a patch as runnable SQL.
_REWRITE_ARTIFACTS_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
  artifact_id STRING,
  analysis_id STRING,
  source_sql STRING,
  source_sql_hash STRING,
  source_sql_hash_version STRING,
  rewritten_sql STRING,
  output_format STRING,
  model STRING,
  feedback STRING,
  parent_id STRING,
  validation_method STRING,
  validation_passed BOOLEAN,
  validation_error STRING,
  user_email STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (analysis_id, created_at)
"""

# Table name → DDL template mapping
_TABLE_DDLS: dict[str, str] = {
    "profiler_analysis_header": _HEADER_DDL,
    "profiler_analysis_actions": _ACTIONS_DDL,
    "profiler_analysis_table_scans": _TABLE_SCANS_DDL,
    "profiler_analysis_hot_operators": _HOT_OPERATORS_DDL,
    "profiler_analysis_stages": _STAGES_DDL,
    "profiler_analysis_raw": _RAW_DDL,
    "profiler_comparison_pairs": _COMPARISON_PAIRS_DDL,
    "profiler_comparison_metrics": _COMPARISON_METRICS_DDL,
    "profiler_knowledge_documents": _KNOWLEDGE_DOCUMENTS_DDL,
    "profiler_knowledge_tags": _KNOWLEDGE_TAGS_DDL,
    "profiler_metric_directions": _METRIC_DIRECTIONS_DDL,
    "profiler_compare_result": _COMPARE_RESULT_DDL,
    "profiler_feedback": _FEEDBACK_DDL,
    "profiler_feedback_export_log": _FEEDBACK_EXPORT_LOG_DDL,
    "profiler_rewrite_artifacts": _REWRITE_ARTIFACTS_DDL,
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class TableWriterConfig:
    """Configuration for the table writer."""

    catalog: str
    schema: str
    databricks_host: str
    databricks_token: str
    http_path: str
    enabled: bool = True

    @classmethod
    def from_env(cls) -> TableWriterConfig:
        """Load configuration from config file + environment variables.

        Priority: env vars > config file > defaults.
        """
        from core.config_store import get_setting

        enabled_str = get_setting("table_write_enabled", "false")
        return cls(
            catalog=get_setting("catalog", "main"),
            schema=get_setting("schema", "profiler"),
            databricks_host=os.environ.get("DATABRICKS_HOST", ""),
            databricks_token=os.environ.get("DATABRICKS_TOKEN", ""),
            http_path=get_setting("http_path", ""),
            enabled=enabled_str.lower() in ("true", "1", "yes"),
        )


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class TableWriter:
    """Writes ProfileAnalysis results to Databricks managed Delta tables."""

    def __init__(self, config: TableWriterConfig) -> None:
        self._config = config
        self._tables_ensured: set[str] = set()

    def _fqn(self, table_name: str) -> str:
        """Return fully-qualified three-part table name (validated)."""
        from core.sql_safe import safe_fqn

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
        """Create a databricks-sql-connector connection.

        Supports two authentication modes:
        1. PAT token (databricks_token is non-empty) — used by CLI / local dev
        2. Databricks SDK credentials_provider — used by Databricks Apps
           (auto-detects service principal via DATABRICKS_CLIENT_ID/SECRET)
        """
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

    def _ensure_table(self, cursor: Any, table_name: str) -> None:
        """Execute CREATE TABLE IF NOT EXISTS (idempotent)."""
        if table_name in self._tables_ensured:
            return
        ddl_template = _TABLE_DDLS.get(table_name)
        if ddl_template is None:
            logger.warning("Unknown table: %s", table_name)
            return
        fqn = self._fqn(table_name)
        ddl = ddl_template.format(fqn=fqn)
        logger.info("Ensuring table exists: %s", fqn)
        cursor.execute(ddl)
        self._tables_ensured.add(table_name)

        # Migrate existing tables: add new columns if missing
        if table_name == "profiler_analysis_actions":
            self._migrate_actions_columns(cursor, fqn)
        if table_name == "profiler_analysis_header":
            self._migrate_header_columns(cursor, fqn)
        if table_name == "profiler_compare_result":
            self._migrate_compare_result_columns(cursor, fqn)

    def _migrate_header_columns(self, cursor: Any, fqn: str) -> None:
        """Add new columns to existing header table if missing."""
        new_columns = {
            "prompt_version": "STRING",
            "lang": "STRING",
            "estimated_cost_usd": "DOUBLE",
            "has_explain": "BOOLEAN",
        }
        try:
            cursor.execute(f"DESCRIBE TABLE {fqn}")
            existing = {row[0].lower() for row in cursor.fetchall()}
            for col_name, col_type in new_columns.items():
                if col_name.lower() not in existing:
                    logger.info("Adding column %s to %s", col_name, fqn)
                    cursor.execute(f"ALTER TABLE {fqn} ADD COLUMNS ({col_name} {col_type})")
        except Exception:
            logger.debug("Column migration check skipped for %s", fqn, exc_info=True)

    def _migrate_actions_columns(self, cursor: Any, fqn: str) -> None:
        """Add new columns to existing actions table if missing."""
        new_columns = {
            "risk": "STRING",
            "risk_reason": "STRING",
            "verification_steps_json": "STRING",
        }
        try:
            cursor.execute(f"DESCRIBE TABLE {fqn}")
            existing = {row[0].lower() for row in cursor.fetchall()}
            for col_name, col_type in new_columns.items():
                if col_name.lower() not in existing:
                    logger.info("Adding column %s to %s", col_name, fqn)
                    cursor.execute(f"ALTER TABLE {fqn} ADD COLUMNS ({col_name} {col_type})")
        except Exception:
            logger.debug("Column migration check skipped for %s", fqn, exc_info=True)

    def _migrate_compare_result_columns(self, cursor: Any, fqn: str) -> None:
        """Add net_score column to existing compare_result table if missing."""
        new_columns = {"net_score": "DOUBLE"}
        try:
            cursor.execute(f"DESCRIBE TABLE {fqn}")
            existing = {row[0].lower() for row in cursor.fetchall()}
            for col_name, col_type in new_columns.items():
                if col_name.lower() not in existing:
                    logger.info("Adding column %s to %s", col_name, fqn)
                    cursor.execute(f"ALTER TABLE {fqn} ADD COLUMNS ({col_name} {col_type})")
        except Exception:
            logger.debug("Column migration check skipped for %s", fqn, exc_info=True)

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def write(
        self,
        analysis: ProfileAnalysis,
        report: str = "",
        raw_profile_json: str = "",
        lang: str = "",
    ) -> str | None:
        """Write analysis results to all tables. Returns analysis_id or None on skip."""
        if not self._config.enabled:
            logger.debug("Table write disabled; skipping")
            return None

        if not self._config.http_path:
            logger.warning("PROFILER_WAREHOUSE_HTTP_PATH not set; skipping table write")
            return None

        analysis_id = str(uuid.uuid4())
        analyzed_at = datetime.now(UTC)
        query_id = analysis.query_metrics.query_id

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    self._write_header(
                        cursor,
                        analysis_id,
                        analyzed_at,
                        query_id,
                        analysis,
                        report,
                        lang=lang,
                    )
                    self._write_actions(
                        cursor,
                        analysis_id,
                        analyzed_at,
                        query_id,
                        analysis,
                    )
                    self._write_table_scans(
                        cursor,
                        analysis_id,
                        analyzed_at,
                        query_id,
                        analysis,
                    )
                    self._write_hot_operators(
                        cursor,
                        analysis_id,
                        analyzed_at,
                        query_id,
                        analysis,
                    )
                    self._write_stages(
                        cursor,
                        analysis_id,
                        analyzed_at,
                        query_id,
                        analysis,
                    )
                    try:
                        self._write_raw(
                            cursor,
                            analysis_id,
                            analyzed_at,
                            query_id,
                            analysis,
                            raw_profile_json,
                        )
                    except Exception:
                        logger.warning(
                            "Raw table write failed (payload too large?), "
                            "analysis_id=%s — other tables written OK",
                            analysis_id,
                        )

            logger.info(
                "Analysis written to tables: analysis_id=%s, query_id=%s",
                analysis_id,
                query_id,
            )
            return analysis_id

        except Exception as e:
            logger.exception("Failed to write analysis to tables: %s", e)
            return None

    # -----------------------------------------------------------------------
    # Per-table writers
    # -----------------------------------------------------------------------

    @staticmethod
    def _compute_cost_usd(analysis: ProfileAnalysis) -> float | None:
        """Compute estimated cost using the same logic as the report."""
        try:
            from core.dbsql_cost import estimate_query_cost

            cost = estimate_query_cost(analysis.query_metrics, analysis.warehouse_info)
            return cost.estimated_cost_usd if cost else None
        except Exception:
            return None

    def _write_header(
        self,
        cursor: Any,
        analysis_id: str,
        analyzed_at: datetime,
        query_id: str,
        analysis: ProfileAnalysis,
        report: str,
        lang: str = "",
    ) -> None:
        table_name = "profiler_analysis_header"
        self._ensure_table(cursor, table_name)

        qm = analysis.query_metrics
        bi = analysis.bottleneck_indicators
        sa = analysis.sql_analysis
        st = sa.structure if sa else None
        wi = analysis.warehouse_info

        from core.constants import Severity

        alert_counts = {
            Severity.CRITICAL: 0,
            Severity.HIGH: 0,
            Severity.MEDIUM: 0,
            Severity.INFO: 0,
        }
        for alert in bi.alerts:
            if alert.severity in alert_counts:
                alert_counts[alert.severity] += 1

        extra_json = json.dumps(qm.extra_metrics, ensure_ascii=False) if qm.extra_metrics else None

        ctx = analysis.analysis_context
        tags_json = json.dumps(ctx.tags, ensure_ascii=False) if ctx.tags else None

        params = {
            "analysis_id": analysis_id,
            "analyzed_at": analyzed_at,
            "query_id": query_id,
            "query_status": qm.status,
            "query_text": qm.query_text,
            # v3: comparison / tracking context
            "query_text_normalized": ctx.query_text_normalized or None,
            "query_fingerprint": ctx.query_fingerprint or None,
            "query_fingerprint_version": ctx.query_fingerprint_version or None,
            "experiment_id": ctx.experiment_id or None,
            "variant": ctx.variant or None,
            "variant_group": ctx.variant_group or None,
            "baseline_flag": ctx.baseline_flag,
            "tags_json": tags_json,
            "source_run_id": ctx.source_run_id or None,
            "source_job_id": ctx.source_job_id or None,
            "source_job_run_id": ctx.source_job_run_id or None,
            "analysis_notes": ctx.analysis_notes or None,
            "metric_direction_version": "v1",
            "knowledge_version": None,
            "prompt_version": ctx.prompt_version or None,
            # v3: Query family
            "query_family_id": ctx.query_family_id or None,
            "purpose_signature": ctx.purpose_signature or None,
            "variant_type": ctx.variant_type or None,
            "feature_json": ctx.feature_json or None,
            "total_time_ms": qm.total_time_ms,
            "compilation_time_ms": qm.compilation_time_ms,
            "execution_time_ms": qm.execution_time_ms,
            "photon_total_time_ms": qm.photon_total_time_ms,
            "task_total_time_ms": qm.task_total_time_ms,
            "read_bytes": qm.read_bytes,
            "read_remote_bytes": qm.read_remote_bytes,
            "read_cache_bytes": qm.read_cache_bytes,
            "spill_to_disk_bytes": qm.spill_to_disk_bytes,
            "pruned_bytes": qm.pruned_bytes,
            "rows_read_count": qm.rows_read_count,
            "rows_produced_count": qm.rows_produced_count,
            "read_files_count": qm.read_files_count,
            "pruned_files_count": qm.pruned_files_count,
            "read_partitions_count": qm.read_partitions_count,
            "bytes_read_from_cache_percentage": qm.bytes_read_from_cache_percentage,
            "write_remote_bytes": qm.write_remote_bytes,
            "write_remote_files": qm.write_remote_files,
            "network_sent_bytes": qm.network_sent_bytes,
            "cache_hit_ratio": bi.cache_hit_ratio,
            "remote_read_ratio": bi.remote_read_ratio,
            "photon_ratio": bi.photon_ratio,
            "spill_bytes": bi.spill_bytes,
            "filter_rate": bi.filter_rate,
            "bytes_pruning_ratio": bi.bytes_pruning_ratio,
            "shuffle_impact_ratio": bi.shuffle_impact_ratio,
            "cloud_storage_retry_ratio": bi.cloud_storage_retry_ratio,
            "has_data_skew": bi.has_data_skew,
            "skewed_partitions": bi.skewed_partitions,
            "rescheduled_scan_ratio": bi.rescheduled_scan_ratio,
            "oom_fallback_count": bi.oom_fallback_count,
            "statement_type": st.statement_type if st else None,
            "join_count": st.join_count if st else None,
            "subquery_count": st.subquery_count if st else None,
            "cte_count": st.cte_count if st else None,
            "complexity_score": st.complexity_score if st else None,
            "has_distinct": st.has_distinct if st else None,
            "has_group_by": st.has_group_by if st else None,
            "has_order_by": st.has_order_by if st else None,
            "has_limit": st.has_limit if st else None,
            "has_union": st.has_union if st else None,
            "critical_alert_count": alert_counts[Severity.CRITICAL],
            "high_alert_count": alert_counts[Severity.HIGH],
            "medium_alert_count": alert_counts[Severity.MEDIUM],
            "info_alert_count": alert_counts[Severity.INFO],
            "action_card_count": len(analysis.action_cards),
            "hot_operator_count": len(analysis.hot_operators),
            "stage_count": len(analysis.stage_info),
            "scanned_table_count": len(analysis.top_scanned_tables),
            "endpoint_id": analysis.endpoint_id,
            "warehouse_id": wi.warehouse_id if wi else None,
            "warehouse_name": wi.name if wi else None,
            "warehouse_size": wi.cluster_size if wi else None,
            "warehouse_type": wi.warehouse_type if wi else None,
            "warehouse_is_serverless": wi.is_serverless if wi else None,
            "warehouse_state": wi.state if wi else None,
            "estimated_cost_usd": self._compute_cost_usd(analysis),
            "report_markdown": report or None,
            "extra_metrics_json": extra_json,
            "lang": lang or None,
            "has_explain": bool(analysis.explain_analysis and analysis.explain_analysis.sections),
            "created_at": analyzed_at,
        }

        cols = ", ".join(params.keys())
        placeholders = ", ".join(f":{k}" for k in params.keys())
        sql = f"INSERT INTO {self._fqn(table_name)} ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, parameters=params)

    def _write_actions(
        self,
        cursor: Any,
        analysis_id: str,
        analyzed_at: datetime,
        query_id: str,
        analysis: ProfileAnalysis,
    ) -> None:
        if not analysis.action_cards:
            return
        table_name = "profiler_analysis_actions"
        self._ensure_table(cursor, table_name)
        fqn = self._fqn(table_name)

        for idx, card in enumerate(analysis.action_cards, 1):
            params = {
                "analysis_id": analysis_id,
                "query_id": query_id,
                "analyzed_at": analyzed_at,
                "action_rank": idx,
                "problem": card.problem,
                "evidence": json.dumps(card.evidence, ensure_ascii=False),
                "likely_cause": card.likely_cause,
                "fix": card.fix,
                "fix_sql": card.fix_sql or None,
                "expected_impact": card.expected_impact,
                "effort": card.effort,
                "priority_score": card.priority_score,
                "validation_metric": card.validation_metric,
                "risk": card.risk or None,
                "risk_reason": card.risk_reason or None,
                "verification_steps_json": json.dumps(card.verification_steps, ensure_ascii=False)
                if card.verification_steps
                else None,
                "created_at": analyzed_at,
            }
            cols = ", ".join(params.keys())
            placeholders = ", ".join(f":{k}" for k in params.keys())
            sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, parameters=params)

    def _write_table_scans(
        self,
        cursor: Any,
        analysis_id: str,
        analyzed_at: datetime,
        query_id: str,
        analysis: ProfileAnalysis,
    ) -> None:
        if not analysis.top_scanned_tables:
            return
        table_name = "profiler_analysis_table_scans"
        self._ensure_table(cursor, table_name)
        fqn = self._fqn(table_name)

        for ts in analysis.top_scanned_tables:
            params = {
                "analysis_id": analysis_id,
                "query_id": query_id,
                "analyzed_at": analyzed_at,
                "table_name": ts.table_name,
                "bytes_read": ts.bytes_read,
                "bytes_pruned": ts.bytes_pruned,
                "files_read": ts.files_read,
                "files_pruned": ts.files_pruned,
                "rows_scanned": ts.rows_scanned,
                "file_pruning_rate": ts.file_pruning_rate,
                "bytes_pruning_rate": ts.bytes_pruning_rate,
                "current_clustering_keys": json.dumps(
                    ts.current_clustering_keys, ensure_ascii=False
                ),
                "recommended_clustering_keys": json.dumps(
                    ts.recommended_clustering_keys, ensure_ascii=False
                ),
                "created_at": analyzed_at,
            }
            cols = ", ".join(params.keys())
            placeholders = ", ".join(f":{k}" for k in params.keys())
            sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, parameters=params)

    def _write_hot_operators(
        self,
        cursor: Any,
        analysis_id: str,
        analyzed_at: datetime,
        query_id: str,
        analysis: ProfileAnalysis,
    ) -> None:
        if not analysis.hot_operators:
            return
        table_name = "profiler_analysis_hot_operators"
        self._ensure_table(cursor, table_name)
        fqn = self._fqn(table_name)

        for op in analysis.hot_operators:
            params = {
                "analysis_id": analysis_id,
                "query_id": query_id,
                "analyzed_at": analyzed_at,
                "operator_rank": op.rank,
                "node_id": op.node_id,
                "node_name": op.node_name,
                "duration_ms": op.duration_ms,
                "time_share_percent": op.time_share_percent,
                "rows_in": op.rows_in,
                "rows_out": op.rows_out,
                "spill_bytes": op.spill_bytes,
                "peak_memory_bytes": op.peak_memory_bytes,
                "is_photon": op.is_photon,
                "bottleneck_type": op.bottleneck_type,
                "created_at": analyzed_at,
            }
            cols = ", ".join(params.keys())
            placeholders = ", ".join(f":{k}" for k in params.keys())
            sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, parameters=params)

    def _write_stages(
        self,
        cursor: Any,
        analysis_id: str,
        analyzed_at: datetime,
        query_id: str,
        analysis: ProfileAnalysis,
    ) -> None:
        if not analysis.stage_info:
            return
        table_name = "profiler_analysis_stages"
        self._ensure_table(cursor, table_name)
        fqn = self._fqn(table_name)

        for stage in analysis.stage_info:
            params = {
                "analysis_id": analysis_id,
                "query_id": query_id,
                "analyzed_at": analyzed_at,
                "stage_id": stage.stage_id,
                "status": stage.status,
                "duration_ms": stage.duration_ms,
                "num_tasks": stage.num_tasks,
                "num_complete_tasks": stage.num_complete_tasks,
                "num_failed_tasks": stage.num_failed_tasks,
                "num_killed_tasks": stage.num_killed_tasks,
                "note": stage.note,
                "created_at": analyzed_at,
            }
            cols = ", ".join(params.keys())
            placeholders = ", ".join(f":{k}" for k in params.keys())
            sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, parameters=params)

    def _write_raw(
        self,
        cursor: Any,
        analysis_id: str,
        analyzed_at: datetime,
        query_id: str,
        analysis: ProfileAnalysis,
        raw_profile_json: str,
    ) -> None:
        table_name = "profiler_analysis_raw"
        self._ensure_table(cursor, table_name)
        fqn = self._fqn(table_name)

        try:
            analysis_json = json.dumps(asdict(analysis), ensure_ascii=False, default=str)
        except Exception:
            logger.warning("Failed to serialize ProfileAnalysis to JSON")
            analysis_json = None

        params = {
            "analysis_id": analysis_id,
            "query_id": query_id,
            "analyzed_at": analyzed_at,
            "profile_json": _compress_json(raw_profile_json),
            "analysis_json": _compress_json(analysis_json),
            "created_at": analyzed_at,
        }
        cols = ", ".join(params.keys())
        placeholders = ", ".join(f":{k}" for k in params.keys())
        sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, parameters=params)

    # -----------------------------------------------------------------------
    # v3: Comparison / Knowledge writers
    # -----------------------------------------------------------------------

    def write_comparison_result(self, result: ComparisonResult) -> str | None:
        """Write a comparison result (pair + per-metric diffs). Returns comparison_id."""
        if not self._config.enabled or not self._config.http_path:
            return None

        now = datetime.now(UTC)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    self._write_comparison_pair(cursor, result, now)
                    self._write_comparison_metrics(cursor, result, now)

            logger.info("Comparison written: comparison_id=%s", result.comparison_id)
            return result.comparison_id
        except Exception:
            logger.exception("Failed to write comparison result")
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
        """Write a flat comparison result row. Returns comparison_id or None."""
        if not self._config.enabled or not self._config.http_path:
            return None

        now = datetime.now(UTC)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = "profiler_compare_result"
                    self._ensure_table(cursor, table_name)
                    fqn = self._fqn(table_name)

                    params = {
                        "comparison_id": comparison_id,
                        "compared_at": now,
                        "baseline_analyzed_at": baseline.get("analyzed_at"),
                        "baseline_query_id": baseline.get("query_id"),
                        "baseline_experiment": baseline.get("experiment") or None,
                        "baseline_variant": baseline.get("variant") or None,
                        "baseline_duration_ms": baseline.get("duration_ms"),
                        "baseline_alerts": baseline.get("alerts", 0),
                        "candidate_analyzed_at": candidate.get("analyzed_at"),
                        "candidate_query_id": candidate.get("query_id"),
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

            logger.info("Compare result written: comparison_id=%s", comparison_id)
            return comparison_id
        except Exception:
            logger.exception("Failed to write compare result")
            return None

    def write_feedback(
        self,
        *,
        analysis_id: str | None,
        target_type: str,
        sentiment: str,
        category: str,
        free_text: str,
        user_email: str,
        target_id: str | None = None,
    ) -> str | None:
        """Persist a user feedback record (L5, 2026-04-26).

        Returns the new feedback_id on success, ``None`` when writes are
        disabled / unavailable. ``user_email`` MUST come from the trusted
        request header (see ``services.user_context``); the route layer
        is responsible for not passing client-controlled values.
        """
        if not self._config.enabled or not self._config.http_path:
            return None

        feedback_id = str(uuid.uuid4())
        now = datetime.now(UTC)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = "profiler_feedback"
                    self._ensure_table(cursor, table_name)
                    params = {
                        "feedback_id": feedback_id,
                        "analysis_id": analysis_id or None,
                        "target_type": target_type,
                        "target_id": target_id or None,
                        "sentiment": sentiment,
                        "category": category,
                        "free_text": free_text or None,
                        "user_email": user_email or None,
                        "created_at": now,
                    }
                    cols = ", ".join(params.keys())
                    placeholders = ", ".join(f":{k}" for k in params.keys())
                    fqn = self._fqn(table_name)
                    sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
                    cursor.execute(sql, parameters=params)

            logger.info(
                "Feedback written: id=%s analysis_id=%s category=%s",
                feedback_id, analysis_id or "(none)", category,
            )
            return feedback_id
        except Exception:
            logger.exception("Failed to write feedback")
            return None

    def write_feedback_export_audit(
        self,
        *,
        export_id: str,
        workspace_slug: str,
        user_email_hash: str | None,
        user_email_domain: str | None,
        scope: str,                 # "per_analysis" | "bulk"
        since_ts: datetime | None,
        until_ts: datetime | None,
        feedback_count: int,
        bundle_count: int,
        size_bytes: int,
        profile_included: bool,
        success: bool,
        error_reason: str = "",
    ) -> bool:
        """Append one row to ``profiler_feedback_export_log`` (Codex (a)
        requirement: every bulk export gets an audit row).

        Returns True when the write succeeded. Failure is non-fatal —
        the caller should still surface the ZIP, audit failures are
        logged as warnings."""
        if not self._config.enabled or not self._config.http_path:
            return False
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = "profiler_feedback_export_log"
                    self._ensure_table(cursor, table_name)
                    params = {
                        "export_id": export_id,
                        "exported_at": datetime.now(UTC),
                        "workspace_slug": workspace_slug or None,
                        "user_email_hash": user_email_hash or None,
                        "user_email_domain": user_email_domain or None,
                        "scope": scope,
                        "since_ts": since_ts,
                        "until_ts": until_ts,
                        "feedback_count": int(feedback_count),
                        "bundle_count": int(bundle_count),
                        "size_bytes": int(size_bytes),
                        "profile_included": bool(profile_included),
                        "success": bool(success),
                        "error_reason": (error_reason or "")[:500] or None,
                    }
                    cols = ", ".join(params.keys())
                    placeholders = ", ".join(f":{k}" for k in params.keys())
                    fqn = self._fqn(table_name)
                    sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
                    cursor.execute(sql, parameters=params)
            return True
        except Exception:
            logger.exception("Failed to write feedback export audit row")
            return False

    def write_rewrite_artifact(
        self,
        *,
        artifact_id: str,
        analysis_id: str,
        source_sql: str,
        source_sql_hash: str,
        source_sql_hash_version: str = "v1",
        rewritten_sql: str,
        output_format: str = "full",
        model: str,
        feedback: str | None = None,
        parent_id: str | None = None,
        validation_method: str | None = None,
        validation_passed: bool | None = None,
        validation_error: str | None = None,
        user_email: str | None = None,
    ) -> bool:
        """Append one row to ``profiler_rewrite_artifacts`` (Phase 3 of
        ``docs/v6/query-rewrite-extraction.md``).

        Append-only — every rewrite attempt persists. Multi-model
        compare is a query joining rows that share ``source_sql_hash``
        and have ``parent_id IS NULL``. Refine chain is a tree walk
        on ``parent_id``.

        Returns True on success, False on failure / writes-disabled.
        Failure is non-fatal — the rewrite is still surfaced to the
        user; persistence drift only affects history / compare.
        """
        if not self._config.enabled or not self._config.http_path:
            return False
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = "profiler_rewrite_artifacts"
                    self._ensure_table(cursor, table_name)
                    params = {
                        "artifact_id": artifact_id,
                        "analysis_id": analysis_id,
                        "source_sql": source_sql or None,
                        "source_sql_hash": source_sql_hash,
                        "source_sql_hash_version": source_sql_hash_version,
                        "rewritten_sql": rewritten_sql or None,
                        "output_format": output_format,
                        "model": model or None,
                        "feedback": feedback or None,
                        "parent_id": parent_id or None,
                        "validation_method": validation_method or None,
                        "validation_passed": validation_passed,
                        "validation_error": (validation_error or "")[:500] or None,
                        "user_email": user_email or None,
                        "created_at": datetime.now(UTC),
                    }
                    cols = ", ".join(params.keys())
                    placeholders = ", ".join(f":{k}" for k in params.keys())
                    fqn = self._fqn(table_name)
                    sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
                    cursor.execute(sql, parameters=params)
            return True
        except Exception:
            logger.exception("Failed to write rewrite artifact %s", artifact_id)
            return False

    def write_knowledge_document(self, document: KnowledgeDocument) -> str | None:
        """Write a knowledge document. Returns document_id."""
        if not self._config.enabled or not self._config.http_path:
            return None

        now = datetime.now(UTC)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = "profiler_knowledge_documents"
                    self._ensure_table(cursor, table_name)
                    tags_json = (
                        json.dumps(document.tags, ensure_ascii=False) if document.tags else None
                    )
                    params = {
                        "document_id": document.document_id,
                        "knowledge_type": document.knowledge_type,
                        "source_type": document.source_type,
                        "source_analysis_id": document.source_analysis_id or None,
                        "source_comparison_id": document.source_comparison_id or None,
                        "query_fingerprint": document.query_fingerprint or None,
                        "experiment_id": document.experiment_id or None,
                        "variant": document.variant or None,
                        "title": document.title,
                        "summary": document.summary or None,
                        "body_markdown": document.body_markdown or None,
                        "problem_category": document.problem_category or None,
                        "root_cause": document.root_cause or None,
                        "recommendation": document.recommendation or None,
                        "expected_impact": document.expected_impact or None,
                        "confidence_score": document.confidence_score,
                        "applicability_scope": document.applicability_scope or None,
                        "status": document.status,
                        "tags_json": tags_json,
                        "created_by": None,
                        "created_at": now,
                        "updated_at": now,
                    }
                    cols = ", ".join(params.keys())
                    placeholders = ", ".join(f":{k}" for k in params.keys())
                    sql = f"INSERT INTO {self._fqn(table_name)} ({cols}) VALUES ({placeholders})"
                    cursor.execute(sql, parameters=params)

            logger.info("Knowledge document written: document_id=%s", document.document_id)
            return document.document_id
        except Exception:
            logger.exception("Failed to write knowledge document")
            return None

    def write_knowledge_tags(self, document_id: str, tags: dict[str, str]) -> None:
        """Write knowledge tags for a document."""
        if not self._config.enabled or not self._config.http_path or not tags:
            return

        now = datetime.now(UTC)
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = "profiler_knowledge_tags"
                    self._ensure_table(cursor, table_name)
                    fqn = self._fqn(table_name)
                    for tag_name, tag_value in tags.items():
                        params = {
                            "document_id": document_id,
                            "tag_name": tag_name,
                            "tag_value": tag_value,
                            "created_at": now,
                        }
                        cols = ", ".join(params.keys())
                        placeholders = ", ".join(f":{k}" for k in params.keys())
                        sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
                        cursor.execute(sql, parameters=params)
            logger.info("Knowledge tags written for document_id=%s", document_id)
        except Exception:
            logger.exception("Failed to write knowledge tags")

    # -----------------------------------------------------------------------
    # v3: Internal comparison writers
    # -----------------------------------------------------------------------

    def _write_comparison_pair(self, cursor: Any, result: ComparisonResult, now: datetime) -> None:
        table_name = "profiler_comparison_pairs"
        self._ensure_table(cursor, table_name)
        params = {
            "comparison_id": result.comparison_id,
            "pair_status": "COMPLETED",
            "pair_type": "explicit_pair",
            "baseline_analysis_id": result.baseline_analysis_id,
            "candidate_analysis_id": result.candidate_analysis_id,
            "query_fingerprint": result.query_fingerprint or None,
            "experiment_id": result.experiment_id or None,
            "baseline_variant": result.baseline_variant or None,
            "candidate_variant": result.candidate_variant or None,
            "baseline_analyzed_at": None,
            "candidate_analyzed_at": None,
            "comparison_scope": "full",
            "comparison_reason": None,
            "requested_by": None,
            "request_source": "api",
            "tags_json": None,
            "created_at": now,
            "updated_at": now,
        }
        cols = ", ".join(params.keys())
        placeholders = ", ".join(f":{k}" for k in params.keys())
        sql = f"INSERT INTO {self._fqn(table_name)} ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, parameters=params)

    def _write_comparison_metrics(
        self, cursor: Any, result: ComparisonResult, now: datetime
    ) -> None:
        table_name = "profiler_comparison_metrics"
        self._ensure_table(cursor, table_name)
        fqn = self._fqn(table_name)

        for md in result.metric_diffs:
            percent_diff = (
                md.relative_diff_ratio * 100 if md.relative_diff_ratio is not None else None
            )
            params = {
                "comparison_id": result.comparison_id,
                "baseline_analysis_id": result.baseline_analysis_id,
                "candidate_analysis_id": result.candidate_analysis_id,
                "query_fingerprint": result.query_fingerprint or None,
                "experiment_id": result.experiment_id or None,
                "baseline_variant": result.baseline_variant or None,
                "candidate_variant": result.candidate_variant or None,
                "metric_name": md.metric_name,
                "metric_group": md.metric_group or None,
                "direction_when_increase": md.direction_when_increase or None,
                "baseline_value": md.baseline_value,
                "candidate_value": md.candidate_value,
                "absolute_diff": md.absolute_diff,
                "relative_diff_ratio": md.relative_diff_ratio,
                "percent_diff": percent_diff,
                "changed_flag": md.changed_flag,
                "improvement_flag": md.improvement_flag,
                "regression_flag": md.regression_flag,
                "severity": md.severity,
                "summary_text": md.summary_text or None,
                "created_at": now,
            }
            cols = ", ".join(params.keys())
            placeholders = ", ".join(f":{k}" for k in params.keys())
            sql = f"INSERT INTO {fqn} ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, parameters=params)

    # -----------------------------------------------------------------------
    # Delete
    # -----------------------------------------------------------------------

    # Tables that store per-analysis data (keyed by analysis_id)
    _ANALYSIS_TABLES = [
        "profiler_analysis_header",
        "profiler_analysis_actions",
        "profiler_analysis_table_scans",
        "profiler_analysis_hot_operators",
        "profiler_analysis_stages",
        "profiler_analysis_raw",
    ]

    def delete_analysis(self, analysis_id: str) -> bool:
        """Delete an analysis and all related records from Delta tables.

        Removes rows from header + 5 child tables.
        Returns True on success, False on failure/skip.
        """
        if not self._config.enabled or not self._config.http_path:
            return False

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    for table_name in self._ANALYSIS_TABLES:
                        fqn = self._fqn(table_name)
                        cursor.execute(
                            f"DELETE FROM {fqn} WHERE analysis_id = :aid",
                            parameters={"aid": analysis_id},
                        )
            logger.info("Analysis deleted: %s", analysis_id)
            return True
        except Exception:
            logger.exception("Failed to delete analysis: %s", analysis_id)
            return False

    def delete_analyses(self, analysis_ids: list[str]) -> int:
        """Delete multiple analyses. Returns count of successful deletions."""
        count = 0
        for aid in analysis_ids:
            if self.delete_analysis(aid):
                count += 1
        return count
