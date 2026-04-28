#!/usr/bin/env python3
"""Deploy curated SQL views and seed data to Databricks.

Creates comparison views, Genie-optimized views, and metric direction
seed data in the configured catalog/schema.

Usage:
    python scripts/deploy_views.py --catalog main --schema profiler

Environment variables:
    DATABRICKS_HOST              Workspace URL
    DATABRICKS_TOKEN             PAT token
    PROFILER_WAREHOUSE_HTTP_PATH SQL warehouse HTTP path
    PROFILER_CATALOG             (default: main)
    PROFILER_SCHEMA              (default: profiler)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL templates (parameterized with {catalog} and {schema})
# ---------------------------------------------------------------------------

VIEW_LATEST_BY_FINGERPRINT = """
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_latest_analysis_by_fingerprint AS
WITH ranked AS (
  SELECT
    h.*,
    ROW_NUMBER() OVER (
      PARTITION BY h.query_fingerprint,
                   COALESCE(h.experiment_id, '__default__'),
                   COALESCE(h.variant, '__default__')
      ORDER BY h.analyzed_at DESC, h.created_at DESC
    ) AS row_num
  FROM {catalog}.{schema}.profiler_analysis_header h
  WHERE h.query_fingerprint IS NOT NULL
)
SELECT * FROM ranked WHERE row_num = 1
"""

VIEW_COMPARISON_DIFF = """
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_comparison_diff AS
WITH pairs AS (
  SELECT
    p.comparison_id,
    p.query_fingerprint,
    p.experiment_id,
    p.baseline_variant,
    p.candidate_variant,
    b.analysis_id AS baseline_analysis_id,
    c.analysis_id AS candidate_analysis_id,
    STACK(
      14,
      'total_time_ms', cast(b.total_time_ms as double), cast(c.total_time_ms as double),
      'execution_time_ms', cast(b.execution_time_ms as double), cast(c.execution_time_ms as double),
      'read_bytes', cast(b.read_bytes as double), cast(c.read_bytes as double),
      'read_remote_bytes', cast(b.read_remote_bytes as double), cast(c.read_remote_bytes as double),
      'read_cache_bytes', cast(b.read_cache_bytes as double), cast(c.read_cache_bytes as double),
      'spill_to_disk_bytes', cast(b.spill_to_disk_bytes as double), cast(c.spill_to_disk_bytes as double),
      'spill_bytes', cast(b.spill_bytes as double), cast(c.spill_bytes as double),
      'bytes_read_from_cache_percentage', cast(b.bytes_read_from_cache_percentage as double), cast(c.bytes_read_from_cache_percentage as double),
      'photon_ratio', b.photon_ratio, c.photon_ratio,
      'remote_read_ratio', b.remote_read_ratio, c.remote_read_ratio,
      'bytes_pruning_ratio', b.bytes_pruning_ratio, c.bytes_pruning_ratio,
      'shuffle_impact_ratio', b.shuffle_impact_ratio, c.shuffle_impact_ratio,
      'cloud_storage_retry_ratio', b.cloud_storage_retry_ratio, c.cloud_storage_retry_ratio,
      'oom_fallback_count', cast(b.oom_fallback_count as double), cast(c.oom_fallback_count as double)
    ) AS (metric_name, baseline_value, candidate_value)
  FROM {catalog}.{schema}.profiler_comparison_pairs p
  INNER JOIN {catalog}.{schema}.profiler_analysis_header b
    ON p.baseline_analysis_id = b.analysis_id
  INNER JOIN {catalog}.{schema}.profiler_analysis_header c
    ON p.candidate_analysis_id = c.analysis_id
),
scored AS (
  SELECT
    p.*,
    d.metric_group,
    d.increase_effect,
    d.decrease_effect,
    d.regression_threshold_ratio,
    d.improvement_threshold_ratio,
    candidate_value - baseline_value AS absolute_diff,
    CASE WHEN baseline_value IS NULL OR baseline_value = 0 THEN NULL
         ELSE (candidate_value - baseline_value) / baseline_value END AS relative_diff_ratio
  FROM pairs p
  LEFT JOIN {catalog}.{schema}.profiler_metric_directions d
    ON p.metric_name = d.metric_name
)
SELECT
  comparison_id, query_fingerprint, experiment_id,
  baseline_variant, candidate_variant,
  baseline_analysis_id, candidate_analysis_id,
  metric_name, metric_group, baseline_value, candidate_value,
  absolute_diff, relative_diff_ratio,
  relative_diff_ratio * 100 AS percent_diff,
  CASE WHEN absolute_diff > 0 THEN increase_effect
       WHEN absolute_diff < 0 THEN decrease_effect
       ELSE 'NEUTRAL' END AS change_effect,
  CASE WHEN absolute_diff > 0 AND increase_effect = 'WORSENS'
            AND COALESCE(relative_diff_ratio, 0) >= COALESCE(regression_threshold_ratio, 0) THEN true
       WHEN absolute_diff < 0 AND decrease_effect = 'WORSENS'
            AND ABS(COALESCE(relative_diff_ratio, 0)) >= COALESCE(regression_threshold_ratio, 0) THEN true
       ELSE false END AS regression_flag,
  CASE WHEN absolute_diff > 0 AND increase_effect = 'IMPROVES'
            AND COALESCE(relative_diff_ratio, 0) >= COALESCE(improvement_threshold_ratio, 0) THEN true
       WHEN absolute_diff < 0 AND decrease_effect = 'IMPROVES'
            AND ABS(COALESCE(relative_diff_ratio, 0)) >= COALESCE(improvement_threshold_ratio, 0) THEN true
       ELSE false END AS improvement_flag
FROM scored
"""

VIEW_REGRESSION_CANDIDATES = """
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_regression_candidates AS
WITH metric_flags AS (
  SELECT
    comparison_id, query_fingerprint, experiment_id,
    baseline_variant, candidate_variant,
    COUNT_IF(regression_flag) AS regression_metric_count,
    COUNT_IF(improvement_flag) AS improvement_metric_count,
    MAX(CASE WHEN metric_name = 'total_time_ms' AND regression_flag THEN 1 ELSE 0 END) AS total_time_regressed,
    MAX(CASE WHEN metric_name = 'spill_to_disk_bytes' AND regression_flag THEN 1 ELSE 0 END) AS spill_regressed,
    MAX(CASE WHEN metric_name = 'remote_read_ratio' AND regression_flag THEN 1 ELSE 0 END) AS remote_read_regressed,
    MAX(CASE WHEN metric_name = 'photon_ratio' AND regression_flag THEN 1 ELSE 0 END) AS photon_regressed
  FROM {catalog}.{schema}.vw_comparison_diff
  GROUP BY ALL
)
SELECT p.*, f.regression_metric_count, f.improvement_metric_count,
  CASE WHEN f.total_time_regressed = 1 AND f.spill_regressed = 1 THEN 'HIGH'
       WHEN f.total_time_regressed = 1 AND f.remote_read_regressed = 1 THEN 'HIGH'
       WHEN f.total_time_regressed = 1 AND f.photon_regressed = 1 THEN 'MEDIUM'
       WHEN f.regression_metric_count >= 3 THEN 'MEDIUM'
       ELSE 'LOW' END AS regression_severity
FROM {catalog}.{schema}.profiler_comparison_pairs p
INNER JOIN metric_flags f ON p.comparison_id = f.comparison_id
WHERE f.regression_metric_count > 0
"""

VIEW_GENIE_PROFILE = """
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_genie_profile_summary AS
SELECT
  h.analysis_id, h.analyzed_at,
  h.query_fingerprint, h.query_family_id, h.purpose_signature,
  h.experiment_id, h.variant, h.variant_type,
  h.query_status, h.statement_type AS sql_statement_type, h.query_text,
  h.total_time_ms AS total_query_time_ms, h.execution_time_ms, h.compilation_time_ms,
  h.read_bytes AS bytes_read, h.read_remote_bytes AS remote_bytes_read,
  h.read_cache_bytes AS cache_bytes_read, h.spill_to_disk_bytes,
  h.rows_read_count AS rows_read, h.rows_produced_count AS rows_produced,
  h.bytes_read_from_cache_percentage AS cache_hit_percentage,
  h.photon_ratio AS photon_usage_ratio, h.remote_read_ratio, h.bytes_pruning_ratio,
  h.shuffle_impact_ratio, h.cloud_storage_retry_ratio, h.has_data_skew,
  h.oom_fallback_count, h.join_count, h.subquery_count,
  h.complexity_score AS sql_complexity_score,
  h.critical_alert_count, h.high_alert_count,
  h.action_card_count AS recommendation_count, h.scanned_table_count,
  h.warehouse_name, h.warehouse_size, h.warehouse_type,
  h.warehouse_is_serverless AS is_serverless_warehouse
FROM {catalog}.{schema}.profiler_analysis_header h
"""

VIEW_GENIE_COMPARISON = """
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_genie_comparison_summary AS
SELECT
  p.comparison_id, p.created_at AS comparison_created_at,
  p.query_fingerprint, p.experiment_id,
  p.baseline_variant, p.candidate_variant,
  r.regression_severity, r.regression_metric_count, r.improvement_metric_count,
  MAX(CASE WHEN d.metric_name = 'total_time_ms' THEN d.baseline_value END) AS baseline_total_time_ms,
  MAX(CASE WHEN d.metric_name = 'total_time_ms' THEN d.candidate_value END) AS candidate_total_time_ms,
  MAX(CASE WHEN d.metric_name = 'total_time_ms' THEN d.percent_diff END) AS total_time_change_percent,
  MAX(CASE WHEN d.metric_name = 'read_bytes' THEN d.percent_diff END) AS bytes_read_change_percent,
  MAX(CASE WHEN d.metric_name = 'spill_to_disk_bytes' THEN d.percent_diff END) AS spill_change_percent,
  MAX(CASE WHEN d.metric_name = 'photon_ratio' THEN d.percent_diff END) AS photon_ratio_change_percent
FROM {catalog}.{schema}.profiler_comparison_pairs p
LEFT JOIN {catalog}.{schema}.vw_comparison_diff d ON p.comparison_id = d.comparison_id
LEFT JOIN {catalog}.{schema}.vw_regression_candidates r ON p.comparison_id = r.comparison_id
GROUP BY ALL
"""

VIEW_GENIE_RECOMMENDATIONS = """
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_genie_recommendations AS
SELECT
  d.document_id, d.created_at, d.knowledge_type, d.source_type,
  d.query_fingerprint, d.experiment_id, d.variant,
  d.title AS recommendation_title, d.summary AS recommendation_summary,
  d.problem_category, d.root_cause, d.recommendation AS recommended_action,
  d.expected_impact, d.confidence_score, d.status
FROM {catalog}.{schema}.profiler_knowledge_documents d
"""

SEED_METRIC_DIRECTIONS = """
INSERT INTO {catalog}.{schema}.profiler_metric_directions
SELECT s.metric_name, s.metric_group, s.display_name, s.unit,
       s.increase_effect, s.decrease_effect, s.preferred_trend,
       s.regression_threshold_ratio, s.improvement_threshold_ratio,
       s.notes, s.active_flag, s.version, current_timestamp()
FROM (VALUES
  ('total_time_ms',                    'latency',       'Total Time',           'ms',    'WORSENS',  'IMPROVES', 'DOWN', CAST(0.10 AS DOUBLE), CAST(0.10 AS DOUBLE), 'Lower is better',            true, 'v1'),
  ('execution_time_ms',                'latency',       'Execution Time',       'ms',    'WORSENS',  'IMPROVES', 'DOWN', CAST(0.10 AS DOUBLE), CAST(0.10 AS DOUBLE), 'Lower is better',            true, 'v1'),
  ('compilation_time_ms',              'latency',       'Compilation Time',     'ms',    'WORSENS',  'IMPROVES', 'DOWN', CAST(0.10 AS DOUBLE), CAST(0.10 AS DOUBLE), 'Lower is better',            true, 'v1'),
  ('read_bytes',                       'io',            'Bytes Read',           'bytes', 'WORSENS',  'IMPROVES', 'DOWN', CAST(0.15 AS DOUBLE), CAST(0.15 AS DOUBLE), 'Less data scanned is better', true, 'v1'),
  ('read_remote_bytes',                'io',            'Remote Bytes Read',    'bytes', 'WORSENS',  'IMPROVES', 'DOWN', CAST(0.15 AS DOUBLE), CAST(0.15 AS DOUBLE), 'Less remote read is better',  true, 'v1'),
  ('read_cache_bytes',                 'cache',         'Bytes From Cache',     'bytes', 'IMPROVES', 'WORSENS',  'UP',   CAST(0.10 AS DOUBLE), CAST(0.10 AS DOUBLE), 'More cache is better',        true, 'v1'),
  ('bytes_read_from_cache_percentage', 'cache',         'Cache Hit Percentage', 'ratio', 'IMPROVES', 'WORSENS',  'UP',   CAST(0.05 AS DOUBLE), CAST(0.05 AS DOUBLE), 'Higher hit rate is better',   true, 'v1'),
  ('spill_to_disk_bytes',              'spill',         'Spill To Disk',        'bytes', 'WORSENS',  'IMPROVES', 'DOWN', CAST(0.05 AS DOUBLE), CAST(0.05 AS DOUBLE), 'Less spill is better',        true, 'v1'),
  ('spill_bytes',                      'spill',         'Operator Spill',       'bytes', 'WORSENS',  'IMPROVES', 'DOWN', CAST(0.05 AS DOUBLE), CAST(0.05 AS DOUBLE), 'Less spill is better',        true, 'v1'),
  ('photon_ratio',                     'engine',        'Photon Ratio',         'ratio', 'IMPROVES', 'WORSENS',  'UP',   CAST(0.05 AS DOUBLE), CAST(0.05 AS DOUBLE), 'Higher Photon is better',     true, 'v1'),
  ('remote_read_ratio',                'io',            'Remote Read Ratio',    'ratio', 'WORSENS',  'IMPROVES', 'DOWN', CAST(0.05 AS DOUBLE), CAST(0.05 AS DOUBLE), 'Lower remote is better',      true, 'v1'),
  ('bytes_pruning_ratio',              'io',            'Bytes Pruning Ratio',  'ratio', 'IMPROVES', 'WORSENS',  'UP',   CAST(0.05 AS DOUBLE), CAST(0.05 AS DOUBLE), 'Higher pruning is better',    true, 'v1'),
  ('shuffle_impact_ratio',             'shuffle',       'Shuffle Impact',       'ratio', 'WORSENS',  'IMPROVES', 'DOWN', CAST(0.10 AS DOUBLE), CAST(0.10 AS DOUBLE), 'Lower shuffle is better',     true, 'v1'),
  ('cloud_storage_retry_ratio',        'cloud_storage', 'Cloud Storage Retry',  'ratio', 'WORSENS',  'IMPROVES', 'DOWN', CAST(0.05 AS DOUBLE), CAST(0.05 AS DOUBLE), 'Lower retry is better',       true, 'v1'),
  ('oom_fallback_count',               'engine',        'OOM Fallback Count',   'count', 'WORSENS',  'IMPROVES', 'DOWN', CAST(0.00 AS DOUBLE), CAST(0.00 AS DOUBLE), 'Any increase is bad',         true, 'v1')
) AS s(metric_name, metric_group, display_name, unit, increase_effect, decrease_effect, preferred_trend, regression_threshold_ratio, improvement_threshold_ratio, notes, active_flag, version)
WHERE s.metric_name NOT IN (SELECT metric_name FROM {catalog}.{schema}.profiler_metric_directions)
"""

VIEW_VARIANT_RANKING = """
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_variant_ranking AS
WITH latest AS (
  SELECT * FROM {catalog}.{schema}.vw_latest_analysis_by_fingerprint
),
baseline AS (
  SELECT * FROM latest WHERE baseline_flag = true
),
paired AS (
  SELECT
    c.query_fingerprint,
    c.experiment_id,
    c.variant,
    c.analysis_id,
    c.analyzed_at,
    c.warehouse_name,
    c.warehouse_size,

    -- Key metrics: baseline vs candidate
    b.total_time_ms AS baseline_total_time_ms,
    c.total_time_ms AS candidate_total_time_ms,
    b.execution_time_ms AS baseline_execution_time_ms,
    c.execution_time_ms AS candidate_execution_time_ms,
    b.read_bytes AS baseline_read_bytes,
    c.read_bytes AS candidate_read_bytes,
    b.spill_bytes AS baseline_spill_bytes,
    c.spill_bytes AS candidate_spill_bytes,
    b.photon_ratio AS baseline_photon_ratio,
    c.photon_ratio AS candidate_photon_ratio,
    b.shuffle_impact_ratio AS baseline_shuffle_impact,
    c.shuffle_impact_ratio AS candidate_shuffle_impact,

    -- Diff ratios
    CASE WHEN b.total_time_ms > 0
         THEN (c.total_time_ms - b.total_time_ms) * 1.0 / b.total_time_ms END AS total_time_diff_ratio,
    CASE WHEN b.execution_time_ms > 0
         THEN (c.execution_time_ms - b.execution_time_ms) * 1.0 / b.execution_time_ms END AS execution_time_diff_ratio,
    CASE WHEN b.read_bytes > 0
         THEN (c.read_bytes - b.read_bytes) * 1.0 / b.read_bytes END AS read_bytes_diff_ratio,
    CASE WHEN b.spill_bytes > 0
         THEN (c.spill_bytes - b.spill_bytes) * 1.0 / b.spill_bytes END AS spill_diff_ratio,
    CASE WHEN b.photon_ratio > 0
         THEN (c.photon_ratio - b.photon_ratio) * 1.0 / b.photon_ratio
         ELSE c.photon_ratio END AS photon_diff_ratio,

    -- Weighted ranking score (higher = better)
    -- WORSENS metrics: negative diff = improvement -> positive score
    -- IMPROVES metrics: positive diff = improvement -> positive score
    (
      - 5.0 * COALESCE(CASE WHEN b.total_time_ms > 0
                        THEN (c.total_time_ms - b.total_time_ms) * 1.0 / b.total_time_ms END, 0.0)
      - 3.0 * COALESCE(CASE WHEN b.execution_time_ms > 0
                        THEN (c.execution_time_ms - b.execution_time_ms) * 1.0 / b.execution_time_ms END, 0.0)
      - 2.0 * COALESCE(CASE WHEN b.read_bytes > 0
                        THEN (c.read_bytes - b.read_bytes) * 1.0 / b.read_bytes END, 0.0)
      - 4.0 * COALESCE(CASE WHEN b.spill_bytes > 0
                        THEN (c.spill_bytes - b.spill_bytes) * 1.0 / b.spill_bytes END, 0.0)
      + 2.0 * COALESCE(CASE WHEN b.photon_ratio > 0
                        THEN (c.photon_ratio - b.photon_ratio) * 1.0 / b.photon_ratio
                        ELSE c.photon_ratio END, 0.0)
      - 2.0 * COALESCE(CASE WHEN b.shuffle_impact_ratio > 0
                        THEN (c.shuffle_impact_ratio - b.shuffle_impact_ratio) * 1.0 / b.shuffle_impact_ratio END, 0.0)
    ) AS ranking_score,

    -- Disqualification: critical regression guardrail
    CASE
      WHEN b.total_time_ms > 0
           AND c.total_time_ms > b.total_time_ms * 1.10 THEN true
      WHEN b.spill_bytes > 0
           AND c.spill_bytes > b.spill_bytes * 1.20 THEN true
      WHEN COALESCE(c.oom_fallback_count, 0) > COALESCE(b.oom_fallback_count, 0) THEN true
      ELSE false
    END AS is_disqualified
  FROM latest c
  INNER JOIN baseline b
    ON c.query_fingerprint = b.query_fingerprint
   AND c.experiment_id = b.experiment_id
  WHERE c.baseline_flag = false OR c.baseline_flag IS NULL
)
SELECT *,
       ROW_NUMBER() OVER (
         PARTITION BY query_fingerprint, experiment_id
         ORDER BY is_disqualified ASC, ranking_score DESC, analyzed_at DESC
       ) AS rank_in_experiment,
       CASE
         WHEN is_disqualified THEN 'DISQUALIFIED'
         WHEN ranking_score > 0 THEN 'IMPROVED'
         WHEN ranking_score < 0 THEN 'REGRESSED'
         ELSE 'NEUTRAL'
       END AS verdict
FROM paired
"""

# L5 (2026-04-26): monthly triage view for user feedback.
# Joins profiler_feedback against the analysis header so the reviewer
# can see what query was being analyzed when feedback was filed.
VIEW_FEEDBACK_TRIAGE = """
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_feedback_triage AS
SELECT
  f.feedback_id,
  f.created_at,
  f.user_email,
  f.category,
  f.target_type,
  f.target_id,
  f.sentiment,
  f.free_text,
  f.analysis_id,
  h.analyzed_at AS analysis_analyzed_at,
  h.query_id   AS analysis_query_id,
  h.experiment_id AS analysis_experiment_id,
  h.variant      AS analysis_variant,
  -- Codex (a)(d) フォロー: per-action 改善要望と whole-report 欠落申告を
  -- 集計時に区別しやすくするためフラグ列を追加
  CASE WHEN f.target_type = 'action' THEN true ELSE false END AS is_per_action_feedback
FROM {catalog}.{schema}.profiler_feedback f
LEFT JOIN {catalog}.{schema}.profiler_analysis_header h
  ON f.analysis_id = h.analysis_id
ORDER BY f.created_at DESC
"""

# Ordered list: views with dependencies must come after their sources
ALL_STATEMENTS: list[tuple[str, str]] = [
    ("vw_latest_analysis_by_fingerprint", VIEW_LATEST_BY_FINGERPRINT),
    ("seed: profiler_metric_directions", SEED_METRIC_DIRECTIONS),
    ("vw_comparison_diff", VIEW_COMPARISON_DIFF),
    ("vw_regression_candidates", VIEW_REGRESSION_CANDIDATES),
    ("vw_genie_profile_summary", VIEW_GENIE_PROFILE),
    ("vw_genie_comparison_summary", VIEW_GENIE_COMPARISON),
    ("vw_genie_recommendations", VIEW_GENIE_RECOMMENDATIONS),
    ("vw_variant_ranking", VIEW_VARIANT_RANKING),
    ("vw_feedback_triage", VIEW_FEEDBACK_TRIAGE),
]


# Tables to drop and recreate when --reset-tables is used
_ALL_TABLES = [
    "profiler_analysis_header",
    "profiler_analysis_actions",
    "profiler_analysis_table_scans",
    "profiler_analysis_hot_operators",
    "profiler_analysis_stages",
    "profiler_analysis_raw",
    "profiler_comparison_pairs",
    "profiler_comparison_metrics",
    "profiler_knowledge_documents",
    "profiler_knowledge_tags",
    "profiler_metric_directions",
    "profiler_feedback",
]


def _connect(host: str, http_path: str, token: str):
    from databricks import sql as dbsql

    if host.startswith("https://"):
        host = host[len("https://"):]
    if host.startswith("http://"):
        host = host[len("http://"):]
    host = host.rstrip("/")
    return dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )


def reset_tables(catalog: str, schema: str, http_path: str, host: str, token: str) -> None:
    """Drop all existing tables and let TableWriter recreate them with the new schema."""
    logger.info("Resetting tables in %s.%s ...", catalog, schema)
    conn = _connect(host, http_path, token)
    try:
        with conn.cursor() as cursor:
            for table_name in _ALL_TABLES:
                fqn = f"{catalog}.{schema}.{table_name}"
                logger.info("  DROP TABLE IF EXISTS %s", fqn)
                cursor.execute(f"DROP TABLE IF EXISTS {fqn}")
            # Also drop views that depend on these tables
            for name, _ in ALL_STATEMENTS:
                if name.startswith("vw_"):
                    fqn = f"{catalog}.{schema}.{name}"
                    logger.info("  DROP VIEW IF EXISTS %s", fqn)
                    cursor.execute(f"DROP VIEW IF EXISTS {fqn}")
    finally:
        conn.close()
    logger.info("All tables and views dropped. They will be auto-created on next write/deploy.")


def deploy(catalog: str, schema: str, http_path: str, host: str, token: str) -> None:
    """Deploy all tables, views and seed data."""
    import sys as _sys

    # Add databricks-apps to path so we can import table_writer DDLs
    _app_dir = str(
        __import__("pathlib").Path(__file__).resolve().parent.parent / "databricks-apps"
    )
    if _app_dir not in _sys.path:
        _sys.path.insert(0, _app_dir)

    from services.table_writer import _TABLE_DDLS

    logger.info("Connecting to %s ...", host)
    conn = _connect(host, http_path, token)

    try:
        with conn.cursor() as cursor:
            # Step 1: Create all tables (idempotent)
            for table_name, ddl_template in _TABLE_DDLS.items():
                fqn = f"{catalog}.{schema}.{table_name}"
                ddl = ddl_template.format(fqn=fqn)
                logger.info("Creating table: %s ...", table_name)
                cursor.execute(ddl)
                logger.info("  OK: %s", table_name)

            # Step 2: Create views and seed data
            for name, sql_template in ALL_STATEMENTS:
                sql = sql_template.format(catalog=catalog, schema=schema)
                logger.info("Deploying: %s ...", name)
                cursor.execute(sql)
                logger.info("  OK: %s", name)
    finally:
        conn.close()

    logger.info("All tables, views and seed data deployed successfully.")


def main():
    parser = argparse.ArgumentParser(description="Deploy curated views to Databricks")
    parser.add_argument("--catalog", default=os.environ.get("PROFILER_CATALOG", "main"))
    parser.add_argument("--schema", default=os.environ.get("PROFILER_SCHEMA", "profiler"))
    parser.add_argument("--http-path", default=os.environ.get("PROFILER_WAREHOUSE_HTTP_PATH", ""))
    parser.add_argument("--host", default=os.environ.get("DATABRICKS_HOST", ""))
    parser.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN", ""))
    parser.add_argument(
        "--reset-tables",
        action="store_true",
        help="DROP all existing tables and views before deploying (destroys data!)",
    )
    args = parser.parse_args()

    if not args.host or not args.token or not args.http_path:
        logger.error(
            "Missing required config. Set DATABRICKS_HOST, DATABRICKS_TOKEN, "
            "and PROFILER_WAREHOUSE_HTTP_PATH."
        )
        sys.exit(1)

    if args.reset_tables:
        confirm = input(
            f"This will DROP all profiler tables in {args.catalog}.{args.schema}. "
            "All data will be lost. Continue? [y/N] "
        )
        if confirm.lower() != "y":
            logger.info("Aborted.")
            sys.exit(0)
        reset_tables(args.catalog, args.schema, args.http_path, args.host, args.token)

    deploy(args.catalog, args.schema, args.http_path, args.host, args.token)


if __name__ == "__main__":
    main()
