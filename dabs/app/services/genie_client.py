"""Client for Databricks Genie Conversation API."""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# Gold table suffixes to register in Genie Space
_GOLD_TABLE_SUFFIXES = [
    "gold_application_summary",
    "gold_job_performance",
    "gold_stage_performance",
    "gold_executor_analysis",
    "gold_spot_instance_analysis",
    "gold_bottleneck_report",
    "gold_job_detail",
    "gold_job_concurrency",
    "gold_cross_app_concurrency",
    "gold_spark_config_analysis",
    "gold_sql_photon_analysis",
    "gold_narrative_summary",
    "gold_autoscale_timeline",
    "spark_comparison_pairs",
    "spark_comparison_metrics",
    "spark_compare_result",
]

_SYSTEM_INSTRUCTIONS = """You are a Spark performance analysis expert. Users are analyzing Spark job performance data from Gold tables.

Key tables:
- gold_application_summary: High-level app metrics (duration, jobs, shuffle, spill, GC)
- gold_job_performance: Per-job metrics (duration, stages, tasks)
- gold_stage_performance: Per-stage metrics (input/output, shuffle, spill, task skew)
- gold_executor_analysis: Per-executor resource usage (CPU, GC, memory, disk)
- gold_bottleneck_report: Pre-computed bottleneck classifications (DATA_SKEW, SMALL_FILES, HEAVY_SHUFFLE, etc.)
- gold_job_concurrency: Job parallelism and overlap analysis
- gold_spot_instance_analysis: Spot instance / node loss events
- gold_spark_config_analysis: Spark configuration analysis (changed from defaults)
- gold_sql_photon_analysis: SQL execution Photon utilization
- gold_narrative_summary: LLM-generated narrative reports
- gold_autoscale_timeline: Executor scaling events correlated with active stages. Columns: event_ts, event_type (SCALE_OUT/SCALE_IN), executor_id, worker_count, active_stage_count, total_active_tasks, active_stage_ids, active_stage_names, active_bottleneck_types, active_spill_mb, active_shuffle_mb.
- spark_comparison_pairs: Spark comparison pairs. Columns: comparison_id, baseline_app_id, candidate_app_id, cluster_id, regression_detected, regression_severity.
- spark_comparison_metrics: Per-metric Spark comparison results. Columns: comparison_id, metric_name, metric_group, baseline_value, candidate_value, percent_diff, regression_flag, improvement_flag.
- spark_compare_result: Flat comparison result history. Columns: comparison_id, compared_at, baseline_app_id, candidate_app_id, baseline_experiment, baseline_variant, baseline_duration_ms, candidate_duration_ms, regression_detected, regression_severity, net_score, report_markdown.

Rules:
1. Always filter by app_id when the user is asking about a specific application
2. Show durations in human-readable format (seconds/minutes)
3. Show data sizes in MB/GB with appropriate precision
4. When comparing metrics, calculate percentage differences
5. Highlight bottlenecks with severity HIGH first
6. Use the bottleneck_type column to classify issues (DATA_SKEW, SMALL_FILES, HEAVY_SHUFFLE, DISK_SPILL, etc.)
7. When the context provides a comparison_id, filter spark_compare_result and spark_comparison_metrics by comparison_id. Use both baseline_app_id and candidate_app_id to query individual app data.
"""

# DBSQL profiler table names
_DBSQL_TABLE_NAMES = [
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
    "profiler_compare_result",
]

_DBSQL_SYSTEM_INSTRUCTIONS = """You are a Databricks SQL query performance analysis expert. Users are analyzing DBSQL query profiler data.

CRITICAL: All tables are linked by analysis_id. When the user provides an analysis_id in the context, you MUST add WHERE analysis_id = '<id>' to EVERY query. Never return data from other analyses.

Table relationships (all joined on analysis_id):
- profiler_analysis_header: Master table. One row per analysis. Contains analysis_id, query_id, query_text, fingerprint, family_id, experiment_id, variant, total_time_ms, status, warehouse_id, analyzed_at.
- profiler_analysis_actions: Recommended optimization actions. Columns: analysis_id, action_rank, severity, category, title, description, impact_estimate.
- profiler_analysis_table_scans: Table scan details. Columns: analysis_id, table_name, rows_read, bytes_read, pruning_ratio, scan_type.
- profiler_analysis_hot_operators: Hot operators consuming most resources. Columns: analysis_id, operator_name, bottleneck_type, duration_ms, rows_produced, bytes_processed.
- profiler_analysis_stages: Stage-level metrics. Columns: analysis_id, stage_id, duration_ms, rows_read, bytes_read, bytes_written, spill_bytes.
- profiler_analysis_raw: Raw profile JSON for deep inspection.
- profiler_comparison_pairs: Before/After comparison pairs. Columns: comparison_id, baseline_id, candidate_id.
- profiler_comparison_metrics: Per-metric comparison results. Columns: comparison_id, metric_name, baseline_value, candidate_value, direction, pct_change, is_regression.
- profiler_knowledge_documents: Auto-generated knowledge base documents about query patterns.
- profiler_knowledge_tags: Tags for knowledge documents.
- profiler_metric_directions: Defines whether higher or lower is better for each metric.
- profiler_compare_result: Flat comparison result history. Columns: comparison_id, compared_at, baseline_analyzed_at, baseline_query_id, baseline_experiment, baseline_variant, baseline_duration_ms, baseline_alerts, candidate_analyzed_at, candidate_query_id, candidate_experiment, candidate_variant, candidate_duration_ms, candidate_alerts, regression_detected, regression_severity, net_score, report_markdown.

Rules:
1. ALWAYS filter by analysis_id when the context provides one — this is the most important rule
2. Show durations in human-readable format (seconds/minutes)
3. Show data sizes in MB/GB with appropriate precision
4. Use profiler_analysis_actions for actionable recommendations (ordered by action_rank)
5. When comparing, use profiler_metric_directions to determine improvement vs regression
6. For query text, use profiler_analysis_header.query_text
7. When the context provides a comparison_id, filter profiler_compare_result and profiler_comparison_metrics by comparison_id. Use both baseline_analysis_id and candidate_analysis_id to query individual analysis data.
"""


@dataclass
class GenieConfig:
    """Configuration for Genie API client."""

    host: str
    catalog: str
    schema: str
    table_prefix: str
    warehouse_id: str

    @classmethod
    def from_env(cls) -> GenieConfig:
        """Load from config store + environment."""
        import os

        from core.config_store import get_setting

        host = os.environ.get("DATABRICKS_HOST", "")
        if not host:
            try:
                from databricks.sdk.core import Config

                cfg = Config()
                host = cfg.host or ""
            except Exception:
                pass
        host = host.replace("https://", "").replace("http://", "").rstrip("/")

        # Extract warehouse_id from http_path
        http_path = get_setting("spark_perf_http_path", "")
        wh_id = ""
        if http_path and "/warehouses/" in http_path:
            wh_id = http_path.split("/warehouses/")[-1]

        return cls(
            host=host,
            catalog=get_setting("spark_perf_catalog", "main"),
            schema=get_setting("spark_perf_schema", "default"),
            table_prefix=get_setting("spark_perf_table_prefix", "PERF_"),
            warehouse_id=wh_id,
        )


@dataclass
class DbsqlGenieConfig:
    """Configuration for DBSQL Genie API client."""

    host: str
    catalog: str
    schema: str
    warehouse_id: str

    @classmethod
    def from_env(cls) -> DbsqlGenieConfig:
        """Load from DBSQL config store + environment."""
        import os

        from core.config_store import get_setting

        host = os.environ.get("DATABRICKS_HOST", "")
        if not host:
            try:
                from databricks.sdk.core import Config

                cfg = Config()
                host = cfg.host or ""
            except Exception:
                pass
        host = host.replace("https://", "").replace("http://", "").rstrip("/")

        # Extract warehouse_id from DBSQL http_path
        http_path = get_setting("http_path", "")
        wh_id = ""
        if http_path and "/warehouses/" in http_path:
            wh_id = http_path.split("/warehouses/")[-1]

        return cls(
            host=host,
            catalog=get_setting("catalog", "main"),
            schema=get_setting("schema", "profiler"),
            warehouse_id=wh_id,
        )


class GenieClient:
    """Client for Databricks Genie Conversation API."""

    def __init__(self, config: GenieConfig | DbsqlGenieConfig) -> None:
        self._config = config
        self._session = None

    def _get_session(self):
        """Get authenticated requests session using SP credentials."""
        if self._session is not None:
            return self._session

        import os

        import requests

        session = requests.Session()
        session.headers["Content-Type"] = "application/json"

        # Try env var token first (set by deploy.sh or CLI)
        env_token = os.environ.get("DATABRICKS_TOKEN", "")
        if env_token:
            session.headers["Authorization"] = f"Bearer {env_token}"
            logger.info("Genie session: env token")
            self._session = session
            return session

        try:
            from databricks.sdk.core import Config

            cfg = Config()
            headers = cfg.authenticate()
            if callable(headers):
                headers = headers()
            if isinstance(headers, dict) and "Authorization" in headers:
                session.headers["Authorization"] = headers["Authorization"]
                logger.info("Genie session: SP auth (SDK), auth_type=%s", cfg.auth_type)
                self._session = session
                return session
        except Exception as e:
            logger.warning("SDK authenticate() failed: %s", e)

        logger.warning("Genie session: NO token available")
        return session

    def _api_url(self, path: str) -> str:
        return f"https://{self._config.host}{path}"

    def _spark_perf_table_fqns(self) -> list[str]:
        """Build dot-separated table FQNs for Spark Perf space creation."""
        result = []
        for suffix in _GOLD_TABLE_SUFFIXES:
            table_name = f"{self._config.table_prefix}{suffix}"
            result.append(f"{self._config.catalog}.{self._config.schema}.{table_name}")
        return result

    def _dbsql_table_fqns(self) -> list[str]:
        """Build dot-separated table FQNs for DBSQL space creation."""
        return [
            f"{self._config.catalog}.{self._config.schema}.{name}" for name in _DBSQL_TABLE_NAMES
        ]

    def _create_space_generic(
        self, title: str, description: str, table_fqns: list[str], instructions: str
    ) -> str:
        """Create a Genie Space with given tables. Returns space_id."""
        tables = [{"identifier": fqn} for fqn in sorted(table_fqns)]

        space_obj = {
            "version": 2,
            "data_sources": {
                "tables": tables,
            },
            "instructions": {
                "text_instructions": [
                    {
                        "id": uuid.uuid4().hex,
                        "content": [instructions],
                    }
                ],
                "example_question_sqls": [],
            },
        }

        payload = {
            "title": title,
            "description": description,
            "warehouse_id": self._config.warehouse_id,
            "serialized_space": json.dumps(space_obj),
        }

        session = self._get_session()
        logger.info(
            "create_space (SP auth): title=%s, warehouse_id=%s, tables=%d",
            title,
            self._config.warehouse_id,
            len(tables),
        )
        r = session.post(self._api_url("/api/2.0/genie/spaces"), json=payload)
        if r.status_code != 200:
            logger.error("create_space failed: status=%s, body=%s", r.status_code, r.text[:500])
        r.raise_for_status()
        data = r.json()
        space_id = data.get("space_id") or data.get("id", "")
        logger.info("Created Genie Space: %s", space_id)
        return space_id

    def create_space(self) -> str:
        """Create a Genie Space with Spark Perf Gold tables. Returns space_id."""
        return self._create_space_generic(
            title="Spark Performance Analysis",
            description="Interactive analysis of Spark job performance Gold tables",
            table_fqns=self._spark_perf_table_fqns(),
            instructions=_SYSTEM_INSTRUCTIONS,
        )

    def create_dbsql_space(self) -> str:
        """Create a Genie Space with DBSQL profiler tables. Returns space_id."""
        return self._create_space_generic(
            title="DBSQL Query Performance Analysis",
            description="Interactive analysis of Databricks SQL query profiler tables",
            table_fqns=self._dbsql_table_fqns(),
            instructions=_DBSQL_SYSTEM_INSTRUCTIONS,
        )

    def validate_space(self, space_id: str, expected_tables: list[str] | None = None) -> bool:
        """Check if a Genie Space exists, is accessible, and has the expected tables.

        Args:
            space_id: The Genie Space ID to validate.
            expected_tables: If provided, verify the space contains these table FQNs.
                             If the tables don't match, returns False to trigger recreation.
        Returns:
            True if the space is valid and tables match (or no check requested).
        """
        session = self._get_session()
        try:
            r = session.get(self._api_url(f"/api/2.0/genie/spaces/{space_id}"))
            if r.status_code != 200:
                return False

            if expected_tables:
                data = r.json()
                serialized = data.get("serialized_space", "")
                if serialized:
                    try:
                        space_obj = json.loads(serialized)
                        registered = sorted(
                            t.get("identifier", "")
                            for t in space_obj.get("data_sources", {}).get("tables", [])
                        )
                        expected_sorted = sorted(expected_tables)
                        if registered != expected_sorted:
                            logger.info(
                                "Genie Space %s table mismatch: registered=%d, expected=%d. Will recreate.",
                                space_id,
                                len(registered),
                                len(expected_sorted),
                            )
                            return False
                    except (json.JSONDecodeError, KeyError):
                        pass
            return True
        except Exception:
            return False

    def start_conversation(self, space_id: str, message: str) -> dict[str, str]:
        """Start a new conversation. Returns {conversation_id, message_id}."""
        session = self._get_session()
        r = session.post(
            self._api_url(f"/api/2.0/genie/spaces/{space_id}/start-conversation"),
            json={"content": message},
        )
        r.raise_for_status()
        data = r.json()
        return {
            "conversation_id": data.get("conversation_id", ""),
            "message_id": data.get("message_id", ""),
        }

    def send_message(self, space_id: str, conversation_id: str, message: str) -> str:
        """Send follow-up message. Returns message_id."""
        session = self._get_session()
        r = session.post(
            self._api_url(
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages"
            ),
            json={"content": message},
        )
        r.raise_for_status()
        data = r.json()
        return data.get("message_id", data.get("id", ""))

    def get_message_status(
        self, space_id: str, conversation_id: str, message_id: str
    ) -> dict[str, Any]:
        """Get message status and response. Returns {status, text, attachments, sql}."""
        session = self._get_session()
        r = session.get(
            self._api_url(
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}"
            ),
        )
        r.raise_for_status()
        data = r.json()
        logger.info("get_message_status: keys=%s, status=%s", list(data.keys()), data.get("status"))

        status = data.get("status", "UNKNOWN")

        # Genie response text is in 'reply' or 'content' depending on API version
        text = ""
        reply = data.get("reply", {})
        if isinstance(reply, dict):
            text = reply.get("content", reply.get("text", ""))
        if not text:
            # Check attachments for text response
            for att in data.get("attachments", []):
                if att.get("text", {}).get("content"):
                    text = att["text"]["content"]
                    break
        if not text:
            text = data.get("content", "")

        attachments = []
        sql_query = ""

        for att in data.get("attachments", []):
            att_type = att.get("type", "")
            att_id = att.get("id", "")

            # Text attachment
            if att.get("text"):
                t = att["text"].get("content", "")
                if t and not text:
                    text = t

            # Query attachment
            if att.get("query"):
                q = att["query"]
                if isinstance(q, dict):
                    sql_query = q.get("query", q.get("content", ""))
                    att_id = att_id or q.get("id", "")
                else:
                    sql_query = str(q)
                attachments.append({"id": att_id, "type": "QUERY", "query": sql_query})
            elif att_type == "QUERY":
                attachments.append({"id": att_id, "type": "QUERY"})

        logger.info(
            "get_message_status result: status=%s, text_len=%d, attachments=%d",
            status,
            len(text),
            len(attachments),
        )

        return {
            "status": status,
            "text": text,
            "attachments": attachments,
            "sql": sql_query,
        }

    def get_query_result(
        self, space_id: str, conversation_id: str, message_id: str, attachment_id: str
    ) -> dict[str, Any]:
        """Get query result for an attachment. Returns {columns, rows}."""
        session = self._get_session()
        r = session.get(
            self._api_url(
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}"
                f"/messages/{message_id}/query-result/{attachment_id}"
            ),
        )
        r.raise_for_status()
        data = r.json()

        columns = []
        rows = []

        # Parse statement_response format
        stmt = data.get("statement_response", data)
        manifest = stmt.get("manifest", {})
        for col in manifest.get("schema", {}).get("columns", []):
            columns.append(col.get("name", ""))

        result = stmt.get("result", {})
        for chunk in result.get("data_array", []):
            rows.append(chunk)

        return {"columns": columns, "rows": rows}
