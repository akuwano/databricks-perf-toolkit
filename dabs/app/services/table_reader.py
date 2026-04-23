"""Table reader for loading past analysis results from Databricks Delta tables.

Provides read-back API so that past ProfileAnalysis records can be loaded
and fed into ComparisonService for before/after comparison.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from core.models import (
    AnalysisContext,
    BottleneckIndicators,
    ProfileAnalysis,
    QueryMetrics,
)

from .table_writer import TableWriterConfig

logger = logging.getLogger(__name__)


@dataclass
class AnalysisWithReport:
    """Full analysis with report markdown and metadata from Delta."""

    analysis: ProfileAnalysis
    report_markdown: str = ""
    warehouse_name: str = ""
    warehouse_size: str = ""
    action_card_count: int = 0
    critical_alert_count: int = 0
    high_alert_count: int = 0
    medium_alert_count: int = 0


@dataclass
class AnalysisSummary:
    """Lightweight summary of a stored analysis (for listing/selection)."""

    analysis_id: str = ""
    analyzed_at: datetime | None = None
    query_id: str = ""
    query_fingerprint: str = ""
    experiment_id: str = ""
    variant: str = ""
    total_time_ms: int = 0
    read_bytes: int = 0
    spill_bytes: int = 0
    warehouse_name: str = ""
    warehouse_size: str = ""
    action_card_count: int = 0
    critical_alert_count: int = 0
    lang: str = ""
    estimated_cost_usd: float | None = None
    # Whether EXPLAIN text was attached at analysis time. ``None`` means the
    # underlying row pre-dates the column (legacy analyses before the
    # has_explain migration).
    has_explain: bool | None = None


class TableReader:
    """Reads analysis results from Databricks managed Delta tables."""

    def __init__(self, config: TableWriterConfig) -> None:
        self._config = config

    def _fqn(self, table_name: str) -> str:
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
        """Get SQL connection using SP credentials."""
        from . import get_sp_sql_connection

        return get_sp_sql_connection(self._config.http_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_analysis_by_id(self, analysis_id: str) -> ProfileAnalysis | None:
        """Load a ProfileAnalysis by its analysis_id."""
        sql = f"""
            SELECT * FROM {self._fqn("profiler_analysis_header")}
            WHERE analysis_id = :analysis_id
            LIMIT 1
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters={"analysis_id": analysis_id})
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    columns = [desc[0] for desc in cursor.description]
                    return self._row_to_analysis(dict(zip(columns, row)))
        except Exception:
            logger.exception("Failed to load analysis: %s", analysis_id)
            return None

    def get_analysis_with_report(self, analysis_id: str) -> AnalysisWithReport | None:
        """Load ProfileAnalysis, report, and metadata by analysis_id."""
        sql = f"""
            SELECT * FROM {self._fqn("profiler_analysis_header")}
            WHERE analysis_id = :analysis_id
            LIMIT 1
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters={"analysis_id": analysis_id})
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    columns = [desc[0] for desc in cursor.description]
                    row_dict = dict(zip(columns, row))
                    return AnalysisWithReport(
                        analysis=self._row_to_analysis(row_dict),
                        report_markdown=row_dict.get("report_markdown", "") or "",
                        warehouse_name=row_dict.get("warehouse_name", "") or "",
                        warehouse_size=row_dict.get("warehouse_size", "") or "",
                        action_card_count=row_dict.get("action_card_count", 0) or 0,
                        critical_alert_count=row_dict.get("critical_alert_count", 0) or 0,
                        high_alert_count=row_dict.get("high_alert_count", 0) or 0,
                        medium_alert_count=row_dict.get("medium_alert_count", 0) or 0,
                    )
        except Exception:
            logger.exception("Failed to load analysis with report: %s", analysis_id)
            return None

    def get_analysis_summary(self, analysis_id: str) -> AnalysisSummary | None:
        """Load a lightweight summary for a single analysis."""
        # See list_analyses() for the SELECT * rationale (migration safety).
        sql = f"""
            SELECT *
            FROM {self._fqn("profiler_analysis_header")}
            WHERE analysis_id = :analysis_id
            LIMIT 1
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters={"analysis_id": analysis_id})
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    columns = [desc[0] for desc in cursor.description]
                    return self._row_to_summary(dict(zip(columns, row)))
        except Exception:
            logger.exception("Failed to load analysis summary: %s", analysis_id)
            return None

    def list_analyses(
        self,
        query_fingerprint: str | None = None,
        experiment_id: str | None = None,
        variant: str | None = None,
        limit: int = 50,
    ) -> list[AnalysisSummary]:
        """List stored analyses with optional filters."""
        conditions = []
        params: dict[str, Any] = {}

        if query_fingerprint:
            conditions.append("query_fingerprint = :fp")
            params["fp"] = query_fingerprint
        if experiment_id:
            conditions.append("experiment_id = :exp")
            params["exp"] = experiment_id
        if variant:
            conditions.append("variant = :var")
            params["var"] = variant

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        # Use SELECT * so that newly added columns (e.g., has_explain) do not
        # break this query before the writer-side migration has run against
        # a given deployment. _row_to_summary uses row.get() and tolerates
        # missing keys.
        sql = f"""
            SELECT *
            FROM {self._fqn("profiler_analysis_header")}
            {where}
            ORDER BY analyzed_at DESC
            LIMIT {limit}
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters=params)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    return [self._row_to_summary(dict(zip(columns, row))) for row in rows]
        except Exception:
            logger.exception("Failed to list analyses")
            return []

    def get_latest_by_fingerprint(
        self,
        query_fingerprint: str,
        experiment_id: str | None = None,
        variant: str | None = None,
    ) -> ProfileAnalysis | None:
        """Load the most recent analysis for a given fingerprint."""
        conditions = ["query_fingerprint = :fp"]
        params: dict[str, Any] = {"fp": query_fingerprint}

        if experiment_id:
            conditions.append("experiment_id = :exp")
            params["exp"] = experiment_id
        if variant:
            conditions.append("variant = :var")
            params["var"] = variant

        where = " AND ".join(conditions)
        sql = f"""
            SELECT * FROM {self._fqn("profiler_analysis_header")}
            WHERE {where}
            ORDER BY analyzed_at DESC
            LIMIT 1
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters=params)
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    columns = [desc[0] for desc in cursor.description]
                    return self._row_to_analysis(dict(zip(columns, row)))
        except Exception:
            logger.exception(
                "Failed to load latest analysis for fingerprint: %s", query_fingerprint
            )
            return None

    def find_baseline(
        self,
        query_family_id: str,
        experiment_id: str | None = None,
    ) -> ProfileAnalysis | None:
        """Find the most recent baseline analysis for a query family.

        Searches for analyses with baseline_flag=true matching the
        given query_family_id (and optionally experiment_id).
        """
        conditions = ["query_family_id = :fam", "baseline_flag = true"]
        params: dict[str, Any] = {"fam": query_family_id}

        if experiment_id:
            conditions.append("experiment_id = :exp")
            params["exp"] = experiment_id

        where = " AND ".join(conditions)
        sql = f"""
            SELECT * FROM {self._fqn("profiler_analysis_header")}
            WHERE {where}
            ORDER BY analyzed_at DESC
            LIMIT 1
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters=params)
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    columns = [desc[0] for desc in cursor.description]
                    return self._row_to_analysis(dict(zip(columns, row)))
        except Exception:
            logger.exception("Failed to find baseline for family: %s", query_family_id)
            return None

    # ------------------------------------------------------------------
    # Row mapping
    # ------------------------------------------------------------------

    def _row_to_analysis(self, row: dict[str, Any]) -> ProfileAnalysis:
        """Convert a header row dict to a ProfileAnalysis."""
        return ProfileAnalysis(
            query_metrics=QueryMetrics(
                query_id=row.get("query_id", ""),
                status=row.get("query_status", ""),
                query_text=row.get("query_text", ""),
                total_time_ms=row.get("total_time_ms", 0) or 0,
                compilation_time_ms=row.get("compilation_time_ms", 0) or 0,
                execution_time_ms=row.get("execution_time_ms", 0) or 0,
                read_bytes=row.get("read_bytes", 0) or 0,
                read_remote_bytes=row.get("read_remote_bytes", 0) or 0,
                read_cache_bytes=row.get("read_cache_bytes", 0) or 0,
                spill_to_disk_bytes=row.get("spill_to_disk_bytes", 0) or 0,
                photon_total_time_ms=row.get("photon_total_time_ms", 0) or 0,
                task_total_time_ms=row.get("task_total_time_ms", 0) or 0,
                read_files_count=row.get("read_files_count", 0) or 0,
                pruned_files_count=row.get("pruned_files_count", 0) or 0,
                pruned_bytes=row.get("pruned_bytes", 0) or 0,
                rows_read_count=row.get("rows_read_count", 0) or 0,
                rows_produced_count=row.get("rows_produced_count", 0) or 0,
                bytes_read_from_cache_percentage=row.get("bytes_read_from_cache_percentage", 0)
                or 0,
                write_remote_bytes=row.get("write_remote_bytes", 0) or 0,
                write_remote_files=row.get("write_remote_files", 0) or 0,
                network_sent_bytes=row.get("network_sent_bytes", 0) or 0,
                read_partitions_count=row.get("read_partitions_count", 0) or 0,
            ),
            bottleneck_indicators=BottleneckIndicators(
                cache_hit_ratio=row.get("cache_hit_ratio", 0.0) or 0.0,
                remote_read_ratio=row.get("remote_read_ratio", 0.0) or 0.0,
                photon_ratio=row.get("photon_ratio", 0.0) or 0.0,
                spill_bytes=row.get("spill_bytes", 0) or 0,
                filter_rate=row.get("filter_rate", 0.0) or 0.0,
                bytes_pruning_ratio=row.get("bytes_pruning_ratio", 0.0) or 0.0,
                shuffle_impact_ratio=row.get("shuffle_impact_ratio", 0.0) or 0.0,
                cloud_storage_retry_ratio=row.get("cloud_storage_retry_ratio", 0.0) or 0.0,
                has_data_skew=bool(row.get("has_data_skew", False)),
                skewed_partitions=row.get("skewed_partitions", 0) or 0,
                rescheduled_scan_ratio=row.get("rescheduled_scan_ratio", 0.0) or 0.0,
                oom_fallback_count=row.get("oom_fallback_count", 0) or 0,
            ),
            endpoint_id=row.get("endpoint_id", ""),
            analysis_context=AnalysisContext(
                query_fingerprint=row.get("query_fingerprint", "") or "",
                query_fingerprint_version=row.get("query_fingerprint_version", "") or "",
                experiment_id=row.get("experiment_id", "") or "",
                variant=row.get("variant", "") or "",
                variant_group=row.get("variant_group", "") or "",
                baseline_flag=bool(row.get("baseline_flag", False)),
                tags=json.loads(row["tags_json"]) if row.get("tags_json") else {},
                source_run_id=row.get("source_run_id", "") or "",
                source_job_id=row.get("source_job_id", "") or "",
                source_job_run_id=row.get("source_job_run_id", "") or "",
                analysis_notes=row.get("analysis_notes", "") or "",
                query_text_normalized=row.get("query_text_normalized", "") or "",
                query_family_id=row.get("query_family_id", "") or "",
                purpose_signature=row.get("purpose_signature", "") or "",
                variant_type=row.get("variant_type", "") or "",
                feature_json=row.get("feature_json", "") or "",
            ),
        )

    def _row_to_summary(self, row: dict[str, Any]) -> AnalysisSummary:
        """Convert a summary row dict to an AnalysisSummary."""
        raw_has_explain = row.get("has_explain")
        has_explain = None if raw_has_explain is None else bool(raw_has_explain)
        return AnalysisSummary(
            analysis_id=row.get("analysis_id", ""),
            analyzed_at=row.get("analyzed_at"),
            query_id=row.get("query_id", "") or "",
            query_fingerprint=row.get("query_fingerprint", "") or "",
            experiment_id=row.get("experiment_id", "") or "",
            variant=row.get("variant", "") or "",
            total_time_ms=row.get("total_time_ms", 0) or 0,
            read_bytes=row.get("read_bytes", 0) or 0,
            spill_bytes=row.get("spill_bytes", 0) or 0,
            warehouse_name=row.get("warehouse_name", "") or "",
            warehouse_size=row.get("warehouse_size", "") or "",
            action_card_count=row.get("action_card_count", 0) or 0,
            critical_alert_count=row.get("critical_alert_count", 0) or 0,
            lang=row.get("lang", "") or "",
            estimated_cost_usd=row.get("estimated_cost_usd"),
            has_explain=has_explain,
        )

    # -----------------------------------------------------------------------
    # Compare result history
    # -----------------------------------------------------------------------

    def list_compare_results(self, limit: int = 50) -> list[dict[str, Any]]:
        """List past comparison results (without report_markdown)."""
        sql = f"""
            SELECT
                comparison_id, compared_at,
                baseline_analyzed_at, baseline_query_id,
                baseline_experiment, baseline_variant,
                baseline_duration_ms, baseline_alerts,
                candidate_analyzed_at, candidate_query_id,
                candidate_experiment, candidate_variant,
                candidate_duration_ms, candidate_alerts,
                regression_detected, regression_severity, net_score
            FROM {self._fqn("profiler_compare_result")}
            ORDER BY compared_at DESC
            LIMIT {limit}
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    return [dict(zip(columns, row)) for row in rows]
        except Exception:
            logger.exception("Failed to list compare results")
            return []

    def get_compare_result(self, comparison_id: str) -> dict[str, Any] | None:
        """Load a single compare result including report_markdown."""
        sql = f"""
            SELECT * FROM {self._fqn("profiler_compare_result")}
            WHERE comparison_id = :comparison_id
            LIMIT 1
        """
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql, parameters={"comparison_id": comparison_id})
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    columns = [desc[0] for desc in cursor.description]
                    return dict(zip(columns, row))
        except Exception:
            logger.exception("Failed to load compare result: %s", comparison_id)
            return None
