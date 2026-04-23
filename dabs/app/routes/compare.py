"""Blueprint for comparison routes: /compare, /api/v1/compare."""

import logging
import re

from flask import Blueprint, jsonify, render_template, request

logger = logging.getLogger(__name__)


def _extract_net_score(summary: str) -> float:
    """Extract net_score from comparison summary text."""
    m = re.search(r"Net score:\s*([+-]?\d+\.?\d*)", summary or "")
    return float(m.group(1)) if m else 0.0


bp = Blueprint("compare", __name__)


@bp.route("/compare")
def compare_page():
    """Comparison page for selecting two analyses."""
    from app import get_databricks_credentials, get_locale
    from core.serving_client import list_chat_models

    databricks_host, _ = get_databricks_credentials()
    models = list_chat_models(host=databricks_host)
    return render_template(
        "compare.html",
        current_lang=get_locale(),
        available_models=models,
    )


@bp.route("/api/v1/compare", methods=["POST"])
def run_compare():
    """Compare two analyses by their analysis_ids."""
    from app import NotFoundError, UserInputError, get_databricks_credentials, get_locale
    from core.comparison_reporter import generate_comparison_report
    from core.models import ComparisonRequest
    from core.usecases import run_comparison_pipeline
    from services.table_reader import TableReader
    from services.table_writer import TableWriterConfig

    data = request.get_json()
    if not data:
        raise UserInputError("JSON body required")

    baseline_id = data.get("baseline_analysis_id", "")
    candidate_id = data.get("candidate_analysis_id", "")

    if not baseline_id or not candidate_id:
        raise UserInputError("baseline_analysis_id and candidate_analysis_id are required")

    config = TableWriterConfig.from_env()
    if not config.http_path:
        return jsonify({"error": "PROFILER_WAREHOUSE_HTTP_PATH not configured"}), 503

    reader = TableReader(config)

    baseline = reader.get_analysis_by_id(baseline_id)
    if baseline is None:
        raise NotFoundError(f"Baseline analysis not found: {baseline_id}")

    candidate = reader.get_analysis_by_id(candidate_id)
    if candidate is None:
        raise NotFoundError(f"Candidate analysis not found: {candidate_id}")

    req = ComparisonRequest(
        baseline_analysis_id=baseline_id,
        candidate_analysis_id=candidate_id,
        request_source="web",
    )
    comparison = run_comparison_pipeline(baseline, candidate, req)

    # LLM analysis
    llm_summary = ""
    if data.get("enable_llm_summary", False):
        from core.comparison_llm import generate_comparison_llm_summary

        databricks_host, databricks_token = get_databricks_credentials()
        # Fallback: try SDK auth if env vars not set
        if not databricks_host:
            try:
                from databricks.sdk.core import Config

                cfg = Config()
                databricks_host = cfg.host
            except Exception:
                pass

        if databricks_host:
            llm_summary = generate_comparison_llm_summary(
                comparison,
                model=data.get("model", "databricks-claude-sonnet-4-5"),
                databricks_host=databricks_host,
                databricks_token=databricks_token or "",
                lang=get_locale(),
            )
            logger.info("LLM comparison summary: %d chars", len(llm_summary))

    report = generate_comparison_report(comparison, llm_summary=llm_summary, lang=get_locale())

    # Persist comparison
    try:
        from services.table_writer import TableWriter

        writer = TableWriter(config)
        writer.write_comparison_result(comparison)

        # Persist flat compare result for history display
        baseline_summary = reader.get_analysis_summary(baseline_id)
        candidate_summary = reader.get_analysis_summary(candidate_id)
        writer.write_compare_result(
            comparison_id=comparison.comparison_id,
            baseline={
                "analyzed_at": baseline_summary.analyzed_at if baseline_summary else None,
                "query_id": baseline.query_metrics.query_id,
                "experiment": baseline.analysis_context.experiment_id,
                "variant": baseline.analysis_context.variant,
                "duration_ms": baseline.query_metrics.total_time_ms,
                "alerts": baseline_summary.critical_alert_count if baseline_summary else 0,
            },
            candidate={
                "analyzed_at": candidate_summary.analyzed_at if candidate_summary else None,
                "query_id": candidate.query_metrics.query_id,
                "experiment": candidate.analysis_context.experiment_id,
                "variant": candidate.analysis_context.variant,
                "duration_ms": candidate.query_metrics.total_time_ms,
                "alerts": candidate_summary.critical_alert_count if candidate_summary else 0,
            },
            regression_detected=comparison.regression_detected,
            regression_severity=comparison.regression_severity,
            report_markdown=report,
            net_score=_extract_net_score(comparison.summary),
        )
    except Exception as e:
        logger.warning("Comparison persist failed (non-fatal): %s", e)

    return jsonify(
        {
            "comparison_id": comparison.comparison_id,
            "regression_detected": comparison.regression_detected,
            "regression_severity": comparison.regression_severity,
            "summary": comparison.summary,
            "llm_analysis_included": bool(llm_summary),
            "report_markdown": report,
            "metric_diffs": [
                {
                    "metric_name": m.metric_name,
                    "metric_group": m.metric_group,
                    "baseline_value": m.baseline_value,
                    "candidate_value": m.candidate_value,
                    "percent_diff": (
                        m.relative_diff_ratio * 100 if m.relative_diff_ratio is not None else None
                    ),
                    "regression_flag": m.regression_flag,
                    "improvement_flag": m.improvement_flag,
                    "severity": m.severity,
                }
                for m in comparison.metric_diffs
            ],
        }
    )


@bp.route("/api/v1/compare/history")
def compare_history():
    """List past SQL comparison results."""
    from services.table_reader import TableReader
    from services.table_writer import TableWriterConfig

    config = TableWriterConfig.from_env()
    if not config.http_path:
        return jsonify([])

    reader = TableReader(config)
    results = reader.list_compare_results(limit=200)

    # Serialize datetime for JSON
    for r in results:
        for key in ("compared_at", "baseline_analyzed_at", "candidate_analyzed_at"):
            if r.get(key) is not None:
                r[key] = str(r[key])

    return jsonify(results)


@bp.route("/api/v1/compare/history/delete", methods=["POST"])
def compare_history_delete():
    """Delete SQL comparison results by comparison_ids."""
    from services.table_reader import TableReader
    from services.table_writer import TableWriterConfig

    data = request.get_json()
    if not data or not data.get("comparison_ids"):
        return jsonify({"error": "comparison_ids required"}), 400

    config = TableWriterConfig.from_env()
    if not config.http_path:
        return jsonify({"error": "Warehouse not configured"}), 503

    reader = TableReader(config)
    ids = data["comparison_ids"]
    deleted = 0
    try:
        with reader._get_connection() as conn:
            with conn.cursor() as cursor:
                fqn = reader._fqn("profiler_compare_result")
                for cid in ids:
                    cursor.execute(
                        f"DELETE FROM {fqn} WHERE comparison_id = :cid",
                        parameters={"cid": cid},
                    )
                    deleted += 1
    except Exception as e:
        logger.exception("Failed to delete compare results")
        return jsonify({"error": str(e), "deleted": deleted, "requested": len(ids)}), 502

    return jsonify({"deleted": deleted, "requested": len(ids)})


@bp.route("/compare/report/<comparison_id>")
def compare_report_page(comparison_id: str):
    """Display a SQL comparison report."""
    from app import NotFoundError, get_locale
    from services.table_reader import TableReader
    from services.table_writer import TableWriterConfig

    config = TableWriterConfig.from_env()
    if not config.http_path:
        raise NotFoundError("Warehouse not configured")

    reader = TableReader(config)
    result = reader.get_compare_result(comparison_id)
    if result is None:
        raise NotFoundError(f"Comparison not found: {comparison_id}")

    return render_template(
        "compare_report.html",
        comparison=result,
        compare_type="sql",
        current_lang=get_locale(),
    )
