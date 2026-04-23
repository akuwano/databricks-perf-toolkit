"""Blueprint for Spark Performance routes: /spark-perf, /api/v1/spark-perf/*."""

import logging
import re
import time

from flask import Blueprint, Response, jsonify, render_template, request

logger = logging.getLogger(__name__)


def _extract_net_score(summary: str) -> float:
    """Extract net_score from comparison summary text."""
    m = re.search(r"Net score:\s*([+-]?\d+\.?\d*)", summary or "")
    return float(m.group(1)) if m else 0.0


bp = Blueprint("spark_perf", __name__)


def _sanitize_summary_text(text: str) -> str:
    """Fix legacy data where summary_text contains raw LLM JSON.

    New reports are saved correctly by parse_spark_perf_response().
    This handles old records that were saved before the fix.
    """
    from core.llm_prompts.spark_perf_prompts import _extract_json_from_text

    stripped = text.strip()
    if not (stripped.startswith("{") or stripped.startswith("```")):
        return text
    data = _extract_json_from_text(text)
    if data and data.get("summary_text"):
        extracted = data["summary_text"]
        # Find remainder after JSON (appended Call 2 sections)
        import json

        json_str = json.dumps(data, ensure_ascii=False)
        idx = text.find("}", len(json_str) - 10)
        if idx >= 0:
            remainder = text[idx + 1 :].strip()
            if remainder:
                return extracted + "\n\n" + remainder
        return extracted
    return text


def _get_spark_perf_reader():
    """Create a SparkPerfReader from saved config."""
    from services.spark_perf_reader import SparkPerfConfig, SparkPerfReader

    config = SparkPerfConfig.from_env()
    if not config.http_path:
        return None
    return SparkPerfReader(config)


@bp.route("/spark-perf")
def spark_perf_page():
    """Spark Job Analysis page."""
    from app import APP_VERSION, get_locale
    from core.serving_client import list_chat_models

    return render_template(
        "spark_perf.html",
        current_lang=get_locale(),
        available_models=list_chat_models(),
        app_version=APP_VERSION,
    )


@bp.route("/spark-perf/report/<app_id>")
def spark_perf_report_view(app_id: str):
    """Dedicated Spark Performance report view page."""
    from app import get_locale

    reader = _get_spark_perf_reader()
    if reader is None:
        return "Spark Perf not configured", 503

    # Fetch KPI data (lightweight, safe to embed inline)
    summary = reader.get_application_summary(app_id) or {}
    bottlenecks = reader.get_bottleneck_report(app_id)

    bottleneck_count = len(
        [b for b in bottlenecks if (b.get("severity") or "").upper() in ("HIGH", "MEDIUM")]
    )

    # Report markdown is fetched async via /api/v1/spark-perf/narrative/<app_id>
    return render_template(
        "spark_perf_report.html",
        app_id=app_id,
        summary=summary,
        bottleneck_count=bottleneck_count,
        current_lang=get_locale(),
    )


@bp.route("/api/v1/spark-perf/narrative/<app_id>")
def spark_perf_narrative_by_app(app_id: str):
    """Get narrative report for a specific app_id (used by report view page)."""
    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"report": ""}), 503

    narrative = reader.get_narrative_summary(app_id=app_id)
    if not narrative or not narrative.get("summary_text"):
        return jsonify({"report": ""})

    parts = [_sanitize_summary_text(narrative["summary_text"])]
    if narrative.get("top3_text"):
        parts.append("\n---\n")
        parts.append(narrative["top3_text"])

    return jsonify({"report": "\n".join(parts)})


@bp.route("/api/v1/spark-perf/narrative/<app_id>/download")
def spark_perf_narrative_download(app_id: str):
    """Download Spark Perf narrative report as Markdown file."""
    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf not configured"}), 503

    narrative = reader.get_narrative_summary(app_id=app_id)
    if not narrative or not narrative.get("summary_text"):
        return jsonify({"error": "No report available"}), 404

    parts = [_sanitize_summary_text(narrative["summary_text"])]
    if narrative.get("top3_text"):
        parts.append("\n---\n")
        parts.append(narrative["top3_text"])

    md = "\n".join(parts)
    filename = f"spark_report_{app_id[:24]}.md"

    return Response(
        md,
        mimetype="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@bp.route("/api/v1/spark-perf/narratives/update", methods=["POST"])
def spark_perf_update_narrative():
    """Update experiment_id and/or variant for a Spark app narrative."""
    data = request.get_json(silent=True) or {}
    app_id = data.get("app_id", "").strip()
    if not app_id:
        return jsonify({"error": "app_id is required"}), 400

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf not configured"}), 503

    updates = []
    params = {"app_id": app_id}
    if "experiment_id" in data:
        updates.append("experiment_id = :exp")
        params["exp"] = data["experiment_id"].strip() or None
    if "variant" in data:
        updates.append("variant = :var")
        params["var"] = data["variant"].strip() or None

    if not updates:
        return jsonify({"error": "Nothing to update"}), 400

    try:
        with reader._get_connection() as conn:
            with conn.cursor() as cursor:
                # Update narrative summary
                fqn = reader._fqn("gold_narrative_summary")
                sql = f"UPDATE {fqn} SET {', '.join(updates)} WHERE app_id = :app_id"
                cursor.execute(sql, parameters=params)

                # Cascade to spark_compare_result (baseline side)
                compare_fqn = reader._fqn("spark_compare_result")
                b_updates = []
                b_params = {"app_id": app_id}
                if "experiment_id" in data:
                    b_updates.append("baseline_experiment = :exp")
                    b_params["exp"] = data["experiment_id"].strip() or None
                if "variant" in data:
                    b_updates.append("baseline_variant = :var")
                    b_params["var"] = data["variant"].strip() or None
                try:
                    cursor.execute(
                        f"UPDATE {compare_fqn} SET {', '.join(b_updates)} WHERE baseline_app_id = :app_id",
                        parameters=b_params,
                    )
                except Exception:
                    pass

                # Cascade to spark_compare_result (candidate side)
                c_updates = []
                c_params = {"app_id": app_id}
                if "experiment_id" in data:
                    c_updates.append("candidate_experiment = :exp")
                    c_params["exp"] = data["experiment_id"].strip() or None
                if "variant" in data:
                    c_updates.append("candidate_variant = :var")
                    c_params["var"] = data["variant"].strip() or None
                try:
                    cursor.execute(
                        f"UPDATE {compare_fqn} SET {', '.join(c_updates)} WHERE candidate_app_id = :app_id",
                        parameters=c_params,
                    )
                except Exception:
                    pass

        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("Failed to update narrative")
        return jsonify({"error": str(e)}), 502


@bp.route("/api/v1/spark-perf/narratives/delete", methods=["POST"])
def spark_perf_delete_narratives():
    """Delete Spark narrative summaries by app_ids (narratives only)."""
    data = request.get_json(silent=True) or {}
    app_ids = data.get("app_ids", [])
    if not app_ids:
        return jsonify({"error": "app_ids is required"}), 400

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf not configured"}), 503

    deleted = 0
    try:
        fqn = reader._fqn("gold_narrative_summary")
        with reader._get_connection() as conn:
            with conn.cursor() as cursor:
                for app_id in app_ids:
                    cursor.execute(
                        f"DELETE FROM {fqn} WHERE app_id = :app_id", parameters={"app_id": app_id}
                    )
                    deleted += 1
    except Exception as e:
        logger.exception("Failed to delete narratives")
        return jsonify(
            {"error": f"Delete failed: {e}", "deleted": deleted, "requested": len(app_ids)}
        ), 502

    return jsonify({"deleted": deleted, "requested": len(app_ids)})


# All Silver/Gold tables that have app_id column
_ALL_APP_ID_TABLES = [
    # Silver
    "silver_application_events",
    "silver_job_events",
    "silver_stage_events",
    "silver_task_events",
    "silver_executor_events",
    "silver_resource_profiles",
    "silver_spark_config",
    "silver_sql_executions",
    "silver_streaming_events",
    # Gold
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
    "gold_streaming_query_summary",
    "gold_streaming_batch_detail",
    # Narrative
    "gold_narrative_summary",
]


@bp.route("/api/v1/spark-perf/applications/delete", methods=["POST"])
def spark_perf_delete_applications():
    """Delete all data for given app_ids from all Silver/Gold/Narrative tables."""
    data = request.get_json(silent=True) or {}
    app_ids = data.get("app_ids", [])
    if not app_ids:
        return jsonify({"error": "app_ids is required"}), 400

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf not configured"}), 503

    tables_deleted = {}
    try:
        with reader._get_connection() as conn:
            with conn.cursor() as cursor:
                for table_suffix in _ALL_APP_ID_TABLES:
                    fqn = reader._fqn(table_suffix)
                    count = 0
                    for app_id in app_ids:
                        try:
                            cursor.execute(
                                f"DELETE FROM {fqn} WHERE app_id = :app_id",
                                parameters={"app_id": app_id},
                            )
                            count += 1
                        except Exception as te:
                            logger.warning("Delete from %s failed (may not exist): %s", fqn, te)
                    if count:
                        tables_deleted[table_suffix] = count
    except Exception as e:
        logger.exception("Failed to delete application data")
        return jsonify({"error": f"Delete failed: {e}", "tables_deleted": tables_deleted}), 502

    return jsonify(
        {
            "deleted_app_ids": app_ids,
            "tables_cleaned": len(tables_deleted),
            "details": tables_deleted,
        }
    )


@bp.route("/api/v1/spark-perf/applications")
def spark_perf_applications():
    """List Spark applications with pagination."""
    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("page_size", 20, type=int)
    page_size = min(page_size, 100)  # cap

    total = reader.count_applications()
    items = reader.list_applications(page=page, page_size=page_size)

    return jsonify(
        {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
        }
    )


@bp.route("/api/v1/spark-perf/summary")
def spark_perf_summary():
    """Get application summary + bottleneck report."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    summary = reader.get_application_summary(app_id)
    bottlenecks = reader.get_bottleneck_report(app_id)

    return jsonify(
        {
            "summary": summary,
            "bottlenecks": bottlenecks,
        }
    )


@bp.route("/api/v1/spark-perf/stages")
def spark_perf_stages():
    """Get stage performance."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    return jsonify(reader.get_stage_performance(app_id))


@bp.route("/api/v1/spark-perf/executors")
def spark_perf_executors():
    """Get executor analysis."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    return jsonify(reader.get_executor_analysis(app_id))


@bp.route("/api/v1/spark-perf/concurrency")
def spark_perf_concurrency():
    """Get job concurrency analysis."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    return jsonify(reader.get_job_concurrency(app_id))


@bp.route("/api/v1/spark-perf/jobs")
def spark_perf_jobs():
    """Get job performance."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    return jsonify(reader.get_job_performance(app_id))


@bp.route("/api/v1/spark-perf/narrative")
def spark_perf_narrative():
    """Get LLM narrative summary, optionally translated."""
    from app import get_locale

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    result = reader.get_narrative_summary()
    if not result:
        return jsonify(
            {"summary_text": "", "job_analysis_text": "", "node_analysis_text": "", "top3_text": ""}
        )

    # Translate if lang parameter differs from source language
    target_lang = request.args.get("lang", get_locale())
    model = request.args.get("model", "databricks-claude-sonnet-4-6")
    if target_lang == "en" and result.get("summary_text"):
        # Check if text is likely Japanese (contains CJK characters)
        if re.search(r"[\u3000-\u9fff]", result.get("summary_text", "")):
            result = _translate_narrative(result, target_lang, model=model)

    return jsonify(result)


def _translate_narrative(
    result: dict, target_lang: str, model: str = "databricks-claude-sonnet-4-6"
) -> dict:
    """Translate narrative summary via LLM."""
    try:
        from app import get_databricks_credentials
        from core.llm_client import call_llm_with_retry, create_openai_client

        databricks_host, databricks_token = get_databricks_credentials()
        if not databricks_host:
            return result

        client = create_openai_client(databricks_host, databricks_token or "")
        translated = {}
        for key in ("summary_text", "job_analysis_text", "node_analysis_text", "top3_text"):
            text = result.get(key, "")
            if not text:
                translated[key] = ""
                continue

            messages = [
                {
                    "role": "system",
                    "content": "You are a translator. Translate the following text to English. Keep all Markdown formatting, metrics, and technical terms intact. Do not add explanations.",
                },
                {"role": "user", "content": text},
            ]
            translated[key] = call_llm_with_retry(
                client=client,
                model=model,
                messages=messages,
                max_tokens=2048,
                temperature=0.1,
            )
        return translated
    except Exception as e:
        logger.warning("Narrative translation failed: %s", e)
        return result


@bp.route("/api/v1/spark-perf/spot")
def spark_perf_spot():
    """Get spot instance / node loss analysis."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    return jsonify(reader.get_spot_instance_analysis(app_id))


@bp.route("/api/v1/spark-perf/sql-photon")
def spark_perf_sql_photon():
    """Get SQL/Photon analysis."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    return jsonify(reader.get_sql_photon_analysis(app_id))


@bp.route("/api/v1/spark-perf/streaming")
def spark_perf_streaming():
    """Get streaming query data for a specific app."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    queries = reader.get_streaming_query_summary(app_id)
    summary = reader.get_streaming_summary(app_id)
    return jsonify({"queries": queries, "summary": summary})


@bp.route("/api/v1/spark-perf/streaming/batches")
def spark_perf_streaming_batches():
    """Get streaming batch detail for time-series charts."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    query_id = request.args.get("query_id")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    batches = reader.get_streaming_batch_detail(app_id, query_id=query_id)
    return jsonify({"batches": batches})


@bp.route("/api/v1/spark-perf/compare-list")
def spark_perf_compare_list():
    """List applications available for comparison, optionally filtered by cluster_id."""
    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    cluster_id = request.args.get("cluster_id", "").strip()
    experiment_id = request.args.get("experiment_id", "").strip()
    variant = request.args.get("variant", "").strip()
    try:
        items = reader.list_applications_for_compare(
            cluster_id=cluster_id or None,
            experiment_id=experiment_id or None,
            variant=variant or None,
        )
    except Exception:
        logger.exception("Failed to list applications for comparison")
        return jsonify({"error": "Failed to list applications"}), 500

    return jsonify({"items": items})


@bp.route("/api/v1/spark-perf/compare", methods=["POST"])
def spark_perf_compare():
    """Compare two Spark applications using direction-aware metric diffs."""
    from app import UserInputError, get_databricks_credentials, get_locale
    from core.spark_comparison import compare_spark_apps
    from core.spark_comparison_reporter import generate_spark_comparison_report

    data = request.get_json(silent=True)
    if not data:
        raise UserInputError("JSON body required")

    baseline_app_id = (data.get("baseline_app_id") or "").strip()
    candidate_app_id = (data.get("candidate_app_id") or "").strip()

    if not baseline_app_id or not candidate_app_id:
        raise UserInputError("baseline_app_id and candidate_app_id are required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    # Fetch data for both apps
    baseline_summary = reader.get_application_summary(baseline_app_id)
    if baseline_summary is None:
        raise UserInputError(f"Baseline application not found: {baseline_app_id}")

    candidate_summary = reader.get_application_summary(candidate_app_id)
    if candidate_summary is None:
        raise UserInputError(f"Candidate application not found: {candidate_app_id}")

    baseline_stages = reader.get_stage_performance(baseline_app_id)
    candidate_stages = reader.get_stage_performance(candidate_app_id)
    baseline_streaming = reader.get_streaming_query_summary(baseline_app_id)
    candidate_streaming = reader.get_streaming_query_summary(candidate_app_id)

    # Run comparison
    comparison = compare_spark_apps(
        baseline_summary=baseline_summary,
        candidate_summary=candidate_summary,
        baseline_stages=baseline_stages,
        candidate_stages=candidate_stages,
        baseline_streaming=baseline_streaming,
        candidate_streaming=candidate_streaming,
    )

    # Optional LLM summary
    llm_summary = ""
    if data.get("enable_llm_summary", False):
        from core.comparison_llm import generate_comparison_llm_summary

        databricks_host, databricks_token = get_databricks_credentials()
        if not databricks_host:
            try:
                from databricks.sdk.core import Config

                cfg = Config()
                databricks_host = cfg.host
            except Exception as e:
                logger.warning("Failed to resolve Databricks host from SDK: %s", e)

        if databricks_host:
            llm_summary = generate_comparison_llm_summary(
                comparison,
                model=data.get("model", "databricks-claude-sonnet-4-5"),
                databricks_host=databricks_host,
                databricks_token=databricks_token or "",
                lang=get_locale(),
                context="spark",
            )
            logger.info("Spark LLM comparison summary: %d chars", len(llm_summary))

    # Generate report
    report = generate_spark_comparison_report(
        comparison,
        baseline_summary=baseline_summary,
        candidate_summary=candidate_summary,
        llm_summary=llm_summary,
        lang=get_locale(),
    )

    # Persist comparison
    try:
        from services.spark_comparison_writer import SparkComparisonWriter
        from services.spark_perf_reader import SparkPerfConfig

        config = SparkPerfConfig.from_env()
        writer = SparkComparisonWriter(config)
        cluster_id = baseline_summary.get("cluster_id") or candidate_summary.get("cluster_id") or ""
        writer.write_comparison(comparison, cluster_id=str(cluster_id))

        # Persist flat compare result for history display
        # Fetch narrative metadata (analyzed_at, experiment, variant)
        baseline_narr = reader.get_narrative_summary(baseline_app_id) or {}
        candidate_narr = reader.get_narrative_summary(candidate_app_id) or {}
        baseline_bottlenecks = reader.get_bottleneck_report(baseline_app_id)
        candidate_bottlenecks = reader.get_bottleneck_report(candidate_app_id)
        # Count only HIGH/MEDIUM severity (matching the compare list display)
        _hm = ("HIGH", "MEDIUM")
        baseline_alert_count = len(
            [b for b in baseline_bottlenecks if (b.get("severity") or "").upper() in _hm]
        )
        candidate_alert_count = len(
            [b for b in candidate_bottlenecks if (b.get("severity") or "").upper() in _hm]
        )
        writer.write_compare_result(
            comparison_id=comparison.comparison_id,
            baseline={
                "analyzed_at": baseline_narr.get("generated_at"),
                "app_id": baseline_app_id,
                "experiment": baseline_narr.get("experiment_id") or "",
                "variant": baseline_narr.get("variant") or "",
                "duration_ms": baseline_summary.get("duration_ms"),
                "alerts": baseline_alert_count,
            },
            candidate={
                "analyzed_at": candidate_narr.get("generated_at"),
                "app_id": candidate_app_id,
                "experiment": candidate_narr.get("experiment_id") or "",
                "variant": candidate_narr.get("variant") or "",
                "duration_ms": candidate_summary.get("duration_ms"),
                "alerts": candidate_alert_count,
            },
            regression_detected=comparison.regression_detected,
            regression_severity=comparison.regression_severity,
            report_markdown=report,
            net_score=_extract_net_score(comparison.summary),
        )
    except Exception as e:
        logger.warning("Spark comparison persist failed (non-fatal): %s", e)

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


@bp.route("/api/v1/spark-perf/compare/history")
def spark_compare_history():
    """List past Spark comparison results."""
    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify([])

    results = reader.list_compare_results(limit=200)

    for r in results:
        for key in ("compared_at", "baseline_analyzed_at", "candidate_analyzed_at"):
            if r.get(key) is not None:
                r[key] = str(r[key])

    return jsonify(results)


@bp.route("/api/v1/spark-perf/compare/history/delete", methods=["POST"])
def spark_compare_history_delete():
    """Delete Spark comparison results by comparison_ids."""
    data = request.get_json(silent=True) or {}
    ids = data.get("comparison_ids", [])
    if not ids:
        return jsonify({"error": "comparison_ids required"}), 400

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf not configured"}), 503

    deleted = 0
    try:
        fqn = reader._fqn("spark_compare_result")
        with reader._get_connection() as conn:
            with conn.cursor() as cursor:
                for cid in ids:
                    cursor.execute(
                        f"DELETE FROM {fqn} WHERE comparison_id = :cid",
                        parameters={"cid": cid},
                    )
                    deleted += 1
    except Exception as e:
        logger.exception("Failed to delete spark compare results")
        return jsonify({"error": str(e), "deleted": deleted, "requested": len(ids)}), 502

    return jsonify({"deleted": deleted, "requested": len(ids)})


@bp.route("/compare/spark-report/<comparison_id>")
def spark_compare_report_page(comparison_id: str):
    """Display a Spark comparison report."""
    from app import NotFoundError, get_locale

    reader = _get_spark_perf_reader()
    if reader is None:
        raise NotFoundError("Spark Perf not configured")

    result = reader.get_compare_result(comparison_id)
    if result is None:
        raise NotFoundError(f"Spark comparison not found: {comparison_id}")

    return render_template(
        "compare_report.html",
        comparison=result,
        compare_type="spark",
        current_lang=get_locale(),
    )


@bp.route("/api/v1/spark-perf/report")
def spark_perf_report():
    """Return LLM narrative report, or a message if not yet generated."""
    from app import UserInputError

    app_id = request.args.get("app_id")
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf HTTP path not configured"}), 503

    lang = request.args.get("lang", "en")
    model = request.args.get("model", "databricks-claude-sonnet-4-6")
    logger.info("spark_perf_report: app_id=%s, lang=%s, model=%s", app_id, lang, model)

    # Try to use LLM-generated narrative from gold_narrative_summary first
    narrative = reader.get_narrative_summary(app_id=app_id)
    if narrative and narrative.get("summary_text"):
        report_parts = []
        summary_text = _sanitize_summary_text(narrative.get("summary_text", ""))
        top3_text = narrative.get("top3_text", "")
        if summary_text:
            report_parts.append(summary_text)
        if top3_text:
            report_parts.append("\n---\n")
            report_parts.append(top3_text)
        report = "\n".join(report_parts)

        return jsonify({"report": report})

    # No narrative — prompt user to create report
    msg = (
        "No analysis report found for this application.\n\n"
        "Please run **Create Report** to generate the LLM analysis."
        if lang == "en"
        else "このアプリケーションの分析レポートが見つかりません。\n\n"
        "**Create Report** を実行してLLM分析を生成してください。"
    )
    return jsonify({"report": msg})


# ---------------------------------------------------------------------------
# ETL Job Trigger
# ---------------------------------------------------------------------------

# In-memory store for ETL run tracking
_etl_runs: dict[int, dict] = {}


@bp.route("/api/v1/spark-perf/etl-runs", methods=["POST"])
def spark_perf_etl_trigger():
    """Trigger Spark Perf ETL pipeline job."""
    from app import UserInputError
    from services.job_launcher import JobLauncher, JobLauncherConfig

    data = request.get_json(silent=True)
    if not data:
        raise UserInputError("JSON body required")

    log_root = (data.get("log_root") or "").strip()
    cluster_id = (data.get("cluster_id") or "").strip()

    if not log_root:
        raise UserInputError("log_root is required")
    if not cluster_id:
        raise UserInputError("cluster_id is required")

    config = JobLauncherConfig.from_env()
    if not config.etl_job_id:
        return jsonify({"error": "spark_perf_etl_job_id is not configured"}), 503

    launcher = JobLauncher(config)
    try:
        result = launcher.trigger_etl(
            log_root=log_root,
            cluster_id=cluster_id,
            catalog=data.get("catalog", ""),
            schema=data.get("schema", ""),
            table_prefix=data.get("table_prefix", ""),
        )
    except Exception as e:
        logger.exception("ETL trigger failed")
        return jsonify({"error": f"Job trigger failed: {e}"}), 502

    _etl_runs[result["run_id"]] = {
        "log_root": log_root,
        "cluster_id": cluster_id,
    }

    return jsonify(
        {
            "run_id": result["run_id"],
            "status": "PENDING",
            "run_page_url": result.get("run_page_url", ""),
        }
    )


# In-memory store for app-side report generation (same pattern as analysis_store)
_report_store: dict[str, dict] = {}
_REPORT_STORE_MAX_ENTRIES = 50  # Max concurrent/recent reports to keep
_REPORT_STORE_TTL_SEC = 3600  # Evict completed/failed entries after 1 hour


def _evict_stale_reports() -> None:
    """Remove completed/failed reports older than TTL, and cap total entries."""
    now = time.monotonic()
    to_delete = []
    for rid, entry in _report_store.items():
        if entry["status"] in ("COMPLETED", "FAILED"):
            age = now - entry.get("started_at", now)
            if age > _REPORT_STORE_TTL_SEC:
                to_delete.append(rid)
    for rid in to_delete:
        del _report_store[rid]

    # Hard cap: evict oldest entries if over limit
    if len(_report_store) > _REPORT_STORE_MAX_ENTRIES:
        sorted_entries = sorted(_report_store.items(), key=lambda x: x[1].get("started_at", 0))
        for rid, _ in sorted_entries[: len(_report_store) - _REPORT_STORE_MAX_ENTRIES]:
            del _report_store[rid]


def _run_spark_perf_report_background(
    report_id: str,
    reader_config: dict,
    app_id: str,
    model: str,
    lang: str,
    experiment_id: str,
    variant: str,
    databricks_host: str = "",
    databricks_token: str = "",
) -> None:
    """Background thread for app-side Spark Perf report generation."""
    try:
        from core.spark_perf_llm import run_spark_perf_report
        from services.spark_perf_reader import SparkPerfConfig, SparkPerfReader

        config = SparkPerfConfig(**reader_config)
        reader = SparkPerfReader(config)

        def on_stage(stage: str, **kwargs) -> None:
            _report_store[report_id]["stage"] = stage
            _report_store[report_id]["stage_started_at"] = time.monotonic()
            if "prompt_tokens" in kwargs:
                _report_store[report_id]["prompt_tokens"] = kwargs["prompt_tokens"]

        _report_store[report_id]["status"] = "RUNNING"

        result = run_spark_perf_report(
            reader=reader,
            app_id=app_id,
            model=model,
            databricks_host=databricks_host,
            databricks_token=databricks_token,
            lang=lang,
            experiment_id=experiment_id,
            variant=variant,
            on_stage=on_stage,
        )

        _report_store[report_id]["status"] = "COMPLETED"
        _report_store[report_id]["stage"] = "done"
        _report_store[report_id]["result"] = result

    except Exception as e:
        logger.exception("Spark Perf report generation failed: %s", e)
        _report_store[report_id]["status"] = "FAILED"
        _report_store[report_id]["error"] = str(e)


@bp.route("/api/v1/spark-perf/summary-runs", methods=["POST"])
def spark_perf_summary_trigger():
    """Trigger Spark Perf LLM summary generation (app-side)."""
    import uuid
    from threading import Thread

    from app import UserInputError

    _evict_stale_reports()

    data = request.get_json(silent=True) or {}
    app_id = (data.get("app_id") or "").strip()
    if not app_id:
        raise UserInputError("app_id is required")

    reader = _get_spark_perf_reader()
    if reader is None:
        return jsonify({"error": "Spark Perf not configured"}), 503

    model = data.get("model_endpoint", "").strip() or "databricks-claude-sonnet-4-6"
    lang = data.get("output_language", "en").strip()
    experiment_id = data.get("experiment_id", "").strip()
    variant = data.get("variant", "").strip()

    # Build reader config dict for background thread (can't pass reader object directly)
    from dataclasses import asdict

    reader_config = asdict(reader._config)

    # Get credentials for LLM client (capture before spawning thread)
    from app import get_databricks_credentials

    databricks_host, databricks_token = get_databricks_credentials()

    report_id = str(uuid.uuid4())
    _report_store[report_id] = {
        "status": "PENDING",
        "stage": "queued",
        "app_id": app_id,
        "error": None,
        "result": None,
        "started_at": time.monotonic(),
        "stage_started_at": time.monotonic(),
    }

    thread = Thread(
        target=_run_spark_perf_report_background,
        args=(report_id, reader_config, app_id, model, lang, experiment_id, variant),
        kwargs={
            "databricks_host": databricks_host or "",
            "databricks_token": databricks_token or "",
        },
        daemon=True,
    )
    thread.start()

    logger.info(
        "Spark Perf report started: report_id=%s, app_id=%s, model=%s", report_id, app_id, model
    )

    return jsonify(
        {
            "report_id": report_id,
            "status": "PENDING",
        }
    )


@bp.route("/api/v1/spark-perf/summary-runs/<report_id>/status")
def spark_perf_summary_status(report_id: str):
    """Poll app-side report generation status."""
    entry = _report_store.get(report_id)
    if not entry:
        return jsonify({"error": "Report not found"}), 404

    # Map to same state format the UI expects
    status = entry["status"]
    stage = entry.get("stage", "unknown")

    # Stage display messages
    stage_messages = {
        "queued": "Queued...",
        "collecting_data": "Collecting data from Gold tables...",
        "filtering_knowledge": "Selecting relevant knowledge...",
        "llm_call_1": "LLM Call 1: Generating Sections 1-2 + Actions...",
        "llm_call_2": "LLM Call 2: Generating Sections 3-7...",
        "writing": "Writing report...",
        "done": "Complete",
    }

    now = time.monotonic()
    elapsed = round(now - entry.get("started_at", now))
    stage_elapsed = round(now - entry.get("stage_started_at", now))

    result = {
        "report_id": report_id,
        "state": status,
        "result_state": "SUCCESS" if status == "COMPLETED" else None,
        "stage": stage,
        "stage_message": stage_messages.get(stage, stage),
        "elapsed_sec": elapsed,
        "stage_elapsed_sec": stage_elapsed,
        "app_id": entry.get("app_id", ""),
        "prompt_tokens": entry.get("prompt_tokens"),
    }

    if status == "FAILED":
        result["state_message"] = entry.get("error", "Unknown error")
        result["result_state"] = "FAILED"

    return jsonify(result)


@bp.route("/api/v1/spark-perf/etl-runs/<int:run_id>/status")
def spark_perf_etl_status(run_id: int):
    """Poll job run status (works for both ETL and Summary runs)."""
    from services.job_launcher import JobLauncher, JobLauncherConfig

    config = JobLauncherConfig.from_env()
    launcher = JobLauncher(config)

    try:
        status = launcher.get_run_status(run_id)
    except Exception as e:
        logger.exception("Job status check failed: run_id=%s", run_id)
        return jsonify({"error": f"Status check failed: {e}"}), 502

    return jsonify(status)
