"""Blueprint for history routes: /history, /api/v1/history, /api/v1/history/delete."""

import logging

from flask import Blueprint, Response, jsonify, render_template, request

logger = logging.getLogger(__name__)

bp = Blueprint("history", __name__)


@bp.route("/history")
def history_page():
    """Analysis history page."""
    from app import get_locale

    return render_template(
        "history.html",
        current_lang=get_locale(),
    )


@bp.route("/api/v1/history")
def list_history():
    """List past analyses from Delta tables."""
    from services.table_reader import TableReader
    from services.table_writer import TableWriterConfig

    config = TableWriterConfig.from_env()
    if not config.http_path:
        return jsonify({"error": "PROFILER_WAREHOUSE_HTTP_PATH not configured"}), 503

    reader = TableReader(config)
    fingerprint = request.args.get("fingerprint")
    experiment_id = request.args.get("experiment_id")
    variant = request.args.get("variant")
    limit_str = request.args.get("limit", "50") or "50"
    limit = max(1, min(int(limit_str) if limit_str.isdigit() else 50, 200))

    summaries = reader.list_analyses(
        query_fingerprint=fingerprint,
        experiment_id=experiment_id,
        variant=variant,
        limit=limit,
    )

    return jsonify(
        [
            {
                "analysis_id": s.analysis_id,
                "analyzed_at": str(s.analyzed_at) if s.analyzed_at else None,
                "query_id": s.query_id,
                "query_fingerprint": s.query_fingerprint,
                "experiment_id": s.experiment_id,
                "variant": s.variant,
                "total_time_ms": s.total_time_ms,
                "read_bytes": s.read_bytes,
                "spill_bytes": s.spill_bytes,
                "warehouse_name": s.warehouse_name,
                "warehouse_size": s.warehouse_size,
                "action_card_count": s.action_card_count,
                "critical_alert_count": s.critical_alert_count,
                "lang": s.lang,
                "estimated_cost_usd": s.estimated_cost_usd,
                "has_explain": s.has_explain,
            }
            for s in summaries
        ]
    )


@bp.route("/api/v1/history/delete", methods=["POST"])
def delete_history():
    """Delete selected analyses from Delta tables."""
    from app import UserInputError
    from services.table_writer import TableWriter, TableWriterConfig

    data = request.get_json()
    if not data:
        raise UserInputError("JSON body required")

    analysis_ids = data.get("analysis_ids", [])
    if not analysis_ids:
        raise UserInputError("analysis_ids list is required")

    config = TableWriterConfig.from_env()
    if not config.http_path:
        return jsonify({"error": "PROFILER_WAREHOUSE_HTTP_PATH not configured"}), 503

    config.enabled = True
    writer = TableWriter(config)
    deleted = writer.delete_analyses(analysis_ids)

    logger.info("Deleted %d/%d analyses", deleted, len(analysis_ids))
    return jsonify(
        {
            "deleted": deleted,
            "requested": len(analysis_ids),
        }
    )


@bp.route("/api/v1/history/update", methods=["POST"])
def update_history():
    """Update experiment_id and/or variant for an analysis."""
    from app import UserInputError
    from services.table_writer import TableWriterConfig

    data = request.get_json()
    if not data:
        raise UserInputError("JSON body required")

    analysis_id = data.get("analysis_id", "").strip()
    if not analysis_id:
        raise UserInputError("analysis_id is required")

    config = TableWriterConfig.from_env()
    if not config.http_path:
        return jsonify({"error": "Warehouse not configured"}), 503

    updates = []
    params = {"aid": analysis_id}
    if "experiment_id" in data:
        updates.append("experiment_id = :exp")
        params["exp"] = data["experiment_id"].strip() or None
    if "variant" in data:
        updates.append("variant = :var")
        params["var"] = data["variant"].strip() or None

    if not updates:
        return jsonify({"error": "Nothing to update"}), 400

    try:
        from services.table_reader import TableReader

        reader = TableReader(config)
        with reader._get_connection() as conn:
            with conn.cursor() as cursor:
                # Update analysis header
                fqn = reader._fqn("profiler_analysis_header")
                sql = f"UPDATE {fqn} SET {', '.join(updates)} WHERE analysis_id = :aid"
                cursor.execute(sql, parameters=params)

                # Cascade to profiler_compare_result
                # compare_result stores query_id (not analysis_id), so look it up first
                try:
                    header_fqn = reader._fqn("profiler_analysis_header")
                    cursor.execute(
                        f"SELECT query_id FROM {header_fqn} WHERE analysis_id = :aid LIMIT 1",
                        parameters={"aid": analysis_id},
                    )
                    row = cursor.fetchone()
                    query_id = row[0] if row else None

                    if query_id:
                        compare_fqn = reader._fqn("profiler_compare_result")
                        b_updates = []
                        b_params = {"qid": query_id}
                        if "experiment_id" in data:
                            b_updates.append("baseline_experiment = :exp")
                            b_params["exp"] = data["experiment_id"].strip() or None
                        if "variant" in data:
                            b_updates.append("baseline_variant = :var")
                            b_params["var"] = data["variant"].strip() or None
                        cursor.execute(
                            f"UPDATE {compare_fqn} SET {', '.join(b_updates)} WHERE baseline_query_id = :qid",
                            parameters=b_params,
                        )

                        c_updates = []
                        c_params = {"qid": query_id}
                        if "experiment_id" in data:
                            c_updates.append("candidate_experiment = :exp")
                            c_params["exp"] = data["experiment_id"].strip() or None
                        if "variant" in data:
                            c_updates.append("candidate_variant = :var")
                            c_params["var"] = data["variant"].strip() or None
                        cursor.execute(
                            f"UPDATE {compare_fqn} SET {', '.join(c_updates)} WHERE candidate_query_id = :qid",
                            parameters=c_params,
                        )
                except Exception:
                    pass  # Table may not exist yet

        return jsonify({"ok": True})
    except Exception as e:
        logger.exception("Failed to update analysis")
        return jsonify({"error": str(e)}), 502


@bp.route("/api/v1/history/<analysis_id>/download")
def download_report(analysis_id: str):
    """Download the Markdown report for a stored analysis."""
    from services.table_reader import TableReader
    from services.table_writer import TableWriterConfig

    config = TableWriterConfig.from_env()
    if not config.http_path:
        return jsonify({"error": "Warehouse not configured"}), 503

    reader = TableReader(config)
    result = reader.get_analysis_with_report(analysis_id)
    if result is None:
        return jsonify({"error": "Analysis not found"}), 404

    md = result.report_markdown or ""
    if not md:
        return jsonify({"error": "No report available for this analysis"}), 404

    query_id = result.analysis.query_metrics.query_id or "unknown"
    filename = f"report_{query_id[:16]}.md"

    return Response(
        md,
        mimetype="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
