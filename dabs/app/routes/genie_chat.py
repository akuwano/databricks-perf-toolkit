"""Blueprint for Genie Chat API endpoints.

The LLM-based SQL Query Rewrite feature lived here through v6.7.2 and
moved to ``routes.query_rewrite`` in v6.7.3 — see
``docs/v6/query-rewrite-extraction.md``.
"""

import logging

from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

bp = Blueprint("genie_chat", __name__)


def _get_genie_client():
    """Create GenieClient from Spark Perf config (SP auth)."""
    from services.genie_client import GenieClient, GenieConfig

    config = GenieConfig.from_env()
    if not config.host or not config.warehouse_id:
        return None
    return GenieClient(config)


def _get_dbsql_genie_client():
    """Create GenieClient from DBSQL config (SP auth)."""
    from services.genie_client import DbsqlGenieConfig, GenieClient

    config = DbsqlGenieConfig.from_env()
    if not config.host or not config.warehouse_id:
        return None
    return GenieClient(config)


def _get_space_id() -> str:
    """Get persisted Spark Perf Genie Space ID."""
    from core.config_store import get_setting

    return get_setting("genie_space_id", "")


def _get_dbsql_space_id() -> str:
    """Get persisted DBSQL Genie Space ID."""
    from core.config_store import get_setting

    return get_setting("dbsql_genie_space_id", "")


@bp.route("/api/v1/genie/ensure-space", methods=["POST"])
def ensure_space():
    """Create Genie Space if not exists or invalid, return space_id."""
    space_id = _get_space_id()

    client = _get_genie_client()
    if client is None:
        return jsonify({"error": "Genie not configured (missing host or warehouse)"}), 503

    # Validate existing space (including table list check)
    expected_tables = client._spark_perf_table_fqns()
    if space_id and client.validate_space(space_id, expected_tables=expected_tables):
        return jsonify({"space_id": space_id})

    # Create new space (existing was missing, trashed, tables changed, or never created)
    try:
        space_id = client.create_space()
        from core.config_store import save_config

        save_config({"genie_space_id": space_id})
        return jsonify({"space_id": space_id})
    except Exception as e:
        logger.exception("Failed to create Genie Space")
        return jsonify({"error": f"Failed to create Genie Space: {e}"}), 502


@bp.route("/api/v1/genie/conversations", methods=["POST"])
def start_conversation():
    """Start a new Genie conversation with app_id context."""
    data = request.get_json(silent=True) or {}
    message = data.get("message", "").strip()
    app_id = data.get("app_id", "").strip()

    if not message:
        return jsonify({"error": "message is required"}), 400

    space_id = _get_space_id()
    if not space_id:
        return jsonify({"error": "Genie Space not configured. Click 'Ask Genie' first."}), 400

    client = _get_genie_client()
    if client is None:
        return jsonify({"error": "Genie not configured"}), 503

    # Inject context
    comparison_id = data.get("comparison_id", "").strip()
    baseline_app_id = data.get("baseline_app_id", "").strip()
    candidate_app_id = data.get("candidate_app_id", "").strip()

    if comparison_id and baseline_app_id and candidate_app_id:
        context_msg = (
            f"[Context: The user is comparing two Spark applications. "
            f"Baseline app_id='{baseline_app_id}', Candidate app_id='{candidate_app_id}'. "
            f"Query all Gold tables using app_id IN ('{baseline_app_id}','{candidate_app_id}') to compare both applications. "
            f"Cross-reference all tables to find performance differences.]\n\n"
            f"{message}"
        )
    elif app_id:
        context_msg = (
            f"[Context: The user is viewing Spark application app_id='{app_id}'. "
            f"Filter all queries by this app_id unless the user explicitly asks about other applications.]\n\n"
            f"{message}"
        )
    else:
        context_msg = message

    try:
        result = client.start_conversation(space_id, context_msg)
        return jsonify(result)
    except Exception as e:
        logger.exception("Failed to start Genie conversation")
        return jsonify({"error": f"Failed to start conversation: {e}"}), 502


@bp.route("/api/v1/genie/conversations/<conv_id>/messages", methods=["POST"])
def send_message(conv_id):
    """Send follow-up message in existing conversation."""
    data = request.get_json(silent=True) or {}
    message = data.get("message", "").strip()
    app_id = data.get("app_id", "").strip()

    if not message:
        return jsonify({"error": "message is required"}), 400

    space_id = _get_space_id()
    if not space_id:
        return jsonify({"error": "Genie Space not configured"}), 400

    client = _get_genie_client()
    if client is None:
        return jsonify({"error": "Genie not configured"}), 503

    # Inject context for follow-up too (Genie may lose context)
    comparison_id = data.get("comparison_id", "").strip()
    baseline_app_id = data.get("baseline_app_id", "").strip()
    candidate_app_id = data.get("candidate_app_id", "").strip()

    if comparison_id:
        context_msg = f"[comparison_id='{comparison_id}' baseline='{baseline_app_id}' candidate='{candidate_app_id}'] {message}"
    elif app_id:
        context_msg = f"[app_id='{app_id}'] {message}"
    else:
        context_msg = message

    try:
        msg_id = client.send_message(space_id, conv_id, context_msg)
        return jsonify({"message_id": msg_id})
    except Exception as e:
        logger.exception("Failed to send Genie message")
        return jsonify({"error": f"Failed to send message: {e}"}), 502


@bp.route("/api/v1/genie/conversations/<conv_id>/messages/<msg_id>/status")
def message_status(conv_id, msg_id):
    """Poll message status."""
    space_id = _get_space_id()
    if not space_id:
        return jsonify({"error": "Genie Space not configured"}), 400

    client = _get_genie_client()
    if client is None:
        return jsonify({"error": "Genie not configured"}), 503

    try:
        result = client.get_message_status(space_id, conv_id, msg_id)
        return jsonify(result)
    except Exception as e:
        logger.exception("Failed to get message status")
        return jsonify({"error": f"Status check failed: {e}"}), 502


@bp.route("/api/v1/genie/conversations/<conv_id>/messages/<msg_id>/results/<attachment_id>")
def query_result(conv_id, msg_id, attachment_id):
    """Get query result for a message attachment."""
    space_id = _get_space_id()
    if not space_id:
        return jsonify({"error": "Genie Space not configured"}), 400

    client = _get_genie_client()
    if client is None:
        return jsonify({"error": "Genie not configured"}), 503

    try:
        result = client.get_query_result(space_id, conv_id, msg_id, attachment_id)
        return jsonify(result)
    except Exception as e:
        logger.exception("Failed to get query result")
        return jsonify({"error": f"Query result failed: {e}"}), 502


# ── DBSQL Genie endpoints ──────────────────────────────────────────────


@bp.route("/api/v1/genie/dbsql/ensure-space", methods=["POST"])
def dbsql_ensure_space():
    """Create DBSQL Genie Space if not exists or invalid, return space_id."""
    space_id = _get_dbsql_space_id()

    client = _get_dbsql_genie_client()
    if client is None:
        return jsonify({"error": "DBSQL Genie not configured (missing host or warehouse)"}), 503

    # Validate existing space (including table list check)
    expected_tables = client._dbsql_table_fqns()
    if space_id and client.validate_space(space_id, expected_tables=expected_tables):
        return jsonify({"space_id": space_id})

    # Ensure all DBSQL tables exist before registering in Genie Space
    try:
        from routes.settings import _ensure_all_tables

        _ensure_all_tables()
    except Exception as e:
        logger.warning("Table initialization before Genie Space creation failed: %s", e)

    # Create new space
    try:
        space_id = client.create_dbsql_space()
        from core.config_store import save_config

        save_config({"dbsql_genie_space_id": space_id})
        return jsonify({"space_id": space_id})
    except Exception as e:
        logger.exception("Failed to create DBSQL Genie Space")
        return jsonify({"error": f"Failed to create DBSQL Genie Space: {e}"}), 502


@bp.route("/api/v1/genie/dbsql/conversations", methods=["POST"])
def dbsql_start_conversation():
    """Start a new DBSQL Genie conversation with analysis_id context."""
    data = request.get_json(silent=True) or {}
    message = data.get("message", "").strip()
    analysis_id = data.get("analysis_id", "").strip()

    if not message:
        return jsonify({"error": "message is required"}), 400

    space_id = _get_dbsql_space_id()
    if not space_id:
        return jsonify({"error": "DBSQL Genie Space not configured. Click 'Ask Genie' first."}), 400

    client = _get_dbsql_genie_client()
    if client is None:
        return jsonify({"error": "DBSQL Genie not configured"}), 503

    # Inject context
    report_summary = data.get("report_summary", "").strip()
    comparison_id = data.get("comparison_id", "").strip()
    baseline_id = data.get("baseline_id", "").strip()
    candidate_id = data.get("candidate_id", "").strip()

    if comparison_id and baseline_id and candidate_id:
        context_parts = [
            "[Context: The user is comparing two DBSQL query analyses.",
            f"Baseline query_id='{baseline_id}', Candidate query_id='{candidate_id}'.",
            f"Query profiler_analysis_header using query_id IN ('{baseline_id}','{candidate_id}') to get analysis_ids first.",
            "Then use those analysis_ids to query all other profiler tables.",
            "Cross-reference all tables to find performance differences.",
            "]",
        ]
        context_msg = " ".join(context_parts) + f"\n\n{message}"
    elif analysis_id:
        context_parts = [
            f"[Context: The user is viewing DBSQL query analysis with analysis_id='{analysis_id}'.",
            f"IMPORTANT: Add WHERE analysis_id = '{analysis_id}' to ALL queries.",
        ]
        if report_summary:
            context_parts.append(f"Report summary: {report_summary[:300]}")
        context_parts.append("]")
        context_msg = " ".join(context_parts) + f"\n\n{message}"
    else:
        context_msg = message

    try:
        result = client.start_conversation(space_id, context_msg)
        return jsonify(result)
    except Exception as e:
        logger.exception("Failed to start DBSQL Genie conversation")
        return jsonify({"error": f"Failed to start conversation: {e}"}), 502


@bp.route("/api/v1/genie/dbsql/conversations/<conv_id>/messages", methods=["POST"])
def dbsql_send_message(conv_id):
    """Send follow-up message in existing DBSQL conversation."""
    data = request.get_json(silent=True) or {}
    message = data.get("message", "").strip()
    analysis_id = data.get("analysis_id", "").strip()

    if not message:
        return jsonify({"error": "message is required"}), 400

    space_id = _get_dbsql_space_id()
    if not space_id:
        return jsonify({"error": "DBSQL Genie Space not configured"}), 400

    client = _get_dbsql_genie_client()
    if client is None:
        return jsonify({"error": "DBSQL Genie not configured"}), 503

    # Inject context for follow-up too
    comparison_id = data.get("comparison_id", "").strip()
    baseline_id = data.get("baseline_id", "").strip()
    candidate_id = data.get("candidate_id", "").strip()

    if comparison_id:
        context_msg = f"[comparison_id='{comparison_id}' baseline='{baseline_id}' candidate='{candidate_id}'] {message}"
    elif analysis_id:
        context_msg = f"[analysis_id='{analysis_id}'] {message}"
    else:
        context_msg = message

    try:
        msg_id = client.send_message(space_id, conv_id, context_msg)
        return jsonify({"message_id": msg_id})
    except Exception as e:
        logger.exception("Failed to send DBSQL Genie message")
        return jsonify({"error": f"Failed to send message: {e}"}), 502


@bp.route("/api/v1/genie/dbsql/conversations/<conv_id>/messages/<msg_id>/status")
def dbsql_message_status(conv_id, msg_id):
    """Poll DBSQL message status."""
    space_id = _get_dbsql_space_id()
    if not space_id:
        return jsonify({"error": "DBSQL Genie Space not configured"}), 400

    client = _get_dbsql_genie_client()
    if client is None:
        return jsonify({"error": "DBSQL Genie not configured"}), 503

    try:
        result = client.get_message_status(space_id, conv_id, msg_id)
        return jsonify(result)
    except Exception as e:
        logger.exception("Failed to get DBSQL message status")
        return jsonify({"error": f"Status check failed: {e}"}), 502


@bp.route("/api/v1/genie/dbsql/conversations/<conv_id>/messages/<msg_id>/results/<attachment_id>")
def dbsql_query_result(conv_id, msg_id, attachment_id):
    """Get query result for a DBSQL message attachment."""
    space_id = _get_dbsql_space_id()
    if not space_id:
        return jsonify({"error": "DBSQL Genie Space not configured"}), 400

    client = _get_dbsql_genie_client()
    if client is None:
        return jsonify({"error": "DBSQL Genie not configured"}), 503

    try:
        result = client.get_query_result(space_id, conv_id, msg_id, attachment_id)
        return jsonify(result)
    except Exception as e:
        logger.exception("Failed to get DBSQL query result")
        return jsonify({"error": f"Query result failed: {e}"}), 502
