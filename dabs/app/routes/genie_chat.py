"""Blueprint for Genie Chat API endpoints (incl. LLM-based query rewrite)."""

import logging
import os
import time
import uuid
from threading import Thread

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


# =========================================================================
# LLM-based Query Rewrite
# =========================================================================


def _load_analysis_for_rewrite(analysis_id: str):
    """Load ProfileAnalysis from in-memory store or Delta (fallback)."""
    from app import analysis_store

    # Try in-memory first
    entry = analysis_store.get(analysis_id)
    if entry and entry.get("status") == "completed":
        return entry.get("analysis")

    # Fallback to Delta
    try:
        from services.table_reader import TableReader
        from services.table_writer import TableWriterConfig

        config = TableWriterConfig.from_env()
        if not config.http_path:
            return None
        reader = TableReader(config)
        result = reader.get_analysis_with_report(analysis_id)
        return result.analysis if result else None
    except Exception:
        logger.debug("Delta fallback failed for rewrite: %s", analysis_id)
        return None


# In-memory store for async rewrite tasks (with TTL-based cleanup)
_rewrite_tasks: dict[str, dict] = {}
_REWRITE_TASK_TTL = 600  # 10 minutes


def _purge_stale_tasks() -> None:
    """Remove tasks older than TTL to prevent memory leaks."""
    now = time.monotonic()
    stale = [k for k, v in _rewrite_tasks.items() if now - v.get("_ts", 0) > _REWRITE_TASK_TTL]
    for k in stale:
        _rewrite_tasks.pop(k, None)


def _run_rewrite_task(
    task_id: str,
    analysis,
    model: str,
    host: str,
    token: str,
    lang: str,
    is_serverless: bool,
    rec: dict,
    feedback: str = "",
    previous_rewrite: str = "",
):
    """Background worker for LLM rewrite (initial or feedback fix)."""
    try:
        if feedback and previous_rewrite:
            # Feedback fix mode
            from core.llm import fix_rewrite_with_llm

            original_sql = analysis.query_metrics.query_text or ""
            result = fix_rewrite_with_llm(
                original_sql=original_sql,
                previous_rewrite=previous_rewrite,
                feedback=feedback,
                model=model,
                databricks_host=host,
                databricks_token=token,
                lang=lang,
            )
        else:
            # Initial rewrite mode
            from core.llm import rewrite_with_llm
            from core.llm_prompts.knowledge import load_tuning_knowledge

            knowledge = load_tuning_knowledge(lang)
            result = rewrite_with_llm(
                analysis=analysis,
                model=model,
                databricks_host=host,
                databricks_token=token,
                tuning_knowledge=knowledge,
                lang=lang,
                is_serverless=is_serverless,
            )
        resp = {"rewrite": result or "", "model_used": model}
        if rec and (
            rec.get("reason") or rec.get("token_constrained") or rec["recommended_model"] != model
        ):
            resp["model_recommendation"] = rec
        _rewrite_tasks[task_id] = {"status": "completed", "result": resp}
    except Exception as e:
        logger.exception("Rewrite task %s failed", task_id)
        _rewrite_tasks[task_id] = {"status": "failed", "error": str(e)}


@bp.route("/api/v1/rewrite", methods=["POST"])
def rewrite_query():
    """Start async SQL rewrite. Returns task_id for polling."""
    from app import get_locale

    data = request.get_json(silent=True) or {}
    analysis_id = data.get("analysis_id", "").strip()
    model = data.get("model", "").strip()

    if not analysis_id:
        return jsonify({"error": "analysis_id is required"}), 400

    analysis = _load_analysis_for_rewrite(analysis_id)
    if not analysis:
        return jsonify({"error": "Analysis not found or not completed"}), 404

    if not analysis.query_metrics.query_text:
        return jsonify({"error": "No SQL text available in this analysis"}), 400

    from core.llm import recommend_rewrite_model

    rec = recommend_rewrite_model(analysis)
    if not model:
        model = os.environ.get("LLM_MODEL", "databricks-claude-sonnet-4-6")

    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    is_serverless = False
    if hasattr(analysis, "warehouse_info") and analysis.warehouse_info:
        wh_type = getattr(analysis.warehouse_info, "warehouse_type", "") or ""
        is_serverless = "serverless" in wh_type.lower()

    lang = data.get("lang") or get_locale()
    feedback = data.get("feedback", "").strip()
    previous_rewrite = data.get("previous_rewrite", "").strip()

    task_id = str(uuid.uuid4())[:8]
    _purge_stale_tasks()
    _rewrite_tasks[task_id] = {"status": "running", "_ts": time.monotonic()}

    thread = Thread(
        target=_run_rewrite_task,
        args=(
            task_id,
            analysis,
            model,
            host,
            token,
            lang,
            is_serverless,
            rec if not feedback else {},
            feedback,
            previous_rewrite,
        ),
        daemon=True,
    )
    thread.start()

    return jsonify({"task_id": task_id, "status": "running"})


@bp.route("/api/v1/rewrite/<task_id>")
def rewrite_status(task_id):
    """Poll rewrite task status."""
    task = _rewrite_tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404

    if task["status"] == "running":
        return jsonify({"status": "running"})

    if task["status"] == "failed":
        # Clean up
        _rewrite_tasks.pop(task_id, None)
        return jsonify({"status": "failed", "error": task.get("error", "Unknown error")}), 502

    # Completed
    result = task.get("result", {})
    _rewrite_tasks.pop(task_id, None)
    return jsonify({"status": "completed", **result})


# =========================================================================
# SQL Validation (EXPLAIN → sqlglot fallback)
# =========================================================================

# Connection/infra errors → fallback to sqlglot
_EXPLAIN_CONNECTION_ERRORS = (
    "connection",
    "timeout",
    "refused",
    "reset by peer",
    "Could not connect",
    "EOF",
    "TEMPORARILY_UNAVAILABLE",
)

# Environment errors (table/schema/permission) → fallback to sqlglot for syntax-only check
# These are not syntax errors the user can fix in the SQL itself.
_EXPLAIN_ENVIRONMENT_ERRORS = (
    "TABLE_OR_VIEW_NOT_FOUND",
    "SCHEMA_NOT_FOUND",
    "CATALOG_NOT_FOUND",
    "does not exist",
    "PERMISSION_DENIED",
    "ACCESS_DENIED",
    "INSUFFICIENT_PRIVILEGES",
    "Unauthorized",
    "Forbidden",
    "403",
    "401",
)

# True syntax errors → return directly to user for fixing
_EXPLAIN_SYNTAX_ERRORS = (
    "ParseException",
    "PARSE_SYNTAX_ERROR",
    "mismatched input",
    "no viable alternative",
    "extraneous input",
)


def _validate_with_explain(sql: str) -> dict:
    """Run EXPLAIN on the configured SQL Warehouse.

    Returns {"valid": True} or {"valid": False, "error": "...", "method": "explain"}.
    Raises RuntimeError if connection to Warehouse fails.

    IMPORTANT — syntax-check only (NOT a full EXPLAIN source):
    This function exists purely to validate that the rewritten SQL parses
    and resolves. The Databricks Apps service principal does NOT hold the
    broader catalog/schema privileges required to produce a complete
    EXPLAIN EXTENDED / FORMATTED output on arbitrary user tables
    (deploy.sh intentionally does not grant them — see "No features
    requiring extra permissions" policy).

    Callers must therefore assume:
      - The returned plan text may be partial or empty
      - Signal extraction (CTE reuse, implicit CAST, Photon fallback, etc.)
        from this EXPLAIN output is NOT reliable
      - Any feature that needs full EXPLAIN must be fed by the original
        analysis's `explain_analysis` (attached by the user at upload
        time), not by re-running EXPLAIN here.

    Do NOT upgrade to EXPLAIN EXTENDED / FORMATTED here without first
    confirming the SP has the required privileges — upgrading silently
    produces partial results that look valid but mislead downstream
    consumers.
    """
    from services.table_writer import TableWriterConfig

    config = TableWriterConfig.from_env()
    if not config.http_path:
        raise RuntimeError("No SQL Warehouse configured")

    from databricks import sql as dbsql
    from services import _sdk_credentials_provider

    host = config.databricks_host.replace("https://", "").replace("http://", "")

    # Connection — if this fails, fallback to sqlglot
    try:
        if config.databricks_token:
            conn = dbsql.connect(
                server_hostname=host,
                http_path=config.http_path,
                access_token=config.databricks_token,
            )
        else:
            from databricks.sdk.core import Config

            cfg = Config()
            effective_host = host or (cfg.host or "").replace("https://", "")
            conn = dbsql.connect(
                server_hostname=effective_host,
                http_path=config.http_path,
                credentials_provider=_sdk_credentials_provider(cfg),
            )
    except Exception as e:
        raise RuntimeError(f"Cannot connect to SQL Warehouse: {e}") from e

    # Guard: pre-validate with sqlglot to block multi-statement injection
    import sqlglot

    try:
        stmts = sqlglot.parse(sql, dialect="databricks")
        if len(stmts) != 1:
            return {
                "valid": False,
                "error": "Only single SQL statements are allowed",
                "method": "explain",
            }
    except Exception:
        pass  # sqlglot failure is not a blocker — EXPLAIN will catch real issues

    # EXPLAIN — classify errors into syntax (fixable) vs environment (fallback to sqlglot)
    try:
        cursor = conn.cursor()
        cursor.execute(f"EXPLAIN {sql}")
        rows = cursor.fetchall()
        plan_text = "\n".join(str(r[0]) for r in rows) if rows else ""
        cursor.close()
        # Check if EXPLAIN output contains error indicators in plan rows
        plan_lower = plan_text.lower()
        for pattern in _EXPLAIN_SYNTAX_ERRORS:
            if pattern.lower() in plan_lower:
                return {"valid": False, "error": plan_text[:500], "method": "explain"}
        for pattern in _EXPLAIN_ENVIRONMENT_ERRORS:
            if pattern.lower() in plan_lower:
                raise RuntimeError(f"EXPLAIN environment error: {plan_text[:200]}")
        return {"valid": True, "method": "explain"}
    except RuntimeError:
        raise  # Re-raise environment errors for sqlglot fallback
    except Exception as e:
        err_str = str(e)
        err_lower = err_str.lower()
        # Connection errors → fallback
        for pattern in _EXPLAIN_CONNECTION_ERRORS:
            if pattern.lower() in err_lower:
                raise RuntimeError(f"EXPLAIN unavailable: {err_str[:200]}") from e
        # Environment errors (table/permission) → fallback to sqlglot for syntax check
        for pattern in _EXPLAIN_ENVIRONMENT_ERRORS:
            if pattern.lower() in err_lower:
                raise RuntimeError(f"EXPLAIN environment error: {err_str[:200]}") from e
        # Syntax errors → return directly for Request Fix
        return {"valid": False, "error": err_str[:500], "method": "explain"}
    finally:
        conn.close()


def _validate_with_sqlglot(sql: str) -> dict:
    """Local syntax check using sqlglot.

    Returns {"valid": True} or {"valid": False, "error": "...", "method": "sqlglot"}.
    """
    import sqlglot

    last_error = None
    for dialect in ("databricks", "spark"):
        try:
            sqlglot.parse_one(sql, dialect=dialect)
            return {"valid": True, "method": "sqlglot"}
        except sqlglot.errors.ParseError as e:
            last_error = str(e)[:500]
            continue
        except Exception:
            continue
    return {"valid": False, "error": last_error or "Failed to parse SQL", "method": "sqlglot"}


@bp.route("/api/v1/rewrite/validate", methods=["POST"])
def validate_rewrite():
    """Validate SQL syntax: EXPLAIN on WH, fallback to sqlglot if unavailable."""
    data = request.get_json(silent=True) or {}
    sql = data.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "sql is required"}), 400

    fallback_reason = None
    try:
        result = _validate_with_explain(sql)
        return jsonify(result)
    except RuntimeError as e:
        fallback_reason = str(e)
        logger.info("EXPLAIN unavailable, falling back to sqlglot: %s", e)
    except Exception as e:
        fallback_reason = str(e)
        logger.warning("EXPLAIN failed unexpectedly, falling back to sqlglot: %s", e)

    result = _validate_with_sqlglot(sql)
    result["fallback_reason"] = fallback_reason
    return jsonify(result)
