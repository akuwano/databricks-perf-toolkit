"""Blueprint for the LLM-based SQL Query Rewrite feature.

Extracted from ``routes/genie_chat.py`` in v6.7.3 per
``docs/v6/query-rewrite-extraction.md`` Phase 1. The rewrite feature
shares no runtime state with the Genie chat panel — it merely lived
in the same blueprint historically. Splitting it out makes the
3 routes / 7 helpers reviewable in isolation, and unblocks Phase 2
(dedicated ``/rewrite/<analysis_id>`` UX).

Public routes (paths preserved for UI compatibility):
- ``POST /api/v1/rewrite``                — start an async rewrite task
- ``GET  /api/v1/rewrite/<task_id>``      — poll task status
- ``POST /api/v1/rewrite/validate``       — validate SQL syntax via
  EXPLAIN (warehouse-side) with sqlglot fallback

Logger name moves from ``routes.genie_chat`` to ``routes.query_rewrite``.
External log filters (if any) targeting the old name need to be
updated in lockstep with this PR.
"""

import hashlib
import logging
import os
import time
import uuid
from threading import Thread

from flask import Blueprint, jsonify, render_template, request

logger = logging.getLogger(__name__)

bp = Blueprint("query_rewrite", __name__)


# v1: strip + collapse whitespace, preserve case. Codex Q1 review:
# tag every persisted row with the version so a future v2 scheme
# (comment stripping, semicolon normalisation, dialect-aware
# canonicalisation) can be backfilled without invalidating existing
# group/compare queries that pinned to v1.
SOURCE_SQL_HASH_VERSION = "v1"


def _hash_source_sql(sql: str) -> str:
    """Phase 3 (v6.7.5) hash key for grouping rewrite attempts.

    Multi-model compare and refine-chain reconstruction join on this
    value, so the normalisation MUST stay deterministic and stable
    across releases. We strip leading/trailing whitespace and collapse
    interior runs of whitespace; case is preserved (case-sensitive
    object names matter on Databricks).
    """
    normalised = " ".join((sql or "").split())
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()


# Codex Q4 review (v6.7.6): admin override for the history endpoint.
# Mirrors the FEEDBACK_EXPORT_ADMIN_EMAILS pattern. Empty / unset →
# dev mode (all-allowed with a warning logged once); set in prod via
# local-overrides.yml.
_ADMIN_ENV_KEY = "REWRITE_HISTORY_ADMIN_EMAILS"


# ---------------------------------------------------------------------------
# Dedicated UX page (Phase 2 — v6.7.4)
# ---------------------------------------------------------------------------


@bp.route("/rewrite/<analysis_id>")
def rewrite_page(analysis_id: str):
    """Render the dedicated Query Rewrite page.

    Phase 2 of ``docs/v6/query-rewrite-extraction.md``: the chat
    bubble UX in ``shared_result.html`` is fine for one-shot tries
    but cramps the actual workflow (compare source/rewrite, iterate,
    keep history). This page hosts the same backend (``/api/v1/rewrite``)
    behind a layout designed for that workflow.
    """
    from app import analysis_store, get_locale
    from core.i18n import gettext as _t

    current_lang = get_locale()
    analysis = _load_analysis_for_rewrite(analysis_id)
    if analysis is None:
        return render_template(
            "error.html",
            error=_t("Analysis not found"),
            current_lang=current_lang,
        ), 404

    source_sql = analysis.query_metrics.query_text or ""
    if not source_sql:
        return render_template(
            "error.html",
            error=_t("This analysis has no SQL text available for rewrite."),
            current_lang=current_lang,
        ), 400

    is_serverless = False
    if hasattr(analysis, "warehouse_info") and analysis.warehouse_info:
        wh_type = getattr(analysis.warehouse_info, "warehouse_type", "") or ""
        is_serverless = "serverless" in wh_type.lower()

    # Existing in-memory store hit means a "live" analysis page; the
    # Delta fallback path returns an analysis but no in-memory entry,
    # which is fine — the rewrite page only needs the SQL + flags.
    in_memory = analysis_id in analysis_store

    return render_template(
        "rewrite.html",
        analysis_id=analysis_id,
        source_sql=source_sql,
        is_serverless=is_serverless,
        in_memory=in_memory,
        current_lang=current_lang,
    )


# ---------------------------------------------------------------------------
# Analysis loader (in-memory store + Delta fallback)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Async task tracking (in-memory, TTL-bounded)
# ---------------------------------------------------------------------------

_rewrite_tasks: dict[str, dict] = {}
_REWRITE_TASK_TTL = 600  # 10 minutes


def _purge_stale_tasks() -> None:
    """Remove tasks older than TTL to prevent memory leaks."""
    now = time.monotonic()
    stale = [k for k, v in _rewrite_tasks.items() if now - v.get("_ts", 0) > _REWRITE_TASK_TTL]
    for k in stale:
        _rewrite_tasks.pop(k, None)


def _persist_rewrite_artifact(
    *,
    analysis_id: str,
    source_sql: str,
    rewritten_sql: str,
    model: str,
    feedback: str = "",
    parent_id: str = "",
    user_email: str = "",
    output_format: str = "full",
) -> str | None:
    """Phase 3 (v6.7.5): append-only persistence of every rewrite attempt.

    Writes through ``TableWriter.write_rewrite_artifact``; failures are
    logged and swallowed because the user-facing rewrite has already
    been delivered — persistence drift only affects history / compare,
    never the live response. Returns the new ``artifact_id`` on success
    so the caller can stamp it onto the API response (lets the UI
    correlate live results with persisted rows).

    Codex Q2 review (v6.7.6): failures emit a structured ``warning``
    log (not silent ``info``) with analysis_id / model / reason so a
    dashboard / alert can trend the success rate.
    """
    try:
        from services.table_writer import TableWriter, TableWriterConfig

        config = TableWriterConfig.from_env()
        if not config.enabled or not config.http_path:
            return None
        writer = TableWriter(config)
        artifact_id = str(uuid.uuid4())
        ok = writer.write_rewrite_artifact(
            artifact_id=artifact_id,
            analysis_id=analysis_id,
            source_sql=source_sql,
            source_sql_hash=_hash_source_sql(source_sql),
            source_sql_hash_version=SOURCE_SQL_HASH_VERSION,
            rewritten_sql=rewritten_sql,
            model=model,
            feedback=feedback or None,
            parent_id=parent_id or None,
            user_email=user_email or None,
            output_format=output_format,
        )
        if not ok:
            logger.warning(
                "rewrite_artifact_persist_failed analysis_id=%s model=%s "
                "reason=writer_returned_false",
                analysis_id, model,
            )
            return None
        logger.info(
            "rewrite_artifact_persisted artifact_id=%s analysis_id=%s model=%s",
            artifact_id, analysis_id, model,
        )
        return artifact_id
    except Exception as e:
        logger.warning(
            "rewrite_artifact_persist_failed analysis_id=%s model=%s reason=%s: %s",
            analysis_id, model, type(e).__name__, str(e)[:200],
        )
        return None


def _compute_output_format(analysis, model: str) -> tuple[str, bool]:
    """v6.7.9 (Codex review of model simplification): predict whether
    the LLM will emit a full rewrite or a diff patch, and surface the
    distinction to the UI / history.

    The rewrite system prompt switches to a diff + Manual Merge Steps
    layout when ``estimate_rewrite_tokens(analysis) > model_max``, so
    callers can compute the same flag deterministically here without
    waiting for the LLM response. Returns
    ``(output_format, token_constrained)`` where ``output_format`` is
    ``"diff"`` when the prompt will request a patch and ``"full"``
    otherwise.
    """
    from core.llm import estimate_rewrite_tokens
    from core.llm_client import get_model_max_tokens

    needed = estimate_rewrite_tokens(analysis)
    model_max = get_model_max_tokens(model)
    constrained = needed > model_max
    return ("diff" if constrained else "full"), constrained


def _run_rewrite_task(
    task_id: str,
    analysis,
    model: str,
    host: str,
    token: str,
    lang: str,
    is_serverless: bool,
    feedback: str = "",
    previous_rewrite: str = "",
    analysis_id: str = "",
    parent_id: str = "",
    user_email: str = "",
):
    """Background worker for LLM rewrite (initial or feedback fix)."""
    try:
        output_format, token_constrained = _compute_output_format(analysis, model)
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
        resp = {
            "rewrite": result or "",
            "model_used": model,
            # v6.7.9 (Codex review): callers need to know whether the
            # output is executable SQL or a diff patch. ``token_constrained``
            # tells the UI why; ``output_format`` is the actionable
            # discriminator (Validate skip / Refine disable / banner).
            "output_format": output_format,
            "token_constrained": token_constrained,
        }
        # Phase 3: persist before stamping the response so the
        # artifact_id (when the write succeeded) travels with the
        # live result.
        if analysis_id and result:
            artifact_id = _persist_rewrite_artifact(
                analysis_id=analysis_id,
                source_sql=analysis.query_metrics.query_text or "",
                rewritten_sql=result,
                model=model,
                feedback=feedback,
                parent_id=parent_id,
                user_email=user_email,
                output_format=output_format,
            )
            if artifact_id:
                resp["artifact_id"] = artifact_id
        _rewrite_tasks[task_id] = {"status": "completed", "result": resp}
    except Exception as e:
        logger.exception("Rewrite task %s failed", task_id)
        _rewrite_tasks[task_id] = {"status": "failed", "error": str(e)}


# ---------------------------------------------------------------------------
# Rewrite routes (start task + poll status)
# ---------------------------------------------------------------------------


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

    # v6.7.9: drop the misleading "Auto-pick" UX where
    # ``recommend_rewrite_model`` only emitted a banner and never
    # actually changed the chosen model. Default to sonnet-4-6 (max
    # output 64K — biggest budget in the catalogue, so the diff/
    # token_constrained path trips less often than with opus-4-7).
    # Users can still override via the model selector. ``LLM_MODEL``
    # env var keeps working for ops overrides.
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
    parent_id = data.get("parent_id", "").strip()

    # Codex Q4: capture trusted user identity at request time so the
    # background thread (which has no Flask request context) persists
    # rows as the right owner. Empty in dev / local-flask runs.
    from services.user_context import get_user_email_from_headers

    user_email = get_user_email_from_headers(request.headers)

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
            feedback,
            previous_rewrite,
            analysis_id,
            parent_id,
            user_email,
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


# ---------------------------------------------------------------------------
# History endpoint (Phase 3 — v6.7.5)
# ---------------------------------------------------------------------------


def _is_history_admin(user_email: str) -> bool:
    """Codex Q4: admin override for the history endpoint.

    Mirrors ``routes.feedback._admin_allowed``. When
    ``REWRITE_HISTORY_ADMIN_EMAILS`` is unset we run in dev mode (all
    callers admin, warning logged once per process). When set, only
    case-insensitive matches in the comma-separated list count as
    admins.
    """
    allowlist = (os.environ.get(_ADMIN_ENV_KEY) or "").strip()
    if not allowlist:
        if not getattr(_is_history_admin, "_warned", False):
            logger.warning(
                "Rewrite history endpoint running without %s configured — "
                "all authenticated users have admin access. Configure the "
                "env var in production.",
                _ADMIN_ENV_KEY,
            )
            _is_history_admin._warned = True  # type: ignore[attr-defined]
        return True
    if not user_email:
        return False
    allowed = {e.strip().lower() for e in allowlist.split(",") if e.strip()}
    return user_email.strip().lower() in allowed


@bp.route("/api/v1/rewrite/history")
def rewrite_history():
    """Return persisted rewrite attempts.

    Either ``analysis_id`` (rewrites for one analysis, history view) or
    ``source_sql_hash`` (rewrites of the same SQL across analyses /
    models / refine chains, multi-model compare view) is required.

    Both filters can be combined when the caller wants the
    intersection (e.g. "history of this analysis_id AND this source
    sql variant").

    Codex Q4 (v6.7.6): non-admin callers only see their own rows
    (matched on ``X-Forwarded-Email``). Admin emails listed in
    ``REWRITE_HISTORY_ADMIN_EMAILS`` see everything.
    """
    from services.user_context import get_user_email_from_headers

    user_email = get_user_email_from_headers(request.headers)

    analysis_id = request.args.get("analysis_id", "").strip() or None
    sha = request.args.get("source_sql_hash", "").strip() or None
    limit = request.args.get("limit", default=100, type=int)

    if not analysis_id and not sha:
        return jsonify({"error": "analysis_id or source_sql_hash required"}), 400

    try:
        from services.table_reader import TableReader
        from services.table_writer import TableWriterConfig

        config = TableWriterConfig.from_env()
        if not config.http_path:
            return jsonify({"items": [], "persistence": "disabled"})
        reader = TableReader(config)
        owner_filter = None if _is_history_admin(user_email) else (user_email or None)
        if owner_filter is None and not _is_history_admin(user_email):
            # Caller has no identity (no forwarded headers) AND is not
            # admin — return empty rather than leaking everything.
            return jsonify({
                "items": [],
                "count": 0,
                "filter": {
                    "analysis_id": analysis_id,
                    "source_sql_hash": sha,
                    "owner_only": True,
                },
                "persistence": "no_identity",
            })
        rows = reader.list_rewrite_artifacts(
            analysis_id=analysis_id,
            source_sql_hash=sha,
            user_email=owner_filter,
            limit=min(max(limit, 1), 500),
        )
    except Exception:
        logger.exception("Failed to load rewrite history")
        return jsonify({"items": [], "persistence": "error"})

    # Convert datetime to ISO so the response stays JSON-safe.
    out = []
    for row in rows:
        record = dict(row)
        ts = record.get("created_at")
        if ts is not None and hasattr(ts, "isoformat"):
            record["created_at"] = ts.isoformat()
        out.append(record)

    return jsonify({
        "items": out,
        "count": len(out),
        "filter": {
            "analysis_id": analysis_id,
            "source_sql_hash": sha,
            "owner_only": bool(owner_filter),
        },
    })
