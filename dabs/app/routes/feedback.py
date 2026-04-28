"""User feedback route (L5, 2026-04-26).

Codex spec:
- B (whole-report 欠落申告) only — no per-Action thumbs in v1
- user_email determined server-side from trusted Databricks Apps headers,
  ignoring any client-supplied identity (anti-spoofing)
- Delta storage via TableWriter
- Categories pinned to a small enum (4-5) — UI is free-form on top
"""

from __future__ import annotations

import io
import logging
from typing import Any

from flask import Blueprint, jsonify, render_template, request, send_file

logger = logging.getLogger(__name__)
bp = Blueprint("feedback", __name__)

# Codex (f) — minimal 5-category set. UI labels (in JA) are derived from
# this dict so adding a category later only touches one place.
ALLOWED_CATEGORIES: dict[str, str] = {
    "missed_observation": "重要観点の欠落 (例: 触れるべき問題が抜けている)",
    "wrong_recommendation": "誤った推奨 (例: 提案された fix が間違い・的外れ)",
    "wrong_sql": "SQL の誤り (構文エラー / 動作しない / 不適切)",
    "unclear_explanation": "説明が分かりにくい (理由・根拠が不明瞭)",
    "other": "その他",
}

ALLOWED_TARGET_TYPES = {"whole_report", "missing", "action", "finding"}
ALLOWED_SENTIMENTS = {"missing", "down", "up"}


@bp.route("/api/v1/feedback", methods=["POST"])
def submit_feedback() -> Any:
    """Persist a feedback record. Server-side determines user_email."""
    from services import TableWriter, TableWriterConfig
    from services.user_context import get_user_email_from_headers

    payload = request.get_json(silent=True) or {}

    category = str(payload.get("category", "")).strip()
    if category not in ALLOWED_CATEGORIES:
        return jsonify({"error": "invalid_category", "allowed": list(ALLOWED_CATEGORIES.keys())}), 400

    # target_type / sentiment default to the most common B-only path:
    # "missing" — user is reporting that something is absent from the report.
    target_type = str(payload.get("target_type", "missing")).strip() or "missing"
    if target_type not in ALLOWED_TARGET_TYPES:
        return jsonify({"error": "invalid_target_type", "allowed": list(ALLOWED_TARGET_TYPES)}), 400

    sentiment = str(payload.get("sentiment", "missing")).strip() or "missing"
    if sentiment not in ALLOWED_SENTIMENTS:
        return jsonify({"error": "invalid_sentiment", "allowed": list(ALLOWED_SENTIMENTS)}), 400

    free_text = str(payload.get("free_text", "")).strip()
    if not free_text and category != "other":
        # Other categories without a description aren't useful for triage.
        return jsonify({"error": "free_text_required"}), 400
    if len(free_text) > 4000:
        free_text = free_text[:4000]

    analysis_id = str(payload.get("analysis_id", "") or "").strip() or None
    target_id = str(payload.get("target_id", "") or "").strip() or None

    # Trusted user email — server-determined ONLY. Browser-provided fields
    # are deliberately ignored to prevent spoofing.
    user_email = get_user_email_from_headers(request.headers)

    try:
        config = TableWriterConfig.from_env()
    except Exception as e:  # nosec
        logger.warning("Feedback config load failed: %s", e)
        return jsonify({"error": "config_unavailable"}), 503

    if not config.enabled or not config.http_path:
        return jsonify({"error": "feedback_writes_disabled"}), 503

    writer = TableWriter(config)
    feedback_id = writer.write_feedback(
        analysis_id=analysis_id,
        target_type=target_type,
        sentiment=sentiment,
        category=category,
        free_text=free_text,
        user_email=user_email,
        target_id=target_id,
    )
    if feedback_id is None:
        return jsonify({"error": "write_failed"}), 500
    return jsonify({"feedback_id": feedback_id, "ok": True})


@bp.route("/api/v1/feedback/categories", methods=["GET"])
def list_categories() -> Any:
    """Return the allowed categories with JA UI labels.

    Used by the report template to render the dropdown without
    hard-coding labels in HTML.
    """
    return jsonify(
        {
            "categories": [
                {"id": cid, "label": label}
                for cid, label in ALLOWED_CATEGORIES.items()
            ]
        }
    )


# ---------------------------------------------------------------------------
# L5 Phase 1: feedback bundle (ZIP download for cross-tenant feedback collection)
#
# Two endpoints:
#   POST /api/v1/feedback/bundle/<analysis_id>/prepare  → signed token (5 min)
#   GET  /api/v1/feedback/bundle/<analysis_id>?token=<>&include_profile=<bool>
# Codex (d) demanded the split — the public /shared link grants viewing,
# not bundle download.
# ---------------------------------------------------------------------------


def _load_bundle_sources(analysis_id: str):
    """Fetch all data needed to build the bundle from Delta + rebuild
    canonical Report from the persisted analysis."""
    from services import TableWriterConfig
    from services.feedback_bundle import BundleSources, decompress_stored_json

    config = TableWriterConfig.from_env()
    if not config.enabled or not config.http_path:
        return None, "writes_disabled"

    # Header + report
    try:
        from services.table_reader import TableReader

        # TableReader reuses TableWriterConfig — same connection details.
        reader = TableReader(config)
        analysis_with_report = reader.get_analysis_with_report(analysis_id)
    except Exception as e:  # nosec
        logger.exception("Failed to load analysis header for bundle")
        return None, f"load_failed: {e}"

    if analysis_with_report is None:
        return None, "analysis_not_found"

    analysis = analysis_with_report.analysis
    report_md = analysis_with_report.report_markdown or ""

    # Raw profile JSON — read from profiler_analysis_raw directly
    raw_profile_text: str | None = None
    try:
        with reader._get_connection() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cursor:
                fqn = reader._fqn("profiler_analysis_raw")  # type: ignore[attr-defined]
                cursor.execute(
                    f"SELECT profile_json FROM {fqn} "
                    f"WHERE analysis_id = :aid ORDER BY analyzed_at DESC LIMIT 1",
                    parameters={"aid": analysis_id},
                )
                row = cursor.fetchone()
                if row and row[0]:
                    raw_profile_text = decompress_stored_json(row[0])
    except Exception:
        logger.exception("Failed to load raw profile JSON; bundle will skip profile")

    # Feedback rows
    feedback_rows: list[dict] = []
    try:
        with reader._get_connection() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cursor:
                fqn = reader._fqn("profiler_feedback")  # type: ignore[attr-defined]
                cursor.execute(
                    f"SELECT feedback_id, analysis_id, target_type, target_id, "
                    f"sentiment, category, free_text, user_email, created_at "
                    f"FROM {fqn} WHERE analysis_id = :aid ORDER BY created_at ASC",
                    parameters={"aid": analysis_id},
                )
                cols = [d[0] for d in cursor.description]
                for row in cursor.fetchall():
                    feedback_rows.append(dict(zip(cols, row)))
    except Exception:
        logger.exception("Failed to load feedback rows; continuing with empty list")

    # Canonical report — rebuild from analysis (loose; LLM-direct
    # canonical isn't persisted, so we use the normalizer adapter).
    canonical: dict = {}
    try:
        from core.v6_schema import build_canonical_report

        canonical = build_canonical_report(analysis, llm_text=report_md, language="ja")
    except Exception:
        logger.exception("Failed to rebuild canonical report; using empty payload")

    tool_version = ""
    try:
        from app import APP_VERSION  # noqa: WPS433

        tool_version = APP_VERSION
    except Exception:
        tool_version = ""

    sources = BundleSources(
        analysis_id=analysis_id,
        report_markdown=report_md,
        canonical_report=canonical,
        raw_profile_json_text=raw_profile_text,
        feedback_rows=feedback_rows,
        tool_version=tool_version,
        profile_fingerprint=getattr(analysis.query_metrics, "query_fingerprint", "") or "",
        report_version="v1",
        query_id=getattr(analysis.query_metrics, "query_id", "") or "",
    )
    return sources, None


@bp.route("/api/v1/feedback/bundle/<analysis_id>/prepare", methods=["POST"])
def prepare_bundle_token(analysis_id: str) -> Any:
    """Issue a short-lived signed token for the bundle download.

    The two-step flow (prepare → download) means a leaked /shared/<id>
    URL doesn't grant download access — the customer must intentionally
    request a fresh token.
    """
    from services.feedback_bundle import make_signed_token

    if not analysis_id or len(analysis_id) > 200:
        return jsonify({"error": "invalid_analysis_id"}), 400

    token, expires_at = make_signed_token(analysis_id)
    return jsonify(
        {
            "token": token,
            "expires_at": expires_at,
            "ttl_seconds": expires_at - int(__import__("time").time()),
        }
    )


@bp.route("/api/v1/feedback/bundle/<analysis_id>", methods=["GET"])
def download_bundle(analysis_id: str) -> Any:
    """Stream the ZIP bundle. Requires a fresh signed token in ?token=."""
    from services.feedback_bundle import (
        HARD_SIZE_LIMIT_BYTES,
        build_bundle_zip,
        verify_signed_token,
    )

    token = (request.args.get("token") or "").strip()
    if not verify_signed_token(token, analysis_id):
        return jsonify({"error": "invalid_or_expired_token"}), 403

    include_profile = (request.args.get("include_profile") or "").lower() in (
        "1", "true", "yes",
    )

    sources, err = _load_bundle_sources(analysis_id)
    if err == "analysis_not_found":
        return jsonify({"error": "analysis_not_found"}), 404
    if err is not None or sources is None:
        return jsonify({"error": err or "load_failed"}), 503

    result = build_bundle_zip(sources, include_profile=include_profile)
    if len(result.bytes_) > HARD_SIZE_LIMIT_BYTES:
        return (
            jsonify(
                {
                    "error": "bundle_too_large",
                    "size_bytes": len(result.bytes_),
                    "limit": HARD_SIZE_LIMIT_BYTES,
                    "hint": "include_profile=false で再試行してください",
                }
            ),
            413,
        )

    return send_file(
        io.BytesIO(result.bytes_),
        mimetype="application/zip",
        as_attachment=True,
        download_name=result.filename,
    )


# ---------------------------------------------------------------------------
# L5 Phase 1.5: bulk feedback bundle (Codex-modified spec)
# ---------------------------------------------------------------------------

# When set, only these emails (comma-separated, case-insensitive) can
# call /feedback/bundle/bulk/*. Empty / unset → dev mode: all-allowed
# with a warning logged. Codex (a) requirement.
_ADMIN_ENV_KEY = "FEEDBACK_EXPORT_ADMIN_EMAILS"

# A "bulk-special" analysis id used by the signed token; the prepare
# endpoint binds the token to this constant so verify_signed_token can
# stay generic. Period is illegal in token parts so this stays unique.
_BULK_TOKEN_BINDING = "__bulk__"


def _admin_allowed(user_email: str) -> tuple[bool, str]:
    """Return ``(allowed, reason)``. Codex (a) admin gating."""
    import os as _os
    allowlist = (_os.environ.get(_ADMIN_ENV_KEY) or "").strip()
    if not allowlist:
        # Dev mode: permit but log so we don't accidentally ship without
        # configuring the gate in prod.
        logger.warning(
            "Bulk feedback export running without %s configured — all "
            "authenticated users may export. Configure the env var in "
            "production.",
            _ADMIN_ENV_KEY,
        )
        return True, "dev_mode"
    if not user_email:
        return False, "missing_user_email"
    allowed = {e.strip().lower() for e in allowlist.split(",") if e.strip()}
    if user_email.strip().lower() in allowed:
        return True, "allowlisted"
    return False, "not_in_allowlist"


def _parse_iso_date(s: str | None):
    """Lenient ISO 8601 parser → datetime or None.

    Accepts plain dates ("2026-04-01") and full timestamps with offset.
    """
    if not s:
        return None
    from datetime import datetime as _dt
    txt = s.strip()
    if not txt:
        return None
    # Allow trailing Z (UTC)
    txt = txt.replace("Z", "+00:00")
    try:
        return _dt.fromisoformat(txt)
    except ValueError:
        try:
            return _dt.fromisoformat(txt + "T00:00:00+00:00")
        except ValueError:
            return None


def _summarize_bulk_scope(reader, since_ts, until_ts) -> dict:
    """Run a cheap COUNT/MIN over profiler_feedback to populate the
    prepare summary. Returns counts + recommended size estimate without
    touching the analysis tables."""
    from services.feedback_bundle import hash_user_email  # noqa: WPS433

    summary = {
        "feedback_count": 0,
        "distinct_analyses": 0,
        "oldest_feedback_at": None,
        "newest_feedback_at": None,
        "all_time_count": 0,
        "all_time_distinct_analyses": 0,
        "estimated_size_mb": 0.0,
        "limits_hit": [],
    }
    try:
        with reader._get_connection() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cursor:
                fqn = reader._fqn("profiler_feedback")  # type: ignore[attr-defined]
                # Window-scoped totals
                where = []
                params: dict[str, object] = {}
                if since_ts:
                    where.append("created_at >= :since_ts")
                    params["since_ts"] = since_ts
                if until_ts:
                    where.append("created_at <= :until_ts")
                    params["until_ts"] = until_ts
                where_sql = ("WHERE " + " AND ".join(where)) if where else ""
                cursor.execute(
                    f"SELECT COUNT(1), COUNT(DISTINCT analysis_id), "
                    f"MIN(created_at), MAX(created_at) "
                    f"FROM {fqn} {where_sql}",
                    parameters=params,
                )
                row = cursor.fetchone()
                if row:
                    summary["feedback_count"] = int(row[0] or 0)
                    summary["distinct_analyses"] = int(row[1] or 0)
                    summary["oldest_feedback_at"] = (
                        row[2].isoformat() if hasattr(row[2], "isoformat") else None
                    )
                    summary["newest_feedback_at"] = (
                        row[3].isoformat() if hasattr(row[3], "isoformat") else None
                    )
                # All-time totals (Codex (c): show "全期間との差分")
                cursor.execute(
                    f"SELECT COUNT(1), COUNT(DISTINCT analysis_id) FROM {fqn}",
                )
                row_all = cursor.fetchone()
                if row_all:
                    summary["all_time_count"] = int(row_all[0] or 0)
                    summary["all_time_distinct_analyses"] = int(row_all[1] or 0)
        # Rough size estimate: 30 KB / analysis (redacted). Caller can
        # rely on this for a soft warning; the hard cap is enforced when
        # the ZIP is actually built.
        from services.feedback_bundle import BULK_MAX_ANALYSES, BULK_HARD_SIZE_LIMIT_BYTES

        per_analysis_kb = 30
        size_bytes = summary["distinct_analyses"] * per_analysis_kb * 1024
        summary["estimated_size_mb"] = round(size_bytes / (1024 * 1024), 2)
        if summary["distinct_analyses"] > BULK_MAX_ANALYSES:
            summary["limits_hit"].append("count")
        if size_bytes > BULK_HARD_SIZE_LIMIT_BYTES:
            summary["limits_hit"].append("size")
    except Exception:
        logger.exception("Failed to summarize bulk scope; returning zeros")
    return summary


def _load_bulk_sources(since_ts, until_ts):
    """Load all the data the bulk bundle needs in one pass.

    Codex (d): we cap at ``BULK_MAX_ANALYSES`` analyses; any feedback
    rows beyond that cap or whose analysis_id is unreachable end up in
    ``orphan_feedback`` with an explicit ``orphan_reason``.
    """
    from services import TableWriterConfig
    from services.feedback_bundle import (
        BULK_MAX_ANALYSES,
        BulkAnalysisInput,
        BulkSources,
        decompress_stored_json,
    )

    config = TableWriterConfig.from_env()
    if not config.enabled or not config.http_path:
        return None, "writes_disabled"

    from services.table_reader import TableReader
    reader = TableReader(config)

    # 1) Fetch feedback rows in window
    feedback_by_aid: dict[str, list[dict]] = {}
    orphan_rows: list[dict] = []
    try:
        with reader._get_connection() as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cursor:
                fqn = reader._fqn("profiler_feedback")  # type: ignore[attr-defined]
                where = []
                params: dict[str, object] = {}
                if since_ts:
                    where.append("created_at >= :since_ts")
                    params["since_ts"] = since_ts
                if until_ts:
                    where.append("created_at <= :until_ts")
                    params["until_ts"] = until_ts
                where_sql = ("WHERE " + " AND ".join(where)) if where else ""
                cursor.execute(
                    f"SELECT feedback_id, analysis_id, target_type, target_id, "
                    f"sentiment, category, free_text, user_email, created_at "
                    f"FROM {fqn} {where_sql} ORDER BY created_at ASC",
                    parameters=params,
                )
                cols = [d[0] for d in cursor.description]
                for row in cursor.fetchall():
                    rd = dict(zip(cols, row))
                    aid = rd.get("analysis_id") or ""
                    if not aid:
                        rd["orphan_reason"] = "null_analysis_id"
                        orphan_rows.append(rd)
                        continue
                    feedback_by_aid.setdefault(aid, []).append(rd)
    except Exception:
        logger.exception("Failed to load feedback rows for bulk export")
        return None, "load_failed"

    # 2) Cap the analysis set; rows beyond the cap → orphan
    aids = list(feedback_by_aid.keys())
    capped = aids[:BULK_MAX_ANALYSES]
    overflow = aids[BULK_MAX_ANALYSES:]
    for aid in overflow:
        for r in feedback_by_aid[aid]:
            r["orphan_reason"] = "exceeded_bulk_cap"
            orphan_rows.append(r)

    # 3) Per-analysis sources (header + raw profile + canonical)
    inputs: list[BulkAnalysisInput] = []
    try:
        from core.v6_schema import build_canonical_report  # noqa: WPS433
    except Exception:
        build_canonical_report = None  # type: ignore[assignment]

    raw_fqn = reader._fqn("profiler_analysis_raw")  # type: ignore[attr-defined]
    for aid in capped:
        try:
            awr = reader.get_analysis_with_report(aid)
        except Exception:
            awr = None
        if awr is None:
            # analysis row missing → all of this aid's feedback is orphan
            for r in feedback_by_aid[aid]:
                r["orphan_reason"] = "analysis_missing"
                orphan_rows.append(r)
            continue
        analysis = awr.analysis
        report_md = awr.report_markdown or ""

        # Raw profile
        raw_text: str | None = None
        try:
            with reader._get_connection() as conn:  # type: ignore[attr-defined]
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"SELECT profile_json FROM {raw_fqn} "
                        f"WHERE analysis_id = :aid ORDER BY analyzed_at DESC LIMIT 1",
                        parameters={"aid": aid},
                    )
                    row = cursor.fetchone()
                    if row and row[0]:
                        raw_text = decompress_stored_json(row[0])
        except Exception:
            logger.exception("Failed to load raw profile for %s", aid)

        canonical: dict = {}
        if build_canonical_report is not None:
            try:
                canonical = build_canonical_report(analysis, llm_text=report_md, language="ja")
            except Exception:
                logger.exception("canonical rebuild failed for %s", aid)

        inputs.append(BulkAnalysisInput(
            analysis_id=aid,
            report_markdown=report_md,
            canonical_report=canonical,
            raw_profile_json_text=raw_text,
            feedback_rows=feedback_by_aid[aid],
            profile_fingerprint=getattr(analysis.query_metrics, "query_fingerprint", "") or "",
            report_version="v1",
            query_id=getattr(analysis.query_metrics, "query_id", "") or "",
        ))

    tool_version = ""
    try:
        from app import APP_VERSION  # noqa: WPS433
        tool_version = APP_VERSION
    except Exception:
        tool_version = ""

    sources = BulkSources(
        workspace_slug=config.catalog or "workspace",  # rough proxy for now
        workspace_id=config.catalog or "",
        tool_version=tool_version,
        since_ts=(since_ts.isoformat() if since_ts else None),
        until_ts=(until_ts.isoformat() if until_ts else None),
        analyses=inputs,
        orphan_feedback_rows=orphan_rows,
    )
    return sources, None


@bp.route("/api/v1/feedback/bundle/bulk/prepare", methods=["POST"])
def prepare_bulk_bundle_token() -> Any:
    """Issue a signed token + summary for the bulk export modal."""
    from services import TableWriterConfig
    from services.feedback_bundle import make_signed_token
    from services.table_reader import TableReader
    from services.user_context import get_user_email_from_headers

    user_email = get_user_email_from_headers(request.headers)
    allowed, reason = _admin_allowed(user_email)
    if not allowed:
        return jsonify({"error": "forbidden", "reason": reason}), 403

    payload = request.get_json(silent=True) or {}
    since_ts = _parse_iso_date(payload.get("since"))
    until_ts = _parse_iso_date(payload.get("until"))

    config = TableWriterConfig.from_env()
    if not config.enabled or not config.http_path:
        return jsonify({"error": "writes_disabled"}), 503

    reader = TableReader(config)
    summary = _summarize_bulk_scope(reader, since_ts, until_ts)
    token, expires_at = make_signed_token(_BULK_TOKEN_BINDING)
    return jsonify(
        {
            "token": token,
            "expires_at": expires_at,
            "summary": summary,
            "admin_mode": reason,
        }
    )


@bp.route("/api/v1/feedback/bundle/bulk", methods=["GET"])
def download_bulk_bundle() -> Any:
    """Stream the bulk ZIP. Token + admin gate enforced."""
    from services import TableWriter, TableWriterConfig
    from services.feedback_bundle import (
        BULK_HARD_SIZE_LIMIT_BYTES,
        build_bulk_bundle_zip,
        email_domain,
        hash_user_email,
        verify_signed_token,
    )
    from services.user_context import get_user_email_from_headers

    user_email = get_user_email_from_headers(request.headers)
    allowed, _reason = _admin_allowed(user_email)
    if not allowed:
        return jsonify({"error": "forbidden"}), 403

    token = (request.args.get("token") or "").strip()
    if not verify_signed_token(token, _BULK_TOKEN_BINDING):
        return jsonify({"error": "invalid_or_expired_token"}), 403

    since_ts = _parse_iso_date(request.args.get("since"))
    until_ts = _parse_iso_date(request.args.get("until"))

    sources, err = _load_bulk_sources(since_ts, until_ts)
    if err is not None or sources is None:
        return jsonify({"error": err or "load_failed"}), 503

    if not sources.analyses and not sources.orphan_feedback_rows:
        return jsonify({"error": "no_feedback_in_range"}), 404

    result = build_bulk_bundle_zip(sources)

    # Enforce size cap. On overflow we ask the user to narrow the date
    # window rather than streaming a paginated download (Codex (d)).
    if len(result.bytes_) > BULK_HARD_SIZE_LIMIT_BYTES:
        return (
            jsonify(
                {
                    "error": "bulk_bundle_too_large",
                    "size_bytes": len(result.bytes_),
                    "limit": BULK_HARD_SIZE_LIMIT_BYTES,
                    "hint": "since/until を狭めて再試行してください",
                }
            ),
            413,
        )

    # Audit log (Codex (a))
    try:
        config = TableWriterConfig.from_env()
        TableWriter(config).write_feedback_export_audit(
            export_id=result.bundle_id,
            workspace_slug=sources.workspace_slug,
            user_email_hash=hash_user_email(user_email),
            user_email_domain=email_domain(user_email),
            scope="bulk",
            since_ts=since_ts,
            until_ts=until_ts,
            feedback_count=int(result.metadata.get("feedback_count") or 0),
            bundle_count=int(result.metadata.get("bundle_count") or 0),
            size_bytes=len(result.bytes_),
            profile_included=False,
            success=True,
        )
    except Exception:
        logger.exception("Audit log write failed; continuing")

    return send_file(
        io.BytesIO(result.bytes_),
        mimetype="application/zip",
        as_attachment=True,
        download_name=result.filename,
    )


@bp.route("/feedback/export", methods=["GET"])
def feedback_export_page() -> Any:
    """Workspace-admin landing page for the bulk feedback export.

    Codex (a) demanded this be a separate page from /history so the
    permission boundary is explicit. The page itself does not query
    Delta — it relies on /api/v1/feedback/bundle/bulk/prepare for the
    summary the user sees before clicking Download.
    """
    from services.user_context import get_user_email_from_headers

    user_email = get_user_email_from_headers(request.headers)
    allowed, reason = _admin_allowed(user_email)
    return render_template(
        "feedback_export.html",
        admin_allowed=allowed,
        admin_reason=reason,
        admin_user_email=user_email,
    )
