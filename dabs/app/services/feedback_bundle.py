"""L5 customer feedback bundle assembly (Codex-modified spec, 2026-04-26).

Builds a ZIP bundle that customers can download from /shared/<id> and
forward to the vendor. Two endpoints in routes/feedback.py drive this:
  * POST  /api/v1/feedback/bundle/<analysis_id>/prepare
        → returns a short-lived signed token (5 min HMAC).
  * GET   /api/v1/feedback/bundle/<analysis_id>?token=<>&include_profile=<bool>
        → streams the ZIP.

Bundle contents (Codex spec):
  metadata.json        — bundle_format_version=1, redact_stats, ts, fingerprint
  report.md            — markdown the user read
  canonical_report.json — compact canonical Report
  feedback.json        — feedback rows, user_email→hash + domain only
  profile_redacted.json — always; SQL literals stripped, names retained
  checksums.json       — SHA256 per file (manifest tampering detection)
  README.txt           — explicit "reduced sensitivity, NOT anonymized"
  profile.json         — opt-in only when customer ticks the checkbox

Security notes:
- ``user_email`` is hashed with a per-deployment salt + lowered email.
  The ``user_email_domain`` is preserved separately for triage org-level
  attribution without per-user identification (Codex (d) recommendation).
- Token is HMAC-SHA256 over (analysis_id, expiry, nonce). Expiry default
  300s. We don't store tokens; verification is stateless.
- The public /shared/<id> link permits viewing only — bundle download
  always requires a token from /prepare. (Codex (d) recommendation.)
"""

from __future__ import annotations

import base64
import gzip
import hashlib
import hmac
import io
import json
import logging
import os
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# Token TTL — short enough that a leaked /shared link doesn't grant
# long-lived ZIP download. 5 minutes covers slow networks but no longer.
_TOKEN_TTL_SECONDS_DEFAULT = 300

# Per-deployment secrets. In production these live in env / runtime
# config. The fallback values are intentionally weak so dev catches
# missing config quickly.
_BUNDLE_SECRET_ENV = "FEEDBACK_BUNDLE_SECRET"
_EMAIL_SALT_ENV = "FEEDBACK_EMAIL_SALT"
_BUNDLE_SECRET_FALLBACK = "dev-bundle-secret-CHANGE-IN-PROD"
_EMAIL_SALT_FALLBACK = "dev-email-salt-CHANGE-IN-PROD"

# Bundle size guards (Codex (h)). Soft warning at 50 MB, hard cap at 100 MB.
SOFT_SIZE_LIMIT_BYTES = 50 * 1024 * 1024
HARD_SIZE_LIMIT_BYTES = 100 * 1024 * 1024


# ---------------------------------------------------------------------------
# Identity hashing — Codex (d): never bundle raw user_email.
# ---------------------------------------------------------------------------


def _email_salt() -> str:
    return os.environ.get(_EMAIL_SALT_ENV) or _EMAIL_SALT_FALLBACK


def hash_user_email(email: str | None) -> str | None:
    if not email:
        return None
    salted = f"{_email_salt()}:{email.strip().lower()}"
    return "sha256:" + hashlib.sha256(salted.encode("utf-8")).hexdigest()[:16]


def email_domain(email: str | None) -> str | None:
    if not email or "@" not in email:
        return None
    return email.split("@", 1)[1].strip().lower()


# ---------------------------------------------------------------------------
# Signed token
# ---------------------------------------------------------------------------


def _bundle_secret() -> str:
    return os.environ.get(_BUNDLE_SECRET_ENV) or _BUNDLE_SECRET_FALLBACK


def make_signed_token(
    analysis_id: str,
    *,
    ttl_seconds: int = _TOKEN_TTL_SECONDS_DEFAULT,
) -> tuple[str, int]:
    """Issue a token bound to ``analysis_id``. Returns ``(token, expires_at)``."""
    expires_at = int(time.time()) + max(30, int(ttl_seconds))
    nonce = uuid.uuid4().hex[:16]
    payload = f"{analysis_id}:{expires_at}:{nonce}"
    sig = hmac.new(
        _bundle_secret().encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{expires_at}.{nonce}.{sig}", expires_at


def verify_signed_token(token: str, analysis_id: str) -> bool:
    """Return True iff the token is intact, unexpired, and bound to id."""
    if not token or not analysis_id:
        return False
    parts = token.split(".")
    if len(parts) != 3:
        return False
    expires_at_s, nonce, sig = parts
    try:
        expires_at = int(expires_at_s)
    except ValueError:
        return False
    if expires_at < int(time.time()):
        return False
    payload = f"{analysis_id}:{expires_at}:{nonce}"
    expected = hmac.new(
        _bundle_secret().encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(sig, expected)


# ---------------------------------------------------------------------------
# Profile JSON decompression (matches table_writer._compress_json)
# ---------------------------------------------------------------------------


def decompress_stored_json(stored: str | None) -> str | None:
    """Inverse of services.table_writer._compress_json."""
    if not stored:
        return None
    try:
        decompressed = gzip.decompress(base64.b64decode(stored))
        return decompressed.decode("utf-8")
    except Exception:
        logger.exception("Failed to decompress stored JSON")
        return None


# ---------------------------------------------------------------------------
# Bundle assembly
# ---------------------------------------------------------------------------


@dataclass
class BundleSources:
    """Inputs needed to build a bundle. Caller (route) is responsible for
    fetching these from Delta — keeps this module easy to unit-test."""

    analysis_id: str
    report_markdown: str = ""
    canonical_report: dict | None = None
    raw_profile_json_text: str | None = None  # raw, NOT redacted yet
    feedback_rows: list[dict] | None = None  # rows from profiler_feedback
    tool_version: str = ""
    profile_fingerprint: str = ""
    report_version: str = ""
    query_id: str = ""


@dataclass
class BundleResult:
    bytes_: bytes
    filename: str
    bundle_id: str
    metadata: dict


_README_BODY = """\
# Profiler Feedback Bundle

This ZIP file contains a Databricks SQL Profiler analysis result and
the feedback the customer entered for that analysis. It was generated
by the customer and may contain operational data from their
environment.

## ⚠ This is REDUCED-SENSITIVITY, not full anonymization

`profile_redacted.json` strips:
  * SQL literals (numbers, strings) inside SQL / filter expressions
  * SQL comments
  * File paths, error messages, prepared-statement parameters
  * Min / max / clustering-key bounds (raw values)

`profile_redacted.json` PRESERVES:
  * Schema names: catalog, schema, table, column identifiers
  * Operator metadata (peak memory, shuffle bytes, durations)
  * Plan structure and statistics aggregates

If table or column names themselves are considered sensitive in your
context, please review the bundle before sending it. The vendor will
treat names as schema-level identifiers, not as PII.

## Files

  metadata.json          — bundle format version, fingerprints, redact stats
  report.md              — the human-facing performance report
  canonical_report.json  — structured Findings / Actions
  feedback.json          — feedback rows; user_email is salted+hashed
  profile_redacted.json  — reduced-sensitivity profile (always present)
  checksums.json         — SHA256 of every file above
  profile.json           — RAW profile (only present if you opted in)

## What the vendor uses this for

  * Identifying recurring quality gaps in recommendations
  * Adding new golden cases that prevent regressions
  * Improving the analysis pipeline based on real workloads

## Removing the bundle from circulation

The bundle is stateless on the customer side — once downloaded it
exists only as the file you forwarded. There is no automatic upload.
If you delete the file, no copy remains in the application.
"""


def _serialize_compact(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _serialize_pretty(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")


def _sanitize_feedback_rows(rows: list[dict] | None) -> list[dict]:
    """Strip user_email → hash + domain. Sort by created_at ASC.

    Codex (d) requirement.
    """
    if not rows:
        return []
    cleaned: list[dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        email = r.get("user_email") or ""
        clone = {
            "feedback_id": r.get("feedback_id"),
            "analysis_id": r.get("analysis_id"),
            "target_type": r.get("target_type"),
            "target_id": r.get("target_id"),
            "sentiment": r.get("sentiment"),
            "category": r.get("category"),
            "free_text": r.get("free_text"),
            "user_email_hash": hash_user_email(email),
            "user_email_domain": email_domain(email),
            "created_at": (
                r["created_at"].isoformat()
                if hasattr(r.get("created_at"), "isoformat")
                else r.get("created_at")
            ),
        }
        cleaned.append(clone)
    cleaned.sort(key=lambda x: str(x.get("created_at") or ""))
    return cleaned


def build_bundle_zip(
    sources: BundleSources,
    *,
    include_profile: bool = False,
) -> BundleResult:
    """Assemble the ZIP bytes + metadata. Caller is responsible for
    surfacing it as an HTTP response (``send_file`` / similar)."""
    from .profile_redactor import redact_profile

    bundle_id = str(uuid.uuid4())
    exported_at = datetime.now(timezone.utc).isoformat()

    # ---- Profile redaction (always) -------------------------------------
    profile_redacted: Any = {}
    redact_stats: dict = {}
    raw_text = sources.raw_profile_json_text or ""
    if raw_text:
        try:
            parsed = json.loads(raw_text)
            redacted_obj, stats = redact_profile(parsed)
            profile_redacted = redacted_obj
            redact_stats = {
                "sql_redacted_count": stats.sql_redacted_count,
                "parse_failures": stats.parse_failures,
                "unparseable_sql_paths": stats.unparseable_sql_paths,
                "opaque_redacted_count": stats.opaque_redacted_count,
                "bounds_redacted_count": stats.bounds_redacted_count,
                "comments_stripped_count": stats.comments_stripped_count,
            }
        except Exception:
            logger.exception("Profile redaction failed; bundling empty redacted")
            profile_redacted = {"error": "redaction_failed"}
            redact_stats = {"error": "redaction_failed"}

    # ---- Feedback rows --------------------------------------------------
    feedback_payload = _sanitize_feedback_rows(sources.feedback_rows)

    # ---- Canonical report -----------------------------------------------
    canonical_payload = sources.canonical_report or {}

    # ---- Metadata -------------------------------------------------------
    metadata = {
        "bundle_format_version": 1,
        "bundle_id": bundle_id,
        "analysis_id": sources.analysis_id,
        "tool_version": sources.tool_version,
        "exported_at": exported_at,
        "profile_fingerprint": sources.profile_fingerprint,
        "report_version": sources.report_version,
        "query_id": sources.query_id,
        "profile_included": include_profile,
        "feedback_count": len(feedback_payload),
        "redact_stats": redact_stats,
    }

    # ---- File map -------------------------------------------------------
    files: dict[str, bytes] = {
        "metadata.json": _serialize_pretty(metadata),
        "report.md": (sources.report_markdown or "").encode("utf-8"),
        "canonical_report.json": _serialize_compact(canonical_payload),
        "feedback.json": _serialize_compact(feedback_payload),
        "profile_redacted.json": _serialize_compact(profile_redacted),
        "README.txt": _README_BODY.encode("utf-8"),
    }
    if include_profile and raw_text:
        files["profile.json"] = raw_text.encode("utf-8")

    # ---- Checksums (Codex (h)) -----------------------------------------
    checksums = {
        name: hashlib.sha256(blob).hexdigest()
        for name, blob in files.items()
    }
    files["checksums.json"] = _serialize_pretty(checksums)

    # ---- Build ZIP ------------------------------------------------------
    buf = io.BytesIO()
    # ZIP_DEFLATED is required for the size to actually shrink.
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, blob in files.items():
            zf.writestr(name, blob)
    raw = buf.getvalue()

    filename = (
        f"feedback_bundle_{sources.analysis_id}_{exported_at.replace(':', '').replace('-', '')[:14]}Z.zip"
    )
    return BundleResult(bytes_=raw, filename=filename, bundle_id=bundle_id, metadata=metadata)


# ---------------------------------------------------------------------------
# Bulk bundle (L5 Phase 1.5, Codex-modified spec, 2026-04-26)
#
# Codex (b) layout: bundles/<analysis_id>/{report.md,canonical_report.json,
# profile_redacted.json,feedback.json}. orphan_feedback.json captures
# feedback rows with no resolvable analysis_id. profile.json is NEVER
# included in bulk (Codex (e)).
# ---------------------------------------------------------------------------

# Codex (d) recommended caps. The size cap dominates: at 100 MB the
# email/SSO upload pipeline becomes painful for many customers.
BULK_MAX_ANALYSES = 200
BULK_HARD_SIZE_LIMIT_BYTES = 100 * 1024 * 1024  # 100 MB
BULK_SCHEMA_VERSION = 2  # bundle_format_version=2 means bulk layout

_BULK_README = """\
# Profiler Feedback Bulk Bundle

This ZIP packages multiple per-analysis bundles plus orphan feedback
rows from one workspace into a single download. Use it for monthly
batch sharing with the vendor.

## ⚠ This is REDUCED-SENSITIVITY, not full anonymization

Each ``bundles/<analysis_id>/`` subdirectory mirrors the structure of a
single per-analysis bundle, with one exception: **``profile.json`` (raw)
is NEVER included in bulk exports** to prevent accidental oversharing.
If you need the raw profile for a single analysis, use the per-analysis
ZIP from ``/shared/<analysis_id>``.

## Files

  manifest.json          — workspace_id, exported_at, date_range,
                           feedback_count, bundle_count, schema_version=2,
                           source_app_version, redact_stats (aggregate)
  bundles/<aid>/         — one self-contained reduced-sensitivity bundle
                           per analysis_id, same shape as per-analysis ZIP
                           minus profile.json
  orphan_feedback.json   — feedback rows whose analysis_id is null,
                           missing, or pointing at a deleted analysis;
                           each row has an `orphan_reason`
  checksums.json         — SHA256 of every file above
  README.txt             — this file

## Removal

The bundle is stateless on the customer side — once downloaded it
exists only as the file you forwarded. There is no automatic upload.
If you delete the file, no copy remains in the application.
"""


@dataclass
class BulkAnalysisInput:
    """One analysis worth of source data inside a bulk bundle.

    The route layer pre-fetches these from Delta and hands a list to
    ``build_bulk_bundle_zip``. Keeping the per-analysis pre-fetch
    explicit (rather than letting this module talk to Delta) makes
    the unit tests trivial.
    """

    analysis_id: str
    report_markdown: str = ""
    canonical_report: dict | None = None
    raw_profile_json_text: str | None = None
    feedback_rows: list[dict] | None = None  # ALL feedback for this analysis
    profile_fingerprint: str = ""
    report_version: str = ""
    query_id: str = ""


@dataclass
class BulkSources:
    workspace_slug: str = ""
    workspace_id: str = ""
    tool_version: str = ""
    since_ts: str | None = None  # ISO 8601 string or None
    until_ts: str | None = None
    analyses: list[BulkAnalysisInput] | None = None
    # Orphan feedback rows: feedback whose analysis_id is empty, null,
    # or pointing at an analysis we couldn't find. The route layer is
    # responsible for tagging each row with ``orphan_reason``.
    orphan_feedback_rows: list[dict] | None = None


def _build_per_analysis_files(item: BulkAnalysisInput) -> tuple[dict[str, bytes], dict]:
    """Build the file map for one ``bundles/<aid>/`` subdirectory.

    Returns ``(files, redact_stats)`` so the manifest can aggregate
    redact stats across analyses.
    """
    from .profile_redactor import redact_profile

    files: dict[str, bytes] = {}

    # Profile redaction
    profile_redacted: Any = {}
    redact_stats: dict = {}
    raw_text = item.raw_profile_json_text or ""
    if raw_text:
        try:
            parsed = json.loads(raw_text)
            redacted_obj, stats = redact_profile(parsed)
            profile_redacted = redacted_obj
            redact_stats = {
                "sql_redacted_count": stats.sql_redacted_count,
                "parse_failures": stats.parse_failures,
                "unparseable_sql_paths": stats.unparseable_sql_paths,
                "opaque_redacted_count": stats.opaque_redacted_count,
                "bounds_redacted_count": stats.bounds_redacted_count,
                "comments_stripped_count": stats.comments_stripped_count,
            }
        except Exception:
            logger.exception(
                "Profile redaction failed for %s; emitting empty redacted",
                item.analysis_id,
            )
            profile_redacted = {"error": "redaction_failed"}
            redact_stats = {"error": "redaction_failed"}

    # Sanitize feedback rows scoped to THIS analysis (not the whole bulk).
    feedback_payload = _sanitize_feedback_rows(item.feedback_rows)

    files["report.md"] = (item.report_markdown or "").encode("utf-8")
    files["canonical_report.json"] = _serialize_compact(item.canonical_report or {})
    files["profile_redacted.json"] = _serialize_compact(profile_redacted)
    files["feedback.json"] = _serialize_compact(feedback_payload)

    return files, redact_stats


def _aggregate_redact_stats(per_bundle: list[dict]) -> dict:
    """Aggregate counter fields across bundles. Per Codex (i) we keep
    bundle-level stats but expose totals on the manifest for quick
    triage."""
    totals = {
        "sql_redacted_count": 0,
        "parse_failures": 0,
        "opaque_redacted_count": 0,
        "bounds_redacted_count": 0,
        "comments_stripped_count": 0,
        "unparseable_paths_count": 0,
        "redaction_errors": 0,
    }
    for s in per_bundle:
        if not isinstance(s, dict):
            continue
        if s.get("error"):
            totals["redaction_errors"] += 1
            continue
        for k in ("sql_redacted_count", "parse_failures", "opaque_redacted_count",
                  "bounds_redacted_count", "comments_stripped_count"):
            totals[k] += int(s.get(k, 0) or 0)
        totals["unparseable_paths_count"] += len(s.get("unparseable_sql_paths") or [])
    return totals


def _sanitize_orphan_rows(rows: list[dict] | None) -> list[dict]:
    """Sanitize orphan feedback rows the same way as per-analysis, plus
    require ``orphan_reason`` on each. Codex (f) recommendation."""
    if not rows:
        return []
    out: list[dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        email = r.get("user_email") or ""
        out.append({
            "feedback_id": r.get("feedback_id"),
            "analysis_id": r.get("analysis_id"),
            "target_type": r.get("target_type"),
            "target_id": r.get("target_id"),
            "sentiment": r.get("sentiment"),
            "category": r.get("category"),
            "free_text": r.get("free_text"),
            "user_email_hash": hash_user_email(email),
            "user_email_domain": email_domain(email),
            "created_at": (
                r["created_at"].isoformat()
                if hasattr(r.get("created_at"), "isoformat")
                else r.get("created_at")
            ),
            # Codex (f): orphan_reason is REQUIRED on every orphan row.
            "orphan_reason": r.get("orphan_reason") or "unspecified",
        })
    out.sort(key=lambda x: str(x.get("created_at") or ""))
    return out


def build_bulk_bundle_zip(sources: BulkSources) -> BundleResult:
    """Assemble the bulk ZIP. Caller is responsible for enforcing the
    admin gate; this function trusts its inputs."""
    bundle_id = str(uuid.uuid4())
    exported_at = datetime.now(timezone.utc).isoformat()

    analyses = list(sources.analyses or [])
    orphan_rows = _sanitize_orphan_rows(sources.orphan_feedback_rows)

    # ---- Per-bundle files -----------------------------------------------
    files: dict[str, bytes] = {}
    per_bundle_redact: list[dict] = []
    feedback_count_total = 0
    for item in analyses:
        sub_files, sub_stats = _build_per_analysis_files(item)
        per_bundle_redact.append(sub_stats)
        feedback_count_total += len(item.feedback_rows or [])
        prefix = f"bundles/{item.analysis_id}/"
        for name, blob in sub_files.items():
            files[prefix + name] = blob

    files["orphan_feedback.json"] = _serialize_compact(orphan_rows)

    # ---- Manifest (Codex (i) required fields) ---------------------------
    manifest = {
        "bundle_format_version": BULK_SCHEMA_VERSION,
        "bundle_id": bundle_id,
        "schema_version": BULK_SCHEMA_VERSION,
        "workspace_id": sources.workspace_id or "",
        "workspace_slug": sources.workspace_slug or "",
        "exported_at": exported_at,
        "date_range": {
            "since": sources.since_ts,
            "until": sources.until_ts,
        },
        "bundle_count": len(analyses),
        "feedback_count": feedback_count_total + len(orphan_rows),
        "feedback_count_by_scope": {
            "per_analysis": feedback_count_total,
            "orphan": len(orphan_rows),
        },
        "source_app_version": sources.tool_version or "",
        "redact_stats": {
            "per_bundle": per_bundle_redact,
            "aggregate": _aggregate_redact_stats(per_bundle_redact),
        },
        # profile_included is always False in bulk (Codex (e)) — keep
        # the field so ingestion pipelines don't have to special-case
        # the absence.
        "profile_included": False,
    }
    files["manifest.json"] = _serialize_pretty(manifest)
    files["README.txt"] = _BULK_README.encode("utf-8")

    # ---- Checksums + ZIP build (mirrors per-analysis path) -------------
    checksums = {
        name: hashlib.sha256(blob).hexdigest()
        for name, blob in files.items()
    }
    files["checksums.json"] = _serialize_pretty(checksums)

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, blob in files.items():
            zf.writestr(name, blob)
    raw = buf.getvalue()

    safe_slug = (sources.workspace_slug or "workspace").replace(" ", "_")[:40]
    ts_compact = exported_at.replace(":", "").replace("-", "")[:14] + "Z"
    filename = f"feedback_bulk_{safe_slug}_{ts_compact}.zip"
    return BundleResult(
        bytes_=raw,
        filename=filename,
        bundle_id=bundle_id,
        metadata=manifest,
    )
