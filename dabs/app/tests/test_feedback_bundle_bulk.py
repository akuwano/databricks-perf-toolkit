"""Tests for L5 Phase 1.5 bulk bundle (Codex-modified spec, 2026-04-26)."""

from __future__ import annotations

import io
import json
import os
import sys
import zipfile
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.feedback_bundle import (
    BulkAnalysisInput,
    BulkSources,
    BULK_HARD_SIZE_LIMIT_BYTES,
    BULK_MAX_ANALYSES,
    build_bulk_bundle_zip,
)


def _zip_files(blob: bytes) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        return {n: zf.read(n) for n in zf.namelist()}


# ---- Layout ----


def test_bulk_zip_has_directory_layout_per_analysis():
    """Codex (b): each analysis gets its own bundles/<aid>/ subdir."""
    sources = BulkSources(
        workspace_slug="acme",
        workspace_id="ws-001",
        tool_version="6.5.0-test",
        analyses=[
            BulkAnalysisInput(
                analysis_id="aid-A",
                report_markdown="# A",
                canonical_report={},
                raw_profile_json_text='{"query": {"queryText": "SELECT 1"}}',
                feedback_rows=[],
            ),
            BulkAnalysisInput(
                analysis_id="aid-B",
                report_markdown="# B",
                canonical_report={},
                raw_profile_json_text="",
                feedback_rows=[],
            ),
        ],
    )
    files = _zip_files(build_bulk_bundle_zip(sources).bytes_)
    expected_subset = {
        "manifest.json",
        "README.txt",
        "checksums.json",
        "orphan_feedback.json",
        "bundles/aid-A/report.md",
        "bundles/aid-A/canonical_report.json",
        "bundles/aid-A/profile_redacted.json",
        "bundles/aid-A/feedback.json",
        "bundles/aid-B/report.md",
    }
    assert expected_subset.issubset(set(files.keys()))


def test_bulk_zip_never_includes_raw_profile_json():
    """Codex (e): bulk profile.json is NEVER included."""
    sources = BulkSources(
        workspace_slug="acme",
        analyses=[
            BulkAnalysisInput(
                analysis_id="aid-A",
                raw_profile_json_text='{"sensitive": "secret"}',
            ),
        ],
    )
    files = _zip_files(build_bulk_bundle_zip(sources).bytes_)
    assert "bundles/aid-A/profile.json" not in files
    # And the redacted variant must exist
    assert "bundles/aid-A/profile_redacted.json" in files


def test_bulk_manifest_contains_codex_required_fields():
    """Codex (i): workspace_id, workspace_slug, exported_at, date_range,
    bundle_count, feedback_count, schema_version, source_app_version."""
    sources = BulkSources(
        workspace_slug="acme",
        workspace_id="ws-001",
        tool_version="6.5.0-test",
        since_ts="2026-04-01T00:00:00Z",
        until_ts="2026-04-26T00:00:00Z",
        analyses=[
            BulkAnalysisInput(
                analysis_id="aid-A",
                feedback_rows=[
                    {
                        "feedback_id": "f1",
                        "analysis_id": "aid-A",
                        "user_email": "alice@databricks.com",
                        "created_at": "2026-04-15T00:00:00Z",
                        "category": "missed_observation",
                        "free_text": "x",
                    }
                ],
            ),
        ],
    )
    files = _zip_files(build_bulk_bundle_zip(sources).bytes_)
    manifest = json.loads(files["manifest.json"])
    for key in (
        "workspace_id",
        "workspace_slug",
        "exported_at",
        "date_range",
        "bundle_count",
        "feedback_count",
        "schema_version",
        "source_app_version",
    ):
        assert key in manifest, f"missing {key}"
    assert manifest["schema_version"] == 2
    assert manifest["bundle_count"] == 1
    assert manifest["feedback_count"] >= 1
    assert manifest["date_range"]["since"] == "2026-04-01T00:00:00Z"
    assert manifest["profile_included"] is False


def test_bulk_manifest_redact_stats_aggregate():
    """Codex (i): per_bundle stats kept + aggregate exposed."""
    sources = BulkSources(
        workspace_slug="acme",
        analyses=[
            BulkAnalysisInput(
                analysis_id="aid-A",
                raw_profile_json_text='{"query": {"queryText": "SELECT * FROM t WHERE x = \'secret\' LIMIT 99"}}',
            ),
            BulkAnalysisInput(
                analysis_id="aid-B",
                raw_profile_json_text='{"query": {"queryText": "SELECT 1 FROM dual LIMIT 5"}}',
            ),
        ],
    )
    files = _zip_files(build_bulk_bundle_zip(sources).bytes_)
    manifest = json.loads(files["manifest.json"])
    rs = manifest["redact_stats"]
    assert "per_bundle" in rs
    assert "aggregate" in rs
    assert isinstance(rs["per_bundle"], list)
    assert rs["aggregate"]["sql_redacted_count"] >= 1


# ---- Orphan handling (Codex (f)) ----


def test_orphan_feedback_keeps_required_reason_field():
    sources = BulkSources(
        workspace_slug="acme",
        analyses=[],
        orphan_feedback_rows=[
            {
                "feedback_id": "orf-1",
                "analysis_id": None,
                "user_email": "x@y.com",
                "created_at": "2026-04-26T00:00:00Z",
                "free_text": "orphan",
                "orphan_reason": "null_analysis_id",
            },
            {
                # No orphan_reason → must default to 'unspecified'
                "feedback_id": "orf-2",
                "analysis_id": None,
                "user_email": "z@y.com",
                "created_at": "2026-04-26T00:00:01Z",
                "free_text": "no_reason",
            },
        ],
    )
    files = _zip_files(build_bulk_bundle_zip(sources).bytes_)
    rows = json.loads(files["orphan_feedback.json"])
    assert {r["orphan_reason"] for r in rows} == {"null_analysis_id", "unspecified"}
    # email must still be hashed
    for r in rows:
        assert "user_email" not in r
        assert r["user_email_hash"].startswith("sha256:")


def test_orphan_section_present_even_when_empty():
    """orphan_feedback.json is always emitted (consumer expects it)."""
    sources = BulkSources(workspace_slug="acme", analyses=[])
    files = _zip_files(build_bulk_bundle_zip(sources).bytes_)
    assert "orphan_feedback.json" in files
    assert json.loads(files["orphan_feedback.json"]) == []


# ---- Per-analysis feedback scoping ----


def test_each_bundle_only_carries_its_own_feedback():
    """Bulk has central + per-bundle feedback. The per-bundle file must
    only contain rows belonging to that analysis."""
    sources = BulkSources(
        workspace_slug="acme",
        analyses=[
            BulkAnalysisInput(
                analysis_id="aid-A",
                feedback_rows=[
                    {
                        "feedback_id": "fA1",
                        "analysis_id": "aid-A",
                        "user_email": "x@y.com",
                        "created_at": "2026-04-15T00:00:00Z",
                        "category": "missed_observation",
                        "free_text": "for A",
                    }
                ],
            ),
            BulkAnalysisInput(
                analysis_id="aid-B",
                feedback_rows=[
                    {
                        "feedback_id": "fB1",
                        "analysis_id": "aid-B",
                        "user_email": "x@y.com",
                        "created_at": "2026-04-16T00:00:00Z",
                        "category": "missed_observation",
                        "free_text": "for B",
                    }
                ],
            ),
        ],
    )
    files = _zip_files(build_bulk_bundle_zip(sources).bytes_)
    fb_a = json.loads(files["bundles/aid-A/feedback.json"])
    fb_b = json.loads(files["bundles/aid-B/feedback.json"])
    assert len(fb_a) == 1 and fb_a[0]["feedback_id"] == "fA1"
    assert len(fb_b) == 1 and fb_b[0]["feedback_id"] == "fB1"


# ---- Bulk-specific README ----


def test_bulk_readme_warns_no_raw_profile():
    sources = BulkSources(workspace_slug="acme", analyses=[])
    files = _zip_files(build_bulk_bundle_zip(sources).bytes_)
    readme = files["README.txt"].decode()
    assert "profile.json" in readme.lower()
    assert "never" in readme.lower()  # "is NEVER included"


# ---- Caps ----


def test_bulk_max_analyses_constant_is_capped_at_a_safe_value():
    assert BULK_MAX_ANALYSES <= 500
    assert BULK_HARD_SIZE_LIMIT_BYTES >= 50 * 1024 * 1024


# ---- Filename ----


def test_bulk_filename_uses_workspace_slug():
    sources = BulkSources(workspace_slug="my workspace 1", analyses=[])
    res = build_bulk_bundle_zip(sources)
    assert res.filename.startswith("feedback_bulk_")
    assert "my_workspace_1" in res.filename


# ---- Route-level: admin gate + token ----


@pytest.fixture()
def client():
    os.environ.setdefault("DATABRICKS_HOST", "https://test.cloud.databricks.com")
    from app import app as flask_app
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


def test_bulk_prepare_rejects_unauthorized(client):
    """When FEEDBACK_EXPORT_ADMIN_EMAILS is set and caller's email
    isn't in it, return 403."""
    with patch.dict(os.environ, {"FEEDBACK_EXPORT_ADMIN_EMAILS": "admin@x.com"}):
        res = client.post(
            "/api/v1/feedback/bundle/bulk/prepare",
            headers={"X-Forwarded-Email": "rando@y.com"},
        )
        assert res.status_code == 403
        assert res.get_json()["error"] == "forbidden"


def test_bulk_prepare_dev_mode_when_env_unset(client):
    """No FEEDBACK_EXPORT_ADMIN_EMAILS set → dev mode allows everyone."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("FEEDBACK_EXPORT_ADMIN_EMAILS", None)
        with patch("services.TableWriterConfig") as MC:
            MC.from_env.return_value.enabled = False
            MC.from_env.return_value.http_path = ""
            res = client.post(
                "/api/v1/feedback/bundle/bulk/prepare",
                headers={"X-Forwarded-Email": "anyone@y.com"},
            )
            # writes_disabled (503) — not a 403, proving dev_mode admit
            assert res.status_code in (200, 503)


def test_bulk_download_rejects_invalid_token(client):
    """Token bound to per-analysis IDs should not work for /bulk."""
    with patch.dict(os.environ, {}, clear=False):
        os.environ.pop("FEEDBACK_EXPORT_ADMIN_EMAILS", None)
        res = client.get(
            "/api/v1/feedback/bundle/bulk?token=garbage",
            headers={"X-Forwarded-Email": "anyone@y.com"},
        )
        assert res.status_code == 403


def test_bulk_export_page_renders(client):
    """Page should render even without admin (shows the 403 message)."""
    with patch.dict(os.environ, {"FEEDBACK_EXPORT_ADMIN_EMAILS": "admin@x.com"}):
        res = client.get(
            "/feedback/export",
            headers={"X-Forwarded-Email": "rando@y.com"},
        )
        assert res.status_code == 200
        body = res.data.decode()
        assert "権限がありません" in body or "Permission" in body
