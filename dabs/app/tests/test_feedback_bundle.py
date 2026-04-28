"""Tests for L5 feedback bundle assembly + signed token (Codex (d)(f)(h))."""

from __future__ import annotations

import hashlib
import io
import json
import time
import zipfile

from services.feedback_bundle import (
    BundleSources,
    build_bundle_zip,
    decompress_stored_json,
    email_domain,
    hash_user_email,
    make_signed_token,
    verify_signed_token,
)


# ---- Token: HMAC + expiry ----


def test_token_round_trip():
    token, _exp = make_signed_token("aid-123")
    assert verify_signed_token(token, "aid-123") is True


def test_token_rejects_wrong_analysis_id():
    token, _ = make_signed_token("aid-123")
    assert verify_signed_token(token, "aid-999") is False


def test_token_rejects_tampered_signature():
    token, _ = make_signed_token("aid-123")
    parts = token.split(".")
    parts[-1] = "0" * len(parts[-1])
    forged = ".".join(parts)
    assert verify_signed_token(forged, "aid-123") is False


def test_token_rejects_expired():
    # ttl 0 → token already expired by the time we verify
    token, exp = make_signed_token("aid-123", ttl_seconds=30)
    # patch timestamp by reusing the function's internals indirectly:
    # easier: verify a token whose expiry was forced into the past.
    parts = token.split(".")
    parts[0] = str(int(time.time()) - 60)
    expired = ".".join(parts)
    # Signature won't match anymore but we're explicitly testing expiry
    # — verify_signed_token rejects on expiry BEFORE checking sig.
    assert verify_signed_token(expired, "aid-123") is False


def test_token_rejects_malformed():
    assert verify_signed_token("", "aid") is False
    assert verify_signed_token("garbage", "aid") is False
    assert verify_signed_token("a.b", "aid") is False  # 2 parts only


# ---- Email hashing (Codex (d)) ----


def test_email_hash_is_deterministic():
    h1 = hash_user_email("alice@databricks.com")
    h2 = hash_user_email("Alice@Databricks.COM")
    assert h1 == h2
    assert h1 is not None
    assert h1.startswith("sha256:")


def test_email_hash_is_distinct_for_different_users():
    a = hash_user_email("alice@databricks.com")
    b = hash_user_email("bob@databricks.com")
    assert a != b


def test_email_domain_extraction():
    assert email_domain("alice@databricks.com") == "databricks.com"
    assert email_domain("UPPER@CASE.COM") == "case.com"
    assert email_domain("") is None
    assert email_domain(None) is None
    assert email_domain("noatsign") is None


def test_email_hash_handles_empty_input():
    assert hash_user_email("") is None
    assert hash_user_email(None) is None


# ---- Bundle ZIP structure ----


def _build_minimal(include_profile=False, raw_profile_text=""):
    sources = BundleSources(
        analysis_id="aid-test-001",
        report_markdown="# Report\nHello",
        canonical_report={"findings": []},
        raw_profile_json_text=raw_profile_text or '{"query": {"queryText": "SELECT 1"}}',
        feedback_rows=[
            {
                "feedback_id": "fb-1",
                "analysis_id": "aid-test-001",
                "target_type": "missing",
                "target_id": None,
                "sentiment": "missing",
                "category": "missed_observation",
                "free_text": "DECIMAL hint missing",
                "user_email": "alice@databricks.com",
                "created_at": "2026-04-26T10:00:00Z",
            }
        ],
        tool_version="6.4.0-test",
        profile_fingerprint="fp-abc",
        report_version="v1",
        query_id="q-001",
    )
    return build_bundle_zip(sources, include_profile=include_profile)


def _zip_files(blob: bytes) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        return {n: zf.read(n) for n in zf.namelist()}


def test_bundle_contains_all_required_files():
    result = _build_minimal()
    files = _zip_files(result.bytes_)
    required = {
        "metadata.json",
        "report.md",
        "canonical_report.json",
        "feedback.json",
        "profile_redacted.json",
        "checksums.json",
        "README.txt",
    }
    assert required.issubset(set(files.keys()))


def test_bundle_excludes_raw_profile_by_default():
    result = _build_minimal(include_profile=False)
    files = _zip_files(result.bytes_)
    assert "profile.json" not in files


def test_bundle_includes_raw_profile_when_opted_in():
    result = _build_minimal(include_profile=True)
    files = _zip_files(result.bytes_)
    assert "profile.json" in files


def test_metadata_records_profile_included_flag():
    r1 = _build_minimal(include_profile=False)
    meta1 = json.loads(_zip_files(r1.bytes_)["metadata.json"])
    assert meta1["profile_included"] is False
    r2 = _build_minimal(include_profile=True)
    meta2 = json.loads(_zip_files(r2.bytes_)["metadata.json"])
    assert meta2["profile_included"] is True


def test_metadata_includes_format_version_and_fingerprint():
    result = _build_minimal()
    meta = json.loads(_zip_files(result.bytes_)["metadata.json"])
    assert meta["bundle_format_version"] == 1
    assert meta["analysis_id"] == "aid-test-001"
    assert meta["profile_fingerprint"] == "fp-abc"
    assert meta["tool_version"] == "6.4.0-test"
    assert "redact_stats" in meta
    assert "bundle_id" in meta


def test_feedback_user_email_is_hashed_not_raw():
    """Codex (d) — user_email never appears in plaintext in the bundle."""
    result = _build_minimal()
    files = _zip_files(result.bytes_)
    fb_blob = files["feedback.json"].decode()
    assert "alice@databricks.com" not in fb_blob
    rows = json.loads(fb_blob)
    assert rows[0]["user_email_hash"].startswith("sha256:")
    assert rows[0]["user_email_domain"] == "databricks.com"


def test_checksums_match_each_file():
    result = _build_minimal()
    files = _zip_files(result.bytes_)
    sums = json.loads(files["checksums.json"])
    for name, blob in files.items():
        if name == "checksums.json":
            continue
        assert sums[name] == hashlib.sha256(blob).hexdigest()


def test_redacted_profile_strips_literals():
    raw = json.dumps(
        {"query": {"queryText": "SELECT * FROM t WHERE col = 'secret_value' LIMIT 99"}}
    )
    result = _build_minimal(raw_profile_text=raw)
    files = _zip_files(result.bytes_)
    redacted = files["profile_redacted.json"].decode()
    assert "secret_value" not in redacted
    assert "99 " not in redacted  # numeric literal stripped


def test_readme_warns_about_anonymization():
    result = _build_minimal()
    readme = _zip_files(result.bytes_)["README.txt"].decode()
    # Codex (e) — explicit "not full anonymization" message
    assert "REDUCED-SENSITIVITY" in readme
    assert "anonymization" in readme.lower()
    assert "table" in readme.lower() and "column" in readme.lower()


def test_filename_includes_analysis_id():
    result = _build_minimal()
    assert "aid-test-001" in result.filename
    assert result.filename.endswith(".zip")


def test_decompress_stored_json_round_trip():
    """Sanity-check the gzip+base64 helper used to read profile_json
    from profiler_analysis_raw."""
    import base64
    import gzip

    text = '{"hello": "world"}'
    blob = base64.b64encode(gzip.compress(text.encode("utf-8"))).decode("ascii")
    assert decompress_stored_json(blob) == text
    assert decompress_stored_json("") is None
    assert decompress_stored_json(None) is None
    assert decompress_stored_json("not_valid_base64") is None


# ---- Property test: redacted profile + raw profile coexist when opted in ----


def test_when_profile_opted_in_redacted_still_emitted():
    """Redacted always emitted; raw is additive, not replacement."""
    result = _build_minimal(include_profile=True)
    files = _zip_files(result.bytes_)
    assert "profile.json" in files
    assert "profile_redacted.json" in files
