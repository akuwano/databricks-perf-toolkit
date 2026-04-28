"""Route tests for L5 feedback endpoint (Codex spec, 2026-04-26)."""

from __future__ import annotations

import json
import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture()
def client():
    os.environ.setdefault("DATABRICKS_HOST", "https://test.cloud.databricks.com")
    from app import app as flask_app
    flask_app.config["TESTING"] = True
    return flask_app.test_client()


# ---- Categories endpoint ----


def test_categories_returns_five_with_jp_labels(client):
    res = client.get("/api/v1/feedback/categories")
    assert res.status_code == 200
    data = res.get_json()
    cats = data["categories"]
    ids = [c["id"] for c in cats]
    assert "missed_observation" in ids
    assert "wrong_recommendation" in ids
    assert "wrong_sql" in ids
    assert "unclear_explanation" in ids
    assert "other" in ids
    # JP labels (first one mentions 重要観点)
    label_blob = " ".join(c["label"] for c in cats)
    assert "重要観点" in label_blob or "欠落" in label_blob


# ---- Validation ----


def test_invalid_category_returns_400(client):
    res = client.post(
        "/api/v1/feedback",
        json={"category": "garbage", "free_text": "x"},
    )
    assert res.status_code == 400
    assert res.get_json()["error"] == "invalid_category"


def test_empty_category_returns_400(client):
    res = client.post("/api/v1/feedback", json={})
    assert res.status_code == 400


def test_free_text_required_for_non_other(client):
    res = client.post(
        "/api/v1/feedback",
        json={"category": "missed_observation", "free_text": ""},
    )
    assert res.status_code == 400
    assert res.get_json()["error"] == "free_text_required"


def test_other_category_allows_empty_text(client):
    """'other' is the only category where empty text is accepted —
    user might just want to flag without details."""
    with patch("services.TableWriter") as MW, patch(
        "services.TableWriterConfig"
    ) as MC:
        MC.from_env.return_value.enabled = True
        MC.from_env.return_value.http_path = "/sql/1.0/warehouses/abc"
        MW.return_value.write_feedback.return_value = "fb-uuid"
        res = client.post(
            "/api/v1/feedback",
            json={"category": "other", "free_text": ""},
        )
        assert res.status_code == 200
        assert res.get_json()["ok"] is True


def test_per_action_feedback_persists_target_id(client):
    """target_type='action' + target_id='action_3' は per-card 改善要望
    のフロー。target_id は free-form 文字列として保存されることを確認。"""
    captured = {}
    with patch("services.TableWriter") as MW, patch(
        "services.TableWriterConfig"
    ) as MC:
        MC.from_env.return_value.enabled = True
        MC.from_env.return_value.http_path = "/sql/1.0/warehouses/abc"
        def _capture(**kwargs):
            captured.update(kwargs)
            return "fb-uuid"
        MW.return_value.write_feedback.side_effect = _capture
        res = client.post(
            "/api/v1/feedback",
            json={
                "category": "wrong_recommendation",
                "free_text": "Action #3 の DECIMAL 推奨はもっと具体的にしてほしい",
                "target_type": "action",
                "target_id": "action_3",
                "sentiment": "down",
            },
        )
        assert res.status_code == 200
        assert captured["target_type"] == "action"
        assert captured["target_id"] == "action_3"
        assert captured["sentiment"] == "down"


def test_invalid_target_type_rejected(client):
    res = client.post(
        "/api/v1/feedback",
        json={
            "category": "missed_observation",
            "free_text": "anything",
            "target_type": "bogus",
        },
    )
    assert res.status_code == 400


def test_writes_disabled_returns_503(client):
    """When TableWriter is disabled (e.g. local dev), the route must
    surface that distinctly from a generic write failure."""
    with patch("services.TableWriterConfig") as MC:
        MC.from_env.return_value.enabled = False
        MC.from_env.return_value.http_path = ""
        res = client.post(
            "/api/v1/feedback",
            json={"category": "missed_observation", "free_text": "x"},
        )
        assert res.status_code == 503
        assert "disabled" in res.get_json()["error"]


# ---- User identity from headers ----


def test_user_email_pulled_from_x_forwarded_email(client):
    """Server-side user_email determination — browser body is ignored."""
    captured = {}
    with patch("services.TableWriter") as MW, patch(
        "services.TableWriterConfig"
    ) as MC:
        MC.from_env.return_value.enabled = True
        MC.from_env.return_value.http_path = "/sql/1.0/warehouses/abc"
        def _capture(**kwargs):
            captured.update(kwargs)
            return "fb-uuid"
        MW.return_value.write_feedback.side_effect = _capture
        res = client.post(
            "/api/v1/feedback",
            headers={"X-Forwarded-Email": "alice@databricks.com"},
            json={
                "category": "missed_observation",
                "free_text": "DECIMAL was missing",
                # Attempt to spoof — must be ignored:
                "user_email": "attacker@evil.com",
            },
        )
        assert res.status_code == 200
        assert captured["user_email"] == "alice@databricks.com"


def test_anonymous_when_no_trusted_header(client):
    """Local dev path: no header → empty user_email, but write succeeds."""
    captured = {}
    with patch("services.TableWriter") as MW, patch(
        "services.TableWriterConfig"
    ) as MC:
        MC.from_env.return_value.enabled = True
        MC.from_env.return_value.http_path = "/sql/1.0/warehouses/abc"
        def _capture(**kwargs):
            captured.update(kwargs)
            return "fb-uuid"
        MW.return_value.write_feedback.side_effect = _capture
        res = client.post(
            "/api/v1/feedback",
            json={"category": "wrong_sql", "free_text": "syntax broken"},
        )
        assert res.status_code == 200
        assert captured["user_email"] == ""


# ---- Free text length cap ----


def test_free_text_truncated_to_4000_chars(client):
    captured = {}
    with patch("services.TableWriter") as MW, patch(
        "services.TableWriterConfig"
    ) as MC:
        MC.from_env.return_value.enabled = True
        MC.from_env.return_value.http_path = "/sql/1.0/warehouses/abc"
        def _capture(**kwargs):
            captured.update(kwargs)
            return "fb-uuid"
        MW.return_value.write_feedback.side_effect = _capture
        long_text = "a" * 5000
        res = client.post(
            "/api/v1/feedback",
            json={"category": "missed_observation", "free_text": long_text},
        )
        assert res.status_code == 200
        assert len(captured["free_text"]) == 4000


# ---- user_context helper ----


# ---- Bundle endpoints (L5 Phase 1) ----


def test_prepare_token_returns_token(client):
    res = client.post("/api/v1/feedback/bundle/aid-xyz/prepare")
    assert res.status_code == 200
    data = res.get_json()
    assert "token" in data
    assert data["token"]
    assert data["expires_at"] > 0


def test_prepare_token_rejects_huge_id(client):
    res = client.post("/api/v1/feedback/bundle/" + ("x" * 250) + "/prepare")
    assert res.status_code == 400


def test_bundle_download_rejects_missing_token(client):
    res = client.get("/api/v1/feedback/bundle/aid-xyz")
    assert res.status_code == 403
    assert "expired_token" in res.get_json()["error"]


def test_bundle_download_rejects_invalid_token(client):
    res = client.get("/api/v1/feedback/bundle/aid-xyz?token=garbage")
    assert res.status_code == 403


def test_bundle_download_404_when_analysis_missing(client):
    """With a valid token but no underlying analysis, returns 404."""
    from services.feedback_bundle import make_signed_token

    token, _ = make_signed_token("aid-not-found")
    with patch("services.TableWriterConfig") as MC:
        MC.from_env.return_value.enabled = True
        MC.from_env.return_value.http_path = "/sql/1.0/warehouses/abc"
        MC.from_env.return_value.catalog = "main"
        MC.from_env.return_value.schema = "profiler"
        MC.from_env.return_value.databricks_host = "test"
        MC.from_env.return_value.databricks_token = "test"
        with patch("services.table_reader.TableReader") as MR:
            MR.return_value.get_analysis_with_report.return_value = None
            res = client.get(
                f"/api/v1/feedback/bundle/aid-not-found?token={token}"
            )
            assert res.status_code == 404


def test_user_context_helper_first_present_wins():
    from services.user_context import get_user_email_from_headers
    # Email present → wins
    assert (
        get_user_email_from_headers([("X-Forwarded-Email", "a@x.com")])
        == "a@x.com"
    )
    # Email empty, falls through to preferred-username
    assert (
        get_user_email_from_headers(
            [("X-Forwarded-Email", ""), ("X-Forwarded-Preferred-Username", "alice")]
        )
        == "alice"
    )
    # All absent → empty
    assert get_user_email_from_headers([("Other", "x")]) == ""
    # None → empty
    assert get_user_email_from_headers(None) == ""
