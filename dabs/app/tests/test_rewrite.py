"""Tests for LLM-based query rewrite skill."""

import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.constants import Severity
from core.llm_prompts.prompts import (
    create_rewrite_fix_system_prompt,
    create_rewrite_fix_user_prompt,
    create_rewrite_system_prompt,
    create_rewrite_user_prompt,
)
from core.models import (
    ActionCard,
    Alert,
    BottleneckIndicators,
    OperatorHotspot,
    ProfileAnalysis,
    QueryMetrics,
    SQLAnalysis,
    TableScanMetrics,
)


def _make_analysis(**overrides) -> ProfileAnalysis:
    """Build a minimal ProfileAnalysis for rewrite tests."""
    defaults = dict(
        query_metrics=QueryMetrics(
            query_id="test-q1",
            query_text="SELECT o.*, c.name FROM orders o JOIN customers c ON o.cust_id = c.id WHERE o.total > 100",
            total_time_ms=15000,
            execution_time_ms=14000,
            read_bytes=500_000_000,
            spill_to_disk_bytes=200_000_000,
        ),
        bottleneck_indicators=BottleneckIndicators(
            spill_bytes=200_000_000,
            photon_ratio=0.3,
            alerts=[
                Alert(
                    severity=Severity.CRITICAL,
                    category="spill",
                    message="Disk spill 200MB detected",
                    metric_name="spill_bytes",
                    current_value="200MB",
                    threshold=">100MB",
                ),
                Alert(
                    severity=Severity.HIGH,
                    category="cache",
                    message="Cache hit ratio low",
                    metric_name="cache_hit_ratio",
                    current_value="15%",
                    threshold=">80%",
                ),
            ],
        ),
        action_cards=[
            ActionCard(
                problem="Disk spill on join",
                fix="Add broadcast hint",
                fix_sql="/*+ BROADCAST(c) */",
                expected_impact="Reduce shuffle by 80%",
            ),
        ],
        hot_operators=[
            OperatorHotspot(
                node_name="SortMergeJoin",
                duration_ms=8000,
                rows_out=500000,
            ),
        ],
        top_scanned_tables=[
            TableScanMetrics(
                table_name="orders",
                bytes_read=400_000_000,
                rows_scanned=1_000_000,
                files_read=80,
                files_pruned=20,
            ),
        ],
        sql_analysis=SQLAnalysis(),
        stage_info=[],
        data_flow=[],
    )
    defaults.update(overrides)
    return ProfileAnalysis(**defaults)


# =============================================================================
# Prompt construction tests
# =============================================================================


class TestRewriteSystemPrompt:
    """Tests for create_rewrite_system_prompt()."""

    def test_english_prompt_structure(self):
        prompt = create_rewrite_system_prompt("", "en")
        assert "rewriter" in prompt.lower()
        assert "exact same result set" in prompt
        assert "action plan" in prompt.lower()
        assert "Pre-execution" in prompt
        assert "ANALYZE TABLE" in prompt
        assert "Rewritten SQL" in prompt

    def test_japanese_prompt_structure(self):
        prompt = create_rewrite_system_prompt("", "ja")
        assert "リライター" in prompt
        assert "完全に同じ結果セット" in prompt
        assert "アクションプラン" in prompt
        assert "事前実行" in prompt
        assert "ANALYZE TABLE" in prompt
        assert "リライト後 SQL" in prompt

    def test_includes_knowledge(self):
        knowledge = "Use BROADCAST hint for small tables under 200MB."
        prompt = create_rewrite_system_prompt(knowledge, "en")
        assert "BROADCAST" in prompt
        assert "Tuning Knowledge" in prompt

    def test_serverless_constraints(self):
        prompt = create_rewrite_system_prompt("", "en", is_serverless=True)
        assert "Serverless" in prompt
        assert "SET parameter" in prompt or "SET パラメータ" in prompt

    def test_serverless_constraints_ja(self):
        prompt = create_rewrite_system_prompt("", "ja", is_serverless=True)
        assert "Serverless" in prompt
        assert "SET パラメータ" in prompt


class TestRewriteUserPrompt:
    """Tests for create_rewrite_user_prompt()."""

    def test_contains_original_sql(self):
        analysis = _make_analysis()
        prompt = create_rewrite_user_prompt(analysis, "en")
        assert "SELECT o.*, c.name" in prompt
        assert "Original SQL" in prompt

    def test_contains_alerts(self):
        analysis = _make_analysis()
        prompt = create_rewrite_user_prompt(analysis, "en")
        assert "[CRITICAL][spill]" in prompt
        assert "[HIGH][cache]" in prompt

    def test_contains_numbered_action_cards(self):
        analysis = _make_analysis()
        prompt = create_rewrite_user_prompt(analysis, "en")
        assert "#1:" in prompt
        assert "BROADCAST" in prompt
        assert "Disk spill on join" in prompt

    def test_contains_hot_operators(self):
        analysis = _make_analysis()
        prompt = create_rewrite_user_prompt(analysis, "en")
        assert "SortMergeJoin" in prompt

    def test_contains_table_scans(self):
        analysis = _make_analysis()
        prompt = create_rewrite_user_prompt(analysis, "en")
        assert "orders" in prompt

    def test_contains_key_metrics(self):
        analysis = _make_analysis()
        prompt = create_rewrite_user_prompt(analysis, "en")
        assert "Spill" in prompt
        assert "Photon" in prompt

    def test_preserves_full_sql(self):
        """Rewrite prompt must include the complete SQL — never truncate."""
        long_sql = "SELECT " + "col, " * 2000 + "id FROM t"
        analysis = _make_analysis(
            query_metrics=QueryMetrics(query_id="q2", query_text=long_sql, total_time_ms=1000)
        )
        prompt = create_rewrite_user_prompt(analysis, "en")
        assert "truncated" not in prompt
        assert long_sql in prompt


class TestRewriteTokenEstimation:
    """Tests for estimate_rewrite_tokens and recommend_rewrite_model."""

    def test_short_query_minimum(self):
        from core.llm import estimate_rewrite_tokens

        analysis = _make_analysis()
        tokens = estimate_rewrite_tokens(analysis)
        assert tokens >= 8192  # minimum floor

    def test_long_query_scales(self):
        from core.llm import estimate_rewrite_tokens

        long_sql = "SELECT " + "col, " * 5000 + "id FROM t"
        analysis = _make_analysis(
            query_metrics=QueryMetrics(query_id="q", query_text=long_sql, total_time_ms=1000)
        )
        tokens = estimate_rewrite_tokens(analysis)
        assert tokens > 8192

    def test_recommend_model_returns_dict(self):
        from core.llm import recommend_rewrite_model

        analysis = _make_analysis()
        rec = recommend_rewrite_model(analysis)
        assert "recommended_model" in rec
        assert "estimated_tokens" in rec
        assert "max_tokens" in rec

    def test_recommend_model_flags_constrained_for_huge_query(self):
        from core.llm import recommend_rewrite_model

        # ~200K chars → needs ~100K tokens, exceeds all models
        huge_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(40000)) + " FROM t"
        analysis = _make_analysis(
            query_metrics=QueryMetrics(query_id="q", query_text=huge_sql, total_time_ms=1000)
        )
        rec = recommend_rewrite_model(analysis)
        assert rec.get("token_constrained") is True
        assert "Diff" in rec.get("reason", "") or "diff" in rec.get("reason", "").lower()


class TestRewriteSystemPromptTokenConstrained:
    """Tests for token-constrained fallback in system prompt."""

    def test_en_diff_format_instructions(self):
        prompt = create_rewrite_system_prompt("", "en", token_constrained=True)
        assert "diff" in prompt.lower()
        assert "Manual Merge Steps" in prompt

    def test_ja_diff_format_instructions(self):
        prompt = create_rewrite_system_prompt("", "ja", token_constrained=True)
        assert "差分" in prompt
        assert "手動マージ手順" in prompt

    def test_not_present_when_unconstrained(self):
        prompt = create_rewrite_system_prompt("", "en", token_constrained=False)
        assert "Manual Merge Steps" not in prompt

    def test_japanese_labels(self):
        analysis = _make_analysis()
        prompt = create_rewrite_user_prompt(analysis, "ja")
        assert "ボトルネックアラート" in prompt
        assert "ホットオペレータ" in prompt


# =============================================================================
# API endpoint tests
# =============================================================================


@pytest.fixture()
def app():
    os.environ.setdefault("DATABRICKS_HOST", "https://test.cloud.databricks.com")
    from app import app as flask_app

    flask_app.config["TESTING"] = True
    return flask_app


class TestRewriteEndpoint:
    """Tests for POST /api/v1/rewrite."""

    def test_requires_analysis_id(self, app):
        with app.test_client() as c:
            r = c.post("/api/v1/rewrite", json={})
            assert r.status_code == 400
            assert "analysis_id" in r.get_json()["error"]

    def test_returns_404_for_unknown_id(self, app):
        with app.test_client() as c:
            r = c.post("/api/v1/rewrite", json={"analysis_id": "nonexistent"})
            assert r.status_code == 404

    def test_returns_400_when_no_sql(self, app):
        from app import analysis_store

        aid = "test-no-sql"
        analysis_store[aid] = {
            "status": "completed",
            "analysis": _make_analysis(
                query_metrics=QueryMetrics(query_id="q", query_text="", total_time_ms=0)
            ),
        }
        try:
            with app.test_client() as c:
                r = c.post("/api/v1/rewrite", json={"analysis_id": aid})
                assert r.status_code == 400
                assert "No SQL" in r.get_json()["error"]
        finally:
            analysis_store.pop(aid, None)

    def test_returns_task_id(self, app):
        """POST /api/v1/rewrite returns a task_id for async polling."""
        from app import analysis_store

        aid = "test-rewrite-async"
        analysis_store[aid] = {
            "status": "completed",
            "analysis": _make_analysis(),
        }
        try:
            with app.test_client() as c:
                r = c.post("/api/v1/rewrite", json={"analysis_id": aid})
                assert r.status_code == 200
                data = r.get_json()
                assert "task_id" in data
                assert data["status"] == "running"
        finally:
            analysis_store.pop(aid, None)

    @patch("core.llm.rewrite_with_llm", return_value="```sql\nSELECT 1\n```")
    @patch("core.llm_prompts.knowledge.load_tuning_knowledge", return_value="k")
    def test_poll_returns_completed(self, mock_knowledge, mock_rewrite, app):
        """GET /api/v1/rewrite/<task_id> returns completed result."""
        import time

        from app import analysis_store

        aid = "test-rewrite-poll"
        analysis_store[aid] = {
            "status": "completed",
            "analysis": _make_analysis(),
        }
        try:
            with app.test_client() as c:
                r = c.post("/api/v1/rewrite", json={"analysis_id": aid})
                task_id = r.get_json()["task_id"]
                # Wait for background thread
                for _ in range(20):
                    time.sleep(0.1)
                    pr = c.get(f"/api/v1/rewrite/{task_id}")
                    data = pr.get_json()
                    if data.get("status") != "running":
                        break
                assert data["status"] == "completed"
                assert "SELECT 1" in data["rewrite"]
        finally:
            analysis_store.pop(aid, None)

    @patch("routes.query_rewrite._load_analysis_for_rewrite", return_value=None)
    def test_returns_404_when_delta_also_missing(self, mock_load, app):
        """When analysis is not in memory AND not in Delta, return 404."""
        with app.test_client() as c:
            r = c.post("/api/v1/rewrite", json={"analysis_id": "missing-everywhere"})
            assert r.status_code == 404

    def test_poll_unknown_task_returns_404(self, app):
        with app.test_client() as c:
            r = c.get("/api/v1/rewrite/nonexistent")
            assert r.status_code == 404

    @patch(
        "core.llm.fix_rewrite_with_llm",
        return_value="Fix applied: fixed ON condition\n```sql\nSELECT 1\n```",
    )
    def test_feedback_fix(self, mock_fix, app):
        """POST /api/v1/rewrite with feedback triggers fix mode."""
        import time

        from app import analysis_store

        aid = "test-rewrite-fix"
        analysis_store[aid] = {
            "status": "completed",
            "analysis": _make_analysis(),
        }
        try:
            with app.test_client() as c:
                r = c.post(
                    "/api/v1/rewrite",
                    json={
                        "analysis_id": aid,
                        "feedback": "ON condition is wrong on line 5",
                        "previous_rewrite": "SELECT a FROM b JOIN c ON a.id = c.wrong_id",
                    },
                )
                task_id = r.get_json()["task_id"]
                for _ in range(20):
                    time.sleep(0.1)
                    pr = c.get(f"/api/v1/rewrite/{task_id}")
                    data = pr.get_json()
                    if data.get("status") != "running":
                        break
                assert data["status"] == "completed"
                assert "Fix applied" in data["rewrite"] or "fix" in data["rewrite"].lower()
                mock_fix.assert_called_once()
        finally:
            analysis_store.pop(aid, None)


# =============================================================================
# Fix prompt tests
# =============================================================================


class TestRewriteFixPrompts:
    """Tests for rewrite fix prompts."""

    def test_en_fix_system_prompt(self):
        prompt = create_rewrite_fix_system_prompt("en")
        assert "fixer" in prompt.lower()
        assert "ONLY the issue" in prompt
        assert "Fix applied" in prompt

    def test_ja_fix_system_prompt(self):
        prompt = create_rewrite_fix_system_prompt("ja")
        assert "修正者" in prompt
        assert "のみ" in prompt
        assert "修正内容" in prompt

    def test_fix_user_prompt_contains_all_parts(self):
        prompt = create_rewrite_fix_user_prompt(
            original_sql="SELECT a FROM t",
            previous_rewrite="SELECT /*+ BROADCAST(t) */ a FROM t",
            feedback="BROADCAST hint causes error",
            lang="en",
        )
        assert "SELECT a FROM t" in prompt
        assert "BROADCAST" in prompt
        assert "causes error" in prompt
        assert "Original SQL" in prompt
        assert "Previous Rewrite" in prompt
        assert "User Feedback" in prompt


# =============================================================================
# Validation endpoint tests
# =============================================================================


class TestValidateEndpoint:
    """Tests for POST /api/v1/rewrite/validate."""

    def test_requires_sql(self, app):
        with app.test_client() as c:
            r = c.post("/api/v1/rewrite/validate", json={})
            assert r.status_code == 400

    @patch("routes.query_rewrite._validate_with_explain")
    def test_explain_success(self, mock_explain, app):
        mock_explain.return_value = {"valid": True, "method": "explain"}
        with app.test_client() as c:
            r = c.post("/api/v1/rewrite/validate", json={"sql": "SELECT 1"})
            assert r.status_code == 200
            data = r.get_json()
            assert data["valid"] is True
            assert data["method"] == "explain"

    @patch("routes.query_rewrite._validate_with_explain")
    def test_explain_syntax_error(self, mock_explain, app):
        mock_explain.return_value = {
            "valid": False,
            "error": "PARSE_SYNTAX_ERROR",
            "method": "explain",
        }
        with app.test_client() as c:
            r = c.post("/api/v1/rewrite/validate", json={"sql": "SELEC 1"})
            data = r.get_json()
            assert data["valid"] is False
            assert data["method"] == "explain"

    @patch(
        "routes.query_rewrite._validate_with_explain", side_effect=RuntimeError("PERMISSION_DENIED")
    )
    def test_fallback_to_sqlglot(self, mock_explain, app):
        with app.test_client() as c:
            r = c.post("/api/v1/rewrite/validate", json={"sql": "SELECT 1"})
            data = r.get_json()
            assert data["valid"] is True
            assert data["method"] == "sqlglot"
            assert "fallback_reason" in data

    @patch(
        "routes.query_rewrite._validate_with_explain", side_effect=RuntimeError("connection refused")
    )
    def test_fallback_sqlglot_syntax_error(self, mock_explain, app):
        with app.test_client() as c:
            r = c.post("/api/v1/rewrite/validate", json={"sql": "SELEC BADQUERY FROM"})
            data = r.get_json()
            assert data["valid"] is False
            assert data["method"] == "sqlglot"


class TestRewritePage:
    """Tests for GET /rewrite/<analysis_id> (Phase 2 dedicated UX, v6.7.4)."""

    def test_renders_with_completed_analysis(self, app):
        from app import analysis_store

        aid = "rewrite-page-ok"
        analysis_store[aid] = {
            "status": "completed",
            "analysis": _make_analysis(),
        }
        try:
            with app.test_client() as c:
                r = c.get(f"/rewrite/{aid}")
                assert r.status_code == 200
                body = r.get_data(as_text=True)
                # Source SQL is rendered into the page (substring check
                # — full SQL with HTML-escaped chars also fine).
                assert "SELECT o.*, c.name FROM orders o JOIN" in body
                # Toolbar buttons present so the page is the new
                # dedicated UX, not a re-render of shared_result.html.
                assert "Generate rewrite" in body or "rw-go" in body
                assert "rw-history" in body
        finally:
            analysis_store.pop(aid, None)

    def test_returns_404_when_analysis_missing(self, app):
        with app.test_client() as c:
            r = c.get("/rewrite/this-id-does-not-exist")
            assert r.status_code == 404

    def test_returns_400_when_analysis_has_no_sql(self, app):
        from app import analysis_store

        aid = "rewrite-page-empty-sql"
        analysis = _make_analysis(
            query_metrics=QueryMetrics(query_id="empty", query_text="")
        )
        analysis_store[aid] = {"status": "completed", "analysis": analysis}
        try:
            with app.test_client() as c:
                r = c.get(f"/rewrite/{aid}")
                assert r.status_code == 400
        finally:
            analysis_store.pop(aid, None)


class TestRewritePersistence:
    """Phase 3 (v6.7.5) — RewriteArtifact append-only persistence."""

    def test_hash_source_sql_is_stable_under_whitespace_collapse(self):
        from routes.query_rewrite import _hash_source_sql

        assert _hash_source_sql("SELECT 1") == _hash_source_sql("  SELECT   1  ")
        assert _hash_source_sql("SELECT 1\nFROM t") == _hash_source_sql("SELECT 1 FROM t")

    def test_hash_source_sql_is_case_sensitive(self):
        """Object names are case-sensitive on Databricks; the hash
        must NOT collapse case."""
        from routes.query_rewrite import _hash_source_sql

        assert _hash_source_sql("SELECT * FROM t") != _hash_source_sql("select * from T")

    @patch("routes.query_rewrite._persist_rewrite_artifact", return_value="art-123")
    @patch("core.llm.rewrite_with_llm", return_value="```sql\nSELECT 1\n```")
    def test_completed_response_carries_artifact_id(self, mock_llm, mock_persist, app):
        """When persistence succeeds, the completed-task response
        includes ``artifact_id`` so the UI can link history back to
        the live result."""
        from app import analysis_store

        aid = "rewrite-persist-1"
        analysis_store[aid] = {"status": "completed", "analysis": _make_analysis()}
        try:
            with app.test_client() as c:
                r = c.post("/api/v1/rewrite", json={"analysis_id": aid})
                assert r.status_code == 200
                task_id = r.get_json()["task_id"]
                # Poll a bit (background thread)
                import time as _t
                for _ in range(40):
                    pr = c.get(f"/api/v1/rewrite/{task_id}")
                    data = pr.get_json()
                    if data.get("status") != "running":
                        break
                    _t.sleep(0.05)
                assert data["status"] == "completed"
                assert data.get("artifact_id") == "art-123"
                # Persistence was called with the right inputs.
                mock_persist.assert_called_once()
                kwargs = mock_persist.call_args.kwargs
                assert kwargs["analysis_id"] == aid
                assert "SELECT 1" in kwargs["rewritten_sql"]
        finally:
            analysis_store.pop(aid, None)

    @patch("routes.query_rewrite._persist_rewrite_artifact", return_value=None)
    @patch("core.llm.rewrite_with_llm", return_value="SELECT 1")
    def test_persistence_failure_does_not_break_response(self, mock_llm, mock_persist, app):
        from app import analysis_store

        aid = "rewrite-persist-fail"
        analysis_store[aid] = {"status": "completed", "analysis": _make_analysis()}
        try:
            with app.test_client() as c:
                r = c.post("/api/v1/rewrite", json={"analysis_id": aid})
                task_id = r.get_json()["task_id"]
                import time as _t
                for _ in range(40):
                    data = c.get(f"/api/v1/rewrite/{task_id}").get_json()
                    if data.get("status") != "running":
                        break
                    _t.sleep(0.05)
                assert data["status"] == "completed"
                assert data["rewrite"]
                assert "artifact_id" not in data  # write returned None
        finally:
            analysis_store.pop(aid, None)

    @patch("routes.query_rewrite._persist_rewrite_artifact", return_value="art-2")
    @patch("core.llm.fix_rewrite_with_llm", return_value="SELECT /*+ BROADCAST(d) */ 2")
    def test_parent_id_round_trip_on_refine(self, mock_fix, mock_persist, app):
        """Codex Q5 (v6.7.6): when the UI sends ``parent_id`` on a
        feedback / refine call, the persistence helper must receive
        the same value so the refine chain is reconstructable."""
        from app import analysis_store

        aid = "rewrite-parent-1"
        analysis_store[aid] = {"status": "completed", "analysis": _make_analysis()}
        try:
            with app.test_client() as c:
                r = c.post(
                    "/api/v1/rewrite",
                    json={
                        "analysis_id": aid,
                        "feedback": "use BROADCAST",
                        "previous_rewrite": "SELECT 1",
                        "parent_id": "art-1",
                    },
                )
                assert r.status_code == 200
                task_id = r.get_json()["task_id"]
                import time as _t
                for _ in range(40):
                    data = c.get(f"/api/v1/rewrite/{task_id}").get_json()
                    if data.get("status") != "running":
                        break
                    _t.sleep(0.05)
                assert data["status"] == "completed"
                assert data.get("artifact_id") == "art-2"
                # The persistence helper saw parent_id="art-1" verbatim.
                kwargs = mock_persist.call_args.kwargs
                assert kwargs["parent_id"] == "art-1"
                assert kwargs["feedback"] == "use BROADCAST"
        finally:
            analysis_store.pop(aid, None)


class TestRewriteHistoryEndpoint:
    """GET /api/v1/rewrite/history — Phase 3 list endpoint."""

    def test_requires_filter(self, app):
        with app.test_client() as c:
            r = c.get("/api/v1/rewrite/history")
            assert r.status_code == 400

    def test_persistence_disabled_returns_empty(self, app, monkeypatch):
        """When no warehouse is configured, the endpoint returns an
        empty list rather than 500. Disabled state is signalled in the
        ``persistence`` field for the UI."""
        # Force config.http_path to be empty by monkeypatching
        # TableWriterConfig.from_env so the route's "no warehouse"
        # short-circuit fires.
        from services import table_writer as tw

        class _Cfg:
            enabled = True
            http_path = ""
        monkeypatch.setattr(tw.TableWriterConfig, "from_env", classmethod(lambda cls: _Cfg()))
        with app.test_client() as c:
            r = c.get("/api/v1/rewrite/history?analysis_id=x")
            assert r.status_code == 200
            data = r.get_json()
            assert data["items"] == []
            assert data["persistence"] == "disabled"

    def test_owner_only_filter_passed_in_non_admin_mode(self, app, monkeypatch):
        """Codex Q4 (v6.7.6): when the admin allowlist is set and the
        caller is not on it, the reader is called with their email so
        only their rows come back."""
        from services import table_reader as tr
        from services import table_writer as tw

        class _Cfg:
            enabled = True
            http_path = "/sql/1.0/warehouses/test"
            databricks_host = "https://test.cloud"
            databricks_token = "tok"
            catalog = "main"
            schema = "profiler"
        captured: dict = {}

        def fake_list(self, **kwargs):
            captured.update(kwargs)
            return [{
                "artifact_id": "art-1",
                "analysis_id": kwargs.get("analysis_id"),
                "user_email": "alice@example.com",
                "rewritten_sql": "SELECT 1",
                "model": "m",
                "created_at": None,
            }]
        monkeypatch.setenv("REWRITE_HISTORY_ADMIN_EMAILS", "admin@example.com")
        monkeypatch.setattr(tw.TableWriterConfig, "from_env", classmethod(lambda cls: _Cfg()))
        monkeypatch.setattr(tr.TableReader, "list_rewrite_artifacts", fake_list)
        with app.test_client() as c:
            r = c.get(
                "/api/v1/rewrite/history?analysis_id=x",
                headers={"X-Forwarded-Email": "alice@example.com"},
            )
            assert r.status_code == 200
            data = r.get_json()
            assert captured.get("user_email") == "alice@example.com"
            assert data["filter"]["owner_only"] is True

    def test_admin_caller_sees_all_rows(self, app, monkeypatch):
        """Admin email → reader called with user_email=None so the
        owner filter is dropped."""
        from services import table_reader as tr
        from services import table_writer as tw

        class _Cfg:
            enabled = True
            http_path = "/sql/1.0/warehouses/test"
            databricks_host = "https://test.cloud"
            databricks_token = "tok"
            catalog = "main"
            schema = "profiler"
        captured: dict = {}

        def fake_list(self, **kwargs):
            captured.update(kwargs)
            return []
        monkeypatch.setenv("REWRITE_HISTORY_ADMIN_EMAILS", "admin@example.com")
        monkeypatch.setattr(tw.TableWriterConfig, "from_env", classmethod(lambda cls: _Cfg()))
        monkeypatch.setattr(tr.TableReader, "list_rewrite_artifacts", fake_list)
        with app.test_client() as c:
            r = c.get(
                "/api/v1/rewrite/history?analysis_id=x",
                headers={"X-Forwarded-Email": "admin@example.com"},
            )
            assert r.status_code == 200
            assert captured.get("user_email") is None
            assert r.get_json()["filter"]["owner_only"] is False

    def test_no_identity_in_prod_returns_empty(self, app, monkeypatch):
        """When admin allowlist is set but the caller has no
        forwarded email, the endpoint refuses to leak rows."""
        from services import table_writer as tw

        class _Cfg:
            enabled = True
            http_path = "/sql/1.0/warehouses/test"
            databricks_host = "https://test.cloud"
            databricks_token = "tok"
            catalog = "main"
            schema = "profiler"
        monkeypatch.setenv("REWRITE_HISTORY_ADMIN_EMAILS", "admin@example.com")
        monkeypatch.setattr(tw.TableWriterConfig, "from_env", classmethod(lambda cls: _Cfg()))
        with app.test_client() as c:
            r = c.get("/api/v1/rewrite/history?analysis_id=x")
            assert r.status_code == 200
            data = r.get_json()
            assert data["items"] == []
            assert data["persistence"] == "no_identity"
