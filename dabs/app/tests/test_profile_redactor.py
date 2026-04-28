"""Tests for L5 profile redactor (Codex (c)(f)(g) recommendations)."""

from __future__ import annotations

from services.profile_redactor import (
    RedactStats,
    redact_profile,
    redact_sql_literals,
)


# ---- redact_sql_literals: golden inputs ----


def test_strips_string_literal():
    out, ok, _ = redact_sql_literals("SELECT * FROM t WHERE col = 'secret_customer'")
    assert ok is True
    assert "secret_customer" not in out
    assert "?" in out


def test_strips_numeric_literal():
    out, ok, _ = redact_sql_literals("SELECT * FROM t LIMIT 99999")
    assert ok is True
    assert "99999" not in out


def test_strips_in_clause_literals():
    out, ok, _ = redact_sql_literals("SELECT * FROM t WHERE id IN (1, 2, 3, 4, 5)")
    assert ok is True
    for n in ("1", "2", "3", "4", "5"):
        # The integers should be replaced — check none of them appear as
        # a standalone integer literal. Other digits (column names) are
        # fine; we just ensure no literal cluster remains.
        assert f" {n})" not in out
        assert f" {n}," not in out


def test_strips_line_comment():
    sql = "SELECT 1 -- 顧客名: ABC"
    out, ok, ccount = redact_sql_literals(sql)
    assert ok is True
    assert "ABC" not in out
    assert ccount == 1


def test_strips_block_comment():
    sql = "SELECT 1 /* customer = ABC corp */ FROM t"
    out, ok, ccount = redact_sql_literals(sql)
    assert ok is True
    assert "ABC" not in out
    assert ccount == 1


def test_unparseable_sql_returns_marker_not_original():
    """Codex (c) hard requirement — never leak original on parser fail."""
    sql = "this is not valid SQL ★ お客様 secret_value_42 hello"
    out, ok, _ = redact_sql_literals(sql)
    assert ok is False
    assert out == "<UNPARSEABLE_SQL>"
    assert "secret_value_42" not in out
    assert "お客様" not in out


def test_table_and_column_names_preserved():
    sql = "SELECT ss_customer_sk, ss_quantity FROM store_sales WHERE ss_quantity > 100"
    out, ok, _ = redact_sql_literals(sql)
    assert ok is True
    assert "ss_customer_sk" in out
    assert "store_sales" in out
    assert "100" not in out  # literal stripped


# ---- Property test: secret token must not survive ----


def test_secret_token_not_in_output_for_any_case():
    """Codex (f) property test: a known secret literal must never appear
    in redacted output, no matter where it sits in the SQL."""
    SECRET = "SUPER_SECRET_LITERAL_42"
    cases = [
        f"SELECT * FROM t WHERE col = '{SECRET}'",
        f"SELECT * FROM t WHERE col IN ('{SECRET}')",
        f"SELECT '{SECRET}' AS s",
        f"-- {SECRET}\nSELECT 1",
        f"/* {SECRET} */ SELECT 1",
    ]
    for sql in cases:
        out, _ok, _ = redact_sql_literals(sql)
        assert SECRET not in out, f"leak: {sql!r} → {out!r}"


# ---- Deep-walk redact_profile ----


def test_redact_profile_strips_query_text_in_payload():
    payload = {
        "query": {
            "queryText": "SELECT * FROM customer WHERE name = 'Alice Smith' LIMIT 50",
        }
    }
    out, stats = redact_profile(payload)
    assert "Alice Smith" not in out["query"]["queryText"]
    assert stats.sql_redacted_count >= 1


def test_redact_profile_handles_aggregate_expressions_list():
    payload = {
        "nodes": [
            {
                "node_id": "55064",
                "aggregate_expressions": [
                    "sum((store_sales.ss_quantity * store_sales.ss_sales_price))",
                ],
            }
        ]
    }
    out, _stats = redact_profile(payload)
    expr = out["nodes"][0]["aggregate_expressions"][0]
    # Schema-level identifiers preserved
    assert "ss_quantity" in expr
    assert "ss_sales_price" in expr


def test_redact_profile_strips_file_paths():
    payload = {
        "scan": {
            "path": "s3://customer-bucket-secret-xyz/data/year=2024/",
            "files": ["s3://customer-bucket-secret-xyz/a.parquet"],
        }
    }
    out, stats = redact_profile(payload)
    assert "customer-bucket-secret-xyz" not in str(out)
    assert stats.opaque_redacted_count >= 1


def test_redact_profile_strips_error_messages():
    payload = {
        "error": "Spark exception on table customer.financial_records: row 42 invalid",
    }
    out, stats = redact_profile(payload)
    assert "financial_records" not in str(out["error"])
    assert stats.opaque_redacted_count >= 1


def test_redact_profile_strips_clustering_key_bounds():
    """Codex (g) — clustering_key_bounds expose min/max raw values."""
    payload = {
        "table_scan": {
            "clustering_key_bounds": {
                "ss_sold_date_sk": {"min": "2024-01-01", "max": "2024-12-31"},
            }
        }
    }
    out, stats = redact_profile(payload)
    blob = str(out)
    assert "2024-01-01" not in blob
    assert "2024-12-31" not in blob
    assert stats.bounds_redacted_count >= 1


def test_redact_profile_strips_session_parameters():
    payload = {
        "parameters": {
            "user_var": "alice@databricks.com",
            "session_id": "session-12345",
        }
    }
    out, stats = redact_profile(payload)
    assert "alice@databricks.com" not in str(out)
    assert stats.opaque_redacted_count >= 1


def test_redact_profile_preserves_metric_numbers():
    """Numeric metric values must NOT be redacted — they're operator stats,
    not user data. Required for golden creation."""
    payload = {
        "metrics": {
            "peak_memory_bytes": 1300000000000,
            "shuffle_bytes": 350000000000,
        }
    }
    out, _stats = redact_profile(payload)
    assert out["metrics"]["peak_memory_bytes"] == 1300000000000


def test_redact_profile_unparseable_path_recorded():
    payload = {
        "query": {"queryText": "totally invalid 🚫 SQL with secret_X"},
    }
    _out, stats = redact_profile(payload)
    assert stats.parse_failures >= 1
    assert any("queryText" in p for p in stats.unparseable_sql_paths)


def test_redact_profile_returns_independent_copy():
    """Caller must be able to discard the redacted payload without
    affecting the original."""
    payload = {"query": {"queryText": "SELECT 'foo'"}}
    out, _ = redact_profile(payload)
    assert out is not payload
    assert payload["query"]["queryText"] == "SELECT 'foo'"


def test_redact_stats_default_init():
    s = RedactStats()
    assert s.sql_redacted_count == 0
    assert s.parse_failures == 0
    assert s.unparseable_sql_paths == []


# ---- Codex (g) edge cases: comments / paths in nested locations ----


def test_redact_strips_user_field():
    payload = {"user": "alice@databricks.com"}
    out, _ = redact_profile(payload)
    assert "alice" not in str(out["user"])


def test_redact_strips_query_tags():
    payload = {"queryTags": ["customer:ACME", "env:prod-east-1"]}
    out, _ = redact_profile(payload)
    assert "ACME" not in str(out)
