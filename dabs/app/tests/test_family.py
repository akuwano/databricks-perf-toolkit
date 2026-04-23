"""Tests for core.family module."""

from core.family import (
    PURPOSE_SIGNATURE_VERSION,
    detect_variant_type,
    extract_purpose_features,
    generate_purpose_signature,
)


class TestExtractPurposeFeatures:
    def test_empty_sql(self):
        assert extract_purpose_features("") == {}

    def test_simple_select(self):
        features = extract_purpose_features("SELECT a, b FROM users WHERE id = 1")
        assert "users" in features["tables"]
        assert "id" in features["filter_columns"]

    def test_tables_sorted(self):
        features = extract_purpose_features(
            "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        )
        assert features["tables"] == sorted(features["tables"])

    def test_hints_ignored(self):
        sql_no_hint = "SELECT * FROM orders o JOIN customers c ON o.cid = c.id"
        sql_hint = "SELECT /*+ BROADCAST(c) */ * FROM orders o JOIN customers c ON o.cid = c.id"
        f1 = extract_purpose_features(sql_no_hint)
        f2 = extract_purpose_features(sql_hint)
        # Tables and join keys should be the same
        assert f1["tables"] == f2["tables"]

    def test_aggregates_detected(self):
        features = extract_purpose_features(
            "SELECT customer_id, SUM(amount), COUNT(*) FROM orders GROUP BY customer_id"
        )
        assert "SUM" in features.get("aggregates", []) or "sum" in features.get("aggregates", [])

    def test_group_by_detected(self):
        features = extract_purpose_features("SELECT region, COUNT(*) FROM sales GROUP BY region")
        assert "region" in features.get("group_by", [])

    def test_join_keys_detected(self):
        features = extract_purpose_features("SELECT * FROM a JOIN b ON a.id = b.a_id")
        assert len(features.get("join_keys", [])) > 0


class TestGeneratePurposeSignature:
    def test_empty_sql(self):
        assert generate_purpose_signature("") == ""

    def test_returns_hex_hash(self):
        sig = generate_purpose_signature("SELECT * FROM users")
        assert len(sig) == 64
        assert all(c in "0123456789abcdef" for c in sig)

    def test_same_purpose_same_signature(self):
        sql1 = "SELECT * FROM users WHERE id = 1"
        sql2 = "SELECT * FROM users WHERE id = 999"
        assert generate_purpose_signature(sql1) == generate_purpose_signature(sql2)

    def test_hint_does_not_change_signature(self):
        sql1 = "SELECT * FROM orders o JOIN customers c ON o.cid = c.id"
        sql2 = "SELECT /*+ BROADCAST(c) */ * FROM orders o JOIN customers c ON o.cid = c.id"
        assert generate_purpose_signature(sql1) == generate_purpose_signature(sql2)

    def test_different_tables_different_signature(self):
        sig1 = generate_purpose_signature("SELECT * FROM users")
        sig2 = generate_purpose_signature("SELECT * FROM orders")
        assert sig1 != sig2

    def test_alias_change_same_signature(self):
        sql1 = "SELECT * FROM users u WHERE u.id = 1"
        sql2 = "SELECT * FROM users t WHERE t.id = 1"
        assert generate_purpose_signature(sql1) == generate_purpose_signature(sql2)


class TestDetectVariantType:
    def test_same_sql(self):
        sql = "SELECT * FROM t WHERE id = 1"
        assert detect_variant_type(sql, sql) == "same_sql"

    def test_same_sql_diff_warehouse(self):
        sql = "SELECT * FROM t WHERE id = 1"
        assert detect_variant_type(sql, sql, "Small", "Large") == "same_sql_diff_warehouse"

    def test_diff_hint(self):
        sql1 = "SELECT * FROM a JOIN b ON a.id = b.aid"
        sql2 = "SELECT /*+ BROADCAST(b) */ * FROM a JOIN b ON a.id = b.aid"
        result = detect_variant_type(sql1, sql2)
        assert result == "diff_hint"

    def test_empty_sql(self):
        assert detect_variant_type("", "SELECT 1") == "unknown"

    def test_version(self):
        assert PURPOSE_SIGNATURE_VERSION == "v1"
