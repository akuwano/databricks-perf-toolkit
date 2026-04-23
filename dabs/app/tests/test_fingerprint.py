"""Tests for core.fingerprint module."""

from core.fingerprint import FINGERPRINT_VERSION, generate_fingerprint, normalize_sql


class TestNormalizeSql:
    def test_empty_string(self):
        assert normalize_sql("") == ""

    def test_whitespace_only(self):
        assert normalize_sql("   ") == ""

    def test_collapse_whitespace(self):
        result = normalize_sql("SELECT   a,  b   FROM   t")
        assert "  " not in result

    def test_lowercase(self):
        result = normalize_sql("SELECT A FROM T WHERE B = 1")
        assert result == result.lower()

    def test_replace_numeric_literals(self):
        result = normalize_sql("SELECT * FROM t WHERE id = 42 AND price > 3.14")
        assert "42" not in result
        assert "3.14" not in result
        assert "?" in result

    def test_replace_string_literals(self):
        result = normalize_sql("SELECT * FROM t WHERE name = 'hello'")
        assert "'hello'" not in result
        assert "?" in result

    def test_remove_single_line_comments(self):
        result = normalize_sql("SELECT 1 -- this is a comment\nFROM t")
        assert "comment" not in result

    def test_remove_block_comments(self):
        result = normalize_sql("SELECT /* inline */ 1 FROM t")
        assert "inline" not in result

    def test_semantically_equivalent_queries_same_fingerprint(self):
        sql1 = "SELECT * FROM users WHERE id = 1"
        sql2 = "SELECT * FROM users WHERE id = 99"
        assert normalize_sql(sql1) == normalize_sql(sql2)

    def test_different_queries_different_fingerprint(self):
        sql1 = "SELECT * FROM users"
        sql2 = "SELECT * FROM orders"
        assert normalize_sql(sql1) != normalize_sql(sql2)


class TestGenerateFingerprint:
    def test_empty_string(self):
        assert generate_fingerprint("") == ""

    def test_returns_hex_hash(self):
        result = generate_fingerprint("SELECT 1")
        assert len(result) == 64  # SHA-256 hex
        assert all(c in "0123456789abcdef" for c in result)

    def test_same_query_same_fingerprint(self):
        fp1 = generate_fingerprint("SELECT * FROM t WHERE x = 1")
        fp2 = generate_fingerprint("SELECT * FROM t WHERE x = 999")
        assert fp1 == fp2

    def test_different_query_different_fingerprint(self):
        fp1 = generate_fingerprint("SELECT * FROM t1")
        fp2 = generate_fingerprint("SELECT * FROM t2")
        assert fp1 != fp2

    def test_whitespace_insensitive(self):
        fp1 = generate_fingerprint("SELECT  *  FROM  t")
        fp2 = generate_fingerprint("SELECT * FROM t")
        assert fp1 == fp2

    def test_case_insensitive(self):
        fp1 = generate_fingerprint("SELECT * FROM T")
        fp2 = generate_fingerprint("select * from t")
        assert fp1 == fp2

    def test_fingerprint_version(self):
        assert FINGERPRINT_VERSION == "v1"
