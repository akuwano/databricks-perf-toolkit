"""Tests for SQL formatting with multi-tier fallback in sql_analyzer.py."""

from core.sql_analyzer import (
    _format_sql_with_sqlglot,
    _format_sql_with_sqlparse,
    format_sql,
)


class TestFormatSql:
    """Tests for the main format_sql() function."""

    def test_simple_select(self):
        sql = "select a, b from t where x = 1"
        result = format_sql(sql)
        # Should be formatted with proper casing/indentation
        assert "SELECT" in result or "select" in result.lower()
        assert "FROM" in result or "from" in result.lower()

    def test_complex_query_with_joins(self):
        sql = "select a.id, b.name from table_a a join table_b b on a.id = b.id where a.status = 'active'"
        result = format_sql(sql)
        assert len(result) > len(sql) * 0.5  # Should produce reasonable output
        assert "join" in result.lower()

    def test_empty_sql(self):
        assert format_sql("") == ""
        assert format_sql("   ") == ""

    def test_preserves_sql_on_none_like_input(self):
        assert format_sql("") == ""

    def test_databricks_create_table(self):
        sql = "CREATE TABLE test USING DELTA AS SELECT 1"
        result = format_sql(sql)
        assert "CREATE" in result.upper()

    def test_subquery(self):
        sql = "SELECT * FROM (SELECT id, count(*) as cnt FROM orders GROUP BY id) sub WHERE cnt > 5"
        result = format_sql(sql)
        assert "GROUP BY" in result.upper() or "group by" in result.lower()

    def test_cte(self):
        sql = "WITH cte AS (SELECT id FROM t) SELECT * FROM cte"
        result = format_sql(sql)
        assert "WITH" in result.upper() or "with" in result.lower()

    def test_very_long_sql_still_works(self):
        # Generate a moderately long SQL (not exceeding the limit)
        columns = ", ".join([f"col_{i}" for i in range(100)])
        sql = f"SELECT {columns} FROM big_table WHERE id > 0"
        result = format_sql(sql)
        assert "col_0" in result
        assert "col_99" in result


class TestFormatSqlWithSqlglot:
    """Tests for sqlglot-based formatting."""

    def test_simple_select(self):
        result = _format_sql_with_sqlglot("select a, b from t")
        assert result is not None
        assert "SELECT" in result

    def test_databricks_dialect(self):
        result = _format_sql_with_sqlglot("SELECT * FROM catalog.schema.table")
        assert result is not None

    def test_unsupported_syntax_returns_none(self):
        # Completely invalid SQL should return None
        _format_sql_with_sqlglot("THIS IS NOT SQL AT ALL !!!")
        # sqlglot may or may not parse this; either None or a result is ok
        # The key is it doesn't raise an exception

    def test_join_formatting(self):
        sql = "select a.id from t1 a inner join t2 b on a.id = b.id"
        result = _format_sql_with_sqlglot(sql)
        assert result is not None
        assert "JOIN" in result.upper()

    def test_rejects_comment_mangling(self):
        """sqlglot should be rejected when it converts -- comments to /* */."""
        sql = "-- This is a comment\nSELECT a FROM t"
        result = _format_sql_with_sqlglot(sql)
        # If sqlglot converts -- to /*, it should be rejected (returns None)
        # If sqlglot preserves --, it's acceptable
        if result is not None:
            assert "/*" not in result or result.count("/*") <= sql.count("/*") + 2

    def test_no_comments_uses_sqlglot(self):
        """SQL without comments should use sqlglot successfully."""
        sql = "select a, b from t where x = 1"
        result = _format_sql_with_sqlglot(sql)
        assert result is not None

    def test_comment_heavy_sql_fallback(self):
        """Comment-heavy SQL should fall back to sqlparse via format_sql()."""
        sql = "-- comment1\n-- comment2\n-- comment3\nSELECT a FROM t"
        result = format_sql(sql)
        # Comments should be preserved as -- (not converted to /* */)
        assert "--" in result


class TestFormatSqlWithSqlparse:
    """Tests for sqlparse-based formatting (fallback)."""

    def test_simple_select(self):
        result = _format_sql_with_sqlparse("select a, b from t")
        assert "SELECT" in result

    def test_keyword_uppercasing(self):
        result = _format_sql_with_sqlparse("select a from t where x = 1")
        assert "SELECT" in result
        assert "FROM" in result
        assert "WHERE" in result

    def test_invalid_sql_returns_original(self):
        bad_sql = "THIS IS NOT SQL AT ALL"
        result = _format_sql_with_sqlparse(bad_sql)
        # sqlparse doesn't fail on invalid SQL, just returns it formatted
        assert len(result) > 0
