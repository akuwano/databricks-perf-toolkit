"""Tests for shared SQL pattern detectors (CTE multi-references, non-sargable filters)."""

from core.models import NodeMetrics
from core.sql_analyzer import _strip_comments_lightweight, remove_comments
from core.sql_patterns import (
    analyze_cte_multi_references,
    collect_non_sargable_filter_functions,
)


def _make_large_sql_with_comments(target_length: int = 86_000) -> str:
    """Synthesize a large SQL string with comments, without committing a fixture file."""
    header = (
        "WITH base AS (\n"
        "    SELECT 1 AS id, '-- not comment in string' AS note\n"
        "    /* block comment to strip */\n"
        ")\n"
        "SELECT * FROM base\n"
    )
    filler = "UNION ALL SELECT 1 AS id, 'value -- keep' AS note -- strip me\n"
    sql = header
    while len(sql) < target_length:
        sql += filler
    return sql


class TestAnalyzeCteMultiReferences:
    def test_empty_query_returns_empty(self):
        assert analyze_cte_multi_references("") == []
        assert analyze_cte_multi_references("   ") == []

    def test_no_cte_returns_empty(self):
        assert analyze_cte_multi_references("SELECT * FROM t") == []

    def test_cte_referenced_once_not_flagged(self):
        # total=2 (WITH name AS ... + 1 ref) → refs=1 → skip
        sql = "WITH a AS (SELECT 1) SELECT * FROM a"
        assert analyze_cte_multi_references(sql) == []

    def test_cte_referenced_twice_flagged(self):
        # WITH a, FROM a, JOIN a b, a.id → total=4 → refs=3 → emit
        sql = "WITH a AS (SELECT id FROM t) SELECT * FROM a JOIN a b ON a.id=b.id"
        result = analyze_cte_multi_references(sql)
        assert len(result) == 1
        assert result[0][0] == "a"
        assert result[0][1] == 4  # total occurrences including a.id

    def test_multiple_ctes(self):
        sql = """WITH
            a AS (SELECT 1),
            b AS (SELECT 2)
        SELECT * FROM a JOIN a x ON 1=1 JOIN b ON 1=1 JOIN b y ON 1=1 JOIN b z ON 1=1"""
        result = analyze_cte_multi_references(sql)
        names = {r[0] for r in result}
        assert "a" in names
        assert "b" in names

    def test_comments_stripped(self):
        sql = """-- comment mentions a a a
        WITH a AS (SELECT 1) SELECT 1 FROM a JOIN a b ON 1=1"""
        result = analyze_cte_multi_references(sql)
        # Comment occurrences of `a` should not inflate the count
        # WITH a + FROM a + JOIN a → 3
        assert len(result) == 1
        assert result[0][1] == 3

    def test_malformed_sql_returns_empty(self):
        assert analyze_cte_multi_references("not even close to SQL !@#$") == []

    def test_large_sql_does_not_crash(self):
        # Regression: sqlparse 0.5.5 raised SQLParseError on 86K-char SQL.
        # analyze_cte_multi_references() must no longer propagate that crash.
        sql = _make_large_sql_with_comments()
        # Should not raise; result may be empty because sqlglot struggles too,
        # but a crash-free return is the contract.
        result = analyze_cte_multi_references(sql)
        assert isinstance(result, list)


class TestRemoveCommentsLargeSqlGuard:
    def test_large_sql_strips_comments_via_lightweight_path(self):
        sql = _make_large_sql_with_comments()
        cleaned = remove_comments(sql)
        assert isinstance(cleaned, str)
        assert "block comment to strip" not in cleaned
        assert "-- strip me" not in cleaned
        # String-literal content preserved:
        assert "'-- not comment in string'" in cleaned
        assert "'value -- keep'" in cleaned


class TestStripCommentsLightweight:
    def test_preserves_line_comment_markers_inside_single_quotes(self):
        sql = "SELECT '-- keep', col FROM t -- remove me\nWHERE id = 1"
        result = _strip_comments_lightweight(sql)
        assert "'-- keep'" in result
        assert "-- remove me" not in result

    def test_preserves_block_comment_markers_inside_double_quotes(self):
        sql = 'SELECT "/* keep */" AS txt, col FROM t /* remove me */ WHERE id = 1'
        result = _strip_comments_lightweight(sql)
        assert '"/* keep */"' in result
        assert "remove me" not in result

    def test_handles_ansi_doubled_single_quote_escape(self):
        sql = "SELECT 'it''s -- still string' AS txt -- remove me\nFROM t"
        result = _strip_comments_lightweight(sql)
        assert "'it''s -- still string'" in result
        assert "-- remove me" not in result

    def test_empty_input_returns_empty_string(self):
        assert _strip_comments_lightweight("") == ""

    def test_strips_unterminated_trailing_line_comment(self):
        sql = "SELECT 1 -- trailing no newline"
        assert _strip_comments_lightweight(sql) == "SELECT 1"


class TestCollectNonSargableFilterFunctions:
    def test_no_nodes_returns_empty(self):
        assert collect_non_sargable_filter_functions([]) == []

    def test_no_filters_returns_empty(self):
        nm = NodeMetrics(node_name="Scan t", filter_conditions=[])
        assert collect_non_sargable_filter_functions([nm]) == []

    def test_detects_year_function(self):
        nm = NodeMetrics(node_name="Scan t", filter_conditions=["YEAR(dt) = 2024"])
        result = collect_non_sargable_filter_functions([nm])
        assert "YEAR" in result

    def test_detects_multiple_functions(self):
        nm = NodeMetrics(
            node_name="Scan t",
            filter_conditions=["UPPER(name) = 'X'", "YEAR(dt) = 2024"],
        )
        result = collect_non_sargable_filter_functions([nm])
        assert set(result) == {"UPPER", "YEAR"}
        assert result == sorted(result)  # sorted output

    def test_plain_comparison_not_detected(self):
        nm = NodeMetrics(
            node_name="Scan t",
            filter_conditions=["dt >= '2024-01-01'", "id IN (1,2,3)"],
        )
        assert collect_non_sargable_filter_functions([nm]) == []

    def test_dedupes_across_nodes(self):
        nm1 = NodeMetrics(node_name="Scan t1", filter_conditions=["YEAR(dt) = 2024"])
        nm2 = NodeMetrics(node_name="Scan t2", filter_conditions=["year(d) = 2025"])
        result = collect_non_sargable_filter_functions([nm1, nm2])
        assert result == ["YEAR"]  # case-insensitive dedupe
