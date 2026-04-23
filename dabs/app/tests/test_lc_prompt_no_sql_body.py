"""LC LLM prompt must not include the raw SQL body.

Phase 3 (v5.16.23): all information needed for Liquid Clustering key
selection is already structured (Candidate Columns with operator +
context, I/O Metrics, Top Scanned Tables with cardinality, Shuffle
Details). Including the raw SQL body is redundant and ran into a
2000-char truncation limit that could lose WHERE columns. Remove it.

Genie rewrite, Stage 1/2/3 analysis prompts, Delta table persistence,
and the Web UI all continue to use ``analysis.query_metrics.query_text``
unchanged — this test only verifies the LC LLM prompt.
"""

from __future__ import annotations

from core.llm_prompts.prompts import create_clustering_prompt


class TestLcPromptOmitsSqlBody:
    def test_user_prompt_does_not_contain_sql_body(self):
        """A 5000-char SQL must NOT appear in the LC LLM user prompt."""
        long_sql = "SELECT * FROM t1 JOIN t2 ON t1.k = t2.k WHERE " + "x > 0 AND " * 500
        _system, user = create_clustering_prompt(
            target_table="t1",
            candidate_columns=[{"column": "k", "context": "join", "operator": "=", "table": "t1"}],
            top_scanned_tables=[{"table_name": "t1", "bytes_read": 100 * 1024**3}],
            filter_rate=0.0,
            read_files_count=1000,
            pruned_files_count=0,
            query_sql=long_sql,
            lang="en",
        )
        # Raw SQL body must not be in the prompt.
        # "SELECT * FROM t1" is generic enough to appear only if we
        # embedded the actual SQL — not expected to show up from labels.
        assert "x > 0 AND x > 0 AND" not in user
        # The "## Query to Analyze" heading must be gone.
        assert "Query to Analyze" not in user
        # Structured inputs are still present.
        assert "Candidate Columns" in user
        assert "Top Scanned Tables" in user
        assert "I/O Metrics" in user

    def test_ja_variant_also_omits_sql_body(self):
        _system, user = create_clustering_prompt(
            target_table="t",
            candidate_columns=[],
            top_scanned_tables=[],
            filter_rate=0.0,
            read_files_count=0,
            pruned_files_count=0,
            query_sql="SELECT * FROM some_table_name WHERE some_col = 123",
            lang="ja",
        )
        assert "some_table_name" not in user
        assert "分析対象クエリ" not in user
        assert "候補カラム" in user

    def test_query_sql_parameter_accepts_empty_or_none_without_error(self):
        """Backward compat: the parameter still exists (for callers that
        pass it) but is ignored. Empty/None must not raise."""
        _system, user = create_clustering_prompt(
            target_table="t",
            candidate_columns=[],
            top_scanned_tables=[],
            filter_rate=0.0,
            read_files_count=0,
            pruned_files_count=0,
            query_sql="",
            lang="en",
        )
        assert "Candidate Columns" in user


class TestStructuredInputsStillDominate:
    """With the SQL body removed, structured inputs must continue to
    drive the recommendation. Regression guard against accidentally
    dropping any of them too."""

    def test_candidate_columns_rendered(self):
        _system, user = create_clustering_prompt(
            target_table="t",
            candidate_columns=[
                {"column": "order_date", "context": "where", "operator": "BETWEEN", "table": "t"},
                {"column": "user_id", "context": "join", "operator": "=", "table": "t"},
            ],
            top_scanned_tables=[],
            filter_rate=0.0,
            read_files_count=0,
            pruned_files_count=0,
            query_sql="",
            lang="en",
        )
        assert "order_date" in user
        assert "user_id" in user
        assert "BETWEEN" in user

    def test_top_scanned_tables_rendered_with_clustering_info(self):
        _system, user = create_clustering_prompt(
            target_table="fact_sales",
            candidate_columns=[],
            top_scanned_tables=[
                {
                    "table_name": "fact_sales",
                    "bytes_read": 500 * 1024**3,
                    "current_clustering_keys": ["order_date", "user_sk"],
                    "clustering_key_cardinality": {"order_date": "low", "user_sk": "high"},
                }
            ],
            filter_rate=0.0,
            read_files_count=1000,
            pruned_files_count=0,
            query_sql="",
            lang="en",
        )
        assert "fact_sales" in user
        assert "order_date" in user
        assert "low-card" in user
        assert "high-card" in user
