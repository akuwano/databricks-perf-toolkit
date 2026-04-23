"""Surface clustering keys + column types in LLM prompts.

Regression guard for: LLM guessing clustering state from table name
suffix (e.g. "_lc") or asking the user to check schema manually when
the data is already available from JSON + EXPLAIN.
"""

from core.explain_parser import ExplainExtended, RelationInfo
from core.llm_prompts.prompts import (
    create_rewrite_user_prompt,
    create_structured_analysis_prompt,
)
from core.models import ProfileAnalysis, QueryMetrics, TableScanMetrics


def _analysis_with_clustering(table: str, keys: list[str], card: dict) -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics(query_text="SELECT 1")
    a.top_scanned_tables = [
        TableScanMetrics(
            table_name=table,
            bytes_read=100 * 1024 * 1024,
            rows_scanned=1_000_000,
            current_clustering_keys=keys,
            clustering_key_cardinality=card,
        )
    ]
    return a


class TestMainPromptSurfacesClusteringKeys:
    def test_clustering_keys_visible_in_main_prompt(self):
        a = _analysis_with_clustering(
            "main.base.store_sales_delta_lc",
            ["ss_sold_date_sk"],
            {"ss_sold_date_sk": "high"},
        )
        prompt = create_structured_analysis_prompt(a, lang="en")
        assert "store_sales_delta_lc" in prompt
        assert "ss_sold_date_sk" in prompt
        # The main prompt must clearly indicate the table HAS clustering keys
        # so the LLM doesn't guess from the "_lc" suffix.
        assert "clustering" in prompt.lower()

    def test_no_clustering_keys_marked_explicit(self):
        a = _analysis_with_clustering(
            "main.base.plain_table",
            [],
            {},
        )
        prompt = create_structured_analysis_prompt(a, lang="en")
        assert "plain_table" in prompt
        # Should state "no clustering keys" rather than leaving it ambiguous
        assert "no clustering" in prompt.lower() or "none" in prompt.lower()


class TestMainPromptSurfacesColumnTypes:
    def test_join_key_types_visible_when_explain_attached(self):
        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(query_text="SELECT 1")
        ex = ExplainExtended()
        ex.scan_schemas = {
            "main.base.store_sales": {"ss_customer_sk": "decimal(38,0)", "ss_quantity": "int"},
            "main.base.customer": {"c_customer_sk": "bigint"},
        }
        ex.relations = [
            RelationInfo(table_name="main.base.store_sales", columns=["ss_customer_sk"]),
            RelationInfo(table_name="main.base.customer", columns=["c_customer_sk"]),
        ]
        a.explain_analysis = ex
        prompt = create_structured_analysis_prompt(a, lang="en")
        # Both the decimal side and the bigint side must be surfaced so
        # the LLM can diagnose a type-mismatch without guessing.
        assert "decimal(38,0)" in prompt
        assert "bigint" in prompt

    def test_no_types_no_crash(self):
        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(query_text="SELECT 1")
        a.explain_analysis = ExplainExtended()  # empty
        prompt = create_structured_analysis_prompt(a, lang="en")
        assert "Fact Pack" in prompt or "SQL Query" in prompt


class TestRewritePromptSurfacesClusteringAndTypes:
    def test_rewrite_table_scan_shows_clustering_keys(self):
        a = _analysis_with_clustering(
            "main.base.store_sales_delta_lc",
            ["ss_sold_date_sk"],
            {"ss_sold_date_sk": "high"},
        )
        prompt = create_rewrite_user_prompt(a, lang="en")
        assert "store_sales_delta_lc" in prompt
        assert "ss_sold_date_sk" in prompt

    def test_rewrite_prompt_shows_decimal_on_join_key_type(self):
        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(query_text="SELECT * FROM ss JOIN c")
        ex = ExplainExtended()
        ex.scan_schemas = {
            "main.base.store_sales": {"ss_customer_sk": "decimal(38,0)"},
        }
        a.explain_analysis = ex
        prompt = create_rewrite_user_prompt(a, lang="en")
        assert "decimal(38,0)" in prompt
        assert "ss_customer_sk" in prompt
