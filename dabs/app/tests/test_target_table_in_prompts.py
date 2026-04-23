"""TargetTableInfo must be surfaced to every LLM stage prompt.

Without this, Stage 2/3 will reject Stage 1's "clustering overhead"
recommendation as "can't confirm from Fact Pack" — same trap we already
fixed for EXPLAIN v2 insights.
"""

import pytest
from core.llm_prompts.prompts import (
    create_refine_prompt,
    create_review_prompt,
    create_structured_analysis_prompt,
)
from core.models import ProfileAnalysis, QueryMetrics, TargetTableInfo


@pytest.fixture(autouse=True)
def _restore_locale():
    from core.i18n import get_language, set_language

    prev = get_language()
    yield
    set_language(prev)


def _analysis_with_lc_target() -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics(query_text="INSERT INTO t ...", total_time_ms=900_000)
    a.target_table_info = TargetTableInfo(
        catalog="ck_db_ws",
        database="default",
        table="mycloudcur_incremental_2xl_sf",
        provider="delta",
        clustering_columns=[
            ["MYCLOUD_STARTMONTH"],
            ["MYCLOUD_STARTYEAR"],
            ["LINEITEM_USAGEACCOUNTID"],
        ],
        hierarchical_clustering_columns=["mycloud_startmonth", "mycloud_startyear"],
        properties={
            "delta.checkpointPolicy": "v2",
            "delta.parquet.compression.codec": "zstd",
        },
    )
    return a


class TestTargetTableReachesAllStages:
    def _stage_prompts(self, a):
        return {
            "stage1": create_structured_analysis_prompt(a, lang="en"),
            "stage2": create_review_prompt(a, "prior analysis", "primary-model", "en"),
            "stage3": create_refine_prompt(
                "initial", "review", "primary", "review", "en", analysis=a
            ),
        }

    def test_target_table_name_visible(self):
        prompts = self._stage_prompts(_analysis_with_lc_target())
        for name, prompt in prompts.items():
            assert "mycloudcur_incremental_2xl_sf" in prompt, (
                f"Target table name missing from {name}"
            )

    def test_provider_and_is_delta_visible(self):
        prompts = self._stage_prompts(_analysis_with_lc_target())
        for name, prompt in prompts.items():
            # Either "delta" or "Delta" as provider/format signal
            assert "delta" in prompt.lower(), f"Delta provider/format missing from {name}"

    def test_clustering_keys_visible(self):
        prompts = self._stage_prompts(_analysis_with_lc_target())
        for _name, prompt in prompts.items():
            assert "MYCLOUD_STARTMONTH" in prompt
            assert "LINEITEM_USAGEACCOUNTID" in prompt

    def test_hierarchical_clustering_visible(self):
        prompts = self._stage_prompts(_analysis_with_lc_target())
        for _name, prompt in prompts.items():
            lower = prompt.lower()
            assert "hierarchical" in lower or "階層" in prompt
            assert "mycloud_startmonth" in lower

    def test_section_omitted_when_no_target(self):
        """Plain SELECT queries have no target_table_info — the
        Target Table Configuration section should NOT render empty."""
        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(query_text="SELECT 1")
        a.target_table_info = None
        prompts = self._stage_prompts(a)
        for _name, prompt in prompts.items():
            assert "Target Table Configuration" not in prompt
            assert "ターゲットテーブル設定" not in prompt

    def test_section_omitted_for_select_statement_even_if_target_present(self):
        """If statement_type is SELECT we must NOT emit the target section
        even when target_table_info happens to be populated (e.g., from a
        false-positive extraction). This keeps SELECT-only prompts lean."""
        from core.models import QueryStructure, SQLAnalysis

        a = _analysis_with_lc_target()
        a.query_metrics = QueryMetrics(query_text="SELECT * FROM t")
        a.sql_analysis = SQLAnalysis(structure=QueryStructure(statement_type="SELECT"))
        prompts = self._stage_prompts(a)
        for _name, prompt in prompts.items():
            assert "Target Table Configuration" not in prompt
            assert "ターゲットテーブル設定" not in prompt
