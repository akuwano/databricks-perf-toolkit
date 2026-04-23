"""LLM prompt enrichment from Phase-1 EXPLAIN v2 insights.

Verifies that both the main analysis prompt and the rewrite prompt surface
the new structured signals so the LLM can ground its recommendations in
concrete EXPLAIN evidence.
"""

from core.explain_parser import (
    CteReuseInfo,
    ExplainExtended,
    FilterPushdownInfo,
    ImplicitCastSite,
    PhotonFallbackOp,
)
from core.llm_prompts.prompts import (
    create_rewrite_user_prompt,
    create_structured_analysis_prompt,
)
from core.models import ProfileAnalysis, QueryMetrics


def _analysis_with_explain(ex: ExplainExtended) -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics(query_text="SELECT 1")
    a.explain_analysis = ex
    return a


class TestAnalysisPromptEnrichment:
    def test_implicit_cast_on_join_surfaces_in_prompt(self):
        ex = ExplainExtended()
        ex.implicit_cast_sites = [
            ImplicitCastSite(
                context="join",
                column_ref="ss_item_sk#1",
                to_type="bigint",
                node_name="PhotonShuffledHashJoin",
            )
        ]
        a = _analysis_with_explain(ex)
        prompt = create_structured_analysis_prompt(a, lang="en")
        # Must mention the signal name and the concrete column/type
        assert "implicit CAST" in prompt or "Implicit CAST" in prompt
        assert "ss_item_sk" in prompt
        assert "bigint" in prompt

    def test_cte_reuse_miss_surfaces_in_prompt(self):
        ex = ExplainExtended()
        ex.cte_references = [CteReuseInfo(cte_id="16", reference_count=3)]
        ex.has_reused_exchange = False
        a = _analysis_with_explain(ex)
        prompt = create_structured_analysis_prompt(a, lang="en")
        assert "CTE" in prompt
        assert "re-comput" in prompt.lower() or "not reused" in prompt.lower()

    def test_cte_reused_no_miss_not_reported_as_miss(self):
        ex = ExplainExtended()
        ex.cte_references = [CteReuseInfo(cte_id="16", reference_count=3)]
        ex.has_reused_exchange = True
        a = _analysis_with_explain(ex)
        prompt = create_structured_analysis_prompt(a, lang="en")
        assert "not reused" not in prompt.lower()
        assert "re-comput" not in prompt.lower()

    def test_photon_fallback_surfaces_in_prompt(self):
        ex = ExplainExtended()
        ex.photon_fallback_ops = [
            PhotonFallbackOp(
                node_name="HashAggregate",
                raw_line="HashAggregate(keys=[k#1], functions=[pivotfirst(...)])",
            )
        ]
        a = _analysis_with_explain(ex)
        prompt = create_structured_analysis_prompt(a, lang="en")
        assert "Photon fallback" in prompt or "non-Photon" in prompt
        assert "HashAggregate" in prompt

    def test_pushdown_gap_surfaces_in_prompt(self):
        ex = ExplainExtended()
        ex.filter_pushdown = [
            FilterPushdownInfo(
                table_name="main.base.store_sales",
                has_data_filters=True,
                has_partition_filters=False,
                partition_filters_empty=True,
            )
        ]
        a = _analysis_with_explain(ex)
        prompt = create_structured_analysis_prompt(a, lang="en")
        assert "main.base.store_sales" in prompt
        assert "partition" in prompt.lower()

    def test_no_v2_signals_no_v2_section(self):
        ex = ExplainExtended()
        a = _analysis_with_explain(ex)
        prompt = create_structured_analysis_prompt(a, lang="en")
        # The v2 subheader should not appear when nothing matched
        assert "EXPLAIN v2 Insights" not in prompt


class TestRewritePromptEnrichment:
    def test_cast_on_join_key_ranked_critical_in_rewrite_prompt(self):
        ex = ExplainExtended()
        ex.implicit_cast_sites = [
            ImplicitCastSite(
                context="join",
                column_ref="ss_item_sk#1",
                to_type="bigint",
                node_name="PhotonShuffledHashJoin",
            )
        ]
        a = _analysis_with_explain(ex)
        # Run the main analysis prompt pipeline to populate alerts (since the
        # rewrite prompt reads alerts, we need the analyzer wiring). Instead,
        # pre-populate the analysis by calling enhance_bottleneck_with_explain.
        from core.analyzers.explain_analysis import enhance_bottleneck_with_explain

        enhance_bottleneck_with_explain(a.bottleneck_indicators, ex)
        prompt = create_rewrite_user_prompt(a, lang="en")
        # The CRITICAL alert must propagate into the rewrite prompt's alert section
        assert "CRITICAL" in prompt
        assert "ss_item_sk" in prompt

    def test_rewrite_prompt_empty_analysis_does_not_crash(self):
        a = _analysis_with_explain(ExplainExtended())
        prompt = create_rewrite_user_prompt(a, lang="en")
        assert "Original SQL" in prompt
