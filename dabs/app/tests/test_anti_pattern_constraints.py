"""Anti-pattern constraints for LLM recommendations.

Verifies that the LLM prompt + knowledge base do not recommend
materialization techniques that either don't work (TEMP VIEW does not
materialize), are unavailable in the target environment (CACHE TABLE
on Serverless), or impose write-cost overhead (CTAS as a default fix).

Background: real production reports showed the LLM recommending
"convert CTE to TEMP VIEW" as a fix for multi-reference re-computation.
A Temp View is a catalog alias, not a materialization, so the underlying
query re-executes on every reference — same as a CTE. This regression
guard locks in the correction.
"""

from pathlib import Path

import pytest
from core.llm_prompts.prompts import _constraints_block, _serverless_constraints_block

KNOWLEDGE_DIR = Path(__file__).resolve().parent.parent / "core" / "knowledge"


class TestKnowledgeBaseNoMisleadingAdvice:
    """The misleading 'use Temp View for multi-reference CTE' section must
    be removed from both JA and EN knowledge files."""

    def test_ja_patterns_file_has_no_multi_reference_tempview_claim(self):
        content = (KNOWLEDGE_DIR / "dbsql_sql_patterns.md").read_text(encoding="utf-8")
        # This wording explicitly claimed Temp View is a materialization
        # fix for multi-reference CTEs — it is not.
        assert "複数回参照する場合はTemp View" not in content, (
            "JA knowledge must not claim Temp View resolves multi-reference re-computation"
        )
        assert "Spark最適化に依存" not in content, (
            "JA knowledge's vague 'depends on Spark optimization' for Temp View "
            "materialization is misleading — must be removed"
        )

    def test_en_patterns_file_has_no_multi_reference_tempview_claim(self):
        content = (KNOWLEDGE_DIR / "dbsql_sql_patterns_en.md").read_text(encoding="utf-8")
        assert "Use Temp Views for multi-reference scenarios" not in content, (
            "EN knowledge must not claim Temp View resolves multi-reference re-computation"
        )
        assert "Spark-optimized" not in content, (
            "EN knowledge's vague 'Spark-optimized' materialization claim for "
            "Temp View is misleading — must be removed"
        )


class TestConstraintsBlockAntiPatterns:
    """_constraints_block applies to every stage prompt (analyze/review/refine)
    regardless of warehouse type. It must explicitly forbid the three
    misleading materialization patterns."""

    @pytest.mark.parametrize("lang", ["ja", "en"])
    def test_tempview_is_not_materialization(self, lang):
        block = _constraints_block(lang)
        lower = block.lower()
        # The block must mention that Temp View does not materialize
        assert "temp view" in lower or "temporary view" in lower, (
            f"{lang} constraints block must mention TEMP VIEW limitation"
        )
        # Must associate it with "no materialization guarantee" (or the JA
        # equivalent). Avoid absolute claims — actual reuse depends on
        # optimizer + AQE (ReusedExchange). The wording should convey that
        # Temp View is a catalog alias, not a materialization primitive.
        if lang == "ja":
            assert "実体化" in block and ("保証しません" in block or "エイリアス" in block), (
                "JA block must state Temp View does NOT guarantee materialization"
            )
        else:
            assert "materializ" in lower and (
                "not guarantee" in lower
                or "does not materialize" in lower
                or "not materialize" in lower
                or "catalog alias" in lower
            ), "EN block must state Temp View does NOT guarantee materialization"

    @pytest.mark.parametrize("lang", ["ja", "en"])
    def test_reusedexchange_is_referenced_as_decision_criterion(self, lang):
        """The real reuse/recomputation determinant is ReusedExchange in
        the physical plan — not the CTE-vs-TempView choice. Block must say
        so, so the LLM does not assert blanket recomputation."""
        block = _constraints_block(lang)
        assert "ReusedExchange" in block, (
            f"{lang} constraints block must cite ReusedExchange as the true "
            "reuse signal (prevents blanket 'CTE always recomputes' claims)"
        )

    @pytest.mark.parametrize("lang", ["ja", "en"])
    def test_ctas_has_write_cost_caveat(self, lang):
        block = _constraints_block(lang)
        lower = block.lower()
        # CTAS / CREATE TABLE AS SELECT — must have a cost caveat
        assert (
            ("ctas" in lower)
            or ("create table as select" in lower)
            or ("create or replace table" in lower)
        ), f"{lang} constraints must address CTAS pattern"

    @pytest.mark.parametrize("lang", ["ja", "en"])
    def test_cte_multi_ref_prefers_rewrite(self, lang):
        """CTE multi-reference solution must favour query rewrite over
        physical materialization."""
        block = _constraints_block(lang)
        if lang == "ja":
            assert "書き換え" in block or "クエリ書き換え" in block
        else:
            assert "rewrite" in block.lower()


class TestServerlessConstraintsBlockCacheTableForbidden:
    """Serverless SQL does not support CACHE TABLE — the prompt must say
    so explicitly so the LLM never recommends it."""

    @pytest.mark.parametrize("lang", ["ja", "en"])
    def test_cache_table_unavailable_noted(self, lang):
        block = _serverless_constraints_block(lang)
        lower = block.lower()
        assert "cache table" in lower, (
            f"{lang} serverless block must mention CACHE TABLE availability"
        )
        # Must explicitly state it is NOT available / forbidden
        if lang == "ja":
            assert "使用不可" in block or "利用不可" in block or "推奨してはならない" in block, (
                "JA serverless block must forbid CACHE TABLE"
            )
        else:
            assert (
                "not available" in lower
                or "unavailable" in lower
                or "do not recommend" in lower
                or "do not use" in lower
            ), "EN serverless block must forbid CACHE TABLE"


class TestExplainV2InsightsNoTempViewRecommendation:
    """`_format_explain_v2_insights()` is the explain-derived hint block
    that feeds directly into the analysis prompt. It must NOT suggest
    'materializing into a temp view' as a fix for multi-reference CTE
    without ReusedExchange — that contradicts the knowledge base and
    misleads users.
    """

    def _cte_no_reuse_analysis(self):
        """Build a minimal ExplainAnalysis payload that triggers the
        'CTE not reused' branch in _format_explain_v2_insights()."""
        from types import SimpleNamespace

        cte = SimpleNamespace(cte_id="1", reference_count=3)
        return SimpleNamespace(
            cte_references=[cte],
            has_reused_exchange=False,
            photon_fallback_ops=[],
            implicit_cast_sites=[],
            aggregate_phases=[],
            exchanges=[],
            is_adaptive=False,
            is_final_plan=False,
            photon_explanation=None,
            filter_pushdown=[],
            join_strategies=[],
            optimizer_statistics=None,
        )

    def test_cte_hint_does_not_recommend_temp_view_as_fix(self):
        from core.llm_prompts.prompts import _format_explain_v2_insights

        ea = self._cte_no_reuse_analysis()
        lines = _format_explain_v2_insights(ea)
        text = "\n".join(lines).lower()

        # Must mention CTE re-computation
        assert "cte" in text and "re-comput" in text.replace("recomput", "re-comput")

        # Must NOT recommend Temp View as the primary materialization fix
        assert "materializing into a temp view" not in text, (
            "_format_explain_v2_insights must not recommend Temp View as a "
            "materialization fix — Temp View is a catalog alias, not a "
            "materialization primitive (see constraints block / knowledge)"
        )

    def test_cte_hint_prefers_query_rewrite(self):
        from core.llm_prompts.prompts import _format_explain_v2_insights

        ea = self._cte_no_reuse_analysis()
        lines = _format_explain_v2_insights(ea)
        text = "\n".join(lines).lower()

        # Should steer toward query-level rewrite (per constraints block)
        assert "restructur" in text or "rewrite" in text, (
            "Multi-reference CTE hint should steer toward query rewrite "
            "(GROUP BY consolidation / window functions / UNION ALL) before "
            "any physical materialization"
        )
