"""Tests for ANALYZE TABLE filtering and alternative-causes card
when optimizer statistics are confirmed fresh.
"""

from core.analyzers.recommendations import generate_action_cards
from core.constants import JoinType
from core.models import (
    BottleneckIndicators,
    HashResizeHotspot,
    JoinInfo,
    NodeMetrics,
    ProfileAnalysis,
    QueryMetrics,
    QueryStructure,
    SQLAnalysis,
)
from core.usecases import _merge_llm_action_plan


def _qm() -> QueryMetrics:
    return QueryMetrics(query_id="t", status="FINISHED", total_time_ms=1000, execution_time_ms=1000)


def _agg(node_id: str, *, resize: int, group_exprs: list[str]) -> NodeMetrics:
    return NodeMetrics(
        node_id=node_id,
        node_name="Grouping Aggregate",
        node_tag="PHOTON_GROUPING_AGG_EXEC",
        grouping_expressions=group_exprs,
        extra_metrics={
            "Number of times hash table was resized": resize,
            "Avg hash probes per row": 100,
        },
    )


def _make_indicators_with_fresh_stats(resize: int = 5000) -> BottleneckIndicators:
    bi = BottleneckIndicators()
    bi.hash_table_resize_count = resize
    bi.avg_hash_probes_per_row = 100.0
    bi.statistics_confirmed_fresh = True
    bi.hash_resize_hotspots = [
        HashResizeHotspot(
            node_id="1",
            node_tag="PHOTON_GROUPING_AGG_EXEC",
            node_name="Grouping Aggregate",
            resize=resize,
            probes=100.0,
            keys=["db.s.t.k"],
            key_kind="group",
        ),
    ]
    return bi


class TestAlternativeCausesCard:
    def test_card_emitted_when_stats_fresh_and_high_resize(self):
        bi = _make_indicators_with_fresh_stats()
        cards = generate_action_cards(
            bi,
            [],
            _qm(),
            [],
            [JoinInfo(join_type=JoinType.SHUFFLE_HASH)],
            SQLAnalysis(structure=QueryStructure(join_count=1)),
            [],
        )
        alt = [c for c in cards if "up-to-date" in c.problem or "統計情報は最新" in c.problem]
        assert len(alt) == 1
        # Card explicitly instructs NOT to re-run ANALYZE TABLE
        card = alt[0]
        assert "do not" in card.fix.lower() or "NOT" in card.fix
        # fix_sql is empty — alt card has no SQL to run
        assert card.fix_sql == ""
        # Must list alternative causes
        cause = card.likely_cause
        for kw in ("row explosion", "skew", "NULL", "type mismatch", "duplicate"):
            assert kw.lower() in cause.lower()

    def test_card_not_emitted_when_stats_not_fresh(self):
        bi = BottleneckIndicators()
        bi.hash_table_resize_count = 5000
        bi.statistics_confirmed_fresh = False
        cards = generate_action_cards(
            bi,
            [],
            _qm(),
            [],
            [JoinInfo(join_type=JoinType.SHUFFLE_HASH)],
            SQLAnalysis(structure=QueryStructure(join_count=1)),
            [],
        )
        assert not [c for c in cards if "up-to-date" in c.problem]

    def test_card_not_emitted_when_resize_low(self):
        bi = BottleneckIndicators()
        bi.hash_table_resize_count = 5
        bi.statistics_confirmed_fresh = True
        cards = generate_action_cards(
            bi,
            [],
            _qm(),
            [],
            [JoinInfo(join_type=JoinType.SHUFFLE_HASH)],
            SQLAnalysis(structure=QueryStructure(join_count=1)),
            [],
        )
        assert not [c for c in cards if "up-to-date" in c.problem]


class TestLLMAnalyzeTableFilter:
    def _make_analysis(self, stats_fresh: bool) -> ProfileAnalysis:
        bi = BottleneckIndicators()
        bi.hash_table_resize_count = 5000
        bi.statistics_confirmed_fresh = stats_fresh
        analysis = ProfileAnalysis(
            query_metrics=_qm(),
            bottleneck_indicators=bi,
            action_cards=[],
        )
        return analysis

    def _llm_text_with_analyze_action(self) -> str:
        """Synthesize an LLM response with an ACTION_PLAN_JSON including ANALYZE TABLE."""
        return """
<!-- ACTION_PLAN_JSON -->
```json
[
  {
    "problem": "Table statistics may be outdated",
    "fix": "Run ANALYZE TABLE to update statistics",
    "fix_sql": "ANALYZE TABLE main.sales COMPUTE STATISTICS FOR ALL COLUMNS;",
    "expected_impact": "medium",
    "effort": "low",
    "risk": "low",
    "risk_reason": "Read-only operation"
  },
  {
    "problem": "JOIN key skew",
    "fix": "Enable AQE skew handling",
    "fix_sql": "SET spark.sql.adaptive.skewJoin.enabled = true;",
    "expected_impact": "high",
    "effort": "low",
    "risk": "low",
    "risk_reason": "Setting is reversible"
  }
]
```
"""

    def test_analyze_action_dropped_when_stats_fresh(self):
        # Phase 2a (v5.16.19): LLM cards go to analysis.llm_action_cards,
        # not analysis.action_cards. The ANALYZE filter still runs.
        analysis = self._make_analysis(stats_fresh=True)
        _merge_llm_action_plan(analysis, self._llm_text_with_analyze_action())
        card_problems = [c.problem for c in analysis.llm_action_cards]
        # ANALYZE card removed
        assert not any("outdated" in p.lower() for p in card_problems)
        # Other card kept
        assert any("skew" in p.lower() for p in card_problems)

    def test_analyze_action_kept_when_stats_not_fresh(self):
        analysis = self._make_analysis(stats_fresh=False)
        _merge_llm_action_plan(analysis, self._llm_text_with_analyze_action())
        card_problems = [c.problem for c in analysis.llm_action_cards]
        assert any("outdated" in p.lower() for p in card_problems)

    def test_caveat_injected_when_stats_state_unknown(self):
        """When EXPLAIN is not attached (stats_fresh=False), the ANALYZE
        recommendation should stay but gain an inline caveat listing the
        alternative causes of hash resize."""
        analysis = self._make_analysis(stats_fresh=False)
        _merge_llm_action_plan(analysis, self._llm_text_with_analyze_action())
        analyze_cards = [c for c in analysis.llm_action_cards if "outdated" in c.problem.lower()]
        assert analyze_cards, "ANALYZE card should survive when stats state is unknown"
        fix = analyze_cards[0].fix
        # Caveat sentinel + each cause label must be present
        assert "Predictive Optimization" in fix or "予測最適化" in fix
        for keyword in (
            "行数爆発",
            "重複 GROUP BY",
            "スキュー",
            "NULL",
            "型不一致",
            "DECIMAL",
            "UDF",
            "メモリ",
        ):
            assert keyword in fix, f"expected {keyword!r} in caveat"

    def test_caveat_injected_on_realistic_ja_llm_output(self):
        """The caveat must also fire for the LLM's actual Japanese wording
        as observed in production reports (e.g. 'テーブル統計の更新' in
        the problem field and 'ANALYZE TABLE ...' in fix_sql)."""
        analysis = self._make_analysis(stats_fresh=False)
        ja_text = """
<!-- ACTION_PLAN_JSON -->
```json
[
  {
    "priority": 6,
    "problem": "テーブル統計の更新（ANALYZE TABLE）",
    "fix": "主要テーブルに対して ANALYZE TABLE を実行してオプティマイザ統計を更新",
    "fix_sql": "ANALYZE TABLE skato.aisin_poc.store_sales_delta_lc COMPUTE STATISTICS FOR ALL COLUMNS;",
    "risk": "low",
    "risk_reason": "読み取り専用",
    "expected_impact": "medium",
    "effort": "low"
  }
]
```
"""
        _merge_llm_action_plan(analysis, ja_text)
        cards = [c for c in analysis.llm_action_cards if "テーブル統計" in c.problem]
        assert cards, "ANALYZE action should be preserved with caveat"
        fix = cards[0].fix
        # Caveat must list all 8 causes the user requested
        for kw in (
            "行数爆発",
            "重複 GROUP BY",
            "キー値スキュー",
            "NULL 集中",
            "型不一致",
            "DECIMAL",
            "UDF",
            "メモリ圧迫",
        ):
            assert kw in fix, f"caveat missing keyword {kw!r}; fix was:\n{fix}"

    def test_caveat_not_duplicated_on_repeat_merge(self):
        analysis = self._make_analysis(stats_fresh=False)
        text = self._llm_text_with_analyze_action()
        _merge_llm_action_plan(analysis, text)
        first_fix = next(
            c.fix for c in analysis.llm_action_cards if "outdated" in c.problem.lower()
        )
        assert first_fix.count("予測最適化") == 1
        # Simulate re-merge on same analysis (rare, but guard against double-append)
        _merge_llm_action_plan(analysis, text)
        second_fix = next(
            c.fix for c in analysis.llm_action_cards if "outdated" in c.problem.lower()
        )
        # Caveat appears exactly once
        assert second_fix.count("予測最適化") == 1

    def test_preserves_alternative_causes_card(self):
        """Phase 2a (v5.16.19): rule-based and LLM cards are kept in
        separate lists. The "up-to-date" rule-based card stays in
        action_cards untouched; the LLM ANALYZE-TABLE recommendation is
        filtered out of llm_action_cards because stats are fresh."""
        from core.models import ActionCard

        analysis = self._make_analysis(stats_fresh=True)
        alt = ActionCard(
            problem="Statistics are up-to-date — consider alternative causes for hash resize",
            fix="Do NOT re-run ANALYZE TABLE",
            fix_sql="",
            expected_impact="high",
            effort="low",
        )
        analysis.action_cards = [alt]
        _merge_llm_action_plan(analysis, self._llm_text_with_analyze_action())
        # Rule-based card untouched
        assert any("up-to-date" in c.problem for c in analysis.action_cards)
        # ANALYZE filtered out of LLM cards (stats fresh)
        assert not any("outdated" in c.problem.lower() for c in analysis.llm_action_cards)
