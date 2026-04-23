"""Rule-based ActionCards covering CRITICAL/HIGH/MEDIUM alerts must survive
LLM merge. When the LLM generates a card covering the same alert, its
enriched content wins (hybrid dedup); when the LLM misses an alert,
the rule-based card is kept as a fail-safe coverage guarantee.

Regression: the user reported that the CRITICAL "Shuffle operations are
dominant" card was being dropped because it was not listed in
_preservation_markers. Same class as the earlier "Hierarchical Clustering
candidate" bug. v5.15.5 closes the class by registering all 11 rule-based
problem markers (EN + JA) and adding hybrid dedup so preservation no
longer causes duplication when the LLM covers the same alert.
"""

import pytest
from core.models import ActionCard, BottleneckIndicators, ProfileAnalysis
from core.usecases import _merge_llm_action_plan


def _analysis_with(cards: list[ActionCard]) -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.bottleneck_indicators = BottleneckIndicators()
    a.action_cards = list(cards)
    return a


def _llm_text(actions: list[dict]) -> str:
    import json as _json

    return "narrative\n<!-- ACTION_PLAN_JSON -->\n```json\n" + _json.dumps(actions) + "\n```"


# ---------------------------------------------------------------------------
# (1) Coverage: each of the 11 added rule-based problems survives the merge
#     when the LLM output does NOT cover it.
# ---------------------------------------------------------------------------


_PROBLEMS_EN = [
    "Shuffle operations are dominant",
    "I/O delay due to disk spill",
    "Processing imbalance due to data skew",
    "Photon-unsupported operation: UDF call",  # dynamic suffix
    "Low Photon utilization",
    "Low cache hit ratio",
    "Low file pruning efficiency",
    "Scan operation accounts for 55% of total time",  # dynamic
    "Non-Photon join accounts for 42% of total time",  # dynamic
    "High rescheduled scan ratio indicates scan locality degradation",
    'CTE "base" is referenced 4 times — Spark may re-execute it each time',  # dynamic
    # Hash-resize variants (recommendations.py generates 4 wordings)
    "Mitigate hash resize on JOIN and GROUP BY hot columns",
    "Mitigate hash resize on hot GROUP BY columns",
    "Mitigate hash resize on hot JOIN keys",
    "Mitigate hash table resize / high probe count",
    # Other preserved problems not covered by the list above
    "Investigate data distribution on hot join/grouping columns",
    "Statistics are up-to-date — consider alternative causes for hash resize",
    "Large shuffle absorbed by AQE — improve physical layout + review data types",
    "Hierarchical Clustering candidate detected",
]

_PROBLEMS_JA = [
    "Shuffle操作の支配",
    "ディスクスピルによるI/O遅延",
    "データスキューによる処理不均衡",
    "Photon非対応の操作: UDF呼び出し",  # dynamic suffix
    "Photon利用率の低さ",
    "キャッシュヒット率の低さ",
    "ファイルプルーニング効率の低さ",
    "Scan操作が全体の55%を占有",
    "非Photon Joinが全体の42%を占有",
    "リスケジュールスキャン率の高さはスキャンローカリティの低下を示しています",
    'CTE "base" が 4 回参照されています — Spark は毎回再実行する可能性があります',
    # Hash-resize JA variants (including the fallback whose wording
    # splits "のリサイズ / プローブ過多を緩和" — regression for v5.16.1)
    "JOIN キーと GROUP BY ホットカラムのハッシュリサイズを緩和",
    "GROUP BY ホットカラムのハッシュリサイズを緩和",
    "JOIN ホットキーのハッシュリサイズを緩和",
    "ハッシュテーブルのリサイズ / プローブ過多を緩和",
    # Other preserved problems whose JA translation relies on
    # existing markers via substring match
    "ホット化している JOIN / GROUP BY カラムのデータ分布を調査",
    "統計情報は最新 — ハッシュリサイズの別要因を検討",
    "AQE により吸収された大容量シャッフル — 物理レイアウト改善 + データ型見直し",
    "階層型クラスタリング候補を検出",
]


class TestAllRuleBasedCardsSurviveWhenLlmMisses:
    """When the LLM output is unrelated, every registered rule-based card
    must appear in the merged result. No silent drops."""

    @pytest.mark.parametrize("problem", _PROBLEMS_EN)
    def test_en_problem_preserved(self, problem):
        a = _analysis_with([ActionCard(problem=problem, fix="do X")])
        llm = _llm_text([{"problem": "Completely unrelated recommendation", "fix": "Set foo=bar"}])
        _merge_llm_action_plan(a, llm)
        titles = [c.problem for c in a.action_cards]
        assert problem in titles, f"Rule-based card '{problem}' lost — titles={titles}"

    @pytest.mark.parametrize("problem", _PROBLEMS_JA)
    def test_ja_problem_preserved(self, problem):
        a = _analysis_with([ActionCard(problem=problem, fix="do X")])
        llm = _llm_text([{"problem": "無関係な推奨", "fix": "foo=barを設定"}])
        _merge_llm_action_plan(a, llm)
        titles = [c.problem for c in a.action_cards]
        assert problem in titles, f"JA rule-based card '{problem}' lost — titles={titles}"


# ---------------------------------------------------------------------------
# (2) Hybrid dedup: when the LLM produces a card that covers the same
#     alert, the LLM-enriched card wins and the rule-based one is dropped.
# ---------------------------------------------------------------------------


class TestLlmAndRuleBasedCoexistWithoutDedup:
    """Phase 2a (v5.16.19): hybrid dedup was removed. Rule-based and
    LLM cards can now coexist verbatim — rule-based stays in
    ``analysis.action_cards`` (from registry), LLM goes to
    ``analysis.llm_action_cards``. Both may address the same alert
    with different wording; the reporter renders them in separate
    sections so the distinction is explicit."""

    def test_rule_and_llm_coexist_when_both_address_shuffle(self):
        a = _analysis_with(
            [
                ActionCard(
                    problem="Shuffle operations are dominant",
                    fix="Expand broadcast threshold",
                    fix_sql="-- rule-based",
                )
            ]
        )
        llm = _llm_text(
            [
                {
                    "problem": "Shuffle operations are dominant across multiple joins",
                    "fix": "Use BROADCAST(dim) hint; pre-aggregate fact side",
                    "fix_sql": "SELECT /*+ BROADCAST(dim) */ ...",
                    "expected_impact": "high",
                    "effort": "low",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        # Rule-based card untouched
        assert len(a.action_cards) == 1
        assert "rule-based" in a.action_cards[0].fix_sql
        # LLM card is in its own list
        assert len(a.llm_action_cards) == 1
        assert "BROADCAST(dim)" in a.llm_action_cards[0].fix_sql

    def test_ja_coverage_also_coexist(self):
        """Same-language (JA) both-layer output coexists unchanged."""
        a = _analysis_with([ActionCard(problem="Shuffle操作の支配", fix="Broadcastを使う")])
        llm = _llm_text(
            [
                {
                    "problem": "Shuffle操作の支配: JOIN間で過剰なシャッフル",
                    "fix": "BROADCAST(dim) ヒントと事前集約",
                    "fix_sql": "-- llm enriched",
                    "expected_impact": "high",
                    "effort": "low",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        assert len(a.action_cards) == 1  # rule-based untouched
        assert len(a.llm_action_cards) == 1
        assert "llm enriched" in a.llm_action_cards[0].fix_sql

    def test_cross_locale_coexist(self):
        """Rule-based JA + LLM EN for the same alert coexist (no dedup)."""
        a = _analysis_with([ActionCard(problem="Shuffle操作の支配", fix="-- rule-based")])
        llm = _llm_text(
            [
                {
                    "problem": "Shuffle operations are dominant in join stage",
                    "fix": "-- llm",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        assert len(a.action_cards) == 1
        assert len(a.llm_action_cards) == 1
        assert "llm" in a.llm_action_cards[0].fix


# ---------------------------------------------------------------------------
# (3) Coverage when LLM misses the alert entirely.
# ---------------------------------------------------------------------------


class TestFailSafeCoverageWhenLlmMisses:
    def test_rule_and_llm_coexist_when_different_alerts(self):
        a = _analysis_with(
            [
                ActionCard(problem="Shuffle operations are dominant", fix="-- rule"),
                ActionCard(problem="I/O delay due to disk spill", fix="-- rule spill"),
            ]
        )
        llm = _llm_text(
            [
                {
                    "problem": "Increase cluster size to absorb memory pressure",
                    "fix": "-- llm completely different",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        # Rule-based cards untouched
        rule_titles = [c.problem for c in a.action_cards]
        assert "Shuffle operations are dominant" in rule_titles
        assert "I/O delay due to disk spill" in rule_titles
        # LLM-only card lands in llm_action_cards
        llm_titles = [c.problem for c in a.llm_action_cards]
        assert "Increase cluster size to absorb memory pressure" in llm_titles


# ---------------------------------------------------------------------------
# (4) Dynamic problem strings preserved via prefix marker.
# ---------------------------------------------------------------------------


class TestDynamicProblemsPreserved:
    def test_cte_multi_ref_with_any_name_preserved(self):
        a = _analysis_with(
            [ActionCard(problem='CTE "my_custom_cte" is referenced 5 times', fix="-- rule")]
        )
        _merge_llm_action_plan(a, _llm_text([{"problem": "unrelated", "fix": "x"}]))
        titles = [c.problem for c in a.action_cards]
        assert any('CTE "my_custom_cte"' in t for t in titles)

    def test_scan_operation_share_dynamic_preserved(self):
        a = _analysis_with(
            [ActionCard(problem="Scan operation accounts for 73% of total time", fix="-- rule")]
        )
        _merge_llm_action_plan(a, _llm_text([{"problem": "unrelated", "fix": "x"}]))
        titles = [c.problem for c in a.action_cards]
        assert any("Scan operation accounts for" in t for t in titles)


# ---------------------------------------------------------------------------
# (5) CTE marker uses prefix match — LLM prose mentioning a CTE name in the
#     middle of a sentence must NOT false-positive as coverage of the
#     CTE multi-reference alert.
# ---------------------------------------------------------------------------


class TestCteMarkerPrefixOnly:
    def test_rule_cte_card_survives_when_llm_only_mentions_cte_in_prose(self):
        """LLM output `Add a CTE "tmp" to reduce shuffle` must NOT trigger
        dedup of the rule-based `CTE "base" is referenced 4 times` card."""
        a = _analysis_with(
            [
                ActionCard(
                    problem='CTE "base" is referenced 4 times',
                    fix="-- rule",
                )
            ]
        )
        llm = _llm_text(
            [
                {
                    "problem": 'Add a CTE "tmp" to reduce shuffle volume',
                    "fix": "WITH tmp AS (SELECT ...) ...",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        titles = [c.problem for c in a.action_cards]
        # Rule-based card must survive — LLM is talking about a different
        # concept (introducing a CTE) not the multi-reference alert.
        assert any('CTE "base"' in t for t in titles), (
            f"Rule CTE card was incorrectly deduped by unrelated LLM prose: {titles}"
        )

    def test_cte_rule_and_llm_coexist_when_both_address_same_cte(self):
        """Phase 2a: no more dedup. Rule-based CTE card stays in
        action_cards; LLM's version of the same alert goes to
        llm_action_cards."""
        a = _analysis_with(
            [
                ActionCard(
                    problem='CTE "base" is referenced 4 times',
                    fix="-- rule",
                )
            ]
        )
        llm = _llm_text(
            [
                {
                    "problem": 'CTE "base" is referenced 4 times — consolidate UNION ALL',
                    "fix": "-- llm enriched",
                    "fix_sql": "SELECT ... FROM base UNION ALL ...",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        # Rule-based kept
        assert len(a.action_cards) == 1
        assert a.action_cards[0].fix == "-- rule"
        # LLM version in its own list
        assert len(a.llm_action_cards) == 1
        assert "llm enriched" in a.llm_action_cards[0].fix
