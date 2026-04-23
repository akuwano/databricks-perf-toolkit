from __future__ import annotations

from core.action_classify import classify_root_cause_group
from core.models import ActionCard, BottleneckIndicators, ProfileAnalysis, QueryMetrics
from core.usecases import _select_top_action_cards


def _card(problem: str, priority: float, group: str = "", category: str = "") -> ActionCard:
    card = ActionCard(
        problem=problem,
        fix="fix",
        expected_impact="medium",
        effort="low",
        root_cause_group=group,
        coverage_category=category,
    )
    card.priority_score = priority
    return card


def _analysis(cards: list[ActionCard]) -> ProfileAnalysis:
    return ProfileAnalysis(
        query_metrics=QueryMetrics(),
        bottleneck_indicators=BottleneckIndicators(),
        action_cards=cards,
    )


def test_preserved_three_all_included_plus_non_preserved():
    """Three preserved cards must surface plus non-preserved fill the
    remaining slots (up to the default Top-10 limit)."""
    analysis = _analysis(
        [
            _card("Investigate data distribution", 9.0, "data_skew", "PARALLELISM"),
            _card("Statistics are up-to-date", 8.5, "statistics_freshness", "QUERY"),
            _card("Mitigate hash resize", 8.0, "spill_memory_pressure", "MEMORY"),
            _card("Low cache hit ratio", 7.0, "cache_utilization", "DATA"),
            _card("Other", 6.5, "join_strategy", "QUERY"),
            _card("Extra", 6.0, "scan_efficiency", "DATA"),
        ]
    )
    selected = _select_top_action_cards(analysis)
    # 6 total cards, all fit under the Top-10 default.
    assert len(selected) == 6
    assert {c.problem for c in selected} >= {
        "Investigate data distribution",
        "Statistics are up-to-date",
        "Mitigate hash resize",
    }


def test_all_cards_pass_through_unchanged():
    """Phase 2c (v5.16.19): _select_top_action_cards is a pass-through
    shim. All input cards are returned verbatim (no dedup, no rerank,
    no cap under the safety bound)."""
    analysis = _analysis(
        [
            _card("Investigate data distribution", 10.0, "data_skew", "PARALLELISM"),
            _card("Statistics are up-to-date", 9.5, "statistics_freshness", "QUERY"),
            _card("Mitigate hash resize", 9.0, "spill_memory_pressure", "MEMORY"),
            _card("Large shuffle absorbed by AQE", 8.5, "shuffle_overhead", "PARALLELISM"),
            _card("Hierarchical Clustering candidate detected", 8.0, "scan_efficiency", "DATA"),
            _card("Low cache hit ratio", 7.5, "cache_utilization", "DATA"),
            _card("Other non-preserved", 7.0, "join_strategy", "QUERY"),
            _card("Extra non-preserved", 6.8, "cache_utilization", "DATA"),
        ]
    )
    selected = _select_top_action_cards(analysis)
    assert len(selected) == 8
    assert [c.problem for c in selected] == [c.problem for c in analysis.action_cards]


def test_registry_priority_order_preserved_through_shim():
    """Order is preserved verbatim — the registry emits cards in
    priority_rank order and the shim must not re-sort them."""
    analysis = _analysis(
        [
            _card("Investigate data distribution", 9.0, "data_skew", "PARALLELISM"),
            _card("Shuffle A", 8.0, "shuffle_overhead", "PARALLELISM"),
            _card("Shuffle B", 7.95, "shuffle_overhead", "PARALLELISM"),
            _card("Shuffle C", 7.9, "shuffle_overhead", "PARALLELISM"),
            _card("Cache", 7.7, "cache_utilization", "DATA"),
            _card("Photon", 7.6, "photon_compatibility", "COMPUTE"),
            _card("Scan", 7.5, "scan_efficiency", "DATA"),
        ]
    )
    selected = _select_top_action_cards(analysis)
    # All 7 cards pass through (no diversity dedup, no rerank).
    assert len(selected) == 7
    assert len([c for c in selected if c.root_cause_group == "shuffle_overhead"]) == 3


def test_fewer_than_five_candidates_returns_all():
    analysis = _analysis(
        [
            _card("A", 3.0, "data_skew", "PARALLELISM"),
            _card("B", 2.0, "scan_efficiency", "DATA"),
            _card("C", 1.0, "sql_pattern", "QUERY"),
        ]
    )
    selected = _select_top_action_cards(analysis)
    assert [c.problem for c in selected] == ["A", "B", "C"]


def test_classifier_examples():
    assert classify_root_cause_group("broadcast hint") == "join_strategy"
    assert classify_root_cause_group("OPTIMIZE TABLE") == "scan_efficiency"


def test_llm_failure_fallback_deterministic_behavior():
    analysis = _analysis(
        [
            _card("Investigate data distribution", 9.0, "data_skew", "PARALLELISM"),
            _card("Cache", 8.0, "cache_utilization", "DATA"),
            _card("Photon", 7.0, "photon_compatibility", "COMPUTE"),
        ]
    )
    selected = _select_top_action_cards(analysis)
    assert [c.problem for c in selected] == ["Investigate data distribution", "Cache", "Photon"]
