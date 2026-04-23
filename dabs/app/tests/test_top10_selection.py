"""Top-5 was expanded to Top-10 so every Top Alert has space to
receive a corresponding action card.

Regression: a shared report with a CRITICAL Shuffle alert had no
action for it. Two design flaws compounded:

  (1) ``min(3, len(preserved), limit)`` capped preserved cards at 3,
      so when 5+ alerts were active the 4th+ preserved card was
      evicted regardless of priority.

  (2) The LLM hybrid dedup replaced the rule-based shuffle card with
      an LLM version that did NOT inherit ``is_preserved=True``, so
      it competed in the non-preserved diversity rerank and could lose.

Fix: raise the limit default to 10 and drop the 3-cap so preserved
cards fill as many slots as they need, leaving the tail for
non-preserved / LLM-only recommendations.
"""

from __future__ import annotations

from core.models import ActionCard, BottleneckIndicators, ProfileAnalysis, QueryMetrics
from core.usecases import _select_top_action_cards


def _card(
    problem: str,
    priority: float,
    group: str = "",
    category: str = "",
    impact: str = "medium",
    effort: str = "low",
) -> ActionCard:
    c = ActionCard(
        problem=problem,
        fix="fix",
        expected_impact=impact,
        effort=effort,
        root_cause_group=group,
        coverage_category=category,
    )
    c.priority_score = priority
    return c


def _analysis(cards: list[ActionCard]) -> ProfileAnalysis:
    return ProfileAnalysis(
        query_metrics=QueryMetrics(),
        bottleneck_indicators=BottleneckIndicators(),
        action_cards=cards,
    )


class TestDefaultLimit:
    def test_default_limit_large_enough(self):
        """Phase 2c (v5.16.19): Top-N selection became a no-op — the
        default limit is now a pathological-case safety bound (>=20)
        rather than an intentional cap."""
        import inspect

        sig = inspect.signature(_select_top_action_cards)
        assert sig.parameters["limit"].default >= 20, (
            f"Expected limit safety bound >= 20, got {sig.parameters['limit'].default}"
        )


class TestAllPreservedSurviveWithoutThreeCap:
    """The 3-cap on preserved cards was the root cause of the
    "Top Alert has no corresponding action" bug. Dropping it means
    all preserved cards — up to the overall limit — reach Top-N."""

    def test_five_preserved_all_surface_in_top10(self):
        # 5 rule-based preserved cards — one per top alert
        cards = [
            _card("I/O delay due to disk spill", 9.0, "spill_memory_pressure", "MEMORY"),
            _card("Shuffle operations are dominant", 15.0, "shuffle_overhead", "PARALLELISM"),
            _card("Low file pruning efficiency", 1.2, "scan_efficiency", "DATA"),
            _card("Low cache hit ratio", 6.0, "cache_utilization", "COMPUTE"),
            _card("Processing imbalance due to data skew", 9.0, "data_skew", "PARALLELISM"),
        ]
        selected = _select_top_action_cards(_analysis(cards))
        problems = {c.problem for c in selected}
        # All five preserved cards must appear in the result.
        for expected in {
            "I/O delay due to disk spill",
            "Shuffle operations are dominant",
            "Low file pruning efficiency",
            "Low cache hit ratio",
            "Processing imbalance due to data skew",
        }:
            assert expected in problems, f"Preserved card '{expected}' dropped from Top-10"

    def test_critical_shuffle_card_not_dropped_even_with_low_priority_pruning(self):
        """User regression: CRITICAL Shuffle card missing from Top-N
        because 3-cap + priority_score sort pushed it below pruning."""
        cards = [
            # Fake 3 preserved with very high priority that would saturate the 3-cap
            _card("I/O delay due to disk spill", 100.0, "spill_memory_pressure", "MEMORY"),
            _card("Low cache hit ratio", 99.0, "cache_utilization", "COMPUTE"),
            _card("Low file pruning efficiency", 98.0, "scan_efficiency", "DATA"),
            # Then the shuffle card at lower priority — must still survive
            _card("Shuffle operations are dominant", 15.0, "shuffle_overhead", "PARALLELISM"),
            _card("Processing imbalance due to data skew", 12.0, "data_skew", "PARALLELISM"),
        ]
        selected = _select_top_action_cards(_analysis(cards))
        problems = {c.problem for c in selected}
        assert "Shuffle operations are dominant" in problems, (
            f"Shuffle card dropped from Top-10; got {problems}"
        )


class TestAllCardsPassThrough:
    """Phase 2c (v5.16.19): _select_top_action_cards is a pass-through
    shim — it returns ``action_cards`` verbatim (capped at the safety
    bound). The diversity-rerank and preservation-aware selection were
    removed because the registry already emits cards in the desired
    priority_rank order."""

    def test_all_cards_returned_when_below_limit(self):
        cards = [
            _card("I/O delay due to disk spill", 9.0, "spill_memory_pressure", "MEMORY"),
            _card("Shuffle operations are dominant", 15.0, "shuffle_overhead", "PARALLELISM"),
        ]
        cards += [
            _card(f"LLM novel idea {i}", 8.0 - i * 0.1, f"group_{i}", f"CAT_{i}") for i in range(10)
        ]
        selected = _select_top_action_cards(_analysis(cards))
        assert len(selected) == 12
        # Order is preserved verbatim — no re-sort, no dedup.
        assert [c.problem for c in selected] == [c.problem for c in cards]

    def test_all_cards_returned_with_mixed_pool(self):
        cards = [
            _card("Investigate data distribution", 9.0, "data_skew", "PARALLELISM"),
            _card("Statistics are up-to-date", 8.5, "statistics_freshness", "QUERY"),
        ]
        cards += [_card(f"Idea {i}", 7.0 - i * 0.1, f"g{i}", f"C{i}") for i in range(8)]
        selected = _select_top_action_cards(_analysis(cards))
        assert len(selected) == 10
