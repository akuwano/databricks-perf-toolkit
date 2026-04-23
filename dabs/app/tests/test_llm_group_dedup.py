"""LLM vs rule-based group-overlap dedup (Phase 2a, v5.16.19).

`_merge_llm_action_plan` now drops LLM-generated cards whose classified
``root_cause_group`` matches (or is declared equivalent to) any group
already covered by a registry-emitted rule-based card. Empty/unknown
groups stay (fail-open).

These tests lock down the contract so future edits don't silently
regress into either the previous hybrid-marker behavior (too strict)
or the naive "append everything" (too noisy).
"""

from __future__ import annotations

from core.action_classify import groups_overlap
from core.models import ActionCard, BottleneckIndicators, ProfileAnalysis, QueryMetrics
from core.usecases import _merge_llm_action_plan


class TestGroupsOverlap:
    def test_same_group_overlaps(self):
        assert groups_overlap("shuffle_overhead", "shuffle_overhead")

    def test_declared_symmetric_pair(self):
        assert groups_overlap("data_skew", "shuffle_overhead")
        assert groups_overlap("shuffle_overhead", "data_skew")

    def test_scan_and_delta_write_overlap(self):
        assert groups_overlap("scan_efficiency", "delta_write_overhead")
        assert groups_overlap("delta_write_overhead", "scan_efficiency")

    def test_spill_and_stats_overlap(self):
        assert groups_overlap("spill_memory_pressure", "statistics_freshness")

    def test_unrelated_groups_do_not_overlap(self):
        assert not groups_overlap("photon_compatibility", "cache_utilization")
        assert not groups_overlap("join_strategy", "sql_pattern")

    def test_empty_group_never_overlaps(self):
        """Fail-open: an unclassified LLM card should not be dropped."""
        assert not groups_overlap("", "shuffle_overhead")
        assert not groups_overlap("shuffle_overhead", "")
        assert not groups_overlap("", "")


def _analysis_with_rule_cards(*groups: str) -> ProfileAnalysis:
    rule_cards = []
    for g in groups:
        c = ActionCard(problem=f"rule card for {g}", fix="rule fix")
        c.root_cause_group = g
        rule_cards.append(c)
    return ProfileAnalysis(
        query_metrics=QueryMetrics(),
        bottleneck_indicators=BottleneckIndicators(),
        action_cards=rule_cards,
    )


def _llm_text(problem: str, fix: str = "") -> str:
    """Build the minimal ACTION_PLAN_JSON block our parser accepts.

    ``parse_action_plan_from_llm`` expects a top-level JSON array inside
    a fenced ``json`` code block that follows the ACTION_PLAN_JSON
    marker (see ``llm_prompts.parsing.parse_action_plan_from_llm``).
    """
    import json

    payload = [
        {
            "problem": problem,
            "fix": fix or f"fix for {problem}",
            "fix_sql": "",
            "expected_impact": "medium",
            "effort": "medium",
            "risk": "",
            "risk_reason": "",
            "verification": [],
        }
    ]
    return f"<!-- ACTION_PLAN_JSON -->\n```json\n{json.dumps(payload)}\n```"


class TestLlmGroupDedupInMerge:
    def test_same_group_llm_dropped(self):
        """LLM card with group ``shuffle_overhead`` is dropped when a
        rule-based card already covers ``shuffle_overhead``."""
        analysis = _analysis_with_rule_cards("shuffle_overhead")
        # Classifier keyword path: "repartition" / "coalesce" / "aqe" /
        # "shuffle" land in shuffle_overhead. Avoid "broadcast" because
        # it short-circuits to join_strategy.
        _merge_llm_action_plan(
            analysis,
            _llm_text(
                "Tune spark.sql.shuffle.partitions and use coalesce on the"
                " shuffle stage to cut partition count"
            ),
        )
        assert analysis.llm_action_cards == [], (
            "Expected LLM card to be dropped — same group as rule card"
        )

    def test_overlapping_group_llm_dropped(self):
        """LLM card in an equivalent group is also dropped."""
        analysis = _analysis_with_rule_cards("shuffle_overhead")
        # "data skew" keyword → classified as data_skew, which overlaps
        # with shuffle_overhead in _GROUP_OVERLAPS_RAW.
        _merge_llm_action_plan(
            analysis,
            _llm_text("Mitigate data skew by salting the join key"),
        )
        assert analysis.llm_action_cards == []

    def test_unrelated_group_llm_kept(self):
        """LLM card in a disjoint group is kept."""
        analysis = _analysis_with_rule_cards("shuffle_overhead")
        _merge_llm_action_plan(
            analysis,
            _llm_text("Replace UDF with built-in function for Photon support"),
        )
        assert len(analysis.llm_action_cards) == 1
        assert analysis.llm_action_cards[0].root_cause_group == "photon_compatibility"

    def test_unclassified_llm_kept(self):
        """LLM card whose text doesn't match any classifier keyword is
        kept (fail-open). No rule card can prove equivalence."""
        analysis = _analysis_with_rule_cards("shuffle_overhead")
        _merge_llm_action_plan(
            analysis,
            _llm_text("Review overall query complexity and result set size"),
        )
        assert len(analysis.llm_action_cards) == 1
        # Classifier returns "" on unmatched text — confirm that stays.
        assert analysis.llm_action_cards[0].root_cause_group == ""

    def test_no_rule_cards_means_all_llm_kept(self):
        """With no rule-based cards, the dedup has nothing to check
        against — every LLM card survives."""
        analysis = ProfileAnalysis(
            query_metrics=QueryMetrics(),
            bottleneck_indicators=BottleneckIndicators(),
            action_cards=[],
        )
        _merge_llm_action_plan(
            analysis,
            _llm_text(
                "Tune spark.sql.shuffle.partitions and use coalesce on the"
                " shuffle stage to cut partition count"
            ),
        )
        assert len(analysis.llm_action_cards) == 1
