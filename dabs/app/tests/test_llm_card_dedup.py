"""LLM novel-recommendations section must dedup against rule-based cards.

User feedback (v5.16.24): after Phase 2a the "LLM 独自提案（追加）"
section often contained near-duplicates of the rule-based cards above
it. The caveat at the top of the section was not enough — the
perception was of an uncurated, sloppy report. Rule-based cards are
the primary source of truth; LLM cards that address the same root-
cause group must be dropped so only genuinely novel LLM suggestions
surface.

Dedup strategy: classify each LLM card via
``core.action_classify.classify_root_cause_group`` on ``problem +
fix``. Drop the LLM card iff its group is non-empty AND already
present in the rule-based ``analysis.action_cards`` group set. LLM
cards whose group is ``""`` (unknown / novel topic) are kept.
"""

from __future__ import annotations

import json

from core.models import ActionCard, BottleneckIndicators, ProfileAnalysis, QueryMetrics
from core.usecases import _merge_llm_action_plan


def _analysis(rule_based: list[ActionCard]) -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics()
    a.bottleneck_indicators = BottleneckIndicators()
    a.action_cards = list(rule_based)
    return a


def _rule_card(problem: str, group: str) -> ActionCard:
    return ActionCard(problem=problem, fix="rule", root_cause_group=group)


def _llm_text(actions: list[dict]) -> str:
    return "narrative\n<!-- ACTION_PLAN_JSON -->\n```json\n" + json.dumps(actions) + "\n```"


# ---------------------------------------------------------------------------
# (1) Dedup: LLM cards matching rule-based groups must be dropped
# ---------------------------------------------------------------------------


class TestLlmDedupDropsOverlapping:
    def test_shuffle_overlap_dropped(self):
        """Rule-based has shuffle_overhead card → LLM's shuffle card is dropped."""
        a = _analysis([_rule_card("Shuffle operations are dominant", "shuffle_overhead")])
        llm = _llm_text(
            [
                {
                    "problem": "Increase broadcast threshold to reduce shuffle",
                    "fix": "SET spark.sql.autoBroadcastJoinThreshold = 209715200;",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        titles = [c.problem for c in a.llm_action_cards]
        assert "Increase broadcast threshold to reduce shuffle" not in titles, (
            f"LLM shuffle card should be deduped; got: {titles}"
        )

    def test_spill_overlap_dropped(self):
        a = _analysis([_rule_card("I/O delay due to disk spill", "spill_memory_pressure")])
        llm = _llm_text(
            [
                {
                    "problem": "Disk spill — expand cluster",
                    "fix": "Scale up the warehouse to reduce spill",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        titles = [c.problem for c in a.llm_action_cards]
        assert not titles, f"LLM spill card should be deduped; got: {titles}"

    def test_multiple_overlaps_all_dropped(self):
        a = _analysis(
            [
                _rule_card("I/O delay due to disk spill", "spill_memory_pressure"),
                _rule_card("Shuffle operations are dominant", "shuffle_overhead"),
                _rule_card("Low cache hit ratio", "cache_utilization"),
            ]
        )
        llm = _llm_text(
            [
                {"problem": "Spill on sort — grow memory", "fix": "increase memory"},
                {"problem": "Shuffle volume too large", "fix": "use BROADCAST hint"},
                {"problem": "Cache utilization low", "fix": "re-run to warm cache"},
            ]
        )
        _merge_llm_action_plan(a, llm)
        assert a.llm_action_cards == []


# ---------------------------------------------------------------------------
# (2) Novel LLM cards must be kept
# ---------------------------------------------------------------------------


class TestNovelLlmKept:
    def test_novel_topic_kept(self):
        """LLM suggestion with no overlapping group must appear in
        ``llm_action_cards``."""
        a = _analysis([_rule_card("I/O delay due to disk spill", "spill_memory_pressure")])
        llm = _llm_text(
            [
                {
                    "problem": "Consider switching to DLT for incremental refresh",
                    "fix": "CREATE OR REFRESH STREAMING TABLE ...",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        titles = [c.problem for c in a.llm_action_cards]
        assert "Consider switching to DLT for incremental refresh" in titles

    def test_mix_of_overlap_and_novel(self):
        a = _analysis([_rule_card("Shuffle operations are dominant", "shuffle_overhead")])
        llm = _llm_text(
            [
                # Overlap — should be dropped
                {"problem": "Shuffle is high — use BROADCAST", "fix": "BROADCAST hint"},
                # Novel — should be kept
                {
                    "problem": "Enable Delta CDF for downstream consumers",
                    "fix": "ALTER TABLE t SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
                },
            ]
        )
        _merge_llm_action_plan(a, llm)
        titles = [c.problem for c in a.llm_action_cards]
        assert "Enable Delta CDF for downstream consumers" in titles
        assert all("Shuffle" not in t and "BROADCAST" not in t for t in titles)


# ---------------------------------------------------------------------------
# (3) Unknown group stays (conservative)
# ---------------------------------------------------------------------------


class TestUnknownGroupKept:
    def test_unclassifiable_llm_card_kept(self):
        """When the classifier returns no group for the LLM card, keep
        it — a missing classification should not cause silent dropping."""
        a = _analysis([_rule_card("I/O delay due to disk spill", "spill_memory_pressure")])
        llm = _llm_text(
            [
                {
                    "problem": "Review the query purpose with the business owner",
                    "fix": "Schedule a meeting to validate the join logic",
                }
            ]
        )
        _merge_llm_action_plan(a, llm)
        titles = [c.problem for c in a.llm_action_cards]
        assert "Review the query purpose with the business owner" in titles
