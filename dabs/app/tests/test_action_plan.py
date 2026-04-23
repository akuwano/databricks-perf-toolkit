"""Tests for Action Plan Generator — model, parser, merge, reporter."""

from core.llm_prompts import parse_action_plan_from_llm
from core.models import ActionCard


class TestActionCardExtension:
    """Iteration 1: ActionCard model extension."""

    def test_backward_compatible_construction(self):
        """Existing code constructing ActionCard without new fields still works."""
        card = ActionCard(
            problem="Spill detected",
            evidence=["spill_bytes: 5 GB"],
            likely_cause="Large join without broadcast",
            fix="Add BROADCAST hint",
            expected_impact="high",
            effort="medium",
            priority_score=8.5,
            validation_metric="spill_bytes",
        )
        assert card.problem == "Spill detected"
        assert card.priority_score == 8.5

    def test_new_fields_default_empty(self):
        """New fields default to empty values."""
        card = ActionCard()
        assert card.risk == ""
        assert card.risk_reason == ""
        assert card.verification_steps == []

    def test_new_fields_settable(self):
        """New fields can be set at construction."""
        card = ActionCard(
            problem="Low cache hit",
            risk="high",
            risk_reason="Cache miss causes full S3 reads under concurrent load",
            verification_steps=[
                {"metric": "cache_hit_ratio", "expected": "> 80%"},
                {"sql": "SELECT bytes_read_from_cache_percentage ...", "expected": "> 80"},
            ],
        )
        assert card.risk == "high"
        assert card.risk_reason == "Cache miss causes full S3 reads under concurrent load"
        assert len(card.verification_steps) == 2
        assert card.verification_steps[0]["metric"] == "cache_hit_ratio"

    def test_priority_level_property_by_rank(self):
        """priority_level is derived from rank position, not score threshold."""
        cards = [
            ActionCard(priority_score=10.0),
            ActionCard(priority_score=8.0),
            ActionCard(priority_score=6.0),
            ActionCard(priority_score=4.0),
            ActionCard(priority_score=2.0),
            ActionCard(priority_score=1.0),
        ]
        # Assign priority levels based on rank
        levels = ActionCard.assign_priority_levels(cards)
        assert levels[0] == "P0"  # rank 1 → P0
        assert levels[1] == "P0"  # rank 2 → P0
        assert levels[2] == "P1"  # rank 3 → P1
        assert levels[3] == "P1"  # rank 4 → P1
        assert levels[4] == "P1"  # rank 5 → P1
        assert levels[5] == "P2"  # rank 6 → P2

    def test_priority_level_few_cards(self):
        """With few cards, all get appropriate levels."""
        cards = [ActionCard(priority_score=5.0)]
        levels = ActionCard.assign_priority_levels(cards)
        assert levels[0] == "P0"

    def test_priority_level_empty(self):
        """Empty list returns empty."""
        assert ActionCard.assign_priority_levels([]) == []

    def test_existing_properties_unchanged(self):
        """impact_score and effort_score properties still work."""
        card = ActionCard(expected_impact="high", effort="low")
        assert card.impact_score == 5
        assert card.effort_score == 1


class TestParseActionPlanFromLLM:
    """Iteration 2: LLM output parser."""

    def test_parse_valid_json(self):
        text = """## 7. Recommendations

### Priority 1: Fix spill
Some markdown text here.

<!-- ACTION_PLAN_JSON -->
```json
[
  {
    "priority": 1,
    "problem": "Disk spill detected",
    "fix": "Add BROADCAST hint",
    "fix_sql": "SELECT /*+ BROADCAST(t) */ ...",
    "risk": "medium",
    "risk_reason": "May increase driver memory",
    "expected_impact": "high",
    "effort": "low",
    "verification": [
      {"metric": "spill_to_disk_bytes", "expected": "0 bytes"}
    ]
  }
]
```
"""
        actions = parse_action_plan_from_llm(text)
        assert len(actions) == 1
        assert actions[0]["problem"] == "Disk spill detected"
        assert actions[0]["risk"] == "medium"
        assert len(actions[0]["verification"]) == 1

    def test_parse_multiple_actions(self):
        text = """<!-- ACTION_PLAN_JSON -->
```json
[
  {"priority": 1, "problem": "Spill", "fix": "BROADCAST", "risk": "high"},
  {"priority": 2, "problem": "Low cache", "fix": "Optimize layout", "risk": "low"}
]
```"""
        actions = parse_action_plan_from_llm(text)
        assert len(actions) == 2
        assert actions[0]["problem"] == "Spill"
        assert actions[1]["problem"] == "Low cache"

    def test_parse_no_json_block(self):
        """Returns empty list when no JSON block found."""
        text = "## 7. Recommendations\n\nJust some markdown, no JSON."
        actions = parse_action_plan_from_llm(text)
        assert actions == []

    def test_parse_invalid_json(self):
        """Returns empty list on malformed JSON."""
        text = """<!-- ACTION_PLAN_JSON -->
```json
[{"broken: json}]
```"""
        actions = parse_action_plan_from_llm(text)
        assert actions == []

    def test_parse_partial_fields(self):
        """Missing fields get defaults."""
        text = """<!-- ACTION_PLAN_JSON -->
```json
[{"problem": "Something", "fix": "Do this"}]
```"""
        actions = parse_action_plan_from_llm(text)
        assert len(actions) == 1
        assert actions[0]["problem"] == "Something"
        assert actions[0].get("risk", "") == ""
        assert actions[0].get("verification", []) == []

    def test_parse_without_marker(self):
        """Can find JSON block even without ACTION_PLAN_JSON marker."""
        text = """## 7. Recommendations

```json
[{"problem": "Test", "fix": "Fix it", "risk": "low"}]
```"""
        actions = parse_action_plan_from_llm(text)
        assert len(actions) == 1

    def test_parse_empty_text(self):
        assert parse_action_plan_from_llm("") == []
        assert parse_action_plan_from_llm(None) == []


class TestRuleBasedFallback:
    """Iteration 3: Rule-based cards have risk + verification_steps."""

    def _generate_cards(self, **indicator_overrides):
        from core.analyzers import generate_action_cards
        from core.models import BottleneckIndicators, QueryMetrics

        # Default to scan-dominant + non-trivial task time so that the
        # scan_impact / photon-time gates (v5.16.5) don't suppress
        # cards under test.
        indicator_overrides.setdefault("scan_impact_ratio", 0.5)
        bi = BottleneckIndicators(**indicator_overrides)
        qm = QueryMetrics(total_time_ms=60000, task_total_time_ms=60000)
        return generate_action_cards(bi, [], qm, [], [])

    def test_spill_card_has_risk(self):
        cards = self._generate_cards(spill_bytes=5_000_000_000)
        spill_cards = [c for c in cards if "spill" in c.problem.lower()]
        assert len(spill_cards) > 0
        card = spill_cards[0]
        assert card.risk in ("low", "medium", "high")
        assert card.risk_reason != ""
        assert len(card.verification_steps) > 0

    def test_skew_card_has_risk(self):
        cards = self._generate_cards(has_data_skew=True, skewed_partitions=5)
        skew_cards = [c for c in cards if "skew" in c.problem.lower()]
        assert len(skew_cards) > 0
        card = skew_cards[0]
        assert card.risk != ""
        assert len(card.verification_steps) > 0

    def test_cache_card_has_risk(self):
        cards = self._generate_cards(cache_hit_ratio=0.15)
        cache_cards = [c for c in cards if "cache" in c.problem.lower()]
        assert len(cache_cards) > 0
        card = cache_cards[0]
        assert card.risk != ""
        assert len(card.verification_steps) > 0

    def test_all_generated_cards_have_risk(self):
        """All cards generated by rule engine should have risk set."""
        cards = self._generate_cards(
            spill_bytes=2_000_000_000,
            has_data_skew=True,
            skewed_partitions=3,
            cache_hit_ratio=0.1,
        )
        for card in cards:
            assert card.risk in ("low", "medium", "high"), f"Card '{card.problem}' missing risk"
            assert card.risk_reason != "", f"Card '{card.problem}' missing risk_reason"
            assert len(card.verification_steps) > 0, f"Card '{card.problem}' missing verification"


class TestGenerateActionPlanSection:
    """Iteration 5: Action Plan report section."""

    def test_basic_output(self):
        from core.reporters import generate_action_plan_section

        cards = [
            ActionCard(
                problem="Spill detected",
                evidence=["spill: 5 GB"],
                fix="Add BROADCAST hint",
                expected_impact="high",
                effort="medium",
                priority_score=8.5,
                risk="high",
                risk_reason="May cause OOM",
                verification_steps=[{"metric": "spill_bytes", "expected": "0"}],
            ),
            ActionCard(
                problem="Low cache",
                evidence=["cache: 10%"],
                fix="Re-run query",
                expected_impact="low",
                effort="low",
                priority_score=3.0,
                risk="low",
                risk_reason="Safe",
                verification_steps=[{"metric": "cache_hit_ratio", "expected": "> 30%"}],
            ),
        ]
        result = generate_action_plan_section(cards)
        assert "Recommended Actions" in result
        assert "Spill detected" in result
        assert "Impact: HIGH" in result
        assert "Effort: MEDIUM" in result
        assert "Risk" in result or "risk" in result.lower()
        assert "Verification" in result or "verification" in result.lower()

    def test_priority_ordering(self):
        from core.reporters import generate_action_plan_section

        cards = [
            ActionCard(
                problem=f"Issue {i}",
                priority_score=10 - i,
                expected_impact="high",
                effort="low",
                risk="medium",
                risk_reason="test",
                verification_steps=[{"metric": "m", "expected": "ok"}],
            )
            for i in range(6)
        ]
        result = generate_action_plan_section(cards)
        assert "### 1. Issue 0" in result
        assert "### 6. Issue 5" in result

    def test_empty_cards(self):
        from core.reporters import generate_action_plan_section

        result = generate_action_plan_section([])
        assert result == ""
