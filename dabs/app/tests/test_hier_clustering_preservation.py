"""Hierarchical Clustering card must survive LLM action-plan merge.

Regression fix for the user-reported "階層型クラスタの適用が出なくなった"
issue. The rule-based analyzer emits a card whenever current clustering
keys include a low-cardinality column, but _merge_llm_action_plan()
replaces all non-preserved rule-based cards with the LLM's output and
the LLM often omits this specific advisory.

Add the card's problem title to the preservation markers so it
survives the merge whenever the rule-based condition fires.
"""

from core.models import ActionCard, BottleneckIndicators, ProfileAnalysis
from core.usecases import _merge_llm_action_plan


def _make_analysis_with_hier_card() -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.bottleneck_indicators = BottleneckIndicators()
    a.action_cards = [
        ActionCard(
            problem="Hierarchical Clustering candidate detected",
            evidence=["Low-cardinality clustering key: CS_SOLD_DATE_SK"],
            fix="Use Hierarchical Clustering to nest low-cardinality keys",
            fix_sql="ALTER TABLE ...",
            expected_impact="high",
            effort="low",
        )
    ]
    return a


def test_hier_clustering_card_survives_llm_merge():
    a = _make_analysis_with_hier_card()
    # LLM returns a completely different action list (as markdown + JSON block)
    llm_text = """Some narrative text.

<!-- ACTION_PLAN_JSON -->
```json
[
  {
    "priority": 1,
    "problem": "Reduce shuffle partitions",
    "fix": "Set spark.sql.shuffle.partitions = 400",
    "fix_sql": "",
    "expected_impact": "medium",
    "effort": "low"
  }
]
```
"""
    _merge_llm_action_plan(a, llm_text)
    titles = [c.problem for c in a.action_cards]
    assert any("Hierarchical Clustering" in t for t in titles), (
        f"Hierarchical Clustering card lost after LLM merge: {titles}"
    )


def test_hier_clustering_card_preserved_when_no_llm_actions():
    a = _make_analysis_with_hier_card()
    _merge_llm_action_plan(a, "")  # empty LLM text
    titles = [c.problem for c in a.action_cards]
    assert any("Hierarchical Clustering" in t for t in titles)
