"""Tests for the strengthened ``fix_sql`` prompt contract (Path A).

Codex 2026-04-26 review concluded that the LLM was leaving fix_sql
empty for table-design / DDL recommendations because the prompt
contract treated the field as optional ("if applicable" / 「あれば」).
The fix is to make REQUIRED + approved-syntax allowlist visible in
THREE locations in the prompt:

  1. JSON schema field description (`_action_plan_json_schema`)
  2. Recommendation creation rules body (`create_system_prompt`)
  3. Forbidden-adjacent positive allowlist (newly added next to 2)

These tests pin the customer-visible contract so a future model swap
or prompt edit can't silently regress.
"""

from __future__ import annotations

from core.llm_prompts import create_system_prompt, create_structured_system_prompt
from core.llm_prompts.prompts import _action_plan_json_schema


def _system_prompt(lang: str) -> str:
    return create_system_prompt(tuning_knowledge="test knowledge", lang=lang)


# ---------------------------------------------------------------------------
# 箇所 1: JSON schema field description
# ---------------------------------------------------------------------------


class TestFixSqlSchemaContract:
    def test_ja_schema_says_required_for_sql_actions(self):
        block = _action_plan_json_schema("ja")
        # The description for fix_sql must use the word "必須" so the
        # LLM stops treating it as optional.
        assert "必須" in block
        # The allowlist of action types must be enumerated.
        for kw in ("DDL", "DML", "OPTIMIZE", "ALTER", "ANALYZE"):
            assert kw in block, f"missing {kw!r} in JA schema"

    def test_en_schema_says_required_for_sql_actions(self):
        block = _action_plan_json_schema("en")
        assert "Required" in block or "required" in block
        for kw in ("DDL", "DML", "OPTIMIZE", "ALTER", "ANALYZE"):
            assert kw in block, f"missing {kw!r} in EN schema"

    def test_ja_schema_drops_legacy_optional_phrasing(self):
        """The legacy "（あれば）" wording was the root cause — it told
        the model fix_sql was optional. It must be gone."""
        block = _action_plan_json_schema("ja")
        assert "（あれば）" not in block
        assert "あれば)" not in block

    def test_en_schema_drops_legacy_optional_phrasing(self):
        block = _action_plan_json_schema("en")
        assert "if applicable" not in block.lower()


# ---------------------------------------------------------------------------
# 箇所 2: recommendation creation rules body
# ---------------------------------------------------------------------------


class TestRecommendationRulesBody:
    def test_ja_rules_extend_required_to_all_sql_actions(self):
        """Old wording limited the requirement to "DDL examples for
        Hierarchical Clustering". New wording must extend it to any
        SQL action."""
        prompt = _system_prompt("ja")
        # The old narrow phrasing must be replaced.
        assert "DDL 例を示す場合は" not in prompt
        # The new broader rule must be present.
        assert "fix_sql" in prompt
        assert "必須" in prompt

    def test_en_rules_extend_required_to_all_sql_actions(self):
        prompt = _system_prompt("en")
        assert "showing DDL examples for Hierarchical Clustering" not in prompt
        assert "fix_sql" in prompt
        assert "required" in prompt.lower()


# ---------------------------------------------------------------------------
# 箇所 3: approved-syntax allowlist (newly added)
# ---------------------------------------------------------------------------


class TestApprovedSyntaxAllowlist:
    def test_ja_lists_canonical_hc_property_name(self):
        prompt = _system_prompt("ja")
        # The canonical property name must appear so the LLM has a
        # positive example to copy from rather than hedging.
        assert "delta.liquid.hierarchicalClusteringColumns" in prompt

    def test_en_lists_canonical_hc_property_name(self):
        prompt = _system_prompt("en")
        assert "delta.liquid.hierarchicalClusteringColumns" in prompt

    def test_ja_lists_three_approved_syntaxes(self):
        prompt = _system_prompt("ja")
        # All three approved syntaxes must appear together so the LLM
        # treats them as a coherent allowlist.
        assert "ALTER TABLE" in prompt
        assert "CLUSTER BY" in prompt
        assert "OPTIMIZE" in prompt
        assert "FULL" in prompt

    def test_en_lists_three_approved_syntaxes(self):
        prompt = _system_prompt("en")
        assert "ALTER TABLE" in prompt
        assert "CLUSTER BY" in prompt
        assert "OPTIMIZE" in prompt
        assert "FULL" in prompt


# ---------------------------------------------------------------------------
# Structured prompt (the v6 path) carries the same contract
# ---------------------------------------------------------------------------


class TestStructuredPromptCarriesContract:
    def test_structured_ja_has_required_keyword(self):
        prompt = create_structured_system_prompt("test knowledge", lang="ja")
        assert "必須" in prompt or "REQUIRED" in prompt

    def test_structured_en_has_required_keyword(self):
        prompt = create_structured_system_prompt("test knowledge", lang="en")
        assert "required" in prompt.lower()
