"""Prompt snapshot tests for V6 feature flag wiring (W3.5 #5).

Codex 指摘: V6 flag を実装した後、prompt がきちんと flag に追従して
変化することを snapshot しておかないと、A/B 評価時に「flag が効いていない」
配線ミスを見落とす危険がある。

このテストは "値そのものの厳密一致" は取らず、flag on/off 間で
顕著なテキスト差分が出ることを確認する pragmatic snapshot。
"""

from __future__ import annotations

import pytest

from core import feature_flags
from core.llm_prompts.knowledge import (
    _ALWAYS_INCLUDE_SECTION_IDS_V6_MINIMUM,
    ALWAYS_INCLUDE_SECTION_IDS,
    get_always_include_section_ids,
)


@pytest.fixture(autouse=True)
def _reset_flags(monkeypatch):
    for flag in feature_flags.ALL_FLAGS:
        monkeypatch.delenv(flag, raising=False)
    feature_flags.reset_cache()
    yield
    feature_flags.reset_cache()


# ----- helper -----


def _set_flag(monkeypatch, flag: str, on: bool) -> None:
    monkeypatch.setenv(flag, "1" if on else "0")
    feature_flags.reset_cache()


# ----- ALWAYS_INCLUDE wiring -----


def test_always_include_set_changes_with_flag(monkeypatch):
    """flag off → 3要素、flag on → 1要素のみ。"""
    _set_flag(monkeypatch, feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, False)
    legacy = get_always_include_section_ids()
    assert legacy == ALWAYS_INCLUDE_SECTION_IDS
    assert len(legacy) == 3

    _set_flag(monkeypatch, feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, True)
    minimum = get_always_include_section_ids()
    assert minimum == _ALWAYS_INCLUDE_SECTION_IDS_V6_MINIMUM
    assert minimum == ["bottleneck_summary"]
    assert "spark_params" not in minimum
    assert "appendix" not in minimum


# ----- review knowledge skip wiring -----


def test_review_knowledge_skip_short_circuits(monkeypatch, mocker=None):
    """V6_REVIEW_NO_KNOWLEDGE=on のとき filter_knowledge_for_analysis は
    呼ばれず、knowledge は空文字で system prompt に渡る。

    具体実装をモックする代わりに、create_review_system_prompt が
    knowledge='' でも error なく文字列を返すかを smoke test として確認。
    """
    from core.llm_prompts.prompts import create_review_system_prompt

    _set_flag(monkeypatch, feature_flags.V6_REVIEW_NO_KNOWLEDGE, True)
    p_empty = create_review_system_prompt(
        tuning_knowledge="", lang="en", is_serverless=False, is_streaming=False
    )
    p_loaded = create_review_system_prompt(
        tuning_knowledge="## section\nsome knowledge text",
        lang="en",
        is_serverless=False,
        is_streaming=False,
    )
    assert isinstance(p_empty, str) and len(p_empty) > 100
    assert isinstance(p_loaded, str) and len(p_loaded) > 100
    # The two prompts MUST differ — knowledge content shows up in one
    assert "some knowledge text" in p_loaded
    assert "some knowledge text" not in p_empty


# ----- refine micro knowledge wiring -----


def test_refine_micro_knowledge_smoke(monkeypatch):
    """create_refine_system_prompt は flag に直接依存しない (llm.py 側で
    呼び出し前に knowledge を絞る) ので、ここでは prompt が空 knowledge
    でも問題なく構築できることだけ確認。"""
    from core.llm_prompts.prompts import create_refine_system_prompt

    _set_flag(monkeypatch, feature_flags.V6_REFINE_MICRO_KNOWLEDGE, True)
    p = create_refine_system_prompt(
        tuning_knowledge="",
        lang="en",
        is_serverless=False,
        is_streaming=False,
    )
    assert isinstance(p, str)
    assert len(p) > 100


# ----- feature flag snapshot -----


def test_snapshot_matches_runtime(monkeypatch):
    """feature_flags.snapshot() の結果が個別 accessor と一致することを確認。

    V6 標準では全 flag が default-on。ここでは
    V6_ALWAYS_INCLUDE_MINIMUM だけを kill-switch off にして、
    snapshot と accessor が同じ整合性で値を返すことを検証する。
    """
    _set_flag(monkeypatch, feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, False)
    snap = feature_flags.snapshot()
    assert snap[feature_flags.V6_CANONICAL_SCHEMA] is True
    assert snap[feature_flags.V6_REVIEW_NO_KNOWLEDGE] is True
    assert snap[feature_flags.V6_ALWAYS_INCLUDE_MINIMUM] is False
    # Direct accessors agree
    assert feature_flags.canonical_schema() is True
    assert feature_flags.review_no_knowledge() is True
    assert feature_flags.always_include_minimum() is False


# ----- knowledge.filter wiring smoke -----


def test_filter_knowledge_minimum_drops_legacy_force_includes(monkeypatch):
    """V6_ALWAYS_INCLUDE_MINIMUM=on のとき、alert がない場合に
    spark_params / appendix が結果に含まれていない。"""
    from core.llm_prompts.knowledge import filter_knowledge_for_analysis

    # Real knowledge format: ## heading FIRST, then <!-- section_id -->
    # Real knowledge format: ## heading FIRST, then <!-- section_id -->
    # Each section's content uses a unique sentinel (BSUM/SP_BODY/AP_BODY) so
    # we can check inclusion/exclusion without matching on heading text.
    parsed = (
        "## bottleneck summary\n<!-- section_id: bottleneck_summary -->\nBSUM\n"
        "## spark params\n<!-- section_id: spark_params -->\nSP_BODY\n"
        "## appendix\n<!-- section_id: appendix -->\nAP_BODY\n"
    )
    _set_flag(monkeypatch, feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, True)
    out = filter_knowledge_for_analysis(parsed, alerts=[], max_chars=99999)
    assert "BSUM" in out  # always-include kept
    assert "SP_BODY" not in out  # dropped
    assert "AP_BODY" not in out  # dropped


def test_recommendation_no_force_fill_appends_skip_directive(monkeypatch):
    """V6_RECOMMENDATION_NO_FORCE_FILL の on/off で recommendation block
    末尾の 'skip if not grounded' 文言が出し分けられること (W3.5 #6)。

    V6 標準では default-on なので "on" 状態が default。kill-switch
    として off にした場合に文言が外れることを検証する。
    """
    from core.llm_prompts.prompts import _recommendation_format_block

    # default ON — directive MUST be present
    block_on = _recommendation_format_block("ja")
    block_on_en = _recommendation_format_block("en")
    assert "V6_RECOMMENDATION_NO_FORCE_FILL" in block_on
    assert "V6_RECOMMENDATION_NO_FORCE_FILL" in block_on_en
    # The directive must explicitly mention numeric prediction restriction
    assert "数値予測" in block_on or "Numeric predictions" in block_on_en

    # Kill-switch path: explicit off removes the directive.
    _set_flag(monkeypatch, feature_flags.V6_RECOMMENDATION_NO_FORCE_FILL, False)
    block_off = _recommendation_format_block("ja")
    block_off_en = _recommendation_format_block("en")
    assert "V6_RECOMMENDATION_NO_FORCE_FILL" not in block_off
    assert "V6_RECOMMENDATION_NO_FORCE_FILL" not in block_off_en
    # Block shorter when off (directive absent)
    assert len(block_on) > len(block_off)


def test_filter_knowledge_legacy_keeps_all_three_when_disabled(monkeypatch):
    """Kill-switch path: V6_ALWAYS_INCLUDE_MINIMUM explicitly disabled
    falls back to legacy "include all 3 sections" behavior."""
    from core.llm_prompts.knowledge import filter_knowledge_for_analysis

    parsed = (
        "## bottleneck summary\n<!-- section_id: bottleneck_summary -->\nBSUM\n"
        "## spark params\n<!-- section_id: spark_params -->\nSP_BODY\n"
        "## appendix\n<!-- section_id: appendix -->\nAP_BODY\n"
    )
    _set_flag(monkeypatch, feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, False)
    out = filter_knowledge_for_analysis(parsed, alerts=[], max_chars=99999)
    assert "BSUM" in out
    assert "SP_BODY" in out
    assert "AP_BODY" in out
