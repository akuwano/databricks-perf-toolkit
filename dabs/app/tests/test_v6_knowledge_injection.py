"""Tests for V6 knowledge injection wiring (Week 3 Day 4)."""

from __future__ import annotations

import pytest

from core import feature_flags
from core.llm_prompts.knowledge import (
    ALWAYS_INCLUDE_SECTION_IDS,
    _ALWAYS_INCLUDE_SECTION_IDS_V6_MINIMUM,
    get_always_include_section_ids,
)


@pytest.fixture(autouse=True)
def _reset_flags(monkeypatch):
    for flag in feature_flags.ALL_FLAGS:
        monkeypatch.delenv(flag, raising=False)
    feature_flags.reset_cache()
    yield
    feature_flags.reset_cache()


def test_always_include_legacy_when_flag_disabled(monkeypatch):
    """V6_ALWAYS_INCLUDE_MINIMUM disabled (kill-switch path) → legacy
    [bottleneck_summary, spark_params, appendix]."""
    monkeypatch.setenv(feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, "0")
    feature_flags.reset_cache()
    ids = get_always_include_section_ids()
    assert ids == ALWAYS_INCLUDE_SECTION_IDS
    assert "spark_params" in ids
    assert "appendix" in ids


def test_always_include_minimum_with_flag(monkeypatch):
    """flag on → V6 minimal [bottleneck_summary] only."""
    monkeypatch.setenv(feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, "1")
    feature_flags.reset_cache()
    ids = get_always_include_section_ids()
    assert ids == _ALWAYS_INCLUDE_SECTION_IDS_V6_MINIMUM
    assert ids == ["bottleneck_summary"]
    assert "spark_params" not in ids
    assert "appendix" not in ids


def test_always_include_returns_list_copy():
    """Helper returns a fresh list — caller mutation must not affect global."""
    ids = get_always_include_section_ids()
    ids.append("unsafe")
    ids2 = get_always_include_section_ids()
    assert "unsafe" not in ids2


def test_filter_knowledge_score_uses_helper(monkeypatch):
    """When V6_ALWAYS_INCLUDE_MINIMUM=1, scoring should NOT auto-include
    spark_params (because the helper returns the minimal set)."""
    monkeypatch.setenv(feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, "1")
    feature_flags.reset_cache()

    from core.llm_prompts.knowledge import _score_sections_by_alerts

    sections = {
        "bottleneck_summary": "x",
        "spark_params": "y",
        "appendix": "z",
        "spill": "w",
    }
    scores = _score_sections_by_alerts(sections, alerts=[])
    # bottleneck_summary should be force-included
    assert scores.get("bottleneck_summary") == 100
    # spark_params + appendix should NOT be force-included anymore
    assert scores.get("spark_params") is None
    assert scores.get("appendix") is None


def test_filter_knowledge_score_legacy_includes_all_three(monkeypatch):
    """Kill-switch path: when V6_ALWAYS_INCLUDE_MINIMUM is explicitly
    disabled, all 3 legacy ALWAYS_INCLUDE sections are force-included."""
    monkeypatch.setenv(feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, "0")
    feature_flags.reset_cache()
    from core.llm_prompts.knowledge import _score_sections_by_alerts

    sections = {
        "bottleneck_summary": "x",
        "spark_params": "y",
        "appendix": "z",
    }
    scores = _score_sections_by_alerts(sections, alerts=[])
    assert scores["bottleneck_summary"] == 100
    assert scores["spark_params"] == 100
    assert scores["appendix"] == 100
