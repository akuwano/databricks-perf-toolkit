"""Tests for the compact Top Alerts rendering (Codex Top-Alerts review).

Originally gated by ``V6_COMPACT_TOP_ALERTS`` (retired v6.6.4); the
compact subsection + issue-tag references are now the V6 standard.
The legacy ``## 2. Top Alerts`` standalone section and positional
``→ アラート #N`` references were deleted with the flag.

Surviving tests cover:
- ``match_card_to_issue_tags`` — root_cause_group / keyword fallback
  resolution that drives the ``→ Addresses: shuffle, spill`` tag.
- ``_format_alert_reference`` — issue-tag rendering in the new default.
- ``generate_report`` integration — Section-1 ``### Key Alerts``
  subsection rendering.
"""

from __future__ import annotations

from core.constants import Severity
from core.models import (
    ActionCard,
    Alert,
    BottleneckIndicators,
    ProfileAnalysis,
    QueryMetrics,
)
from core.reporters import generate_report
from core.reporters.alert_crossref import (
    match_card_to_alert_numbers,
    match_card_to_issue_tags,
    sort_alerts_by_severity,
)
from core.reporters.summary import _format_alert_reference


def _alert(category: str, severity: Severity, message: str) -> Alert:
    return Alert(
        message=message,
        severity=severity,
        category=category,
        metric_name="m",
        current_value="v",
        threshold="t",
    )


def _shuffle_card() -> ActionCard:
    return ActionCard(
        problem="Shuffle dominates",
        evidence=[],
        likely_cause="Shuffle key skewed",
        fix="Use REPARTITION on shuffle key",
        fix_sql="",
        expected_impact="high",
        effort="medium",
        root_cause_group="shuffle_overhead",
    )


def _spill_card_with_filter() -> ActionCard:
    """Spill mitigation that also references a WHERE filter — should
    cross-link with scan_efficiency on the secondary rule."""
    return ActionCard(
        problem="Reduce spill via earlier filter",
        evidence=[],
        likely_cause="Excess data downstream",
        fix="Add WHERE clause to drop rows before the join",
        fix_sql="",
        expected_impact="high",
        effort="low",
        root_cause_group="scan_efficiency",
    )


# ---- match_card_to_issue_tags ----


def test_issue_tags_for_shuffle_alert_only():
    alerts = [_alert("shuffle", Severity.CRITICAL, "Shuffle hot")]
    assert match_card_to_issue_tags(_shuffle_card(), alerts) == ["shuffle"]


def test_issue_tags_empty_when_no_match():
    alerts = [_alert("photon", Severity.HIGH, "Photon blocker")]
    assert match_card_to_issue_tags(_shuffle_card(), alerts) == []


def test_issue_tags_secondary_rule_spill_via_where_filter():
    """A scan_efficiency card with WHERE-filter text also addresses
    a spill alert via the cross-group secondary link."""
    alerts = [
        _alert("spill", Severity.CRITICAL, "Spill 100GB"),
        _alert("io", Severity.HIGH, "Pruning low"),
    ]
    tags = match_card_to_issue_tags(_spill_card_with_filter(), alerts)
    # Order: alert iteration order, scan first then spill (because
    # secondary rule appends after primary group matches per alert).
    assert "spill" in tags
    assert "scan" in tags


def test_issue_tags_no_duplicates_across_alerts():
    alerts = [
        _alert("shuffle", Severity.CRITICAL, "Shuffle hot"),
        _alert("shuffle", Severity.HIGH, "Another shuffle"),
    ]
    tags = match_card_to_issue_tags(_shuffle_card(), alerts)
    assert tags == ["shuffle"]


def test_issue_tags_multi_group_card_returns_alert_order():
    """When the card matches multiple groups, the tag order follows the
    alerts list (CRITICAL → HIGH after sort)."""
    alerts = sort_alerts_by_severity([
        _alert("photon", Severity.HIGH, "Photon"),
        _alert("shuffle", Severity.CRITICAL, "Shuffle"),
    ])
    card = ActionCard(
        problem="Generic",
        evidence=[],
        likely_cause="",
        fix="repartition + photon",
        fix_sql="",
        expected_impact="high",
        effort="low",
        root_cause_group="",  # forces keyword fallback → multi-match
    )
    tags = match_card_to_issue_tags(card, alerts)
    # CRITICAL (shuffle) before HIGH (photon) since alerts already sorted
    assert tags == ["shuffle", "photon"]


# ---- _format_alert_reference (V6 standard: tag-based) ----


def test_format_alert_reference_uses_issue_tags():
    """V6 standard: tag-based reference, no positional ``#N`` anchors."""
    alerts = [_alert("shuffle", Severity.CRITICAL, "Shuffle")]
    ref = _format_alert_reference(_shuffle_card(), alerts)
    assert "shuffle" in ref
    assert "#" not in ref


def test_format_alert_reference_no_alerts_returns_empty():
    assert _format_alert_reference(_shuffle_card(), None) == ""
    assert _format_alert_reference(_shuffle_card(), []) == ""


def test_format_alert_reference_no_match_falls_back_to_general():
    """When no card-to-alert linkage is found, the reference renders
    the parenthesized general-recommendation fallback."""
    alerts = [_alert("photon", Severity.HIGH, "Photon")]
    ref = _format_alert_reference(_shuffle_card(), alerts)
    assert "shuffle" not in ref
    assert "(" in ref and ")" in ref


# ---- generate_report integration ----


def _minimal_analysis_with_alerts() -> ProfileAnalysis:
    qm = QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000, query_id="q1")
    bi = BottleneckIndicators()
    bi.alerts = [
        _alert("shuffle", Severity.CRITICAL, "Shuffle takes 41% of total time"),
        _alert("aggregation", Severity.HIGH, "Hash table resized 58000 times\n  Top contributors:\n    foo\n    bar"),
        _alert("cache", Severity.MEDIUM, "Cache hit ratio low"),
    ]
    return ProfileAnalysis(
        query_metrics=qm,
        bottleneck_indicators=bi,
        action_cards=[],
    )


def test_generate_report_collapses_to_key_alerts_subsection():
    """V6 standard: ``## 2. Top Alerts`` standalone section is gone;
    Key Alerts surface as a Section-1 subsection. Multi-line "Top
    contributors" detail is stripped from the executive summary
    (still allowed in Appendix H)."""
    a = _minimal_analysis_with_alerts()
    rpt = generate_report(a, llm_sections={}, lang="en")
    assert "## 2. Top Alerts" not in rpt
    assert "### Key Alerts" in rpt
    body, sep, _appendix = rpt.partition("# 📎 ")
    assert sep, "Appendix divider missing — test fixture broken"
    assert "Top contributors" not in body


def test_generate_report_caps_key_alerts_at_two():
    """Compact subsection shows max 2 alerts; the rest land behind an
    Appendix H pointer."""
    qm = QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000, query_id="q1")
    bi = BottleneckIndicators()
    bi.alerts = [
        _alert("shuffle", Severity.CRITICAL, "S"),
        _alert("spill", Severity.CRITICAL, "P"),
        _alert("aggregation", Severity.HIGH, "H1"),
        _alert("photon", Severity.HIGH, "H2"),
    ]
    a = ProfileAnalysis(query_metrics=qm, bottleneck_indicators=bi, action_cards=[])
    rpt = generate_report(a, llm_sections={}, lang="en")
    # Cap applies to the Key Alerts subsection only; Appendix H may
    # mention severity labels independently. Check the body before
    # the appendix marker.
    body, _sep, _appendix = rpt.partition("# 📎 ")
    assert body.count("[CRITICAL]") + body.count("[HIGH]") <= 2
    assert "Appendix" in rpt or "more" in rpt


def test_generate_report_silent_when_no_critical_high():
    """No CRITICAL/HIGH alerts → no Key Alerts subsection rendered."""
    qm = QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000, query_id="q1")
    bi = BottleneckIndicators()
    bi.alerts = [_alert("cache", Severity.MEDIUM, "Cache low")]
    a = ProfileAnalysis(query_metrics=qm, bottleneck_indicators=bi, action_cards=[])
    rpt = generate_report(a, llm_sections={}, lang="en")
    assert "## 2. Top Alerts" not in rpt
    assert "### Key Alerts" not in rpt


# ---- Backward-compat invariant: match_card_to_alert_numbers still callable ----


def test_match_card_to_alert_numbers_still_callable():
    """The numbered helper is preserved for any caller that still
    needs positional anchors (e.g. eval baselines comparing legacy
    output). It is no longer wired into the report rendering."""
    alerts = [
        _alert("shuffle", Severity.CRITICAL, "Shuffle"),
        _alert("photon", Severity.HIGH, "Photon"),
    ]
    nums = match_card_to_alert_numbers(_shuffle_card(), alerts)
    assert nums == [1]
