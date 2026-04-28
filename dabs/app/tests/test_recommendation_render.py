"""Tests for the 3-band SizingRecommendation rendering in reports.

The recommendation block is appended to both warehouse-known and
fallback cost outputs. These tests pin down the customer-visible
format (Codex 2026-04-26 spec): main row = Recommended (★ implied),
secondary rows = Minimum viable + Oversized beyond, scope notice,
confidence, and the Pro/Classic uptime caveat.
"""

from __future__ import annotations

from core.dbsql_cost import estimate_query_cost
from core.models import QueryMetrics
from core.reporters.sections import _format_recommendation_block


def _make_query(
    *,
    typename: str = "LakehouseSqlQuery",
    execution_time_ms: int = 60_000,
    task_total_time_ms: int = 60_000 * 60,
) -> QueryMetrics:
    return QueryMetrics(
        query_id="q",
        execution_time_ms=execution_time_ms,
        query_typename=typename,
        task_total_time_ms=task_total_time_ms,
    )


def test_block_is_empty_when_recommendation_missing():
    assert _format_recommendation_block(None) == ""


def test_block_contains_three_band_table_headers():
    cost = estimate_query_cost(_make_query(), None)
    block = _format_recommendation_block(cost.recommendation)
    assert "Recommended" in block
    assert "Minimum viable" in block
    # Oversized beyond is conditional — present for normal workloads.
    assert "Oversized beyond" in block


def test_block_contains_billing_label():
    cost = estimate_query_cost(_make_query(typename="LakehouseSqlQuery"), None)
    block = _format_recommendation_block(cost.recommendation)
    # Serverless: "Estimated execution cost" must appear in the table header
    assert "Estimated execution cost" in block


def test_classic_block_carries_uptime_disclaimer():
    """Pro/Classic must carry the "actual billing is uptime-based" note
    so customers don't read the value as their literal bill."""
    cost = estimate_query_cost(_make_query(typename="SqlQuery"), None)
    block = _format_recommendation_block(cost.recommendation)
    assert "Runtime-equivalent" in block or "dedicated" in block.lower()
    assert "uptime" in block.lower()


def test_serverless_block_omits_uptime_disclaimer():
    """Serverless billing IS per-query — the uptime disclaimer would be
    misleading."""
    cost = estimate_query_cost(_make_query(typename="LakehouseSqlQuery"), None)
    block = _format_recommendation_block(cost.recommendation)
    # The uptime disclaimer is the long sentence starting with "Note:
    # actual billing for Classic/Pro" — not the brief mention in the
    # rationale list.
    assert "actual billing for Classic/Pro" not in block


def test_block_contains_scope_notice():
    """Recommendation must NOT be presented as universal — the scope
    notice tells the reader it only applies to the observed run."""
    cost = estimate_query_cost(_make_query(), None)
    block = _format_recommendation_block(cost.recommendation)
    assert "observed" in block.lower()


def test_block_lists_confidence_and_rationale():
    cost = estimate_query_cost(_make_query(), None)
    block = _format_recommendation_block(cost.recommendation)
    assert "Confidence" in block
    # Rationale items are rendered as Markdown list items.
    assert "- " in block
