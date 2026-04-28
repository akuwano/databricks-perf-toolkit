"""Tests for the decimal_heavy_aggregate alert + ActionCard.

Promotes the V5-only LLM-driven DECIMAL recommendation to a deterministic
rule (Codex (e) follow-up, 2026-04-26) so V6 canonical_schema compression
cannot drop it. The detector fires when a node's peak aggregate memory
crosses a threshold AND any aggregate expression contains arithmetic.
"""

from __future__ import annotations

from core.analyzers.bottleneck import (
    _apply_decimal_heavy_aggregate_alert,
    _collect_decimal_heavy_aggregate_examples,
)
from core.analyzers.recommendations_registry import (
    CARDS,
    Context,
    _build_decimal_heavy_aggregate,
    _detect_decimal_heavy_aggregate,
    generate_from_registry,
)
from core.models import (
    BottleneckIndicators,
    NodeMetrics,
    QueryMetrics,
    TableScanMetrics,
)


_GIB = 1024**3


def _make_agg_node(node_id: str, peak_gb: int, exprs: list[str]) -> NodeMetrics:
    return NodeMetrics(
        node_id=node_id,
        node_name="GroupingAggregate",
        peak_memory_bytes=peak_gb * _GIB,
        aggregate_expressions=exprs,
    )


# ---- Detector ----


def test_collect_returns_empty_below_threshold():
    nm = _make_agg_node("1", peak_gb=10, exprs=["SUM(a * b)"])
    assert _collect_decimal_heavy_aggregate_examples([nm]) == []


def test_collect_returns_empty_without_arithmetic():
    nm = _make_agg_node("1", peak_gb=200, exprs=["SUM(a)", "MAX(b)"])
    assert _collect_decimal_heavy_aggregate_examples([nm]) == []


def test_collect_picks_up_heavy_node_with_multiplication():
    nm = _make_agg_node("42", peak_gb=200, exprs=["SUM(ss_quantity * ss_sales_price)"])
    found = _collect_decimal_heavy_aggregate_examples([nm])
    assert len(found) == 1
    assert found[0][0] == "42"
    assert "ss_quantity * ss_sales_price" in found[0][1]


def test_collect_picks_up_addition_subtraction_division():
    nodes = [
        _make_agg_node("1", peak_gb=150, exprs=["SUM(a + b)"]),
        _make_agg_node("2", peak_gb=150, exprs=["SUM(a - b)"]),
        _make_agg_node("3", peak_gb=150, exprs=["AVG(a / b)"]),
    ]
    found = _collect_decimal_heavy_aggregate_examples(nodes)
    assert {nid for nid, _ in found} == {"1", "2", "3"}


def test_collect_truncates_long_expressions():
    long_expr = "SUM(" + ("col_xxxxxxxx * " * 30) + "tail)"
    nm = _make_agg_node("1", peak_gb=200, exprs=[long_expr])
    found = _collect_decimal_heavy_aggregate_examples([nm])
    assert len(found) == 1
    assert len(found[0][1]) <= 120
    assert found[0][1].endswith("...")


def test_apply_alert_sets_indicator_and_examples():
    bi = BottleneckIndicators()
    nm = _make_agg_node("99", peak_gb=300, exprs=["SUM(qty * price)"])
    _apply_decimal_heavy_aggregate_alert(bi, [nm])
    assert bi.decimal_heavy_aggregate is True
    assert len(bi.decimal_heavy_aggregate_examples) == 1
    assert bi.decimal_heavy_aggregate_examples[0][0] == "99"
    # Alert should also be added
    assert any(
        a.metric_name == "decimal_heavy_aggregate" for a in bi.alerts
    )


def test_apply_alert_silent_when_no_qualifying_node():
    bi = BottleneckIndicators()
    nm = _make_agg_node("1", peak_gb=10, exprs=["SUM(a * b)"])  # too small
    _apply_decimal_heavy_aggregate_alert(bi, [nm])
    assert bi.decimal_heavy_aggregate is False
    assert bi.decimal_heavy_aggregate_examples == []
    assert not any(
        a.metric_name == "decimal_heavy_aggregate" for a in bi.alerts
    )


# ---- ActionCard ----


def _ctx_with_indicator(examples: list[tuple[str, str]] | None) -> Context:
    bi = BottleneckIndicators()
    if examples:
        bi.decimal_heavy_aggregate = True
        bi.decimal_heavy_aggregate_examples = examples
    return Context(
        indicators=bi,
        query_metrics=QueryMetrics(total_time_ms=600_000, task_total_time_ms=600_000),
        top_scanned_tables=[
            TableScanMetrics(
                table_name="cat.sch.store_sales",
                bytes_read=2 * (1024**4),  # 2 TB
            )
        ],
    )


def test_card_does_not_fire_when_indicator_false():
    ctx = _ctx_with_indicator(None)
    assert _detect_decimal_heavy_aggregate(ctx) is False


def test_card_fires_when_indicator_set():
    ctx = _ctx_with_indicator([("42", "SUM(qty * price)")])
    assert _detect_decimal_heavy_aggregate(ctx) is True
    cards = _build_decimal_heavy_aggregate(ctx)
    assert len(cards) == 1
    card = cards[0]
    # Evidence cites node id and expression
    blob = " ".join(card.evidence)
    assert "42" in blob
    assert "qty * price" in blob
    # Fix template uses target table name
    assert "store_sales" in card.fix_sql
    assert "DESCRIBE TABLE" in card.fix_sql
    # Card encourages narrowing rather than blanket BIGINT migration
    assert "DECIMAL(18, 2)" in card.fix_sql or "BIGINT" in card.fix_sql


def test_card_evidence_includes_additional_node_count_when_multiple():
    examples = [
        ("1", "SUM(a * b)"),
        ("2", "SUM(c * d)"),
        ("3", "AVG(e / f)"),
    ]
    ctx = _ctx_with_indicator(examples)
    cards = _build_decimal_heavy_aggregate(ctx)
    blob = " ".join(cards[0].evidence)
    assert "2 more" in blob


def test_card_registered_in_cards_tuple():
    ids = {c.card_id for c in CARDS}
    assert "decimal_heavy_aggregate" in ids


def test_card_priority_between_hash_resize_and_aqe_absorbed():
    """48 is sandwiched between hash_resize=50 and aqe_absorbed=45.
    Confirms intentional placement so the card appears with sql-pattern peers."""
    by_id = {c.card_id: c.priority_rank for c in CARDS}
    assert by_id["decimal_heavy_aggregate"] == 48
    assert by_id["hash_resize"] > by_id["decimal_heavy_aggregate"] > by_id["aqe_absorbed"]


# ---- End-to-end via registry ----


def test_registry_emits_card_for_qualifying_indicator():
    bi = BottleneckIndicators()
    bi.decimal_heavy_aggregate = True
    bi.decimal_heavy_aggregate_examples = [("99", "SUM(qty * price)")]
    ctx = Context(
        indicators=bi,
        query_metrics=QueryMetrics(total_time_ms=600_000, task_total_time_ms=600_000),
        top_scanned_tables=[TableScanMetrics(table_name="cat.sch.fact_sales")],
    )
    cards, _fired = generate_from_registry(ctx)
    problems = [c.problem for c in cards]
    assert any("DECIMAL" in p for p in problems)
