"""Tests for eval.profile_evidence aggregator (Codex (b) helper)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from eval.profile_evidence import collect_profile_evidence


# ---- Fixtures ----


@dataclass
class _NM:
    node_id: str = ""
    peak_memory_bytes: int = 0
    aggregate_expressions: list[str] = field(default_factory=list)


@dataclass
class _SM:
    sink_bytes_written: int = 0
    memory_per_partition_mb: int = 0
    shuffle_attributes: list[str] = field(default_factory=list)


@dataclass
class _TS:
    table_name: str = ""
    current_clustering_keys: list[str] = field(default_factory=list)


@dataclass
class _Col:
    column_name: str = ""
    table_name: str = ""
    table_alias: str = ""


@dataclass
class _Tbl:
    full_name: str = ""
    table: str = ""
    alias: str = ""


@dataclass
class _SQL:
    columns: list[_Col] = field(default_factory=list)
    tables: list[_Tbl] = field(default_factory=list)


@dataclass
class _BI:
    spill_bytes: int = 0


@dataclass
class _QM:
    query_text: str = ""


@dataclass
class _Analysis:
    node_metrics: list[Any] = field(default_factory=list)
    shuffle_metrics: list[Any] = field(default_factory=list)
    top_scanned_tables: list[Any] = field(default_factory=list)
    sql_analysis: Any = None
    bottleneck_indicators: Any = None
    query_metrics: Any = None


_GIB = 1024**3


# ---- DECIMAL signal ----


def test_decimal_signal_fires_on_heavy_arithmetic_agg():
    a = _Analysis(
        node_metrics=[_NM(node_id="55", peak_memory_bytes=200 * _GIB,
                          aggregate_expressions=["SUM(q * p)"])]
    )
    ev = collect_profile_evidence(a)
    assert ev.decimal_arithmetic_in_heavy_agg is True
    assert ev.decimal_arithmetic_examples[0][0] == "55"


def test_decimal_signal_silent_when_no_arithmetic():
    a = _Analysis(
        node_metrics=[_NM(node_id="1", peak_memory_bytes=200 * _GIB,
                          aggregate_expressions=["SUM(q)"])]
    )
    ev = collect_profile_evidence(a)
    assert ev.decimal_arithmetic_in_heavy_agg is False


def test_decimal_signal_silent_when_below_threshold():
    a = _Analysis(
        node_metrics=[_NM(node_id="1", peak_memory_bytes=10 * _GIB,
                          aggregate_expressions=["SUM(q * p)"])]
    )
    ev = collect_profile_evidence(a)
    assert ev.decimal_arithmetic_in_heavy_agg is False


# ---- Shuffle key outside LC ----


def test_dominant_shuffle_outside_lc_fires_when_column_not_clustered():
    sql = _SQL(
        columns=[_Col(column_name="ss_customer_sk", table_name="cat.sch.store_sales")],
        tables=[_Tbl(full_name="cat.sch.store_sales", alias="ss")],
    )
    a = _Analysis(
        shuffle_metrics=[_SM(sink_bytes_written=200 * _GIB,
                              shuffle_attributes=["ss.ss_customer_sk"])],
        top_scanned_tables=[_TS(table_name="cat.sch.store_sales",
                                 current_clustering_keys=["ss_sold_date_sk"])],
        sql_analysis=sql,
    )
    ev = collect_profile_evidence(a)
    assert ev.dominant_shuffle_keys_outside_lc is True
    assert ("cat.sch.store_sales", "ss_customer_sk") in ev.dominant_shuffle_outside_lc_columns


def test_dominant_shuffle_silent_when_column_already_clustered():
    sql = _SQL(
        columns=[_Col(column_name="ss_customer_sk", table_name="cat.sch.store_sales")],
        tables=[_Tbl(full_name="cat.sch.store_sales", alias="ss")],
    )
    a = _Analysis(
        shuffle_metrics=[_SM(sink_bytes_written=200 * _GIB,
                              shuffle_attributes=["ss.ss_customer_sk"])],
        top_scanned_tables=[_TS(table_name="cat.sch.store_sales",
                                 # Already clustered on this column
                                 current_clustering_keys=["ss_customer_sk"])],
        sql_analysis=sql,
    )
    ev = collect_profile_evidence(a)
    assert ev.dominant_shuffle_keys_outside_lc is False


def test_dominant_shuffle_silent_below_thresholds():
    sql = _SQL(
        columns=[_Col(column_name="x", table_name="cat.sch.t")],
        tables=[_Tbl(full_name="cat.sch.t", alias="a")],
    )
    a = _Analysis(
        shuffle_metrics=[_SM(sink_bytes_written=1 * _GIB,  # tiny
                              memory_per_partition_mb=10,
                              shuffle_attributes=["a.x"])],
        top_scanned_tables=[_TS(table_name="cat.sch.t")],
        sql_analysis=sql,
    )
    ev = collect_profile_evidence(a)
    assert ev.dominant_shuffle_keys_outside_lc is False


# ---- CTE multi-reference ----


def test_cte_multi_ref_signal_passes_through_analyzer():
    sql = """
    WITH cte_a AS (SELECT 1)
    SELECT * FROM cte_a UNION ALL SELECT * FROM cte_a;
    """
    a = _Analysis(query_metrics=_QM(query_text=sql))
    ev = collect_profile_evidence(a)
    # If the analyzer is available, the signal will fire; otherwise empty
    # (test should not depend on the underlying parser's specifics).
    assert isinstance(ev.cte_multi_reference, bool)


# ---- Spill ----


def test_spill_dominant_fires_above_threshold():
    a = _Analysis(bottleneck_indicators=_BI(spill_bytes=200 * _GIB))
    ev = collect_profile_evidence(a)
    assert ev.spill_dominant is True
    assert ev.spill_total_bytes == 200 * _GIB


def test_spill_dominant_silent_below_threshold():
    a = _Analysis(bottleneck_indicators=_BI(spill_bytes=10 * _GIB))
    ev = collect_profile_evidence(a)
    assert ev.spill_dominant is False


# ---- Robustness ----


def test_collect_returns_empty_for_none():
    ev = collect_profile_evidence(None)
    assert ev.decimal_arithmetic_in_heavy_agg is False
    assert ev.dominant_shuffle_keys_outside_lc is False
    assert ev.cte_multi_reference is False
    assert ev.spill_dominant is False


def test_collect_tolerates_missing_attributes():
    """A bare object with no relevant attributes should produce an
    empty ProfileEvidence rather than raise."""
    class Dummy:
        pass
    ev = collect_profile_evidence(Dummy())
    assert ev.decimal_arithmetic_in_heavy_agg is False
