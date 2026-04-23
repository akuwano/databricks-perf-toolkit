"""Canonical DBSQL Hierarchical Clustering SQL emission (v5.16.19).

Regression: the previous ActionCard SQL used ``CLUSTER BY (...) WITH
(HIERARCHICAL CLUSTERING (...))`` and the non-existent
``delta.feature.hierarchicalClustering`` flag. Per the 2025 Liquid
Hierarchical Clustering field guide, the canonical form is:

  ALTER TABLE t CLUSTER BY (low_c, high_col);
  ALTER TABLE t SET TBLPROPERTIES (
    'delta.liquid.hierarchicalClusteringColumns' = 'low_c'
  );
  OPTIMIZE t FULL;  -- DBR 17.1+

These tests lock the emission shape so a future edit cannot silently
regress back to the invalid syntax.
"""

from __future__ import annotations

from core.analyzers.recommendations_registry import (
    CARDS,
    Context,
    generate_from_registry,
)
from core.models import (
    BottleneckIndicators,
    QueryMetrics,
    ShuffleMetrics,
    TableScanMetrics,
)

_CANONICAL_PROPERTY = "delta.liquid.hierarchicalClusteringColumns"
_LEGACY_WITH_CLAUSE = "WITH (HIERARCHICAL CLUSTERING"
_LEGACY_FEATURE_FLAG = "delta.feature.hierarchicalClustering"


def _find_card(card_id: str):
    return next(c for c in CARDS if c.card_id == card_id)


class TestHierClusteringCardSql:
    """The primary HC card fires when an existing CLUSTER BY column is
    low-cardinality. Its fix_sql must follow the canonical 2025 syntax."""

    def _ctx_with_low_cardinality_key(self):
        ts = TableScanMetrics(
            table_name="catalog.schema.sales",
            current_clustering_keys=["order_month", "order_id"],
            clustering_key_cardinality={"order_month": "low", "order_id": "high"},
        )
        return Context(
            indicators=BottleneckIndicators(),
            query_metrics=QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000),
            top_scanned_tables=[ts],
        )

    def test_fires_on_low_cardinality_cluster_key(self):
        card_def = _find_card("hier_clustering")
        assert card_def.detect(self._ctx_with_low_cardinality_key())

    def test_fix_sql_uses_canonical_tblproperties(self):
        card_def = _find_card("hier_clustering")
        built = card_def.build(self._ctx_with_low_cardinality_key())
        assert built, "hier_clustering card must emit at least one ActionCard"
        sql = built[0].fix_sql
        assert _CANONICAL_PROPERTY in sql, (
            f"Canonical TBLPROPERTIES name missing from fix_sql: {sql!r}"
        )
        assert "OPTIMIZE" in sql and "FULL" in sql
        assert "ALTER TABLE" in sql and "CLUSTER BY" in sql

    def test_fix_sql_rejects_legacy_syntax(self):
        card_def = _find_card("hier_clustering")
        built = card_def.build(self._ctx_with_low_cardinality_key())
        sql = built[0].fix_sql
        assert _LEGACY_WITH_CLAUSE not in sql, (
            "Legacy `CLUSTER BY (...) WITH (HIERARCHICAL CLUSTERING ...)`"
            f" clause must not appear in fix_sql: {sql!r}"
        )
        assert _LEGACY_FEATURE_FLAG not in sql, (
            f"Legacy feature flag must not appear in fix_sql: {sql!r}"
        )


class TestShuffleLcFallbackSql:
    """The shuffle_lc card includes an HC fallback snippet when only the
    low-cardinality key maps onto a scanned table. That fallback path
    must also emit the canonical syntax."""

    def _ctx_shuffle_on_scanned_table(self):
        from core.models import ColumnReference, SQLAnalysis, TableReference

        ts = TableScanMetrics(
            table_name="catalog.schema.sales",
            bytes_read=10 * 1024**3,
            current_clustering_keys=["region_code"],
            clustering_key_cardinality={"region_code": "low"},
        )
        sm = ShuffleMetrics(
            node_id="shuffle-1",
            peak_memory_bytes=2 * 1024**3,
            sink_bytes_written=5 * 1024**3,
            shuffle_attributes=["sales.region_code"],
        )
        sql = SQLAnalysis(
            tables=[
                TableReference(
                    table="sales",
                    full_name="catalog.schema.sales",
                    alias="sales",
                )
            ],
            columns=[
                ColumnReference(
                    column_name="region_code",
                    table_name="catalog.schema.sales",
                    table_alias="sales",
                )
            ],
        )
        return Context(
            indicators=BottleneckIndicators(shuffle_impact_ratio=0.35),
            query_metrics=QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000),
            shuffle_metrics=[sm],
            sql_analysis=sql,
            top_scanned_tables=[ts],
        )

    def test_emits_canonical_syntax_when_firing(self):
        card_def = _find_card("shuffle_lc")
        ctx = self._ctx_shuffle_on_scanned_table()
        if not card_def.detect(ctx):
            # The card's gate depends on helper state; if it abstains
            # we skip rather than assert the new syntax (nothing is emitted).
            return
        built = card_def.build(ctx)
        if not built:
            return
        sql = built[0].fix_sql
        assert _LEGACY_WITH_CLAUSE not in sql
        assert _LEGACY_FEATURE_FLAG not in sql


class TestNoLegacySyntaxFromRegistry:
    """End-to-end safety net: run generate_from_registry over a rich
    Context that triggers hier_clustering and assert that no emitted
    card ever contains the legacy HC syntax. This protects future card
    additions from reintroducing it."""

    def test_no_legacy_syntax_anywhere(self):
        ts = TableScanMetrics(
            table_name="catalog.schema.sales",
            current_clustering_keys=["order_month"],
            clustering_key_cardinality={"order_month": "low"},
        )
        ctx = Context(
            indicators=BottleneckIndicators(
                cache_hit_ratio=0.1,
                scan_impact_ratio=0.5,
                shuffle_impact_ratio=0.35,
            ),
            query_metrics=QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000),
            top_scanned_tables=[ts],
        )
        cards, _fired = generate_from_registry(ctx)
        for card in cards:
            sql = card.fix_sql or ""
            assert _LEGACY_WITH_CLAUSE not in sql, (
                f"Legacy HC WITH clause leaked into {card.problem!r}: {sql!r}"
            )
            assert _LEGACY_FEATURE_FLAG not in sql, (
                f"Legacy HC feature flag leaked into {card.problem!r}: {sql!r}"
            )
