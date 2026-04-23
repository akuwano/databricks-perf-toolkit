"""Lakehouse Federation detection + suppression (v5.18.0).

Pipeline expectations:
  1. ``extract_node_metrics`` flags each ``ROW_DATA_SOURCE_SCAN_EXEC``
     node as ``is_federation_scan=True``.
  2. ``populate_federation_signals`` aggregates node-level flags onto
     the query and surfaces ``is_federation_query``, ``federation_tables``,
     and a best-effort ``federation_source_type``.
  3. ``generate_from_registry`` suppresses cards that give Delta-only
     advice (LC / file pruning / stats-fresh / Photon blocker etc.)
     when the query is federated.
  4. ``federation_query`` card fires with concrete pushdown /
     fetchSize / materialize advice.

The real-world regression profile lives at
``json/query-profile_01f13e1f-0d0c-1f8d-b5c1-5ef745e96241.json``
(BigQuery federation, `pococha_bq_prod` catalog). The tests load it
directly so future format changes surface here.
"""

from __future__ import annotations

import json
from pathlib import Path

from core.analyzers import analyze_from_dict
from core.analyzers.recommendations_registry import (
    _FEDERATION_SUPPRESSED_CARDS,
    CARDS,
    Context,
    generate_from_registry,
)
from core.extractors import (
    _guess_federation_source_type,
    extract_node_metrics,
    populate_federation_signals,
)
from core.models import BottleneckIndicators, NodeMetrics, QueryMetrics

_REPO_ROOT = Path(__file__).resolve().parents[3]
_BQ_FEDERATION_PROFILE = (
    _REPO_ROOT / "json" / "query-profile_01f13e1f-0d0c-1f8d-b5c1-5ef745e96241.json"
)


def _fed_scan_node(table: str) -> NodeMetrics:
    return NodeMetrics(
        node_id="7",
        node_name=f"Row Data Source Scan {table}",
        node_tag="ROW_DATA_SOURCE_SCAN_EXEC",
        duration_ms=5000,
        is_federation_scan=True,
    )


def _delta_scan_node(table: str) -> NodeMetrics:
    return NodeMetrics(
        node_id="3",
        node_name=f"Scan {table}",
        node_tag="UNKNOWN_DATA_SOURCE_SCAN_EXEC",
        duration_ms=5000,
    )


# ---------------------------------------------------------------------------
# Extractor-level detection
# ---------------------------------------------------------------------------


class TestNodeLevelFederationDetection:
    def test_row_data_source_scan_flagged(self):
        """``ROW_DATA_SOURCE_SCAN_EXEC`` node tag must set the flag."""
        data = {
            "graphs": [
                {
                    "nodes": [
                        {
                            "id": "7",
                            "name": "Row Data Source Scan pococha_bq_prod.source.tbl",
                            "tag": "ROW_DATA_SOURCE_SCAN_EXEC",
                            "hidden": False,
                            "keyMetrics": {},
                            "metrics": [],
                            "metadata": [],
                        }
                    ]
                }
            ]
        }
        nodes = extract_node_metrics(data)
        assert len(nodes) == 1
        assert nodes[0].is_federation_scan is True

    def test_delta_scan_not_flagged(self):
        """Regular Delta scan nodes must NOT be flagged."""
        data = {
            "graphs": [
                {
                    "nodes": [
                        {
                            "id": "3",
                            "name": "Scan main.public.orders",
                            "tag": "UNKNOWN_DATA_SOURCE_SCAN_EXEC",
                            "hidden": False,
                            "keyMetrics": {},
                            "metrics": [],
                            "metadata": [],
                        }
                    ]
                }
            ]
        }
        nodes = extract_node_metrics(data)
        assert nodes[0].is_federation_scan is False


class TestPopulateFederationSignals:
    def test_aggregates_tables_and_flag(self):
        qm = QueryMetrics()
        nodes = [
            _fed_scan_node("pococha_bq_prod.source.db_reincarnation_device_histories"),
        ]
        populate_federation_signals(qm, nodes)
        assert qm.is_federation_query is True
        assert qm.federation_tables == ["pococha_bq_prod.source.db_reincarnation_device_histories"]

    def test_source_type_bigquery_heuristic(self):
        qm = QueryMetrics()
        nodes = [_fed_scan_node("pococha_bq_prod.source.db_foo")]
        populate_federation_signals(qm, nodes)
        assert qm.federation_source_type == "bigquery"

    def test_source_type_unknown_when_heuristic_fails(self):
        qm = QueryMetrics()
        nodes = [_fed_scan_node("my_catalog.schema.table")]
        populate_federation_signals(qm, nodes)
        assert qm.is_federation_query is True
        assert qm.federation_source_type == ""

    def test_no_federation_leaves_flag_false(self):
        qm = QueryMetrics()
        nodes = [_delta_scan_node("main.public.orders")]
        populate_federation_signals(qm, nodes)
        assert qm.is_federation_query is False
        assert qm.federation_tables == []
        assert qm.federation_source_type == ""

    def test_mixed_catalogs_do_not_override_source(self):
        qm = QueryMetrics()
        nodes = [
            _fed_scan_node("pococha_bq_prod.src.a"),
            _fed_scan_node("sf_analytics.public.b"),
        ]
        populate_federation_signals(qm, nodes)
        # Two different sources → leave empty and rely on per-table display
        assert qm.is_federation_query is True
        assert qm.federation_source_type == ""


class TestSourceTypeHeuristic:
    def test_bigquery_catalog(self):
        assert _guess_federation_source_type("pococha_bq_prod.src.t") == "bigquery"

    def test_snowflake_catalog(self):
        assert _guess_federation_source_type("sf_analytics.public.orders") == "snowflake"

    def test_postgres_catalog(self):
        assert _guess_federation_source_type("pg_prod.public.users") == "postgresql"

    def test_mysql_catalog(self):
        assert _guess_federation_source_type("mysql_shard.inv.items") == "mysql"

    def test_redshift_catalog(self):
        assert _guess_federation_source_type("redshift_ds.public.fact") == "redshift"

    def test_unknown_catalog(self):
        assert _guess_federation_source_type("main.analytics.orders") == ""

    def test_avoid_substring_false_positives(self):
        # "mysqldump" / "labour" etc. must not match
        assert _guess_federation_source_type("mysqldump.schema.t") == ""
        assert _guess_federation_source_type("pgsrc_legacy.schema.t") == ""


# ---------------------------------------------------------------------------
# Registry suppression
# ---------------------------------------------------------------------------


def _fed_ctx() -> Context:
    qm = QueryMetrics(
        total_time_ms=60_000,
        task_total_time_ms=60_000,
        is_federation_query=True,
        federation_source_type="bigquery",
        federation_tables=["pococha_bq_prod.source.tbl"],
    )
    return Context(
        indicators=BottleneckIndicators(
            cache_hit_ratio=0.1,
            scan_impact_ratio=0.5,
            filter_rate=0.1,
            has_data_skew=False,
        ),
        query_metrics=qm,
    )


class TestFederationCardFires:
    def test_federation_query_card_registered(self):
        card_def = next(c for c in CARDS if c.card_id == "federation_query")
        assert card_def.priority_rank == 97

    def test_federation_query_detect_true(self):
        card_def = next(c for c in CARDS if c.card_id == "federation_query")
        assert card_def.detect(_fed_ctx())

    def test_federation_query_detect_false_on_delta(self):
        card_def = next(c for c in CARDS if c.card_id == "federation_query")
        qm = QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000)
        ctx = Context(indicators=BottleneckIndicators(), query_metrics=qm)
        assert not card_def.detect(ctx)

    def test_fix_sql_mentions_pushdown_and_fetchsize(self):
        card_def = next(c for c in CARDS if c.card_id == "federation_query")
        built = card_def.build(_fed_ctx())
        assert built
        sql = built[0].fix_sql
        assert "EXPLAIN FORMATTED" in sql
        assert "fetchSize" in sql
        assert "CREATE TABLE" in sql  # materialize-to-Delta suggestion

    def test_fix_text_mentions_concrete_levers(self):
        card_def = next(c for c in CARDS if c.card_id == "federation_query")
        built = card_def.build(_fed_ctx())
        fix = built[0].fix.lower()
        for kw in ("pushdown", "fetchsize", "materialization"):
            assert kw in fix, f"fix text missing {kw!r}: {fix}"


class TestSuppressMisleadingCards:
    """When ``is_federation_query`` is True, Delta-only cards must not
    appear in the registry output — but non-federated queries are
    untouched."""

    def test_expected_suppressed_set(self):
        """Pin the explicit suppression set so future additions are
        deliberate."""
        assert "low_file_pruning" in _FEDERATION_SUPPRESSED_CARDS
        assert "low_cache" in _FEDERATION_SUPPRESSED_CARDS
        assert "hier_clustering" in _FEDERATION_SUPPRESSED_CARDS
        assert "stats_fresh" in _FEDERATION_SUPPRESSED_CARDS
        assert "rescheduled_scan" in _FEDERATION_SUPPRESSED_CARDS
        assert "photon_blocker" in _FEDERATION_SUPPRESSED_CARDS
        assert "scan_hot" in _FEDERATION_SUPPRESSED_CARDS
        assert "shuffle_lc" in _FEDERATION_SUPPRESSED_CARDS

    def test_suppressed_cards_do_not_fire_under_federation(self):
        """Inject conditions that would normally fire ``low_cache`` and
        ``low_file_pruning`` but mark the query federated — they must
        be suppressed."""
        _, fired = generate_from_registry(_fed_ctx())
        for cid in _FEDERATION_SUPPRESSED_CARDS:
            assert cid not in fired, f"{cid} leaked into federation output"

    def test_federation_query_card_wins(self):
        """The federation card must be in the fired set."""
        _, fired = generate_from_registry(_fed_ctx())
        assert "federation_query" in fired

    def test_non_federated_query_unaffected(self):
        """Same conditions, no federation flag → suppression does NOT
        apply; ``low_cache`` / ``low_file_pruning`` can still fire."""
        qm = QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000)
        ctx = Context(
            indicators=BottleneckIndicators(
                cache_hit_ratio=0.1,
                scan_impact_ratio=0.5,
                filter_rate=0.1,
            ),
            query_metrics=qm,
        )
        _, fired = generate_from_registry(ctx)
        assert "federation_query" not in fired
        # At least one of the suppressed cards should fire when non-federated
        assert fired & {"low_cache", "low_file_pruning"}


# ---------------------------------------------------------------------------
# End-to-end on the real-world BigQuery profile
# ---------------------------------------------------------------------------


class TestRealWorldBigQueryProfile:
    """Smoke test against the user-reported BigQuery federation profile.

    Skipped silently if the fixture is missing (e.g. when running in a
    sandboxed checkout without ``json/``)."""

    def test_profile_flags_federation_and_source(self):
        if not _BQ_FEDERATION_PROFILE.exists():
            return  # fixture not present — skip
        with _BQ_FEDERATION_PROFILE.open() as f:
            data = json.load(f)
        analysis = analyze_from_dict(data)
        qm = analysis.query_metrics
        assert qm.is_federation_query is True
        assert qm.federation_source_type == "bigquery"
        assert qm.federation_tables, "Expected at least one federated table"
        # No LC / stats-fresh noise in the emitted cards
        card_ids = {c.root_cause_group for c in (analysis.action_cards or []) if c.root_cause_group}
        assert "federation" in card_ids or any(
            c.problem and "Lakehouse Federation" in c.problem for c in (analysis.action_cards or [])
        ), "Expected a federation_query card in the output"
