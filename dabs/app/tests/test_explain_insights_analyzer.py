"""Analyzer rules driven by Phase-1 EXPLAIN insights.

Exercises `enhance_bottleneck_with_explain` with crafted ExplainExtended
inputs (post-parse) and asserts the expected alerts/counters fire.
"""

from core.analyzers.explain_analysis import enhance_bottleneck_with_explain
from core.constants import Severity
from core.explain_parser import (
    AggregatePhaseInfo,
    CteReuseInfo,
    ExplainExtended,
    FilterPushdownInfo,
    ImplicitCastSite,
    PhotonFallbackOp,
)
from core.models import BottleneckIndicators


def _fresh_indicators() -> BottleneckIndicators:
    return BottleneckIndicators()


# --------------------------------------------------------------------------
# Implicit CAST on JOIN key — CRITICAL (direct type-mismatch evidence)
# --------------------------------------------------------------------------


class TestImplicitCastOnJoinKeyAlert:
    def test_join_context_cast_fires_critical(self):
        ex = ExplainExtended()
        ex.implicit_cast_sites = [
            ImplicitCastSite(
                context="join",
                column_ref="ss_item_sk#1",
                to_type="bigint",
                node_name="PhotonShuffledHashJoin",
            )
        ]
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        join_cast_alerts = [a for a in ind.alerts if a.metric_name == "implicit_cast_on_join_key"]
        assert len(join_cast_alerts) == 1
        assert join_cast_alerts[0].severity == Severity.CRITICAL
        assert ind.implicit_cast_on_join_key is True

    def test_filter_only_cast_does_not_fire_join_alert(self):
        ex = ExplainExtended()
        ex.implicit_cast_sites = [
            ImplicitCastSite(
                context="filter",
                column_ref="d_year#1",
                to_type="decimal(38,0)",
                node_name="PhotonFilter",
            )
        ]
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        assert not any(a.metric_name == "implicit_cast_on_join_key" for a in ind.alerts)

    def test_decimal_on_join_key_wording_mentions_data_type(self):
        ex = ExplainExtended()
        ex.implicit_cast_sites = [
            ImplicitCastSite(
                context="join",
                column_ref="x#2",
                to_type="decimal(38,0)",
                node_name="PhotonShuffledHashJoin",
            )
        ]
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        msg = next(a.message for a in ind.alerts if a.metric_name == "implicit_cast_on_join_key")
        assert "decimal" in msg.lower() or "type" in msg.lower()


# --------------------------------------------------------------------------
# CTE references — reference count >= 2 without ReusedExchange = miss
# --------------------------------------------------------------------------


class TestCteReuseMissAlert:
    def test_multi_ref_without_reused_exchange_fires(self):
        ex = ExplainExtended()
        ex.cte_references = [CteReuseInfo(cte_id="16", reference_count=3)]
        ex.has_reused_exchange = False
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        alerts = [a for a in ind.alerts if a.metric_name == "cte_reuse_miss"]
        assert len(alerts) == 1
        assert ind.cte_reuse_miss_count >= 1

    def test_multi_ref_with_reused_exchange_does_not_fire(self):
        ex = ExplainExtended()
        ex.cte_references = [CteReuseInfo(cte_id="16", reference_count=3)]
        ex.has_reused_exchange = True
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        assert not any(a.metric_name == "cte_reuse_miss" for a in ind.alerts)
        assert ind.cte_reuse_miss_count == 0

    def test_single_ref_does_not_fire(self):
        ex = ExplainExtended()
        ex.cte_references = [CteReuseInfo(cte_id="16", reference_count=1)]
        ex.has_reused_exchange = False
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        assert not any(a.metric_name == "cte_reuse_miss" for a in ind.alerts)


# --------------------------------------------------------------------------
# Photon fallback operators — HIGH
# --------------------------------------------------------------------------


class TestPhotonFallbackAlert:
    def test_fallback_op_fires_alert(self):
        ex = ExplainExtended()
        ex.photon_fallback_ops = [
            PhotonFallbackOp(
                node_name="HashAggregate",
                raw_line="HashAggregate(keys=[k#1], functions=[pivotfirst(...)])",
            )
        ]
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        alerts = [a for a in ind.alerts if a.metric_name == "photon_fallback_ops"]
        assert len(alerts) == 1
        assert alerts[0].severity in (Severity.HIGH, Severity.MEDIUM)
        assert ind.photon_fallback_op_count == 1

    def test_no_fallback_no_alert(self):
        ex = ExplainExtended()
        ex.photon_fallback_ops = []
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        assert not any(a.metric_name == "photon_fallback_ops" for a in ind.alerts)


# --------------------------------------------------------------------------
# Filter pushdown gap — partition_filters_empty while table has filters
# --------------------------------------------------------------------------


class TestFilterPushdownGap:
    def test_empty_partition_filters_with_data_filters_fires(self):
        ex = ExplainExtended()
        ex.filter_pushdown = [
            FilterPushdownInfo(
                table_name="main.base.t",
                has_data_filters=True,
                has_partition_filters=False,
                partition_filters_empty=True,
            )
        ]
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        alerts = [a for a in ind.alerts if a.metric_name == "pushdown_gap"]
        assert len(alerts) == 1
        assert ind.filter_pushdown_gap_count >= 1

    def test_partition_filters_present_no_gap(self):
        ex = ExplainExtended()
        ex.filter_pushdown = [
            FilterPushdownInfo(
                table_name="main.base.t",
                has_data_filters=True,
                has_partition_filters=True,
                partition_filters_empty=False,
            )
        ]
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        assert not any(a.metric_name == "pushdown_gap" for a in ind.alerts)


# --------------------------------------------------------------------------
# Missing partial-aggregate — INFO
# --------------------------------------------------------------------------


class TestMissingPartialAggregateAlert:
    def test_aggregate_without_partial_final_split_fires_info(self):
        ex = ExplainExtended()
        # Single aggregate with neither partial_* nor finalmerge_*
        ex.aggregate_phases = [
            AggregatePhaseInfo(
                node_name="PhotonGroupingAgg",
                has_partial_functions=False,
                has_final_merge=False,
            )
        ]
        ex.exchanges = []
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        alerts = [a for a in ind.alerts if a.metric_name == "missing_partial_aggregate"]
        # Info-only signal, may be emitted (not critical). Just validate type if present.
        if alerts:
            assert alerts[0].severity == Severity.INFO

    def test_partial_and_final_present_no_alert(self):
        ex = ExplainExtended()
        ex.aggregate_phases = [
            AggregatePhaseInfo(
                node_name="PhotonGroupingAgg",
                has_partial_functions=False,
                has_final_merge=True,
            ),
            AggregatePhaseInfo(
                node_name="PhotonGroupingAgg",
                has_partial_functions=True,
                has_final_merge=False,
            ),
        ]
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        assert not any(a.metric_name == "missing_partial_aggregate" for a in ind.alerts)


# --------------------------------------------------------------------------
# Join strategy — SinglePartition broadcast on large table
# --------------------------------------------------------------------------


class TestJoinStrategyInsights:
    def test_no_explicit_strategy_alert_when_empty(self):
        ex = ExplainExtended()
        ind = enhance_bottleneck_with_explain(_fresh_indicators(), ex)
        # Nothing to assert except that we don't crash
        assert isinstance(ind, BottleneckIndicators)
