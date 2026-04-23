"""EXPLAIN EXTENDED richer-signal extraction (Phase 1).

Covers signals beyond the existing DFP / Runtime Filter / stats-freshness /
implicit-CAST-in-aggregate parsing:

- CTE references and reference counts
- ReusedExchange presence
- Join build side and broadcast mode
- Filter pushdown completeness (Data / Partition / Dictionary / Optional)
- Implicit CAST sites categorized by context (join / filter / aggregate / project)
- Aggregate phase split (Partial vs Final, partial_* / finalmerge_*)
- Photon fallback operators in the physical plan (non-Photon-prefixed)
"""

from core.explain_parser import parse_explain_extended

# --------------------------------------------------------------------------
# CTE references
# --------------------------------------------------------------------------


class TestCteReferences:
    def test_extracts_cte_reference_counts(self):
        text = """== Physical Plan ==
PhotonResultStage
+- PhotonShuffleExchangeSource
   +- CTERelationRef 16 [references: 3], true
      +- PhotonScan parquet main.base.store_sales
"""
        res = parse_explain_extended(text)
        ctes = res.cte_references
        assert any(c.cte_id == "16" and c.reference_count == 3 for c in ctes)

    def test_multiple_cte_refs(self):
        text = """== Physical Plan ==
CTERelationRef 16 [references: 2]
CTERelationRef 17 [references: 1]
CTERelationRef 18 [references: 4]
"""
        res = parse_explain_extended(text)
        ids = {c.cte_id: c.reference_count for c in res.cte_references}
        assert ids == {"16": 2, "17": 1, "18": 4}

    def test_reused_exchange_detected(self):
        text = """== Physical Plan ==
PhotonShuffledHashJoin
+- ReusedExchange [a#1, b#2], PhotonShuffleExchangeSink ...
"""
        res = parse_explain_extended(text)
        assert res.has_reused_exchange is True

    def test_no_cte_means_empty_list(self):
        text = """== Physical Plan ==
PhotonResultStage
+- PhotonScan parquet catalog.schema.t
"""
        res = parse_explain_extended(text)
        assert res.cte_references == []
        assert res.has_reused_exchange is False


# --------------------------------------------------------------------------
# Join strategies
# --------------------------------------------------------------------------


class TestJoinStrategies:
    def test_broadcast_hash_join_build_right(self):
        text = """== Physical Plan ==
PhotonBroadcastHashJoin [ss_item_sk#1], [item_sk#2], Inner, BuildRight
"""
        res = parse_explain_extended(text)
        assert len(res.join_strategies) == 1
        js = res.join_strategies[0]
        assert js.is_broadcast is True
        assert js.build_side == "Right"
        assert js.join_type == "Inner"

    def test_shuffled_hash_join_build_left(self):
        text = """== Physical Plan ==
PhotonShuffledHashJoin [a#1], [b#2], LeftOuter, BuildLeft
"""
        res = parse_explain_extended(text)
        js = res.join_strategies[0]
        assert js.is_broadcast is False
        assert js.build_side == "Left"
        assert js.join_type == "LeftOuter"

    def test_executor_broadcast_mode(self):
        text = """== Physical Plan ==
PhotonBroadcastHashJoin [a#1], [b#2], Inner, BuildRight
+- BroadcastExchange HashedRelationBroadcastMode, EXECUTOR_BROADCAST, [plan_id=42]
"""
        res = parse_explain_extended(text)
        # Either the join row itself or the broadcast exchange should carry
        # the broadcast_mode. At least one entry must mention EXECUTOR_BROADCAST.
        assert any(js.broadcast_mode == "EXECUTOR_BROADCAST" for js in res.join_strategies)


# --------------------------------------------------------------------------
# Filter pushdown
# --------------------------------------------------------------------------


class TestFilterPushdown:
    def test_data_and_partition_filters_present(self):
        text = """== Physical Plan ==
PhotonScan parquet main.base.store_sales[ss_item_sk#1,ss_sold_date_sk#2] DataFilters: [isnotnull(ss_item_sk#1)], PartitionFilters: [isnotnull(ss_sold_date_sk#2)], ReadSchema: struct<ss_item_sk:int>
"""
        res = parse_explain_extended(text)
        assert len(res.filter_pushdown) == 1
        fp = res.filter_pushdown[0]
        assert fp.has_data_filters is True
        assert fp.has_partition_filters is True
        assert fp.partition_filters_empty is False

    def test_empty_partition_filters(self):
        text = """== Physical Plan ==
PhotonScan parquet main.base.orders[o_id#1] DataFilters: [(o_id#1 > 10)], PartitionFilters: [], ReadSchema: struct<o_id:int>
"""
        res = parse_explain_extended(text)
        fp = res.filter_pushdown[0]
        assert fp.has_partition_filters is False
        assert fp.partition_filters_empty is True

    def test_optional_data_filters_detected(self):
        text = """== Physical Plan ==
PhotonScan parquet main.base.t[a#1] OptionalDataFilters: [hashedrelationcontains(a#1)], DataFilters: [], PartitionFilters: [], ReadSchema: struct<a:int>
"""
        res = parse_explain_extended(text)
        fp = res.filter_pushdown[0]
        assert fp.has_optional_filters is True


# --------------------------------------------------------------------------
# Implicit CAST sites
# --------------------------------------------------------------------------


class TestImplicitCastSites:
    def test_cast_in_filter_context(self):
        text = """== Physical Plan ==
PhotonFilter (cast(d_year#1 as decimal(38,0)) = 2000)
"""
        res = parse_explain_extended(text)
        filt_casts = [c for c in res.implicit_cast_sites if c.context == "filter"]
        assert len(filt_casts) >= 1
        assert filt_casts[0].from_type == "" or filt_casts[0].to_type == "decimal(38,0)"

    def test_cast_in_join_context(self):
        text = """== Physical Plan ==
PhotonShuffledHashJoin [cast(a#1 as bigint)], [b#2], Inner, BuildRight
"""
        res = parse_explain_extended(text)
        join_casts = [c for c in res.implicit_cast_sites if c.context == "join"]
        assert len(join_casts) >= 1

    def test_extracts_target_type(self):
        text = """== Physical Plan ==
PhotonFilter cast(x#1 as decimal(38,0))
"""
        res = parse_explain_extended(text)
        casts = res.implicit_cast_sites
        assert any("decimal" in c.to_type for c in casts)


# --------------------------------------------------------------------------
# Aggregate phase detection
# --------------------------------------------------------------------------


class TestAggregatePhases:
    def test_partial_and_final_split_detected(self):
        text = """== Physical Plan ==
PhotonGroupingAgg(keys=[k#1], functions=[finalmerge_sum(merge sum#10L)])
+- PhotonShuffleExchangeSource
   +- PhotonGroupingAgg(keys=[k#1], functions=[partial_sum(v#2L)])
"""
        res = parse_explain_extended(text)
        phases = res.aggregate_phases
        assert any(p.has_partial_functions for p in phases)
        assert any(p.has_final_merge for p in phases)

    def test_missing_partial_flagged(self):
        text = """== Physical Plan ==
PhotonGroupingAgg(keys=[k#1], functions=[sum(v#2L)])
"""
        res = parse_explain_extended(text)
        phases = res.aggregate_phases
        assert len(phases) >= 1
        assert not any(p.has_partial_functions for p in phases)
        assert not any(p.has_final_merge for p in phases)


# --------------------------------------------------------------------------
# Photon fallback operators
# --------------------------------------------------------------------------


class TestPhotonFallbackOps:
    def test_hash_aggregate_without_photon_prefix_is_fallback(self):
        text = """== Physical Plan ==
HashAggregate(keys=[k#1], functions=[pivotfirst(x#2, ...)])
+- PhotonShuffleExchangeSource
"""
        res = parse_explain_extended(text)
        fallbacks = res.photon_fallback_ops
        assert any("HashAggregate" in f.node_name for f in fallbacks)

    def test_all_photon_means_no_fallbacks(self):
        text = """== Physical Plan ==
PhotonResultStage
+- PhotonGroupingAgg(keys=[k#1], functions=[sum(v#2L)])
   +- PhotonShuffleExchangeSource
      +- PhotonScan parquet t
"""
        res = parse_explain_extended(text)
        assert res.photon_fallback_ops == []

    def test_columnartorow_not_treated_as_fallback(self):
        # ColumnarToRow is a legitimate Photon->JVM boundary, not a fallback
        text = """== Physical Plan ==
ColumnarToRow
+- PhotonScan parquet t
"""
        res = parse_explain_extended(text)
        assert res.photon_fallback_ops == []
