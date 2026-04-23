"""3-stage LLM pipeline signal leakage regression tests.

Historic bug: EXPLAIN-derived signals (clustering keys, column types,
build sides, CTE references, AQE state, partition counts, non-JOIN CAST,
Photon fallback operator names, reference_nodes, table format) only
reached Stage 1 (analyze) and were silently dropped from Stage 2
(review) and Stage 3 (refine). The user-facing report comes from
Stage 3, so everything missing there disappeared from recommendations.

These tests build a single rich ProfileAnalysis with every Phase-1
signal populated and then verify each signal survives into all three
stage prompts.
"""

from core.explain_parser import (
    AggregatePhaseInfo,
    CteReuseInfo,
    ExplainExtended,
    FilterPushdownInfo,
    ImplicitCastSite,
    JoinStrategy,
    OptimizerStatistics,
    PhotonExplanation,
    PhotonFallbackOp,
    PhotonUnsupportedItem,
    RelationInfo,
)
from core.llm_prompts.prompts import (
    create_refine_prompt,
    create_review_prompt,
    create_structured_analysis_prompt,
)
from core.models import ProfileAnalysis, QueryMetrics, TableScanMetrics


def _rich_analysis() -> ProfileAnalysis:
    """Build an analysis where every Phase-1 signal is populated."""
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics(
        query_text="SELECT * FROM ss JOIN c",
        read_bytes=500_000_000,
    )
    a.top_scanned_tables = [
        TableScanMetrics(
            table_name="skato.aisin_poc.store_sales_delta_lc",
            bytes_read=500_000_000,
            current_clustering_keys=["SS_SOLD_DATE_SK", "SS_ITEM_SK"],
            clustering_key_cardinality={
                "SS_SOLD_DATE_SK": "high",
                "SS_ITEM_SK": "high",
            },
        ),
        TableScanMetrics(
            table_name="skato.aisin_poc.plain_no_cluster",
            bytes_read=10_000_000,
            current_clustering_keys=[],
            clustering_key_cardinality={},
        ),
    ]
    ex = ExplainExtended()
    ex.scan_schemas = {
        "skato.aisin_poc.store_sales_delta_lc": {
            "SS_CUSTOMER_SK": "decimal(38,0)",
            "SS_QUANTITY": "int",
            "SS_SOLD_DATE_SK": "decimal(38,0)",
        },
        "skato.aisin_poc.plain_no_cluster": {"id": "bigint"},
    }
    ex.relations = [
        RelationInfo(
            table_name="skato.aisin_poc.store_sales_delta_lc",
            columns=["SS_CUSTOMER_SK", "SS_QUANTITY"],
            format="delta",
        ),
        RelationInfo(
            table_name="skato.aisin_poc.plain_no_cluster",
            columns=["id"],
            format="parquet",
        ),
    ]
    ex.cte_references = [
        CteReuseInfo(cte_id="16", reference_count=3),
        CteReuseInfo(cte_id="17", reference_count=2),
    ]
    ex.has_reused_exchange = False
    ex.join_strategies = [
        JoinStrategy(
            node_name="PhotonShuffledHashJoin",
            join_type="Inner",
            build_side="Right",
            is_broadcast=False,
        ),
        JoinStrategy(
            node_name="PhotonShuffledHashJoin",
            join_type="Inner",
            build_side="Left",
            is_broadcast=False,
        ),
    ]
    ex.filter_pushdown = [
        FilterPushdownInfo(
            table_name="skato.aisin_poc.store_sales_delta_lc",
            has_data_filters=True,
            has_partition_filters=False,
            partition_filters_empty=True,
        )
    ]
    ex.implicit_cast_sites = [
        ImplicitCastSite(
            context="join",
            column_ref="ss_customer_sk#8",
            to_type="bigint",
            node_name="PhotonShuffledHashJoin",
        ),
        ImplicitCastSite(
            context="filter",
            column_ref="d_year#13048",
            to_type="decimal(38,0)",
            node_name="PhotonFilter",
        ),
    ]
    ex.aggregate_phases = [
        AggregatePhaseInfo(node_name="PhotonGroupingAgg", has_partial_functions=True),
        AggregatePhaseInfo(node_name="PhotonGroupingAgg", has_final_merge=True),
    ]
    ex.photon_fallback_ops = [
        PhotonFallbackOp(
            node_name="HashAggregate",
            raw_line="HashAggregate(keys=[k#1], functions=[pivotfirst(...)])",
        )
    ]
    ex.is_adaptive = True
    ex.is_final_plan = False
    ex.exchanges = []  # partitioning count covered in join_strategies test
    ex.photon_explanation = PhotonExplanation(
        fully_supported=False,
        unsupported_items=[PhotonUnsupportedItem(expression="pivotfirst(x,...)")],
        reference_nodes=["HashAggregate(keys=[k#1], functions=[pivotfirst(...)])"],
    )
    ex.optimizer_statistics = OptimizerStatistics(full_tables=["ss", "c"])
    a.explain_analysis = ex
    return a


def _stage_prompts(a: ProfileAnalysis) -> dict[str, str]:
    """Return prompts produced by each of the 3 LLM stages."""
    # Stage 1: analyze
    s1 = create_structured_analysis_prompt(a, lang="en")
    # Stage 2: review (takes an analysis + llm output)
    s2 = create_review_prompt(a, "some prior analysis output", "primary-model", "en")
    # Stage 3: refine (takes analysis + initial + review outputs)
    s3 = create_refine_prompt(
        "initial analysis",
        "review output",
        "primary-model",
        "review-model",
        "en",
        analysis=a,
    )
    return {"stage1": s1, "stage2": s2, "stage3": s3}


# --------------------------------------------------------------------------
# Clustering keys
# --------------------------------------------------------------------------


class TestClusteringKeysReachAllStages:
    def test_clustering_key_name_present(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "SS_SOLD_DATE_SK" in prompt, f"SS_SOLD_DATE_SK missing from {name} prompt"

    def test_cardinality_label_present(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "high-card" in prompt or "high" in prompt, (
                f"cardinality info missing from {name} prompt"
            )

    def test_none_configured_for_empty_clustering_table(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "plain_no_cluster" in prompt, f"table name missing from {name} prompt"
            assert "none configured" in prompt.lower(), (
                f"'none configured' missing from {name} prompt"
            )


# --------------------------------------------------------------------------
# Column types (from ReadSchema)
# --------------------------------------------------------------------------


class TestColumnTypesReachAllStages:
    def test_decimal_type_visible(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "decimal(38,0)" in prompt, f"decimal(38,0) missing from {name} prompt"

    def test_join_key_column_name_visible(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "SS_CUSTOMER_SK" in prompt, f"SS_CUSTOMER_SK missing from {name} prompt"


# --------------------------------------------------------------------------
# Table format (delta / parquet) — drives LC applicability
# --------------------------------------------------------------------------


class TestTableFormatReachAllStages:
    def test_format_label_visible(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            # Either "delta" next to the lc table or the plain parquet one
            lower = prompt.lower()
            assert "delta" in lower or "parquet" in lower, (
                f"table format missing from {name} prompt"
            )


# --------------------------------------------------------------------------
# CTE references
# --------------------------------------------------------------------------


class TestCteReferencesReachAllStages:
    def test_cte_ref_id_and_count_visible(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            # The CTE reuse miss helper renders "#16×3" or similar; accept the
            # count and the id in any format
            assert "16" in prompt, f"CTE id 16 missing from {name} prompt"
            assert "CTE" in prompt or "cte" in prompt, f"CTE mention missing from {name} prompt"


# --------------------------------------------------------------------------
# Photon fallback operator names
# --------------------------------------------------------------------------


class TestPhotonFallbackReachAllStages:
    def test_fallback_op_name_visible(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "HashAggregate" in prompt, (
                f"HashAggregate (non-Photon op) missing from {name} prompt"
            )


# --------------------------------------------------------------------------
# AQE state — isFinalPlan context
# --------------------------------------------------------------------------


class TestAqeStateReachAllStages:
    def test_aqe_state_mentioned(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "AQE" in prompt, f"AQE state missing from {name} prompt"


# --------------------------------------------------------------------------
# Non-JOIN implicit CAST — filter-context
# --------------------------------------------------------------------------


class TestNonJoinCastReachAllStages:
    def test_filter_cast_surfaced(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            # Either explicit "filter" context mention or d_year column ref
            lower = prompt.lower()
            assert "filter" in lower and "cast" in lower, (
                f"non-JOIN CAST missing from {name} prompt"
            )


# --------------------------------------------------------------------------
# Photon reference nodes (operator location hints)
# --------------------------------------------------------------------------


class TestPhotonReferenceNodesReachAllStages:
    def test_reference_node_detail_visible(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "Reference" in prompt or "reference" in prompt, (
                f"Photon reference_node missing from {name} prompt"
            )


# --------------------------------------------------------------------------
# Join build side
# --------------------------------------------------------------------------


class TestJoinBuildSideReachAllStages:
    def test_build_side_visible(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            lower = prompt.lower()
            assert "build" in lower and ("right" in lower or "left" in lower), (
                f"Build side missing from {name} prompt"
            )


# --------------------------------------------------------------------------
# Pushdown gap — already an alert, but table name should also propagate
# --------------------------------------------------------------------------


class TestPushdownGapReachAllStages:
    def test_pushdown_gap_table_name_visible(self):
        prompts = _stage_prompts(_rich_analysis())
        for name, prompt in prompts.items():
            assert "store_sales_delta_lc" in prompt, (
                f"pushdown-gap table name missing from {name} prompt"
            )
