"""Tests for DBSQL query cost estimation."""

import pytest
from core.dbsql_cost import (
    DBU_PRICE_CLASSIC,
    DBU_PRICE_PRO,
    DBU_PRICE_SERVERLESS,
    estimate_query_cost,
    format_cost_usd,
)
from core.models import QueryMetrics
from core.warehouse_client import WarehouseInfo


def _make_warehouse(
    *,
    cluster_size: str = "Medium",
    max_num_clusters: int = 1,
    enable_serverless: bool = False,
    warehouse_type: str = "CLASSIC",
) -> WarehouseInfo:
    return WarehouseInfo(
        warehouse_id="test-wh-001",
        name="Test Warehouse",
        cluster_size=cluster_size,
        min_num_clusters=1,
        max_num_clusters=max_num_clusters,
        enable_serverless_compute=enable_serverless,
        warehouse_type=warehouse_type,
    )


def _make_query(
    *,
    execution_time_ms: int = 60_000,
    typename: str = "",
    task_total_time_ms: int = 0,
) -> QueryMetrics:
    return QueryMetrics(
        query_id="test-query-001",
        execution_time_ms=execution_time_ms,
        query_typename=typename,
        task_total_time_ms=task_total_time_ms,
    )


# ---------------------------------------------------------------------------
# estimate_query_cost — with warehouse info (unchanged behaviour)
# ---------------------------------------------------------------------------


class TestEstimateQueryCost:
    def test_returns_none_when_no_warehouse_and_no_typename(self):
        """Neither warehouse info nor query_typename → None."""
        qm = _make_query(typename="")
        result = estimate_query_cost(qm, None)
        assert result is None

    def test_serverless_cost(self):
        wh = _make_warehouse(
            cluster_size="Medium",
            enable_serverless=True,
        )
        qm = _make_query(execution_time_ms=3_600_000)  # 1 hour
        result = estimate_query_cost(qm, wh)

        assert result is not None
        assert result.billing_model == "Serverless"
        assert result.is_per_query is True
        assert result.dbu_unit_price == DBU_PRICE_SERVERLESS
        # 1 hour × 16 DBU/hour = 16 DBU
        assert result.estimated_dbu == 16.0
        # 16 DBU × $0.70 = $11.20
        assert result.estimated_cost_usd == 11.20

    def test_pro_cost(self):
        wh = _make_warehouse(
            cluster_size="Large",
            warehouse_type="PRO",
        )
        qm = _make_query(execution_time_ms=1_800_000)  # 30 minutes
        result = estimate_query_cost(qm, wh)

        assert result is not None
        assert result.billing_model == "Pro"
        assert result.is_per_query is False
        assert result.dbu_unit_price == DBU_PRICE_PRO
        # 0.5 hour × 32 DBU/hour = 16 DBU
        assert result.estimated_dbu == 16.0
        # 16 DBU × $0.55 = $8.80
        assert result.estimated_cost_usd == 8.80

    def test_classic_cost(self):
        wh = _make_warehouse(
            cluster_size="Small",
            warehouse_type="CLASSIC",
        )
        qm = _make_query(execution_time_ms=900_000)  # 15 minutes
        result = estimate_query_cost(qm, wh)

        assert result is not None
        assert result.billing_model == "Classic"
        assert result.is_per_query is False
        assert result.dbu_unit_price == DBU_PRICE_CLASSIC
        # 0.25 hour × 8 DBU/hour = 2 DBU
        assert result.estimated_dbu == 2.0
        # 2 DBU × $0.22 = $0.44
        assert result.estimated_cost_usd == 0.44

    def test_zero_execution_time(self):
        wh = _make_warehouse()
        qm = _make_query(execution_time_ms=0)
        result = estimate_query_cost(qm, wh)

        assert result is not None
        assert result.estimated_dbu == 0.0
        assert result.estimated_cost_usd == 0.0
        assert "No execution time" in result.note

    def test_multi_cluster_scales_dbu(self):
        wh = _make_warehouse(
            cluster_size="Medium",
            max_num_clusters=4,
        )
        qm = _make_query(execution_time_ms=3_600_000)  # 1 hour
        result = estimate_query_cost(qm, wh)

        assert result is not None
        # 1 hour × (16 DBU × 4 clusters) = 64 DBU
        assert result.dbu_per_hour == 64
        assert result.estimated_dbu == 64.0
        assert result.estimated_cost_usd == 64.0 * DBU_PRICE_CLASSIC

    def test_cluster_size_preserved(self):
        wh = _make_warehouse(cluster_size="2X-Large")
        qm = _make_query()
        result = estimate_query_cost(qm, wh)

        assert result is not None
        assert result.cluster_size == "2X-Large"

    def test_short_query_sub_cent_cost(self):
        """A 1-second query on a Small classic warehouse."""
        wh = _make_warehouse(cluster_size="Small", warehouse_type="CLASSIC")
        qm = _make_query(execution_time_ms=1_000)  # 1 second
        result = estimate_query_cost(qm, wh)

        assert result is not None
        # (1000 / 3_600_000) × 8 = 0.00222 DBU
        assert result.estimated_dbu == pytest.approx(0.0022, abs=0.0001)
        # 0.00222 × $0.22 = $0.000489
        assert result.estimated_cost_usd < 0.01

    def test_serverless_note_mentions_per_query(self):
        wh = _make_warehouse(enable_serverless=True)
        qm = _make_query(execution_time_ms=60_000)
        result = estimate_query_cost(qm, wh)
        assert "Per-query" in result.note

    def test_classic_note_mentions_uptime(self):
        wh = _make_warehouse(warehouse_type="CLASSIC")
        qm = _make_query(execution_time_ms=60_000)
        result = estimate_query_cost(qm, wh)
        assert "uptime" in result.note

    def test_with_warehouse_info_not_estimated_size(self):
        wh = _make_warehouse()
        qm = _make_query()
        result = estimate_query_cost(qm, wh)
        assert result.is_estimated_size is False

    def test_with_warehouse_info_no_reference_costs(self):
        """When warehouse info is available, no reference costs are generated."""
        wh = _make_warehouse()
        qm = _make_query(execution_time_ms=60_000)
        result = estimate_query_cost(qm, wh)
        assert result.reference_costs == []


# ---------------------------------------------------------------------------
# estimate_query_cost — fallback (size inferred from parallelism)
# ---------------------------------------------------------------------------


class TestEstimateQueryCostFallback:
    """When warehouse_info is unavailable we infer the likely cluster size
    from observed parallelism and price the query at that size.

    Inference uses ``1 DBU ≈ 3 vCPU`` (empirical Serverless anchor):
    implied_dbu_h = parallelism / 3, then snap to nearest T-shirt size.
    Confidence:
      - high    : parallelism >= 80 (likely saturated — estimate tracks billing)
      - medium  : 20 <= parallelism < 80
      - low     : parallelism < 20 (minimum-required-size estimate only)
    """

    def test_serverless_infers_size_and_prices_accordingly(self):
        """For 1 hour wall-clock at parallelism 144 (= 2XL saturated)
        the inferred size must be 2X-Large, priced at 144 DBU/h × $0.70."""
        qm = _make_query(
            execution_time_ms=3_600_000,  # 1 hour
            typename="LakehouseSqlQuery",
            task_total_time_ms=3_600_000 * 432,  # parallelism = 432 ≈ 2XL vCPU
        )
        result = estimate_query_cost(qm, None)

        assert result is not None
        assert result.billing_model == "Serverless"
        assert result.is_per_query is True
        assert result.is_estimated_size is True
        assert result.parallelism_ratio == 432.0
        # implied DBU/h = 432/3 = 144 → nearest size = 2X-Large (144 DBU/h)
        assert "2X-Large" in result.cluster_size
        assert result.dbu_per_hour == 144
        assert result.estimated_dbu == pytest.approx(144.0, abs=0.1)
        assert result.estimated_cost_usd == pytest.approx(144.0 * DBU_PRICE_SERVERLESS, abs=0.1)

    def test_low_parallelism_infers_small_with_low_confidence(self):
        """parallelism=10 → implied DBU/h ≈ 3 → nearest ~2X-Small, low conf."""
        qm = _make_query(
            execution_time_ms=3_600_000,
            typename="LakehouseSqlQuery",
            task_total_time_ms=3_600_000 * 10,
        )
        result = estimate_query_cost(qm, None)
        assert result is not None
        assert result.parallelism_ratio == 10.0
        assert "low" in result.cluster_size  # confidence tag surfaces in label
        assert result.dbu_per_hour <= 24  # Medium or smaller

    def test_classic_pricing_and_size_inference(self):
        qm = _make_query(
            execution_time_ms=1_800_000,
            typename="SqlQuery",
            task_total_time_ms=1_800_000 * 120,  # parallelism = 120
        )
        result = estimate_query_cost(qm, None)
        assert result is not None
        assert result.billing_model == "Classic"
        assert result.parallelism_ratio == 120.0
        # implied DBU/h = 120/3 = 40 → Large (40)
        assert "Large" in result.cluster_size
        assert result.dbu_per_hour == 40

    def test_zero_task_total_defaults_to_medium_low_conf(self):
        qm = _make_query(
            execution_time_ms=3_600_000,
            typename="LakehouseSqlQuery",
            task_total_time_ms=0,
        )
        result = estimate_query_cost(qm, None)
        assert result is not None
        assert result.parallelism_ratio == 0.0
        assert result.dbu_per_hour == 24  # Default Medium when no signal

    def test_reference_table_covers_every_size(self):
        qm = _make_query(
            execution_time_ms=3_600_000,
            typename="LakehouseSqlQuery",
            task_total_time_ms=3_600_000 * 100,
        )
        result = estimate_query_cost(qm, None)
        assert result is not None
        sizes = [r.cluster_size for r in result.reference_costs]
        assert sizes == [
            "2X-Small",
            "X-Small",
            "Small",
            "Medium",
            "Large",
            "X-Large",
            "2X-Large",
            "3X-Large",
            "4X-Large",
            "5X-Large",
        ]
        two_xl = next(r for r in result.reference_costs if r.cluster_size == "2X-Large")
        assert two_xl.estimated_cost_usd == pytest.approx(144.0 * DBU_PRICE_SERVERLESS, abs=0.01)

    def test_zero_execution_produces_empty_reference(self):
        qm = _make_query(
            execution_time_ms=0,
            typename="LakehouseSqlQuery",
            task_total_time_ms=0,
        )
        result = estimate_query_cost(qm, None)
        assert result is not None
        assert result.reference_costs == []

    def test_note_mentions_inferred_size_and_confidence(self):
        qm = _make_query(
            execution_time_ms=60_000,
            typename="LakehouseSqlQuery",
            task_total_time_ms=60_000 * 240,  # parallelism = 240, high conf
        )
        result = estimate_query_cost(qm, None)
        lower = result.note.lower()
        assert "inferred" in lower
        assert "parallelism" in lower
        assert "confidence" in lower

    def test_low_parallelism_note_flags_minimum_required(self):
        qm = _make_query(
            execution_time_ms=60_000,
            typename="LakehouseSqlQuery",
            task_total_time_ms=60_000 * 10,  # parallelism=10, low conf
        )
        result = estimate_query_cost(qm, None)
        lower = result.note.lower()
        assert "minimum-required" in lower
        assert "over-provisioned" in lower

    def test_no_typename_returns_none(self):
        qm = _make_query(execution_time_ms=60_000, typename="")
        result = estimate_query_cost(qm, None)
        assert result is None

    def test_abi_san_2xl_regression_is_within_20_percent_of_actual_billing(self):
        """Regression for the user-reported `$93.97 (4XL仮定)` bug on the
        saturated 2XL profile (parallelism ≈ 452). The size-inferred
        estimate must land within ±20% of actual 2XL billing."""
        qm = _make_query(
            execution_time_ms=915_319,
            typename="LakehouseSqlQuery",
            task_total_time_ms=414_132_966,
        )
        result = estimate_query_cost(qm, None)
        actual_2xl_billing = (915_319 / 3_600_000) * 144 * DBU_PRICE_SERVERLESS
        ratio = result.estimated_cost_usd / actual_2xl_billing
        assert 0.8 <= ratio <= 1.2, (
            f"Inferred estimate {result.estimated_cost_usd:.2f} is outside "
            f"±20% of actual 2XL billing {actual_2xl_billing:.2f} (ratio={ratio:.2f})"
        )
        # Must identify 2X-Large (not 4XL as the old code did)
        assert "2X-Large" in result.cluster_size
        assert "4X-Large" not in result.cluster_size


# ---------------------------------------------------------------------------
# format_cost_usd
# ---------------------------------------------------------------------------


class TestFormatCostUsd:
    def test_zero(self):
        assert format_cost_usd(0) == "$0.000"

    def test_normal(self):
        assert format_cost_usd(2.80) == "$2.800"

    def test_sub_cent(self):
        assert format_cost_usd(0.0012) == "$0.001"

    def test_exactly_one_cent(self):
        assert format_cost_usd(0.01) == "$0.010"

    def test_large_cost(self):
        assert format_cost_usd(123.456) == "$123.456"
