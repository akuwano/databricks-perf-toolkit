"""Tests for core.dbu_pricing — instance type parsing and DBU cost estimation."""

import pytest
from core.dbu_pricing import (
    compute_price_per_hour,
    dbu_rate_per_hour,
    estimate_dbu_cost,
    parse_instance_type,
)


class TestParseInstanceType:
    def test_aws_i3_xlarge(self):
        spec = parse_instance_type("i3.xlarge")
        assert spec.cloud == "aws"
        assert spec.family == "i3"
        assert spec.vcpus == 4
        assert spec.category == "storage"

    def test_aws_m5_2xlarge(self):
        spec = parse_instance_type("m5.2xlarge")
        assert spec.cloud == "aws"
        assert spec.vcpus == 8
        assert spec.category == "general"

    def test_aws_c5_4xlarge(self):
        spec = parse_instance_type("c5.4xlarge")
        assert spec.vcpus == 16
        assert spec.category == "compute"

    def test_aws_r5d_xlarge(self):
        spec = parse_instance_type("r5d.xlarge")
        assert spec.vcpus == 4
        assert spec.category == "memory"

    def test_gcp_n2_standard_4(self):
        spec = parse_instance_type("n2-standard-4")
        assert spec.cloud == "gcp"
        assert spec.vcpus == 4

    def test_unknown_returns_fallback(self):
        spec = parse_instance_type("custom_unknown_type")
        assert spec.cloud == "unknown"
        assert spec.vcpus == 4
        assert spec.category == "unknown"

    def test_empty_string(self):
        spec = parse_instance_type("")
        assert spec.vcpus == 4
        assert spec.category == "unknown"


class TestDbuRatePerHour:
    def test_known_instance_lookup(self):
        rate, method = dbu_rate_per_hour("i3.xlarge")
        assert rate > 0
        assert method == "lookup"

    def test_unknown_size_uses_formula(self):
        rate, method = dbu_rate_per_hour("m5.24xlarge")
        assert rate > 0
        assert method == "formula"

    def test_unparseable_uses_fallback(self):
        rate, method = dbu_rate_per_hour("???")
        assert rate > 0
        assert method == "fallback"

    def test_photon_doubles_rate(self):
        rate_base, _ = dbu_rate_per_hour("i3.xlarge", photon=False)
        rate_photon, _ = dbu_rate_per_hour("i3.xlarge", photon=True)
        assert rate_photon == pytest.approx(rate_base * 2.0)


class TestEstimateDBUCost:
    def test_fixed_cluster_60min(self):
        result = estimate_dbu_cost(
            worker_node_type="i3.xlarge",
            driver_node_type="i3.xlarge",
            duration_min=60.0,
            autoscale_cost=[],
            min_workers=4,
            max_workers=4,
            photon_enabled=False,
        )
        assert result["estimated_total_dbu"] > 0
        assert result["driver_dbu"] > 0
        assert result["worker_dbu"] > 0
        assert result["photon_multiplier"] == 1.0
        assert "pricing_note" in result

    def test_autoscale_cluster(self):
        result = estimate_dbu_cost(
            worker_node_type="m5.2xlarge",
            driver_node_type="m5.xlarge",
            duration_min=30.0,
            autoscale_cost=[
                {"worker_count": 2, "cumulative_min": 12.0, "pct_of_total": 40.0},
                {"worker_count": 8, "cumulative_min": 18.0, "pct_of_total": 60.0},
            ],
            min_workers=2,
            max_workers=10,
            photon_enabled=False,
        )
        assert result["estimated_total_dbu"] > 0
        assert result["worker_dbu"] > result["driver_dbu"]

    def test_fixed_cluster_ignores_incomplete_autoscale(self):
        """When min==max, autoscale_cost should be ignored entirely.

        Regression test for the case where a fixed-size cluster had an
        autoscale timeline containing only ramp-up events (e.g., 1.2s of
        events capturing wc=1..10 transitions) with no steady-state
        record. Using that timeline would underestimate worker_dbu by
        ~99%. The fix: for min==max, always use constant worker count.
        """
        # 11.4 min app, 10 fixed workers, but autoscale_cost only captures 1.2s ramp-up
        ramp_up_autoscale = [
            {"worker_count": wc, "cumulative_min": 0.002, "pct_of_total": 10.0}
            for wc in range(1, 11)
        ]
        result_ramp = estimate_dbu_cost(
            worker_node_type="m5.2xlarge",
            driver_node_type="m5.2xlarge",
            duration_min=11.4,
            autoscale_cost=ramp_up_autoscale,
            min_workers=10,
            max_workers=10,  # fixed cluster
            photon_enabled=False,
        )
        # Compare with correct calculation (no autoscale)
        result_fixed = estimate_dbu_cost(
            worker_node_type="m5.2xlarge",
            driver_node_type="m5.2xlarge",
            duration_min=11.4,
            autoscale_cost=[],
            min_workers=10,
            max_workers=10,
            photon_enabled=False,
        )
        # With the fix, both should produce the same result
        assert result_ramp["estimated_total_dbu"] == pytest.approx(
            result_fixed["estimated_total_dbu"]
        )
        assert result_ramp["worker_dbu"] == pytest.approx(result_fixed["worker_dbu"])

    def test_azure_instance_lookup(self):
        """Azure VMs in lookup should produce accurate USD."""
        result = estimate_dbu_cost(
            worker_node_type="Standard_D8s_v3",
            driver_node_type="Standard_D8s_v3",
            duration_min=60.0,
            autoscale_cost=[],
            min_workers=4,
            max_workers=4,
            photon_enabled=False,
        )
        # D8s_v3 lookup USD = 0.384/h, 5 nodes (4 workers + driver) * 60min = 5hr
        assert result["estimated_compute_usd"] > 1.0  # sanity check
        assert result["pricing_method"] == "lookup"

    def test_gcp_instance_lookup(self):
        """GCP VMs in lookup should produce accurate USD."""
        result = estimate_dbu_cost(
            worker_node_type="n2-standard-8",
            driver_node_type="n2-standard-4",
            duration_min=60.0,
            autoscale_cost=[],
            min_workers=4,
            max_workers=4,
            photon_enabled=False,
        )
        assert result["estimated_compute_usd"] > 0
        assert result["pricing_method"] == "lookup"

    def test_gcp_highmem_category_detected(self):
        """n2-highmem-* should be parsed as memory category (not general)."""
        from core.dbu_pricing import parse_instance_type

        spec = parse_instance_type("n2-highmem-16")
        assert spec.cloud == "gcp"
        assert spec.vcpus == 16
        assert spec.category == "memory"

    def test_gcp_highcpu_category_detected(self):
        """n2-highcpu-* should be parsed as compute category."""
        from core.dbu_pricing import parse_instance_type

        spec = parse_instance_type("n2-highcpu-32")
        assert spec.category == "compute"

    def test_autoscale_applied_when_min_lt_max(self):
        """When min < max (true autoscaling), autoscale_cost is used."""
        autoscale = [
            {"worker_count": 2, "cumulative_min": 5.0, "pct_of_total": 50.0},
            {"worker_count": 8, "cumulative_min": 5.0, "pct_of_total": 50.0},
        ]
        result_scale = estimate_dbu_cost(
            worker_node_type="m5.xlarge",
            driver_node_type="m5.xlarge",
            duration_min=10.0,
            autoscale_cost=autoscale,
            min_workers=2,
            max_workers=10,  # autoscaling
            photon_enabled=False,
        )
        # Fallback using min=2 would give a different (smaller) worker_dbu
        result_min_fallback = estimate_dbu_cost(
            worker_node_type="m5.xlarge",
            driver_node_type="m5.xlarge",
            duration_min=10.0,
            autoscale_cost=[],
            min_workers=2,
            max_workers=10,
            photon_enabled=False,
        )
        assert result_scale["worker_dbu"] != pytest.approx(result_min_fallback["worker_dbu"])

    def test_photon_doubles_total(self):
        kwargs = dict(
            worker_node_type="i3.xlarge",
            driver_node_type="i3.xlarge",
            duration_min=60.0,
            autoscale_cost=[],
            min_workers=4,
            max_workers=4,
        )
        result_base = estimate_dbu_cost(**kwargs, photon_enabled=False)
        result_photon = estimate_dbu_cost(**kwargs, photon_enabled=True)
        assert result_photon["estimated_total_dbu"] == pytest.approx(
            result_base["estimated_total_dbu"] * 2.0
        )
        assert result_photon["photon_multiplier"] == 2.0

    def test_zero_duration_returns_zeros(self):
        result = estimate_dbu_cost(
            worker_node_type="i3.xlarge",
            driver_node_type="i3.xlarge",
            duration_min=0.0,
            autoscale_cost=[],
            min_workers=4,
            max_workers=4,
            photon_enabled=False,
        )
        assert result["estimated_total_dbu"] == 0.0

    def test_empty_instance_still_estimates(self):
        result = estimate_dbu_cost(
            worker_node_type="",
            driver_node_type="",
            duration_min=60.0,
            autoscale_cost=[],
            min_workers=4,
            max_workers=4,
            photon_enabled=False,
        )
        assert result["estimated_total_dbu"] > 0
        assert "fallback" in result["pricing_method"] or "formula" in result["pricing_method"]

    def test_dbu_per_hour_consistency(self):
        result = estimate_dbu_cost(
            worker_node_type="i3.xlarge",
            driver_node_type="i3.xlarge",
            duration_min=120.0,
            autoscale_cost=[],
            min_workers=4,
            max_workers=4,
            photon_enabled=False,
        )
        expected_per_hour = result["estimated_total_dbu"] / 2.0
        assert result["estimated_dbu_per_hour"] == pytest.approx(expected_per_hour, rel=0.01)


class TestComputePricePerHour:
    def test_known_instance_us_east(self):
        price, method = compute_price_per_hour("i3.xlarge", "us-east-1")
        assert price > 0
        assert method == "lookup"

    def test_tokyo_region_multiplier(self):
        price_us, _ = compute_price_per_hour("i3.xlarge", "us-east-1")
        price_jp, _ = compute_price_per_hour("i3.xlarge", "ap-northeast-1")
        assert price_jp > price_us

    def test_unknown_region_uses_base(self):
        price_us, _ = compute_price_per_hour("i3.xlarge", "us-east-1")
        price_unknown, _ = compute_price_per_hour("i3.xlarge", "unknown-region-99")
        assert price_unknown == price_us


class TestEstimateDBUCostUSD:
    """Tests for USD cost estimation in estimate_dbu_cost."""

    def _base_kwargs(self, **overrides):
        defaults = dict(
            worker_node_type="i3.xlarge",
            driver_node_type="i3.xlarge",
            duration_min=60.0,
            autoscale_cost=[],
            min_workers=4,
            max_workers=4,
            photon_enabled=False,
            region="us-east-1",
        )
        defaults.update(overrides)
        return defaults

    def test_includes_usd_fields(self):
        result = estimate_dbu_cost(**self._base_kwargs())
        assert "estimated_total_usd" in result
        assert "estimated_dbu_usd" in result
        assert "estimated_compute_usd" in result
        assert result["estimated_total_usd"] > 0
        assert result["estimated_total_usd"] == pytest.approx(
            result["estimated_dbu_usd"] + result["estimated_compute_usd"], rel=0.01
        )

    def test_region_affects_compute_not_dbu(self):
        result_us = estimate_dbu_cost(**self._base_kwargs(region="us-east-1"))
        result_jp = estimate_dbu_cost(**self._base_kwargs(region="ap-northeast-1"))
        assert result_jp["estimated_compute_usd"] > result_us["estimated_compute_usd"]
        assert result_jp["estimated_dbu_usd"] == result_us["estimated_dbu_usd"]

    def test_photon_increases_dbu_usd(self):
        """Photon: DBU doubles (2x) AND unit price doubles ($0.15→$0.30) = 4x."""
        result_base = estimate_dbu_cost(**self._base_kwargs(photon_enabled=False))
        result_photon = estimate_dbu_cost(**self._base_kwargs(photon_enabled=True))
        assert result_photon["estimated_dbu_usd"] == pytest.approx(
            result_base["estimated_dbu_usd"] * 4.0, rel=0.01
        )

    def test_zero_duration_usd_zero(self):
        result = estimate_dbu_cost(**self._base_kwargs(duration_min=0))
        assert result["estimated_total_usd"] == 0.0

    def test_region_in_output(self):
        result = estimate_dbu_cost(**self._base_kwargs(region="us-west-2"))
        assert result["region"] == "us-west-2"


class TestBuildCostSection:
    """Tests for Markdown Section G generation."""

    def test_en_output(self):
        from core.spark_perf_markdown import build_cost_section

        estimate = {
            "estimated_total_usd": 5.82,
            "estimated_dbu_usd": 1.88,
            "estimated_compute_usd": 3.94,
            "estimated_total_dbu": 12.54,
            "estimated_dbu_per_hour": 6.27,
            "photon_multiplier": 1.0,
            "pricing_note": "Jobs Compute, i3.xlarge (4 vCPU), 0.28 DBU/hr/node",
            "region": "us-west-2",
            "sku": "Jobs Compute",
            "worker_node_type": "i3.xlarge",
            "driver_node_type": "i3.xlarge",
            "worker_vcpus": 4,
            "driver_vcpus": 4,
            "worker_count_label": "4→8",
            "duration_min": 30.0,
            "dbu_unit_price": 0.15,
            "worker_dbu_rate": 0.28,
            "driver_dbu_rate": 0.28,
            "worker_compute_rate": 0.312,
            "driver_compute_rate": 0.312,
            "region_multiplier": 1.0,
            "driver_dbu": 0.14,
            "worker_dbu": 12.40,
        }
        result = build_cost_section(estimate, lang="en")
        assert "H. Cost Estimate" in result
        assert "$5.820" in result
        assert "$1.880" in result
        assert "$3.940" in result
        # Breakdown table
        assert "Driver" in result
        assert "Worker" in result
        assert "i3.xlarge" in result
        assert "4→8" in result
        # Assumptions
        assert "Jobs Compute" in result
        assert "$0.15/DBU" in result
        assert "On-Demand" in result

    def test_ja_output(self):
        from core.spark_perf_markdown import build_cost_section

        estimate = {
            "estimated_total_usd": 5.82,
            "estimated_dbu_usd": 1.88,
            "estimated_compute_usd": 3.94,
            "estimated_total_dbu": 12.54,
            "estimated_dbu_per_hour": 6.27,
            "photon_multiplier": 1.0,
            "pricing_note": "Jobs Compute, i3.xlarge (4 vCPU), 0.28 DBU/hr/node",
            "region": "us-west-2",
        }
        result = build_cost_section(estimate, lang="ja")
        assert "H." in result
        assert "$5.820" in result

    def test_zero_usd_shows_not_available(self):
        from core.spark_perf_markdown import build_cost_section

        estimate = {"estimated_total_usd": 0.0}
        result = build_cost_section(estimate, lang="en")
        assert "H. Cost Estimate" in result
        assert "not available" in result
