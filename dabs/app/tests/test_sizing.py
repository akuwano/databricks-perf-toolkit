"""Tests for cluster right-sizing recommendations."""

from core.dbu_pricing import SizingRecommendation, generate_sizing_recommendations


def _base_executor_summary(**overrides):
    defaults = {
        "executor_count": 8,
        "total_cores": 4,
        "executor_memory_mb": 8192,
        "avg_cpu_efficiency_pct": 55.0,
        "avg_gc_pct": 5.0,
        "total_disk_spill_mb": 0,
        "total_memory_spill_mb": 0,
        "executors_with_spill": 0,
        "straggler_count": 0,
        "underutilized_count": 0,
        "diagnosis_counts": {},
    }
    defaults.update(overrides)
    return defaults


def _base_app_summary(**overrides):
    defaults = {
        "worker_node_type": "i3.xlarge",
        "driver_node_type": "i3.xlarge",
        "min_workers": 4,
        "max_workers": 8,
        "duration_min": 30.0,
        "region": "us-east-1",
    }
    defaults.update(overrides)
    return defaults


def _make_bottleneck(bt: str, count: int = 1, severity: str = "HIGH"):
    return {
        "bottleneck_type": bt,
        "count": count,
        "max_severity": severity,
        "total_duration_ms": 60000,
    }


class TestGenerateSizingRecommendations:
    def test_high_spill_recommends_up(self):
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(
                total_disk_spill_mb=5000, executors_with_spill=5
            ),
            app_summary=_base_app_summary(),
            bottleneck_summary=[_make_bottleneck("DISK_SPILL", count=5)],
            autoscale_cost=[],
            scaling_event_counts={},
        )
        assert len(recs) >= 1
        up_recs = [r for r in recs if r.direction == "UP"]
        assert len(up_recs) >= 1
        assert up_recs[0].severity == "HIGH"

    def test_high_gc_recommends_up(self):
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(avg_gc_pct=20.0),
            app_summary=_base_app_summary(),
            bottleneck_summary=[_make_bottleneck("HIGH_GC", count=4)],
            autoscale_cost=[],
            scaling_event_counts={},
        )
        up_recs = [r for r in recs if r.direction == "UP"]
        assert len(up_recs) >= 1

    def test_low_cpu_recommends_down(self):
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(
                avg_cpu_efficiency_pct=20.0, underutilized_count=6, executor_count=8
            ),
            app_summary=_base_app_summary(),
            bottleneck_summary=[],
            autoscale_cost=[],
            scaling_event_counts={},
        )
        down_recs = [r for r in recs if r.direction == "DOWN"]
        assert len(down_recs) >= 1

    def test_well_sized_returns_empty(self):
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(avg_cpu_efficiency_pct=85.0),
            app_summary=_base_app_summary(),
            bottleneck_summary=[],
            autoscale_cost=[],
            scaling_event_counts={},
        )
        assert len(recs) == 0

    def test_heavy_shuffle_consolidate(self):
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(executor_count=10),
            app_summary=_base_app_summary(
                min_workers=10, max_workers=10, worker_node_type="m5.xlarge"
            ),
            bottleneck_summary=[_make_bottleneck("HEAVY_SHUFFLE", count=5, severity="MEDIUM")],
            autoscale_cost=[],
            scaling_event_counts={},
        )
        consol = [r for r in recs if r.direction == "CONSOLIDATE"]
        assert len(consol) >= 1

    def test_autoscale_at_max(self):
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(),
            app_summary=_base_app_summary(min_workers=2, max_workers=8),
            bottleneck_summary=[],
            autoscale_cost=[
                {"worker_count": 2, "cumulative_min": 3.0, "pct_of_total": 10.0},
                {"worker_count": 8, "cumulative_min": 27.0, "pct_of_total": 90.0},
            ],
            scaling_event_counts={},
        )
        scale_recs = [r for r in recs if r.direction == "SCALE_LIMIT"]
        assert len(scale_recs) >= 1
        assert "max" in scale_recs[0].rationale.lower()

    def test_spot_preemption_advisory(self):
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(),
            app_summary=_base_app_summary(),
            bottleneck_summary=[],
            autoscale_cost=[],
            scaling_event_counts={"SPOT_PREEMPTION": 5},
        )
        spot_recs = [r for r in recs if r.direction == "SPOT"]
        assert len(spot_recs) >= 1
        assert spot_recs[0].severity == "INFO"

    def test_duplicate_direction_dedup(self):
        """DISK_SPILL + HIGH_GC both suggest UP → should produce single UP recommendation."""
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(
                avg_gc_pct=18.0, total_disk_spill_mb=5000, executors_with_spill=5
            ),
            app_summary=_base_app_summary(),
            bottleneck_summary=[
                _make_bottleneck("DISK_SPILL", count=5),
                _make_bottleneck("HIGH_GC", count=3),
            ],
            autoscale_cost=[],
            scaling_event_counts={},
        )
        up_recs = [r for r in recs if r.direction == "UP"]
        assert len(up_recs) == 1  # deduplicated
        assert "DISK_SPILL" in up_recs[0].signal or "HIGH_GC" in up_recs[0].signal

    def test_cost_delta_populated(self):
        recs = generate_sizing_recommendations(
            executor_summary=_base_executor_summary(
                total_disk_spill_mb=5000, executors_with_spill=5
            ),
            app_summary=_base_app_summary(),
            bottleneck_summary=[_make_bottleneck("DISK_SPILL", count=5)],
            autoscale_cost=[],
            scaling_event_counts={},
        )
        assert len(recs) >= 1
        assert recs[0].current_usd_per_hr > 0

    def test_empty_inputs(self):
        recs = generate_sizing_recommendations(
            executor_summary={},
            app_summary={},
            bottleneck_summary=[],
            autoscale_cost=[],
            scaling_event_counts={},
        )
        assert recs == []


class TestBuildSizingSection:
    def test_en_output(self):
        from core.spark_perf_markdown import build_sizing_section

        recs = [
            SizingRecommendation(
                signal="DISK_SPILL",
                severity="HIGH",
                direction="UP",
                current_instance="i3.xlarge",
                recommended_instance="i3.2xlarge",
                rationale="5 stages with disk spill",
                current_usd_per_hr=1.56,
                recommended_usd_per_hr=2.18,
                cost_delta_pct=39.7,
            ),
        ]
        result = build_sizing_section(recs, _base_app_summary(), lang="en")
        assert "I." in result
        assert "DISK_SPILL" in result
        assert "i3.xlarge" in result
        assert "i3.2xlarge" in result

    def test_well_sized_message(self):
        from core.spark_perf_markdown import build_sizing_section

        result = build_sizing_section([], _base_app_summary(), lang="en")
        assert "I. Cluster Right-Sizing" in result
        assert "well-sized" in result
