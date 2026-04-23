"""Tests for the driver-overhead alert and action card.

Driver overhead = Waiting in queue + Scheduling + Waiting for compute
(the three pre-execution bars in the Databricks Query Profile UI).

Detection derives queue_ms from explicit queuedProvisioning/Overload fields,
and scheduling+compute-wait from the timestamp gap between queue-start and
compilation-start when the explicit values are null/absent.
"""

from __future__ import annotations

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.analyzers.recommendations_registry import (
    CARDS,
    Context,
    generate_from_registry,
)
from core.models import QueryMetrics


def _qm(**overrides) -> QueryMetrics:
    base = dict(
        query_id="t",
        status="FINISHED",
        total_time_ms=60_000,
        compilation_time_ms=1000,
        execution_time_ms=58_000,
    )
    base.update(overrides)
    return QueryMetrics(**base)


def _calc(qm: QueryMetrics):
    return calculate_bottleneck_indicators(qm, [], [], [])


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


class TestDetectorNoFire:
    def test_trivial_overhead(self):
        """Real profile: 250ms pre-compile gap out of 11s total → 2.3%. No alert."""
        qm = _qm(
            total_time_ms=11021,
            compilation_time_ms=6027,
            query_start_time_ms=1769417389224,
            overloading_queue_start_ts=1769417389223,
            query_compilation_start_ts=1769417389473,
        )
        bi = _calc(qm)
        assert bi.driver_overhead_severity.value == "ok"
        assert not any(a.category == "driver_overhead" for a in bi.alerts)

    def test_no_timestamps_no_queue(self):
        qm = _qm()
        bi = _calc(qm)
        assert bi.driver_overhead_severity.value == "ok"
        assert bi.driver_overhead_ms == 0


class TestDetectorQueueDominant:
    def test_provisioning_queue_fires_medium(self):
        qm = _qm(queued_provisioning_time_ms=6000)  # 6s / 60s = 10%
        bi = _calc(qm)
        assert bi.driver_overhead_severity.value == "medium"
        assert bi.queue_wait_ms == 6000

    def test_overload_queue_fires_high_on_absolute(self):
        qm = _qm(queued_overload_time_ms=35_000)  # 35s absolute
        bi = _calc(qm)
        assert bi.driver_overhead_severity.value == "high"

    def test_high_on_ratio(self):
        qm = _qm(total_time_ms=20_000, queued_overload_time_ms=7000)  # 35%
        bi = _calc(qm)
        assert bi.driver_overhead_severity.value == "high"


class TestDetectorSchedulingDominant:
    def test_scheduling_from_timestamp_gap_fires(self):
        """Large pre-compile gap with zero queue → all residual is scheduling."""
        # total=60s, pre-compile gap = 5s, queue = 0 → sched = 5s, 8.3%, >=3s absolute
        qm = _qm(
            query_start_time_ms=1_000_000,
            query_compilation_start_ts=1_005_000,
        )
        bi = _calc(qm)
        assert bi.scheduling_compute_wait_ms == 5000
        assert bi.queue_wait_ms == 0
        # 5s absolute ≥ 3s → fires MEDIUM
        assert bi.driver_overhead_severity.value == "medium"

    def test_scheduling_ratio_only_fires(self):
        """Below 3s absolute but above 15% ratio."""
        qm = _qm(
            total_time_ms=10_000,
            query_start_time_ms=1_000_000,
            query_compilation_start_ts=1_001_600,  # 1.6s gap = 16%
        )
        bi = _calc(qm)
        assert bi.scheduling_compute_wait_ms == 1600
        assert bi.driver_overhead_severity.value == "medium"


class TestDerivation:
    def test_queue_subtracted_from_pre_compile_gap(self):
        """Scheduling residual = gap − explicit queue."""
        qm = _qm(
            queued_overload_time_ms=2000,
            overloading_queue_start_ts=1_000_000,
            query_compilation_start_ts=1_005_000,  # 5s gap
        )
        bi = _calc(qm)
        assert bi.queue_wait_ms == 2000
        assert bi.scheduling_compute_wait_ms == 3000  # 5000 - 2000
        assert bi.driver_overhead_ms == 5000

    def test_gap_smaller_than_queue_clamps_to_zero(self):
        """Mis-ordered timestamps shouldn't produce negative scheduling time."""
        qm = _qm(
            queued_provisioning_time_ms=5000,
            provisioning_queue_start_ts=1_000_000,
            query_compilation_start_ts=1_002_000,  # 2s gap < 5s queue
        )
        bi = _calc(qm)
        assert bi.scheduling_compute_wait_ms == 0


# ---------------------------------------------------------------------------
# Registry card
# ---------------------------------------------------------------------------


class TestCardRegistered:
    def test_card_in_registry_at_rank_32(self):
        entries = [c for c in CARDS if c.card_id == "driver_overhead"]
        assert len(entries) == 1
        assert entries[0].priority_rank == 32

    def test_card_emitted_queue_heavy(self):
        qm = _qm(queued_provisioning_time_ms=10_000)
        bi = _calc(qm)
        ctx = Context(indicators=bi, query_metrics=qm)
        cards, fired = generate_from_registry(ctx)
        assert "driver_overhead" in fired
        card = next(c for c in cards if c.root_cause_group == "driver_overhead")
        assert card.fix_sql == ""  # infra-only, no SQL remediation
        assert "warm pool" in card.fix.lower() or "auto-stop" in card.fix.lower()

    def test_card_emitted_scheduling_heavy(self):
        qm = _qm(
            query_start_time_ms=1_000_000,
            query_compilation_start_ts=1_005_000,
        )
        bi = _calc(qm)
        ctx = Context(indicators=bi, query_metrics=qm)
        cards, fired = generate_from_registry(ctx)
        assert "driver_overhead" in fired
        card = next(c for c in cards if c.root_cause_group == "driver_overhead")
        # Scheduling-dominant fix talks about concurrent queries / warehouse size
        assert "concurrent" in card.fix.lower() or "warehouse" in card.fix.lower()

    def test_card_not_emitted_when_trivial(self):
        qm = _qm(
            total_time_ms=11021,
            query_start_time_ms=1769417389224,
            overloading_queue_start_ts=1769417389223,
            query_compilation_start_ts=1769417389473,
        )
        bi = _calc(qm)
        ctx = Context(indicators=bi, query_metrics=qm)
        _, fired = generate_from_registry(ctx)
        assert "driver_overhead" not in fired
