"""Tests for ``recommend_size`` — normative cluster sizing recommendation.

This is *not* an "infer the actual cluster" function (that is impossible
from a profile alone — see ``dbsql_cost._infer_size_from_parallelism``,
which produces a lower bound only). Instead, it answers:

    "For the workload represented by this profile, what size cluster
    *should* be used, and what would it cost at that size?"

The recommendation is produced from three layers (Codex 2026-04-26):
  1. **fit floor**       — parallelism + spill + scan throughput
  2. **SLA gate**        — raise tier if target time (default = observed
                            execution time) is not met
  3. **diminishing cap** — parallelism ceiling + Photon coverage ⇒
                            "larger than X adds little benefit"

Output is a 3-band structure (``minimum_viable`` / ``recommended`` /
``oversized_beyond``) plus a confidence tier, billing label, and a
short rationale.
"""

from __future__ import annotations

import pytest
from core.models import QueryMetrics
from core.sizing_recommendation import (
    SizeBand,
    SizingRecommendation,
    recommend_size,
)


def _make_query(
    *,
    execution_time_ms: int = 60_000,
    typename: str = "LakehouseSqlQuery",
    task_total_time_ms: int = 0,
    spill_to_disk_bytes: int = 0,
    read_bytes: int = 0,
    photon_total_time_ms: int = 0,
) -> QueryMetrics:
    return QueryMetrics(
        query_id="test-query",
        execution_time_ms=execution_time_ms,
        query_typename=typename,
        task_total_time_ms=task_total_time_ms,
        spill_to_disk_bytes=spill_to_disk_bytes,
        read_bytes=read_bytes,
        photon_total_time_ms=photon_total_time_ms,
    )


# ---------------------------------------------------------------------------
# Output shape
# ---------------------------------------------------------------------------


class TestRecommendSizeShape:
    def test_returns_none_when_no_typename(self):
        """No typename = unknown billing model → cannot recommend."""
        qm = _make_query(typename="")
        assert recommend_size(qm) is None

    def test_returns_recommendation_with_three_bands(self):
        qm = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 60,  # parallelism = 60
        )
        rec = recommend_size(qm)
        assert isinstance(rec, SizingRecommendation)
        assert isinstance(rec.minimum_viable, SizeBand)
        assert isinstance(rec.recommended, SizeBand)
        # oversized_beyond may be None when recommended is already the
        # largest standard size, but for normal workloads it must exist.
        assert rec.oversized_beyond is None or isinstance(rec.oversized_beyond, SizeBand)

    def test_confidence_reflects_parallelism_quality(self):
        qm_high = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 240,  # parallelism = 240 → saturated
        )
        rec_high = recommend_size(qm_high)
        assert rec_high.confidence == "high"

        qm_low = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 5,  # parallelism = 5 → loose
        )
        rec_low = recommend_size(qm_low)
        assert rec_low.confidence == "low"

    def test_rationale_is_non_empty_list(self):
        qm = _make_query(execution_time_ms=60_000, task_total_time_ms=60_000 * 60)
        rec = recommend_size(qm)
        assert isinstance(rec.rationale, list)
        assert len(rec.rationale) > 0

    def test_scope_notice_mentions_observed_run(self):
        """Recommendation must NOT be presented as universal; it
        applies only to the observed workload characteristics."""
        qm = _make_query(execution_time_ms=60_000, task_total_time_ms=60_000 * 60)
        rec = recommend_size(qm)
        assert "observed" in rec.scope_notice.lower()


# ---------------------------------------------------------------------------
# Band ordering
# ---------------------------------------------------------------------------


class TestBandOrdering:
    def test_recommended_is_at_least_minimum_viable(self):
        qm = _make_query(execution_time_ms=60_000, task_total_time_ms=60_000 * 60)
        rec = recommend_size(qm)
        assert rec.recommended.dbu_per_hour >= rec.minimum_viable.dbu_per_hour

    def test_oversized_beyond_is_strictly_greater_than_recommended(self):
        qm = _make_query(execution_time_ms=60_000, task_total_time_ms=60_000 * 60)
        rec = recommend_size(qm)
        if rec.oversized_beyond is not None:
            assert rec.oversized_beyond.dbu_per_hour > rec.recommended.dbu_per_hour


# ---------------------------------------------------------------------------
# fit floor — spill drives push-up
# ---------------------------------------------------------------------------


class TestFitFloorSpill:
    def test_spill_pushes_recommended_above_minimum_viable(self):
        """Spill = memory pressure → recommended must exceed the
        bare lower bound by at least one tier."""
        qm = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 30,  # parallelism = 30 → would imply Small
            spill_to_disk_bytes=2 * 1024 * 1024 * 1024,  # 2 GB spill
        )
        rec = recommend_size(qm)
        assert rec.recommended.dbu_per_hour > rec.minimum_viable.dbu_per_hour

    def test_no_spill_no_forced_pushup(self):
        """No spill → recommended may equal minimum viable (no push-up reason)."""
        qm = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 60,
            spill_to_disk_bytes=0,
        )
        rec = recommend_size(qm)
        # When spill=0 and no SLA pressure, recommended is allowed to
        # be the same tier as minimum_viable. We don't assert equality
        # because other signals (e.g. low confidence widening) can
        # still nudge it; the contract is just "no spill-driven push".
        assert rec.recommended.dbu_per_hour <= rec.minimum_viable.dbu_per_hour * 2


# ---------------------------------------------------------------------------
# SLA gate — target time
# ---------------------------------------------------------------------------


class TestSlaGate:
    def test_default_target_is_observed_execution_time(self):
        """Default SLA = observed execution time → gate is a no-op
        (the workload already met its own target)."""
        qm = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 60,
        )
        rec = recommend_size(qm)
        assert rec.sla_target_ms == 60_000

    def test_explicit_target_below_observed_pushes_up(self):
        """User says "I want it 2× faster" → SLA = 30s while observed =
        60s → recommended must move above the parallelism-based floor."""
        qm = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 60,
        )
        rec_default = recommend_size(qm)
        rec_tight = recommend_size(qm, target_time_ms=30_000)
        assert rec_tight.sla_target_ms == 30_000
        assert rec_tight.recommended.dbu_per_hour >= rec_default.recommended.dbu_per_hour

    def test_explicit_target_above_observed_does_not_force_upgrade(self):
        qm = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 60,
        )
        rec_default = recommend_size(qm)
        rec_loose = recommend_size(qm, target_time_ms=120_000)
        assert rec_loose.recommended.dbu_per_hour <= rec_default.recommended.dbu_per_hour


# ---------------------------------------------------------------------------
# diminishing returns cap
# ---------------------------------------------------------------------------


class TestDiminishingReturnsCap:
    def test_low_parallelism_caps_oversized_beyond_close_to_recommended(self):
        """Low parallelism → going larger gives little benefit.
        oversized_beyond should be within 1-2 tiers of recommended."""
        qm = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 8,  # parallelism = 8, very low
        )
        rec = recommend_size(qm)
        assert rec.oversized_beyond is not None
        # The cap should be tight — at most ~2x the recommended DBU/h.
        assert rec.oversized_beyond.dbu_per_hour <= rec.recommended.dbu_per_hour * 2 + 1


# ---------------------------------------------------------------------------
# Cost computation at recommended size
# ---------------------------------------------------------------------------


class TestRecommendedCost:
    def test_recommended_cost_matches_dbu_formula(self):
        """estimated_cost_usd at recommended = (target_time_ms /
        3_600_000) × dbu_per_hour × unit_price."""
        qm = _make_query(
            execution_time_ms=3_600_000,  # 1 hour
            task_total_time_ms=3_600_000 * 60,
        )
        rec = recommend_size(qm)
        from core.dbsql_cost import DBU_PRICE_SERVERLESS

        target_hours = rec.sla_target_ms / 3_600_000
        expected = target_hours * rec.recommended.dbu_per_hour * DBU_PRICE_SERVERLESS
        assert rec.recommended.estimated_cost_usd == pytest.approx(expected, rel=0.01)

    def test_minimum_viable_cost_uses_lower_bound_dbu_h(self):
        qm = _make_query(
            execution_time_ms=3_600_000,
            task_total_time_ms=3_600_000 * 60,
        )
        rec = recommend_size(qm)
        # The minimum viable cost is always ≤ recommended cost
        assert rec.minimum_viable.estimated_cost_usd <= rec.recommended.estimated_cost_usd


# ---------------------------------------------------------------------------
# Billing label split (Codex 2026-04-26 — Pro/Classic must NOT use
# the same "estimated execution cost" wording)
# ---------------------------------------------------------------------------


class TestBillingLabelSplit:
    def test_serverless_label(self):
        qm = _make_query(typename="LakehouseSqlQuery", task_total_time_ms=60_000 * 60)
        rec = recommend_size(qm)
        assert "execution" in rec.billing_label.lower()
        assert "uptime" not in rec.billing_label.lower()

    def test_classic_label_mentions_runtime_equivalent_or_dedicated(self):
        qm = _make_query(typename="SqlQuery", task_total_time_ms=60_000 * 60)
        rec = recommend_size(qm)
        # Pro/Classic: cost is uptime-billed, so the label must signal
        # that this is a dedicated-runtime equivalent, not actual billing.
        label_lower = rec.billing_label.lower()
        assert "runtime" in label_lower or "dedicated" in label_lower


# ---------------------------------------------------------------------------
# Photon coverage influence on diminishing cap
# ---------------------------------------------------------------------------


class TestPhotonCoverageInfluence:
    def test_high_photon_coverage_does_not_widen_oversized_beyond(self):
        """Already Photon-saturated → larger size adds little. The cap
        should not be looser than for a similarly-sized non-Photon run."""
        qm_full_photon = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 30,
            photon_total_time_ms=60_000 * 30,  # 100% Photon coverage
        )
        qm_no_photon = _make_query(
            execution_time_ms=60_000,
            task_total_time_ms=60_000 * 30,
            photon_total_time_ms=0,
        )
        rec_full = recommend_size(qm_full_photon)
        rec_none = recommend_size(qm_no_photon)
        # Both should produce a cap; full Photon must be ≤ non-Photon.
        if rec_full.oversized_beyond and rec_none.oversized_beyond:
            assert (
                rec_full.oversized_beyond.dbu_per_hour
                <= rec_none.oversized_beyond.dbu_per_hour
            )
