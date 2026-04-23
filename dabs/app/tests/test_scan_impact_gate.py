"""Rule-based I/O alerts and action cards must be gated on
``scan_impact_ratio`` so they do not surface on compute-bound queries.

Regression: a query dominated by Generate + Grouping Aggregate (JSON
parsing) surfaced "Low file pruning efficiency (0.0%)" as a HIGH alert
and pushed "Apply Liquid Clustering" into the Top-5 action plan, even
though the LLM itself noted I/O is not the primary bottleneck here.

Design (option B, two-step gate):
  scan_impact_ratio >= 25%  → full HIGH alert + ActionCard emitted
  10%  <= scan < 25%         → alert demoted to MEDIUM, card demoted
                               (expected_impact="low") + not preserved
  scan_impact_ratio < 10%   → alert demoted to INFO, card suppressed

Photon gate:
  task_total_time_ms >= 5000 → full severity
  500 <= task_total < 5000   → demoted to MEDIUM
  task_total < 500           → suppressed
"""

from __future__ import annotations

import pytest
from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.analyzers.recommendations import generate_action_cards
from core.constants import Severity
from core.models import (
    NodeMetrics,
    QueryMetrics,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _scan_node(duration_ms: int, name: str = "Scan table_a") -> NodeMetrics:
    return NodeMetrics(node_id=f"s-{duration_ms}", node_name=name, duration_ms=duration_ms)


def _compute_node(duration_ms: int, name: str = "Grouping Aggregate") -> NodeMetrics:
    return NodeMetrics(node_id=f"c-{duration_ms}", node_name=name, duration_ms=duration_ms)


def _qm(
    *,
    task_total_time_ms: int = 100_000,
    total_time_ms: int = 60_000,
    read_files: int = 1000,
    pruned_files: int = 0,
    read_bytes: int = 100 * 1024**3,
    read_cache_bytes: int = 0,
    read_remote_bytes: int = 100 * 1024**3,
    photon_total_time_ms: int = 0,
) -> QueryMetrics:
    return QueryMetrics(
        query_text="SELECT ...",
        total_time_ms=total_time_ms,
        task_total_time_ms=task_total_time_ms,
        read_bytes=read_bytes,
        read_cache_bytes=read_cache_bytes,
        read_remote_bytes=read_remote_bytes,
        read_files_count=read_files,
        pruned_files_count=pruned_files,
        photon_total_time_ms=photon_total_time_ms,
    )


# ---------------------------------------------------------------------------
# (1) scan_impact_ratio is computed correctly
# ---------------------------------------------------------------------------


class TestScanImpactRatioComputation:
    def test_io_bound_query_ratio_above_point3(self):
        nodes = [_scan_node(30_000), _scan_node(20_000), _compute_node(10_000)]
        qm = _qm(task_total_time_ms=100_000)
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        # scan total = 50_000 / task_total 100_000 = 0.5
        assert bi.scan_impact_ratio == pytest.approx(0.5, abs=0.01)

    def test_compute_bound_query_ratio_near_zero(self):
        nodes = [_scan_node(1_000), _compute_node(99_000)]
        qm = _qm(task_total_time_ms=100_000)
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        assert bi.scan_impact_ratio == pytest.approx(0.01, abs=0.005)

    def test_zero_task_time_does_not_divide_by_zero(self):
        bi = calculate_bottleneck_indicators(_qm(task_total_time_ms=0), [], [], [])
        assert bi.scan_impact_ratio == 0.0


# ---------------------------------------------------------------------------
# (2) Compute-bound query: pruning / cache alerts should NOT surface as HIGH
# ---------------------------------------------------------------------------


class TestComputeBoundSuppressesIoAlerts:
    def test_low_pruning_with_tiny_scan_share_demoted_to_info(self):
        """filter_rate=0 but scan_impact=1% → alert must be INFO, not HIGH."""
        nodes = [_scan_node(1_000), _compute_node(99_000)]
        qm = _qm(
            task_total_time_ms=100_000,
            read_files=1000,
            pruned_files=0,
        )
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        pruning_alerts = [a for a in bi.alerts if a.metric_name == "filter_rate"]
        assert pruning_alerts, "pruning metric should still produce an alert record"
        assert pruning_alerts[0].severity == Severity.INFO, (
            f"filter_rate alert should be INFO on compute-bound query, got "
            f"{pruning_alerts[0].severity}"
        )

    def test_low_bytes_pruning_with_tiny_scan_share_demoted(self):
        nodes = [_scan_node(1_000), _compute_node(99_000)]
        qm = _qm(task_total_time_ms=100_000)
        qm.pruned_bytes = 0  # 0% bytes pruning
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        bp = [a for a in bi.alerts if a.metric_name == "bytes_pruning_ratio"]
        assert bp and bp[0].severity == Severity.INFO

    def test_low_cache_with_tiny_scan_share_demoted(self):
        nodes = [_scan_node(1_000), _compute_node(99_000)]
        qm = _qm(
            task_total_time_ms=100_000,
            read_bytes=100 * 1024**3,
            read_cache_bytes=0,  # 0% cache hit
            read_remote_bytes=100 * 1024**3,
        )
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        cache_alerts = [a for a in bi.alerts if a.metric_name == "cache_hit_ratio"]
        # Cache baseline alert is already INFO — check we don't re-elevate.
        assert all(a.severity in (Severity.INFO, Severity.MEDIUM) for a in cache_alerts)

    def test_remote_read_alert_demoted_to_info_on_compute_bound(self):
        nodes = [_scan_node(1_000), _compute_node(99_000)]
        qm = _qm(
            task_total_time_ms=100_000,
            read_bytes=100 * 1024**3,
            read_cache_bytes=0,
            read_remote_bytes=100 * 1024**3,  # 100% remote
        )
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        rr = [a for a in bi.alerts if a.metric_name == "remote_read_ratio"]
        assert rr and rr[0].severity == Severity.INFO, (
            f"remote_read alert should be INFO on compute-bound query, got "
            f"{[a.severity for a in rr]}"
        )


# ---------------------------------------------------------------------------
# (3) IO-bound query: alerts fire at full HIGH severity (regression guard)
# ---------------------------------------------------------------------------


class TestIoBoundPreservesHighAlerts:
    def test_pruning_alert_stays_high_when_scan_dominates(self):
        nodes = [_scan_node(50_000), _compute_node(10_000)]  # scan = 50/60 ≈ 83%
        qm = _qm(
            task_total_time_ms=60_000,
            read_files=1000,
            pruned_files=0,
        )
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        pruning = [a for a in bi.alerts if a.metric_name == "filter_rate"]
        assert pruning and pruning[0].severity == Severity.HIGH

    def test_remote_read_alert_stays_high_when_scan_dominates(self):
        nodes = [_scan_node(50_000), _compute_node(10_000)]
        qm = _qm(
            task_total_time_ms=60_000,
            read_bytes=100 * 1024**3,
            read_cache_bytes=0,
            read_remote_bytes=100 * 1024**3,
        )
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        rr = [a for a in bi.alerts if a.metric_name == "remote_read_ratio"]
        assert rr and rr[0].severity == Severity.HIGH


# ---------------------------------------------------------------------------
# (4) Mid-band (10% <= scan < 25%): demoted to MEDIUM
# ---------------------------------------------------------------------------


class TestMidBandDemotesToMedium:
    def test_pruning_alert_medium_when_scan_is_mid_band(self):
        # scan = 15 / 100 = 15%
        nodes = [_scan_node(15_000), _compute_node(85_000)]
        qm = _qm(task_total_time_ms=100_000, read_files=1000, pruned_files=0)
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        pruning = [a for a in bi.alerts if a.metric_name == "filter_rate"]
        assert pruning and pruning[0].severity == Severity.MEDIUM


# ---------------------------------------------------------------------------
# (5) ActionCards: gated on same scan_impact_ratio
# ---------------------------------------------------------------------------


class TestActionCardGating:
    def test_low_file_pruning_card_suppressed_on_compute_bound(self):
        nodes = [_scan_node(1_000), _compute_node(99_000)]
        qm = _qm(task_total_time_ms=100_000, read_files=1000, pruned_files=0)
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        cards = generate_action_cards(bi, [], qm, [], [])
        pruning_cards = [c for c in cards if "pruning" in c.problem.lower()]
        assert pruning_cards == [], (
            f"Low file pruning card must be suppressed on compute-bound queries; got {[c.problem for c in pruning_cards]}"
        )

    def test_low_file_pruning_card_demoted_in_mid_band(self):
        """scan 15% → card emitted but with expected_impact='low'."""
        nodes = [_scan_node(15_000), _compute_node(85_000)]
        qm = _qm(task_total_time_ms=100_000, read_files=1000, pruned_files=0)
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        cards = generate_action_cards(bi, [], qm, [], [])
        pruning_cards = [c for c in cards if "pruning" in c.problem.lower()]
        assert pruning_cards, "Mid-band should still emit card (demoted, not suppressed)"
        assert pruning_cards[0].expected_impact == "low"

    def test_low_file_pruning_card_full_impact_when_io_bound(self):
        nodes = [_scan_node(50_000), _compute_node(10_000)]
        qm = _qm(task_total_time_ms=60_000, read_files=1000, pruned_files=0)
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        cards = generate_action_cards(bi, [], qm, [], [])
        pruning_cards = [c for c in cards if "pruning" in c.problem.lower()]
        assert pruning_cards
        # Default impact when scan dominates is "medium" (per existing code)
        assert pruning_cards[0].expected_impact in ("medium", "high")


# ---------------------------------------------------------------------------
# (6) Photon gate: task_total_time_ms threshold
# ---------------------------------------------------------------------------


class TestPhotonTimeGate:
    def test_photon_low_on_tiny_query_demoted_to_info(self):
        """task_total < 500ms → suppressed (INFO)."""
        qm = _qm(
            task_total_time_ms=200,
            photon_total_time_ms=0,  # 0% photon
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        photon_alerts = [a for a in bi.alerts if a.metric_name == "photon_ratio"]
        assert not photon_alerts or all(a.severity == Severity.INFO for a in photon_alerts)

    def test_photon_low_on_small_query_demoted_to_medium(self):
        """500 <= task_total < 5000 → MEDIUM."""
        qm = _qm(
            task_total_time_ms=2_000,
            photon_total_time_ms=0,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        photon_alerts = [a for a in bi.alerts if a.metric_name == "photon_ratio"]
        assert photon_alerts
        assert photon_alerts[0].severity == Severity.MEDIUM

    def test_photon_low_on_large_query_critical(self):
        """task_total >= 5000 → CRITICAL (existing behavior preserved)."""
        qm = _qm(
            task_total_time_ms=60_000,
            photon_total_time_ms=0,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        photon_alerts = [a for a in bi.alerts if a.metric_name == "photon_ratio"]
        assert photon_alerts
        assert photon_alerts[0].severity == Severity.CRITICAL
