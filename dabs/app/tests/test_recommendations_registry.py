"""Spark Perf-style static-priority registry for rule-based ActionCards.

Phase 1 (v5.16.11) introduces the registry with 5 pilot cards.
These tests lock down the contract so future card migrations and
priority re-assignments are safe.
"""

from __future__ import annotations

import pytest
from core.analyzers.recommendations_registry import (
    CARDS,
    Context,
    generate_from_registry,
    migrated_card_ids,
)
from core.models import BottleneckIndicators, QueryMetrics

# ---------------------------------------------------------------------------
# Structural invariants
# ---------------------------------------------------------------------------


class TestRegistryStructure:
    def test_all_priority_ranks_unique(self):
        ranks = [c.priority_rank for c in CARDS]
        assert len(ranks) == len(set(ranks)), f"priority_rank collisions: {sorted(ranks)}"

    def test_all_card_ids_unique(self):
        ids = [c.card_id for c in CARDS]
        assert len(ids) == len(set(ids)), f"card_id collisions: {sorted(ids)}"

    def test_priority_ranks_within_expected_range(self):
        for c in CARDS:
            assert 10 <= c.priority_rank <= 100, (
                f"{c.card_id}: rank {c.priority_rank} outside 10-100"
            )

    def test_registered_card_ids(self):
        """Phase 1 (17) + compilation_overhead + driver_overhead +
        federation_query (v5.18.0) + cluster_underutilization +
        compilation_absolute_heavy + decimal_heavy_aggregate
        (V6 alert coverage expansion 2026-04-26) = 23."""
        expected = {
            "disk_spill",
            "federation_query",
            "shuffle_dominant",
            "shuffle_lc",
            "data_skew",
            "low_file_pruning",
            "low_cache",
            "compilation_overhead",
            "photon_blocker",
            "photon_low",
            "scan_hot",
            "non_photon_join",
            "hier_clustering",
            "hash_resize",
            "decimal_heavy_aggregate",
            "aqe_absorbed",
            "cte_multi_ref",
            "investigate_dist",
            "stats_fresh",
            "driver_overhead",
            "rescheduled_scan",
            "cluster_underutilization",
            "compilation_absolute_heavy",
        }
        assert migrated_card_ids() == frozenset(expected)

    def test_tier1_ranks_above_tier2(self):
        """Tier 1 (spill/shuffle) must outrank Tier 2 (cache/photon/scan)."""
        spill = next(c for c in CARDS if c.card_id == "disk_spill")
        shuffle = next(c for c in CARDS if c.card_id == "shuffle_dominant")
        cache = next(c for c in CARDS if c.card_id == "low_cache")
        photon = next(c for c in CARDS if c.card_id == "photon_low")
        assert spill.priority_rank > cache.priority_rank
        assert shuffle.priority_rank > photon.priority_rank


# ---------------------------------------------------------------------------
# Detect + build contracts
# ---------------------------------------------------------------------------


def _ctx(**indicator_overrides) -> Context:
    # Default photon_ratio=1.0 so the photon_low card doesn't fire in
    # tests that exercise other cards. Override in photon-specific tests.
    indicator_overrides.setdefault("photon_ratio", 1.0)
    return Context(
        indicators=BottleneckIndicators(**indicator_overrides),
        query_metrics=QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000),
    )


class TestDiskSpillCard:
    def test_detect_fires_with_positive_spill(self):
        card_def = next(c for c in CARDS if c.card_id == "disk_spill")
        ctx = _ctx(spill_bytes=2 * 1024**3)
        assert card_def.detect(ctx)
        built = card_def.build(ctx)
        assert len(built) == 1
        assert "spill" in built[0].problem.lower()
        # priority_score follows priority_rank / 10.0 = 10.0
        assert built[0].priority_score == pytest.approx(10.0)

    def test_detect_false_without_spill(self):
        card_def = next(c for c in CARDS if c.card_id == "disk_spill")
        assert not card_def.detect(_ctx(spill_bytes=0))


class TestShuffleDominantCard:
    def test_detect_above_threshold(self):
        card_def = next(c for c in CARDS if c.card_id == "shuffle_dominant")
        assert card_def.detect(_ctx(shuffle_impact_ratio=0.3))

    def test_detect_below_threshold(self):
        card_def = next(c for c in CARDS if c.card_id == "shuffle_dominant")
        assert not card_def.detect(_ctx(shuffle_impact_ratio=0.1))

    def test_impact_high_when_ratio_high(self):
        card_def = next(c for c in CARDS if c.card_id == "shuffle_dominant")
        ctx = _ctx(shuffle_impact_ratio=0.5)
        built = card_def.build(ctx)
        assert built[0].expected_impact == "high"

    def test_impact_medium_when_ratio_mid(self):
        card_def = next(c for c in CARDS if c.card_id == "shuffle_dominant")
        ctx = _ctx(shuffle_impact_ratio=0.25)
        built = card_def.build(ctx)
        assert built[0].expected_impact == "medium"


class TestLowCacheCardGating:
    def test_fires_with_low_cache_and_meaningful_scan(self):
        card_def = next(c for c in CARDS if c.card_id == "low_cache")
        assert card_def.detect(_ctx(cache_hit_ratio=0.1, scan_impact_ratio=0.5))

    def test_suppressed_on_compute_bound(self):
        """scan_impact < 10% → suppressed even when cache is low."""
        card_def = next(c for c in CARDS if c.card_id == "low_cache")
        assert not card_def.detect(_ctx(cache_hit_ratio=0.1, scan_impact_ratio=0.05))


class TestPhotonLowGating:
    def test_suppressed_on_tiny_query(self):
        card_def = next(c for c in CARDS if c.card_id == "photon_low")
        ctx = Context(
            indicators=BottleneckIndicators(photon_ratio=0.1),
            query_metrics=QueryMetrics(task_total_time_ms=200),
        )
        assert not card_def.detect(ctx)

    def test_fires_on_large_non_serverless_query(self):
        card_def = next(c for c in CARDS if c.card_id == "photon_low")
        ctx = Context(
            indicators=BottleneckIndicators(photon_ratio=0.2),
            query_metrics=QueryMetrics(task_total_time_ms=60_000),
            is_serverless=False,
        )
        assert card_def.detect(ctx)

    def test_serverless_renders_shuffle_hash_hint(self):
        """Phase 3 (v5.16.20): registry now handles serverless too
        (legacy branch deleted). The serverless branch must emit
        SHUFFLE_HASH/BROADCAST hints instead of SET configs."""
        card_def = next(c for c in CARDS if c.card_id == "photon_low")
        ctx = Context(
            indicators=BottleneckIndicators(photon_ratio=0.2),
            query_metrics=QueryMetrics(task_total_time_ms=60_000),
            is_serverless=True,
        )
        assert card_def.detect(ctx)
        built = card_def.build(ctx)
        assert built, "Photon low card must fire on serverless too"
        sql = built[0].fix_sql
        assert "SHUFFLE_HASH" in sql or "BROADCAST" in sql, (
            f"Expected SHUFFLE_HASH/BROADCAST hint in serverless Photon fix_sql, got: {sql}"
        )


class TestRescheduledScanCard:
    def test_fires_above_critical_threshold(self):
        card_def = next(c for c in CARDS if c.card_id == "rescheduled_scan")
        assert card_def.detect(_ctx(rescheduled_scan_ratio=0.1))

    def test_below_warning_threshold_does_not_fire(self):
        card_def = next(c for c in CARDS if c.card_id == "rescheduled_scan")
        assert not card_def.detect(_ctx(rescheduled_scan_ratio=0.005))


# ---------------------------------------------------------------------------
# Serverless uniform detection — Phase 2 removes the per-card serverless
# abstention so the registry fires the same cards regardless of warehouse
# flavor. ``photon_low`` still has its own duration gate and is covered
# separately above.
# ---------------------------------------------------------------------------


class TestServerlessUniformDetection:
    """After Phase 2 (v5.16.19) the preservation / hybrid-dedup / Top-N
    pipeline is gone and with it the per-card serverless carve-outs.
    Cards that previously returned False on serverless must now fire
    uniformly on both flavors — the registry is the single source of
    truth regardless of warehouse kind."""

    def _both_flavors(self, card_id: str, **detect_triggers):
        card_def = next(c for c in CARDS if c.card_id == card_id)
        ctx_ns = Context(
            indicators=BottleneckIndicators(**detect_triggers),
            query_metrics=QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000),
            is_serverless=False,
        )
        ctx_s = Context(
            indicators=BottleneckIndicators(**detect_triggers),
            query_metrics=QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000),
            is_serverless=True,
        )
        return card_def, ctx_ns, ctx_s

    def test_shuffle_dominant_fires_uniformly(self):
        card_def, ctx_ns, ctx_s = self._both_flavors("shuffle_dominant", shuffle_impact_ratio=0.3)
        assert card_def.detect(ctx_ns)
        assert card_def.detect(ctx_s)

    def test_data_skew_fires_uniformly(self):
        card_def, ctx_ns, ctx_s = self._both_flavors("data_skew", has_data_skew=True)
        assert card_def.detect(ctx_ns)
        assert card_def.detect(ctx_s)

    def test_hash_resize_fires_uniformly(self):
        from core.models import HashResizeHotspot, JoinInfo, SQLAnalysis

        hotspot = HashResizeHotspot(
            node_id="1",
            resize=100,
            keys=["a.x"],
            key_kind="join",
        )
        card_def = next(c for c in CARDS if c.card_id == "hash_resize")
        base = dict(
            indicators=BottleneckIndicators(
                hash_table_resize_count=100, hash_resize_hotspots=[hotspot]
            ),
            query_metrics=QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000),
            join_info=[JoinInfo(node_name="HashJoin")],
            sql_analysis=SQLAnalysis(),
        )
        ctx_ns = Context(**base, is_serverless=False)
        ctx_s = Context(**base, is_serverless=True)
        assert card_def.detect(ctx_ns)
        assert card_def.detect(ctx_s)

    def test_non_photon_join_fires_uniformly(self):
        from core.models import OperatorHotspot

        hot = OperatorHotspot(
            node_id="1",
            node_name="HashJoin",
            duration_ms=30_000,
            time_share_percent=50.0,
            bottleneck_type="join",
            is_photon=False,
        )
        card_def = next(c for c in CARDS if c.card_id == "non_photon_join")
        base = dict(
            indicators=BottleneckIndicators(),
            query_metrics=QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000),
            hot_operators=[hot],
        )
        ctx_ns = Context(**base, is_serverless=False)
        ctx_s = Context(**base, is_serverless=True)
        assert card_def.detect(ctx_ns)
        assert card_def.detect(ctx_s)


# ---------------------------------------------------------------------------
# End-to-end emission ordering
# ---------------------------------------------------------------------------


class TestGenerateFromRegistry:
    def test_cards_emitted_in_priority_rank_order(self):
        """When multiple cards fire, they must appear highest-priority first."""
        ctx = _ctx(
            spill_bytes=2 * 1024**3,
            shuffle_impact_ratio=0.35,
            cache_hit_ratio=0.1,
            scan_impact_ratio=0.5,
            rescheduled_scan_ratio=0.1,
        )
        cards, fired = generate_from_registry(ctx)
        titles = [c.problem.lower() for c in cards]
        # Order must be descending by priority_rank. With filter_rate=0
        # the low_file_pruning card (rank 80) also fires between shuffle
        # (95) and low_cache (75). rescheduled (30) is last.
        assert "spill" in titles[0] or "i/o delay" in titles[0]  # 100
        assert "shuffle" in titles[1]  # 95
        assert "pruning" in titles[2] or "cache" in titles[2]  # 80 or 75
        assert "rescheduled" in titles[-1]  # 30 (last)

    def test_fired_set_reflects_emitted_cards_only(self):
        """fired set must NOT include card_ids whose detect returned False."""
        ctx = _ctx(spill_bytes=2 * 1024**3)  # only spill fires
        _, fired = generate_from_registry(ctx)
        assert fired == frozenset({"disk_spill"})

    def test_empty_fired_when_nothing_detected(self):
        ctx = _ctx()  # all defaults — no indicators triggered
        cards, fired = generate_from_registry(ctx)
        assert cards == []
        assert fired == frozenset()


# ---------------------------------------------------------------------------
# Integration: generate_action_cards uses registry without double-emission
# ---------------------------------------------------------------------------


class TestLegacyIntegration:
    def test_no_double_emission_on_pilot_cards(self):
        """A single analysis run must not emit both registry and legacy
        versions of the same card_id."""
        from core.analyzers.recommendations import generate_action_cards

        indicators = BottleneckIndicators(
            spill_bytes=2 * 1024**3,
            shuffle_impact_ratio=0.4,
            scan_impact_ratio=0.5,
            cache_hit_ratio=0.1,
            rescheduled_scan_ratio=0.1,
            photon_ratio=0.9,  # don't fire photon
        )
        qm = QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000)
        cards = generate_action_cards(indicators, [], qm, [], [])
        problems = [c.problem for c in cards]
        # Each pilot card title appears at most once
        assert problems.count("I/O delay due to disk spill") == 1
        assert problems.count("Shuffle operations are dominant") == 1
        assert problems.count("Low cache hit ratio") == 1
        # rescheduled_scan uses a different title
        scan_cards = [p for p in problems if "rescheduled" in p.lower()]
        assert len(scan_cards) == 1


# ---------------------------------------------------------------------------
# v6.6.9: quick_win_sort_key — order by simpler-and-more-effective first
# ---------------------------------------------------------------------------


class TestQuickWinSort:
    """The reader's first action should be the cheapest viable fix at
    the highest available impact tier. Sort key:
        (impact desc, effort asc, priority_score desc).
    """

    def _card(self, *, impact: str, effort: str, score: float):
        from core.models import ActionCard

        return ActionCard(
            problem=f"i={impact} e={effort} s={score}",
            evidence=[],
            likely_cause="",
            fix="",
            expected_impact=impact,
            effort=effort,
            priority_score=score,
        )

    def test_high_low_beats_high_high(self):
        from core.analyzers.recommendations import quick_win_sort_key

        cheap = self._card(impact="high", effort="low", score=10.0)
        expensive = self._card(impact="high", effort="high", score=10.0)
        ordered = sorted([expensive, cheap], key=quick_win_sort_key)
        assert ordered[0] is cheap

    def test_high_impact_beats_medium_even_when_easier(self):
        """A LOW-effort MEDIUM-impact card must NOT outrank a
        HIGH-effort HIGH-impact card — impact is the primary key."""
        from core.analyzers.recommendations import quick_win_sort_key

        big_payoff = self._card(impact="high", effort="high", score=5.0)
        easy_medium = self._card(impact="medium", effort="low", score=5.0)
        ordered = sorted([easy_medium, big_payoff], key=quick_win_sort_key)
        assert ordered[0] is big_payoff

    def test_priority_score_breaks_ties(self):
        from core.analyzers.recommendations import quick_win_sort_key

        a = self._card(impact="high", effort="low", score=3.0)
        b = self._card(impact="high", effort="low", score=8.0)
        ordered = sorted([a, b], key=quick_win_sort_key)
        assert ordered[0] is b  # higher priority_score wins the tiebreak

    def test_unknown_impact_or_effort_lands_last_in_bucket(self):
        from core.analyzers.recommendations import quick_win_sort_key

        known = self._card(impact="high", effort="high", score=1.0)
        missing = self._card(impact="", effort="", score=99.0)
        ordered = sorted([missing, known], key=quick_win_sort_key)
        assert ordered[0] is known

    def test_full_six_card_ordering(self):
        from core.analyzers.recommendations import quick_win_sort_key

        cards = [
            self._card(impact="low", effort="low", score=1.0),       # 5
            self._card(impact="medium", effort="high", score=1.0),   # 4
            self._card(impact="high", effort="high", score=1.0),     # 2
            self._card(impact="high", effort="low", score=1.0),      # 0 — cheapest big win
            self._card(impact="medium", effort="low", score=1.0),    # 3
            self._card(impact="high", effort="medium", score=1.0),   # 1
        ]
        ordered = sorted(cards, key=quick_win_sort_key)
        impacts_efforts = [(c.expected_impact, c.effort) for c in ordered]
        assert impacts_efforts == [
            ("high", "low"),
            ("high", "medium"),
            ("high", "high"),
            ("medium", "low"),
            ("medium", "high"),
            ("low", "low"),
        ]
