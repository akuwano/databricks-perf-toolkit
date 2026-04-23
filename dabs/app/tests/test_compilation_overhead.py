"""Tests for the compilation/file-pruning overhead alert and action card.

Fires when driver-side compilation dominates wall-clock time (>=30%) AND
the overhead is explained by metadata/pruning evidence (many pruned files,
high metadataTimeMs, or metadata share of compilation).

Regression case: user-reported profile with
``compilationTimeMs=6027`` and ``totalTimeMs=11021`` (55%), 28,438 pruned
files, 2,070ms metadata time — the "Optimizing query & pruning files" UI
bar on the Databricks Query Profile.
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
        total_time_ms=11021,
        compilation_time_ms=6027,
        execution_time_ms=4745,
        metadata_time_ms=2070,
        pruned_files_count=28438,
        read_files_count=885,
        read_bytes=582_946_018,
    )
    base.update(overrides)
    return QueryMetrics(**base)


def _calc(qm: QueryMetrics):
    return calculate_bottleneck_indicators(qm, [], [], [])


# ---------------------------------------------------------------------------
# Detector: _analyze_compilation_overhead
# ---------------------------------------------------------------------------


class TestDetectorFires:
    def test_real_profile_fires_high(self):
        qm = _qm()
        bi = _calc(qm)
        assert bi.compilation_pruning_heavy is True
        assert bi.compilation_time_ratio > 0.5
        assert bi.compilation_severity.name == "HIGH"
        alerts = [a for a in bi.alerts if a.category == "compilation"]
        assert alerts, "at least one compilation alert expected"
        assert any("compilation_time_ratio" == a.metric_name for a in alerts)

    def test_medium_severity_at_30_to_50_percent(self):
        qm = _qm(total_time_ms=15000, compilation_time_ms=5000)  # 33%
        bi = _calc(qm)
        assert bi.compilation_pruning_heavy is True
        assert bi.compilation_severity.name == "MEDIUM"

    def test_fires_via_metadata_only(self):
        """Heavy metadata work alone (without prunedFiles) should still fire."""
        qm = _qm(pruned_files_count=0, metadata_time_ms=2000)
        bi = _calc(qm)
        assert bi.compilation_pruning_heavy is True

    def test_fires_via_metadata_share(self):
        """Even when metadata_time_ms is small absolute but >=25% of compile."""
        qm = _qm(
            total_time_ms=8000,
            compilation_time_ms=3000,  # ratio = 37.5%
            pruned_files_count=0,
            metadata_time_ms=800,  # absolute <1000, but 800/3000 = 26.7% >= 25%
        )
        bi = _calc(qm)
        assert bi.compilation_pruning_heavy is True


class TestDetectorDoesNotFire:
    def test_ratio_below_threshold(self):
        qm = _qm(total_time_ms=100_000, compilation_time_ms=6027)  # 6%
        bi = _calc(qm)
        assert bi.compilation_pruning_heavy is False

    def test_absolute_compile_too_small(self):
        """Below 3s absolute — tiny query, don't alert even at 90% ratio."""
        qm = _qm(total_time_ms=2000, compilation_time_ms=1800)  # 90% but <3s
        bi = _calc(qm)
        assert bi.compilation_pruning_heavy is False

    def test_no_metadata_evidence(self):
        """Heavy compile but no metadata/pruning evidence — likely parse-heavy SQL,
        not file-stats driven. Detector should stay quiet."""
        qm = _qm(pruned_files_count=100, metadata_time_ms=100)
        bi = _calc(qm)
        assert bi.compilation_pruning_heavy is False

    def test_zero_total_time(self):
        qm = _qm(total_time_ms=0, compilation_time_ms=6027)
        bi = _calc(qm)
        assert bi.compilation_pruning_heavy is False


# ---------------------------------------------------------------------------
# Card registry
# ---------------------------------------------------------------------------


class TestCardRegistered:
    def test_card_in_registry_at_rank_72(self):
        entries = [c for c in CARDS if c.card_id == "compilation_overhead"]
        assert len(entries) == 1
        assert entries[0].priority_rank == 72

    def test_card_emitted_when_indicators_set(self):
        qm = _qm()
        bi = _calc(qm)
        ctx = Context(indicators=bi, query_metrics=qm)
        cards, fired = generate_from_registry(ctx)
        assert "compilation_overhead" in fired
        card = next(c for c in cards if c.root_cause_group == "compilation_overhead")
        assert card.coverage_category == "QUERY"
        assert card.expected_impact == "high"
        assert "OPTIMIZE" in card.fix_sql
        assert "VACUUM" in card.fix_sql

    def test_card_not_emitted_when_detector_silent(self):
        qm = _qm(total_time_ms=100_000, compilation_time_ms=500)
        bi = _calc(qm)
        ctx = Context(indicators=bi, query_metrics=qm)
        _, fired = generate_from_registry(ctx)
        assert "compilation_overhead" not in fired
