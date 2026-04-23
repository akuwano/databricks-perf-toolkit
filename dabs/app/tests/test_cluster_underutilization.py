"""Tests for cluster_underutilization (3-variant) and
compilation_absolute_heavy (INFO) cards.
"""

from __future__ import annotations

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.analyzers.recommendations_registry import (
    CARDS,
    Context,
    generate_from_registry,
)
from core.models import NodeMetrics, QueryMetrics


def _qm(**overrides) -> QueryMetrics:
    base = dict(
        query_id="t",
        status="FINISHED",
        total_time_ms=120_000,
        execution_time_ms=120_000,
        task_total_time_ms=600_000,  # parallelism = 5x, below ceiling
    )
    base.update(overrides)
    return QueryMetrics(**base)


def _scan_node(
    node_id: str, local: int, non_local: int, task_total_time_ms: int = 0
) -> NodeMetrics:
    return NodeMetrics(
        node_id=node_id,
        node_name=f"Scan table_{node_id}",
        node_tag="UNKNOWN_DATA_SOURCE_SCAN_EXEC",
        local_scan_tasks=local,
        non_local_scan_tasks=non_local,
        duration_ms=task_total_time_ms,
    )


# ---------------------------------------------------------------------------
# Variant selection
# ---------------------------------------------------------------------------


class TestExternalContentionVariant:
    def test_rescheduled_10pct_fires_medium(self):
        qm = _qm()
        # 50 local, 10 non-local = 16.7% rescheduled — MEDIUM
        bi = calculate_bottleneck_indicators(qm, [_scan_node("1", 50, 10)], [], [])
        assert bi.cluster_underutilization_variant == "external_contention"
        assert bi.cluster_underutilization_severity.value == "medium"

    def test_rescheduled_40pct_fires_high(self):
        qm = _qm()
        # 6 local, 4 non-local = 40% rescheduled — HIGH
        bi = calculate_bottleneck_indicators(qm, [_scan_node("1", 6, 4)], [], [])
        assert bi.cluster_underutilization_variant == "external_contention"
        assert bi.cluster_underutilization_severity.value == "high"


class TestDriverOverheadVariant:
    def test_aqe_replan_count_triggers_driver_overhead(self):
        qm = _qm(aqe_replan_count=13)
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == "driver_overhead"

    def test_subquery_count_triggers_driver_overhead(self):
        qm = _qm(subquery_count=12)
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == "driver_overhead"

    def test_many_bhj_plus_long_exec_triggers_driver(self):
        qm = _qm(
            total_time_ms=150_000,
            execution_time_ms=150_000,
            task_total_time_ms=600_000,
            broadcast_hash_join_count=11,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == "driver_overhead"

    def test_bhj_alone_short_exec_does_not_trigger_driver(self):
        """BHJ-only signal requires exec >= 120s gate."""
        qm = _qm(
            total_time_ms=90_000,
            execution_time_ms=90_000,
            task_total_time_ms=450_000,
            broadcast_hash_join_count=11,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == "serial_plan"

    def test_two_signals_high_severity(self):
        qm = _qm(aqe_replan_count=13, subquery_count=12)
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_severity.value == "high"


class TestSerialPlanVariant:
    def test_no_special_signals_falls_through(self):
        qm = _qm()
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == "serial_plan"
        assert bi.cluster_underutilization_severity.value == "medium"


# ---------------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------------


class TestGating:
    def test_short_query_excluded(self):
        qm = _qm(total_time_ms=30_000, execution_time_ms=30_000, task_total_time_ms=100_000)
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == ""

    def test_high_parallelism_excluded(self):
        qm = _qm(task_total_time_ms=3_000_000)  # parallelism 25x
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == ""

    def test_queue_dominant_excluded(self):
        qm = _qm(queued_overload_time_ms=6000)
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        # driver_overhead (queue) card owns this, underutilization stays quiet
        assert bi.cluster_underutilization_variant == ""


# ---------------------------------------------------------------------------
# Registry card
# ---------------------------------------------------------------------------


class TestCardRegistered:
    def test_cluster_underutilization_in_registry(self):
        entries = [c for c in CARDS if c.card_id == "cluster_underutilization"]
        assert len(entries) == 1
        assert entries[0].priority_rank == 28

    def test_compilation_absolute_heavy_in_registry(self):
        entries = [c for c in CARDS if c.card_id == "compilation_absolute_heavy"]
        assert len(entries) == 1
        assert entries[0].priority_rank == 25

    def test_card_emitted_external_contention(self):
        qm = _qm()
        bi = calculate_bottleneck_indicators(qm, [_scan_node("1", 5, 5)], [], [])
        ctx = Context(indicators=bi, query_metrics=qm)
        cards, fired = generate_from_registry(ctx)
        assert "cluster_underutilization" in fired
        card = next(c for c in cards if c.root_cause_group == "cluster_underutilization")
        assert "another query" in card.likely_cause.lower()

    def test_card_emitted_driver_overhead(self):
        qm = _qm(aqe_replan_count=13, subquery_count=12)
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        ctx = Context(indicators=bi, query_metrics=qm)
        cards, fired = generate_from_registry(ctx)
        assert "cluster_underutilization" in fired
        card = next(c for c in cards if c.root_cause_group == "cluster_underutilization")
        assert "driver" in card.likely_cause.lower()

    def test_card_emitted_serial_plan(self):
        qm = _qm()
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        ctx = Context(indicators=bi, query_metrics=qm)
        cards, fired = generate_from_registry(ctx)
        assert "cluster_underutilization" in fired
        card = next(c for c in cards if c.root_cause_group == "cluster_underutilization")
        assert "topologically" in card.likely_cause.lower() or "narrow" in card.likely_cause.lower()


# ---------------------------------------------------------------------------
# compilation_absolute_heavy
# ---------------------------------------------------------------------------


class TestCompilationAbsoluteHeavy:
    def test_fires_when_compile_high_absolute_low_ratio(self):
        qm = QueryMetrics(
            total_time_ms=300_000,  # 5 min total
            compilation_time_ms=10_000,  # 10s compile = 3.3%
            execution_time_ms=290_000,
            pruned_files_count=20_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        # ratio is 3.3% — compilation_overhead must NOT fire
        assert bi.compilation_pruning_heavy is False
        # absolute heavy MUST fire
        assert bi.compilation_absolute_heavy is True

    def test_silent_when_compilation_overhead_already_fired(self):
        qm = QueryMetrics(
            total_time_ms=11_000,
            compilation_time_ms=6000,  # 55% ratio → compilation_overhead fires
            execution_time_ms=5000,
            pruned_files_count=20_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.compilation_pruning_heavy is True
        # Dedup: absolute heavy stays quiet so we don't double-alert
        assert bi.compilation_absolute_heavy is False

    def test_silent_when_below_absolute_threshold(self):
        qm = QueryMetrics(
            total_time_ms=300_000,
            compilation_time_ms=4000,  # 4s < 5s threshold
            execution_time_ms=296_000,
            pruned_files_count=20_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.compilation_absolute_heavy is False

    def test_silent_when_no_metadata_evidence(self):
        qm = QueryMetrics(
            total_time_ms=300_000,
            compilation_time_ms=10_000,
            execution_time_ms=290_000,
            pruned_files_count=100,  # below 1000
            metadata_time_ms=200,  # below 500
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.compilation_absolute_heavy is False

    def test_info_alert_is_non_actionable(self):
        qm = QueryMetrics(
            total_time_ms=300_000,
            compilation_time_ms=10_000,
            execution_time_ms=290_000,
            pruned_files_count=20_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        info_alerts = [a for a in bi.alerts if a.metric_name == "compilation_time_ms"]
        assert info_alerts, "INFO alert should be emitted"
        assert info_alerts[0].severity.value == "info"
        assert info_alerts[0].is_actionable is False


# ---------------------------------------------------------------------------
# Taxonomy registration (v5.19.0 Codex fix)
# ---------------------------------------------------------------------------


class TestTaxonomyRegistration:
    """The two new root_cause_groups must be registered in
    ``action_classify`` so LLM dedup (group-overlap) and coverage-
    category accounting see them — otherwise LLM cards on the same
    topic leak through as duplicates and the rerank treats the rule
    cards as "unclassified" (missing the diversity bonus / penalty)."""

    def test_cluster_underutilization_in_root_cause_groups(self):
        from core.action_classify import GROUP_TO_CATEGORY, ROOT_CAUSE_GROUPS

        assert "cluster_underutilization" in ROOT_CAUSE_GROUPS
        assert GROUP_TO_CATEGORY["cluster_underutilization"] == "COMPUTE"

    def test_compilation_absolute_in_root_cause_groups(self):
        from core.action_classify import GROUP_TO_CATEGORY, ROOT_CAUSE_GROUPS

        assert "compilation_absolute" in ROOT_CAUSE_GROUPS
        assert GROUP_TO_CATEGORY["compilation_absolute"] == "COMPUTE"

    def test_classifier_recognizes_cluster_underutilization_text(self):
        """LLM-generated card text that talks about effective
        parallelism / serial plans must classify into this group so
        the group-overlap dedup drops it when a rule card already
        covers it."""
        from core.action_classify import classify_root_cause_group

        text = (
            "Cluster underutilization — effective parallelism is 4.3x "
            "against a 60-second query, suggesting the serial plan is "
            "leaving the warehouse idle."
        )
        assert classify_root_cause_group(text) == "cluster_underutilization"

    def test_classifier_recognizes_compilation_absolute_text(self):
        from core.action_classify import classify_root_cause_group

        text = "Absolute heavy compile: 10 seconds on a 5-minute query."
        assert classify_root_cause_group(text) == "compilation_absolute"


# ---------------------------------------------------------------------------
# Severity: BHJ hit requires long exec (v5.19.0 Codex fix)
# ---------------------------------------------------------------------------


class TestDriverVariantSeverityBHJGate:
    """The HIGH-severity bar for the ``driver_overhead`` variant needs
    >= 2 independent driver-load signals. BHJ-only hits must ALSO be
    gated on ``exec >= 120s`` so a short-lived query with 5 broadcast
    hash joins does not get pushed to HIGH purely on plan shape."""

    def test_bhj_alone_without_long_exec_stays_medium(self):
        """AQE satisfied + BHJ present but exec < 120s → BHJ does
        NOT count as a second hit → MEDIUM."""
        qm = _qm(
            total_time_ms=60_000,
            execution_time_ms=60_000,  # below 120s gate
            task_total_time_ms=300_000,
            aqe_replan_count=5,
            broadcast_hash_join_count=5,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == "driver_overhead"
        assert bi.cluster_underutilization_severity.value == "medium"

    def test_bhj_with_long_exec_counts_as_hit(self):
        qm = _qm(
            total_time_ms=180_000,
            execution_time_ms=180_000,  # above 120s gate
            task_total_time_ms=900_000,
            aqe_replan_count=5,
            broadcast_hash_join_count=5,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.cluster_underutilization_variant == "driver_overhead"
        assert bi.cluster_underutilization_severity.value == "high"
