"""Shuffle-related bottleneck analysis.

Extracted from ``bottleneck.py`` to keep the orchestrator readable.
Responsibilities:
  * Compute ``shuffle_impact_ratio`` and raise impact alerts
  * Detect memory-inefficient shuffles (with sanity gates against
    physically impossible per-partition values)
  * Aggregate AQE-layout signals (self-repartition, spill) across all
    shuffle nodes — used downstream to distinguish "data volume /
    physical layout" from "key skew"
  * Detect AQE skew and oversized AQE partitions
"""

from ..constants import THRESHOLDS, Severity
from ..i18n import gettext as _
from ..models import BottleneckIndicators, QueryMetrics, ShuffleMetrics
from ._helpers import _add_alert


def analyze_shuffle(
    indicators: BottleneckIndicators,
    shuffle_metrics: list[ShuffleMetrics],
    query_metrics: QueryMetrics,
) -> None:
    """Populate shuffle-related fields on ``indicators`` and emit alerts."""
    # Shuffle impact
    total_shuffle_time = sum(sm.duration_ms for sm in shuffle_metrics)
    if query_metrics.task_total_time_ms > 0:
        indicators.shuffle_impact_ratio = total_shuffle_time / query_metrics.task_total_time_ms
        ratio_str = f"{indicators.shuffle_impact_ratio:.1%}"
        if indicators.shuffle_impact_ratio >= THRESHOLDS["shuffle_critical"]:
            indicators.shuffle_severity = Severity.CRITICAL
            _add_alert(
                indicators,
                severity=Severity.CRITICAL,
                category="shuffle",
                message=_("Shuffle operations account for {ratio} of total time").format(
                    ratio=ratio_str
                ),
                metric_name="shuffle_impact_ratio",
                current_value=ratio_str,
                threshold="<20%",
                recommendation=_(
                    "Consider increasing broadcast join threshold "
                    "(spark.sql.autoBroadcastJoinThreshold) to 200MB. "
                    "Expected improvement: 30-50% reduction in shuffle time"
                ),
            )
        elif indicators.shuffle_impact_ratio >= THRESHOLDS["shuffle_high"]:
            indicators.shuffle_severity = Severity.HIGH
            _add_alert(
                indicators,
                severity=Severity.HIGH,
                category="shuffle",
                message=_("Shuffle operations account for {ratio} of total time").format(
                    ratio=ratio_str
                ),
                metric_name="shuffle_impact_ratio",
                current_value=ratio_str,
                threshold="<20%",
            )

    # Shuffle memory efficiency check — aggregated alert instead of per-node.
    # Apply two sanity gates to suppress false positives:
    #   (1) skip "lightweight" shuffles where no spill occurred and
    #       sink_bytes_written is < 1 GB. These are harmless final coalesces
    #       that happen to report cumulative peak memory in the hundreds of GB.
    #   (2) suppress physically impossible values (> 10 GB/partition without
    #       any spill) — such readings indicate the per-partition formula is
    #       breaking down, not a real memory problem.
    inefficient_shuffles = [
        sm
        for sm in shuffle_metrics
        if not sm.is_memory_efficient
        and not sm.is_lightweight_shuffle
        and not (
            sm.memory_per_partition_mb > THRESHOLDS["shuffle_memory_absurd_mb"]
            and sm.sink_num_spills == 0
        )
    ]
    if inefficient_shuffles:
        worst = max(inefficient_shuffles, key=lambda s: s.memory_per_partition_mb)
        worst_mb = worst.memory_per_partition_mb
        count = len(inefficient_shuffles)

        # Severity based on worst partition size (aligned with optimization_priority)
        if worst_mb >= 2048:  # >2GB
            severity = Severity.CRITICAL
        elif worst_mb >= 512:  # >512MB
            severity = Severity.HIGH
        else:  # >128MB
            severity = Severity.MEDIUM

        message = _(
            "Shuffle memory efficiency issues in {count} node(s), "
            "worst: {worst_mb}MB/partition (Node {node_id})"
        ).format(count=count, worst_mb=f"{worst_mb:.0f}", node_id=worst.node_id)

        # Build recommendation from worst node
        recommendation = ""
        if worst.shuffle_attributes:
            attrs = ", ".join(worst.shuffle_attributes)
            optimal = (
                int(worst.peak_memory_bytes / (128 * 1024 * 1024)) or worst.partition_count * 2
            )
            recommendation = _("Consider using REPARTITION({partitions}, {attrs}) hint").format(
                partitions=optimal, attrs=attrs
            )

        _add_alert(
            indicators,
            severity=severity,
            category="shuffle",
            message=message,
            metric_name="memory_per_partition",
            current_value=f"{worst_mb:.0f}MB (worst of {count})",
            threshold="<128MB",
            recommendation=recommendation,
        )

    # Aggregate AQE-layout signals across all shuffle nodes. When AQE
    # self-repartitioned AT LEAST ONE exchange AND no shuffle spilled,
    # the workload looks like "data volume outgrew initial partitioning"
    # (a layout/volume problem AQE handled correctly), not key skew.
    _aqe_layout_ratio = 0.0
    _any_self_repartition = False
    _any_shuffle_spill = False
    _shuffle_bytes_sum = 0
    for sm in shuffle_metrics:
        _shuffle_bytes_sum += int(sm.sink_bytes_written or 0)
        if sm.sink_num_spills and sm.sink_num_spills > 0:
            _any_shuffle_spill = True
        if sm.aqe_self_repartition_count and sm.aqe_self_repartition_count > 0:
            _any_self_repartition = True
            if sm.aqe_original_num_partitions > 0 and sm.aqe_intended_num_partitions > 0:
                ratio = sm.aqe_intended_num_partitions / sm.aqe_original_num_partitions
                if ratio > _aqe_layout_ratio:
                    _aqe_layout_ratio = ratio
    indicators.aqe_self_repartition_seen = _any_self_repartition and not _any_shuffle_spill
    indicators.max_aqe_partition_growth_ratio = _aqe_layout_ratio
    indicators.shuffle_bytes_written_total = _shuffle_bytes_sum

    # AQE skew detection
    for sm in shuffle_metrics:
        if sm.aqe_skewed_partitions > 0:
            indicators.has_data_skew = True
            indicators.skewed_partitions += sm.aqe_skewed_partitions
        # Check average partition size
        if sm.avg_aqe_partition_size_mb > THRESHOLDS["aqe_partition_size_warning_mb"]:
            size_str = f"{sm.avg_aqe_partition_size_mb:.0f}MB"
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="shuffle",
                message=_(
                    "Average partition size for AQEShuffleRead is large ({size} > 128MB)"
                ).format(size=size_str),
                metric_name="avg_aqe_partition_size",
                current_value=size_str,
                threshold="<128MB",
            )

    if indicators.has_data_skew:
        _add_alert(
            indicators,
            severity=Severity.HIGH,
            category="shuffle",
            message=_("Data skew detected ({count} partitions)").format(
                count=indicators.skewed_partitions
            ),
            metric_name="skewed_partitions",
            current_value=str(indicators.skewed_partitions),
            threshold="0",
            recommendation=_("Please verify spark.sql.adaptive.skewJoin.enabled=true"),
        )
