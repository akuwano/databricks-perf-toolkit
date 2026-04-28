"""Bottleneck indicator calculation.

Phase 1 (LLM-driven): Existing rule-based alerts are maintained for backward
compatibility. Additionally, factual signals (BottleneckSignal) are extracted
without severity judgment — LLM uses these to determine actual severity.
"""

from ..constants import THRESHOLDS, JoinType, Severity
from ..i18n import gettext as _
from ..models import (
    BottleneckIndicators,
    BottleneckSignal,
    CloudStorageMetrics,
    JoinInfo,
    NodeMetrics,
    PhotonBlocker,
    QueryMetrics,
    ShuffleMetrics,
    SpillOperatorInfo,
)
from ..sql_patterns import analyze_cte_multi_references, collect_non_sargable_filter_functions
from ..utils import format_bytes
from ._helpers import _add_alert
from .hash_analysis import (
    detect_duplicate_groupby,
    extract_hash_resize_hotspots,
    generate_hash_resize_alerts,
)
from .shuffle_analysis import analyze_shuffle


def _apply_sql_pattern_alerts(
    indicators: BottleneckIndicators,
    node_metrics: list[NodeMetrics],
    query_metrics: QueryMetrics,
) -> None:
    """Non-sargable filters (metadata) and multi-reference CTEs (query text)."""
    funcs = collect_non_sargable_filter_functions(node_metrics)
    if funcs:
        _add_alert(
            indicators,
            severity=Severity.MEDIUM,
            category="io",
            message=_(
                "Function applied to column in filter condition ({func}) — prevents Data Skipping and predicate pushdown"
            ).format(func=funcs[0]),
            metric_name="non_sargable_filter",
            current_value=", ".join(funcs),
            threshold=_("sargable range/literal predicates"),
            recommendation=_(
                'Rewrite as range condition: WHERE col >= "2024-01-01" instead of WHERE YEAR(col) = 2024'
            ),
        )

    for name, total_occ in analyze_cte_multi_references(query_metrics.query_text or ""):
        refs = total_occ - 1
        sev = (
            Severity.HIGH
            if refs >= 3 and query_metrics.read_bytes > THRESHOLDS["shuffle_high_volume_bytes"]
            else Severity.MEDIUM
        )
        _add_alert(
            indicators,
            severity=sev,
            category="query_pattern",
            message=_(
                'CTE "{name}" is referenced {n} times — Spark may re-execute it each time'
            ).format(name=name, n=total_occ),
            metric_name="multi_reference_cte",
            current_value=str(total_occ),
            threshold=_("≥2 references beyond WITH"),
            recommendation=_(
                "Do NOT rely on CREATE TEMP VIEW — it is a catalog alias and does NOT guarantee "
                "single materialization. Persist the shared result with CTAS / Delta, or rewrite "
                "the query so the CTE body runs once. Confirm reuse via ReusedExchange in EXPLAIN."
            ),
        )


import re as _re

_RE_CAST_CALL = _re.compile(r"\bCAST\s*\(", _re.IGNORECASE)


def _collect_join_key_casts(
    node_metrics: list[NodeMetrics],
) -> list[tuple[str, str]]:
    """Return ``[(node_id, key_expr)]`` for JOIN keys wrapped in CAST(...).

    Reads ``NodeMetrics.join_keys_left`` / ``join_keys_right`` which are
    populated from profile JSON ``LEFT_KEYS`` / ``RIGHT_KEYS`` metadata
    on join nodes. A CAST wrapper on either side blocks predicate
    pushdown and can inflate hash-table memory — a CRITICAL-severity
    anti-pattern. This complements the EXPLAIN-based detector in
    ``explain_analysis.py`` so the alert fires even without EXPLAIN.
    """
    sites: list[tuple[str, str]] = []
    for nm in node_metrics:
        if "join" not in (nm.node_name or "").lower():
            continue
        for key in (nm.join_keys_left or []) + (nm.join_keys_right or []):
            if key and _RE_CAST_CALL.search(key):
                sites.append((nm.node_id or "?", key))
    return sites


# Threshold: aggregate node peak memory above this triggers DECIMAL review.
# 100 GB picks up the Q23-class hotspots (1.3 TB peak agg) without firing
# on routine medium aggs (~10 GB). Tunable.
_DECIMAL_AGG_PEAK_MEMORY_THRESHOLD_BYTES = 100 * (1024**3)
_DECIMAL_AGG_ARITHMETIC_PATTERN = _re.compile(r"[*+\-/]")


def _collect_decimal_heavy_aggregate_examples(
    node_metrics: list[NodeMetrics],
) -> list[tuple[str, str]]:
    """Return ``[(node_id, expr_excerpt)]`` for nodes that combine
    (a) large aggregate peak memory and (b) arithmetic in at least one
    aggregate expression.

    Empty list when no node qualifies — caller treats that as "not
    detected" and skips the alert.
    """
    found: list[tuple[str, str]] = []
    for nm in node_metrics:
        if (nm.peak_memory_bytes or 0) < _DECIMAL_AGG_PEAK_MEMORY_THRESHOLD_BYTES:
            continue
        if not nm.aggregate_expressions:
            continue
        for expr in nm.aggregate_expressions:
            if not expr:
                continue
            if _DECIMAL_AGG_ARITHMETIC_PATTERN.search(expr):
                # Truncate long expressions to keep alert/card text terse
                excerpt = expr.strip()
                if len(excerpt) > 120:
                    excerpt = excerpt[:117] + "..."
                found.append((nm.node_id or "?", excerpt))
                break  # one example per node is enough
    return found


def _apply_decimal_heavy_aggregate_alert(
    indicators: BottleneckIndicators,
    node_metrics: list[NodeMetrics],
) -> None:
    """Emit a MEDIUM alert when a heavy aggregate runs arithmetic on
    columns that may be wide DECIMAL.

    Rationale: ``SUM(quantity * price)`` and friends widen DECIMAL(38, x)
    to DECIMAL(38, 18) implicitly, inflating per-row CPU and hash-table
    memory. The fix is data-driven (DESCRIBE TABLE → narrow type or
    INT/BIGINT) so the card stays in "investigation" mode rather than
    proposing a specific ALTER. V5 caught this for Q23 via LLM; V6
    canonical-schema compression dropped it. Promoting to a rule-based
    indicator gives V6 a stable hook so the canonical Report carries
    the recommendation regardless of LLM compression decisions.
    """
    examples = _collect_decimal_heavy_aggregate_examples(node_metrics)
    if not examples:
        return
    indicators.decimal_heavy_aggregate = True
    indicators.decimal_heavy_aggregate_examples = examples[:3]
    sample = examples[0][1]
    _add_alert(
        indicators,
        severity=Severity.MEDIUM,
        category="aggregation",
        message=_(
            "Heavy aggregate with arithmetic on numeric columns — verify DECIMAL "
            "precision (sample: Node #{nid}: `{expr}`)"
        ).format(nid=examples[0][0], expr=sample),
        metric_name="decimal_heavy_aggregate",
        current_value=str(len(examples)),
        threshold=_("integer-typed or narrow DECIMAL"),
        recommendation=_(
            "Use DESCRIBE TABLE to confirm precision/scale of the columns in the "
            "aggregate. If they are DECIMAL(38, 0) but only store integer values, "
            "migrating to INT/BIGINT removes the implicit widening. If wider "
            "precision is required, narrow it (e.g., DECIMAL(18, 2)) instead of "
            "the default 38 to shrink hash-table memory."
        ),
    )


def _apply_join_key_cast_alert(
    indicators: BottleneckIndicators,
    node_metrics: list[NodeMetrics],
) -> None:
    """Emit a CRITICAL alert when profile JSON reveals CAST(...) on a
    JOIN key. Mirrors the message/category of the EXPLAIN-based
    detector (``explain_analysis.py::_apply_explain_v2_signals``) so
    the two code paths remain consistent — the EXPLAIN-side detector
    subsequently skips when this flag is already set, preventing
    double-fires when both sources detect the same issue.

    Severity rationale: implicit CAST on a join key defeats DFP /
    runtime filters entirely and can dramatically inflate hash-table
    memory (especially with DECIMAL widening). Although correctness
    is preserved, the performance cliff is severe enough that we
    surface it at CRITICAL so it outranks generic perf signals.
    """
    sites = _collect_join_key_casts(node_metrics)
    if not sites:
        return

    indicators.implicit_cast_on_join_key = True
    # Show up to 3 examples plus "+N more" suffix for brevity.
    examples = [key[:80] for _nid, key in sites[:3]]
    extra = f" (+{len(sites) - 3} more)" if len(sites) > 3 else ""
    _add_alert(
        indicators,
        severity=Severity.CRITICAL,
        category="join",
        message=_(
            "Implicit CAST detected on JOIN key(s): {examples}{extra}. "
            "This usually means the join columns have mismatched data "
            "types at the source, which blocks predicate pushdown and "
            "can inflate hash-table memory (especially with DECIMAL)."
        ).format(examples=", ".join(examples), extra=extra),
        metric_name="implicit_cast_on_join_key",
        current_value=str(len(sites)),
        threshold="0",
        recommendation=_(
            "Align JOIN key data types at the source tables (for example, "
            "use the same INTEGER / BIGINT on both sides instead of mixing "
            "DECIMAL with INTEGER). If a type change is not possible, cast "
            "once on the smaller side only and verify the join still "
            "pushes down."
        ),
    )


def _compute_scan_impact_ratio(
    node_metrics: list[NodeMetrics], query_metrics: QueryMetrics
) -> float:
    """Share of task time spent in scan operators.

    Used to gate IO-related alerts/cards so that compute-bound queries
    (dominated by Generate / Aggregate / JSON parsing) do not surface
    irrelevant pruning/cache recommendations.
    """
    if query_metrics.task_total_time_ms <= 0:
        return 0.0
    scan_time = sum(nm.duration_ms for nm in node_metrics if "scan" in nm.node_name.lower())
    return scan_time / query_metrics.task_total_time_ms


def _gate_io_severity(requested: Severity, scan_impact_ratio: float) -> Severity:
    """Apply the scan-impact two-step gate to an IO-related alert severity.

    - scan >= scan_impact_dominant (25%) → pass through unchanged
    - scan >= scan_impact_mid (10%)      → demote CRITICAL/HIGH to MEDIUM
    - scan <  scan_impact_mid            → demote everything to INFO
    """
    if scan_impact_ratio >= THRESHOLDS["scan_impact_dominant"]:
        return requested
    if scan_impact_ratio >= THRESHOLDS["scan_impact_mid"]:
        if requested in (Severity.CRITICAL, Severity.HIGH):
            return Severity.MEDIUM
        return requested
    return Severity.INFO


def _gate_photon_severity(requested: Severity, task_total_time_ms: int) -> Severity:
    """Gate Photon alerts by query duration (tiny/short queries are noise)."""
    if task_total_time_ms >= THRESHOLDS["photon_small_query_ms"]:
        return requested
    if task_total_time_ms >= THRESHOLDS["photon_tiny_query_ms"]:
        if requested in (Severity.CRITICAL, Severity.HIGH):
            return Severity.MEDIUM
        return requested
    return Severity.INFO


_COMPILATION_RATIO_MEDIUM = 0.30
_COMPILATION_RATIO_HIGH = 0.50
_COMPILATION_ABS_MIN_MS = 3000
_COMPILATION_PRUNED_FILES_MIN = 10_000
_COMPILATION_METADATA_MS_MIN = 1000
_COMPILATION_METADATA_SHARE_MIN = 0.25

_DRIVER_OVERHEAD_QUEUE_ABS_MS = 5000
_DRIVER_OVERHEAD_QUEUE_RATIO = 0.10
_DRIVER_OVERHEAD_SCHED_ABS_MS = 3000
_DRIVER_OVERHEAD_SCHED_RATIO = 0.15
_DRIVER_OVERHEAD_COMBINED_ABS_MS = 5000
_DRIVER_OVERHEAD_COMBINED_RATIO = 0.10
_DRIVER_OVERHEAD_HIGH_ABS_MS = 30000
_DRIVER_OVERHEAD_HIGH_RATIO = 0.30

_UNDERUTIL_MIN_EXEC_MS = 45_000
_UNDERUTIL_PARALLELISM_CEILING = 20.0
_UNDERUTIL_EXTERNAL_RESCHED_MIN = 0.10
_UNDERUTIL_EXTERNAL_RESCHED_HIGH = 0.30
_UNDERUTIL_DRIVER_AQE_MIN = 5
_UNDERUTIL_DRIVER_SUBQUERY_MIN = 3
_UNDERUTIL_DRIVER_BHJ_MIN = 5
_UNDERUTIL_DRIVER_BHJ_LONG_EXEC_MS = 120_000

_COMPILE_ABSOLUTE_HEAVY_MS = 5000
_COMPILE_ABSOLUTE_PRUNED_FILES_MIN = 1000
_COMPILE_ABSOLUTE_METADATA_MS_MIN = 500


def _analyze_compilation_overhead(
    indicators: BottleneckIndicators, query_metrics: QueryMetrics
) -> None:
    """Detect driver-side compilation/pruning overhead.

    Fires when SQL parsing + Catalyst + Delta log replay + file-level
    stats pruning dominate wall-clock and the time is explained by
    metadata/pruning evidence. Typical root cause is small-file
    proliferation or long-unvacuumed Delta log.
    """
    total_ms = query_metrics.total_time_ms
    compile_ms = query_metrics.compilation_time_ms
    if total_ms <= 0 or compile_ms < _COMPILATION_ABS_MIN_MS:
        return
    ratio = compile_ms / total_ms
    indicators.compilation_time_ratio = ratio
    if ratio < _COMPILATION_RATIO_MEDIUM:
        return

    metadata_ms = query_metrics.metadata_time_ms
    pruned_files = query_metrics.pruned_files_count
    metadata_share = metadata_ms / compile_ms if compile_ms > 0 else 0.0
    evidence = (
        pruned_files >= _COMPILATION_PRUNED_FILES_MIN
        or metadata_ms >= _COMPILATION_METADATA_MS_MIN
        or metadata_share >= _COMPILATION_METADATA_SHARE_MIN
    )
    if not evidence:
        return
    indicators.compilation_pruning_heavy = True
    severity = Severity.HIGH if ratio >= _COMPILATION_RATIO_HIGH else Severity.MEDIUM
    indicators.compilation_severity = severity
    _add_alert(
        indicators,
        severity=severity,
        category="compilation",
        message=_(
            "Compilation/file pruning dominates execution ({pct:.0%} of total, {secs:.1f}s)"
        ).format(pct=ratio, secs=compile_ms / 1000),
        metric_name="compilation_time_ratio",
        current_value=f"{ratio:.0%} ({compile_ms / 1000:.1f}s)",
        threshold="<30%",
        recommendation=_(
            "OPTIMIZE to compact small files; VACUUM to shorten Delta log; "
            "enable Predictive Optimization; re-run to rule out cold-cache"
        ),
    )


def _analyze_driver_overhead(indicators: BottleneckIndicators, query_metrics: QueryMetrics) -> None:
    """Detect driver-side wait: queue + scheduling + waiting-for-compute.

    Source of truth:
      - queue_wait_ms = queued_provisioning_time_ms + queued_overload_time_ms
      - scheduling_compute_wait_ms = queryCompilationStartTimestamp
          - (overloadingQueueStartTimestamp
             or provisioningQueueStartTimestamp
             or queryStartTimeMs)
          - queue_wait_ms
        (residual pre-compile gap after subtracting the explicit queue)

    Fires when any of:
      1. queue ≥ 5s OR ≥10% of total
      2. scheduling+compute-wait ≥ 3s OR ≥15% of total
      3. combined ≥ 5s AND ≥10% of total

    Severity HIGH at ≥30s absolute or ≥30% ratio; MEDIUM otherwise.
    """
    total_ms = query_metrics.total_time_ms
    if total_ms <= 0:
        return

    queue_ms = query_metrics.queued_provisioning_time_ms + query_metrics.queued_overload_time_ms
    queue_start = (
        query_metrics.overloading_queue_start_ts
        or query_metrics.provisioning_queue_start_ts
        or query_metrics.query_start_time_ms
    )
    compile_start = query_metrics.query_compilation_start_ts
    pre_compile_gap = 0
    if compile_start and queue_start and compile_start > queue_start:
        pre_compile_gap = compile_start - queue_start
    sched_compute_ms = max(0, pre_compile_gap - queue_ms)

    indicators.queue_wait_ms = queue_ms
    indicators.scheduling_compute_wait_ms = sched_compute_ms
    indicators.driver_overhead_ms = queue_ms + sched_compute_ms
    indicators.driver_overhead_ratio = indicators.driver_overhead_ms / total_ms

    queue_ratio = queue_ms / total_ms
    sched_ratio = sched_compute_ms / total_ms
    combined_ratio = indicators.driver_overhead_ratio

    queue_hit = (
        queue_ms >= _DRIVER_OVERHEAD_QUEUE_ABS_MS or queue_ratio >= _DRIVER_OVERHEAD_QUEUE_RATIO
    )
    sched_hit = (
        sched_compute_ms >= _DRIVER_OVERHEAD_SCHED_ABS_MS
        or sched_ratio >= _DRIVER_OVERHEAD_SCHED_RATIO
    )
    combined_hit = (
        indicators.driver_overhead_ms >= _DRIVER_OVERHEAD_COMBINED_ABS_MS
        and combined_ratio >= _DRIVER_OVERHEAD_COMBINED_RATIO
    )
    if not (queue_hit or sched_hit or combined_hit):
        return

    if (
        indicators.driver_overhead_ms >= _DRIVER_OVERHEAD_HIGH_ABS_MS
        or combined_ratio >= _DRIVER_OVERHEAD_HIGH_RATIO
    ):
        severity = Severity.HIGH
    else:
        severity = Severity.MEDIUM
    indicators.driver_overhead_severity = severity
    _add_alert(
        indicators,
        severity=severity,
        category="driver_overhead",
        message=_(
            "Driver overhead dominates execution "
            "(queue={q:.1f}s, sched/compute-wait={s:.1f}s, total={t:.1f}s = {pct:.0%})"
        ).format(
            q=queue_ms / 1000,
            s=sched_compute_ms / 1000,
            t=indicators.driver_overhead_ms / 1000,
            pct=combined_ratio,
        ),
        metric_name="driver_overhead_ms",
        current_value=f"{indicators.driver_overhead_ms / 1000:.1f}s ({combined_ratio:.0%})",
        threshold="<5s or <10% of total",
        recommendation=_(
            "Queue-heavy: use Serverless warm pools / extend auto-stop / "
            "raise max clusters. Scheduling-heavy: reduce concurrent queries "
            "and review cluster size."
        ),
    )


def _analyze_cluster_underutilization(
    indicators: BottleneckIndicators, query_metrics: QueryMetrics
) -> None:
    """Detect low cluster utilization with a 3-way variant classifier.

    Fires when:
      - execution_time_ms >= 60s       (short queries excluded)
      - effective_parallelism < 20     (Medium warehouse proxy threshold)
      - no queue wait                  (queue is the driver_overhead card's turf)

    Variant (evaluated in order, first match wins):
      1. external_contention — rescheduled_scan_ratio >= 10%
      2. driver_overhead     — aqe_replan_count >= 5 OR subquery_count >= 3
                                OR (broadcast_hash_join_count >= 5 AND exec >= 120s)
      3. serial_plan         — fallback
    """
    exec_ms = query_metrics.execution_time_ms
    task_ms = query_metrics.task_total_time_ms
    if exec_ms <= 0 or task_ms <= 0:
        return
    eff_par = task_ms / exec_ms
    # Always populate the indicator (useful for signal extraction / UI)
    # even when the card's firing gates don't trip.
    indicators.effective_parallelism = eff_par
    if exec_ms < _UNDERUTIL_MIN_EXEC_MS:
        return
    if eff_par >= _UNDERUTIL_PARALLELISM_CEILING:
        return
    # Skip if queue-dominated — driver_overhead already owns that story.
    if indicators.queue_wait_ms >= _DRIVER_OVERHEAD_QUEUE_ABS_MS:
        return

    resched = indicators.rescheduled_scan_ratio or 0.0
    if resched >= _UNDERUTIL_EXTERNAL_RESCHED_MIN:
        variant = "external_contention"
        severity = Severity.HIGH if resched >= _UNDERUTIL_EXTERNAL_RESCHED_HIGH else Severity.MEDIUM
    elif (
        query_metrics.aqe_replan_count >= _UNDERUTIL_DRIVER_AQE_MIN
        or query_metrics.subquery_count >= _UNDERUTIL_DRIVER_SUBQUERY_MIN
        or (
            query_metrics.broadcast_hash_join_count >= _UNDERUTIL_DRIVER_BHJ_MIN
            and exec_ms >= _UNDERUTIL_DRIVER_BHJ_LONG_EXEC_MS
        )
    ):
        variant = "driver_overhead"
        # Count the same gated conditions as the detection branch —
        # specifically, BHJ counts only when combined with long exec.
        # Without the long-exec gate, a short-lived query with 5 BHJs
        # would be pushed to HIGH purely on plan shape, which is
        # stronger than the detection gate actually allows.
        aqe_hit = query_metrics.aqe_replan_count >= _UNDERUTIL_DRIVER_AQE_MIN
        subq_hit = query_metrics.subquery_count >= _UNDERUTIL_DRIVER_SUBQUERY_MIN
        bhj_hit = (
            query_metrics.broadcast_hash_join_count >= _UNDERUTIL_DRIVER_BHJ_MIN
            and exec_ms >= _UNDERUTIL_DRIVER_BHJ_LONG_EXEC_MS
        )
        hits = sum([aqe_hit, subq_hit, bhj_hit])
        severity = Severity.HIGH if hits >= 2 else Severity.MEDIUM
    else:
        variant = "serial_plan"
        severity = Severity.MEDIUM

    indicators.cluster_underutilization_variant = variant
    indicators.cluster_underutilization_severity = severity
    variant_label = {
        "external_contention": _("external CPU contention"),
        "driver_overhead": _("driver overhead"),
        "serial_plan": _("serial plan topology"),
    }[variant]
    _add_alert(
        indicators,
        severity=severity,
        category="cluster",
        message=_(
            "Cluster underutilized (effective parallelism {par:.1f}x, exec {secs:.0f}s) — {v}"
        ).format(par=eff_par, secs=exec_ms / 1000, v=variant_label),
        metric_name="effective_parallelism",
        current_value=f"{eff_par:.1f}x",
        threshold="~20x or higher",
        recommendation=_(
            "external_contention: isolate this query on another warehouse or "
            "stagger concurrent loads. driver_overhead: simplify the plan "
            "(fewer subqueries / broadcasts) or upgrade driver capacity. "
            "serial_plan: use REPARTITION hints or pre-aggregate to widen "
            "stages."
        ),
    )


def _analyze_compilation_absolute_heavy(
    indicators: BottleneckIndicators, query_metrics: QueryMetrics
) -> None:
    """Advisory INFO alert for absolute-heavy compilation even when ratio
    is low (e.g. a 5-minute query that spends 10s in compile).

    Guard: skip when the existing compilation_overhead card already fired
    (avoid double-alerting) and when no metadata/pruning evidence exists.
    """
    if indicators.compilation_pruning_heavy:
        return
    if query_metrics.compilation_time_ms < _COMPILE_ABSOLUTE_HEAVY_MS:
        return
    evidence = (
        query_metrics.pruned_files_count >= _COMPILE_ABSOLUTE_PRUNED_FILES_MIN
        or query_metrics.metadata_time_ms >= _COMPILE_ABSOLUTE_METADATA_MS_MIN
    )
    if not evidence:
        return
    indicators.compilation_absolute_heavy = True
    _add_alert(
        indicators,
        severity=Severity.INFO,
        category="compilation",
        message=_(
            "Compilation absolute time is high ({secs:.1f}s) — although ratio "
            "is small, this often indicates small-files or long Delta log on "
            "the scanned table(s)."
        ).format(secs=query_metrics.compilation_time_ms / 1000),
        metric_name="compilation_time_ms",
        current_value=f"{query_metrics.compilation_time_ms / 1000:.1f}s",
        # The alert FIRES when compile time is >= 5s; ``threshold`` is
        # conventionally the value the metric should stay under, so
        # express the healthy range as "< 5s absolute". Keeping it in
        # that direction so the dashboard chrome (which assumes
        # "current exceeds threshold") renders correctly.
        threshold="< 5s absolute",
        recommendation=_(
            "Schedule OPTIMIZE + VACUUM maintenance on the target table(s); "
            "enable Predictive Optimization for UC-managed tables."
        ),
        is_actionable=False,  # INFO-level advisory
    )


def calculate_bottleneck_indicators(
    query_metrics: QueryMetrics,
    node_metrics: list[NodeMetrics],
    shuffle_metrics: list[ShuffleMetrics],
    join_info: list[JoinInfo],
) -> BottleneckIndicators:
    """Calculate bottleneck indicators based on dbsql_tuning.md thresholds."""
    indicators = BottleneckIndicators()

    # Scan impact ratio — computed first so downstream alert emitters
    # can gate their severity on whether scan actually dominates.
    indicators.scan_impact_ratio = _compute_scan_impact_ratio(node_metrics, query_metrics)

    # Profile-only CAST-on-JOIN-key detector (v5.16.21). Fires a
    # CRITICAL alert when NodeMetrics.join_keys_left/right contain a
    # CAST(...) wrapper. The EXPLAIN-based detector in
    # explain_analysis.py will later observe indicators.implicit_cast_on_join_key
    # and skip re-firing the same alert.
    _apply_join_key_cast_alert(indicators, node_metrics)

    # 2026-04-26: Heavy aggregate + arithmetic → DECIMAL review prompt.
    # Promotes the V5-only LLM-driven DECIMAL recommendation to a rule
    # so V6 canonical_schema compression cannot drop it. Card lives at
    # priority rank 48 in the registry.
    _apply_decimal_heavy_aggregate_alert(indicators, node_metrics)

    # Cache hit ratio
    # Note: Per tuning guide section 6, cache hit ratio should be evaluated as a trend
    # across repeated runs, not a single execution. Low cache on first run is expected.
    if query_metrics.read_bytes > 0:
        indicators.cache_hit_ratio = query_metrics.read_cache_bytes / query_metrics.read_bytes
        ratio_str = f"{indicators.cache_hit_ratio:.1%}"
        if indicators.cache_hit_ratio >= THRESHOLDS["cache_hit_high"]:
            indicators.cache_severity = Severity.OK
        elif indicators.cache_hit_ratio >= THRESHOLDS["cache_hit_medium"]:
            indicators.cache_severity = Severity.MEDIUM
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="cache",
                message=_("Cache hit ratio is moderate ({ratio})").format(ratio=ratio_str),
                metric_name="cache_hit_ratio",
                current_value=ratio_str,
                threshold=">80%",
                is_actionable=True,
            )
        else:
            # Changed from CRITICAL to INFO - low cache on single run is often just cold cache
            indicators.cache_severity = Severity.INFO
            _add_alert(
                indicators,
                severity=Severity.INFO,
                category="cache",
                message=_(
                    "Cache hit ratio is low ({ratio}) - likely cold run, validate on re-execution"
                ).format(ratio=ratio_str),
                metric_name="cache_hit_ratio",
                current_value=ratio_str,
                threshold=">80%",
                recommendation=_("Re-run query to verify cache effect before scaling up"),
                is_actionable=False,  # Info-level, not immediately actionable
            )

    # Remote read ratio (new)
    if query_metrics.read_bytes > 0:
        indicators.remote_read_ratio = query_metrics.read_remote_bytes / query_metrics.read_bytes
        ratio_str = f"{indicators.remote_read_ratio:.1%}"
        if indicators.remote_read_ratio >= THRESHOLDS["remote_read_critical"]:
            gated = _gate_io_severity(Severity.HIGH, indicators.scan_impact_ratio)
            indicators.remote_read_severity = gated
            _add_alert(
                indicators,
                severity=gated,
                category="io",
                message=_("Remote read ratio is very high ({ratio})").format(ratio=ratio_str),
                metric_name="remote_read_ratio",
                current_value=ratio_str,
                threshold="<80%",
                recommendation=_("Consider enabling disk cache or scaling up cluster"),
            )
        elif indicators.remote_read_ratio >= THRESHOLDS["remote_read_high"]:
            gated = _gate_io_severity(Severity.MEDIUM, indicators.scan_impact_ratio)
            indicators.remote_read_severity = gated
            _add_alert(
                indicators,
                severity=gated,
                category="io",
                message=_("Remote read ratio is high ({ratio})").format(ratio=ratio_str),
                metric_name="remote_read_ratio",
                current_value=ratio_str,
                threshold="<80%",
            )

    # Photon efficiency
    if query_metrics.task_total_time_ms > 0:
        indicators.photon_ratio = (
            query_metrics.photon_total_time_ms / query_metrics.task_total_time_ms
        )
        ratio_str = f"{indicators.photon_ratio:.1%}"
        if indicators.photon_ratio >= THRESHOLDS["photon_high"]:
            indicators.photon_severity = Severity.OK
        elif indicators.photon_ratio >= THRESHOLDS["photon_medium"]:
            gated = _gate_photon_severity(Severity.MEDIUM, query_metrics.task_total_time_ms)
            indicators.photon_severity = gated
            _add_alert(
                indicators,
                severity=gated,
                category="photon",
                message=_("Photon utilization has room for improvement ({ratio})").format(
                    ratio=ratio_str
                ),
                metric_name="photon_ratio",
                current_value=ratio_str,
                threshold=">80%",
            )
        else:
            gated = _gate_photon_severity(Severity.CRITICAL, query_metrics.task_total_time_ms)
            indicators.photon_severity = gated if gated != Severity.INFO else Severity.INFO
            _add_alert(
                indicators,
                severity=gated,
                category="photon",
                message=_("Photon utilization is low ({ratio})").format(ratio=ratio_str),
                metric_name="photon_ratio",
                current_value=ratio_str,
                threshold=">80%",
                recommendation=_(
                    "Consider setting spark.sql.join.preferSortMergeJoin=false if Sort-Merge joins are used. "
                    "Expected improvement: 2-4x faster with Photon-enabled joins"
                ),
            )

    # Spill analysis - aggregate total and identify top spill operators
    total_spill = query_metrics.spill_to_disk_bytes

    # Collect nodes with spill
    spill_nodes = [(nm, nm.spill_bytes) for nm in node_metrics if nm.spill_bytes > 0]
    spill_nodes.sort(key=lambda x: x[1], reverse=True)

    # Calculate total spill from nodes
    total_node_spill = sum(s[1] for s in spill_nodes)
    total_spill = max(total_spill, total_node_spill)

    # Build spill operator info (top 5)
    if total_spill > 0:
        for nm, spill in spill_nodes[:5]:
            spill_share = (spill / total_spill * 100) if total_spill > 0 else 0
            indicators.spill_operators.append(
                SpillOperatorInfo(
                    node_id=nm.node_id,
                    node_name=nm.node_name,
                    spill_bytes=spill,
                    peak_memory_bytes=nm.peak_memory_bytes,
                    rows_processed=nm.rows_num or nm.rows_scanned,
                    spill_share_percent=spill_share,
                )
            )

    indicators.spill_bytes = total_spill
    spill_gb = total_spill / (1024**3)
    spill_size_str = f"{spill_gb:.2f} GB"

    if spill_gb >= THRESHOLDS["spill_critical_gb"]:
        indicators.spill_severity = Severity.CRITICAL
        _add_alert(
            indicators,
            severity=Severity.CRITICAL,
            category="spill",
            message=_("Significant disk spill is occurring ({size})").format(size=spill_size_str),
            metric_name="spill_to_disk_bytes",
            current_value=spill_size_str,
            threshold="<1GB",
            recommendation=_(
                "Memory configuration and partition strategy need review. "
                "Consider expanding cluster size (increasing worker nodes). "
                "Expected improvement: 2-5x faster after eliminating spill"
            ),
        )
    elif spill_gb >= THRESHOLDS["spill_high_gb"]:
        indicators.spill_severity = Severity.HIGH
        _add_alert(
            indicators,
            severity=Severity.HIGH,
            category="spill",
            message=_("Disk spill is occurring ({size})").format(size=spill_size_str),
            metric_name="spill_to_disk_bytes",
            current_value=spill_size_str,
            threshold="0",
            recommendation=_("Please verify spark.sql.adaptive.skewJoin.enabled=true"),
        )
    elif total_spill > 0:
        indicators.spill_severity = Severity.MEDIUM
        _add_alert(
            indicators,
            severity=Severity.MEDIUM,
            category="spill",
            message=_("Minor disk spill is occurring ({size})").format(
                size=format_bytes(total_spill)
            ),
            metric_name="spill_to_disk_bytes",
            current_value=format_bytes(total_spill),
            threshold="0",
        )

    # Filter efficiency (file-based pruning rate)
    total_files = query_metrics.read_files_count + query_metrics.pruned_files_count
    if total_files > 0:
        indicators.filter_rate = query_metrics.pruned_files_count / total_files
        ratio_str = f"{indicators.filter_rate:.1%}"
        if indicators.filter_rate < THRESHOLDS["filter_low"]:
            gated = _gate_io_severity(Severity.HIGH, indicators.scan_impact_ratio)
            indicators.filter_severity = gated
            _add_alert(
                indicators,
                severity=gated,
                category="io",
                message=_("File pruning efficiency is low ({ratio})").format(ratio=ratio_str),
                metric_name="filter_rate",
                current_value=ratio_str,
                threshold=">20%",
                recommendation=_("Please check Liquid Clustering configuration"),
            )

    # Bytes pruning efficiency (new)
    total_bytes = query_metrics.read_bytes + query_metrics.pruned_bytes
    if total_bytes > 0:
        indicators.bytes_pruning_ratio = query_metrics.pruned_bytes / total_bytes
        ratio_str = f"{indicators.bytes_pruning_ratio:.1%}"
        if indicators.bytes_pruning_ratio >= THRESHOLDS["bytes_pruning_good"]:
            indicators.bytes_pruning_severity = Severity.OK
        elif indicators.bytes_pruning_ratio >= THRESHOLDS["bytes_pruning_low"]:
            indicators.bytes_pruning_severity = Severity.MEDIUM
        else:
            gated = _gate_io_severity(Severity.HIGH, indicators.scan_impact_ratio)
            indicators.bytes_pruning_severity = gated
            _add_alert(
                indicators,
                severity=gated,
                category="io",
                message=_("Bytes pruning efficiency is low ({ratio})").format(ratio=ratio_str),
                metric_name="bytes_pruning_ratio",
                current_value=ratio_str,
                threshold=">50%",
            )

    # Predictive I/O metrics (new)
    indicators.data_filters_batches_skipped = sum(
        nm.data_filters_batches_skipped for nm in node_metrics
    )
    indicators.data_filters_rows_skipped = sum(nm.data_filters_rows_skipped for nm in node_metrics)

    # Scan locality metrics (Verbose mode only, per tuning guide section 6.3)
    indicators.local_scan_tasks_total = sum(nm.local_scan_tasks for nm in node_metrics)
    indicators.non_local_scan_tasks_total = sum(nm.non_local_scan_tasks for nm in node_metrics)
    total_scan_tasks = indicators.local_scan_tasks_total + indicators.non_local_scan_tasks_total
    if total_scan_tasks > 0:
        indicators.rescheduled_scan_ratio = indicators.non_local_scan_tasks_total / total_scan_tasks
        ratio_str = f"{indicators.rescheduled_scan_ratio:.1%}"
        # Serverless: scan locality is not user-controllable, lower severity
        is_serverless = query_metrics.query_typename == "LakehouseSqlQuery"
        if indicators.rescheduled_scan_ratio >= THRESHOLDS["scan_locality_critical"]:
            sev = Severity.MEDIUM if is_serverless else Severity.HIGH
            indicators.rescheduled_scan_severity = sev
            _add_alert(
                indicators,
                severity=sev,
                category="cluster",
                message=_(
                    "Rescheduled scan ratio is high ({ratio}) - scan locality degradation detected"
                ).format(ratio=ratio_str),
                metric_name="rescheduled_scan_ratio",
                current_value=ratio_str,
                threshold="<5%",
                recommendation=_(
                    "Check per-node cache hit ratio vs non-local task count to identify cause "
                    "(cold node from scale-out, CPU contention from concurrent queries, or file layout issues)"
                )
                if not is_serverless
                else _(
                    "Serverless warehouse manages node placement automatically. "
                    "High rescheduled scan ratio may indicate cold start or scale-out. "
                    "Re-run the query to verify if this persists"
                ),
            )
        elif indicators.rescheduled_scan_ratio >= THRESHOLDS["scan_locality_warning"]:
            indicators.rescheduled_scan_severity = Severity.MEDIUM
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="cluster",
                message=_(
                    "Rescheduled scan ratio is elevated ({ratio}) - monitor for performance impact"
                ).format(ratio=ratio_str),
                metric_name="rescheduled_scan_ratio",
                current_value=ratio_str,
                threshold="<1%",
            )

    # Shuffle analysis: impact ratio, memory efficiency, AQE-layout, AQE skew
    analyze_shuffle(indicators, shuffle_metrics, query_metrics)

    # Cloud storage metrics (enhanced)
    total_requests = sum(nm.cloud_storage_request_count for nm in node_metrics)
    total_retries = sum(nm.cloud_storage_retry_count for nm in node_metrics)
    total_request_duration = sum(nm.cloud_storage_request_duration_ms for nm in node_metrics)
    total_retry_duration = sum(nm.cloud_storage_retry_duration_ms for nm in node_metrics)

    indicators.cloud_storage_metrics = CloudStorageMetrics(
        total_request_count=total_requests,
        total_retry_count=total_retries,
        total_request_duration_ms=total_request_duration,
        total_retry_duration_ms=total_retry_duration,
        retry_ratio=total_retries / total_requests if total_requests > 0 else 0,
        avg_request_duration_ms=total_request_duration / total_requests
        if total_requests > 0
        else 0,
    )

    if total_requests > 0:
        indicators.cloud_storage_retry_ratio = total_retries / total_requests

        # Only raise alerts when retry_duration > 0 (confirms real overhead).
        # retry_count with duration=0 is an unverified internal counter
        # (metricType=UNKNOWN_TYPE) and should not trigger throttling warnings.
        if total_retry_duration > 0:
            ratio_str = f"{indicators.cloud_storage_retry_ratio:.1%}"
            if indicators.cloud_storage_retry_ratio >= THRESHOLDS["cloud_storage_retry_critical"]:
                indicators.cloud_storage_severity = Severity.CRITICAL
                _add_alert(
                    indicators,
                    severity=Severity.CRITICAL,
                    category="cloud_storage",
                    message=_("Cloud storage retry rate is high ({ratio})").format(ratio=ratio_str),
                    metric_name="cloud_storage_retry_ratio",
                    current_value=ratio_str,
                    threshold="<5%",
                    recommendation=_(
                        "Cloud storage access may be a bottleneck - check for throttling"
                    ),
                )
            elif indicators.cloud_storage_retry_ratio >= THRESHOLDS["cloud_storage_retry_warning"]:
                indicators.cloud_storage_severity = Severity.HIGH
                _add_alert(
                    indicators,
                    severity=Severity.HIGH,
                    category="cloud_storage",
                    message=_("Cloud storage retry rate is elevated ({ratio})").format(
                        ratio=ratio_str
                    ),
                    metric_name="cloud_storage_retry_ratio",
                    current_value=ratio_str,
                    threshold="<5%",
                )

    # Join type analysis
    sort_merge_joins = [j for j in join_info if j.join_type == JoinType.SORT_MERGE]
    if sort_merge_joins:
        _add_alert(
            indicators,
            severity=Severity.HIGH,
            category="join",
            message=_("Sort-Merge join is used in {count} places (not supported by Photon)").format(
                count=len(sort_merge_joins)
            ),
            metric_name="sort_merge_join_count",
            current_value=str(len(sort_merge_joins)),
            threshold="0",
            recommendation=_(
                "Consider setting spark.sql.join.preferSortMergeJoin=false and "
                "spark.databricks.adaptive.joinFallback=true. "
                "Expected improvement: Enable Photon acceleration for joins"
            ),
        )
        # Add as Photon blocker
        indicators.photon_blockers.append(
            PhotonBlocker(
                reason="Sort-Merge Join",
                count=len(sort_merge_joins),
                impact="HIGH",
                action="SET spark.sql.join.preferSortMergeJoin = false",
            )
        )

    # Result cache hit annotation
    if query_metrics.result_from_cache:
        _add_alert(
            indicators,
            severity=Severity.INFO,
            category="result_cache",
            message=_(
                "Query result was served from result cache — execution metrics are not meaningful"
            ),
            metric_name="result_from_cache",
            current_value="true",
            is_actionable=False,
        )

    # Queue time (Serverless cold start / overload)
    _QUEUE_THRESHOLD_MS = 5000  # 5 seconds
    if query_metrics.queued_provisioning_time_ms > _QUEUE_THRESHOLD_MS:
        secs = query_metrics.queued_provisioning_time_ms / 1000
        _add_alert(
            indicators,
            severity=Severity.MEDIUM if secs < 30 else Severity.HIGH,
            category="queue",
            message=_("Serverless provisioning queue wait: {secs:.1f}s").format(secs=secs),
            metric_name="queued_provisioning_time_ms",
            current_value=f"{secs:.1f}s",
            threshold="<5s",
            recommendation=_(
                "Consider Serverless warm pools or pre-warming to reduce cold start latency"
            ),
        )
    if query_metrics.queued_overload_time_ms > _QUEUE_THRESHOLD_MS:
        secs = query_metrics.queued_overload_time_ms / 1000
        _add_alert(
            indicators,
            severity=Severity.MEDIUM if secs < 30 else Severity.HIGH,
            category="queue",
            message=_("Serverless overload queue wait: {secs:.1f}s").format(secs=secs),
            metric_name="queued_overload_time_ms",
            current_value=f"{secs:.1f}s",
            threshold="<5s",
            recommendation=_("Reduce concurrent query load or increase warehouse scaling limits"),
        )

    # Result fetch time (large result set detection)
    _FETCH_THRESHOLD_MS = 5000  # 5 seconds absolute
    _FETCH_RATIO_THRESHOLD = 0.10  # >10% of total time
    if (
        query_metrics.result_fetch_time_ms > _FETCH_THRESHOLD_MS
        and query_metrics.total_time_ms > 0
        and query_metrics.result_fetch_time_ms / query_metrics.total_time_ms
        > _FETCH_RATIO_THRESHOLD
    ):
        secs = query_metrics.result_fetch_time_ms / 1000
        pct = query_metrics.result_fetch_time_ms / query_metrics.total_time_ms
        _add_alert(
            indicators,
            severity=Severity.MEDIUM,
            category="io",
            message=_(
                "Result fetch time is {secs:.1f}s ({pct:.0%} of total) — large result set"
            ).format(
                secs=secs,
                pct=pct,
            ),
            metric_name="result_fetch_time_ms",
            current_value=f"{secs:.1f}s ({pct:.0%})",
            threshold="<5s or <10% of total",
            recommendation=_(
                "Add LIMIT, use CREATE TABLE AS SELECT, or push aggregation closer to source"
            ),
        )

    # Compilation phase breakdown (slow compilation detection)
    _COMPILATION_SLOW_THRESHOLD_MS = 10000  # 10 seconds total
    if (
        query_metrics.compilation_time_ms > _COMPILATION_SLOW_THRESHOLD_MS
        and query_metrics.planning_phases
    ):
        dominant = max(query_metrics.planning_phases, key=lambda p: p.get("duration_ms", 0))
        dominant_phase = dominant.get("phase", "UNKNOWN")
        dominant_ms = dominant.get("duration_ms", 0)
        _add_alert(
            indicators,
            severity=Severity.MEDIUM,
            category="compilation",
            message=_(
                "Slow compilation ({total:.1f}s) — dominant phase: {phase} ({phase_sec:.1f}s)"
            ).format(
                total=query_metrics.compilation_time_ms / 1000,
                phase=dominant_phase,
                phase_sec=dominant_ms / 1000,
            ),
            metric_name="compilation_time_ms",
            current_value=f"{query_metrics.compilation_time_ms / 1000:.1f}s",
            threshold="<10s",
            recommendation=_(
                "Simplify query structure (reduce CTEs/subqueries/UNION branches) "
                "or run ANALYZE TABLE to update statistics"
            ),
        )

    # Metadata resolution time
    _METADATA_SLOW_THRESHOLD_MS = 30000  # 30 seconds
    if query_metrics.metadata_time_ms > _METADATA_SLOW_THRESHOLD_MS:
        secs = query_metrics.metadata_time_ms / 1000
        _add_alert(
            indicators,
            severity=Severity.MEDIUM,
            category="compilation",
            message=_("High metadata resolution time: {secs:.1f}s").format(secs=secs),
            metric_name="metadata_time_ms",
            current_value=f"{secs:.1f}s",
            threshold="<30s",
            recommendation=_(
                "Check for slow external metastore or large number of table partitions"
            ),
        )

    _analyze_compilation_overhead(indicators, query_metrics)
    _analyze_driver_overhead(indicators, query_metrics)

    # Analyze extra_metrics from node metrics for derived indicators
    _analyze_extra_metrics(
        indicators, node_metrics, query_metrics.execution_time_ms, query_metrics.read_bytes
    )

    _apply_sql_pattern_alerts(indicators, node_metrics, query_metrics)

    # Run after extra_metrics so rescheduled_scan_ratio is populated before
    # cluster_underutilization consults it for variant classification.
    _analyze_cluster_underutilization(indicators, query_metrics)
    _analyze_compilation_absolute_heavy(indicators, query_metrics)

    # =========================================================================
    # Phase 1: Extract factual signals for LLM-driven severity determination.
    # These signals carry no severity — LLM decides importance based on
    # the combination of signals, context, and reference thresholds.
    # =========================================================================
    indicators.detected_signals = _extract_signals(indicators, query_metrics)

    return indicators


def _analyze_extra_metrics(
    indicators: BottleneckIndicators,
    node_metrics: list[NodeMetrics],
    execution_time_ms: int = 0,
    read_bytes: int = 0,
) -> None:
    """Analyze extra_metrics from nodes to compute derived indicators.

    This function extracts insights from unmapped metrics labels stored in
    extra_metrics, enabling detection of issues that the standard metrics
    don't capture (e.g., OOM fallback, hash join internals, I/O wait breakdown).

    Args:
        indicators: BottleneckIndicators to update
        node_metrics: List of node metrics with extra_metrics populated
        execution_time_ms: Query execution time for sanity checks
        read_bytes: Total bytes read for ratio calculations
    """
    observed_labels: set[str] = set()

    # ShuffleMetrics (when provided) is the authoritative source of shuffle
    # bytes written and is already populated in the caller's AQE loop.
    # Only accumulate from node_metrics.extra_metrics as a fallback.
    _fallback_shuffle_bytes = indicators.shuffle_bytes_written_total == 0

    for nm in node_metrics:
        extra = nm.extra_metrics
        if not extra:
            continue

        # Collect all observed labels for discovery/debugging
        observed_labels.update(extra.keys())

        # OOM fallback detection (Photon -> non-Photon fallback)
        marked_for_oom = extra.get("Marked for OOM fallback", 0)
        if marked_for_oom > 0:
            indicators.oom_fallback_count += 1
            indicators.oom_fallback_nodes.append(nm.node_name)

        # Hash join internal metrics
        # Note: "Time in hash build" is in nanoseconds (TIMING_METRIC_NS)
        hash_build_time_ns = extra.get("Time in hash build", 0)
        if hash_build_time_ns > 0:
            indicators.hash_build_time_total_ms += hash_build_time_ns // 1_000_000

        hash_resize = extra.get("Number of times hash table was resized", 0)
        if hash_resize > 0:
            indicators.hash_table_resize_count += hash_resize

        avg_probes = extra.get("Avg hash probes per row", 0)
        if avg_probes > 0:
            # Keep max for worst-case detection
            indicators.avg_hash_probes_per_row = max(indicators.avg_hash_probes_per_row, avg_probes)

        # Shuffle I/O volume — see _fallback_shuffle_bytes above.
        if _fallback_shuffle_bytes:
            sink_bytes = extra.get("Sink - Num bytes written", 0)
            if sink_bytes > 0:
                indicators.shuffle_bytes_written_total += sink_bytes
        remote_bytes = extra.get("Source - Remote bytes read", 0)
        if remote_bytes > 0:
            indicators.shuffle_remote_bytes_read_total += remote_bytes
        local_bytes = extra.get("Source - Local bytes read", 0)
        if local_bytes > 0:
            indicators.shuffle_local_bytes_read_total += local_bytes

        # I/O wait metrics
        fetch_wait = extra.get("Source - Fetch wait time", 0)
        if fetch_wait > 0:
            indicators.io_fetch_wait_time_total_ms += fetch_wait

        # Note: "Source - Time taken to decompress data" is in nanoseconds (TIMING_METRIC_NS)
        decompress_time_ns = extra.get("Source - Time taken to decompress data", 0)
        if decompress_time_ns > 0:
            indicators.io_decompress_time_total_ms += decompress_time_ns // 1_000_000

        # Note: "Source - Prism server queueing time" is in nanoseconds (TIMING_METRIC_NS)
        prism_queue_ns = extra.get("Source - Prism server queueing time", 0)
        if prism_queue_ns > 0:
            indicators.prism_queue_time_total_ms += prism_queue_ns // 1_000_000

        # Spill detailed metrics
        spill_count = extra.get("Num spills to disk due to memory pressure", 0)
        if spill_count > 0:
            indicators.spill_count_total += spill_count

        spill_rows = extra.get("Num rows spilled to disk due to memory pressure", 0)
        if spill_rows > 0:
            indicators.spill_rows_total += spill_rows

        spill_partitions = extra.get("Num spill partitions created", 0)
        if spill_partitions > 0:
            indicators.spill_partitions_total += spill_partitions

    # Store top observed labels (for discovery)
    indicators.observed_extra_labels = sorted(observed_labels)[:50]

    # Hash resize hotspots (skew attribution) + duplicate GROUP BY detection
    indicators.hash_resize_hotspots = extract_hash_resize_hotspots(node_metrics)
    detect_duplicate_groupby(indicators)

    # Generate warnings/recommendations based on extra_metrics analysis
    _generate_extra_metrics_warnings(indicators, execution_time_ms, read_bytes, node_metrics)


def _generate_extra_metrics_warnings(
    indicators: BottleneckIndicators,
    execution_time_ms: int = 0,
    read_bytes: int = 0,
    node_metrics: list | None = None,
) -> None:
    """Generate warnings and recommendations based on extra_metrics analysis.

    Args:
        indicators: BottleneckIndicators with extra_metrics derived values
        execution_time_ms: Query execution time for sanity checks
    """
    # OOM fallback detection (Photon -> non-Photon fallback)
    if indicators.oom_fallback_count > 0:
        nodes_str = ", ".join(indicators.oom_fallback_nodes[:3])
        if len(indicators.oom_fallback_nodes) > 3:
            nodes_str += "..."
        _add_alert(
            indicators,
            severity=Severity.CRITICAL,
            category="memory",
            message=_("OOM fallback detected in {count} operators: {nodes}").format(
                count=indicators.oom_fallback_count, nodes=nodes_str
            ),
            metric_name="oom_fallback_count",
            current_value=str(indicators.oom_fallback_count),
            threshold="0",
            recommendation=_(
                "Increase cluster memory or reduce data per partition to avoid OOM fallback"
            ),
        )

    # Hash-resize alerts (severe/suspect skew, AQE-layout, generic)
    generate_hash_resize_alerts(indicators)

    # I/O wait analysis
    # Sanity check: if decompression time exceeds 10x query execution time,
    # it's likely a metric reporting error (e.g., cumulative counter, unit mismatch)
    decompress_time_valid = True
    if execution_time_ms > 0:
        if indicators.io_decompress_time_total_ms > execution_time_ms * 10:
            decompress_time_valid = False

    # Use validated decompression time for total calculation
    effective_decompress_time = (
        indicators.io_decompress_time_total_ms if decompress_time_valid else 0
    )
    total_io_wait = (
        indicators.io_fetch_wait_time_total_ms
        + effective_decompress_time
        + indicators.prism_queue_time_total_ms
    )
    if total_io_wait > 60000:  # > 1 minute of I/O wait
        if indicators.prism_queue_time_total_ms > indicators.io_fetch_wait_time_total_ms:
            _add_alert(
                indicators,
                severity=Severity.HIGH,
                category="io",
                message=_(
                    "Storage server queue time is high ({time}ms) - storage may be overloaded"
                ).format(time=f"{indicators.prism_queue_time_total_ms:,}"),
                metric_name="prism_queue_time_total_ms",
                current_value=f"{indicators.prism_queue_time_total_ms:,}ms",
                threshold="<60s",
            )
        elif (
            decompress_time_valid
            and indicators.io_decompress_time_total_ms > indicators.io_fetch_wait_time_total_ms
        ):
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="io",
                message=_(
                    "Data decompression time is high ({time}ms) - consider different compression"
                ).format(time=f"{indicators.io_decompress_time_total_ms:,}"),
                metric_name="io_decompress_time_total_ms",
                current_value=f"{indicators.io_decompress_time_total_ms:,}ms",
                threshold="<60s",
            )
        else:
            _add_alert(
                indicators,
                severity=Severity.HIGH,
                category="io",
                message=_("I/O fetch wait time is high ({time}ms)").format(
                    time=f"{indicators.io_fetch_wait_time_total_ms:,}"
                ),
                metric_name="io_fetch_wait_time_total_ms",
                current_value=f"{indicators.io_fetch_wait_time_total_ms:,}ms",
                threshold="<60s",
            )

    # Detailed spill analysis
    if indicators.spill_count_total > 0:
        if indicators.spill_partitions_total > 100:
            _add_alert(
                indicators,
                severity=Severity.HIGH,
                category="spill",
                message=_(
                    "High number of spill partitions ({count}) - significant memory pressure"
                ).format(count=indicators.spill_partitions_total),
                metric_name="spill_partitions_total",
                current_value=str(indicators.spill_partitions_total),
                threshold="<100",
            )

    # Shuffle I/O volume analysis
    if indicators.shuffle_bytes_written_total > 0 and read_bytes > 0:
        shuffle_write_ratio = indicators.shuffle_bytes_written_total / read_bytes
        if shuffle_write_ratio > 0.50:
            shuffle_gb = indicators.shuffle_bytes_written_total / (1024**3)
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="shuffle",
                message=_(
                    "Shuffle writes are {ratio:.0%} of total read bytes ({size:.1f}GB) - "
                    "consider reducing shuffle with bucketing or broadcast joins"
                ).format(ratio=shuffle_write_ratio, size=shuffle_gb),
                metric_name="shuffle_bytes_written",
                current_value=f"{shuffle_gb:.1f}GB",
                threshold="<50% of read bytes",
                recommendation=_(
                    "Use broadcast joins for small tables, bucketing for repeated joins, "
                    "or repartition to reduce shuffle volume"
                ),
            )


# =============================================================================
# Phase 1: Factual signal extraction (no severity judgment)
# =============================================================================


def _extract_signals(bi: BottleneckIndicators, qm: QueryMetrics) -> list[BottleneckSignal]:
    """Extract factual signals from computed indicators.

    Each signal is a factual observation with context — no severity judgment.
    LLM uses these signals + reference thresholds to determine severity.
    """
    signals: list[BottleneckSignal] = []
    read_bytes = qm.read_bytes or 1  # avoid division by zero

    # --- Cache ---
    if bi.cache_hit_ratio < THRESHOLDS["cache_hit_medium"]:
        signals.append(
            BottleneckSignal(
                signal_id="low_cache_hit",
                category="cache",
                description="Cache hit ratio is below the improvement threshold",
                observed_value=f"{bi.cache_hit_ratio:.1%}",
                reference_value=f"good: >{THRESHOLDS['cache_hit_high']:.0%}, needs improvement: <{THRESHOLDS['cache_hit_medium']:.0%}",
                context={"read_bytes": qm.read_bytes, "cache_bytes": qm.read_cache_bytes},
            )
        )

    # --- Remote Read ---
    if bi.remote_read_ratio > THRESHOLDS["remote_read_high"]:
        signals.append(
            BottleneckSignal(
                signal_id="high_remote_read",
                category="io",
                description="High proportion of data read from remote storage",
                observed_value=f"{bi.remote_read_ratio:.1%}",
                reference_value=f"warning: >{THRESHOLDS['remote_read_high']:.0%}",
                context={
                    "read_bytes": qm.read_bytes,
                    "remote_bytes": qm.read_remote_bytes,
                    "is_significant": qm.read_bytes > 1_000_000_000,
                },  # >1GB
            )
        )

    # --- Spill ---
    if bi.spill_bytes > 0:
        spill_gb = bi.spill_bytes / (1024**3)
        spill_ratio = bi.spill_bytes / read_bytes if read_bytes > 0 else 0
        signals.append(
            BottleneckSignal(
                signal_id="spill_detected",
                category="spill",
                description="Disk spill detected",
                observed_value=format_bytes(bi.spill_bytes),
                reference_value=f"critical: >{THRESHOLDS['spill_critical_gb']:.0f}GB, high: >{THRESHOLDS['spill_high_gb']:.0f}GB",
                context={
                    "spill_gb": round(spill_gb, 2),
                    "spill_ratio_of_read": round(spill_ratio, 4),
                    "spill_count": bi.spill_count_total,
                    "spill_partitions": bi.spill_partitions_total,
                    "peak_memory_bytes": max(
                        (op.spill_bytes for op in bi.spill_operators), default=0
                    ),
                },
            )
        )

    # --- Photon ---
    if bi.photon_ratio < THRESHOLDS["photon_medium"]:
        has_blockers = bool(bi.photon_blockers)
        any(
            j.join_type == JoinType.SORT_MERGE
            for j in []  # join_info not available here
        ) if False else False  # placeholder — join_info passed separately
        signals.append(
            BottleneckSignal(
                signal_id="low_photon_utilization",
                category="photon",
                description="Photon utilization is below target",
                observed_value=f"{bi.photon_ratio:.1%}",
                reference_value=f"good: >{THRESHOLDS['photon_high']:.0%}, critical: <{THRESHOLDS['photon_low']:.0%}",
                context={
                    "has_photon_blockers": has_blockers,
                    "blocker_count": len(bi.photon_blockers),
                    "oom_fallback_count": bi.oom_fallback_count,
                    "execution_time_ms": qm.execution_time_ms,
                    "is_short_query": qm.execution_time_ms < 5000,
                },
            )
        )

    # --- Shuffle ---
    if bi.shuffle_impact_ratio > THRESHOLDS["shuffle_high"]:
        signals.append(
            BottleneckSignal(
                signal_id="high_shuffle_impact",
                category="shuffle",
                description="Shuffle operations consume significant execution time",
                observed_value=f"{bi.shuffle_impact_ratio:.1%}",
                reference_value=f"critical: >{THRESHOLDS['shuffle_critical']:.0%}, high: >{THRESHOLDS['shuffle_high']:.0%}",
                context={"execution_time_ms": qm.execution_time_ms},
            )
        )

    # --- Shuffle I/O Volume ---
    if bi.shuffle_bytes_written_total > 0 and read_bytes > 0:
        shuffle_write_ratio = bi.shuffle_bytes_written_total / read_bytes
        if shuffle_write_ratio > 0.50:
            signals.append(
                BottleneckSignal(
                    signal_id="high_shuffle_data_volume",
                    category="shuffle",
                    description="Shuffle write volume is high relative to total data read",
                    observed_value=format_bytes(bi.shuffle_bytes_written_total),
                    reference_value="warning: >50% of read bytes",
                    context={
                        "shuffle_bytes_written": bi.shuffle_bytes_written_total,
                        "shuffle_remote_bytes_read": bi.shuffle_remote_bytes_read_total,
                        "shuffle_write_ratio": round(shuffle_write_ratio, 4),
                        "read_bytes": qm.read_bytes,
                    },
                )
            )

    # --- Shuffle Locality ---
    total_shuffle_read = bi.shuffle_remote_bytes_read_total + bi.shuffle_local_bytes_read_total
    if total_shuffle_read > 0:
        remote_ratio = bi.shuffle_remote_bytes_read_total / total_shuffle_read
        if remote_ratio > 0.80:
            signals.append(
                BottleneckSignal(
                    signal_id="low_shuffle_locality",
                    category="shuffle",
                    description="Most shuffle reads are remote — low data locality",
                    observed_value=f"{remote_ratio:.0%} remote",
                    reference_value="warning: >80% remote shuffle reads",
                    context={
                        "remote_bytes": bi.shuffle_remote_bytes_read_total,
                        "local_bytes": bi.shuffle_local_bytes_read_total,
                        "remote_ratio": round(remote_ratio, 4),
                    },
                )
            )

    # --- Data Skew ---
    if bi.has_data_skew:
        signals.append(
            BottleneckSignal(
                signal_id="data_skew_detected",
                category="skew",
                description="Data skew detected in partitions",
                observed_value=f"{bi.skewed_partitions} skewed partitions",
                reference_value="threshold: >0 skewed partitions",
                context={"skewed_partitions": bi.skewed_partitions},
            )
        )

    # --- File Pruning ---
    if bi.filter_rate < THRESHOLDS.get("filter_low", 0.20):
        signals.append(
            BottleneckSignal(
                signal_id="low_file_pruning",
                category="io",
                description="File pruning efficiency is low",
                observed_value=f"{bi.filter_rate:.1%}",
                reference_value=f"warning: <{THRESHOLDS.get('filter_low', 0.20):.0%}",
                context={"read_files": qm.read_files_count, "pruned_files": qm.pruned_files_count},
            )
        )

    # --- Cloud Storage Retries ---
    if bi.cloud_storage_retry_ratio > THRESHOLDS.get("cloud_storage_retry_ratio", 0.01):
        signals.append(
            BottleneckSignal(
                signal_id="cloud_storage_retries",
                category="cloud_storage",
                description="Elevated cloud storage retry rate",
                observed_value=f"{bi.cloud_storage_retry_ratio:.1%}",
                reference_value="warning: >1%",
                context={
                    "request_count": bi.cloud_storage_metrics.total_request_count,
                    "retry_count": bi.cloud_storage_metrics.total_retry_count,
                },
            )
        )

    # --- OOM Fallback ---
    if bi.oom_fallback_count > 0:
        signals.append(
            BottleneckSignal(
                signal_id="oom_fallback",
                category="photon",
                description="Photon OOM fallback to non-Photon execution detected",
                observed_value=f"{bi.oom_fallback_count} fallbacks",
                reference_value="expected: 0",
                context={"nodes": bi.oom_fallback_nodes[:5]},
            )
        )

    # --- Hash Performance ---
    if bi.avg_hash_probes_per_row > 2.0:
        signals.append(
            BottleneckSignal(
                signal_id="high_hash_probes",
                category="join",
                description="High average hash probes per row (possible hash collision)",
                observed_value=f"{bi.avg_hash_probes_per_row:.1f} probes/row",
                reference_value="expected: <2.0",
                context={"resize_count": bi.hash_table_resize_count},
            )
        )

    # --- Result Cache ---
    if qm.result_from_cache:
        signals.append(
            BottleneckSignal(
                signal_id="result_from_cache",
                category="result_cache",
                description="Query result was served from result cache — execution was skipped",
                observed_value="true",
                reference_value="false means actual execution occurred",
            )
        )

    # --- Queue Time ---
    total_queue = qm.queued_provisioning_time_ms + qm.queued_overload_time_ms
    if total_queue > 5000:
        signals.append(
            BottleneckSignal(
                signal_id="high_queue_time",
                category="queue",
                description="Significant time spent waiting in Serverless queue",
                observed_value=f"{total_queue / 1000:.1f}s total",
                reference_value="expected: <5s",
                context={
                    "provisioning_ms": qm.queued_provisioning_time_ms,
                    "overload_ms": qm.queued_overload_time_ms,
                },
            )
        )

    # --- Result Fetch Time ---
    if qm.result_fetch_time_ms > 5000 and qm.total_time_ms > 0:
        fetch_pct = qm.result_fetch_time_ms / qm.total_time_ms
        if fetch_pct > 0.10:
            signals.append(
                BottleneckSignal(
                    signal_id="high_result_fetch_time",
                    category="io",
                    description="Result fetch time indicates large result set returned to client",
                    observed_value=f"{qm.result_fetch_time_ms / 1000:.1f}s ({fetch_pct:.0%} of total)",
                    reference_value="expected: <5s or <10% of total",
                    context={
                        "fetch_ms": qm.result_fetch_time_ms,
                        "total_ms": qm.total_time_ms,
                        "rows_produced": qm.rows_produced_count,
                    },
                )
            )

    # --- Slow Compilation ---
    if qm.compilation_time_ms > 10000 and qm.planning_phases:
        dominant = max(qm.planning_phases, key=lambda p: p.get("duration_ms", 0))
        signals.append(
            BottleneckSignal(
                signal_id="slow_compilation",
                category="compilation",
                description="Query compilation time is unusually high",
                observed_value=f"{qm.compilation_time_ms / 1000:.1f}s",
                reference_value="expected: <10s",
                context={
                    "dominant_phase": dominant.get("phase", ""),
                    "dominant_ms": dominant.get("duration_ms", 0),
                    "phases": qm.planning_phases,
                },
            )
        )

    # --- Write Fallback ---
    if bi.write_fallback_detected:
        signals.append(
            BottleneckSignal(
                signal_id="write_photon_fallback",
                category="photon",
                description="Write operator falls back to non-Photon execution",
                observed_value="UNIMPLEMENTED_OPERATOR",
                reference_value="expected: Photon supported",
            )
        )

    # --- Large Write Volume ---
    if qm.write_remote_bytes > 1_000_000_000:  # >1GB
        write_gb = qm.write_remote_bytes / (1024**3)
        signals.append(
            BottleneckSignal(
                signal_id="large_write_volume",
                category="io",
                description="Large volume of data written to remote storage",
                observed_value=format_bytes(qm.write_remote_bytes),
                reference_value="notable: >1GB",
                context={
                    "write_bytes": qm.write_remote_bytes,
                    "write_files": qm.write_remote_files,
                    "write_rows": qm.write_remote_rows,
                    "write_gb": round(write_gb, 2),
                },
            )
        )

    # --- Write Amplification ---
    if qm.write_remote_bytes > 0 and qm.read_bytes > 0:
        write_read_ratio = qm.write_remote_bytes / qm.read_bytes
        if write_read_ratio > 2.0:
            signals.append(
                BottleneckSignal(
                    signal_id="write_amplification",
                    category="io",
                    description="Write volume significantly exceeds read volume",
                    observed_value=f"{write_read_ratio:.1f}x read bytes",
                    reference_value="warning: >2x read bytes",
                    context={
                        "write_bytes": qm.write_remote_bytes,
                        "read_bytes": qm.read_bytes,
                        "ratio": round(write_read_ratio, 2),
                    },
                )
            )

    # --- Small Write Files ---
    if qm.write_remote_files > 0 and qm.write_remote_bytes > 0:
        avg_file_size = qm.write_remote_bytes / qm.write_remote_files
        if avg_file_size < 8_000_000:  # avg < 8MB per file
            signals.append(
                BottleneckSignal(
                    signal_id="small_write_files",
                    category="io",
                    description="Written files are small on average — consider OPTIMIZE or coalescing",
                    observed_value=f"{avg_file_size / (1024**2):.1f}MB avg ({qm.write_remote_files} files)",
                    reference_value="expected: >8MB avg file size",
                    context={
                        "avg_file_bytes": int(avg_file_size),
                        "file_count": qm.write_remote_files,
                        "total_bytes": qm.write_remote_bytes,
                    },
                )
            )

    # --- Type Cast Overhead ---
    if bi.cast_in_join_filter >= 1 or bi.cast_count >= 20:
        signals.append(
            BottleneckSignal(
                signal_id="type_cast_overhead",
                category="execution_plan",
                description="Type cast operations detected that may impact performance",
                observed_value=f"{bi.cast_count} total, {bi.cast_in_join_filter} in join/filter",
                reference_value="warning: join/filter casts >0 or total >20",
                context={
                    "cast_count": bi.cast_count,
                    "cast_in_join_filter": bi.cast_in_join_filter,
                },
            )
        )

    # --- Partition Strategy ---
    if bi.partition_column_count >= 3:
        signals.append(
            BottleneckSignal(
                signal_id="multi_column_partition",
                category="io",
                description=f"Table uses {bi.partition_column_count}-column partitioning",
                observed_value=f"{bi.partition_column_count} columns ({', '.join(bi.partition_columns)})",
                reference_value="consider Liquid Clustering for 3+ partition columns",
                context={
                    "partition_columns": bi.partition_columns,
                    "bytes_pruning_ratio": bi.bytes_pruning_ratio,
                    "filter_rate": bi.filter_rate,
                },
            )
        )

    return signals
