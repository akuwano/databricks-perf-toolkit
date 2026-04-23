"""Comparison service for Spark application before/after analysis.

Computes per-metric diffs between a baseline and candidate Spark
application, applying direction-aware improvement/regression detection.
Reuses MetricDiff and ComparisonResult from core.models.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from typing import Any

from .models import ComparisonResult, MetricDiff

# metric_name -> (metric_group, effect_when_value_increases, weight)
SPARK_COMPARABLE_METRICS: dict[str, tuple[str, str, float]] = {
    # Application-level metrics (from gold_application_summary)
    "duration_ms": ("latency", "WORSENS", 5.0),
    "total_exec_run_ms": ("latency", "WORSENS", 4.0),
    "total_gc_time_ms": ("gc", "WORSENS", 3.0),
    "gc_overhead_pct": ("gc", "WORSENS", 3.0),
    "total_input_gb": ("io", "WORSENS", 2.0),
    "total_shuffle_gb": ("shuffle", "WORSENS", 3.0),
    "total_spill_gb": ("spill", "WORSENS", 4.0),
    "stages_with_disk_spill": ("spill", "WORSENS", 2.0),
    "job_success_rate": ("reliability", "IMPROVES", 3.0),
    "total_tasks": ("parallelism", "NEUTRAL", 1.0),
    # Cost estimation metrics (from dbu_estimate, flattened into summary)
    "estimated_total_dbu": ("cost", "WORSENS", 2.0),
    "estimated_dbu_per_hour": ("cost", "WORSENS", 2.0),
    "estimated_total_usd": ("cost", "WORSENS", 3.0),
    # Stage-aggregated metrics (computed from gold_stage_performance)
    "max_task_skew_ratio": ("skew", "WORSENS", 3.0),
    "avg_cpu_efficiency_pct": ("cpu", "IMPROVES", 3.0),
    "total_disk_spill_mb": ("spill", "WORSENS", 3.0),
    "total_shuffle_read_mb": ("shuffle", "WORSENS", 2.0),
    "bottleneck_count": ("bottleneck", "WORSENS", 3.0),
    # Streaming-aggregated metrics (computed from gold_streaming_query_summary)
    "stream_avg_batch_duration_ms": ("streaming_latency", "WORSENS", 4.0),
    "stream_max_batch_duration_ms": ("streaming_latency", "WORSENS", 3.0),
    "stream_avg_processed_rows_per_sec": ("streaming_throughput", "IMPROVES", 4.0),
    "stream_max_state_memory_bytes": ("streaming_state", "WORSENS", 3.0),
    "stream_total_rows_dropped_by_watermark": ("streaming_state", "WORSENS", 2.0),
    "stream_bottleneck_count": ("streaming_bottleneck", "WORSENS", 3.0),
    "stream_avg_commit_ms": ("streaming_latency", "WORSENS", 2.0),
}

# Metrics where regression is considered HIGH severity
_HIGH_SEVERITY_METRICS = {"duration_ms", "total_spill_gb", "total_exec_run_ms"}

# Default threshold ratio for regression/improvement detection (10%)
_DEFAULT_THRESHOLD = 0.10

# Metrics that are noisy at small absolute values
_NOISE_FLOOR: dict[str, float] = {
    "total_gc_time_ms": 5_000,  # <5s GC is noise
    "gc_overhead_pct": 1.0,  # <1% GC overhead is noise
}

# Application-level metric keys (read directly from summary dict)
_APP_LEVEL_METRICS = {
    "duration_ms",
    "total_exec_run_ms",
    "total_gc_time_ms",
    "gc_overhead_pct",
    "total_input_gb",
    "total_shuffle_gb",
    "total_spill_gb",
    "stages_with_disk_spill",
    "job_success_rate",
    "total_tasks",
    "estimated_total_dbu",
    "estimated_dbu_per_hour",
    "estimated_total_usd",
}

# Stage-aggregated metric keys (computed from stage list)
_STAGE_AGG_METRICS = {
    "max_task_skew_ratio",
    "avg_cpu_efficiency_pct",
    "total_disk_spill_mb",
    "total_shuffle_read_mb",
    "bottleneck_count",
}

# Streaming-aggregated metric keys (computed from streaming query list)
_STREAMING_AGG_METRICS = {
    "stream_avg_batch_duration_ms",
    "stream_max_batch_duration_ms",
    "stream_avg_processed_rows_per_sec",
    "stream_max_state_memory_bytes",
    "stream_total_rows_dropped_by_watermark",
    "stream_bottleneck_count",
    "stream_avg_commit_ms",
}


def _aggregate_stages(stages: list[dict[str, Any]]) -> dict[str, float]:
    """Compute aggregated metrics from a list of stage dicts."""
    if not stages:
        return {
            "max_task_skew_ratio": 0.0,
            "avg_cpu_efficiency_pct": 0.0,
            "total_disk_spill_mb": 0.0,
            "total_shuffle_read_mb": 0.0,
            "bottleneck_count": 0.0,
        }

    max_skew = 0.0
    cpu_effs: list[float] = []
    total_spill = 0.0
    total_shuffle_read = 0.0
    bottleneck_count = 0

    for s in stages:
        skew = s.get("task_skew_ratio") or s.get("skew_ratio") or 0.0
        if skew and float(skew) > max_skew:
            max_skew = float(skew)

        cpu_eff = s.get("cpu_efficiency_pct") or s.get("cpu_efficiency") or 0.0
        if cpu_eff:
            cpu_effs.append(float(cpu_eff))

        spill = s.get("disk_spill_mb") or s.get("spill_disk_mb") or 0.0
        total_spill += float(spill)

        shuffle = s.get("shuffle_read_mb") or 0.0
        total_shuffle_read += float(shuffle)

        bottleneck = s.get("bottleneck_type") or s.get("bottleneck") or ""
        if bottleneck and str(bottleneck).lower() not in ("", "none", "null"):
            bottleneck_count += 1

    avg_cpu = sum(cpu_effs) / len(cpu_effs) if cpu_effs else 0.0

    return {
        "max_task_skew_ratio": max_skew,
        "avg_cpu_efficiency_pct": avg_cpu,
        "total_disk_spill_mb": total_spill,
        "total_shuffle_read_mb": total_shuffle_read,
        "bottleneck_count": float(bottleneck_count),
    }


def _aggregate_streaming(queries: list[dict[str, Any]]) -> dict[str, float]:
    """Compute aggregated metrics from streaming query summaries."""
    if not queries:
        return {}

    avg_durations = [float(q.get("avg_batch_duration_ms", 0) or 0) for q in queries]
    max_durations = [float(q.get("max_batch_duration_ms", 0) or 0) for q in queries]
    throughputs = [float(q.get("avg_processed_rows_per_sec", 0) or 0) for q in queries]
    state_mems = [float(q.get("max_state_memory_bytes", 0) or 0) for q in queries]
    watermark_drops = [float(q.get("total_rows_dropped_by_watermark", 0) or 0) for q in queries]
    commit_times = [float(q.get("avg_commit_ms", 0) or 0) for q in queries]
    bn_count = sum(
        1 for q in queries if q.get("bottleneck_type") and q["bottleneck_type"] != "STREAM_OK"
    )

    return {
        "stream_avg_batch_duration_ms": sum(avg_durations) / len(avg_durations)
        if avg_durations
        else 0,
        "stream_max_batch_duration_ms": max(max_durations) if max_durations else 0,
        "stream_avg_processed_rows_per_sec": sum(throughputs) / len(throughputs)
        if throughputs
        else 0,
        "stream_max_state_memory_bytes": max(state_mems) if state_mems else 0,
        "stream_total_rows_dropped_by_watermark": sum(watermark_drops),
        "stream_bottleneck_count": float(bn_count),
        "stream_avg_commit_ms": sum(commit_times) / len(commit_times) if commit_times else 0,
    }


def _extract_metric(
    summary: dict[str, Any],
    stage_agg: dict[str, float],
    metric_name: str,
    streaming_agg: dict[str, float] | None = None,
) -> float | None:
    """Extract a metric value from summary dict, stage aggregates, or streaming aggregates."""
    if metric_name in _APP_LEVEL_METRICS:
        v = summary.get(metric_name)
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    elif metric_name in _STAGE_AGG_METRICS:
        v = stage_agg.get(metric_name)
        return float(v) if v is not None else None
    elif metric_name in _STREAMING_AGG_METRICS and streaming_agg:
        v = streaming_agg.get(metric_name)
        return float(v) if v is not None else None
    return None


def compare_spark_apps(
    baseline_summary: dict[str, Any],
    candidate_summary: dict[str, Any],
    baseline_stages: list[dict[str, Any]],
    candidate_stages: list[dict[str, Any]],
    threshold: float = _DEFAULT_THRESHOLD,
    baseline_streaming: list[dict[str, Any]] | None = None,
    candidate_streaming: list[dict[str, Any]] | None = None,
) -> ComparisonResult:
    """Compare two Spark applications and return a ComparisonResult.

    Args:
        baseline_summary: gold_application_summary row for baseline app.
        candidate_summary: gold_application_summary row for candidate app.
        baseline_stages: gold_stage_performance rows for baseline app.
        candidate_stages: gold_stage_performance rows for candidate app.
        threshold: Minimum relative change to flag as regression/improvement.
        baseline_streaming: gold_streaming_query_summary rows for baseline app.
        candidate_streaming: gold_streaming_query_summary rows for candidate app.

    Returns:
        ComparisonResult with metric diffs, verdict, and summary.
    """
    baseline_agg = _aggregate_stages(baseline_stages)
    candidate_agg = _aggregate_stages(candidate_stages)
    baseline_stream_agg = _aggregate_streaming(baseline_streaming or [])
    candidate_stream_agg = _aggregate_streaming(candidate_streaming or [])

    # Determine if streaming comparison should be skipped
    _both_have_streaming = bool(baseline_stream_agg) and bool(candidate_stream_agg)
    _streaming_skipped = False
    if not _both_have_streaming and (bool(baseline_stream_agg) or bool(candidate_stream_agg)):
        _streaming_skipped = True

    result = ComparisonResult(
        comparison_id=str(uuid.uuid4()),
        baseline_analysis_id=baseline_summary.get("app_id", ""),
        candidate_analysis_id=candidate_summary.get("app_id", ""),
    )

    for metric_name, (group, increase_effect, _weight) in SPARK_COMPARABLE_METRICS.items():
        # Skip streaming metrics unless both apps have streaming data
        if metric_name in _STREAMING_AGG_METRICS and not _both_have_streaming:
            continue
        bv = _extract_metric(baseline_summary, baseline_agg, metric_name, baseline_stream_agg)
        cv = _extract_metric(candidate_summary, candidate_agg, metric_name, candidate_stream_agg)
        result.metric_diffs.append(
            _build_metric_diff(metric_name, group, increase_effect, bv, cv, threshold)
        )

    regressions = [m for m in result.metric_diffs if m.regression_flag]

    # Net score: weighted sum of improvements minus regressions
    net_score = 0.0
    for m in result.metric_diffs:
        _, _, w = SPARK_COMPARABLE_METRICS.get(m.metric_name, ("", "", 1.0))
        if m.improvement_flag and m.relative_diff_ratio is not None:
            net_score += w * abs(m.relative_diff_ratio)
        elif m.regression_flag and m.relative_diff_ratio is not None:
            net_score -= w * abs(m.relative_diff_ratio)

    # Overall verdict
    high_regressions = [m for m in regressions if m.severity == "HIGH"]
    result.regression_detected = bool(high_regressions) or (net_score < 0 and bool(regressions))
    result.regression_severity = (
        _summarize_severity(regressions) if result.regression_detected else "NONE"
    )
    result.summary = _build_summary(
        result.metric_diffs, net_score, streaming_skipped=_streaming_skipped
    )
    return result


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _build_metric_diff(
    name: str,
    group: str,
    increase_effect: str,
    bv: float | None,
    cv: float | None,
    threshold: float,
) -> MetricDiff:
    if bv is None or cv is None:
        return MetricDiff(
            metric_name=name,
            metric_group=group,
            direction_when_increase=increase_effect,
        )

    diff = cv - bv
    ratio = None if bv == 0 else diff / bv
    abs_ratio = abs(ratio) if ratio is not None else 0.0

    # Check noise floor
    noise_floor = _NOISE_FLOOR.get(name, 0)
    below_noise = bv < noise_floor and cv < noise_floor

    regression = not below_noise and (
        (diff > 0 and increase_effect == "WORSENS" and abs_ratio >= threshold)
        or (diff < 0 and increase_effect == "IMPROVES" and abs_ratio >= threshold)
    )
    improvement = not below_noise and (
        (diff < 0 and increase_effect == "WORSENS" and abs_ratio >= threshold)
        or (diff > 0 and increase_effect == "IMPROVES" and abs_ratio >= threshold)
    )

    # NEUTRAL metrics never flag as regression or improvement
    if increase_effect == "NEUTRAL":
        regression = False
        improvement = False

    severity = "NONE"
    if regression:
        severity = "HIGH" if name in _HIGH_SEVERITY_METRICS else "MEDIUM"

    sign = "+" if diff >= 0 else ""
    return MetricDiff(
        metric_name=name,
        metric_group=group,
        direction_when_increase=increase_effect,
        baseline_value=bv,
        candidate_value=cv,
        absolute_diff=diff,
        relative_diff_ratio=ratio,
        changed_flag=diff != 0,
        improvement_flag=improvement,
        regression_flag=regression,
        severity=severity,
        summary_text=f"{name}: {bv} -> {cv} ({sign}{diff})",
    )


def _summarize_severity(regressions: Iterable[MetricDiff]) -> str:
    regressions = list(regressions)
    if any(m.severity == "HIGH" for m in regressions):
        return "HIGH"
    if regressions:
        return "MEDIUM"
    return "NONE"


def _build_summary(
    diffs: list[MetricDiff], net_score: float = 0.0, *, streaming_skipped: bool = False
) -> str:
    regressed = [m.metric_name for m in diffs if m.regression_flag]
    improved = [m.metric_name for m in diffs if m.improvement_flag]
    parts = []
    if regressed:
        parts.append(f"Regressed: {', '.join(regressed)}")
    if improved:
        parts.append(f"Improved: {', '.join(improved)}")
    if not parts:
        parts.append("No significant changes")
    parts.append(f"Net score: {net_score:+.2f}")
    if streaming_skipped:
        parts.append("Streaming metrics skipped (only one app has streaming queries)")
    return "; ".join(parts)
