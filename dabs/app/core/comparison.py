"""Comparison service for before/after profile analysis.

Computes per-metric diffs between a baseline and candidate
ProfileAnalysis, applying direction-aware improvement/regression
detection.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable

from .models import (
    ComparisonRequest,
    ComparisonResult,
    MetricDiff,
    ProfileAnalysis,
)

# metric_name -> (metric_group, effect_when_value_increases)
COMPARABLE_METRICS: dict[str, tuple[str, str]] = {
    "total_time_ms": ("latency", "WORSENS"),
    "execution_time_ms": ("latency", "WORSENS"),
    "compilation_time_ms": ("latency", "WORSENS"),
    "read_bytes": ("io", "WORSENS"),
    "read_remote_bytes": ("io", "WORSENS"),
    "read_cache_bytes": ("cache", "IMPROVES"),
    "spill_to_disk_bytes": ("spill", "WORSENS"),
    "spill_bytes": ("spill", "WORSENS"),
    "bytes_read_from_cache_percentage": ("cache", "IMPROVES"),
    "photon_ratio": ("engine", "IMPROVES"),
    "remote_read_ratio": ("io", "WORSENS"),
    "bytes_pruning_ratio": ("io", "IMPROVES"),
    "shuffle_impact_ratio": ("shuffle", "WORSENS"),
    "cloud_storage_retry_ratio": ("cloud_storage", "WORSENS"),
    "oom_fallback_count": ("engine", "WORSENS"),
    "queued_provisioning_time_ms": ("latency", "WORSENS"),
    "queued_overload_time_ms": ("latency", "WORSENS"),
    "result_fetch_time_ms": ("latency", "WORSENS"),
    "shuffle_bytes_written_total": ("shuffle", "WORSENS"),
    "shuffle_remote_bytes_read_total": ("shuffle", "WORSENS"),
    "write_remote_bytes": ("io", "WORSENS"),
    "write_remote_files": ("io", "WORSENS"),
    "write_remote_rows": ("io", "WORSENS"),
}

# Metrics where regression is considered HIGH severity
_HIGH_SEVERITY_METRICS = {"total_time_ms", "spill_to_disk_bytes", "spill_bytes"}

# Default threshold ratio for regression/improvement detection (10%)
_DEFAULT_THRESHOLD = 0.10

# Metrics that are noisy at small absolute values — ignore regression if baseline < threshold
_NOISE_FLOOR: dict[str, float] = {
    "compilation_time_ms": 10_000,  # <10s compilation is noise
    "cloud_storage_retry_ratio": 0.01,
    "queued_provisioning_time_ms": 3_000,  # <3s queue is noise
    "queued_overload_time_ms": 3_000,
    "result_fetch_time_ms": 3_000,  # <3s fetch is noise
    "shuffle_bytes_written_total": 100_000_000,  # <100MB shuffle is noise
    "shuffle_remote_bytes_read_total": 100_000_000,
    "write_remote_bytes": 10_000_000,  # <10MB write is noise
    "write_remote_files": 5,  # <5 files is noise
    "write_remote_rows": 1000,  # <1000 rows is noise
}

# Cache/IO metrics whose regression should be suppressed when total IO improves significantly
_IO_DEPENDENT_METRICS = {
    "read_cache_bytes",
    "bytes_read_from_cache_percentage",
    "remote_read_ratio",
}

# Weights for net score calculation (higher = more important)
_METRIC_WEIGHTS: dict[str, float] = {
    "total_time_ms": 5.0,
    "execution_time_ms": 4.0,
    "spill_to_disk_bytes": 4.0,
    "spill_bytes": 4.0,
    "photon_ratio": 3.0,
    "shuffle_impact_ratio": 3.0,
    "read_bytes": 2.0,
    "read_remote_bytes": 2.0,
    "read_cache_bytes": 2.0,
    "bytes_read_from_cache_percentage": 2.0,
    "remote_read_ratio": 1.5,
    "bytes_pruning_ratio": 1.5,
    "compilation_time_ms": 1.0,
    "cloud_storage_retry_ratio": 1.0,
    "oom_fallback_count": 3.0,
    "queued_provisioning_time_ms": 1.0,
    "queued_overload_time_ms": 1.0,
    "result_fetch_time_ms": 1.0,
    "shuffle_bytes_written_total": 2.0,
    "shuffle_remote_bytes_read_total": 1.5,
    "write_remote_bytes": 2.0,
    "write_remote_files": 1.0,
    "write_remote_rows": 1.0,
}


class ComparisonService:
    """Compares two ProfileAnalysis results and produces metric diffs."""

    def __init__(self, threshold: float = _DEFAULT_THRESHOLD) -> None:
        self._threshold = threshold

    def compare_analyses(
        self,
        baseline: ProfileAnalysis,
        candidate: ProfileAnalysis,
        request: ComparisonRequest,
    ) -> ComparisonResult:
        """Compare baseline vs candidate and return a ComparisonResult."""
        result = ComparisonResult(
            comparison_id=str(uuid.uuid4()),
            baseline_analysis_id=request.baseline_analysis_id,
            candidate_analysis_id=request.candidate_analysis_id,
            query_fingerprint=baseline.analysis_context.query_fingerprint,
            experiment_id=baseline.analysis_context.experiment_id,
            baseline_variant=baseline.analysis_context.variant,
            candidate_variant=candidate.analysis_context.variant,
        )

        for metric_name, (group, increase_effect) in COMPARABLE_METRICS.items():
            bv = self._extract_metric(baseline, metric_name)
            cv = self._extract_metric(candidate, metric_name)
            result.metric_diffs.append(
                self._build_metric_diff(metric_name, group, increase_effect, bv, cv)
            )

        # Suppress cache/IO-dependent regressions when total IO improved significantly
        read_bytes_diff = next(
            (m for m in result.metric_diffs if m.metric_name == "read_bytes"), None
        )
        io_improved = (
            read_bytes_diff is not None
            and read_bytes_diff.improvement_flag
            and read_bytes_diff.relative_diff_ratio is not None
            and abs(read_bytes_diff.relative_diff_ratio) >= 0.3  # >30% IO reduction
        )
        if io_improved:
            for m in result.metric_diffs:
                if m.metric_name in _IO_DEPENDENT_METRICS and m.regression_flag:
                    m.regression_flag = False
                    m.severity = "NONE"

        regressions = [m for m in result.metric_diffs if m.regression_flag]
        [m for m in result.metric_diffs if m.improvement_flag]

        # Net score: weighted sum of improvements minus regressions
        net_score = 0.0
        for m in result.metric_diffs:
            w = _METRIC_WEIGHTS.get(m.metric_name, 1.0)
            if m.improvement_flag and m.relative_diff_ratio is not None:
                net_score += w * abs(m.relative_diff_ratio)
            elif m.regression_flag and m.relative_diff_ratio is not None:
                net_score -= w * abs(m.relative_diff_ratio)

        # Overall verdict: only flag regression if net score is negative
        # OR a high-severity metric regressed significantly
        high_regressions = [m for m in regressions if m.severity == "HIGH"]
        result.regression_detected = bool(high_regressions) or (net_score < 0 and bool(regressions))
        result.regression_severity = (
            self._summarize_severity(regressions) if result.regression_detected else "NONE"
        )
        result.summary = self._build_summary(result.metric_diffs, net_score)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_metric(self, analysis: ProfileAnalysis, name: str) -> float | None:
        """Extract a metric value from QueryMetrics or BottleneckIndicators."""
        for obj in (analysis.query_metrics, analysis.bottleneck_indicators):
            if hasattr(obj, name):
                v = getattr(obj, name)
                return float(v) if v is not None else None
        return None

    def _build_metric_diff(
        self,
        name: str,
        group: str,
        increase_effect: str,
        bv: float | None,
        cv: float | None,
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

        # Check noise floor: ignore regression if baseline is too small to matter
        noise_floor = _NOISE_FLOOR.get(name, 0)
        below_noise = bv < noise_floor and cv < noise_floor

        regression = not below_noise and (
            (diff > 0 and increase_effect == "WORSENS" and abs_ratio >= self._threshold)
            or (diff < 0 and increase_effect == "IMPROVES" and abs_ratio >= self._threshold)
        )
        improvement = (
            diff < 0 and increase_effect == "WORSENS" and abs_ratio >= self._threshold
        ) or (diff > 0 and increase_effect == "IMPROVES" and abs_ratio >= self._threshold)

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

    def _summarize_severity(self, regressions: Iterable[MetricDiff]) -> str:
        regressions = list(regressions)
        if any(m.severity == "HIGH" for m in regressions):
            return "HIGH"
        if regressions:
            return "MEDIUM"
        return "NONE"

    def _build_summary(self, diffs: list[MetricDiff], net_score: float = 0.0) -> str:
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
        return "; ".join(parts)
