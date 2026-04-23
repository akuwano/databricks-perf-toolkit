"""Build compact text summaries for sharing analysis results (Slack, etc.)."""

from __future__ import annotations

from .models import BottleneckIndicators, QueryMetrics


def _format_time(ms: int) -> str:
    if ms >= 60_000:
        return f"{ms / 60_000:.1f} min"
    if ms >= 1_000:
        return f"{ms / 1_000:.1f}s"
    return f"{ms} ms"


def _format_bytes_gb(b: int) -> str:
    if b >= 1_073_741_824:
        return f"{b / 1_073_741_824:.1f} GB"
    if b >= 1_048_576:
        return f"{b / 1_048_576:.1f} MB"
    if b >= 1_024:
        return f"{b / 1_024:.0f} KB"
    return f"{b} B"


def build_slack_summary(
    analysis_id: str,
    query_metrics: QueryMetrics,
    bottleneck_indicators: BottleneckIndicators,
    action_count: int = 0,
    base_url: str = "",
    warehouse_name: str = "",
    warehouse_size: str = "",
    top_action: str = "",
    critical_count: int | None = None,
    warning_count: int | None = None,
) -> str:
    """Build a compact plain-text summary suitable for Slack.

    Returns a multi-line string (4-6 lines) with key metrics and a link.
    critical_count/warning_count override in-memory list lengths when
    loading from Delta (where lists are not stored).
    """
    qid = query_metrics.query_id[:12] if query_metrics.query_id else "unknown"

    # Line 1: Header
    wh_info = ""
    if warehouse_name:
        wh_info = f" ({warehouse_name}"
        if warehouse_size:
            wh_info += f" / {warehouse_size}"
        wh_info += ")"
    line1 = f"DBSQL Analysis: {qid}{wh_info}"

    # Line 2: Key metrics
    time_str = _format_time(query_metrics.total_time_ms)
    cache_pct = f"{bottleneck_indicators.cache_hit_ratio * 100:.1f}%"
    photon_pct = f"{bottleneck_indicators.photon_ratio * 100:.1f}%"
    spill_str = _format_bytes_gb(bottleneck_indicators.spill_bytes)
    line2 = f"Time: {time_str} | Cache: {cache_pct} | Photon: {photon_pct} | Spill: {spill_str}"

    # Line 3: Issues + actions
    n_critical = (
        critical_count if critical_count is not None else len(bottleneck_indicators.critical_issues)
    )
    n_warnings = warning_count if warning_count is not None else len(bottleneck_indicators.warnings)
    line3 = f"{n_critical} critical, {n_warnings} warnings | {action_count} actions"

    lines = [line1, line2, line3]

    # Line 4: Top action (optional)
    if top_action:
        lines.append(f"Top: {top_action}")

    # Line 5: Link (optional)
    if base_url:
        url = base_url.rstrip("/")
        lines.append(f"Link: {url}/shared/{analysis_id}")

    return "\n".join(lines)
