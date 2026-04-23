"""Markdown report generator for Spark application comparison results.

Generates a human-readable before/after diff report with
improvement/regression indicators, color-coded severity, and summary.
Supports English and Japanese output.
"""

from __future__ import annotations

from typing import Any

from .models import ComparisonResult, MetricDiff

# i18n labels
_LABELS = {
    "en": {
        "title": "Spark Application Comparison Report",
        "comparison_id": "Comparison ID",
        "overview": "Overview",
        "baseline": "Baseline",
        "candidate": "Candidate",
        "app_id": "Application ID",
        "cluster": "Cluster",
        "duration": "Duration",
        "node_type": "Node Type",
        "workers": "Workers",
        "verdict": "Verdict",
        "regression_detected": "Regression detected",
        "no_regression": "No regression detected",
        "severity": "Severity",
        "cluster_config": "Cluster Configuration",
        "dbr_version": "DBR Version",
        "availability": "Availability",
        "metric_comparison": "Metric Comparison",
        "metric": "Metric",
        "change": "Change",
        "status": "Status",
        "regressions": "Regressions",
        "improvements": "Improvements",
        "ai_analysis": "AI Analysis",
    },
    "ja": {
        "title": "Sparkアプリケーション比較レポート",
        "comparison_id": "比較ID",
        "overview": "概要",
        "baseline": "ベースライン",
        "candidate": "候補",
        "app_id": "アプリケーションID",
        "cluster": "クラスタ",
        "duration": "実行時間",
        "node_type": "ノードタイプ",
        "workers": "ワーカー数",
        "verdict": "判定",
        "regression_detected": "性能劣化を検出",
        "no_regression": "性能劣化なし",
        "severity": "深刻度",
        "cluster_config": "クラスタ構成",
        "dbr_version": "DBRバージョン",
        "availability": "可用性",
        "metric_comparison": "メトリクス比較",
        "metric": "メトリクス",
        "change": "変化",
        "status": "ステータス",
        "regressions": "性能劣化",
        "improvements": "改善",
        "ai_analysis": "AI分析",
    },
}

_STATUS_LABELS = {
    "en": {"REGRESSION": "REGRESSION", "IMPROVED": "IMPROVED", "changed": "changed"},
    "ja": {"REGRESSION": "悪化", "IMPROVED": "改善", "changed": "変化"},
}


def _format_value(metric_name: str, value: float | None) -> str:
    """Format a metric value based on its name convention."""
    if value is None:
        return "N/A"
    if metric_name.endswith("_ms"):
        return _format_duration(value)
    if metric_name.endswith("_gb"):
        return f"{value:.2f} GB"
    if metric_name.endswith("_mb"):
        return f"{value:.2f} MB"
    if metric_name.endswith("_pct"):
        return f"{value:.1f}%"
    if "ratio" in metric_name or "rate" in metric_name:
        return f"{value:.2f}" if value <= 1.0 else f"{value:.1f}%"
    if "count" in metric_name or metric_name in (
        "total_tasks",
        "stages_with_disk_spill",
        "bottleneck_count",
    ):
        return f"{int(value):,}"
    return f"{value:,.2f}"


def _format_duration(ms: float) -> str:
    if ms >= 60_000:
        return f"{ms / 60_000:.1f} min"
    if ms >= 1_000:
        return f"{ms / 1_000:.2f} s"
    return f"{int(ms)} ms"


def _change_indicator(diff: MetricDiff, lang: str = "en") -> str:
    sl = _STATUS_LABELS.get(lang, _STATUS_LABELS["en"])
    if diff.regression_flag:
        return sl["REGRESSION"]
    if diff.improvement_flag:
        return sl["IMPROVED"]
    if diff.changed_flag:
        return sl["changed"]
    return "-"


def _percent_str(diff: MetricDiff) -> str:
    if diff.relative_diff_ratio is None:
        return "N/A"
    pct = diff.relative_diff_ratio * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def _safe_get(d: dict[str, Any] | None, key: str, default: str = "N/A") -> str:
    """Safely get a value from a dict, returning default if missing."""
    if d is None:
        return default
    v = d.get(key)
    if v is None:
        return default
    return str(v)


def generate_spark_comparison_report(
    result: ComparisonResult,
    baseline_summary: dict[str, Any] | None = None,
    candidate_summary: dict[str, Any] | None = None,
    llm_summary: str = "",
    lang: str = "en",
) -> str:
    """Generate a Markdown comparison report for Spark applications.

    Args:
        result: ComparisonResult with metric diffs.
        baseline_summary: gold_application_summary row for baseline.
        candidate_summary: gold_application_summary row for candidate.
        llm_summary: Optional LLM-generated analysis text.
        lang: Output language ("en" or "ja").

    Returns:
        Markdown report string.
    """
    L = _LABELS.get(lang, _LABELS["en"])
    lines: list[str] = []

    # Header
    lines.append(f"# {L['title']}")
    lines.append("")
    lines.append(f"**{L['comparison_id']}:** `{result.comparison_id}`")
    lines.append("")

    # Overview table
    lines.append(f"## {L['overview']}")
    lines.append("")
    lines.append(f"| | {L['baseline']} | {L['candidate']} |")
    lines.append("|---|---|---|")
    lines.append(
        f"| **{L['app_id']}** "
        f"| `{_safe_get(baseline_summary, 'app_id')}` "
        f"| `{_safe_get(candidate_summary, 'app_id')}` |"
    )
    lines.append(
        f"| **{L['cluster']}** "
        f"| {_safe_get(baseline_summary, 'cluster_id')} "
        f"| {_safe_get(candidate_summary, 'cluster_id')} |"
    )

    # Duration
    b_dur = baseline_summary.get("duration_min") if baseline_summary else None
    c_dur = candidate_summary.get("duration_min") if candidate_summary else None
    b_dur_str = f"{b_dur:.1f} min" if b_dur is not None else "N/A"
    c_dur_str = f"{c_dur:.1f} min" if c_dur is not None else "N/A"
    lines.append(f"| **{L['duration']}** | {b_dur_str} | {c_dur_str} |")

    lines.append(
        f"| **{L['node_type']}** "
        f"| {_safe_get(baseline_summary, 'worker_node_type')} "
        f"| {_safe_get(candidate_summary, 'worker_node_type')} |"
    )

    # Workers
    b_workers = _format_workers(baseline_summary)
    c_workers = _format_workers(candidate_summary)
    lines.append(f"| **{L['workers']}** | {b_workers} | {c_workers} |")
    lines.append("")

    # Verdict
    lines.append(f"## {L['verdict']}")
    lines.append("")
    if result.regression_detected:
        lines.append(
            f"**{L['regression_detected']}** ({L['severity']}: **{result.regression_severity}**)"
        )
    else:
        lines.append(f"**{L['no_regression']}**")
    lines.append("")
    lines.append(f"> {result.summary}")
    lines.append("")

    # Cluster Configuration Comparison
    lines.append(f"## {L['cluster_config']}")
    lines.append("")
    lines.append(f"| | {L['baseline']} | {L['candidate']} |")
    lines.append("|---|---|---|")
    lines.append(
        f"| **{L['node_type']}** "
        f"| {_safe_get(baseline_summary, 'worker_node_type')} "
        f"| {_safe_get(candidate_summary, 'worker_node_type')} |"
    )
    lines.append(f"| **{L['workers']}** | {b_workers} | {c_workers} |")
    lines.append(
        f"| **{L['dbr_version']}** "
        f"| {_safe_get(baseline_summary, 'dbr_version')} "
        f"| {_safe_get(candidate_summary, 'dbr_version')} |"
    )
    lines.append(
        f"| **{L['availability']}** "
        f"| {_safe_get(baseline_summary, 'availability')} "
        f"| {_safe_get(candidate_summary, 'availability')} |"
    )
    lines.append("")

    # Group metrics
    groups: dict[str, list[MetricDiff]] = {}
    for md in result.metric_diffs:
        g = md.metric_group or "other"
        groups.setdefault(g, []).append(md)

    # Metric diff table
    lines.append(f"## {L['metric_comparison']}")
    lines.append("")

    for group_name, diffs in groups.items():
        lines.append(f"### {group_name.replace('_', ' ').title()}")
        lines.append("")
        lines.append(
            f"| {L['metric']} | {L['baseline']} | {L['candidate']} | {L['change']} | {L['status']} |"
        )
        lines.append("|--------|----------|-----------|--------|--------|")

        for md in diffs:
            name = md.metric_name.replace("_", " ").title()
            bv = _format_value(md.metric_name, md.baseline_value)
            cv = _format_value(md.metric_name, md.candidate_value)
            pct = _percent_str(md)
            indicator = _change_indicator(md, lang)
            lines.append(f"| {name} | {bv} | {cv} | {pct} | {indicator} |")

        lines.append("")

    # Regressions
    regressions = [m for m in result.metric_diffs if m.regression_flag]
    if regressions:
        lines.append(f"## {L['regressions']}")
        lines.append("")
        for md in regressions:
            name = md.metric_name.replace("_", " ").title()
            lines.append(
                f"- **{name}** ({md.severity}): "
                f"{_format_value(md.metric_name, md.baseline_value)} -> "
                f"{_format_value(md.metric_name, md.candidate_value)} "
                f"({_percent_str(md)})"
            )
        lines.append("")

    # Improvements
    improvements = [m for m in result.metric_diffs if m.improvement_flag]
    if improvements:
        lines.append(f"## {L['improvements']}")
        lines.append("")
        for md in improvements:
            name = md.metric_name.replace("_", " ").title()
            lines.append(
                f"- **{name}**: "
                f"{_format_value(md.metric_name, md.baseline_value)} -> "
                f"{_format_value(md.metric_name, md.candidate_value)} "
                f"({_percent_str(md)})"
            )
        lines.append("")

    # LLM Summary
    if llm_summary:
        lines.append(f"## {L['ai_analysis']}")
        lines.append("")
        lines.append(llm_summary)
        lines.append("")

    return "\n".join(lines)


def _format_workers(summary: dict[str, Any] | None) -> str:
    """Format worker count range from summary dict."""
    if summary is None:
        return "N/A"
    min_w = summary.get("min_workers")
    max_w = summary.get("max_workers")
    if min_w is None and max_w is None:
        return "N/A"
    if min_w == max_w:
        return str(min_w) if min_w is not None else "N/A"
    return f"{min_w or '?'}-{max_w or '?'}"
