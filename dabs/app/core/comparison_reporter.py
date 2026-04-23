"""Markdown report generator for comparison results.

Generates a human-readable before/after diff report with
improvement/regression indicators, color-coded severity, and summary.
Supports English and Japanese output.
"""

from __future__ import annotations

from .models import ComparisonResult, MetricDiff

# i18n labels
_LABELS = {
    "en": {
        "title": "Profile Comparison Report",
        "comparison_id": "Comparison ID",
        "overview": "Overview",
        "variant": "Variant",
        "analysis_id": "Analysis ID",
        "experiment": "Experiment",
        "fingerprint": "Fingerprint",
        "verdict": "Verdict",
        "regression_detected": "Regression detected",
        "no_regression": "No regression detected",
        "severity": "Severity",
        "metric_comparison": "Metric Comparison",
        "metric": "Metric",
        "baseline": "Baseline",
        "candidate": "Candidate",
        "change": "Change",
        "status": "Status",
        "regressions": "Regressions",
        "improvements": "Improvements",
        "ai_analysis": "AI Analysis",
    },
    "ja": {
        "title": "プロファイル比較レポート",
        "comparison_id": "比較ID",
        "overview": "概要",
        "variant": "バリアント",
        "analysis_id": "分析ID",
        "experiment": "実験",
        "fingerprint": "フィンガープリント",
        "verdict": "判定",
        "regression_detected": "性能劣化を検出",
        "no_regression": "性能劣化なし",
        "severity": "深刻度",
        "metric_comparison": "メトリクス比較",
        "metric": "メトリクス",
        "baseline": "ベースライン",
        "candidate": "候補",
        "change": "変化",
        "status": "ステータス",
        "regressions": "性能劣化",
        "improvements": "改善",
        "ai_analysis": "AI分析",
    },
}

_STATUS_LABELS = {
    "en": {"REGRESSION": "REGRESSION", "IMPROVED": "IMPROVED", "changed": "changed"},
    "ja": {"REGRESSION": "悪化", "IMPROVED": "改善", "changed": "軽微な変化"},
}


def _format_value(metric_name: str, value: float | None) -> str:
    if value is None:
        return "N/A"
    if "ratio" in metric_name or "percentage" in metric_name or "rate" in metric_name:
        return f"{value:.1%}" if value <= 1.0 else f"{value:.1f}%"
    if "bytes" in metric_name:
        return _format_bytes(value)
    if "ms" in metric_name:
        return _format_duration(value)
    if "count" in metric_name:
        return f"{int(value):,}"
    return f"{value:,.2f}"


def _format_bytes(b: float) -> str:
    if b >= 1024**3:
        return f"{b / 1024**3:.2f} GB"
    if b >= 1024**2:
        return f"{b / 1024**2:.2f} MB"
    if b >= 1024:
        return f"{b / 1024:.2f} KB"
    return f"{int(b)} B"


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


def generate_comparison_report(
    result: ComparisonResult, llm_summary: str = "", lang: str = "en"
) -> str:
    """Generate a Markdown comparison report from a ComparisonResult."""
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
        f"| **{L['variant']}** | {result.baseline_variant or 'N/A'} | {result.candidate_variant or 'N/A'} |"
    )
    lines.append(
        f"| **{L['analysis_id']}** | `{result.baseline_analysis_id[:12]}...` | `{result.candidate_analysis_id[:12]}...` |"
    )
    if result.experiment_id:
        lines.append(f"| **{L['experiment']}** | {result.experiment_id} | {result.experiment_id} |")
    if result.query_fingerprint:
        lines.append(
            f"| **{L['fingerprint']}** | `{result.query_fingerprint[:16]}...` | `{result.query_fingerprint[:16]}...` |"
        )
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
