"""Rule-based summary and recommendations."""

from __future__ import annotations

from ..constants import Severity
from ..i18n import gettext as _
from ..models import (
    ActionCard,
    Alert,
    QueryMetrics,
    StreamingContext,
)
from ..utils import format_bytes, format_time_ms


def generate_rule_based_summary(
    alerts: list[Alert],
    qm: QueryMetrics | None = None,
    action_cards: list[ActionCard] | None = None,
) -> str:
    """Generate rule-based Executive Summary for --no-llm mode (Section 1).

    Produces a concise summary from alerts, query metrics, and top action cards.

    Args:
        alerts: List of Alert objects
        qm: QueryMetrics for context (status, execution time, data read)
        action_cards: Top action cards for bottleneck summary

    Returns:
        Markdown formatted Executive Summary
    """
    if not alerts:
        return f"{_('No significant issues detected. Query performance appears normal.')}\n"

    # Find highest severity
    severity_rank = {
        Severity.CRITICAL: 4,
        Severity.HIGH: 3,
        Severity.MEDIUM: 2,
        Severity.INFO: 1,
    }
    sorted_alerts = sorted(alerts, key=lambda a: severity_rank.get(a.severity, 0), reverse=True)
    top_alert = sorted_alerts[0]

    lines = []
    # Severity badge
    badge = top_alert.severity.value.upper()
    lines.append(f"**{badge}**\n")

    # Query status context (especially important for FAILED queries)
    if qm:
        status_line = f"{_('Status')}: **{qm.status}**"
        status_line += f" | {_('Total Execution Time')}: **{format_time_ms(qm.total_time_ms)}**"
        if qm.read_bytes > 0:
            status_line += f" | {_('Total Read')}: **{format_bytes(qm.read_bytes)}**"
        lines.append(status_line + "\n")

    # Top issue summary
    lines.append(f"{top_alert.message}\n")

    # Top bottlenecks from action cards (up to 2)
    if action_cards:
        top_cards = sorted(action_cards, key=lambda c: c.priority_score, reverse=True)[:2]
        lines.append(f"**{_('Key Findings')}:**\n")
        for card in top_cards:
            impact_label = card.expected_impact.upper() if card.expected_impact else "?"
            lines.append(f"- **[{impact_label}]** {card.problem}")
            if card.likely_cause:
                lines.append(f"  - {_('Likely Cause')}: {card.likely_cause}")
        lines.append("")

    # Count by severity
    counts: dict[str, int] = {}
    for alert in alerts:
        key = alert.severity.value.upper()
        counts[key] = counts.get(key, 0) + 1

    count_parts = []
    for sev_name in ["CRITICAL", "HIGH", "MEDIUM", "INFO"]:
        if sev_name in counts:
            count_parts.append(f"{counts[sev_name]} {sev_name}")
    if count_parts:
        lines.append(f"{_('Total alerts')}: {', '.join(count_parts)}\n")

    return "\n".join(lines)


def generate_rule_based_recommendations(action_cards: list[ActionCard]) -> str:
    """Generate rule-based Recommendations for --no-llm mode (Section 7).

    Groups action cards by impact into Priority 1/2/3.

    Args:
        action_cards: List of ActionCard sorted by priority_score

    Returns:
        Markdown formatted Recommendations section
    """
    if not action_cards:
        return ""

    # Group by expected_impact into priority levels
    priority_groups: dict[str, list[ActionCard]] = {
        "Priority 1": [],
        "Priority 2": [],
        "Priority 3": [],
    }

    for card in sorted(action_cards, key=lambda c: c.priority_score, reverse=True):
        if card.expected_impact == "high":
            priority_groups["Priority 1"].append(card)
        elif card.expected_impact == "medium":
            priority_groups["Priority 2"].append(card)
        else:
            priority_groups["Priority 3"].append(card)

    lines = []
    for priority_label, cards in priority_groups.items():
        if not cards:
            continue
        lines.append(f"### {priority_label}\n")
        for card in cards:
            lines.append(f"**{card.problem}**\n")
            lines.append(f"- {_('Fix')}: {card.fix}")
            if card.likely_cause:
                lines.append(f"- {_('Likely Cause')}: {card.likely_cause}")
            if card.fix_sql:
                lines.append(f"\n```sql\n{card.fix_sql}\n```")
            lines.append("")

    return "\n".join(lines)


def generate_streaming_executive_summary(
    alerts: list[Alert],
    ctx: StreamingContext,
    batch_stats: dict,
    *,
    action_cards: list[ActionCard] | None = None,
) -> str:
    """Generate Executive Summary for streaming DLT/SDP queries.

    Focuses on per-batch metrics instead of cumulative totals.
    """
    severity_rank = {
        Severity.CRITICAL: 4,
        Severity.HIGH: 3,
        Severity.MEDIUM: 2,
        Severity.INFO: 1,
    }
    lines: list[str] = []

    # Severity badge from alerts
    if alerts:
        top = max(alerts, key=lambda a: severity_rank.get(a.severity, 0))
        lines.append(f"**{top.severity.value.upper()}**\n")

    # Primary KPIs: batch-oriented
    kpi_parts = [f"{_('Status')}: **RUNNING (Streaming)**"]
    kpi_parts.append(
        f"{_('Finished Batches')}: **{batch_stats['finished_count']}/{batch_stats['batch_count']}**"
    )
    if batch_stats["batch_count"] > 0:
        kpi_parts.append(
            f"{_('Avg Batch Duration')}: **{format_time_ms(int(batch_stats['duration_avg_ms']))}**"
        )
        if batch_stats.get("duration_p95_ms", 0) > 0:
            kpi_parts.append(f"P95: **{format_time_ms(batch_stats['duration_p95_ms'])}**")
    lines.append(" | ".join(kpi_parts) + "\n")

    # Variance and slow batch summary
    if batch_stats["batch_count"] > 0:
        cv = batch_stats.get("duration_cv", 0)
        slow = batch_stats.get("slow_batches", [])
        detail_parts = []
        if cv > 0:
            detail_parts.append(f"{_('Duration CV')}: {cv:.2f}")
        if slow:
            detail_parts.append(
                f"**{len(slow)} {_('slow batch(es)')}** (>{int(batch_stats['duration_avg_ms'] * 2):,} ms)"
            )
        if detail_parts:
            lines.append(" | ".join(detail_parts) + "\n")

    # Top alert message
    if alerts:
        top_alert = max(alerts, key=lambda a: severity_rank.get(a.severity, 0))
        lines.append(f"{top_alert.message}\n")

    # Top bottlenecks from action cards
    if action_cards:
        top_cards = sorted(action_cards, key=lambda c: c.priority_score, reverse=True)[:2]
        lines.append(f"**{_('Key Findings')}:**\n")
        for card in top_cards:
            impact_label = card.expected_impact.upper() if card.expected_impact else "?"
            lines.append(f"- **[{impact_label}]** {card.problem}")
            if card.likely_cause:
                lines.append(f"  - {_('Likely Cause')}: {card.likely_cause}")
        lines.append("")

    return "\n".join(lines)


def generate_top5_recommendations_section(
    action_cards: list[ActionCard],
    selected_action_cards: list[ActionCard] | None = None,
    alerts: list[Alert] | None = None,
) -> str:
    """Generate Top-N recommendations (default 10) with remaining items collapsed.

    The Top-N list is already curated by priority + diversity, so the
    ordering *is* the priority signal — no P0/P1/P2 label is needed.
    Each card is numbered (1.-10.) to make that ordering obvious, and
    severity is expressed through an inline ``Impact`` badge rather
    than a separate grouping.

    v5.16.8: limit was raised from 5 → 10 so every Top Alert has room
    to receive its corresponding action card. The earlier 3-cap on
    preserved cards was also removed upstream.

    When ``alerts`` is supplied (already severity-sorted by the caller),
    each recommended action surfaces a ``→ アラート #N`` reference tag
    so the reader can see which Top Alert each action solves. The
    Top-N list is also re-ordered so that actions addressing CRITICAL
    alerts come before those addressing HIGH/MEDIUM ones.
    """
    if not action_cards:
        return ""

    selected_action_cards = selected_action_cards or action_cards[:10]

    # Re-sort Top-5 so CRITICAL-addressing actions surface first when
    # alert context is available. Ties preserve existing order (impact/
    # effort / diversity rerank already done upstream).
    if alerts:
        from .alert_crossref import alert_severity_rank_for_card

        selected_action_cards = sorted(
            selected_action_cards,
            key=lambda c: alert_severity_rank_for_card(c, alerts),
        )

    selected_ids = {id(card) for card in selected_action_cards}
    preserved = [card for card in action_cards if getattr(card, "is_preserved", False)]
    others = [card for card in action_cards if id(card) not in selected_ids]

    lines: list[str] = []
    if len(preserved) >= 5:
        lines.append(f"### {_('Must-read alerts')}\n")
        for card in preserved:
            lines.append(f"- **{card.problem}**")
        lines.append("")

    lines.append(f"### {_('Top 10 recommended actions')}\n")
    lines.append(
        _(
            "Apply the highest-impact items first, then re-analyze to uncover the next opportunities."
        )
    )
    lines.append("")
    for idx, card in enumerate(selected_action_cards, start=1):
        badge_bits: list[str] = []
        if card.expected_impact:
            badge_bits.append(f"{_('Impact')}: {card.expected_impact.upper()}")
        if card.effort:
            badge_bits.append(f"{_('Effort')}: {card.effort.upper()}")
        # Alert reference tag: `→ アラート #2, #4` or `→ (全般)`.
        ref_tag = _format_alert_reference(card, alerts) if alerts is not None else ""
        badge = f"  — {', '.join(badge_bits)}" if badge_bits else ""
        lines.append(f"**{idx}. {card.problem}**{badge}{ref_tag}\n")
        lines.append(f"- {_('Fix')}: {card.fix}")
        if card.selected_because:
            lines.append(f"- {_('Selected because')}: {card.selected_because}")
        if card.fix_sql:
            lines.append(f"\n```sql\n{card.fix_sql}\n```")
        lines.append("")

    if others:
        lines.append(f"<details><summary>{_('Other recommendations')}</summary>\n")
        for card in others:
            lines.append(f"**{card.problem}**\n")
            lines.append(f"- {_('Fix')}: {card.fix}")
            lines.append("")
        lines.append("</details>")

    return "\n".join(lines)


def _format_alert_reference(card: ActionCard, alerts: list[Alert] | None) -> str:
    """Format `  → アラート #2, #4` reference for a card. Empty if no alerts."""
    if not alerts:
        return ""
    from .alert_crossref import match_card_to_alert_numbers

    nums = match_card_to_alert_numbers(card, alerts)
    if nums:
        rendered = ", ".join(f"#{n}" for n in nums)
        # "Addresses alert" avoids collision with the existing "Alert"→"注意"
        # translation which is used as a section-header label.
        return f"  → {_('Addresses alert')} {rendered}"
    return f"  → ({_('General recommendation')})"
