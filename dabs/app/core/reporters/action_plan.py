"""Recommended Actions section — unified action plan with Impact/Effort."""

from ..i18n import gettext as _
from ..models import ActionCard


def _impact_icon(level: str) -> str:
    return {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(level.lower(), "⚪")


def _effort_icon(level: str) -> str:
    return {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(level.lower(), "⚪")


def _quick_summary(action_cards: list[ActionCard], max_items: int = 3) -> str:
    """Generate a brief Quick Summary of the top action items."""
    top = action_cards[:max_items]
    lines: list[str] = [f"**{_('Quick Summary')}**\n"]
    for i, card in enumerate(top, 1):
        impact = card.expected_impact.upper() or "MEDIUM"
        lines.append(f"{i}. **{card.problem}** — Impact: {impact}")
    lines.append("")
    return "\n".join(lines)


def generate_action_plan_section(
    action_cards: list[ActionCard], *, include_header: bool = True
) -> str:
    """Generate unified Recommended Actions section.

    Format:
      🔴 Impact: HIGH | 🟡 Effort: LOW

    Cards should be sorted by priority_score descending before calling.
    """
    if not action_cards:
        return ""

    lines: list[str] = []

    if include_header:
        lines.append(f"\n## 🎯 {_('Recommended Actions')}\n")

    # Quick Summary at the top (top 3 items, one line each)
    lines.append(_quick_summary(action_cards))

    for idx, card in enumerate(action_cards, 1):
        impact = card.expected_impact.upper() or "MEDIUM"
        effort = card.effort.upper() or "MEDIUM"

        lines.append(f"### {idx}. {card.problem}\n")
        lines.append(
            f"{_impact_icon(card.expected_impact)} Impact: {impact} | "
            f"{_effort_icon(card.effort)} Effort: {effort}\n"
        )

        # Rationale (evidence)
        if card.evidence:
            lines.append(f"\n**{_('Rationale')}**\n")
            for e in card.evidence:
                lines.append(f"- {e}")
            lines.append("")

        # Cause hypothesis
        if card.likely_cause:
            lines.append(f"**{_('Cause Hypothesis')}:** {card.likely_cause}\n")

        # Fix
        if card.fix:
            lines.append(f"**{_('Improvement')}:** {card.fix}\n")

        # Fix SQL
        if card.fix_sql:
            lines.append(f"```sql\n{card.fix_sql}\n```\n")

        # Fix risk
        if card.risk:
            risk_text = f"**{_('Fix Risk')}:** {card.risk.upper()}"
            if card.risk_reason:
                risk_text += f" — {card.risk_reason}"
            lines.append(f"{risk_text}\n")

        # Verification
        if card.verification_steps:
            lines.append(f"**{_('Verification Metric')}:**\n")
            for step in card.verification_steps:
                metric = step.get("metric", "")
                sql = step.get("sql", "")
                expected = step.get("expected", "")
                if metric:
                    lines.append(f"- `{metric}` → {_('Expected')}: {expected}")
                elif sql:
                    lines.append(f"- {_('Run')}: `{sql}` → {_('Expected')}: {expected}")
            lines.append("")
        elif card.validation_metric:
            lines.append(f"**{_('Verification Metric')}:** `{card.validation_metric}`\n")

        lines.append("")

    return "\n".join(lines)
