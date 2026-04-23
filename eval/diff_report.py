"""Report output for diff results (JSON + console summary)."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .diff_judge import DiffReport, DiffVerdict


def diff_to_json(report: DiffReport) -> str:
    """Serialize DiffReport to JSON string."""
    return json.dumps(asdict(report), indent=2, ensure_ascii=False, default=str)


def diff_to_console(report: DiffReport) -> str:
    """Generate human-readable diff report."""
    lines: list[str] = []

    lines.append(f"Diff Report: current vs {report.git_ref} ({report.num_profiles} profiles)")
    lines.append("=" * 65)
    lines.append("")

    verdict_icons = {
        "improved": "✅",
        "regressed": "❌",
        "unchanged": "➖",
        "error": "⚠️",
    }

    for v in report.verdicts:
        icon = verdict_icons.get(v.verdict, "?")
        name = Path(v.profile_path).stem[:25] if v.profile_path else "unknown"
        cards = f"{v.baseline_card_count}→{v.current_card_count} cards"
        reason = v.reasoning[:60] if v.reasoning else ""

        lines.append(f"  {icon} {name:<25s}  {cards:<12s}  {v.verdict.upper()}")
        if reason:
            lines.append(f"     {reason}")

        # Show deltas if available
        if v.verdict not in ("error", "") and any(
            d != 3 for d in [v.diagnosis_delta, v.evidence_delta, v.fix_delta, v.coverage_delta]
        ):
            deltas = (
                f"diag={v.diagnosis_delta}/5  "
                f"evidence={v.evidence_delta}/5  "
                f"fix={v.fix_delta}/5  "
                f"coverage={v.coverage_delta}/5"
            )
            lines.append(f"     [{deltas}]")
        lines.append("")

    lines.append("-" * 65)
    lines.append(f"Summary: {report.summary}")
    lines.append("")

    return "\n".join(lines)
