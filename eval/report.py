"""Report output for evaluation results (JSON + console summary)."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from .models import EvalReport, QueryEvalResult


def to_json(report: EvalReport) -> str:
    """Serialize EvalReport to JSON string."""
    return json.dumps(asdict(report), indent=2, ensure_ascii=False, default=str)


def to_console(report: EvalReport) -> str:
    """Generate human-readable console summary."""
    lines: list[str] = []
    model = report.config.get("primary_model", "unknown")
    judge = report.config.get("judge_model", "")

    lines.append(f"Eval Report ({report.num_queries} queries, model={model})")
    lines.append("=" * 60)
    lines.append("")

    # Overall scores
    lines.append(f"  L1 Syntax Pass Rate:       {report.overall_l1_syntax:6.1%}")
    lines.append(f"  L1 Serverless Compliance:  {report.overall_l1_serverless:6.1%}")
    lines.append(f"  L2 Evidence Grounding:     {report.overall_l2_grounding:6.1%} avg")
    if report.overall_l3_diagnosis > 0:
        lines.append(f"  L3 Diagnosis Accuracy:     {report.overall_l3_diagnosis:5.1f}/5 avg")
    if report.overall_l4_relevance > 0:
        lines.append(f"  L4 Fix Relevance:          {report.overall_l4_relevance:5.1f}/5 avg")
        lines.append(f"  L4 Fix Feasibility:        {report.overall_l4_feasibility:5.1f}/5 avg")
    if judge:
        lines.append(f"  Judge model:               {judge}")
    lines.append("")

    # Per-query results
    lines.append("Per-Query Results:")
    lines.append("-" * 60)

    for qr in report.query_results:
        if qr.pipeline_error:
            lines.append(f"  {_short_id(qr)}  ERROR: {qr.pipeline_error[:60]}")
            continue

        parts = [
            f"{_short_id(qr)}",
            f"{qr.num_action_cards:2d} cards",
            f"L1={qr.l1_syntax_pass_rate:.0%}",
            f"L2={qr.l2_avg_grounding:.0%}",
        ]
        if qr.l3_avg_diagnosis > 0:
            parts.append(f"L3={qr.l3_avg_diagnosis:.1f}")
        if qr.l4_avg_relevance > 0:
            parts.append(f"L4={qr.l4_avg_relevance:.1f}")

        # Warnings
        warnings = []
        parse_errors = sum(
            1 for c in qr.card_results if c.l1.has_fix_sql and not c.l1.parses_ok
        )
        if parse_errors:
            warnings.append(f"{parse_errors} parse error(s)")
        serverless_violations = sum(
            1 for c in qr.card_results if not c.l1.serverless_compliant
        )
        if serverless_violations:
            warnings.append(f"{serverless_violations} serverless violation(s)")

        line = "  " + "  ".join(parts)
        if warnings:
            line += "  ⚠ " + ", ".join(warnings)
        lines.append(line)

    # Worst cards section
    worst = _find_worst_cards(report)
    if worst:
        lines.append("")
        lines.append("Worst Cards:")
        lines.append("-" * 60)
        for query_id, card_idx, problem, reason in worst[:5]:
            lines.append(f"  {query_id} #{card_idx}: \"{problem[:40]}\" — {reason}")

    lines.append("")
    return "\n".join(lines)


def _short_id(qr: QueryEvalResult) -> str:
    """Short display ID for a query result."""
    if qr.query_id and len(qr.query_id) > 20:
        return qr.query_id[:8] + "..."
    if qr.query_id:
        return qr.query_id
    return Path(qr.profile_path).stem[:15]


def _find_worst_cards(
    report: EvalReport,
) -> list[tuple[str, int, str, str]]:
    """Find the worst-scoring cards across all queries."""
    items: list[tuple[float, str, int, str, str]] = []

    for qr in report.query_results:
        qid = _short_id(qr)
        for cr in qr.card_results:
            # Parse errors
            if cr.l1.has_fix_sql and not cr.l1.parses_ok:
                items.append((0.0, qid, cr.card_index, cr.problem, "invalid SQL syntax"))
            # Serverless violations
            if not cr.l1.serverless_compliant:
                configs = ", ".join(cr.l1.unsupported_configs[:2])
                items.append((0.5, qid, cr.card_index, cr.problem, f"serverless: {configs}"))
            # Low L3 diagnosis
            if cr.l3 and cr.l3.diagnosis_score <= 2:
                items.append((
                    cr.l3.diagnosis_score,
                    qid, cr.card_index, cr.problem,
                    f"L3.diagnosis={cr.l3.diagnosis_score}",
                ))
            # Low L4 feasibility
            if cr.l4 and cr.l4.fix_feasibility <= 2:
                items.append((
                    cr.l4.fix_feasibility,
                    qid, cr.card_index, cr.problem,
                    f"L4.feasibility={cr.l4.fix_feasibility}",
                ))

    items.sort(key=lambda x: x[0])
    return [(qid, idx, prob, reason) for _, qid, idx, prob, reason in items]
