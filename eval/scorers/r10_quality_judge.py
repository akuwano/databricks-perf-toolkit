"""R10 Layer B — LLM-judge wrapper (V6.1 Day 4-5).

Bridges the canonical Report into the existing L3/L4 LLM-as-judge so the
R10 add-on can populate ``layer_b_score``. The judge is invoked per-action
and aggregated into a single 0..1 score.

Design (docs/eval/r10_quality_addon_design.md §6):
- Layer B = (L3_avg + L4_avg) / 2 / 5  (1-5 scale → 0-1)
- LLM API failure → return None (caller falls back to Layer A only)
- Cost control: top-N actions only (default 5, sorted by R10 layer A
  proxy via priority_rank or first-N order)
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def score_layer_b(
    canonical_report: dict[str, Any],
    *,
    databricks_host: str,
    databricks_token: str,
    judge_model: str = "databricks-claude-sonnet-4",
    profile_summary: str = "",
    query_sql: str = "",
    top_n: int = 5,
) -> tuple[float | None, list[str]]:
    """Run the L3/L4 judge on up to ``top_n`` Actions and return
    (layer_b_score, reasons).

    Returns:
        (None, []) when the judge cannot run (no API access, no actions).
        (score in [0,1], reasons[]) otherwise.
    """
    if not databricks_host or not databricks_token:
        return None, []

    findings = canonical_report.get("findings") or []
    if not findings:
        return None, []

    # Lazy import — judge depends on core modules
    try:
        from core.models import ActionCard
        from .l3l4_judge import score_l3l4
    except ImportError as e:
        logger.warning("R10 Layer B unavailable (import error): %s", e)
        return None, []

    # Collect candidate actions, sorted by priority_rank (high first)
    candidates: list[tuple[dict, dict]] = []
    for f in findings:
        for a in f.get("actions") or []:
            candidates.append((f, a))
    if not candidates:
        return None, []

    candidates.sort(
        key=lambda fa: int(fa[1].get("priority_rank", 0) or 0),
        reverse=True,
    )
    candidates = candidates[: max(1, top_n)]

    diag_scores: list[int] = []
    feas_scores: list[int] = []
    rel_scores: list[int] = []
    reasons: list[str] = []

    for finding, action in candidates:
        # Build a synthetic ActionCard for the judge prompt
        card = ActionCard(
            problem=finding.get("title") or finding.get("issue_id") or "",
            evidence=[
                f"{e.get('metric')}={e.get('value_display')}"
                for e in (finding.get("evidence") or [])
                if isinstance(e, dict) and e.get("metric")
            ],
            likely_cause=action.get("why") or finding.get("description") or "",
            fix=action.get("what") or "",
            fix_sql=action.get("fix_sql") or "",
            expected_impact=action.get("expected_effect") or "",
            effort=action.get("effort") or "medium",
        )
        try:
            l3, l4 = score_l3l4(
                card,
                profile_summary,
                query_sql,
                databricks_host,
                databricks_token,
                judge_model=judge_model,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("Layer B judge failed for action %s: %s", action.get("action_id"), e)
            continue

        if l3.diagnosis_score:
            diag_scores.append(l3.diagnosis_score)
        if l4.fix_feasibility:
            feas_scores.append(l4.fix_feasibility)
        if l4.fix_relevance:
            rel_scores.append(l4.fix_relevance)
        if l3.reasoning:
            reasons.append(f"{action.get('action_id', '?')}: {l3.reasoning[:120]}")

    counts = sum(map(len, [diag_scores, feas_scores, rel_scores]))
    if counts == 0:
        return None, []

    # Layer B aggregate: weighted average of L3 diagnosis (50%), L4 fix
    # feasibility (25%), L4 fix relevance (25%) — normalize 1-5 → 0-1.
    def _avg(xs):
        return sum(xs) / len(xs) if xs else 0.0

    layer_b_raw = (
        _avg(diag_scores) * 0.50
        + _avg(feas_scores) * 0.25
        + _avg(rel_scores) * 0.25
    )
    layer_b_score = round(max(0.0, min(1.0, layer_b_raw / 5.0)), 4)

    return layer_b_score, reasons
