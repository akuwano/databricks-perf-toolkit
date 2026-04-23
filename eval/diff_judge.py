"""LLM-as-judge for comparing baseline vs current ActionCard sets."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from core.llm_client import create_openai_client

from .models import CardEvalResult, QueryEvalResult

logger = logging.getLogger(__name__)

_DIFF_SYSTEM_PROMPT = """\
You are an expert Databricks SQL performance evaluator.
Your job is to compare two sets of ActionCard recommendations generated for the same
query profile — a BASELINE version and a CURRENT version — and judge whether the
CURRENT version is better, worse, or equivalent.

Consider:
1. Diagnosis quality: Are bottlenecks identified more accurately?
2. Evidence quality: Are evidence claims more specific and grounded in data?
3. Fix quality: Are fixes more actionable, feasible, and likely to improve performance?
4. Coverage: Were important recommendations lost or new useful ones added?
5. SQL quality: Is fix_sql more correct and executable?

Output ONLY valid JSON with no additional text."""

_DIFF_USER_PROMPT = """\
## Profile
Path: {profile_path}

## BASELINE ActionCards ({baseline_count} cards)
{baseline_cards}

## CURRENT ActionCards ({current_count} cards)
{current_cards}

## Scoring
Compare CURRENT against BASELINE on these dimensions (1-5 each):
- **diagnosis_delta**: Is bottleneck identification better (5), same (3), or worse (1)?
- **evidence_delta**: Is evidence more grounded (5), same (3), or less (1)?
- **fix_delta**: Are fixes more actionable/feasible (5), same (3), or worse (1)?
- **coverage_delta**: Are more important issues covered (5), same (3), or issues lost (1)?
- **overall_verdict**: "improved", "regressed", or "unchanged"

Output JSON:
{{"diagnosis_delta": <1-5>, "evidence_delta": <1-5>, "fix_delta": <1-5>, \
"coverage_delta": <1-5>, "overall_verdict": "<improved|regressed|unchanged>", \
"reasoning": "<concise explanation of key differences>"}}"""


@dataclass
class DiffVerdict:
    """LLM judge comparison of baseline vs current for one profile."""

    profile_path: str = ""
    verdict: str = ""  # improved | regressed | unchanged
    diagnosis_delta: int = 3
    evidence_delta: int = 3
    fix_delta: int = 3
    coverage_delta: int = 3
    reasoning: str = ""
    baseline_card_count: int = 0
    current_card_count: int = 0


@dataclass
class DiffReport:
    """Aggregate diff report across all profiles."""

    timestamp: str = ""
    git_ref: str = ""
    num_profiles: int = 0
    verdicts: list[DiffVerdict] = field(default_factory=list)
    summary: str = ""
    config: dict = field(default_factory=dict)


def _format_cards(cards: list[CardEvalResult]) -> str:
    """Format ActionCard eval results for the judge prompt."""
    if not cards:
        return "(no cards)"
    parts = []
    for c in cards:
        lines = [f"### Card {c.card_index + 1}: {c.problem}"]
        lines.append(f"Impact: {c.expected_impact} | Effort: {c.effort}")
        if c.l1.has_fix_sql:
            status = "valid" if c.l1.parses_ok else f"INVALID ({c.l1.parse_error[:60]})"
            lines.append(f"Fix SQL: {status}")
        if c.l2.evidence_count > 0:
            lines.append(f"Evidence grounding: {c.l2.grounded_count}/{c.l2.evidence_count}")
        if c.l3:
            lines.append(f"L3 diagnosis={c.l3.diagnosis_score}/5, evidence={c.l3.evidence_quality}/5")
        if c.l4:
            lines.append(f"L4 relevance={c.l4.fix_relevance}/5, feasibility={c.l4.fix_feasibility}/5, improvement={c.l4.expected_improvement}/5")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def judge_diff(
    baseline: QueryEvalResult,
    current: QueryEvalResult,
    databricks_host: str,
    databricks_token: str,
    judge_model: str = "databricks-claude-sonnet-4-6",
) -> DiffVerdict:
    """Compare baseline vs current eval results for one profile using LLM judge."""
    profile_path = current.profile_path or baseline.profile_path

    try:
        client = create_openai_client(databricks_host, databricks_token)
        user_msg = _DIFF_USER_PROMPT.format(
            profile_path=Path(profile_path).name,
            baseline_count=len(baseline.card_results),
            baseline_cards=_format_cards(baseline.card_results),
            current_count=len(current.card_results),
            current_cards=_format_cards(current.card_results),
        )

        response = client.chat.completions.create(
            model=judge_model,
            messages=[
                {"role": "system", "content": _DIFF_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
            max_tokens=500,
        )

        content = response.choices[0].message.content or ""
        data = _parse_response(content)

        return DiffVerdict(
            profile_path=profile_path,
            verdict=data.get("overall_verdict", "unchanged"),
            diagnosis_delta=data.get("diagnosis_delta", 3),
            evidence_delta=data.get("evidence_delta", 3),
            fix_delta=data.get("fix_delta", 3),
            coverage_delta=data.get("coverage_delta", 3),
            reasoning=data.get("reasoning", ""),
            baseline_card_count=len(baseline.card_results),
            current_card_count=len(current.card_results),
        )
    except Exception as e:
        logger.warning("Diff judge failed for %s: %s", profile_path, e)
        return DiffVerdict(
            profile_path=profile_path,
            verdict="error",
            reasoning=f"Judge error: {e}",
            baseline_card_count=len(baseline.card_results),
            current_card_count=len(current.card_results),
        )


def _parse_response(content: str) -> dict[str, Any]:
    """Parse JSON from judge response."""
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        data = json.loads(text)
        for key in ("diagnosis_delta", "evidence_delta", "fix_delta", "coverage_delta"):
            if key in data:
                data[key] = max(1, min(5, int(data[key])))
        valid_verdicts = ("improved", "regressed", "unchanged")
        if data.get("overall_verdict") not in valid_verdicts:
            data["overall_verdict"] = "unchanged"
        return data
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("Failed to parse diff judge response: %s", e)
        return {"reasoning": f"Parse error: {text[:200]}"}
