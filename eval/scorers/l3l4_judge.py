"""L3/L4: LLM-as-judge scorer for diagnosis accuracy and fix effectiveness."""

from __future__ import annotations

import json
import logging
from typing import Any

from core.llm_client import create_openai_client
from core.models import ActionCard, BottleneckIndicators, QueryMetrics

from ..models import L3Score, L4Score

logger = logging.getLogger(__name__)

_JUDGE_SYSTEM_PROMPT = """\
You are an expert Databricks SQL performance evaluator.
Your job is to evaluate the quality of a performance optimization recommendation
given the query profile metrics and original SQL.

Rate STRICTLY on a 1-5 scale. Be critical — most recommendations should score 3.
Only give 5 for exceptionally accurate and impactful recommendations.

Output ONLY valid JSON with no additional text."""

_JUDGE_USER_PROMPT = """\
## Query Profile Summary
{profile_summary}

## Original SQL
```sql
{query_sql}
```

## ActionCard to Evaluate
- **Problem:** {problem}
- **Evidence:** {evidence}
- **Likely Cause:** {likely_cause}
- **Fix:** {fix}
- **Fix SQL:**
```sql
{fix_sql}
```
- **Expected Impact:** {expected_impact}
- **Effort:** {effort}

## Rating Criteria
Rate each dimension 1-5:
- **diagnosis_score**: Is the bottleneck correctly identified based on the profile data?
- **evidence_quality**: Is the evidence specific, verifiable, and derived from actual metrics?
- **fix_relevance**: Does the fix directly address the diagnosed bottleneck?
- **fix_feasibility**: Is the fix SQL syntactically valid and executable without side effects?
- **expected_improvement**: How likely is meaningful performance improvement?

Output JSON:
{{"diagnosis_score": <1-5>, "evidence_quality": <1-5>, "fix_relevance": <1-5>, \
"fix_feasibility": <1-5>, "expected_improvement": <1-5>, "reasoning": "<brief explanation>"}}"""


def score_l3l4(
    card: ActionCard,
    profile_summary: str,
    query_sql: str,
    databricks_host: str,
    databricks_token: str,
    judge_model: str = "databricks-claude-sonnet-4",
) -> tuple[L3Score, L4Score]:
    """Evaluate an ActionCard using LLM-as-judge.

    Returns (L3Score, L4Score) tuple. On failure, returns scores with 0 values.
    """
    try:
        client = create_openai_client(databricks_host, databricks_token)
        user_msg = _JUDGE_USER_PROMPT.format(
            profile_summary=profile_summary,
            query_sql=query_sql[:2000],  # Truncate long SQL
            problem=card.problem,
            evidence="\n".join(f"- {e}" for e in card.evidence) if card.evidence else "(none)",
            likely_cause=card.likely_cause or "(none)",
            fix=card.fix or "(none)",
            fix_sql=card.fix_sql or "(none)",
            expected_impact=card.expected_impact,
            effort=card.effort,
        )

        response = client.chat.completions.create(
            model=judge_model,
            messages=[
                {"role": "system", "content": _JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            temperature=0.0,
            max_tokens=500,
        )

        content = response.choices[0].message.content or ""
        scores = _parse_judge_response(content)

        l3 = L3Score(
            card_index=0,
            diagnosis_score=scores.get("diagnosis_score", 0),
            evidence_quality=scores.get("evidence_quality", 0),
            reasoning=scores.get("reasoning", ""),
        )
        l4 = L4Score(
            card_index=0,
            fix_relevance=scores.get("fix_relevance", 0),
            fix_feasibility=scores.get("fix_feasibility", 0),
            expected_improvement=scores.get("expected_improvement", 0),
            reasoning=scores.get("reasoning", ""),
        )
        return l3, l4

    except Exception as e:
        logger.warning("LLM judge failed for card '%s': %s", card.problem[:50], e)
        return (
            L3Score(card_index=0, reasoning=f"Judge error: {e}"),
            L4Score(card_index=0, reasoning=f"Judge error: {e}"),
        )


def _parse_judge_response(content: str) -> dict[str, Any]:
    """Parse JSON response from judge LLM, handling markdown code blocks."""
    # Strip markdown code blocks if present
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # Remove first and last ``` lines
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        data = json.loads(text)
        # Clamp scores to 1-5
        for key in ("diagnosis_score", "evidence_quality", "fix_relevance",
                     "fix_feasibility", "expected_improvement"):
            if key in data:
                data[key] = max(1, min(5, int(data[key])))
        return data
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("Failed to parse judge response: %s\nContent: %s", e, text[:200])
        return {"reasoning": f"Parse error: {text[:200]}"}


def build_profile_summary(
    qm: QueryMetrics,
    bi: BottleneckIndicators,
) -> str:
    """Build a concise profile summary string for the judge prompt."""
    lines = []
    if qm.total_time_ms:
        lines.append(f"Total time: {qm.total_time_ms}ms")
    if qm.execution_time_ms:
        lines.append(f"Execution time: {qm.execution_time_ms}ms")
    if qm.spill_to_disk_bytes:
        gb = qm.spill_to_disk_bytes / (1024**3)
        lines.append(f"Spill to disk: {gb:.2f} GB")
    if qm.read_bytes:
        gb = qm.read_bytes / (1024**3)
        lines.append(f"Data read: {gb:.2f} GB")
    if bi.cache_hit_ratio:
        lines.append(f"Cache hit ratio: {bi.cache_hit_ratio:.1%}")
    if bi.photon_ratio:
        lines.append(f"Photon utilization: {bi.photon_ratio:.1%}")
    if bi.shuffle_impact_ratio:
        lines.append(f"Shuffle impact: {bi.shuffle_impact_ratio:.1%}")
    if bi.filter_rate:
        lines.append(f"Filter efficiency: {bi.filter_rate:.1%}")
    if bi.spill_bytes:
        gb = bi.spill_bytes / (1024**3)
        lines.append(f"Spill bytes: {gb:.2f} GB")
    if qm.read_files_count:
        lines.append(f"Files read: {qm.read_files_count}")
    if qm.pruned_files_count:
        lines.append(f"Files pruned: {qm.pruned_files_count}")
    # Shuffle metrics
    if bi.shuffle_bytes_written_total:
        gb = bi.shuffle_bytes_written_total / (1024**3)
        lines.append(f"Shuffle bytes written: {gb:.2f} GB")
    if bi.shuffle_remote_bytes_read_total:
        gb = bi.shuffle_remote_bytes_read_total / (1024**3)
        lines.append(f"Shuffle remote bytes read: {gb:.2f} GB")
    # Write metrics
    if qm.write_remote_bytes:
        gb = qm.write_remote_bytes / (1024**3)
        lines.append(f"Write bytes: {gb:.2f} GB")
    if qm.write_remote_files:
        lines.append(f"Write files: {qm.write_remote_files}")
    if qm.write_remote_rows:
        lines.append(f"Write rows: {qm.write_remote_rows}")
    return "\n".join(lines) if lines else "No metrics available"
