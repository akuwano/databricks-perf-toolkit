"""LLM output parsing and formatting."""

import json
import logging
import re

logger = logging.getLogger(__name__)


# W3.5 #1: V6 canonical Report JSON extractor.
# The LLM, when V6_CANONICAL_SCHEMA=on, is instructed to emit a fenced
# block with language tag `json:canonical_v6`. This regex extracts the
# JSON content non-greedily so multiple blocks (rare) work.
_V6_CANONICAL_BLOCK_RE = re.compile(
    r"```json:canonical_v6\s*\n(.*?)\n```",
    re.DOTALL,
)


def extract_v6_canonical_block(llm_output: str) -> dict | None:
    """Extract the V6 canonical Report JSON block from LLM output.

    Returns:
        Parsed dict matching schemas/report_v6.schema.json (best-effort —
        caller should still validate with eval/scorers/r4_schema), or None
        when no block is present / parse fails.
    """
    if not llm_output:
        return None
    match = _V6_CANONICAL_BLOCK_RE.search(llm_output)
    if not match:
        return None
    payload = match.group(1).strip()
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as e:
        logger.warning("V6 canonical block JSON parse failed: %s", e)
        return None
    if not isinstance(parsed, dict):
        logger.warning("V6 canonical block must be an object, got %s", type(parsed).__name__)
        return None
    return parsed


def parse_review_json(review_output: str) -> dict | None:
    """Parse JSON review output from LLM."""
    if not review_output:
        return None

    text = review_output.strip()

    if text.startswith("```"):
        first_newline = text.index("\n") if "\n" in text else len(text)
        text = text[first_newline + 1 :]
        if text.rstrip().endswith("```"):
            text = text.rstrip()[:-3].rstrip()

    try:
        result = json.loads(text)
        if isinstance(result, dict) and "issues" in result:
            return result
        logger.warning("Review JSON missing 'issues' key, falling back to text")
        return None
    except json.JSONDecodeError:
        logger.warning("Failed to parse review as JSON, falling back to text")
        return None


def format_review_for_refine(review_output: str) -> str:
    """Format review output for the refine stage."""
    parsed = parse_review_json(review_output)
    if parsed is None:
        return review_output

    parts = [f"Review Overall: {parsed.get('overall', 'N/A')}"]
    parts.append(f"Pass: {parsed.get('pass', 'N/A')}")

    issues = parsed.get("issues", [])
    if issues:
        parts.append(f"\nIssues ({len(issues)}):")
        for i, issue in enumerate(issues, 1):
            parts.append(f"\n  Issue {i}:")
            parts.append(f"    Type: {issue.get('type', 'unknown')}")
            parts.append(f"    Location: {issue.get('location', 'N/A')}")
            parts.append(f'    Claim: "{issue.get("claim", "N/A")}"')
            parts.append(f"    Problem: {issue.get('problem', 'N/A')}")
            parts.append(f"    Fix: {issue.get('fix', 'N/A')}")

    additions = parsed.get("additions", [])
    if additions:
        parts.append(f"\nAdditions ({len(additions)}):")
        for addition in additions:
            parts.append(f"  - {addition}")

    return "\n".join(parts)


def parse_llm_sections(llm_output: str) -> dict[str, str]:
    """Parse LLM output into named sections by ## headers."""
    if not llm_output or not llm_output.strip():
        return {}

    section_patterns: list[tuple[str, str]] = [
        (r"executive\s*summary|エグゼクティブサマリー", "executive_summary"),
        (r"root\s*cause\s*analysis|根本原因分析", "root_cause_analysis"),
        (r"recommended?\s*actions?|recommendations?|推奨アクション|推奨事項", "recommendations"),
        (r"optimized\s*sql|最適化済み\s*SQL", "optimized_sql"),
        (r"conclusion|結論", "conclusion"),
    ]

    header_re = re.compile(r"^##\s+(?:\d+\.\s*)?(.+)$", re.MULTILINE)
    matches = list(header_re.finditer(llm_output))

    if not matches:
        return {}

    sections: dict[str, str] = {}
    unmatched_parts: list[str] = []

    preamble = llm_output[: matches[0].start()].strip()
    if preamble:
        unmatched_parts.append(preamble)

    for i, match in enumerate(matches):
        header_text = match.group(1).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(llm_output)
        content = llm_output[start:end].strip()

        matched = False
        for pattern, key in section_patterns:
            if re.search(pattern, header_text, re.IGNORECASE):
                sections[key] = content
                matched = True
                break

        if not matched and content:
            unmatched_parts.append(f"## {header_text}\n\n{content}")

    if unmatched_parts:
        sections["_unmatched"] = "\n\n".join(unmatched_parts)

    return sections


def parse_action_plan_from_llm(text: str | None) -> list[dict]:
    """Extract structured Action Plan JSON from LLM output.

    Looks for a ```json``` code block (optionally preceded by
    <!-- ACTION_PLAN_JSON --> marker) and parses it as a list of dicts.

    Returns empty list on missing/invalid JSON.
    """
    if not text:
        return []

    # Try to find JSON block after ACTION_PLAN_JSON marker first
    marker_pattern = re.compile(
        r"<!--\s*ACTION_PLAN_JSON\s*-->\s*```json\s*\n(.*?)```",
        re.DOTALL,
    )
    match = marker_pattern.search(text)

    # Fallback: find any ```json block containing a JSON array
    if not match:
        fallback_pattern = re.compile(r"```json\s*\n(\[.*?\])\s*```", re.DOTALL)
        match = fallback_pattern.search(text)

    if not match:
        return []

    try:
        data = json.loads(match.group(1))
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    item.setdefault("confidence", "")
                    item.setdefault("confidence_reason", "")
            return data
    except (json.JSONDecodeError, ValueError):
        logger.warning("Failed to parse Action Plan JSON from LLM output")

    return []


def parse_rerank_output(text: str | None) -> dict | None:
    """Parse Top-5 rerank JSON from LLM output."""
    if not text:
        return None
    match = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text)
    payload = match.group(1) if match else text.strip()
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, ValueError):
        logger.warning("Failed to parse rerank JSON from LLM output")
        return None
    if not isinstance(data, dict):
        return None
    if not isinstance(data.get("selected_ids"), list):
        return None
    if not isinstance(data.get("selection_rationale"), dict):
        return None
    return data
