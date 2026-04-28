"""Q4: Action specificity scorer.

Scores each ActionCard on 6 dimensions:
- target: target object/column/table identified
- what: action verb / change type clear
- why: reason / root cause explained
- how: concrete SQL / config / DDL provided
- expected_effect: quantitative or directional impact
- verification: how to verify the fix worked

A card is "specific" if >=5 of 6 dimensions are present.
The query-level score = ratio of specific cards / total cards.

See: docs/eval/report_quality_rubric.md section 4
"""

from __future__ import annotations

import re

from ..models import ActionabilityScore

# Heuristic keyword sets for each dimension. Mechanical first pass; Week 3+ may
# add LLM judge for ambiguous cases.

_TARGET_PATTERNS = [
    r"\b[A-Za-z_][\w]*\.[A-Za-z_][\w]*",  # table.column
    r"\bcolumn\b",
    r"\btable\b",
    r"\b列\b",
    r"\bテーブル\b",
]

_HOW_PATTERNS = [
    r"\bALTER\s+TABLE\b",
    r"\bCREATE\s+(OR\s+REPLACE\s+)?(TABLE|MATERIALIZED VIEW)\b",
    r"\bCLUSTER\s+BY\b",
    r"\bOPTIMIZE\b",
    r"\bSET\s+spark\.",
    r"\bANALYZE\s+TABLE\b",
    r"```sql",
    r"\bspark\.sql\.",
    r"\bspark\.databricks\.",
]

_EFFECT_PATTERNS = [
    r"\d+\s*[%％]",  # "30%"
    r"\d+x\b",  # "2x"
    r"\b短縮\b",
    r"\b削減\b",
    r"\b改善\b",
    r"\bimprove",
    r"\breduce",
    r"\bspeedup\b",
    r"\bfaster\b",
]

_VERIFY_PATTERNS = [
    r"\bEXPLAIN\b",
    r"\bDESCRIBE\b",
    r"\b検証\b",
    r"\b確認\b",
    r"\bverify\b",
    r"\bcheck\b",
    r"\bcompare\b",
    r"\b比較\b",
]

_WHY_PATTERNS = [
    r"\bため\b",
    r"\bので\b",
    r"\bbecause\b",
    r"\bdue to\b",
    r"\b原因\b",
    r"\bcause\b",
    r"\b起因\b",
]


def _has_pattern(text: str, patterns: list[str]) -> bool:
    """True if any pattern matches case-insensitively."""
    if not text:
        return False
    for pat in patterns:
        if re.search(pat, text, re.IGNORECASE):
            return True
    return False


def score_actionability(card) -> ActionabilityScore:
    """Score one ActionCard for action specificity.

    Reads card.problem, card.fix_sql, card.expected_impact, card.evidence
    (whichever exist) and checks each of 6 dimensions.
    """
    problem = getattr(card, "problem", "") or ""
    fix_sql = getattr(card, "fix_sql", "") or ""
    expected_impact = getattr(card, "expected_impact", "") or ""
    rationale = getattr(card, "rationale", "") or ""
    verification = getattr(card, "verification", "") or ""
    evidence = getattr(card, "evidence", "") or ""

    combined = " ".join([problem, fix_sql, expected_impact, rationale, verification, evidence])

    has_target = _has_pattern(combined, _TARGET_PATTERNS)
    # `what` = action verb in fix_sql or problem (use HOW patterns as proxy
    # since most actionable cards include a SQL-shape action)
    has_what = bool(fix_sql.strip()) or _has_pattern(problem, [r"\b変更\b", r"\b追加\b", r"\b削除\b"])
    has_why = _has_pattern(combined, _WHY_PATTERNS) or bool(rationale.strip())
    has_how = bool(fix_sql.strip()) or _has_pattern(combined, _HOW_PATTERNS)
    has_expected_effect = bool(expected_impact.strip()) or _has_pattern(
        combined, _EFFECT_PATTERNS
    )
    has_verification = bool(verification.strip()) or _has_pattern(combined, _VERIFY_PATTERNS)

    present_count = sum(
        [has_target, has_what, has_why, has_how, has_expected_effect, has_verification]
    )

    return ActionabilityScore(
        card_index=getattr(card, "card_index", 0) or 0,
        has_target=has_target,
        has_what=has_what,
        has_why=has_why,
        has_how=has_how,
        has_expected_effect=has_expected_effect,
        has_verification=has_verification,
        is_specific=present_count >= 5,
    )


def aggregate_actionability(scores: list[ActionabilityScore]) -> float:
    """Query-level actionability ratio = specific_cards / total_cards."""
    if not scores:
        return 0.0
    specific = sum(1 for s in scores if s.is_specific)
    return specific / len(scores)


# ---------------------------------------------------------------------------
# Canonical Action direct evaluation (W2.5 #2)
#
# CardEvalResult adapter 経由だと fix_sql が空のままになり 0% 固定になる。
# canonical Report の Action dict を直接受け、schema レベルで分離されている
# 6 dimension をそのまま採点する。
# ---------------------------------------------------------------------------


def score_canonical_action(
    action: dict,
    card_index: int = 0,
    *,
    profile_known_identifiers: set[str] | None = None,
) -> ActionabilityScore:
    """Score one canonical Action dict (matches schemas/report_v6.schema.json).

    W5 Day 4: 7 dimensions. The new ``has_citation`` dim is True when
    ``fix_sql_skeleton`` references at least one identifier (table or
    column) that exists in the profile.

    Args:
        action: canonical Action dict.
        card_index: position in the report.
        profile_known_identifiers: set of lowercased table/column names
            present in the profile. When None or empty, the citation dim
            is awarded if a non-trivial skeleton exists at all (lenient
            mode for back-compat with rule-based runs).
    """
    target = (action.get("target") or "").strip()
    what_field = (action.get("what") or "").strip()
    why = (action.get("why") or "").strip()
    fix_sql = (action.get("fix_sql") or "").strip()
    expected_effect = (action.get("expected_effect") or "").strip()
    expected_quant = action.get("expected_effect_quantitative") or None
    verification = action.get("verification") or []
    fix_type = (action.get("fix_type") or "").strip()
    skeleton = (action.get("fix_sql_skeleton") or "").strip()
    skeleton_method = (action.get("fix_sql_skeleton_method") or "").strip()

    # Original 6 dimensions
    has_target = bool(target)
    has_what = bool(what_field) or bool(fix_type)
    has_why = bool(why)
    has_how = bool(fix_sql) or (
        fix_type in {"investigation", "operational", "pattern"} and bool(what_field)
    )
    has_expected_effect = bool(expected_effect) or bool(expected_quant)
    has_verification = bool(verification) and isinstance(verification, list)

    # W5 Day 4: citation
    has_citation = _check_citation(skeleton, skeleton_method, profile_known_identifiers)

    present_count = sum(
        [
            has_target, has_what, has_why, has_how,
            has_expected_effect, has_verification, has_citation,
        ]
    )

    return ActionabilityScore(
        card_index=card_index,
        has_target=has_target,
        has_what=has_what,
        has_why=has_why,
        has_how=has_how,
        has_expected_effect=has_expected_effect,
        has_verification=has_verification,
        has_citation=has_citation,
        is_specific=present_count >= 6,  # was >= 5 in W1
    )


def _check_citation(
    skeleton: str,
    skeleton_method: str,
    profile_known_identifiers: set[str] | None,
) -> bool:
    """W5 Day 4: citation dimension.

    Strict mode (profile_known_identifiers provided): skeleton must
    contain at least one of those identifiers (case-insensitive).

    Lenient mode (no profile identifiers): a non-trivial skeleton is
    enough — i.e. ``fullsql``/``sqlglot``/``head_tail`` with non-empty
    text. This keeps backward-compatibility while still penalizing
    Actions that have *no* SQL (synthetic/raw text only).
    """
    if not skeleton:
        return False
    if skeleton_method in {"fullsql", "sqlglot", "head_tail", "bypass"}:
        if not profile_known_identifiers:
            return True
        s = skeleton.lower()
        return any(ident.lower() in s for ident in profile_known_identifiers)
    return False


def score_canonical_report_actions(
    report: dict,
    *,
    profile_known_identifiers: set[str] | None = None,
) -> list[ActionabilityScore]:
    """Score every Action across every Finding in the canonical Report."""
    out: list[ActionabilityScore] = []
    findings = report.get("findings") or []
    idx = 0
    for f in findings:
        for a in f.get("actions") or []:
            out.append(
                score_canonical_action(
                    a,
                    card_index=idx,
                    profile_known_identifiers=profile_known_identifiers,
                )
            )
            idx += 1
    return out


# ---------------------------------------------------------------------------
# W6 Day 3 (Codex W5 #3): split lenient vs strict reporting.
#
# Lenient = no profile_known_identifiers — citation passes when any non-empty
# skeleton exists.
# Strict  = profile_known_identifiers populated — citation passes only when
# the skeleton actually references a known table/column.
#
# Both paths are run side-by-side in the goldens_runner so the W6 markdown
# can show "94.83% lenient / X% strict" — Codex W5 review pointed out that
# rule-based 94.83% is partly a measurement artifact, not a quality gain.
# ---------------------------------------------------------------------------


def score_canonical_report_actions_dual(
    report: dict,
    *,
    profile_known_identifiers: set[str] | None = None,
) -> dict[str, list[ActionabilityScore]]:
    """Return {'lenient': [...], 'strict': [...]} for the same report."""
    lenient = score_canonical_report_actions(report, profile_known_identifiers=None)
    strict = score_canonical_report_actions(
        report,
        profile_known_identifiers=profile_known_identifiers or set(),
    )
    return {"lenient": lenient, "strict": strict}
