"""Tests for the ``fix_sql_required_for_sql_actions`` structural invariant.

Codex 2026-04-26 review: when an Action's ``what`` describes a SQL
action (ALTER TABLE / CLUSTER BY / SET TBLPROPERTIES / OPTIMIZE /
ANALYZE / DML / SET), but ``fix_sql`` is empty, that's a regression
signal — the LLM dropped the customer-actionable SQL even when the
prompt now requires it (Iter 1 / Path A).

Unlike the profile-signature invariants in ``invariants.py``, this is
a *structural* invariant: it doesn't gate on profile evidence. It
fires whenever a canonical Report contains any actions, and is
satisfied when no SQL-action Action has empty fix_sql.
"""

from __future__ import annotations

from eval.profile_evidence import ProfileEvidence
from eval.scorers.invariants import score_invariants


def _empty_evidence() -> ProfileEvidence:
    return ProfileEvidence()


# ---------------------------------------------------------------------------
# Trigger conditions
# ---------------------------------------------------------------------------


def test_does_not_fire_when_no_actions_present():
    """No actions = nothing to validate. The invariant must remain
    no-op so it doesn't dilute the aggregate score."""
    canonical = {"findings": []}
    score = score_invariants(_empty_evidence(), canonical, llm_narrative="")
    # Either truly no-op, or the new invariant is not in the fired list.
    assert "fix_sql_required_for_sql_actions" not in score.fired_invariants


def test_fires_when_any_action_mentions_sql_keyword():
    """Even non-SQL recommendations exist alongside, the invariant
    fires because at least one action mentions a SQL keyword."""
    canonical = {
        "findings": [
            {
                "actions": [
                    {"what": "ALTER TABLE store_sales CLUSTER BY (...)", "fix_sql": "ALTER TABLE store_sales CLUSTER BY (ss_sold_date_sk);"},
                    {"what": "Resize the warehouse to Large", "fix_sql": ""},
                ]
            }
        ]
    }
    score = score_invariants(_empty_evidence(), canonical, llm_narrative="")
    assert "fix_sql_required_for_sql_actions" in score.fired_invariants


# ---------------------------------------------------------------------------
# Satisfied / violated outcomes
# ---------------------------------------------------------------------------


def test_satisfied_when_all_sql_actions_have_fix_sql():
    canonical = {
        "findings": [
            {
                "actions": [
                    {
                        "what": "Run ALTER TABLE ... CLUSTER BY",
                        "fix_sql": "ALTER TABLE foo CLUSTER BY (a);",
                    },
                    {
                        "what": "Then OPTIMIZE FULL",
                        "fix_sql": "OPTIMIZE foo FULL;",
                    },
                ]
            }
        ]
    }
    score = score_invariants(_empty_evidence(), canonical, llm_narrative="")
    assert "fix_sql_required_for_sql_actions" in score.satisfied_invariants
    assert all(
        v.invariant_id != "fix_sql_required_for_sql_actions" for v in score.violations
    )


def test_violated_when_sql_action_has_empty_fix_sql():
    """The exact failure mode the customer reported: prose-only
    advice without the actual SQL command."""
    canonical = {
        "findings": [
            {
                "actions": [
                    {
                        "what": "Use Liquid Clustering with CLUSTER BY syntax, putting ss_sold_date_sk first",
                        "fix_sql": "",
                    }
                ]
            }
        ]
    }
    score = score_invariants(_empty_evidence(), canonical, llm_narrative="")
    violations = [
        v
        for v in score.violations
        if v.invariant_id == "fix_sql_required_for_sql_actions"
    ]
    assert len(violations) == 1
    # The violation must surface the offending action text so the
    # eval reviewer can see exactly which card regressed.
    ev_text = " ".join(violations[0].evidence)
    assert "CLUSTER BY" in ev_text


def test_partial_violation_when_only_some_actions_omit_fix_sql():
    canonical = {
        "findings": [
            {
                "actions": [
                    {
                        "what": "ALTER TABLE foo CLUSTER BY (a);",
                        "fix_sql": "ALTER TABLE foo CLUSTER BY (a);",
                    },
                    {
                        "what": "Also run OPTIMIZE foo FULL",
                        "fix_sql": "",
                    },
                ]
            }
        ]
    }
    score = score_invariants(_empty_evidence(), canonical, llm_narrative="")
    violations = [
        v
        for v in score.violations
        if v.invariant_id == "fix_sql_required_for_sql_actions"
    ]
    assert len(violations) == 1


def test_non_sql_action_with_empty_fix_sql_is_not_a_violation():
    """Warehouse sizing / channel selection / human review actions
    legitimately have no SQL — they must not trigger the invariant."""
    canonical = {
        "findings": [
            {
                "actions": [
                    {
                        "what": "Increase the warehouse size to Large",
                        "fix_sql": "",
                    },
                    {
                        "what": "Have a DBA review the final plan",
                        "fix_sql": "",
                    },
                ]
            }
        ]
    }
    score = score_invariants(_empty_evidence(), canonical, llm_narrative="")
    violations = [
        v
        for v in score.violations
        if v.invariant_id == "fix_sql_required_for_sql_actions"
    ]
    assert violations == []


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------


def test_violation_subtracts_from_score():
    canonical = {
        "findings": [
            {
                "actions": [
                    {
                        "what": "ALTER TABLE foo CLUSTER BY (a);",
                        "fix_sql": "",
                    }
                ]
            }
        ]
    }
    score = score_invariants(_empty_evidence(), canonical, llm_narrative="")
    # Single violation → score < 1.0 (existing 0.25-per-violation model).
    assert score.score < 1.0
