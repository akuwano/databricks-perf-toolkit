"""Tests for V6.1 Layer B LLM judge wrapper (Day 4-5)."""

from __future__ import annotations

from eval.scorers.r10_quality_judge import score_layer_b


def test_no_credentials_returns_none():
    score, reasons = score_layer_b(
        canonical_report={"findings": []},
        databricks_host="",
        databricks_token="",
    )
    assert score is None
    assert reasons == []


def test_empty_findings_returns_none():
    score, reasons = score_layer_b(
        canonical_report={"findings": []},
        databricks_host="x",
        databricks_token="x",
    )
    assert score is None


def test_no_actions_returns_none():
    """Findings exist but every finding has no actions."""
    score, reasons = score_layer_b(
        canonical_report={"findings": [{"issue_id": "x", "actions": []}]},
        databricks_host="x",
        databricks_token="x",
    )
    assert score is None


def test_top_n_default_5_capped(monkeypatch):
    """When more than 5 actions exist, only top-5 by priority_rank are
    sent to the judge. Stub the judge to count calls."""
    calls = {"n": 0}

    class _StubL3:
        diagnosis_score = 4
        evidence_quality = 4
        reasoning = "test"

    class _StubL4:
        fix_relevance = 4
        fix_feasibility = 4
        expected_improvement = 4
        reasoning = "test"

    def fake_score_l3l4(card, *args, **kwargs):
        calls["n"] += 1
        return _StubL3(), _StubL4()

    # Build report with 8 actions, mixed priority_rank
    report = {
        "findings": [
            {
                "issue_id": "x",
                "evidence": [],
                "actions": [
                    {
                        "action_id": f"a{i}",
                        "priority_rank": 90 - i * 10,
                        "what": f"do {i}",
                    }
                    for i in range(8)
                ],
            }
        ]
    }

    monkeypatch.setattr("eval.scorers.l3l4_judge.score_l3l4", fake_score_l3l4)
    score, reasons = score_layer_b(
        canonical_report=report,
        databricks_host="x",
        databricks_token="x",
        top_n=5,
    )
    assert score is not None
    assert calls["n"] == 5  # capped at top_n
    # 4*0.5 + 4*0.25 + 4*0.25 = 4.0 → 4/5 = 0.8
    assert abs(score - 0.8) < 1e-3


def test_aggregation_weighted_average(monkeypatch):
    """L3 diagnosis weight = 0.5, L4 fix_feasibility = 0.25,
    L4 fix_relevance = 0.25."""
    class _L3:
        def __init__(self, d): self.diagnosis_score = d; self.evidence_quality = 0; self.reasoning = ""

    class _L4:
        def __init__(self, fr, ff): self.fix_relevance = fr; self.fix_feasibility = ff; self.expected_improvement = 0; self.reasoning = ""

    def fake(card, *a, **kw):
        return _L3(5), _L4(3, 1)  # diag=5, rel=3, feas=1

    report = {
        "findings": [
            {"issue_id": "x", "evidence": [], "actions": [{"action_id": "a1"}]}
        ]
    }
    monkeypatch.setattr("eval.scorers.l3l4_judge.score_l3l4", fake)
    score, _ = score_layer_b(
        canonical_report=report,
        databricks_host="x",
        databricks_token="x",
    )
    # 5*0.5 + 1*0.25 + 3*0.25 = 2.5 + 0.25 + 0.75 = 3.5 → 3.5/5 = 0.70
    assert abs(score - 0.70) < 1e-3
