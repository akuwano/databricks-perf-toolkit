"""Tests for V6.2 Tier 1: Layer B per-case dispatch with deterministic
case ranking and missing-canonical handling.

Codex 2026-04-26 design:
- Per-case judge with ``top_n=5`` rather than a single representative
  case. Selection is deterministic (priority via missed coverage,
  schema-invalid signals; case_id ascending tiebreak).
- Cases without a canonical (old baselines, skipped, pipeline errors)
  do **not** get a placeholder — they're surfaced as
  ``layer_b_skipped_missing_canonical`` so R10 gating sees the
  difference between "judged empty" and "no data to judge".
"""

from __future__ import annotations

from unittest.mock import patch

from eval.ab_runner import (
    _rank_for_layer_b,
    _run_layer_b_for_condition,
    _select_layer_b_cases,
)


def _rec(
    case_id: str,
    *,
    canonical: dict | None = None,
    schema_valid: bool = True,
    recall_missed: list[str] | None = None,
    skipped: bool = False,
) -> dict:
    rec: dict = {
        "case_id": case_id,
        "schema_valid": schema_valid,
        "recall_missed": recall_missed or [],
    }
    if skipped:
        rec["skipped_reason"] = "profile_not_found"
    if canonical is not None:
        rec["canonical_report"] = canonical
    return rec


def _canon(issue: str = "shuffle_overhead") -> dict:
    return {
        "schema_version": 1,
        "findings": [{"issue_id": issue, "actions": [{"what": "Optimize"}]}],
    }


# ---------------------------------------------------------------------------
# Ranking — "high signal" cases come first, deterministic tiebreak
# ---------------------------------------------------------------------------


class TestRankForLayerB:
    def test_recall_misses_outrank_clean_cases(self):
        """A case with missed coverage carries more diagnostic value
        for the judge than an all-green case."""
        clean = _rec("a_clean", recall_missed=[])
        miss = _rec("z_miss", recall_missed=["spill_dominant"])
        # Lower key sorts first (higher rank).
        assert _rank_for_layer_b(miss) < _rank_for_layer_b(clean)

    def test_schema_invalid_outranks_schema_valid(self):
        bad = _rec("z_bad_schema", schema_valid=False)
        ok = _rec("a_ok", schema_valid=True)
        assert _rank_for_layer_b(bad) < _rank_for_layer_b(ok)

    def test_case_id_ascending_breaks_ties(self):
        """Same priority tier → case_id ascending. Determinism matters
        so judge cost stays predictable across reruns."""
        a = _rec("aaa")
        b = _rec("bbb")
        assert _rank_for_layer_b(a) < _rank_for_layer_b(b)


# ---------------------------------------------------------------------------
# Case selection — top_n + skip filter
# ---------------------------------------------------------------------------


class TestSelectLayerBCases:
    def test_skips_records_with_skipped_reason(self):
        recs = [
            _rec("ok1", canonical=_canon()),
            _rec("skipped", skipped=True),
            _rec("ok2", canonical=_canon()),
        ]
        out = _select_layer_b_cases(recs, top_n=10)
        assert [r["case_id"] for r in out] == ["ok1", "ok2"]

    def test_caps_at_top_n(self):
        recs = [_rec(f"c{i}", canonical=_canon()) for i in range(10)]
        out = _select_layer_b_cases(recs, top_n=3)
        assert len(out) == 3

    def test_top_n_zero_returns_empty(self):
        """``top_n=0`` means "judge nothing" — must return an empty list,
        not silently take 1 case (Codex 2026-04-26 review)."""
        recs = [_rec("c1", canonical=_canon())]
        out = _select_layer_b_cases(recs, top_n=0)
        assert out == []

    def test_top_n_negative_returns_empty(self):
        recs = [_rec("c1", canonical=_canon())]
        out = _select_layer_b_cases(recs, top_n=-3)
        assert out == []

    def test_high_signal_cases_picked_first(self):
        clean = _rec("a", canonical=_canon())
        miss = _rec("z", canonical=_canon(), recall_missed=["x"])
        bad = _rec("m", canonical=_canon(), schema_valid=False)
        out = _select_layer_b_cases([clean, miss, bad], top_n=2)
        # bad+miss share the top tier (one schema, one recall miss);
        # alphabetical tiebreak → m before z.
        assert [r["case_id"] for r in out] == ["m", "z"]


# ---------------------------------------------------------------------------
# Per-condition Layer B run — aggregate score + skip counters
# ---------------------------------------------------------------------------


class _FakeJudge:
    """Stand-in for ``score_layer_b`` so we can assert the dispatch
    logic without making real Databricks API calls."""

    def __init__(self, score: float | None = 0.8, reasons: list[str] | None = None):
        self.score = score
        self.reasons = reasons or ["judge happy"]
        self.calls: list[dict] = []

    def __call__(self, *, canonical_report, **kwargs):
        self.calls.append({"case_canonical_findings_n": len(canonical_report.get("findings") or [])})
        return self.score, list(self.reasons)


class TestRunLayerBForCondition:
    def test_calls_judge_once_per_top_case(self):
        recs = [
            _rec("c1", canonical=_canon()),
            _rec("c2", canonical=_canon()),
            _rec("c3", canonical=_canon()),
        ]
        judge = _FakeJudge(score=0.7)
        with patch("eval.ab_runner.score_layer_b", judge):
            score, reasons, skips = _run_layer_b_for_condition(
                recs,
                host="h",
                token="t",
                judge_model="m",
                top_n=2,
            )
        assert len(judge.calls) == 2
        assert score == 0.7  # mean of [0.7, 0.7]

    def test_aggregates_score_as_mean(self):
        recs = [
            _rec("c1", canonical=_canon()),
            _rec("c2", canonical=_canon()),
        ]
        scores = iter([0.6, 1.0])

        def fake(*, canonical_report, **kwargs):
            return next(scores), ["r"]

        with patch("eval.ab_runner.score_layer_b", side_effect=fake):
            score, _reasons, _skips = _run_layer_b_for_condition(
                recs, host="h", token="t", judge_model="m", top_n=2
            )
        assert score == 0.8

    def test_skip_missing_canonical(self):
        """Old baselines / pipeline errors → no canonical → judge is
        not called for that case + the skip counter increments."""
        recs = [
            _rec("c_with", canonical=_canon()),
            _rec("c_without", canonical=None),
        ]
        judge = _FakeJudge(score=0.9)
        with patch("eval.ab_runner.score_layer_b", judge):
            score, _reasons, skips = _run_layer_b_for_condition(
                recs, host="h", token="t", judge_model="m", top_n=5
            )
        assert len(judge.calls) == 1
        assert skips["missing_canonical"] == 1
        assert score == 0.9

    def test_returns_none_when_no_judgeable_cases(self):
        recs = [_rec("c1", canonical=None)]
        with patch("eval.ab_runner.score_layer_b", _FakeJudge(score=0.5)):
            score, reasons, skips = _run_layer_b_for_condition(
                recs, host="h", token="t", judge_model="m", top_n=5
            )
        assert score is None
        assert reasons == []
        assert skips["missing_canonical"] == 1

    def test_returns_none_when_host_or_token_missing(self):
        """Defensive: don't even try when API access is absent."""
        recs = [_rec("c1", canonical=_canon())]
        with patch("eval.ab_runner.score_layer_b", _FakeJudge()):
            score, reasons, _skips = _run_layer_b_for_condition(
                recs, host="", token="t", judge_model="m", top_n=5
            )
        assert score is None
        assert reasons == []


# ---------------------------------------------------------------------------
# V6.2-4: schema_version mismatch handling (Codex 2026-04-26)
# ---------------------------------------------------------------------------


def _canon_v(version: int, issue: str = "shuffle_overhead") -> dict:
    return {
        "schema_version": version,
        "findings": [{"issue_id": issue, "actions": [{"what": "Optimize"}]}],
    }


class TestLayerBSchemaMismatch:
    def test_skipped_canonical_with_mismatched_schema(self):
        """Old sidecars carrying schema_version != current → skip the
        case with a dedicated counter (NOT folded into
        ``missing_canonical``)."""
        future_version = 99
        recs = [
            _rec("c_now", canonical=_canon_v(1)),
            _rec("c_old", canonical=_canon_v(future_version)),
        ]
        judge = _FakeJudge(score=0.9)
        with patch("eval.ab_runner.score_layer_b", judge):
            score, _reasons, skips = _run_layer_b_for_condition(
                recs,
                host="h",
                token="t",
                judge_model="m",
                top_n=5,
            )
        assert len(judge.calls) == 1
        assert skips["schema_mismatch"] == 1
        assert skips["missing_canonical"] == 0
        assert score == 0.9

    def test_canonical_without_schema_version_treated_as_mismatch(self):
        """Defensive: a canonical with no ``schema_version`` field
        cannot be safely judged. Count it as schema_mismatch so the
        signal is the same as an explicitly-bad version."""
        recs = [_rec("c", canonical={"findings": [{"issue_id": "x", "actions": []}]})]
        judge = _FakeJudge(score=0.9)
        with patch("eval.ab_runner.score_layer_b", judge):
            score, _reasons, skips = _run_layer_b_for_condition(
                recs, host="h", token="t", judge_model="m", top_n=5
            )
        assert len(judge.calls) == 0
        assert skips["schema_mismatch"] == 1
        assert score is None

    def test_canonical_v1_passes(self):
        """Sanity: schema_version=1 (current) is judged normally."""
        recs = [_rec("c", canonical=_canon_v(1))]
        judge = _FakeJudge(score=0.5)
        with patch("eval.ab_runner.score_layer_b", judge):
            _score, _reasons, skips = _run_layer_b_for_condition(
                recs, host="h", token="t", judge_model="m", top_n=5
            )
        assert len(judge.calls) == 1
        assert skips["schema_mismatch"] == 0
