"""End-to-end smoke for V6.2 Tier 1 canonical persistence.

This test exercises the full path that the four prior commits set up:

  1. ``_evaluate_one_case`` produced a record carrying ``canonical_report``.
  2. ``_write_baseline_json`` split it into baseline + sidecar.
  3. ``_load_baseline_with_canonical`` re-attached the canonical onto
     each rec.
  4. ``_run_layer_b_for_condition`` invoked the LLM judge per case
     with the real canonical (mocked here so no API call is made).

Without this end-to-end test the four units could individually be
green while the wiring between them was broken (different ``case_id``
keying, dict mutation across calls, etc.).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from eval.ab_runner import _run_layer_b_for_condition
from eval.goldens_runner import (
    _load_baseline_with_canonical,
    _sidecar_path_for_baseline,
    _write_baseline_json,
)


def _canon(issue: str, *, version: int = 1) -> dict:
    return {
        "schema_version": version,
        "summary": {"headline": f"Heavy {issue}"},
        "findings": [
            {
                "issue_id": issue,
                "severity": "high",
                "actions": [
                    {
                        "what": f"Apply remediation for {issue}",
                        "fix_sql": "OPTIMIZE t FULL;",
                    }
                ],
            }
        ],
    }


def _record(case_id: str, *, canonical: dict | None = None, **extra) -> dict:
    rec = {"case_id": case_id, "schema_valid": True, **extra}
    if canonical is not None:
        rec["canonical_report"] = canonical
    return rec


def test_full_roundtrip_baseline_to_layer_b(tmp_path: Path):
    """Goldens write → AB load → Layer B judge with the real canonical."""
    baseline = tmp_path / "smoke__baseline.json"
    records = [
        _record("c_evaluated_1", canonical=_canon("shuffle_overhead")),
        _record("c_evaluated_2", canonical=_canon("spill_dominant")),
        _record("c_skipped", skipped_reason="profile_not_found"),
    ]

    # 1. goldens_runner writes
    _write_baseline_json(records, baseline)
    sidecar = _sidecar_path_for_baseline(baseline)
    assert sidecar.exists(), "sidecar must be written alongside the baseline"

    # Sanity: main baseline carries refs only, sidecar carries dicts.
    with open(baseline) as f:
        main_payload = json.load(f)
    assert all(
        "canonical_report" not in c for c in main_payload["cases"]
    ), "canonical dict leaked into main baseline"

    # 2. ab_runner re-loads via the sidecar-aware helper
    loaded = _load_baseline_with_canonical(baseline)
    assert loaded["sidecar_present"] is True
    assert loaded["sidecar_schema_version"] == 1
    cases = loaded["cases"]
    by_id = {c["case_id"]: c for c in cases}
    assert by_id["c_evaluated_1"]["canonical_report"]["findings"][0]["issue_id"] == (
        "shuffle_overhead"
    )
    assert by_id["c_skipped"]["canonical_report"] is None

    # 3. Layer B dispatcher consumes the reattached canonical
    judge_calls: list[str] = []

    def fake_judge(*, canonical_report, **kwargs):
        # Record which issue_id we judged so we can assert wiring.
        iid = canonical_report["findings"][0]["issue_id"]
        judge_calls.append(iid)
        return 0.9, [f"judged {iid}"]

    with patch("eval.ab_runner.score_layer_b", side_effect=fake_judge):
        score, reasons, skips = _run_layer_b_for_condition(
            cases,
            host="https://example.cloud.databricks.com",
            token="dapi-fake",
            judge_model="databricks-claude-sonnet-4",
            top_n=5,
        )

    # Both evaluated cases were judged; the skipped case was filtered
    # out before reaching the judge. No missing-canonical / schema
    # mismatch on the happy path.
    assert sorted(judge_calls) == ["shuffle_overhead", "spill_dominant"]
    assert skips == {"missing_canonical": 0, "schema_mismatch": 0}
    assert score == 0.9
    assert any("judged shuffle_overhead" in r for r in reasons)
    assert any("judged spill_dominant" in r for r in reasons)


def test_old_baseline_without_sidecar_lands_in_missing_bucket(tmp_path: Path):
    """Pre-v6.6.4 baselines have no sidecar — Layer B must surface
    ``missing_canonical`` instead of pretending to judge."""
    baseline = tmp_path / "old__baseline.json"
    baseline.parent.mkdir(parents=True, exist_ok=True)
    # Hand-crafted legacy shape (no sidecar emitted).
    with open(baseline, "w") as f:
        json.dump(
            {"cases": [{"case_id": "legacy_case", "schema_valid": True}]}, f
        )

    loaded = _load_baseline_with_canonical(baseline)
    assert loaded["sidecar_present"] is False

    with patch("eval.ab_runner.score_layer_b") as mock_judge:
        score, _reasons, skips = _run_layer_b_for_condition(
            loaded["cases"],
            host="h",
            token="t",
            judge_model="m",
            top_n=5,
        )
    mock_judge.assert_not_called()
    assert skips["missing_canonical"] == 1
    assert score is None
