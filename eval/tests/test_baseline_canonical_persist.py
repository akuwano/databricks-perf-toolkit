"""Tests for V6.2 Tier 1: canonical Report sidecar persistence.

Codex 2026-04-26 design: per-case canonical Report is persisted in a
sidecar JSON (``<baseline>_canonical_reports.json``) keyed by case_id.
The main baseline JSON keeps only a lightweight ``canonical_report_ref``
+ ``has_canonical_report`` boolean so summary diffs stay readable.

Why sidecar and not inline:
- summary diff (metrics) and payload diff (Report content) are
  separable when reviewing PRs
- schema migrations can rewrite sidecars without touching baselines
- old baselines without sidecars remain loadable (Layer B will skip
  those cases with an explicit reason instead of fabricating empty
  canonicals)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from eval.goldens_runner import (
    _load_baseline_with_canonical,
    _sidecar_path_for_baseline,
    _write_baseline_json,
)


def _baseline_path(tmp: Path) -> Path:
    return tmp / "smoke__baseline.json"


def _sample_canonical(issue: str = "shuffle_overhead") -> dict:
    return {
        "schema_version": 1,
        "summary": {"headline": f"Heavy {issue}"},
        "findings": [
            {
                "issue_id": issue,
                "severity": "high",
                "actions": [{"what": "Run OPTIMIZE", "fix_sql": "OPTIMIZE t FULL;"}],
            }
        ],
    }


def _sample_record(case_id: str, *, canonical: dict | None = None) -> dict:
    """Per-case record as returned by ``_evaluate_one_case``."""
    rec = {
        "case_id": case_id,
        "schema_valid": True,
        "actionability_specific_ratio": 1.0,
    }
    if canonical is not None:
        rec["canonical_report"] = canonical
    return rec


# ---------------------------------------------------------------------------
# Sidecar path
# ---------------------------------------------------------------------------


class TestSidecarPath:
    def test_sidecar_path_appends_canonical_reports_suffix(self):
        baseline = Path("/tmp/eval/baselines/smoke__baseline.json")
        side = _sidecar_path_for_baseline(baseline)
        assert side.name == "smoke__baseline_canonical_reports.json"
        assert side.parent == baseline.parent

    def test_sidecar_path_handles_no_extension(self):
        baseline = Path("/tmp/eval/baselines/smoke")
        side = _sidecar_path_for_baseline(baseline)
        assert side.name == "smoke_canonical_reports.json"


# ---------------------------------------------------------------------------
# Write — split canonical out into sidecar, leave refs in baseline
# ---------------------------------------------------------------------------


class TestWriteBaselineSplitsCanonical:
    def test_writes_baseline_without_canonical_dict(self, tmp_path):
        rec = _sample_record("case_q23", canonical=_sample_canonical())
        _write_baseline_json([rec], _baseline_path(tmp_path))
        with open(_baseline_path(tmp_path)) as f:
            payload = json.load(f)
        case = payload["cases"][0]
        assert "canonical_report" not in case
        assert case["canonical_report_ref"] == "case_q23"
        assert case["has_canonical_report"] is True

    def test_writes_sidecar_keyed_by_case_id(self, tmp_path):
        rec = _sample_record("case_q23", canonical=_sample_canonical())
        baseline = _baseline_path(tmp_path)
        _write_baseline_json([rec], baseline)
        sidecar = _sidecar_path_for_baseline(baseline)
        assert sidecar.exists()
        with open(sidecar) as f:
            sidecar_payload = json.load(f)
        # Sidecar carries its own schema_version metadata so future
        # migrations can be detected.
        assert sidecar_payload.get("schema_version") == 1
        assert "cases" in sidecar_payload
        assert sidecar_payload["cases"]["case_q23"]["findings"][0]["issue_id"] == (
            "shuffle_overhead"
        )

    def test_record_without_canonical_marks_has_canonical_false(self, tmp_path):
        """``has_canonical_report=False`` lets the loader tell "no
        canonical" apart from "lookup miss"."""
        rec = _sample_record("case_no_canon", canonical=None)
        _write_baseline_json([rec], _baseline_path(tmp_path))
        with open(_baseline_path(tmp_path)) as f:
            payload = json.load(f)
        case = payload["cases"][0]
        assert case["has_canonical_report"] is False
        assert "canonical_report_ref" not in case
        # Sidecar still exists but the case_id is absent from it.
        sidecar = _sidecar_path_for_baseline(_baseline_path(tmp_path))
        with open(sidecar) as f:
            sidecar_payload = json.load(f)
        assert "case_no_canon" not in sidecar_payload["cases"]

    def test_skipped_record_has_no_canonical_ref(self, tmp_path):
        """Skipped cases (e.g. profile_not_found) don't have a
        canonical and must round-trip cleanly."""
        rec = {"case_id": "case_skipped", "skipped_reason": "profile_not_found"}
        _write_baseline_json([rec], _baseline_path(tmp_path))
        with open(_baseline_path(tmp_path)) as f:
            payload = json.load(f)
        case = payload["cases"][0]
        assert case["has_canonical_report"] is False
        assert "canonical_report_ref" not in case

    def test_duplicate_case_ids_logged_and_first_wins(self, tmp_path, caplog):
        """Codex 2026-04-26: duplicate case_id is silent corruption
        territory. The writer must warn and refuse to overwrite the
        first canonical so the bug surfaces in logs rather than
        flipping a sidecar entry under the same key."""
        import logging

        rec_a = _sample_record("dup", canonical=_sample_canonical("first"))
        rec_b = _sample_record("dup", canonical=_sample_canonical("second"))
        baseline = _baseline_path(tmp_path)
        with caplog.at_level(logging.WARNING):
            _write_baseline_json([rec_a, rec_b], baseline)
        assert any("duplicate case_id" in r.message.lower() for r in caplog.records)
        sidecar = _sidecar_path_for_baseline(baseline)
        with open(sidecar) as f:
            sidecar_payload = json.load(f)
        # First canonical wins; the second is dropped from the sidecar.
        assert sidecar_payload["cases"]["dup"]["findings"][0]["issue_id"] == "first"

    def test_sidecar_is_deterministic_when_inputs_unchanged(self, tmp_path):
        """Same input → same byte output (Codex: sort_keys=True for
        diff stability)."""
        rec = _sample_record("c1", canonical=_sample_canonical())
        baseline = _baseline_path(tmp_path)
        _write_baseline_json([rec], baseline)
        sidecar = _sidecar_path_for_baseline(baseline)
        first = sidecar.read_bytes()
        # Rewrite — sidecar must be byte-identical.
        _write_baseline_json([rec], baseline)
        second = sidecar.read_bytes()
        assert first == second


# ---------------------------------------------------------------------------
# Load — reattach canonical onto records
# ---------------------------------------------------------------------------


class TestLoadBaselineWithCanonical:
    def test_load_attaches_canonical_when_sidecar_present(self, tmp_path):
        rec = _sample_record("case_q23", canonical=_sample_canonical())
        baseline = _baseline_path(tmp_path)
        _write_baseline_json([rec], baseline)

        loaded = _load_baseline_with_canonical(baseline)
        cases = loaded["cases"]
        assert cases[0]["case_id"] == "case_q23"
        assert cases[0]["canonical_report"]["findings"][0]["issue_id"] == (
            "shuffle_overhead"
        )

    def test_load_handles_missing_sidecar(self, tmp_path):
        """Old baselines (pre-v6.6.4) have no sidecar. Loader must
        still succeed; Layer B skips those cases by checking
        ``has_canonical_report`` / ``canonical_report`` is None."""
        # Write a manual baseline without the sidecar.
        baseline = _baseline_path(tmp_path)
        baseline.parent.mkdir(parents=True, exist_ok=True)
        with open(baseline, "w") as f:
            json.dump(
                {
                    "cases": [
                        {"case_id": "old", "schema_valid": True}
                    ]
                },
                f,
            )
        loaded = _load_baseline_with_canonical(baseline)
        assert loaded["cases"][0].get("canonical_report") is None
        # The loader must signal that sidecar was absent so callers can
        # surface a stable skip reason rather than silently no-op.
        assert loaded.get("sidecar_present") is False

    def test_load_marks_case_canonical_none_when_lookup_misses(self, tmp_path):
        rec_with = _sample_record("c1", canonical=_sample_canonical())
        rec_without = _sample_record("c2", canonical=None)
        baseline = _baseline_path(tmp_path)
        _write_baseline_json([rec_with, rec_without], baseline)
        loaded = _load_baseline_with_canonical(baseline)
        cases = {c["case_id"]: c for c in loaded["cases"]}
        assert cases["c1"]["canonical_report"] is not None
        assert cases["c2"]["canonical_report"] is None


# ---------------------------------------------------------------------------
# Schema version awareness — surfaced for downstream Layer B gating
# ---------------------------------------------------------------------------


class TestSchemaVersion:
    def test_loader_surfaces_sidecar_schema_version(self, tmp_path):
        rec = _sample_record("c1", canonical=_sample_canonical())
        baseline = _baseline_path(tmp_path)
        _write_baseline_json([rec], baseline)
        loaded = _load_baseline_with_canonical(baseline)
        assert loaded.get("sidecar_schema_version") == 1
