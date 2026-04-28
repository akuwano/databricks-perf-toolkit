"""V6 R5 two-stage acceptance gate (Week 6 Day 4).

Combines:
  - Stage 1: regression_detector (baseline 比較、no block tier breach)
  - Stage 2: V6 acceptance criteria (絶対品質、schema=100% / Q3≥80% etc)

Decision matrix (TODO.md V6 W6 Day 1):

| Stage 1 | Stage 2 | Verdict |
|---------|---------|---------|
| pass    | pass    | adopt   |
| pass    | fail    | hold    |
| fail    | (any)   | reject  |

See: docs/eval/regression_detector_design.md §5
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .regression_detector import detect_regression

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage 2 criteria (V6 acceptance)
# ---------------------------------------------------------------------------

DEFAULT_ACCEPTANCE: dict[str, float] = {
    "schema_pass":                    1.00,
    "q3_composite":                   0.80,
    "actionability_specific":         0.80,
    "failure_taxonomy":               0.70,
    "recall_strict":                  0.50,
    "hallucination_clean":            0.85,
    "ungrounded_numeric_max":         0.15,
    "parse_success_rate":             0.90,
    "case_regressions_max":           1,
    "canonical_parse_failure_max":    0.05,
}


@dataclass
class StageGateResult:
    current_run: str
    baseline_run: str | None
    stage1_verdict: str  # "pass" | "fail"
    stage2_verdict: str  # "pass" | "fail"
    overall_verdict: str  # "adopt" | "hold" | "reject"
    stage1_violations: list[dict[str, Any]] = field(default_factory=list)
    stage2_violations: list[dict[str, Any]] = field(default_factory=list)
    measured: dict[str, float] = field(default_factory=dict)
    acceptance: dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Stage 2 evaluation
# ---------------------------------------------------------------------------


def _evaluated(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in records if "skipped_reason" not in r]


def _avg_field(records: list[dict[str, Any]], field_name: str) -> float:
    vals = [r.get(field_name) for r in _evaluated(records) if r.get(field_name) is not None]
    vals = [v for v in vals if isinstance(v, (int, float))]
    return sum(vals) / len(vals) if vals else 0.0


def _schema_pass_rate(records: list[dict[str, Any]]) -> float:
    ev = _evaluated(records)
    if not ev:
        return 1.0
    return sum(1 for r in ev if r.get("schema_valid") is True) / len(ev)


def _canonical_failure_rate(records: list[dict[str, Any]]) -> float:
    ev = _evaluated(records)
    if not ev:
        return 0.0
    fails = sum(
        1 for r in ev
        if r.get("canonical_source") in (None, "normalizer_fallback", "missing")
    )
    return fails / len(ev)


def _parse_success_rate(records: list[dict[str, Any]]) -> float:
    methods: list[str] = []
    for r in _evaluated(records):
        methods.extend(r.get("skeleton_methods") or [])
    if not methods:
        return 1.0
    # V6.1: include merge/view/insert when V6_SQL_SKELETON_EXTENDED=1
    success_set = {"fullsql", "sqlglot", "bypass", "merge", "view", "insert"}
    succ = sum(1 for m in methods if m in success_set)
    return succ / len(methods)


def evaluate_stage2(
    current_payload: dict[str, Any],
    *,
    case_regressions_count: int = 0,
    acceptance: dict[str, float] | None = None,
) -> tuple[str, list[dict[str, Any]], dict[str, float]]:
    """Evaluate the absolute V6 acceptance criteria on a single baseline."""
    acceptance = acceptance or DEFAULT_ACCEPTANCE
    records = current_payload.get("cases") or []

    measured = {
        "schema_pass":                _schema_pass_rate(records),
        "q3_composite":               _avg_field(records, "evidence_grounding_composite"),
        "actionability_specific":     _avg_field(records, "actionability_specific_ratio"),
        "failure_taxonomy":           _avg_field(records, "failure_taxonomy_score"),
        "recall_strict":              _avg_field(records, "recall_strict_ratio"),
        "hallucination_clean":        _avg_field(records, "hallucination_score_avg"),
        "ungrounded_numeric_avg":     _avg_field(records, "ungrounded_numeric_ratio"),
        "parse_success_rate":         _parse_success_rate(records),
        "canonical_parse_failure":    _canonical_failure_rate(records),
        "case_regressions":           float(case_regressions_count),
    }

    violations: list[dict[str, Any]] = []

    def _add(metric: str, current: float, target: float, op: str) -> None:
        violations.append({
            "metric": metric, "current": round(current, 4),
            "target": target, "op": op,
        })

    if measured["schema_pass"] < acceptance["schema_pass"]:
        _add("schema_pass", measured["schema_pass"], acceptance["schema_pass"], ">=")
    if measured["q3_composite"] < acceptance["q3_composite"]:
        _add("q3_composite", measured["q3_composite"], acceptance["q3_composite"], ">=")
    if measured["actionability_specific"] < acceptance["actionability_specific"]:
        _add("actionability_specific", measured["actionability_specific"], acceptance["actionability_specific"], ">=")
    if measured["failure_taxonomy"] < acceptance["failure_taxonomy"]:
        _add("failure_taxonomy", measured["failure_taxonomy"], acceptance["failure_taxonomy"], ">=")
    if measured["recall_strict"] < acceptance["recall_strict"]:
        _add("recall_strict", measured["recall_strict"], acceptance["recall_strict"], ">=")
    if measured["hallucination_clean"] < acceptance["hallucination_clean"]:
        _add("hallucination_clean", measured["hallucination_clean"], acceptance["hallucination_clean"], ">=")
    if measured["ungrounded_numeric_avg"] > acceptance["ungrounded_numeric_max"]:
        _add("ungrounded_numeric", measured["ungrounded_numeric_avg"], acceptance["ungrounded_numeric_max"], "<=")
    if measured["parse_success_rate"] < acceptance["parse_success_rate"]:
        _add("parse_success_rate", measured["parse_success_rate"], acceptance["parse_success_rate"], ">=")
    if measured["canonical_parse_failure"] > acceptance["canonical_parse_failure_max"]:
        _add("canonical_parse_failure", measured["canonical_parse_failure"], acceptance["canonical_parse_failure_max"], "<=")
    if measured["case_regressions"] > acceptance["case_regressions_max"]:
        _add("case_regressions", int(measured["case_regressions"]), acceptance["case_regressions_max"], "<=")

    verdict = "fail" if violations else "pass"
    return verdict, violations, measured


# ---------------------------------------------------------------------------
# Two-stage public API
# ---------------------------------------------------------------------------


def run_stage_gate(
    current_path: Path,
    *,
    baseline_path: Path | None = None,
    acceptance: dict[str, float] | None = None,
) -> StageGateResult:
    """Run both stages and produce a verdict."""
    current = json.loads(current_path.read_text())

    # Stage 1
    stage1_violations: list[dict[str, Any]] = []
    stage1_verdict = "pass"
    if baseline_path is not None and baseline_path.exists():
        regr = detect_regression(current_path, baseline_path)
        stage1_violations = regr.block_violations
        if regr.verdict == "block":
            stage1_verdict = "fail"
    else:
        # No baseline supplied → stage 1 is pass-by-default (informational only)
        stage1_verdict = "pass"

    # Stage 2
    case_regressions_count = sum(
        1 for v in stage1_violations
    )  # crude proxy when stage1 had block items
    stage2_verdict, stage2_violations, measured = evaluate_stage2(
        current,
        case_regressions_count=case_regressions_count,
        acceptance=acceptance,
    )

    # Decision matrix
    if stage1_verdict == "fail":
        overall = "reject"
    elif stage2_verdict == "fail":
        overall = "hold"
    else:
        overall = "adopt"

    return StageGateResult(
        current_run=current_path.stem,
        baseline_run=(baseline_path.stem if baseline_path else None),
        stage1_verdict=stage1_verdict,
        stage2_verdict=stage2_verdict,
        overall_verdict=overall,
        stage1_violations=stage1_violations,
        stage2_violations=stage2_violations,
        measured=measured,
        acceptance=acceptance or DEFAULT_ACCEPTANCE,
    )


def to_dict(result: StageGateResult) -> dict[str, Any]:
    return asdict(result)
