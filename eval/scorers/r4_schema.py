"""R4: Canonical schema validation scorer.

Validates a generated canonical Report dict against
`schemas/report_v6.schema.json` and returns a SchemaScore that captures:
- valid: True/False
- error_count
- by_path: top-N (path, message) pairs for human inspection

Used by goldens_runner to populate per-case schema compliance metrics.

See: docs/v6/output_contract.md §9 (validation stages)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA_PATH = REPO_ROOT / "schemas" / "report_v6.schema.json"

_VALIDATOR_CACHE: dict[str, Draft202012Validator] = {}


def _validator_for(schema_path: Path) -> Draft202012Validator:
    key = str(schema_path)
    if key not in _VALIDATOR_CACHE:
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
        Draft202012Validator.check_schema(schema)
        _VALIDATOR_CACHE[key] = Draft202012Validator(schema)
    return _VALIDATOR_CACHE[key]


@dataclass
class SchemaScore:
    """Schema validation result for one canonical Report dict."""

    valid: bool = True
    error_count: int = 0
    findings_count: int = 0
    actions_count: int = 0
    appendix_count: int = 0
    by_path: list[tuple[str, str]] = field(default_factory=list)
    sampled_issue_ids: list[str] = field(default_factory=list)


def score_schema(
    canonical_report: dict[str, Any],
    *,
    schema_path: Path | None = None,
    max_errors: int = 10,
) -> SchemaScore:
    """Validate one canonical Report and return a SchemaScore."""
    if not isinstance(canonical_report, dict):
        return SchemaScore(valid=False, error_count=1, by_path=[("$", "report is not a dict")])

    validator = _validator_for(schema_path or DEFAULT_SCHEMA_PATH)
    errors: list[ValidationError] = sorted(validator.iter_errors(canonical_report), key=lambda e: list(e.absolute_path))

    findings = canonical_report.get("findings", []) or []
    appendix = canonical_report.get("appendix_excluded_findings", []) or []
    actions = sum(len(f.get("actions", []) or []) for f in findings)

    by_path: list[tuple[str, str]] = []
    for err in errors[:max_errors]:
        path = "$." + ".".join(str(p) for p in err.absolute_path) if err.absolute_path else "$"
        msg = err.message
        if len(msg) > 200:
            msg = msg[:200] + "..."
        by_path.append((path, msg))

    sampled_ids = [f.get("issue_id", "<missing>") for f in findings[:5]]

    return SchemaScore(
        valid=len(errors) == 0,
        error_count=len(errors),
        findings_count=len(findings),
        actions_count=actions,
        appendix_count=len(appendix),
        by_path=by_path,
        sampled_issue_ids=sampled_ids,
    )


def aggregate_schema_pass_rate(scores: list[SchemaScore]) -> float:
    """Across cases: ratio of cases that passed schema validation."""
    if not scores:
        return 1.0
    return sum(1 for s in scores if s.valid) / len(scores)
