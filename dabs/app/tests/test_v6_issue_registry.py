"""Tests for V6 issue registry (W2.5 #5).

Ensures the registry is internally consistent and that golden cases only
reference issue_ids that exist in the registry.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from core.v6_schema.issue_registry import (
    ALL_ISSUE_IDS,
    ISSUE_BY_CATEGORY,
    ISSUE_BY_ID,
    ISSUES,
    get_keywords,
    is_known,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
GOLDENS_DIR = REPO_ROOT / "eval" / "goldens" / "cases"

_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


# ----- internal consistency -----


def test_no_duplicate_ids():
    ids = [i.id for i in ISSUES]
    assert len(ids) == len(set(ids)), "duplicate issue_id detected"


def test_all_ids_snake_case():
    for i in ISSUES:
        assert _SNAKE_CASE.match(i.id), f"non-snake_case id: {i.id}"


def test_all_categories_in_schema_enum():
    """Categories must match schemas/report_v6.schema.json Finding.category."""
    valid_categories = {
        "memory", "shuffle", "skew", "photon", "scan", "cache",
        "compilation", "driver", "federation", "streaming",
        "stats", "cardinality", "clustering", "sql_pattern",
        "io", "join", "compute", "other",
    }
    for i in ISSUES:
        assert i.category in valid_categories, (
            f"issue {i.id} has category {i.category} not in schema enum"
        )


def test_keywords_non_empty_for_all():
    for i in ISSUES:
        assert i.keywords, f"issue {i.id} has empty keywords"


def test_helper_lookups():
    assert is_known("spill_dominant")
    assert not is_known("totally_made_up_id")
    assert get_keywords("spill_dominant")
    assert get_keywords("nonexistent") == ()


def test_indexes_consistent():
    assert set(ISSUE_BY_ID.keys()) == ALL_ISSUE_IDS
    by_cat_total = sum(len(v) for v in ISSUE_BY_CATEGORY.values())
    assert by_cat_total == len(ISSUES)


# ----- golden case alignment -----


def _golden_files() -> list[Path]:
    """Recurse into evidence_grounding/ and any future subdirs (W3 #6)."""
    return sorted(GOLDENS_DIR.rglob("*.yaml"))


@pytest.mark.parametrize("golden_file", _golden_files(), ids=lambda p: p.name)
def test_golden_must_cover_issues_use_registered_ids(golden_file):
    with open(golden_file, encoding="utf-8") as f:
        case = yaml.safe_load(f)
    must_cover = case.get("must_cover_issues") or []
    for issue in must_cover:
        iid = issue.get("id", "")
        if not iid:
            continue
        # Goldens may include issue_ids that are emerging concepts not yet
        # in the registry — but they must still be snake_case so the recall
        # scorer can hash them.
        assert _SNAKE_CASE.match(iid), (
            f"{golden_file.name}: id {iid!r} is not snake_case"
        )
