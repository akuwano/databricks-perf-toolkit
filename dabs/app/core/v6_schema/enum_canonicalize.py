"""Step 3 of ``enrich_llm_canonical``: rewrite enum-typed values.

Walks ``out['findings']`` and ``out['appendix_excluded_findings']``
(both blocks may be present in canonical Reports), and rewrites the
three enum fields with the alias maps from ``aliases.py``:

  - ``Finding.issue_id``     ← ``ISSUE_ID_ALIASES_LLM``
  - ``Finding.category``     ← ``CATEGORY_ALIASES_LLM``
  - ``Action.fix_type``      ← ``FIX_TYPE_ALIASES``

A shared ``AliasHitCounts`` tracker accumulates per-rewrite counts so
the eval ab_runner and production telemetry can report
``alias_hit_rate`` and ``hits_per_case_avg`` without re-parsing the
output.

Verification entry reshaping lives in ``verification_reshape.py``;
this module only handles enum rewrites.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .aliases import (
    apply_category_alias,
    apply_fix_type_alias,
    apply_issue_id_alias,
)

if TYPE_CHECKING:
    from .alias_telemetry import AliasHitCounts


def canonicalize_enums(
    out: dict[str, Any],
    tracker: "AliasHitCounts | None" = None,
) -> dict[str, Any]:
    """Rewrite findings + appendix in place. Returns ``out`` for
    chaining inside the orchestrator."""
    for finding_block in ("findings", "appendix_excluded_findings"):
        for f in out.get(finding_block) or []:
            if "issue_id" in f:
                f["issue_id"] = apply_issue_id_alias(f["issue_id"], tracker)
            if "category" in f:
                f["category"] = apply_category_alias(f["category"], tracker)
            for a in f.get("actions") or []:
                if "fix_type" in a:
                    a["fix_type"] = apply_fix_type_alias(a["fix_type"], tracker)
    return out
