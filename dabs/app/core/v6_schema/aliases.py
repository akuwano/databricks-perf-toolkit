"""Single source of truth for V6 canonical Report alias maps.

Three alias maps coerce near-canonical LLM output into the schema's
enum values when the LLM emits a recognised synonym instead of the
canonical token. The admission rule for each entry is documented in
``docs/v6/alias-admission-rule.md`` (3-criterion: recurrence +
prompt-resistance + unambiguous mapping).

This module is the **only** place these maps should be defined. The
rest of the codebase (normalizer, enrich pipeline, tests, telemetry)
must import from here so admission decisions stay reviewable in one
diff.

Telemetry: each ``apply_*`` helper takes an optional
``AliasHitCounts`` tracker and increments the matching counter on a
hit. ``None`` is the default so callers without telemetry needs see
no behavioural change.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .alias_telemetry import AliasHitCounts


# LLM sometimes uses near-canonical fix_type values that the schema
# doesn't accept. Map them so the report still validates without
# requiring a prompt change cycle.
FIX_TYPE_ALIASES: dict[str, str] = {
    "sql": "rewrite",
    "query_rewrite": "rewrite",
    "sql_rewrite": "rewrite",
    "ddl_or_sql_rewrite": "rewrite",
    "schema_change": "ddl",
    "alter": "ddl",
    "config": "configuration",
    "config_change": "configuration",
    "tuning": "configuration",
    "optimize": "maintenance",
    "vacuum": "maintenance",
    "monitoring": "investigation",
    "diagnose": "investigation",
    "analyze": "investigation",
    "human_review": "investigation",
    "review": "investigation",
    "manual": "operational",
}


# Schema enum for ``Finding.category``. LLM sometimes uses near-canonical
# aliases (most commonly ``cluster`` for ``clustering``) that the schema
# rejects. Distinct from the V5-mode ``_normalize_category`` (which maps
# free text → category and falls back to "other"); this one only maps
# known LLM-emitted alias variants of valid categories.
CATEGORY_ALIASES_LLM: dict[str, str] = {
    "cluster": "clustering",
    "compute": "compilation",
    "schema": "stats",
    "data_skew": "skew",
    "data_quality": "stats",
}


# Conservative issue_id alias map — covers verbatim spelling variations
# the LLM emitted in smoke n=5 (Codex 2026-04-26 review). Only safe-by-
# inspection mappings; avoid fuzzy matching to keep the allowlist
# (prompt-side) the primary control.
ISSUE_ID_ALIASES_LLM: dict[str, str] = {
    "disk_spill_dominant": "spill_dominant",
    "spill_to_disk_dominant": "spill_dominant",
    "zero_file_pruning": "low_file_pruning",
    "full_table_scan_no_pruning": "low_file_pruning",
    "full_table_scan_zero_pruning": "low_file_pruning",
    "full_table_scan_no_clustering": "missing_clustering",
    "cache_hit_ratio_medium": "low_cache_hit",
    "cache_hit_ratio_low": "low_cache_hit",
    "low_delta_cache_hit": "low_cache_hit",
    "photon_utilization_below_threshold": "photon_partial_fallback",
    "implicit_cast_join_key": "implicit_cast_on_join_key",
    "duplicate_cte_scan": "cte_recompute",
}


def apply_fix_type_alias(value: Any, tracker: "AliasHitCounts | None" = None) -> Any:
    """Map ``value`` through ``FIX_TYPE_ALIASES``; record on hit."""
    if not isinstance(value, str):
        return value
    mapped = FIX_TYPE_ALIASES.get(value.lower(), value)
    if tracker is not None and mapped != value:
        tracker.record("fix_type")
    return mapped


def apply_category_alias(value: Any, tracker: "AliasHitCounts | None" = None) -> Any:
    """Map ``value`` through ``CATEGORY_ALIASES_LLM``; record on hit."""
    if not isinstance(value, str):
        return value
    mapped = CATEGORY_ALIASES_LLM.get(value.lower(), value)
    if tracker is not None and mapped != value:
        tracker.record("category")
    return mapped


def apply_issue_id_alias(value: Any, tracker: "AliasHitCounts | None" = None) -> Any:
    """Map ``value`` through ``ISSUE_ID_ALIASES_LLM``; record on hit."""
    if not isinstance(value, str):
        return value
    mapped = ISSUE_ID_ALIASES_LLM.get(value.lower(), value)
    if tracker is not None and mapped != value:
        tracker.record("issue_id")
    return mapped
