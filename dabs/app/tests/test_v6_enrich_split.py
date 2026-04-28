"""Tests for the v6.7.1 enrich-pipeline split.

The v6.6.5+ ADR (``docs/v6/alias-admission-rule.md``) called for
splitting ``enrich_llm_canonical`` into four ordered phases:

  1. ``metadata_repair``    — schema_version / report_id / etc.
  2. ``context_rebuild``    — context object from analysis
  3. ``enum_canonicalize``  — alias maps for fix_type/category/issue_id
  4. ``verification_reshape`` — coerce verification dicts into a valid
     oneOf branch

This test module locks down the contract Codex flagged as the main
risk area:

  - **Phase isolation**: each phase can be tested in isolation and
    only mutates its own slice of the dict.
  - **Alias source-of-truth**: the alias maps live in a single place
    (``aliases.py``) and the rest of the code imports from there.
  - **Idempotence**: running the pipeline twice on the same input
    produces the same output (catches order-dependent bugs).
  - **End-to-end equivalence**: a fixed input maps to a fixed output
    so future re-orderings of the steps cannot silently change
    behaviour.
"""

from __future__ import annotations

from core.models import BottleneckIndicators, ProfileAnalysis, QueryMetrics
from core.v6_schema.alias_telemetry import AliasHitCounts
from core.v6_schema.aliases import (
    CATEGORY_ALIASES_LLM,
    FIX_TYPE_ALIASES,
    ISSUE_ID_ALIASES_LLM,
    apply_category_alias,
    apply_fix_type_alias,
    apply_issue_id_alias,
)
from core.v6_schema.context_rebuild import build_context, rebuild_context
from core.v6_schema.enrich import enrich_llm_canonical
from core.v6_schema.enum_canonicalize import canonicalize_enums
from core.v6_schema.metadata_repair import repair_metadata
from core.v6_schema.verification_reshape import (
    VALID_VERIFICATION_TYPES,
    reshape_verification_entry,
    reshape_verifications,
)


def _analysis(query_id: str = "q-1") -> ProfileAnalysis:
    return ProfileAnalysis(
        query_metrics=QueryMetrics(query_id=query_id),
        bottleneck_indicators=BottleneckIndicators(),
    )


# ---------------------------------------------------------------------------
# aliases.py — single source of truth
# ---------------------------------------------------------------------------


class TestAliasSingleSourceOfTruth:
    def test_fix_type_aliases_includes_known_entries(self):
        # Spot-check entries that have shipped (regression guard).
        assert FIX_TYPE_ALIASES["sql_rewrite"] == "rewrite"
        assert FIX_TYPE_ALIASES["query_rewrite"] == "rewrite"
        assert FIX_TYPE_ALIASES["alter"] == "ddl"

    def test_category_aliases_includes_known_entries(self):
        assert CATEGORY_ALIASES_LLM["cluster"] == "clustering"
        assert CATEGORY_ALIASES_LLM["data_skew"] == "skew"

    def test_issue_id_aliases_includes_known_entries(self):
        assert ISSUE_ID_ALIASES_LLM["disk_spill_dominant"] == "spill_dominant"
        assert ISSUE_ID_ALIASES_LLM["zero_file_pruning"] == "low_file_pruning"

    def test_apply_helpers_pass_through_canonical_values(self):
        # Canonical values must not be re-mapped (would drift under
        # repeated normalize calls — see idempotence test below).
        assert apply_fix_type_alias("rewrite") == "rewrite"
        assert apply_category_alias("clustering") == "clustering"
        assert apply_issue_id_alias("spill_dominant") == "spill_dominant"

    def test_legacy_imports_still_resolve(self):
        """Backward compat: existing tests / callers may still import
        the underscore-prefixed helpers from normalizer.py."""
        from core.v6_schema.normalizer import (
            _normalize_finding_category,
            _normalize_fix_type,
            _normalize_issue_id,
        )

        assert _normalize_fix_type("sql_rewrite") == "rewrite"
        assert _normalize_finding_category("cluster") == "clustering"
        assert _normalize_issue_id("disk_spill_dominant") == "spill_dominant"


# ---------------------------------------------------------------------------
# Phase isolation: each phase only mutates its own slice
# ---------------------------------------------------------------------------


class TestPhaseIsolation:
    def test_repair_metadata_only_touches_top_level(self):
        out: dict = {"findings": [{"issue_id": "disk_spill_dominant"}]}
        repair_metadata(out, _analysis())
        # Top-level metadata filled
        assert "schema_version" in out
        assert "report_id" in out
        assert "generated_at" in out
        assert out["query_id"] == "q-1"
        # Findings untouched (alias map is a different phase)
        assert out["findings"][0]["issue_id"] == "disk_spill_dominant"

    def test_rebuild_context_only_writes_context_key(self):
        out: dict = {"findings": [{"issue_id": "disk_spill_dominant"}]}
        rebuild_context(out, _analysis(), "en")
        assert "context" in out
        # Aliases not yet applied — that's a different phase.
        assert out["findings"][0]["issue_id"] == "disk_spill_dominant"

    def test_canonicalize_enums_only_rewrites_findings(self):
        out: dict = {
            "schema_version": "v6.0",  # already filled
            "findings": [
                {
                    "issue_id": "disk_spill_dominant",
                    "category": "cluster",
                    "actions": [{"fix_type": "sql_rewrite"}],
                }
            ],
        }
        canonicalize_enums(out)
        assert out["findings"][0]["issue_id"] == "spill_dominant"
        assert out["findings"][0]["category"] == "clustering"
        assert out["findings"][0]["actions"][0]["fix_type"] == "rewrite"
        # schema_version untouched (a different phase owns it).
        assert out["schema_version"] == "v6.0"

    def test_reshape_verifications_only_walks_actions(self):
        out: dict = {
            "findings": [
                {
                    "issue_id": "spill_dominant",  # already canonical
                    "actions": [
                        {
                            "fix_type": "rewrite",
                            "verification": [
                                {"type": "sql", "metric": "SELECT 1"}  # reshape
                            ],
                        }
                    ],
                }
            ],
        }
        reshape_verifications(out)
        v = out["findings"][0]["actions"][0]["verification"][0]
        assert v["type"] == "sql"
        assert v["sql"] == "SELECT 1"
        # issue_id stayed canonical (different phase).
        assert out["findings"][0]["issue_id"] == "spill_dominant"


# ---------------------------------------------------------------------------
# Idempotence — Codex pitfall guard
# ---------------------------------------------------------------------------


class TestIdempotence:
    def test_enrich_twice_yields_same_findings_and_context(self):
        """Running ``enrich_llm_canonical`` twice on its own output
        must not drift — any per-step transform that re-mapped a
        canonical value would break this."""
        extracted = {
            "findings": [
                {
                    "issue_id": "disk_spill_dominant",
                    "category": "cluster",
                    "actions": [
                        {
                            "fix_type": "sql_rewrite",
                            "verification": [
                                {"type": "sql", "metric": "SELECT 1"}
                            ],
                        }
                    ],
                }
            ],
        }
        once = enrich_llm_canonical(extracted, _analysis())
        # Strip non-deterministic fields (uuid, timestamp) so we can
        # compare structurally.
        for k in ("report_id", "generated_at"):
            once.pop(k, None)
        twice = enrich_llm_canonical(once, _analysis())
        for k in ("report_id", "generated_at"):
            twice.pop(k, None)
        assert once == twice

    def test_canonicalize_enums_does_not_drift(self):
        """Calling ``canonicalize_enums`` on already-canonical input
        is a no-op and leaves the alias tracker at zero."""
        already = {
            "findings": [
                {
                    "issue_id": "spill_dominant",
                    "category": "clustering",
                    "actions": [{"fix_type": "rewrite"}],
                }
            ],
        }
        tracker = AliasHitCounts()
        canonicalize_enums(already, tracker)
        assert tracker.total == 0
        assert already["findings"][0]["issue_id"] == "spill_dominant"


# ---------------------------------------------------------------------------
# End-to-end equivalence — fixed input → fixed output
# ---------------------------------------------------------------------------


class TestEndToEndEquivalence:
    def test_full_pipeline_fixed_input(self):
        """A canonical Report with all four phases triggered. Locks
        the behaviour so future re-orderings of the steps cannot
        silently change the output."""
        extracted = {
            "findings": [
                {
                    "issue_id": "disk_spill_dominant",          # alias
                    "category": "cluster",                        # alias
                    "actions": [
                        {
                            "fix_type": "sql_rewrite",            # alias
                            "verification": [
                                {"type": "sql", "metric": "SELECT 1"},  # reshape
                                {"type": "metric", "metric": "x"},      # canonical
                            ],
                        }
                    ],
                }
            ],
        }
        tracker = AliasHitCounts()
        out = enrich_llm_canonical(
            extracted, _analysis("q-99"), language="ja", alias_tracker=tracker
        )
        # Phase 1: metadata
        assert out["schema_version"] == "v6.0"
        assert out["query_id"] == "q-99"
        assert out["report_id"]      # uuid present
        assert out["generated_at"]   # timestamp present
        # Phase 2: context (language honored)
        assert out["context"]["language"] == "ja"
        # Phase 3: enums (3 alias hits recorded)
        f = out["findings"][0]
        assert f["issue_id"] == "spill_dominant"
        assert f["category"] == "clustering"
        a = f["actions"][0]
        assert a["fix_type"] == "rewrite"
        assert tracker.to_dict() == {"fix_type": 1, "category": 1, "issue_id": 1, "total": 3}
        # Phase 4: verifications (one reshaped, one passthrough)
        v = a["verification"]
        assert v[0] == {"type": "sql", "sql": "SELECT 1", "expected": ""}
        assert v[1] == {"type": "metric", "metric": "x"}

    def test_unknown_alias_passes_through_for_validator(self):
        """Per Codex's "unknown/invalid" pitfall: the normalizer must
        only coerce known aliases, never invent fallbacks for
        unfamiliar values. The schema validator is the one that
        ultimately rejects unknown enums."""
        extracted = {
            "findings": [
                {
                    "issue_id": "totally_made_up_id",
                    "category": "fake_category",
                    "actions": [{"fix_type": "made_up_fix"}],
                }
            ],
        }
        tracker = AliasHitCounts()
        out = enrich_llm_canonical(extracted, _analysis(), alias_tracker=tracker)
        f = out["findings"][0]
        assert f["issue_id"] == "totally_made_up_id"
        assert f["category"] == "fake_category"
        assert f["actions"][0]["fix_type"] == "made_up_fix"
        assert tracker.total == 0


# ---------------------------------------------------------------------------
# Module re-exports — VALID_VERIFICATION_TYPES sanity
# ---------------------------------------------------------------------------


def test_valid_verification_types_published():
    """The schema's three branches; locked here so the verification
    reshape phase and the schema definition stay in lockstep."""
    assert VALID_VERIFICATION_TYPES == ("metric", "sql", "explain")


def test_reshape_verification_entry_smoke():
    # Coverage check on the lowest-level helper.
    assert reshape_verification_entry({"type": "metric", "metric": "x"}) == {
        "type": "metric",
        "metric": "x",
    }
    # SQL-keyword detection in non-canonical type → coerce.
    out = reshape_verification_entry({"type": "query", "sql": "DESCRIBE t"})
    assert out["type"] == "sql"


def test_build_context_pure_helper():
    """``build_context`` must return a fresh dict (not mutate analysis)."""
    a = _analysis()
    ctx = build_context(a.query_metrics, a, "en")
    assert ctx["language"] == "en"
    assert ctx["is_serverless"] is False
    assert ctx["is_streaming"] is False
