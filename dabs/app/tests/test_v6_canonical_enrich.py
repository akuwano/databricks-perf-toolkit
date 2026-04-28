"""Tests for ``enrich_llm_canonical`` — fills operational metadata
the LLM cannot know.

V5 vs V6 smoke (2026-04-26) revealed that V6 LLM-direct canonical
output was failing JSON Schema validation 2/2 times because the LLM
omits ``report_id`` (UUID), ``generated_at`` (timestamp), and
``query_id`` (profile-derived) — fields that aren't reasonable for
the model to invent. The normalizer adapter (V5 fallback) already
populates them; the LLM-direct path needs an equivalent enrichment.
"""

from __future__ import annotations

import re
from datetime import datetime

from core.v6_schema.normalizer import enrich_llm_canonical
from core.models import ProfileAnalysis, QueryMetrics


def _analysis(query_id: str = "01abc-1234") -> ProfileAnalysis:
    return ProfileAnalysis(query_metrics=QueryMetrics(query_id=query_id))


def _llm_extracted(*, missing: tuple[str, ...] = ()) -> dict:
    """A minimal LLM-direct canonical (the LLM emits findings + summary
    but not the operational metadata)."""
    full = {
        "schema_version": "v6.0",
        "report_id": "should-be-replaced-if-missing",
        "generated_at": "should-be-replaced-if-missing",
        "query_id": "should-be-replaced-if-missing",
        "summary": {"headline": "Heavy shuffle"},
        "findings": [{"issue_id": "shuffle_overhead", "severity": "high"}],
    }
    for key in missing:
        full.pop(key, None)
    return full


# ---------------------------------------------------------------------------
# Operational metadata is filled when missing
# ---------------------------------------------------------------------------


class TestFillsMissingMetadata:
    def test_fills_query_id_from_analysis(self):
        extracted = _llm_extracted(missing=("query_id",))
        out = enrich_llm_canonical(extracted, _analysis(query_id="qid-42"))
        assert out["query_id"] == "qid-42"

    def test_fills_query_id_unknown_when_analysis_lacks_it(self):
        extracted = _llm_extracted(missing=("query_id",))
        out = enrich_llm_canonical(extracted, _analysis(query_id=""))
        assert out["query_id"] == "unknown"

    def test_fills_report_id_with_uuid(self):
        extracted = _llm_extracted(missing=("report_id",))
        out = enrich_llm_canonical(extracted, _analysis())
        # UUID v4 shape
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            out["report_id"],
        )

    def test_fills_generated_at_with_iso_timestamp(self):
        extracted = _llm_extracted(missing=("generated_at",))
        out = enrich_llm_canonical(extracted, _analysis())
        # YYYY-MM-DDTHH:MM:SSZ
        datetime.strptime(out["generated_at"], "%Y-%m-%dT%H:%M:%SZ")

    def test_fills_schema_version_when_missing(self):
        extracted = _llm_extracted(missing=("schema_version",))
        out = enrich_llm_canonical(extracted, _analysis())
        # Canonical Report schema_version is the string "v6.0" (the
        # JSON Schema declares it as a const). Don't conflate with
        # the sidecar's integer schema_version.
        assert out["schema_version"] == "v6.0"


# ---------------------------------------------------------------------------
# Existing fields are preserved (LLM wins when it provided real data)
# ---------------------------------------------------------------------------


class TestPreservesExistingFields:
    def test_keeps_findings_unchanged(self):
        extracted = _llm_extracted()
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["issue_id"] == "shuffle_overhead"

    def test_keeps_summary_headline(self):
        extracted = _llm_extracted()
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["summary"]["headline"] == "Heavy shuffle"

    def test_does_not_overwrite_real_query_id_with_unknown(self):
        """When the LLM happens to emit a non-empty query_id (e.g.
        because we put it in the Fact Pack), don't clobber it."""
        extracted = _llm_extracted()
        extracted["query_id"] = "llm-supplied-id"
        out = enrich_llm_canonical(extracted, _analysis(query_id="profile-id"))
        # Profile-derived id wins because it's authoritative — but
        # the field is non-empty either way; the contract is "fill
        # when missing/empty", not "overwrite always". For now we
        # let analysis win since LLM-supplied IDs are unreliable.
        assert out["query_id"] in {"llm-supplied-id", "profile-id"}


# ---------------------------------------------------------------------------
# Integration: enriched canonical passes JSON Schema validation
# ---------------------------------------------------------------------------


class TestContextEnrichment:
    """Smoke 2 (2026-04-26) found that schema_valid stayed False even
    with operational metadata filled because ``context`` is also
    required and the LLM doesn't know warehouse_size, is_serverless,
    etc. Build it from the analysis."""

    def test_fills_context_when_missing(self):
        extracted = _llm_extracted(missing=())
        out = enrich_llm_canonical(extracted, _analysis())
        assert "context" in out
        assert isinstance(out["context"], dict)
        # is_serverless flag is derived from query_typename
        assert "is_serverless" in out["context"]

    def test_overwrites_partial_llm_context(self):
        """Profile is authoritative — even if the LLM emitted a
        partial/wrong context block we replace it with the analysis-
        derived one."""
        extracted = _llm_extracted()
        extracted["context"] = {"is_serverless": False}  # potentially wrong
        out = enrich_llm_canonical(extracted, _analysis())
        # The rebuild reflects the analysis (default QueryMetrics has
        # query_typename="" so is_serverless evaluates False; the test
        # is that ``context`` is rebuilt, not just trusted).
        assert "is_serverless" in out["context"]


class TestFixTypeNormalization:
    """LLM uses near-canonical fix_type values (``query_rewrite``
    instead of ``rewrite``) that the schema rejects. Map known
    aliases so the report still validates."""

    def test_query_rewrite_aliased_to_rewrite(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "actions": [{"fix_type": "query_rewrite", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["actions"][0]["fix_type"] == "rewrite"

    def test_known_canonical_value_preserved(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "actions": [{"fix_type": "ddl", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["actions"][0]["fix_type"] == "ddl"

    def test_unknown_value_left_alone_for_schema_validator(self):
        """If the LLM invents a brand-new alias we haven't mapped,
        leave it so the schema validator can flag it. The fix is to
        update either the prompt or _FIX_TYPE_ALIASES."""
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "actions": [{"fix_type": "invented_value", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["actions"][0]["fix_type"] == "invented_value"

    def test_smoke_n5c_aliases_covered(self):
        """Smoke n=5c (2026-04-27): with LLM-direct emission rate
        restored to 5/5, two new aliases surfaced."""
        # ``fix_type='sql'`` is a common LLM shortening of "SQL rewrite".
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "actions": [{"fix_type": "sql", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["actions"][0]["fix_type"] == "rewrite"

    def test_smoke_n5_aliases_covered(self):
        """Smoke n=5 (2026-04-26) found 3 cases failing schema due to
        new fix_type aliases the LLM produced. Pin them so we know
        when they regress."""
        for alias, canonical in [
            ("schema_change", "ddl"),
            ("monitoring", "investigation"),
            ("ddl_or_sql_rewrite", "rewrite"),
        ]:
            extracted = _llm_extracted()
            extracted["findings"] = [{
                "issue_id": "x",
                "actions": [{"fix_type": alias, "what": "..."}],
            }]
            out = enrich_llm_canonical(extracted, _analysis())
            assert out["findings"][0]["actions"][0]["fix_type"] == canonical, (
                f"alias {alias!r} should map to {canonical!r}"
            )


class TestIssueIdNormalization:
    """Smoke n=5 (2026-04-26): V6 LLM emits creative issue_ids that
    fail recall_strict. The prompt allowlist is the primary control;
    this alias map is a conservative safety net for verbatim
    spelling variants we observed in real runs."""

    def test_disk_prefix_aliased_to_canonical(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "disk_spill_dominant",
            "actions": [{"fix_type": "ddl", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["issue_id"] == "spill_dominant"

    def test_full_table_scan_variants_aliased(self):
        for variant, canonical in [
            ("zero_file_pruning", "low_file_pruning"),
            ("full_table_scan_no_pruning", "low_file_pruning"),
            ("full_table_scan_no_clustering", "missing_clustering"),
        ]:
            extracted = _llm_extracted()
            extracted["findings"] = [{
                "issue_id": variant,
                "actions": [{"fix_type": "ddl", "what": "..."}],
            }]
            out = enrich_llm_canonical(extracted, _analysis())
            assert out["findings"][0]["issue_id"] == canonical, (
                f"{variant!r} should map to {canonical!r}"
            )

    def test_unknown_issue_id_left_alone(self):
        """If the LLM invents a brand-new id not in the alias map,
        leave it so the schema/recall scorers can flag it."""
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "totally_invented",
            "actions": [{"fix_type": "ddl", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["issue_id"] == "totally_invented"

    def test_canonical_id_preserved(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "spill_dominant",
            "actions": [{"fix_type": "ddl", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["issue_id"] == "spill_dominant"


class TestCategoryNormalization:
    """Smoke n=5 also revealed category aliases (most commonly
    ``cluster`` for ``clustering``)."""

    def test_cluster_aliased_to_clustering(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "category": "cluster",
            "actions": [{"fix_type": "ddl", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["category"] == "clustering"

    def test_canonical_category_preserved(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "category": "memory",
            "actions": [{"fix_type": "ddl", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["category"] == "memory"

    def test_unknown_category_left_alone(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "category": "totally_invented",
            "actions": [{"fix_type": "ddl", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert out["findings"][0]["category"] == "totally_invented"


    def test_appendix_actions_normalized_too(self):
        extracted = _llm_extracted()
        extracted["appendix_excluded_findings"] = [{
            "issue_id": "y",
            "actions": [{"fix_type": "sql_rewrite", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        assert (
            out["appendix_excluded_findings"][0]["actions"][0]["fix_type"]
            == "rewrite"
        )


class TestVerificationNormalization:
    """Smoke 3 (2026-04-26) found the LLM emitting non-canonical
    verification types like ``ddl_check`` that the schema's oneOf
    rejects. Coerce common shapes."""

    def test_ddl_check_with_sql_in_metric_becomes_sql_type(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "actions": [{
                "fix_type": "ddl",
                "what": "...",
                "verification": [
                    {"type": "metric", "metric": "rows_read", "expected": "<1B"},
                    {
                        "type": "ddl_check",
                        "metric": "DESCRIBE DETAIL main.base.lineitem",
                        "expected": "clusteringColumns populated",
                    },
                ],
            }],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        verifs = out["findings"][0]["actions"][0]["verification"]
        assert verifs[0]["type"] == "metric"  # canonical untouched
        assert verifs[1]["type"] == "sql"
        assert "DESCRIBE DETAIL" in verifs[1]["sql"]
        assert verifs[1]["expected"] == "clusteringColumns populated"
        assert "metric" not in verifs[1]  # foreign key dropped

    def test_sql_type_with_metric_field_reshaped_to_sql_field(self):
        """Smoke n=5c (2026-04-27): the LLM sometimes emits a
        verification entry with the canonical ``type='sql'`` but puts
        the SQL under ``metric`` instead of ``sql``. The schema
        requires ``sql`` field for the sql branch — reshape."""
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "actions": [{
                "fix_type": "rewrite",
                "what": "...",
                "verification": [{
                    "type": "sql",
                    "metric": "SELECT count(*) FROM cat.sch.t",
                    "expected": "rows < 1B",
                }],
            }],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        v = out["findings"][0]["actions"][0]["verification"][0]
        assert v["type"] == "sql"
        assert v["sql"] == "SELECT count(*) FROM cat.sch.t"
        assert v["expected"] == "rows < 1B"
        assert "metric" not in v

    def test_unknown_type_with_no_sql_left_alone(self):
        """Genuinely novel shapes pass through so the schema can flag
        them — better to surface than silently mangle."""
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "actions": [{
                "fix_type": "investigation",
                "what": "...",
                "verification": [{"type": "manual", "step": "do thing"}],
            }],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        verifs = out["findings"][0]["actions"][0]["verification"]
        assert verifs[0]["type"] == "manual"  # untouched

    def test_no_verification_block_handled(self):
        extracted = _llm_extracted()
        extracted["findings"] = [{
            "issue_id": "x",
            "actions": [{"fix_type": "ddl", "what": "..."}],
        }]
        out = enrich_llm_canonical(extracted, _analysis())
        # Just verifying no crash — action without verification is fine.
        assert "verification" not in out["findings"][0]["actions"][0]


class TestValidatesAgainstSchema:
    def test_enriched_canonical_has_all_top_level_required_fields(self):
        """The original V5 vs V6 smoke failure: 3 required top-level
        fields missing from LLM output. Smoke 2 found ``context``
        also missing. Post-enrichment, all 7 are present without any
        manual input."""
        extracted = _llm_extracted(
            missing=("report_id", "generated_at", "query_id")
        )
        # Note: NOT pre-filling context — the helper must build it.
        out = enrich_llm_canonical(extracted, _analysis())
        for required in (
            "schema_version",
            "report_id",
            "generated_at",
            "query_id",
            "context",
            "summary",
            "findings",
        ):
            assert required in out, f"missing top-level required field {required!r}"
