"""Tests for ``core.v6_schema.evidence_sanitize`` (v6.7.2).

Closes the n=32 known gap: ``cross_join_explosion_q1`` produced
``"value_raw": false`` (Boolean), which the schema rejects because
``value_raw`` allows only ``number | string | null``. The prompt
directive is the primary control; this module is the post-process
safety net (two-tier defence pattern from v6.6.4 ``fix_sql``).
"""

from __future__ import annotations

from core.models import BottleneckIndicators, ProfileAnalysis, QueryMetrics
from core.v6_schema.enrich import enrich_llm_canonical
from core.v6_schema.evidence_sanitize import _is_valid_value_raw, sanitize_evidence


def _analysis() -> ProfileAnalysis:
    return ProfileAnalysis(
        query_metrics=QueryMetrics(query_id="q-1"),
        bottleneck_indicators=BottleneckIndicators(),
    )


class TestIsValidValueRaw:
    def test_int_is_valid(self):
        assert _is_valid_value_raw(42) is True

    def test_float_is_valid(self):
        assert _is_valid_value_raw(3.14) is True

    def test_string_is_valid(self):
        assert _is_valid_value_raw("12 GB") is True

    def test_none_is_valid(self):
        assert _is_valid_value_raw(None) is True

    def test_boolean_is_invalid(self):
        """The whole point of v6.7.2: Boolean is a Python ``int``
        subclass but the schema rejects it. Must be filtered before
        ``isinstance(int)`` short-circuits to True."""
        assert _is_valid_value_raw(True) is False
        assert _is_valid_value_raw(False) is False

    def test_list_is_invalid(self):
        assert _is_valid_value_raw([1, 2, 3]) is False

    def test_dict_is_invalid(self):
        assert _is_valid_value_raw({"x": 1}) is False


class TestSanitizeEvidence:
    def test_drops_boolean_value_raw(self):
        out = {
            "findings": [
                {
                    "evidence": [
                        {"metric": "cross_join_detected", "value_raw": False, "value_display": "no"},
                    ],
                }
            ],
        }
        sanitize_evidence(out)
        ev = out["findings"][0]["evidence"][0]
        assert "value_raw" not in ev
        assert ev["value_display"] == "no"  # other fields untouched

    def test_keeps_valid_value_raw(self):
        out = {
            "findings": [
                {
                    "evidence": [
                        {"metric": "spill_bytes", "value_raw": 12884901888, "value_display": "12 GB"},
                        {"metric": "cache_hit_ratio", "value_raw": 0.0, "value_display": "0%"},
                        {"metric": "warehouse_size", "value_raw": "Small", "value_display": "Small"},
                        {"metric": "missing_field", "value_raw": None, "value_display": "—"},
                    ],
                }
            ],
        }
        sanitize_evidence(out)
        evs = out["findings"][0]["evidence"]
        assert evs[0]["value_raw"] == 12884901888
        assert evs[1]["value_raw"] == 0.0
        assert evs[2]["value_raw"] == "Small"
        assert evs[3]["value_raw"] is None

    def test_walks_appendix_excluded_findings(self):
        out = {
            "appendix_excluded_findings": [
                {
                    "evidence": [
                        {"metric": "x", "value_raw": True, "value_display": "yes"},
                    ],
                }
            ],
        }
        sanitize_evidence(out)
        assert "value_raw" not in out["appendix_excluded_findings"][0]["evidence"][0]

    def test_drops_list_and_dict_value_raw(self):
        out = {
            "findings": [
                {
                    "evidence": [
                        {"metric": "a", "value_raw": [1, 2], "value_display": "[1, 2]"},
                        {"metric": "b", "value_raw": {"k": "v"}, "value_display": "{...}"},
                    ],
                }
            ],
        }
        sanitize_evidence(out)
        for ev in out["findings"][0]["evidence"]:
            assert "value_raw" not in ev


class TestEnrichIntegration:
    def test_pipeline_drops_boolean_value_raw(self):
        """End-to-end: the cross_join_explosion_q1 reproduction."""
        extracted = {
            "findings": [
                {
                    "issue_id": "row_count_explosion",
                    "category": "sql_pattern",
                    "evidence": [
                        {
                            "metric": "cross_join_detected",
                            "value_raw": False,  # ← schema-invalid
                            "value_display": "not detected",
                        }
                    ],
                    "actions": [],
                }
            ],
        }
        out = enrich_llm_canonical(extracted, _analysis())
        ev = out["findings"][0]["evidence"][0]
        assert "value_raw" not in ev
        # Schema-required fields preserved.
        assert ev["metric"] == "cross_join_detected"
        assert ev["value_display"] == "not detected"

    def test_pipeline_idempotent_with_sanitize(self):
        """Phase 5 must not perturb already-sanitised input."""
        extracted = {
            "findings": [
                {
                    "evidence": [
                        {"metric": "x", "value_raw": 42, "value_display": "42"},
                    ],
                }
            ],
        }
        once = enrich_llm_canonical(extracted, _analysis())
        for k in ("report_id", "generated_at"):
            once.pop(k, None)
        twice = enrich_llm_canonical(once, _analysis())
        for k in ("report_id", "generated_at"):
            twice.pop(k, None)
        assert once == twice
