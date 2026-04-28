"""Tests for ``core.v6_schema.alias_telemetry``.

The module is shared between the production app pipeline (where it
records normalizer alias hits per analysis) and the eval ab_runner
(where it aggregates across cases to compute alias_hit_rate). Lock the
contract in one place since both consumers depend on it.
"""

from __future__ import annotations

import pytest

from core.v6_schema.alias_telemetry import AliasHitCounts, aggregate
from core.v6_schema.normalizer import (
    _normalize_finding_category,
    _normalize_fix_type,
    _normalize_issue_id,
    enrich_llm_canonical,
)


class TestAliasHitCounts:
    def test_empty_tracker_total_is_zero(self):
        t = AliasHitCounts()
        assert t.total == 0
        assert t.to_dict() == {"fix_type": 0, "category": 0, "issue_id": 0, "total": 0}

    def test_record_increments_correct_field(self):
        t = AliasHitCounts()
        t.record("fix_type")
        t.record("fix_type")
        t.record("category")
        t.record("issue_id")
        assert t.fix_type == 2
        assert t.category == 1
        assert t.issue_id == 1
        assert t.total == 4

    def test_unknown_kind_is_silent_noop(self):
        """Adding a fourth alias map later must not crash old callers."""
        t = AliasHitCounts()
        t.record("not_a_real_alias_kind")
        assert t.total == 0


class TestNormalizerHitTracking:
    def test_fix_type_alias_records_hit(self):
        t = AliasHitCounts()
        result = _normalize_fix_type("sql_rewrite", t)
        assert result == "rewrite"
        assert t.fix_type == 1

    def test_fix_type_passthrough_does_not_record(self):
        t = AliasHitCounts()
        # ``rewrite`` is already canonical
        result = _normalize_fix_type("rewrite", t)
        assert result == "rewrite"
        assert t.fix_type == 0

    def test_category_alias_records_hit(self):
        t = AliasHitCounts()
        result = _normalize_finding_category("cluster", t)
        assert result == "clustering"
        assert t.category == 1

    def test_issue_id_alias_records_hit(self):
        t = AliasHitCounts()
        result = _normalize_issue_id("disk_spill_dominant", t)
        assert result == "spill_dominant"
        assert t.issue_id == 1

    def test_no_tracker_means_no_tracking(self):
        """Calling normalize with tracker=None (the default) must still
        normalize correctly — telemetry is opt-in."""
        assert _normalize_fix_type("sql_rewrite") == "rewrite"
        assert _normalize_finding_category("cluster") == "clustering"
        assert _normalize_issue_id("disk_spill_dominant") == "spill_dominant"


class TestEnrichIntegration:
    def _analysis(self):
        from core.models import BottleneckIndicators, ProfileAnalysis, QueryMetrics

        return ProfileAnalysis(
            query_metrics=QueryMetrics(query_id="q-1"),
            bottleneck_indicators=BottleneckIndicators(),
        )

    def test_enrich_records_all_three_alias_types(self):
        tracker = AliasHitCounts()
        extracted = {
            "findings": [
                {
                    "issue_id": "disk_spill_dominant",   # → spill_dominant
                    "category": "cluster",                # → clustering
                    "actions": [
                        {
                            "fix_type": "sql_rewrite",   # → rewrite
                            "verification": [],
                        }
                    ],
                }
            ],
        }
        out = enrich_llm_canonical(
            extracted, self._analysis(), alias_tracker=tracker
        )
        assert out["findings"][0]["issue_id"] == "spill_dominant"
        assert out["findings"][0]["category"] == "clustering"
        assert out["findings"][0]["actions"][0]["fix_type"] == "rewrite"
        assert tracker.to_dict() == {
            "fix_type": 1,
            "category": 1,
            "issue_id": 1,
            "total": 3,
        }

    def test_enrich_without_tracker_is_backward_compatible(self):
        """Existing callers (tests, code that doesn't care about
        telemetry) must keep working without any changes."""
        extracted = {
            "findings": [
                {"issue_id": "disk_spill_dominant", "category": "cluster", "actions": []}
            ],
        }
        out = enrich_llm_canonical(extracted, self._analysis())
        assert out["findings"][0]["issue_id"] == "spill_dominant"


class TestAggregate:
    def test_empty_list_returns_zero_baseline(self):
        result = aggregate([])
        assert result["cases"] == 0
        assert result["alias_hit_rate"] == 0.0
        assert result["hits_total"] == 0

    def test_aggregates_per_case_counts(self):
        trackers = [
            AliasHitCounts(fix_type=2, category=0, issue_id=1),
            AliasHitCounts(),  # no hits
            AliasHitCounts(fix_type=0, category=3, issue_id=0),
        ]
        agg = aggregate(trackers)
        assert agg["fix_type_total"] == 2
        assert agg["category_total"] == 3
        assert agg["issue_id_total"] == 1
        assert agg["hits_total"] == 6
        assert agg["cases"] == 3
        assert agg["cases_with_any_hit"] == 2
        # 2/3 cases had at least one alias hit
        assert agg["alias_hit_rate"] == pytest.approx(0.6667, rel=1e-3)
        # 6 hits over 3 cases
        assert agg["hits_per_case_avg"] == pytest.approx(2.0)

    def test_alias_hit_rate_zero_when_no_hits(self):
        trackers = [AliasHitCounts(), AliasHitCounts(), AliasHitCounts()]
        agg = aggregate(trackers)
        assert agg["alias_hit_rate"] == 0.0
        assert agg["hits_total"] == 0
