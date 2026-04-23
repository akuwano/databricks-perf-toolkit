"""Tests for core.knowledge module."""

from core.knowledge import KnowledgeService
from core.models import (
    ActionCard,
    AnalysisContext,
    ComparisonResult,
    OperatorHotspot,
    ProfileAnalysis,
    QueryMetrics,
)


class TestKnowledgeService:
    def setup_method(self):
        self.service = KnowledgeService()

    def test_build_from_analysis_with_action_cards(self):
        analysis = ProfileAnalysis(
            query_metrics=QueryMetrics(query_text="SELECT 1"),
            action_cards=[
                ActionCard(
                    problem="High spill detected",
                    likely_cause="Insufficient memory",
                    fix="Add BROADCAST hint",
                    fix_sql="SELECT /*+ BROADCAST(t) */ ...",
                    expected_impact="high",
                ),
            ],
            hot_operators=[
                OperatorHotspot(bottleneck_type="spill"),
            ],
            analysis_context=AnalysisContext(
                query_fingerprint="fp123",
                experiment_id="exp-1",
                variant="baseline",
                tags={"env": "prod"},
            ),
        )

        doc = self.service.build_from_analysis(analysis)
        assert doc.document_id  # non-empty UUID
        assert doc.knowledge_type == "recommendation"
        assert doc.source_type == "analysis"
        assert doc.title == "High spill detected"
        assert doc.problem_category == "spill"
        assert doc.query_fingerprint == "fp123"
        assert doc.status == "active"
        assert doc.tags == {"env": "prod"}

    def test_build_from_analysis_without_action_cards(self):
        analysis = ProfileAnalysis(
            analysis_context=AnalysisContext(query_fingerprint="fp456"),
        )
        doc = self.service.build_from_analysis(analysis)
        assert doc.title == "Profiler analysis finding"
        assert doc.problem_category == ""

    def test_build_from_comparison_regression(self):
        comparison = ComparisonResult(
            comparison_id="cmp-1",
            query_fingerprint="fp123",
            experiment_id="exp-1",
            baseline_variant="before",
            candidate_variant="after",
            regression_detected=True,
            summary="Regressed: total_time_ms",
        )
        doc = self.service.build_from_comparison(comparison)
        assert doc.knowledge_type == "regression_case"
        assert doc.source_comparison_id == "cmp-1"
        assert doc.expected_impact == "high"

    def test_build_from_comparison_no_regression(self):
        comparison = ComparisonResult(
            comparison_id="cmp-2",
            baseline_variant="before",
            candidate_variant="after",
            regression_detected=False,
            summary="No significant changes",
        )
        doc = self.service.build_from_comparison(comparison)
        assert doc.knowledge_type == "tuning_pattern"
        assert doc.expected_impact == "medium"

    def test_build_tags(self):
        tags = self.service.build_tags("doc-1", {"env": "prod", "team": "data"})
        assert len(tags) == 2
        assert tags[0].document_id == "doc-1"
        tag_names = {t.tag_name for t in tags}
        assert tag_names == {"env", "team"}

    def test_build_tags_empty(self):
        tags = self.service.build_tags("doc-1", {})
        assert tags == []
