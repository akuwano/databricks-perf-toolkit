"""Knowledge service for building and managing knowledge documents.

Generates structured knowledge entries from analysis results and
comparison outcomes, enabling team-wide knowledge sharing and
pattern-based recommendations.
"""

from __future__ import annotations

import uuid

from .models import (
    ComparisonResult,
    KnowledgeDocument,
    KnowledgeTag,
    ProfileAnalysis,
)


class KnowledgeService:
    """Builds knowledge documents from analyses and comparisons."""

    def build_from_analysis(self, analysis: ProfileAnalysis) -> KnowledgeDocument:
        """Create a knowledge document from a single analysis."""
        top_action = analysis.action_cards[0] if analysis.action_cards else None
        ctx = analysis.analysis_context

        return KnowledgeDocument(
            document_id=str(uuid.uuid4()),
            knowledge_type="recommendation",
            source_type="analysis",
            query_fingerprint=ctx.query_fingerprint,
            experiment_id=ctx.experiment_id,
            variant=ctx.variant,
            title=top_action.problem if top_action else "Profiler analysis finding",
            summary=top_action.likely_cause if top_action else "",
            body_markdown=top_action.fix_sql if top_action and top_action.fix_sql else "",
            problem_category=(
                analysis.hot_operators[0].bottleneck_type if analysis.hot_operators else ""
            ),
            recommendation=top_action.fix if top_action else "",
            expected_impact=top_action.expected_impact if top_action else "",
            confidence_score=0.7,
            applicability_scope="fingerprint",
            status="active",
            tags=ctx.tags,
        )

    def build_from_comparison(self, comparison: ComparisonResult) -> KnowledgeDocument:
        """Create a knowledge document from a comparison result."""
        return KnowledgeDocument(
            document_id=str(uuid.uuid4()),
            knowledge_type=(
                "regression_case" if comparison.regression_detected else "tuning_pattern"
            ),
            source_type="comparison",
            source_comparison_id=comparison.comparison_id,
            query_fingerprint=comparison.query_fingerprint,
            experiment_id=comparison.experiment_id,
            variant=comparison.candidate_variant,
            title=(f"Comparison: {comparison.baseline_variant} vs {comparison.candidate_variant}"),
            summary=comparison.summary,
            body_markdown=comparison.summary,
            problem_category="comparison",
            recommendation="Review regressed metrics and related action cards",
            expected_impact="high" if comparison.regression_detected else "medium",
            confidence_score=0.8,
            applicability_scope="fingerprint",
            status="active",
        )

    def build_tags(self, document_id: str, tags: dict[str, str]) -> list[KnowledgeTag]:
        """Convert a tag dict to a list of KnowledgeTag objects."""
        return [
            KnowledgeTag(document_id=document_id, tag_name=k, tag_value=v) for k, v in tags.items()
        ]
