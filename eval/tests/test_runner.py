"""Tests for eval runner."""

import json
from unittest.mock import patch

from core.models import ActionCard, ProfileAnalysis, QueryMetrics
from core.usecases import LLMConfig, PipelineOptions, PipelineResult
from eval.runner import evaluate_profile


class TestEvaluateProfile:
    def test_missing_file(self, tmp_path):
        result = evaluate_profile(
            str(tmp_path / "nonexistent.json"),
            LLMConfig(),
            PipelineOptions(skip_llm=True),
            skip_judge=True,
        )
        assert "Failed to load" in result.pipeline_error

    @patch("eval.runner.run_analysis_pipeline")
    def test_minimal_profile(self, mock_pipeline, tmp_path):
        profile = tmp_path / "test.json"
        profile.write_text(json.dumps({
            "query": {
                "id": "q-test-001",
                "queryText": "SELECT 1",
                "metrics": {"totalTimeMs": 100},
            },
            "graphs": [],
        }))

        mock_pipeline.return_value = PipelineResult(
            analysis=ProfileAnalysis(
                query_metrics=QueryMetrics(query_id="q-test-001", query_text="SELECT 1"),
                action_cards=[
                    ActionCard(
                        problem="Test issue",
                        evidence=["totalTimeMs is high"],
                        fix_sql="SELECT 1",
                        expected_impact="low",
                        effort="low",
                    ),
                ],
            ),
            llm_analysis="Test LLM output",
            llm_enabled=True,
        )

        result = evaluate_profile(
            str(profile),
            LLMConfig(),
            PipelineOptions(skip_llm=True),
            skip_judge=True,
        )

        assert result.query_id == "q-test-001"
        assert result.pipeline_error == ""
        assert result.num_action_cards == 1
        assert result.l1_syntax_pass_rate == 1.0
        assert len(result.card_results) == 1
        assert result.card_results[0].l3 is None  # Judge skipped

    @patch("eval.runner.run_analysis_pipeline")
    def test_pipeline_failure(self, mock_pipeline, tmp_path):
        profile = tmp_path / "test.json"
        profile.write_text('{"query": {"id": "q1", "metrics": {}}, "graphs": []}')

        mock_pipeline.side_effect = RuntimeError("Boom")

        result = evaluate_profile(
            str(profile),
            LLMConfig(),
            PipelineOptions(skip_llm=True),
            skip_judge=True,
        )
        assert "Pipeline failed" in result.pipeline_error
