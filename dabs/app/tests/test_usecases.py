"""Tests for core.usecases pipeline orchestrator."""

import pytest
from core.usecases import (
    LLMConfig,
    PipelineOptions,
    PipelineResult,
    run_analysis_pipeline,
)


@pytest.fixture
def sample_profile_data():
    """Minimal valid profile data for pipeline tests."""
    return {
        "query": {
            "id": "test-query-001",
            "status": "FINISHED",
            "queryText": "SELECT 1",
            "metrics": {},
        },
        "graphs": [],
    }


class TestLLMConfig:
    def test_default_models(self):
        cfg = LLMConfig()
        assert cfg.primary_model == "databricks-claude-opus-4-7"
        assert cfg.review_model == "databricks-claude-opus-4-7"
        assert cfg.refine_model == "databricks-gpt-5-5"

    def test_refine_model_explicit(self):
        cfg = LLMConfig(refine_model="model-b")
        assert cfg.refine_model == "model-b"

    def test_is_available_true(self):
        cfg = LLMConfig(databricks_host="https://host", databricks_token="tok")
        assert cfg.is_available is True

    def test_is_available_false_no_host(self):
        cfg = LLMConfig(databricks_host="", databricks_token="tok")
        assert cfg.is_available is False

    def test_is_available_false_no_token(self):
        cfg = LLMConfig(databricks_host="https://host", databricks_token="")
        assert cfg.is_available is False


class TestPipelineOptions:
    def test_defaults(self):
        opts = PipelineOptions()
        assert opts.skip_llm is False
        assert opts.skip_review is False
        assert opts.skip_refine is False
        assert opts.enable_report_review is False
        assert opts.enable_report_refine is False
        assert opts.verbose is False
        assert opts.explain_text is None
        assert opts.lang == "en"


class TestRunAnalysisPipeline:
    def test_metrics_only_no_llm(self, sample_profile_data):
        """Pipeline with skip_llm should produce a report without LLM calls."""
        llm_config = LLMConfig()
        options = PipelineOptions(skip_llm=True)

        result = run_analysis_pipeline(sample_profile_data, llm_config, options)

        assert isinstance(result, PipelineResult)
        assert result.report  # non-empty report
        assert result.llm_enabled is False
        assert result.llm_analysis == ""
        assert result.analysis is not None

    def test_stage_callback_called(self, sample_profile_data):
        """on_stage callback should be invoked for each pipeline stage."""
        stages = []
        llm_config = LLMConfig()
        options = PipelineOptions(skip_llm=True)

        run_analysis_pipeline(
            sample_profile_data,
            llm_config,
            options,
            on_stage=lambda s: stages.append(s),
        )

        assert "metrics" in stages
        assert "report" in stages
        assert "done" in stages
        # LLM stages should not appear when skip_llm=True
        assert "llm_initial" not in stages

    def test_llm_not_available_skips_gracefully(self, sample_profile_data):
        """Pipeline should skip LLM when credentials are empty."""
        llm_config = LLMConfig(databricks_host="", databricks_token="")
        options = PipelineOptions(skip_llm=False)

        result = run_analysis_pipeline(sample_profile_data, llm_config, options)

        assert result.llm_enabled is False
        assert result.report  # still produces a report
