"""Tests for LLM prompt versioning."""

from core.llm_prompts import PROMPT_VERSION


class TestPromptVersion:
    """PROMPT_VERSION is defined and usable."""

    def test_version_is_string(self):
        assert isinstance(PROMPT_VERSION, str)

    def test_version_not_empty(self):
        assert len(PROMPT_VERSION) > 0

    def test_version_format(self):
        """Version should be semver-like or descriptive."""
        assert PROMPT_VERSION.startswith("v")


class TestAnalysisContextPromptVersion:
    """AnalysisContext carries prompt_version."""

    def test_default_prompt_version(self):
        from core.models import AnalysisContext

        ctx = AnalysisContext()
        assert ctx.prompt_version == ""

    def test_set_prompt_version(self):
        from core.models import AnalysisContext

        ctx = AnalysisContext(prompt_version=PROMPT_VERSION)
        assert ctx.prompt_version == PROMPT_VERSION
