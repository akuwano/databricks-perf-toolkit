"""Tests for core.llm_prompts.spark_perf_prompts — response parsing and JSON extraction."""

import json

from core.llm_prompts.spark_perf_prompts import (
    _extract_json_from_text,
    parse_spark_perf_response,
)


class TestExtractJsonFromText:
    """Tests for _extract_json_from_text brace-depth parser."""

    def test_simple_json(self):
        text = '{"summary_text": "hello", "top3_text": ""}'
        result = _extract_json_from_text(text)
        assert result is not None
        assert result["summary_text"] == "hello"

    def test_json_with_code_fence(self):
        text = '```json\n{"summary_text": "hello"}\n```'
        result = _extract_json_from_text(text)
        assert result is not None
        assert result["summary_text"] == "hello"

    def test_json_with_trailing_markdown(self):
        text = '{"summary_text": "# 1. Summary\\nContent"}\n\n---\n\n# Appendix\n## A. Photon'
        result = _extract_json_from_text(text)
        assert result is not None
        assert "Summary" in result["summary_text"]

    def test_json_with_nested_braces_in_string(self):
        text = '{"summary_text": "code: {a: 1, b: 2}", "top3_text": ""}'
        result = _extract_json_from_text(text)
        assert result is not None
        assert "{a: 1, b: 2}" in result["summary_text"]

    def test_json_with_escaped_quotes(self):
        text = '{"summary_text": "He said \\"hello\\"", "top3_text": ""}'
        result = _extract_json_from_text(text)
        assert result is not None
        assert "hello" in result["summary_text"]

    def test_no_json(self):
        result = _extract_json_from_text("Just plain markdown text")
        assert result is None

    def test_preamble_before_json(self):
        text = 'Here is the result:\n{"summary_text": "content"}'
        result = _extract_json_from_text(text)
        assert result is not None
        assert result["summary_text"] == "content"

    def test_invalid_json(self):
        text = '{"summary_text": broken}'
        result = _extract_json_from_text(text)
        assert result is None

    def test_empty_string(self):
        assert _extract_json_from_text("") is None

    def test_json_with_newlines_in_value(self):
        text = '{"summary_text": "line1\\nline2\\nline3", "top3_text": ""}'
        result = _extract_json_from_text(text)
        assert result is not None
        assert "line1" in result["summary_text"]


class TestParseSparkPerfResponse:
    """Tests for parse_spark_perf_response."""

    def test_valid_json(self):
        response = json.dumps(
            {
                "summary_text": "# 1. Summary",
                "job_analysis_text": "",
                "node_analysis_text": "",
                "top3_text": "",
            }
        )
        result = parse_spark_perf_response(response)
        assert result["summary_text"] == "# 1. Summary"

    def test_code_fence_wrapped(self):
        inner = json.dumps(
            {
                "summary_text": "content",
                "job_analysis_text": "",
                "node_analysis_text": "",
                "top3_text": "",
            }
        )
        response = f"```json\n{inner}\n```"
        result = parse_spark_perf_response(response)
        assert result["summary_text"] == "content"

    def test_fallback_brace_depth(self):
        """When standard JSON parse fails, brace-depth extraction should work."""
        response = 'Some preamble\n{"summary_text": "extracted", "job_analysis_text": "", "node_analysis_text": "", "top3_text": ""}\ntrailing'
        result = parse_spark_perf_response(response)
        assert result["summary_text"] == "extracted"

    def test_complete_fallback_to_raw(self):
        """When all parsing fails, raw text goes to summary_text."""
        response = "Just markdown content without any JSON"
        result = parse_spark_perf_response(response)
        assert result["summary_text"] == response
        assert result["top3_text"] == ""

    def test_empty_summary_in_json(self):
        response = json.dumps({"summary_text": "", "top3_text": "actions"})
        result = parse_spark_perf_response(response)
        assert result["summary_text"] == ""
        assert result["top3_text"] == "actions"


class TestAppendixPreambleStrip:
    """Test the regex patterns used in spark_perf_llm.py to strip LLM preamble."""

    @staticmethod
    def _strip_preamble(text: str) -> str:
        """Replicate the preamble stripping logic from spark_perf_llm.py."""
        import re

        appendix = text.strip()
        match = re.search(
            r"^#+ *(?:Appendix|付録|詳細分析)", appendix, re.MULTILINE | re.IGNORECASE
        )
        if not match:
            match = re.search(r"^##+ *A[\.\s]", appendix, re.MULTILINE)
        if match:
            appendix = appendix[match.start() :]
        return appendix

    def test_english_appendix(self):
        text = "Here is the analysis:\n\n# Appendix: Detailed Analysis\n\n## A. Photon"
        result = self._strip_preamble(text)
        assert result.startswith("# Appendix")

    def test_japanese_appendix(self):
        text = "以下は分析結果です。\n\n# 付録: 詳細分析\n\n## A. Photon"
        result = self._strip_preamble(text)
        assert result.startswith("# 付録")

    def test_detailed_analysis_heading(self):
        text = "前置き\n\n# 詳細分析\n\n## A. Photon"
        result = self._strip_preamble(text)
        assert result.startswith("# 詳細分析")

    def test_appendix_with_colon(self):
        text = "Preamble\n\n# Appendix:\n\n## A. Photon"
        result = self._strip_preamble(text)
        assert result.startswith("# Appendix:")

    def test_section_a_fallback(self):
        text = "Preamble\n\n## A. Photon Utilization Analysis\n\nContent here"
        result = self._strip_preamble(text)
        assert result.startswith("## A. Photon")

    def test_no_appendix_heading_returns_as_is(self):
        text = "## B. Some other section\n\nContent"
        result = self._strip_preamble(text)
        assert result == text.strip()

    def test_case_insensitive(self):
        text = "intro\n\n# APPENDIX\n\ncontent"
        result = self._strip_preamble(text)
        assert result.startswith("# APPENDIX")


class TestCall2DataSections:
    """Tests for _build_call2_data_sections — SQL Plan Top 5 integration."""

    def test_sql_plan_top5_included(self):
        from core.llm_prompts.spark_perf_prompts import _build_call2_data_sections

        fact_pack = {
            "app_summary": {"app_id": "app-1", "duration_min": 10},
            "sql_plan_top5": [
                {"execution_id": 1, "duration_sec": 100.0, "photon_pct": 80.0, "smj_count": 1},
                {"execution_id": 2, "duration_sec": 50.0, "photon_pct": 95.0, "smj_count": 0},
            ],
        }
        result = _build_call2_data_sections(fact_pack)
        assert "SQL Plan Analysis (Top 5 by Duration)" in result
        assert '"execution_id": 1' in result
        assert '"smj_count": 1' in result

    def test_sql_plan_top5_omitted_when_empty(self):
        from core.llm_prompts.spark_perf_prompts import _build_call2_data_sections

        fact_pack = {
            "app_summary": {"app_id": "app-1"},
            "sql_plan_top5": [],
        }
        result = _build_call2_data_sections(fact_pack)
        assert "SQL Plan Analysis" not in result

    def test_sql_plan_top5_omitted_when_missing(self):
        from core.llm_prompts.spark_perf_prompts import _build_call2_data_sections

        fact_pack = {"app_summary": {"app_id": "app-1"}}
        result = _build_call2_data_sections(fact_pack)
        assert "SQL Plan Analysis" not in result


class TestDBUEstimateInPrompts:
    """Tests for DBU cost estimate integration in LLM prompts."""

    def test_dbu_estimate_in_call1(self):
        from core.llm_prompts.spark_perf_prompts import create_spark_perf_analysis_prompt

        fact_pack = {
            "app_summary": {"app_id": "app-1", "duration_min": 60},
            "dbu_estimate": {
                "estimated_total_dbu": 12.5,
                "estimated_dbu_per_hour": 12.5,
                "driver_dbu": 0.28,
                "worker_dbu": 12.22,
                "photon_multiplier": 1.0,
                "pricing_method": "lookup",
                "pricing_note": "Jobs Compute, i3.xlarge (4 vCPU), 0.28 DBU/hr/node",
            },
        }
        result = create_spark_perf_analysis_prompt(fact_pack, lang="en")
        assert "DBU Cost Estimate" in result
        assert "12.5" in result

    def test_dbu_estimate_omitted_when_zero(self):
        from core.llm_prompts.spark_perf_prompts import create_spark_perf_analysis_prompt

        fact_pack = {
            "app_summary": {"app_id": "app-1"},
            "dbu_estimate": {"estimated_total_dbu": 0},
        }
        result = create_spark_perf_analysis_prompt(fact_pack, lang="en")
        assert "DBU Cost Estimate" not in result
