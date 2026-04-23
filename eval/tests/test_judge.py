"""Tests for L3/L4 LLM-as-judge scorer."""

from unittest.mock import MagicMock, patch

from core.models import ActionCard
from eval.scorers.l3l4_judge import (
    _parse_judge_response,
    build_profile_summary,
    score_l3l4,
)
from core.models import BottleneckIndicators, QueryMetrics


class TestParseJudgeResponse:
    def test_valid_json(self):
        content = '{"diagnosis_score": 4, "evidence_quality": 3, "fix_relevance": 5, "fix_feasibility": 4, "expected_improvement": 3, "reasoning": "Good"}'
        result = _parse_judge_response(content)
        assert result["diagnosis_score"] == 4
        assert result["fix_relevance"] == 5
        assert result["reasoning"] == "Good"

    def test_json_in_code_block(self):
        content = '```json\n{"diagnosis_score": 4, "evidence_quality": 3, "fix_relevance": 5, "fix_feasibility": 4, "expected_improvement": 3, "reasoning": "OK"}\n```'
        result = _parse_judge_response(content)
        assert result["diagnosis_score"] == 4

    def test_score_clamping(self):
        content = '{"diagnosis_score": 10, "evidence_quality": 0, "fix_relevance": 5, "fix_feasibility": 4, "expected_improvement": -1, "reasoning": "test"}'
        result = _parse_judge_response(content)
        assert result["diagnosis_score"] == 5  # Clamped to max
        assert result["evidence_quality"] == 1  # Clamped to min
        assert result["expected_improvement"] == 1  # Clamped to min

    def test_invalid_json(self):
        result = _parse_judge_response("not json at all")
        assert "reasoning" in result
        assert "Parse error" in result["reasoning"]


class TestBuildProfileSummary:
    def test_basic_summary(self):
        qm = QueryMetrics(
            total_time_ms=10000,
            execution_time_ms=9000,
            spill_to_disk_bytes=5 * 1024**3,
            read_bytes=10 * 1024**3,
        )
        bi = BottleneckIndicators(
            cache_hit_ratio=0.5,
            photon_ratio=0.8,
        )
        summary = build_profile_summary(qm, bi)
        assert "10000ms" in summary
        assert "Cache hit ratio: 50.0%" in summary
        assert "Photon utilization: 80.0%" in summary

    def test_empty_metrics(self):
        summary = build_profile_summary(QueryMetrics(), BottleneckIndicators())
        assert summary == "No metrics available"


class TestScoreL3L4:
    @patch("eval.scorers.l3l4_judge.create_openai_client")
    def test_successful_judge(self, mock_create):
        mock_client = MagicMock()
        mock_create.return_value = mock_client

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = (
            '{"diagnosis_score": 4, "evidence_quality": 3, "fix_relevance": 5, '
            '"fix_feasibility": 4, "expected_improvement": 3, "reasoning": "Good recommendation"}'
        )
        mock_client.chat.completions.create.return_value = mock_response

        card = ActionCard(
            problem="High spill",
            evidence=["Spill: 5GB"],
            fix="Add REPARTITION hint",
            fix_sql="SELECT /*+ REPARTITION(10) */ * FROM t",
        )

        l3, l4 = score_l3l4(card, "Total time: 10000ms", "SELECT * FROM t", "host", "token")
        assert l3.diagnosis_score == 4
        assert l3.evidence_quality == 3
        assert l4.fix_relevance == 5
        assert l4.fix_feasibility == 4
        assert "Good recommendation" in l3.reasoning

    @patch("eval.scorers.l3l4_judge.create_openai_client")
    def test_judge_failure_returns_zeros(self, mock_create):
        mock_create.side_effect = Exception("Connection failed")

        card = ActionCard(problem="Test")
        l3, l4 = score_l3l4(card, "", "", "host", "token")
        assert l3.diagnosis_score == 0
        assert l4.fix_relevance == 0
        assert "Judge error" in l3.reasoning
