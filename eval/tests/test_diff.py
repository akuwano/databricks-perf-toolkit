"""Tests for diff judge, runner, and report."""

from eval.diff_judge import DiffVerdict, DiffReport, _format_cards, _parse_response
from eval.diff_report import diff_to_console, diff_to_json
from eval.diff_runner import _report_from_json, _numeric_verdict
from eval.models import CardEvalResult, L1Score, L2Score, QueryEvalResult, EvalReport


class TestParseResponse:
    def test_valid_json(self):
        content = '{"diagnosis_delta": 4, "evidence_delta": 3, "fix_delta": 5, "coverage_delta": 3, "overall_verdict": "improved", "reasoning": "better fixes"}'
        result = _parse_response(content)
        assert result["overall_verdict"] == "improved"
        assert result["diagnosis_delta"] == 4

    def test_markdown_code_block(self):
        content = '```json\n{"diagnosis_delta": 2, "evidence_delta": 2, "fix_delta": 2, "coverage_delta": 2, "overall_verdict": "regressed", "reasoning": "worse"}\n```'
        result = _parse_response(content)
        assert result["overall_verdict"] == "regressed"

    def test_clamps_scores(self):
        content = '{"diagnosis_delta": 10, "evidence_delta": -1, "fix_delta": 3, "coverage_delta": 3, "overall_verdict": "improved", "reasoning": "ok"}'
        result = _parse_response(content)
        assert result["diagnosis_delta"] == 5
        assert result["evidence_delta"] == 1

    def test_invalid_verdict_defaults(self):
        content = '{"overall_verdict": "maybe", "reasoning": "unsure"}'
        result = _parse_response(content)
        assert result["overall_verdict"] == "unchanged"

    def test_invalid_json(self):
        result = _parse_response("not json at all")
        assert "Parse error" in result.get("reasoning", "")


class TestFormatCards:
    def test_empty_cards(self):
        assert _format_cards([]) == "(no cards)"

    def test_basic_card(self):
        card = CardEvalResult(
            card_index=0,
            problem="Spill detected",
            expected_impact="high",
            effort="low",
            l1=L1Score(0, True, True),
            l2=L2Score(0, 2, 2, grounding_ratio=1.0),
        )
        result = _format_cards([card])
        assert "Spill detected" in result
        assert "Impact: high" in result


class TestNumericVerdict:
    def _make_qr(self, l1=1.0, l2=1.0):
        return QueryEvalResult(
            profile_path="test.json",
            l1_syntax_pass_rate=l1,
            l2_avg_grounding=l2,
            num_action_cards=1,
        )

    def test_improved(self):
        baseline = self._make_qr(l1=0.5, l2=0.5)
        current = self._make_qr(l1=1.0, l2=1.0)
        v = _numeric_verdict(baseline, current)
        assert v.verdict == "improved"

    def test_regressed(self):
        baseline = self._make_qr(l1=1.0, l2=1.0)
        current = self._make_qr(l1=0.3, l2=0.3)
        v = _numeric_verdict(baseline, current)
        assert v.verdict == "regressed"

    def test_unchanged(self):
        baseline = self._make_qr(l1=0.8, l2=0.8)
        current = self._make_qr(l1=0.82, l2=0.78)
        v = _numeric_verdict(baseline, current)
        assert v.verdict == "unchanged"


class TestReportFromJson:
    def test_round_trip(self):
        from eval.report import to_json
        report = EvalReport(
            timestamp="2026-04-08T00:00:00Z",
            num_queries=1,
            query_results=[
                QueryEvalResult(
                    query_id="q1",
                    profile_path="test.json",
                    num_action_cards=1,
                    l1_syntax_pass_rate=1.0,
                    l2_avg_grounding=0.8,
                ),
            ],
            overall_l1_syntax=1.0,
            overall_l2_grounding=0.8,
            config={"primary_model": "test"},
        )
        json_str = to_json(report)
        restored = _report_from_json(json_str)
        assert restored.num_queries == 1
        assert restored.overall_l1_syntax == 1.0
        assert restored.query_results[0].query_id == "q1"

    def test_invalid_json(self):
        report = _report_from_json("not valid json")
        assert "error" in report.config


class TestDiffToConsole:
    def test_basic_output(self):
        report = DiffReport(
            git_ref="v4.25.0",
            num_profiles=2,
            verdicts=[
                DiffVerdict(profile_path="a.json", verdict="improved", reasoning="better", baseline_card_count=3, current_card_count=4),
                DiffVerdict(profile_path="b.json", verdict="regressed", reasoning="worse", baseline_card_count=2, current_card_count=1),
            ],
            summary="1 improved, 1 regressed",
        )
        output = diff_to_console(report)
        assert "v4.25.0" in output
        assert "IMPROVED" in output
        assert "REGRESSED" in output
        assert "1 improved, 1 regressed" in output


class TestDiffToJson:
    def test_serializes(self):
        report = DiffReport(git_ref="v1", num_profiles=0, verdicts=[], summary="")
        import json
        data = json.loads(diff_to_json(report))
        assert data["git_ref"] == "v1"
