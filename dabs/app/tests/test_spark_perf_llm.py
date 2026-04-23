"""Tests for core.spark_perf_llm — output validation logic."""

from core.spark_perf_llm import _CALL1_REQUIRED_PATTERNS, _CALL2_REQUIRED_PATTERNS, _validate_output


class TestValidateOutput:
    """Tests for _validate_output section checker."""

    def test_all_sections_present_ja(self):
        text = """# Sparkパフォーマンスレポート
## エグゼクティブサマリー
...
# 1. ボトルネック分析サマリー
## ボトルネック評価
| # | アラート | 値 | 閾値 | Impact | Effort | Priority |
# 2. 推奨アクション
### 1. Resolve Disk Spill
"""
        missing = _validate_output(text, _CALL1_REQUIRED_PATTERNS)
        assert missing == []

    def test_all_sections_present_en(self):
        text = """# Spark Performance Report
## Executive Summary
...
# 1. Bottleneck Analysis Summary
## Bottleneck Evaluation
| # | Alert | Value | Threshold | Impact | Effort | Priority |
# 2. Recommended Actions
"""
        missing = _validate_output(text, _CALL1_REQUIRED_PATTERNS)
        assert missing == []

    def test_missing_section_2(self):
        text = """# 1. ボトルネック分析サマリー
## ボトルネック評価
| Impact | Effort | Priority |
"""
        missing = _validate_output(text, _CALL1_REQUIRED_PATTERNS)
        assert "section_2" in missing

    def test_missing_bottleneck_eval(self):
        text = """# 1. ボトルネック分析サマリー
# 2. 推奨アクション
"""
        missing = _validate_output(text, _CALL1_REQUIRED_PATTERNS)
        assert "bottleneck_eval" in missing

    def test_empty_text(self):
        missing = _validate_output("", _CALL1_REQUIRED_PATTERNS)
        assert len(missing) == len(_CALL1_REQUIRED_PATTERNS)

    def test_call2_appendix_present(self):
        text = """# Appendix: 詳細分析
## A. Photon 利用状況分析
"""
        missing = _validate_output(text, _CALL2_REQUIRED_PATTERNS)
        assert missing == []

    def test_call2_missing_appendix(self):
        text = """## A. Photon 利用状況分析
"""
        missing = _validate_output(text, _CALL2_REQUIRED_PATTERNS)
        # "appendix" heading missing but section_a present
        assert "appendix" in missing
        assert "section_a" not in missing
