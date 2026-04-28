"""Tests for structured LLM prompt and section parsing."""

from core.constants import Severity
from core.llm import parse_llm_sections
from core.models import (
    Alert,
    BottleneckIndicators,
    DataFlowEntry,
    ProfileAnalysis,
    QueryMetrics,
)

# --- parse_llm_sections tests ---


class TestParseLlmSections:
    """Tests for parse_llm_sections()."""

    def test_basic_section_split(self):
        text = """## 1. Executive Summary

CRITICAL: OOM failure due to JOIN data explosion.

## 4. Root Cause Analysis

### 4.1 Direct Cause
OOM Kill (Exit Code 137).

### 4.2 Root Cause
JOIN explosion chain.

## 7. Recommendations

### Priority 1: Fix JOIN conditions

## 8. Conclusion

Fix the JOIN.
"""
        sections = parse_llm_sections(text)
        assert "executive_summary" in sections
        assert "root_cause_analysis" in sections
        assert "recommendations" in sections
        assert "conclusion" in sections

    def test_executive_summary_content(self):
        text = """## 1. Executive Summary

CRITICAL: OOM failure due to JOIN data explosion.
The query failed with exit code 137.

## 4. Root Cause Analysis

Details here.
"""
        sections = parse_llm_sections(text)
        assert "CRITICAL" in sections["executive_summary"]
        assert "OOM" in sections["executive_summary"]

    def test_empty_input(self):
        sections = parse_llm_sections("")
        assert sections == {}

    def test_no_matching_sections_collected_as_unmatched(self):
        text = """## Some Random Header

Content here.

## Another Header

More content.
"""
        sections = parse_llm_sections(text)
        assert "_unmatched" in sections
        assert "Some Random Header" in sections["_unmatched"]
        assert "Another Header" in sections["_unmatched"]
        assert "executive_summary" not in sections

    def test_japanese_section_headers(self):
        text = """## 1. エグゼクティブサマリー

CRITICAL: JOINによるデータ爆発でOOM障害が発生。

## 4. 根本原因分析

### 4.1 直接原因
OOM Kill (Exit Code 137)

## 7. 推奨事項

### Priority 1: JOIN条件の見直し

## 8. 結論

JOIN条件を修正してください。
"""
        sections = parse_llm_sections(text)
        assert "executive_summary" in sections
        assert "root_cause_analysis" in sections
        assert "recommendations" in sections
        assert "conclusion" in sections

    def test_section_content_trimmed(self):
        text = """## 1. Executive Summary

  Content with whitespace.

## 8. Conclusion

  Final words.
"""
        sections = parse_llm_sections(text)
        assert sections["executive_summary"] == "Content with whitespace."
        assert sections["conclusion"] == "Final words."

    def test_partial_sections(self):
        """Only some sections present."""
        text = """## 1. Executive Summary

Summary only.
"""
        sections = parse_llm_sections(text)
        assert "executive_summary" in sections
        assert "root_cause_analysis" not in sections

    def test_section_number_flexible(self):
        """Should work with or without section numbers."""
        text = """## Executive Summary

No numbers.

## Root Cause Analysis

Content.

## Recommendations

Recs.

## Conclusion

End.
"""
        sections = parse_llm_sections(text)
        assert "executive_summary" in sections
        assert "root_cause_analysis" in sections
        assert "recommendations" in sections
        assert "conclusion" in sections

    def test_optimized_sql_recognized(self):
        """Optimized SQL section should be parsed as a named section."""
        text = """## Recommendations

Use BROADCAST hint.

## Optimized SQL

```sql
SELECT /*+ BROADCAST(dim) */ * FROM fact JOIN dim ON fact.id = dim.id
```

## Conclusion

Done.
"""
        sections = parse_llm_sections(text)
        assert "optimized_sql" in sections
        assert "BROADCAST" in sections["optimized_sql"]
        assert "recommendations" in sections
        assert "conclusion" in sections

    def test_optimized_sql_japanese(self):
        """Japanese '最適化済みSQL' should also be recognized."""
        text = """## 推奨事項

BROADCASTヒントを使用。

## 最適化済みSQL

```sql
SELECT /*+ BROADCAST(dim) */ * FROM fact
```

## 結論

完了。
"""
        sections = parse_llm_sections(text)
        assert "optimized_sql" in sections
        assert "BROADCAST" in sections["optimized_sql"]
        assert "recommendations" in sections
        assert "conclusion" in sections

    def test_old_prompt_format_preserves_unmatched(self):
        """Old-style LLM output (Summary, I/O Analysis, etc.) should be
        collected as _unmatched, with Recommendations matched."""
        text = """## サマリー

クエリは7時間実行されました。

## I/O分析

リモート読み取り率98%。

## Shuffle分析

シャッフルは問題なし。

## 実行プラン分析

SortMerge JOINが検出されました。

## ボトルネック分析

ディスクスピル25GB。

## 推奨事項

1. Broadcast JOINを使用
2. パーティション追加

## 最適化済みSQL

```sql
SELECT /*+ BROADCAST(small) */ ...
```
"""
        sections = parse_llm_sections(text)
        # "推奨事項" should match recommendations
        assert "recommendations" in sections
        assert "Broadcast JOIN" in sections["recommendations"]
        # "最適化済みSQL" should now match optimized_sql (no longer _unmatched)
        assert "optimized_sql" in sections
        assert "BROADCAST" in sections["optimized_sql"]
        # All other sections should be in _unmatched
        assert "_unmatched" in sections
        assert "サマリー" in sections["_unmatched"]
        assert "I/O分析" in sections["_unmatched"]
        assert "ボトルネック分析" in sections["_unmatched"]

    def test_preamble_before_first_header(self):
        """Content before the first ## header should be in _unmatched."""
        text = """This is a preamble paragraph.

## Executive Summary

The summary.
"""
        sections = parse_llm_sections(text)
        assert "executive_summary" in sections
        assert "_unmatched" in sections
        assert "preamble" in sections["_unmatched"]


# --- Evidence-constrained format tests (#9) ---


class TestEvidenceConstrainedFormat:
    """Tests for the evidence-constrained recommendation format."""

    def test_structured_system_prompt_en_contains_rationale(self):
        prompt = _make_structured_system_prompt("en")
        assert "**Rationale**" in prompt

    def test_structured_system_prompt_en_contains_counter_evidence(self):
        prompt = _make_structured_system_prompt("en")
        assert "Counter-evidence" in prompt

    def test_structured_system_prompt_en_contains_cause_hypothesis(self):
        prompt = _make_structured_system_prompt("en")
        assert "**Cause Hypothesis**" in prompt or "Cause Hypothesis" in prompt

    def test_structured_system_prompt_en_contains_impact_effort(self):
        prompt = _make_structured_system_prompt("en")
        assert "Impact: HIGH" in prompt
        assert "Effort: LOW" in prompt

    def test_structured_system_prompt_en_forbids_priority_output(self):
        """Priority was removed from the human-readable markdown in
        v5.16.3: the Top 5 ordering and Impact/Effort badges already
        express severity, so a separate Priority column/score is
        redundant. The JSON Action Plan schema still keeps a priority
        field for structured consumers, so only the *output format
        instructions* must forbid it."""
        prompt = _make_structured_system_prompt("en")
        before_json = prompt.split("<!-- ACTION_PLAN_JSON -->")[0]
        # No "Priority: X/10" or "Priority | ..." formatting remnants
        assert "Priority:" not in before_json
        assert "| Priority |" not in before_json

    def test_structured_system_prompt_en_contains_hard_rules(self):
        prompt = _make_structured_system_prompt("en")
        assert "HARD RULES" in prompt
        assert "NEVER assert facts not present in the Fact Pack" in prompt

    def test_structured_system_prompt_ja_contains_rationale(self):
        prompt = _make_structured_system_prompt("ja")
        assert "**根拠**" in prompt

    def test_structured_system_prompt_ja_contains_counter_evidence(self):
        prompt = _make_structured_system_prompt("ja")
        assert "**反証**" in prompt or "反証" in prompt

    def test_structured_system_prompt_ja_contains_hard_rules(self):
        prompt = _make_structured_system_prompt("ja")
        assert "厳守ルール" in prompt
        assert "Fact Packに存在しない事実を断定してはならない" in prompt

    def test_system_prompt_en_also_updated(self):
        from core.llm_prompts import create_system_prompt

        prompt = create_system_prompt("some knowledge", lang="en")
        assert "**Rationale**" in prompt
        assert "HARD RULES" in prompt

    def test_system_prompt_ja_also_updated(self):
        from core.llm_prompts import create_system_prompt

        prompt = create_system_prompt("some knowledge", lang="ja")
        assert "**根拠**" in prompt
        assert "厳守ルール" in prompt

    def test_confidence_uses_needs_verification(self):
        prompt = _make_structured_system_prompt("en")
        assert "needs_verification" in prompt

    def test_confidence_ja_uses_needs_verification(self):
        prompt = _make_structured_system_prompt("ja")
        assert "needs_verification" in prompt


def _make_structured_system_prompt(lang: str) -> str:
    """Helper to create a structured system prompt for testing."""
    from core.llm_prompts import create_structured_system_prompt

    return create_structured_system_prompt("test knowledge", lang=lang)


def _make_analysis_with_alerts_and_flow() -> ProfileAnalysis:
    """Helper to create a ProfileAnalysis with alerts and data flow."""
    return ProfileAnalysis(
        query_metrics=QueryMetrics(query_id="test-123", status="FINISHED"),
        bottleneck_indicators=BottleneckIndicators(
            alerts=[
                Alert(
                    severity=Severity.CRITICAL,
                    category="spill",
                    message="Disk spill 8.5GB",
                    metric_name="spill_bytes",
                    current_value="8.5GB",
                    threshold=">5GB",
                ),
                Alert(
                    severity=Severity.HIGH,
                    category="cache",
                    message="Cache hit ratio low",
                    metric_name="cache_hit_ratio",
                    current_value="25%",
                    threshold=">80%",
                    conflicts_with=["spill:spill_bytes"],
                ),
                Alert(
                    severity=Severity.MEDIUM,
                    category="shuffle",
                    message="Shuffle impact moderate",
                    metric_name="shuffle_impact_ratio",
                    current_value="30%",
                    threshold="<20%",
                ),
            ],
        ),
        data_flow=[
            DataFlowEntry(
                node_id="1",
                operation="Scan (orders)",
                output_rows=1000000,
                duration_ms=5000,
            ),
            DataFlowEntry(
                node_id="2",
                operation="Inner Join",
                output_rows=500000,
                duration_ms=3000,
            ),
            DataFlowEntry(
                node_id="3",
                operation="HashAggregate",
                output_rows=100,
                duration_ms=2000,
            ),
        ],
    )


# --- Fact Pack Summary tests (#10) ---


class TestFactPackSummary:
    """Tests for the structured Fact Pack Summary block."""

    def test_summary_block_appears_before_query_info(self):
        from core.llm_prompts import create_structured_analysis_prompt

        analysis = _make_analysis_with_alerts_and_flow()
        prompt = create_structured_analysis_prompt(analysis, lang="en")
        summary_pos = prompt.index("## Fact Pack Summary")
        query_pos = prompt.index("## Query Information")
        assert summary_pos < query_pos

    def test_top_alerts_limited_to_5(self):
        from core.llm_prompts.prompts import _build_fact_pack_summary

        analysis = _make_analysis_with_alerts_and_flow()
        # Add more alerts to exceed 5
        for i in range(10):
            analysis.bottleneck_indicators.alerts.append(
                Alert(
                    severity=Severity.INFO,
                    category="io",
                    message=f"Info alert {i}",
                    metric_name=f"metric_{i}",
                    current_value="0",
                    threshold="0",
                )
            )
        summary = _build_fact_pack_summary(analysis, "en")
        # Count alert lines (lines starting with "  - [")
        alert_lines = [line for line in summary.split("\n") if line.strip().startswith("- [")]
        assert len(alert_lines) <= 5

    def test_dominant_operations_limited_to_3(self):
        from core.llm_prompts.prompts import _build_fact_pack_summary

        analysis = _make_analysis_with_alerts_and_flow()
        summary = _build_fact_pack_summary(analysis, "en")
        assert "dominant_operations:" in summary
        op_lines = [line for line in summary.split("\n") if "% of total duration" in line]
        assert len(op_lines) <= 3

    def test_alert_contradictions_listed(self):
        from core.llm_prompts.prompts import _build_fact_pack_summary

        analysis = _make_analysis_with_alerts_and_flow()
        summary = _build_fact_pack_summary(analysis, "en")
        assert "alert_contradictions:" in summary
        assert "Disk spill" in summary or "Cache hit" in summary

    def test_minimal_summary_when_empty(self):
        from core.llm_prompts.prompts import _build_fact_pack_summary

        analysis = ProfileAnalysis(
            query_metrics=QueryMetrics(),
            bottleneck_indicators=BottleneckIndicators(),
        )
        summary = _build_fact_pack_summary(analysis, "en")
        # Even with no alerts/flow, sql_context is always present
        assert "sql_context:" in summary
        assert "sql_provided: false" in summary

    def test_summary_ja_header(self):
        from core.llm_prompts.prompts import _build_fact_pack_summary

        analysis = _make_analysis_with_alerts_and_flow()
        summary = _build_fact_pack_summary(analysis, "ja")
        assert "## ファクトパック概要" in summary

    def test_review_prompt_contains_fact_pack_summary(self):
        from core.llm_prompts import create_review_prompt

        analysis = _make_analysis_with_alerts_and_flow()
        prompt = create_review_prompt(analysis, "test report", "model-a", lang="en")
        assert "Fact Pack Summary" in prompt

    def test_lakehouse_federation_block_emitted(self):
        """v6.6.8: when ``is_federation_query`` is set, surface the
        detection in the Fact Pack so the LLM treats it as ground truth
        instead of hedging ('possibly via Lakehouse Federation')."""
        from core.llm_prompts.prompts import _build_fact_pack_summary

        analysis = _make_analysis_with_alerts_and_flow()
        analysis.query_metrics.is_federation_query = True
        analysis.query_metrics.federation_source_type = "bigquery"
        analysis.query_metrics.federation_tables = [
            "bq_prod.example.users",
        ]
        summary = _build_fact_pack_summary(analysis, "en")
        assert "lakehouse_federation:" in summary
        assert "is_federation_query: true" in summary
        assert "source_type: bigquery" in summary
        assert "bq_prod.example.users" in summary
        assert "ROW_DATA_SOURCE_SCAN_EXEC" in summary

    def test_lakehouse_federation_block_absent_when_not_federation(self):
        from core.llm_prompts.prompts import _build_fact_pack_summary

        analysis = _make_analysis_with_alerts_and_flow()
        # default is_federation_query=False
        summary = _build_fact_pack_summary(analysis, "en")
        assert "lakehouse_federation:" not in summary

    def test_lakehouse_federation_unknown_source_type(self):
        """When the catalog name doesn't carry a source hint, source_type
        falls back to 'unknown' rather than dropping the block."""
        from core.llm_prompts.prompts import _build_fact_pack_summary

        analysis = _make_analysis_with_alerts_and_flow()
        analysis.query_metrics.is_federation_query = True
        analysis.query_metrics.federation_source_type = ""
        analysis.query_metrics.federation_tables = ["main.x.y"]
        summary = _build_fact_pack_summary(analysis, "en")
        assert "lakehouse_federation:" in summary
        assert "source_type: unknown" in summary


# --- Confidence Criteria tests (#11) ---


class TestConfidenceCriteria:
    """Tests for explicit confidence criteria in system prompts."""

    def test_system_prompt_en_contains_confidence_criteria(self):
        prompt = _make_structured_system_prompt("en")
        assert "### Confidence Criteria" in prompt

    def test_system_prompt_ja_contains_confidence_criteria(self):
        prompt = _make_structured_system_prompt("ja")
        assert "### 確度判定基準" in prompt

    def test_confidence_criteria_defines_high(self):
        prompt = _make_structured_system_prompt("en")
        assert "**high**:" in prompt
        assert "knowledge AND" in prompt

    def test_confidence_criteria_defines_medium(self):
        prompt = _make_structured_system_prompt("en")
        assert "**medium**:" in prompt

    def test_confidence_criteria_defines_needs_verification(self):
        prompt = _make_structured_system_prompt("en")
        assert "**needs_verification**:" in prompt
        assert "NOT found in knowledge" in prompt

    def test_action_plan_json_contains_confidence(self):
        prompt = _make_structured_system_prompt("en")
        assert '"confidence":' in prompt
        assert '"confidence_reason":' in prompt

    def test_action_plan_json_ja_contains_confidence(self):
        prompt = _make_structured_system_prompt("ja")
        assert '"confidence":' in prompt
        assert '"confidence_reason":' in prompt


class TestParseActionPlanConfidence:
    """Tests for Action Plan parser handling confidence fields."""

    def test_parse_with_confidence_fields(self):
        from core.llm_prompts import parse_action_plan_from_llm

        text = """<!-- ACTION_PLAN_JSON -->
```json
[{"priority": 1, "problem": "spill", "fix": "increase memory",
  "confidence": "high", "confidence_reason": "matches knowledge section 4"}]
```"""
        result = parse_action_plan_from_llm(text)
        assert len(result) == 1
        assert result[0]["confidence"] == "high"
        assert result[0]["confidence_reason"] == "matches knowledge section 4"

    def test_parse_defaults_confidence_when_missing(self):
        from core.llm_prompts import parse_action_plan_from_llm

        text = """<!-- ACTION_PLAN_JSON -->
```json
[{"priority": 1, "problem": "spill", "fix": "increase memory"}]
```"""
        result = parse_action_plan_from_llm(text)
        assert len(result) == 1
        assert result[0]["confidence"] == ""
        assert result[0]["confidence_reason"] == ""
