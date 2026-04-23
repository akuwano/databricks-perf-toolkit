"""Tests for new report structure functions in reporters.py."""

import pytest
from core.constants import Severity
from core.models import (
    ActionCard,
    Alert,
    BottleneckIndicators,
    DataFlowEntry,
    ProfileAnalysis,
    QueryMetrics,
    StageInfo,
)
from core.reporters import (
    generate_data_flow_section,
    generate_performance_metrics,
    generate_query_overview,
    generate_report,
    generate_rule_based_recommendations,
    generate_rule_based_summary,
    generate_stage_execution_section,
)

# --- Fixtures ---


@pytest.fixture
def sample_qm():
    return QueryMetrics(
        query_id="test-query-123",
        status="FAILED",
        total_time_ms=320000,
        compilation_time_ms=5000,
        execution_time_ms=315000,
        read_bytes=50_000_000_000,
        read_remote_bytes=40_000_000_000,
        read_cache_bytes=10_000_000_000,
        photon_total_time_ms=200_000,
        task_total_time_ms=500_000,
        read_files_count=1000,
        pruned_files_count=3000,
        pruned_bytes=100_000_000_000,
        rows_read_count=50_000_000,
        rows_produced_count=10_000_000,
    )


@pytest.fixture
def sample_bi():
    bi = BottleneckIndicators()
    bi.cache_hit_ratio = 0.20
    bi.photon_ratio = 0.40
    bi.spill_bytes = 5_000_000_000
    bi.remote_read_ratio = 0.80
    bi.filter_rate = 0.15
    bi.bytes_pruning_ratio = 0.667
    bi.alerts = [
        Alert(
            severity=Severity.CRITICAL,
            category="spill",
            message="Disk spill of 4.66 GB detected.",
            recommendation="Scale up cluster or add REPARTITION hints.",
        ),
        Alert(
            severity=Severity.HIGH,
            category="cache",
            message="Cache hit ratio is only 20%.",
            recommendation="Re-run query to verify cache.",
        ),
        Alert(
            severity=Severity.MEDIUM,
            category="photon",
            message="Photon utilization at 40%.",
        ),
    ]
    return bi


@pytest.fixture
def sample_stages():
    return [
        StageInfo(
            stage_id="1",
            status="COMPLETE",
            duration_ms=9000,
            num_tasks=49,
            num_complete_tasks=49,
            num_failed_tasks=0,
        ),
        StageInfo(
            stage_id="2",
            status="COMPLETE",
            duration_ms=16500,
            num_tasks=73,
            num_complete_tasks=73,
            num_failed_tasks=0,
        ),
        StageInfo(
            stage_id="3",
            status="SKIPPED",
            duration_ms=0,
            num_tasks=10,
            num_complete_tasks=0,
            num_failed_tasks=0,
        ),
        StageInfo(
            stage_id="4",
            status="FAILED",
            duration_ms=18576000,
            num_tasks=393,
            num_complete_tasks=0,
            num_failed_tasks=947,
            note="OOM Kill (Exit Code 137)",
        ),
    ]


@pytest.fixture
def sample_data_flow():
    return [
        DataFlowEntry(
            node_id="100",
            operation="Scan table_a",
            output_rows=1_950_176,
            duration_ms=1000,
            peak_memory_bytes=16_000_000,
        ),
        DataFlowEntry(
            node_id="200",
            operation="Scan table_b",
            output_rows=24_110_484,
            duration_ms=6000,
            peak_memory_bytes=184_000_000,
        ),
        DataFlowEntry(
            node_id="300",
            operation="Inner Join",
            output_rows=13_200_000_000,
            duration_ms=279_000,
            peak_memory_bytes=1_970_000_000,
            join_keys="a.user_id = b.user_id",
        ),
        DataFlowEntry(
            node_id="400",
            operation="LEFT OUTER JOIN",
            output_rows=5_520_000_000_000,
            duration_ms=37_500,
            peak_memory_bytes=73_000_000_000,
            join_keys="user_id, quest_name = user_id, quest_name",
        ),
    ]


@pytest.fixture
def sample_action_cards():
    return [
        ActionCard(
            problem="Disk Spill detected (4.66 GB)",
            evidence=["spill_to_disk_bytes: 5,000,000,000"],
            likely_cause="Insufficient memory for JOIN operations",
            fix="Scale up cluster or use REPARTITION hints",
            expected_impact="high",
            effort="medium",
            priority_score=8.5,
        ),
        ActionCard(
            problem="Low Photon utilization (40%)",
            evidence=["photon_ratio: 40%"],
            likely_cause="Sort-Merge JOIN blocking Photon",
            fix="Use Broadcast JOIN for small tables",
            expected_impact="medium",
            effort="low",
            priority_score=6.0,
        ),
        ActionCard(
            problem="Cache hit ratio low (20%)",
            evidence=["cache_hit_ratio: 20%"],
            likely_cause="First execution or large dataset",
            fix="Re-run query to populate cache",
            expected_impact="low",
            effort="low",
            priority_score=3.0,
        ),
    ]


@pytest.fixture
def sample_analysis(sample_qm, sample_bi, sample_stages, sample_data_flow, sample_action_cards):
    return ProfileAnalysis(
        query_metrics=sample_qm,
        bottleneck_indicators=sample_bi,
        stage_info=sample_stages,
        data_flow=sample_data_flow,
        action_cards=sample_action_cards,
    )


# --- generate_query_overview tests ---


class TestGenerateQueryOverview:
    def test_contains_query_id(self, sample_qm):
        result = generate_query_overview(sample_qm)
        assert "test-query-123" in result

    def test_contains_status(self, sample_qm):
        result = generate_query_overview(sample_qm)
        assert "FAILED" in result

    def test_contains_execution_time(self, sample_qm):
        result = generate_query_overview(sample_qm)
        # Should show formatted time
        assert "5 min 20" in result or "320" in result

    def test_table_format(self, sample_qm):
        result = generate_query_overview(sample_qm)
        assert "|" in result  # Markdown table


# --- generate_performance_metrics tests ---


class TestGeneratePerformanceMetrics:
    def test_contains_time_metrics(self, sample_qm, sample_bi):
        result = generate_performance_metrics(sample_qm, sample_bi)
        assert "Time" in result or "時間" in result or "time" in result.lower()

    def test_contains_io_metrics(self, sample_qm, sample_bi):
        result = generate_performance_metrics(sample_qm, sample_bi)
        assert "I/O" in result or "IO" in result

    def test_contains_photon_ratio(self, sample_qm, sample_bi):
        result = generate_performance_metrics(sample_qm, sample_bi)
        assert "40" in result  # photon ratio 40%

    def test_contains_cache_ratio(self, sample_qm, sample_bi):
        result = generate_performance_metrics(sample_qm, sample_bi)
        assert "20" in result  # cache hit 20%

    def test_shuffle_section_when_data_exists(self, sample_qm):
        bi = BottleneckIndicators()
        bi.shuffle_bytes_written_total = 2_000_000_000  # 2GB
        bi.shuffle_remote_bytes_read_total = 1_500_000_000
        bi.shuffle_local_bytes_read_total = 500_000_000
        result = generate_performance_metrics(sample_qm, bi)
        assert "Shuffle" in result
        assert "75.0%" in result  # remote ratio 1.5G / 2G

    def test_no_shuffle_section_when_zero(self, sample_qm, sample_bi):
        result = generate_performance_metrics(sample_qm, sample_bi)
        assert "Shuffle I/O" not in result and "Shuffle Write" not in result

    def test_write_section_when_data_exists(self, sample_qm):
        qm = QueryMetrics(
            read_bytes=10_000_000_000,
            write_remote_bytes=500_000_000,  # 500MB
            write_remote_files=50,
            write_remote_rows=1_000_000,
        )
        bi = BottleneckIndicators()
        result = generate_performance_metrics(qm, bi)
        assert "Write" in result
        assert "1,000,000" in result  # rows
        assert "50" in result  # files

    def test_no_write_section_when_zero(self, sample_qm, sample_bi):
        result = generate_performance_metrics(sample_qm, sample_bi)
        assert "Write I/O" not in result and "Bytes Written" not in result

    def test_write_read_ratio_shown(self):
        qm = QueryMetrics(
            read_bytes=1_000_000_000,
            write_remote_bytes=2_000_000_000,
        )
        bi = BottleneckIndicators()
        result = generate_performance_metrics(qm, bi)
        assert "200.0%" in result  # write/read ratio


# --- generate_stage_execution_section tests ---


class TestGenerateStageExecutionSection:
    def test_contains_all_stages(self, sample_stages):
        result = generate_stage_execution_section(sample_stages)
        for stage in sample_stages:
            assert stage.stage_id in result

    def test_marks_failed_stage(self, sample_stages):
        result = generate_stage_execution_section(sample_stages)
        assert "FAILED" in result

    def test_shows_failure_note(self, sample_stages):
        result = generate_stage_execution_section(sample_stages)
        assert "OOM" in result

    def test_table_format(self, sample_stages):
        result = generate_stage_execution_section(sample_stages)
        assert "|" in result  # Markdown table

    def test_empty_stages(self):
        result = generate_stage_execution_section([])
        # Should either return empty or a note
        assert result == "" or "N/A" in result or "No" in result.lower()


# --- generate_data_flow_section tests ---


class TestGenerateDataFlowSection:
    def test_contains_scan_operations(self, sample_data_flow):
        result = generate_data_flow_section(sample_data_flow)
        assert "Scan" in result

    def test_contains_join_operations(self, sample_data_flow):
        result = generate_data_flow_section(sample_data_flow)
        assert "Join" in result or "JOIN" in result

    def test_shows_row_counts(self, sample_data_flow):
        result = generate_data_flow_section(sample_data_flow)
        # Should show formatted row counts
        assert "1,950,176" in result or "1.95" in result or "2M" in result or "1.9M" in result

    def test_shows_join_keys(self, sample_data_flow):
        result = generate_data_flow_section(sample_data_flow)
        assert "user_id" in result

    def test_table_format(self, sample_data_flow):
        result = generate_data_flow_section(sample_data_flow)
        assert "|" in result  # Markdown table

    def test_empty_flow(self):
        result = generate_data_flow_section([])
        assert result == "" or "N/A" in result or "No" in result.lower()


# --- generate_rule_based_summary tests ---


class TestGenerateRuleBasedSummary:
    def test_shows_highest_severity_badge(self, sample_bi):
        result = generate_rule_based_summary(sample_bi.alerts)
        assert "CRITICAL" in result

    def test_references_critical_alert(self, sample_bi):
        result = generate_rule_based_summary(sample_bi.alerts)
        assert "spill" in result.lower() or "Spill" in result

    def test_empty_alerts(self):
        result = generate_rule_based_summary([])
        # Should still produce a summary, possibly generic
        assert len(result) > 0

    def test_info_only_alerts(self):
        alerts = [
            Alert(
                severity=Severity.INFO,
                category="cache",
                message="Cache is performing well.",
            )
        ]
        result = generate_rule_based_summary(alerts)
        assert "CRITICAL" not in result

    def test_includes_query_metrics(self, sample_bi, sample_qm):
        result = generate_rule_based_summary(sample_bi.alerts, qm=sample_qm)
        assert "FAILED" in result
        assert "5 min" in result

    def test_includes_action_card_findings(self, sample_bi, sample_qm, sample_action_cards):
        result = generate_rule_based_summary(
            sample_bi.alerts,
            qm=sample_qm,
            action_cards=sample_action_cards,
        )
        assert "Key Findings" in result or "主要な問題" in result
        assert "Disk Spill" in result or "spill" in result.lower()


# --- generate_rule_based_recommendations tests ---


class TestGenerateRuleBasedRecommendations:
    def test_groups_by_priority(self, sample_action_cards):
        result = generate_rule_based_recommendations(sample_action_cards)
        assert "Priority 1" in result or "priority 1" in result.lower()

    def test_contains_fix_text(self, sample_action_cards):
        result = generate_rule_based_recommendations(sample_action_cards)
        assert "Scale up" in result or "REPARTITION" in result

    def test_empty_cards(self):
        result = generate_rule_based_recommendations([])
        assert result == "" or "No" in result.lower() or "N/A" in result

    def test_high_impact_first(self, sample_action_cards):
        result = generate_rule_based_recommendations(sample_action_cards)
        lines = result.split("\n")
        # Find positions of Priority sections
        p1_pos = next((i for i, line in enumerate(lines) if "Priority 1" in line), -1)
        p2_pos = next((i for i, line in enumerate(lines) if "Priority 2" in line), -1)
        if p1_pos >= 0 and p2_pos >= 0:
            assert p1_pos < p2_pos


# --- generate_report (new signature) tests ---


class TestGenerateReportNewStructure:
    def test_new_signature_with_llm_sections(self, sample_analysis):
        llm_sections = {
            "executive_summary": "CRITICAL: OOM failure due to JOIN explosion.",
            "root_cause_analysis": "### 4.1 Direct Cause\nOOM Kill.\n### 4.2 Root Cause\nJOIN explosion.",
            "recommendations": "### Priority 1: Fix JOIN\nAdd filters.",
            "conclusion": "Fix the JOIN conditions.",
        }
        result = generate_report(
            analysis=sample_analysis,
            llm_sections=llm_sections,
        )
        # Should contain all major sections
        assert "Executive Summary" in result or "エグゼクティブサマリー" in result
        assert "Query Overview" in result or "クエリ概要" in result
        assert "Performance Metrics" in result or "パフォーマンス" in result
        assert "Root Cause" in result or "根本原因" in result
        assert "Stage" in result
        assert "Data Flow" in result or "データフロー" in result
        assert "Recommended Actions" in result or "推奨" in result
        # Conclusion removed — covered by Executive Summary
        assert "Appendix" in result

    def test_no_llm_mode(self, sample_analysis):
        """With empty llm_sections, should use rule-based fallback."""
        result = generate_report(
            analysis=sample_analysis,
            llm_sections={},
        )
        # Should still produce a full report
        assert "Query Overview" in result or "test-query-123" in result
        assert "Stage" in result

    def test_section_order(self, sample_analysis):
        llm_sections = {
            "executive_summary": "Summary here.",
            "root_cause_analysis": "Root cause here.",
            "recommendations": "Recs here.",
            "conclusion": "Conclusion here.",
        }
        result = generate_report(
            analysis=sample_analysis,
            llm_sections=llm_sections,
        )
        lines = result.split("\n")
        # Find positions of key sections
        section_positions = {}
        for i, line in enumerate(lines):
            if "Executive Summary" in line or "エグゼクティブサマリー" in line:
                section_positions["exec_summary"] = i
            elif "Query Overview" in line or "クエリ概要" in line:
                section_positions["query_overview"] = i
            elif "Root Cause" in line or "根本原因" in line:
                section_positions["root_cause"] = i
            elif "Stage" in line and ("Execution" in line or "ステージ" in line):
                section_positions["stage"] = i
            elif "Data Flow" in line or "データフロー" in line:
                section_positions["data_flow"] = i
            elif "Recommendation" in line or "推奨" in line:
                section_positions["recommendations"] = i
            elif "Conclusion" in line or "結論" in line:
                section_positions["conclusion"] = i

        # Verify new order: exec_summary < recommendations < root_cause < query_overview (appendix)
        ordered_keys = [
            "exec_summary",
            "recommendations",
            "root_cause",
            "query_overview",
        ]
        found_keys = [k for k in ordered_keys if k in section_positions]
        for i in range(len(found_keys) - 1):
            assert section_positions[found_keys[i]] < section_positions[found_keys[i + 1]], (
                f"{found_keys[i]} (line {section_positions[found_keys[i]]}) "
                f"should come before {found_keys[i + 1]} (line {section_positions[found_keys[i + 1]]})"
            )

    def test_appendix_sections_present(self, sample_analysis):
        """Appendix sections should be present."""
        result = generate_report(
            analysis=sample_analysis,
            llm_sections={},
        )
        # Should have bottleneck details in appendix
        assert "Bottleneck" in result or "ボトルネック" in result

    def test_llm_content_embedded(self, sample_analysis):
        llm_sections = {
            "executive_summary": "CRITICAL: OOM failure detected in stage 4.",
        }
        result = generate_report(
            analysis=sample_analysis,
            llm_sections=llm_sections,
        )
        assert "CRITICAL: OOM failure detected in stage 4." in result

    def test_raw_llm_fallback_when_sections_empty(self, sample_analysis):
        """When llm_sections is empty but raw_llm_analysis has text,
        it should appear as a fallback LLM Analysis Report section."""
        raw_text = "This is a free-form LLM analysis with no ## headers."
        result = generate_report(
            analysis=sample_analysis,
            llm_sections={},
            raw_llm_analysis=raw_text,
        )
        assert "LLM Analysis Report" in result or "LLM分析レポート" in result
        assert raw_text in result

    def test_parsed_sections_note_when_no_unmatched(self, sample_analysis):
        """When llm_sections has matched content (no _unmatched),
        a note about successful parsing should appear."""
        llm_sections = {
            "executive_summary": "Summary from LLM.",
        }
        raw_text = "This raw text should not appear as fallback."
        result = generate_report(
            analysis=sample_analysis,
            llm_sections=llm_sections,
            raw_llm_analysis=raw_text,
        )
        assert "executive_summary" in result
        assert raw_text not in result

    def test_no_raw_llm_fallback_when_empty_string(self, sample_analysis):
        """Empty raw_llm_analysis should not create a fallback section."""
        result = generate_report(
            analysis=sample_analysis,
            llm_sections={},
            raw_llm_analysis="",
        )
        assert "LLM Analysis Report" not in result
        assert "LLM分析レポート" not in result

    def test_unmatched_sections_shown_as_llm_analysis(self, sample_analysis):
        """When _unmatched key exists, those sections should appear
        under LLM Analysis Report."""
        llm_sections = {
            "recommendations": "Use broadcast join.",
            "_unmatched": "## Summary\n\nQuery ran for 7 hours.\n\n## I/O Analysis\n\nHigh remote read.",
        }
        result = generate_report(
            analysis=sample_analysis,
            llm_sections=llm_sections,
        )
        assert "LLM Analysis Report" in result or "LLM分析レポート" in result
        assert "Query ran for 7 hours" in result
        assert "I/O Analysis" in result
