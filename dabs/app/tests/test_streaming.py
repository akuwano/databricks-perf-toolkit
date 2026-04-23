"""Tests for streaming DLT/SDP profile support."""

from __future__ import annotations

from core.analyzers import analyze_from_dict
from core.extractors import (
    compute_batch_statistics,
    extract_sql_analysis,
    extract_streaming_context,
    is_streaming_profile,
)
from core.models import MicroBatchMetrics, SQLAnalysis, StreamingContext

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_streaming_profile(
    *,
    batch_count: int = 3,
    is_streaming: bool = True,
    status: str = "RUNNING",
    statement_type: str = "REFRESH",
    entry_point: str = "DLT",
    target_table: str = "main.base3.my_table",
    active_plan_index: int = 0,
) -> dict:
    """Build a minimal streaming profile fixture."""
    plan_metadatas = []
    for i in range(batch_count):
        plan_metadatas.append(
            {
                "__typename": "SqlgatewayHistoryQueryPlanMetaData",
                "id": f"plan-{i}",
                "queryId": "query-abc",
                "queryStartTimeMs": str(1000000 + i * 5000),
                "durationMs": str(1000 + i * 200),
                "metrics": {
                    "readBytes": str(50000 + i * 10000),
                    "rowsReadCount": str(1000 + i * 500),
                    "totalTimeMs": str(900 + i * 180),
                },
                "statusId": "FINISHED",
                "queryTextAndError": None,
                "callStack": {"callStackEntries": []},
            }
        )
    if batch_count > 0:
        active_plan_id = plan_metadatas[active_plan_index]["id"]
    else:
        active_plan_id = ""

    return {
        "version": 1.3,
        "query": {
            "__typename": "LakehouseSqlQuery",
            "id": "query-abc",
            "status": status,
            "isFinal": False,
            "statementType": statement_type,
            "queryText": f"REFRESH STREAMING TABLE {target_table}",
            "queryStartTimeMs": 1000000,
            "queryEndTimeMs": None,
            "metrics": {
                "totalTimeMs": 50000,
                "executionTimeMs": 49000,
                "readBytes": 500000,
                "taskTotalTimeMs": 21000,
            },
            "queryMetadata": {
                "isStreaming": is_streaming,
                "writeDataset": target_table,
            },
            "internalQuerySource": {
                "entryPoint": entry_point,
            },
        },
        "planMetadatas": plan_metadatas,
        "graphs": [
            {
                "nodes": [],
                "edges": [],
                "stageData": [],
                "executionId": 100,
            }
        ],
        "activePlanId": active_plan_id,
    }


def _make_normal_nested_profile() -> dict:
    """Build a minimal normal (non-streaming) nested profile."""
    return {
        "query": {
            "id": "query-normal",
            "status": "FINISHED",
            "queryText": "SELECT * FROM t",
            "metrics": {"totalTimeMs": 1000},
            "queryMetadata": {"isStreaming": False},
        },
        "planMetadatas": [
            {
                "id": "plan-0",
                "queryId": "query-normal",
                "durationMs": "800",
                "statusId": "FINISHED",
                "metrics": {"readBytes": "100", "rowsReadCount": "50", "totalTimeMs": "800"},
            }
        ],
        "graphs": [{"nodes": [], "edges": [], "stageData": []}],
    }


def _make_flat_profile() -> dict:
    """Build a minimal flat profile (no query wrapper)."""
    return {
        "id": "query-flat",
        "status": "FINISHED",
        "queryText": "SELECT 1",
        "metrics": {"totalTimeMs": 500},
        "graphs": [],
    }


# ---------------------------------------------------------------------------
# Phase 1: Detection
# ---------------------------------------------------------------------------


class TestIsStreamingProfile:
    def test_detects_streaming_profile(self):
        data = _make_streaming_profile()
        assert is_streaming_profile(data) is True

    def test_non_streaming_nested_profile(self):
        data = _make_normal_nested_profile()
        assert is_streaming_profile(data) is False

    def test_flat_profile(self):
        data = _make_flat_profile()
        assert is_streaming_profile(data) is False

    def test_missing_query_metadata(self):
        data = _make_streaming_profile()
        del data["query"]["queryMetadata"]
        assert is_streaming_profile(data) is False

    def test_is_streaming_false(self):
        data = _make_streaming_profile(is_streaming=False)
        assert is_streaming_profile(data) is False


# ---------------------------------------------------------------------------
# Phase 1: Extraction
# ---------------------------------------------------------------------------


class TestExtractStreamingContext:
    def test_returns_none_for_non_streaming(self):
        data = _make_normal_nested_profile()
        assert extract_streaming_context(data) is None

    def test_returns_none_for_flat_profile(self):
        data = _make_flat_profile()
        assert extract_streaming_context(data) is None

    def test_extracts_target_table(self):
        data = _make_streaming_profile(target_table="catalog.schema.tbl")
        ctx = extract_streaming_context(data)
        assert ctx is not None
        assert ctx.target_table == "catalog.schema.tbl"

    def test_extracts_entry_point(self):
        data = _make_streaming_profile(entry_point="DLT")
        ctx = extract_streaming_context(data)
        assert ctx.entry_point == "DLT"

    def test_extracts_statement_type(self):
        data = _make_streaming_profile()
        ctx = extract_streaming_context(data)
        assert ctx.statement_type == "REFRESH"

    def test_extracts_batch_count(self):
        data = _make_streaming_profile(batch_count=5)
        ctx = extract_streaming_context(data)
        assert ctx.batch_count == 5
        assert len(ctx.batches) == 5

    def test_extracts_active_plan_id(self):
        data = _make_streaming_profile(batch_count=3, active_plan_index=1)
        ctx = extract_streaming_context(data)
        assert ctx.active_plan_id == "plan-1"

    def test_batch_metrics_have_correct_fields(self):
        data = _make_streaming_profile(batch_count=2)
        ctx = extract_streaming_context(data)
        b0 = ctx.batches[0]
        assert isinstance(b0, MicroBatchMetrics)
        assert b0.plan_id == "plan-0"
        assert b0.status == "FINISHED"
        assert b0.duration_ms == 1000
        assert b0.read_bytes == 50000
        assert b0.rows_read_count == 1000
        assert b0.total_time_ms == 900
        assert b0.query_start_time_ms == 1000000

    def test_batch_metrics_second_batch(self):
        data = _make_streaming_profile(batch_count=2)
        ctx = extract_streaming_context(data)
        b1 = ctx.batches[1]
        assert b1.plan_id == "plan-1"
        assert b1.duration_ms == 1200
        assert b1.read_bytes == 60000

    def test_is_final_false(self):
        data = _make_streaming_profile()
        ctx = extract_streaming_context(data)
        assert ctx.is_final is False

    def test_empty_plan_metadatas(self):
        data = _make_streaming_profile(batch_count=0)
        # batch_count=0 means no planMetadatas, but isStreaming=True
        ctx = extract_streaming_context(data)
        assert ctx is not None
        assert ctx.batch_count == 0
        assert ctx.batches == []


# ---------------------------------------------------------------------------
# Phase 2: SQL Analysis Guard
# ---------------------------------------------------------------------------


class TestExtractSqlAnalysisStreaming:
    def test_refresh_streaming_table_returns_raw_sql_only(self):
        data = _make_streaming_profile(target_table="catalog.schema.tbl")
        result = extract_sql_analysis(data)
        assert isinstance(result, SQLAnalysis)
        assert result.raw_sql == "REFRESH STREAMING TABLE catalog.schema.tbl"
        # Should not attempt to parse — tables list should be empty
        assert result.tables == []

    def test_refresh_table_also_skipped(self):
        """REFRESH TABLE (non-streaming) should also be skipped."""
        data = {
            "query": {
                "queryText": "REFRESH TABLE my_catalog.my_schema.my_table",
                "metrics": {},
            },
        }
        result = extract_sql_analysis(data)
        assert result.raw_sql == "REFRESH TABLE my_catalog.my_schema.my_table"

    def test_regular_select_still_analyzed(self):
        data = _make_normal_nested_profile()
        result = extract_sql_analysis(data)
        # Should actually parse the SQL
        assert result.raw_sql != ""


class TestExtractSqlAnalysisGracefulDegradation:
    """Regression: analyze_sql() failures (sqlparse/sqlglot/recursion) must not
    propagate out of extract_sql_analysis(). Degrade to a minimal SQLAnalysis
    carrying the raw query instead.
    """

    def test_returns_minimal_sqlanalysis_when_analyze_sql_raises(self):
        from unittest.mock import patch

        large_sql = (
            "SELECT 1 AS id, 'value -- keep' AS txt\nUNION ALL\n" * 5
            + "SELECT 2 AS id, 'tail' AS txt"
        )
        data = {
            "query": {
                "id": "query-large",
                "status": "FINISHED",
                "queryText": large_sql,
                "metrics": {"totalTimeMs": 1000},
                "queryMetadata": {"isStreaming": False},
            }
        }

        with patch("core.extractors.analyze_sql", side_effect=RecursionError("too deep")):
            result = extract_sql_analysis(data)

        assert isinstance(result, SQLAnalysis)
        assert result.raw_sql == large_sql
        assert result.formatted_sql == large_sql


# ---------------------------------------------------------------------------
# Phase 3: Pipeline Integration
# ---------------------------------------------------------------------------


class TestAnalyzeFromDictStreaming:
    def test_streaming_profile_has_streaming_context(self):
        data = _make_streaming_profile(batch_count=5)
        analysis = analyze_from_dict(data)
        assert analysis.streaming_context is not None
        assert analysis.streaming_context.is_streaming is True
        assert analysis.streaming_context.batch_count == 5

    def test_non_streaming_profile_has_none_streaming_context(self):
        data = _make_normal_nested_profile()
        analysis = analyze_from_dict(data)
        assert analysis.streaming_context is None

    def test_streaming_profile_still_extracts_query_metrics(self):
        data = _make_streaming_profile()
        analysis = analyze_from_dict(data)
        assert analysis.query_metrics.total_time_ms == 50000

    def test_streaming_profile_still_has_bottleneck_indicators(self):
        data = _make_streaming_profile()
        analysis = analyze_from_dict(data)
        assert analysis.bottleneck_indicators is not None


# ---------------------------------------------------------------------------
# Phase 4: Batch Statistics
# ---------------------------------------------------------------------------


class TestComputeBatchStatistics:
    def test_basic_statistics(self):
        data = _make_streaming_profile(batch_count=3)
        ctx = extract_streaming_context(data)
        stats = compute_batch_statistics(ctx)
        # batch 0: 1000ms, batch 1: 1200ms, batch 2: 1400ms
        assert stats["batch_count"] == 3
        assert stats["finished_count"] == 3
        assert stats["duration_min_ms"] == 1000
        assert stats["duration_max_ms"] == 1400
        assert stats["duration_avg_ms"] == 1200.0

    def test_read_bytes_statistics(self):
        data = _make_streaming_profile(batch_count=3)
        ctx = extract_streaming_context(data)
        stats = compute_batch_statistics(ctx)
        # batch 0: 50000, batch 1: 60000, batch 2: 70000
        assert stats["read_bytes_min"] == 50000
        assert stats["read_bytes_max"] == 70000

    def test_detects_slow_batches(self):
        """A batch > 2x avg duration is flagged as slow."""
        ctx = StreamingContext(
            is_streaming=True,
            batch_count=4,
            batches=[
                MicroBatchMetrics(plan_id="p0", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p1", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p2", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p3", duration_ms=500, status="FINISHED"),
            ],
        )
        stats = compute_batch_statistics(ctx)
        assert len(stats["slow_batches"]) == 1
        assert stats["slow_batches"][0]["plan_id"] == "p3"

    def test_empty_batches(self):
        ctx = StreamingContext(is_streaming=True, batch_count=0, batches=[])
        stats = compute_batch_statistics(ctx)
        assert stats["batch_count"] == 0
        assert stats["duration_min_ms"] == 0
        assert stats["duration_avg_ms"] == 0

    def test_single_batch(self):
        ctx = StreamingContext(
            is_streaming=True,
            batch_count=1,
            batches=[MicroBatchMetrics(plan_id="p0", duration_ms=500, status="FINISHED")],
        )
        stats = compute_batch_statistics(ctx)
        assert stats["duration_avg_ms"] == 500
        assert stats["slow_batches"] == []

    def test_duration_p95(self):
        data = _make_streaming_profile(batch_count=10)
        ctx = extract_streaming_context(data)
        stats = compute_batch_statistics(ctx)
        assert stats["duration_p95_ms"] > 0

    def test_running_batch_counted(self):
        ctx = StreamingContext(
            is_streaming=True,
            batch_count=2,
            batches=[
                MicroBatchMetrics(plan_id="p0", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p1", duration_ms=0, status="RUNNING"),
            ],
        )
        stats = compute_batch_statistics(ctx)
        assert stats["finished_count"] == 1
        assert stats["running_count"] == 1


# ---------------------------------------------------------------------------
# Phase 5: Report Generation
# ---------------------------------------------------------------------------


class TestGenerateStreamingSection:
    def test_includes_streaming_banner_en(self):
        from core.reporters.sections import generate_streaming_section

        ctx = StreamingContext(
            is_streaming=True,
            target_table="catalog.schema.tbl",
            entry_point="DLT",
            batch_count=5,
            batches=[
                MicroBatchMetrics(plan_id=f"p{i}", duration_ms=1000 + i * 100, status="FINISHED")
                for i in range(5)
            ],
        )
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_section(ctx, stats)
        assert "Streaming" in md
        assert "snapshot" in md.lower() or "Snapshot" in md

    def test_includes_target_table(self):
        from core.reporters.sections import generate_streaming_section

        ctx = StreamingContext(
            is_streaming=True,
            target_table="my_catalog.my_schema.my_tbl",
            batch_count=1,
            batches=[MicroBatchMetrics(plan_id="p0", duration_ms=500, status="FINISHED")],
        )
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_section(ctx, stats)
        assert "my_catalog.my_schema.my_tbl" in md

    def test_includes_batch_count(self):
        from core.reporters.sections import generate_streaming_section

        ctx = StreamingContext(
            is_streaming=True,
            batch_count=7,
            batches=[
                MicroBatchMetrics(plan_id=f"p{i}", duration_ms=100, status="FINISHED")
                for i in range(7)
            ],
        )
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_section(ctx, stats)
        assert "7" in md

    def test_includes_duration_statistics(self):
        from core.reporters.sections import generate_streaming_section

        ctx = StreamingContext(
            is_streaming=True,
            batch_count=3,
            batches=[
                MicroBatchMetrics(plan_id="p0", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p1", duration_ms=200, status="FINISHED"),
                MicroBatchMetrics(plan_id="p2", duration_ms=300, status="FINISHED"),
            ],
        )
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_section(ctx, stats)
        assert "100" in md  # min
        assert "300" in md  # max

    def test_includes_slow_batch_warning(self):
        from core.reporters.sections import generate_streaming_section

        ctx = StreamingContext(
            is_streaming=True,
            batch_count=4,
            batches=[
                MicroBatchMetrics(plan_id="p0", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p1", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p2", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p3", duration_ms=500, status="FINISHED"),
            ],
        )
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_section(ctx, stats)
        assert "slow" in md.lower() or "Slow" in md

    def test_not_generated_when_empty_batches(self):
        from core.reporters.sections import generate_streaming_section

        ctx = StreamingContext(is_streaming=True, batch_count=0, batches=[])
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_section(ctx, stats)
        # Should still produce a section but with "no batches" info
        assert "Streaming" in md


# ---------------------------------------------------------------------------
# Phase 6: LLM Prompt Integration
# ---------------------------------------------------------------------------


class TestStreamingPrompts:
    def test_system_prompt_includes_streaming_constraints_ja(self):
        from core.llm_prompts.prompts import create_structured_system_prompt

        prompt = create_structured_system_prompt(
            "tuning knowledge",
            lang="ja",
            is_streaming=True,
        )
        assert "ストリーミング" in prompt or "Streaming" in prompt
        assert "マイクロバッチ" in prompt or "micro-batch" in prompt.lower()

    def test_system_prompt_includes_streaming_constraints_en(self):
        from core.llm_prompts.prompts import create_structured_system_prompt

        prompt = create_structured_system_prompt(
            "tuning knowledge",
            lang="en",
            is_streaming=True,
        )
        assert "Streaming" in prompt
        assert "micro-batch" in prompt.lower()

    def test_system_prompt_no_streaming_by_default(self):
        from core.llm_prompts.prompts import create_structured_system_prompt

        prompt = create_structured_system_prompt(
            "tuning knowledge",
            lang="en",
            is_streaming=False,
        )
        # Should NOT contain streaming constraints block
        assert "Streaming Query Constraints" not in prompt

    def test_analysis_prompt_includes_streaming_context(self):
        from core.llm_prompts.prompts import create_structured_analysis_prompt

        data = _make_streaming_profile(batch_count=5)
        analysis = analyze_from_dict(data)
        prompt = create_structured_analysis_prompt(analysis, lang="en")
        assert "Streaming" in prompt
        assert "micro-batch" in prompt.lower() or "Micro-Batch" in prompt
        assert "5" in prompt  # batch count

    def test_analysis_prompt_no_streaming_for_normal(self):
        from core.llm_prompts.prompts import create_structured_analysis_prompt

        data = _make_normal_nested_profile()
        analysis = analyze_from_dict(data)
        prompt = create_structured_analysis_prompt(analysis, lang="en")
        assert "Micro-Batch" not in prompt

    def test_review_system_prompt_includes_streaming(self):
        from core.llm_prompts.prompts import create_review_system_prompt

        prompt = create_review_system_prompt(
            "tuning knowledge",
            lang="en",
            is_streaming=True,
        )
        assert "Streaming" in prompt

    def test_refine_system_prompt_includes_streaming(self):
        from core.llm_prompts.prompts import create_refine_system_prompt

        prompt = create_refine_system_prompt(
            "tuning knowledge",
            lang="en",
            is_streaming=True,
        )
        assert "Streaming" in prompt

    def test_streaming_constraints_block_warns_against_cumulative(self):
        from core.llm_prompts.prompts import create_structured_system_prompt

        prompt = create_structured_system_prompt(
            "tuning knowledge",
            lang="en",
            is_streaming=True,
        )
        assert "cumulative" in prompt.lower()

    def test_analysis_prompt_labels_cumulative_for_streaming(self):
        from core.llm_prompts.prompts import create_structured_analysis_prompt

        data = _make_streaming_profile(batch_count=5)
        analysis = analyze_from_dict(data)
        prompt = create_structured_analysis_prompt(analysis, lang="en")
        assert "Cumulative" in prompt

    def test_analysis_prompt_includes_slowest_batches(self):
        from core.llm_prompts.prompts import create_structured_analysis_prompt

        # Build analysis with a slow batch (>2x avg) to trigger Slowest section
        data = _make_streaming_profile(batch_count=4)
        # Inject a very slow batch manually
        data["planMetadatas"][3]["durationMs"] = "10000"
        analysis = analyze_from_dict(data)
        prompt = create_structured_analysis_prompt(analysis, lang="en")
        assert "Slowest" in prompt or "slowest" in prompt


# ---------------------------------------------------------------------------
# Phase 8: Streaming-aware Report Sections
# ---------------------------------------------------------------------------


class TestStreamingExecutiveSummary:
    def test_rule_based_summary_shows_batch_avg(self):
        from core.reporters.summary import generate_streaming_executive_summary

        data = _make_streaming_profile(batch_count=5)
        analysis = analyze_from_dict(data)
        ctx = analysis.streaming_context
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_executive_summary(
            analysis.bottleneck_indicators.alerts,
            ctx,
            stats,
            action_cards=analysis.action_cards,
        )
        # Should contain batch avg duration, not total execution time
        assert "Avg Batch Duration" in md or "avg" in md.lower()

    def test_rule_based_summary_no_total_execution_time(self):
        from core.reporters.summary import generate_streaming_executive_summary

        data = _make_streaming_profile(batch_count=5)
        analysis = analyze_from_dict(data)
        ctx = analysis.streaming_context
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_executive_summary(
            analysis.bottleneck_indicators.alerts,
            ctx,
            stats,
        )
        # Should NOT show "Total Execution Time" as primary metric
        assert "Total Execution Time" not in md

    def test_rule_based_summary_shows_slow_batches(self):
        from core.reporters.summary import generate_streaming_executive_summary

        ctx = StreamingContext(
            is_streaming=True,
            batch_count=4,
            batches=[
                MicroBatchMetrics(plan_id="p0", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p1", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p2", duration_ms=100, status="FINISHED"),
                MicroBatchMetrics(plan_id="p3", duration_ms=500, status="FINISHED"),
            ],
        )
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_executive_summary([], ctx, stats)
        assert "slow" in md.lower() or "Slow" in md


class TestStreamingPerformanceMetrics:
    def test_generates_micro_batch_performance(self):
        from core.reporters.query_metrics import generate_streaming_performance_metrics

        data = _make_streaming_profile(batch_count=5)
        analysis = analyze_from_dict(data)
        ctx = analysis.streaming_context
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_performance_metrics(ctx, stats, analysis.query_metrics)
        assert "Micro-Batch" in md
        assert "Avg" in md or "avg" in md

    def test_includes_cumulative_snapshot(self):
        from core.reporters.query_metrics import generate_streaming_performance_metrics

        data = _make_streaming_profile(batch_count=3)
        analysis = analyze_from_dict(data)
        ctx = analysis.streaming_context
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_performance_metrics(ctx, stats, analysis.query_metrics)
        assert "Cumulative" in md

    def test_no_total_execution_time_as_primary(self):
        from core.reporters.query_metrics import generate_streaming_performance_metrics

        data = _make_streaming_profile(batch_count=3)
        analysis = analyze_from_dict(data)
        ctx = analysis.streaming_context
        stats = compute_batch_statistics(ctx)
        md = generate_streaming_performance_metrics(ctx, stats, analysis.query_metrics)
        # Total Execution Time should be in cumulative appendix, not as primary
        lines_before_cumulative = md.split("Cumulative")[0]
        assert "Total Execution Time" not in lines_before_cumulative


class TestGenerateReportStreaming:
    def test_report_uses_streaming_summary(self):
        from core.reporters import generate_report

        data = _make_streaming_profile(batch_count=5)
        analysis = analyze_from_dict(data)
        report = generate_report(analysis)
        # Section 1 should have batch-oriented metrics
        section1_start = report.find("Executive Summary")
        section2_start = report.find("Top Alerts")
        if section2_start == -1:
            section2_start = report.find("Recommended Actions")
        section1 = report[section1_start:section2_start]
        assert "Avg Batch Duration" in section1 or "avg" in section1.lower()

    def test_report_uses_streaming_performance(self):
        from core.reporters import generate_report

        data = _make_streaming_profile(batch_count=5)
        analysis = analyze_from_dict(data)
        report = generate_report(analysis)
        # Section 4 should be Micro-Batch Performance
        assert "Micro-Batch" in report
