"""Tests for core.spark_perf_reporter — Spark Perf Markdown report generation."""

from core.spark_perf_reporter import generate_spark_perf_report


def _make_summary():
    return {
        "app_id": "app-20260320-0000",
        "app_name": "test_app",
        "cluster_id": "cluster-01",
        "duration_min": 14.5,
        "total_jobs": 23,
        "succeeded_jobs": 23,
        "failed_jobs": 0,
        "job_success_rate": 100.0,
        "total_stages": 42,
        "completed_stages": 25,
        "failed_stages": 0,
        "total_tasks": 70669,
        "total_input_gb": 0.0,
        "total_shuffle_gb": 4.53,
        "total_spill_gb": 0.0,
        "gc_overhead_pct": 0.0,
        "total_exec_run_ms": 870000,
    }


def _make_bottlenecks():
    return [
        {
            "stage_id": 20,
            "bottleneck_type": "DATA_SKEW",
            "severity": "HIGH",
            "duration_ms": 305100,
            "num_tasks": 10000,
            "task_skew_ratio": 31.3,
            "disk_spill_mb": 0,
            "shuffle_read_mb": 4485,
            "recommendation": "Enable AQE skew join",
        },
        {
            "stage_id": 0,
            "bottleneck_type": "DATA_SKEW",
            "severity": "MEDIUM",
            "duration_ms": 34700,
            "num_tasks": 10000,
            "task_skew_ratio": 39.8,
            "disk_spill_mb": 0,
            "shuffle_read_mb": 0,
            "recommendation": "Check key distribution",
        },
    ]


def _make_stages():
    return [
        {
            "stage_id": 20,
            "stage_name": "broadcastHashJoin",
            "status": "COMPLETE",
            "bottleneck_type": "DATA_SKEW",
            "severity": "HIGH",
            "duration_ms": 305100,
            "num_tasks": 10000,
            "failed_tasks": 0,
            "task_skew_ratio": 31.3,
            "gc_overhead_pct": 0.5,
            "cpu_efficiency_pct": 6.3,
            "shuffle_read_mb": 4485,
            "shuffle_write_mb": 122,
            "disk_spill_mb": 0,
            "memory_spill_mb": 0,
            "task_p50_ms": 10,
            "task_p95_ms": 50,
            "task_max_ms": 313,
            "recommendation": "Enable AQE skew join",
        },
    ]


def _make_executors():
    return [
        {
            "executor_id": "0",
            "host": "10.0.0.1",
            "total_cores": 4,
            "total_tasks": 500,
            "avg_task_ms": 120,
            "gc_pct": 2.5,
            "cpu_efficiency_pct": 85.0,
            "is_straggler": False,
            "shuffle_read_gb": 1.2,
            "disk_spill_mb": 0,
            "peak_memory_mb": 2048,
        },
    ]


def _make_jobs():
    return [
        {
            "job_id": 0,
            "status": "SUCCEEDED",
            "duration_ms": 305000,
            "total_tasks": 10000,
            "failed_tasks": 0,
        },
    ]


class TestGenerateSparkPerfReport:
    """Iteration 1-3: Full report generation."""

    def test_returns_markdown_string(self):
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            stages=_make_stages(),
            executors=_make_executors(),
            jobs=_make_jobs(),
        )
        assert isinstance(report, str)
        assert len(report) > 100

    def test_has_title(self):
        report = generate_spark_perf_report(summary=_make_summary())
        assert "Spark" in report

    def test_has_executive_summary(self):
        report = generate_spark_perf_report(
            summary=_make_summary(), bottlenecks=_make_bottlenecks()
        )
        assert "1." in report
        assert "14" in report or "14.5" in report  # duration_min

    def test_has_5s_evaluation(self):
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            stages=_make_stages(),
        )
        assert "Skew" in report or "スキュー" in report
        assert "Spill" in report or "スピル" in report
        assert "Shuffle" in report or "シャッフル" in report

    def test_has_job_overview(self):
        report = generate_spark_perf_report(summary=_make_summary())
        assert "23" in report  # total_jobs
        assert "70,669" in report or "70669" in report  # total_tasks

    def test_has_stage_analysis(self):
        report = generate_spark_perf_report(
            summary=_make_summary(),
            stages=_make_stages(),
            bottlenecks=_make_bottlenecks(),
        )
        assert "Stage 20" in report or "stage_id" in report

    def test_has_bottleneck_detail(self):
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
        )
        assert "Data Skew" in report or "データスキュー" in report or "DATA_SKEW" in report

    def test_has_executor_section(self):
        report = generate_spark_perf_report(
            summary=_make_summary(),
            executors=_make_executors(),
        )
        assert "Executor" in report or "executor" in report

    def test_has_recommended_actions(self):
        report = generate_spark_perf_report(
            summary=_make_summary(),
            stages=_make_stages(),
            executors=_make_executors(),
            jobs=_make_jobs(),
            bottlenecks=_make_bottlenecks(),
        )
        assert "推奨アクション" in report or "Top Findings" in report

    def test_narrative_integrated(self):
        narrative = {
            "summary_text": "**This app has critical data skew issues.**",
            "job_analysis_text": "Job 11 is the bottleneck.",
            "node_analysis_text": "Executor 0 is a straggler.",
            "top3_text": "1. Fix data skew\n2. Optimize shuffle\n3. Tune partitions",
        }
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            narrative=narrative,
        )
        assert "critical data skew" in report
        # narrative summary_text is used in executive summary section
        assert "エグゼクティブサマリー" in report

    def test_narrative_none_fallback(self):
        """Without narrative, report still generates with rule-based content."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            stages=_make_stages(),
            executors=_make_executors(),
        )
        assert "## 1." in report
        assert len(report) > 200

    def test_empty_data(self):
        report = generate_spark_perf_report(summary={})
        assert isinstance(report, str)
        assert "Spark" in report


def _make_spark_config(photon_value: str = "true"):
    """Create spark_config list with photon.enabled setting."""
    return [
        {
            "config_key": "spark.databricks.photon.enabled",
            "actual_value": photon_value,
            "category": "Photon",
        },
        {"config_key": "spark.sql.adaptive.enabled", "actual_value": "true", "category": "AQE"},
    ]


def _make_sql_photon_zero():
    """Create sql_photon data with 0% Photon utilization."""
    return [
        {
            "execution_id": "1",
            "photon_pct": 0,
            "duration_sec": 5.0,
            "non_photon_op_list": "Scan, Filter, Project",
        },
        {
            "execution_id": "2",
            "photon_pct": 0,
            "duration_sec": 3.0,
            "non_photon_op_list": "BatchEvalPython",
        },
    ]


def _make_sql_photon_active():
    """Create sql_photon data with active Photon utilization."""
    return [
        {"execution_id": "1", "photon_pct": 85.0, "duration_sec": 5.0, "non_photon_op_list": ""},
        {
            "execution_id": "2",
            "photon_pct": 30.0,
            "duration_sec": 3.0,
            "non_photon_op_list": "BatchEvalPython",
        },
    ]


class TestPhotonClusterConfigAware:
    """Photon section should check cluster config before concluding."""

    def test_photon_disabled_cluster_shows_disabled_message(self):
        """When Photon is disabled on cluster, report should say so."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            sql_photon=_make_sql_photon_zero(),
            spark_config=_make_spark_config("false"),
        )
        assert "Photon が無効" in report or "Photon is disabled" in report
        # Should NOT say "Photon enabled but zero"
        assert "Photon が有効ですが" not in report

    def test_photon_enabled_but_zero_shows_fallback_message(self):
        """When Photon is enabled but 0%, report should note the fallback."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            sql_photon=_make_sql_photon_zero(),
            spark_config=_make_spark_config("true"),
        )
        assert "Photon が有効ですが" in report or "Photon is enabled" in report
        # Should NOT say "Photon disabled"
        assert "Photon が無効" not in report

    def test_photon_config_unknown_shows_unknown_message(self):
        """When no spark_config is provided, report should say config unknown."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            sql_photon=_make_sql_photon_zero(),
            spark_config=[],
        )
        assert "設定が不明" in report or "configuration is unknown" in report

    def test_photon_active_shows_full_analysis(self):
        """When Photon is active (> 0%), full analysis should be shown."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            sql_photon=_make_sql_photon_active(),
            spark_config=_make_spark_config("true"),
        )
        assert "Photon" in report
        assert "57.5" in report or "平均" in report  # avg of 85 and 30
        # Should not show disabled/unknown messages
        assert "Photon が無効" not in report
        assert "設定が不明" not in report

    def test_photon_disabled_alert_in_executive_summary(self):
        """Executive summary alert should reflect Photon disabled status."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            sql_photon=_make_sql_photon_zero(),
            spark_config=_make_spark_config("false"),
            bottlenecks=_make_bottlenecks(),
        )
        assert "Photon が無効" in report or "Photon is disabled" in report


def _make_executors_high_serialization():
    """Executors with high serialization percentage."""
    return [
        {
            "executor_id": "0",
            "host": "10.0.0.1",
            "total_cores": 4,
            "total_tasks": 500,
            "avg_task_ms": 120,
            "gc_pct": 2.5,
            "cpu_efficiency_pct": 85.0,
            "serialization_pct": 25.0,
            "peak_memory_mb": 2048,
        },
        {
            "executor_id": "1",
            "host": "10.0.0.2",
            "total_cores": 4,
            "total_tasks": 400,
            "avg_task_ms": 130,
            "gc_pct": 1.5,
            "cpu_efficiency_pct": 80.0,
            "serialization_pct": 8.0,
            "peak_memory_mb": 1024,
        },
    ]


class TestBottleneckSummary:
    """Bottleneck summary in section 2 should include Serialization and Photon."""

    def test_serialization_appears_in_summary(self):
        """High serialization executors should add SERIALIZATION to bottleneck summary."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            executors=_make_executors_high_serialization(),
        )
        assert "SERIALIZATION" in report
        assert "HIGH" in report  # max_ser=25% > 20% → HIGH

    def test_photon_appears_in_summary(self):
        """Low Photon utilization should add PHOTON to bottleneck summary."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            sql_photon=_make_sql_photon_zero(),
        )
        assert "PHOTON" in report
        assert "MEDIUM" in report  # Photon < 50% → MEDIUM

    def test_no_serialization_when_low(self):
        """Normal serialization should not add SERIALIZATION to summary."""
        low_ser_executors = [
            {
                "executor_id": "0",
                "host": "10.0.0.1",
                "total_cores": 4,
                "total_tasks": 500,
                "avg_task_ms": 120,
                "gc_pct": 2.5,
                "cpu_efficiency_pct": 85.0,
                "serialization_pct": 2.0,
                "peak_memory_mb": 2048,
            },
        ]
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=[],
            executors=low_ser_executors,
        )
        assert "SERIALIZATION" not in report

    def test_no_photon_when_good(self):
        """Good Photon utilization should not add PHOTON to summary."""
        good_photon = [
            {"execution_id": "1", "photon_pct": 90.0, "duration_sec": 5.0},
            {"execution_id": "2", "photon_pct": 80.0, "duration_sec": 3.0},
        ]
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=[],
            sql_photon=good_photon,
        )
        assert "PHOTON" not in report

    def test_summary_sorted_by_severity(self):
        """Bottleneck summary should show higher severity first."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            executors=_make_executors_high_serialization(),
            sql_photon=_make_sql_photon_zero(),
        )
        # DATA_SKEW (HIGH) should appear before PHOTON (MEDIUM)
        skew_pos = report.find("DATA_SKEW")
        photon_pos = report.find("PHOTON")
        assert skew_pos < photon_pos, "HIGH severity should appear before MEDIUM"


class TestIsPhotonEnabledEdgeCases:
    """_is_photon_enabled should handle edge cases."""

    def test_empty_actual_value_returns_none(self):
        """Empty actual_value should be treated as unknown, not disabled."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            sql_photon=_make_sql_photon_zero(),
            spark_config=[{"config_key": "spark.databricks.photon.enabled", "actual_value": ""}],
        )
        assert "設定が不明" in report or "configuration is unknown" in report

    def test_none_actual_value_returns_none(self):
        """None actual_value should be treated as unknown."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            sql_photon=_make_sql_photon_zero(),
            spark_config=[{"config_key": "spark.databricks.photon.enabled", "actual_value": None}],
        )
        assert "設定が不明" in report or "configuration is unknown" in report


class TestNarrativeHeadingExtraction:
    """Tests for heading regex in generate_spark_perf_report (old + new structures)."""

    def test_old_executive_summary_heading_extracted(self):
        narrative = {
            "summary_text": (
                "# 1. エグゼクティブサマリー\n\n"
                "ジョブ概要テキスト\n\n"
                "# 2. 推奨アクション\n\nアクション内容"
            )
        }
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            lang="ja",
            narrative=narrative,
        )
        assert "ジョブ概要テキスト" in report

    def test_new_bottleneck_summary_heading_extracted(self):
        narrative = {
            "summary_text": (
                "# 1. ボトルネック分析サマリー\n\n"
                "ボトルネック評価テーブル\n\n"
                "# 2. 推奨アクション\n\nアクション内容"
            )
        }
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            lang="ja",
            narrative=narrative,
        )
        assert "ボトルネック評価テーブル" in report

    def test_english_bottleneck_analysis_summary_heading(self):
        narrative = {
            "summary_text": (
                "# 1. Bottleneck Analysis Summary\n\n"
                "Evaluation table here\n\n"
                "# 2. Recommended Actions\n\nAction content"
            )
        }
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            lang="en",
            narrative=narrative,
        )
        assert "Evaluation table here" in report

    def test_appendix_stripped_in_fallback(self):
        narrative = {
            "summary_text": (
                "Some intro content\n\n"
                "# Appendix: Detailed Analysis\n\n"
                "## A. Photon\n\nPhoton details"
            )
        }
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            lang="en",
            narrative=narrative,
        )
        assert "Photon details" not in report


class TestCountUnitFormatting:
    """count_unit label should format consistently across languages."""

    def test_japanese_no_space(self):
        """Japanese count_unit should not have a space before it."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            lang="ja",
        )
        # Should contain "2件" (no space between number and 件)
        assert "2件" in report

    def test_english_has_space(self):
        """English count_unit should have a space before 'items'."""
        report = generate_spark_perf_report(
            summary=_make_summary(),
            bottlenecks=_make_bottlenecks(),
            lang="en",
        )
        # Should contain "2 items" (space between number and items)
        assert "2 items" in report
