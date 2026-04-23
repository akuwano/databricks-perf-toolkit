"""Tests for services.spark_perf_reader module."""

import os
from unittest.mock import MagicMock, patch

import pytest
from services.spark_perf_reader import SparkPerfConfig, SparkPerfReader


class TestSparkPerfConfig:
    def test_creation_with_explicit_values(self):
        config = SparkPerfConfig(
            catalog="my_catalog",
            schema="my_schema",
            table_prefix="PERF_",
            databricks_host="https://host.com",
            databricks_token="tok",
            http_path="/sql/1.0/warehouses/abc",
        )
        assert config.catalog == "my_catalog"
        assert config.schema == "my_schema"
        assert config.table_prefix == "PERF_"

    def test_from_env_defaults(self):
        with patch.dict(os.environ, {"DBSQL_PROFILER_CONFIG": "/tmp/_no_config.json"}, clear=True):
            config = SparkPerfConfig.from_env()
        assert config.catalog == "main"
        assert config.schema == "default"
        assert config.table_prefix == "PERF_"
        assert config.http_path == ""

    def test_from_env_custom(self):
        env = {
            "DBSQL_PROFILER_CONFIG": "/tmp/_no_config.json",
            "SPARK_PERF_CATALOG": "prod_catalog",
            "SPARK_PERF_SCHEMA": "perf_schema",
            "SPARK_PERF_TABLE_PREFIX": "MY_",
            "SPARK_PERF_HTTP_PATH": "/sql/1.0/warehouses/xyz",
            "DATABRICKS_HOST": "https://host.com",
            "DATABRICKS_TOKEN": "tok",
        }
        with patch.dict(os.environ, env, clear=True):
            config = SparkPerfConfig.from_env()
        assert config.catalog == "prod_catalog"
        assert config.schema == "perf_schema"
        assert config.table_prefix == "MY_"
        assert config.http_path == "/sql/1.0/warehouses/xyz"


@pytest.fixture
def config():
    return SparkPerfConfig(
        catalog="main",
        schema="base2",
        table_prefix="PERF_",
        databricks_host="https://host.com",
        databricks_token="tok",
        http_path="/sql/1.0/warehouses/abc",
    )


@pytest.fixture
def reader(config):
    return SparkPerfReader(config)


class TestSparkPerfReaderFqn:
    def test_fqn_with_prefix(self):
        config = SparkPerfConfig(
            catalog="main",
            schema="base2",
            table_prefix="PERF_",
            databricks_host="",
            databricks_token="",
            http_path="",
        )
        reader = SparkPerfReader(config)
        assert reader._fqn("gold_application_summary") == "main.base2.PERF_gold_application_summary"

    def test_fqn_with_empty_prefix(self):
        config = SparkPerfConfig(
            catalog="cat",
            schema="sch",
            table_prefix="",
            databricks_host="",
            databricks_token="",
            http_path="",
        )
        reader = SparkPerfReader(config)
        assert reader._fqn("gold_stages") == "cat.sch.gold_stages"

    def test_fqn_with_custom_prefix(self):
        config = SparkPerfConfig(
            catalog="main",
            schema="base2",
            table_prefix="TEST_",
            databricks_host="",
            databricks_token="",
            http_path="",
        )
        reader = SparkPerfReader(config)
        assert reader._fqn("gold_executor_analysis") == "main.base2.TEST_gold_executor_analysis"


class TestSparkPerfReaderListApplications:
    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_list_of_dicts(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [("app_id",), ("app_name",), ("start_ts",)]
        mock_cursor.fetchall.return_value = [
            ("app-1", "MyApp", "2026-01-01 00:00:00"),
            ("app-2", "OtherApp", "2026-01-02 00:00:00"),
        ]
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.list_applications()
        assert len(result) == 2
        assert result[0]["app_id"] == "app-1"
        assert result[1]["app_name"] == "OtherApp"

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_empty_on_error(self, mock_conn, reader):
        mock_conn.side_effect = Exception("connection failed")
        result = reader.list_applications()
        assert result == []


class TestSparkPerfReaderQueryTable:
    @patch.object(SparkPerfReader, "_get_connection")
    def test_query_table_returns_dicts(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [("stage_id",), ("bottleneck_type",), ("severity",)]
        mock_cursor.fetchall.return_value = [
            (1, "DISK_SPILL", "HIGH"),
            (2, "OK", "NONE"),
        ]
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader._query_table("gold_bottleneck_report", app_id="app-1")
        assert len(result) == 2
        assert result[0]["bottleneck_type"] == "DISK_SPILL"

    @patch.object(SparkPerfReader, "_get_connection")
    def test_query_table_returns_empty_on_error(self, mock_conn, reader):
        mock_conn.side_effect = Exception("table not found")
        result = reader._query_table("gold_nonexistent", app_id="app-1")
        assert result == []


class TestSparkPerfReaderPublicMethods:
    """Test the 6 public query methods that delegate to _query_table."""

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_application_summary_returns_dict(self, mock_qt, reader):
        mock_qt.return_value = [{"app_id": "app-1", "duration_min": 5.0}]
        result = reader.get_application_summary("app-1")
        assert result is not None
        assert result["app_id"] == "app-1"
        mock_qt.assert_called_once_with("gold_application_summary", app_id="app-1")

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_application_summary_returns_none_when_empty(self, mock_qt, reader):
        mock_qt.return_value = []
        assert reader.get_application_summary("app-x") is None

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_bottleneck_report(self, mock_qt, reader):
        mock_qt.return_value = [{"stage_id": 1, "severity": "HIGH"}]
        result = reader.get_bottleneck_report("app-1")
        assert len(result) == 1
        mock_qt.assert_called_once_with("gold_bottleneck_report", app_id="app-1")

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_stage_performance(self, mock_qt, reader):
        mock_qt.return_value = [{"stage_id": 1}]
        result = reader.get_stage_performance("app-1")
        assert len(result) == 1
        mock_qt.assert_called_once_with("gold_stage_performance", app_id="app-1")

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_executor_analysis(self, mock_qt, reader):
        mock_qt.return_value = [{"executor_id": "0"}]
        result = reader.get_executor_analysis("app-1")
        assert len(result) == 1
        mock_qt.assert_called_once_with("gold_executor_analysis", app_id="app-1")

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_job_concurrency(self, mock_qt, reader):
        mock_qt.return_value = [{"job_id": 1}]
        result = reader.get_job_concurrency("app-1")
        assert len(result) == 1
        mock_qt.assert_called_once_with("gold_job_concurrency", app_id="app-1")

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_sql_photon_analysis(self, mock_qt, reader):
        mock_qt.return_value = [{"execution_id": 1}]
        result = reader.get_sql_photon_analysis("app-1")
        assert len(result) == 1
        mock_qt.assert_called_once_with("gold_sql_photon_analysis", app_id="app-1")

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_job_performance(self, mock_qt, reader):
        mock_qt.return_value = [{"job_id": 1, "status": "SUCCEEDED"}]
        result = reader.get_job_performance("app-1")
        assert len(result) == 1
        mock_qt.assert_called_once_with("gold_job_performance", app_id="app-1")

    @patch.object(SparkPerfReader, "_get_connection")
    def test_get_narrative_summary_returns_dict(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [("summary_text",), ("top3_text",)]
        mock_cursor.fetchone.return_value = ("Summary here", "Top3 here")
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_narrative_summary()
        assert result is not None
        assert result["summary_text"] == "Summary here"
        assert result["top3_text"] == "Top3 here"

    @patch.object(SparkPerfReader, "_get_connection")
    def test_get_narrative_summary_returns_none_on_error(self, mock_conn, reader):
        mock_conn.side_effect = Exception("no table")
        result = reader.get_narrative_summary()
        assert result is None

    @patch.object(SparkPerfReader, "_query_table")
    def test_get_spot_instance_analysis(self, mock_qt, reader):
        mock_qt.return_value = [{"executor_id": "0", "removal_type": "SPOT_PREEMPTION"}]
        result = reader.get_spot_instance_analysis("app-1")
        assert len(result) == 1
        mock_qt.assert_called_once_with("gold_spot_instance_analysis", app_id="app-1")

    @patch.object(SparkPerfReader, "_get_connection")
    def test_get_narrative_summary_4_sections(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("summary_text",),
            ("job_analysis_text",),
            ("node_analysis_text",),
            ("top3_text",),
        ]
        mock_cursor.fetchone.return_value = ("Summary", "Jobs", "Nodes", "Top3")
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_narrative_summary()
        assert result["summary_text"] == "Summary"
        assert result["job_analysis_text"] == "Jobs"
        assert result["node_analysis_text"] == "Nodes"
        assert result["top3_text"] == "Top3"


class TestConcurrencySummary:
    """Tests for get_concurrency_summary composite method."""

    @patch.object(SparkPerfReader, "get_cross_app_concurrency")
    @patch.object(SparkPerfReader, "get_job_concurrency")
    def test_basic_summary(self, mock_jc, mock_ca, reader):
        mock_jc.return_value = [
            {"concurrent_jobs_at_start": 0, "cpu_efficiency_pct": 70.0},
            {"concurrent_jobs_at_start": 2, "cpu_efficiency_pct": 60.0},
            {"concurrent_jobs_at_start": 3, "cpu_efficiency_pct": 55.0},
        ]
        mock_ca.return_value = []
        result = reader.get_concurrency_summary("app-1")
        assert result["total_jobs"] == 3
        assert result["max_concurrent_jobs"] == 3
        assert result["jobs_with_concurrency"] == 2
        assert result["cross_app_jobs_detected"] == 0
        assert result["avg_cpu_solo_pct"] == 70.0
        assert result["avg_cpu_concurrent_pct"] == 57.5
        assert result["cpu_diff_pp"] == -12.5  # negative = degradation

    @patch.object(SparkPerfReader, "get_cross_app_concurrency")
    @patch.object(SparkPerfReader, "get_job_concurrency")
    def test_empty_concurrency_with_cross_app(self, mock_jc, mock_ca, reader):
        mock_jc.return_value = []
        mock_ca.return_value = [{"cross_app_concurrent_count": 2}]
        result = reader.get_concurrency_summary("app-1")
        assert result["cross_app_jobs_detected"] == 1
        assert result["total_jobs"] == 0

    @patch.object(SparkPerfReader, "get_cross_app_concurrency")
    @patch.object(SparkPerfReader, "get_job_concurrency")
    def test_all_empty(self, mock_jc, mock_ca, reader):
        mock_jc.return_value = []
        mock_ca.return_value = []
        result = reader.get_concurrency_summary("app-1")
        assert result == {}


class TestExecutorSummary:
    """Tests for get_executor_summary composite method."""

    @patch.object(SparkPerfReader, "get_executor_analysis")
    def test_basic_summary(self, mock_ea, reader):
        mock_ea.return_value = [
            {
                "executor_id": "0",
                "cpu_efficiency_pct": 40.0,
                "gc_overhead_pct": 5.0,
                "disk_spill_mb": 100.0,
                "memory_spill_mb": 200.0,
                "serialization_pct": 10.0,
                "resource_diagnosis": "MEMORY_PRESSURE,LOW_CPU",
                "is_straggler": "YES",
                "load_vs_avg": 1.5,
                "total_cores": 4,
                "executor_memory_mb": 24468,
                "offheap_size_mb": 0,
            },
            {
                "executor_id": "1",
                "cpu_efficiency_pct": 50.0,
                "gc_overhead_pct": 3.0,
                "disk_spill_mb": 50.0,
                "memory_spill_mb": 100.0,
                "serialization_pct": 8.0,
                "resource_diagnosis": "LOW_CPU",
                "is_straggler": "NO",
                "load_vs_avg": 0.8,
                "total_cores": 4,
                "executor_memory_mb": 24468,
                "offheap_size_mb": 0,
            },
        ]
        result = reader.get_executor_summary("app-1")
        assert result["executor_count"] == 2
        assert result["avg_cpu_efficiency_pct"] == 45.0
        assert result["total_disk_spill_mb"] == 150.0
        assert result["straggler_count"] == 1
        assert result["diagnosis_counts"]["MEMORY_PRESSURE"] == 1
        assert result["diagnosis_counts"]["LOW_CPU"] == 2

    @patch.object(SparkPerfReader, "get_executor_analysis")
    def test_empty_executors(self, mock_ea, reader):
        mock_ea.return_value = []
        assert reader.get_executor_summary("app-1") == {}

    @patch.object(SparkPerfReader, "get_executor_analysis")
    def test_underutilized_detection(self, mock_ea, reader):
        mock_ea.return_value = [
            {
                "executor_id": "0",
                "cpu_efficiency_pct": 30.0,
                "gc_overhead_pct": 1.0,
                "disk_spill_mb": 0,
                "memory_spill_mb": 0,
                "serialization_pct": 0,
                "resource_diagnosis": "",
                "is_straggler": "NO",
                "load_vs_avg": 0.2,
                "total_cores": 4,
                "executor_memory_mb": 24468,
                "offheap_size_mb": 0,
            },
        ]
        result = reader.get_executor_summary("app-1")
        assert result["underutilized_count"] == 1

    @patch.object(SparkPerfReader, "get_executor_analysis")
    def test_null_metrics_marked_as_no_data(self, mock_ea, reader):
        """When cpu_efficiency_pct etc. are None, result should say 'no data' not 0%."""
        mock_ea.return_value = [
            {
                "executor_id": "0",
                "cpu_efficiency_pct": None,
                "gc_overhead_pct": None,
                "disk_spill_mb": 0,
                "memory_spill_mb": 0,
                "serialization_pct": None,
                "resource_diagnosis": "",
                "is_straggler": "NO",
                "load_vs_avg": 1.0,
                "total_cores": 4,
                "executor_memory_mb": 24468,
                "offheap_size_mb": 0,
            },
        ]
        result = reader.get_executor_summary("app-1")
        assert result["avg_cpu_efficiency_pct"] is None
        assert result["cpu_efficiency_note"] == "no data recorded"
        assert result["avg_gc_pct"] is None
        assert result["gc_note"] == "no data recorded"
        assert result["avg_serialization_pct"] is None

    @patch.object(SparkPerfReader, "get_executor_analysis")
    def test_zero_cpu_is_not_no_data(self, mock_ea, reader):
        """When cpu_efficiency_pct = 0.0 (actual zero), it should report 0.0 not 'no data'."""
        mock_ea.return_value = [
            {
                "executor_id": "0",
                "cpu_efficiency_pct": 0.0,
                "gc_overhead_pct": 0.0,
                "disk_spill_mb": 0,
                "memory_spill_mb": 0,
                "serialization_pct": 0.0,
                "resource_diagnosis": "",
                "is_straggler": "NO",
                "load_vs_avg": 1.0,
                "total_cores": 4,
                "executor_memory_mb": 24468,
                "offheap_size_mb": 0,
            },
        ]
        result = reader.get_executor_summary("app-1")
        assert result["avg_cpu_efficiency_pct"] == 0.0
        assert "cpu_efficiency_note" not in result
        assert result["avg_gc_pct"] == 0.0
        assert "gc_note" not in result


class TestAutoscaleCostSummary:
    """Tests for get_autoscale_cost_summary composite method."""

    @patch.object(SparkPerfReader, "get_application_summary")
    @patch.object(SparkPerfReader, "get_autoscale_timeline")
    def test_basic_cost_distribution(self, mock_at, mock_as, reader):
        mock_at.return_value = [
            {
                "worker_count_after": 1,
                "event_ts": "2026-04-04 04:41:17",
                "event_type": "EXECUTOR_ADDED",
            },
            {
                "worker_count_after": 5,
                "event_ts": "2026-04-04 04:45:50",
                "event_type": "EXECUTOR_ADDED",
            },
            {
                "worker_count_after": 10,
                "event_ts": "2026-04-04 04:49:00",
                "event_type": "EXECUTOR_ADDED",
            },
            {
                "worker_count_after": 0,
                "event_ts": "2026-04-04 05:05:00",
                "event_type": "EXECUTOR_REMOVED",
            },
        ]
        mock_as.return_value = {
            "start_ts": "2026-04-04 04:41:00",
            "end_ts": "2026-04-04 05:05:10",
        }
        result = reader.get_autoscale_cost_summary("app-1")
        # Worker counts present: 1, 5, 10, and possibly 0 from synthetic end event
        wcs = {r["worker_count"] for r in result}
        assert {1, 5, 10}.issubset(wcs)
        # Worker count 10 should have the most time (~16 min)
        wc10 = [r for r in result if r["worker_count"] == 10][0]
        assert wc10["pct_of_total"] > 50

    @patch.object(SparkPerfReader, "get_autoscale_timeline")
    def test_empty_timeline(self, mock_at, reader):
        mock_at.return_value = []
        assert reader.get_autoscale_cost_summary("app-1") == []

    @patch.object(SparkPerfReader, "get_application_summary")
    @patch.object(SparkPerfReader, "get_autoscale_timeline")
    def test_clips_events_outside_app_window(self, mock_at, mock_as, reader):
        """Events after end_ts (e.g., cluster cleanup) must be excluded."""
        mock_at.return_value = [
            {"worker_count_after": 10, "event_ts": "2026-04-03 00:04:33"},
            # These events are long after the app ends:
            {"worker_count_after": 5, "event_ts": "2026-04-03 01:00:00"},
            {"worker_count_after": 0, "event_ts": "2026-04-03 03:15:59"},
        ]
        mock_as.return_value = {
            "start_ts": "2026-04-03 00:04:24",
            "end_ts": "2026-04-03 00:06:42",  # 2.3 min app
        }
        result = reader.get_autoscale_cost_summary("app-1")
        # Only wc=10 should be recorded (for the 2.3 min app duration)
        assert all(r["worker_count"] == 10 for r in result)
        # Total duration should match app duration (~2 min), not 3 hours
        total_min = sum(r["cumulative_min"] for r in result)
        assert total_min < 3.0, f"Expected <3 min, got {total_min}"

    @patch.object(SparkPerfReader, "get_application_summary")
    @patch.object(SparkPerfReader, "get_autoscale_timeline")
    def test_extends_to_end_ts_for_steady_state(self, mock_at, mock_as, reader):
        """Scale-up in 1 sec then steady state for 11 min: should capture 11 min at final wc."""
        mock_at.return_value = [
            {"worker_count_after": 1, "event_ts": "2026-04-03 08:24:29.100"},
            {"worker_count_after": 10, "event_ts": "2026-04-03 08:24:30.300"},
        ]
        mock_as.return_value = {
            "start_ts": "2026-04-03 08:24:29.000",
            "end_ts": "2026-04-03 08:35:51.900",  # ~11.4 min later
        }
        result = reader.get_autoscale_cost_summary("app-1")
        wc10 = [r for r in result if r["worker_count"] == 10][0]
        # Steady state at wc=10 from 08:24:30 to 08:35:51 ≈ 11.3 min
        assert wc10["cumulative_min"] > 10.0
        assert wc10["pct_of_total"] > 95  # wc=10 dominates

    @patch.object(SparkPerfReader, "get_application_summary")
    @patch.object(SparkPerfReader, "get_autoscale_timeline")
    def test_missing_app_window_falls_back_to_event_deltas(self, mock_at, mock_as, reader):
        """If start_ts/end_ts are missing, behave like the old un-clipped aggregation."""
        mock_at.return_value = [
            {"worker_count_after": 1, "event_ts": "2026-04-04 04:41:17"},
            {"worker_count_after": 10, "event_ts": "2026-04-04 04:45:00"},
        ]
        mock_as.return_value = {}  # no start_ts/end_ts
        result = reader.get_autoscale_cost_summary("app-1")
        # Should still compute delta between the 2 events
        assert len(result) == 1
        assert result[0]["worker_count"] == 1


class TestScalingEventCounts:
    """Tests for get_scaling_event_counts composite method."""

    @patch.object(SparkPerfReader, "get_spot_instance_analysis")
    @patch.object(SparkPerfReader, "get_autoscale_timeline")
    def test_mixed_events(self, mock_at, mock_spot, reader):
        mock_at.return_value = [
            {"event_type": "EXECUTOR_ADDED", "event_reason": ""},
            {"event_type": "EXECUTOR_ADDED", "event_reason": ""},
            {"event_type": "EXECUTOR_REMOVED", "event_reason": "AUTOSCALE_IN"},
            {"event_type": "EXECUTOR_REMOVED", "event_reason": "cluster termination"},
        ]
        mock_spot.return_value = []
        result = reader.get_scaling_event_counts("app-1")
        assert result["SCALE_OUT"] == 2
        assert result["AUTOSCALE_IN"] == 1
        assert result["CLUSTER_SHUTDOWN"] == 1
        assert "SPOT_PREEMPTION" not in result  # zero counts excluded

    @patch.object(SparkPerfReader, "get_spot_instance_analysis")
    @patch.object(SparkPerfReader, "get_autoscale_timeline")
    def test_spot_preemption_from_spot_table(self, mock_at, mock_spot, reader):
        mock_at.return_value = []
        mock_spot.return_value = [
            {"is_unexpected_loss": True},
            {"is_unexpected_loss": True},
        ]
        result = reader.get_scaling_event_counts("app-1")
        assert result["NODE_LOST"] == 2


class TestSqlPlanTopN:
    """Tests for get_sql_plan_top_n composite method."""

    _COLUMNS = [
        ("execution_id",),
        ("description_short",),
        ("duration_sec",),
        ("total_operators",),
        ("photon_operators",),
        ("photon_pct",),
        ("bhj_count",),
        ("photon_bhj_count",),
        ("smj_count",),
        ("total_join_count",),
        ("non_photon_op_list",),
        ("scan_tables",),
        ("scan_filters",),
        ("scan_column_count",),
    ]

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_top_n_results(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = self._COLUMNS
        mock_cursor.fetchall.return_value = [
            (
                1,
                "SELECT * FROM t1",
                125.3,
                47,
                38,
                80.9,
                3,
                2,
                1,
                6,
                "Sort,Exchange",
                "cat.sch.t1",
                "col1 > 0",
                12,
            ),
            (
                2,
                "INSERT INTO t2 SEL",
                80.1,
                30,
                25,
                83.3,
                1,
                1,
                0,
                2,
                "HashAggregate",
                "cat.sch.t2",
                None,
                5,
            ),
            (
                3,
                "MERGE INTO t3",
                45.0,
                20,
                10,
                50.0,
                0,
                0,
                2,
                2,
                "SortMergeJoin",
                "cat.sch.t3",
                "id = 1",
                3,
            ),
        ]
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_sql_plan_top_n("app-1")
        assert len(result) == 3
        assert result[0]["execution_id"] == 1
        assert result[0]["duration_sec"] == 125.3
        assert result[0]["photon_pct"] == 80.9
        assert result[0]["smj_count"] == 1
        assert result[2]["non_photon_op_list"] == "SortMergeJoin"

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_empty_on_error(self, mock_conn, reader):
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("query failed")
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_sql_plan_top_n("app-1")
        assert result == []

    @patch.object(SparkPerfReader, "_get_connection")
    def test_custom_limit(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = self._COLUMNS
        mock_cursor.fetchall.return_value = []
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        reader.get_sql_plan_top_n("app-1", limit=5)
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "LIMIT 5" in executed_sql


class TestSerializationSummary:
    """Tests for get_serialization_summary composite method."""

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_summary_dict(self, mock_conn, reader):
        # First call: column check query
        check_cursor = MagicMock()
        check_cursor.description = [("stage_id",), ("deserialize_ms",), ("result_serialize_ms",)]
        check_cursor.fetchall.return_value = []
        # Second call: aggregation query
        agg_cursor = MagicMock()
        agg_cursor.description = [
            ("total_deserialize_ms",),
            ("total_result_serialize_ms",),
            ("total_serialization_ms",),
            ("total_exec_run_ms",),
            ("serialization_pct",),
            ("stages_with_high_ser",),
        ]
        agg_cursor.fetchone.return_value = (5000, 3000, 8000, 80000, 10.0, 3)

        # Two connections opened sequentially
        conn1 = MagicMock()
        conn1.__enter__ = lambda s: s
        conn1.__exit__ = MagicMock(return_value=False)
        conn1.cursor.return_value.__enter__ = lambda s: check_cursor
        conn1.cursor.return_value.__exit__ = MagicMock(return_value=False)

        conn2 = MagicMock()
        conn2.__enter__ = lambda s: s
        conn2.__exit__ = MagicMock(return_value=False)
        conn2.cursor.return_value.__enter__ = lambda s: agg_cursor
        conn2.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_conn.side_effect = [conn1, conn2]

        result = reader.get_serialization_summary("app-1")
        assert result["total_serialization_ms"] == 8000
        assert result["stages_with_high_ser"] == 3

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_empty_on_error(self, mock_conn, reader):
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("query failed")
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_serialization_summary("app-1")
        assert result == {}


class TestUdfAnalysis:
    """Tests for get_udf_analysis method."""

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_udf_rows(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("execution_id",),
            ("description_short",),
            ("duration_sec",),
            ("non_photon_op_list",),
        ]
        mock_cursor.fetchall.return_value = [
            (1, "SELECT udf(col)", 25.0, "BatchEvalPython, Sort"),
            (2, "SELECT pandas_udf(col)", 10.0, "ArrowEvalPython, Exchange"),
        ]
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_udf_analysis("app-1")
        assert len(result) == 2
        assert result[0]["non_photon_op_list"] == "BatchEvalPython, Sort"

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_empty_when_no_udfs(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("execution_id",),
            ("description_short",),
            ("duration_sec",),
            ("non_photon_op_list",),
        ]
        mock_cursor.fetchall.return_value = []
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_udf_analysis("app-1")
        assert result == []


class TestSkewAnalysis:
    """Tests for get_skew_analysis composite method."""

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_skew_data(self, mock_conn, reader):
        # First call: skew stages
        stage_cursor = MagicMock()
        stage_cursor.description = [
            ("stage_id",),
            ("stage_name",),
            ("duration_ms",),
            ("task_skew_ratio",),
            ("shuffle_skew_ratio",),
            ("data_skew_gap_mb",),
            ("task_p50_ms",),
            ("task_max_ms",),
            ("task_count",),
            ("shuffle_read_mb",),
        ]
        stage_cursor.fetchall.return_value = [
            (20, "broadcastHashJoin", 305000, 31.3, 12.5, 450.0, 100, 3130, 200, 500.0),
        ]
        # Second call: join SQLs
        join_cursor = MagicMock()
        join_cursor.description = [
            ("execution_id",),
            ("description_short",),
            ("duration_sec",),
            ("smj_count",),
            ("bhj_count",),
            ("total_join_count",),
        ]
        join_cursor.fetchall.return_value = [
            (1, "SELECT * FROM t1 JOIN t2", 125.0, 2, 1, 3),
        ]

        conn1 = MagicMock()
        conn1.__enter__ = lambda s: s
        conn1.__exit__ = MagicMock(return_value=False)
        conn1.cursor.return_value.__enter__ = lambda s: stage_cursor
        conn1.cursor.return_value.__exit__ = MagicMock(return_value=False)

        conn2 = MagicMock()
        conn2.__enter__ = lambda s: s
        conn2.__exit__ = MagicMock(return_value=False)
        conn2.cursor.return_value.__enter__ = lambda s: join_cursor
        conn2.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_conn.side_effect = [conn1, conn2]

        result = reader.get_skew_analysis("app-1")
        assert len(result["skew_stages"]) == 1
        assert result["skew_stages"][0]["task_skew_ratio"] == 31.3
        assert len(result["join_sqls"]) == 1
        assert result["skew_summary"]["has_smj"] is True

    @patch.object(SparkPerfReader, "_get_connection")
    def test_no_skew_returns_empty(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [("stage_id",)]
        mock_cursor.fetchall.return_value = []
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_skew_analysis("app-1")
        assert result == {}

    @patch.object(SparkPerfReader, "_get_connection")
    def test_error_returns_empty(self, mock_conn, reader):
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("query failed")
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_skew_analysis("app-1")
        assert result == {}


class TestDriverRiskAnalysis:
    """Tests for get_driver_risk_analysis composite method."""

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_risk_data(self, mock_conn, reader):
        # First call: column check
        check_cursor = MagicMock()
        check_cursor.description = [("stage_id",), ("result_size_mb",), ("failure_reason",)]
        check_cursor.fetchall.return_value = []
        # Second call: risk stages
        stage_cursor = MagicMock()
        stage_cursor.description = [
            ("stage_id",),
            ("stage_name",),
            ("total_result_size_mb",),
            ("task_count",),
            ("failure_reason",),
        ]
        stage_cursor.fetchall.return_value = [
            (5, "collect at MyApp", 512.0, 100, None),
            (8, "failed stage", 0.0, 50, "java.lang.OutOfMemoryError: Java heap space"),
        ]
        # Third call: collect operators from SQL plans
        sql_cursor = MagicMock()
        sql_cursor.description = [
            ("execution_id",),
            ("description_short",),
            ("duration_sec",),
            ("non_photon_op_list",),
        ]
        sql_cursor.fetchall.return_value = [
            (1, "SELECT collect_list(x)", 10.0, "CollectLimit, Sort"),
        ]

        conn1 = MagicMock()
        conn1.__enter__ = lambda s: s
        conn1.__exit__ = MagicMock(return_value=False)
        conn1.cursor.return_value.__enter__ = lambda s: check_cursor
        conn1.cursor.return_value.__exit__ = MagicMock(return_value=False)

        conn2 = MagicMock()
        conn2.__enter__ = lambda s: s
        conn2.__exit__ = MagicMock(return_value=False)
        conn2.cursor.return_value.__enter__ = lambda s: stage_cursor
        conn2.cursor.return_value.__exit__ = MagicMock(return_value=False)

        conn3 = MagicMock()
        conn3.__enter__ = lambda s: s
        conn3.__exit__ = MagicMock(return_value=False)
        conn3.cursor.return_value.__enter__ = lambda s: sql_cursor
        conn3.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_conn.side_effect = [conn1, conn2, conn3]

        result = reader.get_driver_risk_analysis("app-1")
        assert result["oom_stages"] == 1
        assert result["total_result_size_mb"] > 0
        assert len(result["collect_operators"]) == 1

    @patch.object(SparkPerfReader, "_get_connection")
    def test_no_risk_returns_empty(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [("stage_id",), ("total_result_size_mb",), ("failure_reason",)]
        mock_cursor.fetchall.return_value = []
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_driver_risk_analysis("app-1")
        assert result == {}

    @patch.object(SparkPerfReader, "_get_connection")
    def test_driver_risk_error_returns_empty(self, mock_conn, reader):
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("query failed")
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_driver_risk_analysis("app-1")
        assert result == {}


class TestStreamingDeepAnalysis:
    """Tests for get_streaming_deep_analysis composite method."""

    @patch.object(SparkPerfReader, "_get_connection")
    def test_returns_state_growth(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("query_id",),
            ("batch_id",),
            ("state_memory_used_bytes",),
            ("state_num_rows_total",),
            ("state_rows_dropped_by_watermark",),
            ("watermark",),
            ("source_num_files_outstanding",),
            ("source_num_bytes_outstanding",),
        ]
        mock_cursor.fetchall.return_value = [
            ("q1", 1, 10_000_000, 1000, 0, "2026-04-11T00:00:00", None, None),
            ("q1", 2, 20_000_000, 2000, 50, "2026-04-11T00:01:00", None, None),
            ("q1", 3, 28_000_000, 2800, 100, "2026-04-11T00:02:00", None, None),
        ]
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_streaming_deep_analysis("app-1")
        assert "state_growth" in result
        assert result["state_growth"]["first_state_mb"] > 0
        assert result["state_growth"]["last_state_mb"] > result["state_growth"]["first_state_mb"]

    @patch.object(SparkPerfReader, "_get_connection")
    def test_no_streaming_returns_empty(self, mock_conn, reader):
        mock_cursor = MagicMock()
        mock_cursor.description = [("query_id",)]
        mock_cursor.fetchall.return_value = []
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_streaming_deep_analysis("app-1")
        assert result == {}

    @patch.object(SparkPerfReader, "_get_connection")
    def test_error_returns_empty(self, mock_conn, reader):
        mock_conn.return_value.__enter__ = lambda s: s
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("query failed")
        mock_conn.return_value.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.return_value.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = reader.get_streaming_deep_analysis("app-1")
        assert result == {}


# ---------------------------------------------------------------------------
# get_parallelism_analysis
# ---------------------------------------------------------------------------


def _make_stage(
    stage_id: int = 0,
    stage_name: str = "scan",
    task_count: int = 10,
    input_mb: float = 0,
    output_mb: float = 0,
    shuffle_read_mb: float = 0,
    bottleneck_type: str = "IO_BOUND",
) -> dict:
    return {
        "stage_id": stage_id,
        "stage_name": stage_name,
        "task_count": task_count,
        "input_mb": input_mb,
        "output_mb": output_mb,
        "shuffle_read_mb": shuffle_read_mb,
        "bottleneck_type": bottleneck_type,
    }


class TestGetParallelismAnalysis:
    """Tests for SparkPerfReader.get_parallelism_analysis()."""

    def test_empty_stages_returns_empty(self, reader):
        with patch.object(reader, "get_stage_performance", return_value=[]):
            result = reader.get_parallelism_analysis("app-1")
        assert result == {}

    def test_skipped_stages_ignored(self, reader):
        stages = [_make_stage(bottleneck_type="SKIPPED", input_mb=10000, task_count=2)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert result == {}

    def test_no_issues_returns_empty(self, reader):
        # 100MB / 10 tasks = 10MB/task — within normal range
        stages = [_make_stage(input_mb=100, task_count=10)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert result == {}

    def test_read_low_detected(self, reader):
        # 3000MB / 4 tasks = 750MB/task > 256MB → READ_LOW
        stages = [_make_stage(stage_id=1, input_mb=3000, task_count=4)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert result["total_issues"] == 1
        assert result["type_counts"]["READ_LOW"] == 1
        assert result["issues"][0]["type"] == "READ_LOW"
        assert result["issues"][0]["severity"] == "MEDIUM"

    def test_read_high_detected(self, reader):
        # 50MB / 200 tasks = 0.25MB/task < 1MB & tasks > 100 → READ_HIGH
        stages = [_make_stage(stage_id=2, input_mb=50, task_count=200)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert result["total_issues"] == 1
        assert result["type_counts"]["READ_HIGH"] == 1
        assert result["issues"][0]["severity"] == "LOW"

    def test_write_low_detected(self, reader):
        # 2000MB / 4 tasks = 500MB/task > 256MB → WRITE_LOW
        stages = [_make_stage(stage_id=3, output_mb=2000, task_count=4)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert result["total_issues"] == 1
        assert result["type_counts"]["WRITE_LOW"] == 1

    def test_write_high_detected(self, reader):
        # 30MB / 200 tasks = 0.15MB/task < 1MB & tasks > 100 → WRITE_HIGH
        stages = [_make_stage(stage_id=4, output_mb=30, task_count=200)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert result["total_issues"] == 1
        assert result["type_counts"]["WRITE_HIGH"] == 1

    def test_shuffle_low_detected(self, reader):
        # 4000MB / 8 tasks = 500MB/task > 256MB → SHUFFLE_LOW
        stages = [_make_stage(stage_id=5, shuffle_read_mb=4000, task_count=8)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert result["total_issues"] == 1
        assert result["type_counts"]["SHUFFLE_LOW"] == 1

    def test_multiple_issues_across_stages(self, reader):
        stages = [
            _make_stage(stage_id=1, input_mb=3000, task_count=4),  # READ_LOW
            _make_stage(stage_id=2, output_mb=30, task_count=200),  # WRITE_HIGH
            _make_stage(stage_id=3, shuffle_read_mb=4000, task_count=8),  # SHUFFLE_LOW
        ]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert result["total_issues"] == 3
        assert result["analyzed_stage_count"] == 3
        assert len(result["worst_issues"]) == 3

    def test_enriched_fields_present(self, reader):
        stages = [_make_stage(stage_id=1, input_mb=3000, task_count=4)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert "analyzed_stage_count" in result
        assert "total_stage_count" in result
        assert "worst_issues" in result
        assert result["analyzed_stage_count"] >= 1
        assert result["issues"][0]["mb_per_task"] == 750

    def test_recommendations_in_english(self, reader):
        stages = [
            _make_stage(stage_id=1, input_mb=3000, task_count=4),
            _make_stage(stage_id=2, output_mb=2000, task_count=4),
            _make_stage(stage_id=3, shuffle_read_mb=4000, task_count=8),
        ]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        for issue in result["issues"]:
            # All recommendations should be in English
            assert all(ord(c) < 128 for c in issue["recommendation"]), (
                f"Non-ASCII in recommendation: {issue['recommendation']}"
            )

    def test_issues_limited_to_10(self, reader):
        # 15 stages each with READ_LOW
        stages = [_make_stage(stage_id=i, input_mb=3000, task_count=4) for i in range(15)]
        with patch.object(reader, "get_stage_performance", return_value=stages):
            result = reader.get_parallelism_analysis("app-1")
        assert len(result["issues"]) == 10
        assert result["total_issues"] == 15
