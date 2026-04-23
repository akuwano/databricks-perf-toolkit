"""Tests for extract_stage_info() and extract_data_flow()."""

from core.extractors import extract_data_flow, extract_stage_info
from core.models import DataFlowEntry, StageInfo

# --- Fixtures ---

SAMPLE_PROFILE = {
    "query": {"id": "test-query"},
    "graphs": [
        {"nodes": [{"id": "0", "name": "Summary"}]},
        {
            "nodes": [
                {
                    "id": "100",
                    "name": "Scan table_a",
                    "tag": "SCAN_EXEC",
                    "keyMetrics": {
                        "rowsNum": 1950176,
                        "durationMs": 1000,
                        "peakMemoryBytes": 16000000,
                    },
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "200",
                    "name": "Scan table_b",
                    "tag": "SCAN_EXEC",
                    "keyMetrics": {
                        "rowsNum": 24110484,
                        "durationMs": 6000,
                        "peakMemoryBytes": 184000000,
                    },
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "300",
                    "name": "Inner Join",
                    "tag": "PHOTON_BROADCAST_HASH_JOIN_EXEC",
                    "keyMetrics": {
                        "rowsNum": 13200000000,
                        "durationMs": 279000,
                        "peakMemoryBytes": 1970000000,
                    },
                    "metadata": [
                        {"key": "JOIN_TYPE", "label": "Type", "value": "Inner", "values": []},
                        {
                            "key": "LEFT_KEYS",
                            "label": "Left keys",
                            "values": ["a.user_id"],
                            "metaValues": [{"value": "a.user_id"}],
                        },
                        {
                            "key": "RIGHT_KEYS",
                            "label": "Right keys",
                            "values": ["b.user_id"],
                            "metaValues": [{"value": "b.user_id"}],
                        },
                    ],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "400",
                    "name": "LEFT OUTER JOIN",
                    "tag": "PHOTON_SHUFFLED_HASH_JOIN_EXEC",
                    "keyMetrics": {
                        "rowsNum": 5520000000000,
                        "durationMs": 37500,
                        "peakMemoryBytes": 73000000000,
                    },
                    "metadata": [
                        {"key": "JOIN_TYPE", "label": "Type", "value": "Left Outer", "values": []},
                        {
                            "key": "LEFT_KEYS",
                            "label": "Left keys",
                            "values": ["user_id", "quest_name"],
                            "metaValues": [{"value": "user_id"}, {"value": "quest_name"}],
                        },
                        {
                            "key": "RIGHT_KEYS",
                            "label": "Right keys",
                            "values": ["user_id", "quest_name"],
                            "metaValues": [{"value": "user_id"}, {"value": "quest_name"}],
                        },
                    ],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "500",
                    "name": "Shuffle",
                    "tag": "SHUFFLE_EXEC",
                    "keyMetrics": {"rowsNum": 1720000000, "durationMs": 2300, "peakMemoryBytes": 0},
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
                {
                    "id": "999",
                    "name": "Write",
                    "tag": "WRITE_EXEC",
                    "keyMetrics": {"rowsNum": 0, "durationMs": 100, "peakMemoryBytes": 0},
                    "metadata": [],
                    "metrics": [],
                    "hidden": False,
                },
            ],
            "edges": [
                {"fromId": "100", "toId": "300"},
                {"fromId": "200", "toId": "300"},
                {"fromId": "300", "toId": "400"},
                {"fromId": "400", "toId": "500"},
                {"fromId": "500", "toId": "999"},
            ],
            "stageData": [
                {
                    "stageId": "1",
                    "status": "COMPLETE",
                    "keyMetrics": {"durationMs": 9000},
                    "numTasks": 49,
                    "numCompleteTasks": 49,
                    "numFailedTasks": 0,
                    "numKilledTasks": 0,
                    "numActiveTasks": 0,
                    "numCompletedIndices": 49,
                },
                {
                    "stageId": "2",
                    "status": "COMPLETE",
                    "keyMetrics": {"durationMs": 16500},
                    "numTasks": 73,
                    "numCompleteTasks": 73,
                    "numFailedTasks": 0,
                    "numKilledTasks": 0,
                    "numActiveTasks": 0,
                    "numCompletedIndices": 73,
                },
                {
                    "stageId": "3",
                    "status": "SKIPPED",
                    "keyMetrics": {},
                    "numTasks": 10,
                    "numCompleteTasks": 0,
                    "numFailedTasks": 0,
                    "numKilledTasks": 0,
                    "numActiveTasks": 0,
                    "numCompletedIndices": 0,
                },
                {
                    "stageId": "4",
                    "status": "FAILED",
                    "keyMetrics": {"durationMs": 18576000},
                    "numTasks": 393,
                    "numCompleteTasks": 0,
                    "numFailedTasks": 947,
                    "numKilledTasks": 0,
                    "numActiveTasks": 0,
                    "numCompletedIndices": 0,
                    "failureReason": "OOM Kill (Exit Code 137)",
                },
            ],
        },
    ],
}


# --- extract_stage_info tests ---


class TestExtractStageInfo:
    def test_extracts_all_stages(self):
        stages = extract_stage_info(SAMPLE_PROFILE)
        assert len(stages) == 4

    def test_stage_fields(self):
        stages = extract_stage_info(SAMPLE_PROFILE)
        s1 = stages[0]
        assert isinstance(s1, StageInfo)
        assert s1.stage_id == "1"
        assert s1.status == "COMPLETE"
        assert s1.duration_ms == 9000
        assert s1.num_tasks == 49
        assert s1.num_complete_tasks == 49
        assert s1.num_failed_tasks == 0

    def test_failed_stage(self):
        stages = extract_stage_info(SAMPLE_PROFILE)
        failed = [s for s in stages if s.is_failed]
        assert len(failed) == 1
        assert failed[0].stage_id == "4"
        assert failed[0].num_failed_tasks == 947
        assert failed[0].duration_ms == 18576000

    def test_skipped_stage(self):
        stages = extract_stage_info(SAMPLE_PROFILE)
        skipped = [s for s in stages if s.status == "SKIPPED"]
        assert len(skipped) == 1
        assert skipped[0].duration_ms == 0

    def test_empty_graphs(self):
        stages = extract_stage_info({"query": {}, "graphs": []})
        assert stages == []

    def test_no_stage_data(self):
        data = {"query": {}, "graphs": [{"nodes": []}, {"nodes": [], "edges": []}]}
        stages = extract_stage_info(data)
        assert stages == []

    def test_old_format_snake_case(self):
        """Old format uses snake_case keys."""
        import json

        data = {
            "query": {},
            "graphs": [
                "{}",  # Old format: JSON string
                json.dumps(
                    {
                        "nodes": [],
                        "edges": [],
                        "stage_data": [
                            {
                                "stage_id": "10",
                                "status": "COMPLETE",
                                "key_metrics": {"duration_ms": 5000},
                                "num_tasks": 20,
                                "num_complete_tasks": 20,
                                "num_failed_tasks": 0,
                                "num_killed_tasks": 0,
                            }
                        ],
                    }
                ),
            ],
        }
        stages = extract_stage_info(data)
        assert len(stages) == 1
        assert stages[0].stage_id == "10"
        assert stages[0].duration_ms == 5000


# --- extract_data_flow tests ---


class TestExtractDataFlow:
    def test_extracts_scan_and_join_nodes(self):
        flow = extract_data_flow(SAMPLE_PROFILE)
        operations = [e.operation for e in flow]
        assert any("Scan" in op for op in operations)
        assert any("Join" in op or "JOIN" in op for op in operations)

    def test_entry_fields(self):
        flow = extract_data_flow(SAMPLE_PROFILE)
        assert all(isinstance(e, DataFlowEntry) for e in flow)
        scan_entries = [e for e in flow if "Scan" in e.operation]
        assert len(scan_entries) >= 1
        assert scan_entries[0].output_rows > 0
        assert scan_entries[0].duration_ms > 0

    def test_join_keys_extracted(self):
        flow = extract_data_flow(SAMPLE_PROFILE)
        join_entries = [e for e in flow if "Join" in e.operation or "JOIN" in e.operation]
        assert len(join_entries) >= 1
        assert any(e.join_keys != "" for e in join_entries)

    def test_data_explosion_visible(self):
        """Row count should increase through JOINs, showing data explosion."""
        flow = extract_data_flow(SAMPLE_PROFILE)
        rows = [e.output_rows for e in flow if e.output_rows > 0]
        assert len(rows) >= 2
        # At least one entry should show significantly more rows than the smallest
        assert max(rows) > min(rows) * 10

    def test_empty_graphs(self):
        flow = extract_data_flow({"query": {}, "graphs": []})
        assert flow == []

    def test_order_follows_dag(self):
        """Entries should be ordered from sources to sinks."""
        flow = extract_data_flow(SAMPLE_PROFILE)
        if len(flow) >= 2:
            # Scan nodes should come before JOIN nodes
            scan_indices = [i for i, e in enumerate(flow) if "Scan" in e.operation]
            join_indices = [
                i for i, e in enumerate(flow) if "Join" in e.operation or "JOIN" in e.operation
            ]
            if scan_indices and join_indices:
                assert min(scan_indices) < min(join_indices)
