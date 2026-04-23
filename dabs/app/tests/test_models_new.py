"""Tests for new data models: StageInfo and DataFlowEntry."""

from core.models import DataFlowEntry, ProfileAnalysis, StageInfo


class TestStageInfo:
    """Tests for StageInfo dataclass."""

    def test_default_values(self):
        stage = StageInfo()
        assert stage.stage_id == ""
        assert stage.status == ""
        assert stage.duration_ms == 0
        assert stage.num_tasks == 0
        assert stage.num_complete_tasks == 0
        assert stage.num_failed_tasks == 0
        assert stage.num_killed_tasks == 0
        assert stage.note == ""

    def test_creation_with_values(self):
        stage = StageInfo(
            stage_id="5364",
            status="FAILED",
            duration_ms=18576000,
            num_tasks=393,
            num_complete_tasks=0,
            num_failed_tasks=947,
            num_killed_tasks=0,
            note="OOM Kill",
        )
        assert stage.stage_id == "5364"
        assert stage.status == "FAILED"
        assert stage.duration_ms == 18576000
        assert stage.num_tasks == 393
        assert stage.num_failed_tasks == 947
        assert stage.note == "OOM Kill"

    def test_is_failed_property(self):
        failed = StageInfo(status="FAILED")
        assert failed.is_failed is True

        complete = StageInfo(status="COMPLETE")
        assert complete.is_failed is False

        skipped = StageInfo(status="SKIPPED")
        assert skipped.is_failed is False


class TestDataFlowEntry:
    """Tests for DataFlowEntry dataclass."""

    def test_default_values(self):
        entry = DataFlowEntry()
        assert entry.node_id == ""
        assert entry.operation == ""
        assert entry.output_rows == 0
        assert entry.duration_ms == 0
        assert entry.peak_memory_bytes == 0
        assert entry.join_keys == ""

    def test_creation_with_values(self):
        entry = DataFlowEntry(
            node_id="355837",
            operation="LEFT OUTER JOIN",
            output_rows=5_520_000_000_000,
            duration_ms=37500,
            peak_memory_bytes=73 * 1024**3,
            join_keys="user_id, quest_name + inequality",
        )
        assert entry.node_id == "355837"
        assert entry.operation == "LEFT OUTER JOIN"
        assert entry.output_rows == 5_520_000_000_000
        assert entry.join_keys == "user_id, quest_name + inequality"

    def test_formatted_rows(self):
        """Output rows should be formattable for display."""
        entry = DataFlowEntry(output_rows=5_520_000_000_000)
        assert f"{entry.output_rows:,}" == "5,520,000,000,000"


class TestProfileAnalysisExtension:
    """Tests for new fields in ProfileAnalysis."""

    def test_stage_info_field_exists(self):
        analysis = ProfileAnalysis()
        assert hasattr(analysis, "stage_info")
        assert analysis.stage_info == []

    def test_data_flow_field_exists(self):
        analysis = ProfileAnalysis()
        assert hasattr(analysis, "data_flow")
        assert analysis.data_flow == []

    def test_stage_info_populated(self):
        stages = [
            StageInfo(stage_id="1", status="COMPLETE", duration_ms=9000),
            StageInfo(stage_id="2", status="FAILED", num_failed_tasks=947),
        ]
        analysis = ProfileAnalysis(stage_info=stages)
        assert len(analysis.stage_info) == 2
        assert analysis.stage_info[1].status == "FAILED"

    def test_data_flow_populated(self):
        flow = [
            DataFlowEntry(operation="Scan (wave)", output_rows=1_950_176),
            DataFlowEntry(operation="Inner Join", output_rows=13_200_000_000),
        ]
        analysis = ProfileAnalysis(data_flow=flow)
        assert len(analysis.data_flow) == 2
        assert analysis.data_flow[1].output_rows == 13_200_000_000
