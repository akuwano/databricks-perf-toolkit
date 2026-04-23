"""Tests for new QueryMetrics fields: result cache, queue time, fetch time, planning phases."""

from core.extractors import extract_query_metrics


def _make_profile(metrics: dict, **query_extras) -> dict:
    """Build a minimal nested profile structure."""
    query = {"id": "test-1", "status": "FINISHED", "queryText": "SELECT 1", **query_extras}
    query["metrics"] = {
        "totalTimeMs": 1000,
        "compilationTimeMs": 100,
        "executionTimeMs": 800,
        "readBytes": 0,
        "readRemoteBytes": 0,
        "readCacheBytes": 0,
        "spillToDiskBytes": 0,
        "photonTotalTimeMs": 0,
        "taskTotalTimeMs": 0,
        "readFilesCount": 0,
        "prunedFilesCount": 0,
        "prunedBytes": 0,
        "rowsReadCount": 0,
        "rowsProducedCount": 0,
        "bytesReadFromCachePercentage": 0,
        "writeRemoteBytes": 0,
        "writeRemoteFiles": 0,
        "networkSentBytes": 0,
        "readPartitionsCount": 0,
        **metrics,
    }
    return {"query": query, "graphs": []}


class TestResultFromCache:
    """result_from_cache field extraction."""

    def test_true(self):
        data = _make_profile({"resultFromCache": True})
        qm = extract_query_metrics(data)
        assert qm.result_from_cache is True

    def test_false(self):
        data = _make_profile({"resultFromCache": False})
        qm = extract_query_metrics(data)
        assert qm.result_from_cache is False

    def test_missing_defaults_to_false(self):
        data = _make_profile({})
        qm = extract_query_metrics(data)
        assert qm.result_from_cache is False

    def test_none_defaults_to_false(self):
        data = _make_profile({"resultFromCache": None})
        qm = extract_query_metrics(data)
        assert qm.result_from_cache is False


class TestResultFetchTimeMs:
    """resultFetchTimeMs field extraction."""

    def test_extracted(self):
        data = _make_profile({"resultFetchTimeMs": 5000})
        qm = extract_query_metrics(data)
        assert qm.result_fetch_time_ms == 5000

    def test_missing_defaults_to_zero(self):
        data = _make_profile({})
        qm = extract_query_metrics(data)
        assert qm.result_fetch_time_ms == 0

    def test_not_in_extra_metrics(self):
        """Mapped field should NOT appear in extra_metrics."""
        data = _make_profile({"resultFetchTimeMs": 100})
        qm = extract_query_metrics(data)
        assert "resultFetchTimeMs" not in qm.extra_metrics


class TestQueuedTimes:
    """queuedProvisioningTimeMs and queuedOverloadTimeMs extraction."""

    def test_provisioning_extracted(self):
        data = _make_profile({"queuedProvisioningTimeMs": 3000})
        qm = extract_query_metrics(data)
        assert qm.queued_provisioning_time_ms == 3000

    def test_overload_extracted(self):
        data = _make_profile({"queuedOverloadTimeMs": 1500})
        qm = extract_query_metrics(data)
        assert qm.queued_overload_time_ms == 1500

    def test_missing_defaults_to_zero(self):
        data = _make_profile({})
        qm = extract_query_metrics(data)
        assert qm.queued_provisioning_time_ms == 0
        assert qm.queued_overload_time_ms == 0

    def test_none_defaults_to_zero(self):
        data = _make_profile(
            {
                "queuedProvisioningTimeMs": None,
                "queuedOverloadTimeMs": None,
            }
        )
        qm = extract_query_metrics(data)
        assert qm.queued_provisioning_time_ms == 0
        assert qm.queued_overload_time_ms == 0

    def test_not_in_extra_metrics(self):
        data = _make_profile(
            {
                "queuedProvisioningTimeMs": 500,
                "queuedOverloadTimeMs": 200,
            }
        )
        qm = extract_query_metrics(data)
        assert "queuedProvisioningTimeMs" not in qm.extra_metrics
        assert "queuedOverloadTimeMs" not in qm.extra_metrics


class TestMetadataTimeMs:
    """metadataTimeMs extraction."""

    def test_extracted(self):
        data = _make_profile({"metadataTimeMs": 1277})
        qm = extract_query_metrics(data)
        assert qm.metadata_time_ms == 1277

    def test_missing_defaults_to_zero(self):
        data = _make_profile({})
        qm = extract_query_metrics(data)
        assert qm.metadata_time_ms == 0


class TestPlanningPhases:
    """planningPhases extraction."""

    def test_extracted(self):
        phases = [
            {"__typename": "PlanningPhase", "phase": "ANALYSIS", "durationMs": 2},
            {"__typename": "PlanningPhase", "phase": "OPTIMIZATION", "durationMs": 262},
            {"__typename": "PlanningPhase", "phase": "PLANNING", "durationMs": 4},
        ]
        data = _make_profile({"planningPhases": phases})
        qm = extract_query_metrics(data)
        assert len(qm.planning_phases) == 3
        assert qm.planning_phases[0] == {"phase": "ANALYSIS", "duration_ms": 2}
        assert qm.planning_phases[1] == {"phase": "OPTIMIZATION", "duration_ms": 262}
        assert qm.planning_phases[2] == {"phase": "PLANNING", "duration_ms": 4}

    def test_empty_list(self):
        data = _make_profile({"planningPhases": []})
        qm = extract_query_metrics(data)
        assert qm.planning_phases == []

    def test_missing_defaults_to_empty(self):
        data = _make_profile({})
        qm = extract_query_metrics(data)
        assert qm.planning_phases == []

    def test_none_defaults_to_empty(self):
        data = _make_profile({"planningPhases": None})
        qm = extract_query_metrics(data)
        assert qm.planning_phases == []

    def test_five_phases(self):
        """Real-world profile with 5 phases including PARSING and REPLANNING."""
        phases = [
            {"__typename": "PlanningPhase", "phase": "ANALYSIS", "durationMs": 1597},
            {"__typename": "PlanningPhase", "phase": "PARSING", "durationMs": 8},
            {"__typename": "PlanningPhase", "phase": "REPLANNING", "durationMs": 86},
            {"__typename": "PlanningPhase", "phase": "OPTIMIZATION", "durationMs": 1497},
            {"__typename": "PlanningPhase", "phase": "PLANNING", "durationMs": 100},
        ]
        data = _make_profile({"planningPhases": phases})
        qm = extract_query_metrics(data)
        assert len(qm.planning_phases) == 5
        phase_names = [p["phase"] for p in qm.planning_phases]
        assert "PARSING" in phase_names
        assert "REPLANNING" in phase_names

    def test_not_in_extra_metrics(self):
        phases = [{"phase": "ANALYSIS", "durationMs": 10}]
        data = _make_profile({"planningPhases": phases})
        qm = extract_query_metrics(data)
        assert "planningPhases" not in qm.extra_metrics
