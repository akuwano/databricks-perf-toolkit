"""Tests for core.profile_validator module."""

from core.profile_validator import validate_profile


class TestValidateProfile:
    def test_valid_profile(self):
        data = {
            "query": {"id": "q-1", "status": "FINISHED", "metrics": {}},
            "graphs": [{"nodes": []}],
        }
        result = validate_profile(data)
        assert result.valid
        assert not result.errors

    def test_missing_query(self):
        data = {"graphs": []}
        result = validate_profile(data)
        assert not result.valid
        assert any("query" in e.lower() for e in result.errors)

    def test_missing_graphs(self):
        data = {"query": {"id": "q-1", "status": "FINISHED", "metrics": {}}}
        result = validate_profile(data)
        assert not result.valid
        assert any("graphs" in e.lower() for e in result.errors)

    def test_empty_dict(self):
        result = validate_profile({})
        assert not result.valid

    def test_result_from_cache_allowed_without_graphs(self):
        """Cache-hit profiles legitimately have no graphs (plansState=EMPTY,
        planMetadatas=[]). The downstream pipeline already handles this
        case (verdict=skipped_cached). The validator must not block them
        with HTTP 400. Reported 2026-04-27 on a real cache-hit profile."""
        data = {
            "version": "1",
            "query": {
                "id": "q-cached",
                "status": "FINISHED",
                "metrics": {"resultFromCache": True, "totalTimeMs": 9421},
                "plansState": "EMPTY",
            },
            "planMetadatas": [],
            # No "graphs" field — plansState=EMPTY means no plan was generated
        }
        result = validate_profile(data)
        assert result.valid, f"errors: {result.errors}"
        # Should warn so the user knows analysis is limited
        assert any("cache" in w.lower() for w in result.warnings), (
            f"warnings: {result.warnings}"
        )

    def test_cache_query_id_set_allowed_without_graphs(self):
        """Alternative cache-hit signal: top-level cacheQueryId set."""
        data = {
            "query": {
                "id": "q-cached-2",
                "status": "FINISHED",
                "metrics": {"totalTimeMs": 100},
                "cacheQueryId": "01f0fd07-4dcf-1ed0-85ac-3e179928a371",
                "plansState": "EMPTY",
            },
        }
        result = validate_profile(data)
        assert result.valid, f"errors: {result.errors}"

    def test_streaming_profile_allowed_without_graphs(self):
        """DLT/SDP streaming profiles can have planMetadatas instead
        of a unified graphs field. The streaming code path uses
        micro-batch metrics, not the global graph."""
        data = {
            "query": {
                "id": "q-streaming",
                "status": "RUNNING",
                "metrics": {"totalTimeMs": 60_000},
                "queryMetadata": {"isStreaming": True},
            },
            "planMetadatas": [
                {"id": "p-1", "statusId": "FINISHED", "metrics": {}},
                {"id": "p-2", "statusId": "FINISHED", "metrics": {}},
            ],
        }
        result = validate_profile(data)
        assert result.valid, f"errors: {result.errors}"
        assert any("streaming" in w.lower() for w in result.warnings)

    def test_missing_graphs_still_rejected_for_normal_profiles(self):
        """Don't relax for non-cached profiles — those genuinely need
        graphs to analyze."""
        data = {
            "query": {
                "id": "q-normal",
                "status": "FINISHED",
                "metrics": {"resultFromCache": False, "totalTimeMs": 1000},
            }
        }
        result = validate_profile(data)
        assert not result.valid
        assert any("graphs" in e.lower() for e in result.errors)

    def test_verbose_detection_with_verbose(self):
        data = {
            "query": {"id": "q-1", "status": "FINISHED", "metrics": {}},
            "graphs": [
                {
                    "nodes": [
                        {
                            "metrics": [
                                {"label": "Number of local scan tasks", "value": 5},
                                {"label": "Cache hits size", "value": 100},
                            ]
                        }
                    ]
                }
            ],
        }
        result = validate_profile(data)
        assert result.valid
        assert result.is_verbose
        assert not result.warnings

    def test_verbose_detection_without_verbose(self):
        data = {
            "query": {"id": "q-1", "status": "FINISHED", "metrics": {}},
            "graphs": [
                {
                    "nodes": [
                        {
                            "metrics": [
                                {"label": "Cumulative time", "value": 100},
                            ]
                        }
                    ]
                }
            ],
        }
        result = validate_profile(data)
        assert result.valid
        assert not result.is_verbose
        assert any("verbose" in w.lower() for w in result.warnings)

    def test_no_nodes(self):
        data = {
            "query": {"id": "q-1", "status": "FINISHED", "metrics": {}},
            "graphs": [{"nodes": []}],
        }
        result = validate_profile(data)
        assert result.valid
        assert any("node" in w.lower() or "empty" in w.lower() for w in result.warnings)
