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
