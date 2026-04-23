"""Tests for /api/docs — auto-generated API documentation page."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture()
def app():
    os.environ.setdefault("DATABRICKS_HOST", "https://test.cloud.databricks.com")
    from app import app as flask_app

    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture()
def client(app):
    return app.test_client()


class TestApiDocsPage:
    """GET /api/docs returns auto-generated API documentation."""

    def test_returns_200(self, client):
        resp = client.get("/api/docs")
        assert resp.status_code == 200
        assert b"html" in resp.data.lower()

    def test_lists_api_endpoints(self, client):
        html = client.get("/api/docs").data.decode()
        # Should contain key endpoints
        assert "/api/v1/analyze" in html
        assert "/api/v1/history" in html
        assert "/api/v1/settings" in html
        assert "/api/v1/spark-perf" in html
        assert "/api/v1/workload" in html

    def test_shows_methods(self, client):
        html = client.get("/api/docs").data.decode()
        assert "GET" in html
        assert "POST" in html

    def test_shows_docstrings(self, client):
        html = client.get("/api/docs").data.decode()
        # Docstrings from route functions should appear
        assert "analysis" in html.lower() or "analyze" in html.lower()


class TestApiDocsJson:
    """GET /api/docs.json returns machine-readable endpoint list."""

    def test_returns_json(self, client):
        resp = client.get("/api/docs.json")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "endpoints" in data
        assert len(data["endpoints"]) > 10

    def test_endpoint_structure(self, client):
        data = client.get("/api/docs.json").get_json()
        ep = data["endpoints"][0]
        assert "path" in ep
        assert "methods" in ep
        assert "description" in ep
        assert "blueprint" in ep
