"""Tests for /api/v1/spark-perf/streaming — Streaming query endpoints."""

import os
import sys
from unittest.mock import MagicMock, patch

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


def _mock_reader():
    reader = MagicMock()
    reader.get_streaming_query_summary.return_value = [
        {
            "query_id": "q-abc-123",
            "source_type": "CloudFiles",
            "sink_type": "Delta",
            "total_batches": 10,
            "avg_batch_duration_ms": 5000,
            "avg_processed_rows_per_sec": 1000.0,
            "bottleneck_type": "STREAM_OK",
            "severity": "NONE",
        },
    ]
    reader.get_streaming_summary.return_value = {
        "query_count": 1,
        "total_batches": 10,
        "avg_batch_duration_ms": 5000,
    }
    reader.get_streaming_batch_detail.return_value = [
        {
            "batch_id": 0,
            "batch_duration_ms": 5000,
            "add_batch_ms": 4000,
            "query_planning_ms": 200,
        },
    ]
    return reader


class TestStreamingAPI:
    """GET /api/v1/spark-perf/streaming."""

    def test_requires_app_id(self, client):
        resp = client.get("/api/v1/spark-perf/streaming")
        assert resp.status_code == 400

    @patch("routes.spark_perf._get_spark_perf_reader", return_value=None)
    def test_returns_503_when_not_configured(self, mock_reader, client):
        resp = client.get("/api/v1/spark-perf/streaming?app_id=app-1")
        assert resp.status_code == 503

    @patch("routes.spark_perf._get_spark_perf_reader")
    def test_returns_queries_and_summary(self, mock_get_reader, client):
        reader = _mock_reader()
        mock_get_reader.return_value = reader

        resp = client.get("/api/v1/spark-perf/streaming?app_id=app-1")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "queries" in data
        assert "summary" in data
        assert len(data["queries"]) == 1
        assert data["queries"][0]["query_id"] == "q-abc-123"
        assert data["summary"]["query_count"] == 1
        reader.get_streaming_query_summary.assert_called_once_with("app-1")
        reader.get_streaming_summary.assert_called_once_with("app-1")


class TestStreamingBatchesAPI:
    """GET /api/v1/spark-perf/streaming/batches."""

    def test_requires_app_id(self, client):
        resp = client.get("/api/v1/spark-perf/streaming/batches")
        assert resp.status_code == 400

    @patch("routes.spark_perf._get_spark_perf_reader", return_value=None)
    def test_returns_503_when_not_configured(self, mock_reader, client):
        resp = client.get("/api/v1/spark-perf/streaming/batches?app_id=app-1")
        assert resp.status_code == 503

    @patch("routes.spark_perf._get_spark_perf_reader")
    def test_returns_batches(self, mock_get_reader, client):
        reader = _mock_reader()
        mock_get_reader.return_value = reader

        resp = client.get("/api/v1/spark-perf/streaming/batches?app_id=app-1")
        assert resp.status_code == 200
        data = resp.get_json()
        assert "batches" in data
        assert len(data["batches"]) == 1
        assert data["batches"][0]["batch_id"] == 0
        reader.get_streaming_batch_detail.assert_called_once_with("app-1", query_id=None)

    @patch("routes.spark_perf._get_spark_perf_reader")
    def test_filters_by_query_id(self, mock_get_reader, client):
        reader = _mock_reader()
        mock_get_reader.return_value = reader

        resp = client.get("/api/v1/spark-perf/streaming/batches?app_id=app-1&query_id=q-abc")
        assert resp.status_code == 200
        reader.get_streaming_batch_detail.assert_called_once_with("app-1", query_id="q-abc")
