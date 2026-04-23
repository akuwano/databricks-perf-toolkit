"""Tests for services/genie_client.py."""

from __future__ import annotations

import json
import sys
import types
from unittest.mock import MagicMock, patch

import pytest
from services.genie_client import (
    _DBSQL_TABLE_NAMES,
    _GOLD_TABLE_SUFFIXES,
    DbsqlGenieConfig,
    GenieClient,
    GenieConfig,
)


def _ensure_databricks_sdk_mock():
    """Ensure databricks.sdk.core is importable (may be a mock)."""
    if "databricks" not in sys.modules or not hasattr(sys.modules.get("databricks", None), "sdk"):
        databricks_mod = sys.modules.setdefault("databricks", types.ModuleType("databricks"))
        sdk_mod = types.ModuleType("databricks.sdk")
        databricks_mod.sdk = sdk_mod  # type: ignore[attr-defined]
        sys.modules["databricks.sdk"] = sdk_mod
        core_mod = types.ModuleType("databricks.sdk.core")
        sdk_mod.core = core_mod  # type: ignore[attr-defined]
        sys.modules["databricks.sdk.core"] = core_mod
        core_mod.Config = MagicMock  # type: ignore[attr-defined]


_ensure_databricks_sdk_mock()


# ---------------------------------------------------------------------------
# GenieConfig.from_env
# ---------------------------------------------------------------------------


class TestGenieConfigFromEnv:
    """Test GenieConfig.from_env() configuration loading."""

    def test_from_env_with_env_var(self):
        with (
            patch.dict("os.environ", {"DATABRICKS_HOST": "https://my-host.cloud.databricks.com/"}),
            patch(
                "core.config_store.get_setting",
                side_effect=lambda key, default="": {
                    "spark_perf_http_path": "/sql/1.0/warehouses/abc123",
                    "spark_perf_catalog": "analytics",
                    "spark_perf_schema": "perf",
                    "spark_perf_table_prefix": "TEST_",
                }.get(key, default),
            ),
        ):
            cfg = GenieConfig.from_env()

        assert cfg.host == "my-host.cloud.databricks.com"
        assert cfg.catalog == "analytics"
        assert cfg.schema == "perf"
        assert cfg.table_prefix == "TEST_"
        assert cfg.warehouse_id == "abc123"

    def test_from_env_strips_protocol_and_trailing_slash(self):
        with (
            patch.dict("os.environ", {"DATABRICKS_HOST": "http://host.example.com/"}),
            patch("core.config_store.get_setting", return_value=""),
        ):
            cfg = GenieConfig.from_env()

        assert cfg.host == "host.example.com"

    def test_from_env_defaults_when_no_settings(self):
        with (
            patch.dict("os.environ", {"DATABRICKS_HOST": "https://host"}),
            patch("core.config_store.get_setting", side_effect=lambda key, default="": default),
        ):
            cfg = GenieConfig.from_env()

        assert cfg.catalog == "main"
        assert cfg.schema == "default"
        assert cfg.table_prefix == "PERF_"
        assert cfg.warehouse_id == ""

    def test_from_env_falls_back_to_sdk_config(self):
        mock_sdk_cfg = MagicMock()
        mock_sdk_cfg.host = "https://sdk-host.databricks.com"

        with (
            patch.dict("os.environ", {}, clear=True),
            patch("core.config_store.get_setting", return_value=""),
            patch("databricks.sdk.core.Config", return_value=mock_sdk_cfg),
        ):
            cfg = GenieConfig.from_env()

        assert cfg.host == "sdk-host.databricks.com"

    def test_from_env_sdk_config_exception(self):
        """When SDK Config raises, host should be empty."""
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("core.config_store.get_setting", return_value=""),
            patch("databricks.sdk.core.Config", side_effect=Exception("no auth")),
        ):
            cfg = GenieConfig.from_env()

        assert cfg.host == ""


# ---------------------------------------------------------------------------
# DbsqlGenieConfig.from_env
# ---------------------------------------------------------------------------


class TestDbsqlGenieConfigFromEnv:
    """Test DbsqlGenieConfig.from_env() configuration loading."""

    def test_from_env_basic(self):
        with (
            patch.dict("os.environ", {"DATABRICKS_HOST": "https://dbsql-host.com"}),
            patch(
                "core.config_store.get_setting",
                side_effect=lambda key, default="": {
                    "http_path": "/sql/1.0/warehouses/wh999",
                    "catalog": "my_catalog",
                    "schema": "my_schema",
                }.get(key, default),
            ),
        ):
            cfg = DbsqlGenieConfig.from_env()

        assert cfg.host == "dbsql-host.com"
        assert cfg.catalog == "my_catalog"
        assert cfg.schema == "my_schema"
        assert cfg.warehouse_id == "wh999"

    def test_from_env_defaults(self):
        with (
            patch.dict("os.environ", {"DATABRICKS_HOST": "https://h"}),
            patch("core.config_store.get_setting", side_effect=lambda key, default="": default),
        ):
            cfg = DbsqlGenieConfig.from_env()

        assert cfg.catalog == "main"
        assert cfg.schema == "profiler"
        assert cfg.warehouse_id == ""


# ---------------------------------------------------------------------------
# GenieClient._spark_perf_table_fqns
# ---------------------------------------------------------------------------


class TestSparkPerfTableFqns:
    """Test FQN generation for Spark Perf tables."""

    def test_generates_correct_fqns(self):
        config = GenieConfig(
            host="host",
            catalog="cat",
            schema="sch",
            table_prefix="PFX_",
            warehouse_id="wh1",
        )
        client = GenieClient(config)
        fqns = client._spark_perf_table_fqns()

        assert len(fqns) == len(_GOLD_TABLE_SUFFIXES)
        for fqn in fqns:
            assert fqn.startswith("cat.sch.PFX_")
        assert "cat.sch.PFX_gold_application_summary" in fqns

    def test_empty_prefix(self):
        config = GenieConfig(
            host="host",
            catalog="c",
            schema="s",
            table_prefix="",
            warehouse_id="",
        )
        client = GenieClient(config)
        fqns = client._spark_perf_table_fqns()

        assert "c.s.gold_application_summary" in fqns


# ---------------------------------------------------------------------------
# GenieClient._dbsql_table_fqns
# ---------------------------------------------------------------------------


class TestDbsqlTableFqns:
    """Test FQN generation for DBSQL tables."""

    def test_generates_correct_fqns(self):
        config = DbsqlGenieConfig(
            host="host",
            catalog="mycat",
            schema="mysch",
            warehouse_id="wh",
        )
        client = GenieClient(config)
        fqns = client._dbsql_table_fqns()

        assert len(fqns) == len(_DBSQL_TABLE_NAMES)
        for name in _DBSQL_TABLE_NAMES:
            assert f"mycat.mysch.{name}" in fqns


# ---------------------------------------------------------------------------
# GenieClient._create_space_generic
# ---------------------------------------------------------------------------


class TestCreateSpaceGeneric:
    """Test _create_space_generic HTTP call and payload structure."""

    def _make_client(self) -> GenieClient:
        config = GenieConfig(
            host="workspace.databricks.com",
            catalog="cat",
            schema="sch",
            table_prefix="P_",
            warehouse_id="wh123",
        )
        return GenieClient(config)

    def test_payload_structure(self):
        client = self._make_client()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"space_id": "sp-001"}

        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        with patch.object(client, "_get_session", return_value=mock_session):
            space_id = client._create_space_generic(
                title="Test Space",
                description="desc",
                table_fqns=["cat.sch.table1", "cat.sch.table2"],
                instructions="Be helpful.",
            )

        assert space_id == "sp-001"
        mock_session.post.assert_called_once()

        call_args = mock_session.post.call_args
        url = call_args[0][0]
        assert url == "https://workspace.databricks.com/api/2.0/genie/spaces"

        payload = call_args[1]["json"]
        assert payload["title"] == "Test Space"
        assert payload["description"] == "desc"
        assert payload["warehouse_id"] == "wh123"

        # serialized_space should be valid JSON
        space_obj = json.loads(payload["serialized_space"])
        assert space_obj["version"] == 2
        tables = space_obj["data_sources"]["tables"]
        # Tables should be sorted
        assert tables == [
            {"identifier": "cat.sch.table1"},
            {"identifier": "cat.sch.table2"},
        ]
        assert "Be helpful." in space_obj["instructions"]["text_instructions"][0]["content"]

    def test_raises_on_non_200(self):
        client = self._make_client()

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_response.text = "Forbidden"
        mock_response.raise_for_status.side_effect = Exception("403 Forbidden")

        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        with patch.object(client, "_get_session", return_value=mock_session):
            with pytest.raises(Exception, match="403"):
                client._create_space_generic("t", "d", ["a.b.c"], "inst")


# ---------------------------------------------------------------------------
# GenieClient.start_conversation
# ---------------------------------------------------------------------------


class TestStartConversation:
    """Test start_conversation HTTP call."""

    def test_returns_ids(self):
        config = GenieConfig(
            host="host.com",
            catalog="c",
            schema="s",
            table_prefix="P_",
            warehouse_id="wh",
        )
        client = GenieClient(config)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "conversation_id": "conv-123",
            "message_id": "msg-456",
        }
        mock_response.raise_for_status = MagicMock()

        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        with patch.object(client, "_get_session", return_value=mock_session):
            result = client.start_conversation("sp-1", "Hello Genie")

        assert result == {"conversation_id": "conv-123", "message_id": "msg-456"}

        call_url = mock_session.post.call_args[0][0]
        assert "/api/2.0/genie/spaces/sp-1/start-conversation" in call_url

        call_payload = mock_session.post.call_args[1]["json"]
        assert call_payload == {"content": "Hello Genie"}


# ---------------------------------------------------------------------------
# GenieClient.get_message_status
# ---------------------------------------------------------------------------


class TestGetMessageStatus:
    """Test get_message_status parsing for various states."""

    def _make_client(self) -> GenieClient:
        config = GenieConfig(
            host="host.com",
            catalog="c",
            schema="s",
            table_prefix="P_",
            warehouse_id="wh",
        )
        return GenieClient(config)

    def _mock_get(self, client: GenieClient, response_data: dict) -> dict:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = response_data
        mock_response.raise_for_status = MagicMock()

        mock_session = MagicMock()
        mock_session.get.return_value = mock_response

        with patch.object(client, "_get_session", return_value=mock_session):
            return client.get_message_status("sp", "conv", "msg")

    def test_completed_with_reply(self):
        client = self._make_client()
        result = self._mock_get(
            client,
            {
                "status": "COMPLETED",
                "reply": {"content": "Here is the answer."},
                "attachments": [],
            },
        )

        assert result["status"] == "COMPLETED"
        assert result["text"] == "Here is the answer."
        assert result["sql"] == ""
        assert result["attachments"] == []

    def test_completed_with_query_attachment(self):
        client = self._make_client()
        result = self._mock_get(
            client,
            {
                "status": "COMPLETED",
                "reply": {},
                "attachments": [
                    {
                        "id": "att-1",
                        "type": "QUERY",
                        "query": {"query": "SELECT * FROM t", "id": "q-1"},
                    }
                ],
            },
        )

        assert result["status"] == "COMPLETED"
        assert result["sql"] == "SELECT * FROM t"
        assert len(result["attachments"]) == 1
        assert result["attachments"][0]["type"] == "QUERY"

    def test_failed_status(self):
        client = self._make_client()
        result = self._mock_get(
            client,
            {
                "status": "FAILED",
                "content": "Something went wrong",
                "attachments": [],
            },
        )

        assert result["status"] == "FAILED"
        assert result["text"] == "Something went wrong"

    def test_pending_status(self):
        client = self._make_client()
        result = self._mock_get(
            client,
            {
                "status": "EXECUTING_QUERY",
                "attachments": [],
            },
        )

        assert result["status"] == "EXECUTING_QUERY"
        assert result["text"] == ""

    def test_text_from_attachment_text(self):
        client = self._make_client()
        result = self._mock_get(
            client,
            {
                "status": "COMPLETED",
                "reply": {},
                "attachments": [
                    {"text": {"content": "Answer from attachment"}},
                ],
            },
        )

        assert result["text"] == "Answer from attachment"


# ---------------------------------------------------------------------------
# GenieClient._get_session (SP auth)
# ---------------------------------------------------------------------------


class TestGetSession:
    """Test SP auth session creation."""

    def test_session_with_sdk_auth(self):
        config = GenieConfig(
            host="host.com",
            catalog="c",
            schema="s",
            table_prefix="P_",
            warehouse_id="wh",
        )
        client = GenieClient(config)

        mock_cfg = MagicMock()
        mock_cfg.auth_type = "pat"
        mock_cfg.authenticate.return_value = {"Authorization": "Bearer tok123"}

        mock_session = MagicMock()
        mock_session.headers = {}

        with (
            patch("requests.Session", return_value=mock_session),
            patch("databricks.sdk.core.Config", return_value=mock_cfg),
            patch.dict("os.environ", {"DATABRICKS_TOKEN": ""}, clear=False),
        ):
            session = client._get_session()

        assert session.headers["Authorization"] == "Bearer tok123"

    def test_session_with_callable_headers(self):
        config = GenieConfig(
            host="host.com",
            catalog="c",
            schema="s",
            table_prefix="P_",
            warehouse_id="wh",
        )
        client = GenieClient(config)

        mock_cfg = MagicMock()
        mock_cfg.auth_type = "oauth"
        mock_cfg.authenticate.return_value = lambda: {"Authorization": "Bearer dynamic"}

        mock_session = MagicMock()
        mock_session.headers = {}

        with (
            patch("requests.Session", return_value=mock_session),
            patch("databricks.sdk.core.Config", return_value=mock_cfg),
            patch.dict("os.environ", {"DATABRICKS_TOKEN": ""}, clear=False),
        ):
            session = client._get_session()

        assert session.headers["Authorization"] == "Bearer dynamic"

    def test_session_sdk_failure_returns_unauthenticated_session(self):
        config = GenieConfig(
            host="host.com",
            catalog="c",
            schema="s",
            table_prefix="P_",
            warehouse_id="wh",
        )
        client = GenieClient(config)

        mock_session = MagicMock()
        mock_session.headers = {}

        with (
            patch("requests.Session", return_value=mock_session),
            patch("databricks.sdk.core.Config", side_effect=Exception("no creds")),
            patch.dict("os.environ", {"DATABRICKS_TOKEN": ""}, clear=False),
        ):
            session = client._get_session()

        # Should still return a session, just without Authorization
        assert session is mock_session
        assert "Authorization" not in session.headers
