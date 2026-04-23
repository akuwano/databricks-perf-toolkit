"""Tests for core.serving_client."""

from unittest.mock import MagicMock, patch

from core.serving_client import (
    DEFAULT_MODELS,
    ServingModel,
    _build_display_name,
    _defaults,
    list_chat_models,
)


class TestDefaults:
    def test_returns_default_models(self):
        result = _defaults()
        assert len(result) == len(DEFAULT_MODELS)
        assert all(isinstance(m, ServingModel) for m in result)

    def test_default_model_names(self):
        result = _defaults()
        names = [m.name for m in result]
        assert "databricks-claude-opus-4-6" in names
        assert "databricks-gpt-5-4" in names


class TestBuildDisplayName:
    def test_with_foundation_model_name(self):
        entities = [{"foundation_model_name": "Claude Opus 4.6"}]
        assert _build_display_name("databricks-claude-opus-4-6", entities) == "Claude Opus 4.6"

    def test_without_entities(self):
        result = _build_display_name("databricks-gpt-5-4", [])
        assert "Gpt" in result or "5" in result

    def test_strip_databricks_prefix(self):
        result = _build_display_name("databricks-my-model", [])
        assert not result.startswith("databricks-")


class TestListChatModels:
    def test_no_host_returns_defaults(self):
        with patch.dict("os.environ", {}, clear=True):
            result = list_chat_models(host="")
        assert len(result) == len(DEFAULT_MODELS)

    @patch(
        "core.serving_client._get_auth_headers",
        return_value={"Authorization": "Bearer test", "Content-Type": "application/json"},
    )
    @patch("core.serving_client.requests.get")
    def test_api_success(self, mock_get, _mock_auth):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "endpoints": [
                {
                    "name": "databricks-test-model",
                    "state": {"ready": "READY"},
                    "task": "llm/v1/chat",
                    "config": {"served_entities": []},
                },
                {
                    "name": "not-ready-model",
                    "state": {"ready": "NOT_READY"},
                    "task": "llm/v1/chat",
                    "config": {"served_entities": []},
                },
            ]
        }
        mock_get.return_value = mock_response

        result = list_chat_models(host="https://example.com")
        assert len(result) == 1
        assert result[0].name == "databricks-test-model"

    @patch(
        "core.serving_client._get_auth_headers",
        return_value={"Authorization": "Bearer test", "Content-Type": "application/json"},
    )
    @patch("core.serving_client.requests.get")
    def test_api_failure_returns_defaults(self, mock_get, _mock_auth):
        mock_get.side_effect = Exception("connection error")
        result = list_chat_models(host="https://example.com")
        assert len(result) == len(DEFAULT_MODELS)

    @patch(
        "core.serving_client._get_auth_headers",
        return_value={"Authorization": "Bearer test", "Content-Type": "application/json"},
    )
    @patch("core.serving_client.requests.get")
    def test_api_non_200_returns_defaults(self, mock_get, _mock_auth):
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response

        result = list_chat_models(host="https://example.com")
        assert len(result) == len(DEFAULT_MODELS)
