"""
Databricks Model Serving Endpoints client.

Lists available Foundation Model endpoints for LLM model selection.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import requests  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# Default models shown when API is unavailable
DEFAULT_MODELS = [
    {"name": "databricks-claude-opus-4-6", "display": "Claude Opus 4.6"},
    {"name": "databricks-gpt-5-4", "display": "GPT-5-4"},
    {"name": "databricks-meta-llama-3-3-70b-instruct", "display": "Llama 3.3 70B"},
]


@dataclass
class ServingModel:
    """A serving endpoint suitable for LLM chat completions."""

    name: str  # endpoint name (used as model parameter)
    display_name: str  # human-readable name for UI
    state: str = "READY"  # endpoint state


def _ensure_https(host: str) -> str:
    if not host:
        return host
    if not host.startswith("https://") and not host.startswith("http://"):
        return f"https://{host}"
    return host


def _get_auth_headers() -> dict[str, str]:
    """Get auth headers using Databricks SDK (SP credentials)."""
    try:
        from databricks.sdk.core import Config

        cfg = Config()
        headers: dict[str, str] = dict(cfg.authenticate())
        headers["Content-Type"] = "application/json"
        return headers
    except Exception as e:
        logger.warning("Failed to get SDK auth for serving endpoints: %s", e)
        return {}


def list_chat_models(
    host: str | None = None,
) -> list[ServingModel]:
    """List available chat/completions serving endpoints.

    Fetches from Databricks Serving Endpoints API and filters to
    endpoints that are READY and support llm/v1/chat task.
    Uses SP credentials via Databricks SDK.

    Falls back to DEFAULT_MODELS if the API call fails.

    Args:
        host: Databricks workspace URL

    Returns:
        List of ServingModel sorted by name
    """
    if not host:
        # Try to get host from environment
        import os

        host = os.environ.get("DATABRICKS_HOST", "")

    if not host:
        logger.info("No host configured, returning default models")
        return _defaults()

    host = _ensure_https(host)
    headers = _get_auth_headers()
    if not headers:
        logger.info("No auth available, returning default models")
        return _defaults()

    try:
        url = f"{host}/api/2.0/serving-endpoints"
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            logger.warning(
                "Serving endpoints API returned %d, using defaults",
                response.status_code,
            )
            return _defaults()

        data = response.json()
        endpoints = data.get("endpoints", [])

        models = []
        for ep in endpoints:
            name = ep.get("name", "")
            state = ep.get("state", {}).get("ready", "")

            # Only include READY endpoints
            if state != "READY":
                continue

            # Check if this is a chat/completions endpoint
            task = ep.get("task", "")
            config = ep.get("config", {})
            served_entities = config.get("served_entities", [])

            is_chat = (
                task == "llm/v1/chat"
                or any(e.get("foundation_model_name", "") for e in served_entities)
                or name.startswith("databricks-")
            )

            if not is_chat:
                continue

            # Build display name
            display = _build_display_name(name, served_entities)
            models.append(ServingModel(name=name, display_name=display))

        if not models:
            logger.info("No chat endpoints found, using defaults")
            return _defaults()

        models.sort(key=lambda m: m.name)
        logger.info("Found %d chat model endpoints", len(models))
        return models

    except requests.RequestException as e:
        logger.warning("Failed to fetch serving endpoints: %s", e)
        return _defaults()
    except Exception as e:
        logger.warning("Unexpected error listing models: %s", e)
        return _defaults()


def _build_display_name(name: str, served_entities: list) -> str:
    """Build a human-readable display name from endpoint name."""
    # Try to extract foundation model name
    for entity in served_entities:
        fm_name = entity.get("foundation_model_name", "")
        if fm_name:
            return str(fm_name)

    # Clean up the endpoint name
    display = name
    if display.startswith("databricks-"):
        display = display[len("databricks-") :]

    # Capitalize parts
    parts = display.split("-")
    return " ".join(p.capitalize() for p in parts)


def _defaults() -> list[ServingModel]:
    """Return default model list."""
    return [ServingModel(name=m["name"], display_name=m["display"]) for m in DEFAULT_MODELS]
