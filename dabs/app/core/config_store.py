"""Persistent configuration store for user preferences.

Saves settings (catalog, schema, warehouse HTTP path, etc.) to a local
JSON file so the user doesn't have to re-enter them each time.

Priority (highest first):
  1. Environment variables
  2. User config file (~/.dbsql_profiler_config.json) — Web UI changes
  3. Runtime config (runtime-config.json) — generated at deploy time
  4. Hardcoded defaults
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = os.path.expanduser("~/.dbsql_profiler_config.json")

# runtime-config.json sits next to this file's package root (dabs/app/)
_RUNTIME_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "runtime-config.json",
)

_runtime_config_cache: dict[str, Any] | None = None


def _reset_runtime_config_cache() -> None:
    """Reset the runtime config cache (for testing)."""
    global _runtime_config_cache
    _runtime_config_cache = None


def _config_path() -> str:
    return os.environ.get("DBSQL_PROFILER_CONFIG", _DEFAULT_CONFIG_PATH)


def load_config() -> dict[str, Any]:
    """Load saved configuration from disk (user config file)."""
    path = _config_path()
    try:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                data: dict[str, Any] = json.load(f)
                return data
    except Exception as e:
        logger.warning("Failed to load config from %s: %s", path, e)
    return {}


def _load_runtime_config() -> dict[str, Any]:
    """Load deploy-time runtime-config.json (cached, read once)."""
    global _runtime_config_cache
    if _runtime_config_cache is not None:
        return _runtime_config_cache
    try:
        if os.path.exists(_RUNTIME_CONFIG_PATH):
            with open(_RUNTIME_CONFIG_PATH, encoding="utf-8") as f:
                _runtime_config_cache = json.load(f)
                logger.info("Loaded runtime config from %s", _RUNTIME_CONFIG_PATH)
                return _runtime_config_cache
    except Exception as e:
        logger.warning("Failed to load runtime config from %s: %s", _RUNTIME_CONFIG_PATH, e)
    _runtime_config_cache = {}
    return _runtime_config_cache


def save_config(config: dict[str, Any]) -> None:
    """Save configuration to disk (merges with existing)."""
    path = _config_path()
    existing = load_config()
    existing.update(config)
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
        logger.info("Config saved to %s", path)
    except Exception as e:
        logger.warning("Failed to save config to %s: %s", path, e)


def reset_keys(keys: list[str]) -> None:
    """Remove specified keys from user config, falling back to runtime/default."""
    path = _config_path()
    existing = load_config()
    changed = False
    for key in keys:
        if key in existing:
            del existing[key]
            changed = True
    if changed:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2, ensure_ascii=False)
            logger.info("Config keys reset: %s", keys)
        except Exception as e:
            logger.warning("Failed to reset config keys: %s", e)


def get_setting(key: str, default: str = "") -> str:
    """Get a setting with priority: env var > user config > runtime config > default.

    Key mapping:
        catalog         -> PROFILER_CATALOG
        schema          -> PROFILER_SCHEMA
        http_path       -> PROFILER_WAREHOUSE_HTTP_PATH
        table_write_enabled -> PROFILER_TABLE_WRITE_ENABLED
    """
    env_map = {
        "catalog": "PROFILER_CATALOG",
        "schema": "PROFILER_SCHEMA",
        "http_path": "PROFILER_WAREHOUSE_HTTP_PATH",
        "table_write_enabled": "PROFILER_TABLE_WRITE_ENABLED",
        "spark_perf_catalog": "SPARK_PERF_CATALOG",
        "spark_perf_schema": "SPARK_PERF_SCHEMA",
        "spark_perf_table_prefix": "SPARK_PERF_TABLE_PREFIX",
        "spark_perf_http_path": "SPARK_PERF_HTTP_PATH",
        "spark_perf_etl_job_id": "SPARK_PERF_ETL_JOB_ID",
        "spark_perf_summary_job_id": "SPARK_PERF_SUMMARY_JOB_ID",
        "genie_space_id": "GENIE_SPACE_ID",
        "dbsql_genie_space_id": "DBSQL_GENIE_SPACE_ID",
    }

    # 1. Environment variable (highest priority)
    env_key = env_map.get(key)
    if env_key:
        env_val = os.environ.get(env_key)
        if env_val:
            return env_val

    # 2. User config file (Web UI changes)
    config = load_config()
    if key in config and config[key]:
        return str(config[key])

    # 3. Runtime config (deploy-time defaults)
    runtime = _load_runtime_config()
    if key in runtime and runtime[key]:
        return str(runtime[key])

    # 4. Default
    return default


def get_setting_with_source(key: str, default: str = "") -> dict[str, str]:
    """Get a setting value and which layer it came from.

    Returns {"value": "...", "source": "env|user_config|runtime_config|default"}.
    """
    env_map = {
        "catalog": "PROFILER_CATALOG",
        "schema": "PROFILER_SCHEMA",
        "http_path": "PROFILER_WAREHOUSE_HTTP_PATH",
        "table_write_enabled": "PROFILER_TABLE_WRITE_ENABLED",
        "spark_perf_catalog": "SPARK_PERF_CATALOG",
        "spark_perf_schema": "SPARK_PERF_SCHEMA",
        "spark_perf_table_prefix": "SPARK_PERF_TABLE_PREFIX",
        "spark_perf_http_path": "SPARK_PERF_HTTP_PATH",
        "spark_perf_etl_job_id": "SPARK_PERF_ETL_JOB_ID",
        "spark_perf_summary_job_id": "SPARK_PERF_SUMMARY_JOB_ID",
        "genie_space_id": "GENIE_SPACE_ID",
        "dbsql_genie_space_id": "DBSQL_GENIE_SPACE_ID",
    }

    env_key = env_map.get(key)
    if env_key:
        env_val = os.environ.get(env_key)
        if env_val:
            return {"value": env_val, "source": "env"}

    config = load_config()
    if key in config and config[key]:
        return {"value": str(config[key]), "source": "user_config"}

    runtime = _load_runtime_config()
    if key in runtime and runtime[key]:
        return {"value": str(runtime[key]), "source": "runtime_config"}

    return {"value": default, "source": "default"}


def get_config_paths() -> dict[str, str]:
    """Return file paths used for configuration."""
    return {
        "user_config": _config_path(),
        "runtime_config": _RUNTIME_CONFIG_PATH,
    }
