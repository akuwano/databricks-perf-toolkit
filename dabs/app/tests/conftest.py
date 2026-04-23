"""Shared test fixtures."""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _reset_runtime_config():
    """Disable runtime-config.json during tests to avoid leaking state."""
    import core.config_store as cs

    cs._runtime_config_cache = None
    with patch.object(cs, "_RUNTIME_CONFIG_PATH", "/tmp/_no_such_runtime_config.json"):
        yield
    cs._runtime_config_cache = None
