"""Tests for V6 feature_flags module.

V6 is the standard (Codex 2026-04-26 review):
  - flags default ON (kill switches, not experimental opt-ins)
  - explicit falsy (env / runtime-config) disables that specific
    V6 behavior, falling back to v5 as a triage path
  - 3 supported off-patterns: default-on, single-flag off,
    legacy full-off (every V6_* set false)
"""

from __future__ import annotations

import pytest

from core import feature_flags
from core.feature_flags import (
    ALL_FLAGS,
    V6_ALWAYS_INCLUDE_MINIMUM,
    V6_CANONICAL_SCHEMA,
    V6_REVIEW_NO_KNOWLEDGE,
    snapshot,
)


@pytest.fixture(autouse=True)
def _reset_flags(monkeypatch):
    """Ensure each test starts from a clean cache and clean env."""
    for flag in ALL_FLAGS:
        monkeypatch.delenv(flag, raising=False)
    feature_flags.reset_cache()
    yield
    feature_flags.reset_cache()


# ---------------------------------------------------------------------------
# Default-on contract (post-2026-04-26)
# ---------------------------------------------------------------------------


def test_default_all_flags_on():
    """V6 standard: when nothing is set, every flag is ON."""
    snap = snapshot()
    for flag in ALL_FLAGS:
        assert snap[flag] is True, f"{flag} should default to True"


def test_canonical_schema_on_by_default():
    assert feature_flags.canonical_schema() is True


# ---------------------------------------------------------------------------
# Explicit env values still win
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["1", "true", "True", "yes", "ON"])
def test_env_var_truthy_keeps_flag_on(monkeypatch, value):
    """Truthy env is redundant with the new default but must still
    work — operators may set it explicitly for documentation."""
    monkeypatch.setenv(V6_CANONICAL_SCHEMA, value)
    feature_flags.reset_cache()
    assert feature_flags.canonical_schema() is True


@pytest.mark.parametrize("value", ["0", "false", "False", "no", "OFF", ""])
def test_env_var_falsy_disables_flag(monkeypatch, value):
    """The kill-switch path: falsy env disables this V6 behavior."""
    monkeypatch.setenv(V6_CANONICAL_SCHEMA, value)
    feature_flags.reset_cache()
    assert feature_flags.canonical_schema() is False


def test_individual_accessors_independent(monkeypatch):
    """Disabling one flag must not affect the others — they remain
    on by default."""
    monkeypatch.setenv(V6_REVIEW_NO_KNOWLEDGE, "0")
    feature_flags.reset_cache()
    assert feature_flags.review_no_knowledge() is False
    assert feature_flags.canonical_schema() is True
    assert feature_flags.always_include_minimum() is True


def test_snapshot_reflects_individual_overrides(monkeypatch):
    """Snapshot exposes the resolved per-flag values for diagnostics."""
    monkeypatch.setenv(V6_CANONICAL_SCHEMA, "0")
    monkeypatch.setenv(V6_ALWAYS_INCLUDE_MINIMUM, "false")
    feature_flags.reset_cache()
    snap = snapshot()
    assert snap[V6_CANONICAL_SCHEMA] is False
    assert snap[V6_ALWAYS_INCLUDE_MINIMUM] is False
    # Untouched flags stay on.
    assert snap[V6_REVIEW_NO_KNOWLEDGE] is True


# ---------------------------------------------------------------------------
# Runtime-config interaction
# ---------------------------------------------------------------------------


def test_runtime_config_falsy_disables_default_on(monkeypatch):
    """When env is unset but runtime-config marks the flag falsy, the
    default-on must yield to the explicit override."""
    monkeypatch.delenv(V6_CANONICAL_SCHEMA, raising=False)

    fake_settings = {V6_CANONICAL_SCHEMA.lower(): "false"}

    def fake_get_setting(key, default=None):
        return fake_settings.get(key, default)

    import core.config_store as config_store_mod

    monkeypatch.setattr(config_store_mod, "get_setting", fake_get_setting)
    feature_flags.reset_cache()

    assert feature_flags.canonical_schema() is False


def test_env_falsy_overrides_runtime_truthy(monkeypatch):
    """env var still wins over runtime-config — env "0" disables even
    when runtime says true."""
    fake_settings = {V6_CANONICAL_SCHEMA.lower(): "true"}

    def fake_get_setting(key, default=None):
        return fake_settings.get(key, default)

    import core.config_store as config_store_mod

    monkeypatch.setattr(config_store_mod, "get_setting", fake_get_setting)
    monkeypatch.setenv(V6_CANONICAL_SCHEMA, "0")
    feature_flags.reset_cache()

    assert feature_flags.canonical_schema() is False


# ---------------------------------------------------------------------------
# Supported off-patterns (Codex 2026-04-26): default-on / single-flag
# off / legacy full-off. These pin the patterns we explicitly test for.
# ---------------------------------------------------------------------------


def test_legacy_full_off_pattern(monkeypatch):
    """All V6 flags falsy → full v5 fallback."""
    for flag in ALL_FLAGS:
        monkeypatch.setenv(flag, "0")
    feature_flags.reset_cache()
    snap = snapshot()
    for flag in ALL_FLAGS:
        assert snap[flag] is False
