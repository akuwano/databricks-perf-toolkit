"""Tests for the structured ``LLM_CALL`` telemetry log line.

Minimal-version L5 instrumentation (v6.6.x): every successful
``call_llm_with_retry`` emits one logfmt-style line so V5/V6 cost
comparisons can be done from log search alone, without DB writes.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from core.llm_client import (
    _emit_llm_call_telemetry,
    _flag_bitmask,
    _TELEMETRY_FLAG_BITS,
    call_llm_with_retry,
)


def _make_response(content: str = "ok", finish: str = "stop"):
    """Build a fake OpenAI-shaped response with usage stats."""
    msg = MagicMock()
    msg.content = content
    choice = MagicMock()
    choice.finish_reason = finish
    choice.message = msg
    response = MagicMock()
    response.choices = [choice]
    response.usage = MagicMock(prompt_tokens=12345, completion_tokens=678, total_tokens=13023)
    return response


def _capture_logs(caplog, level=logging.INFO):
    return [r.getMessage() for r in caplog.records if r.levelno >= level]


# ---- _emit_llm_call_telemetry shape ----


def test_telemetry_line_has_required_keys(caplog):
    caplog.set_level(logging.INFO, logger="core.llm_client")
    usage = MagicMock(prompt_tokens=1000, completion_tokens=200, total_tokens=1200)
    _emit_llm_call_telemetry(
        stage="analyze",
        model="claude-opus-4-7",
        prompt_chars=4321,
        latency_ms=15234,
        finish_reason="stop",
        usage=usage,
        attempt=0,
        max_tokens=4096,
        extra={"knowledge_chars": 12000, "is_federation": False},
    )
    lines = [l for l in _capture_logs(caplog) if l.startswith("LLM_CALL")]
    assert len(lines) == 1
    line = lines[0]
    for required in (
        "stage=analyze",
        "model=claude-opus-4-7",
        "prompt_chars=4321",
        "prompt_tokens=1000",
        "completion_tokens=200",
        "total_tokens=1200",
        "latency_ms=15234",
        "max_tokens=4096",
        "finish_reason=stop",
        "attempt=0",
        "knowledge_chars=12000",
        "is_federation=False",
    ):
        assert required in line, f"missing {required!r} in {line!r}"


def test_telemetry_line_handles_missing_usage(caplog):
    """Some Databricks endpoints occasionally omit usage on streamed
    responses. Telemetry must not crash and must still emit the rest."""
    caplog.set_level(logging.INFO, logger="core.llm_client")
    _emit_llm_call_telemetry(
        stage="review",
        model="m",
        prompt_chars=100,
        latency_ms=200,
        finish_reason="stop",
        usage=None,
        attempt=0,
        max_tokens=4096,
    )
    lines = [l for l in _capture_logs(caplog) if l.startswith("LLM_CALL")]
    assert lines and "prompt_tokens=- completion_tokens=- total_tokens=-" in lines[0]


def test_telemetry_line_no_extra_no_crash(caplog):
    caplog.set_level(logging.INFO, logger="core.llm_client")
    _emit_llm_call_telemetry(
        stage="rerank",
        model="m",
        prompt_chars=10,
        latency_ms=1,
        finish_reason="stop",
        usage=MagicMock(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        attempt=0,
        max_tokens=10,
        extra=None,
    )
    assert any(l.startswith("LLM_CALL") for l in _capture_logs(caplog))


def test_extra_lists_joined_with_pipe(caplog):
    caplog.set_level(logging.INFO, logger="core.llm_client")
    _emit_llm_call_telemetry(
        stage="analyze",
        model="m",
        prompt_chars=10,
        latency_ms=1,
        finish_reason="stop",
        usage=MagicMock(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        attempt=0,
        max_tokens=10,
        extra={"knowledge_sections": ["bottleneck_summary", "io", "photon"]},
    )
    line = next(l for l in _capture_logs(caplog) if l.startswith("LLM_CALL"))
    assert "knowledge_sections=bottleneck_summary|io|photon" in line


def test_extra_whitespace_in_value_normalized(caplog):
    """logfmt invariant: keys/values must not contain spaces. We replace
    spaces inside values with '_' so the line stays parseable."""
    caplog.set_level(logging.INFO, logger="core.llm_client")
    _emit_llm_call_telemetry(
        stage="-",
        model="m",
        prompt_chars=10,
        latency_ms=1,
        finish_reason="stop",
        usage=MagicMock(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        attempt=0,
        max_tokens=10,
        extra={"target_table": "cat.sch.my table"},
    )
    line = next(l for l in _capture_logs(caplog) if l.startswith("LLM_CALL"))
    assert "target_table=cat.sch.my_table" in line


def test_extra_drops_none_values(caplog):
    caplog.set_level(logging.INFO, logger="core.llm_client")
    _emit_llm_call_telemetry(
        stage="-",
        model="m",
        prompt_chars=1,
        latency_ms=1,
        finish_reason="stop",
        usage=MagicMock(prompt_tokens=1, completion_tokens=1, total_tokens=2),
        attempt=0,
        max_tokens=10,
        extra={"a": "x", "b": None},
    )
    line = next(l for l in _capture_logs(caplog) if l.startswith("LLM_CALL"))
    assert "a=x" in line
    assert "b=None" not in line  # None is silently dropped


# ---- _flag_bitmask ----


def test_flag_bitmask_width_matches_active_flags():
    """Bitmask width tracks the live ``_TELEMETRY_FLAG_BITS`` tuple.
    When a flag is retired (e.g. V6_COMPACT_TOP_ALERTS in v6.6.4),
    the width shrinks accordingly."""
    bm = _flag_bitmask()
    assert len(bm) == len(_TELEMETRY_FLAG_BITS)
    assert set(bm) <= {"0", "1", "?"}


def test_flag_bitmask_reflects_disabled_env(monkeypatch):
    """V6 flags default-on; explicit "0" flips the bit to 0."""
    monkeypatch.setenv("V6_CANONICAL_SCHEMA", "0")
    # Force feature_flags to re-resolve
    from core import feature_flags
    feature_flags.reset_cache()
    bm = _flag_bitmask()
    feature_flags.reset_cache()
    # Bit 0 is V6_CANONICAL_SCHEMA, the rest stay at default-on.
    assert bm[0] == "0"
    assert bm[1] == "1"


# ---- call_llm_with_retry threading ----


def test_call_llm_with_retry_propagates_stage_and_extra(caplog):
    caplog.set_level(logging.INFO, logger="core.llm_client")
    client = MagicMock()
    client.chat.completions.create.return_value = _make_response()
    out = call_llm_with_retry(
        client=client,
        model="m",
        messages=[{"role": "user", "content": "hi"}],
        max_tokens=128,
        stage="analyze",
        extra_telemetry={"knowledge_chars": 1234},
    )
    assert out == "ok"
    line = next(l for l in _capture_logs(caplog) if l.startswith("LLM_CALL"))
    assert "stage=analyze" in line
    assert "knowledge_chars=1234" in line


def test_call_llm_with_retry_default_stage_is_dash(caplog):
    """Backward-compat: callers that don't pass stage still emit the line."""
    caplog.set_level(logging.INFO, logger="core.llm_client")
    client = MagicMock()
    client.chat.completions.create.return_value = _make_response()
    call_llm_with_retry(
        client=client,
        model="m",
        messages=[{"role": "user", "content": "hi"}],
        max_tokens=128,
    )
    line = next(l for l in _capture_logs(caplog) if l.startswith("LLM_CALL"))
    assert "stage=-" in line
