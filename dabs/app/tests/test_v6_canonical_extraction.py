"""Tests for V6 canonical-direct LLM output extraction (W3.5 #1)."""

from __future__ import annotations

import pytest

from core import feature_flags
from core.llm_prompts.parsing import extract_v6_canonical_block


@pytest.fixture(autouse=True)
def _reset_flags(monkeypatch):
    for flag in feature_flags.ALL_FLAGS:
        monkeypatch.delenv(flag, raising=False)
    feature_flags.reset_cache()
    yield
    feature_flags.reset_cache()


_VALID_BLOCK = """\
# Report

Some narrative report goes here...

## 1. ヘッドライン

問題が見つかりました。

```json:canonical_v6
{
  "schema_version": "v6.0",
  "summary": {"headline": "Spill detected", "verdict": "needs_attention"},
  "findings": [
    {
      "issue_id": "spill_dominant",
      "category": "memory",
      "severity": "high",
      "title": "Spill",
      "evidence": [
        {"metric": "peak_memory_bytes", "value_display": "12 GB", "value_raw": 12884901888, "source": "profile.queryMetrics", "grounded": true}
      ],
      "actions": []
    }
  ]
}
```
"""


def test_extract_returns_dict_when_block_present():
    result = extract_v6_canonical_block(_VALID_BLOCK)
    assert isinstance(result, dict)
    assert result["schema_version"] == "v6.0"
    assert result["findings"][0]["issue_id"] == "spill_dominant"


def test_extract_returns_none_when_no_block():
    assert extract_v6_canonical_block("just a regular report\n## 1. headline\n") is None


def test_extract_returns_none_for_empty_input():
    assert extract_v6_canonical_block("") is None
    assert extract_v6_canonical_block(None) is None  # type: ignore[arg-type]


def test_extract_returns_none_for_invalid_json():
    bad = """\
text
```json:canonical_v6
{ this is not valid json }
```
"""
    assert extract_v6_canonical_block(bad) is None


def test_extract_returns_none_for_non_object():
    not_obj = """\
text
```json:canonical_v6
[1,2,3]
```
"""
    assert extract_v6_canonical_block(not_obj) is None


def test_extract_first_block_when_multiple():
    """If multiple canonical blocks appear, return the first parsed."""
    multi = """\
text
```json:canonical_v6
{"schema_version": "v6.0", "summary": {"headline": "first"}, "findings": []}
```

later

```json:canonical_v6
{"schema_version": "v6.0", "summary": {"headline": "second"}, "findings": []}
```
"""
    result = extract_v6_canonical_block(multi)
    assert result is not None
    assert result["summary"]["headline"] == "first"


# ----- prompt directive flag wiring -----


def test_directive_includes_source_taxonomy_allowlist():
    """V5 vs V6 smoke 4 (2026-04-26) revealed the LLM emits creative
    ``source`` values like "I/O Metrics" / "Detected Signals" that
    fail the ``valid_source_ratio`` taxonomy check (V6 ratio = 0.0
    on the spill_heavy case). The directive must list the allowed
    prefixes so the LLM has explicit guidance."""
    from core.llm_prompts.prompts import _v6_canonical_output_directive

    for lang in ("ja", "en"):
        d = _v6_canonical_output_directive(lang)
        # The four canonical taxonomy prefixes must be enumerated.
        assert "profile." in d, f"{lang}: missing profile.* prefix"
        assert "alert:" in d, f"{lang}: missing alert: prefix"
        assert "node[" in d, f"{lang}: missing node[...] prefix"
        assert "knowledge:" in d, f"{lang}: missing knowledge: prefix"


def test_directive_inlines_canonical_issue_id_allowlist():
    """Smoke n=5 (2026-04-26) showed V6 LLM emits creative issue_ids
    (``full_outer_join_data_explosion`` etc.) instead of the canonical
    registry. Recall_strict went to 0.000 across all cases. The
    directive must enumerate the 31 canonical IDs so the LLM has the
    exact list to pick from."""
    from core.llm_prompts.prompts import _v6_canonical_output_directive
    from core.v6_schema.issue_registry import ALL_ISSUE_IDS

    for lang in ("ja", "en"):
        d = _v6_canonical_output_directive(lang)
        # All 31 canonical issue IDs must appear in the directive body
        # (so the LLM sees the exact strings).
        for issue_id in ALL_ISSUE_IDS:
            assert issue_id in d, f"{lang}: missing canonical issue_id {issue_id!r}"


def test_directive_includes_numeric_grounding_rule():
    """Smoke 4: V6 ungrounded_numeric_ratio=0.6 because the LLM
    cites derived ratios in narrative ("89倍", "5倍") without
    reflecting them in evidence. The directive must require every
    narrative-cited number to appear in some evidence value_display."""
    from core.llm_prompts.prompts import _v6_canonical_output_directive

    for lang in ("ja", "en"):
        d = _v6_canonical_output_directive(lang)
        # Look for the directive's intent — either Japanese "数値"
        # phrasing or English "numeric" phrasing.
        if lang == "ja":
            assert "数値" in d
            assert "evidence" in d.lower()
        else:
            assert "numeric" in d.lower() or "number" in d.lower()
            assert "evidence" in d.lower()


def test_directive_off_when_flag_disabled(monkeypatch):
    """Kill-switch path: V6_CANONICAL_SCHEMA explicitly disabled →
    directive collapses to empty string (legacy v5 prompt)."""
    from core.llm_prompts.prompts import _v6_canonical_output_directive

    monkeypatch.setenv(feature_flags.V6_CANONICAL_SCHEMA, "0")
    feature_flags.reset_cache()
    assert _v6_canonical_output_directive("ja") == ""
    assert _v6_canonical_output_directive("en") == ""


def test_directive_on_by_default():
    """V6 standard: directive present without any explicit setting."""
    from core.llm_prompts.prompts import _v6_canonical_output_directive

    out_ja = _v6_canonical_output_directive("ja")
    out_en = _v6_canonical_output_directive("en")
    assert "json:canonical_v6" in out_ja
    assert "json:canonical_v6" in out_en
    assert "V6_CANONICAL_SCHEMA" in out_ja
    assert "V6_CANONICAL_SCHEMA" in out_en
    # Directive contains the schema-essential keys
    for keyword in ("issue_id", "evidence", "actions", "schema_version"):
        assert keyword in out_ja
        assert keyword in out_en
