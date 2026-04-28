"""V6 feature flags — **default-on kill switches**, not experimental opt-ins.

V6 has been validated and adopted. These flags exist so that *if* a
specific V6 behavior causes a regression, that one behavior can be
disabled in production without redeploying. They are NOT experimental
toggles — flipping them off should be considered an emergency action,
not a configuration choice. See docs/v6/why-default-on.md (ADR
2026-04-26) for the full reasoning.

All flags default to **on**. The v5 path is retained only as a
historical/diagnostic fallback; setting any flag to a falsy value via
env var or runtime-config disables that specific V6 behavior.

Source priority (highest first):
  1. env var (e.g. V6_CANONICAL_SCHEMA=0 to disable)
  2. runtime-config.json key (matching the env var name lowercased)
  3. default (True)

Supported off-patterns (Codex 2026-04-26):
  - default-on        — normal operation (no env / no runtime override)
  - single-flag off   — one flag flipped for triage
  - legacy full-off   — every V6_* set to false (V5 mode)
Other partial combinations are not exercised by tests; reach for one
of the three above when troubleshooting.

Usage:
    from core import feature_flags
    if feature_flags.canonical_schema():
        report = build_canonical_report(...)
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any


# ---------------------------------------------------------------------------
# Public flag names (env var form). Keep names stable — eval/scorers and
# downstream tooling will reference these exact strings.
# ---------------------------------------------------------------------------

V6_CANONICAL_SCHEMA = "V6_CANONICAL_SCHEMA"
V6_REVIEW_NO_KNOWLEDGE = "V6_REVIEW_NO_KNOWLEDGE"
V6_REFINE_MICRO_KNOWLEDGE = "V6_REFINE_MICRO_KNOWLEDGE"
V6_ALWAYS_INCLUDE_MINIMUM = "V6_ALWAYS_INCLUDE_MINIMUM"
V6_SKIP_CONDENSED_KNOWLEDGE = "V6_SKIP_CONDENSED_KNOWLEDGE"
V6_RECOMMENDATION_NO_FORCE_FILL = "V6_RECOMMENDATION_NO_FORCE_FILL"
V6_SQL_SKELETON_EXTENDED = "V6_SQL_SKELETON_EXTENDED"
# V6_COMPACT_TOP_ALERTS retired in v6.6.4: the compact subsection
# rendering became the V6 standard, the legacy "## 2. Top Alerts"
# standalone section was deleted, and the kill-switch retained no
# meaningful triage value (UI rendering is reversible by hand,
# unlike data-flow flags).

ALL_FLAGS = (
    V6_CANONICAL_SCHEMA,
    V6_REVIEW_NO_KNOWLEDGE,
    V6_REFINE_MICRO_KNOWLEDGE,
    V6_ALWAYS_INCLUDE_MINIMUM,
    V6_SKIP_CONDENSED_KNOWLEDGE,
    V6_RECOMMENDATION_NO_FORCE_FILL,
    V6_SQL_SKELETON_EXTENDED,
)


# Lazy-loaded runtime config so we don't depend on importing during module
# init (settings module loads slowly in some environments).
_runtime_overrides: dict[str, Any] | None = None


def _truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _load_runtime_overrides() -> dict[str, Any]:
    """Load V6_* keys from runtime-config.json if present.

    Lookup is best-effort and silent — runtime-config may not exist in
    test environments. Cached so repeated checks don't re-read the file.
    """
    global _runtime_overrides
    if _runtime_overrides is not None:
        return _runtime_overrides
    overrides: dict[str, Any] = {}
    try:
        # Defer import to avoid circular when settings module imports flags
        from core.config_store import get_setting  # noqa: WPS433
    except ImportError:
        get_setting = None  # type: ignore[assignment]
    if get_setting is not None:
        for flag in ALL_FLAGS:
            try:
                v = get_setting(flag.lower(), None)
            except Exception:  # nosec
                v = None
            if v is not None:
                overrides[flag] = v
    _runtime_overrides = overrides
    return overrides


def reset_cache() -> None:
    """Clear cached overrides — used by tests when env / runtime mutates."""
    global _runtime_overrides
    _runtime_overrides = None
    _is_enabled.cache_clear()


@lru_cache(maxsize=64)
def _is_enabled(flag: str) -> bool:
    """Return True iff the named flag is enabled.

    V6 flags are **default-on kill switches** — unset means "use the
    standard V6 behavior", explicit falsy means "fall back to v5".

    Order:
      1. env var
      2. runtime-config.json
      3. default True (V6 standard)
    """
    env = os.environ.get(flag)
    if env is not None:
        return _truthy(env)
    overrides = _load_runtime_overrides()
    if flag in overrides:
        return _truthy(overrides[flag])
    return True


# ---------------------------------------------------------------------------
# Public accessors. Keep the names short — they will appear at call sites.
# ---------------------------------------------------------------------------


def canonical_schema() -> bool:
    """V6_CANONICAL_SCHEMA: emit canonical Finding/Action JSON directly from
    the LLM (Day 3+). When off, the legacy ActionCard path is used and the
    canonical Report is produced via the normalizer adapter.
    """
    return _is_enabled(V6_CANONICAL_SCHEMA)


def review_no_knowledge() -> bool:
    """V6_REVIEW_NO_KNOWLEDGE: skip knowledge injection in Stage 2 review.

    Codex指摘 #1: review が knowledge で誤 reject する主因を取り除く。
    """
    return _is_enabled(V6_REVIEW_NO_KNOWLEDGE)


def refine_micro_knowledge() -> bool:
    """V6_REFINE_MICRO_KNOWLEDGE: in Stage 3 refine, only inject knowledge
    sections that the review explicitly flagged (max 4 KB).

    Codex指摘 #2: refine への全文 knowledge 投入をやめる。
    """
    return _is_enabled(V6_REFINE_MICRO_KNOWLEDGE)


def always_include_minimum() -> bool:
    """V6_ALWAYS_INCLUDE_MINIMUM: collapse ALWAYS_INCLUDE_SECTION_IDS to
    [bottleneck_summary] only (drops spark_params + appendix).

    Codex指摘 #3.
    """
    return _is_enabled(V6_ALWAYS_INCLUDE_MINIMUM)


def skip_condensed_knowledge() -> bool:
    """V6_SKIP_CONDENSED_KNOWLEDGE: do not run the secondary
    `_summarize_sections_with_llm()` pass that builds a condensed
    knowledge view (Codex指摘 #4).
    """
    return _is_enabled(V6_SKIP_CONDENSED_KNOWLEDGE)


def recommendation_no_force_fill() -> bool:
    """V6_RECOMMENDATION_NO_FORCE_FILL: relax the Recommendation format
    block so the LLM is allowed to omit fields when grounding is missing
    (Codex指摘 #8).
    """
    return _is_enabled(V6_RECOMMENDATION_NO_FORCE_FILL)


def sql_skeleton_extended() -> bool:
    """V6_SQL_SKELETON_EXTENDED (V6.1): enable structure extraction for
    MERGE / CREATE VIEW / INSERT instead of bypassing them. Default on
    (V6 standard); flip falsy only to triage a SQL-type-specific
    regression. V6 W5/W6 historical baselines were captured pre-flip
    and are retained for diagnostic comparison.
    """
    return _is_enabled(V6_SQL_SKELETON_EXTENDED)


def snapshot() -> dict[str, bool]:
    """Return the current resolved flag values as a flat dict — useful for
    logging and including in `pipeline_version` metadata."""
    return {f: _is_enabled(f) for f in ALL_FLAGS}
