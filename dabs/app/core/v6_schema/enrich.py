"""Orchestrates the V6 LLM-direct canonical Report enrichment.

The pipeline runs five steps in order — the order is part of the
contract because each step assumes the previous one has run:

  1. ``metadata_repair`` — fill missing schema_version / report_id /
     generated_at / query_id. No findings touched.
  2. ``context_rebuild`` — overwrite ``out['context']`` from the
     analysis (authoritative).
  3. ``enum_canonicalize`` — rewrite findings/appendix enum fields
     (issue_id / category / fix_type) via the alias maps. Optional
     ``AliasHitCounts`` tracker counts rewrites.
  4. ``verification_reshape`` — coerce LLM-emitted verification
     dicts into one of the schema's three oneOf branches.
  5. ``evidence_sanitize`` — drop ``Evidence.value_raw`` entries
     carrying schema-invalid types (Booleans, lists, dicts). v6.7.2.

The split is per the v6.6.5+ refactor plan in
``docs/v6/alias-admission-rule.md``. Idempotent: running the pipeline
twice on the same input yields the same output (locked down by
``test_enrich_split_idempotent``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from core.models import ProfileAnalysis

from .context_rebuild import rebuild_context
from .enum_canonicalize import canonicalize_enums
from .evidence_sanitize import sanitize_evidence
from .metadata_repair import repair_metadata
from .verification_reshape import reshape_verifications

if TYPE_CHECKING:
    from .alias_telemetry import AliasHitCounts


def enrich_llm_canonical(
    extracted: dict[str, Any],
    analysis: ProfileAnalysis,
    *,
    language: str = "en",
    alias_tracker: "AliasHitCounts | None" = None,
) -> dict[str, Any]:
    """Fill fields the LLM cannot reasonably invent (operational
    metadata + ``context``) and normalize known enum aliases.

    Returns a new dict (caller's input is not mutated). Existing
    truthy values are preserved; missing values get sensible defaults.
    ``query_id`` is always sourced from the analysis (LLM-supplied is
    unreliable).

    The five-step pipeline is intentionally explicit so each phase
    can be tested in isolation. See module docstring for the contract.
    """
    out: dict[str, Any] = dict(extracted)
    repair_metadata(out, analysis)
    rebuild_context(out, analysis, language)
    canonicalize_enums(out, alias_tracker)
    reshape_verifications(out)
    sanitize_evidence(out)
    return out
