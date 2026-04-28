"""Phase 5 of ``enrich_llm_canonical``: clean up Evidence objects.

The schema constrains ``Evidence.value_raw`` to ``number | string | null``
(see ``schemas/report_v6.schema.json``). The LLM occasionally emits a
Boolean — most famously the n=32 ``cross_join_explosion_q1`` case
where ``value_raw: false`` was used to mean "no cross-join detected".
The schema rejects Booleans, so the canonical Report fails validation
even though the rest of the finding is sound.

This phase drops ``value_raw`` when it carries a type the schema
forbids. Dropping is preferred over coercion because:

  - ``value_raw`` is optional in the schema, so dropping is safe.
  - Coercing ``False`` to ``null`` would invent semantics; the LLM
    meant *something* by emitting it, and the better next step is
    a prompt-side fix, not a normaliser hallucination.

The prompt directive in ``_v6_canonical_output_directive`` was
strengthened in v6.7.2 to teach the LLM the type rule. This phase is
the post-process safety net (two-tier defence pattern that v6.6.4
established for ``fix_sql``).
"""

from __future__ import annotations

from typing import Any


def _is_valid_value_raw(value: Any) -> bool:
    """``True`` only when the schema would accept the value.

    Allowed: ``int``, ``float``, ``str``, ``None``.
    Forbidden: ``bool`` (Booleans are a subclass of ``int`` in Python,
    hence the explicit ``isinstance(..., bool)`` check first), ``list``,
    ``dict``, anything else.
    """
    if isinstance(value, bool):
        return False
    if value is None:
        return True
    return isinstance(value, (int, float, str))


def sanitize_evidence(out: dict[str, Any]) -> dict[str, Any]:
    """Walk findings + appendix and drop schema-invalid ``value_raw``
    entries on each evidence dict. Mutates and returns ``out``."""
    for finding_block in ("findings", "appendix_excluded_findings"):
        for f in out.get(finding_block) or []:
            for ev in f.get("evidence") or []:
                if not isinstance(ev, dict):
                    continue
                if "value_raw" in ev and not _is_valid_value_raw(ev["value_raw"]):
                    ev.pop("value_raw")
    return out
