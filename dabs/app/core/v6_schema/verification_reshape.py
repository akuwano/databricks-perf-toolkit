"""Step 4 of ``enrich_llm_canonical``: reshape verification entries.

Verification (under ``Action.verification[]``) uses a JSON-Schema
``oneOf`` with three branches: ``type=metric``, ``type=sql``,
``type=explain``. ``additionalProperties: false`` per branch means a
foreign key invalidates the whole entry. This step coerces common
LLM-emitted near-misses into one of the valid branches:

  - ``{type:'sql', metric:'<sql>'}`` → ``{type:'sql', sql:'<sql>'}``
  - ``{type:'ddl_check', metric:'<sql>'}`` → ``type:'sql'``
  - ``{type:'query'/'analyze'/...}`` carrying SQL-looking content →
    coerced to ``type:'sql'`` only when the content actually contains
    a SQL keyword (DESCRIBE/SELECT/EXPLAIN/SHOW/ANALYZE TABLE).

Unknown shapes that lack a SQL signal are returned unchanged so the
schema validator can still flag genuinely new patterns for prompt-side
correction.
"""

from __future__ import annotations

from typing import Any


# Schema enum for ``Verification.type`` (oneOf branch keys).
VALID_VERIFICATION_TYPES = ("metric", "sql", "explain")


def reshape_verification_entry(entry: Any) -> Any:
    """Coerce a single verification dict into a valid branch when
    possible. Returns the entry unchanged when no coercion applies."""
    if not isinstance(entry, dict):
        return entry
    typ = entry.get("type")
    # ``type=sql`` is valid, but the schema requires the value to live
    # under ``sql`` (not ``metric``). Smoke n=5c showed the LLM
    # sometimes writes ``{type:'sql', metric:'...'}``. Reshape.
    if typ == "sql" and "sql" not in entry and "metric" in entry:
        return {
            "type": "sql",
            "sql": entry["metric"],
            "expected": entry.get("expected", ""),
        }
    if typ in VALID_VERIFICATION_TYPES:
        return entry  # Already canonical.

    # If the entry carries SQL-looking content under a non-canonical
    # type, coerce to the ``sql`` branch.
    sql_value = entry.get("sql") or entry.get("metric") or ""
    if isinstance(sql_value, str) and any(
        kw in sql_value.upper()
        for kw in ("DESCRIBE", "SHOW ", "SELECT ", "EXPLAIN", "ANALYZE TABLE")
    ):
        expected = entry.get("expected") or ""
        return {"type": "sql", "sql": sql_value, "expected": expected}
    return entry


def reshape_verifications(out: dict[str, Any]) -> dict[str, Any]:
    """Walk findings + appendix and reshape each ``verification`` list
    in place. Returns ``out`` for chaining."""
    for finding_block in ("findings", "appendix_excluded_findings"):
        for f in out.get(finding_block) or []:
            for a in f.get("actions") or []:
                if isinstance(a.get("verification"), list):
                    a["verification"] = [
                        reshape_verification_entry(v) for v in a["verification"]
                    ]
    return out
