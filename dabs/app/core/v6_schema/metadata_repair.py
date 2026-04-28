"""Step 1 of ``enrich_llm_canonical``: fill operational metadata.

The LLM cannot reasonably invent UUIDs, timestamps, or authoritative
query identifiers. This step plugs those holes so the canonical Report
passes top-level required-field validation without forcing a prompt
change cycle.

Authoritative source per field:
- ``schema_version``: package constant (``models.SCHEMA_VERSION``).
- ``report_id``: fresh UUID4 — generated here, the LLM-supplied value
  is discarded if present (LLMs hallucinate UUIDs).
- ``generated_at``: current UTC time in ISO-8601 ``Z`` form.
- ``query_id``: profile is authoritative — overwrite any LLM-supplied
  value when the profile carries one. Fall back to ``"unknown"`` only
  when the profile itself is missing the id.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from core.models import ProfileAnalysis

from ._constants import SCHEMA_VERSION


def repair_metadata(out: dict[str, Any], analysis: ProfileAnalysis) -> dict[str, Any]:
    """Fill missing top-level metadata. Mutates ``out`` in place and
    also returns it for chaining inside the orchestrator."""
    qm = analysis.query_metrics

    if not out.get("schema_version"):
        out["schema_version"] = SCHEMA_VERSION
    if not out.get("report_id"):
        out["report_id"] = str(uuid.uuid4())
    if not out.get("generated_at"):
        out["generated_at"] = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    profile_qid = (qm.query_id if qm else "") or ""
    if profile_qid:
        out["query_id"] = profile_qid
    elif not out.get("query_id"):
        out["query_id"] = "unknown"

    return out
