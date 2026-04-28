"""V6 canonical report schema package.

Provides:
- normalizer.py: convert existing ActionCard / Alert / ProfileAnalysis
  into the canonical Report dict (matches schemas/report_v6.schema.json).
- issue_registry.py: single-source registry for canonical issue_ids.
  Anything that emits or matches Finding.issue_id MUST go through this
  registry rather than hard-coded strings (W2.5 #5).
- See docs/v6/canonical_schema_inventory.md and docs/v6/output_contract.md
  for design.
"""

from .issue_registry import (
    ALL_ISSUE_IDS,
    ISSUE_BY_CATEGORY,
    ISSUE_BY_ID,
    IssueDef,
    get_definition,
    get_keywords,
    is_known,
)
from .normalizer import build_canonical_report, enrich_llm_canonical

__all__ = [
    "build_canonical_report",
    "enrich_llm_canonical",
    "IssueDef",
    "ISSUES_BY_ID",
    "ISSUE_BY_ID",
    "ISSUE_BY_CATEGORY",
    "ALL_ISSUE_IDS",
    "is_known",
    "get_keywords",
    "get_definition",
]
