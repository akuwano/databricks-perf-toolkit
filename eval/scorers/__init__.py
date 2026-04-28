"""Evaluation scorers for SQL recommendation quality.

V6 scorer ↔ rubric mapping (docs/eval/report_quality_rubric.md section 7):

| Rubric             | Scorer                       | Type                       |
|--------------------|------------------------------|----------------------------|
| L1 Format/Schema   | l1_syntax.score_l1           | mechanical                 |
| L2 Evidence        | l2_evidence (existing)       | mechanical + LLM judge     |
| L3 Diagnosis       | l3l4_judge (existing)        | LLM-as-judge               |
| L4 Actionability   | l3l4_judge (existing)        | LLM-as-judge               |
| Hallucination      | hallucination.score_*        | mechanical + LLM (Week 3+) |
| Action specificity | actionability.score_*        | mechanical                 |
| Critical recall    | recall.score_recall          | mechanical                 |
| Regression         | diff_judge (Week 6 redesign) | aggregation                |
"""

from .actionability import (
    aggregate_actionability,
    score_actionability,
    score_canonical_action,
    score_canonical_report_actions,
)
from .hallucination import (
    aggregate_hallucination,
    score_canonical_report_hallucination,
    score_hallucination,
)
from .r4_schema import aggregate_schema_pass_rate, score_schema
from .evidence_grounding import (
    aggregate_evidence_grounding,
    score_evidence_grounding,
)
from .failure_taxonomy import (
    CATEGORIES as FAILURE_CATEGORIES,
    aggregate_failure_taxonomy,
    score_failure_taxonomy,
)
from .recall import score_canonical_recall, score_recall

__all__ = [
    "score_actionability",
    "score_canonical_action",
    "score_canonical_report_actions",
    "aggregate_actionability",
    "score_hallucination",
    "score_canonical_report_hallucination",
    "aggregate_hallucination",
    "score_recall",
    "score_canonical_recall",
    "score_evidence_grounding",
    "aggregate_evidence_grounding",
    "score_failure_taxonomy",
    "aggregate_failure_taxonomy",
    "FAILURE_CATEGORIES",
    "score_schema",
    "aggregate_schema_pass_rate",
]
