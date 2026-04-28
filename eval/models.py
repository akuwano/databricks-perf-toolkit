"""Evaluation data models for SQL recommendation quality scoring."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class L1Score:
    """L1: Syntax validity score for a single ActionCard."""

    card_index: int
    has_fix_sql: bool
    parses_ok: bool
    parse_error: str = ""
    serverless_compliant: bool = True
    unsupported_configs: list[str] = field(default_factory=list)


@dataclass
class L2Score:
    """L2: Evidence grounding score for a single ActionCard."""

    card_index: int
    evidence_count: int
    grounded_count: int
    ungrounded_evidence: list[str] = field(default_factory=list)
    grounding_ratio: float = 1.0  # 0.0-1.0; default 1.0 for no-evidence cards


@dataclass
class L3Score:
    """L3: Diagnosis accuracy via LLM-as-judge."""

    card_index: int
    diagnosis_score: int = 0  # 1-5: bottleneck identification accuracy
    evidence_quality: int = 0  # 1-5: specificity and verifiability of evidence
    reasoning: str = ""


@dataclass
class L4Score:
    """L4: Fix effectiveness prediction via LLM-as-judge."""

    card_index: int
    fix_relevance: int = 0  # 1-5: does fix address the diagnosed bottleneck?
    fix_feasibility: int = 0  # 1-5: executable without side effects?
    expected_improvement: int = 0  # 1-5: likelihood of meaningful improvement
    reasoning: str = ""


@dataclass
class ActionabilityScore:
    """Q4: Action specificity score for a single ActionCard.

    W1-W3: 6 dimensions (target, what, why, how, expected_effect, verification).
    W5 Day 4: 7th dimension `citation` — fix_sql_skeleton references
    identifiers that exist in profile evidence. >=6 of 7 → specific.
    """

    card_index: int
    has_target: bool = False
    has_what: bool = False
    has_why: bool = False
    has_how: bool = False
    has_expected_effect: bool = False
    has_verification: bool = False
    has_citation: bool = False  # W5: skeleton-grounded identifier reference
    is_specific: bool = False  # True if >=6 of 7 dimensions are present (was 5/6)


@dataclass
class HallucinationScore:
    """Hallucination detection score for a single ActionCard.

    mechanical first pass: check forbidden_claims keyword hits, missing
    table/column references. LLM judge for nuanced cases (Week 3+).
    """

    card_index: int
    forbidden_claim_hits: list[str] = field(default_factory=list)
    unsupported_value_claims: list[str] = field(default_factory=list)
    score: float = 1.0  # 1.0 = no hallucination, 0.0 = severe


@dataclass
class RecallScore:
    """Critical issue recall score for a query.

    Compares report content against golden case must_cover_issues.
    """

    must_cover_count: int = 0
    covered_count: int = 0
    missed_issues: list[str] = field(default_factory=list)
    recall_ratio: float = 1.0  # 0.0-1.0


@dataclass
class CardEvalResult:
    """Combined evaluation result for a single ActionCard."""

    card_index: int
    problem: str = ""
    expected_impact: str = ""
    effort: str = ""
    l1: L1Score = field(default_factory=lambda: L1Score(0, False, True))
    l2: L2Score = field(default_factory=lambda: L2Score(0, 0, 0))
    l3: L3Score | None = None
    l4: L4Score | None = None
    actionability: ActionabilityScore | None = None
    hallucination: HallucinationScore | None = None


@dataclass
class QueryEvalResult:
    """Evaluation results for one profile/query."""

    query_id: str = ""
    profile_path: str = ""
    num_action_cards: int = 0
    card_results: list[CardEvalResult] = field(default_factory=list)
    # L1/L2 aggregates
    l1_syntax_pass_rate: float = 0.0
    l1_serverless_pass_rate: float = 0.0
    l2_avg_grounding: float = 0.0
    # L3/L4 aggregates
    l3_avg_diagnosis: float = 0.0
    l3_avg_evidence_quality: float = 0.0
    l4_avg_relevance: float = 0.0
    l4_avg_feasibility: float = 0.0
    l4_avg_improvement: float = 0.0
    # V6 aggregates (Week 1 Day 4 stubs)
    actionability_specific_ratio: float = 0.0  # Q4: >=5/6 dims ratio
    hallucination_score_avg: float = 1.0  # Hallucination: 1.0 = clean
    recall: RecallScore | None = None  # Critical issue recall (per query)
    # Metadata
    pipeline_error: str = ""
    primary_model: str = ""
    llm_analysis_excerpt: str = ""


@dataclass
class EvalReport:
    """Aggregate evaluation report across all queries."""

    timestamp: str = ""
    num_queries: int = 0
    query_results: list[QueryEvalResult] = field(default_factory=list)
    # Overall aggregates
    overall_l1_syntax: float = 0.0
    overall_l1_serverless: float = 0.0
    overall_l2_grounding: float = 0.0
    overall_l3_diagnosis: float = 0.0
    overall_l4_relevance: float = 0.0
    overall_l4_feasibility: float = 0.0
    config: dict = field(default_factory=dict)
