"""Evaluation runner: executes analysis pipeline and scores results."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.usecases import LLMConfig, PipelineOptions, run_analysis_pipeline

from .models import CardEvalResult, EvalReport, QueryEvalResult
from .scorers.l1_syntax import score_l1
from .scorers.l2_evidence import score_l2
from .scorers.l3l4_judge import build_profile_summary, score_l3l4

logger = logging.getLogger(__name__)


def evaluate_profile(
    profile_path: str,
    llm_config: LLMConfig,
    options: PipelineOptions,
    *,
    judge_model: str = "databricks-claude-sonnet-4",
    skip_judge: bool = False,
) -> QueryEvalResult:
    """Run analysis pipeline on a profile and score all ActionCards.

    Args:
        profile_path: Path to profile JSON file
        llm_config: LLM configuration for analysis pipeline
        options: Pipeline options (skip_llm should be False for LLM evaluation)
        judge_model: Model to use for LLM-as-judge scoring
        skip_judge: If True, skip L3/L4 LLM-as-judge (only run L1/L2)
    """
    # Load profile
    try:
        with open(profile_path, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError, ValueError) as e:
        return QueryEvalResult(
            profile_path=profile_path,
            pipeline_error=f"Failed to load profile: {e}",
        )

    # Run pipeline
    try:
        result = run_analysis_pipeline(data, llm_config, options)
    except (RuntimeError, ValueError, TypeError, OSError, KeyError, AttributeError) as e:
        return QueryEvalResult(
            profile_path=profile_path,
            pipeline_error=f"Pipeline failed: {e}",
        )

    analysis = result.analysis
    query_id = analysis.query_metrics.query_id or Path(profile_path).stem
    query_sql = analysis.query_metrics.query_text or ""

    # Detect serverless
    is_serverless = (
        analysis.query_metrics.query_typename == "LakehouseSqlQuery"
        or (analysis.warehouse_info.is_serverless if analysis.warehouse_info else False)
    )

    # Build profile summary for judge
    profile_summary = build_profile_summary(
        analysis.query_metrics,
        analysis.bottleneck_indicators,
    )

    # Score each ActionCard
    card_results: list[CardEvalResult] = []
    for i, card in enumerate(analysis.action_cards):
        l1 = score_l1(card, is_serverless=is_serverless)
        l1.card_index = i

        l2 = score_l2(card, data, analysis)
        l2.card_index = i

        l3, l4 = None, None
        if not skip_judge and llm_config.is_available:
            l3, l4 = score_l3l4(
                card,
                profile_summary,
                query_sql,
                llm_config.databricks_host,
                llm_config.databricks_token,
                judge_model=judge_model,
            )
            l3.card_index = i
            l4.card_index = i

        card_results.append(CardEvalResult(
            card_index=i,
            problem=card.problem,
            expected_impact=card.expected_impact,
            effort=card.effort,
            l1=l1,
            l2=l2,
            l3=l3,
            l4=l4,
        ))

    # Compute aggregates
    return _aggregate_query_result(
        query_id=query_id,
        profile_path=profile_path,
        card_results=card_results,
        primary_model=llm_config.primary_model,
        llm_text=result.llm_analysis or "",
    )


def evaluate_profiles(
    profile_paths: list[str],
    llm_config: LLMConfig,
    options: PipelineOptions,
    *,
    judge_model: str = "databricks-claude-sonnet-4",
    skip_judge: bool = False,
) -> EvalReport:
    """Batch evaluate multiple profiles and aggregate results."""
    query_results: list[QueryEvalResult] = []

    for i, path in enumerate(profile_paths):
        logger.info("Evaluating [%d/%d]: %s", i + 1, len(profile_paths), Path(path).name)
        qr = evaluate_profile(
            path, llm_config, options,
            judge_model=judge_model, skip_judge=skip_judge,
        )
        query_results.append(qr)

    return _aggregate_report(query_results, llm_config, judge_model)


def _aggregate_query_result(
    query_id: str,
    profile_path: str,
    card_results: list[CardEvalResult],
    primary_model: str,
    llm_text: str,
) -> QueryEvalResult:
    """Compute aggregate scores for a single query."""
    n = len(card_results)
    if n == 0:
        return QueryEvalResult(
            query_id=query_id,
            profile_path=profile_path,
            primary_model=primary_model,
            llm_analysis_excerpt=llm_text[:200],
        )

    # L1
    cards_with_sql = [c for c in card_results if c.l1.has_fix_sql]
    l1_syntax = (
        sum(1 for c in cards_with_sql if c.l1.parses_ok) / len(cards_with_sql)
        if cards_with_sql else 1.0
    )
    l1_serverless = (
        sum(1 for c in cards_with_sql if c.l1.serverless_compliant) / len(cards_with_sql)
        if cards_with_sql else 1.0
    )

    # L2
    l2_ratios = [c.l2.grounding_ratio for c in card_results]
    l2_avg = sum(l2_ratios) / len(l2_ratios) if l2_ratios else 1.0

    # L3/L4
    l3_cards = [c for c in card_results if c.l3 is not None]
    l4_cards = [c for c in card_results if c.l4 is not None]

    l3_diag = _avg([c.l3.diagnosis_score for c in l3_cards]) if l3_cards else 0.0
    l3_evq = _avg([c.l3.evidence_quality for c in l3_cards]) if l3_cards else 0.0
    l4_rel = _avg([c.l4.fix_relevance for c in l4_cards]) if l4_cards else 0.0
    l4_feas = _avg([c.l4.fix_feasibility for c in l4_cards]) if l4_cards else 0.0
    l4_imp = _avg([c.l4.expected_improvement for c in l4_cards]) if l4_cards else 0.0

    return QueryEvalResult(
        query_id=query_id,
        profile_path=profile_path,
        num_action_cards=n,
        card_results=card_results,
        l1_syntax_pass_rate=l1_syntax,
        l1_serverless_pass_rate=l1_serverless,
        l2_avg_grounding=l2_avg,
        l3_avg_diagnosis=l3_diag,
        l3_avg_evidence_quality=l3_evq,
        l4_avg_relevance=l4_rel,
        l4_avg_feasibility=l4_feas,
        l4_avg_improvement=l4_imp,
        primary_model=primary_model,
        llm_analysis_excerpt=llm_text[:200],
    )


def _aggregate_report(
    query_results: list[QueryEvalResult],
    llm_config: LLMConfig,
    judge_model: str,
) -> EvalReport:
    """Compute overall aggregates across all queries."""
    valid = [q for q in query_results if not q.pipeline_error]
    if not valid:
        return EvalReport(
            timestamp=datetime.now(timezone.utc).isoformat(),
            num_queries=len(query_results),
            query_results=query_results,
            config={"primary_model": llm_config.primary_model, "judge_model": judge_model},
        )

    return EvalReport(
        timestamp=datetime.now(timezone.utc).isoformat(),
        num_queries=len(query_results),
        query_results=query_results,
        overall_l1_syntax=_avg([q.l1_syntax_pass_rate for q in valid]),
        overall_l1_serverless=_avg([q.l1_serverless_pass_rate for q in valid]),
        overall_l2_grounding=_avg([q.l2_avg_grounding for q in valid]),
        overall_l3_diagnosis=_avg([q.l3_avg_diagnosis for q in valid if q.l3_avg_diagnosis > 0]),
        overall_l4_relevance=_avg([q.l4_avg_relevance for q in valid if q.l4_avg_relevance > 0]),
        overall_l4_feasibility=_avg([q.l4_avg_feasibility for q in valid if q.l4_avg_feasibility > 0]),
        config={
            "primary_model": llm_config.primary_model,
            "review_model": llm_config.review_model,
            "refine_model": llm_config.refine_model,
            "judge_model": judge_model,
        },
    )


def _avg(values: list[float | int]) -> float:
    return sum(values) / len(values) if values else 0.0
