"""
LLM integration functions for query profile analysis.

This module is the public API surface. Implementation is split across:
- llm_client: error classes, retry logic, OpenAI client factory
- llm_prompts: prompt builders, knowledge routing, output parsing
"""

import json
import logging
import re

# Re-export everything from sub-modules for backward compatibility
from .llm_client import (  # noqa: F401
    LLMError,
    LLMRateLimitError,
    LLMServiceError,
    LLMTimeoutError,
    call_llm_with_retry,
    create_openai_client,
)
from .llm_prompts import (  # noqa: F401
    ALWAYS_INCLUDE_SECTIONS,
    CATEGORY_TO_KNOWLEDGE_SECTIONS,
    create_analysis_prompt,
    create_clustering_prompt,
    create_refine_prompt,
    create_refine_system_prompt,
    create_report_refine_prompt,
    create_report_refine_system_prompt,
    create_report_review_prompt,
    create_report_review_system_prompt,
    create_rerank_prompt,
    create_review_prompt,
    create_review_system_prompt,
    create_rewrite_fix_system_prompt,
    create_rewrite_fix_user_prompt,
    create_rewrite_system_prompt,
    create_rewrite_user_prompt,
    create_structured_analysis_prompt,
    create_structured_system_prompt,
    create_system_prompt,
    filter_knowledge_by_alerts,
    filter_knowledge_for_analysis,
    format_review_for_refine,
    get_knowledge_section_refs,
    load_tuning_knowledge,
    parse_knowledge_sections,
    parse_llm_sections,
    parse_rerank_output,
    parse_review_json,
)
from .models import ActionCard, Alert, ProfileAnalysis

logger = logging.getLogger(__name__)

# Backward-compatible alias
_call_llm_with_retry = call_llm_with_retry
_filter_knowledge_for_analysis = filter_knowledge_for_analysis


# =============================================================================
# Public LLM orchestration functions
# =============================================================================


def analyze_with_llm(
    analysis: ProfileAnalysis,
    model: str,
    databricks_host: str,
    databricks_token: str,
    tuning_knowledge: str,
    lang: str | None = None,
    is_serverless: bool = False,
    is_streaming: bool = False,
    is_federation: bool = False,
) -> str:
    """Send metrics to LLM for analysis.

    Returns:
        LLM analysis result or empty string on failure.
    """
    client = create_openai_client(databricks_host, databricks_token)

    from .llm_client import KNOWLEDGE_MAX_CHARS

    relevant_knowledge = filter_knowledge_for_analysis(
        tuning_knowledge,
        analysis.bottleneck_indicators.alerts,
        max_chars=KNOWLEDGE_MAX_CHARS,
        llm_client=client,
        llm_model=model,
    )

    system_prompt = create_structured_system_prompt(
        relevant_knowledge,
        lang,
        is_serverless=is_serverless,
        is_streaming=is_streaming,
        is_federation=is_federation,
    )
    user_prompt = create_structured_analysis_prompt(analysis, lang)

    try:
        return call_llm_with_retry(
            client=client,
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=16384,
            temperature=0.2,
            stage="analyze",
            extra_telemetry={
                "system_chars": len(system_prompt),
                "user_chars": len(user_prompt),
                "is_federation": is_federation,
            },
        )
    except LLMTimeoutError as e:
        logger.error("LLM analysis timed out: %s", e)
        raise  # Propagate timeout to caller for specific error message
    except LLMError as e:
        logger.error("LLM analysis failed: %s", e)
        return ""


def estimate_rewrite_tokens(analysis: ProfileAnalysis) -> int:
    """Estimate output tokens needed for a complete SQL rewrite.

    Used only for model recommendation (whether a model can handle it).
    The actual API call always uses the model's full max_tokens.
    """
    sql_len = len(analysis.query_metrics.query_text or "")
    return max(8192, int(sql_len / 3 * 2) + 512)


def recommend_rewrite_model(analysis: ProfileAnalysis) -> dict:
    """Suggest best model for rewriting based on query length.

    Returns dict with 'recommended_model', 'reason', and 'estimated_tokens'.
    """
    from .llm_client import _MODEL_MAX_OUTPUT_TOKENS

    needed = estimate_rewrite_tokens(analysis)

    # Rank models by max output tokens (descending)
    ranked = sorted(_MODEL_MAX_OUTPUT_TOKENS.items(), key=lambda x: -x[1])
    for model_name, max_tok in ranked:
        if max_tok >= needed:
            return {
                "recommended_model": model_name,
                "estimated_tokens": needed,
                "max_tokens": max_tok,
                "reason": "",
            }

    # All models may be too small — recommend the largest, flag as constrained
    best_name, best_max = ranked[0]
    return {
        "recommended_model": best_name,
        "estimated_tokens": needed,
        "max_tokens": best_max,
        "token_constrained": True,
        "reason": (
            f"Query requires ~{needed:,} output tokens but max available is {best_max:,}. "
            f"Diff-format rewrite with manual merge steps will be generated."
        ),
    }


def rewrite_with_llm(
    analysis: ProfileAnalysis,
    model: str,
    databricks_host: str,
    databricks_token: str,
    tuning_knowledge: str,
    lang: str | None = None,
    is_serverless: bool = False,
) -> str:
    """Generate optimized SQL rewrite based on bottleneck analysis.

    Returns:
        Markdown with rewritten SQL and explanation, or empty string on failure.
    """
    client = create_openai_client(databricks_host, databricks_token)

    from .llm_client import KNOWLEDGE_MAX_CHARS, get_model_max_tokens

    relevant_knowledge = filter_knowledge_for_analysis(
        tuning_knowledge,
        analysis.bottleneck_indicators.alerts,
        max_chars=KNOWLEDGE_MAX_CHARS,
        llm_client=client,
        llm_model=model,
    )

    # Always use the model's full max output tokens — rewrite must not be truncated.
    # estimate_rewrite_tokens is only used for model recommendation warnings.
    needed = estimate_rewrite_tokens(analysis)
    model_max = get_model_max_tokens(model)
    token_constrained = needed > model_max
    max_tokens = model_max

    effective_lang = lang or "en"
    system_prompt = create_rewrite_system_prompt(
        relevant_knowledge,
        effective_lang,
        is_serverless=is_serverless,
        token_constrained=token_constrained,
    )
    user_prompt = create_rewrite_user_prompt(analysis, effective_lang)

    try:
        return call_llm_with_retry(
            client=client,
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=0.1,
            stage="rewrite",
            extra_telemetry={
                "token_constrained": token_constrained,
                "knowledge_chars": len(relevant_knowledge or ""),
            },
        )
    except LLMTimeoutError as e:
        logger.error("LLM rewrite timed out: %s", e)
        raise
    except LLMError as e:
        logger.error("LLM rewrite failed: %s", e)
        return ""


def fix_rewrite_with_llm(
    original_sql: str,
    previous_rewrite: str,
    feedback: str,
    model: str,
    databricks_host: str,
    databricks_token: str,
    lang: str | None = None,
) -> str:
    """Fix a previously rewritten SQL based on user feedback.

    Returns:
        Markdown with fixed SQL, or empty string on failure.
    """
    client = create_openai_client(databricks_host, databricks_token)

    from .llm_client import get_model_max_tokens

    effective_lang = lang or "en"
    system_prompt = create_rewrite_fix_system_prompt(effective_lang)
    user_prompt = create_rewrite_fix_user_prompt(
        original_sql,
        previous_rewrite,
        feedback,
        effective_lang,
    )

    max_tokens = get_model_max_tokens(model)

    try:
        return call_llm_with_retry(
            client=client,
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=0.1,
            stage="rewrite_fix",
        )
    except LLMTimeoutError as e:
        logger.error("LLM rewrite fix timed out: %s", e)
        raise
    except LLMError as e:
        logger.error("LLM rewrite fix failed: %s", e)
        return ""


def review_with_llm(
    analysis: ProfileAnalysis,
    llm_analysis: str,
    primary_model: str,
    review_model: str,
    databricks_host: str,
    databricks_token: str,
    tuning_knowledge: str,
    lang: str | None = None,
    is_serverless: bool = False,
    is_streaming: bool = False,
) -> str:
    """Send analysis to review LLM for validation.

    Returns:
        Review result or empty string on failure.
    """
    client = create_openai_client(databricks_host, databricks_token)

    from .llm_client import KNOWLEDGE_MAX_CHARS

    # V6 (Codex指摘 #1): Stage 2 review に knowledge を入れない。
    # review は format / evidence consistency のみで判定し、誤 reject を減らす。
    try:
        from . import feature_flags as _ff  # noqa: WPS433
    except ImportError:
        _ff = None
    if _ff is not None and _ff.review_no_knowledge():
        logger.info("V6 review_no_knowledge=on: skipping knowledge injection in review")
        relevant_knowledge = ""
    else:
        relevant_knowledge = filter_knowledge_for_analysis(
            tuning_knowledge,
            analysis.bottleneck_indicators.alerts,
            max_chars=KNOWLEDGE_MAX_CHARS,
            llm_client=client,
            llm_model=review_model,
        )

    system_prompt = create_review_system_prompt(
        relevant_knowledge, lang, is_serverless=is_serverless, is_streaming=is_streaming
    )
    user_prompt = create_review_prompt(analysis, llm_analysis, primary_model, lang)

    try:
        return call_llm_with_retry(
            client=client,
            model=review_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=4096,
            temperature=0.2,
            stage="review",
            extra_telemetry={
                "knowledge_chars": len(relevant_knowledge or ""),
                "system_chars": len(system_prompt),
                "user_chars": len(user_prompt),
            },
        )
    except LLMError as e:
        logger.error("LLM review failed: %s", e)
        return ""


def refine_with_llm(
    analysis: ProfileAnalysis,
    initial_llm_analysis: str,
    review_analysis: str,
    refine_model: str,
    databricks_host: str,
    databricks_token: str,
    tuning_knowledge: str,
    primary_model: str,
    review_model: str,
    lang: str | None = None,
    is_serverless: bool = False,
    is_streaming: bool = False,
) -> str:
    """Refine initial analysis based on review comments.

    Returns:
        Refined analysis or empty string on failure.
    """
    client = create_openai_client(databricks_host, databricks_token)

    from .llm_client import KNOWLEDGE_MAX_CHARS

    # V6 (Codex指摘 #2): Stage 3 refine の knowledge は最大 4 KB に縮小、
    # 全文投入をやめる。flag off では従来挙動。
    try:
        from . import feature_flags as _ff  # noqa: WPS433
    except ImportError:
        _ff = None
    if _ff is not None and _ff.refine_micro_knowledge():
        max_chars = 4096
        logger.info("V6 refine_micro_knowledge=on: knowledge budget capped to %d chars", max_chars)
    else:
        max_chars = KNOWLEDGE_MAX_CHARS
    relevant_knowledge = filter_knowledge_for_analysis(
        tuning_knowledge,
        analysis.bottleneck_indicators.alerts,
        max_chars=max_chars,
        llm_client=client,
        llm_model=refine_model,
    )

    system_prompt = create_refine_system_prompt(
        relevant_knowledge, lang, is_serverless=is_serverless, is_streaming=is_streaming
    )
    user_prompt = create_refine_prompt(
        initial_llm_analysis,
        review_analysis,
        primary_model,
        review_model,
        lang,
        analysis=analysis,
    )

    try:
        return call_llm_with_retry(
            client=client,
            model=refine_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=16384,
            temperature=0.2,
            stage="refine",
            extra_telemetry={
                "knowledge_chars": len(relevant_knowledge or ""),
                "knowledge_max_chars": max_chars,
                "system_chars": len(system_prompt),
                "user_chars": len(user_prompt),
            },
        )
    except LLMError as e:
        logger.error("LLM refinement failed: %s", e)
        return ""


def review_report_with_llm(
    report_markdown: str,
    review_model: str,
    databricks_host: str,
    databricks_token: str,
    tuning_knowledge: str,
    lang: str | None = None,
    report_context: dict | None = None,
    alerts: list[Alert] | None = None,
) -> str:
    """Review generated report with LLM.

    Returns:
        Review result or empty string on failure.
    """
    client = create_openai_client(databricks_host, databricks_token)

    from .llm_client import KNOWLEDGE_MAX_CHARS

    relevant_knowledge = filter_knowledge_for_analysis(
        tuning_knowledge,
        alerts or [],
        max_chars=KNOWLEDGE_MAX_CHARS,
        llm_client=client,
        llm_model=review_model,
    )

    system_prompt = create_report_review_system_prompt(relevant_knowledge, lang)
    user_prompt = create_report_review_prompt(report_markdown, report_context, lang)

    try:
        return call_llm_with_retry(
            client=client,
            model=review_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=4096,
            temperature=0.2,
            stage="report_review",
            extra_telemetry={
                "knowledge_chars": len(relevant_knowledge or ""),
                "report_chars": len(report_markdown or ""),
            },
        )
    except LLMError as e:
        logger.error("LLM report review failed: %s", e)
        return ""


def refine_report_with_llm(
    report_markdown: str,
    report_review_markdown: str,
    refine_model: str,
    databricks_host: str,
    databricks_token: str,
    tuning_knowledge: str,
    lang: str | None = None,
    alerts: list[Alert] | None = None,
) -> str:
    """Refine report based on review comments.

    Returns:
        Refined report or empty string on failure.
    """
    client = create_openai_client(databricks_host, databricks_token)

    from .llm_client import KNOWLEDGE_MAX_CHARS

    relevant_knowledge = filter_knowledge_for_analysis(
        tuning_knowledge,
        alerts or [],
        max_chars=KNOWLEDGE_MAX_CHARS,
        llm_client=client,
        llm_model=refine_model,
    )

    system_prompt = create_report_refine_system_prompt(relevant_knowledge, lang)
    user_prompt = create_report_refine_prompt(report_markdown, report_review_markdown, lang)

    try:
        return call_llm_with_retry(
            client=client,
            model=refine_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=16384,
            temperature=0.2,
            stage="report_refine",
            extra_telemetry={
                "knowledge_chars": len(relevant_knowledge or ""),
                "report_chars": len(report_markdown or ""),
                "review_chars": len(report_review_markdown or ""),
            },
        )
    except LLMError as e:
        logger.error("LLM report refinement failed: %s", e)
        return ""


def recommend_clustering_with_llm(
    query_sql: str,
    target_table: str,
    candidate_columns: list[dict],
    top_scanned_tables: list[dict],
    filter_rate: float,
    read_files_count: int,
    pruned_files_count: int,
    model: str,
    databricks_host: str,
    databricks_token: str,
    explain_summary: str = "",
    lang: str | None = None,
    shuffle_metrics=None,
) -> dict | None:
    """Get clustering recommendation from LLM.

    ``shuffle_metrics`` (list[ShuffleMetrics]) is passed through to the
    prompt so the LC LLM can consider runtime shuffle keys as candidates.

    Returns:
        Dictionary with clustering recommendation or None on failure.
    """
    client = create_openai_client(databricks_host, databricks_token)

    system_prompt, user_prompt = create_clustering_prompt(
        query_sql=query_sql,
        target_table=target_table,
        candidate_columns=candidate_columns,
        top_scanned_tables=top_scanned_tables,
        filter_rate=filter_rate,
        read_files_count=read_files_count,
        pruned_files_count=pruned_files_count,
        explain_summary=explain_summary,
        lang=lang,
        shuffle_metrics=shuffle_metrics,
    )

    try:
        response = call_llm_with_retry(
            client=client,
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=1000,
            temperature=0.1,
            stage="clustering",
            extra_telemetry={
                "target_table": target_table,
                "candidate_columns": len(candidate_columns or []),
            },
        )

        # Parse JSON from response
        json_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", response)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = response.strip()

        result = json.loads(json_str)

        if not isinstance(result.get("recommended_keys"), list):
            logger.warning("LLM clustering response missing recommended_keys")
            return None

        return {
            "target_table": result.get("target_table", target_table),
            "recommended_keys": result.get("recommended_keys", [])[:4],
            "workload_pattern": result.get("workload_pattern", "unknown"),
            "rationale": result.get("rationale", ""),
            "confidence": float(result.get("confidence", 0.5)),
            "alternatives": result.get("alternatives", []),
        }

    except json.JSONDecodeError as e:
        logger.warning("LLM clustering response JSON parse error: %s", e)
        return None
    except LLMError as e:
        logger.warning("LLM clustering recommendation failed: %s", e)
        return None
    except Exception as e:
        logger.warning("Unexpected error in clustering recommendation: %s", e)
        return None


def select_top_actions_with_llm(
    action_cards: list[ActionCard],
    review_model: str,
    databricks_host: str,
    databricks_token: str,
    lang: str | None = None,
) -> dict | None:
    """Rerank candidate action cards with LLM."""
    if not action_cards:
        return None
    client = create_openai_client(databricks_host, databricks_token)
    prompt = create_rerank_prompt(action_cards, lang)
    try:
        response = call_llm_with_retry(
            client=client,
            model=review_model,
            messages=[
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=1200,
            temperature=0,
            stage="rerank",
            extra_telemetry={"action_card_count": len(action_cards or [])},
        )
        return parse_rerank_output(response)
    except LLMError as e:
        logger.warning("LLM Top-5 rerank failed: %s", e)
        return None
    except Exception as e:
        logger.warning("Unexpected Top-5 rerank failure: %s", e)
        return None
