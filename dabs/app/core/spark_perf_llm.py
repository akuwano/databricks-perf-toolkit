"""Spark Perf app-side LLM report generation orchestrator.

Two-call LLM strategy for complete, high-quality reports:
  Call 1: Sections 1-2 + Recommended Actions (analysis + action plan)
  Call 2: Sections 3-7 (detailed analysis with LLM commentary)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from services.spark_perf_reader import SparkPerfReader

logger = logging.getLogger(__name__)

# Required patterns in Call 1 output (at least one must match per group)
_CALL1_REQUIRED_PATTERNS = {
    "section_1": [r"#.*ボトルネック分析サマリー", r"#.*Bottleneck Analysis Summary", r"#.*1\."],
    "section_2": [r"#.*推奨アクション", r"#.*Recommended Actions", r"#.*2\."],
    "bottleneck_eval": [r"ボトルネック評価", r"Bottleneck Evaluation", r"Impact.*Effort.*Priority"],
}

# Required patterns in Call 2 output
_CALL2_REQUIRED_PATTERNS = {
    "appendix": [r"Appendix|付録|詳細分析"],
    "section_a": [r"##.*A[\.\s]|Photon"],
}

_CALL1_MAX_RETRIES = 2


def _validate_output(text: str, patterns: dict[str, list[str]]) -> list[str]:
    """Check that required sections are present in generated text.

    Returns list of missing section names (empty = all present).
    """
    import re

    missing = []
    for name, pats in patterns.items():
        if not any(re.search(p, text, re.IGNORECASE) for p in pats):
            missing.append(name)
    return missing


def _call1_with_validation(client, model, messages, max_tokens) -> dict[str, Any]:
    """Call LLM for sections 1-2 with JSON parse retry and section validation."""
    from .llm_client import call_llm_with_retry
    from .llm_prompts.spark_perf_prompts import parse_spark_perf_response

    result: dict[str, Any] = {
        "summary_text": "",
        "job_analysis_text": "",
        "node_analysis_text": "",
        "top3_text": "",
    }
    for attempt in range(_CALL1_MAX_RETRIES):
        # Increase temperature on retry to get different output
        temperature = 0.1 + (attempt * 0.15)
        response = call_llm_with_retry(
            client=client,
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        result = parse_spark_perf_response(response)
        summary = result.get("summary_text", "")

        # Check 1: Did we get actual content (not raw JSON)?
        if summary.strip().startswith("{"):
            logger.warning(
                "Call 1 attempt %d: summary_text contains raw JSON, retrying", attempt + 1
            )
            continue

        # Check 2: Is summary non-empty?
        if len(summary.strip()) < 100:
            logger.warning(
                "Call 1 attempt %d: summary_text too short (%d chars), retrying",
                attempt + 1,
                len(summary),
            )
            continue

        # Check 3: Required sections present?
        missing = _validate_output(summary, _CALL1_REQUIRED_PATTERNS)
        if missing:
            logger.warning(
                "Call 1 attempt %d: missing sections %s, accepting anyway", attempt + 1, missing
            )

        return result

    # Final attempt exhausted — return last result as-is
    logger.warning(
        "Call 1: all %d attempts produced suboptimal output, using last result", _CALL1_MAX_RETRIES
    )
    return result


def run_spark_perf_report(
    reader: SparkPerfReader,
    app_id: str,
    model: str,
    databricks_host: str = "",
    databricks_token: str = "",
    lang: str = "ja",
    experiment_id: str = "",
    variant: str = "",
    on_stage: Any = None,
) -> dict[str, str]:
    """Run app-side Spark Perf LLM analysis with 2-call strategy.

    Call 1: Sections 1-2 + Recommended Actions
    Call 2: Sections 3-7 (Photon/Concurrency/Executor/Plan/I/O)
    """
    from .llm_client import (
        KNOWLEDGE_MAX_CHARS,
        call_llm_with_retry,
        create_openai_client,
        get_model_max_tokens,
    )
    from .llm_prompts.knowledge import (
        filter_spark_knowledge,
        load_spark_tuning_knowledge,
    )
    from .llm_prompts.spark_perf_prompts import (
        assemble_spark_perf_fact_pack,
        create_spark_perf_analysis_prompt,
        create_spark_perf_sections_prompt,
        create_spark_perf_sections_system_prompt,
        create_spark_perf_system_prompt,
    )

    def notify(stage: str, **kwargs: Any) -> None:
        if on_stage:
            on_stage(stage, **kwargs)

    # Stage 1: Collect data
    notify("collecting_data")
    logger.info("Spark Perf report: collecting data for app_id=%s", app_id)
    fact_pack = assemble_spark_perf_fact_pack(reader, app_id)

    if not fact_pack.get("app_summary"):
        raise ValueError(f"No application summary found for app_id={app_id}")

    total_jobs = fact_pack["app_summary"].get("total_jobs", 0) or 0
    streaming_queries = fact_pack.get("streaming_queries", [])
    if total_jobs == 0 and not streaming_queries:
        raise ValueError(
            f"Application {app_id} has 0 jobs and no streaming queries — nothing to analyze. "
            "This may be an idle cluster session with no executed workloads."
        )

    # Stage 2: Load and filter knowledge
    notify("filtering_knowledge")
    bottleneck_types = [b.get("bottleneck_type", "") for b in fact_pack.get("bottlenecks", [])]
    logger.info("Spark Perf report: detected bottleneck types: %d items", len(bottleneck_types))

    client = create_openai_client(databricks_host, databricks_token)
    max_tokens = get_model_max_tokens(model)

    knowledge = load_spark_tuning_knowledge(lang=lang)
    filtered_knowledge = filter_spark_knowledge(
        knowledge,
        bottleneck_types,
        max_chars=KNOWLEDGE_MAX_CHARS,
        llm_client=client,
        llm_model=model,
    )

    # Stage 3: LLM Call 1 — Sections 1-2 + Recommended Actions
    system1 = create_spark_perf_system_prompt(filtered_knowledge, lang)
    user1 = create_spark_perf_analysis_prompt(fact_pack, lang)
    notify("llm_call_1", prompt_tokens=(len(system1) + len(user1)) // 4)

    logger.info(
        "Spark Perf Call 1: model=%s, max_tokens=%d, system=%d chars, user=%d chars",
        model,
        max_tokens,
        len(system1),
        len(user1),
    )

    call1_messages = [
        {"role": "system", "content": system1},
        {"role": "user", "content": user1},
    ]
    result = _call1_with_validation(client, model, call1_messages, max_tokens)

    # Stage 4: LLM Call 2 — Appendix A-E
    system2 = create_spark_perf_sections_system_prompt(filtered_knowledge, lang)
    user2 = create_spark_perf_sections_prompt(fact_pack, lang)
    notify("llm_call_2", prompt_tokens=(len(system2) + len(user2)) // 4)

    logger.info(
        "Spark Perf Call 2: model=%s, max_tokens=%d, system=%d chars, user=%d chars",
        model,
        max_tokens,
        len(system2),
        len(user2),
    )

    response2 = call_llm_with_retry(
        client=client,
        model=model,
        messages=[
            {"role": "system", "content": system2},
            {"role": "user", "content": user2},
        ],
        max_tokens=max_tokens,
        temperature=0.1,
    )

    # Validate Call 2 output
    missing2 = _validate_output(response2, _CALL2_REQUIRED_PATTERNS)
    if missing2:
        logger.warning("Call 2: missing expected sections %s", missing2)

    # Combine: summary_text = Call1.summary_text + Call2 appendix
    # Strip any preamble before Appendix heading (handles LLM variations)
    import re

    appendix = response2.strip()
    match = re.search(r"^#+ *(?:Appendix|付録|詳細分析)", appendix, re.MULTILINE | re.IGNORECASE)
    if not match:
        match = re.search(r"^##+ *A[\.\s]", appendix, re.MULTILINE)
    if match:
        appendix = appendix[match.start() :]

    # Insert Python-generated sections (B preamble, D)
    from .spark_perf_markdown import (
        build_cost_section,
        build_serialization_section,
        build_sizing_section,
        build_sql_execution_table,
        build_streaming_section,
    )

    # Insert SQL execution table (Python-generated) at beginning of Section B
    sql_table_md = build_sql_execution_table(
        sql_plan_top5=fact_pack.get("sql_plan_top5", []),
        lang=lang,
    )
    if sql_table_md:
        b_match = re.search(r"(^##+ *B[\.\s][^\n]*\n)", appendix, re.MULTILINE)
        if b_match:
            insert_pos = b_match.end()
            appendix = appendix[:insert_pos] + "\n" + sql_table_md + "\n" + appendix[insert_pos:]

    # Insert D. Serialization Analysis (Python-generated) between C and E
    serialization_md = build_serialization_section(
        serialization_summary=fact_pack.get("serialization_summary", {}),
        udf_analysis=fact_pack.get("udf_analysis", []),
        high_ser_jobs=fact_pack.get("high_ser_jobs", []),
        lang=lang,
    )
    e_match = re.search(r"^##+ *E[\.\s]", appendix, re.MULTILINE)
    if e_match and serialization_md:
        appendix = (
            appendix[: e_match.start()] + serialization_md + "\n\n" + appendix[e_match.start() :]
        )
    elif serialization_md:
        appendix += "\n\n" + serialization_md

    combined_summary = result.get("summary_text", "")
    if appendix:
        combined_summary += "\n\n---\n\n" + appendix

    # Append G. Streaming Analysis (Python-generated, always shown)
    streaming_md = build_streaming_section(
        streaming_queries=fact_pack.get("streaming_queries", []),
        streaming_summary=fact_pack.get("streaming_summary", {}),
        streaming_batches=fact_pack.get("streaming_batches"),
        idle_events=fact_pack.get("streaming_idle_events"),
        lang=lang,
    )
    combined_summary += "\n\n" + streaming_md

    # Append streaming deep analysis sub-sections to Section G
    from .spark_perf_markdown import build_streaming_analysis_comment, build_streaming_deep_section

    streaming_deep = fact_pack.get("streaming_deep", {})
    deep_md = build_streaming_deep_section(streaming_deep, lang=lang)
    if deep_md:
        combined_summary += "\n\n" + deep_md

    # Append streaming analysis comment AFTER all sub-sections (State/Watermark included)
    from .spark_perf_markdown import estimate_trigger_interval_ms as _est_trigger

    _global_trigger = _est_trigger(
        fact_pack.get("streaming_idle_events"),
        fact_pack.get("streaming_batches"),
    )
    analysis_comment_md = build_streaming_analysis_comment(
        streaming_summary=fact_pack.get("streaming_summary", {}),
        streaming_batches=fact_pack.get("streaming_batches"),
        streaming_queries=fact_pack.get("streaming_queries"),
        deep_analysis=streaming_deep,
        trigger_interval_ms=_global_trigger,
        lang=lang,
    )
    if analysis_comment_md:
        combined_summary += "\n\n" + analysis_comment_md

    # Append H. Cost Estimate (Python-generated, always shown)
    cost_md = build_cost_section(fact_pack.get("dbu_estimate", {}), lang=lang)
    combined_summary += "\n\n" + cost_md

    # Append I. Cluster Right-Sizing (Python-generated, always shown)
    # Reuse sizing_recommendations already computed in fact_pack (no duplicate queries)
    sizing_recs = fact_pack.get("sizing_recommendations", [])
    sizing_md = build_sizing_section(sizing_recs, fact_pack.get("app_summary", {}), lang=lang)
    combined_summary += "\n\n" + sizing_md

    result["summary_text"] = combined_summary

    # Stage 5: Write to Delta table
    notify("writing")
    prompt_tokens = (len(system1) + len(user1) + len(system2) + len(user2)) // 4
    call1_output_len = len(result.get("summary_text", "")) + len(result.get("top3_text", ""))
    total_tokens = prompt_tokens + (call1_output_len + len(response2)) // 4
    result["prompt_tokens"] = prompt_tokens
    result["total_tokens"] = total_tokens

    # Cost is stored in gold_application_summary (ETL-computed), not in narrative.
    success = reader.write_narrative_summary(
        app_id=app_id,
        model_name=model,
        output_lang=lang,
        summary_text=result.get("summary_text", ""),
        job_analysis_text=result.get("job_analysis_text", ""),
        node_analysis_text=result.get("node_analysis_text", ""),
        top3_text=result.get("top3_text", ""),
        prompt_tokens=prompt_tokens,
        total_tokens=total_tokens,
        experiment_id=experiment_id,
        variant=variant,
    )
    if not success:
        logger.warning("Failed to persist narrative for app_id=%s (non-fatal)", app_id)

    notify("done")
    logger.info("Spark Perf report: completed for app_id=%s", app_id)
    return result
