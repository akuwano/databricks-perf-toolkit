"""
Prompt construction, knowledge routing, and LLM output parsing.

All ``create_*_prompt`` helpers, knowledge-section routing, and
``parse_*`` / ``format_*`` post-processing live here.
"""

# Bump this version when prompts change in a way that affects output quality.
# Stored in analysis header for tracking which prompt produced which result.
PROMPT_VERSION = "v4.11.0"

from .knowledge import (
    ALWAYS_INCLUDE_SECTIONS,
    CATEGORY_TO_KNOWLEDGE_SECTIONS,
    SPARK_CATEGORY_TO_SECTION_IDS,
    filter_knowledge_by_alerts,
    filter_knowledge_for_analysis,
    filter_spark_knowledge,
    get_knowledge_section_refs,
    load_spark_tuning_knowledge,
    load_tuning_knowledge,
    parse_knowledge_sections,
)
from .parsing import (
    format_review_for_refine,
    parse_action_plan_from_llm,
    parse_llm_sections,
    parse_rerank_output,
    parse_review_json,
)
from .prompts import (
    _build_review_fact_pack,
    _severity_order,
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
)

__all__ = [
    "ALWAYS_INCLUDE_SECTIONS",
    "CATEGORY_TO_KNOWLEDGE_SECTIONS",
    "PROMPT_VERSION",
    "create_analysis_prompt",
    "create_clustering_prompt",
    "create_refine_prompt",
    "create_refine_system_prompt",
    "create_report_refine_prompt",
    "create_report_refine_system_prompt",
    "create_report_review_prompt",
    "create_report_review_system_prompt",
    "create_rerank_prompt",
    "create_review_prompt",
    "create_review_system_prompt",
    "create_rewrite_fix_system_prompt",
    "create_rewrite_fix_user_prompt",
    "create_rewrite_system_prompt",
    "create_rewrite_user_prompt",
    "create_structured_analysis_prompt",
    "create_structured_system_prompt",
    "create_system_prompt",
    "filter_knowledge_by_alerts",
    "filter_knowledge_for_analysis",
    "filter_spark_knowledge",
    "format_review_for_refine",
    "get_knowledge_section_refs",
    "load_spark_tuning_knowledge",
    "load_tuning_knowledge",
    "SPARK_CATEGORY_TO_SECTION_IDS",
    "parse_action_plan_from_llm",
    "parse_knowledge_sections",
    "parse_llm_sections",
    "parse_rerank_output",
    "parse_review_json",
]
