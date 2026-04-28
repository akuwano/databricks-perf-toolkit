#!/usr/bin/env python3
"""
Databricks SQL Query Profile Analyzer

Analyzes Databricks SQL query profile JSON files using LLM (Databricks Foundation Model APIs)
and generates performance reports in Markdown format.

Based on dbsql_tuning.md best practices.

This module maintains backward compatibility by re-exporting from the core module.
"""

# Re-export everything from core for backward compatibility
# Import main for CLI execution
from cli import main
from core import (
    THRESHOLDS,
    # Models
    BottleneckIndicators,
    JoinInfo,
    # Constants
    JoinType,
    NodeMetrics,
    ProfileAnalysis,
    QueryMetrics,
    Severity,
    ShuffleMetrics,
    # Analyzers
    analyze_from_dict,
    analyze_profile,
    # LLM
    analyze_with_llm,
    calculate_bottleneck_indicators,
    classify_join_type,
    create_analysis_prompt,
    create_system_prompt,
    # Extractors
    extract_join_info,
    extract_node_metrics,
    extract_query_metrics,
    extract_shuffle_metrics,
    # Utils
    format_bytes,
    format_time_ms,
    # Reporters
    generate_bottleneck_summary,
    load_tuning_knowledge,
    refine_with_llm,
    review_with_llm,
)

__all__ = [
    # Constants
    "JoinType",
    "Severity",
    "THRESHOLDS",
    # Models
    "BottleneckIndicators",
    "JoinInfo",
    "NodeMetrics",
    "ProfileAnalysis",
    "QueryMetrics",
    "ShuffleMetrics",
    # Utils
    "format_bytes",
    "format_time_ms",
    # Extractors
    "extract_join_info",
    "extract_node_metrics",
    "extract_query_metrics",
    "extract_shuffle_metrics",
    # Analyzers
    "analyze_from_dict",
    "analyze_profile",
    "calculate_bottleneck_indicators",
    "classify_join_type",
    # LLM
    "analyze_with_llm",
    "create_analysis_prompt",
    "create_system_prompt",
    "load_tuning_knowledge",
    "refine_with_llm",
    "review_with_llm",
    # Reporters
    "generate_bottleneck_summary",
    # CLI
    "main",
]

if __name__ == "__main__":
    main()
