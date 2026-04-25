"""
Analysis functions for query profiles.

This package provides bottleneck detection, EXPLAIN analysis,
hot operator extraction, and action card generation.
"""

import json
from typing import Any

from ..evidence import build_evidence
from ..extractors import (
    extract_data_flow,
    extract_data_flow_dag,
    extract_endpoint_id,
    extract_join_info,
    extract_node_metrics,
    extract_query_metrics,
    extract_shuffle_metrics,
    extract_sql_analysis,
    extract_stage_info,
    extract_streaming_context,
    extract_table_scan_metrics,
    extract_target_table_info,
    normalize_profile_data,
    populate_federation_signals,
)
from ..models import ProfileAnalysis
from .bottleneck import (
    _analyze_extra_metrics,
    _generate_extra_metrics_warnings,
    calculate_bottleneck_indicators,
)
from .explain_analysis import (
    PHOTON_BLOCKER_RULES,
    _classify_photon_blocker,
    _detect_simple_photon_blockers,
    _extract_function_name,
    _extract_photon_blockers_from_explain,
    _parse_photon_explanation_structured,
    enhance_bottleneck_with_explain,
)
from .operators import (
    _update_top_scanned_with_clustering,
    extract_hot_operators,
)
from .recommendations import (
    generate_action_cards,
    generate_sql_improvement_examples,
)


def analyze_profile(profile_path: str) -> ProfileAnalysis:
    """Analyze a query profile JSON file."""
    with open(profile_path, encoding="utf-8") as f:
        data = json.load(f)

    return analyze_from_dict(data)


def analyze_from_dict(
    data: dict[str, Any],
    llm_clustering_config: dict | None = None,
) -> ProfileAnalysis:
    """Analyze a query profile from a dictionary (for Web API use).

    Args:
        data: Query profile data dictionary
        llm_clustering_config: Optional LLM configuration for clustering recommendation
            {
                "model": str,  # e.g., "databricks-claude-opus-4-7"
                "databricks_host": str,
                "databricks_token": str,
                "lang": str  # "en" or "ja"
            }
    """
    # Normalize profile data to handle different formats (DBSQL, Spark Connect, etc.)
    data = normalize_profile_data(data)

    query_metrics = extract_query_metrics(data)
    node_metrics = extract_node_metrics(data)
    # v5.18.0: Lakehouse Federation detection before bottleneck analysis
    # so downstream gates (cards that would misfire on federated
    # scans) can consult ``query_metrics.is_federation_query``.
    populate_federation_signals(query_metrics, node_metrics)
    shuffle_metrics = extract_shuffle_metrics(data)
    join_info = extract_join_info(data)
    sql_analysis = extract_sql_analysis(data)
    bottleneck_indicators = calculate_bottleneck_indicators(
        query_metrics, node_metrics, shuffle_metrics, join_info
    )

    # Extract hot operators
    hot_operators = extract_hot_operators(node_metrics, query_metrics)

    # Extract top scanned tables with I/O metrics (needed for LC recommendations)
    top_scanned_tables = extract_table_scan_metrics(node_metrics, sql_analysis)

    # Generate action cards (with optional LLM clustering recommendation)
    action_cards = generate_action_cards(
        bottleneck_indicators,
        hot_operators,
        query_metrics,
        shuffle_metrics,
        join_info,
        sql_analysis,
        top_scanned_tables,
        llm_clustering_config,
    )

    # Generate SQL improvement examples (deprecated, kept for compatibility)
    sql_improvement_examples = generate_sql_improvement_examples(
        bottleneck_indicators,
        hot_operators,
        shuffle_metrics,
        join_info,
        sql_analysis,
    )

    # Extract endpoint ID for warehouse lookup
    endpoint_id = extract_endpoint_id(data)

    # Extract stage execution info and data flow for new report structure
    stage_info = extract_stage_info(data)
    data_flow = extract_data_flow(data)
    data_flow_dag = extract_data_flow_dag(data)

    # Extract streaming context if applicable
    streaming_context = extract_streaming_context(data)

    # Extract target table metadata (INSERT/CTAS/MERGE only, else None).
    # Authoritative source for "is target Delta?" — the Write node's
    # IS_DELTA flag only reflects the file-level writer.
    target_table_info = extract_target_table_info(data)

    # If we have a Delta + Liquid Clustering write target, check whether the
    # pre-write re-shuffle is spilling (classic ClusterOnWrite overhead).
    from .explain_analysis import detect_lc_cluster_on_write_overhead

    detect_lc_cluster_on_write_overhead(bottleneck_indicators, shuffle_metrics, target_table_info)

    # Create intermediate ProfileAnalysis for building evidence
    analysis = ProfileAnalysis(
        query_metrics=query_metrics,
        node_metrics=node_metrics,
        shuffle_metrics=shuffle_metrics,
        join_info=join_info,
        bottleneck_indicators=bottleneck_indicators,
        sql_analysis=sql_analysis,
        hot_operators=hot_operators,
        action_cards=action_cards,
        sql_improvement_examples=sql_improvement_examples,
        top_scanned_tables=top_scanned_tables,
        endpoint_id=endpoint_id,
        stage_info=stage_info,
        data_flow=data_flow,
        data_flow_dag=data_flow_dag,
        streaming_context=streaming_context,
        target_table_info=target_table_info,
    )

    # Build evidence bundle from analysis and raw data
    analysis.evidence_bundle = build_evidence(analysis, data)

    return analysis


# Re-export all public names for backward compatibility
__all__ = [
    "analyze_from_dict",
    "analyze_profile",
    "calculate_bottleneck_indicators",
    "enhance_bottleneck_with_explain",
    "extract_hot_operators",
    "generate_action_cards",
    "generate_sql_improvement_examples",
    # Internal but used in tests
    "_add_alert",
    "_analyze_extra_metrics",
    "_classify_photon_blocker",
    "_detect_simple_photon_blockers",
    "_extract_function_name",
    "_extract_photon_blockers_from_explain",
    "_generate_extra_metrics_warnings",
    "_parse_photon_explanation_structured",
    "_update_top_scanned_with_clustering",
    "CONFLICTING_RECOMMENDATIONS",
    "PHOTON_BLOCKER_RULES",
]

# Re-export _add_alert and CONFLICTING_RECOMMENDATIONS from _helpers
from ._helpers import CONFLICTING_RECOMMENDATIONS, _add_alert  # noqa: E402, F401
