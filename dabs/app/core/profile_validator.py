"""Profile JSON validation before analysis.

Checks structure, required fields, and verbose mode availability
to give the user early feedback before spending time on LLM analysis.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Metric labels that indicate verbose mode
_VERBOSE_INDICATORS = {
    "Number of local scan tasks",
    "Number of non-local (rescheduled) scan tasks",
    "Cache hits size",
    "Cache misses size",
    "Cloud storage request count",
    "Cloud storage retry count",
    "Peak memory usage",
    "Data filters - batches skipped",
}


@dataclass
class ValidationResult:
    """Result of profile validation."""

    valid: bool = True
    is_verbose: bool = False
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def validate_profile(data: dict[str, Any]) -> ValidationResult:
    """Validate a query profile JSON structure.

    Checks:
        1. Required top-level keys (query, graphs)
        2. Query has id and status
        3. Graphs contain nodes
        4. Verbose mode detection (checks for detailed per-node metrics)

    Args:
        data: Parsed query profile JSON.

    Returns:
        ValidationResult with errors, warnings, and verbose flag.
    """
    result = ValidationResult()

    if not data:
        result.valid = False
        result.errors.append("Empty profile data")
        return result

    # Check required keys
    has_query = "query" in data
    has_graphs = "graphs" in data

    if not has_query and not ("id" in data or "metrics" in data):
        result.valid = False
        result.errors.append("Missing 'query' field in profile JSON")

    # Two legitimate cases for missing ``graphs``:
    # 1. Result-cache hit (plansState=EMPTY, planMetadatas=[]) — the
    #    downstream pipeline handles them via the "skipped_cached" verdict.
    # 2. Streaming (DLT/SDP) profile — micro-batch info lives in
    #    ``planMetadatas`` and the streaming code path doesn't require
    #    a global execution graph.
    is_cache_hit = False
    is_streaming = False
    q = data.get("query") or {}
    if isinstance(q, dict):
        m = q.get("metrics") or {}
        is_cache_hit = bool(
            (isinstance(m, dict) and m.get("resultFromCache"))
            or q.get("cacheQueryId")
        )
        qm = q.get("queryMetadata") or {}
        if isinstance(qm, dict):
            is_streaming = bool(qm.get("isStreaming"))

    if not has_graphs and not (is_cache_hit or is_streaming):
        result.valid = False
        result.errors.append("Missing 'graphs' field in profile JSON")

    if not result.valid:
        return result

    if not has_graphs and is_cache_hit:
        # Cache hit with no plan — analysis runs in degraded mode.
        result.warnings.append(
            "Profile is a result-cache hit (no execution plan). Analysis "
            "will be limited to query metadata; bottleneck / scan / shuffle "
            "details are unavailable."
        )
    if not has_graphs and is_streaming:
        # Streaming without a global graph — micro-batch metrics are
        # the analysis source.
        result.warnings.append(
            "Streaming profile without a global execution graph. "
            "Analysis will use micro-batch metrics from planMetadatas."
        )

    # Check graphs have nodes
    graphs = data.get("graphs", [])
    total_nodes = 0
    for g in graphs:
        if isinstance(g, dict):
            nodes = g.get("nodes", [])
            total_nodes += len(nodes)

    if total_nodes == 0:
        result.warnings.append("Profile contains no execution plan nodes (empty graphs)")

    # Check verbose mode
    verbose_found = False
    for g in graphs:
        if not isinstance(g, dict):
            continue
        for node in g.get("nodes", []):
            if not isinstance(node, dict):
                continue
            for metric in node.get("metrics", []):
                if isinstance(metric, dict):
                    label = metric.get("label", "")
                    if label in _VERBOSE_INDICATORS:
                        verbose_found = True
                        break
            if verbose_found:
                break
        if verbose_found:
            break

    result.is_verbose = verbose_found
    if not verbose_found and total_nodes > 0:
        result.warnings.append(
            "Profile is not in Verbose mode. Advanced metrics (peak memory, "
            "cloud storage retries, scan locality, data filter statistics) "
            "will be unavailable. Re-download with 'Verbose' option for best results."
        )

    return result
