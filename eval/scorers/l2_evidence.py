"""L2: Evidence grounding scorer for ActionCard recommendations."""

from __future__ import annotations

import re
from typing import Any

from core.models import ActionCard, ProfileAnalysis

from ..models import L2Score


def score_l2(
    card: ActionCard,
    profile_data: dict[str, Any],
    analysis: ProfileAnalysis,
) -> L2Score:
    """Score how well an ActionCard's evidence is grounded in actual profile data.

    Checks each evidence string against:
    1. Metric names from QueryMetrics fields and node metric labels
    2. Numeric values (with unit tolerance)
    3. Operator/node names from the execution graph
    """
    if not card.evidence:
        return L2Score(card_index=0, evidence_count=0, grounded_count=0, grounding_ratio=1.0)

    vocab = _build_vocabulary(profile_data, analysis)
    grounded = 0
    ungrounded: list[str] = []

    for ev_text in card.evidence:
        if _is_grounded(ev_text, vocab):
            grounded += 1
        else:
            ungrounded.append(ev_text)

    count = len(card.evidence)
    return L2Score(
        card_index=0,
        evidence_count=count,
        grounded_count=grounded,
        ungrounded_evidence=ungrounded,
        grounding_ratio=grounded / count if count > 0 else 1.0,
    )


def _build_vocabulary(
    profile_data: dict[str, Any],
    analysis: ProfileAnalysis,
) -> dict[str, set[str]]:
    """Build a vocabulary of metric names, values, and node names from the profile."""
    vocab: dict[str, set[str]] = {
        "metric_names": set(),
        "numeric_values": set(),
        "node_names": set(),
    }

    # 1. QueryMetrics field names and values
    qm = analysis.query_metrics
    for field_name in (
        "total_time_ms", "compilation_time_ms", "execution_time_ms",
        "read_bytes", "read_remote_bytes", "read_cache_bytes",
        "spill_to_disk_bytes", "photon_total_time_ms", "task_total_time_ms",
        "read_files_count", "pruned_files_count", "pruned_bytes",
        "rows_read_count", "rows_produced_count",
        "bytes_read_from_cache_percentage",
        "write_remote_bytes", "write_remote_files", "write_remote_rows",
        "network_sent_bytes",
        "read_partitions_count",
        "queued_provisioning_time_ms", "queued_overload_time_ms",
        "result_fetch_time_ms",
    ):
        vocab["metric_names"].add(field_name)
        # Also add camelCase variant (e.g., spillToDiskBytes)
        camel = _snake_to_camel(field_name)
        vocab["metric_names"].add(camel)
        val = getattr(qm, field_name, None)
        if val is not None and val != 0:
            vocab["numeric_values"].add(str(val))

    # 2. BottleneckIndicators
    bi = analysis.bottleneck_indicators
    for field_name in (
        "cache_hit_ratio", "remote_read_ratio", "photon_ratio",
        "spill_bytes", "filter_rate", "bytes_pruning_ratio",
        "shuffle_impact_ratio", "cloud_storage_retry_ratio",
        "shuffle_bytes_written_total", "shuffle_remote_bytes_read_total",
        "shuffle_local_bytes_read_total",
    ):
        vocab["metric_names"].add(field_name)
        val = getattr(bi, field_name, None)
        if val is not None and val != 0:
            vocab["numeric_values"].add(str(val))
            # Add percentage form
            if isinstance(val, float) and val <= 1.0:
                vocab["numeric_values"].add(f"{val:.1%}")
                vocab["numeric_values"].add(f"{val * 100:.1f}")

    # 3. Node names and metric labels from graph
    for graph in profile_data.get("graphs", []):
        # Handle graph objects that may be JSON strings
        if isinstance(graph, str):
            try:
                import json as _json
                graph = _json.loads(graph)
            except Exception:
                continue
        if not isinstance(graph, dict):
            continue
        nodes = graph.get("nodes", [])
        if isinstance(nodes, str):
            try:
                import json as _json
                nodes = _json.loads(nodes)
            except Exception:
                nodes = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            name = node.get("name", "")
            if name:
                vocab["node_names"].add(name)
                # Also add short form (e.g., "SortMergeJoin" from "SortMergeJoin [...]")
                short = name.split("[")[0].split("(")[0].strip()
                if short:
                    vocab["node_names"].add(short)
            # Metric labels
            for m in node.get("metrics", []):
                if not isinstance(m, dict):
                    continue
                label = m.get("label", "")
                if label:
                    vocab["metric_names"].add(label)
                value = m.get("value", "")
                if value:
                    vocab["numeric_values"].add(str(value))

    return vocab


def _is_grounded(evidence_text: str, vocab: dict[str, set[str]]) -> bool:
    """Check if an evidence string references data from the profile vocabulary."""
    ev_lower = evidence_text.lower()

    # Check metric names (case-insensitive substring match)
    for name in vocab["metric_names"]:
        if name.lower() in ev_lower:
            return True

    # Check node/operator names
    for name in vocab["node_names"]:
        if name.lower() in ev_lower:
            return True

    # Check numeric values (extract numbers from evidence and match)
    ev_numbers = set(re.findall(r"[\d,]+\.?\d*", evidence_text))
    profile_numbers = {v.replace(",", "") for v in vocab["numeric_values"]}
    for ev_num in ev_numbers:
        clean = ev_num.replace(",", "")
        if clean in profile_numbers:
            return True

    return False


def _snake_to_camel(name: str) -> str:
    """Convert snake_case to camelCase."""
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])
