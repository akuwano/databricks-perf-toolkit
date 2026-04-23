"""
Evidence layer for LLM analysis.

This module provides functionality to extract evidence snippets from raw profile JSON
that can be used by LLM to provide more accurate and grounded recommendations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import ProfileAnalysis


# =============================================================================
# Data Models
# =============================================================================


@dataclass
class EvidenceLocator:
    """JSONPath-like locator for evidence source."""

    path: str = ""  # e.g., "graphs[0].nodes[12]"
    path_kind: str = "jsonpath-ish"
    anchors: dict[str, Any] = field(default_factory=dict)  # e.g., {"nodeId": "12"}


@dataclass
class EvidenceSnippet:
    """Extracted snippet from raw profile JSON."""

    format: str = "json"  # json, yaml, text
    content: str = ""  # The actual snippet content
    redactions: list[str] = field(default_factory=list)  # Fields that were redacted


@dataclass
class EvidenceItem:
    """A single piece of evidence with locator and snippet."""

    id: str = ""  # e.g., "ev_spill_node_12"
    category: str = ""  # spill, hot_node, join, shuffle, scan, photon_blocker
    title: str = ""  # Human-readable title
    locator: EvidenceLocator = field(default_factory=EvidenceLocator)
    snippet: EvidenceSnippet = field(default_factory=EvidenceSnippet)
    why_selected: str = ""  # Reason for selection
    score: float = 0.0  # Ranking score
    links: dict[str, str] = field(default_factory=dict)  # Related metadata


@dataclass
class EvidenceBudget:
    """Budget constraints for evidence generation."""

    max_items: int = 20
    max_chars_total: int = 15000
    max_chars_per_item: int = 1000
    max_depth: int = 4  # Max JSON nesting depth


@dataclass
class EvidenceSource:
    """Metadata about the evidence source."""

    profile_schema: str = "dbsql_profiler"
    query_id: str = ""


@dataclass
class EvidenceBundle:
    """Complete bundle of evidence items."""

    source: EvidenceSource = field(default_factory=EvidenceSource)
    items: list[EvidenceItem] = field(default_factory=list)
    budgets: EvidenceBudget = field(default_factory=EvidenceBudget)
    index: dict[str, list[int]] = field(default_factory=dict)  # category -> item indices

    def get_by_category(self, category: str) -> list[EvidenceItem]:
        """Get evidence items by category."""
        indices = self.index.get(category, [])
        return [self.items[i] for i in indices if i < len(self.items)]

    def total_chars(self) -> int:
        """Calculate total characters in all snippets."""
        return sum(len(item.snippet.content) for item in self.items)


# =============================================================================
# Helper Functions
# =============================================================================


def _find_node_in_raw(raw_data: dict[str, Any], node_id: str) -> tuple[dict[str, Any] | None, str]:
    """Find a node in raw profile data by node_id."""
    graphs = raw_data.get("graphs", [])
    for g_idx, graph in enumerate(graphs):
        if isinstance(graph, str):
            try:
                graph = json.loads(graph)
            except json.JSONDecodeError:
                continue
        if not isinstance(graph, dict):
            continue

        nodes = graph.get("nodes", [])
        for n_idx, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            if node.get("id") == node_id:
                return node, f"graphs[{g_idx}].nodes[{n_idx}]"

    return None, ""


def _find_node_in_raw_by_name(
    raw_data: dict[str, Any], node_name: str
) -> tuple[dict[str, Any] | None, str]:
    """Find a node in raw profile data by node_name."""
    graphs = raw_data.get("graphs", [])
    for g_idx, graph in enumerate(graphs):
        if isinstance(graph, str):
            try:
                graph = json.loads(graph)
            except json.JSONDecodeError:
                continue
        if not isinstance(graph, dict):
            continue

        nodes = graph.get("nodes", [])
        for n_idx, node in enumerate(nodes):
            if not isinstance(node, dict):
                continue
            if node.get("name") == node_name:
                return node, f"graphs[{g_idx}].nodes[{n_idx}]"

    return None, ""


def _truncate_snippet(content: str, max_chars: int) -> str:
    """Truncate snippet content to max characters."""
    if len(content) <= max_chars:
        return content
    return content[: max_chars - 20] + "\n... (truncated)"


def _extract_node_snippet(node: dict[str, Any], max_depth: int = 3, max_chars: int = 1000) -> str:
    """Extract a compact snippet from a node."""
    relevant_keys = ["id", "name", "tag", "keyMetrics", "metadata", "metrics"]

    snippet_dict: dict[str, Any] = {}
    for key in relevant_keys:
        if key in node:
            value = node[key]
            if key in ("metrics", "metadata") and isinstance(value, list):
                snippet_dict[key] = value[:10]
                if len(value) > 10:
                    snippet_dict[f"_{key}_total"] = len(value)
            else:
                snippet_dict[key] = value

    try:
        content = json.dumps(snippet_dict, indent=2, ensure_ascii=False)
        return _truncate_snippet(content, max_chars)
    except (TypeError, ValueError):
        return "{}"


# =============================================================================
# Evidence Builders
# =============================================================================


def _build_hot_node_evidence(
    analysis: ProfileAnalysis,
    raw_data: dict[str, Any],
    budget: EvidenceBudget,
) -> list[EvidenceItem]:
    """Build evidence for hot (high duration/memory) nodes."""
    items = []
    max_items = 5

    sorted_nodes = sorted(analysis.node_metrics, key=lambda n: n.duration_ms, reverse=True)

    for rank, nm in enumerate(sorted_nodes[:max_items]):
        if nm.duration_ms <= 0:
            continue

        node, locator_path = _find_node_in_raw(raw_data, nm.node_id)
        if not node:
            continue

        snippet_content = _extract_node_snippet(node, budget.max_depth, budget.max_chars_per_item)

        total_duration = sum(n.duration_ms for n in analysis.node_metrics)
        share = nm.duration_ms / total_duration if total_duration > 0 else 0

        item = EvidenceItem(
            id=f"ev_hot_node_{nm.node_id}",
            category="hot_node",
            title=f"Hot Operator: {nm.node_name}",
            locator=EvidenceLocator(
                path=locator_path,
                anchors={"nodeId": nm.node_id, "nodeName": nm.node_name},
            ),
            snippet=EvidenceSnippet(content=snippet_content),
            why_selected=f"Rank #{rank + 1} by duration ({nm.duration_ms}ms, {share:.1%} of total)",
            score=share,
            links={"operator": nm.node_name, "tag": nm.node_tag},
        )
        items.append(item)

    return items


def _build_spill_evidence(
    analysis: ProfileAnalysis,
    raw_data: dict[str, Any],
    budget: EvidenceBudget,
) -> list[EvidenceItem]:
    """Build evidence for nodes with disk spill."""
    items = []
    max_items = 5

    spill_nodes = [nm for nm in analysis.node_metrics if nm.spill_bytes and nm.spill_bytes > 0]
    sorted_nodes = sorted(spill_nodes, key=lambda n: n.spill_bytes or 0, reverse=True)

    total_spill = analysis.query_metrics.spill_to_disk_bytes or 0

    for _rank, nm in enumerate(sorted_nodes[:max_items]):
        node, locator_path = _find_node_in_raw(raw_data, nm.node_id)
        if not node:
            continue

        snippet_content = _extract_node_snippet(node, budget.max_depth, budget.max_chars_per_item)

        share = nm.spill_bytes / total_spill if total_spill > 0 else 0
        spill_gb = (nm.spill_bytes or 0) / (1024**3)

        item = EvidenceItem(
            id=f"ev_spill_node_{nm.node_id}",
            category="spill",
            title=f"Spill Operator: {nm.node_name}",
            locator=EvidenceLocator(path=locator_path, anchors={"nodeId": nm.node_id}),
            snippet=EvidenceSnippet(content=snippet_content),
            why_selected=f"Spill: {spill_gb:.2f}GB ({share:.1%} of total spill)",
            score=share,
            links={"operator": nm.node_name, "spill_bytes": str(nm.spill_bytes)},
        )
        items.append(item)

    return items


def _build_shuffle_evidence(
    analysis: ProfileAnalysis,
    raw_data: dict[str, Any],
    budget: EvidenceBudget,
) -> list[EvidenceItem]:
    """Build evidence for shuffle operations with issues."""
    items = []
    max_items = 5

    sorted_shuffles = sorted(
        analysis.shuffle_metrics, key=lambda s: s.memory_per_partition_mb, reverse=True
    )

    for _rank, sm in enumerate(sorted_shuffles[:max_items]):
        if sm.memory_per_partition_mb < 100:
            continue

        node, locator_path = _find_node_in_raw(raw_data, sm.node_id)
        if not node:
            continue

        snippet_content = _extract_node_snippet(node, budget.max_depth, budget.max_chars_per_item)

        score = min(sm.memory_per_partition_mb / 128, 1.0)

        why = f"Memory/partition: {sm.memory_per_partition_mb:.0f}MB"
        if sm.aqe_skewed_partitions and sm.aqe_skewed_partitions > 0:
            why += f", Skewed partitions: {sm.aqe_skewed_partitions}"

        item = EvidenceItem(
            id=f"ev_shuffle_{sm.node_id}",
            category="shuffle",
            title=f"Shuffle: {sm.node_name}",
            locator=EvidenceLocator(path=locator_path, anchors={"nodeId": sm.node_id}),
            snippet=EvidenceSnippet(content=snippet_content),
            why_selected=why,
            score=score,
            links={
                "partition_count": str(sm.partition_count),
                "peak_memory_bytes": str(sm.peak_memory_bytes),
            },
        )
        items.append(item)

    return items


def _build_join_evidence(
    analysis: ProfileAnalysis,
    raw_data: dict[str, Any],
    budget: EvidenceBudget,
) -> list[EvidenceItem]:
    """Build evidence for join operations."""
    items = []
    max_items = 5

    sorted_joins = sorted(analysis.join_info, key=lambda j: j.duration_ms, reverse=True)

    for _rank, ji in enumerate(sorted_joins[:max_items]):
        if ji.duration_ms <= 0:
            continue

        node, locator_path = _find_node_in_raw_by_name(raw_data, ji.node_name)
        if not node:
            continue

        node_id = node.get("id", ji.node_name)

        snippet_content = _extract_node_snippet(node, budget.max_depth, budget.max_chars_per_item)

        type_scores = {
            "BROADCAST": 0.2,
            "SHUFFLE_HASH": 0.6,
            "SORT_MERGE": 0.8,
            "SHUFFLE_NESTED_LOOP": 0.9,
            "UNKNOWN": 0.5,
        }
        type_score = type_scores.get(ji.join_type.name, 0.5)

        item = EvidenceItem(
            id=f"ev_join_{node_id}",
            category="join",
            title=f"Join: {ji.join_type.name}",
            locator=EvidenceLocator(path=locator_path, anchors={"nodeId": node_id}),
            snippet=EvidenceSnippet(content=snippet_content),
            why_selected=f"Type: {ji.join_type.name}, Duration: {ji.duration_ms}ms",
            score=type_score,
            links={"join_type": ji.join_type.name, "is_photon": str(ji.is_photon)},
        )
        items.append(item)

    return items


def _build_scan_evidence(
    analysis: ProfileAnalysis,
    raw_data: dict[str, Any],
    budget: EvidenceBudget,
) -> list[EvidenceItem]:
    """Build evidence for scan operations (I/O bottlenecks)."""
    items = []
    max_items = 3

    scan_nodes = [nm for nm in analysis.node_metrics if "Scan" in nm.node_name]
    sorted_scans = sorted(scan_nodes, key=lambda n: n.files_read_size or 0, reverse=True)

    for _rank, nm in enumerate(sorted_scans[:max_items]):
        if not nm.files_read_size or nm.files_read_size <= 0:
            continue

        node, locator_path = _find_node_in_raw(raw_data, nm.node_id)
        if not node:
            continue

        snippet_content = _extract_node_snippet(node, budget.max_depth, budget.max_chars_per_item)

        bytes_gb = nm.files_read_size / (1024**3)

        cache_info = ""
        if nm.cache_hits_size and nm.cache_misses_size:
            total_cache = nm.cache_hits_size + nm.cache_misses_size
            cache_hit_ratio = nm.cache_hits_size / total_cache if total_cache > 0 else 0
            cache_info = f", Cache hit: {cache_hit_ratio:.1%}"

        item = EvidenceItem(
            id=f"ev_scan_{nm.node_id}",
            category="scan",
            title=f"Scan: {nm.node_name}",
            locator=EvidenceLocator(path=locator_path, anchors={"nodeId": nm.node_id}),
            snippet=EvidenceSnippet(content=snippet_content),
            why_selected=f"Read: {bytes_gb:.2f}GB{cache_info}",
            score=bytes_gb / 10,
            links={"files_read": str(nm.files_read), "rows_scanned": str(nm.rows_scanned)},
        )
        items.append(item)

    return items


# =============================================================================
# Main Functions
# =============================================================================


def build_evidence(
    analysis: ProfileAnalysis,
    raw_data: dict[str, Any],
    budget: EvidenceBudget | None = None,
) -> EvidenceBundle:
    """Build an evidence bundle from analysis and raw profile data."""
    if budget is None:
        budget = EvidenceBudget()

    bundle = EvidenceBundle(
        source=EvidenceSource(
            profile_schema="dbsql_profiler",
            query_id=analysis.query_metrics.query_id,
        ),
        budgets=budget,
    )

    all_items: list[EvidenceItem] = []

    # Tier-1: Hot nodes, Spill, Shuffle, Join
    all_items.extend(_build_hot_node_evidence(analysis, raw_data, budget))
    all_items.extend(_build_spill_evidence(analysis, raw_data, budget))
    all_items.extend(_build_shuffle_evidence(analysis, raw_data, budget))
    all_items.extend(_build_join_evidence(analysis, raw_data, budget))

    # Tier-2: Scan
    all_items.extend(_build_scan_evidence(analysis, raw_data, budget))

    # Sort by score descending
    all_items.sort(key=lambda x: x.score, reverse=True)

    # Apply budget constraints
    total_chars = 0
    for item in all_items:
        if len(bundle.items) >= budget.max_items:
            break
        if total_chars + len(item.snippet.content) > budget.max_chars_total:
            break

        bundle.items.append(item)
        total_chars += len(item.snippet.content)

        if item.category not in bundle.index:
            bundle.index[item.category] = []
        bundle.index[item.category].append(len(bundle.items) - 1)

    return bundle


def format_evidence_for_prompt(bundle: EvidenceBundle, lang: str = "en") -> str:
    """Format evidence bundle for inclusion in LLM prompt."""
    if not bundle.items:
        return ""

    if lang == "ja":
        header = "## Evidence (プロファイルJSONからの抜粋; 参照用)"
        note = "以下はボトルネック分析の根拠となる元データの抜粋です。"
    else:
        header = "## Evidence (raw profile excerpts; reference-only)"
        note = "Below are excerpts from the raw profile data supporting the analysis."

    lines = [header, "", note, ""]

    for item in bundle.items:
        lines.append(f"### [{item.id}] {item.category} / score={item.score:.2f}")
        lines.append(f"**{item.title}**")
        lines.append(f"- locator: `{item.locator.path}`")
        lines.append(f"- why: {item.why_selected}")

        if item.links:
            links_str = ", ".join(f"{k}={v}" for k, v in item.links.items())
            lines.append(f"- links: {links_str}")

        lines.append("- snippet:")
        lines.append("```json")
        lines.append(item.snippet.content)
        lines.append("```")
        lines.append("")

    return "\n".join(lines)
