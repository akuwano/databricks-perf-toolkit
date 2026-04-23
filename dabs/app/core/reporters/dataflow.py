"""Data flow visualization (Mermaid and ASCII)."""

from ..i18n import gettext as _
from ..models import DataFlowDAG, DataFlowEntry
from ..utils import format_bytes, format_rows_human, format_time_ms


def _escape_mermaid_label(text: str) -> str:
    """Escape special characters for Mermaid node labels."""
    # Mermaid uses " for label delimiters, and [ ] { } | are special
    text = text.replace('"', "&quot;")
    text = text.replace("[", "&#91;")
    text = text.replace("]", "&#93;")
    text = text.replace("|", "&#124;")
    text = text.replace("{", "&#123;")
    text = text.replace("}", "&#125;")
    return text


def _score_node_severity(
    entry: DataFlowEntry,
    max_duration: int,
    max_memory: int,
    parent_rows: dict[str, int],
) -> float:
    """Score a node's severity (higher = worse). Used for Top-N ranking.

    Combines duration, data explosion, and memory into a single score.
    """
    score = 0.0

    # Duration contribution (0-50 points): normalized to max
    if max_duration > 0 and entry.duration_ms > 0:
        score += 50.0 * (entry.duration_ms / max_duration)

    # Data explosion contribution (0-30 points): for joins only
    if "join" in entry.operation.lower() and entry.node_id in parent_rows:
        max_input = parent_rows[entry.node_id]
        if max_input > 0 and entry.output_rows > 0:
            explosion = entry.output_rows / max_input
            # log scale: 1x=0, 10x=15, 100x=30
            import math

            if explosion > 1:
                score += min(30.0, 15.0 * math.log10(explosion))

    # Memory contribution (0-20 points): normalized to max
    if max_memory > 0 and entry.peak_memory_bytes > 0:
        score += 20.0 * (entry.peak_memory_bytes / max_memory)

    return score


def _rank_node_severities(
    entries: list[DataFlowEntry],
    parents_map: dict[str, list[str]],
    entry_map: dict[str, DataFlowEntry],
    max_critical: int = 3,
    max_warning: int = 5,
) -> dict[str, str]:
    """Rank nodes and return severity map (Top-N approach).

    Only the top few worst nodes get highlighted to avoid "crying wolf".

    Args:
        entries: All data flow entries
        parents_map: child_id -> [parent_ids]
        entry_map: node_id -> DataFlowEntry
        max_critical: Maximum nodes to mark as critical
        max_warning: Maximum nodes to mark as warning (in addition to critical)

    Returns:
        Dict of node_id -> "critical" or "warning" (only for highlighted nodes)
    """
    max_duration = max((e.duration_ms for e in entries), default=0)
    max_memory = max((e.peak_memory_bytes for e in entries), default=0)

    parent_rows: dict[str, int] = {}
    for node_id, parent_ids in parents_map.items():
        if parent_ids:
            parent_rows[node_id] = max(
                entry_map[pid].output_rows for pid in parent_ids if pid in entry_map
            )

    # Score all nodes
    scored = [
        (entry, _score_node_severity(entry, max_duration, max_memory, parent_rows))
        for entry in entries
    ]
    # Sort by score descending, filter out zero scores
    scored = [(e, s) for e, s in scored if s > 0]
    scored.sort(key=lambda x: x[1], reverse=True)

    # Only highlight nodes with score above a minimum threshold (10 points)
    min_threshold = 10.0
    result: dict[str, str] = {}
    critical_count = 0
    warning_count = 0

    for entry, score in scored:
        if score < min_threshold:
            break
        if critical_count < max_critical:
            result[entry.node_id] = "critical"
            critical_count += 1
        elif warning_count < max_warning:
            result[entry.node_id] = "warning"
            warning_count += 1
        else:
            break

    return result


def generate_mermaid_flowchart(dag: DataFlowDAG) -> str:
    """Generate Mermaid.js flowchart (graph TD) from data flow DAG.

    Data flows top-down: Scan nodes at top, Joins in middle, final output at bottom.
    Problem nodes are highlighted: red for critical, orange for warning.

    Args:
        dag: DataFlowDAG with entries and edges

    Returns:
        Mermaid flowchart string, or empty string if DAG is empty.
    """
    if not dag.entries or not dag.edges:
        return ""

    entry_map = {e.node_id: e for e in dag.entries}

    # Compute severity rankings (Top-N approach)
    severity_map = _rank_node_severities(dag.entries, dag.parents_map, entry_map)

    lines = ["graph TD"]

    # Style classes
    lines.append("    classDef critical fill:#dc2626,stroke:#991b1b,color:#fff")
    lines.append("    classDef warning fill:#ea580c,stroke:#c2410c,color:#fff")

    # Node definitions
    critical_nodes: list[str] = []
    warning_nodes: list[str] = []

    for entry in dag.entries:
        label_parts = [entry.operation]
        label_parts.append(f"{format_rows_human(entry.output_rows)} rows")
        if entry.duration_ms > 0:
            label_parts.append(format_time_ms(entry.duration_ms))
        if entry.join_keys:
            label_parts.append(entry.join_keys)
        label = _escape_mermaid_label("<br/>".join(label_parts))
        lines.append(f'    n{entry.node_id}["{label}"]')

        severity = severity_map.get(entry.node_id)
        if severity == "critical":
            critical_nodes.append(f"n{entry.node_id}")
        elif severity == "warning":
            warning_nodes.append(f"n{entry.node_id}")

    # Edge definitions
    for edge in dag.edges:
        lines.append(f"    n{edge.from_node_id} --> n{edge.to_node_id}")

    # Apply styles
    if critical_nodes:
        lines.append(f"    class {','.join(critical_nodes)} critical")
    if warning_nodes:
        lines.append(f"    class {','.join(warning_nodes)} warning")

    return "\n".join(lines)


def generate_ascii_tree(dag: DataFlowDAG) -> str:
    """Generate ASCII tree representation of data flow DAG.

    Root at top (final output operator), leaves at bottom (table scans).
    The tree shows what feeds into each operator (parents become children in display).

    Args:
        dag: DataFlowDAG with entries and edges

    Returns:
        ASCII tree string, or empty string if DAG is empty.
    """
    if not dag.entries or not dag.edges:
        return ""

    entry_map = {e.node_id: e for e in dag.entries}
    visited: set[str] = set()

    # Compute severity rankings (Top-N approach)
    severity_map = _rank_node_severities(dag.entries, dag.parents_map, entry_map)

    def _format_node(entry: DataFlowEntry) -> str:
        severity = severity_map.get(entry.node_id)
        marker = ""
        if severity == "critical":
            marker = " *** "
        elif severity == "warning":
            marker = " *  "
        parts = [entry.operation, f"({format_rows_human(entry.output_rows)} rows)"]
        if entry.duration_ms > 0:
            parts.append(format_time_ms(entry.duration_ms))
        if entry.join_keys:
            parts.append(f"[{entry.join_keys}]")
        return " ".join(parts) + marker

    def _build_tree(
        node_id: str, prefix: str, is_last: bool, is_root: bool, lines: list[str]
    ) -> None:
        entry = entry_map.get(node_id)
        if not entry:
            return

        connector = "" if is_root else ("\u2514\u2500\u2500 " if is_last else "\u251c\u2500\u2500 ")

        if node_id in visited:
            lines.append(f"{prefix}{connector}(-> {_format_node(entry)})")
            return
        visited.add(node_id)

        lines.append(f"{prefix}{connector}{_format_node(entry)}")

        # Parents of this node = what feeds into it (shown as children in tree)
        parent_ids = sorted(dag.parents_map.get(node_id, []))
        for i, parent_id in enumerate(parent_ids):
            is_last_child = i == len(parent_ids) - 1
            if is_root:
                child_prefix = ""
            else:
                child_prefix = prefix + ("    " if is_last else "\u2502   ")
            _build_tree(parent_id, child_prefix, is_last_child, False, lines)

    lines: list[str] = []
    for sink_id in dag.sink_node_ids:
        _build_tree(sink_id, "", True, True, lines)

    return "\n".join(lines)


def format_aqe_aos_events(shuffle_metrics: list | None) -> str:
    """Render AQE/AOS runtime-optimization events per shuffle node.

    Each shuffle is inspected for four kinds of runtime intervention:
      1. AQE skew-join split (aqe_skewed_partitions > 0)
      2. AQE auto-repartition (aqe_self_repartition_count > 0)
      3. AQE stage cancellation / re-plan (aqe_cancellation_count /
         aqe_triggered_on_materialized_count > 0)
      4. AOS coordinated repartition (aos_coordinated_repartition_count > 0)

    Returns an empty string when no events detected, so callers can
    conditionally include the section.
    """
    if not shuffle_metrics:
        return ""

    rows: list[tuple[str, str, str]] = []  # (node, kind, detail)
    for sm in shuffle_metrics:
        node_id = getattr(sm, "node_id", "") or "?"

        # 1. AQE skew-join split
        skewed = getattr(sm, "aqe_skewed_partitions", 0) or 0
        if skewed > 0:
            rows.append(
                (
                    f"#{node_id}",
                    _("⚖️ AQE skew-join split"),
                    _("{n} skewed partition(s) were split at runtime").format(n=skewed),
                )
            )

        # 2. AQE auto-repartition (Adp self-triggered)
        self_rep = getattr(sm, "aqe_self_repartition_count", 0) or 0
        if self_rep > 0:
            orig = getattr(sm, "aqe_original_num_partitions", 0) or 0
            intended = getattr(sm, "aqe_intended_num_partitions", 0) or 0
            detail = (
                _(
                    "partitions {orig} → {intended} (×{ratio:.0f}) — AQE auto-repartitioned due to data volume"
                ).format(orig=orig, intended=intended, ratio=(intended / orig if orig else 0.0))
                if orig > 0 and intended > 0
                else _("AQE self-triggered repartition (count={n})").format(n=self_rep)
            )
            rows.append((f"#{node_id}", _("🔀 AQE auto-repartition"), detail))

        # 3. AQE cancellation / re-plan after materialization
        cancel = getattr(sm, "aqe_cancellation_count", 0) or 0
        on_mat = getattr(sm, "aqe_triggered_on_materialized_count", 0) or 0
        if cancel > 0 or on_mat > 0:
            parts = []
            if cancel > 0:
                parts.append(_("cancelled {n} stage(s) and re-planned").format(n=cancel))
            if on_mat > 0:
                parts.append(_("re-optimized after {n} materialization(s)").format(n=on_mat))
            rows.append(
                (
                    f"#{node_id}",
                    _("🔁 AQE re-plan"),
                    "; ".join(parts),
                )
            )

        # 4. AOS coordinated repartition
        aos = getattr(sm, "aos_coordinated_repartition_count", 0) or 0
        if aos > 0:
            old = getattr(sm, "aos_old_num_partitions", 0) or 0
            new = getattr(sm, "aos_new_num_partitions", 0) or 0
            detail = (
                _("partitions {old} → {new} — AOS coordinated repartition").format(old=old, new=new)
                if old > 0 and new > 0
                else _("AOS coordinated repartition (count={n})").format(n=aos)
            )
            rows.append((f"#{node_id}", _("🎯 AOS repartition"), detail))

    if not rows:
        return ""

    lines = [f"### {_('AQE / AOS Runtime Optimization Events')}\n"]
    lines.append(
        _(
            "These events show where Adaptive Query Execution (AQE) or Auto-Optimized "
            "Shuffle (AOS) changed the physical plan at runtime. Presence of these "
            "events means the initial plan was sub-optimal and the engine self-corrected; "
            "the sustainable fix is usually to improve the source layout (Liquid Clustering) "
            "or pre-aggregate to avoid the runtime correction in the first place."
        )
        + "\n"
    )
    lines.append(f"| {_('Node')} | {_('Event')} | {_('Detail')} |")
    lines.append("|:-----|:-----|:-----|")
    for node, kind, detail in rows:
        lines.append(f"| {node} | {kind} | {detail} |")
    lines.append("")
    return "\n".join(lines)


def generate_data_flow_section(
    data_flow: list[DataFlowEntry],
    data_flow_dag: DataFlowDAG | None = None,
    *,
    include_header: bool = True,
    shuffle_metrics: list | None = None,  # noqa: ARG001 — kept for API stability
) -> str:
    """Generate Data Flow Summary section (Section 6).

    Renders DAG visualizations (Mermaid + ASCII tree) when available,
    followed by the detailed metrics table.

    AQE/AOS runtime-optimization events are NOT rendered here; they live
    under section 7 "AQE Shuffle Health" instead (see
    core/reporters/__init__.py).

    Args:
        data_flow: List of DataFlowEntry objects ordered source->sink
        data_flow_dag: Optional DAG structure for visualization
        shuffle_metrics: Accepted for backward compatibility; currently unused.
            AQE/AOS events render under section 7 via format_aqe_aos_events().

    Returns:
        Markdown formatted Data Flow section
    """
    if not data_flow:
        return ""

    lines = []
    if include_header:
        lines.append(f"## {_('Data Flow Summary')}\n")

    # DAG visualization (if available)
    if data_flow_dag and data_flow_dag.edges:
        # Mermaid flowchart
        mermaid = generate_mermaid_flowchart(data_flow_dag)
        if mermaid:
            lines.append(f"### {_('Data Flow Diagram')}\n")
            lines.append("```mermaid")
            lines.append(mermaid)
            lines.append("```\n")

        # ASCII tree (text fallback)
        ascii_tree = generate_ascii_tree(data_flow_dag)
        if ascii_tree:
            lines.append("<details>")
            lines.append(f"<summary>{_('Data Flow Tree')} ({_('text')})</summary>\n")
            lines.append("```")
            lines.append(ascii_tree)
            lines.append("```")
            lines.append("</details>\n")

    # Existing flat table with detailed metrics
    lines.append(f"### {_('Data Flow Details')}\n")
    lines.append(
        f"| {_('Operation')} | {_('Output Rows')} | {_('Duration')} | "
        f"{_('Peak Memory')} | {_('Join Keys')} |"
    )
    lines.append("|:------|------:|------:|------:|:------|")

    for entry in data_flow:
        rows_str = f"{entry.output_rows:,}"
        duration_str = format_time_ms(entry.duration_ms) if entry.duration_ms > 0 else "-"
        memory_str = format_bytes(entry.peak_memory_bytes) if entry.peak_memory_bytes > 0 else "-"
        keys_str = entry.join_keys or "-"

        lines.append(
            f"| {entry.operation} | {rows_str} | {duration_str} | {memory_str} | {keys_str} |"
        )

    lines.append("")

    # NOTE: AQE / AOS runtime-optimization events intentionally no longer
    # render here. They were moved under section 7 "AQE Shuffle Health"
    # (see core/reporters/__init__.py) where they semantically belong —
    # alongside the shuffle-health diagnosis rather than the operator-level
    # data flow appendix.

    return "\n".join(lines)
