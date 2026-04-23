"""Individual report section generators."""

from typing import Any

from ..constants import THRESHOLDS, Severity
from ..dbsql_cost import CostEstimate, estimate_query_cost, format_cost_usd
from ..explain_parser import ExplainExtended, NodeFamily, extract_scan_table_name
from ..i18n import gettext as _
from ..models import (
    Alert,
    BottleneckIndicators,
    NodeMetrics,
    QueryMetrics,
    ShuffleMetrics,
    SQLAnalysis,
    StreamingContext,
)
from ..utils import format_bytes
from ..warehouse_client import WarehouseInfo
from ._helpers import _get_complexity_label, _severity_to_icon, _severity_to_label


def generate_warehouse_section(
    warehouse_info: WarehouseInfo | None,
    endpoint_id: str = "",
    *,
    query_metrics: QueryMetrics | None = None,
    include_header: bool = True,
) -> str:
    """Generate Warehouse information section.

    Args:
        warehouse_info: Warehouse information from API (None if not available)
        endpoint_id: Endpoint ID from profile data
        query_metrics: Query metrics for cost estimation (optional)

    Returns:
        Markdown section string
    """
    section = f"""
---

## 🖥️ {_("Compute Information")}

"""
    if warehouse_info:
        # Warehouse情報が取得できた場合
        warehouse_type_display = warehouse_info.warehouse_type or "CLASSIC"
        if warehouse_info.is_serverless:
            warehouse_type_display = "Serverless"
        elif warehouse_info.is_pro:
            warehouse_type_display = "Pro"

        section += f"""| {_("Item")} | {_("Value")} |
|:-----|:------|
| **{_("Warehouse Name")}** | {warehouse_info.name} |
| **{_("Warehouse ID")}** | `{warehouse_info.warehouse_id}` |
| **{_("Type")}** | {warehouse_type_display} |
| **{_("Size")}** | {warehouse_info.size_description} |
| **{_("Cluster Size")}** | {warehouse_info.cluster_size} |
| **{_("Clusters")}** | {warehouse_info.min_num_clusters} - {warehouse_info.max_num_clusters} |
| **{_("DBSQL Version")}** | {warehouse_info.dbsql_version or _("N/A")} |
| **{_("Channel")}** | {warehouse_info.channel_name.replace("CHANNEL_NAME_", "") if warehouse_info.channel_name else _("N/A")} |
| **{_("Estimated DBU/hour")}** | {warehouse_info.estimated_dbu_per_hour} DBU |
"""
        # Cost estimation
        cost = estimate_query_cost(query_metrics, warehouse_info) if query_metrics else None
        if cost:
            section += _format_cost_rows(cost)

        section += "\n"
    elif endpoint_id:
        # endpoint_idはあるがWarehouse情報が取得できなかった場合
        section += f"""> ⚠️ **{_("Warehouse information could not be retrieved")}**
>
> {_("Endpoint ID")}: `{endpoint_id}`
>
> {_("To retrieve warehouse information, set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.")}

"""
        # Fallback cost estimation from query_typename
        cost = estimate_query_cost(query_metrics, None) if query_metrics else None
        if cost:
            section += _format_cost_table_standalone(cost)

    else:
        # endpoint_idもない場合
        section += f"""> ℹ️ **{_("Warehouse information not available")}**
>
> {_("This profile does not contain endpoint information.")}

"""
        # Fallback cost estimation from query_typename
        cost = estimate_query_cost(query_metrics, None) if query_metrics else None
        if cost:
            section += _format_cost_table_standalone(cost)

    return section


def _format_cost_rows(cost: CostEstimate) -> str:
    """Format cost estimation as Markdown table rows (appended to warehouse table)."""
    lines = []
    label = _("Estimated Query Cost") if cost.is_per_query else _("Estimated Query Cost Share")
    lines.append(f"| **{_('Billing Model')}** | {cost.billing_model} |")
    lines.append(f"| **{_('DBU Unit Price')}** | {format_cost_usd(cost.dbu_unit_price)}/DBU |")
    size_display = cost.cluster_size
    if cost.is_estimated_size:
        size_display += f" ({_('assumed')})"
    lines.append(
        f"| **{_('Cluster Size')}** | {size_display} |"
    ) if cost.is_estimated_size else None
    lines.append(f"| **{_('Estimated DBU')}** | {cost.estimated_dbu:.4f} DBU |")
    lines.append(f"| **{label}** | {format_cost_usd(cost.estimated_cost_usd)} |")
    if not cost.is_per_query:
        lines.append(
            f"\n> *{_('Note: Classic/Pro warehouses are billed per uptime, not per query. This is an estimated share.')}*"
        )
    if cost.is_estimated_size:
        lines.append(
            f"\n> *{_('Note: Cluster size is assumed. Connect warehouse API for accurate estimation.')}*"
        )
    return "\n".join(lines) + "\n"


def _format_cost_table_standalone(cost: CostEstimate) -> str:
    """Format cost estimation as a standalone Markdown table.

    Used when the SQL Warehouse API is unavailable. The primary cost
    value is calculated at the *inferred* cluster size (parallelism-based
    rule inference); this matches real billing closely for saturated
    queries and produces a minimum-required-size estimate for
    under-saturated workloads. See ``cost.note`` for confidence.
    """
    label = _("Estimated Query Cost") if cost.is_per_query else _("Estimated Query Cost Share")
    lines = [
        f"| {_('Item')} | {_('Value')} |",
        "|:-----|:------|",
        f"| **{_('Billing Model')}** | {cost.billing_model} |",
    ]
    # Show inferred cluster size when we have one (fallback path).
    if cost.cluster_size:
        lines.append(f"| **{_('Cluster Size')}** | {cost.cluster_size} |")
    if cost.dbu_per_hour:
        lines.append(f"| **{_('DBU/hour')}** | {cost.dbu_per_hour} |")
    lines.append(f"| **{_('DBU Unit Price')}** | {format_cost_usd(cost.dbu_unit_price)}/DBU |")
    lines.append(f"| **{_('Estimated DBU')}** | {cost.estimated_dbu:.4f} DBU |")
    lines.append(f"| **{label}** | {format_cost_usd(cost.estimated_cost_usd)} |")
    if cost.parallelism_ratio > 0:
        lines.append(f"| **{_('Parallelism Ratio')}** | {cost.parallelism_ratio:.1f}x |")
    lines.append("")

    if not cost.is_per_query:
        lines.append(
            f"> *{_('Note: Classic/Pro warehouses are billed per uptime, not per query. This is an estimated share.')}*"
        )
    # Consumption-mode note explains the model; already surfaced in cost.note.
    if cost.note:
        lines.append(f"> *{cost.note}*")
    lines.append("")

    # Reference cost table by T-shirt size
    if cost.reference_costs:
        lines.append(f"**{_('Reference Cost by Warehouse Size')}**\n")
        lines.append(f"| {_('Size')} | DBU/h | {_('Estimated Cost')} |")
        lines.append("|:------|------:|--------------:|")
        for ref in cost.reference_costs:
            lines.append(
                f"| {ref.cluster_size} | {ref.dbu_per_hour} | "
                f"{format_cost_usd(ref.estimated_cost_usd)} |"
            )
        lines.append("")

    return "\n".join(lines)


def generate_sql_section(sql_analysis: SQLAnalysis, *, include_header: bool = True) -> str:
    """Generate SQL analysis section of the report.

    Args:
        sql_analysis: SQLAnalysis object containing parsed SQL information

    Returns:
        Markdown formatted SQL section
    """
    if not sql_analysis.raw_sql:
        return ""

    lines = []

    # SQL section with formatted query (collapsible)
    if include_header:
        lines.append("---\n")
        lines.append("## SQL\n")
    lines.append("<details>")
    lines.append(f"<summary>{_('Click to expand SQL')}</summary>\n")
    lines.append("```sql")
    lines.append(sql_analysis.formatted_sql or sql_analysis.raw_sql)
    lines.append("```")
    lines.append("</details>\n")

    # Query structure section
    structure = sql_analysis.structure
    if structure.statement_type:
        if include_header:
            lines.append("---\n")
            lines.append(f"## 🏗️ {_('Query Structure')}\n")
        lines.append(f"- **{_('Statement Type')}:** {structure.statement_type}")

        if structure.join_count > 0:
            lines.append(f"- **{_('Join Count')}:** {structure.join_count}")
            if structure.join_types:
                lines.append(f"- **{_('Join Types')}:** {', '.join(structure.join_types)}")
            if structure.join_edges:
                lines.append(f"\n### {_('Join Details')}\n")
                lines.append(f"| {_('Left Table')} | {_('Join Type')} | {_('Right Table')} |")
                lines.append("|:---|:---:|:---|")
                for edge in structure.join_edges:
                    left = edge.left_alias or edge.left_table
                    right = edge.right_alias or edge.right_table
                    lines.append(f"| {left} | {edge.join_type} | {right} |")
                lines.append("")

        if structure.cte_count > 0:
            lines.append(f"- **{_('CTE Count')}:** {structure.cte_count}")
            if structure.cte_names:
                lines.append(f"- **{_('CTE Names')}:** {', '.join(structure.cte_names)}")

        if structure.subquery_count > 0:
            lines.append(f"- **{_('Subquery Count')}:** {structure.subquery_count}")

        if structure.aggregate_functions:
            lines.append(
                f"- **{_('Aggregate Functions')}:** {', '.join(structure.aggregate_functions)}"
            )

        if structure.window_functions:
            lines.append(f"- **{_('Window Functions')}:** {', '.join(structure.window_functions)}")

        # Boolean flags
        flags = []
        if structure.has_distinct:
            flags.append("DISTINCT")
        if structure.has_group_by:
            flags.append("GROUP BY")
        if structure.has_order_by:
            flags.append("ORDER BY")
        if structure.has_limit:
            flags.append("LIMIT")
        if structure.has_union:
            flags.append("UNION")

        if flags:
            lines.append(f"- **{_('Clauses Used')}:** {', '.join(flags)}")

        complexity_label = _get_complexity_label(structure.complexity_score)
        lines.append(
            f"- **{_('Complexity')}:** {complexity_label} ({_('Score')}: {structure.complexity_score})"
        )
        lines.append("")

    return "\n".join(lines)


def generate_explain_section(explain: ExplainExtended, *, include_header: bool = True) -> str:
    """Generate EXPLAIN analysis section of the report.

    Args:
        explain: ExplainExtended object containing parsed EXPLAIN output

    Returns:
        Markdown formatted EXPLAIN section
    """
    lines = []
    if include_header:
        lines.append("---\n")
        lines.append(f"## 📐 {_('Execution Plan Analysis (EXPLAIN)')}\n")

    # Physical Plan analysis
    physical = explain.get_section("Physical Plan")
    if physical and physical.nodes:
        lines.append(f"### {_('Physical Plan Summary')}\n")

        # Count by family
        family_counts: dict[NodeFamily, int] = {}
        for node in physical.nodes:
            family_counts[node.family] = family_counts.get(node.family, 0) + 1

        lines.append(f"| {_('Operator Type')} | {_('Count')} |")
        lines.append("|---------------|--:|")
        for family, count in sorted(family_counts.items(), key=lambda x: -x[1]):
            if family != NodeFamily.UNKNOWN:
                lines.append(f"| {family.value.title()} | {count} |")
        lines.append("")

        # Scan nodes (tables)
        scans = [n for n in physical.nodes if n.family == NodeFamily.SCAN]
        if scans:
            lines.append(f"### {_('Table Scans')}\n")
            lines.append(f"| {_('Table')} | {_('Format')} | DataFilters | PartitionFilters |")
            lines.append("|---------|------------|-------------|------------------|")
            for scan in scans:
                # Use the structured extractor (handles backticks, [cols], (partition=...))
                table = extract_scan_table_name(scan) or "unknown"
                fmt = scan.attrs.get("Format", "parquet")
                data_filters = scan.attrs.get("DataFilters", "")
                if len(data_filters) > 40:
                    data_filters = data_filters[:37] + "..."
                part_filters = scan.attrs.get("PartitionFilters", "")
                if not part_filters:
                    part_filters = _("(None)")
                elif len(part_filters) > 30:
                    part_filters = part_filters[:27] + "..."
                lines.append(f"| `{table}` | {fmt} | {data_filters} | {part_filters} |")
            lines.append("")

        # Join nodes
        joins = [n for n in physical.nodes if n.family == NodeFamily.JOIN]
        if joins:
            lines.append(f"### {_('Join Operators')}\n")
            for i, join in enumerate(joins, 1):
                # Extract join type from node name
                join_name = join.node_name
                if "BroadcastHashJoin" in join_name:
                    join_type = "Broadcast Hash Join"
                elif "ShuffledHashJoin" in join_name:
                    join_type = "Shuffled Hash Join"
                elif "SortMergeJoin" in join_name:
                    join_type = "Sort Merge Join"
                else:
                    join_type = join_name
                is_photon = "Photon" in join_name
                photon_mark = "OK" if is_photon else "X"
                lines.append(f"{i}. **{join_type}** (Photon: {photon_mark})")
            lines.append("")

        # Exchange/Shuffle nodes
        exchanges = [n for n in physical.nodes if n.family == NodeFamily.EXCHANGE]
        if exchanges:
            lines.append(f"### {_('Shuffle/Exchange')}\n")
            lines.append(f"- {_('Exchange operations')}: **{len(exchanges)}**")

            # Check for different partitioning types
            sink_nodes = [n for n in exchanges if "Sink" in n.node_name]
            for sink in sink_nodes:
                raw = sink.raw_line
                if "hashpartitioning" in raw:
                    lines.append(f"- {_('Hash Partitioning detected')}")
                    break
                elif "rangepartitioning" in raw:
                    lines.append(f"- {_('Range Partitioning detected')}")
                    break
                elif "SinglePartition" in raw:
                    lines.append(f"- {_('SinglePartition detected (for broadcast)')}")
                    break
            lines.append("")

        # Aggregation nodes
        aggs = [n for n in physical.nodes if n.family == NodeFamily.AGG]
        if aggs:
            lines.append(f"### {_('Aggregation Operators')}\n")
            lines.append(f"- {_('Aggregation stages')}: **{len(aggs)}**")
            # Check for multi-stage aggregation (partial/final)
            has_partial = any("partial" in n.raw_line.lower() for n in aggs)
            has_final = any("final" in n.raw_line.lower() for n in aggs)
            if has_partial and has_final:
                lines.append(f"- {_('Using multi-stage aggregation (Partial -> Final)')}")
            lines.append("")

    # Optimized Logical Plan analysis
    optimized = explain.get_section("Optimized Logical Plan")
    if optimized and optimized.nodes:
        lines.append(f"### {_('Optimized Logical Plan Summary')}\n")

        # Count tables (Relation nodes)
        relations = [n for n in optimized.nodes if "Relation" in n.node_name]
        if relations:
            lines.append(f"- {_('Referenced tables')}: **{len(relations)}**")

        # Count joins
        joins = [n for n in optimized.nodes if n.family == NodeFamily.JOIN]
        if joins:
            lines.append(f"- {_('Joins')}: **{len(joins)}**")

        # Check for filter pushdown
        filters = [n for n in optimized.nodes if n.family == NodeFamily.FILTER]
        if filters:
            lines.append(f"- {_('Filters')}: **{len(filters)}** ({_('pushdown applied')})")
        lines.append("")

    # Photon Explanation (using parsed structure)
    if explain.photon_explanation and not explain.photon_explanation.fully_supported:
        pe = explain.photon_explanation
        lines.append(f"### {_('Photon Support Status')}\n")
        lines.append(f"> {_('Query is not fully supported by Photon')}\n")

        if pe.unsupported_items:
            lines.append(f"| {_('Expression')} | {_('Category')} | {_('Detail')} |")
            lines.append("|------------|----------|--------|")
            for item in pe.unsupported_items:
                expr = item.expression or "-"
                category = item.category or "-"
                detail = item.detail or item.reason or "-"
                lines.append(f"| `{expr}` | {category} | {detail} |")
            lines.append("")

        if pe.reference_nodes:
            lines.append(f"**{_('Reference node')}:**\n")
            for ref_node in pe.reference_nodes:
                lines.append(f"> `{ref_node}`")
            lines.append("")

    # Optimizer Statistics (using parsed structure)
    if explain.optimizer_statistics:
        os = explain.optimizer_statistics
        has_stats = os.missing_tables or os.partial_tables or os.full_tables
        if has_stats:
            lines.append(f"### {_('Optimizer Statistics')}\n")
            if os.missing_tables:
                lines.append(f"- **{_('Missing statistics')}:** {', '.join(os.missing_tables)}")
            if os.partial_tables:
                lines.append(f"- **{_('Partial statistics')}:** {', '.join(os.partial_tables)}")
            if os.full_tables:
                lines.append(f"- **{_('Full statistics')}:** {', '.join(os.full_tables)}")

            if os.recommended_action:
                lines.append(
                    f"\n**{_('Recommended action')}:** {_('Run ANALYZE TABLE for tables with missing or partial statistics')}"
                )
                lines.append(f"```sql\n{os.recommended_action}\n```")
            lines.append("")

    return "\n".join(lines)


def generate_alerts_section(alerts: list[Alert], *, include_header: bool = True) -> str:
    """Generate structured alerts section grouped by severity.

    Displays alerts in order of severity (CRITICAL > HIGH > MEDIUM > INFO)
    with detailed information including current value vs threshold,
    category, and recommended actions.

    Args:
        alerts: List of Alert objects from BottleneckIndicators

    Returns:
        Markdown formatted alerts section
    """
    if not alerts:
        return ""

    lines = []
    if include_header:
        lines.append("---\n")
        lines.append(f"## 🔔 {_('Performance Alerts')}\n")

    # Group alerts by severity
    severity_order = [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.INFO]
    severity_groups: dict[Severity, list[Alert]] = {sev: [] for sev in severity_order}

    for alert in alerts:
        if alert.severity in severity_groups:
            severity_groups[alert.severity].append(alert)

    # Count summary
    counts = {sev: len(severity_groups[sev]) for sev in severity_order if severity_groups[sev]}
    if counts:
        count_parts = []
        for sev, count in counts.items():
            icon = _severity_to_icon(sev)
            label = _severity_to_label(sev)
            count_parts.append(f"{icon} {count} {label}")
        lines.append(f"{_('Summary')}: {' | '.join(count_parts)}\n")

    # Display by severity group (list-based format for consistency with other sections)
    for severity in severity_order:
        group_alerts = severity_groups[severity]
        if not group_alerts:
            continue

        icon = _severity_to_icon(severity)
        label = _severity_to_label(severity)
        lines.append(f"**{icon} {label}**\n")

        for alert in group_alerts:
            # Main message with category tag (list item)
            category_tag = f"[{alert.category.upper()}]" if alert.category else ""
            lines.append(f"- {category_tag} {alert.message}")

            # Details (nested list)
            details = []
            if alert.current_value:
                details.append(f"{_('Current')}: {alert.current_value}")
            if alert.threshold:
                details.append(f"{_('Target')}: {alert.threshold}")
            if details:
                lines.append(f"  - {' | '.join(details)}")

            # Recommendation
            if alert.recommendation:
                lines.append(f"  - {_('Action')}: {alert.recommendation}")

            # Conflict warning
            if alert.conflicts_with:
                conflicts_str = ", ".join(alert.conflicts_with)
                lines.append(
                    f"  - {_('Note')}: {_('This recommendation may conflict with')}: {conflicts_str}"
                )

        lines.append("")

    return "\n".join(lines)


def generate_bottleneck_summary(bi: BottleneckIndicators, qm: QueryMetrics | None = None) -> str:
    """Generate bottleneck indicator summary as a table.

    Args:
        bi: BottleneckIndicators object
        qm: QueryMetrics object (optional, for showing calculation basis)
    """
    lines = []

    lines.append(
        f"| {_('Indicator')} | {_('Value')} | {_('Status')} | {_('Target')} | {_('Note')} |"
    )
    lines.append("|-----------|-------|--------|-----------|------|")

    # Cache Hit Ratio - with note about cold run possibility
    cache_icon = _severity_to_icon(bi.cache_severity.value)
    cache_label = _severity_to_label(bi.cache_severity.value)
    cache_note = _("Likely cold run - validate on re-execution") if bi.cache_hit_ratio < 0.5 else ""
    lines.append(
        f"| {_('Cache Hit Ratio')} | {bi.cache_hit_ratio:.1%} | {cache_icon} {cache_label} | >80% | {cache_note} |"
    )

    # Remote Read Ratio (new)
    remote_icon = _severity_to_icon(bi.remote_read_severity.value)
    remote_label = _severity_to_label(bi.remote_read_severity.value)
    remote_note = (
        _("Expected for first execution")
        if bi.remote_read_ratio > 0.8 and bi.cache_hit_ratio < 0.2
        else ""
    )
    lines.append(
        f"| {_('Remote Read Ratio')} | {bi.remote_read_ratio:.1%} | {remote_icon} {remote_label} | <80% | {remote_note} |"
    )

    # Photon Utilization - with calculation basis
    photon_icon = _severity_to_icon(bi.photon_severity.value)
    photon_label = _severity_to_label(bi.photon_severity.value)
    photon_note = "= photonTotalTimeMs / taskTotalTimeMs"
    lines.append(
        f"| {_('Photon Utilization')} | {bi.photon_ratio:.1%} | {photon_icon} {photon_label} | >80% | {photon_note} |"
    )

    # Disk Spill
    spill_gb = bi.spill_bytes / (1024**3)
    spill_icon = _severity_to_icon(bi.spill_severity.value)
    spill_label = _severity_to_label(bi.spill_severity.value)
    lines.append(f"| {_('Disk Spill')} | {spill_gb:.2f} GB | {spill_icon} {spill_label} | 0 | |")

    # Filter Efficiency (file-based)
    filter_icon = _severity_to_icon(bi.filter_severity.value)
    filter_label = _severity_to_label(bi.filter_severity.value)
    lines.append(
        f"| {_('Filter Efficiency')} | {bi.filter_rate:.1%} | {filter_icon} {filter_label} | >20% | |"
    )

    # Bytes Pruning (new)
    bytes_icon = _severity_to_icon(bi.bytes_pruning_severity.value)
    bytes_label = _severity_to_label(bi.bytes_pruning_severity.value)
    bytes_note = "= pruned / (read + pruned)"
    lines.append(
        f"| {_('Bytes Pruning')} | {bi.bytes_pruning_ratio:.1%} | {bytes_icon} {bytes_label} | >50% | {bytes_note} |"
    )

    # Shuffle Impact
    shuffle_icon = _severity_to_icon(bi.shuffle_severity.value)
    shuffle_label = _severity_to_label(bi.shuffle_severity.value)
    lines.append(
        f"| {_('Shuffle Impact Ratio')} | {bi.shuffle_impact_ratio:.1%} | {shuffle_icon} {shuffle_label} | <20% | |"
    )

    # Rescheduled Scan Ratio (Scan Locality - Verbose mode only)
    total_scan_tasks = bi.local_scan_tasks_total + bi.non_local_scan_tasks_total
    if total_scan_tasks > 0:
        resched_icon = _severity_to_icon(bi.rescheduled_scan_severity.value)
        resched_label = _severity_to_label(bi.rescheduled_scan_severity.value)
        resched_note = "= non_local / (local + non_local)"
        lines.append(
            f"| {_('Rescheduled Scan Ratio')} | {bi.rescheduled_scan_ratio:.1%} | {resched_icon} {resched_label} | <5% | {resched_note} |"
        )

    return "\n".join(lines)


def generate_io_metrics_section(
    qm: QueryMetrics,
    bi: BottleneckIndicators,
    top_scanned_tables: list | None = None,
    *,
    include_header: bool = True,
    explain_analysis=None,
) -> str:
    """Generate enhanced I/O Metrics section with cache/remote breakdown.

    Args:
        qm: QueryMetrics object
        bi: BottleneckIndicators object
        top_scanned_tables: Optional list of TableScanMetrics for top scanned tables
        explain_analysis: Optional ExplainExtended used to enrich the Top
            Scanned Tables table with column types (from ReadSchema). When
            None, the legacy 4-column table is rendered.

    Returns:
        Markdown formatted I/O Metrics section
    """
    lines = []
    if include_header:
        lines.append(f"## 💾 {_('I/O Metrics')}\n")

    lines.append(f"| {_('Metric')} | {_('Value')} | {_('Details')} |")
    lines.append("|--------|-------|---------|")

    # Total Read
    lines.append(f"| {_('Total Read')} | {format_bytes(qm.read_bytes)} | |")

    # From Cache
    cache_pct = bi.cache_hit_ratio * 100 if qm.read_bytes > 0 else 0
    lines.append(f"| {_('From Cache')} | {format_bytes(qm.read_cache_bytes)} | {cache_pct:.1f}% |")

    # From Remote
    remote_pct = bi.remote_read_ratio * 100 if qm.read_bytes > 0 else 0
    lines.append(
        f"| {_('From Remote')} | {format_bytes(qm.read_remote_bytes)} | {remote_pct:.1f}% |"
    )

    # Files Read/Pruned
    total_files = qm.read_files_count + qm.pruned_files_count
    file_prune_pct = (qm.pruned_files_count / total_files * 100) if total_files > 0 else 0
    lines.append(f"| {_('Files Read')} | {qm.read_files_count:,} | |")
    lines.append(
        f"| {_('Files Pruned')} | {qm.pruned_files_count:,} | {file_prune_pct:.1f}% {_('efficiency')} |"
    )

    # Bytes Pruned
    if qm.pruned_bytes > 0:
        bytes_prune_pct = bi.bytes_pruning_ratio * 100
        lines.append(
            f"| {_('Bytes Pruned')} | {format_bytes(qm.pruned_bytes)} | {bytes_prune_pct:.1f}% {_('efficiency')} |"
        )

    # Predictive I/O (data filter skipping)
    if bi.data_filters_rows_skipped > 0:
        lines.append(
            f"| {_('Rows Skipped (Predictive I/O)')} | {bi.data_filters_rows_skipped:,} | |"
        )
    if bi.data_filters_batches_skipped > 0:
        lines.append(
            f"| {_('Batches Skipped (Predictive I/O)')} | {bi.data_filters_batches_skipped:,} | |"
        )

    lines.append("")

    # Top Scanned Tables section
    if top_scanned_tables:
        lines.append(
            generate_top_scanned_tables_section(
                top_scanned_tables, explain_analysis=explain_analysis
            )
        )

    return "\n".join(lines)


def generate_top_scanned_tables_section(
    top_scanned_tables: list,
    *,
    explain_analysis=None,
) -> str:
    """Render the "Top Scanned Tables" markdown block as a standalone function.

    Exposed separately so that report pipelines that assemble their own
    Performance Metrics section (generate_performance_metrics in
    query_metrics.py) can append this block as well. Previously the block
    was embedded inside generate_io_metrics_section and therefore only
    appeared in the legacy report template.
    """
    if not top_scanned_tables:
        return ""
    lines: list[str] = []
    lines.append(f"### 📊 {_('Top Scanned Tables')}\n")

    # Column-type information comes from EXPLAIN's ReadSchema.
    scan_schemas: dict = {}
    if explain_analysis is not None:
        scan_schemas = dict(getattr(explain_analysis, "scan_schemas", None) or {})

    if scan_schemas:
        lines.append(
            f"| {_('Table')} | {_('Bytes Read')} | {_('Pruning Rate')} | "
            f"{_('Current Clustering')} | {_('Column Types')} |"
        )
        lines.append("|--------|-----------|-------------|-------------------|--------------|")
    else:
        lines.append(
            f"| {_('Table')} | {_('Bytes Read')} | {_('Pruning Rate')} | {_('Current Clustering')} |"
        )
        lines.append("|--------|-----------|-------------|-------------------|")

    for tsm in top_scanned_tables:
        table_display = tsm.table_name
        bytes_read_str = format_bytes(tsm.bytes_read)
        pruning_rate_str = f"{tsm.bytes_pruning_rate:.1%}"

        # Clearer wording: empty list means "none configured" — prevents
        # LLMs from guessing clustering state from table-name suffixes.
        current_keys = (
            ", ".join(tsm.current_clustering_keys)
            if tsm.current_clustering_keys
            else _("none configured")
        )

        if scan_schemas:
            types_map = scan_schemas.get(tsm.table_name, {})
            if types_map:
                # Prefer columns that are also clustering keys, then fill
                # with the first N remaining columns.
                ordered: list[tuple[str, str]] = []
                for k in tsm.current_clustering_keys or []:
                    if k in types_map:
                        ordered.append((k, types_map[k]))
                for c, t in types_map.items():
                    if c not in dict(ordered) and len(ordered) < 6:
                        ordered.append((c, t))
                types_str = ", ".join(f"`{c}`: {t}" for c, t in ordered[:6])
                extra = (
                    f" (+{len(types_map) - len(ordered)} more)"
                    if len(types_map) > len(ordered)
                    else ""
                )
                types_cell = types_str + extra
            else:
                types_cell = "-"
            lines.append(
                f"| `{table_display}` | {bytes_read_str} | {pruning_rate_str} | "
                f"{current_keys} | {types_cell} |"
            )
        else:
            lines.append(
                f"| `{table_display}` | {bytes_read_str} | {pruning_rate_str} | {current_keys} |"
            )

    lines.append("")
    return "\n".join(lines)


def generate_cloud_storage_section(bi: BottleneckIndicators, *, include_header: bool = True) -> str:
    """Generate Cloud Storage Performance section.

    Args:
        bi: BottleneckIndicators object with cloud_storage_metrics

    Returns:
        Markdown formatted Cloud Storage section, or empty string if no data
    """
    csm = bi.cloud_storage_metrics
    if csm.total_request_count == 0:
        return ""

    lines = []
    if include_header:
        lines.append("---\n")
        lines.append(f"## ☁️ {_('Cloud Storage Performance')}\n")

    lines.append(f"| {_('Metric')} | {_('Value')} | {_('Status')} |")
    lines.append("|--------|-------|--------|")

    # Total Requests
    lines.append(f"| {_('Total Requests')} | {csm.total_request_count:,} | |")

    # Total Retries
    retry_status = ""
    if csm.retry_ratio >= THRESHOLDS["cloud_storage_retry_critical"]:
        retry_status = f"● {_('Critical')}"
    elif csm.retry_ratio >= THRESHOLDS["cloud_storage_retry_warning"]:
        retry_status = f"▲ {_('Warning')}"
    else:
        retry_status = f"✓ {_('Good')}"
    lines.append(
        f"| {_('Total Retries')} | {csm.total_retry_count:,} | {csm.retry_ratio:.1%} {retry_status} |"
    )

    # Avg Request Duration
    lines.append(f"| {_('Avg Request Duration')} | {csm.avg_request_duration_ms:.0f} ms | |")

    # Retry Overhead
    if csm.total_retry_duration_ms > 0:
        lines.append(f"| {_('Retry Overhead')} | +{csm.total_retry_duration_ms:,} ms | |")

    lines.append("")

    # Warning message if retry rate is high
    if csm.retry_ratio >= THRESHOLDS["cloud_storage_retry_warning"]:
        lines.append(f"> {_('Warning: Retry rate indicates possible storage throttling')}\n")

    return "\n".join(lines)


def generate_spill_analysis_section(
    bi: BottleneckIndicators, *, include_header: bool = True
) -> str:
    """Generate Spill Analysis section showing top spill operators.

    Args:
        bi: BottleneckIndicators object with spill_operators

    Returns:
        Markdown formatted Spill Analysis section, or empty string if no spill
    """
    if bi.spill_bytes == 0 or not bi.spill_operators:
        return ""

    lines = []
    if include_header:
        lines.append("---\n")
        lines.append(f"## 📤 {_('Spill Analysis')}\n")

    # Summary line
    spill_gb = bi.spill_bytes / (1024**3)
    max_spill_op = bi.spill_operators[0] if bi.spill_operators else None
    max_op_name = max_spill_op.node_name[:30] if max_spill_op else "-"
    lines.append(
        f"**{_('Total Spill')}:** {spill_gb:.2f} GB | **{_('Max Spill Operator')}:** {max_op_name}\n"
    )

    # Table of top spill operators
    lines.append(
        f"| {_('Rank')} | {_('Operator')} | {_('Spill')} | {_('Peak Memory')} | {_('Share')} |"
    )
    lines.append("|:----:|----------|------:|------------:|------:|")

    for rank, op in enumerate(bi.spill_operators, 1):
        name = op.node_name[:35] + "..." if len(op.node_name) > 35 else op.node_name
        spill_str = format_bytes(op.spill_bytes)
        memory_str = format_bytes(op.peak_memory_bytes)
        share_str = f"{op.spill_share_percent:.1f}%"
        lines.append(f"| {rank} | `{name}` | {spill_str} | {memory_str} | {share_str} |")

    lines.append("")
    return "\n".join(lines)


def generate_scan_locality_section(
    node_metrics: list[NodeMetrics], *, include_header: bool = True
) -> str:
    """Generate per-node Scan Locality breakdown section.

    Shows local/non-local task counts and cache hit ratio per Scan node,
    helping identify cold-node placement vs file layout issues.

    Args:
        node_metrics: List of NodeMetrics (filters to Scan nodes with locality data)

    Returns:
        Markdown section, or empty string if no scan locality data.
    """
    scan_nodes = [
        n
        for n in node_metrics
        if (n.local_scan_tasks > 0 or n.non_local_scan_tasks > 0) and "Scan" in n.node_name
    ]
    if not scan_nodes:
        return ""

    lines = []
    if include_header:
        if include_header:
            lines.append("---\n")
            lines.append(f"## {_('Scan Locality (Per-Node)')}\n")
    lines.append(
        f"| {_('Node')} | {_('Local')} | {_('Non-Local')} | {_('Rescheduled %')} | {_('Cache Hit %')} | {_('Pattern')} |"
    )
    lines.append("|--------|-------|-----------|---------------|-------------|---------|")

    for n in scan_nodes:
        total = n.local_scan_tasks + n.non_local_scan_tasks
        resched_pct = (n.non_local_scan_tasks / total * 100) if total > 0 else 0
        cache_total = n.cache_hits_size + n.cache_misses_size
        cache_pct = (n.cache_hits_size / cache_total * 100) if cache_total > 0 else 0

        # Detect pattern
        if resched_pct > 30 and cache_pct < 20:
            pattern = f"**{_('Cold Node')}**"
        elif resched_pct > 30:
            pattern = _("CPU Contention")
        elif resched_pct > 5:
            pattern = _("File Layout")
        else:
            pattern = _("OK")

        name = n.node_name.replace("Scan ", "")
        lines.append(
            f"| {name} | {n.local_scan_tasks} | {n.non_local_scan_tasks} "
            f"| {resched_pct:.1f}% | {cache_pct:.1f}% | {pattern} |"
        )

    lines.append("")

    # Summary note
    cold_nodes = [
        n
        for n in scan_nodes
        if (
            n.non_local_scan_tasks / (n.local_scan_tasks + n.non_local_scan_tasks) > 0.3
            if (n.local_scan_tasks + n.non_local_scan_tasks) > 0
            else False
        )
        and (
            n.cache_hits_size / (n.cache_hits_size + n.cache_misses_size) < 0.2
            if (n.cache_hits_size + n.cache_misses_size) > 0
            else False
        )
    ]
    if cold_nodes:
        lines.append(
            f"> **{_('Cold node pattern detected')}**: "
            + _(
                "High non-local + low cache hit suggests tasks were scheduled "
                "on nodes without cached data (scale-out or CPU contention)."
            )
        )
        lines.append("")

    return "\n".join(lines)


def generate_aqe_shuffle_section(
    shuffle_metrics: list[ShuffleMetrics],
    *,
    include_header: bool = True,
    is_serverless: bool = False,
) -> str:
    """Generate AQE Shuffle Health section.

    Args:
        shuffle_metrics: List of ShuffleMetrics objects

    Returns:
        Markdown formatted AQE Shuffle Health section
    """
    # Filter to only shuffles with AQE data
    aqe_shuffles = [sm for sm in shuffle_metrics if sm.aqe_partitions > 0]
    if not aqe_shuffles:
        return ""

    lines = []
    if include_header:
        if include_header:
            lines.append("---\n")
            lines.append(f"## 🔄 {_('AQE Shuffle Health')}\n")

    lines.append(
        f"| {_('Node')} | {_('Partitions')} | {_('Data Size')} | {_('Avg Size')} | {_('Skewed')} | {_('Status')} |"
    )
    lines.append("|------|-----------|-----------|----------|--------|--------|")

    needs_repartition = False
    max_suggested_partitions = 0

    for sm in aqe_shuffles:
        avg_size_mb = sm.avg_aqe_partition_size_mb
        status = "OK"
        if avg_size_mb > THRESHOLDS["aqe_partition_size_warning_mb"]:
            status = f"▲ {_('Warning')}"
            needs_repartition = True
            # Calculate suggested partitions targeting 512MB
            suggested = int(
                sm.aqe_data_size / (THRESHOLDS["aqe_partition_size_warning_mb"] * 1024 * 1024)
            )
            max_suggested_partitions = max(max_suggested_partitions, suggested)
        else:
            status = f"✓ {_('OK')}"

        name = sm.node_name[:25] + "..." if len(sm.node_name) > 25 else sm.node_name
        data_size_str = format_bytes(sm.aqe_data_size)
        avg_size_str = f"{avg_size_mb:.0f} MB"
        skewed_str = str(sm.aqe_skewed_partitions) if sm.aqe_skewed_partitions > 0 else "-"

        lines.append(
            f"| {name} | {sm.aqe_partitions} | {data_size_str} | {avg_size_str} | {skewed_str} | {status} |"
        )

    lines.append("")

    # Recommendation if needed
    if needs_repartition and max_suggested_partitions > 0:
        if is_serverless:
            lines.append(
                f"> {_('Recommendation')}: {_('Use REPARTITION hint or pre-aggregate CTEs to reduce partition sizes (shuffle.partitions cannot be SET on Serverless).')}\n"
            )
        else:
            lines.append(
                f"> {_('Recommendation')}: `SET spark.sql.shuffle.partitions = {max_suggested_partitions};`\n"
            )

    return "\n".join(lines)


def generate_photon_blockers_section(
    bi: BottleneckIndicators, *, include_header: bool = True
) -> str:
    """Generate Photon Blockers section showing reasons for low Photon utilization.

    Args:
        bi: BottleneckIndicators object with photon_blockers

    Returns:
        Markdown formatted Photon Blockers section, or empty string if none
    """
    if not bi.photon_blockers or bi.photon_ratio >= 0.80:
        return ""

    lines = []
    if include_header:
        lines.append("---\n")
        lines.append(f"## ⚡ {_('Photon Blockers')}\n")

    lines.append(
        f"{_('Photon utilization is')} **{bi.photon_ratio:.1%}**. {_('The following operations are not running on Photon')}:\n"
    )

    lines.append(f"| {_('Reason')} | {_('Count')} | {_('Impact')} | {_('Action')} |")
    lines.append("|--------|-------|--------|--------|")

    for blocker in bi.photon_blockers:
        impact_str = blocker.impact
        action_str = f"`{blocker.action}`" if blocker.action else "-"
        lines.append(f"| {blocker.reason} | {blocker.count} | {impact_str} | {action_str} |")

    lines.append("")

    # Add detailed information for blockers with extended fields (from EXPLAIN)
    blockers_with_details = [
        b for b in bi.photon_blockers if b.unsupported_expression or b.detail_message
    ]

    if blockers_with_details:
        lines.append(f"### {_('Detailed Photon Blocker Information')}\n")
        lines.append(
            f"> {_('The following details are extracted from EXPLAIN EXTENDED output.')}\n"
        )

        for blocker in blockers_with_details:
            # Translate the reason (e.g., "aggregation function" -> "集約関数")
            reason_translated = _(blocker.reason) if blocker.reason else ""
            lines.append(f"#### {reason_translated}\n")

            if blocker.unsupported_expression:
                lines.append(f"**{_('Unsupported Expression')}:**")
                lines.append("```")
                lines.append(blocker.unsupported_expression)
                lines.append("```\n")

            if blocker.detail_message:
                # Translate common detail messages
                detail_translated = _(blocker.detail_message)
                lines.append(f"**{_('Detail')}:** {detail_translated}\n")

            if blocker.reference_node:
                lines.append(f"**{_('Reference Node')}:**")
                lines.append("```")
                # Truncate if too long
                ref_node = blocker.reference_node
                if len(ref_node) > 200:
                    ref_node = ref_node[:200] + "..."
                lines.append(ref_node)
                lines.append("```\n")

            if blocker.sql_rewrite_example:
                lines.append(f"**{_('Recommended Rewrite')}:**")
                lines.append("```sql")
                lines.append(blocker.sql_rewrite_example)
                lines.append("```\n")

            lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Streaming Section
# ---------------------------------------------------------------------------


def generate_streaming_section(
    ctx: StreamingContext,
    batch_stats: dict[str, Any],
) -> str:
    """Generate Streaming Query Analysis section.

    Shows streaming metadata, batch statistics, and slow batch warnings.
    """
    lines: list[str] = []

    lines.append(f"## {_('Streaming Query Snapshot')}\n")
    lines.append(
        f"> **{_('Note')}**: {_('This is a running streaming query snapshot. Metrics are cumulative.')}\n"
    )

    # Metadata table
    lines.append(f"| {_('Property')} | {_('Value')} |")
    lines.append("|----------|-------|")
    if ctx.target_table:
        lines.append(f"| {_('Target Table')} | `{ctx.target_table}` |")
    if ctx.entry_point:
        lines.append(f"| {_('Entry Point')} | {ctx.entry_point} |")
    if ctx.statement_type:
        lines.append(f"| {_('Statement Type')} | {ctx.statement_type} |")
    lines.append(f"| {_('Micro-Batches')} | {batch_stats['batch_count']} |")
    if batch_stats["running_count"] > 0:
        lines.append(f"| {_('Running')} | {batch_stats['running_count']} |")
    lines.append("")

    # Batch duration statistics
    if batch_stats["finished_count"] > 0:
        lines.append(f"### {_('Batch Duration Statistics')}\n")
        lines.append(f"| {_('Metric')} | {_('Value')} |")
        lines.append("|--------|-------|")
        lines.append(f"| Min | {batch_stats['duration_min_ms']:,} ms |")
        lines.append(f"| Avg | {batch_stats['duration_avg_ms']:,.0f} ms |")
        lines.append(f"| P95 | {batch_stats['duration_p95_ms']:,} ms |")
        lines.append(f"| Max | {batch_stats['duration_max_ms']:,} ms |")
        if batch_stats["duration_cv"] > 0:
            lines.append(f"| CV | {batch_stats['duration_cv']:.2f} |")
        lines.append("")

        # Read bytes summary
        lines.append(f"| {_('Metric')} | {_('Value')} |")
        lines.append("|--------|-------|")
        lines.append(f"| {_('Read Bytes (min)')} | {format_bytes(batch_stats['read_bytes_min'])} |")
        lines.append(f"| {_('Read Bytes (max)')} | {format_bytes(batch_stats['read_bytes_max'])} |")
        lines.append(f"| {_('Rows Read (avg)')} | {batch_stats['rows_avg']:,.0f} |")
        lines.append("")

    # Slow batch warnings
    slow = batch_stats.get("slow_batches", [])
    if slow:
        lines.append(f"### {_('Slow Batches Detected')}\n")
        lines.append(
            f"{len(slow)} {_('batch(es) exceeded 2x average duration')} "
            f"({batch_stats['duration_avg_ms']:,.0f} ms):\n"
        )
        for sb in slow:
            lines.append(f"- `{sb['plan_id']}`: {sb['duration_ms']:,} ms")
        lines.append("")

    return "\n".join(lines)
