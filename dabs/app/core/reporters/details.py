"""Detailed analysis report sections."""

from ..i18n import gettext as _
from ..models import ActionCard, BottleneckIndicators, OperatorHotspot, ShuffleMetrics, SQLAnalysis
from ..utils import format_bytes
from ._helpers import _extract_operator_short_name


def generate_hot_operators_section(
    hot_operators: list[OperatorHotspot], *, include_header: bool = True
) -> str:
    """Generate Hot Operators section showing time-consuming operators.

    Args:
        hot_operators: List of OperatorHotspot sorted by duration

    Returns:
        Markdown formatted Hot Operators section
    """
    if not hot_operators:
        return ""

    lines = []
    if include_header:
        if include_header:
            lines.append("---\n")
            lines.append(f"## 🔥 {_('Hot Operators (Top Time-Consuming)')}\n")

    # Summary of top bottleneck types
    bottleneck_counts: dict[str, int] = {}
    for op in hot_operators:
        if op.bottleneck_type:
            bottleneck_counts[op.bottleneck_type] = bottleneck_counts.get(op.bottleneck_type, 0) + 1

    if bottleneck_counts:
        top_type = max(bottleneck_counts.items(), key=lambda x: x[1])
        type_labels = {
            "scan": _("scan"),
            "join": _("join"),
            "shuffle": _("shuffle"),
            "sort": _("sort"),
            "agg": _("agg"),
            "spill": _("spill"),
        }
        lines.append(
            f"**{_('Main Bottleneck Type')}:** {type_labels.get(top_type[0], top_type[0])}\n"
        )

    # Note about time interpretation (per tuning guide section 9.2)
    lines.append(
        f"> **{_('Note')}:** {_('The Task Time column shows durationMs from operator metrics, which represents cumulative task time across all parallel executors, not wall-clock time. This is why individual operator times can exceed the total query time. The Share is calculated against taskTotalTimeMs per tuning guide section 9.2.')}\n"
    )

    # Table header - Changed "Time" to "Task Time" to avoid wall-clock confusion
    lines.append(
        f"| {_('Rank')} | {_('Operator')} | {_('Task Time')} | {_('Share')} | {_('Rows (in/out)')} | {_('Spill')} | {_('Photon')} | {_('Type')} |"
    )
    lines.append("|:----:|-----------|-----:|-------:|-------------:|-------:|:------:|:------:|")

    for op in hot_operators[:10]:
        # Extract meaningful short name for display
        name = _extract_operator_short_name(op.node_name, op.bottleneck_type)

        # Format duration
        if op.duration_ms >= 1000:
            duration_str = f"{op.duration_ms / 1000:.1f}s"
        else:
            duration_str = f"{op.duration_ms}ms"

        # Format rows
        if op.rows_in > 0 or op.rows_out > 0:
            rows_str = f"{op.rows_in:,}/{op.rows_out:,}"
        else:
            rows_str = "-"

        # Format spill
        if op.spill_bytes > 0:
            spill_str = format_bytes(op.spill_bytes)
        else:
            spill_str = "-"

        # Photon indicator
        photon_str = "✓" if op.is_photon else "✗"

        # Bottleneck type with color coding
        type_str = op.bottleneck_type.upper() if op.bottleneck_type else "-"

        # Highlight critical rows
        share_str = (
            f"**{op.time_share_percent:.1f}%**"
            if op.is_critical
            else f"{op.time_share_percent:.1f}%"
        )

        lines.append(
            f"| {op.rank} | `{name}` | {duration_str} | {share_str} | {rows_str} | {spill_str} | {photon_str} | {type_str} |"
        )

    lines.append("")

    # Full operator names in collapsible section
    lines.append(f"<details><summary>{_('Full Operator Names')}</summary>\n")
    for op in hot_operators[:10]:
        lines.append(f"- **{op.rank}.** `{op.node_name}`")
    lines.append("\n</details>\n")

    # Critical operators callout
    critical_ops = [op for op in hot_operators if op.is_critical]
    if critical_ops:
        lines.append(f"> **{_('Warning: The following operators are critical bottlenecks')}:**\n>")
        for op in critical_ops[:3]:
            reason = []
            if op.time_share_percent >= 20:
                reason.append(f"{_('time share')} {op.time_share_percent:.0f}%")
            if op.spill_bytes > 1024**3:
                reason.append(f"{_('spill')} {format_bytes(op.spill_bytes)}")
            lines.append(f"> - `{op.node_name[:40]}` ({', '.join(reason)})")
        lines.append("")

    return "\n".join(lines)


def generate_tuning_guide_section(
    bi: BottleneckIndicators,
    shuffle_metrics: list[ShuffleMetrics],
    *,
    include_header: bool = True,
) -> str:
    """Generate tuning guide section based on detected bottlenecks.

    This function outputs relevant tuning recommendations from the knowledge base
    based on the specific bottleneck indicators detected in the query.

    Args:
        bi: Bottleneck indicators
        shuffle_metrics: Shuffle metrics for memory efficiency analysis
        include_header: Whether to include section header

    Returns:
        Markdown formatted tuning guide section
    """
    lines = []
    if include_header:
        lines.append("---\n")
        lines.append(f"## ⚙️ {_('Tuning Guide')}\n")
    lines.append(f"> {_('The following recommendations are based on detected bottlenecks.')}\n")

    # I/O Efficiency Guide (when filter/pruning is low)
    if bi.filter_rate < 0.3 or bi.bytes_pruning_ratio < 0.5:
        lines.append(f"\n### {_('I/O Optimization')}\n")
        lines.append(
            f"**{_('Current Status')}:** {_('Filter efficiency')} {bi.filter_rate:.1%}, {_('Bytes pruning')} {bi.bytes_pruning_ratio:.1%}\n"
        )
        lines.append(f"\n**{_('Evaluation Criteria')}:**\n")
        lines.append(
            f"| {_('Metric')} | {_('Good')} | {_('Needs Improvement')} | {_('Critical')} |"
        )
        lines.append("|--------|------|------|------|")
        lines.append(f"| {_('Filter Efficiency')} | >50% | 20-50% | <20% |")
        lines.append(f"| {_('Bytes Pruning')} | >50% | 20-50% | <20% |\n")
        lines.append(f"\n**{_('Recommended Actions')}:**\n")
        lines.append(
            f"1. **{_('Partitioning')}:** {_('Partition by frequently filtered columns (date columns recommended)')}"
        )
        lines.append(f"   - {_('Target partition count')}: 1,000-2,000")
        lines.append(f"   - {_('Run OPTIMIZE per partition to avoid small files')}")
        lines.append(
            f"2. **Z-Order:** {_('Apply Z-Order for columns not suitable for partitioning')}"
        )
        lines.append(f"   - {_('Recommended')}: 4 {_('columns or less')}")
        lines.append(f"   - {_('Place Z-Order target columns in first 32 columns')}")
        lines.append(
            f"3. **Liquid Clustering:** {_('Consider as alternative to partitioning + Z-Order')}"
        )
        lines.append(f"   - {_('Effective for reducing Shuffle operations')}")
        lines.append(f"   - {_('Especially useful when memory usage exceeds 100GB')}\n")
        lines.append(f"\n**{_('Example SQL')}:**\n")
        lines.append("```sql")
        lines.append("-- Partitioning")
        lines.append("CREATE TABLE ... PARTITIONED BY (date_column);")
        lines.append("")
        lines.append("-- Z-Order")
        lines.append("OPTIMIZE <table_name> ZORDER BY (col1, col2, col3, col4);")
        lines.append("")
        lines.append("-- Liquid Clustering")
        lines.append("ALTER TABLE <table_name> CLUSTER BY (col1, col2);")
        lines.append("-- FULL is required to re-cluster existing data (not just new records)")
        lines.append("OPTIMIZE <table_name> FULL;")
        lines.append("```\n")

    # Cache Efficiency Guide
    if bi.cache_hit_ratio < 0.5:
        lines.append(f"\n### {_('Cache Efficiency')}\n")
        lines.append(
            f"**{_('Current Status')}:** {_('Cache hit ratio')} {bi.cache_hit_ratio:.1%}, {_('Remote read ratio')} {bi.remote_read_ratio:.1%}\n"
        )
        lines.append(f"\n**{_('Evaluation Criteria')}:**\n")
        lines.append(f"| {_('Cache Hit Ratio')} | {_('Evaluation')} | {_('Action')} |")
        lines.append("|--------|------|------|")
        lines.append(f"| >80% | {_('High')} | {_('Good state')} |")
        lines.append(f"| 50-80% | {_('Medium')} | {_('Consider scale up')} |")
        lines.append(f"| <50% | {_('Low')} | {_('Review cache strategy')} |\n")
        lines.append(f"\n**{_('Recommended Actions')}:**\n")
        lines.append(f"- {_('Re-run the same query to verify cache effect')}")
        lines.append(f"- {_('If cache hit remains low, consider scaling up the cluster')}")
        lines.append(
            f"- {_('Prioritize I/O optimization (partitioning/Liquid) to reduce data read first')}\n"
        )

    # Shuffle Optimization Guide
    inefficient_shuffles = [sm for sm in shuffle_metrics if not sm.is_memory_efficient]
    if bi.shuffle_impact_ratio > 0.2 or inefficient_shuffles:
        lines.append(f"\n### {_('Shuffle Optimization')}\n")
        lines.append(
            f"**{_('Current Status')}:** {_('Shuffle impact ratio')} {bi.shuffle_impact_ratio:.1%}\n"
        )
        if inefficient_shuffles:
            lines.append(
                f"**{_('Inefficient Shuffle Operations')}:** {len(inefficient_shuffles)} {_('detected')}\n"
            )
        lines.append(f"\n**{_('Memory Efficiency Criteria')}:**\n")
        lines.append(f"| {_('Memory/Partition')} | {_('Priority')} | {_('Recommended Action')} |")
        lines.append("|--------|------|------|")
        lines.append(
            f"| >2GB | {_('High')} | {_('Scale up cluster or significantly increase partition count')} |"
        )
        lines.append(
            f"| 1-2GB | {_('High')} | {_('Increase partition count, adjust AQE settings')} |"
        )
        lines.append(f"| 128MB-1GB | {_('Medium')} | {_('Recommend partition count adjustment')} |")
        lines.append(f"| ≤128MB | {_('Low')} | {_('Efficient state')} |\n")
        lines.append(f"\n**{_('REPARTITION Hints')}:**\n")
        lines.append("```sql")
        lines.append("-- Standard repartition")
        lines.append("SELECT /*+ REPARTITION(100, column1, column2) */ ...")
        lines.append("")
        lines.append("-- For Window functions")
        lines.append("SELECT /*+ REPARTITION_BY_RANGE(column1) */ ...")
        lines.append("```\n")

    # Spill Countermeasures Guide
    if bi.spill_bytes > 0:
        spill_gb = bi.spill_bytes / (1024**3)
        lines.append(f"\n### {_('Disk Spill Countermeasures')}\n")
        lines.append(f"**{_('Current Status')}:** {_('Spill')} {spill_gb:.2f} GB\n")
        lines.append(f"\n**{_('Severity Criteria')}:**\n")
        lines.append(f"| {_('Spill Amount')} | {_('Severity')} | {_('Action')} |")
        lines.append("|--------|------|------|")
        lines.append(
            f"| >5GB | {_('Critical')} | {_('Memory configuration and partition strategy review required')} |"
        )
        lines.append(f"| >1GB | {_('Important')} | {_('Optimization strongly recommended')} |")
        lines.append(f"| >0 | {_('Caution')} | {_('Monitor and consider improvement')} |")
        lines.append(f"| 0 | {_('Ideal')} | {_('Optimal state')} |\n")
        lines.append(f"\n**{_('Countermeasures by Priority')}:**\n")
        lines.append(f"1. **{_('Emergency (High Priority)')}:**")
        lines.append(f"   - {_('Scale up cluster (increase worker nodes)')}")
        lines.append(f"   - {_('Change to high-memory instance type')}")
        lines.append(f"2. **{_('Short-term')}:**")
        lines.append("   - `spark.sql.adaptive.coalescePartitions.enabled = true`")
        lines.append("   - `spark.sql.adaptive.skewJoin.enabled = true`")
        lines.append(f"3. **{_('Medium-term')}:**")
        lines.append(f"   - {_('Explicit partition specification (.repartition())')}")
        lines.append(f"   - {_('JOIN strategy optimization (use Broadcast JOIN)')}")
        lines.append(f"   - {_('Implement Liquid Clustering')}\n")

    # Photon Utilization Guide
    if bi.photon_ratio < 0.8:
        lines.append(f"\n### {_('Photon Utilization Improvement')}\n")
        lines.append(
            f"**{_('Current Status')}:** {_('Photon utilization')} {bi.photon_ratio:.1%}\n"
        )
        lines.append(f"\n**{_('Evaluation Criteria')}:**\n")
        lines.append(f"| {_('Utilization')} | {_('Evaluation')} | {_('Description')} |")
        lines.append("|--------|------|------|")
        lines.append(f"| >80% | {_('High')} | {_('Good state')} |")
        lines.append(f"| 50-80% | {_('Medium')} | {_('Room for improvement')} |")
        lines.append(f"| <50% | {_('Low')} | {_('Optimization required')} |\n")
        lines.append(f"\n**{_('Photon Support Status')}:**\n")
        lines.append(f"| {_('Join Type')} | {_('Photon Support')} |")
        lines.append("|--------|------|")
        lines.append(f"| Broadcast Join | ✓ {_('Supported')} |")
        lines.append(f"| Shuffle-Hash Join | ✓ {_('Supported')} |")
        lines.append(f"| Sort-Merge Join | ✗ {_('Not Supported')} |")
        lines.append(f"| Shuffle-Nested Loop Join | ✓ {_('Supported')} |\n")
        if bi.photon_blockers:
            lines.append(f"\n**{_('Detected Photon Blockers')}:**\n")
            for blocker in bi.photon_blockers[:5]:
                lines.append(f"- `{blocker.reason}` ({blocker.impact})")
            lines.append("")

    # Cluster Size Guide
    if bi.cache_hit_ratio < 0.5 or bi.spill_bytes > 0 or bi.shuffle_impact_ratio > 0.4:
        lines.append(f"\n### {_('Cluster Size Adjustment')}\n")
        lines.append(
            f"\n**{_('Scale Out')}:** {_('When concurrent query count is high and queue waiting occurs')}\n"
        )
        lines.append(f"- {_('DBSQL max concurrent queries per cluster')}: 10 ({_('fixed value')})")
        lines.append(f"- {_('Determine max cluster count based on queued query count')}\n")
        lines.append(
            f"\n**{_('Scale Up')}:** {_('When performance degradation is caused by high-load queries')}\n"
        )
        lines.append(f"- {_('CPU/Memory/Disk spill resource issues')}")
        lines.append(f"- {_('Low Delta cache hit ratio')}")
        lines.append(
            f"- {_('Scale up reduces cluster count, enabling effective Delta cache utilization')}\n"
        )

    # Cloud Storage Guide (if retry issues)
    if bi.cloud_storage_retry_ratio > 0.05:
        lines.append(f"\n### {_('Cloud Storage Bottleneck')}\n")
        lines.append(
            f"**{_('Current Status')}:** {_('Retry ratio')} {bi.cloud_storage_retry_ratio:.1%}\n"
        )
        lines.append(f"\n**{_('Judgment Criteria')}:**\n")
        lines.append(f"- {_('High retry count indicates cloud storage access is a bottleneck')}")
        lines.append(f"- {_('Ideal state is retry count = 0')}\n")
        lines.append(f"\n**{_('Metrics to Check')}:**\n")
        lines.append("- Cloud storage request count")
        lines.append("- Cloud storage request duration")
        lines.append("- Cloud storage retry count")
        lines.append("- Cloud storage retry duration\n")

    if len(lines) <= 3:
        # No specific bottlenecks detected
        lines.append(
            f"\n{_('No significant bottlenecks detected. Query performance appears optimal.')}\n"
        )

    return "\n".join(lines)


def generate_recommended_spark_params(
    bi: BottleneckIndicators,
    shuffle_metrics: list[ShuffleMetrics],
    is_serverless: bool = False,
    *,
    include_header: bool = True,
) -> str:
    """Generate recommended Spark parameters based on detected issues.

    Args:
        bi: Bottleneck indicators
        shuffle_metrics: Shuffle metrics
        is_serverless: True when running on Serverless SQL Warehouse
        include_header: Whether to include section header

    Returns:
        Markdown formatted Spark parameters section
    """
    lines = []
    if include_header:
        lines.append("---\n")
        lines.append(f"## 🛠️ {_('Recommended Spark Parameters')}\n")

    # Check for join-related issues
    has_join_issues = bi.photon_ratio < 0.8 or bi.shuffle_impact_ratio > 0.2

    # Check for shuffle/memory issues
    inefficient_shuffles = [sm for sm in shuffle_metrics if not sm.is_memory_efficient]
    has_shuffle_issues = bi.shuffle_impact_ratio > 0.2 or inefficient_shuffles or bi.has_data_skew

    # Check for spill issues
    has_spill_issues = bi.spill_bytes > 0

    if is_serverless:
        # --- Serverless: only supported configs + query rewrite alternatives ---
        lines.append(
            f"> **{_('Serverless SQL Warehouse')}**: "
            f"{_('Only a limited set of Spark configs can be SET. Use query rewrites for other optimizations.')}\n"
        )

        # Supported configs section
        if has_shuffle_issues and bi.shuffle_impact_ratio > 0.4:
            lines.append(f"### {_('Shuffle Optimization (Query Rewrite)')}\n")
            lines.append(
                f"> {_('shuffle.partitions cannot be SET on Serverless. Use query rewrites instead.')}\n"
            )
            lines.append("```sql")
            lines.append("-- Pre-aggregate to reduce shuffle data volume")
            lines.append("WITH agg AS (")
            lines.append("  SELECT key_col, SUM(val) AS total FROM large_table")
            lines.append("  GROUP BY key_col")
            lines.append(")")
            lines.append("SELECT ... FROM agg JOIN ...;")
            lines.append("```\n")

        # Query rewrite alternatives for join optimization
        if has_join_issues:
            lines.append(f"### {_('Join Optimization (Query Rewrite)')}\n")
            lines.append(f"> {_('Use query hints instead of Spark config changes')}\n")
            lines.append("```sql")
            lines.append(f"-- {_('Use BROADCAST hint for small tables')}")
            lines.append("SELECT /*+ BROADCAST(small_table) */ *")
            lines.append("FROM large_table JOIN small_table ON ...;")
            lines.append("")
            lines.append(f"-- {_('Use SHUFFLE_HASH to avoid Sort-Merge Join')}")
            lines.append("SELECT /*+ SHUFFLE_HASH(table) */ *")
            lines.append("FROM table1 JOIN table2 ON ...;")
            lines.append("```\n")

        # Query rewrite alternatives for shuffle/skew
        if has_shuffle_issues or has_spill_issues:
            lines.append(f"### {_('Shuffle/Skew Optimization (Query Rewrite)')}\n")
            lines.append(f"> {_('Use CTEs and query patterns instead of AQE config changes')}\n")
            lines.append("```sql")
            lines.append(f"-- {_('Pre-filter in CTE to reduce shuffle data volume')}")
            lines.append("WITH filtered AS (")
            lines.append("  SELECT * FROM source_table")
            lines.append("  WHERE <partition_filter>  -- Early filter")
            lines.append(")")
            lines.append("SELECT * FROM filtered JOIN other_table ON ...;")
            lines.append("")
            lines.append(f"-- {_('Pre-aggregate in CTE before JOIN')}")
            lines.append("WITH pre_agg AS (")
            lines.append("  SELECT key, SUM(val) AS total FROM large_table GROUP BY key")
            lines.append(")")
            lines.append("SELECT /*+ BROADCAST(pre_agg) */ *")
            lines.append("FROM pre_agg JOIN dimension_table ON ...;")
            if bi.has_data_skew:
                lines.append("")
                lines.append(f"-- {_('CTE pre-aggregation for data skew')}")
                lines.append("WITH pre_agg AS (")
                lines.append("  SELECT key, COUNT(*) AS cnt, SUM(val) AS total")
                lines.append("  FROM skewed_table GROUP BY key")
                lines.append(")")
                lines.append(
                    "SELECT * FROM pre_agg JOIN other_table ON pre_agg.key = other_table.key;"
                )
            lines.append("```\n")
    else:
        # --- Classic/Pro: full config recommendations (existing behavior) ---

        # Always show Join Optimization (most common need)
        lines.append(f"### {_('Join Optimization')}\n")
        if has_join_issues:
            lines.append(f"> {_('Recommended based on detected join/shuffle issues')}\n")
        lines.append("```sql")
        lines.append("SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB")
        lines.append("SET spark.sql.join.preferSortMergeJoin = false;")
        lines.append("SET spark.databricks.adaptive.joinFallback = true;")
        lines.append("```\n")

        # AQE Settings - show expanded when issues detected
        lines.append(f"### {_('AQE (Adaptive Query Execution) Settings')}\n")
        if has_shuffle_issues or has_spill_issues:
            lines.append(
                f"> {_('Extended AQE settings recommended based on detected shuffle/spill issues')}\n"
            )
            lines.append("```sql")
            lines.append("-- Basic AQE settings")
            lines.append("SET spark.sql.adaptive.enabled = true;")
            lines.append("")
            lines.append("-- Partition coalescing (reduces small partition overhead)")
            lines.append("SET spark.sql.adaptive.coalescePartitions.enabled = true;")
            lines.append("SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;")
            lines.append("SET spark.sql.adaptive.coalescePartitions.maxBatchSize = 100;")
            lines.append("")
            lines.append("-- Target partition size (512MB recommended)")
            lines.append(
                "SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 536870912;  -- 512MB"
            )
            lines.append("")
            lines.append("-- Skew join handling")
            lines.append("SET spark.sql.adaptive.skewJoin.enabled = true;")
            lines.append(
                "SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 536870912;  -- 512MB"
            )
            lines.append("```\n")
        else:
            lines.append("```sql")
            lines.append("SET spark.sql.adaptive.enabled = true;")
            lines.append("SET spark.sql.adaptive.coalescePartitions.enabled = true;")
            lines.append(
                "SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 536870912;  -- 512MB"
            )
            lines.append("SET spark.sql.adaptive.skewJoin.enabled = true;")
            lines.append("```\n")

        # Show shuffle-specific settings when needed
        if has_shuffle_issues and bi.shuffle_impact_ratio > 0.4:
            lines.append(f"### {_('Shuffle Partition Settings')}\n")
            lines.append(
                f"> {_('Significant shuffle impact detected. Consider partition count adjustment.')}\n"
            )
            lines.append("```sql")
            lines.append("-- Increase partition count to reduce memory per partition")
            lines.append("SET spark.sql.shuffle.partitions = 400;  -- Default is 200")
            lines.append("```\n")

    return "\n".join(lines)


def generate_validation_checklist(
    action_cards: list[ActionCard],
    bi: BottleneckIndicators,
    sql_analysis: SQLAnalysis | None = None,
    join_info: list | None = None,
    *,
    include_header: bool = True,
) -> str:
    """Generate validation checklist for post-optimization verification.

    Args:
        action_cards: List of action cards
        bi: Bottleneck indicators
        sql_analysis: SQL analysis for query structure (JOIN/WHERE detection)
        join_info: Join information from explain plan

    Returns:
        Markdown formatted validation checklist
    """
    # Determine query characteristics for filtering inappropriate items
    has_join = bool(join_info) or (sql_analysis and sql_analysis.structure.join_count > 0)
    has_where = sql_analysis and any(col.context == "where" for col in sql_analysis.columns)
    lines = []
    if include_header:
        lines.append("---\n")
        lines.append(f"## ✅ {_('Validation Checklist')}\n")
    lines.append(f"{_('Please verify the following metrics after optimization')}:\n")

    checklist_items = set()

    # Add validation metrics from action cards
    for card in action_cards:
        if card.validation_metric:
            checklist_items.add(card.validation_metric)

    # Add standard checks based on indicators
    if bi.spill_bytes > 0:
        checklist_items.add("spill_to_disk_bytes = 0")
    if bi.photon_ratio < 0.8:
        checklist_items.add("photon_ratio >= 80%")
    if bi.cache_hit_ratio < 0.3:
        checklist_items.add(f"cache_hit_ratio >= 30% ({_('on re-run')})")
    if bi.shuffle_impact_ratio > 0.2:
        checklist_items.add("shuffle_impact_ratio < 20%")
    # Only add filter_rate check if WHERE clause exists (otherwise not achievable)
    if bi.filter_rate < 0.3 and has_where:
        checklist_items.add("filter_rate >= 50%")

    # Add scan locality check (Verbose mode metrics)
    if bi.rescheduled_scan_ratio > 0.05:
        checklist_items.add("rescheduled_scan_ratio <= 5%")

    # Add hash join efficiency checks (only when JOINs exist)
    if has_join:
        if bi.hash_table_resize_count >= 100:
            checklist_items.add("hash_table_resize_count < 100")
        if bi.avg_hash_probes_per_row >= 10:
            checklist_items.add("avg_hash_probes_per_row < 10")

    for item in sorted(checklist_items):
        lines.append(f"- [ ] `{item}`")

    lines.append("")
    return "\n".join(lines)
