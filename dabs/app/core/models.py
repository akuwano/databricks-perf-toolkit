"""
Data models for the profiler analyzer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .constants import JoinType, Severity, UsageType

if TYPE_CHECKING:
    from .evidence import EvidenceBundle
    from .explain_parser import ExplainExtended
    from .warehouse_client import WarehouseInfo


@dataclass
class QueryMetrics:
    """High-level query metrics extracted from the profile."""

    query_id: str = ""
    status: str = ""
    query_text: str = ""
    total_time_ms: int = 0
    compilation_time_ms: int = 0
    execution_time_ms: int = 0
    read_bytes: int = 0
    read_remote_bytes: int = 0
    read_cache_bytes: int = 0
    spill_to_disk_bytes: int = 0
    photon_total_time_ms: int = 0
    task_total_time_ms: int = 0
    read_files_count: int = 0
    pruned_files_count: int = 0
    pruned_bytes: int = 0
    rows_read_count: int = 0
    rows_produced_count: int = 0
    bytes_read_from_cache_percentage: int = 0
    write_remote_bytes: int = 0
    write_remote_files: int = 0
    write_remote_rows: int = 0
    network_sent_bytes: int = 0
    read_partitions_count: int = 0
    # Result cache hit: True if query result served from result cache (no execution)
    result_from_cache: bool = False
    # Result fetch time: time spent returning results to client (ms)
    result_fetch_time_ms: int = 0
    # Queue times (Serverless): provisioning and overload queue wait (ms)
    queued_provisioning_time_ms: int = 0
    queued_overload_time_ms: int = 0
    # Timestamps used to derive pre-compile driver overhead
    # (scheduling + waiting-for-compute) when explicit fields are absent.
    query_start_time_ms: int = 0
    provisioning_queue_start_ts: int = 0
    overloading_queue_start_ts: int = 0
    query_compilation_start_ts: int = 0
    # Metadata resolution time (ms)
    metadata_time_ms: int = 0
    # Plan-structure signals derived from the profile graph. Used by
    # the cluster_underutilization detector to distinguish driver-overhead
    # from external-contention variants.
    aqe_replan_count: int = 0  # UNKNOWN_ADAPTIVE_SPARK_PLAN_EXEC count
    subquery_count: int = 0  # SUBQUERY_EXEC + PhotonMetadataSubquery count
    broadcast_hash_join_count: int = 0  # PHOTON_BROADCAST_HASH_JOIN_EXEC count
    total_plan_node_count: int = 0  # full graph node count
    # Compilation phase breakdown: [{"phase": "ANALYSIS", "duration_ms": 2}, ...]
    planning_phases: list[dict[str, Any]] = field(default_factory=list)
    # GraphQL __typename: "LakehouseSqlQuery" (Serverless) or "SqlQuery" (Classic/Pro)
    query_typename: str = ""
    # Extra metrics: unmapped keys from query.metrics (for future extensibility)
    extra_metrics: dict[str, Any] = field(default_factory=dict)
    # Lakehouse Federation detection (v5.18.0): True when any scan node
    # is ``ROW_DATA_SOURCE_SCAN_EXEC`` — i.e. the query reads from an
    # external source (BigQuery, Snowflake, Postgres, …) via Lakehouse
    # Federation rather than from Delta files. Aggregated from
    # ``NodeMetrics.is_federation_scan`` during extraction.
    is_federation_query: bool = False
    # Best-effort source type guess, populated when the catalog name
    # matches a known heuristic (``*_bq_*`` → ``bigquery`` etc.) or
    # future explicit profile metadata. Empty when unknown — callers
    # must treat empty as "Lakehouse Federation, source unknown".
    federation_source_type: str = ""
    # Federated table references (``catalog.schema.table``) gathered
    # from the ``Row Data Source Scan <table>`` node names.
    federation_tables: list[str] = field(default_factory=list)


@dataclass
class NodeMetrics:
    """Metrics extracted from execution plan nodes."""

    node_id: str = ""
    node_name: str = ""
    node_tag: str = ""
    duration_ms: int = 0
    peak_memory_bytes: int = 0
    rows_num: int = 0
    files_read: int = 0
    files_pruned: int = 0
    files_pruned_size: int = 0
    files_read_size: int = 0
    cache_hits_size: int = 0
    cache_misses_size: int = 0
    cloud_storage_request_count: int = 0
    cloud_storage_request_duration_ms: int = 0
    cloud_storage_retry_count: int = 0
    cloud_storage_retry_duration_ms: int = 0
    data_filters_batches_skipped: int = 0
    data_filters_rows_skipped: int = 0
    rows_scanned: int = 0
    rows_output: int = 0
    spill_bytes: int = 0
    is_photon: bool = False
    is_delta: bool = False
    # v5.18.0: True when ``node_tag == "ROW_DATA_SOURCE_SCAN_EXEC"``,
    # which is how Lakehouse Federation scans surface in the profile
    # JSON. These nodes have empty SCAN_DATABASE / SCAN_TABLE /
    # SCAN_FILE_PATHS metadata because the data lives in an external
    # system (BigQuery, Snowflake, …) rather than Delta files.
    is_federation_scan: bool = False
    clustering_keys: list[str] = field(default_factory=list)
    # Per clustering column: (min, max) raw strings from profile metadata when available
    clustering_key_bounds: dict[str, tuple[str | None, str | None]] = field(default_factory=dict)
    filter_conditions: list[str] = field(default_factory=list)
    join_keys_left: list[str] = field(default_factory=list)
    join_keys_right: list[str] = field(default_factory=list)
    join_type: str = ""
    join_algorithm: str = ""
    partition_filters: list[str] = field(default_factory=list)
    # Scan locality metrics (Verbose mode only)
    local_scan_tasks: int = 0
    non_local_scan_tasks: int = 0
    # Aggregate/Grouping expressions from metadata (for implicit CAST detection)
    aggregate_expressions: list[str] = field(default_factory=list)
    grouping_expressions: list[str] = field(default_factory=list)
    # Extra metrics: unmapped labels from node.metrics[] (for future extensibility)
    extra_metrics: dict[str, int] = field(default_factory=dict)


@dataclass
class ShuffleMetrics:
    """Metrics for shuffle operations."""

    node_id: str = ""
    node_name: str = ""
    partition_count: int = 0
    peak_memory_bytes: int = 0
    duration_ms: int = 0
    rows_processed: int = 0
    shuffle_attributes: list[str] = field(default_factory=list)
    aqe_partitions: int = 0
    aqe_data_size: int = 0
    aqe_skewed_partitions: int = 0
    sink_tasks_total: int = 0
    source_tasks_total: int = 0
    # Sink-side working memory and data size — more reliable than the
    # node-level peakMemoryBytes (which accumulates across executors and
    # tasks, producing absurd values when divided by coalesced partition
    # count).
    sink_peak_memory_bytes: int = 0
    sink_bytes_written: int = 0
    sink_num_spills: int = 0
    # AQE-driven repartition metrics. When AQE detects that the current
    # partition count is too coarse for the data volume, it performs a
    # "self-triggered repartition", increasing the partition count
    # without user intervention. Together with no-spill signal, this
    # indicates the issue is data volume / layout, NOT key-value skew
    # (AQE handles skew separately via skew-join handling).
    aqe_original_num_partitions: int = 0
    aqe_intended_num_partitions: int = 0
    aqe_self_repartition_count: int = 0
    # Additional AQE plan-change signals (surfaced in the Data Flow
    # report so readers can see WHEN AQE took action at runtime).
    aqe_cancellation_count: int = 0  # stages AQE cancelled and re-planned
    aqe_triggered_on_materialized_count: int = 0  # AQE re-planned after a stage materialized
    # AOS (Auto-Optimized Shuffle) metrics. AOS coordinates partition
    # counts across multiple shuffles; non-zero coordinated_repartition_count
    # indicates AOS adjusted the physical plan at runtime.
    aos_coordinated_repartition_count: int = 0
    aos_old_num_partitions: int = 0
    aos_new_num_partitions: int = 0
    aos_intended_num_partitions: int = 0

    @property
    def memory_per_partition_mb(self) -> float:
        """Working memory per parallel unit, in MB.

        Primary formula: Sink peak memory / Sink tasks total. This
        reflects actual per-task shuffle working memory and avoids the
        trap of dividing cumulative node-level peak memory by a coalesced
        output partition count (which inflated values into the hundreds
        of GB for harmless shuffles).

        Fallback: peak_memory_bytes / partition_count, used only when
        sink_tasks_total is unavailable AND partition_count >= 2.
        """
        # Preferred: sink_peak_memory_bytes / sink_tasks_total
        if self.sink_peak_memory_bytes > 0 and self.sink_tasks_total > 0:
            return self.sink_peak_memory_bytes / self.sink_tasks_total / (1024 * 1024)
        # Fallback only when it is semantically meaningful
        if self.partition_count >= 2:
            return self.peak_memory_bytes / self.partition_count / (1024 * 1024)
        return 0.0

    @property
    def is_lightweight_shuffle(self) -> bool:
        """True when Sink side wrote little data and did not spill.

        Used by the bottleneck analyzer to suppress false-positive
        "memory inefficiency" alerts on harmless final-coalesce shuffles
        that happen to report large cumulative peak memory numbers.
        """
        return (
            self.sink_num_spills == 0
            and 0 < self.sink_bytes_written <= 1024 * 1024 * 1024  # < 1 GB written
        )

    @property
    def avg_aqe_partition_size_mb(self) -> float:
        """Calculate average AQE partition size in MB."""
        if self.aqe_partitions <= 0:
            return 0.0
        return (self.aqe_data_size / self.aqe_partitions) / (1024 * 1024)

    @property
    def is_memory_efficient(self) -> bool:
        """Check if memory per partition is within threshold (128MB)."""
        return self.memory_per_partition_mb <= 128

    @property
    def optimization_priority(self) -> Severity:
        """Determine optimization priority based on memory per partition."""
        mem_mb = self.memory_per_partition_mb
        if mem_mb > 2048:  # > 2GB
            return Severity.CRITICAL
        elif mem_mb > 1024:  # > 1GB
            return Severity.HIGH
        elif mem_mb > 128:  # > 128MB
            return Severity.MEDIUM
        return Severity.OK


@dataclass
class JoinInfo:
    """Information about a join operation."""

    node_name: str = ""
    join_type: JoinType = JoinType.UNKNOWN
    is_photon: bool | None = None  # None = unknown, True = Photon, False = not Photon
    duration_ms: int = 0


@dataclass
class SpillOperatorInfo:
    """Spill information for a single operator."""

    node_id: str = ""
    node_name: str = ""
    spill_bytes: int = 0
    peak_memory_bytes: int = 0
    rows_processed: int = 0
    spill_share_percent: float = 0.0


@dataclass
class CloudStorageMetrics:
    """Aggregated cloud storage metrics across all nodes."""

    total_request_count: int = 0
    total_retry_count: int = 0
    total_request_duration_ms: int = 0
    total_retry_duration_ms: int = 0
    retry_ratio: float = 0.0
    avg_request_duration_ms: float = 0.0


@dataclass
class HashResizeHotspot:
    """Per-node contribution to hash table resize count (for skew attribution)."""

    node_id: str = ""
    node_tag: str = ""  # e.g. "PHOTON_GROUPING_AGG_EXEC"
    node_name: str = ""  # raw operator name from the profile
    resize: int = 0
    probes: float = 0.0
    keys: list[str] = field(default_factory=list)
    key_kind: str = ""  # "join" | "group"


@dataclass
class PhotonBlocker:
    """Information about a Photon-unsupported operation."""

    reason: str = ""
    count: int = 0
    impact: str = ""  # "HIGH", "MEDIUM", "LOW"
    action: str = ""  # Recommended action
    # Extended fields for EXPLAIN-based analysis
    unsupported_expression: str = ""  # e.g., "pivotfirst(...)"
    detail_message: str = ""  # e.g., "Unsupported aggregation function pivotfirst..."
    reference_node: str = ""  # e.g., "HashAggregate(...)"
    sql_rewrite_example: str = ""  # Concrete SQL rewrite suggestion


@dataclass
class Alert:
    """Structured alert with severity classification.

    Provides more detailed context than simple string warnings,
    enabling better prioritization and filtering in reports.
    """

    severity: Severity = Severity.INFO  # CRITICAL, HIGH, MEDIUM, INFO
    category: str = ""  # "cache", "spill", "shuffle", "photon", "io", "cloud_storage", "join"
    message: str = ""  # Human-readable message
    metric_name: str = ""  # Affected metric (e.g., "cache_hit_ratio")
    current_value: str = ""  # Current value (e.g., "25%")
    threshold: str = ""  # Threshold that was exceeded (e.g., ">80%")
    recommendation: str = ""  # Specific action to take
    is_actionable: bool = True  # False for informational alerts
    conflicts_with: list[str] = field(default_factory=list)  # IDs of conflicting alerts

    @property
    def alert_id(self) -> str:
        """Generate unique ID for conflict detection."""
        return f"{self.category}:{self.metric_name}"


@dataclass
class BottleneckIndicators:
    """Bottleneck indicators calculated from metrics."""

    # Cache efficiency
    cache_hit_ratio: float = 0.0
    cache_severity: Severity = Severity.OK

    # Remote read ratio (new)
    remote_read_ratio: float = 0.0
    remote_read_severity: Severity = Severity.OK

    # Photon efficiency
    photon_ratio: float = 0.0
    photon_severity: Severity = Severity.OK

    # Spill analysis
    spill_bytes: int = 0
    spill_severity: Severity = Severity.OK

    # Filter efficiency (file-based)
    filter_rate: float = 0.0
    filter_severity: Severity = Severity.OK

    # Bytes pruning efficiency (new)
    bytes_pruning_ratio: float = 0.0
    bytes_pruning_severity: Severity = Severity.OK

    # Shuffle impact
    shuffle_impact_ratio: float = 0.0
    shuffle_severity: Severity = Severity.OK

    # Scan impact (fraction of task time spent in scan operators). Used
    # to gate IO-related alerts/action cards so they do not surface on
    # compute-bound queries where pruning/cache improvements barely
    # move the needle.
    scan_impact_ratio: float = 0.0

    # Cloud storage
    cloud_storage_retry_ratio: float = 0.0
    cloud_storage_severity: Severity = Severity.OK
    cloud_storage_metrics: CloudStorageMetrics = field(default_factory=CloudStorageMetrics)

    # Data skew
    has_data_skew: bool = False
    skewed_partitions: int = 0

    # Spill operators (top 5)
    spill_operators: list[SpillOperatorInfo] = field(default_factory=list)

    # Photon blockers
    photon_blockers: list[PhotonBlocker] = field(default_factory=list)

    # Predictive I/O metrics (new)
    data_filters_batches_skipped: int = 0
    data_filters_rows_skipped: int = 0

    # Scan locality metrics (Verbose mode only)
    local_scan_tasks_total: int = 0
    non_local_scan_tasks_total: int = 0
    rescheduled_scan_ratio: float = 0.0
    rescheduled_scan_severity: Severity = Severity.OK

    # Extra metrics derived indicators (from unmapped metrics)
    # OOM fallback detection (Photon -> non-Photon fallback)
    oom_fallback_count: int = 0
    oom_fallback_nodes: list[str] = field(default_factory=list)
    # Hash join internal metrics
    hash_build_time_total_ms: int = 0
    hash_table_resize_count: int = 0
    avg_hash_probes_per_row: float = 0.0
    # Top nodes contributing to hash table resize (for skew attribution).
    hash_resize_hotspots: list[HashResizeHotspot] = field(default_factory=list)
    # True when EXPLAIN confirms all tables have full optimizer statistics.
    # Used downstream to suppress ANALYZE TABLE recommendations and to
    # surface alternative-cause guidance instead.
    statistics_confirmed_fresh: bool = False
    # Aggregated AQE-layout signals from shuffle nodes. When AQE self-
    # repartitioned at least one exchange AND no shuffle spilled, the
    # workload looks like "data volume outgrew initial partitioning",
    # not skew. Used to downgrade the skew diagnosis and surface a
    # Liquid-Clustering / layout recommendation instead.
    aqe_self_repartition_seen: bool = False
    max_aqe_partition_growth_ratio: float = 0.0  # intended / original
    # Shuffle I/O volume
    shuffle_bytes_written_total: int = 0
    shuffle_remote_bytes_read_total: int = 0
    shuffle_local_bytes_read_total: int = 0
    # I/O wait metrics
    io_fetch_wait_time_total_ms: int = 0
    io_decompress_time_total_ms: int = 0
    prism_queue_time_total_ms: int = 0
    # Spill detailed metrics
    spill_count_total: int = 0
    spill_rows_total: int = 0
    spill_partitions_total: int = 0
    # Observed extra metric labels (for discovery/debugging)
    observed_extra_labels: list[str] = field(default_factory=list)

    # Compilation / file-pruning overhead (driver-side).
    # Fires when query compilation (SQL parse, Catalyst optimization, Delta log
    # replay, file-level stats pruning) dominates wall-clock time and that
    # time is explained by heavy metadata work — typical cause is small-files
    # proliferation or unvacuumed Delta log.
    compilation_time_ratio: float = 0.0  # compilationTimeMs / totalTimeMs
    compilation_pruning_heavy: bool = False  # metadata/pruning evidence present
    compilation_severity: Severity = Severity.OK

    # Driver overhead — queue wait + scheduling + waiting-for-compute.
    # Derived from explicit queue fields plus timestamp gaps when the UI's
    # "Scheduling" / "Waiting for compute" bars are not exposed as
    # first-class metrics in the JSON.
    queue_wait_ms: int = 0  # provisioning + overload
    scheduling_compute_wait_ms: int = 0  # pre-compile residual after queue
    driver_overhead_ms: int = 0  # queue_wait + scheduling_compute_wait
    driver_overhead_ratio: float = 0.0  # driver_overhead_ms / totalTimeMs
    driver_overhead_severity: Severity = Severity.OK

    # Cluster under-utilization — executors sit idle relative to available
    # parallelism for long-enough queries. Variant distinguishes external
    # contention (another query stole CPU) from driver overhead (complex
    # plan with many AQE re-plans / subqueries / broadcasts) from a plan
    # that is simply serial by nature.
    effective_parallelism: float = 0.0  # task_total_time_ms / execution_time_ms
    cluster_underutilization_variant: str = ""  # external / driver / serial / ""
    cluster_underutilization_severity: Severity = Severity.OK

    # Compilation absolute-heavy (low-severity advisory). Fires when the
    # absolute compile time is large enough to be worth flagging
    # (>= 5s + pruning evidence) even though the existing
    # compilation_overhead card's ratio gate (>= 30%) doesn't trip because
    # execution time is even longer.
    compilation_absolute_heavy: bool = False

    # Write operator fallback detection
    write_fallback_detected: bool = False

    # Type cast detection
    cast_count: int = 0  # Total CAST expressions in physical plan
    cast_in_join_filter: int = 0  # CAST in join/filter context (harmful)

    # EXPLAIN v2 insights (Phase 2 — derived from ExplainExtended)
    # True when at least one implicit CAST wraps a JOIN-context column ref;
    # direct evidence of a JOIN-key data-type mismatch. Critical for rewrites
    # because it forbids the "just add a WHERE" fix and points to schema.
    implicit_cast_on_join_key: bool = False
    # 2026-04-26 V6 alert coverage expansion (Codex (e) follow-up):
    # large aggregate node (peak_memory >= threshold) whose aggregate
    # expressions contain arithmetic (* / + / -). Wide DECIMAL inputs
    # widen to DECIMAL(38,18) under arithmetic, inflating per-row CPU
    # and hash-table memory. The card recommends DESCRIBE TABLE +
    # type review (DECIMAL → INT/BIGINT when integer-only).
    decimal_heavy_aggregate: bool = False
    # ``[(node_id, expression_excerpt)]`` for the top examples — used
    # by the card to populate evidence without re-walking node_metrics.
    decimal_heavy_aggregate_examples: list[tuple[str, str]] = field(default_factory=list)
    # CTE references with reference_count >= 2 that were NOT materialized as a
    # ReusedExchange. Each one means Spark is re-computing the CTE body.
    cte_reuse_miss_count: int = 0
    # Physical-plan operators without a Photon prefix (excl. known boundary
    # wrappers). Correlates with performance cliffs.
    photon_fallback_op_count: int = 0
    # Scans whose PartitionFilters came out empty despite having DataFilters —
    # partition pruning is effectively disabled for that scan.
    filter_pushdown_gap_count: int = 0

    # Partition strategy
    partition_column_count: int = 0
    partition_columns: list[str] = field(default_factory=list)

    # Summary (legacy - kept for backward compatibility)
    critical_issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    # Structured alerts (new - preferred over string-based warnings)
    alerts: list[Alert] = field(default_factory=list)

    # Detected signals (Phase 1: LLM-driven bottleneck detection)
    # Replaces rule-based Severity judgment with factual signal extraction.
    # LLM uses these signals + reference thresholds to determine severity.
    detected_signals: list[BottleneckSignal] = field(default_factory=list)


@dataclass
class BottleneckSignal:
    """A factual signal detected from metrics — no severity judgment.

    LLM uses these signals with reference thresholds and context
    to determine actual severity and priority.
    """

    signal_id: str = ""  # e.g., "spill_detected", "low_cache_hit", "data_skew"
    category: str = ""  # "spill", "cache", "shuffle", "photon", "io", "skew"
    description: str = ""  # Human-readable description
    observed_value: str = ""  # e.g., "5.1 GB"
    reference_value: str = ""  # e.g., "5.0 GB (threshold)"
    context: dict[str, Any] = field(default_factory=dict)  # Additional context
    # e.g., {"ratio_of_read": 0.005, "peak_memory_bytes": 1073741824}


@dataclass
class TableReference:
    """Reference to a table in a SQL query."""

    catalog: str = ""
    schema: str = ""
    table: str = ""
    alias: str = ""
    full_name: str = ""
    usage_type: UsageType = UsageType.SOURCE


@dataclass
class ColumnReference:
    """Reference to a column in a SQL query."""

    column_name: str = ""
    table_alias: str = ""
    table_name: str = ""
    context: str = ""  # "select", "where", "join", "group_by", "order_by"
    operator: str = ""  # "=", "<", ">", "<=", ">=", "BETWEEN", "IN", "LIKE", etc.


@dataclass
class JoinEdge:
    """Information about a specific JOIN operation between tables."""

    join_type: str = ""  # "INNER JOIN", "LEFT JOIN", etc.
    left_table: str = ""  # Left side table name (or alias)
    left_alias: str = ""  # Left side alias if any
    right_table: str = ""  # Right side table name
    right_alias: str = ""  # Right side alias if any

    def __str__(self) -> str:
        """Return human-readable representation."""
        left = self.left_alias or self.left_table
        right = self.right_alias or self.right_table
        return f"{left} {self.join_type} {right}"


@dataclass
class QueryStructure:
    """Structural analysis of a SQL query."""

    statement_type: str = ""
    join_count: int = 0
    join_types: list[str] = field(default_factory=list)
    join_edges: list[JoinEdge] = field(default_factory=list)
    subquery_count: int = 0
    cte_count: int = 0
    cte_names: list[str] = field(default_factory=list)
    aggregate_functions: list[str] = field(default_factory=list)
    window_functions: list[str] = field(default_factory=list)
    has_distinct: bool = False
    has_group_by: bool = False
    has_order_by: bool = False
    has_limit: bool = False
    has_union: bool = False
    complexity_score: int = 0


@dataclass
class SQLAnalysis:
    """Complete SQL analysis result."""

    raw_sql: str = ""
    formatted_sql: str = ""
    tables: list[TableReference] = field(default_factory=list)
    columns: list[ColumnReference] = field(default_factory=list)
    structure: QueryStructure = field(default_factory=QueryStructure)


@dataclass
class SQLImprovementExample:
    """SQL improvement example with before/after patterns."""

    issue_type: str = ""  # "scan", "join", "shuffle", "spill", "filter"
    title: str = ""  # Short title for the example
    description: str = ""  # Explanation of the improvement
    before_sql: str = ""  # Original problematic pattern (optional)
    after_sql: str = ""  # Improved SQL pattern
    impact: str = ""  # "high", "medium", "low"
    applies_to: list[str] = field(default_factory=list)  # Table names this applies to


@dataclass
class TableScanMetrics:
    """Metrics for table scan operations."""

    table_name: str = ""  # Full table name (catalog.schema.table)
    bytes_read: int = 0  # Size of files read
    bytes_pruned: int = 0  # Size of files pruned
    files_read: int = 0
    files_pruned: int = 0
    rows_scanned: int = 0
    current_clustering_keys: list[str] = field(default_factory=list)  # From profile (usually N/A)
    recommended_clustering_keys: list[str] = field(default_factory=list)  # From SQL analysis
    # Estimated cardinality class per current clustering key column: low | high | unknown
    clustering_key_cardinality: dict[str, str] = field(default_factory=dict)

    @property
    def file_pruning_rate(self) -> float:
        """Calculate file pruning rate."""
        total = self.files_read + self.files_pruned
        if total == 0:
            return 0.0
        return self.files_pruned / total

    @property
    def bytes_pruning_rate(self) -> float:
        """Calculate bytes pruning rate."""
        total = self.bytes_read + self.bytes_pruned
        if total == 0:
            return 0.0
        return self.bytes_pruned / total


@dataclass
class OperatorHotspot:
    """Hot operator identified by time consumption."""

    rank: int = 0
    node_id: str = ""
    node_name: str = ""
    duration_ms: int = 0
    time_share_percent: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    spill_bytes: int = 0
    peak_memory_bytes: int = 0
    is_photon: bool = False
    bottleneck_type: str = ""  # "spill", "shuffle", "scan", "join", "sort", "agg"

    @property
    def is_critical(self) -> bool:
        """Check if this operator is a critical bottleneck."""
        return self.time_share_percent >= 20.0 or self.spill_bytes > 1024**3


@dataclass
class ActionCard:
    """Actionable recommendation with evidence and priority."""

    problem: str = ""  # What is the problem
    evidence: list[str] = field(default_factory=list)  # Metrics that exceeded thresholds
    likely_cause: str = ""  # Root cause hypothesis
    fix: str = ""  # Specific fix (SQL/table design/config)
    fix_sql: str = ""  # Optional SQL snippet
    expected_impact: str = ""  # "high", "medium", "low"
    effort: str = ""  # "low", "medium", "high"
    # Phase 2e (v5.16.19): ``priority_rank`` is the Spark Perf-style
    # static priority (100 = highest, 0 = lowest) assigned by the
    # registry's CardDef. ``priority_score`` is legacy — kept for
    # backward compatibility with consumers that compare floats. New
    # code should prefer ``priority_rank``.
    priority_rank: int = 0
    priority_score: float = 0.0  # Legacy: impact * confidence / effort or priority_rank / 10.0
    validation_metric: str = ""  # What to check after fix
    # v4.8: Action Plan fields
    risk: str = ""  # "low", "medium", "high"
    risk_reason: str = ""  # Why this risk level
    verification_steps: list[dict[str, str]] = field(default_factory=list)
    # Each dict: {"metric": "...", "expected": "..."} or {"sql": "...", "expected": "..."}
    severity: str = ""  # Optional advisory severity e.g. MEDIUM (Hierarchical Clustering hint)
    # v5.16.0: Top-5 action selection fields
    # Cards sharing a root_cause_group address the same underlying issue
    # and should not crowd out Top 5; the rerank picks at most one per
    # group (except when preserved). See core/action_classify.py.
    root_cause_group: str = ""
    # Coarse coverage bucket used to keep Top 5 diverse across facets.
    # One of: COMPUTE / DATA / QUERY / MEMORY / PARALLELISM / "".
    coverage_category: str = ""
    # Rationale from the LLM rerank for why this card made Top 5.
    # Empty when selected purely by the deterministic rule-based path.
    selected_because: str = ""
    # Whether this card corresponds to a preservation-marker-matched
    # alert. Populated by ``_select_top_action_cards`` so the reporter
    # can surface a separate "Must-read alerts" section.
    is_preserved: bool = False

    @property
    def impact_score(self) -> int:
        """Convert expected_impact to numeric score."""
        return {"high": 5, "medium": 3, "low": 1}.get(self.expected_impact, 1)

    @property
    def effort_score(self) -> int:
        """Convert effort to numeric score."""
        return {"low": 1, "medium": 3, "high": 5}.get(self.effort, 3)

    @staticmethod
    def assign_priority_levels(cards: list[ActionCard]) -> list[str]:
        """Assign P0/P1/P2 labels based on rank position (not score threshold).

        Top 2 = P0, next 3 = P1, rest = P2.
        Cards should already be sorted by priority_score descending.
        """
        levels = []
        for i in range(len(cards)):
            if i < 2:
                levels.append("P0")
            elif i < 5:
                levels.append("P1")
            else:
                levels.append("P2")
        return levels


@dataclass
class ClusteringRecommendation:
    """LLM-based clustering recommendation result."""

    target_table: str = ""
    recommended_keys: list[str] = field(default_factory=list)
    workload_pattern: str = ""  # "olap", "oltp", "timeseries", "unknown"
    rationale: str = ""
    confidence: float = 0.0
    alternatives: list[str] = field(default_factory=list)


@dataclass
class StageInfo:
    """Stage execution information from query profile."""

    stage_id: str = ""
    status: str = ""  # COMPLETE, SKIPPED, FAILED, PENDING
    duration_ms: int = 0
    num_tasks: int = 0
    num_complete_tasks: int = 0
    num_failed_tasks: int = 0
    num_killed_tasks: int = 0
    note: str = ""  # Auto-generated (Scan/Join, Shuffle, OOM Kill, etc.)

    @property
    def is_failed(self) -> bool:
        """Check if this stage failed."""
        return self.status == "FAILED"


@dataclass
class DataFlowEntry:
    """Data flow through an operator (mainly JOINs and Scans)."""

    node_id: str = ""
    operation: str = ""  # "Scan (table)", "Inner Join", "LEFT OUTER JOIN", etc.
    output_rows: int = 0
    duration_ms: int = 0
    peak_memory_bytes: int = 0
    join_keys: str = ""  # JOIN key columns (JOIN nodes only)


@dataclass
class DataFlowEdge:
    """Edge between two interesting nodes in the data flow DAG."""

    from_node_id: str = ""
    to_node_id: str = ""


@dataclass
class DataFlowDAG:
    """DAG representation of data flow through interesting operators (Scan/Join)."""

    entries: list[DataFlowEntry] = field(default_factory=list)
    edges: list[DataFlowEdge] = field(default_factory=list)
    children_map: dict[str, list[str]] = field(default_factory=dict)
    parents_map: dict[str, list[str]] = field(default_factory=dict)
    sink_node_ids: list[str] = field(default_factory=list)
    source_node_ids: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# v3: Comparison / Tracking / Knowledge models
# ---------------------------------------------------------------------------


@dataclass
class AnalysisContext:
    """Context for tracking and comparing analyses."""

    query_fingerprint: str = ""
    query_fingerprint_version: str = "v1"
    experiment_id: str = ""
    variant: str = ""
    variant_group: str = ""
    baseline_flag: bool = False
    tags: dict[str, Any] = field(default_factory=dict)
    source_run_id: str = ""
    source_job_id: str = ""
    source_job_run_id: str = ""
    analysis_notes: str = ""
    query_text_normalized: str = ""
    # v3: Query family grouping (same purpose, different conditions)
    query_family_id: str = ""
    purpose_signature: str = ""
    variant_type: str = ""  # same_sql / diff_hint / diff_filter / diff_warehouse / etc.
    feature_json: str = ""  # JSON of purpose features for Genie
    # v4.11: LLM prompt versioning
    prompt_version: str = ""


@dataclass
class ComparisonRequest:
    """Request to compare two analyses."""

    baseline_analysis_id: str = ""
    candidate_analysis_id: str = ""
    comparison_scope: str = "full"  # header_only / with_actions / with_scans / full
    comparison_reason: str = ""
    requested_by: str = ""
    request_source: str = "manual"  # api / batch / notebook / manual
    tags: dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricDiff:
    """Difference for a single metric between baseline and candidate."""

    metric_name: str = ""
    metric_group: str = ""
    direction_when_increase: str = ""  # IMPROVES / WORSENS / NEUTRAL
    baseline_value: float | None = None
    candidate_value: float | None = None
    absolute_diff: float | None = None
    relative_diff_ratio: float | None = None
    changed_flag: bool = False
    improvement_flag: bool = False
    regression_flag: bool = False
    severity: str = "NONE"  # CRITICAL / HIGH / MEDIUM / LOW / NONE
    summary_text: str = ""


@dataclass
class ComparisonResult:
    """Result of comparing two analyses."""

    comparison_id: str = ""
    baseline_analysis_id: str = ""
    candidate_analysis_id: str = ""
    query_fingerprint: str = ""
    experiment_id: str = ""
    baseline_variant: str = ""
    candidate_variant: str = ""
    metric_diffs: list[MetricDiff] = field(default_factory=list)
    regression_detected: bool = False
    regression_severity: str = "NONE"
    summary: str = ""


@dataclass
class KnowledgeDocument:
    """Knowledge entry derived from analysis or comparison."""

    document_id: str = ""
    knowledge_type: str = (
        ""  # recommendation / finding / regression_case / tuning_pattern / manual_note
    )
    source_type: str = ""  # analysis / comparison / human / imported
    source_analysis_id: str = ""
    source_comparison_id: str = ""
    query_fingerprint: str = ""
    experiment_id: str = ""
    variant: str = ""
    title: str = ""
    summary: str = ""
    body_markdown: str = ""
    problem_category: str = ""  # scan / spill / shuffle / photon / join / cache / skew
    root_cause: str = ""
    recommendation: str = ""
    expected_impact: str = ""  # high / medium / low
    confidence_score: float = 0.0
    applicability_scope: str = ""  # query / fingerprint / workload / warehouse
    status: str = "draft"  # draft / active / archived
    tags: dict[str, Any] = field(default_factory=dict)


@dataclass
class KnowledgeTag:
    """Tag for a knowledge document."""

    document_id: str = ""
    tag_name: str = ""
    tag_value: str = ""


@dataclass
class MicroBatchMetrics:
    """Metrics for a single streaming micro-batch execution (from planMetadatas)."""

    plan_id: str = ""
    status: str = ""  # "FINISHED" or "RUNNING"
    duration_ms: int = 0
    read_bytes: int = 0
    rows_read_count: int = 0
    total_time_ms: int = 0
    query_start_time_ms: int = 0


@dataclass
class StreamingContext:
    """Context for streaming DLT/SDP query profiles."""

    is_streaming: bool = False
    target_table: str = ""
    entry_point: str = ""  # "DLT"
    statement_type: str = ""  # "REFRESH"
    is_final: bool = False
    active_plan_id: str = ""
    batch_count: int = 0
    batches: list[MicroBatchMetrics] = field(default_factory=list)


@dataclass
class TargetTableInfo:
    """Metadata about the write target of an INSERT/CTAS/MERGE query.

    Extracted from the DESCRIBE-like block inside
    ``graphs[*].photonExplain[*].params[*].paramValue``. This is the
    authoritative source for whether the target is Delta / Parquet /
    Iceberg — the Write node's ``IS_DELTA`` flag only reflects the
    file-level writer (always parquet for Delta) and must NOT be
    trusted for logical-format judgment.
    """

    catalog: str = ""
    database: str = ""
    table: str = ""
    # "delta" / "parquet" / "iceberg" / "csv" / ""
    provider: str = ""
    # Outer list = each clustering key (LC can nest multi-column keys);
    # inner list = columns in that key. Example: [["a"], ["b"], ["c"]].
    clustering_columns: list[list[str]] = field(default_factory=list)
    # Hierarchical Clustering column names (lowercased by Delta catalog).
    hierarchical_clustering_columns: list[str] = field(default_factory=list)
    partitioned_by: list[str] = field(default_factory=list)
    # All delta.* + miscellaneous properties parsed as key=value.
    properties: dict[str, str] = field(default_factory=dict)
    raw_block: str = ""  # raw DESCRIBE text for debugging / LLM surface

    @property
    def full_name(self) -> str:
        parts = [p for p in (self.catalog, self.database, self.table) if p]
        return ".".join(parts)

    @property
    def is_delta(self) -> bool:
        """True when the target is a Delta table.

        Detection uses a priority-ordered ladder so that an explicit
        non-Delta provider (e.g. ``parquet`` / ``iceberg``) is never
        overridden by a stray ``delta.*`` property left over from a
        migration or a catalog-level default:

        1. **Strong positive** — explicit ``Provider: delta``
        2. **Strong negative** — any other explicit provider → NOT Delta
           (returns False even if ``delta.*`` properties or
           ``DeltaTableV2(...)`` substring happen to be present)
        3. **Medium positive** — ``DeltaTableV2(...)`` wrapper in the
           raw DESCRIBE block (provider was not emitted)
        4. **Weak positive** — any ``delta.*`` TBLPROPERTY present
           (last-resort signal; only consulted when provider is empty
           and no wrapper was seen)

        The Write node's ``IS_DELTA`` flag is intentionally NOT consulted —
        it is a file-level writer marker, not a logical-format judgment.
        """
        prov = self.provider.strip().lower()
        if prov == "delta":
            return True
        if prov:
            # Explicit non-Delta provider wins over downstream weak signals.
            return False
        if "DeltaTableV2(" in self.raw_block:
            return True
        if any(k.startswith("delta.") for k in self.properties):
            return True
        return False


@dataclass
class ProfileAnalysis:
    """Complete analysis of a query profile."""

    query_metrics: QueryMetrics = field(default_factory=QueryMetrics)
    node_metrics: list[NodeMetrics] = field(default_factory=list)
    shuffle_metrics: list[ShuffleMetrics] = field(default_factory=list)
    join_info: list[JoinInfo] = field(default_factory=list)
    bottleneck_indicators: BottleneckIndicators = field(default_factory=BottleneckIndicators)
    sql_analysis: SQLAnalysis = field(default_factory=SQLAnalysis)
    # EXPLAIN EXTENDED analysis (optional)
    explain_analysis: ExplainExtended | None = None
    # New: Hot operators and action cards for actionable insights
    hot_operators: list[OperatorHotspot] = field(default_factory=list)
    action_cards: list[ActionCard] = field(default_factory=list)
    selected_action_cards: list[ActionCard] = field(default_factory=list)
    # Phase 2a (v5.16.19): LLM-generated Action Plan cards. Kept
    # separately from rule-based ``action_cards`` so the reporter can
    # render them in their own "LLM 独自提案" subsection below the
    # registry output. No more hybrid dedup — cards with the same
    # target alert can appear in both lists; the separation makes the
    # distinction explicit.
    llm_action_cards: list[ActionCard] = field(default_factory=list)
    # New: SQL improvement examples (deprecated, kept for compatibility)
    sql_improvement_examples: list[SQLImprovementExample] = field(default_factory=list)
    # New: Top scanned tables with I/O metrics
    top_scanned_tables: list[TableScanMetrics] = field(default_factory=list)
    # New: Evidence bundle for LLM grounding
    evidence_bundle: EvidenceBundle | None = None
    # New: SQL Warehouse information (from API)
    warehouse_info: WarehouseInfo | None = None
    # New: Endpoint ID for warehouse lookup
    endpoint_id: str = ""
    # New: Stage execution information
    stage_info: list[StageInfo] = field(default_factory=list)
    # New: Data flow through operators
    data_flow: list[DataFlowEntry] = field(default_factory=list)
    # New: DAG structure for data flow visualization
    data_flow_dag: DataFlowDAG | None = None
    # Write target table metadata (INSERT/CTAS/MERGE queries only, else None).
    target_table_info: TargetTableInfo | None = None
    # Streaming DLT/SDP context (optional, None for non-streaming profiles)
    streaming_context: StreamingContext | None = None
    # v3: Context for tracking and comparing analyses
    analysis_context: AnalysisContext = field(default_factory=AnalysisContext)
