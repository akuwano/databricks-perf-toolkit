"""
Constants and enumerations for the profiler analyzer.
"""

from enum import Enum


class Severity(Enum):
    """Severity levels for bottleneck indicators."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"  # For informational items that may not indicate a problem
    OK = "ok"


class UsageType(Enum):
    """Table usage types in SQL queries."""

    SOURCE = "source"
    TARGET = "target"
    CTE = "cte"


class JoinType(Enum):
    """Join types with Photon support status."""

    BROADCAST = ("Broadcast", True, 1)  # (name, photon_supported, performance_rank)
    SHUFFLE_HASH = ("ShuffleHash", True, 2)
    SORT_MERGE = ("SortMerge", False, 3)
    SHUFFLE_NESTED_LOOP = ("ShuffleNestedLoop", True, 4)
    UNKNOWN = ("Unknown", False, 5)

    def __init__(self, display_name: str, photon_supported: bool, perf_rank: int):
        self.display_name = display_name
        self.photon_supported = photon_supported
        self.perf_rank = perf_rank


# Threshold constants from dbsql_tuning.md
THRESHOLDS = {
    "cache_hit_high": 0.80,  # >80% = good
    "cache_hit_medium": 0.50,  # 50-80% = needs improvement
    "cache_hit_low": 0.30,  # <30% = critical
    "remote_read_high": 0.80,  # >80% remote read = warning
    "remote_read_critical": 0.95,  # >95% remote read = critical
    "photon_high": 0.80,  # >80% = good
    "photon_medium": 0.50,  # 50-80% = needs improvement
    "photon_low": 0.50,  # <50% = critical
    "spill_critical_gb": 5.0,  # >5GB = critical
    "spill_high_gb": 1.0,  # >1GB = high
    "filter_low": 0.20,  # <20% = low efficiency
    "bytes_pruning_good": 0.50,  # >50% = good bytes pruning
    "bytes_pruning_low": 0.20,  # <20% = poor bytes pruning
    "shuffle_critical": 0.40,  # >=40% = critical bottleneck
    "shuffle_high": 0.20,  # 20-40% = moderate bottleneck
    "memory_per_partition_mb": 128,  # 128MB threshold
    "aqe_partition_size_warning_mb": 128,  # >128MB average = warning
    "broadcast_threshold_mb": 200,  # Recommended broadcast threshold
    "cloud_storage_retry_warning": 0.05,  # >5% retries = warning
    "cloud_storage_retry_critical": 0.10,  # >10% retries = critical
    # Scan locality thresholds (from dbsql_tuning.md section 6.3)
    "scan_locality_warning": 0.01,  # >1% = needs observation
    "scan_locality_critical": 0.05,  # >5% = action required
    # Hash resize attribution (v5.13 — PR #52)
    "hash_resize_high": 100,  # hash_table_resize_count considered high
    "hash_resize_critical": 1000,  # resize count considered critical
    "hash_probes_high": 10,  # avg_hash_probes_per_row considered high
    "hash_probes_critical": 50,  # probes/row considered critical
    "duplicate_groupby_min_resize": 100,  # duplicate GROUP BY alert threshold
    # Shuffle sanity gates (v5.13 — PR #52)
    "shuffle_memory_absurd_mb": 10_240,  # impossibly high mem-per-partition → suppress alert
    "shuffle_high_volume_bytes": 1_000_000_000,  # 1 GB — threshold for "significant shuffle"
    # Scan impact gate (v5.16.5) — IO-related alerts/cards are gated on
    # "is scan actually a dominant cost?" to avoid burying compute-bound
    # Top-5 lists with irrelevant pruning/cache recommendations.
    "scan_impact_dominant": 0.25,  # >=25% → full HIGH + action card
    "scan_impact_mid": 0.10,  # 10-25% → demoted to MEDIUM + impact="low"
    # Photon utilization gate — on tiny/short queries the Photon ratio is
    # noise (cold start / compile-only cost dominates). Suppress/demote.
    "photon_tiny_query_ms": 500,  # <500ms → suppress / INFO
    "photon_small_query_ms": 5_000,  # 500-5000ms → demoted to MEDIUM
}

# Spark configs supported on Databricks SQL (including Serverless SQL Warehouses)
# https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters
# Only these 7 parameters can be SET; all others are rejected.
SERVERLESS_SUPPORTED_SPARK_CONFIGS = frozenset(
    {
        "spark.sql.ansi.enabled",  # ANSI_MODE
        "spark.sql.legacy.timeParserPolicy",  # LEGACY_TIME_PARSER_POLICY
        "spark.sql.files.maxPartitionBytes",  # MAX_FILE_PARTITION_BYTES
        "spark.sql.session.timeZone",  # TIMEZONE
        "spark.databricks.execution.timeout",  # STATEMENT_TIMEOUT
        "spark.databricks.io.cache.enabled",  # USE_CACHED_RESULT
        "spark.databricks.sql.readOnlyExternalMetastore",  # READ_ONLY_EXTERNAL_METASTORE
    }
)
