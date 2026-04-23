"""Action card and SQL improvement example generation."""

import re

from ..constants import SERVERLESS_SUPPORTED_SPARK_CONFIGS, THRESHOLDS
from ..i18n import gettext as _
from ..models import (
    ActionCard,
    BottleneckIndicators,
    JoinInfo,
    OperatorHotspot,
    QueryMetrics,
    ShuffleMetrics,
    SQLAnalysis,
    SQLImprovementExample,
    TableScanMetrics,
)
from .operators import _update_top_scanned_with_clustering


def normalize_table_ref(table_ref: str) -> str:
    """Normalize a table identifier for matching across name styles.

    SQL analysis may return aliases / partially-qualified names; scan
    nodes typically use fully-qualified names. Strip whitespace,
    lowercase, and remove backticks/double-quotes to compare safely.
    """
    if not table_ref:
        return ""
    t = table_ref.strip().lower()
    return t.replace("`", "").replace('"', "")


def _is_notable_shuffle_for_lc(sm: ShuffleMetrics) -> bool:
    """Gate: shuffle is a candidate to consider for Liquid Clustering.

    Mirrors ``_is_notable_shuffle`` in llm_prompts/prompts.py — a GiB-scale
    write OR memory-inefficient shuffle. Trivial shuffles don't move the
    needle even when clustered.
    """
    written = getattr(sm, "sink_bytes_written", 0) or 0
    mpp_mb = getattr(sm, "memory_per_partition_mb", 0) or 0
    if written >= THRESHOLDS["shuffle_high_volume_bytes"]:
        return True
    if mpp_mb >= THRESHOLDS["memory_per_partition_mb"]:
        return True
    return False


def _strip_alias(col: str) -> str:
    """``ce.lineitem_usagetype`` → ``lineitem_usagetype``."""
    return col.split(".")[-1].strip() if col else ""


def _shuffle_keys_on_scanned_table(
    shuffle_metrics: list[ShuffleMetrics],
    sql_analysis: SQLAnalysis | None,
    top_scanned_tables: list[TableScanMetrics] | None,
    lc_target_table_norm: str,
) -> list[tuple[str, ShuffleMetrics]]:
    """Return ``[(column_name, shuffle)]`` pairs whose key belongs to a
    scanned table (preferably the LC target).

    Matching strategy: the shuffle attribute usually appears as
    ``<alias>.<column>``. We look for a ColumnReference in sql_analysis
    whose column matches (case-insensitive) and whose table alias/name
    resolves to one of the scanned tables.
    """
    if not shuffle_metrics or not top_scanned_tables:
        return []
    scanned_table_names = {(t.table_name or "").lower() for t in top_scanned_tables}
    scanned_table_shorts = {n.split(".")[-1] for n in scanned_table_names if n}

    alias_to_table: dict[str, str] = {}
    columns_by_name: dict[str, list] = {}
    if sql_analysis and sql_analysis.columns:
        for c in sql_analysis.columns:
            if c.column_name:
                columns_by_name.setdefault(c.column_name.lower(), []).append(c)
        for t in sql_analysis.tables or []:
            if t.alias:
                alias_to_table[t.alias.lower()] = (t.full_name or t.table or "").lower()

    pairs: list[tuple[str, ShuffleMetrics]] = []
    seen: set[tuple[str, str]] = set()
    for sm in shuffle_metrics:
        if not _is_notable_shuffle_for_lc(sm):
            continue
        for attr in sm.shuffle_attributes or []:
            if not attr:
                continue
            # Split alias.column if present
            parts = attr.split(".")
            col_name = parts[-1].strip()
            col_alias = parts[0].strip() if len(parts) >= 2 else ""
            col_key = col_name.lower()

            # Resolve alias to table name.
            # Conservative rule (v5.16.10): if we can't pin the shuffle
            # attribute down to exactly one table, drop it rather than
            # pick the first candidate — otherwise a bare column name
            # that happens to exist in multiple scanned tables gets
            # mis-attributed and the LC recommendation becomes wrong.
            resolved_table = ""
            if col_alias:
                resolved_table = alias_to_table.get(col_alias.lower(), "")
            if not resolved_table and col_key in columns_by_name:
                candidate_tables: set[str] = set()
                for cr in columns_by_name[col_key]:
                    tn = (cr.table_name or "").lower()
                    if not tn and cr.table_alias:
                        tn = alias_to_table.get(cr.table_alias.lower(), "")
                    if tn:
                        candidate_tables.add(tn)
                if len(candidate_tables) == 1:
                    resolved_table = next(iter(candidate_tables))
                # len == 0 or >= 2 → ambiguous, skip.

            if not resolved_table:
                continue
            # Accept if the resolved table matches any scanned table by
            # full name or short-name suffix.
            resolved_short = resolved_table.split(".")[-1]
            match = (
                resolved_table in scanned_table_names
                or resolved_short in scanned_table_shorts
                or any(resolved_table.endswith(f".{s}") for s in scanned_table_shorts)
            )
            if not match:
                continue
            key = (col_name.lower(), sm.node_id or "")
            if key in seen:
                continue
            seen.add(key)
            pairs.append((col_name, sm))
    # Sort by shuffle peak memory descending so the largest lands first.
    pairs.sort(key=lambda p: p[1].peak_memory_bytes or 0, reverse=True)
    return pairs


def _has_notable_shuffle_on_scanned_table(
    shuffle_metrics: list[ShuffleMetrics],
    sql_analysis: SQLAnalysis | None,
    top_scanned_tables: list[TableScanMetrics] | None,
    lc_target_table_norm: str,
) -> bool:
    return bool(
        _shuffle_keys_on_scanned_table(
            shuffle_metrics, sql_analysis, top_scanned_tables, lc_target_table_norm
        )
    )


def _filter_fix_sql_for_serverless(fix_sql: str) -> str:
    """Remove non-serverless SET spark.* lines from fix_sql.

    Keeps lines whose config key is in SERVERLESS_SUPPORTED_SPARK_CONFIGS.
    Non-SET lines (comments, SQL, blank lines) are always preserved.
    """
    if not fix_sql:
        return fix_sql
    result_lines: list[str] = []
    set_pattern = re.compile(r"^\s*SET\s+(spark\.\S+)\s*=", re.IGNORECASE)
    for line in fix_sql.split("\n"):
        m = set_pattern.match(line)
        if m:
            config_key = m.group(1).rstrip(";")
            if config_key in SERVERLESS_SUPPORTED_SPARK_CONFIGS:
                result_lines.append(line)
            # else: skip non-supported config line
        else:
            result_lines.append(line)
    # Clean up consecutive blank lines left after removal
    cleaned: list[str] = []
    for line in result_lines:
        if line.strip() == "" and cleaned and cleaned[-1].strip() == "":
            continue
        cleaned.append(line)
    return "\n".join(cleaned).strip()


def generate_action_cards(
    indicators: BottleneckIndicators,
    hot_operators: list[OperatorHotspot],
    query_metrics: QueryMetrics,
    shuffle_metrics: list[ShuffleMetrics],
    join_info: list[JoinInfo],
    sql_analysis: SQLAnalysis | None = None,
    top_scanned_tables: list[TableScanMetrics] | None = None,
    llm_clustering_config: dict | None = None,
    is_serverless: bool = False,
    explain_analysis=None,
) -> list[ActionCard]:
    """Generate prioritized action cards based on analysis results.

    Args:
        indicators: Bottleneck indicators
        hot_operators: List of hot operators
        query_metrics: Query metrics
        shuffle_metrics: Shuffle metrics
        join_info: Join information
        sql_analysis: SQL analysis for concrete table/column names
        top_scanned_tables: Top scanned tables by bytes_read for LC recommendations
        llm_clustering_config: Optional LLM configuration for clustering recommendation
            {
                "model": str,  # e.g., "databricks-claude-opus-4-6"
                "databricks_host": str,
                "databricks_token": str,
                "lang": str  # "en" or "ja"
            }

    Returns:
        List of ActionCard sorted by priority score descending
    """
    # Spark Perf-style registry is invoked AFTER the filter_columns /
    # llm_clustering_result precomputation below, so low_file_pruning
    # and other cards that depend on that state can see it via Context.
    cards: list[ActionCard] = []

    # Extract table names for concrete SQL examples
    table_names = []
    if sql_analysis and sql_analysis.tables:
        table_names = [
            t.full_name or t.table for t in sql_analysis.tables if t.full_name or t.table
        ]
    primary_table = table_names[0] if table_names else "<table_name>"

    # Determine the best table for Liquid Clustering recommendations
    # Priority: top scanned table by bytes_read > primary_table
    lc_target_table = primary_table
    if top_scanned_tables:
        # Use the table with highest bytes_read (most I/O intensive)
        lc_target_table = top_scanned_tables[0].table_name

    _normalize_table_ref = normalize_table_ref  # Backwards-compat alias for existing closures
    lc_target_table_norm = _normalize_table_ref(lc_target_table)
    lc_target_table_short = lc_target_table_norm.split(".")[-1] if lc_target_table_norm else ""

    # Build a set of known table aliases from sql_analysis.tables
    known_aliases: dict[str, str] = {}  # alias -> full_name
    if sql_analysis and sql_analysis.tables:
        for t in sql_analysis.tables:
            if t.alias:
                alias_norm = _normalize_table_ref(t.alias)
                full_name = _normalize_table_ref(t.full_name or t.table or "")
                known_aliases[alias_norm] = full_name

    def _table_matches_target(col_table: str) -> bool:
        """Check if a column's table reference matches the LC target table."""
        if not col_table:
            return False

        col_table_norm = _normalize_table_ref(col_table)

        # Direct match
        if col_table_norm == lc_target_table_norm:
            return True

        # Short name match (e.g., "qtz_member" matches "prd_delta.qtz_s3_etl.qtz_member")
        if lc_target_table_short:
            if col_table_norm == lc_target_table_short:
                return True
            if col_table_norm.endswith(f".{lc_target_table_short}"):
                return True
            if lc_target_table_norm.endswith(f".{col_table_norm}"):
                return True

        # Alias resolution (e.g., "m" -> "prd_delta.qtz_s3_etl.qtz_member")
        if col_table_norm in known_aliases:
            resolved = known_aliases[col_table_norm]
            if resolved == lc_target_table_norm:
                return True
            if lc_target_table_short and resolved.endswith(f".{lc_target_table_short}"):
                return True
            if lc_target_table_short and lc_target_table_norm.endswith(
                f".{resolved.split('.')[-1]}"
            ):
                return True

        return False

    # Extract frequently used columns for clustering key candidates
    # Filter to only include columns that belong to the LC target table
    filter_columns = []
    candidate_columns_with_context = []  # For LLM input

    if sql_analysis and sql_analysis.columns:
        for c in sql_analysis.columns:
            if c.context not in ("where", "join", "group_by", "order_by") or not c.column_name:
                continue

            # Collect for LLM (include all contexts for better analysis)
            candidate_columns_with_context.append(
                {
                    "column": c.column_name,
                    "context": c.context,
                    "operator": c.operator or "-",
                    "table": c.table_name or c.table_alias or "",
                }
            )

            # For heuristic fallback, only use where/join
            if c.context not in ("where", "join"):
                continue

            # Check if column belongs to the LC target table
            col_table = c.table_name or c.table_alias or ""
            col_name = c.column_name.lower()

            # Match by table name/alias
            if lc_target_table_short:
                matched = False
                if _table_matches_target(col_table):
                    filter_columns.append(c.column_name)
                    matched = True
                # Column prefix match (TPC-DS style: ss_ for store_sales, etc.)
                elif not col_table and "_" in col_name:
                    prefix = col_name.split("_")[0]
                    table_prefix = (
                        lc_target_table_short[:2] if len(lc_target_table_short) >= 2 else ""
                    )
                    if prefix == table_prefix:
                        filter_columns.append(c.column_name)
                        matched = True
                # If no table info but only one table in query, assume it belongs to target
                if (
                    not matched
                    and not col_table
                    and sql_analysis.tables
                    and len(sql_analysis.tables) == 1
                ):
                    filter_columns.append(c.column_name)
            else:
                # No target table info, include all WHERE/JOIN columns
                filter_columns.append(c.column_name)

        # Remove duplicates while preserving order, limit to 4
        seen = set()
        unique_cols = []
        for col in filter_columns:
            if col not in seen:
                seen.add(col)
                unique_cols.append(col)
        filter_columns = unique_cols[:4]

    # Detect notable shuffles whose partitioning key belongs to a scanned
    # table — these should also trigger the LC LLM even when the usual
    # filter-rate / candidate-column gate does not. Without this hook
    # shuffle-dominant / compute-bound queries never reach the LC LLM
    # and the shuffle key is silently ignored as an LC candidate.
    has_notable_shuffle_on_scanned_table = _has_notable_shuffle_on_scanned_table(
        shuffle_metrics, sql_analysis, top_scanned_tables, lc_target_table_norm
    )

    # Try LLM-based clustering recommendation if configured and either
    #   (a) filter_rate is low with candidate columns (classic scan-bound trigger), OR
    #   (b) a notable shuffle partitioning key belongs to a scanned table.
    llm_clustering_result = None
    if llm_clustering_config and (
        (
            indicators.filter_rate < 0.3
            and (len(candidate_columns_with_context) >= 2 or len(filter_columns) >= 1)
        )
        or has_notable_shuffle_on_scanned_table
    ):
        from ..llm import recommend_clustering_with_llm

        # Prepare top_scanned_tables data for LLM
        top_tables_data = []
        if top_scanned_tables:
            for ts in top_scanned_tables[:5]:
                entry = {
                    "table_name": ts.table_name,
                    "bytes_read": ts.bytes_read,
                }
                if ts.current_clustering_keys:
                    entry["current_clustering_keys"] = ts.current_clustering_keys
                if ts.clustering_key_cardinality:
                    entry["clustering_key_cardinality"] = ts.clustering_key_cardinality
                top_tables_data.append(entry)

        llm_clustering_result = recommend_clustering_with_llm(
            query_sql=sql_analysis.raw_sql if sql_analysis else "",
            target_table=lc_target_table,
            candidate_columns=candidate_columns_with_context,
            top_scanned_tables=top_tables_data,
            filter_rate=indicators.filter_rate,
            read_files_count=query_metrics.read_files_count,
            pruned_files_count=query_metrics.pruned_files_count,
            model=llm_clustering_config.get("model", "databricks-claude-opus-4-6"),
            databricks_host=llm_clustering_config.get("databricks_host", ""),
            databricks_token=llm_clustering_config.get("databricks_token", ""),
            lang=llm_clustering_config.get("lang"),
            shuffle_metrics=shuffle_metrics,
        )

        # Use LLM result if valid
        if llm_clustering_result and llm_clustering_result.get("recommended_keys"):
            filter_columns = llm_clustering_result["recommended_keys"]
            if llm_clustering_result.get("target_table"):
                lc_target_table = llm_clustering_result["target_table"]

            # Update top_scanned_tables with LLM recommendation
            if top_scanned_tables and filter_columns:
                _update_top_scanned_with_clustering(
                    top_scanned_tables, lc_target_table, filter_columns
                )

    # If no LLM was used but we have heuristic filter_columns, update top_scanned_tables
    if (
        not llm_clustering_result
        and filter_columns
        and top_scanned_tables
        and filter_columns[0] != "<filter_columns>"
    ):
        _update_top_scanned_with_clustering(top_scanned_tables, lc_target_table, filter_columns)

    # Invoke the Spark Perf-style registry NOW — after filter_columns /
    # llm_clustering_result / lc_target_table are fully computed. The
    # Context is populated with those precomputed values so cards like
    # low_file_pruning can render their SQL template directly.
    from .recommendations_registry import (
        Context as _RegistryContext,
    )
    from .recommendations_registry import (
        generate_from_registry,
    )

    _registry_ctx = _RegistryContext(
        indicators=indicators,
        query_metrics=query_metrics,
        hot_operators=hot_operators or [],
        shuffle_metrics=shuffle_metrics or [],
        join_info=join_info or [],
        sql_analysis=sql_analysis,
        top_scanned_tables=top_scanned_tables,
        llm_clustering_config=llm_clustering_config,
        is_serverless=is_serverless,
        filter_columns=list(filter_columns),
        llm_clustering_result=llm_clustering_result,
        lc_target_table_override=(
            lc_target_table
            if llm_clustering_result and llm_clustering_result.get("target_table")
            else None
        ),
        explain_analysis=explain_analysis,
    )
    # The fired-ids set is ignored now that Phase 3 has removed the
    # legacy `if <id> not in _skip_legacy` dispatch — the registry is
    # the single source of truth for which cards emit.
    _registry_cards, _ = generate_from_registry(_registry_ctx)
    cards.extend(_registry_cards)

    # Sort by priority score descending
    cards.sort(key=lambda x: x.priority_score, reverse=True)

    return cards


def generate_sql_improvement_examples(
    indicators: BottleneckIndicators,
    hot_operators: list[OperatorHotspot],
    shuffle_metrics: list[ShuffleMetrics],
    join_info: list[JoinInfo],
    sql_analysis: SQLAnalysis | None = None,
) -> list[SQLImprovementExample]:
    """Generate SQL improvement examples based on detected issues.

    Args:
        indicators: Bottleneck indicators
        hot_operators: List of hot operators
        shuffle_metrics: Shuffle metrics
        join_info: Join information
        sql_analysis: SQL analysis for concrete examples

    Returns:
        List of SQLImprovementExample with before/after patterns
    """
    examples: list[SQLImprovementExample] = []

    # Extract table names
    table_names = []
    if sql_analysis and sql_analysis.tables:
        table_names = [
            t.full_name or t.table for t in sql_analysis.tables if t.full_name or t.table
        ]
    primary_table = table_names[0] if table_names else "your_table"

    # Extract columns
    select_columns = []
    filter_columns = []
    if sql_analysis and sql_analysis.columns:
        select_columns = list(
            {c.column_name for c in sql_analysis.columns if c.context == "select" and c.column_name}
        )[:5]
        filter_columns = list(
            {
                c.column_name
                for c in sql_analysis.columns
                if c.context in ("where", "join") and c.column_name
            }
        )[:4]

    # 1. SELECT * → Column Pruning Example
    scan_heavy = any(
        op.bottleneck_type == "scan" and op.time_share_percent >= 20 for op in hot_operators
    )
    if scan_heavy:
        cols_str = ", ".join(select_columns) if select_columns else "id, name, created_at"
        examples.append(
            SQLImprovementExample(
                issue_type="scan",
                title=_("Column Pruning"),
                description=_(
                    "SELECT * reads all columns from disk. Specifying only needed columns reduces I/O significantly."
                ),
                before_sql=f"SELECT * FROM {primary_table} WHERE ...;",
                after_sql=f"SELECT {cols_str}\nFROM {primary_table}\nWHERE ...;",
                impact="high",
                applies_to=table_names[:3],
            )
        )

    # 2. Shuffle Heavy → BROADCAST Hint Example
    if indicators.shuffle_impact_ratio > 0.2:
        small_table = table_names[1] if len(table_names) > 1 else "small_table"
        examples.append(
            SQLImprovementExample(
                issue_type="shuffle",
                title=_("BROADCAST Join Hint"),
                description=_(
                    "When joining with a small table (<200MB), BROADCAST hint eliminates shuffle by sending the small table to all executors."
                ),
                before_sql=f"SELECT *\nFROM {primary_table} t1\nJOIN {small_table} t2\nON t1.key = t2.key;",
                after_sql=f"SELECT /*+ BROADCAST({small_table}) */ *\nFROM {primary_table} t1\nJOIN {small_table} t2\nON t1.key = t2.key;",
                impact="high",
                applies_to=[small_table],
            )
        )

    # 3. Data Skew → CTE Pre-aggregation Example
    if indicators.has_data_skew:
        skew_keys = []
        for sm in shuffle_metrics:
            if sm.aqe_skewed_partitions > 0 and sm.shuffle_attributes:
                skew_keys.extend(sm.shuffle_attributes)
        skew_key = skew_keys[0] if skew_keys else "skew_key"

        examples.append(
            SQLImprovementExample(
                issue_type="shuffle",
                title=_("CTE Pre-aggregation for Skewed Keys"),
                description=_(
                    "When a join key has highly skewed values (few values have many rows), pre-aggregating in a CTE reduces data volume before JOIN and mitigates skew impact."
                ),
                before_sql=f"SELECT *\nFROM table_a a\nJOIN table_b b\nON a.{skew_key} = b.{skew_key};",
                after_sql=f"""-- Pre-aggregate in CTE to reduce data volume before JOIN
WITH pre_agg AS (
  SELECT {skew_key}, COUNT(*) AS cnt, SUM(amount) AS total
  FROM table_a
  GROUP BY {skew_key}
)
SELECT *
FROM pre_agg a
JOIN table_b b
ON a.{skew_key} = b.{skew_key};""",
                impact="high",
                applies_to=table_names[:2],
            )
        )

    # 4. Spill → Pre-aggregation/Pre-filter Example
    if indicators.spill_bytes > 1024**3:  # > 1GB spill
        examples.append(
            SQLImprovementExample(
                issue_type="spill",
                title=_("Pre-filter Data Before Heavy Operations"),
                description=_(
                    "Spill occurs when data exceeds memory. Pre-filtering reduces data volume before expensive operations like joins and aggregations."
                ),
                before_sql="""SELECT dim.*, SUM(fact.amount)
FROM large_fact_table fact
JOIN dimension_table dim ON ...
WHERE fact.date >= '2024-01-01'
GROUP BY ...;""",
                after_sql="""-- Filter early to reduce data volume
WITH filtered_facts AS (
  SELECT *
  FROM large_fact_table
  WHERE date >= '2024-01-01'  -- Push filter early
)
SELECT dim.*, SUM(f.amount)
FROM filtered_facts f
JOIN dimension_table dim ON ...
GROUP BY ...;""",
                impact="high",
                applies_to=table_names[:1],
            )
        )

    # 5. Low Pruning → Partition Filter Example
    if indicators.filter_rate < 0.3:
        filter_col = filter_columns[0] if filter_columns else "date"
        examples.append(
            SQLImprovementExample(
                issue_type="filter",
                title=_("Add Partition Filter"),
                description=_(
                    "Queries without partition filters scan all files. Adding a filter on the partition column dramatically reduces I/O."
                ),
                before_sql=f"SELECT * FROM {primary_table}\nWHERE status = 'active';",
                after_sql=f"SELECT * FROM {primary_table}\nWHERE {filter_col} >= '2024-01-01'  -- Partition filter\n  AND status = 'active';",
                impact="medium",
                applies_to=table_names[:1],
            )
        )

    # 6. Liquid Clustering Example (for low bytes pruning)
    if indicators.bytes_pruning_ratio < 0.3:
        cols_str = ", ".join(filter_columns) if filter_columns else "frequently_filtered_col"
        examples.append(
            SQLImprovementExample(
                issue_type="filter",
                title=_("Apply Liquid Clustering"),
                description=_(
                    "Liquid Clustering co-locates related data, improving file pruning. Use Z-Order only if Liquid Clustering is unavailable."
                ),
                before_sql="-- No optimization applied",
                after_sql=f"""-- Recommended: Liquid Clustering (auto-maintained)
ALTER TABLE {primary_table}
CLUSTER BY ({cols_str});

-- Fallback: Z-Order (only if Liquid Clustering is unavailable)
-- OPTIMIZE {primary_table}
-- ZORDER BY ({cols_str});""",
                impact="medium",
                applies_to=table_names[:1],
            )
        )

    return examples
