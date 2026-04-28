"""
Functions for extracting metrics from query profile data.
"""

import json
import logging
import re
from collections import defaultdict, deque
from datetime import datetime
from typing import Any

from .constants import JoinType
from .models import (
    DataFlowDAG,
    DataFlowEdge,
    DataFlowEntry,
    JoinInfo,
    NodeMetrics,
    QueryMetrics,
    ShuffleMetrics,
    SQLAnalysis,
    StageInfo,
    StreamingContext,
    TableScanMetrics,
    TargetTableInfo,
)
from .sql_analyzer import analyze_sql

logger = logging.getLogger(__name__)


# --- Profile Format Detection and Normalization ---


def is_spark_connect_profile(data: dict[str, Any]) -> bool:
    """Detect if the profile is from Spark Connect.

    Spark Connect profiles have entryPoint="SPARK_CONNECT" in
    query.internalQuerySource.entryPoint.

    Args:
        data: Raw profile data (nested or flat structure)

    Returns:
        True if this is a Spark Connect profile
    """
    # Handle both nested {"query": {...}} and flat structures
    query = data.get("query") if isinstance(data.get("query"), dict) else data
    if not isinstance(query, dict):
        return False

    internal_source = query.get("internalQuerySource") or {}
    entry_point = internal_source.get("entryPoint")
    return entry_point == "SPARK_CONNECT"


def is_streaming_profile(data: dict[str, Any]) -> bool:
    """Detect if the profile is a streaming DLT/SDP profile.

    Streaming profiles have query.queryMetadata.isStreaming == True.

    Args:
        data: Raw profile data (nested or flat structure)

    Returns:
        True if this is a streaming profile
    """
    query = data.get("query") if isinstance(data.get("query"), dict) else data
    if not isinstance(query, dict):
        return False
    query_metadata = query.get("queryMetadata") or {}
    return bool(query_metadata.get("isStreaming"))


def extract_streaming_context(data: dict[str, Any]) -> "StreamingContext | None":
    """Extract streaming metadata and micro-batch metrics from profile data.

    Returns None if this is not a streaming profile.
    """
    if not is_streaming_profile(data):
        return None

    from .models import MicroBatchMetrics, StreamingContext

    query = data.get("query", {})
    query_metadata = query.get("queryMetadata") or {}
    internal_source = query.get("internalQuerySource") or {}

    plan_metadatas = data.get("planMetadatas", [])
    batches = []
    for pm in plan_metadatas:
        metrics = pm.get("metrics") or {}
        batches.append(
            MicroBatchMetrics(
                plan_id=pm.get("id", ""),
                status=pm.get("statusId", ""),
                duration_ms=_safe_int(pm.get("durationMs")),
                read_bytes=_safe_int(metrics.get("readBytes")),
                rows_read_count=_safe_int(metrics.get("rowsReadCount")),
                total_time_ms=_safe_int(metrics.get("totalTimeMs")),
                query_start_time_ms=_safe_int(pm.get("queryStartTimeMs")),
            )
        )

    return StreamingContext(
        is_streaming=True,
        target_table=query_metadata.get("writeDataset") or "",
        entry_point=internal_source.get("entryPoint") or "",
        statement_type=query.get("statementType") or "",
        is_final=bool(query.get("isFinal", False)),
        active_plan_id=data.get("activePlanId") or "",
        batch_count=len(batches),
        batches=batches,
    )


def compute_batch_statistics(ctx: StreamingContext) -> dict[str, Any]:
    """Compute statistical summary of streaming micro-batch metrics.

    Args:
        ctx: StreamingContext with batches list

    Returns:
        Dict with batch_count, finished_count, running_count,
        duration_min/max/avg/p95_ms, read_bytes_min/max/avg,
        rows_min/max/avg, slow_batches, duration_cv
    """
    import statistics as _stats

    batches = ctx.batches
    finished = [b for b in batches if b.status == "FINISHED"]
    running = [b for b in batches if b.status == "RUNNING"]

    if not finished:
        return {
            "batch_count": len(batches),
            "finished_count": 0,
            "running_count": len(running),
            "duration_min_ms": 0,
            "duration_max_ms": 0,
            "duration_avg_ms": 0,
            "duration_p95_ms": 0,
            "read_bytes_min": 0,
            "read_bytes_max": 0,
            "read_bytes_avg": 0,
            "rows_min": 0,
            "rows_max": 0,
            "rows_avg": 0,
            "slow_batches": [],
            "duration_cv": 0.0,
        }

    durations = [b.duration_ms for b in finished]
    read_bytes = [b.read_bytes for b in finished]
    rows = [b.rows_read_count for b in finished]

    avg_dur = _stats.mean(durations)
    stdev_dur = _stats.stdev(durations) if len(durations) >= 2 else 0.0

    # p95 approximation
    sorted_durs = sorted(durations)
    p95_idx = int(len(sorted_durs) * 0.95)
    p95_idx = min(p95_idx, len(sorted_durs) - 1)

    # Detect slow batches (> 2x average)
    slow = []
    if avg_dur > 0 and len(finished) >= 2:
        for b in finished:
            if b.duration_ms > avg_dur * 2:
                slow.append(
                    {
                        "plan_id": b.plan_id,
                        "duration_ms": b.duration_ms,
                        "read_bytes": b.read_bytes,
                        "rows_read_count": b.rows_read_count,
                    }
                )

    return {
        "batch_count": len(batches),
        "finished_count": len(finished),
        "running_count": len(running),
        "duration_min_ms": min(durations),
        "duration_max_ms": max(durations),
        "duration_avg_ms": avg_dur,
        "duration_p95_ms": sorted_durs[p95_idx],
        "read_bytes_min": min(read_bytes),
        "read_bytes_max": max(read_bytes),
        "read_bytes_avg": _stats.mean(read_bytes),
        "rows_min": min(rows),
        "rows_max": max(rows),
        "rows_avg": _stats.mean(rows),
        "slow_batches": slow,
        "duration_cv": (stdev_dur / avg_dur) if avg_dur > 0 else 0.0,
    }


def normalize_profile_data(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize profile data to a common schema for extractors.

    Handles different profile formats:
    1. Standard DBSQL profile (nested or flat)
    2. Spark Connect profile (entryPoint="SPARK_CONNECT")

    Args:
        data: Raw profile data from JSON

    Returns:
        Normalized profile data that extractors can process
    """
    # 1) Ensure nested structure: {"query": {...}, "graphs": [...]}
    if "query" not in data and any(k in data for k in ("id", "metrics", "queryText")):
        data = {"query": data, "graphs": data.get("graphs", [])}

    # 2) Apply Spark Connect specific normalizations if needed
    if is_spark_connect_profile(data):
        data = _normalize_spark_connect_profile(data)

    return data


def _normalize_spark_connect_profile(data: dict[str, Any]) -> dict[str, Any]:
    """Apply Spark Connect specific normalizations.

    Currently Spark Connect profiles have the same structure for graphs/metrics,
    but some metadata fields differ. This function handles those differences.

    Args:
        data: Profile data identified as Spark Connect

    Returns:
        Normalized profile data
    """
    # For now, Spark Connect profiles work with existing extractors.
    # Add field mappings here as differences are discovered.

    # Example future normalizations:
    # - Map different field names to expected names
    # - Handle missing fields with defaults
    # - Convert different data formats

    logger.debug("Processing Spark Connect profile")
    return data


def extract_endpoint_id(data: dict[str, Any]) -> str:
    """Extract the SQL Warehouse endpoint ID from profile data.

    Args:
        data: Raw profile data

    Returns:
        Endpoint ID string, or empty string if not found
    """
    # Handle both nested and flat structures
    query = data.get("query") if isinstance(data.get("query"), dict) else data
    if not isinstance(query, dict):
        return ""
    return query.get("endpointId", "") or ""


def _safe_int(value: Any, default: int = 0) -> int:
    """Safely convert a value to int, handling str/float/None.

    Args:
        value: Value to convert (can be int, float, str, or None)
        default: Default value if conversion fails

    Returns:
        Integer value or default
    """
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            # Handle both "123" and "123.0" formats
            return int(float(value))
        except (ValueError, TypeError):
            return default
    return default


def _parse_graph_safely(graph: Any, graph_index: int) -> dict | None:
    """Safely parse a graph object, handling JSON strings and invalid data.

    Args:
        graph: Graph data (dict or JSON string)
        graph_index: Index of the graph for logging

    Returns:
        Parsed graph dict, or None if parsing failed
    """
    if isinstance(graph, dict):
        return graph

    if isinstance(graph, str):
        try:
            parsed: dict[Any, Any] = json.loads(graph)
            return parsed
        except json.JSONDecodeError as e:
            logger.warning(
                f"Failed to parse graph[{graph_index}] as JSON: {e}. Skipping this graph."
            )
            return None

    logger.warning(
        f"Unexpected graph[{graph_index}] type: {type(graph).__name__}. Skipping this graph."
    )
    return None


def _extract_planning_phases(raw: Any) -> list[dict[str, Any]]:
    """Normalize planningPhases from profile metrics.

    Input format: [{"__typename": "PlanningPhase", "phase": "ANALYSIS", "durationMs": 2}, ...]
    Output format: [{"phase": "ANALYSIS", "duration_ms": 2}, ...]
    """
    if not isinstance(raw, list):
        return []
    result = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        phase = item.get("phase", "")
        duration_ms = _safe_int(item.get("durationMs"))
        if phase:
            result.append({"phase": phase, "duration_ms": duration_ms})
    return result


def extract_query_metrics(data: dict[str, Any]) -> QueryMetrics:
    """Extract high-level query metrics from profile data.

    Handles two JSON structures:
    1. Nested: {"query": {"id": ..., "metrics": {...}}}
    2. Flat: {"id": ..., "metrics": {...}}

    Unmapped metrics keys are stored in extra_metrics for future extensibility.
    """
    # Handle both nested and flat structures
    if "query" in data:
        query = data["query"]
        metrics = query.get("metrics", {})
    else:
        # Flat structure - data is already the query object
        query = data
        metrics = data.get("metrics", {})

    # Keys that are explicitly mapped to QueryMetrics fields
    mapped_keys = {
        "__typename",  # GraphQL type, not a metric
        "totalTimeMs",
        "compilationTimeMs",
        "executionTimeMs",
        "readBytes",
        "readRemoteBytes",
        "readCacheBytes",
        "spillToDiskBytes",
        "photonTotalTimeMs",
        "taskTotalTimeMs",
        "readFilesCount",
        "prunedFilesCount",
        "prunedBytes",
        "rowsReadCount",
        "rowsProducedCount",
        "bytesReadFromCachePercentage",
        "writeRemoteBytes",
        "writeRemoteFiles",
        "writeRemoteRows",
        "networkSentBytes",
        "readPartitionsCount",
        "resultFromCache",
        "resultFetchTimeMs",
        "queuedProvisioningTimeMs",
        "queuedOverloadTimeMs",
        "provisioningQueueStartTimestamp",
        "overloadingQueueStartTimestamp",
        "queryCompilationStartTimestamp",
        "metadataTimeMs",
        "planningPhases",
    }

    # Collect unmapped metrics into extra_metrics
    extra_metrics: dict[str, Any] = {}
    for key, value in metrics.items():
        if key not in mapped_keys and value is not None:
            extra_metrics[key] = value

    # __typename is at query level (not inside metrics)
    query_typename = query.get("__typename", "") or metrics.get("__typename", "")

    # Plan-structure counts derived from the graph. Consumed by the
    # cluster_underutilization detector to classify the idle variant.
    graphs = data.get("graphs") or []
    all_graph_nodes: list[dict[str, Any]] = []
    for g in graphs:
        if isinstance(g, dict):
            nodes = g.get("nodes") or []
            all_graph_nodes.extend(n for n in nodes if isinstance(n, dict))

    def _tag(n: dict[str, Any]) -> str:
        t = n.get("tag", "")
        return t if isinstance(t, str) else ""

    aqe_replan_count = sum(
        1 for n in all_graph_nodes if _tag(n) == "UNKNOWN_ADAPTIVE_SPARK_PLAN_EXEC"
    )
    subquery_count = sum(
        1
        for n in all_graph_nodes
        if _tag(n) in ("SUBQUERY_EXEC", "UNKNOWN_SPARK_PLAN.PhotonMetadataSubquery")
    )
    broadcast_hash_join_count = sum(
        1 for n in all_graph_nodes if _tag(n) == "PHOTON_BROADCAST_HASH_JOIN_EXEC"
    )
    total_plan_node_count = len(all_graph_nodes)

    return QueryMetrics(
        query_id=query.get("id", ""),
        status=query.get("status", ""),
        query_text=query.get("queryText", ""),
        query_typename=query_typename,
        total_time_ms=_safe_int(metrics.get("totalTimeMs")),
        compilation_time_ms=_safe_int(metrics.get("compilationTimeMs")),
        execution_time_ms=_safe_int(metrics.get("executionTimeMs")),
        read_bytes=_safe_int(metrics.get("readBytes")),
        read_remote_bytes=_safe_int(metrics.get("readRemoteBytes")),
        read_cache_bytes=_safe_int(metrics.get("readCacheBytes")),
        spill_to_disk_bytes=_safe_int(metrics.get("spillToDiskBytes")),
        photon_total_time_ms=_safe_int(metrics.get("photonTotalTimeMs")),
        task_total_time_ms=_safe_int(metrics.get("taskTotalTimeMs")),
        read_files_count=_safe_int(metrics.get("readFilesCount")),
        pruned_files_count=_safe_int(metrics.get("prunedFilesCount")),
        pruned_bytes=_safe_int(metrics.get("prunedBytes")),
        rows_read_count=_safe_int(metrics.get("rowsReadCount")),
        rows_produced_count=_safe_int(metrics.get("rowsProducedCount")),
        bytes_read_from_cache_percentage=_safe_int(metrics.get("bytesReadFromCachePercentage")),
        write_remote_bytes=_safe_int(metrics.get("writeRemoteBytes")),
        write_remote_files=_safe_int(metrics.get("writeRemoteFiles")),
        write_remote_rows=_safe_int(metrics.get("writeRemoteRows")),
        network_sent_bytes=_safe_int(metrics.get("networkSentBytes")),
        read_partitions_count=_safe_int(metrics.get("readPartitionsCount")),
        result_from_cache=bool(metrics.get("resultFromCache", False)),
        result_fetch_time_ms=_safe_int(metrics.get("resultFetchTimeMs")),
        queued_provisioning_time_ms=_safe_int(metrics.get("queuedProvisioningTimeMs")),
        queued_overload_time_ms=_safe_int(metrics.get("queuedOverloadTimeMs")),
        query_start_time_ms=_safe_int(query.get("queryStartTimeMs")),
        provisioning_queue_start_ts=_safe_int(metrics.get("provisioningQueueStartTimestamp")),
        overloading_queue_start_ts=_safe_int(metrics.get("overloadingQueueStartTimestamp")),
        query_compilation_start_ts=_safe_int(metrics.get("queryCompilationStartTimestamp")),
        metadata_time_ms=_safe_int(metrics.get("metadataTimeMs")),
        aqe_replan_count=aqe_replan_count,
        subquery_count=subquery_count,
        broadcast_hash_join_count=broadcast_hash_join_count,
        total_plan_node_count=total_plan_node_count,
        planning_phases=_extract_planning_phases(metrics.get("planningPhases")),
        extra_metrics=extra_metrics,
    )


_FEDERATION_TABLE_PREFIX = "Row Data Source Scan "


def _guess_federation_source_type(table_ref: str) -> str:
    """Best-effort source-type heuristic from a fully-qualified table name.

    Profile JSON does not carry the connection provider directly, so
    we fall back to naming conventions. Returns one of
    ``"bigquery" | "snowflake" | "mysql" | "postgresql" | "redshift"``
    or ``""`` when no signal is present — callers treat empty as
    "Lakehouse Federation, source unknown". v5.18.0.
    """
    if not table_ref:
        return ""
    catalog = table_ref.split(".", 1)[0].lower()
    checks = (
        (("bq", "bigquery"), "bigquery"),
        (("sf", "snowflake"), "snowflake"),
        (("mysql",), "mysql"),
        (("pg", "postgres", "postgresql"), "postgresql"),
        (("redshift",), "redshift"),
    )
    for tokens, source in checks:
        for tok in tokens:
            # Look for the token as a whole segment or a surrounding
            # underscore-delimited fragment (``bq_prod`` →
            # ``_bq_``). This avoids false positives like ``sfr`` or
            # ``mysqldump`` in a catalog name.
            if f"_{tok}_" in f"_{catalog}_" or catalog == tok:
                return source
    return ""


def populate_federation_signals(
    query_metrics: QueryMetrics, node_metrics: list[NodeMetrics]
) -> None:
    """Aggregate node-level Lakehouse Federation signals onto the query.

    Sets:
      - ``query_metrics.is_federation_query``
      - ``query_metrics.federation_tables``
      - ``query_metrics.federation_source_type`` (best-effort, first match)

    v5.18.0 — extracted as a standalone helper so the analyze pipeline
    (``analyzers/__init__.py::analyze_from_dict``) can call it between
    node extraction and bottleneck calculation.
    """
    fed_nodes = [nm for nm in node_metrics if nm.is_federation_scan]
    if not fed_nodes:
        return
    query_metrics.is_federation_query = True
    tables: list[str] = []
    for nm in fed_nodes:
        if nm.node_name.startswith(_FEDERATION_TABLE_PREFIX):
            tbl = nm.node_name[len(_FEDERATION_TABLE_PREFIX) :].strip()
            if tbl and tbl not in tables:
                tables.append(tbl)
    query_metrics.federation_tables = tables
    # Pick the first table's heuristic source type; when the query
    # joins multiple federated sources we leave the query-level source
    # empty and rely on per-table display in the report.
    if tables:
        first_guess = _guess_federation_source_type(tables[0])
        if first_guess and all(_guess_federation_source_type(t) == first_guess for t in tables):
            query_metrics.federation_source_type = first_guess


def extract_node_metrics(data: dict[str, Any]) -> list[NodeMetrics]:
    """Extract metrics from execution plan nodes.

    Note:
        Individual graphs that fail to parse are skipped with a warning.
        Unmapped metric labels are stored in extra_metrics for future extensibility.
    """
    node_metrics_list = []
    graphs = data.get("graphs", [])

    # Labels that are explicitly mapped to NodeMetrics fields

    for graph_index, graph in enumerate(graphs):
        # Safely parse graph (may be dict or JSON string)
        parsed_graph = _parse_graph_safely(graph, graph_index)
        if parsed_graph is None:
            continue  # Skip this graph on parse failure

        nodes = parsed_graph.get("nodes", [])
        for node in nodes:
            if node.get("hidden", False):
                continue

            node_name = node.get("name", "")
            node_tag = node.get("tag", "")
            node_id = node.get("id", "")
            key_metrics = node.get("keyMetrics", {})
            metrics_list = node.get("metrics", [])
            metadata = node.get("metadata", [])

            nm = NodeMetrics(
                node_id=node_id,
                node_name=node_name,
                node_tag=node_tag,
                duration_ms=_safe_int(key_metrics.get("durationMs")),
                peak_memory_bytes=_safe_int(key_metrics.get("peakMemoryBytes")),
                rows_num=_safe_int(key_metrics.get("rowsNum")),
            )

            # v5.18.0: Lakehouse Federation scan — tagged as
            # ``ROW_DATA_SOURCE_SCAN_EXEC`` in the profile. Node names
            # look like ``Row Data Source Scan <catalog.schema.table>``.
            # These reads come from an external engine (BigQuery,
            # Snowflake, Postgres, …) via Lakehouse Federation and do
            # NOT hit Delta files, so file-pruning / clustering /
            # shuffle / spill recommendations are misleading for them.
            if node_tag == "ROW_DATA_SOURCE_SCAN_EXEC":
                nm.is_federation_scan = True

            # Check if Photon enabled from metadata
            for meta in metadata:
                if meta.get("key") == "IS_PHOTON" and meta.get("value") == "true":
                    nm.is_photon = True
                    break

            # Extract clustering keys and other metadata
            for meta in metadata:
                key = meta.get("key", "")
                if key == "SCAN_CLUSTERS":
                    values = meta.get("values")
                    if isinstance(values, list):
                        for v in values:
                            if isinstance(v, dict):
                                col = (
                                    v.get("column")
                                    or v.get("name")
                                    or v.get("col")
                                    or v.get("field")
                                    or v.get("columnName")
                                )
                                if not col:
                                    continue
                                cstr = str(col).strip()
                                if not cstr:
                                    continue
                                lo = v.get("min") or v.get("minValue") or v.get("lower")
                                hi = v.get("max") or v.get("maxValue") or v.get("upper")
                                nm.clustering_keys.append(cstr)
                                if lo is not None or hi is not None:
                                    nm.clustering_key_bounds[cstr] = (
                                        None if lo is None else str(lo).strip(),
                                        None if hi is None else str(hi).strip(),
                                    )
                            elif v:
                                nm.clustering_keys.append(str(v))
                    else:
                        value = meta.get("value")
                        if value:
                            nm.clustering_keys.append(str(value))
                elif key == "IS_DELTA" and meta.get("value") == "true":
                    nm.is_delta = True
                elif key == "FILTERS":
                    values = meta.get("values")
                    if isinstance(values, list):
                        nm.filter_conditions = [str(v) for v in values if v]
                elif key == "CONDITION":
                    value = meta.get("value")
                    if value:
                        nm.filter_conditions = [str(value)]
                elif key == "LEFT_KEYS":
                    values = meta.get("values")
                    if isinstance(values, list):
                        nm.join_keys_left = [str(v) for v in values if v]
                elif key == "RIGHT_KEYS":
                    values = meta.get("values")
                    if isinstance(values, list):
                        nm.join_keys_right = [str(v) for v in values if v]
                elif key == "JOIN_TYPE":
                    value = meta.get("value")
                    if value:
                        nm.join_type = str(value)
                elif key == "JOIN_ALGORITHM":
                    value = meta.get("value")
                    if value:
                        nm.join_algorithm = str(value)
                elif key == "PARTITION_FILTERS":
                    values = meta.get("values")
                    if isinstance(values, list):
                        nm.partition_filters = [str(v) for v in values if v]
                    else:
                        value = meta.get("value")
                        if value:
                            nm.partition_filters = [str(value)]
                elif key == "AGGREGATE_EXPRESSIONS":
                    values = meta.get("values")
                    if isinstance(values, list):
                        nm.aggregate_expressions = [str(v) for v in values if v]
                    elif meta.get("value"):
                        nm.aggregate_expressions = [str(meta["value"])]
                elif key == "GROUPING_EXPRESSIONS":
                    values = meta.get("values")
                    if isinstance(values, list):
                        nm.grouping_expressions = [str(v) for v in values if v]
                    elif meta.get("value"):
                        nm.grouping_expressions = [str(meta["value"])]

            # Extract detailed metrics
            for m in metrics_list:
                label = m.get("label", "")
                value = _safe_int(m.get("value"))

                if label == "Files read":
                    nm.files_read = value
                elif label == "Files pruned":
                    nm.files_pruned = value
                elif label == "Size of files pruned":
                    nm.files_pruned_size = value
                elif label == "Size of files read":
                    nm.files_read_size = value
                elif label == "Cache hits size":
                    nm.cache_hits_size = value
                elif label == "Cache misses size":
                    nm.cache_misses_size = value
                elif label == "Cloud storage request count":
                    nm.cloud_storage_request_count = value
                elif label == "Cloud storage request duration":
                    nm.cloud_storage_request_duration_ms = value
                elif label == "Cloud storage retry count":
                    nm.cloud_storage_retry_count = value
                elif label == "Cloud storage retry duration":
                    nm.cloud_storage_retry_duration_ms = value
                elif label == "Data filters - batches skipped":
                    nm.data_filters_batches_skipped = value
                elif label == "Data filters - rows skipped":
                    nm.data_filters_rows_skipped = value
                elif label == "Rows scanned":
                    nm.rows_scanned = value
                elif label == "Number of output rows":
                    nm.rows_output = value
                elif "spilled to disk" in label.lower():
                    nm.spill_bytes = max(nm.spill_bytes, value)
                    # Also store in extra_metrics with full label for detailed analysis
                    nm.extra_metrics[label] = value
                # Scan locality metrics (Verbose mode only)
                elif label == "Number of local scan tasks":
                    nm.local_scan_tasks = value
                elif label == "Number of non-local (rescheduled) scan tasks":
                    nm.non_local_scan_tasks = value
                else:
                    # Store unmapped labels in extra_metrics
                    if label and value != 0:
                        nm.extra_metrics[label] = value

            # Include Scan nodes or nodes with significant duration
            if "Scan" in node_name or nm.duration_ms > 0:
                node_metrics_list.append(nm)

    return node_metrics_list


def extract_shuffle_metrics(data: dict[str, Any]) -> list[ShuffleMetrics]:
    """Extract shuffle operation metrics.

    Note:
        Individual graphs that fail to parse are skipped with a warning.
    """
    shuffle_list = []
    graphs = data.get("graphs", [])

    for graph_index, graph in enumerate(graphs):
        # Safely parse graph (may be dict or JSON string)
        parsed_graph = _parse_graph_safely(graph, graph_index)
        if parsed_graph is None:
            continue  # Skip this graph on parse failure

        nodes = parsed_graph.get("nodes", [])
        for node in nodes:
            node_name = node.get("name", "")
            node_tag = node.get("tag", "")

            # Identify shuffle nodes
            is_shuffle = (
                "SHUFFLE" in node_tag.upper()
                or "shuffle" in node_name.lower()
                or "exchange" in node_name.lower()
            )

            if not is_shuffle:
                continue

            key_metrics = node.get("keyMetrics", {})
            metrics_list = node.get("metrics", [])
            metadata = node.get("metadata", [])

            sm = ShuffleMetrics(
                node_id=node.get("id", ""),
                node_name=node_name,
                peak_memory_bytes=_safe_int(key_metrics.get("peakMemoryBytes")),
                duration_ms=_safe_int(key_metrics.get("durationMs")),
                rows_processed=_safe_int(key_metrics.get("rowsNum")),
            )

            # Extract shuffle attributes from metadata
            for meta in metadata:
                if meta.get("key") == "SHUFFLE_ATTRIBUTES":
                    values = meta.get("values", [])
                    if isinstance(values, list):
                        sm.shuffle_attributes = values

            # Extract detailed metrics
            for m in metrics_list:
                label = m.get("label", "")
                value = _safe_int(m.get("value"))

                if label == "Sink - Number of partitions":
                    sm.partition_count = value
                elif label == "Number of partitions":
                    if sm.partition_count == 0:
                        sm.partition_count = value
                elif label == "AQEShuffleRead - Number of partitions":
                    sm.aqe_partitions = value
                elif label == "AQEShuffleRead - Partition data size":
                    sm.aqe_data_size = value
                elif label == "AQEShuffleRead - Number of skewed partitions":
                    sm.aqe_skewed_partitions = value
                elif label == "Sink - Tasks total":
                    sm.sink_tasks_total = value
                elif label == "Source - Tasks total":
                    sm.source_tasks_total = value
                elif label == "Tasks total":
                    if sm.partition_count == 0:
                        sm.partition_count = value
                # Sink-side working memory and data size — used by the
                # memory_per_partition_mb property as the primary formula
                # (cumulative node-level peakMemoryBytes is misleading
                # when output is coalesced to a single partition).
                elif label == "Sink - Peak memory usage":
                    sm.sink_peak_memory_bytes = value
                elif label == "Sink - Num bytes written":
                    sm.sink_bytes_written = value
                elif label == "Sink - Num spills to disk due to memory pressure":
                    sm.sink_num_spills = value
                # AQE repartition signals — used to distinguish data-volume
                # problems (AQE auto-fixed) from true key skew.
                elif label == "Exchange - Adp original num partitions":
                    sm.aqe_original_num_partitions = value
                elif label == "Exchange - Adp intended num partitions":
                    sm.aqe_intended_num_partitions = value
                elif label == "Exchange - Adp self-triggered repartition count":
                    sm.aqe_self_repartition_count = value
                elif label == "Exchange - Adp total cancellation count":
                    sm.aqe_cancellation_count = value
                elif label == "Exchange - Adp triggered on materialized count":
                    sm.aqe_triggered_on_materialized_count = value
                # AOS (Auto-Optimized Shuffle) — coordinates partition counts
                # across multiple shuffles; non-zero count = AOS re-planned.
                elif label == "Exchange - Aos coordinated repartition count":
                    sm.aos_coordinated_repartition_count = value
                elif label == "Exchange - Aos old number of partitions":
                    sm.aos_old_num_partitions = value
                elif label == "Exchange - Aos new number of partitions":
                    sm.aos_new_num_partitions = value
                elif label == "Exchange - Aos intended number of partitions":
                    sm.aos_intended_num_partitions = value

            # Keep the node if it contributed any shuffle signal we surface
            # downstream: partition count, AQE runtime events, or AOS events.
            # Previously we required partition_count > 0, which dropped nodes
            # whose only signal was an AQE/AOS event.
            has_aqe_aos_event = (
                sm.aqe_self_repartition_count > 0
                or sm.aqe_skewed_partitions > 0
                or sm.aqe_cancellation_count > 0
                or sm.aqe_triggered_on_materialized_count > 0
                or sm.aos_coordinated_repartition_count > 0
            )
            if sm.partition_count > 0 or has_aqe_aos_event:
                shuffle_list.append(sm)

    return shuffle_list


def classify_join_type(join_algorithm: str) -> JoinType:
    """Classify join type from JOIN_ALGORITHM metadata or node name."""
    algo_lower = join_algorithm.lower()
    # Check for broadcast first (includes "Photon Broadcast Hash", "Photon Broadcast Nested Loop")
    if "broadcast" in algo_lower:
        return JoinType.BROADCAST
    # Check for shuffle/shuffled hash (includes "Photon Shuffled Hash")
    elif "shuffle" in algo_lower and "hash" in algo_lower:
        return JoinType.SHUFFLE_HASH
    elif "sort merge" in algo_lower or "sortmerge" in algo_lower:
        return JoinType.SORT_MERGE
    elif "nested loop" in algo_lower or "nestedloop" in algo_lower:
        return JoinType.SHUFFLE_NESTED_LOOP
    return JoinType.UNKNOWN


def extract_join_info(data: dict[str, Any]) -> list[JoinInfo]:
    """Extract join information from execution plan.

    Note:
        Individual graphs that fail to parse are skipped with a warning.
    """
    join_list = []
    graphs = data.get("graphs", [])

    for graph_index, graph in enumerate(graphs):
        # Safely parse graph (may be dict or JSON string)
        parsed_graph = _parse_graph_safely(graph, graph_index)
        if parsed_graph is None:
            continue  # Skip this graph on parse failure

        nodes = parsed_graph.get("nodes", [])
        for node in nodes:
            node_name = node.get("name", "")
            if "Join" not in node_name:
                continue

            key_metrics = node.get("keyMetrics", {})
            metadata = node.get("metadata", [])

            is_photon = None  # None = unknown, True = Photon, False = not Photon
            join_algorithm = ""
            for meta in metadata:
                key = meta.get("key", "")
                if key == "IS_PHOTON":
                    is_photon = meta.get("value") == "true"
                elif key == "JOIN_ALGORITHM":
                    join_algorithm = meta.get("value", "")

            # If IS_PHOTON not explicitly set, infer from JOIN_ALGORITHM
            # Based on tuning guide: Broadcast, Shuffle-Hash, Nested-Loop = Photon supported
            # Only Sort-Merge is not supported
            if is_photon is None and join_algorithm:
                algo_lower = join_algorithm.lower()
                if "sort" in algo_lower and "merge" in algo_lower:
                    # Sort-Merge Join is NOT Photon supported
                    is_photon = False
                elif any(kw in algo_lower for kw in ("broadcast", "hash", "nested", "loop")):
                    # Broadcast, Shuffle-Hash, Nested-Loop are Photon supported
                    is_photon = True

            # Use JOIN_ALGORITHM metadata if available, otherwise fall back to node name
            join_type = classify_join_type(join_algorithm if join_algorithm else node_name)

            join_list.append(
                JoinInfo(
                    node_name=node_name,
                    join_type=join_type,
                    is_photon=is_photon,
                    duration_ms=_safe_int(key_metrics.get("durationMs")),
                )
            )

    return join_list


def extract_sql_analysis(data: dict[str, Any]) -> SQLAnalysis:
    """Extract and analyze SQL from profile data.

    Args:
        data: Profile data dictionary

    Returns:
        SQLAnalysis with parsed SQL information
    """
    # Handle both nested and flat structures
    if "query" in data:
        query = data["query"]
    else:
        query = data

    query_text = query.get("queryText", "")

    if not query_text:
        return SQLAnalysis()

    # Skip full SQL analysis for streaming statements (REFRESH STREAMING TABLE).
    # These are DLT-generated and not user-modifiable; sqlglot may not parse them.
    if query_text.strip().upper().startswith("REFRESH"):
        return SQLAnalysis(raw_sql=query_text)

    try:
        return analyze_sql(query_text)
    except Exception as e:
        logger.warning(
            "analyze_sql failed; returning raw SQL only",
            extra={
                "query_length": len(query_text),
                "path": "analyze_sql",
                "exception_type": type(e).__name__,
            },
        )
        return SQLAnalysis(raw_sql=query_text, formatted_sql=query_text)


def _parse_table_name_from_scan_node(node_name: str) -> str:
    """Parse table name from Scan node name.

    Handles formats like:
    - "Scan catalog.schema.table"
    - "Scan catalog.schema.table:alias"
    - "PhotonScan catalog.schema.table"

    Args:
        node_name: Node name string

    Returns:
        Extracted table name or empty string
    """
    import re

    # Pattern: "Scan <table>" or "PhotonScan <table>" etc.
    match = re.match(r"^(?:Photon)?Scan\s+(.+)$", node_name, re.IGNORECASE)
    if not match:
        return ""

    table_part = match.group(1).strip()

    # Remove alias after colon if present (e.g., "catalog.schema.table:alias")
    if ":" in table_part:
        table_part = table_part.split(":")[0]

    return table_part


def _merge_clustering_bounds(
    a: tuple[str | None, str | None],
    b: tuple[str | None, str | None],
) -> tuple[str | None, str | None]:
    """Merge min/max bounds from multiple scan nodes (widest numeric range when parseable)."""
    if (not a[0] and not a[1]) and (not b[0] and not b[1]):
        return a
    if not b[0] and not b[1]:
        return a
    if not a[0] and not a[1]:
        return b
    try:
        amin = float(str(a[0]).strip())
        amax = float(str(a[1]).strip())
        bmin = float(str(b[0]).strip())
        bmax = float(str(b[1]).strip())
        return str(min(amin, bmin)), str(max(amax, bmax))
    except (TypeError, ValueError):
        return a if (a[0] and a[1]) else b


def _bounds_to_cardinality_class(
    min_v: str | None,
    max_v: str | None,
    rows_scanned: int,
) -> str | None:
    """Return low/high/unknown from min/max strings, or None if not inferable."""
    if not min_v or not max_v:
        return None
    ms = str(min_v).strip()
    mx = str(max_v).strip()
    try:
        a = float(ms)
        b = float(mx)
        if a > b:
            a, b = b, a
        span = int(b - a + 1)
        if span < 1:
            span = 1
        est = min(span, rows_scanned) if rows_scanned > 0 else span
        if est <= 10_000:
            return "low"
        if est >= 200_000:
            return "high"
        return "unknown"
    except ValueError:
        pass
    try:
        ds = ms[:10] if len(ms) >= 10 else ms
        de = mx[:10] if len(mx) >= 10 else mx
        t0 = datetime.strptime(ds, "%Y-%m-%d")
        t1 = datetime.strptime(de, "%Y-%m-%d")
        days = abs((t1 - t0).days) + 1
        est = min(days, rows_scanned) if rows_scanned > 0 else days
        if est <= 10_000:
            return "low"
        if est >= 200_000:
            return "high"
        return "unknown"
    except ValueError:
        return None


def _name_heuristic_cardinality(column: str) -> str:
    """Heuristic low/high/unknown from column name when profile bounds are missing.

    v5.16.17 (A): extended to catch camelCase / concatenated date tokens
    (``startmonth``, ``endyear``, ``billmonth`` …) in addition to the
    underscore-separated forms (``_month``, ``_year``).
    """
    n = column.lower()
    if any(x in n for x in ("_date", "_dt", "_day", "_month", "_year")) or n.startswith("date"):
        return "low"
    # Suffix match without underscore — covers MYCLOUD_STARTMONTH,
    # MYCLOUD_STARTYEAR style names. We intentionally skip ``date`` (too
    # many collisions like ``candidate``, ``update``, ``validate``) and
    # ``hour`` (collides with English words such as ``labour``). Tokens
    # chosen below are much rarer as non-date suffixes.
    if any(n.endswith(s) for s in ("month", "year", "quarter", "week")):
        return "low"
    # ``hour`` is ambiguous as a bare suffix; require an underscore or
    # the whole column name to be exactly "hour".
    if n == "hour" or n.endswith("_hour"):
        return "low"
    if n.endswith("_sk") or n.endswith("_id") or n.endswith("_key"):
        return "high"
    if any(x in n for x in ("_status", "_type", "_category", "_region", "_country")):
        return "low"
    return "unknown"


def _type_heuristic_cardinality(col_type: str | None) -> str:
    """Heuristic low/unknown from the column's SQL type (v5.16.17, C).

    DATE / BOOLEAN / TINYINT are low-cardinality clustering candidates.
    TIMESTAMP is treated as *likely* low because DBSQL Liquid Clustering
    typically clusters on the truncated date boundary rather than the
    raw second/millisecond value — but a bare TIMESTAMP column with
    sub-second granularity is genuinely high-cardinality. The caller's
    priority chain already lets bounds and EXPLAIN stats override this
    so the hint only kicks in when no stronger signal exists.

    Returns ``unknown`` for numeric/string types where the cardinality
    cannot be judged from type alone.
    """
    if not col_type:
        return "unknown"
    t = col_type.lower().strip()
    if t.startswith("date"):
        return "low"
    if t in ("boolean", "bool", "tinyint"):
        return "low"
    # TIMESTAMP: weak hint only — stats/bounds preferred.
    if t.startswith("timestamp"):
        return "low"
    return "unknown"


def _stats_cardinality_class(distinct_count: int | None, rows_scanned: int) -> str:
    """Classify cardinality from an exact distinct count (v5.16.17, D).

    Thresholds mirror ``_bounds_to_cardinality_class`` so that a
    stats-derived ``distinct_count=12`` and a bounds-derived
    ``span=12`` land at the same class.
    """
    if distinct_count is None:
        return "unknown"
    if distinct_count <= 10_000:
        return "low"
    if distinct_count >= 200_000:
        return "high"
    # Middle band: compare against rows for relative judgement
    if rows_scanned > 0 and distinct_count / max(rows_scanned, 1) >= 0.1:
        return "high"
    return "unknown"


def estimate_clustering_key_cardinality(
    column: str,
    min_v: str | None,
    max_v: str | None,
    rows_scanned: int,
    col_type: str | None = None,
    distinct_count: int | None = None,
) -> str:
    """Estimate cardinality class for a clustering key.

    Priority (highest confidence first):
      1. Exact distinct count from EXPLAIN column stats (D, v5.16.17)
      2. Bounds-based (min/max span) from profile JSON SCAN_CLUSTERS
      3. Column type (DATE/TIMESTAMP → low) (C, v5.16.17)
      4. Name heuristic (A, extended in v5.16.17)
    """
    sc = _stats_cardinality_class(distinct_count, rows_scanned)
    if sc in ("low", "high"):
        return sc
    bc = _bounds_to_cardinality_class(min_v, max_v, rows_scanned)
    if bc in ("low", "high"):
        return bc
    tc = _type_heuristic_cardinality(col_type)
    if tc in ("low", "high"):
        return tc
    nh = _name_heuristic_cardinality(column)
    if nh != "unknown":
        return nh
    return bc if bc else "unknown"


def extract_table_scan_metrics(
    node_metrics: list[NodeMetrics],
    sql_analysis: SQLAnalysis | None = None,
    top_n: int = 3,
) -> list[TableScanMetrics]:
    """Extract table-level scan metrics from node metrics.

    Aggregates scan metrics by table name and returns top N tables by bytes read.

    Args:
        node_metrics: List of NodeMetrics from extract_node_metrics
        sql_analysis: Optional SQL analysis for recommended clustering keys
        top_n: Number of top tables to return (default 3)

    Returns:
        List of TableScanMetrics sorted by bytes_read descending
    """
    # Aggregate metrics by table name
    table_metrics: dict[str, TableScanMetrics] = defaultdict(lambda: TableScanMetrics())
    table_col_bounds: dict[str, dict[str, tuple[str | None, str | None]]] = defaultdict(dict)

    for nm in node_metrics:
        # Only process Scan nodes
        if "Scan" not in nm.node_name:
            continue

        table_name = _parse_table_name_from_scan_node(nm.node_name)
        if not table_name:
            continue

        tm = table_metrics[table_name]
        tm.table_name = table_name
        tm.bytes_read += nm.files_read_size
        tm.bytes_pruned += nm.files_pruned_size
        tm.files_read += nm.files_read
        tm.files_pruned += nm.files_pruned
        tm.rows_scanned += nm.rows_scanned
        if nm.clustering_keys:
            existing = {k.lower() for k in tm.current_clustering_keys}
            for key in nm.clustering_keys:
                if key.lower() not in existing:
                    tm.current_clustering_keys.append(key)
                    existing.add(key.lower())
                b = nm.clustering_key_bounds.get(key, (None, None))
                if b[0] or b[1]:
                    cur = table_col_bounds[table_name].get(key)
                    if cur is None:
                        table_col_bounds[table_name][key] = b
                    else:
                        table_col_bounds[table_name][key] = _merge_clustering_bounds(cur, b)

    # Estimate cardinality for each current clustering key (low / high / unknown)
    for tm in table_metrics.values():
        bounds = table_col_bounds.get(tm.table_name, {})
        for col in tm.current_clustering_keys:
            lo, hi = bounds.get(col, (None, None))
            tm.clustering_key_cardinality[col] = estimate_clustering_key_cardinality(
                col, lo, hi, tm.rows_scanned
            )

    # Add recommended clustering keys from SQL analysis
    if sql_analysis and sql_analysis.columns:
        # Build alias-to-table mapping
        alias_to_table: dict[str, str] = {}
        for t in sql_analysis.tables:
            if t.alias:
                alias_to_table[t.alias.lower()] = t.full_name or t.table
            if t.full_name:
                alias_to_table[t.full_name.lower()] = t.full_name
            if t.table:
                alias_to_table[t.table.lower()] = t.full_name or t.table

        # Collect WHERE/JOIN columns per table with scoring
        # Only include columns used with equality operators (=, IN) for clustering keys
        # Range operators (<, >, <=, >=, BETWEEN) are not efficient for clustering
        #
        # Scoring logic (based on Databricks best practices):
        # - JOIN conditions are weighted higher (data co-location benefits)
        # - Equality (=) is preferred over IN (point lookups are most efficient)
        # - Date/time columns get a small bonus (common filter patterns)
        table_column_scores: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))

        # Score weights
        W_JOIN = 1.0  # JOIN conditions - high priority for co-location
        W_WHERE = 0.8  # WHERE conditions - direct filter benefit
        W_EQ = 1.0  # Equality operator - most efficient for data skipping
        W_IN = 0.7  # IN operator - less efficient than equality
        W_DATE_BONUS = 0.2  # Bonus for date/time columns

        for col_ref in sql_analysis.columns:
            if col_ref.context not in ("where", "join"):
                continue

            # Only recommend columns with equality operators for clustering
            # Exclude columns where operator is empty (unknown) or not equality-based
            if col_ref.operator not in ("=", "IN"):
                continue

            # Resolve table name
            resolved_table = ""
            if col_ref.table_alias:
                resolved_table = alias_to_table.get(col_ref.table_alias.lower(), "")
            elif col_ref.table_name:
                resolved_table = alias_to_table.get(col_ref.table_name.lower(), "")

            if resolved_table and col_ref.column_name:
                # Calculate score for this column occurrence
                context_weight = W_JOIN if col_ref.context == "join" else W_WHERE
                operator_weight = W_EQ if col_ref.operator == "=" else W_IN
                base_score = context_weight * operator_weight

                # Date/time column bonus (common naming patterns)
                col_lower = col_ref.column_name.lower()
                if any(pattern in col_lower for pattern in ["date", "_dt", "_ts", "time", "_at"]):
                    base_score += W_DATE_BONUS

                # Accumulate scores (same column appearing multiple times gets higher score)
                table_column_scores[resolved_table.lower()][col_ref.column_name] += base_score

        # Assign recommended keys to table metrics
        for table_name, tm in table_metrics.items():
            table_key = table_name.lower()
            # Try exact match first, then partial match
            col_scores = table_column_scores.get(table_key, {})
            if not col_scores:
                # Try matching just the table part (last component)
                simple_name = table_name.split(".")[-1].lower()
                for key, candidates in table_column_scores.items():
                    if key.endswith(simple_name) or simple_name in key:
                        col_scores = candidates
                        break

            # Sort by score descending and dedupe
            sorted_cols = sorted(col_scores.items(), key=lambda x: (-x[1], x[0]))
            seen = set()
            unique_cols = []
            for col_name, _score in sorted_cols:
                if col_name.lower() not in seen:
                    seen.add(col_name.lower())
                    unique_cols.append(col_name)
            tm.recommended_clustering_keys = unique_cols[:4]

    # Sort by bytes_read descending and return top N
    sorted_tables = sorted(
        table_metrics.values(),
        key=lambda x: x.bytes_read,
        reverse=True,
    )

    return sorted_tables[:top_n]


# --- Stage and Data Flow Extraction ---


def extract_stage_info(data: dict[str, Any]) -> list[StageInfo]:
    """Extract stage execution information from profile data.

    Supports both new format (camelCase) and old format (snake_case).
    Stage data is in graphs[1].stageData (or stage_data).
    """
    graphs = data.get("graphs", [])
    if len(graphs) < 2:
        return []

    parsed_graph = _parse_graph_safely(graphs[1], 1)
    if parsed_graph is None:
        return []

    # Support both camelCase and snake_case
    stage_data = parsed_graph.get("stageData") or parsed_graph.get("stage_data") or []

    stages: list[StageInfo] = []
    for sd in stage_data:
        stage_id = str(sd.get("stageId", sd.get("stage_id", "")))
        status = sd.get("status", "")

        # keyMetrics (camelCase) or key_metrics (snake_case)
        key_metrics = sd.get("keyMetrics") or sd.get("key_metrics") or {}
        duration_ms = _safe_int(key_metrics.get("durationMs", key_metrics.get("duration_ms", 0)))

        num_tasks = _safe_int(sd.get("numTasks", sd.get("num_tasks", 0)))
        num_complete = _safe_int(sd.get("numCompleteTasks", sd.get("num_complete_tasks", 0)))
        num_failed = _safe_int(sd.get("numFailedTasks", sd.get("num_failed_tasks", 0)))
        num_killed = _safe_int(sd.get("numKilledTasks", sd.get("num_killed_tasks", 0)))

        # Auto-generate note from failure reason if available
        note = sd.get("failureReason", "") or ""

        stages.append(
            StageInfo(
                stage_id=stage_id,
                status=status,
                duration_ms=duration_ms,
                num_tasks=num_tasks,
                num_complete_tasks=num_complete,
                num_failed_tasks=num_failed,
                num_killed_tasks=num_killed,
                note=note,
            )
        )

    return stages


_INTERESTING_KEYWORDS = {"scan", "join"}


def _is_interesting_node(node_name: str) -> bool:
    """Check if a node is interesting for data flow analysis (Scan/Join)."""
    name_lower = node_name.lower()
    return any(kw in name_lower for kw in _INTERESTING_KEYWORDS)


def _parse_graph_topology(
    data: dict[str, Any],
) -> (
    tuple[
        dict[str, dict],  # node_map
        dict[str, list[str]],  # children (parent_id -> [child_ids])
        dict[str, list[str]],  # parents (child_id -> [parent_ids])
        list[dict],  # interesting_nodes (sorted by topo order)
        list[str],  # topo_order
    ]
    | None
):
    """Parse graph topology from profile data.

    Selects the graph with the most interesting nodes (Scan/Join) from
    all available graphs, then extracts node map, adjacency lists,
    interesting nodes, and topological ordering.

    Returns:
        Tuple of (node_map, children, parents, interesting_nodes, topo_order),
        or None if no suitable graph is found.
    """
    graphs = data.get("graphs", [])
    if not graphs:
        return None

    # Select the graph with the most Scan/Join nodes
    best_graph = None
    best_count = 0
    for i, raw_graph in enumerate(graphs):
        parsed = _parse_graph_safely(raw_graph, i)
        if parsed is None:
            continue
        count = sum(
            1
            for n in parsed.get("nodes", [])
            if not n.get("hidden", False) and _is_interesting_node(n.get("name", ""))
        )
        if count > best_count:
            best_count = count
            best_graph = parsed

    if best_graph is None or best_count == 0:
        return None

    nodes = best_graph.get("nodes", [])
    edges = best_graph.get("edges", [])

    # Build node lookup
    node_map: dict[str, dict] = {}
    for node in nodes:
        if not node.get("hidden", False):
            node_map[node.get("id", "")] = node

    # Build raw adjacency from edges
    raw_children: dict[str, list[str]] = {}  # fromId -> [toIds]
    raw_parents: dict[str, list[str]] = {}  # toId -> [fromIds]
    for edge in edges:
        from_id = edge.get("fromId", "")
        to_id = edge.get("toId", "")
        raw_children.setdefault(from_id, []).append(to_id)
        raw_parents.setdefault(to_id, []).append(from_id)

    # Collect interesting nodes (Scan, Join)
    interesting_nodes: list[dict] = []
    scan_ids: set[str] = set()
    for node in nodes:
        if node.get("hidden", False):
            continue
        if _is_interesting_node(node.get("name", "")):
            interesting_nodes.append(node)
            if "scan" in node.get("name", "").lower():
                scan_ids.add(node.get("id", ""))

    if not interesting_nodes:
        return node_map, raw_children, raw_parents, [], []

    # Detect edge direction: check if edges go FROM joins TO scans
    # (dependency direction) or FROM scans TO joins (data flow direction).
    # If most scan nodes appear as toId targets from non-scan nodes,
    # edges are in dependency direction and need to be reversed.
    scan_as_target = 0
    scan_as_source = 0
    for edge in edges:
        from_id = edge.get("fromId", "")
        to_id = edge.get("toId", "")
        if to_id in scan_ids and from_id not in scan_ids:
            scan_as_target += 1
        if from_id in scan_ids and to_id not in scan_ids:
            scan_as_source += 1

    # If scans appear more as targets than sources, edges are reversed
    reversed_edges = scan_as_target > scan_as_source
    if reversed_edges:
        # Swap direction: children/parents are inverted
        children = raw_parents  # toId -> [fromIds] becomes data flow direction
        parents = raw_children  # fromId -> [toIds] becomes reverse flow
    else:
        children = raw_children
        parents = raw_parents

    # Topological sort using in-degree (Kahn's algorithm) over all nodes
    # in_degree counts incoming edges in data flow direction
    all_ids = set(node_map.keys())
    in_degree: dict[str, int] = {nid: 0 for nid in all_ids}
    for edge in edges:
        if reversed_edges:
            edge.get("toId", "")
        else:
            edge.get("toId", "")
        # In data flow direction, "to" nodes receive data (higher in-degree)
        target_id = edge.get("toId", "") if not reversed_edges else edge.get("fromId", "")
        if target_id in in_degree:
            in_degree[target_id] += 1

    queue = [nid for nid, deg in in_degree.items() if deg == 0]
    topo_order: list[str] = []
    while queue:
        # Sort for deterministic output
        queue.sort()
        nid = queue.pop(0)
        topo_order.append(nid)
        for child_id in children.get(nid, []):
            if child_id in in_degree:
                in_degree[child_id] -= 1
                if in_degree[child_id] == 0:
                    queue.append(child_id)

    # Sort interesting nodes by topological order
    position: dict[str, int] = {nid: i for i, nid in enumerate(topo_order)}
    interesting_nodes = sorted(
        interesting_nodes, key=lambda n: position.get(n.get("id", ""), 999999)
    )

    return node_map, children, parents, interesting_nodes, topo_order


def extract_data_flow(data: dict[str, Any]) -> list[DataFlowEntry]:
    """Extract data flow information (Scan -> JOIN -> output).

    Builds a DAG from edges and extracts Scan/JOIN nodes with their
    output row counts, duration, and memory usage. Ordered from sources to sinks.
    """
    topology = _parse_graph_topology(data)
    if topology is None:
        return []

    node_map, children, parents, sorted_nodes, topo_order = topology
    if not sorted_nodes:
        return []

    # Build entries
    entries: list[DataFlowEntry] = []
    for node in sorted_nodes:
        node_id = node.get("id", "")
        node_name = node.get("name", "")
        key_metrics = node.get("keyMetrics") or node.get("key_metrics") or {}
        metadata = node.get("metadata", [])

        output_rows = _safe_int(key_metrics.get("rowsNum", key_metrics.get("rows_num", 0)))
        duration_ms = _safe_int(key_metrics.get("durationMs", key_metrics.get("duration_ms", 0)))
        peak_memory = _safe_int(
            key_metrics.get("peakMemoryBytes", key_metrics.get("peak_memory_bytes", 0))
        )

        # Extract JOIN keys from metadata
        join_keys = ""
        if "join" in node_name.lower():
            left_keys = []
            right_keys = []
            for meta in metadata:
                key = meta.get("key", "")
                if key == "LEFT_KEYS":
                    left_keys = [
                        mv.get("value", "") for mv in meta.get("metaValues", []) if mv.get("value")
                    ]
                    if not left_keys:
                        left_keys = [v for v in meta.get("values", []) if v]
                elif key == "RIGHT_KEYS":
                    right_keys = [
                        mv.get("value", "") for mv in meta.get("metaValues", []) if mv.get("value")
                    ]
                    if not right_keys:
                        right_keys = [v for v in meta.get("values", []) if v]
            if left_keys:
                join_keys = ", ".join(left_keys)

        entries.append(
            DataFlowEntry(
                node_id=node_id,
                operation=node_name,
                output_rows=output_rows,
                duration_ms=duration_ms,
                peak_memory_bytes=peak_memory,
                join_keys=join_keys,
            )
        )

    return entries


def _find_interesting_edges(
    interesting_ids: set[str],
    children: dict[str, list[str]],
) -> list[tuple[str, str]]:
    """BFS from each interesting node to find direct interesting-to-interesting edges.

    Traverses through intermediate non-interesting nodes (Filter, Project,
    Exchange, Shuffle) to find which interesting nodes feed into which others.

    Args:
        interesting_ids: Set of node IDs considered "interesting" (Scan, Join)
        children: Adjacency map: parent_id -> [child_ids]

    Returns:
        List of (from_interesting_id, to_interesting_id) tuples
    """
    edges: list[tuple[str, str]] = []
    for source_id in sorted(interesting_ids):
        queue: deque[str] = deque(children.get(source_id, []))
        visited = {source_id}
        while queue:
            node_id = queue.popleft()
            if node_id in visited:
                continue
            visited.add(node_id)
            if node_id in interesting_ids:
                edges.append((source_id, node_id))
            else:
                for child_id in children.get(node_id, []):
                    if child_id not in visited:
                        queue.append(child_id)
    return edges


def extract_data_flow_dag(data: dict[str, Any]) -> DataFlowDAG | None:
    """Extract data flow DAG with edges between interesting nodes.

    Returns DataFlowDAG with entries (flat list), edges, and precomputed
    adjacency maps for visualization.

    Args:
        data: Raw profile data

    Returns:
        DataFlowDAG or None if graphs are insufficient.
    """
    topology = _parse_graph_topology(data)
    if topology is None:
        return None

    node_map, children, parents, sorted_nodes, topo_order = topology
    if not sorted_nodes:
        return None

    # Build flat entries (same as extract_data_flow)
    entries = extract_data_flow(data)
    if not entries:
        return None

    # Find edges between interesting nodes via BFS
    interesting_ids = {n.get("id", "") for n in sorted_nodes}
    raw_edges = _find_interesting_edges(interesting_ids, children)

    dag_edges = [DataFlowEdge(from_node_id=f, to_node_id=t) for f, t in raw_edges]

    # Build adjacency maps for interesting nodes only
    dag_children: dict[str, list[str]] = {}
    dag_parents: dict[str, list[str]] = {}
    for f, t in raw_edges:
        dag_children.setdefault(f, []).append(t)
        dag_parents.setdefault(t, []).append(f)

    # Identify source (no parents in interesting set) and sink (no children) nodes
    source_ids = sorted([nid for nid in interesting_ids if nid not in dag_parents])
    sink_ids = sorted([nid for nid in interesting_ids if nid not in dag_children])

    return DataFlowDAG(
        entries=entries,
        edges=dag_edges,
        children_map=dag_children,
        parents_map=dag_parents,
        sink_node_ids=sink_ids,
        source_node_ids=source_ids,
    )


# ---------------------------------------------------------------------------
# TargetTableInfo — write-target metadata (INSERT / CTAS / MERGE only)
# ---------------------------------------------------------------------------

_RE_CATALOG = re.compile(r"\bCatalog:\s*(?P<v>\S+)")
_RE_DATABASE = re.compile(r"\bDatabase:\s*(?P<v>\S+)")
_RE_TABLE = re.compile(r"\bTable:\s*(?P<v>\S+)")
_RE_PROVIDER = re.compile(r"\bProvider:\s*(?P<v>\S+)")
# Table Properties: [k=v, k=v, clusteringColumns=[["A"],["B"]], ...]
# The outer [...] wraps the property list but contains nested [[...]] for
# clusteringColumns values — a balanced-bracket scan is required (re alone
# cannot match nested structures).
_RE_TABLE_PROPERTIES_START = re.compile(r"Table Properties:\s*\[")


def _slice_table_properties_body(text: str) -> str | None:
    """Return the string inside ``Table Properties: [...]`` by walking
    character-by-character and tracking ``[`` / ``]`` depth.
    """
    m = _RE_TABLE_PROPERTIES_START.search(text)
    if not m:
        return None
    start = m.end()  # position just after the opening '['
    depth = 1
    i = start
    while i < len(text) and depth > 0:
        ch = text[i]
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return text[start:i]
        i += 1
    return None


# clusteringColumns=[["A"],["B"],...]
_RE_CLUSTERING_COLUMNS = re.compile(
    r"clusteringColumns\s*=\s*(?P<v>\[\[.*?\]\](?:\s*\])?)", re.DOTALL
)
# delta.liquid.hierarchicalClusteringColumns=col1, col2
# This value is a comma-separated list; it terminates at the next
# ", delta." / ", other=" boundary or end-of-block.
_RE_HIER_CLUSTERING = re.compile(
    r"delta\.liquid\.hierarchicalClusteringColumns\s*=\s*(?P<v>.+?)(?=,\s*delta\.|,\s*[a-z][A-Za-z0-9_.]*=|$)",
    re.DOTALL,
)


def _parse_clustering_columns_json_like(text: str) -> list[list[str]]:
    """Parse ``[["A"],["B"],["C"]]`` style literal into nested list."""
    import re as _re

    inner_pattern = _re.compile(r'\["([^"]+)"\]')
    return [[m.group(1)] for m in inner_pattern.finditer(text)]


def extract_target_table_info(data: dict[str, Any]) -> TargetTableInfo | None:
    """Extract write target's DDL metadata from photonExplain params.

    Returns None when no INSERT/CTAS/MERGE describe block is found.
    """
    graphs = data.get("graphs") or []
    if not graphs:
        return None
    for g in graphs:
        # Defensive: some profile shapes contain non-dict entries in `graphs`
        # (e.g. plain strings). Skip them rather than crash. (Week 2.5 #1)
        if not isinstance(g, dict):
            continue
        pe_list = g.get("photonExplain") or []
        if not isinstance(pe_list, list):
            continue
        for pe in pe_list:
            if not isinstance(pe, dict):
                continue
            params = pe.get("params") or []
            if not isinstance(params, list):
                continue
            for p in params:
                if not isinstance(p, dict):
                    continue
                pv = p.get("paramValue") or ""
                if not isinstance(pv, str) or not pv:
                    continue
                if "CatalogTable(" not in pv and "DeltaTableV2(" not in pv:
                    continue
                # This block is the describe of the write target.
                info = TargetTableInfo(raw_block=pv[:4096])

                m = _RE_CATALOG.search(pv)
                if m:
                    info.catalog = m.group("v")
                m = _RE_DATABASE.search(pv)
                if m:
                    info.database = m.group("v")
                m = _RE_TABLE.search(pv)
                if m:
                    info.table = m.group("v")
                m = _RE_PROVIDER.search(pv)
                if m:
                    info.provider = m.group("v")

                # Table Properties block (depth-aware slicing)
                body = _slice_table_properties_body(pv)
                if body:
                    # Clustering columns
                    cc_m = _RE_CLUSTERING_COLUMNS.search(body)
                    if cc_m:
                        info.clustering_columns = _parse_clustering_columns_json_like(
                            cc_m.group("v")
                        )
                    # Hierarchical clustering
                    hc_m = _RE_HIER_CLUSTERING.search(body)
                    if hc_m:
                        info.hierarchical_clustering_columns = [
                            c.strip() for c in hc_m.group("v").split(",") if c.strip()
                        ]
                    # Generic delta.* properties (best-effort key=value parse)
                    for kv in re.finditer(r"([a-zA-Z][A-Za-z0-9._]*)\s*=\s*([^,]+?)(?=,|$)", body):
                        k = kv.group(1).strip()
                        v = kv.group(2).strip()
                        # Skip clusteringColumns/hier — already captured
                        if k in ("clusteringColumns",):
                            continue
                        if k.startswith("delta.liquid.hierarchicalClusteringColumns"):
                            continue
                        info.properties[k] = v
                return info
    return None
