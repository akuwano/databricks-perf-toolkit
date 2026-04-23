"""Hot operator extraction and clustering recommendation helpers."""

from ..models import NodeMetrics, OperatorHotspot, QueryMetrics


def extract_hot_operators(
    node_metrics: list[NodeMetrics],
    query_metrics: QueryMetrics,
    top_n: int = 10,
) -> list[OperatorHotspot]:
    """Extract top N operators by time consumption.

    Args:
        node_metrics: List of node metrics
        query_metrics: Query-level metrics for calculating time share
        top_n: Number of top operators to return

    Returns:
        List of OperatorHotspot sorted by duration descending
    """
    total_time = query_metrics.task_total_time_ms or query_metrics.execution_time_ms or 1

    # Sort by duration descending
    sorted_nodes = sorted(node_metrics, key=lambda x: x.duration_ms, reverse=True)

    hotspots = []
    for rank, nm in enumerate(sorted_nodes[:top_n], 1):
        time_share = (nm.duration_ms / total_time) * 100 if total_time > 0 else 0

        # Determine bottleneck type
        bottleneck_type = ""
        node_name_lower = nm.node_name.lower()
        if nm.spill_bytes > 0:
            bottleneck_type = "spill"
        elif "scan" in node_name_lower:
            bottleneck_type = "scan"
        elif "join" in node_name_lower:
            bottleneck_type = "join"
        elif "shuffle" in node_name_lower or "exchange" in node_name_lower:
            bottleneck_type = "shuffle"
        elif "sort" in node_name_lower:
            bottleneck_type = "sort"
        elif "agg" in node_name_lower or "aggregate" in node_name_lower:
            bottleneck_type = "agg"

        hotspot = OperatorHotspot(
            rank=rank,
            node_id=nm.node_id,
            node_name=nm.node_name,
            duration_ms=nm.duration_ms,
            time_share_percent=time_share,
            rows_in=nm.rows_scanned or nm.rows_num,
            rows_out=nm.rows_output or nm.rows_num,
            spill_bytes=nm.spill_bytes,
            peak_memory_bytes=nm.peak_memory_bytes,
            is_photon=nm.is_photon,
            bottleneck_type=bottleneck_type,
        )
        hotspots.append(hotspot)

    return hotspots


def _update_top_scanned_with_clustering(
    top_scanned_tables: list,
    target_table: str,
    clustering_keys: list[str],
) -> None:
    """Update top_scanned_tables with recommended clustering keys.

    Tries to match by table name. If no match found, falls back to updating
    the first table (highest bytes_read).
    """
    if not top_scanned_tables or not clustering_keys:
        return

    target_lower = target_table.lower().replace("`", "").replace('"', "")
    target_short = target_lower.split(".")[-1] if target_lower else ""

    matched = False
    for tsm in top_scanned_tables:
        tsm_lower = tsm.table_name.lower().replace("`", "").replace('"', "")
        tsm_short = tsm_lower.split(".")[-1] if tsm_lower else ""

        # Try various matching strategies
        if (
            tsm_lower == target_lower
            or tsm_short == target_short
            or tsm_lower.endswith(f".{target_short}")
            or target_lower.endswith(f".{tsm_short}")
        ):
            if not tsm.recommended_clustering_keys:
                tsm.recommended_clustering_keys = clustering_keys
            matched = True
            break

    # Fallback: if no match found, update the first table (most I/O intensive)
    if not matched and top_scanned_tables:
        if not top_scanned_tables[0].recommended_clustering_keys:
            top_scanned_tables[0].recommended_clustering_keys = clustering_keys
