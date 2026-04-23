"""Query metrics and stage execution report sections."""

from __future__ import annotations

from ..i18n import gettext as _
from ..models import BottleneckIndicators, QueryMetrics, StageInfo, StreamingContext
from ..utils import format_bytes, format_time_ms


def generate_query_overview(qm: QueryMetrics, *, include_header: bool = True) -> str:
    """Generate Query Overview section (Section 2).

    Args:
        qm: QueryMetrics object

    Returns:
        Markdown formatted Query Overview section
    """
    lines = []
    if include_header:
        lines.append(f"## {_('Query Overview')}\n")
    lines.append(f"| {_('Item')} | {_('Value')} |")
    lines.append("|:-----|:------|")
    lines.append(f"| **{_('Query ID')}** | `{qm.query_id}` |")
    lines.append(f"| **{_('Status')}** | {qm.status} |")
    lines.append(f"| **{_('Total Execution Time')}** | {format_time_ms(qm.total_time_ms)} |")
    lines.append(f"| **{_('Compilation Time')}** | {format_time_ms(qm.compilation_time_ms)} |")
    lines.append(f"| **{_('Execution Time')}** | {format_time_ms(qm.execution_time_ms)} |")
    lines.append(f"| **{_('Rows Read')}** | {qm.rows_read_count:,} |")
    lines.append(f"| **{_('Rows Produced')}** | {qm.rows_produced_count:,} |")
    lines.append("")
    return "\n".join(lines)


def generate_performance_metrics(
    qm: QueryMetrics,
    bi: BottleneckIndicators,
    *,
    include_header: bool = True,
) -> str:
    """Generate Performance Metrics section (Section 3).

    Contains two sub-sections: Time Metrics and I/O Metrics.

    Args:
        qm: QueryMetrics object
        bi: BottleneckIndicators object

    Returns:
        Markdown formatted Performance Metrics section
    """
    lines = []
    if include_header:
        lines.append(f"## {_('Performance Metrics')}\n")

    # 3.1 Time Metrics
    lines.append(f"### {_('Time Metrics')}\n")
    lines.append(f"| {_('Metric')} | {_('Value')} |")
    lines.append("|:-----|------:|")
    lines.append(f"| {_('Total Execution Time')} | {format_time_ms(qm.total_time_ms)} |")
    lines.append(f"| {_('Compilation Time')} | {format_time_ms(qm.compilation_time_ms)} |")
    lines.append(f"| {_('Execution Time')} | {format_time_ms(qm.execution_time_ms)} |")
    lines.append(f"| {_('Task Total Time')} | {format_time_ms(qm.task_total_time_ms)} |")
    lines.append(f"| {_('Photon Total Time')} | {format_time_ms(qm.photon_total_time_ms)} |")
    lines.append(f"| {_('Photon Utilization')} | {bi.photon_ratio:.1%} |")
    lines.append("")

    # 3.2 I/O Metrics
    lines.append(f"### {_('I/O Metrics')}\n")
    lines.append(f"| {_('Metric')} | {_('Value')} | {_('Details')} |")
    lines.append("|:-----|------:|:------|")
    lines.append(f"| {_('Total Read')} | {format_bytes(qm.read_bytes)} | |")

    cache_pct = bi.cache_hit_ratio * 100 if qm.read_bytes > 0 else 0
    lines.append(f"| {_('From Cache')} | {format_bytes(qm.read_cache_bytes)} | {cache_pct:.1f}% |")

    remote_pct = bi.remote_read_ratio * 100 if qm.read_bytes > 0 else 0
    lines.append(
        f"| {_('From Remote')} | {format_bytes(qm.read_remote_bytes)} | {remote_pct:.1f}% |"
    )

    total_files = qm.read_files_count + qm.pruned_files_count
    file_prune_pct = (qm.pruned_files_count / total_files * 100) if total_files > 0 else 0
    lines.append(f"| {_('Files Read')} | {qm.read_files_count:,} | |")
    lines.append(f"| {_('Files Pruned')} | {qm.pruned_files_count:,} | {file_prune_pct:.1f}% |")

    if bi.spill_bytes > 0:
        spill_gb = bi.spill_bytes / (1024**3)
        lines.append(f"| {_('Disk Spill')} | {spill_gb:.2f} GB | |")

    # Cloud Storage Retry Overhead
    csm = bi.cloud_storage_metrics
    if csm.total_request_count > 0 and csm.total_retry_count > 0:
        if csm.total_retry_duration_ms > 0:
            # Real retry overhead detected
            retries_per_req = csm.total_retry_count / csm.total_request_count
            overhead_str = format_time_ms(csm.total_retry_duration_ms)
            if qm.execution_time_ms > 0:
                overhead_pct = csm.total_retry_duration_ms / qm.execution_time_ms * 100
                retry_detail = f"{overhead_pct:.1f}% of exec ({retries_per_req:.1f} retries/req)"
            else:
                retry_detail = f"{retries_per_req:.1f} retries/req"
            lines.append(f"| {_('Cloud Storage Retry')} | +{overhead_str} | {retry_detail} |")
        else:
            # retry_count > 0 but duration = 0 → unverified internal counter
            lines.append(f"| {_('Cloud Storage Retry')} | {_('No overhead detected')} | |")

    # 3.3 Shuffle I/O (only if shuffle data exists)
    if bi.shuffle_bytes_written_total > 0:
        lines.append("")
        lines.append(f"### {_('Shuffle I/O')}\n")
        lines.append(f"| {_('Metric')} | {_('Value')} | {_('Details')} |")
        lines.append("|:-----|------:|:------|")
        lines.append(f"| {_('Shuffle Write')} | {format_bytes(bi.shuffle_bytes_written_total)} | |")
        remote_read = bi.shuffle_remote_bytes_read_total
        local_read = bi.shuffle_local_bytes_read_total
        total_shuffle_read = remote_read + local_read
        if total_shuffle_read > 0:
            remote_pct = remote_read / total_shuffle_read * 100
            lines.append(
                f"| {_('Shuffle Remote Read')} | {format_bytes(remote_read)} | {remote_pct:.1f}% |"
            )
            lines.append(
                f"| {_('Shuffle Local Read')} | {format_bytes(local_read)} | {100 - remote_pct:.1f}% |"
            )
        if qm.read_bytes > 0:
            write_ratio = bi.shuffle_bytes_written_total / qm.read_bytes * 100
            lines.append(f"| {_('Shuffle/Read Ratio')} | {write_ratio:.1f}% | |")

    # 3.4 Write I/O (only if write data exists)
    if qm.write_remote_bytes > 0:
        lines.append("")
        lines.append(f"### {_('Write I/O')}\n")
        lines.append(f"| {_('Metric')} | {_('Value')} | {_('Details')} |")
        lines.append("|:-----|------:|:------|")
        lines.append(f"| {_('Bytes Written')} | {format_bytes(qm.write_remote_bytes)} | |")
        if qm.write_remote_files > 0:
            lines.append(f"| {_('Files Written')} | {qm.write_remote_files:,} | |")
            avg_file_size = qm.write_remote_bytes / qm.write_remote_files
            lines.append(f"| {_('Avg File Size')} | {format_bytes(int(avg_file_size))} | |")
        if qm.write_remote_rows > 0:
            lines.append(f"| {_('Rows Written')} | {qm.write_remote_rows:,} | |")
        if qm.read_bytes > 0:
            write_ratio = qm.write_remote_bytes / qm.read_bytes * 100
            lines.append(f"| {_('Write/Read Ratio')} | {write_ratio:.1f}% | |")

    lines.append("")
    return "\n".join(lines)


def generate_streaming_performance_metrics(
    ctx: StreamingContext,
    batch_stats: dict,
    qm: QueryMetrics,
    *,
    include_header: bool = True,
) -> str:
    """Generate Performance Metrics for streaming queries.

    Primary: per-batch metrics (duration, I/O distribution).
    Secondary: cumulative snapshot (query uptime, total reads) clearly labelled.
    """
    lines: list[str] = []
    if include_header:
        lines.append(f"## {_('Performance Metrics')}\n")

    # Primary: Micro-Batch Duration
    lines.append(f"### {_('Micro-Batch Duration')}\n")
    lines.append(f"| {_('Metric')} | {_('Value')} |")
    lines.append("|:-----|------:|")
    lines.append(f"| {_('Finished Batches')} | {batch_stats['finished_count']} |")
    if batch_stats.get("running_count", 0) > 0:
        lines.append(f"| {_('Running')} | {batch_stats['running_count']} |")
    if batch_stats["finished_count"] > 0:
        lines.append(f"| Min | {format_time_ms(batch_stats['duration_min_ms'])} |")
        lines.append(f"| Avg | {format_time_ms(int(batch_stats['duration_avg_ms']))} |")
        if batch_stats.get("duration_p95_ms", 0) > 0:
            lines.append(f"| P95 | {format_time_ms(batch_stats['duration_p95_ms'])} |")
        lines.append(f"| Max | {format_time_ms(batch_stats['duration_max_ms'])} |")
        if batch_stats.get("duration_cv", 0) > 0:
            lines.append(f"| {_('Duration CV')} | {batch_stats['duration_cv']:.2f} |")
    slow = batch_stats.get("slow_batches", [])
    if slow:
        lines.append(f"| {_('Slow Batches')} | {len(slow)} |")
    lines.append("")

    # Primary: Micro-Batch I/O Distribution
    if batch_stats["finished_count"] > 0:
        lines.append(f"### {_('Micro-Batch I/O Distribution')}\n")
        lines.append(f"| {_('Metric')} | {_('Value')} |")
        lines.append("|:-----|------:|")
        lines.append(
            f"| {_('Read Bytes (min)')} | {format_bytes(batch_stats.get('read_bytes_min', 0))} |"
        )
        lines.append(
            f"| {_('Read Bytes (avg)')} | {format_bytes(batch_stats.get('read_bytes_avg', 0))} |"
        )
        lines.append(
            f"| {_('Read Bytes (max)')} | {format_bytes(batch_stats.get('read_bytes_max', 0))} |"
        )
        lines.append(f"| {_('Rows Read (avg)')} | {batch_stats.get('rows_avg', 0):,.0f} |")
        lines.append("")

    # Secondary: Cumulative Snapshot (clearly labelled)
    lines.append(f"### {_('Cumulative Snapshot')} ({_('query uptime total — not per-batch')})\n")
    lines.append(f"| {_('Metric')} | {_('Value')} |")
    lines.append("|:-----|------:|")
    lines.append(f"| {_('Query Uptime')} | {format_time_ms(qm.total_time_ms)} |")
    if qm.read_bytes > 0:
        lines.append(f"| {_('Total Read (all batches)')} | {format_bytes(qm.read_bytes)} |")
    if qm.task_total_time_ms > 0:
        lines.append(f"| {_('Task Total Time')} | {format_time_ms(qm.task_total_time_ms)} |")
    if qm.photon_total_time_ms > 0:
        lines.append(f"| {_('Photon Total Time')} | {format_time_ms(qm.photon_total_time_ms)} |")
    lines.append("")

    return "\n".join(lines)


def generate_stage_execution_section(
    stages: list[StageInfo], *, include_header: bool = True
) -> str:
    """Generate Stage Execution Analysis section (Section 5).

    Args:
        stages: List of StageInfo objects

    Returns:
        Markdown formatted Stage Execution section
    """
    if not stages:
        return ""

    lines = []
    if include_header:
        lines.append(f"## {_('Stage Execution Analysis')}\n")
    lines.append(
        f"| {_('Stage')} | {_('Status')} | {_('Duration')} | {_('Tasks')} | "
        f"{_('Completed')} | {_('Failed')} | {_('Note')} |"
    )
    lines.append("|:-----:|:------:|------:|------:|------:|------:|:------|")

    for stage in stages:
        duration_str = format_time_ms(stage.duration_ms) if stage.duration_ms > 0 else "-"
        note = stage.note or ""
        if stage.is_failed and not note:
            note = "FAILED"

        lines.append(
            f"| {stage.stage_id} | {stage.status} | {duration_str} | "
            f"{stage.num_tasks} | {stage.num_complete_tasks} | "
            f"{stage.num_failed_tasks} | {note} |"
        )

    lines.append("")
    return "\n".join(lines)
