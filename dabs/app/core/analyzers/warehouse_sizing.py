"""Rule-based SQL warehouse sizing recommendations for query profile analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..dbsql_cost import DBU_PER_HOUR_BY_SIZE
from ..i18n import gettext as _

if TYPE_CHECKING:
    from ..models import QueryMetrics
    from ..warehouse_client import WarehouseInfo

# Ordered T-shirt sizes (ascending DBU/h) — same order as dbsql_cost
_ORDERED_SIZES = list(DBU_PER_HOUR_BY_SIZE.keys())
_SMALL_CLUSTER_SIZES = frozenset({"2X-Small", "X-Small", "Small"})
_SPILL_5GB = 5 * 1024 * 1024 * 1024
_EXEC_5MIN_MS = 300_000
_FAST_EXEC_MS = 30_000


def _severity_rank(sev: str) -> int:
    return {"HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(sev, 0)


def _category_order(cat: str) -> int:
    return {
        "workload_separation": 0,
        "auto_scaling": 1,
        "serverless": 2,
        "upsizing": 3,
        "downsizing": 4,
    }.get(cat, 99)


def _sec_str(ms: int) -> str:
    s = ms / 1000.0
    if s >= 10:
        return f"{s:.0f}"
    return f"{s:.1f}"


@dataclass
class SizingRecommendation:
    """Single warehouse sizing finding."""

    category: str  # workload_separation, auto_scaling, serverless, downsizing, upsizing
    severity: str  # HIGH, MEDIUM, LOW
    summary: str  # 1-line for executive summary
    detail: str  # Multi-line for subsection
    sql_or_action: str  # Concrete action/SQL
    estimated_savings: str  # Cost impact description


def analyze_warehouse_sizing(
    qm: QueryMetrics, wh: WarehouseInfo | None
) -> list[SizingRecommendation]:
    """Derive rule-based warehouse sizing recommendations from metrics and warehouse metadata."""
    out: list[SizingRecommendation] = []

    # --- 1. Workload competition (BI / ETL separation) ---
    q_over = qm.queued_overload_time_ms
    if q_over >= 10_000:
        sev = "HIGH"
    elif q_over >= 3000:
        sev = "MEDIUM"
    else:
        sev = ""

    if sev:
        xs = _sec_str(q_over)
        out.append(
            SizingRecommendation(
                category="workload_separation",
                severity=sev,
                summary=_(
                    "Queue overload {seconds}s detected — consider separating BI and ETL "
                    "workloads to dedicated warehouses"
                ).format(seconds=xs),
                detail=_(
                    "Overload queue time indicates the warehouse may be saturated by mixed "
                    "interactive and batch workloads. Isolating latency-sensitive BI from "
                    "heavy ETL reduces head-of-line blocking."
                ),
                sql_or_action=_(
                    "Create separate warehouses: one for interactive BI (auto-stop 10min), "
                    "one for ETL (auto-stop 5min)"
                ),
                estimated_savings=_(
                    "Cost impact depends on how you split traffic; expect fewer SLA misses "
                    "and more predictable latency rather than automatic DBU reduction."
                ),
            )
        )

    # --- 2. Auto scaling (multi-cluster) ---
    # Require both provisioning wait AND overload queue to reduce false positives
    # from cold-start-only scenarios (provisioning alone can be just startup).
    if (
        wh is not None
        and not wh.is_serverless
        and wh.max_num_clusters == 1
        and qm.queued_provisioning_time_ms >= 3000
        and qm.queued_overload_time_ms >= 1000
    ):
        xp = _sec_str(qm.queued_provisioning_time_ms)
        out.append(
            SizingRecommendation(
                category="auto_scaling",
                severity="MEDIUM",
                summary=_(
                    "Provisioning wait {seconds}s with single-cluster — enable multi-cluster "
                    "auto-scaling"
                ).format(seconds=xp),
                detail=_(
                    "Provisioning queue time with max clusters pinned to 1 suggests new queries "
                    "wait for capacity instead of scaling out. Raising max clusters allows "
                    "the warehouse to add clusters under concurrent load."
                ),
                sql_or_action=_(
                    "Set max_num_clusters based on peak concurrent users (1 cluster per 10–15 users)"
                ),
                estimated_savings=_(
                    "Better concurrency; additional clusters increase DBU/h ceiling when load spikes."
                ),
            )
        )

    # --- 2b. Multi-cluster overload triage ---
    # When multi-cluster is already enabled but overload still occurs,
    # determine whether to scale up (bigger size) or scale out (more clusters).
    if (
        wh is not None
        and not wh.is_serverless
        and wh.max_num_clusters > 1
        and qm.queued_overload_time_ms >= 3000
    ):
        xo = _sec_str(qm.queued_overload_time_ms)
        spill_gb = qm.spill_to_disk_bytes / (1024**3)
        has_skew = getattr(qm, "extra_metrics", {}).get("has_data_skew", False)
        # Check indicators from the query itself
        high_shuffle = (
            qm.task_total_time_ms > 0
            and qm.execution_time_ms > 0
            and (qm.task_total_time_ms - qm.photon_total_time_ms) / max(qm.task_total_time_ms, 1)
            > 0.4
        )
        heavy_spill = qm.spill_to_disk_bytes >= 1 * 1024**3  # >= 1GB
        long_exec = qm.execution_time_ms >= 300_000  # >= 5min
        light_query = qm.execution_time_ms < 300_000 and qm.spill_to_disk_bytes < 100 * 1024 * 1024

        if has_skew or high_shuffle:
            # Data skew / shuffle dominant → query optimization first
            out.append(
                SizingRecommendation(
                    category="concurrent_optimization",
                    severity="MEDIUM",
                    summary=_(
                        "Overload queue {seconds}s with multi-cluster (max={max}) — "
                        "data skew or shuffle dominance detected; optimize query before sizing changes"
                    ).format(seconds=xo, max=wh.max_num_clusters),
                    detail=_(
                        "Overload queue indicates concurrent pressure, but this query shows "
                        "data skew or high shuffle ratio. Sizing changes alone are unlikely to help; "
                        "focus on SQL optimization (pre-aggregation, broadcast hints, skew handling) first."
                    ),
                    sql_or_action=_(
                        "1. Address data skew / shuffle in SQL. "
                        "2. Re-measure overload after optimization. "
                        "3. Then consider sizing changes if overload persists."
                    ),
                    estimated_savings=_(
                        "Query optimization typically has higher ROI than sizing changes when skew is present."
                    ),
                )
            )
        elif heavy_spill or long_exec:
            # Individual query is heavy → scale up
            out.append(
                SizingRecommendation(
                    category="concurrent_scale_up",
                    severity="MEDIUM",
                    summary=_(
                        "Overload queue {seconds}s with multi-cluster (max={max}) + "
                        "heavy query ({exec_s}s, {spill:.1f}GB spill) — consider sizing up"
                    ).format(
                        seconds=xo,
                        max=wh.max_num_clusters,
                        exec_s=_sec_str(qm.execution_time_ms),
                        spill=spill_gb,
                    ),
                    detail=_(
                        "Multi-cluster auto-scaling is enabled but overload queue persists. "
                        "This query is individually heavy (long execution or significant spill), "
                        "suggesting each cluster may lack sufficient resources. "
                        "Consider increasing cluster size one step."
                    ),
                    sql_or_action=_(
                        "Increase warehouse cluster size one step (e.g. {from_size} → next size up). "
                        "Then verify spill reduction and overload queue decrease."
                    ).format(from_size=wh.cluster_size),
                    estimated_savings=_(
                        "Larger clusters reduce per-query memory pressure and spill; "
                        "may also reduce overload by completing queries faster."
                    ),
                )
            )
        elif light_query:
            # Individual query is light → need more clusters
            out.append(
                SizingRecommendation(
                    category="concurrent_scale_out",
                    severity="MEDIUM",
                    summary=_(
                        "Overload queue {seconds}s with multi-cluster (max={max}) — "
                        "query itself is light ({exec_s}s); consider increasing max_num_clusters"
                    ).format(
                        seconds=xo, max=wh.max_num_clusters, exec_s=_sec_str(qm.execution_time_ms)
                    ),
                    detail=_(
                        "Multi-cluster auto-scaling is enabled but overload persists. "
                        "This query completes quickly with minimal resource pressure, "
                        "suggesting the bottleneck is concurrent query volume rather than "
                        "individual query weight. Increase max_num_clusters or separate workloads."
                    ),
                    sql_or_action=_(
                        "Increase max_num_clusters (current: {max}). "
                        "Rule of thumb: 1 cluster per 10-15 concurrent users. "
                        "Also consider workload separation (BI vs ETL on different warehouses)."
                    ).format(max=wh.max_num_clusters),
                    estimated_savings=_(
                        "More clusters reduce queue wait; DBU cost scales with cluster count."
                    ),
                )
            )
        else:
            # Ambiguous — show both options with judgment criteria
            out.append(
                SizingRecommendation(
                    category="concurrent_ambiguous",
                    severity="LOW",
                    summary=_(
                        "Overload queue {seconds}s with multi-cluster (max={max}) — "
                        "check system.query.history to determine if sizing up or scaling out is needed"
                    ).format(seconds=xo, max=wh.max_num_clusters),
                    detail=_(
                        "Overload queue detected but this single query profile is insufficient to "
                        "determine whether the issue is individual query weight or concurrent volume. "
                        "Query system.query.history for the past 7 days to see p50/p90 duration "
                        "and concurrent query count trends."
                    ),
                    sql_or_action=_(
                        "SELECT warehouse_id, DATE(start_time), COUNT(*) AS queries, "
                        "PERCENTILE(total_duration_ms, 0.9)/1000 AS p90_sec "
                        "FROM system.query.history "
                        "WHERE start_time >= NOW() - INTERVAL 7 DAY "
                        "GROUP BY 1, 2 ORDER BY 2"
                    ),
                    estimated_savings=_(
                        "Analyze trends first — sizing changes without data may waste resources."
                    ),
                )
            )

    # --- 3a. Serverless (long provisioning on Pro/Classic) ---
    if (
        wh is not None
        and not wh.is_serverless
        and wh.warehouse_type in ("PRO", "CLASSIC")
        and qm.queued_provisioning_time_ms >= 10_000
    ):
        xp = _sec_str(qm.queued_provisioning_time_ms)
        out.append(
            SizingRecommendation(
                category="serverless",
                severity="HIGH",
                summary=_(
                    "Long provisioning wait ({seconds}s) on Pro/Classic — Serverless eliminates "
                    "cold start"
                ).format(seconds=xp),
                detail=_(
                    "Long provisioning waits often reflect warehouse startup or capacity "
                    "scheduling. Serverless SQL warehouses reduce cold-start friction for many "
                    "workloads; validate fit against your governance and networking constraints."
                ),
                sql_or_action=_(
                    "Evaluate migrating this workload to a Serverless SQL warehouse or "
                    "scheduling queries on warm warehouses"
                ),
                estimated_savings=_(
                    "Fewer wasted minutes waiting in queue; pricing model differs — compare "
                    "per-query Serverless DBU vs Pro/Classic uptime billing."
                ),
            )
        )

    # --- 3b. Classic → Pro / Serverless (Photon) ---
    # Only recommend when execution is long enough to benefit (>60s) to avoid
    # triggering on every Classic warehouse query regardless of workload.
    if (
        wh is not None
        and not wh.is_serverless
        and wh.warehouse_type == "CLASSIC"
        and qm.execution_time_ms >= 60_000
    ):
        out.append(
            SizingRecommendation(
                category="serverless",
                severity="MEDIUM",
                summary=_(
                    "Classic warehouse detected — consider Pro or Serverless for Photon support"
                ),
                detail=_(
                    "Classic warehouses do not run Photon. If this query would benefit from "
                    "Photon vectorization, Pro or Serverless warehouses are the typical upgrade "
                    "paths."
                ),
                sql_or_action=_(
                    "Create a Pro or Serverless warehouse and replay the workload to compare "
                    "Photon utilization and latency"
                ),
                estimated_savings=_(
                    "Potential higher DBU rate with Pro/Serverless offset by faster queries; "
                    "measure wall-clock and DBU per business outcome."
                ),
            )
        )

    # --- 4. Oversizing (downsizing) ---
    if wh is not None and wh.cluster_size:
        cs = wh.cluster_size
        if (
            cs in DBU_PER_HOUR_BY_SIZE
            and _ORDERED_SIZES.index(cs) >= _ORDERED_SIZES.index("Large")
            and qm.execution_time_ms < _FAST_EXEC_MS
            and qm.spill_to_disk_bytes < 100 * 1024 * 1024  # allow <100MB micro-spill
            and qm.queued_overload_time_ms == 0
            and qm.queued_provisioning_time_ms < 5000  # not waiting for capacity
        ):
            smaller = _ORDERED_SIZES[_ORDERED_SIZES.index(cs) - 1]
            dbu_hi = DBU_PER_HOUR_BY_SIZE[cs]
            dbu_lo = DBU_PER_HOUR_BY_SIZE[smaller]
            pct = 0.0
            if dbu_hi > 0:
                pct = round((1.0 - dbu_lo / dbu_hi) * 100.0, 0)
            xs = _sec_str(qm.execution_time_ms)
            out.append(
                SizingRecommendation(
                    category="downsizing",
                    severity="MEDIUM",
                    summary=_(
                        "Query completes in {seconds}s on {size} with no spill/queue — "
                        "consider downsizing to {smaller_size}"
                    ).format(seconds=xs, size=cs, smaller_size=smaller),
                    detail=_(
                        "Short execution with no disk spill and no overload queue suggests the "
                        "allocated warehouse capacity may exceed steady-state needs. Validate "
                        "with repeated runs and peak concurrency before shrinking."
                    ),
                    sql_or_action=_(
                        "Reduce cluster size one step (e.g. {from_size} → {to_size}) and re-measure "
                        "p95 latency and spill"
                    ).format(from_size=cs, to_size=smaller),
                    estimated_savings=_(
                        "Reference DBU/h drops from {hi} to {lo} (~{pct}% lower DBU/h at full "
                        "utilization of that size)"
                    ).format(hi=dbu_hi, lo=dbu_lo, pct=int(pct)),
                )
            )

    # --- 5. Undersizing (bonus) ---
    if wh is not None and wh.cluster_size in _SMALL_CLUSTER_SIZES:
        heavy_spill = qm.spill_to_disk_bytes > _SPILL_5GB
        long_run = qm.execution_time_ms > _EXEC_5MIN_MS
        if heavy_spill or long_run:
            spill_gb = qm.spill_to_disk_bytes / (1024**3)
            xs = _sec_str(qm.execution_time_ms)
            if heavy_spill and long_run:
                summ = _(
                    "Heavy query ({seconds}s, {spill_gb:.1f} GB spill) on {size} — "
                    "consider scaling up"
                ).format(seconds=xs, spill_gb=spill_gb, size=wh.cluster_size)
            elif heavy_spill:
                summ = _(
                    "Large disk spill ({spill_gb:.1f} GB) on {size} — consider scaling up"
                ).format(spill_gb=spill_gb, size=wh.cluster_size)
            else:
                summ = _("Long-running query ({seconds}s) on {size} — consider scaling up").format(
                    seconds=xs, size=wh.cluster_size
                )
            out.append(
                SizingRecommendation(
                    category="upsizing",
                    severity="HIGH" if heavy_spill and long_run else "MEDIUM",
                    summary=summ,
                    detail=_(
                        "Large spill or long runtime on the smallest warehouse tiers often "
                        "indicates memory pressure or insufficient parallelism for the working set."
                    ),
                    sql_or_action=_(
                        "Increase warehouse size one step or enable multi-cluster scaling; "
                        "then re-check spill and total time"
                    ),
                    estimated_savings=_(
                        "Higher DBU/h while undersized can still reduce wall-clock and total DBU "
                        "per successful query if spill drops sharply"
                    ),
                )
            )

    out.sort(key=lambda r: (-_severity_rank(r.severity), _category_order(r.category), r.summary))
    return out


# v6.7.12 (Codex review): the subsection used to print "Current
# warehouse configuration appears appropriate" whenever ``recs`` was
# empty — but ``recs`` is empty for the common case where the SP
# cannot fetch ``warehouse_info`` from the API, so the badge was
# effectively a permanent "no change needed" lie that contradicted
# the 3-band ``SizingRecommendation`` widget and the LLM's Section 7
# "scale up" recommendation. New rules:
#   1. 3-band recommendation present → render it as the primary section.
#   2. ``recs`` (B-list, multi-cluster / overload heuristics) present →
#      append as supplementary observations.
#   3. Both empty → "insufficient information" (NOT "no change needed").
#   4. "✅ no change needed" only when ``warehouse_info`` is present
#      AND its cluster_size matches the 3-band ``recommended`` band.
#      ``minimum_viable`` does NOT count as appropriate — it is the
#      operational floor, not a recommendation.


def _three_band_summary_line(rec: "SizingRecommendation") -> str:
    """One-line summary of the 3-band recommendation for executive bullets."""
    parts = [_("Recommended {size}").format(size=rec.recommended.cluster_size)]
    if rec.recommended.cluster_size != rec.minimum_viable.cluster_size:
        parts.append(
            _("(min viable {size})").format(size=rec.minimum_viable.cluster_size)
        )
    if rec.confidence:
        parts.append(_("confidence: {c}").format(c=rec.confidence))
    return " ".join(parts)


def _is_size_appropriate(
    three_band: "SizingRecommendation | None",
    warehouse_info: "WarehouseInfo | None",
) -> bool:
    """``True`` only when warehouse_info is present AND its cluster_size
    matches the 3-band ``recommended`` band. ``minimum_viable`` does NOT
    count — that is the operational floor, not a recommendation."""
    if three_band is None or warehouse_info is None:
        return False
    current = getattr(warehouse_info, "cluster_size", "") or ""
    return current == three_band.recommended.cluster_size


def format_warehouse_sizing_executive_bullets(
    recs: list[SizingRecommendation],
    three_band: "SizingRecommendation | None" = None,
    warehouse_info: "WarehouseInfo | None" = None,
) -> str:
    """Markdown fragment: bullet list of sizing summaries for Section 1."""
    lines = [f"**{_('Warehouse Sizing')}:**\n"]
    if _is_size_appropriate(three_band, warehouse_info):
        lines.append(
            f"- ✅ {_('Current warehouse size matches the recommended band — no change needed.')}"
        )
    elif three_band is not None:
        lines.append(f"- 📊 **{_three_band_summary_line(three_band)}**")
    if recs:
        for r in recs:
            lines.append(f"- **[{r.severity}]** {r.summary}")
    elif three_band is None:
        lines.append(
            f"- ℹ️ {_('Insufficient signal for a sizing recommendation (warehouse info unavailable and the workload does not show clear under/over-utilization patterns).')}"
        )
    lines.append("")
    return "\n".join(lines)


def format_warehouse_sizing_subsection(
    recs: list[SizingRecommendation],
    three_band: "SizingRecommendation | None" = None,
    warehouse_info: "WarehouseInfo | None" = None,
) -> str:
    """Markdown subsection under Performance Metrics (after cost estimation)."""
    parts: list[str] = [f"### {_('Warehouse Sizing Recommendations')}\n"]

    if _is_size_appropriate(three_band, warehouse_info):
        parts.append(
            f"✅ {_('Current warehouse size matches the recommended band — no change needed.')}\n\n"
        )
    elif three_band is not None:
        # Primary: 3-band. Mirrors the cost-estimation widget's table
        # so this section reads as the canonical sizing output without
        # forcing the reader to scroll back.
        rec = three_band.recommended
        mv = three_band.minimum_viable
        ov = three_band.oversized_beyond
        parts.append(
            f"📊 **{_('Recommended size')}**: {rec.cluster_size} "
            f"({rec.dbu_per_hour} DBU/h, {_('confidence')}: {three_band.confidence})\n"
        )
        if mv.cluster_size != rec.cluster_size:
            parts.append(
                f"- {_('Minimum viable')}: {mv.cluster_size} ({mv.dbu_per_hour} DBU/h)\n"
            )
        if ov is not None:
            parts.append(
                f"- {_('Oversized beyond')}: {ov.cluster_size} ({ov.dbu_per_hour} DBU/h)\n"
            )
        if warehouse_info is not None and getattr(warehouse_info, "cluster_size", ""):
            parts.append(f"- {_('Current')}: {warehouse_info.cluster_size}\n")
        if three_band.rationale:
            parts.append(f"\n**{_('Rationale')}:**\n")
            for r in three_band.rationale:
                parts.append(f"- {r}\n")
        if three_band.scope_notice:
            parts.append(f"\n> {three_band.scope_notice}\n")
        parts.append("")
    elif not recs:
        parts.append(
            f"ℹ️ {_('Insufficient signal for a sizing recommendation. Configure DATABRICKS_HOST / DATABRICKS_TOKEN so the warehouse API can be reached, or analyze a longer-running profile to surface parallelism / spill signals.')}\n\n"
        )
        return "\n".join(parts)

    if recs:
        # B (specialised heuristics: multi-cluster / overload / cold
        # start) — supplementary, never overrides the 3-band primary.
        parts.append(f"\n#### {_('Additional observations')}\n")
        for r in recs:
            badge = "🔴" if r.severity == "HIGH" else "🟠" if r.severity == "MEDIUM" else "🟡"
            parts.append(f"##### {badge} **[{r.severity}]** {r.summary}\n")
            parts.append(f"{r.detail}\n")
            parts.append(f"**{_('Recommended action')}:** {r.sql_or_action}\n")
            if r.estimated_savings:
                parts.append(f"**{_('Cost impact')}:** {r.estimated_savings}\n")
            parts.append("")
    return "\n".join(parts)
