"""Hash-table-resize bottleneck analysis.

Extracted from ``bottleneck.py`` to keep the orchestrator readable.
Responsibilities:
  * Extract per-node hash-resize hotspots from ``extra_metrics``
  * Detect duplicate GROUP BY (same keys across multiple aggregation nodes)
  * Generate hash-resize alerts with skew / AQE-layout / volume diagnosis
"""

from typing import Any

from ..constants import THRESHOLDS, Severity
from ..i18n import gettext as _
from ..models import BottleneckIndicators, HashResizeHotspot, NodeMetrics
from ._helpers import _add_alert


def extract_hash_resize_hotspots(node_metrics: list[NodeMetrics]) -> list[HashResizeHotspot]:
    """Identify per-node hash resize hotspots (skew attribution).

    Rank nodes by resize count and capture their operator type + keys
    so the alert can point at the specific grouping/join column(s).
    """
    hotspots: list[HashResizeHotspot] = []
    for nm in node_metrics:
        resize = nm.extra_metrics.get("Number of times hash table was resized", 0) or 0
        if resize <= 0:
            continue
        keys: list[str] = []
        key_kind = ""
        if nm.join_keys_left or nm.join_keys_right:
            # Pair left and right keys for readability
            pairs = []
            left = nm.join_keys_left or []
            right = nm.join_keys_right or []
            for i in range(max(len(left), len(right))):
                li = left[i] if i < len(left) else "?"
                ri = right[i] if i < len(right) else "?"
                pairs.append(f"{li} ↔ {ri}")
            keys = pairs
            key_kind = "join"
        elif nm.grouping_expressions:
            keys = list(nm.grouping_expressions)
            key_kind = "group"
        hotspots.append(
            HashResizeHotspot(
                node_id=nm.node_id,
                node_tag=nm.node_tag,
                node_name=nm.node_name,
                resize=int(resize),
                probes=float(nm.extra_metrics.get("Avg hash probes per row", 0) or 0),
                keys=keys,
                key_kind=key_kind,
            )
        )
    hotspots.sort(key=lambda h: -h.resize)
    return hotspots[:10]


def detect_duplicate_groupby(indicators: BottleneckIndicators) -> None:
    """Detect duplicate GROUP BY: same grouping keys across multiple aggregation nodes.

    Each such node recomputes the same aggregate. Indicates a missed CTE/Exchange
    reuse or UNION branches that could share a single aggregation.
    """
    group_key_counts: dict[str, list[HashResizeHotspot]] = {}
    for h in indicators.hash_resize_hotspots or []:
        if h.key_kind != "group":
            continue
        if not h.keys:
            continue
        sig = "|".join(sorted(str(k).lower() for k in h.keys))
        group_key_counts.setdefault(sig, []).append(h)

    for _sig, group in group_key_counts.items():
        if len(group) < 2:
            continue
        total_resize = sum(g.resize for g in group)
        if total_resize < THRESHOLDS["duplicate_groupby_min_resize"]:
            continue  # ignore tiny duplicates
        sample_keys = group[0].keys or []
        key_str = ", ".join(str(k) for k in sample_keys[:3])
        if len(sample_keys) > 3:
            key_str += ", ..."
        _add_alert(
            indicators,
            severity=Severity.MEDIUM,
            category="aggregation",
            message=_(
                "Duplicate GROUP BY on [{keys}] across {n} separate aggregation nodes "
                "({total_resize:,} resizes total) — the same aggregate is recomputed"
            ).format(keys=key_str, n=len(group), total_resize=total_resize),
            metric_name="duplicate_aggregation",
            current_value=f"{len(group)} nodes",
            threshold="1 node (unique)",
            recommendation=_(
                "The query likely recomputes the same aggregation multiple times. "
                "Consolidate into a single CTE and reference it where needed; verify "
                "the optimizer materializes it (look for ReusedExchange in EXPLAIN). "
                "For UNION branches with parallel GROUP BY, consider computing the "
                "aggregate once upstream"
            ),
        )


def _build_hotspot_hint(hotspots: list[HashResizeHotspot]) -> str:
    """Return a markdown bullet list of top hotspot contributors."""
    if not hotspots:
        return ""
    # key: (op_tag_cleaned, keys_signature) -> {display_op, display_keys, nodes, total_resize}
    agg: dict[tuple, dict[str, Any]] = {}
    for h in hotspots:
        tag = (h.node_tag or "").replace("PHOTON_", "").replace("_EXEC", "")
        op = tag or h.node_name or "?"
        keys = h.keys or []
        kind = h.key_kind or ""
        sig = (op, tuple(str(k) for k in keys))
        entry = agg.setdefault(
            sig,
            {"op": op, "keys": keys, "kind": kind, "nodes": 0, "resize": 0},
        )
        entry["nodes"] += 1
        entry["resize"] += h.resize

    sorted_entries = sorted(agg.values(), key=lambda e: -e["resize"])[:3]

    bullets: list[str] = []
    for entry in sorted_entries:
        keys = entry["keys"]
        op = entry["op"]
        n = entry["nodes"]
        kind = entry["kind"]
        key_label = "join" if kind == "join" else "group"
        if keys:
            key_str = ", ".join(str(k) for k in keys[:3])
            if len(keys) > 3:
                key_str += ", ..."
            if n > 1:
                bullets.append(
                    f"  - {key_str} ({op} × {n} nodes, {key_label}): {entry['resize']:,} resizes"
                )
            else:
                bullets.append(f"  - {key_str} ({op}, {key_label}): {entry['resize']:,} resizes")
        else:
            if n > 1:
                bullets.append(f"  - {op} × {n} nodes: {entry['resize']:,} resizes")
            else:
                bullets.append(f"  - {op}: {entry['resize']:,} resizes")
    # Markdown hard line break: two trailing spaces + newline.
    # "\n" alone is collapsed to a space by most Markdown renderers.
    nl = "  \n"
    return nl + "Top contributors:" + nl + nl.join(bullets)


def _wording_for_hotspot_kind(hotspots: list[HashResizeHotspot]) -> tuple[str, str, str, str]:
    """Return (where, category, rec_strong, rec_soft) based on dominant hotspot kind.

    Controls alert wording so e.g. GROUPING_AGG skew is not reported as join skew.
    Category drives the alert prefix ("[JOIN]" / "[AGGREGATION]" / "[JOIN/AGGREGATION]")
    so it matches the "where" wording in the message body.
    """
    top_kinds = [h.key_kind for h in hotspots[:3]]
    has_join = any(k == "join" for k in top_kinds)
    has_group = any(k == "group" for k in top_kinds)
    if has_join and has_group:
        return (
            _("join/grouping keys"),
            "join/aggregation",
            _(
                "Investigate key distribution for skew on the hot columns above. "
                "For JOIN skew: enable AQE skew join handling or broadcast the smaller side. "
                "For GROUP BY skew: verify no unintended cardinality (e.g. cross-joined rows) "
                "and consider pre-aggregating upstream"
            ),
            _(
                "Check key distribution for skew. Enable AQE skew handling and pre-aggregate where possible"
            ),
        )
    if has_group:
        return (
            _("grouping keys"),
            "aggregation",
            _(
                "The hot grouping column(s) have extreme cardinality. "
                "First, verify the data itself — unexpected row explosion (e.g. missing filter, "
                "incorrect JOIN producing duplicates) is a common cause. "
                "If data is correct, enable AQE and pre-aggregate upstream to reduce group cardinality"
            ),
            _(
                "Check grouping column(s) for unexpected cardinality. Pre-aggregate or add filters upstream"
            ),
        )
    if has_join:
        return (
            _("join keys"),
            "join",
            _(
                "Investigate join key distribution for skew. "
                "Consider pre-aggregating the build side, enabling AQE skew join handling, "
                "or changing to broadcast join if the smaller side fits in memory"
            ),
            _(
                "Check join key distribution for skew. "
                "Consider enabling AQE skew join handling or broadcast join if the smaller side fits in memory"
            ),
        )
    rec = _("Investigate key distribution. Enable AQE skew handling and consider pre-aggregation")
    return (
        _("hash keys"),
        "join",  # fallback: preserve legacy behavior when kind is unknown
        rec,
        rec,
    )


def generate_hash_resize_alerts(indicators: BottleneckIndicators) -> None:
    """Emit alerts for hash table resize patterns.

    Diagnoses root cause based on combined signals:
      * severe_skew (resize ≥ critical AND probes ≥ critical) → HIGH data-skew alert
      * suspect_skew (high resize + high probes) → MEDIUM suspected-skew alert
      * aqe_layout_not_skew (AQE self-repartitioned + no spill) → MEDIUM layout alert
      * otherwise → MEDIUM generic resize alert
      * high probes alone (not covered above) → MEDIUM probes alert
    """
    hotspots = indicators.hash_resize_hotspots or []
    skew_join_keys_hint = _build_hotspot_hint(hotspots)

    # Brief primer prepended to the hash-resize alerts so readers know
    # what the metric physically represents before the diagnosis text.
    hash_primer = _(
        "Hash resize = Photon doubled and rebuilt its in-memory hash table because "
        "row-count estimates were wrong; many resizes signal a data/structural "
        "issue (not just slowness)."
    )

    has_high_resize = indicators.hash_table_resize_count > 10
    has_high_probes = indicators.avg_hash_probes_per_row > 5
    # If AQE successfully self-repartitioned at least one exchange AND no
    # shuffle spilled, the workload looks like a data-volume/layout issue
    # (which AQE handled correctly at runtime), NOT key skew. Downgrade
    # the skew diagnosis to avoid misleading the reader.
    aqe_layout_not_skew = bool(getattr(indicators, "aqe_self_repartition_seen", False))
    severe_skew = (
        not aqe_layout_not_skew
        and indicators.hash_table_resize_count >= THRESHOLDS["hash_resize_critical"]
        and indicators.avg_hash_probes_per_row >= THRESHOLDS["hash_probes_critical"]
    )
    suspect_skew = (
        not severe_skew
        and not aqe_layout_not_skew
        and indicators.hash_table_resize_count >= THRESHOLDS["hash_resize_high"]
        and indicators.avg_hash_probes_per_row >= THRESHOLDS["hash_probes_high"]
    )

    where, category, rec_strong, rec_soft = _wording_for_hotspot_kind(hotspots)

    if has_high_resize:
        if severe_skew:
            # Both metrics extreme → data skew is the primary cause, not stale stats
            _add_alert(
                indicators,
                severity=Severity.HIGH,
                category=category,
                message=_(
                    "Hash table resized {count} times with {probes} avg probes/row — likely data skew in {where}. {primer}{keys_hint}"
                ).format(
                    count=indicators.hash_table_resize_count,
                    probes=f"{indicators.avg_hash_probes_per_row:.0f}",
                    where=where,
                    primer=hash_primer,
                    keys_hint=skew_join_keys_hint,
                ),
                metric_name="hash_table_resize_count",
                current_value=str(indicators.hash_table_resize_count),
                threshold="<10",
                recommendation=rec_strong,
            )
        elif suspect_skew:
            # Moderate combined signal → skew is the likely cause
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category=category,
                message=_(
                    "Hash table resized {count} times with {probes} avg probes/row — suspected data skew in {where}. {primer}{keys_hint}"
                ).format(
                    count=indicators.hash_table_resize_count,
                    probes=f"{indicators.avg_hash_probes_per_row:.0f}",
                    where=where,
                    primer=hash_primer,
                    keys_hint=skew_join_keys_hint,
                ),
                metric_name="hash_table_resize_count",
                current_value=str(indicators.hash_table_resize_count),
                threshold="<10",
                recommendation=rec_soft,
            )
        elif aqe_layout_not_skew:
            # AQE successfully repartitioned at runtime AND no shuffle spilled.
            # Cause is data volume / physical layout, NOT skew.
            ratio = indicators.max_aqe_partition_growth_ratio
            bytes_gb = indicators.shuffle_bytes_written_total / (1024**3)
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category=category,
                message=_(
                    "Hash table resized {count} times with {probes} avg probes/row, "
                    "but AQE self-repartitioned (×{ratio:.0f}) and no shuffle spilled "
                    "({gb:.1f} GB total) — this is a data-volume / physical-layout "
                    "issue, NOT key skew. {primer}{keys_hint}"
                ).format(
                    count=indicators.hash_table_resize_count,
                    probes=f"{indicators.avg_hash_probes_per_row:.0f}",
                    ratio=ratio,
                    gb=bytes_gb,
                    primer=hash_primer,
                    keys_hint=skew_join_keys_hint,
                ),
                metric_name="hash_table_resize_count",
                current_value=str(indicators.hash_table_resize_count),
                threshold="<10",
                recommendation=_(
                    "AQE handled the volume at runtime; the sustainable fix is to "
                    "improve the physical layout on the hot columns. Consider: "
                    "(1) Liquid Clustering on the hot grouping/join column(s) to "
                    "reduce shuffle; (2) pre-aggregating upstream to cut row count; "
                    "(3) reviewing data types — DECIMAL(38,0) where INTEGER would "
                    "suffice bloats hash/compare/memory per row at this scale. "
                    "Run DESCRIBE <table> and compare column types vs actual values"
                ),
            )
        else:
            # Resize alone → build side exceeded initial estimate (skew, stats, or memory)
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category=category,
                message=_(
                    "Hash table resized {count} times — build side exceeded initial estimate (possible causes: data skew, stale statistics, or memory pressure). {primer}"
                ).format(count=indicators.hash_table_resize_count, primer=hash_primer),
                metric_name="hash_table_resize_count",
                current_value=str(indicators.hash_table_resize_count),
                threshold="<10",
                recommendation=_(
                    "Check join key distribution for skew first. "
                    "If statistics are stale, run ANALYZE TABLE. "
                    "Consider broadcast join if the smaller side is under 200MB"
                ),
            )

    if has_high_probes and not severe_skew and not suspect_skew:
        # High probes alone (not already covered by severe_skew alert)
        _add_alert(
            indicators,
            severity=Severity.MEDIUM,
            category="join",
            message=_(
                "Avg hash probes per row is high ({value}) — indicates join key skew or hash collision"
            ).format(value=f"{indicators.avg_hash_probes_per_row:.1f}"),
            metric_name="avg_hash_probes_per_row",
            current_value=f"{indicators.avg_hash_probes_per_row:.1f}",
            threshold="<5",
            recommendation=_(
                "Check for data skew in join keys; consider salting skewed keys or pre-filtering"
            ),
        )
