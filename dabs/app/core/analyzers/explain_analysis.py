"""EXPLAIN plan analysis and Photon blocker detection."""

from typing import Any

from ..constants import Severity
from ..explain_parser import ExplainExtended, NodeFamily, extract_scan_table_name
from ..i18n import gettext as _
from ..models import BottleneckIndicators, PhotonBlocker
from ._helpers import _add_alert


def enhance_bottleneck_with_explain(
    indicators: BottleneckIndicators,
    explain: ExplainExtended,
) -> BottleneckIndicators:
    """Enhance bottleneck indicators with insights from EXPLAIN analysis.

    This function combines JSON profiler metrics with EXPLAIN plan analysis
    to provide more accurate and actionable bottleneck detection.

    Args:
        indicators: Existing bottleneck indicators from JSON analysis
        explain: Parsed EXPLAIN EXTENDED output

    Returns:
        Enhanced BottleneckIndicators with additional insights
    """
    physical = explain.get_section("Physical Plan")
    stats_section = explain.get_section("Optimizer Statistics (table names per statistics state)")

    # Use parsed optimizer statistics if available (new structured approach)
    if explain.optimizer_statistics:
        opt_stats = explain.optimizer_statistics
        if opt_stats.missing_tables:
            tables_str = ", ".join(opt_stats.missing_tables)
            recommendations = [
                _("Run ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS").format(table=t)
                for t in opt_stats.missing_tables
            ]
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="statistics",
                message=_("Tables missing optimizer statistics: {tables}").format(
                    tables=tables_str
                ),
                metric_name="missing_stats_tables",
                current_value=str(len(opt_stats.missing_tables)),
                threshold="0",
                recommendation=recommendations[0]
                if len(recommendations) == 1
                else _("Run ANALYZE TABLE for: {tables}").format(tables=tables_str),
            )

        if opt_stats.partial_tables:
            _add_alert(
                indicators,
                severity=Severity.INFO,
                category="statistics",
                message=_("Tables with partial optimizer statistics: {tables}").format(
                    tables=", ".join(opt_stats.partial_tables)
                ),
                metric_name="partial_stats_tables",
                current_value=str(len(opt_stats.partial_tables)),
                threshold="0",
                is_actionable=False,
            )
        # Skip legacy parsing if new parser found stats
        stats_section = None

    # Use parsed exchange info for shuffle analysis
    if explain.exchanges:
        [e for e in explain.exchanges if e.partitioning_type == "hash"]
        [e for e in explain.exchanges if e.partitioning_type == "range"]

        # Check for high partition counts that might cause overhead
        high_partition_exchanges = [e for e in explain.exchanges if e.num_partitions > 400]
        if high_partition_exchanges:
            _add_alert(
                indicators,
                severity=Severity.INFO,
                category="shuffle",
                message=_("Exchange operations with high partition count (>{count})").format(
                    count=400
                ),
                metric_name="high_partition_exchanges",
                current_value=str(len(high_partition_exchanges)),
                threshold="0",
                is_actionable=False,
            )

    # Use parsed relation info to enhance table information
    if explain.relations:
        for rel in explain.relations:
            # Check for wide tables (many columns scanned)
            if len(rel.columns) > 50:
                _add_alert(
                    indicators,
                    severity=Severity.INFO,
                    category="io",
                    message=_(
                        "Table `{table}` has {count} columns scanned - consider SELECT specific columns"
                    ).format(table=rel.table_name, count=len(rel.columns)),
                    metric_name="wide_table_scan",
                    current_value=str(len(rel.columns)),
                    threshold="<50",
                    is_actionable=False,
                )

    # 1. Check for missing optimizer statistics
    if stats_section and stats_section.lines:
        import re

        missing_stats_tables: list[str] = []
        partial_stats_tables: list[str] = []

        # The section can come in multiple shapes:
        # - Multi-line with each state on its own line
        # - Single-line flattened output (e.g., copied from UI) containing
        #   "missing = ... partial = ... full = ..."
        normalized_stats_text = " ".join(stats_section.lines)
        normalized_stats_text = " ".join(normalized_stats_text.split())

        # Parse compact "state = tables" groups first.
        # Example: "missing = partial = lineitem full ="
        for state, tables_blob in re.findall(
            r"\b(missing|partial|full)\s*=\s*([^=]*?)(?=\b(?:missing|partial|full)\s*=|$)",
            normalized_stats_text,
            flags=re.IGNORECASE,
        ):
            tables = tables_blob.strip().strip(",")
            table_list = [t.strip() for t in tables.split(",") if t.strip()]
            state_lower = state.lower()
            if state_lower == "missing" and table_list:
                missing_stats_tables.extend(table_list)
            elif state_lower == "partial" and table_list:
                partial_stats_tables.extend(table_list)

        # Fallback: parse line-oriented format.
        if not missing_stats_tables and not partial_stats_tables:
            for line in stats_section.lines:
                line = " ".join(line.strip().split())
                if line.lower().startswith("missing"):
                    tables = line[len("missing") :].strip().lstrip("=").strip()
                    if tables:
                        missing_stats_tables = [t.strip() for t in tables.split(",") if t.strip()]
                elif line.lower().startswith("partial"):
                    tables = line[len("partial") :].strip().lstrip("=").strip()
                    if tables:
                        partial_stats_tables = [t.strip() for t in tables.split(",") if t.strip()]

        # Deduplicate while preserving order
        def dedupe_keep_order(items: list[str]) -> list[str]:
            seen: set[str] = set()
            out: list[str] = []
            for item in items:
                if item not in seen:
                    out.append(item)
                    seen.add(item)
            return out

        missing_stats_tables = dedupe_keep_order(missing_stats_tables)
        partial_stats_tables = dedupe_keep_order(partial_stats_tables)

        if missing_stats_tables:
            tables_str = ", ".join(missing_stats_tables)
            recommendations = [
                _("Run ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS").format(table=t)
                for t in missing_stats_tables
            ]
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="statistics",
                message=_("Tables missing optimizer statistics: {tables}").format(
                    tables=tables_str
                ),
                metric_name="missing_stats_tables",
                current_value=str(len(missing_stats_tables)),
                threshold="0",
                recommendation=recommendations[0]
                if len(recommendations) == 1
                else _("Run ANALYZE TABLE for: {tables}").format(tables=tables_str),
            )

        if partial_stats_tables:
            _add_alert(
                indicators,
                severity=Severity.INFO,
                category="statistics",
                message=_("Tables with partial optimizer statistics: {tables}").format(
                    tables=", ".join(partial_stats_tables)
                ),
                metric_name="partial_stats_tables",
                current_value=str(len(partial_stats_tables)),
                threshold="0",
                is_actionable=False,
            )

    # 2. Check for partition filter pushdown issues
    if physical and physical.nodes:
        scans = [n for n in physical.nodes if n.family == NodeFamily.SCAN]
        for scan in scans:
            part_filters = scan.attrs.get("PartitionFilters", "")
            data_filters = scan.attrs.get("DataFilters", "")

            # Extract table name via structured parser (handles backticks, quoted identifiers)
            table_name = extract_scan_table_name(scan) or "unknown"

            # Check if partition filters are empty but data filters exist
            if not part_filters and data_filters:
                # This might indicate partition pruning is not being used
                if indicators.filter_rate < 0.5:  # Low pruning rate
                    _add_alert(
                        indicators,
                        severity=Severity.MEDIUM,
                        category="io",
                        message=_("Partition filter not used for table `{table}`").format(
                            table=table_name
                        ),
                        metric_name="partition_filter",
                        current_value="not used",
                        threshold="should be used",
                        recommendation=_("Check partitioning strategy for table `{table}`").format(
                            table=table_name
                        ),
                    )

    # 3. Check join strategy vs actual performance
    if physical and physical.nodes:
        joins = [n for n in physical.nodes if n.family == NodeFamily.JOIN]
        exchanges = [n for n in physical.nodes if n.family == NodeFamily.EXCHANGE]

        # Many exchanges with broadcast joins might indicate suboptimal broadcast threshold
        broadcast_joins = [j for j in joins if "Broadcast" in j.node_name]
        shuffle_exchanges = [
            e for e in exchanges if "Sink" in e.node_name and "SinglePartition" not in e.raw_line
        ]

        if len(broadcast_joins) > 0 and len(shuffle_exchanges) > len(broadcast_joins):
            # More shuffle exchanges than broadcast joins - might need larger broadcast threshold
            if indicators.shuffle_impact_ratio > 0.1:  # Shuffle is impactful
                _add_alert(
                    indicators,
                    severity=Severity.MEDIUM,
                    category="shuffle",
                    message=_("Shuffle exchanges are occurring despite broadcast joins"),
                    metric_name="shuffle_with_broadcast",
                    current_value=f"{len(shuffle_exchanges)} shuffles, {len(broadcast_joins)} broadcasts",
                    threshold="shuffles <= broadcasts",
                    recommendation=_(
                        "Consider increasing spark.sql.autoBroadcastJoinThreshold to 200MB or more"
                    ),
                )

        # Check for Sort Merge Join in EXPLAIN (Photon非対応)
        smj_in_explain = [j for j in joins if "SortMerge" in j.node_name]
        if smj_in_explain:
            recommendation = ""
            if indicators.photon_ratio < 0.8:
                recommendation = _(
                    "Set spark.sql.join.preferSortMergeJoin=false or use /*+ SHUFFLE_HASH(table) */ hint"
                )
            _add_alert(
                indicators,
                severity=Severity.CRITICAL,
                category="join",
                message=_(
                    "Sort Merge Join detected in {count} places (not supported by Photon)"
                ).format(count=len(smj_in_explain)),
                metric_name="sort_merge_join_count_explain",
                current_value=str(len(smj_in_explain)),
                threshold="0",
                recommendation=recommendation,
            )

    # 4. Check aggregation strategy
    if physical and physical.nodes:
        aggs = [n for n in physical.nodes if n.family == NodeFamily.AGG]
        if aggs:
            has_partial = any("partial" in n.raw_line.lower() for n in aggs)
            has_final = any("final" in n.raw_line.lower() for n in aggs)

            if len(aggs) > 1 and not (has_partial and has_final):
                # Multiple aggregations without partial/final optimization
                _add_alert(
                    indicators,
                    severity=Severity.INFO,
                    category="agg",
                    message=_(
                        "Multiple aggregation stages but Partial/Final optimization not detected"
                    ),
                    metric_name="agg_optimization",
                    current_value=f"{len(aggs)} stages",
                    threshold="partial/final",
                    is_actionable=False,
                )

    # 5. Check exchange count correlation with spill
    if physical and physical.nodes:
        exchanges = [n for n in physical.nodes if n.family == NodeFamily.EXCHANGE]
        if len(exchanges) > 4 and indicators.spill_bytes > 1024**3:  # Many exchanges + spill > 1GB
            _add_alert(
                indicators,
                severity=Severity.HIGH,
                category="shuffle",
                message=_("Many Exchange operations ({count}) occurring with disk spill").format(
                    count=len(exchanges)
                ),
                metric_name="exchange_with_spill",
                current_value=f"{len(exchanges)} exchanges",
                threshold="<4 with spill",
                recommendation=_(
                    "Consider adjusting shuffle partition count: increase spark.sql.shuffle.partitions"
                ),
            )

    # 6. Check for Photon support from EXPLAIN (use new parsed structure if available)
    if explain.photon_explanation:
        photon_exp = explain.photon_explanation
        if not photon_exp.fully_supported:
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="photon",
                message=_("EXPLAIN indicates query is not fully supported by Photon"),
                metric_name="photon_support",
                current_value="partial/none",
                threshold="fully supported",
            )

            # Add specific Photon blockers from parsed unsupported items
            for item in photon_exp.unsupported_items:
                blocker = PhotonBlocker(
                    reason=item.category or "Unsupported operation",
                    unsupported_expression=item.expression,
                    detail_message=item.detail or item.reason,
                    count=1,
                    impact="HIGH" if "aggregation" in item.category.lower() else "MEDIUM",
                )
                # Avoid duplicates
                existing_exprs = {pb.unsupported_expression for pb in indicators.photon_blockers}
                if item.expression not in existing_exprs:
                    indicators.photon_blockers.append(blocker)

            # Add reference nodes as context
            if photon_exp.reference_nodes:
                for ref_node in photon_exp.reference_nodes[:3]:  # Limit to 3
                    # Try to find matching blocker to add reference node info
                    for blocker in indicators.photon_blockers:
                        if not blocker.detail_message and blocker.unsupported_expression:
                            blocker.detail_message = f"Reference: {ref_node[:100]}"
                            break

        elif indicators.photon_ratio < 0.9:
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="photon",
                message=_("EXPLAIN shows full Photon support but actual Photon utilization is low"),
                metric_name="photon_mismatch",
                current_value=f"{indicators.photon_ratio:.1%}",
                threshold=">90%",
                recommendation=_("Verify that the query is running on a Photon-enabled cluster"),
            )
    else:
        # Fallback to legacy parsing
        photon_section = explain.get_section("Photon Explanation")
        if photon_section and photon_section.lines:
            photon_text = " ".join(photon_section.lines).lower()
            if "not supported" in photon_text or "partially supported" in photon_text:
                _add_alert(
                    indicators,
                    severity=Severity.MEDIUM,
                    category="photon",
                    message=_("EXPLAIN indicates query is not fully supported by Photon"),
                    metric_name="photon_support",
                    current_value="partial/none",
                    threshold="fully supported",
                )
                # Extract specific Photon blocker reasons from the section
                _extract_photon_blockers_from_explain(indicators, photon_section.lines)
            elif "fully supported" in photon_text and indicators.photon_ratio < 0.9:
                _add_alert(
                    indicators,
                    severity=Severity.MEDIUM,
                    category="photon",
                    message=_(
                        "EXPLAIN shows full Photon support but actual Photon utilization is low"
                    ),
                    metric_name="photon_mismatch",
                    current_value=f"{indicators.photon_ratio:.1%}",
                    threshold=">90%",
                    recommendation=_(
                        "Verify that the query is running on a Photon-enabled cluster"
                    ),
                )

    # 7. Check for Window operations (often not fully Photon-supported)
    if physical and physical.nodes:
        window_nodes = [n for n in physical.nodes if n.family == NodeFamily.WINDOW]
        if window_nodes:
            non_photon_windows = [w for w in window_nodes if "Photon" not in w.node_name]
            if non_photon_windows:
                indicators.photon_blockers.append(
                    PhotonBlocker(
                        reason="Window with complex frame",
                        count=len(non_photon_windows),
                        impact="MEDIUM",
                        action="Consider query rewrite to simplify window functions",
                    )
                )

    # 8. Detect write operator Photon fallback (AppendDataExec UNIMPLEMENTED_OPERATOR)
    if physical and physical.nodes:
        import re as _re

        _re_write_op = _re.compile(r"AppendData|WriteToDataSource", _re.IGNORECASE)
        for node in physical.nodes:
            raw = getattr(node, "raw_line", "") or node.node_name
            if _re_write_op.search(raw):
                if "UNIMPLEMENTED" in raw.upper() or "NotSupportedByPhoton" in raw:
                    indicators.write_fallback_detected = True
                    _add_alert(
                        indicators,
                        severity=Severity.INFO,
                        category="photon",
                        message=_(
                            "Write operator `{op}` falls back to non-Photon execution"
                        ).format(op=node.node_name[:60]),
                        metric_name="write_photon_fallback",
                        current_value="UNIMPLEMENTED_OPERATOR",
                        threshold="Photon supported",
                        recommendation=_(
                            "Write fallback is expected for some write operations; low impact for read-heavy queries"
                        ),
                    )
                    break  # One alert is enough

    # 9. Detect excessive CAST operations in physical plan
    if physical and physical.nodes:
        import re as _re2

        _re_cast = _re2.compile(r"cast\(\w+#\d+\s+as\s+\w+\)", _re2.IGNORECASE)
        total_casts = 0
        join_filter_casts = 0
        for node in physical.nodes:
            raw = getattr(node, "raw_line", "") or ""
            casts_in_line = len(_re_cast.findall(raw))
            if casts_in_line > 0:
                total_casts += casts_in_line
                name_upper = node.node_name.upper()
                if any(kw in name_upper for kw in ("JOIN", "FILTER", "SCAN")):
                    join_filter_casts += casts_in_line
        indicators.cast_count = total_casts
        indicators.cast_in_join_filter = join_filter_casts
        if join_filter_casts >= 1 and total_casts >= 5:
            _add_alert(
                indicators,
                severity=Severity.MEDIUM,
                category="execution_plan",
                message=_(
                    "Type cast operations detected in join/filter context ({jf} in join/filter, {total} total)"
                ).format(jf=join_filter_casts, total=total_casts),
                metric_name="cast_in_join_filter",
                current_value=str(join_filter_casts),
                threshold="0",
                recommendation=_(
                    "Align column data types at the source to avoid implicit casts that may block predicate pushdown"
                ),
            )
        elif total_casts >= 20:
            _add_alert(
                indicators,
                severity=Severity.INFO,
                category="execution_plan",
                message=_("High number of type cast operations detected ({total} total)").format(
                    total=total_casts
                ),
                metric_name="cast_count",
                current_value=str(total_casts),
                threshold="20",
                recommendation=_(
                    "Review data types at source tables to reduce unnecessary type conversions"
                ),
            )

    # 10. Detect multi-column partitioning strategy
    if physical and physical.nodes:
        import re as _re3

        _re_part_col = _re3.compile(r"isnotnull\((\w+)#\d+\)")
        max_part_cols: set[str] = set()
        for node in physical.nodes:
            raw = getattr(node, "raw_line", "") or ""
            if "PartitionFilters" in raw:
                cols = set(_re_part_col.findall(raw))
                if len(cols) > len(max_part_cols):
                    max_part_cols = cols
        if max_part_cols:
            indicators.partition_column_count = len(max_part_cols)
            indicators.partition_columns = sorted(max_part_cols)
            if len(max_part_cols) >= 3 and (
                indicators.bytes_pruning_ratio < 0.3 or indicators.filter_rate < 0.3
            ):
                _add_alert(
                    indicators,
                    severity=Severity.INFO,
                    category="io",
                    message=_(
                        "Table uses {n}-column partitioning ({cols}); consider Liquid Clustering for more flexible data layout"
                    ).format(
                        n=len(max_part_cols),
                        cols=", ".join(sorted(max_part_cols)),
                    ),
                    metric_name="partition_column_count",
                    current_value=str(len(max_part_cols)),
                    threshold="3",
                    recommendation=_(
                        "Liquid Clustering adapts to query patterns automatically and may improve pruning efficiency"
                    ),
                )

    # 11. Refine hash join alerts based on optimizer statistics
    # When all tables have full statistics, "stale statistics" is not the cause
    # of hash table resize — strengthen the data skew diagnosis and direct the
    # user to verify data/query correctness (unexpected cardinality).
    opt_stats_refine = explain.optimizer_statistics
    if (
        opt_stats_refine
        and not opt_stats_refine.missing_tables
        and not opt_stats_refine.partial_tables
        and opt_stats_refine.full_tables
    ):
        # Mark statistics as confirmed fresh so downstream (action card
        # generation, LLM merge) can suppress ANALYZE TABLE recommendations
        # and surface alternative-cause guidance instead.
        indicators.statistics_confirmed_fresh = True
        # Build a hotspot hint so the override carries the same per-node detail
        # the original alert produced. Aggregates by key signature so duplicate
        # nodes on the same column collapse into one bullet with a node count.
        from typing import Any as _Any

        _hotspots = indicators.hash_resize_hotspots or []
        _hs_hint = ""
        if _hotspots:
            _agg: dict[tuple, dict[str, _Any]] = {}
            for h in _hotspots:
                _tag = (h.node_tag or "").replace("PHOTON_", "").replace("_EXEC", "")
                _op = _tag or h.node_name or "?"
                _keys = h.keys or []
                _kind = h.key_kind or ""
                _sig = (_op, tuple(str(k) for k in _keys))
                entry = _agg.setdefault(
                    _sig,
                    {
                        "op": _op,
                        "keys": _keys,
                        "kind": _kind,
                        "nodes": 0,
                        "resize": 0,
                    },
                )
                entry["nodes"] += 1
                entry["resize"] += h.resize
            _sorted = sorted(_agg.values(), key=lambda e: -e["resize"])[:3]
            _bullets: list[str] = []
            for entry in _sorted:
                _keys = entry["keys"]
                _op = entry["op"]
                _n = entry["nodes"]
                _kind = entry["kind"]
                _label = "join" if _kind == "join" else "group"
                if _keys:
                    _ks = ", ".join(str(k) for k in _keys[:3])
                    if len(_keys) > 3:
                        _ks += ", ..."
                    if _n > 1:
                        _bullets.append(
                            f"  - {_ks} ({_op} × {_n} nodes, {_label}): {entry['resize']:,} resizes"
                        )
                    else:
                        _bullets.append(f"  - {_ks} ({_op}, {_label}): {entry['resize']:,} resizes")
                else:
                    if _n > 1:
                        _bullets.append(f"  - {_op} × {_n} nodes: {entry['resize']:,} resizes")
                    else:
                        _bullets.append(f"  - {_op}: {entry['resize']:,} resizes")
            _nl = "  \n"
            _hs_hint = _nl + "Top contributors:" + _nl + _nl.join(_bullets)

        # All tables have full statistics — ANALYZE TABLE is not needed.
        _primer = _(
            "Hash resize = Photon doubled and rebuilt its in-memory hash table because "
            "row-count estimates were wrong; many resizes signal a data/structural "
            "issue (not just slowness)."
        )
        for alert in indicators.alerts:
            if alert.metric_name == "hash_table_resize_count":
                # Override regardless of message text — metric_name is the stable key
                alert.message = _(
                    "Hash table resized {count} times — statistics are up-to-date (full), "
                    "suggesting unexpected data distribution or cardinality. {primer}{hint}"
                ).format(
                    count=indicators.hash_table_resize_count,
                    primer=_primer,
                    hint=_hs_hint,
                )
                alert.recommendation = _(
                    "Statistics are confirmed up-to-date, so ANALYZE TABLE will not help. "
                    "First, verify the result set size matches expectations — a cardinality "
                    "explosion often points to a missing JOIN predicate, wrong JOIN key, or "
                    "missing WHERE filter. If data is correct, investigate join key skew "
                    "(enable AQE skew join handling, pre-aggregate the build side, or "
                    "switch to broadcast join if the smaller side fits in memory)"
                )

    # 12. EXPLAIN v2 insights — emit richer diagnostics from Phase-1 signals.
    _apply_explain_v2_insights(indicators, explain)

    return indicators


def _apply_explain_v2_insights(
    indicators: BottleneckIndicators,
    explain: ExplainExtended,
) -> None:
    """Emit alerts and populate counters driven by the Phase-1 v2 insights.

    Signals handled:
    - Implicit CAST on JOIN key → CRITICAL (type-mismatch evidence)
    - CTE referenced >= 2 times but not ReusedExchange → HIGH (re-compute)
    - Photon fallback operators in physical plan → HIGH
    - Scan with PartitionFilters: [] and non-empty DataFilters → MEDIUM
    - Single aggregate without partial/final split → INFO
    """
    # ------------------------------------------------------------------
    # Implicit CAST on JOIN key — direct type-mismatch evidence
    # v5.16.21: profile-only detector in bottleneck.py may have already
    # fired this alert from NodeMetrics.join_keys_left/right. Skip
    # re-firing to avoid duplicates; the flag is the signal of whether
    # the profile path already raised it.
    # ------------------------------------------------------------------
    join_casts = [c for c in explain.implicit_cast_sites if c.context == "join"]
    if join_casts and not indicators.implicit_cast_on_join_key:
        indicators.implicit_cast_on_join_key = True
        # Summarize up to 3 sites for the alert body
        examples = []
        for c in join_casts[:3]:
            col = c.column_ref or "?"
            ty = c.to_type or "?"
            examples.append(f"{col} → {ty}")
        extra = f" (+{len(join_casts) - 3} more)" if len(join_casts) > 3 else ""
        _add_alert(
            indicators,
            severity=Severity.CRITICAL,
            category="join",
            message=_(
                "Implicit CAST detected on JOIN key(s): {examples}{extra}. "
                "This usually means the join columns have mismatched data "
                "types at the source, which blocks predicate pushdown and "
                "can inflate hash-table memory (especially with DECIMAL)."
            ).format(examples=", ".join(examples), extra=extra),
            metric_name="implicit_cast_on_join_key",
            current_value=str(len(join_casts)),
            threshold="0",
            recommendation=_(
                "Align JOIN key data types at the source tables (for example, "
                "use the same INTEGER / BIGINT on both sides instead of mixing "
                "DECIMAL with INTEGER). If a type change is not possible, cast "
                "once on the smaller side only and verify the join still "
                "pushes down."
            ),
        )

    # ------------------------------------------------------------------
    # CTE referenced >= 2 times but not reused via ReusedExchange
    # ------------------------------------------------------------------
    multi_ref_ctes = [c for c in explain.cte_references if c.reference_count >= 2]
    if multi_ref_ctes and not explain.has_reused_exchange:
        indicators.cte_reuse_miss_count = len(multi_ref_ctes)
        ids = ", ".join(f"#{c.cte_id}×{c.reference_count}" for c in multi_ref_ctes[:3])
        _add_alert(
            indicators,
            severity=Severity.HIGH,
            category="execution_plan",
            message=_(
                "{n} CTE(s) referenced multiple times but not reused via "
                "ReusedExchange ({ids}) — Spark is re-computing the CTE body "
                "for each reference."
            ).format(n=len(multi_ref_ctes), ids=ids),
            metric_name="cte_reuse_miss",
            current_value=str(len(multi_ref_ctes)),
            threshold="0",
            recommendation=_(
                "Do NOT use a TEMP VIEW to prevent CTE re-computation — it does "
                "not materialize. Persist the shared result with CTAS / Delta "
                "table, or rewrite the query so the CTE body runs once and is "
                "joined back (often via a grouped aggregation). Confirm reuse "
                "via ReusedExchange in EXPLAIN under AQE."
            ),
        )

    # ------------------------------------------------------------------
    # Photon fallback operators in the physical plan
    # ------------------------------------------------------------------
    if explain.photon_fallback_ops:
        # Dedup by node_name head token (operator type)
        fallback_names = sorted({op.node_name.split()[0] for op in explain.photon_fallback_ops})
        indicators.photon_fallback_op_count = len(explain.photon_fallback_ops)
        _add_alert(
            indicators,
            severity=Severity.HIGH,
            category="photon",
            message=_(
                "Non-Photon operator(s) detected in physical plan: {names}. "
                "These fall back to JVM execution and often correlate with "
                "CPU-bound slowdowns and higher spill risk."
            ).format(names=", ".join(fallback_names[:5])),
            metric_name="photon_fallback_ops",
            current_value=str(indicators.photon_fallback_op_count),
            threshold="0",
            recommendation=_(
                "Inspect the Photon Explanation section for the unsupported "
                "expression / function and rewrite it (e.g., replace PIVOT "
                "with CASE WHEN + GROUP BY, replace Python/Scala UDFs with "
                "built-in SQL, simplify window frames)."
            ),
        )

    # ------------------------------------------------------------------
    # Filter pushdown gap — PartitionFilters: [] with DataFilters present
    # ------------------------------------------------------------------
    gap_scans = [
        fp for fp in explain.filter_pushdown if fp.partition_filters_empty and fp.has_data_filters
    ]
    if gap_scans:
        indicators.filter_pushdown_gap_count = len(gap_scans)
        tables = ", ".join(fp.table_name or "?" for fp in gap_scans[:3])
        _add_alert(
            indicators,
            severity=Severity.MEDIUM,
            category="io",
            message=_(
                "Partition pruning is empty for {n} scan(s) while row filters "
                "exist ({tables}). The table is either not partitioned on "
                "the filter columns, or the filter is applied after the scan."
            ).format(n=len(gap_scans), tables=tables),
            metric_name="pushdown_gap",
            current_value=str(len(gap_scans)),
            threshold="0",
            recommendation=_(
                "Confirm the table partitioning strategy. If partitioning "
                "does not match query filters, migrate to Liquid Clustering "
                "on the filter columns instead."
            ),
        )

    # ------------------------------------------------------------------
    # Missing partial/final aggregate split (volume-reducing opportunity)
    # ------------------------------------------------------------------
    if explain.aggregate_phases:
        has_partial = any(p.has_partial_functions for p in explain.aggregate_phases)
        has_final = any(p.has_final_merge for p in explain.aggregate_phases)
        if not has_partial and not has_final and len(explain.aggregate_phases) == 1:
            _add_alert(
                indicators,
                severity=Severity.INFO,
                category="agg",
                message=_(
                    "Aggregate executes without a partial/final split — "
                    "pre-aggregation before shuffle is not being used."
                ),
                metric_name="missing_partial_aggregate",
                current_value="none",
                threshold="partial+final",
                is_actionable=False,
                recommendation=_(
                    "If the aggregate runs over a large row count, consider "
                    "enabling AQE partial-aggregation or pre-aggregating in a "
                    "CTE before the shuffle."
                ),
            )


def _extract_photon_blockers_from_explain(
    indicators: BottleneckIndicators,
    photon_lines: list[str],
) -> None:
    """Extract specific Photon blocker reasons from EXPLAIN Photon Explanation section.

    Parses the structured Photon Explanation output to extract:
    - Unsupported expressions/functions
    - Detail messages explaining why
    - Reference nodes in the execution plan
    - Generates specific SQL rewrite suggestions

    Example input format:
        Photon does not fully support the query because:
            pivotfirst(L_SHIPMODE#3140, ...) is not supported:
                Unsupported aggregation function pivotfirst for aggregation mode: Partial.
        Reference node:
            HashAggregate(keys=[...], functions=[partial_pivotfirst(...)])

    Args:
        indicators: BottleneckIndicators to update
        photon_lines: Lines from Photon Explanation section
    """
    # Parse structured Photon Explanation
    parsed_blockers = _parse_photon_explanation_structured(photon_lines)

    # Also check for simple keyword-based patterns (fallback)
    simple_blockers = _detect_simple_photon_blockers(photon_lines)

    # Merge and deduplicate
    existing_reasons = {pb.reason for pb in indicators.photon_blockers}

    # Add parsed blockers first (more detailed)
    for blocker in parsed_blockers:
        if blocker.reason not in existing_reasons:
            indicators.photon_blockers.append(blocker)
            existing_reasons.add(blocker.reason)

    # Add simple blockers as fallback only if no similar blocker exists
    # Check for keyword overlap to avoid duplicates like "PIVOT operator" vs "PIVOT operator (pivotfirst)"
    def is_duplicate_blocker(new_reason: str, existing: set[str]) -> bool:
        """Check if new_reason is a duplicate or subset of existing reasons."""
        new_lower = new_reason.lower()
        for existing_reason in existing:
            existing_lower = existing_reason.lower()
            # Check if one is a substring/prefix of the other
            if new_lower in existing_lower or existing_lower in new_lower:
                return True
            # Check for common key terms
            new_terms = set(new_lower.split())
            existing_terms = set(existing_lower.split())
            # If significant overlap in terms, consider duplicate
            common = new_terms & existing_terms
            if common and len(common) >= min(len(new_terms), len(existing_terms)) * 0.5:
                return True
        return False

    for blocker in simple_blockers:
        if not is_duplicate_blocker(blocker.reason, existing_reasons):
            indicators.photon_blockers.append(blocker)
            existing_reasons.add(blocker.reason)


def _parse_photon_explanation_structured(photon_lines: list[str]) -> list[PhotonBlocker]:
    """Parse Photon Explanation using a flexible line-oriented state machine.

    Handles various formats including:
    - "X is not supported:" with detail on next line(s)
    - "X is not supported: detail" on same line
    - "Photon does not support X" format
    - "X is partially supported" format
    - Various indentation levels (spaces/tabs)
    - Multiple blockers with individual Reference nodes

    Returns:
        List of PhotonBlocker with detailed information
    """
    import re

    blockers: list[PhotonBlocker] = []

    # Patterns to detect blocker start (flexible indentation)
    # Pattern 1: "expression is not supported:" or "expression is not supported: detail"
    # Expression can be: function_name, function_name(...), OperatorName, etc.
    # Use greedy match for function with nested parentheses
    pattern_not_supported = re.compile(
        r"^\s*(.+?)\s+is\s+not\s+supported\s*:?\s*(.*)$",
        re.IGNORECASE,
    )
    # Pattern 2: "expression is partially supported"
    pattern_partially_supported = re.compile(
        r"^\s*(.+?)\s+is\s+(?:only\s+)?partially\s+supported\s*:?\s*(.*)$",
        re.IGNORECASE,
    )
    # Pattern 3: "Photon does not support expression" - but NOT the header line
    # Skip lines like "Photon does not fully support the query because:"
    pattern_does_not_support = re.compile(
        r"^\s*Photon\s+does\s+not\s+(?:fully\s+)?support\s+(?!the\s+query)(.+?)(?:\s*:\s*(.*))?$",
        re.IGNORECASE,
    )
    # Pattern 4: "Unsupported expression/function/operator: X"
    pattern_unsupported = re.compile(
        r"^\s*Unsupported\s+(?:expression|function|operator|aggregation[^:]*|type[^:]*)\s*:?\s*(.+)$",
        re.IGNORECASE,
    )
    # Pattern 5: Reference node marker
    pattern_reference_node = re.compile(r"^\s*Reference\s+node\s*:\s*(.*)$", re.IGNORECASE)
    # Pattern to skip: Header lines like "Photon does not fully support the query because:"
    pattern_skip_header = re.compile(
        r"^\s*Photon\s+does\s+not\s+(?:fully\s+)?support\s+the\s+query", re.IGNORECASE
    )
    # Pattern to identify detail/continuation lines (start with common detail prefixes)
    pattern_detail_line = re.compile(
        r"^\s*(?:Unsupported|Cannot|Does not|Not supported|Because|Due to|Reason)",
        re.IGNORECASE,
    )

    # State machine variables
    current_blocker: dict | None = None
    current_indent: int = 0
    pending_reference_node: str = ""

    def get_indent(line: str) -> int:
        """Get indentation level (count leading whitespace)."""
        return len(line) - len(line.lstrip())

    def finalize_blocker(blocker_dict: dict, ref_node: str) -> PhotonBlocker:
        """Convert blocker dict to PhotonBlocker object."""
        expression = blocker_dict.get("expression", "")
        detail = blocker_dict.get("detail", "")
        func_name = _extract_function_name(expression)
        blocker_info = _classify_photon_blocker(func_name, expression, detail)

        return PhotonBlocker(
            reason=blocker_info["reason"],
            count=1,
            impact=blocker_info["impact"],
            action=blocker_info["action"],
            unsupported_expression=expression,
            detail_message=detail,
            reference_node=ref_node,
            sql_rewrite_example=blocker_info.get("sql_rewrite", ""),
        )

    for line in photon_lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Skip header lines
        if pattern_skip_header.match(stripped):
            continue

        indent = get_indent(line)

        # Check for Reference node
        ref_match = pattern_reference_node.match(stripped)
        if ref_match:
            ref_content = ref_match.group(1).strip()
            if current_blocker:
                # Finalize current blocker with this reference node
                blockers.append(finalize_blocker(current_blocker, ref_content))
                current_blocker = None
            else:
                # Store for next blocker or as global reference
                pending_reference_node = ref_content
            continue

        # Check if this is a detail/continuation line first
        is_detail_line = pattern_detail_line.match(stripped) is not None

        # Check for blocker patterns
        blocker_match = None
        expression = ""
        detail = ""

        # Only try blocker patterns if this doesn't look like a detail line
        if not is_detail_line:
            for pattern in [
                pattern_not_supported,
                pattern_partially_supported,
                pattern_does_not_support,
                pattern_unsupported,
            ]:
                match = pattern.match(stripped)
                if match:
                    blocker_match = match
                    if pattern == pattern_unsupported:
                        # Pattern 4 has expression in group 1
                        expression = match.group(1).strip()
                        detail = ""
                    else:
                        expression = match.group(1).strip()
                        detail = (
                            match.group(2).strip()
                            if match.lastindex is not None and match.lastindex >= 2
                            else ""
                        )
                    break

        if blocker_match:
            # Finalize previous blocker if exists
            if current_blocker:
                blockers.append(finalize_blocker(current_blocker, pending_reference_node))
                pending_reference_node = ""

            # Start new blocker
            current_blocker = {"expression": expression, "detail": detail}
            current_indent = indent
        elif current_blocker:
            # Continuation line (more indented than blocker start, or explicitly a detail line)
            if (
                is_detail_line
                or indent > current_indent
                or (
                    indent == current_indent
                    and not any(
                        p.match(stripped)
                        for p in [
                            pattern_not_supported,
                            pattern_partially_supported,
                            pattern_does_not_support,
                            pattern_unsupported,
                            pattern_reference_node,
                        ]
                    )
                )
            ):
                # Append to detail
                existing_detail = current_blocker.get("detail", "")
                if existing_detail:
                    current_blocker["detail"] = existing_detail + " " + stripped
                else:
                    current_blocker["detail"] = stripped

    # Finalize last blocker
    if current_blocker:
        blockers.append(finalize_blocker(current_blocker, pending_reference_node))

    return blockers


def _extract_function_name(expression: str) -> str:
    """Extract the function name from an expression like 'pivotfirst(...)'.

    Args:
        expression: Full expression string

    Returns:
        Function name or the expression if no function found
    """
    import re

    # Match function call pattern: name(...)
    func_match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", expression)
    if func_match:
        return func_match.group(1)
    return expression.split()[0] if expression else ""


# Data-driven Photon blocker classification rules
# Each rule defines: keywords to match, conditions, and output (reason/impact/action/sql_rewrite)
# Rules are evaluated in order - more specific rules should come first
PHOTON_BLOCKER_RULES: list[dict] = [
    {
        "id": "pivot",
        "keywords": ["pivotfirst", "pivot"],
        "func_keywords": ["pivot"],  # Match in func_name specifically
        "reason": "PIVOT operator (pivotfirst function)",
        "impact": "MEDIUM",
        "action": "Rewrite PIVOT to CASE WHEN + GROUP BY",
        "sql_rewrite": """-- Before (PIVOT - not supported by Photon):
SELECT * FROM src
PIVOT (SUM(value) FOR category IN ('A', 'B', 'C'));

-- After (CASE WHEN + GROUP BY - Photon compatible):
SELECT
  group_key,
  SUM(CASE WHEN category = 'A' THEN value ELSE 0 END) AS A,
  SUM(CASE WHEN category = 'B' THEN value ELSE 0 END) AS B,
  SUM(CASE WHEN category = 'C' THEN value ELSE 0 END) AS C
FROM src
GROUP BY group_key;""",
        "tags": ["aggregation", "pivot"],
    },
    {
        "id": "sort_merge_join",
        "keywords": ["sortmergejoin", "sort-merge", "sort merge", "smj"],
        "reason": "Sort-Merge Join",
        "impact": "HIGH",
        "action": "SET spark.sql.join.preferSortMergeJoin = false",
        "sql_rewrite": "-- Classic/Pro: Config change\nSET spark.sql.join.preferSortMergeJoin = false;\nSET spark.databricks.adaptive.joinFallback = true;\n\n-- Serverless / Query rewrite alternative:\nSELECT /*+ SHUFFLE_HASH(table) */ * FROM table1 JOIN table2 ON ...;",
        "tags": ["join"],
        "action_type": "config",  # Indicates this is a config change, not SQL rewrite
    },
    {
        "id": "broadcast_nested_loop",
        "keywords": ["broadcastnestedloopjoin", "broadcast nested loop", "bnlj", "cartesian"],
        "reason": "Broadcast Nested Loop Join (Cartesian)",
        "impact": "HIGH",
        "action": "Add join conditions or use broadcast hint for small tables",
        "sql_rewrite": """-- Broadcast Nested Loop Join indicates missing/inefficient join condition
-- Option 1: Add explicit join condition
SELECT * FROM t1 JOIN t2 ON t1.key = t2.key;

-- Option 2: Use broadcast hint for small table
SELECT /*+ BROADCAST(small_table) */ * FROM large_table JOIN small_table ON ...;""",
        "tags": ["join"],
    },
    {
        "id": "python_udf",
        "keywords": ["pythonudf", "python udf", "pythonevalpython"],
        "requires_all": [],  # Also match if "python" AND "udf" both present
        "reason": "Python UDF",
        "impact": "HIGH",
        "action": "Replace Python UDF with Pandas UDF or built-in functions",
        "sql_rewrite": """-- Python UDFs are not supported by Photon.
-- Options:
-- 1. Replace with built-in SQL functions
-- 2. Use Pandas UDF (better performance but still not Photon)
-- 3. Use SQL TRANSFORM with supported expressions""",
        "tags": ["udf", "python"],
    },
    {
        "id": "scala_udf",
        "keywords": ["scalaudf", "scala udf"],
        "reason": "Scala UDF",
        "impact": "MEDIUM",
        "action": "Replace Scala UDF with built-in functions if possible",
        "sql_rewrite": "",
        "tags": ["udf", "scala"],
    },
    {
        "id": "generic_udf",
        "keywords": ["udf", "user-defined function", "user defined function"],
        "reason": "User-Defined Function (UDF)",
        "impact": "HIGH",
        "action": "Replace UDF with built-in SQL functions",
        "sql_rewrite": """-- UDFs force row-by-row processing. Replace with built-in functions:
-- Before: SELECT my_udf(col) FROM table
-- After:  SELECT built_in_function(col) FROM table

-- Common replacements:
-- JSON parsing UDF -> get_json_object(), from_json()
-- String manipulation UDF -> concat(), substring(), regexp_replace()
-- Date manipulation UDF -> date_add(), datediff(), date_format()""",
        "tags": ["udf"],
    },
    {
        "id": "generator",
        "keywords": [
            "generate",
            "explode",
            "posexplode",
            "inline",
            "stack",
            "lateral view",
            "outer generate",
        ],
        "reason": "Generator function (EXPLODE, LATERAL VIEW)",
        "impact": "LOW",
        "action": "Consider pre-materializing exploded data if called frequently",
        "sql_rewrite": """-- Generator functions can be optimized by pre-materializing
-- Before: SELECT ... FROM table LATERAL VIEW explode(array_col) ...
-- After: Create a pre-exploded table/view for frequently accessed data

CREATE OR REPLACE TEMP VIEW exploded_data AS
SELECT *, exploded_col
FROM source_table
LATERAL VIEW explode(array_col) t AS exploded_col;""",
        "tags": ["generator"],
    },
    {
        "id": "window_function",
        "keywords": [
            "window",
            "windowexec",
            "windowfunction",
            "row_number",
            "rank",
            "dense_rank",
            "lead",
            "lag",
            "ntile",
            "percent_rank",
            "cume_dist",
            "first_value",
            "last_value",
        ],
        "requires_not_supported": True,  # Only match if "not supported/unsupported/partially" also present
        "reason": "Complex window function",
        "impact": "MEDIUM",
        "action": "Simplify window frame or use supported window functions",
        "sql_rewrite": """-- Some window frames are not supported by Photon.
-- Supported: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- Consider: Pre-aggregate or use simpler window specifications

-- Example: Replace complex frame with simple aggregation
-- Before: SUM(col) OVER (ORDER BY date RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW)
-- After: Use date-based join or pre-aggregated table""",
        "tags": ["window"],
    },
    {
        "id": "cast",
        "keywords": ["cast"],
        "func_keywords": ["cast"],
        "requires_not_supported": True,
        "reason": "Unsupported CAST operation",
        "impact": "LOW",
        "action": "Check data type compatibility",
        "sql_rewrite": "",
        "tags": ["cast", "type"],
    },
    {
        "id": "decimal",
        "keywords": ["decimal", "precision", "scale"],
        "requires_not_supported": True,
        "reason": "Decimal precision/scale not supported",
        "impact": "LOW",
        "action": "Adjust decimal precision or use DOUBLE",
        "sql_rewrite": """-- Decimal with high precision may not be supported
-- Option 1: Reduce precision if acceptable
CAST(col AS DECIMAL(18, 2))

-- Option 2: Use DOUBLE for approximate calculations
CAST(col AS DOUBLE)""",
        "tags": ["decimal", "type"],
    },
    {
        "id": "collation",
        "keywords": ["collation", "collate"],
        "reason": "Collation not supported",
        "impact": "LOW",
        "action": "Remove COLLATE clause or use default collation",
        "sql_rewrite": "",
        "tags": ["collation", "string"],
    },
    {
        "id": "complex_expression",
        "keywords": ["expression", "expr"],
        "requires_not_supported": True,
        "reason": "Complex expression not supported",
        "impact": "MEDIUM",
        "action": "Simplify expression or break into multiple steps",
        "sql_rewrite": """-- Break complex expressions into simpler steps
-- Before: complex_func(nested_func(col1, col2), another_func(col3))
-- After: Use CTEs or temp views to simplify

WITH step1 AS (
  SELECT *, simple_func(col1) AS intermediate
  FROM source
)
SELECT *, final_func(intermediate)
FROM step1;""",
        "tags": ["expression"],
    },
    {
        "id": "aggregate",
        "keywords": ["aggregate", "aggregation"],
        "requires_not_supported": True,  # Only match with "not supported" to avoid false positives
        "reason_template": "Unsupported aggregate function: {func}",
        "impact": "MEDIUM",
        "action": "Use Photon-compatible aggregate functions",
        "sql_rewrite": """-- Some aggregate functions are not fully supported by Photon.
-- Photon-compatible aggregates: SUM, COUNT, AVG, MIN, MAX, FIRST, LAST
-- Consider rewriting complex aggregations using these base functions.""",
        "tags": ["aggregation"],
    },
    {
        "id": "subquery",
        "keywords": ["subquery", "correlated", "scalar subquery"],
        "reason": "Correlated or scalar subquery",
        "impact": "MEDIUM",
        "action": "Rewrite subquery as JOIN or CTE",
        "sql_rewrite": """-- Correlated subqueries may not be optimized by Photon
-- Before: SELECT *, (SELECT MAX(val) FROM t2 WHERE t2.key = t1.key) FROM t1
-- After: Use JOIN

SELECT t1.*, t2_agg.max_val
FROM t1
LEFT JOIN (SELECT key, MAX(val) AS max_val FROM t2 GROUP BY key) t2_agg
ON t1.key = t2_agg.key;""",
        "tags": ["subquery"],
    },
    {
        "id": "regex",
        "keywords": ["regexp", "regex", "rlike", "regexp_extract", "regexp_replace"],
        "requires_not_supported": True,
        "reason": "Complex regex operation",
        "impact": "LOW",
        "action": "Simplify regex pattern or use string functions",
        "sql_rewrite": """-- Complex regex may fall back to non-Photon execution
-- Consider using simpler string functions when possible:
-- LIKE, CONTAINS, STARTSWITH, ENDSWITH, SUBSTRING, INSTR""",
        "tags": ["regex", "string"],
    },
]


def _classify_photon_blocker(func_name: str, expression: str, detail: str) -> dict[str, Any]:
    """Classify Photon blocker using data-driven rules.

    Uses a declarative rule system for flexible pattern matching.
    Rules are evaluated in order - more specific rules come first.

    Args:
        func_name: Extracted function name
        expression: Full unsupported expression
        detail: Detail message from EXPLAIN

    Returns:
        Dict with reason, impact, action, sql_rewrite, and optional tags
    """
    import re

    func_lower = func_name.lower()
    expr_lower = expression.lower()
    detail_lower = detail.lower()
    combined = f"{func_lower} {expr_lower} {detail_lower}"

    def word_match(pattern: str, text: str) -> bool:
        """Check if pattern matches as a word (with word boundaries)."""
        return bool(re.search(rf"\b{re.escape(pattern)}\b", text, re.IGNORECASE))

    def any_word_match(patterns: list[str], text: str) -> bool:
        """Check if any pattern matches as a word."""
        return any(word_match(p, text) for p in patterns)

    not_supported_indicators = ["not supported", "unsupported", "partially supported", "cannot"]

    def has_not_supported() -> bool:
        """Check if any 'not supported' indicator is present."""
        return any_word_match(not_supported_indicators, combined)

    # Evaluate rules in order
    for rule in PHOTON_BLOCKER_RULES:
        matched = False

        # Check func_keywords first (more specific)
        if "func_keywords" in rule:
            if any_word_match(rule["func_keywords"], func_lower):
                matched = True

        # Check general keywords
        if not matched and "keywords" in rule:
            if any_word_match(rule["keywords"], combined):
                matched = True

        # Check requires_all condition (e.g., python AND udf both present)
        if not matched and "requires_all" in rule and rule["requires_all"]:
            if all(word_match(kw, combined) for kw in rule["requires_all"]):
                matched = True

        # Check requires_not_supported condition
        if matched and rule.get("requires_not_supported", False):
            if not has_not_supported():
                matched = False

        if matched:
            # Build result
            reason = rule.get("reason_template", rule.get("reason", ""))
            if "{func}" in reason:
                reason = reason.format(func=func_name or "unknown")

            return {
                "reason": _(reason) if reason else "",
                "impact": rule.get("impact", "MEDIUM"),
                "action": _(rule.get("action", "")),
                "sql_rewrite": _(rule.get("sql_rewrite", "")) if rule.get("sql_rewrite") else "",
                "tags": rule.get("tags", []),
                "rule_id": rule.get("id", "unknown"),
            }

    # Default fallback - extract potential tags from the text
    detected_tags = []
    tag_keywords = {
        "join": ["join", "merge", "hash", "broadcast"],
        "aggregation": ["aggregate", "agg", "sum", "count", "avg"],
        "window": ["window", "over", "partition by"],
        "udf": ["udf", "function"],
        "type": ["cast", "decimal", "type"],
    }
    for tag, keywords in tag_keywords.items():
        if any_word_match(keywords, combined):
            detected_tags.append(tag)

    display_name = func_name if func_name else expression[:50] if expression else "unknown"
    return {
        "reason": _("Unsupported operation: {func}").format(func=display_name),
        "impact": "MEDIUM",
        "action": _("Check Databricks documentation for Photon-compatible alternatives"),
        "sql_rewrite": "",
        "tags": detected_tags,
        "rule_id": "unknown",
    }


def _detect_simple_photon_blockers(photon_lines: list[str]) -> list[PhotonBlocker]:
    """Detect Photon blockers using simple keyword matching (fallback method).

    This is a fallback for cases where structured parsing doesn't find blockers.
    It uses keyword detection to identify common Photon-unsupported patterns.

    Args:
        photon_lines: Lines from Photon Explanation section

    Returns:
        List of PhotonBlocker objects
    """

    blockers_found: dict[str, int] = {}
    full_text = " ".join(photon_lines).lower()

    # Detection patterns: (key, patterns_to_match, requires_not_supported)
    detection_rules = [
        # Sort-Merge Join
        ("Sort-Merge Join", ["sort merge", "sortmerge", "sortmergejoin", "smj"], False),
        # PIVOT
        ("PIVOT operator", ["pivot", "pivotfirst"], False),
        # Python UDF (check before generic UDF)
        ("Python UDF", ["python udf", "pythonudf", "pythonevalpython"], False),
        # Scala UDF
        ("Scala UDF", ["scala udf", "scalaudf"], False),
        # Generic UDF
        ("UDF execution", ["udf", "user-defined function", "user defined function"], False),
        # Window functions
        ("Window with complex frame", ["window"], True),
        # Generator functions
        (
            "Generator function",
            ["generate", "explode", "posexplode", "lateral view", "inline"],
            False,
        ),
        # CAST
        ("Unsupported CAST", ["cast"], True),
        # Aggregate
        ("Unsupported aggregate", ["aggregate", "aggregation"], True),
        # Decimal
        ("Decimal precision issue", ["decimal precision", "decimal scale"], False),
        # Collation
        ("Collation not supported", ["collation", "collate"], False),
        # Complex expression
        ("Complex expression", ["complex expression", "expression not supported"], False),
    ]

    # Indicators that something is not supported
    not_supported_indicators = [
        "not supported",
        "unsupported",
        "partially supported",
        "does not support",
        "cannot be",
        "not compatible",
    ]

    has_not_supported = any(ind in full_text for ind in not_supported_indicators)

    for key, patterns, requires_not_supported in detection_rules:
        if requires_not_supported and not has_not_supported:
            continue

        for pattern in patterns:
            if pattern in full_text:
                # For patterns requiring "not supported", check proximity
                if requires_not_supported:
                    # Check if pattern and "not supported" appear in same line
                    for line in photon_lines:
                        line_lower = line.lower()
                        if pattern in line_lower and any(
                            ind in line_lower for ind in not_supported_indicators
                        ):
                            blockers_found[key] = blockers_found.get(key, 0) + 1
                            break
                else:
                    blockers_found[key] = blockers_found.get(key, 0) + 1
                break  # Only count once per key

    # Build PhotonBlocker objects
    blocker_actions = {
        "Sort-Merge Join": "SET spark.sql.join.preferSortMergeJoin = false (or use /*+ SHUFFLE_HASH(table) */ hint on Serverless)",
        "UDF execution": _("Replace with built-in function if possible"),
        "Window with complex frame": _("Consider query rewrite"),
        "Unsupported CAST": _("Check data types compatibility"),
        "Unsupported aggregate": _("Use Photon-compatible aggregate functions"),
    }

    blocker_impacts = {
        "Sort-Merge Join": "HIGH",
        "UDF execution": "LOW",
        "Window with complex frame": "MEDIUM",
        "Unsupported CAST": "LOW",
        "Unsupported aggregate": "MEDIUM",
    }

    blockers = []
    for reason, count in blockers_found.items():
        blockers.append(
            PhotonBlocker(
                reason=reason,
                count=count,
                impact=blocker_impacts.get(reason, "MEDIUM"),
                action=blocker_actions.get(reason, ""),
            )
        )

    return blockers


# ---------------------------------------------------------------------------
# Liquid Clustering ClusterOnWrite overhead detection
# ---------------------------------------------------------------------------


def detect_lc_cluster_on_write_overhead(
    indicators: BottleneckIndicators,
    shuffle_metrics: list,
    target_table_info,
) -> None:
    """Emit a HIGH alert when Delta + Liquid Clustering write causes spill.

    Preconditions for firing (ALL must hold):
      1. Target table is Delta (``target_table_info.is_delta``)
      2. Target has configured clustering_columns (len > 0)
      3. A shuffle wrote a *meaningful* amount to the sink
         (``sink_bytes_written >= 1 GiB``) — this is the scale gate that
         prevents firing on small tables or dev-sized INSERTs
      4. That shuffle shows *at least one* strong memory-pressure signal:
         - ``memory_per_partition_mb >= 256`` (severe per-partition memory), OR
         - ``sink_num_spills >= 3`` (repeated spill events, not a lone flush), OR
         - ``peak_memory_bytes >= 3 × sink_bytes_written`` (memory bloat)

    These thresholds are tighter than naive OR combinations to keep
    false-positive rate low on benign LC writes. The scale gate (#3) is
    the key change from the earlier permissive predicate.
    """
    if target_table_info is None:
        return
    if not getattr(target_table_info, "is_delta", False):
        return
    if not getattr(target_table_info, "clustering_columns", None):
        return
    if not shuffle_metrics:
        return

    MIN_SINK_BYTES = 1 * 1024**3  # 1 GiB scale gate
    MEM_PRESSURE_MB = 256
    MIN_SPILL_COUNT = 3
    PEAK_TO_WRITTEN_RATIO = 3.0

    worst_shuffle = None
    worst_score = 0.0
    for sm in shuffle_metrics:
        written = getattr(sm, "sink_bytes_written", 0) or 0
        if written < MIN_SINK_BYTES:
            continue  # scale gate: only meaningful-sized writes
        mpp = getattr(sm, "memory_per_partition_mb", 0) or 0
        spills = getattr(sm, "sink_num_spills", 0) or 0
        peak = getattr(sm, "peak_memory_bytes", 0) or 0
        pressure_signals = (
            mpp >= MEM_PRESSURE_MB,
            spills >= MIN_SPILL_COUNT,
            peak >= PEAK_TO_WRITTEN_RATIO * written,
        )
        if not any(pressure_signals):
            continue  # healthy — skip
        score = mpp + spills * 100 + (peak / max(written, 1))
        if score > worst_score:
            worst_score = score
            worst_shuffle = sm

    if worst_shuffle is None:
        return

    mpp_mb = int(getattr(worst_shuffle, "memory_per_partition_mb", 0) or 0)
    spill_bytes = getattr(worst_shuffle, "sink_bytes_written", 0) or 0
    spill_gb = spill_bytes / 1024**3
    node_id = getattr(worst_shuffle, "node_id", "?") or "?"

    # Flatten clustering columns: [["A"],["B"]] → ["A","B"]
    cols = [c for group in target_table_info.clustering_columns for c in group]
    cols_str = ", ".join(cols) if cols else "?"

    has_hier = bool(getattr(target_table_info, "hierarchical_clustering_columns", None))
    hier_note = ""
    if has_hier:
        hier_cols = ", ".join(target_table_info.hierarchical_clustering_columns)
        hier_note = _(" Hierarchical Clustering is also configured ({hier}).").format(
            hier=hier_cols
        )

    full_name = getattr(target_table_info, "full_name", "")
    target_label = full_name or getattr(target_table_info, "table", "") or "target"

    _add_alert(
        indicators,
        severity=Severity.HIGH,
        category="io",
        message=_(
            "Liquid Clustering ClusterOnWrite overhead: target `{t}` has {n} clustering "
            "column(s) ({cols}) and the pre-write re-shuffle (Node #{nid}) spills "
            "{mpp:,} MB/partition across {writ:.1f} GB written.{hier}"
        ).format(
            t=target_label,
            n=len(cols),
            cols=cols_str,
            nid=node_id,
            mpp=mpp_mb,
            writ=spill_gb,
            hier=hier_note,
        ),
        metric_name="lc_cluster_on_write_spill",
        current_value=f"{mpp_mb}MB/part, {spill_gb:.1f}GB written",
        threshold=">=1GiB written AND (>=256MB/part OR >=3 spills OR peak >=3x written)",
        recommendation=_(
            "Options (listed by typical effectiveness): "
            "(1) Drop CLUSTER BY before the one-shot load, run OPTIMIZE FULL afterwards to "
            "re-cluster; "
            "(2) Disable eager clustering via `ALTER TABLE ... SET TBLPROPERTIES "
            "('delta.liquid.forceDisableEagerClustering' = 'True')` and run OPTIMIZE FULL "
            "afterwards — keeps LC metadata and skips write-time re-shuffle; "
            "(3) Reduce the number of clustering keys if read-side pruning still holds; "
            "(4) Disable Hierarchical Clustering if enabled and not strictly needed; "
            "(5) Scale up the warehouse temporarily for this INSERT; "
            "(6) Split the load into smaller batches so each shuffle fits in memory."
        ),
    )
