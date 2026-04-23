"""
Report generation functions.

This package provides Markdown report generation from analysis results,
including section generators, data flow visualization, and summary functions.
"""

from typing import Any

from ..analyzers.warehouse_sizing import (
    analyze_warehouse_sizing,
    format_warehouse_sizing_executive_bullets,
    format_warehouse_sizing_subsection,
)
from ..constants import Severity
from ..i18n import gettext as _
from ..models import ProfileAnalysis
from ..utils import format_bytes, format_time_ms
from .action_plan import generate_action_plan_section
from .dataflow import (
    format_aqe_aos_events,
    generate_ascii_tree,
    generate_data_flow_section,
    generate_mermaid_flowchart,
)
from .details import (
    generate_hot_operators_section,
    generate_recommended_spark_params,
    generate_tuning_guide_section,
    generate_validation_checklist,
)
from .query_metrics import (
    generate_performance_metrics,
    generate_query_overview,
    generate_stage_execution_section,
)
from .sections import (
    generate_alerts_section,
    generate_aqe_shuffle_section,
    generate_bottleneck_summary,
    generate_cloud_storage_section,
    generate_explain_section,
    generate_io_metrics_section,
    generate_photon_blockers_section,
    generate_scan_locality_section,
    generate_spill_analysis_section,
    generate_sql_section,
    generate_warehouse_section,
)
from .summary import (
    generate_rule_based_recommendations,
    generate_rule_based_summary,
    generate_top5_recommendations_section,
)


def generate_report_legacy(
    analysis: ProfileAnalysis,
    llm_analysis: str,
    review_analysis: str = "",
    refined_analysis: str = "",
    primary_model: str = "",
    review_model: str = "",
    refine_model: str = "",
    verbose: bool = False,
) -> str:
    """Generate final Markdown report (legacy format).

    This is the old report format kept for backward compatibility.
    Will be removed after CLI/app.py migration to generate_report().
    """
    qm = analysis.query_metrics
    bi = analysis.bottleneck_indicators
    is_serverless = analysis.query_metrics.query_typename == "LakehouseSqlQuery" or (
        analysis.warehouse_info.is_serverless if analysis.warehouse_info else False
    )

    # Calculate key metrics for summary
    spill_gb = bi.spill_bytes / (1024**3)

    report = f"""# 📊 {_("Query Performance Report")}

---

## 📌 {_("Summary")}

| {_("Execution Time")} | {_("Read Data")} | {_("Cache Hit Ratio")} | {_("Photon Utilization")} | {_("Disk Spill")} |
|:--------:|:--------------:|:------------------:|:------------:|:--------------:|
| **{format_time_ms(qm.total_time_ms)}** | **{format_bytes(qm.read_bytes)}** | **{bi.cache_hit_ratio:.0%}** | **{bi.photon_ratio:.0%}** | **{spill_gb:.1f} GB** |

---

## 🧾 {_("Basic Information")}

- **{_("Query ID")}:** `{qm.query_id}`
- **{_("Status")}:** {qm.status}
- **{_("Total Execution Time")}:** {format_time_ms(qm.total_time_ms)} (wall-clock)
- **{_("Compilation Time")}:** {format_time_ms(qm.compilation_time_ms)}
- **{_("Execution Time")}:** {format_time_ms(qm.execution_time_ms)}
- **{_("Task Total Time")}:** {format_time_ms(qm.task_total_time_ms)} ({_("cumulative across all parallel tasks")})
- **{_("Photon Total Time")}:** {format_time_ms(qm.photon_total_time_ms)}
- **{_("Photon Utilization")}:** {bi.photon_ratio:.1%} (= photonTotalTimeMs / taskTotalTimeMs = {qm.photon_total_time_ms:,} / {qm.task_total_time_ms:,})
{generate_warehouse_section(analysis.warehouse_info, analysis.endpoint_id, query_metrics=qm)}{generate_sql_section(analysis.sql_analysis)}{generate_explain_section(analysis.explain_analysis) if analysis.explain_analysis else ""}{generate_io_metrics_section(qm, bi, analysis.top_scanned_tables, explain_analysis=analysis.explain_analysis)}{generate_cloud_storage_section(bi)}---

## 🚦 {_("Bottleneck Indicators")}

{generate_bottleneck_summary(bi)}

{generate_photon_blockers_section(bi)}{generate_spill_analysis_section(bi)}"""

    # Add structured alerts section (preferred) or legacy critical issues/warnings
    if bi.alerts:
        report += generate_alerts_section(bi.alerts)
    else:
        # Legacy: Add critical issues
        if bi.critical_issues:
            report += f"""---

## 🛑 {_("Critical Issues")}

"""
            for issue in bi.critical_issues:
                report += f"> **! {issue}**\n>\n"
            report += "\n"

        # Legacy: Add warnings
        if bi.warnings:
            report += f"""---

## ⚠️ {_("Warnings")}

"""
            for warning in bi.warnings:
                report += f"- {warning}\n"
            report += "\n"

    # Add join analysis
    if analysis.join_info:
        report += f"""---

## 🔗 {_("Join Type Analysis")}

| {_("Join Operator")} | {_("Type")} | {_("Type Support")} | {_("Photon Execution")} |
|----------------|--------|:----------:|:----------:|
"""
        for ji in analysis.join_info:
            type_photon = "OK" if ji.join_type.photon_supported else "X"
            # Handle None (unknown) case for actual Photon execution
            if ji.is_photon is True:
                actual_photon = "OK"
            elif ji.is_photon is False:
                actual_photon = "X"
            else:
                actual_photon = "-"  # Unknown
            node_name = ji.node_name[:40] + "..." if len(ji.node_name) > 40 else ji.node_name
            report += (
                f"| {node_name} | {ji.join_type.display_name} | {type_photon} | {actual_photon} |\n"
            )
        report += "\n"

    # Add shuffle analysis
    inefficient_shuffles = [sm for sm in analysis.shuffle_metrics if not sm.is_memory_efficient]
    if inefficient_shuffles:
        report += f"""---

## 🔀 {_("Shuffle Operations Requiring Optimization")}

> **{_("Evidence source")}:** {_("Partition count from Sink - Number of partitions metric, peak memory from peakMemoryBytes in JSON profile.")}

"""
        for sm in inefficient_shuffles:
            report += f"### {_('Node')} {sm.node_id}\n\n"
            # Show partition count evidence
            report += f"- **{_('Partition Count')}:** {sm.partition_count} ({_('from Sink - Number of partitions metric')})\n"
            # Show peak memory evidence
            peak_mb = sm.peak_memory_bytes / (1024 * 1024)
            report += f"- **{_('Peak Memory')}:** {peak_mb:.0f} MB ({_('from peakMemoryBytes')})\n"
            report += f"- **{_('Memory Usage')}:** {sm.memory_per_partition_mb:.0f} MB/{_('per partition')}\n"
            report += f"- **{_('Optimization Priority')}:** {sm.optimization_priority.value} ({_('exceeds 128MB/partition guideline')})\n"
            if sm.shuffle_attributes:
                # Calculate optimal partitions based on data size targeting 128MB per partition
                target_partition_size_bytes = 128 * 1024 * 1024  # 128MB
                # Prefer aqe_data_size if available, otherwise use peak_memory_bytes
                data_size = sm.aqe_data_size if sm.aqe_data_size > 0 else sm.peak_memory_bytes
                if data_size > 0:
                    optimal = max(1, int(data_size / target_partition_size_bytes))
                    # Ensure we recommend at least current count if it's higher
                    optimal = max(optimal, sm.partition_count)
                else:
                    optimal = max(1, sm.partition_count * 2)
                report += f"- **{_('Recommended')}:** `REPARTITION({optimal}, {', '.join(sm.shuffle_attributes)})`\n"
            report += "\n"

    # Add AQE Shuffle Health section
    report += generate_aqe_shuffle_section(analysis.shuffle_metrics, is_serverless=is_serverless)

    # Add Scan Locality per-node breakdown (Verbose mode)
    report += generate_scan_locality_section(analysis.node_metrics)

    # Add Action Plan + Top Findings section (sorted by priority_score descending)
    if analysis.action_cards:
        sorted_cards = sorted(analysis.action_cards, key=lambda c: c.priority_score, reverse=True)
        report += generate_action_plan_section(sorted_cards)

    # Add Hot Operators section
    if analysis.hot_operators:
        report += generate_hot_operators_section(analysis.hot_operators)

    # Add LLM analysis section(s) based on mode
    if refined_analysis and not verbose:
        # Refined mode (default): show only final refined analysis
        refine_info = f" ({refine_model})" if refine_model else ""
        report += f"""---

## 🤖 {_("LLM Analysis Report")}{refine_info}

{refined_analysis}

"""
    elif verbose and llm_analysis:
        # Verbose mode: show all stages
        primary_info = f" ({primary_model})" if primary_model else ""
        report += f"""---

## 📝 {_("Initial Analysis")}{primary_info}

{llm_analysis}

"""
        if review_analysis:
            reviewer_info = f" ({review_model})" if review_model else ""
            report += f"""---

## 🔍 {_("Review")}{reviewer_info}

{review_analysis}

"""
        if refined_analysis:
            refine_info = f" ({refine_model})" if refine_model else ""
            report += f"""---

## ✨ {_("Final Analysis")}{refine_info}

{refined_analysis}

"""
    elif llm_analysis:
        # Legacy mode (--no-refine): show initial analysis and review separately
        model_info = f" ({primary_model})" if primary_model else ""
        report += f"""---

## 🤖 {_("LLM Analysis Report")}{model_info}

{llm_analysis}

"""
        if review_analysis:
            reviewer_info = f" ({review_model})" if review_model else ""
            report += f"""---

## 🔍 {_("Review Result")}{reviewer_info}

{review_analysis}

"""

    # Add Validation Checklist
    if analysis.action_cards or bi.critical_issues or bi.warnings:
        report += generate_validation_checklist(
            analysis.action_cards, bi, analysis.sql_analysis, analysis.join_info
        )

    # Determine serverless status for config filtering
    is_serverless = analysis.query_metrics.query_typename == "LakehouseSqlQuery" or (
        analysis.warehouse_info.is_serverless if analysis.warehouse_info else False
    )

    # Add dynamic Tuning Guide based on detected bottlenecks
    report += generate_tuning_guide_section(bi, analysis.shuffle_metrics)

    # Add dynamic Recommended Spark Parameters based on detected issues
    report += generate_recommended_spark_params(
        bi, analysis.shuffle_metrics, is_serverless=is_serverless
    )

    report += f"""---

*{_("This report was generated using Databricks Foundation Model APIs.")}*
"""

    return report


def generate_report(
    analysis: ProfileAnalysis,
    llm_sections: dict[str, str] | None = None,
    primary_model: str = "",
    verbose: bool = False,
    raw_llm_analysis: str = "",
    lang: str | None = None,
) -> str:
    """Generate final Markdown report with new structured layout.

    New report structure:
    1. Executive Summary       [LLM or rule-based]
    2. Query Overview           [rule-based]
    3. Performance Metrics      [rule-based]
    4. Root Cause Analysis      [LLM]
    5. Stage Execution Analysis [rule-based]
    6. Data Flow Summary        [rule-based]
    7. Recommendations          [LLM or rule-based]
    8. Conclusion               [LLM]
    --- LLM Analysis (fallback) ---
    --- Appendix ---
    A. Bottleneck Indicators    [rule-based]
    B. SQL Annotation           [rule-based]
    C. Validation Checklist     [rule-based]

    Args:
        analysis: ProfileAnalysis object
        llm_sections: Dict from parse_llm_sections() with keys:
            executive_summary, root_cause_analysis, recommendations, conclusion
        primary_model: Model name for attribution
        verbose: If True, show additional details
        lang: Language code ('en' or 'ja'). If provided, sets the language
              for this report generation (thread-local, safe for concurrency).
        raw_llm_analysis: Raw LLM output text. Used as fallback when
            llm_sections is empty (e.g., old-style free-form LLM output).

    Returns:
        Markdown formatted report
    """
    # Ensure gettext() calls throughout this function use the correct language
    from ..i18n import set_language

    set_language(lang or "en")

    if llm_sections is None:
        llm_sections = {}

    qm = analysis.query_metrics
    bi = analysis.bottleneck_indicators
    is_serverless = analysis.query_metrics.query_typename == "LakehouseSqlQuery" or (
        analysis.warehouse_info.is_serverless if analysis.warehouse_info else False
    )

    sizing_recs = analyze_warehouse_sizing(qm, analysis.warehouse_info)

    parts = []

    # --- Title ---
    parts.append(f"# {_('Query Performance Report')}\n")

    # --- Streaming: precompute batch stats (used by banner, summary, performance) ---
    batch_stats: dict[str, Any] = {}
    if analysis.streaming_context:
        from ..extractors import compute_batch_statistics

        batch_stats = compute_batch_statistics(analysis.streaming_context)

    # --- Streaming Banner (if applicable) ---
    if analysis.streaming_context:
        from .sections import generate_streaming_section

        parts.append(generate_streaming_section(analysis.streaming_context, batch_stats))
        parts.append("")

    # =====================================================================
    # Main body (numbered sections)
    # =====================================================================

    # --- 1. Executive Summary ---
    parts.append("---\n")
    parts.append(f"## 1. {_('Executive Summary')}\n")
    if "executive_summary" in llm_sections:
        parts.append(llm_sections["executive_summary"])
    elif analysis.streaming_context:
        from .summary import generate_streaming_executive_summary

        parts.append(
            generate_streaming_executive_summary(
                bi.alerts,
                analysis.streaming_context,
                batch_stats,
                action_cards=analysis.action_cards,
            )
        )
    else:
        parts.append(generate_rule_based_summary(bi.alerts, qm, analysis.action_cards))
    parts.append(format_warehouse_sizing_executive_bullets(sizing_recs))
    parts.append("")

    # --- 2. Top Alerts ---
    # Sort CRITICAL → HIGH (stable within severity) and number them so the
    # Recommended Actions section can cite `→ アラート #N` references.
    sorted_top_alerts: list = []
    if bi.alerts:
        from .alert_crossref import sort_alerts_by_severity

        critical_high = [a for a in bi.alerts if a.severity in (Severity.CRITICAL, Severity.HIGH)]
        if critical_high:
            sorted_top_alerts = sort_alerts_by_severity(critical_high)[:5]
            parts.append("---\n")
            parts.append(f"## 2. {_('Top Alerts')}\n")
            for idx, a in enumerate(sorted_top_alerts, start=1):
                icon = "🔴" if a.severity == Severity.CRITICAL else "🟠"
                parts.append(f"- **#{idx}** {icon} **[{a.severity.value.upper()}]** {a.message}\n")
            parts.append("")

    # --- 3. Recommended Actions ---
    # Strategy: Top 5 selected cards are surfaced prominently; remaining
    # items fold into a collapsed "Other recommendations" section so the
    # reader can focus on highest-impact items first.
    parts.append("---\n")
    parts.append(f"## 3. {_('Recommended Actions')}\n")
    if "recommendations" in llm_sections:
        rec_text = llm_sections["recommendations"]
        marker_idx = rec_text.find("<!-- ACTION_PLAN_JSON -->")
        if marker_idx > 0:
            rec_text = rec_text[:marker_idx].rstrip()
        parts.append(rec_text)
        parts.append("")
    elif analysis.action_cards:
        top5_section = generate_top5_recommendations_section(
            analysis.action_cards,
            selected_action_cards=analysis.selected_action_cards or None,
            alerts=sorted_top_alerts or None,
        )
        if top5_section:
            parts.append(top5_section)
            parts.append("")
        else:
            sorted_cards = sorted(
                analysis.action_cards, key=lambda c: c.priority_score, reverse=True
            )
            ap = generate_action_plan_section(sorted_cards, include_header=False)
            parts.append(ap)
    else:
        parts.append(generate_rule_based_recommendations(analysis.action_cards))
        parts.append("")

    # Phase 2a (v5.16.19): LLM-generated novel recommendations go in a
    # dedicated subsection below the rule-based Top-N. No more hybrid
    # dedup — readers can see both mechanical (registry) and LLM-novel
    # suggestions clearly separated.
    if analysis.llm_action_cards:
        parts.append(f"\n### {_('Additional LLM-generated recommendations')}\n")
        parts.append(
            _(
                "These supplementary recommendations were generated by the LLM "
                "based on the full analysis context. They may overlap with the "
                "rule-based cards above when both layers identify the same alert."
            )
            + "\n"
        )
        for idx, card in enumerate(analysis.llm_action_cards, start=1):
            badge_bits: list[str] = []
            if card.expected_impact:
                badge_bits.append(f"{_('Impact')}: {card.expected_impact.upper()}")
            if card.effort:
                badge_bits.append(f"{_('Effort')}: {card.effort.upper()}")
            badge = f"  — {', '.join(badge_bits)}" if badge_bits else ""
            parts.append(f"**{idx}. {card.problem}**{badge}\n")
            if card.fix:
                parts.append(f"- {_('Fix')}: {card.fix}")
            if card.fix_sql:
                parts.append(f"\n```sql\n{card.fix_sql}\n```")
            parts.append("")

    # --- 4. Performance Metrics ---
    parts.append("---\n")
    if analysis.streaming_context:
        from .query_metrics import generate_streaming_performance_metrics

        parts.append(f"## 4. {_('Performance Metrics')}\n")
        pm = generate_streaming_performance_metrics(
            analysis.streaming_context,
            batch_stats,
            qm,
            include_header=False,
        )
        parts.append(pm)
    else:
        parts.append(f"## 4. {_('Performance Metrics')}\n")
        pm = generate_performance_metrics(qm, bi, include_header=False)
        parts.append(pm)
        # Top Scanned Tables (clustering keys + column types). The main
        # pipeline's generate_performance_metrics() does NOT include this
        # block, so render it explicitly here alongside the I/O subsection.
        if analysis.top_scanned_tables:
            from .sections import generate_top_scanned_tables_section

            parts.append(
                generate_top_scanned_tables_section(
                    analysis.top_scanned_tables,
                    explain_analysis=analysis.explain_analysis,
                )
            )

    # --- 4b. Cost Estimation (inline) ---
    from ..dbsql_cost import estimate_query_cost, format_cost_usd

    cost = estimate_query_cost(qm, analysis.warehouse_info)
    if cost:
        cost_lines = [f"### {_('Estimated Query Cost')}\n"]
        label = _("Estimated Query Cost") if cost.is_per_query else _("Estimated Query Cost Share")
        cost_lines.append(f"| {_('Item')} | {_('Value')} |")
        cost_lines.append("|:-----|:------|")
        cost_lines.append(f"| **{_('Billing Model')}** | {cost.billing_model} |")
        if cost.cluster_size:
            cost_lines.append(f"| **{_('Cluster Size')}** | {cost.cluster_size} |")
        if cost.dbu_per_hour:
            cost_lines.append(f"| **{_('DBU/hour')}** | {cost.dbu_per_hour} |")
        cost_lines.append(
            f"| **{_('DBU Unit Price')}** | {format_cost_usd(cost.dbu_unit_price)}/DBU |"
        )
        cost_lines.append(f"| **{_('Estimated DBU')}** | {cost.estimated_dbu:.4f} DBU |")
        cost_lines.append(f"| **{label}** | {format_cost_usd(cost.estimated_cost_usd)} |")
        if cost.parallelism_ratio > 0:
            cost_lines.append(f"| **{_('Parallelism Ratio')}** | {cost.parallelism_ratio:.1f}x |")
        cost_lines.append("")
        if cost.reference_costs:
            cost_lines.append(f"**{_('Reference Cost by Warehouse Size')}**\n")
            cost_lines.append(f"| {_('Size')} | DBU/h | {_('Estimated Cost')} |")
            cost_lines.append("|:------|------:|--------------:|")
            for ref in cost.reference_costs:
                cost_lines.append(
                    f"| {ref.cluster_size} | {ref.dbu_per_hour} | "
                    f"{format_cost_usd(ref.estimated_cost_usd)} |"
                )
            cost_lines.append("")
        if cost.note:
            cost_lines.append(f"> *{cost.note}*\n")
        parts.append("\n".join(cost_lines))

    parts.append(format_warehouse_sizing_subsection(sizing_recs))

    # --- 5. Root Cause Analysis ---
    if "root_cause_analysis" in llm_sections:
        parts.append("---\n")
        parts.append(f"## 5. {_('Root Cause Analysis')}\n")
        parts.append(llm_sections["root_cause_analysis"])
        parts.append("")

    # --- 6. Hot Operators ---
    if analysis.hot_operators:
        parts.append("---\n")
        parts.append(f"## 6. {_('Hot Operators')}\n")
        ho = generate_hot_operators_section(analysis.hot_operators, include_header=False)
        parts.append(ho.lstrip("-\n "))

    # --- 7. AQE Shuffle Health ---
    # Section 7 renders when EITHER the shuffle-health table OR the AQE/AOS
    # runtime-optimization events have content. The events subsection was
    # moved here from the Data Flow Details appendix (it describes AQE
    # interventions on shuffle plans and semantically belongs alongside the
    # shuffle-health diagnosis).
    shuffle_content = generate_aqe_shuffle_section(
        analysis.shuffle_metrics, include_header=False, is_serverless=is_serverless
    )
    aqe_events_content = format_aqe_aos_events(analysis.shuffle_metrics)
    if shuffle_content.strip() or aqe_events_content.strip():
        parts.append("---\n")
        parts.append(f"## 7. {_('AQE Shuffle Health')}\n")
        # Add insight summary (only when the shuffle-health table has rows)
        if shuffle_content.strip():
            inefficient = [sm for sm in analysis.shuffle_metrics if not sm.is_memory_efficient]
            total_shuffles = len(analysis.shuffle_metrics)
            if inefficient:
                parts.append(
                    f"> **{_('Insight')}:** {len(inefficient)}/{total_shuffles} {_('shuffle nodes exceed 128MB/partition threshold. REPARTITION hints or query rewrites recommended.')}\n\n"
                )
            elif total_shuffles > 0:
                parts.append(
                    f"> **{_('Insight')}:** {_('All')} {total_shuffles} {_('shuffle nodes are within healthy memory limits.')}\n\n"
                )
            parts.append(shuffle_content.lstrip("-\n "))
        # AQE / AOS runtime-optimization events subsection
        if aqe_events_content.strip():
            parts.append(aqe_events_content)

    # --- 8. Scan Locality (summary) ---
    scan_content = generate_scan_locality_section(analysis.node_metrics, include_header=False)
    if scan_content.strip():
        parts.append("---\n")
        parts.append(f"## 8. {_('Scan Locality')}\n")
        # Add insight summary
        scan_nodes = [
            n
            for n in analysis.node_metrics
            if (n.local_scan_tasks > 0 or n.non_local_scan_tasks > 0) and "Scan" in n.node_name
        ]
        if scan_nodes:
            file_layout_count = sum(
                1
                for n in scan_nodes
                if n.non_local_scan_tasks / max(n.local_scan_tasks + n.non_local_scan_tasks, 1)
                > 0.05
            )
            parts.append(
                f"> **{_('Insight')}:** {file_layout_count}/{len(scan_nodes)} {_('scan nodes show File Layout pattern (>5% rescheduled). Consider Liquid Clustering or OPTIMIZE.')}\n\n"
            )
        parts.append(scan_content.lstrip("-\n "))

    # --- 9. Data Flow ---
    if analysis.data_flow:
        parts.append("---\n")
        parts.append(f"## 9. {_('Data Flow')}\n")
        # Add insight about repeated scans
        table_counts: dict[str, int] = {}
        for entry in analysis.data_flow:
            name = entry.table_name if hasattr(entry, "table_name") and entry.table_name else ""
            if name:
                table_counts[name] = table_counts.get(name, 0) + 1
        repeated = {t: c for t, c in table_counts.items() if c > 1}
        if repeated:
            top = sorted(repeated.items(), key=lambda x: -x[1])[:3]
            parts.append(
                f"> **{_('Insight')}:** {', '.join(f'`{t}` ({c}x)' for t, c in top)} {_('— repeated scans detected. Consider consolidating into a common CTE.')}\n\n"
            )
        df = generate_data_flow_section(
            analysis.data_flow,
            analysis.data_flow_dag,
            include_header=False,
            shuffle_metrics=analysis.shuffle_metrics,
        )
        # Only keep the Mermaid diagram part, move details table to Appendix
        # Split at the details table
        detail_marker = f"### {_('Data Flow Details')}"
        if detail_marker in df:
            idx = df.index(detail_marker)
            parts.append(df[:idx].rstrip())
        else:
            parts.append(df)
        parts.append("")

    # --- 10. Optimized SQL ---
    if "optimized_sql" in llm_sections:
        parts.append("---\n")
        parts.append(f"## 10. {_('Optimized SQL')}\n")
        parts.append(llm_sections["optimized_sql"])
        parts.append("")

    # --- 12. LLM Analysis Report ---
    if "_unmatched" in llm_sections:
        parts.append("---\n")
        parts.append(f"## 11. {_('LLM Analysis Report')}\n")
        parts.append(llm_sections["_unmatched"])
        parts.append("")
    elif not llm_sections and raw_llm_analysis and raw_llm_analysis.strip():
        parts.append("---\n")
        parts.append(f"## 11. {_('LLM Analysis Report')}\n")
        parts.append(raw_llm_analysis.strip())
        parts.append("")
    elif llm_sections:
        parts.append("---\n")
        parts.append(f"## 11. {_('LLM Analysis Report')}\n")
        matched = [k for k in llm_sections if k != "_unmatched"]
        parts.append(
            f"> {_('LLM analysis was successfully parsed into sections')}: {', '.join(matched)}\n"
        )
        parts.append("")

    # =====================================================================
    # 📎 Appendix
    # =====================================================================

    parts.append("\n---\n")
    parts.append(f"# 📎 {_('Appendix')}\n")

    # --- A. Query Overview & Compute ---
    parts.append("---\n")
    parts.append(f"## A. {_('Query Overview')}\n")
    parts.append(generate_query_overview(qm, include_header=False))
    parts.append(
        generate_warehouse_section(
            analysis.warehouse_info, analysis.endpoint_id, query_metrics=qm, include_header=False
        )
    )

    # --- B. SQL / Query Structure ---
    sql_content = generate_sql_section(analysis.sql_analysis, include_header=False)
    if sql_content.strip():
        parts.append("---\n")
        parts.append(f"## B. {_('SQL / Query Structure')}\n")
        parts.append(sql_content)

    # --- C. Stage Execution ---
    if analysis.stage_info:
        parts.append("---\n")
        parts.append(f"## C. {_('Stage Execution')}\n")
        parts.append(
            generate_stage_execution_section(analysis.stage_info, include_header=False).lstrip("\n")
        )

    # --- D. Data Flow Details ---
    if analysis.data_flow:
        df_full = generate_data_flow_section(
            analysis.data_flow,
            analysis.data_flow_dag,
            include_header=False,
            shuffle_metrics=analysis.shuffle_metrics,
        )
        detail_marker = f"### {_('Data Flow Details')}"
        if detail_marker in df_full:
            idx = df_full.index(detail_marker)
            parts.append("---\n")
            parts.append(f"## D. {_('Data Flow Details')}\n")
            parts.append(df_full[idx + len(detail_marker) :].lstrip("\n"))

    # --- E. Scan Locality Details ---
    scan_content_appendix = generate_scan_locality_section(
        analysis.node_metrics, include_header=False
    )
    if scan_content_appendix.strip():
        parts.append("---\n")
        parts.append(f"## E. {_('Scan Locality Details')}\n")
        parts.append(scan_content_appendix.lstrip("-\n "))

    # --- F. Bottleneck Indicators ---
    parts.append("---\n")
    parts.append(f"## F. {_('Bottleneck Indicators')}\n")
    parts.append(generate_bottleneck_summary(bi))
    parts.append("")

    # --- G. Spill Analysis ---
    spill = generate_spill_analysis_section(bi, include_header=False)
    photon = generate_photon_blockers_section(bi, include_header=False)
    if (spill + photon).strip():
        parts.append("---\n")
        parts.append(f"## G. {_('Spill & Photon Analysis')}\n")
        parts.append(photon)
        parts.append(spill)

    # --- H. Alerts ---
    if bi.alerts:
        parts.append("---\n")
        parts.append(f"## H. {_('All Alerts')}\n")
        parts.append(generate_alerts_section(bi.alerts, include_header=False))

    # --- Footer ---
    model_info = f" ({primary_model})" if primary_model else ""
    parts.append(
        f"\n---\n\n*{_('This report was generated using Databricks Foundation Model APIs.')}{model_info}*\n"
    )

    return "\n".join(parts)


# Re-export all public names for backward compatibility
__all__ = [
    "generate_action_plan_section",
    "generate_alerts_section",
    "generate_aqe_shuffle_section",
    "generate_bottleneck_summary",
    "generate_cloud_storage_section",
    "generate_data_flow_section",
    "generate_explain_section",
    "generate_hot_operators_section",
    "generate_io_metrics_section",
    "generate_performance_metrics",
    "generate_photon_blockers_section",
    "generate_query_overview",
    "generate_recommended_spark_params",
    "generate_report",
    "generate_report_legacy",
    "generate_rule_based_recommendations",
    "generate_rule_based_summary",
    "generate_scan_locality_section",
    "generate_spill_analysis_section",
    "generate_sql_section",
    "generate_stage_execution_section",
    "generate_tuning_guide_section",
    "generate_validation_checklist",
    "generate_warehouse_section",
]
