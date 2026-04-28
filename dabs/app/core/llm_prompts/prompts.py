"""LLM prompt construction for all analysis stages."""

import logging

from ..analyzers.bottleneck import collect_non_sargable_filter_functions
from ..constants import Severity
from ..dbsql_cost import estimate_query_cost, format_cost_usd
from ..evidence import format_evidence_for_prompt
from ..i18n import get_language
from ..models import ProfileAnalysis
from ..utils import format_bytes, format_time_ms
from .knowledge import get_knowledge_section_refs
from .parsing import format_review_for_refine

logger = logging.getLogger(__name__)


_WRITE_STATEMENT_TYPES = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "CTAS"})


def _format_target_table_config(analysis) -> list[str]:
    """Return markdown bullet lines describing the INSERT/CTAS/MERGE target.

    Returns an empty list unless:
      - ``analysis.target_table_info`` is present, AND
      - the target has at least one identifying field populated
        (``table``/``provider``/``clustering_columns``), AND
      - the SQL statement type (when known) is a write operation
        (INSERT / UPDATE / DELETE / MERGE / CREATE / CTAS).

    Gating on statement_type keeps the Target Table Configuration section
    out of SELECT-only prompts, which would otherwise pay a token cost for
    metadata the LLM cannot act on.
    """
    info = getattr(analysis, "target_table_info", None)
    if info is None:
        return []

    # Degenerate / stub TargetTableInfo — nothing to say.
    if not info.table and not info.provider and not info.clustering_columns:
        return []

    # Statement-type gate: only emit for write ops when we know the type.
    sql_analysis = getattr(analysis, "sql_analysis", None)
    stmt = ""
    if sql_analysis is not None:
        structure = getattr(sql_analysis, "structure", None)
        if structure is not None:
            stmt = (getattr(structure, "statement_type", "") or "").upper()
    if stmt and stmt not in _WRITE_STATEMENT_TYPES:
        return []

    lines: list[str] = []
    fmt = "Delta" if info.is_delta else (info.provider or "unknown")
    name = info.full_name or info.table or "?"
    lines.append(f"- **`{name}`** (format: **{fmt}**)")
    if info.clustering_columns:
        flat = [c for group in info.clustering_columns for c in group]
        lines.append(f"  - clustering columns: {', '.join(flat)}")
    if info.hierarchical_clustering_columns:
        lines.append(
            "  - hierarchical clustering: " + ", ".join(info.hierarchical_clustering_columns)
        )
    if info.partitioned_by:
        lines.append(f"  - partitioned by: {', '.join(info.partitioned_by)}")
    notable_keys = (
        "delta.checkpointPolicy",
        "delta.parquet.compression.codec",
        "delta.targetFileSize",
        "delta.enableDeletionVectors",
        "delta.enableRowTracking",
        "delta.autoOptimize.optimizeWrite",
        "delta.autoOptimize.autoCompact",
    )
    notable = [f"{k}={info.properties[k]}" for k in notable_keys if k in info.properties]
    if notable:
        lines.append(f"  - properties: {', '.join(notable)}")
    return lines


# A shuffle is "notable" enough to include in the prompt if ANY of these
# hold. Trivial small shuffles don't help the LLM and only cost tokens.
_SHUFFLE_NOTABLE_MPP_MB = 128
_SHUFFLE_NOTABLE_WRITTEN_BYTES = 1 * 1024**3  # 1 GiB


def _is_notable_shuffle(sm) -> bool:
    """Return True when a shuffle carries signal worth surfacing to the LLM."""
    mpp = sm.memory_per_partition_mb or 0
    spills = sm.sink_num_spills or 0
    skew = sm.aqe_skewed_partitions or 0
    written = sm.sink_bytes_written or 0
    return (
        mpp > _SHUFFLE_NOTABLE_MPP_MB
        or spills > 0
        or skew > 0
        or written >= _SHUFFLE_NOTABLE_WRITTEN_BYTES
    )


def _format_shuffle_details(analysis, top_n: int = 5) -> list[str]:
    """Per-shuffle bullets for the LLM prompt.

    Surfaces the single most important field that the old aggregate-only
    summary was hiding: ``shuffle_attributes`` (the partitioning key).
    Without this, the LLM cannot name the specific shuffle-bottleneck
    key when making recommendations.

    Only *notable* shuffles are included — unhealthy memory-per-partition,
    spilling, AQE-skewed, or GiB-scale writes — so tiny/healthy shuffles
    do not inflate the prompt. Sorted by peak_memory_bytes (worst first).
    Empty-attribute shuffles still render, but their key line is omitted
    so "None" / "[]" never appears in the prompt.
    """
    sms = list(getattr(analysis, "shuffle_metrics", None) or [])
    if not sms:
        return []
    sms = [sm for sm in sms if _is_notable_shuffle(sm)]
    if not sms:
        return []
    sms.sort(key=lambda s: s.peak_memory_bytes or 0, reverse=True)
    lines: list[str] = []
    for sm in sms[:top_n]:
        parts = sm.partition_count or 0
        peak_gb = (sm.peak_memory_bytes or 0) / 1024**3
        written_gb = (sm.sink_bytes_written or 0) / 1024**3
        mpp = sm.memory_per_partition_mb
        healthy = "✓ healthy" if sm.is_memory_efficient else "⚠ UNHEALTHY (>128MB/part)"
        lines.append(
            f"- **Node #{sm.node_id}**: {parts:,} partitions, "
            f"peak {peak_gb:.1f} GB ({mpp:.0f} MB/part {healthy}), "
            f"written {written_gb:.1f} GB"
        )
        if sm.shuffle_attributes:
            key_list = ", ".join(f"`{k}`" for k in sm.shuffle_attributes)
            lines.append(f"  - **Partitioning key(s)**: {key_list}")
        if sm.aqe_skewed_partitions and sm.aqe_skewed_partitions > 0:
            lines.append(f"  - AQE skewed partitions: {sm.aqe_skewed_partitions}")
        if (sm.sink_num_spills or 0) > 0:
            lines.append(f"  - Spill: {sm.sink_num_spills} spill events")
    return lines


def _format_table_scan_info(analysis, max_tables: int = 5, max_types: int = 6) -> list[str]:
    """Return markdown bullet lines describing each scanned table.

    Combines four independent data sources so the LLM does NOT need to guess
    table structure from naming conventions or ask the user to re-check the
    schema:
      1. JSON profile → current_clustering_keys + cardinality (from
         SCAN_CLUSTERS metadata)
      2. EXPLAIN → column data types (from ReadSchema: struct<...>)
      3. EXPLAIN → table format (delta / parquet / iceberg) from
         Relation lines — required to judge Liquid Clustering applicability
      4. JSON profile → bytes_read + rows_scanned + pruning

    Returns an empty list when nothing to say so the caller can decide to
    render the subheader.
    """
    lines: list[str] = []
    tables = list(analysis.top_scanned_tables or [])[:max_tables]

    # Fall back to EXPLAIN-derived table list when no JSON table-scan metrics
    # exist but the user attached EXPLAIN — we still want column types to
    # reach the LLM.
    scan_schemas: dict[str, dict[str, str]] = {}
    table_formats: dict[str, str] = {}
    if analysis.explain_analysis:
        scan_schemas = dict(analysis.explain_analysis.scan_schemas or {})
        for rel in analysis.explain_analysis.relations or []:
            if rel.table_name and rel.format:
                table_formats.setdefault(rel.table_name, rel.format)

    # Collect per-table pushed-down filter conditions from scan NodeMetrics.
    # Without this, the LLM cannot tell that a predicate like
    # ``(ce.MYCLOUD_STARTMONTH = 12BD)`` is already applied at the scan,
    # and may wrongly recommend "add a date filter before JOIN".
    table_pushed_filters: dict[str, list[str]] = {}
    for nm in analysis.node_metrics or []:
        if not nm.filter_conditions:
            continue
        if "scan" not in (nm.node_name or "").lower():
            continue
        # Node name shape: "Scan <catalog>.<schema>.<table>" — trailing
        # token identifies the table. Match against `top_scanned_tables`
        # entries by full-name suffix or short-name equality so that
        # different qualification styles still match.
        name_l = nm.node_name.lower()
        for ts in tables:
            tn_l = (ts.table_name or "").lower()
            if not tn_l:
                continue
            short = tn_l.split(".")[-1]
            if tn_l in name_l or name_l.endswith(short) or f" {short}" in name_l:
                table_pushed_filters.setdefault(ts.table_name, []).extend(nm.filter_conditions)
                break

    if not tables and scan_schemas:
        # Synthesize minimal entries for the type-only case
        for tbl_name in list(scan_schemas.keys())[:max_tables]:
            lines.append(f"- **{tbl_name}**")
            types = scan_schemas.get(tbl_name, {})
            if types:
                type_list = ", ".join(f"`{c}`: {t}" for c, t in list(types.items())[:max_types])
                lines.append(f"  - column types: {type_list}")
            lines.append("  - clustering keys: (unknown — JSON table scan metrics not available)")
        return lines

    for ts in tables:
        fmt = table_formats.get(ts.table_name, "")
        fmt_str = f" [{fmt}]" if fmt else ""
        header = (
            f"- **{ts.table_name}**{fmt_str}: {format_bytes(ts.bytes_read)}, "
            f"{ts.rows_scanned:,} rows, pruning={ts.file_pruning_rate:.1%}"
        )
        lines.append(header)
        # Clustering keys — explicit "none" when empty so the LLM does not
        # infer from table-name suffixes.
        keys = list(ts.current_clustering_keys or [])
        card_map = ts.clustering_key_cardinality or {}
        if keys:
            parts = []
            for k in keys:
                card = card_map.get(k, "unknown")
                parts.append(f"`{k}` [{card}-card]")
            lines.append(f"  - current clustering keys: {', '.join(parts)}")
        else:
            lines.append("  - current clustering keys: none configured")
        # Column types (EXPLAIN-derived) — show up to N, preferring columns
        # that are also clustering keys (likely joined/filtered on).
        types = scan_schemas.pop(ts.table_name, {}) if scan_schemas else {}
        if types:
            ordered: list[tuple[str, str]] = []
            for k in keys:
                if k in types:
                    ordered.append((k, types[k]))
            for c, t in types.items():
                if c not in dict(ordered) and len(ordered) < max_types:
                    ordered.append((c, t))
            if ordered:
                type_list = ", ".join(f"`{c}`: {t}" for c, t in ordered[:max_types])
                lines.append(f"  - column types: {type_list}")
        # Pushed-down filters already applied at this scan. Deduplicate
        # while preserving order — the same filter text can repeat when
        # a table is scanned by multiple nodes.
        pushed = table_pushed_filters.get(ts.table_name) or []
        if pushed:
            seen: set[str] = set()
            uniq: list[str] = []
            for f in pushed:
                if f not in seen:
                    uniq.append(f)
                    seen.add(f)
            lines.append(
                "  - pushed filters (already applied at scan): " + ", ".join(f"`{f}`" for f in uniq)
            )

    # Also surface EXPLAIN-only tables that had no JSON scan metrics.
    for tbl_name, types in list(scan_schemas.items())[:max_tables]:
        lines.append(f"- **{tbl_name}** (EXPLAIN only — no JSON scan metrics)")
        if types:
            type_list = ", ".join(f"`{c}`: {t}" for c, t in list(types.items())[:max_types])
            lines.append(f"  - column types: {type_list}")

    return lines


def _format_explain_v2_insights(ea) -> list[str]:
    """Return markdown bullet lines for Phase-1 EXPLAIN v2 signals.

    Lines are intentionally concrete: column refs, table names, and operator
    names are surfaced verbatim so the LLM can ground its recommendations in
    specific evidence. Returns an empty list when nothing matched so the
    caller can decide whether to render a subheader.
    """
    lines: list[str] = []

    # Implicit CAST on JOIN key — highest priority for rewrite correctness.
    join_casts = [c for c in ea.implicit_cast_sites if c.context == "join"]
    if join_casts:
        examples = [f"{c.column_ref or '?'} → {c.to_type or '?'}" for c in join_casts[:3]]
        extra = f" (+{len(join_casts) - 3} more)" if len(join_casts) > 3 else ""
        lines.append(
            f"- **Implicit CAST on JOIN key** ({len(join_casts)} site(s)): "
            f"{', '.join(examples)}{extra}. "
            "This is direct evidence of a JOIN-key data-type mismatch. "
            "When recommending rewrites, prefer aligning source schema types "
            "over adding a WHERE clause."
        )

    # CTE references with re-compute risk.
    multi_ref_ctes = [c for c in ea.cte_references if c.reference_count >= 2]
    if multi_ref_ctes:
        if ea.has_reused_exchange:
            desc = "CTE multi-references present but ReusedExchange detected"
        else:
            ids = ", ".join(f"#{c.cte_id}×{c.reference_count}" for c in multi_ref_ctes[:3])
            lines.append(
                f"- **CTE not reused** ({len(multi_ref_ctes)} CTE(s) referenced ≥2 "
                f"times without ReusedExchange: {ids}). Spark is re-computing "
                "the CTE body — prefer restructuring the query so the body "
                "runs once (GROUP BY consolidation, window functions, UNION "
                "ALL rewrite). Temp View does not materialize; physical "
                "materialization into a Delta table is only justified when "
                "reuse across queries/sessions clearly outweighs the write "
                "cost."
            )
            desc = None
        if desc:
            lines.append(f"- **CTE reuse**: {desc} (no action needed).")

    # Photon fallback operators in physical plan.
    if ea.photon_fallback_ops:
        names = sorted({op.node_name.split()[0] for op in ea.photon_fallback_ops})
        lines.append(
            f"- **Photon fallback** ({len(ea.photon_fallback_ops)} op(s)): "
            f"{', '.join(names[:5])}. These run on JVM and often cause CPU "
            "cliffs. Rewrite to a Photon-supported form (PIVOT→CASE WHEN, "
            "Python/Scala UDF→built-in, simplify window frames)."
        )

    # Filter pushdown gap — partition pruning not effective.
    gap_scans = [
        fp for fp in ea.filter_pushdown if fp.partition_filters_empty and fp.has_data_filters
    ]
    if gap_scans:
        tables = ", ".join(fp.table_name or "?" for fp in gap_scans[:3])
        lines.append(
            f"- **Partition pruning empty** ({len(gap_scans)} scan(s): "
            f"{tables}). Row filters exist but PartitionFilters is []. "
            "Either the table is not partitioned on the filter columns, or "
            "the filter is applied after the scan. Consider Liquid Clustering."
        )

    # Aggregate phase split (INFO — volume-reducing opportunity).
    if ea.aggregate_phases and len(ea.aggregate_phases) == 1:
        p = ea.aggregate_phases[0]
        if not p.has_partial_functions and not p.has_final_merge:
            lines.append(
                "- **Aggregate without partial/final split**: pre-aggregation "
                "before shuffle is not being used. For large inputs, "
                "consider a CTE-level pre-aggregation or enabling AQE "
                "partial aggregation."
            )

    # Join strategy — when a shuffled hash join is paired with visible broadcast
    # mode hints, the plan may be unexpectedly falling back from broadcast.
    broadcast_shuffle_mix = [j for j in ea.join_strategies if not j.is_broadcast and j.build_side]
    if broadcast_shuffle_mix and len(broadcast_shuffle_mix) >= 2:
        lines.append(
            f"- **Shuffled hash joins** ({len(broadcast_shuffle_mix)}): "
            "build side chosen as "
            f"{', '.join(set(j.build_side for j in broadcast_shuffle_mix))}. "
            "If any build side is smaller than autoBroadcastJoinThreshold, "
            "check why broadcast was not selected (stats freshness, size "
            "estimate, or explicit hint)."
        )

    # Non-JOIN implicit CAST sites — still evidence of over-cast chains in
    # filter / aggregate / project contexts that can block pushdown.
    non_join_casts = [c for c in ea.implicit_cast_sites if c.context != "join"]
    if non_join_casts:
        # Group by (context, to_type) to produce compact summary
        from collections import Counter

        grouping = Counter((c.context, c.to_type or "?") for c in non_join_casts)
        examples_str = ", ".join(f"{ctx}→{ty} ×{n}" for (ctx, ty), n in grouping.most_common(3))
        lines.append(
            f"- **Non-JOIN implicit CAST** ({len(non_join_casts)} site(s)): "
            f"{examples_str}. DECIMAL(38,…) promotions and filter-time CAST can "
            "silently disable predicate pushdown. If the source column type "
            "allows, align the literal/type at the source side."
        )

    # Multi-stage aggregate — surface when there are several aggregate nodes
    # and partial/final markers are mixed. Single-agg case is already handled
    # separately (missing_partial_aggregate).
    if ea.aggregate_phases and len(ea.aggregate_phases) >= 2:
        partials = sum(1 for p in ea.aggregate_phases if p.has_partial_functions)
        finals = sum(1 for p in ea.aggregate_phases if p.has_final_merge)
        if partials + finals < len(ea.aggregate_phases):
            lines.append(
                f"- **Aggregate stages** ({len(ea.aggregate_phases)} nodes, "
                f"partial={partials}, final={finals}): some stages lack "
                "partial/final markers. Verify that pre-aggregation is "
                "running before shuffle for each grouping key."
            )

    # Exchange partitioning detail — surfaces the actual intended partition
    # count per exchange so the LLM can reason about over/under-partitioning.
    if ea.exchanges:
        hash_exchanges = [e for e in ea.exchanges if e.partitioning_type == "hash"]
        range_exchanges = [e for e in ea.exchanges if e.partitioning_type == "range"]
        if hash_exchanges:
            sizes = sorted({e.num_partitions for e in hash_exchanges if e.num_partitions})
            if sizes:
                lines.append(
                    f"- **Hash exchanges** ({len(hash_exchanges)}): "
                    f"intended partition counts = {sizes}. "
                    "Compare against spark.sql.shuffle.partitions to detect "
                    "mis-sized shuffles (AQE may have further coalesced)."
                )
        if range_exchanges:
            lines.append(
                f"- **Range exchanges** ({len(range_exchanges)}): present — "
                "usually from ORDER BY / window. Check if the sort is "
                "required by output or only by intermediate processing."
            )

    # AQE state — indicates whether the plan reported represents the final
    # runtime plan or a pre-materialization snapshot.
    if ea.is_adaptive:
        if ea.is_final_plan:
            lines.append(
                "- **AQE**: adaptive plan captured AFTER materialization "
                "(isFinalPlan=true). Metrics reflect actual runtime decisions."
            )
        else:
            lines.append(
                "- **AQE**: adaptive plan captured BEFORE materialization "
                "(isFinalPlan=false). Subquery / broadcast / skew decisions "
                "may still evolve — use metrics not plan shape for final truth."
            )

    # Photon reference nodes — additional context for Photon blockers.
    if ea.photon_explanation and ea.photon_explanation.reference_nodes:
        ref_count = len(ea.photon_explanation.reference_nodes)
        if ref_count > 0:
            first_ref = (ea.photon_explanation.reference_nodes[0] or "")[:120]
            lines.append(
                f"- **Photon Reference Node(s)** ({ref_count}): e.g., "
                f"`{first_ref}`. Use these to locate the exact operator "
                "holding a Photon blocker."
            )

    return lines


# =============================================================================
# Reusable prompt fragments
# =============================================================================


def _v6_canonical_output_directive(lang: str) -> str:
    """V6 (Codex指摘 #1): when V6_CANONICAL_SCHEMA=on, ask the LLM to emit
    a canonical Report JSON block in addition to the human-readable output.

    The block must be wrapped in triple-backtick fence with language tag
    `json:canonical_v6` so downstream extraction is unambiguous.

    Returns empty string when flag is off (preserves v5.19 prompt verbatim).
    """
    try:
        from core import feature_flags  # noqa: WPS433
        if not feature_flags.canonical_schema():
            return ""
    except ImportError:
        return ""

    # Build the canonical issue_id allowlist from the registry. Smoke n=5
    # (2026-04-26) showed V6 LLM-direct emits creative issue_ids
    # ("full_outer_join_data_explosion", "cache_hit_ratio_medium")
    # instead of canonical ones, breaking recall_strict scoring.
    # Codex review (2026-04-27): the 31-id long-form list was too
    # verbose and competed with the canonical_v6 emission directive
    # (40% LLM-direct success rate). Compress to a single-line group
    # of ``id (category)`` tokens — same allowlist, ~80% fewer tokens.
    try:
        from core.v6_schema.issue_registry import ISSUES  # noqa: WPS433

        issue_list = ", ".join(f"{i.id} ({i.category})" for i in ISSUES)
    except ImportError:
        issue_list = ""

    if lang == "ja":
        template = """

---

**【必須】最終出力の最後に canonical_v6 ブロックを置くこと (V6_CANONICAL_SCHEMA=on)**

人間向けレポートを書き終えたら、**応答の最後**に必ず以下の triple-backtick fence で `json:canonical_v6` ブロックを追加してください。これは省略不可です。フォーマット:

```json:canonical_v6
{
  "schema_version": "v6.0",
  "summary": {"headline": "...", "verdict": "healthy|needs_attention|critical|skipped_cached|informational"},
  "findings": [
    {
      "issue_id": "spill_dominant",
      "category": "memory",
      "severity": "high",
      "confidence": "high|medium|low|needs_verification",
      "title": "...",
      "evidence": [
        {"metric": "peak_memory_bytes", "value_display": "12 GB", "value_raw": 12884901888, "source": "profile.queryMetrics", "grounded": true}
      ],
      "actions": [
        {
          "action_id": "increase_warehouse",
          "target": "warehouse_size",
          "fix_type": "configuration",
          "what": "...",
          "why": "...",
          "fix_sql": "SET ...",
          "expected_effect": "...",
          "verification": [{"type": "metric", "metric": "spill_bytes", "expected": "0"}]
        }
      ]
    }
  ]
}
```

ルール:
- **`issue_id` は以下の allowlist から選ぶこと** (creative naming 禁止、該当する id が無ければ finding 自体を出さない): {issue_list}
- profile に存在する metric 名のみを `evidence.metric` に
- `evidence.grounded=true` は profile に metric が実在する場合のみ
- 根拠不足のフィールドは省略すること (formatの全項目を埋めなくて良い)
- `evidence.source` は次の prefix のみ: `profile.{{queryMetrics,alerts,shuffleDetails,scanCoverage,signals}}` / `alert:{{io,shuffle,spill,photon,join,cluster,cache}}` / `node[<id>]` / `knowledge:<section_id>`。「I/O Metrics」等の自由記述は禁止
- **`evidence.value_raw` の型は厳密に `number | string | null` のみ**。Boolean (`true` / `false`)、配列、オブジェクトは schema 違反で reject される。「該当しない / 検出されず」を表したい場合は `value_raw` を**省略する**か `null` を使うこと。値が存在しないのに `false` を入れない
- **数値 grounding（厳守）**: narrative / executive summary / recommendations 内に登場する**全ての数値** (バイト数 / 秒 / 倍率 / パーセンテージ / 件数) は、対応する `evidence.value_display` に **逐語一致** で出ていなければならない。派生比率 (X倍 / Y%) は計算元 metric と derived ratio を別 evidence エントリで明示。アンカー無しの「約 12GB」「概ね 50% 程度」「数倍」のような曖昧表現は禁止 — anchor が無い場合は数値を出さず質的記述で済ませること"""
        return template.replace("{issue_list}", issue_list)
    template = """

---

**[REQUIRED] End your response with a canonical_v6 block (V6_CANONICAL_SCHEMA=on)**

After the human-readable report, **the very last thing in your response** MUST be a triple-backtick fence with language tag `json:canonical_v6` containing the canonical Report. Do NOT omit this block. Format:

```json:canonical_v6
{
  "schema_version": "v6.0",
  "summary": {"headline": "...", "verdict": "healthy|needs_attention|critical|skipped_cached|informational"},
  "findings": [
    {
      "issue_id": "spill_dominant",
      "category": "memory",
      "severity": "high",
      "confidence": "high|medium|low|needs_verification",
      "title": "...",
      "evidence": [
        {"metric": "peak_memory_bytes", "value_display": "12 GB", "value_raw": 12884901888, "source": "profile.queryMetrics", "grounded": true}
      ],
      "actions": [
        {
          "action_id": "increase_warehouse",
          "target": "warehouse_size",
          "fix_type": "configuration",
          "what": "...",
          "why": "...",
          "fix_sql": "SET ...",
          "expected_effect": "...",
          "verification": [{"type": "metric", "metric": "spill_bytes", "expected": "0"}]
        }
      ]
    }
  ]
}
```

Rules:
- **`issue_id` MUST be from this allowlist** (creative naming forbidden; if none fit, omit the finding): {issue_list}
- Only profile-existing metric names go into `evidence.metric`
- `evidence.grounded=true` only when the metric is actually present in the profile
- Omit fields you cannot ground in the profile (do NOT force-fill the format)
- `evidence.source` prefixes only: `profile.{{queryMetrics,alerts,shuffleDetails,scanCoverage,signals}}` / `alert:{{io,shuffle,spill,photon,join,cluster,cache}}` / `node[<id>]` / `knowledge:<section_id>`. No free-form labels like "I/O Metrics".
- **`evidence.value_raw` type is strictly `number | string | null`**. Booleans (`true`/`false`), arrays, and objects violate the schema and are rejected. To indicate "absent / not detected", **omit** `value_raw` or use `null` — do NOT emit `false` to mean "missing".
- **Numeric grounding (MANDATORY)**: every number you cite in narrative / executive summary / recommendations (bytes, seconds, ratios, percentages, counts) MUST appear **verbatim** in some `evidence.value_display`. Derived ratios (Nx, Y%) need source metrics PLUS the ratio itself as separate evidence entries. Hand-wavy expressions ("about 12 GB", "roughly 50%", "several times") are forbidden — when no anchor exists, drop the number and use qualitative wording instead."""
    return template.replace("{issue_list}", issue_list)


def _no_force_fill_block(lang: str) -> str:
    """V6 (Codex指摘 #8): emit "skip if not grounded" instruction when
    `feature_flags.recommendation_no_force_fill()` is on. Empty string
    otherwise — keeps default v5.19 behavior unchanged."""
    try:
        from core import feature_flags  # noqa: WPS433
        if not feature_flags.recommendation_no_force_fill():
            return ""
    except ImportError:
        return ""
    if lang == "ja":
        return (
            "\n\n**根拠が profile に anchor できないフィールドは省略してください。"
            "数値予測（% / GB / 倍 / 秒）は profile evidence に明示的に対応する場合のみ。"
            "推測で format を埋めないでください。 (V6_RECOMMENDATION_NO_FORCE_FILL)**"
        )
    return (
        "\n\n**Omit fields that cannot be anchored in profile evidence. "
        "Numeric predictions (%/GB/x/s) only when they correspond to a specific "
        "profile metric. Do NOT fill the format on speculation. "
        "(V6_RECOMMENDATION_NO_FORCE_FILL)**"
    )


def _korean_output_directive(lang: str) -> str:
    """v6.8.0: Korean LLM-output directive.

    The prompt builders below carry full ja / en branches. Adding a
    parallel ko branch for every prompt would be a major rewrite, so
    when ``lang == "ko"`` we keep the English prompt structure and
    append a single output-language directive instructing the LLM to
    respond in Korean. The LLM follows this reliably in practice, and
    Korean output quality is good enough for v6.8.0 MVP. Section
    headers and structured fields will appear as the English prompt
    requests but the prose is rendered in Korean.

    Returns "" for non-ko languages so existing en/ja prompts are
    unaffected.
    """
    if lang != "ko":
        return ""
    return (
        "\n\n## Output Language (MANDATORY)\n"
        "Write the entire report in Korean (한국어). Keep section headers, "
        "code snippets, SQL keywords, metric names, and JSON keys in their "
        "original English form, but translate all narrative prose, "
        "explanations, and recommendations into natural Korean. "
        "Use Korean technical conventions (e.g., 디스크 스필, 셔플, 캐시 "
        "히트율) where they exist; otherwise leave the English term "
        "unchanged in parentheses. Do NOT mix Japanese phrasing.\n"
    )


def _append_korean_directive(func):
    """Decorator (v6.8.0): append the Korean output directive to a
    system-prompt builder's return value when ``lang == "ko"``.

    All 5 main system-prompt builders (analyze / structured / review /
    refine / rewrite) carry the EN/JA branch internally and accept
    ``lang`` either positionally (2nd) or as kwarg. Rather than weave a
    third branch through every f-string, wrap the builder so the
    Korean directive is appended once at the end. Falls through to the
    EN prompt body, which the LLM then renders in Korean per the
    directive.
    """
    import functools as _ft

    @_ft.wraps(func)
    def _wrapper(*args, **kwargs):
        lang = kwargs.get("lang")
        if lang is None and len(args) >= 2 and isinstance(args[1], str):
            lang = args[1]
        if lang is None:
            lang = get_language()
        return func(*args, **kwargs) + _korean_output_directive(lang)

    return _wrapper


def _recommendation_format_block(lang: str) -> str:
    """Return the evidence-constrained recommendation format template.

    Note on Priority:
      The rendered report ranks actions purely by their position in the
      list, with Impact/Effort as the severity signal. **Do NOT emit any
      Priority column, badge, or score** — earlier templates used
      ``Priority: X/10`` which became redundant once the report moved to
      a curated Top-5 with inline Impact/Effort badges. The Action Plan
      JSON still carries a ``priority`` field for structured consumers,
      but it must never appear in the human-readable markdown.
    """
    if lang == "ja":
        return """**まずサマリー表を出力し、その後に各項目の詳細を記述すること。**

**重要: 出力に `Priority` 列・バッジ・スコア（例: `10/10`）を一切含めないこと。**
サマリー表の列は下記 3 列のみ、各項目の見出し直下のバッジ行も Impact と Effort だけを書くこと。

**並び順（厳守）:** 「**より簡単で効果の大きいもの**」を上に置く。具体的には Impact 降順 (HIGH → MEDIUM → LOW) を主キー、Effort 昇順 (LOW → MEDIUM → HIGH) を副キーで並べる。同 impact 内では low effort の quick win が必ず先頭、HIGH/HIGH（大効果だが大改修）は同 impact の HIGH/LOW より下に置くこと。

| # | アクション | 予測改善 |
|---|-----------|---------|
| 1 | ... | スキャン -X%, Shuffle -Y% |
| 2 | ... | ... |

### 1. [問題タイトル]

🔴 Impact: HIGH | 🟡 Effort: LOW

**根拠**
- [重要度][カテゴリ] Fact Packのアラートまたはメトリクス値を引用（必須）

**原因推定:** なぜこの問題が起きているかの因果仮説

**改善策:** 具体的なアクション。**実行可能なSQLを必ず含めること**（テーブル名・カラム名はFact Packから使用）:
```sql
-- 実行可能なSQL（テーブル名は実テーブルを使う）
```

**予測改善:** メトリクスに基づく改善予測。スピル削減率、スキャン量削減率、Shuffle削減率等を推定。
定量化できない場合は方向性（「改善見込み」「大幅改善」等）で記述。

**確度:** high / medium / needs_verification（下記の確度判定基準を参照）

**反証:** 矛盾するシグナルがあれば記述（なければ "なし"）

### 2, 3... 同様の形式で記述

Impact/Effort の基準:
- Impact: HIGH=大幅な改善が見込める, MEDIUM=一定の改善, LOW=軽微な改善
- Effort: LOW=設定変更のみ, MEDIUM=SQL書き換え必要, HIGH=テーブル再設計必要""" + _no_force_fill_block("ja")
    else:
        return """**Output the summary table FIRST, then detailed items below.**

**IMPORTANT: Do NOT include any `Priority` column, badge, or score (e.g. `10/10`) in the output.**
The summary table has only the 3 columns below, and each item's badge
line under the heading must contain only Impact and Effort.

**Ordering rule (MANDATORY):** put the **easier-and-higher-impact** action first. Primary key is Impact descending (HIGH → MEDIUM → LOW); within the same impact bucket, Effort ascending (LOW → MEDIUM → HIGH). A LOW-effort quick win must always come before a HIGH-effort fix at the same impact tier — HIGH/HIGH (big payoff, big rewrite) ranks below HIGH/LOW.

| # | Action | Predicted Improvement |
|---|--------|-----------------------|
| 1 | ... | Scan -X%, Shuffle -Y% |
| 2 | ... | ... |

### 1. [Problem Title]

🔴 Impact: HIGH | 🟡 Effort: LOW

**Rationale**
- [SEVERITY][CATEGORY] cite specific alert or metric from Fact Pack (REQUIRED)

**Cause Hypothesis:** Why this problem is occurring

**Improvement:** Specific action to take. **MUST include executable SQL** (use real table/column names from Fact Pack):
```sql
-- executable SQL with real table names
```

**Predicted Improvement:** Estimate based on metrics. Scan reduction %, shuffle reduction %, spill reduction %, etc.
When quantification is not possible, use directional language ("expected improvement", "significant reduction").

**Confidence:** high / medium / needs_verification (see Confidence Criteria below)

**Counter-evidence:** Conflicting signals if any (write "None" if none)

### 2, 3... same format

Impact/Effort criteria:
- Impact: HIGH=significant improvement expected, MEDIUM=moderate improvement, LOW=minor improvement
- Effort: LOW=config change only, MEDIUM=SQL rewrite needed, HIGH=table redesign needed""" + _no_force_fill_block("en")


def _constraints_block(lang: str) -> str:
    """Return the constraints block with hard rules and confidence criteria."""
    if lang == "ja":
        return """### 確度判定基準
- **high**: 推奨設定がナレッジに存在し、かつその前提条件が現在のメトリクスで確認済み
- **medium**: 推奨設定がナレッジに存在するが前提条件が不明、または一般的なベストプラクティス
- **needs_verification**: 推奨設定がナレッジに存在しない、またはメトリクスの根拠が不十分

制約:
- 推奨事項には必ず根拠を明示すること（対応するアラートまたはメトリクス値を引用）
- Sparkパラメータや設定値を記載する場合は、提供されたナレッジのコードブロックから正確に引用すること
- ナレッジに記載のない設定値を推奨する場合は確度を「needs_verification」とすること
- 各推奨の確度を上記の判定基準に従って high/medium/needs_verification で示すこと

厳守ルール:
- Fact Packに存在しない事実を断定してはならない
- アラート間に矛盾がある場合、反証フィールドで必ず言及すること
- 各推奨事項は [重要度][カテゴリ] タグで最低1つのアラートを参照すること
- ナレッジに記載のない設定値の確度は必ず「needs_verification」とすること
- 確度がmediumまたはneeds_verificationの場合、具体的な改善率の数値（例:「50-70%短縮」）を断定してはならない。代わりにコンテキスト比率に基づく定性表現（例:「シャッフル比率~15%を上限とする改善」）を使うこと。数値範囲はhigh確度かつナレッジにベンチマークがある場合のみ許可

### マテリアライズに関する禁則事項（厳守）
CTE 多重参照による再計算（Physical Plan に `ReusedExchange` が出ず、同じスキャン/集約が複数ノードで繰り返されている状態）を解消する目的で、以下を**推奨してはなりません**:
- **`CREATE TEMP VIEW` への単純置換** — Temp View は実体化を保証しません（カタログ上のエイリアスのみ）。CTE と同じく、実際に再利用されるかは optimizer と AQE 次第（`ReusedExchange` が Physical Plan に現れれば再利用、現れなければ再計算）。**CTE で再利用されなかった subtree は、単純に Temp View に置換しても同じ理由で再利用されない**ため、再計算問題は解消しません
- **`CREATE TABLE AS SELECT` / `CREATE OR REPLACE TABLE` への物理マテリアライズ（CTAS）** — 書き込みコストが発生するため、**複数セッションや複数クエリでの再利用が明確に見込める場合のみ**推奨可。一度限りのクエリでは書き込みコストが改善効果を上回る可能性があります

**CTE 多重参照の解消は、以下のクエリ書き換えを第一候補とせよ:**
- 複数の GROUP BY を 1 つに統合（window 関数 `MAX() OVER()` / `SUM() OVER()` などで集計結果を共有）
- UNION ALL の両分岐で同じ CTE を参照している場合、参照を集約して JOIN 1 回にまとめる
- EXISTS / IN サブクエリを事前集約または JOIN に置き換える
- 具体的な書き換え SQL は元クエリの構造に合わせて提示すること

### メトリクス根拠の優先ルール（厳守）
- SQL 本文が切り詰められている（`sql_truncated_in_prompt: true`）場合でも、`scan_coverage.verdict` がフルスキャン系（`full-table scan confirmed` もしくは `scan pruning ineffective`）を報告している場合、「SQL が切り詰められているため確認できない」等の hedging は禁止。プルーニング率は SQL 本文に依存しないメトリクスベースの根拠であり、verdict の強弱にかかわらず近似フルスキャンとして扱うこと。ただし verdict 文言が `full-table scan confirmed`（プルーニング厳密に 0%）か `near-full scan`（<1% だが 0 ではない）かは区別して記述すること
- シャッフル・スキュー・broadcast 関連の推奨を書く際、`Shuffle Details` セクションに `Partitioning key(s)` が記載されていれば**原則としてそのキー名を引用**すること。汎用表現（「シャッフルが大きい」「スキューの可能性」等）のみで済ませてはならない。ただし、キーが式・synthetic key・長大な複合キーで可読列名として表現しづらい場合はその旨を注記した上で要約表現も許容する
- `Table Scan Info` の `pushed filters (already applied at scan)` に列挙されている述語は**既にスキャン側で適用済み**である。これらの列への WHERE 句追加 / 絞り込み追加を推奨してはならない（例: 当該スキャンに `(ce.date_col = 12BD)` が列挙されている状態で「JOIN 前に date_col でフィルタを追加せよ」と推奨するのは矛盾である）。既存フィルタが期待どおりに効いていない場合は、フィルタ**追加**ではなく、プルーニング不発の原因（左辺関数化、非 SARGable、クラスタリング不適合等）を指摘すること"""
    else:
        return """### Confidence Criteria
- **high**: Setting/recommendation exists in the provided knowledge AND its precondition is confirmed by current metrics
- **medium**: Setting exists in knowledge BUT precondition is unclear from metrics, OR it is a general best practice
- **needs_verification**: Setting NOT found in knowledge, OR insufficient metric evidence to support it

Constraints:
- Each recommendation MUST cite its evidence (corresponding alert or metric value)
- When suggesting Spark parameters or settings, quote exactly from the provided knowledge code blocks
- If recommending settings not found in the knowledge, set confidence to "needs_verification"
- Show confidence level per the Confidence Criteria above (high/medium/needs_verification)

HARD RULES:
- NEVER assert facts not present in the Fact Pack
- If contradicting evidence exists among alerts, MUST mention it in Counter-evidence field
- Each recommendation MUST reference at least one alert by [SEVERITY][CATEGORY] tag
- Settings not found in knowledge MUST have confidence "needs_verification"
- NEVER state precise numeric improvement predictions (e.g., "50-70% reduction") when confidence is medium or needs_verification. Use qualitative range expressions anchored to context ratios instead (e.g., "bounded by ~15% shuffle impact"). Numeric ranges are ONLY permitted for high-confidence items backed by knowledge-base benchmarks.

### Materialization anti-patterns (MANDATORY)
"Multi-reference CTE re-computation" here means: `ReusedExchange` is absent from the Physical Plan AND the same scan/aggregate appears multiple times for the same underlying data. When addressing this state, the following MUST NOT be recommended:
- **Converting a CTE to `CREATE TEMP VIEW`** — A Temp View does NOT guarantee materialization (it is a catalog alias only). Whether its body is actually reused depends on the optimizer + AQE (reused when `ReusedExchange` appears in the physical plan, otherwise recomputed) — the SAME mechanism that governs CTEs. **If the CTE was NOT being reused, a naive Temp View replacement will also fail to be reused for the same reason** — re-computation is NOT resolved.
- **Physical materialization via `CREATE TABLE AS SELECT` / `CREATE OR REPLACE TABLE` (CTAS)** — This incurs write cost. Recommend ONLY when the intermediate result is reused across multiple sessions or multiple queries; for one-off analytical queries the write cost may exceed the saved recomputation cost.

**For multi-reference CTE re-computation, prefer QUERY REWRITES as the first-line solution:**
- Consolidate multiple GROUP BYs into a single pass using window functions (`MAX() OVER()`, `SUM() OVER()`) to share the aggregation
- When both branches of a UNION ALL reference the same CTE, restructure so the CTE body runs once and the result is joined back
- Replace EXISTS / IN subqueries with pre-aggregation or explicit JOINs
- Provide concrete rewrite SQL matched to the original query structure.

### Metric-based evidence precedence (MANDATORY)
- When the SQL is truncated (`sql_truncated_in_prompt: true`) and the Fact Pack's `scan_coverage.verdict` reports either `full-table scan confirmed` or `scan pruning ineffective`, you MUST still report the (near-)full scan finding. Pruning ratios are metric-based evidence that does NOT depend on SQL text visibility. Hedging like "SQL is truncated so we cannot confirm whether there is a WHERE clause" is FORBIDDEN in this situation. Preserve the verdict's strength — distinguish strict `full-table scan confirmed` (both pruning ratios exactly 0%) from `near-full scan` (<1% but not exactly 0).
- When writing shuffle / skew / broadcast recommendations, if the `Shuffle Details` section lists `Partitioning key(s)`, you SHOULD cite those key names by name. Avoid generic phrasing ("shuffle is large", "possible skew") when the exact key is available. Exception: if the key is a non-trivial expression, a synthetic key, or an unwieldy long composite key, a summary description is acceptable *provided the prompt explicitly notes the key is non-readable*.
- Predicates listed under `pushed filters (already applied at scan)` in `Table Scan Info` are **already applied at the scan layer**. You MUST NOT recommend adding a WHERE clause / filter on those columns (e.g. when a scan lists `(ce.date_col = 12BD)` you cannot say "add a date_col filter before JOIN" — it already exists). If existing filters are not pruning effectively, diagnose *why* (non-SARGable form, function on the left-hand side, clustering key mismatch, etc.) — do NOT propose re-adding the same filter."""


def _action_plan_json_schema(lang: str) -> str:
    """Return the Action Plan JSON schema template for system prompts."""
    if lang == "ja":
        return """推奨事項のMarkdown出力の後に、以下のマーカーとJSON形式でも構造化Action Planを出力してください:

<!-- ACTION_PLAN_JSON -->
```json
[
  {{
    "priority": 1,
    "problem": "問題の簡潔な説明",
    "fix": "具体的な修正アクション",
    "fix_sql": "fix が SQL/DDL/DML/SET/OPTIMIZE/ALTER/ANALYZE 系アクションを述べる場合は必須。実行可能な完全な SQL を 1 行で記載すること。warehouse サイズ変更など non-SQL アクションのみの場合は空文字で可",
    "risk": "low/medium/high",
    "risk_reason": "リスクの理由",
    "expected_impact": "high/medium/low",
    "effort": "low/medium/high",
    "confidence": "high/medium/needs_verification",
    "confidence_reason": "確度の理由（ナレッジセクション名またはメトリクスを引用）",
    "verification": [
      {{"metric": "確認メトリクス名", "expected": "期待値"}},
      {{"sql": "確認用SQL", "expected": "期待される結果"}}
    ]
  }}
]
```"""
    else:
        return """After the Markdown recommendations, also output a structured Action Plan in the following format:

<!-- ACTION_PLAN_JSON -->
```json
[
  {{
    "priority": 1,
    "problem": "Brief problem description",
    "fix": "Specific fix action",
    "fix_sql": "Required when fix describes a SQL/DDL/DML/SET/OPTIMIZE/ALTER/ANALYZE action. Provide a complete executable single-line SQL statement. Use an empty string only for non-SQL actions such as warehouse resizing",
    "risk": "low/medium/high",
    "risk_reason": "Why this risk level",
    "expected_impact": "high/medium/low",
    "effort": "low/medium/high",
    "confidence": "high/medium/needs_verification",
    "confidence_reason": "Why this confidence level (cite knowledge section or metric)",
    "verification": [
      {{"metric": "metric_name", "expected": "expected value"}},
      {{"sql": "verification SQL", "expected": "expected result"}}
    ]
  }}
]
```"""


def _build_fact_pack_summary(analysis: "ProfileAnalysis", lang: str) -> str:
    """Build a machine-readable summary block for LLM focus.

    This YAML-like block is placed before detailed metrics so the LLM
    anchors its analysis on the most important signals first.
    """
    bi = analysis.bottleneck_indicators
    parts: list[str] = []

    # top_alerts: top 5 by severity
    if bi.alerts:
        sorted_alerts = sorted(bi.alerts, key=lambda a: _severity_order(a.severity))[:5]
        lines = ["top_alerts:"]
        for a in sorted_alerts:
            lines.append(
                f"  - [{a.severity.value.upper()}][{a.category}] {a.message} "
                f"(current={a.current_value}, threshold={a.threshold})"
            )
        parts.append("\n".join(lines))

    # dominant_operations: top 3 by duration from data_flow
    if analysis.data_flow:
        sorted_ops = sorted(analysis.data_flow, key=lambda e: e.duration_ms, reverse=True)[:3]
        total_dur = sum(e.duration_ms for e in analysis.data_flow) or 1
        lines = ["dominant_operations:"]
        for op in sorted_ops:
            share = op.duration_ms / total_dur * 100
            lines.append(
                f"  - {op.operation}: {share:.1f}% of total duration, {op.output_rows:,} rows"
            )
        parts.append("\n".join(lines))

    # alert_contradictions: from conflicts_with field on Alert
    if bi.alerts:
        alert_map = {a.alert_id: a for a in bi.alerts if hasattr(a, "alert_id")}
        contradiction_pairs: set[tuple[str, str]] = set()
        for a in bi.alerts:
            for conflict_id in a.conflicts_with:
                _pair_list = sorted([a.alert_id, conflict_id])
                pair: tuple[str, str] = (_pair_list[0], _pair_list[1])
                if pair not in contradiction_pairs and conflict_id in alert_map:
                    contradiction_pairs.add(pair)
        if contradiction_pairs:
            lines = ["alert_contradictions:"]
            for id1, id2 in sorted(contradiction_pairs):
                a1 = alert_map.get(id1)
                a2 = alert_map.get(id2)
                if a1 and a2:
                    lines.append(f"  - {a1.message} vs {a2.message}")
            parts.append("\n".join(lines))

    # confidence_notes: metrics with low confidence
    notes: list[str] = []
    if analysis.shuffle_metrics:
        for sm in analysis.shuffle_metrics:
            if sm.partition_count > 0 and sm.partition_count == getattr(sm, "num_tasks", 0):
                notes.append(
                    "  - partition_count: estimated from task count (not directly reported)"
                )
                break
    if notes:
        parts.append("confidence_notes:\n" + "\n".join(notes))

    # sql_context: signal to LLM that SQL is provided (full text is in ## SQL Query section below)
    qm = analysis.query_metrics
    sql_lines = ["sql_context:"]
    has_sql = bool((analysis.sql_analysis and analysis.sql_analysis.formatted_sql) or qm.query_text)
    sql_lines.append(f"  sql_provided: {'true' if has_sql else 'false'}")
    if has_sql:
        sql_src = (
            analysis.sql_analysis.formatted_sql
            if analysis.sql_analysis and analysis.sql_analysis.formatted_sql
            else qm.query_text
        )
        sql_lines.append(f"  sql_length: {len(sql_src)}")
        truncated = len(sql_src) > 3000
        sql_lines.append(f"  sql_truncated_in_prompt: {'true' if truncated else 'false'}")
        # Extract table names from sql_analysis if available
        if analysis.sql_analysis and analysis.sql_analysis.tables:
            tables = [t.full_name for t in analysis.sql_analysis.tables[:10]]
            sql_lines.append(f"  tables: [{', '.join(tables)}]")
        # Join/CTE counts from sql_analysis.structure
        if analysis.sql_analysis and analysis.sql_analysis.structure:
            s = analysis.sql_analysis.structure
            if s.join_count:
                sql_lines.append(f"  join_count: {s.join_count}")
            if s.cte_count:
                sql_lines.append(f"  cte_count: {s.cte_count}")
            if s.subquery_count:
                sql_lines.append(f"  subquery_count: {s.subquery_count}")
    parts.append("\n".join(sql_lines))

    # scan_coverage: evidence-based scan-effectiveness verdict from
    # metrics. Two strength levels so near-zero pruning (e.g., 0.5%) is
    # not conflated with strictly zero pruning:
    #   - strict full-table scan  → file_prune == 0 AND byte_prune == 0
    #   - near-full scan / pruning ineffective → both < 1% but not both 0
    # The strict verdict short-circuits "SQL was truncated so we cannot
    # confirm" hedging; the weaker verdict notes the concern without
    # forbidding hedging outright.
    bi = analysis.bottleneck_indicators
    total_files = (qm.read_files_count or 0) + (qm.pruned_files_count or 0)
    file_prune = (qm.pruned_files_count / total_files) if total_files > 0 else 0.0
    byte_prune = bi.bytes_pruning_ratio or 0.0
    if (qm.read_bytes or 0) > 0 and file_prune < 0.01 and byte_prune < 0.01:
        strict_full = file_prune == 0.0 and byte_prune == 0.0
        cov_lines = ["scan_coverage:"]
        cov_lines.append(
            f"  files_read: {qm.read_files_count:,}, files_pruned: {qm.pruned_files_count:,}"
        )
        cov_lines.append(f"  file_pruning_rate: {file_prune:.1%}")
        cov_lines.append(f"  bytes_pruning_ratio: {byte_prune:.1%}")
        if strict_full:
            cov_lines.append(
                "  verdict: full-table scan confirmed — both file and byte "
                "pruning are exactly 0%, which is metric-based evidence "
                "independent of SQL text visibility. Do NOT hedge with "
                "'SQL truncated so we cannot confirm'."
            )
        else:
            cov_lines.append(
                "  verdict: scan pruning ineffective (near-full scan) — "
                "pruning <1% but not exactly 0. Treat as near-full scan; "
                "hedging with 'SQL truncated so we cannot confirm' is still "
                "inappropriate because the signal is metric-based."
            )
        parts.append("\n".join(cov_lines))

    # lakehouse_federation: surface the extractor's authoritative
    # detection in the Fact Pack so the LLM treats it as ground truth
    # rather than something to deduce from raw evidence. Without this
    # block, narratives hedge with "BigQuery / Lakehouse Federation 経由
    # である可能性が高い" even though ROW_DATA_SOURCE_SCAN_EXEC has
    # already been observed. v6.6.8.
    if qm.is_federation_query:
        fed_lines = ["lakehouse_federation:"]
        fed_lines.append("  is_federation_query: true")
        if qm.federation_source_type:
            fed_lines.append(f"  source_type: {qm.federation_source_type}")
        else:
            fed_lines.append("  source_type: unknown")
        if qm.federation_tables:
            shown = qm.federation_tables[:5]
            fed_lines.append(f"  tables: [{', '.join(shown)}]")
            if len(qm.federation_tables) > 5:
                fed_lines.append(f"  tables_total: {len(qm.federation_tables)}")
        fed_lines.append(
            "  evidence: ROW_DATA_SOURCE_SCAN_EXEC node tag detected — "
            "this is authoritative metric-based classification, NOT a "
            "guess from the table name. Do NOT hedge with 'possibly via "
            "Lakehouse Federation'; state it as a confirmed fact."
        )
        parts.append("\n".join(fed_lines))

    if not parts:
        return ""

    header = "## ファクトパック概要" if lang == "ja" else "## Fact Pack Summary"
    return header + "\n```yaml\n" + "\n\n".join(parts) + "\n```"


# =============================================================================
# System / user prompt builders
# =============================================================================


def _serverless_constraints_block(lang: str) -> str:
    """Return serverless-specific constraints for the system prompt."""
    if lang == "ja":
        return """
### Serverless SQL Warehouse 制約（厳守）

このクエリはServerless SQL Warehouseで実行されています。
Databricks SQLでは以下の7つのパラメータのみSETが可能です（shuffle.partitionsを含め、それ以外は全て不可）:
- spark.sql.ansi.enabled (ANSI_MODE)
- spark.sql.legacy.timeParserPolicy (LEGACY_TIME_PARSER_POLICY)
- spark.sql.files.maxPartitionBytes (MAX_FILE_PARTITION_BYTES)
- spark.sql.session.timeZone (TIMEZONE)
- spark.databricks.execution.timeout (STATEMENT_TIMEOUT)
- spark.databricks.io.cache.enabled (USE_CACHED_RESULT)
- spark.databricks.sql.readOnlyExternalMetastore (READ_ONLY_EXTERNAL_METASTORE)

上記以外のSET spark.*設定を推奨してはなりません（例: shuffle.partitions, autoBroadcastJoinThreshold, preferSortMergeJoin, adaptive.*等は全て不可）。

**Serverless 固有の禁則:**
- `CACHE TABLE` / `UNCACHE TABLE` は Serverless SQL Warehouse では**使用不可**。推奨してはなりません
- 中間結果のキャッシュや再利用が必要な場合でも、`CACHE TABLE` を推奨せず、代わりにクエリ書き換え（下記）で再計算回数を減らしてください

代わりに、以下の**クエリ書き換え**による最適化を提案してください:
- CTEで事前集約し、JOIN前にデータ量を削減する
- CTEでWHEREフィルタを早期適用し、シャッフルデータを最小化する
- データスキューにはCTEで事前集約しJOIN前にデータ量を削減する（AQE設定の代わり）
- 相関サブクエリをJOINまたはEXISTSに書き換える
- 重複が許容される場合はUNIONの代わりにUNION ALLを使用する
- SELECT * を避け、必要なカラムのみを選択する
- BROADCAST/SHUFFLE_HASH/REPARTITIONヒントを補助手段として使用する
  **ヒントの配置ルール（厳守）:**
  - ヒントはJOINが存在する**同じクエリブロック**のSELECT句直後に `/*+ BROADCAST(alias) */` で記述する
  - CTE内のJOINにはそのCTEのSELECT句に、メインクエリのJOINにはメインのSELECT句に配置する
  - テーブルにエイリアスがある場合は**必ずエイリアス名を使う**（フルテーブル名ではなくエイリアス）
  - 例: `WITH cte AS (SELECT /*+ BROADCAST(d) */ ... FROM fact f JOIN dim d ON ...)` — CTEのSELECT直後
  **注意:** SHUFFLE_HASHヒントは全てのJOIN型でPhoton実行を保証するわけではない。
  LEFT OUTER JOINにSHUFFLE_HASHを適用してもDBRバージョンやキー形状によってはPhoton非対応となる場合がある。
  ヒント適用後はEXPLAIN EXTENDEDでPhoton実行を確認することを必ず推奨すること。小テーブルにはBROADCASTを優先。
- Photon非対応の関数を書き換える（Window RANGE→ROWS、UDF→組み込み関数）

具体的なbefore/afterのSQL例を、クエリプロファイルの実際のテーブル名・カラム名を使って提示してください。"""
    else:
        return """
### Serverless SQL Warehouse Constraints (MANDATORY)

This query runs on a Serverless SQL Warehouse.
Databricks SQL only supports these 7 parameters for SET (shuffle.partitions and all others are NOT available):
- spark.sql.ansi.enabled (ANSI_MODE)
- spark.sql.legacy.timeParserPolicy (LEGACY_TIME_PARSER_POLICY)
- spark.sql.files.maxPartitionBytes (MAX_FILE_PARTITION_BYTES)
- spark.sql.session.timeZone (TIMEZONE)
- spark.databricks.execution.timeout (STATEMENT_TIMEOUT)
- spark.databricks.io.cache.enabled (USE_CACHED_RESULT)
- spark.databricks.sql.readOnlyExternalMetastore (READ_ONLY_EXTERNAL_METASTORE)

Do NOT recommend any other SET spark.* configs (e.g., shuffle.partitions, autoBroadcastJoinThreshold, preferSortMergeJoin, adaptive.* are ALL NOT available).

**Serverless-specific prohibitions:**
- `CACHE TABLE` / `UNCACHE TABLE` are NOT available on Serverless SQL Warehouse — do NOT recommend them
- Even when intermediate caching or reuse seems warranted, do NOT recommend `CACHE TABLE`; instead reduce re-computation via query rewrite (below)

Instead, focus on **SQL QUERY REWRITES** for optimization:
- Pre-aggregate in CTEs before JOIN to reduce data volume
- Add WHERE filters early in CTEs to minimize shuffle
- Use CTE pre-aggregation to reduce data volume before JOIN for data skew (instead of AQE configs)
- Replace correlated subqueries with JOINs or EXISTS
- Use UNION ALL instead of UNION when duplicates are acceptable
- Select only required columns (avoid SELECT *)
- Use BROADCAST/SHUFFLE_HASH/REPARTITION hints as supplementary measures
  **Hint placement rules (MUST follow):**
  - Place hints in the SELECT clause of the **same query block** where the JOIN exists: `SELECT /*+ BROADCAST(alias) */ ...`
  - For JOINs inside a CTE, place the hint in that CTE's SELECT. For JOINs in the main query, place it in the main SELECT.
  - When a table has an alias, **always use the alias** (not the full table name).
  - Example: `WITH cte AS (SELECT /*+ BROADCAST(d) */ ... FROM fact f JOIN dim d ON ...)` — hint in the CTE's SELECT
  **Caveat:** SHUFFLE_HASH does NOT guarantee Photon execution for all join types.
  LEFT OUTER JOIN with SHUFFLE_HASH may fall back to non-Photon depending on DBR version and key shape.
  Always recommend verifying Photon execution via EXPLAIN EXTENDED after applying hints. Prefer BROADCAST for small tables.
- Rewrite Photon-incompatible functions (Window RANGE→ROWS, UDF→built-in)

Always provide concrete before/after SQL examples using actual table and column names from the query profile."""


def _streaming_constraints_block(lang: str) -> str:
    """Return streaming DLT/SDP-specific constraints for the system prompt."""
    if lang == "ja":
        return """
### ストリーミングクエリ制約（厳守）

このクエリはDLT/SDPのストリーミングクエリ（REFRESH STREAMING TABLE）です。
以下の特性を理解した上で分析してください：

- **実行中のスナップショット**: このプロファイルは実行中（RUNNING）のクエリのスナップショットです。最終的なメトリクスではありません。
- **マイクロバッチ実行**: `planMetadatas` に複数のマイクロバッチ履歴が含まれます。各バッチの実行時間・読み取り量のばらつきに注目してください。
- **累積メトリクス**: `query.metrics` は全バッチの累積値です。個別バッチの性能はバッチ統計を参照してください。
- **最適化の焦点**: バッチ間のばらつき（遅延バッチ）、バッチあたりの読み取り量、ターゲットテーブルへの書き込み効率に注目してください。
- **SET推奨の制限**: ストリーミングクエリではSETパラメータの変更は推奨しません。DLTパイプライン設定やテーブル定義の変更を提案してください。
- **累積値をKPIとして使用禁止**: `total_time_ms`（クエリ稼働時間）、`execution_time_ms`、`read_bytes`（全バッチ合計）を主要パフォーマンス指標として使わないでください。これらはストリーミングの「1回の処理時間」ではなく、起動からの累積値です。主要な判断はバッチ統計（平均/P95/CV/遅延バッチ数）に基づいてください。累積値を引用する場合は必ず「累積スナップショット」と明記してください。"""
    else:
        return """
### Streaming Query Constraints (MANDATORY)

This is a DLT/SDP streaming query (REFRESH STREAMING TABLE).
Analyze with the following characteristics in mind:

- **Running Snapshot**: This profile is a snapshot of a RUNNING query. Metrics are not final.
- **Micro-Batch Execution**: `planMetadatas` contains multiple micro-batch history entries. Focus on duration and read volume variance across batches.
- **Cumulative Metrics**: `query.metrics` are cumulative across all batches. Refer to batch statistics for per-batch performance.
- **Optimization Focus**: Pay attention to batch variance (slow batches), per-batch read volume, and write efficiency to the target table.
- **SET Recommendation Restriction**: Do NOT recommend SET parameter changes for streaming queries. Suggest DLT pipeline configuration or table definition changes instead.
- **Do NOT use cumulative values as KPIs**: `total_time_ms` (query uptime), `execution_time_ms`, `read_bytes` (sum across all batches) are NOT per-execution latency metrics. They are cumulative since query start. Base your primary analysis on batch statistics (avg/P95/CV/slow batch count). When referencing cumulative values, always label them as "cumulative snapshot"."""


def _federation_constraints_block(lang: str) -> str:
    """Return Lakehouse Federation-specific constraints (v5.18.0).

    When the query reads from an external source via Lakehouse
    Federation, most Databricks-side tunings do not apply. This block
    is injected into the system prompt so the LLM's recommendations
    stay on the federation-specific levers (pushdown, fetchSize,
    source-side pre-aggregation) rather than proposing LC / disk
    cache / Photon advice that the user cannot act on.
    """
    if lang == "ja":
        return """
### Lakehouse Federation クエリ制約（厳守）

**確定済み事実（authoritative）**: このクエリは **Lakehouse Federation** で外部エンジン (BigQuery / Snowflake / Postgres / MySQL / Redshift 等) を読んでいます。具体的な `source_type` と `tables` はファクトパックの `lakehouse_federation:` ブロックに記載されています — それらは ROW_DATA_SOURCE_SCAN_EXEC node tag に基づく確定情報であり、推測ではありません。「BigQuery 経由である **可能性が高い**」のような hedge は禁止 — 「BigQuery 経由」と断定してください。
以下の制約で推奨してください：

- **無効な推奨（提案禁止）**: Liquid Clustering, パーティション設計変更, ディスクキャッシュ拡大, Photon 適合性, OPTIMIZE / VACUUM, file pruning, SCAN_CLUSTERS 系の全て。外部データに対しては意味を持ちません。
- **有効な推奨の軸**:
  1. **Pushdown 状況の確認**: `EXPLAIN FORMATTED` → `EXTERNAL ENGINE QUERY` / `PushedFilters` / `PushedJoins` を確認。未 push の述語は Databricks 側で評価されるため全件取得に繋がる
  2. **述語書き換え**: 左辺関数化 (`DATE(ts) = '...'`), `CAST`, `UPPER`, `LIKE '%...%'` 等は pushdown 阻害。範囲条件や組み込み関数への置換を提案
  3. **JDBC コネクタのチューニング**: `WITH ('fetchSize' 100000)` / 並列読み (`numPartitions` + `partitionColumn` + `lowerBound` + `upperBound`、source 側で view 作成)
  4. **Snowflake**: `WITH ('partition_size_in_mb' 1000)`
  5. **BigQuery**: JOIN があれば materialization モード。partition 列への関数適用を避け、TIMESTAMP 範囲条件で pruning を通す
  6. **OLTP source (MySQL/Postgres)**: index にフィットする述語、read replica に接続、connection pool 上限
  7. **持続クエリの Delta 化**: 頻繁なダッシュボード / ジョブは `CREATE TABLE ... AS SELECT ...` で Delta に materialize、federation は ad-hoc のみ
  8. **LIMIT pushdown 抑止の認識**: UC のマスキング / 行レベルフィルタ適用下では LIMIT pushdown されない
- **アラート優先度**: federation クエリでは LC / 低 cache / low file pruning / stats fresh / Photon blocker 系は意味を持たないため自動抑制されます (v5.18.0)。Driver overhead / compilation overhead / shuffle / spill / hash resize の方を優先してください
- **ソース別アドバイス**: `federation_source_type` が判明している場合は source 固有 (BigQuery/Snowflake/Postgres 等) の具体 DDL/WITH 句を示す。不明なら「source を確認後」の一言を添えつつ generic 助言"""
    else:
        return """
### Lakehouse Federation Query Constraints (MANDATORY)

**Confirmed (authoritative)**: this query reads through **Lakehouse Federation** from an external engine (BigQuery / Snowflake / Postgres / MySQL / Redshift, …). The specific `source_type` and `tables` are reported in the Fact Pack `lakehouse_federation:` block — those values are confirmed from the ROW_DATA_SOURCE_SCAN_EXEC node tag, not deduced. Do NOT hedge with "possibly via Lakehouse Federation" or "the table name suggests BigQuery"; state the source as a fact.
Use the following constraints when recommending fixes:

- **Forbidden recommendations**: Liquid Clustering, partition redesign, disk-cache expansion, Photon compatibility, OPTIMIZE / VACUUM, file pruning, any SCAN_CLUSTERS advice. These targets do not exist for federated data.
- **Allowed recommendation axes**:
  1. **Verify pushdown status**: run `EXPLAIN FORMATTED` and inspect `EXTERNAL ENGINE QUERY` / `PushedFilters` / `PushedJoins`. Unpushed predicates are evaluated in Databricks AFTER a full fetch, which is the usual root cause of slowness
  2. **Rewrite predicates**: functions on the LHS (`DATE(ts) = '...'`, `CAST`, `UPPER`, `LIKE '%...%'`) defeat pushdown. Propose range conditions or built-in equivalents
  3. **JDBC connector tuning**: `WITH ('fetchSize' 100000)` and parallel reads (`numPartitions` + `partitionColumn` + `lowerBound` + `upperBound`; create views on the source side, not in Databricks)
  4. **Snowflake**: `WITH ('partition_size_in_mb' 1000)`
  5. **BigQuery**: for joins enable materialization mode. Avoid functions on the partition column; use TIMESTAMP range conditions to keep partition pruning active
  6. **OLTP sources (MySQL/Postgres)**: predicates aligned with indexes, point the connection at a read replica, watch connection pool limits
  7. **Materialize steady-state queries to Delta**: frequently-run dashboards / jobs should be copied to Delta via `CREATE TABLE ... AS SELECT ...`; keep federation for ad-hoc work only
  8. **LIMIT pushdown is disabled under UC governance** (column masks / row filters): users reporting "LIMIT 10 is slow" often hit this
- **Alert priority**: for federation queries, alerts about LC / low cache / low file pruning / stats freshness / Photon blockers are automatically suppressed (v5.18.0) because they do not apply. Prioritize driver overhead, compilation overhead, shuffle, spill, hash resize
- **Source-specific guidance**: when `federation_source_type` is known (BigQuery/Snowflake/Postgres/…), emit source-specific DDL / WITH hints. When unknown, caveat with "confirm source type first" but still give generic federation advice"""


@_append_korean_directive
def create_system_prompt(
    tuning_knowledge: str, lang: str | None = None, is_serverless: bool = False
) -> str:
    """Create system prompt with tuning knowledge."""
    if lang is None:
        lang = get_language()

    if lang == "ja":
        return f"""あなたはDatabricks SQLのパフォーマンスチューニングの専門家です。
クエリプロファイルのメトリクスを分析し、パフォーマンス改善のための具体的な推奨事項を提供してください。

以下はDatabricks SQLチューニングのベストプラクティスです：

{tuning_knowledge}

分析レポートは以下の形式で日本語で出力してください：

## サマリー
クエリの概要と主要なメトリクスの説明

## I/O分析
- ファイルプルーニング効率の評価
- キャッシュ効率の評価（閾値: >80%良好, 50-80%要改善, <30%危機的）
- Predictive I/Oの効果

## Shuffle分析
- Shuffle操作のメモリ効率（閾値: ≤128MB/パーティション）
- AQEの効果
- データスキューの有無

## 実行プラン分析
- 結合戦略の評価（Broadcast > ShuffleHash > SortMerge）
- Photon利用率（閾値: >80%良好, 50-80%要改善, <50%危機的）
- Photon非対応処理の特定

## ボトルネック分析
- ディスクスピル（閾値: >5GB危機的, >1GB重要）
- クラウドストレージリトライ
- その他の問題点

## 推奨アクション
各項目にはボトルネックの説明と具体的な対応策の両方を含めること。

{_recommendation_format_block("ja")}

## 最適化済みSQL
上記の分析結果に基づき、SQLの最適化案を提示してください。
- **変更箇所のみ**を抜粋して提示し、未変更部分は `-- ... (変更なし)` で省略すること（全文を出力しない）
- **ただし**: 「オプションA/B/C」のように複数候補を提示する場合、**各オプション内には最低 1 行の具体的な SQL ステートメント（ALTER / SET / SELECT 等の実行可能文）を必ず含めること**。オプションヘッダ + `-- ... (変更なし)` だけで中身が空の提示は禁止
- `fix` が SQL アクションを述べる場合、`fix_sql` は必須です。DDL/DML/SET/OPTIMIZE/ALTER/ANALYZE を含む SQL 系アクションでは、実行可能な**完全な 1 行コマンド**を省略せずに記載してください。warehouse サイズ変更など SQL を伴わない運用アクションだけを推奨する場合に限り、`fix_sql` は空文字で構いません
- Hierarchical Clustering の有効化・無効化・キー変更を含め、構成変更やメンテナンス系の推奨では、省略記法や疑似 SQL ではなく approved syntax を優先してください。例: `ALTER TABLE catalog.schema.table CLUSTER BY (col1, col2)` / `ALTER TABLE catalog.schema.table SET TBLPROPERTIES ('delta.liquid.hierarchicalClusteringColumns' = 'col1,col2')` / `OPTIMIZE catalog.schema.table FULL`
- 未確認の property 名、略称、推測ベースの SQL 構文は出力しないでください。特に TBLPROPERTIES 名は verify 済みの canonical 名のみを使用し、未確認の別名や legacy 名を作らないこと
- SQL 例を出す場合は approved syntax を優先してください。特に次の形式はそのまま使って構いません: `ALTER TABLE ... CLUSTER BY (...)` / `ALTER TABLE ... SET TBLPROPERTIES ('delta.liquid.hierarchicalClusteringColumns' = '...')` / `OPTIMIZE ... FULL`
- BROADCASTヒント、パーティショニングヒント等のクエリヒントを適切に追加
- 非効率なサブクエリがあればCTEへの書き換えを検討
- Photon非対応の関数や構文があれば代替手段を提示
- 変更箇所には `-- [OPTIMIZED]` コメントで理由を明記
- 改善が不要な場合は「最適化の余地なし」と明記してください

{_constraints_block("ja")}
{"" if not is_serverless else _serverless_constraints_block("ja")}
"""
    else:
        return f"""You are a Databricks SQL performance tuning expert.
Analyze the query profile metrics and provide specific recommendations for performance improvement.

Below are the Databricks SQL tuning best practices:

{tuning_knowledge}

Output the analysis report in the following format in English:

## Summary
Overview of the query and explanation of key metrics

## I/O Analysis
- File pruning efficiency evaluation
- Cache efficiency evaluation (thresholds: >80% good, 50-80% needs improvement, <30% critical)
- Predictive I/O effectiveness

## Shuffle Analysis
- Shuffle operation memory efficiency (threshold: ≤128MB/partition)
- AQE effectiveness
- Presence of data skew

## Execution Plan Analysis
- Join strategy evaluation (Broadcast > ShuffleHash > SortMerge)
- Photon utilization (thresholds: >80% good, 50-80% needs improvement, <50% critical)
- Identification of Photon-unsupported operations

## Bottleneck Analysis
- Disk spill (thresholds: >5GB critical, >1GB significant)
- Cloud storage retries
- Other issues

## Recommended Actions
Each item MUST include BOTH the bottleneck description AND the specific remediation.

{_recommendation_format_block("en")}

## Optimized SQL
Based on the analysis above, provide SQL optimization suggestions.
- Show **only the changed parts** of the SQL, abbreviating unchanged sections with `-- ... (no changes)`  (do NOT output the full SQL)
- **However**: when presenting multiple candidates (e.g., Option A/B/C), EACH option MUST contain at least one concrete, executable SQL statement (ALTER / SET / SELECT etc.). An option header followed by only `-- ... (no changes)` is FORBIDDEN
- If `fix` describes a SQL action, `fix_sql` is required. For any SQL-oriented action including DDL, DML, SET, OPTIMIZE, ALTER, or ANALYZE, provide a complete executable single-line command with no abbreviation. Only leave `fix_sql` empty when the recommendation is purely non-SQL, such as resizing a warehouse
- For Hierarchical Clustering and other configuration or maintenance recommendations, prefer approved syntax instead of abbreviated or pseudo-SQL examples. Examples: `ALTER TABLE catalog.schema.table CLUSTER BY (col1, col2)` / `ALTER TABLE catalog.schema.table SET TBLPROPERTIES ('delta.liquid.hierarchicalClusteringColumns' = 'col1,col2')` / `OPTIMIZE catalog.schema.table FULL`
- Do not output unverified property names, shorthand forms, or guessed SQL syntax. For TBLPROPERTIES, use only verified canonical property names and never invent unconfirmed aliases or legacy names
- When providing SQL examples, prefer approved syntax. In particular, the following forms are safe to use as written: `ALTER TABLE ... CLUSTER BY (...)` / `ALTER TABLE ... SET TBLPROPERTIES ('delta.liquid.hierarchicalClusteringColumns' = '...')` / `OPTIMIZE ... FULL`
- Add appropriate query hints (BROADCAST, REPARTITION, etc.) where beneficial
- Rewrite inefficient subqueries as CTEs where applicable
- Replace Photon-incompatible functions/syntax with supported alternatives
- Mark each change with a `-- [OPTIMIZED]` comment explaining the reason
- If no optimization is needed, explicitly state "No optimization needed"

{_constraints_block("en")}
{"" if not is_serverless else _serverless_constraints_block("en")}
"""


def create_analysis_prompt(analysis: ProfileAnalysis, lang: str | None = None) -> str:
    """Create analysis prompt with extracted metrics."""
    if lang is None:
        lang = get_language()

    qm = analysis.query_metrics
    bi = analysis.bottleneck_indicators

    file_pruning_ratio: float = 0.0
    total_files = qm.read_files_count + qm.pruned_files_count
    if total_files > 0:
        file_pruning_ratio = (qm.pruned_files_count / total_files) * 100

    data_skew_str = "Yes" if bi.has_data_skew else "No"
    if lang == "ja":
        data_skew_str = "あり" if bi.has_data_skew else "なし"

    if lang == "ja":
        prompt = f"""以下のクエリプロファイルメトリクスを分析してください：

## クエリ情報
- クエリID: {qm.query_id}
- ステータス: {qm.status}
- クエリ: {qm.query_text}

## 時間メトリクス
- 総実行時間: {format_time_ms(qm.total_time_ms)}
- コンパイル時間: {format_time_ms(qm.compilation_time_ms)}
- 実行時間: {format_time_ms(qm.execution_time_ms)}
- タスク累積時間: {format_time_ms(qm.task_total_time_ms)}

## I/Oメトリクス
- 読み取りバイト数: {format_bytes(qm.read_bytes)}
- リモート読み取り: {format_bytes(qm.read_remote_bytes)}
- キャッシュ読み取り: {format_bytes(qm.read_cache_bytes)}
- キャッシュヒット率: {bi.cache_hit_ratio:.1%} (評価: {bi.cache_severity.value})
- ディスクスピル: {format_bytes(qm.spill_to_disk_bytes)} (評価: {bi.spill_severity.value})

## ファイルメトリクス
- 読み取りファイル数: {qm.read_files_count:,}
- プルーニングファイル数: {qm.pruned_files_count:,}
- プルーニング率: {file_pruning_ratio:.1f}%
- プルーニングバイト数: {format_bytes(qm.pruned_bytes)}

## 行メトリクス
- 読み取り行数: {qm.rows_read_count:,}
- 出力行数: {qm.rows_produced_count:,}

## Photonメトリクス
- Photon処理時間: {format_time_ms(qm.photon_total_time_ms)}
- Photon利用率: {bi.photon_ratio:.1%} (評価: {bi.photon_severity.value})

## Shuffle分析
- シャッフル影響率: {bi.shuffle_impact_ratio:.1%} (評価: {bi.shuffle_severity.value})
- データスキュー検出: {data_skew_str}

## 追加パフォーマンス指標
- バイトプルーニング率: {bi.bytes_pruning_ratio:.1%} (評価: {bi.bytes_pruning_severity.value})
- リスケジュールスキャン率: {bi.rescheduled_scan_ratio:.1%} (ローカル: {bi.local_scan_tasks_total}, 非ローカル: {bi.non_local_scan_tasks_total})
- Predictive I/O - スキップ行数: {bi.data_filters_rows_skipped:,}
- Predictive I/O - スキップバッチ数: {bi.data_filters_batches_skipped:,}
- ハッシュテーブルリサイズ回数: {bi.hash_table_resize_count:,}
- 行あたりの平均ハッシュプローブ数: {bi.avg_hash_probes_per_row:.2f}
- AQE 自律 repartition: {"はい" if bi.aqe_self_repartition_seen else "いいえ"}{f" (×{bi.max_aqe_partition_growth_ratio:.0f} 拡大)" if bi.aqe_self_repartition_seen and bi.max_aqe_partition_growth_ratio > 0 else ""}
- シャッフル書き込み総量: {format_bytes(bi.shuffle_bytes_written_total)}
"""
    else:
        prompt = f"""Please analyze the following query profile metrics:

## Query Information
- Query ID: {qm.query_id}
- Status: {qm.status}
- Query: {qm.query_text}

## Time Metrics
- Total Execution Time: {format_time_ms(qm.total_time_ms)}
- Compilation Time: {format_time_ms(qm.compilation_time_ms)}
- Execution Time: {format_time_ms(qm.execution_time_ms)}
- Cumulative Task Time: {format_time_ms(qm.task_total_time_ms)}

## I/O Metrics
- Bytes Read: {format_bytes(qm.read_bytes)}
- Remote Read: {format_bytes(qm.read_remote_bytes)}
- Cache Read: {format_bytes(qm.read_cache_bytes)}
- Cache Hit Ratio: {bi.cache_hit_ratio:.1%} (Rating: {bi.cache_severity.value})
- Disk Spill: {format_bytes(qm.spill_to_disk_bytes)} (Rating: {bi.spill_severity.value})

## File Metrics
- Files Read: {qm.read_files_count:,}
- Files Pruned: {qm.pruned_files_count:,}
- Pruning Ratio: {file_pruning_ratio:.1f}%
- Pruned Bytes: {format_bytes(qm.pruned_bytes)}

## Row Metrics
- Rows Read: {qm.rows_read_count:,}
- Rows Produced: {qm.rows_produced_count:,}

## Photon Metrics
- Photon Processing Time: {format_time_ms(qm.photon_total_time_ms)}
- Photon Utilization: {bi.photon_ratio:.1%} (Rating: {bi.photon_severity.value})

## Shuffle Analysis
- Shuffle Impact Ratio: {bi.shuffle_impact_ratio:.1%} (Rating: {bi.shuffle_severity.value})
- Data Skew Detected: {data_skew_str}

## Additional Performance Indicators
- Bytes Pruning Ratio: {bi.bytes_pruning_ratio:.1%} (Rating: {bi.bytes_pruning_severity.value})
- Rescheduled Scan Ratio: {bi.rescheduled_scan_ratio:.1%} (Local: {bi.local_scan_tasks_total}, Non-local: {bi.non_local_scan_tasks_total})
- Predictive I/O - Rows Skipped: {bi.data_filters_rows_skipped:,}
- Predictive I/O - Batches Skipped: {bi.data_filters_batches_skipped:,}
- Hash Table Resize Count: {bi.hash_table_resize_count:,}
- Avg Hash Probes per Row: {bi.avg_hash_probes_per_row:.2f}
- AQE self-repartition: {"yes" if bi.aqe_self_repartition_seen else "no"}{f" (x{bi.max_aqe_partition_growth_ratio:.0f} growth)" if bi.aqe_self_repartition_seen and bi.max_aqe_partition_growth_ratio > 0 else ""}
- Total shuffle bytes written: {format_bytes(bi.shuffle_bytes_written_total)}

IMPORTANT GUIDANCE FOR LARGE SHUFFLE / AGGREGATION WORKLOADS:
If total shuffle bytes written > 10 GB OR aqe_self_repartition == yes, ALWAYS include a data-type appropriateness check in the recommendations. Common wastes at this scale:
- DECIMAL(38,0) keys where INTEGER/BIGINT would fit (2x-5x more expensive to hash/compare/store per row)
- STRING columns storing numeric or date values (hash collisions, larger row memory)
- Oversized VARCHAR where actual max length is small
If AQE self-repartition == yes AND no shuffle spilled, the root cause is data volume / physical layout (not key skew — AQE handles skew separately). Prioritize Liquid Clustering on the hot column over AQE skew-join settings.
"""

    # Add shuffle metrics
    if analysis.shuffle_metrics:
        if lang == "ja":
            prompt += "\n## Shuffle操作詳細\n"
            for sm in analysis.shuffle_metrics:
                efficiency_str = "(効率的)" if sm.is_memory_efficient else "(非効率 - 128MB超過)"
                prompt += f"""
### Shuffle (Node {sm.node_id})
- パーティション数: {sm.partition_count:,}
- ピークメモリ: {format_bytes(sm.peak_memory_bytes)}
- メモリ/パーティション: {sm.memory_per_partition_mb:.1f} MB {efficiency_str}
- 実行時間: {format_time_ms(sm.duration_ms)}
- 処理行数: {sm.rows_processed:,}
- 最適化優先度: {sm.optimization_priority.value}
"""
                if sm.shuffle_attributes:
                    prompt += f"- Shuffle属性: {', '.join(sm.shuffle_attributes)}\n"
                if sm.aqe_skewed_partitions > 0:
                    prompt += f"- AQEスキューパーティション数: {sm.aqe_skewed_partitions}\n"
        else:
            prompt += "\n## Shuffle Operation Details\n"
            for sm in analysis.shuffle_metrics:
                efficiency_str = (
                    "(Efficient)" if sm.is_memory_efficient else "(Inefficient - exceeds 128MB)"
                )
                prompt += f"""
### Shuffle (Node {sm.node_id})
- Partition Count: {sm.partition_count:,}
- Peak Memory: {format_bytes(sm.peak_memory_bytes)}
- Memory/Partition: {sm.memory_per_partition_mb:.1f} MB {efficiency_str}
- Duration: {format_time_ms(sm.duration_ms)}
- Rows Processed: {sm.rows_processed:,}
- Optimization Priority: {sm.optimization_priority.value}
"""
                if sm.shuffle_attributes:
                    prompt += f"- Shuffle Attributes: {', '.join(sm.shuffle_attributes)}\n"
                if sm.aqe_skewed_partitions > 0:
                    prompt += f"- AQE Skewed Partitions: {sm.aqe_skewed_partitions}\n"

    # Add join info
    if analysis.join_info:
        if lang == "ja":
            prompt += "\n## 結合タイプ\n"
            for ji in analysis.join_info:
                photon_status = "Photon対応" if ji.is_photon else "Photon非対応"
                prompt += f"- {ji.node_name}: {ji.join_type.display_name} ({photon_status})\n"
        else:
            prompt += "\n## Join Types\n"
            for ji in analysis.join_info:
                photon_status = "Photon-enabled" if ji.is_photon else "Photon-disabled"
                prompt += f"- {ji.node_name}: {ji.join_type.display_name} ({photon_status})\n"

    # Add node-level metrics
    scan_nodes = [nm for nm in analysis.node_metrics if "Scan" in nm.node_name]
    if scan_nodes:
        if lang == "ja":
            prompt += "\n## スキャンノードメトリクス\n"
            for nm in scan_nodes[:5]:
                prompt += f"""
### {nm.node_name}
- ファイル読み取り: {nm.files_read:,}
- ファイルプルーニング: {nm.files_pruned:,}
- プルーニングサイズ: {format_bytes(nm.files_pruned_size)}
- キャッシュヒット: {format_bytes(nm.cache_hits_size)}
- キャッシュミス: {format_bytes(nm.cache_misses_size)}
- クラウドストレージリクエスト数: {nm.cloud_storage_request_count:,}
- クラウドストレージリトライ数: {nm.cloud_storage_retry_count:,}
- データフィルタ - バッチスキップ: {nm.data_filters_batches_skipped:,}
- データフィルタ - 行スキップ: {nm.data_filters_rows_skipped:,}
"""
                if nm.is_delta:
                    prompt += "- Delta テーブル: はい\n"
                if nm.partition_filters:
                    prompt += f"- パーティションフィルタ: {', '.join(nm.partition_filters)}\n"
                if nm.filter_conditions:
                    prompt += f"- データフィルタ条件: {', '.join(nm.filter_conditions)}\n"
        else:
            prompt += "\n## Scan Node Metrics\n"
            for nm in scan_nodes[:5]:
                prompt += f"""
### {nm.node_name}
- Files Read: {nm.files_read:,}
- Files Pruned: {nm.files_pruned:,}
- Pruned Size: {format_bytes(nm.files_pruned_size)}
- Cache Hits: {format_bytes(nm.cache_hits_size)}
- Cache Misses: {format_bytes(nm.cache_misses_size)}
- Cloud Storage Requests: {nm.cloud_storage_request_count:,}
- Cloud Storage Retries: {nm.cloud_storage_retry_count:,}
- Data Filter - Batches Skipped: {nm.data_filters_batches_skipped:,}
- Data Filter - Rows Skipped: {nm.data_filters_rows_skipped:,}
"""
                if nm.is_delta:
                    prompt += "- Delta Table: yes\n"
                if nm.partition_filters:
                    prompt += f"- Partition Filters: {', '.join(nm.partition_filters)}\n"
                if nm.filter_conditions:
                    prompt += f"- Data Filter Conditions: {', '.join(nm.filter_conditions)}\n"

    # Add join node details (algorithm + type)
    join_nodes = [nm for nm in analysis.node_metrics if nm.join_type or nm.join_algorithm]
    if join_nodes:
        if lang == "ja":
            prompt += "\n## Join ノード詳細\n"
            for nm in join_nodes[:10]:
                parts = [f"### {nm.node_name}"]
                if nm.join_type:
                    parts.append(f"- Join 種別: {nm.join_type}")
                if nm.join_algorithm:
                    parts.append(f"- Join アルゴリズム: {nm.join_algorithm}")
                if nm.join_keys_left:
                    parts.append(f"- Left キー: {', '.join(nm.join_keys_left)}")
                if nm.join_keys_right:
                    parts.append(f"- Right キー: {', '.join(nm.join_keys_right)}")
                prompt += "\n".join(parts) + "\n"
        else:
            prompt += "\n## Join Node Details\n"
            for nm in join_nodes[:10]:
                parts = [f"### {nm.node_name}"]
                if nm.join_type:
                    parts.append(f"- Join Type: {nm.join_type}")
                if nm.join_algorithm:
                    parts.append(f"- Join Algorithm: {nm.join_algorithm}")
                if nm.join_keys_left:
                    parts.append(f"- Left Keys: {', '.join(nm.join_keys_left)}")
                if nm.join_keys_right:
                    parts.append(f"- Right Keys: {', '.join(nm.join_keys_right)}")
                prompt += "\n".join(parts) + "\n"

    # Add structured alerts
    if bi.alerts:
        if lang == "ja":
            prompt += "\n## 構造化アラート\n"
            for alert in bi.alerts:
                severity_label = {
                    "critical": "危機的",
                    "high": "高",
                    "medium": "警告",
                    "info": "情報",
                }.get(alert.severity.value, alert.severity.value)
                prompt += f"- **[{severity_label}]** [{alert.category}] {alert.message}\n"
                if alert.current_value and alert.threshold:
                    prompt += f"  - 現在値: {alert.current_value} / 閾値: {alert.threshold}\n"
                if alert.recommendation:
                    prompt += f"  - 推奨: {alert.recommendation}\n"
        else:
            prompt += "\n## Structured Alerts\n"
            for alert in bi.alerts:
                prompt += (
                    f"- **[{alert.severity.value.upper()}]** [{alert.category}] {alert.message}\n"
                )
                if alert.current_value and alert.threshold:
                    prompt += f"  - Current: {alert.current_value} / Threshold: {alert.threshold}\n"
                if alert.recommendation:
                    prompt += f"  - Action: {alert.recommendation}\n"

    # Legacy fallback
    if not bi.alerts and bi.critical_issues:
        if lang == "ja":
            prompt += "\n## 検出された重要な問題\n"
            for issue in bi.critical_issues:
                prompt += f"- **危機的**: {issue}\n"
        else:
            prompt += "\n## Detected Critical Issues\n"
            for issue in bi.critical_issues:
                prompt += f"- **Critical**: {issue}\n"

    if not bi.alerts and bi.warnings:
        if lang == "ja":
            prompt += "\n## 警告\n"
        else:
            prompt += "\n## Warnings\n"
        for warning in bi.warnings:
            prompt += f"- {warning}\n"

    if bi.recommendations:
        if lang == "ja":
            prompt += "\n## 事前分析による推奨事項\n"
        else:
            prompt += "\n## Pre-analysis Recommendations\n"
        for rec in bi.recommendations:
            prompt += f"- {rec}\n"

    # Add formatted SQL
    if analysis.sql_analysis and analysis.sql_analysis.formatted_sql:
        if lang == "ja":
            prompt += "\n## フォーマット済みSQL（完全版）\n"
        else:
            prompt += "\n## Formatted SQL (full version)\n"
        prompt += "```sql\n"
        prompt += analysis.sql_analysis.formatted_sql
        prompt += "\n```\n"

    # Add evidence
    if analysis.evidence_bundle and analysis.evidence_bundle.items:
        evidence_text = format_evidence_for_prompt(analysis.evidence_bundle, lang)
        if evidence_text:
            prompt += "\n" + evidence_text

    return prompt


# =============================================================================
# Review prompts
# =============================================================================


@_append_korean_directive
def create_review_system_prompt(
    tuning_knowledge: str,
    lang: str | None = None,
    is_serverless: bool = False,
    is_streaming: bool = False,
) -> str:
    """Create system prompt for reviewing analysis with JSON structured output."""
    if lang is None:
        lang = get_language()

    serverless_review_ja = """
5. **unsupported_config**: Serverless SQL Warehouseで使用できないSpark設定のSET文が含まれている
   - Databricks SQLでSET可能なパラメータは以下の7つのみです:
     spark.sql.ansi.enabled, spark.sql.legacy.timeParserPolicy, spark.sql.files.maxPartitionBytes,
     spark.sql.session.timeZone, spark.databricks.execution.timeout, spark.databricks.io.cache.enabled,
     spark.databricks.sql.readOnlyExternalMetastore
   - 上記以外のSET文（特にspark.sql.shuffle.partitions, spark.sql.autoBroadcastJoinThreshold,
     spark.sql.adaptive.*, spark.sql.join.preferSortMergeJoin等）は全てissueとして報告すること
   - fixにはクエリ書き換え（CTE事前集約、BROADCAST/SHUFFLE_HASH/REPARTITIONヒント等）を提案すること"""

    serverless_review_en = """
5. **unsupported_config**: SET statements for Spark configs not available on Serverless SQL Warehouse
   - Only these 7 parameters can be SET in Databricks SQL:
     spark.sql.ansi.enabled, spark.sql.legacy.timeParserPolicy, spark.sql.files.maxPartitionBytes,
     spark.sql.session.timeZone, spark.databricks.execution.timeout, spark.databricks.io.cache.enabled,
     spark.databricks.sql.readOnlyExternalMetastore
   - Any other SET statements (especially spark.sql.shuffle.partitions, spark.sql.autoBroadcastJoinThreshold,
     spark.sql.adaptive.*, spark.sql.join.preferSortMergeJoin) MUST be reported as issues
   - Fix should suggest query rewrites (CTE pre-aggregation, BROADCAST/SHUFFLE_HASH/REPARTITION hints, etc.)"""

    if lang == "ja":
        return f"""あなたはDatabricks SQLのパフォーマンスチューニングの専門家であり、ファクトチェッカーです。
他のAIアシスタントが作成したクエリプロファイル分析レポートをレビューし、正確性を検証してください。

以下はDatabricks SQLチューニングのベストプラクティスです：

{tuning_knowledge}

レビュー観点:
1. **hallucination**: ナレッジやメトリクスに根拠のない記述
2. **wrong_value**: Sparkパラメータの値がナレッジと異なる
3. **missing_evidence**: 推奨事項に根拠（アラートやメトリクス値）が欠如
4. **inconsistent_with_alert**: アラートの内容と矛盾する分析
5. **missed_signal**: Fact Packに重要なシグナルがあるのに本文で未言及
6. **wrong_priority**: 周辺要因（queue time, fetch time等）を主因扱いしている、または主因を軽視
7. **wrong_causality**: 因果関係の誤り（例: result fetch timeをscan遅延扱い、metadata timeをshuffle起因扱い）
8. **overgeneralization**: 局所的なシグナルを過度に一般化（例: 1ノードのspillを「クエリ全体の主要ボトルネック」と断定）
9. **conflict_ignored**: Fact Pack内の矛盾するシグナルを無視（例: result_from_cache=true なのにlatency改善を提案、IO改善なのにcache低下を問題視）
10. **unsupported_recommendation**: 推奨事項の前提条件がFact Pack/ナレッジに不足（例: テーブルサイズ不明でbroadcast join推奨、パーティション構造不明でbucketing推奨）
11. **sql_invalid_or_misapplied**: 推奨SQLがDatabricks SQL構文として不正、またはヒントの配置が誤っている（例: BROADCASTヒントがJOINのないSELECTに配置、CTE内のJOINなのに外側のSELECTにヒント配置、エイリアスでなくフルテーブル名をヒントに使用）
12. **redundant_recommendation**: Fact Packに既に記載されている設定・状態と重複する推奨（例: sql_context.sql_provided=trueなのに「SQLが未提示」と記述、既存クラスタリングキーが適切なのに再設定を推奨、統計がfullなのにANALYZE TABLE推奨）{serverless_review_ja if is_serverless else ""}

出力は**必ず以下のJSON形式のみ**で出力してください（前置きや説明テキスト不要）:

```json
{{
  "overall": "優秀|良好|普通|要改善|不十分",
  "issues": [
    {{
      "type": "hallucination|wrong_value|missing_evidence|inconsistent_with_alert|missed_signal|wrong_priority|wrong_causality|overgeneralization|conflict_ignored|unsupported_recommendation|sql_invalid_or_misapplied|redundant_recommendation",
      "location": "問題のあるセクション名や推奨事項番号",
      "claim": "レポート内の問題のある記述を引用",
      "problem": "何が問題かの説明",
      "fix": "修正方法の提案"
    }}
  ],
  "additions": ["見落とされている重要なポイント（あれば）"],
  "pass": true
}}
```

ルール:
- issuesが空の場合は "pass": true
- issuesが1つ以上ある場合は "pass": false
- 各issueの "claim" にはレポートからの直接引用を含めること
- JSON以外のテキストを出力しないこと
{"" if not is_streaming else _streaming_constraints_block("ja")}
"""
    else:
        return f"""You are a Databricks SQL performance tuning expert and fact-checker.
Review the query profile analysis report created by another AI assistant and verify its accuracy.

Below are the Databricks SQL tuning best practices:

{tuning_knowledge}

Review perspectives:
1. **hallucination**: Claims not supported by the knowledge base or metrics
2. **wrong_value**: Spark parameter values that differ from the knowledge base
3. **missing_evidence**: Recommendations lacking evidence (alerts or metric values)
4. **inconsistent_with_alert**: Analysis contradicting alert content
5. **missed_signal**: Important signal present in Fact Pack but not mentioned in analysis
6. **wrong_priority**: Peripheral factors (queue time, fetch time) treated as root cause, or root cause downplayed
7. **wrong_causality**: Causal attribution error (e.g., result fetch time attributed to scan delay, metadata time attributed to shuffle)
8. **overgeneralization**: Local signal over-generalized (e.g., spill in one node declared as "main bottleneck of the entire query")
9. **conflict_ignored**: Contradicting signals in the Fact Pack are ignored (e.g., proposing latency optimization when result_from_cache=true, flagging cache decline when IO improved)
10. **unsupported_recommendation**: Recommendation prerequisites missing from Fact Pack/knowledge (e.g., recommending broadcast join without knowing table size, recommending bucketing without partition structure info)
11. **sql_invalid_or_misapplied**: Recommended SQL is invalid Databricks SQL syntax or hints are misplaced (e.g., BROADCAST hint on a SELECT without JOIN, hint placed in outer SELECT when JOIN is inside a CTE, full table name used instead of alias in hint)
12. **redundant_recommendation**: Recommendation duplicates what is already present in the Fact Pack (e.g., saying "SQL not provided" when sql_context.sql_provided=true, recommending clustering key change when existing keys are already appropriate, recommending ANALYZE TABLE when statistics are full){serverless_review_en if is_serverless else ""}

Output **ONLY the following JSON format** (no preamble or explanation text):

```json
{{
  "overall": "Excellent|Good|Average|Needs Improvement|Insufficient",
  "issues": [
    {{
      "type": "hallucination|wrong_value|missing_evidence|inconsistent_with_alert|missed_signal|wrong_priority|wrong_causality|overgeneralization|conflict_ignored|unsupported_recommendation|sql_invalid_or_misapplied|redundant_recommendation",
      "location": "Section name or recommendation number with the issue",
      "claim": "Quote the problematic statement from the report",
      "problem": "Explanation of what is wrong",
      "fix": "Suggested correction"
    }}
  ],
  "additions": ["Important points missed in the analysis (if any)"],
  "pass": true
}}
```

Rules:
- If issues is empty, set "pass": true
- If issues has 1 or more entries, set "pass": false
- Each issue's "claim" must include a direct quote from the report
- Do NOT output any text outside the JSON
{"" if not is_streaming else _streaming_constraints_block("en")}
"""


def _build_review_fact_pack(analysis: ProfileAnalysis, lang: str) -> str:
    """Build a comprehensive fact pack for the reviewer.

    Includes all metrics, alerts, and evidence so the reviewer can
    cross-check every claim in the analysis against the ground truth.
    """
    qm = analysis.query_metrics
    bi = analysis.bottleneck_indicators

    file_pruning_ratio: float = 0.0
    total_files = qm.read_files_count + qm.pruned_files_count
    if total_files > 0:
        file_pruning_ratio = (qm.pruned_files_count / total_files) * 100

    sections = []

    # Fact Pack Summary for reviewer focus
    summary_block = _build_fact_pack_summary(analysis, lang)
    if summary_block:
        sections.append(summary_block)

    # Target Table Configuration — INSERT/CTAS/MERGE write target's
    # provider, clustering columns, hierarchical clustering, and key
    # delta.* properties. Without this block, Stage 2/3 cannot confirm
    # whether the observed write-side shuffle is ClusterOnWrite overhead.
    tt_lines = _format_target_table_config(analysis)
    if tt_lines:
        header = "### ターゲットテーブル設定" if lang == "ja" else "### Target Table Configuration"
        sections.append(header + "\n" + "\n".join(tt_lines))

    # Table Scan Info — clustering keys (from JSON SCAN_CLUSTERS) + column
    # types (from EXPLAIN ReadSchema) + table format (delta/parquet).
    # Without this block, Stage 2 (review) and Stage 3 (refine) reject the
    # Stage 1 recommendations that cited types or clustering keys because
    # they "cannot verify from Fact Pack" — the whole refinement pipeline
    # then rewrites them into generic "check schema first" guidance.
    scan_lines = _format_table_scan_info(analysis)
    if scan_lines:
        header = "### テーブルスキャン情報" if lang == "ja" else "### Table Scan Info"
        sections.append(header + "\n" + "\n".join(scan_lines))

    # Shuffle Details (per-node) — surface partitioning keys to Stage 2/3
    # so the reviewer can confirm the shuffle-bottleneck key by name
    # without falling back to "cannot determine from metrics".
    shuf_lines = _format_shuffle_details(analysis)
    if shuf_lines:
        header = "### シャッフル詳細" if lang == "ja" else "### Shuffle Details"
        sections.append(header + "\n" + "\n".join(shuf_lines))

    # EXPLAIN v2 Insights — CTE references, join build side, filter
    # pushdown detail, non-JOIN CAST, aggregate phase split, Photon
    # fallback operator names, AQE state, exchange partitioning detail,
    # Photon reference nodes. All of these were Stage-1 exclusive and
    # therefore erased by Stage 3 before the user saw the output.
    if analysis.explain_analysis:
        v2_lines = _format_explain_v2_insights(analysis.explain_analysis)
        if v2_lines:
            header = "### EXPLAIN v2 詳細" if lang == "ja" else "### EXPLAIN v2 Insights"
            sections.append(header + "\n" + "\n".join(v2_lines))

    # Basic metrics
    if lang == "ja":
        sections.append(f"""### 基本メトリクス
- クエリID: {qm.query_id}
- 総実行時間: {format_time_ms(qm.total_time_ms)}
- コンパイル時間: {format_time_ms(qm.compilation_time_ms)}
- 実行時間: {format_time_ms(qm.execution_time_ms)}
- 読み取りデータ: {format_bytes(qm.read_bytes)}
- リモート読み取り: {format_bytes(qm.read_remote_bytes)}
- キャッシュ読み取り: {format_bytes(qm.read_cache_bytes)}
- キャッシュヒット率: {bi.cache_hit_ratio:.1%} ({bi.cache_severity.value})
- Photon利用率: {bi.photon_ratio:.1%} ({bi.photon_severity.value})
- ディスクスピル: {format_bytes(bi.spill_bytes)} ({bi.spill_severity.value})
- シャッフル影響率: {bi.shuffle_impact_ratio:.1%} ({bi.shuffle_severity.value})
- ファイルプルーニング率: {file_pruning_ratio:.1f}%
- バイトプルーニング率: {bi.bytes_pruning_ratio:.1%} ({bi.bytes_pruning_severity.value})""")
    else:
        sections.append(f"""### Core Metrics
- Query ID: {qm.query_id}
- Total Execution Time: {format_time_ms(qm.total_time_ms)}
- Compilation Time: {format_time_ms(qm.compilation_time_ms)}
- Execution Time: {format_time_ms(qm.execution_time_ms)}
- Data Read: {format_bytes(qm.read_bytes)}
- Remote Read: {format_bytes(qm.read_remote_bytes)}
- Cache Read: {format_bytes(qm.read_cache_bytes)}
- Cache Hit Ratio: {bi.cache_hit_ratio:.1%} ({bi.cache_severity.value})
- Photon Utilization: {bi.photon_ratio:.1%} ({bi.photon_severity.value})
- Disk Spill: {format_bytes(bi.spill_bytes)} ({bi.spill_severity.value})
- Shuffle Impact Ratio: {bi.shuffle_impact_ratio:.1%} ({bi.shuffle_severity.value})
- File Pruning Ratio: {file_pruning_ratio:.1f}%
- Bytes Pruning Ratio: {bi.bytes_pruning_ratio:.1%} ({bi.bytes_pruning_severity.value})""")

    # Queue & Result Delivery (ground truth for reviewer)
    total_queue = qm.queued_provisioning_time_ms + qm.queued_overload_time_ms
    if total_queue > 0 or qm.result_from_cache or qm.result_fetch_time_ms > 0:
        if lang == "ja":
            dl = ["### キュー & 結果配信"]
        else:
            dl = ["### Queue & Result Delivery"]
        if qm.result_from_cache:
            dl.append("- Result Cache Hit: true (execution skipped)")
        if total_queue > 0:
            dl.append(
                f"- Queue: {format_time_ms(total_queue)} "
                f"(provisioning={format_time_ms(qm.queued_provisioning_time_ms)}, "
                f"overload={format_time_ms(qm.queued_overload_time_ms)})"
            )
        if qm.result_fetch_time_ms > 0:
            dl.append(f"- Result Fetch: {format_time_ms(qm.result_fetch_time_ms)}")
        sections.append("\n".join(dl))

    # Compilation details
    if qm.compilation_time_ms > 5000 or qm.metadata_time_ms > 0:
        cl = ["### Compilation Details"]
        cl.append(f"- Compilation: {format_time_ms(qm.compilation_time_ms)}")
        if qm.metadata_time_ms > 0:
            cl.append(f"- Metadata Resolution: {format_time_ms(qm.metadata_time_ms)}")
        if qm.planning_phases:
            for p in sorted(
                qm.planning_phases, key=lambda x: x.get("duration_ms", 0), reverse=True
            )[:3]:
                cl.append(
                    f"- Phase {p.get('phase', '?')}: {format_time_ms(p.get('duration_ms', 0))}"
                )
        sections.append("\n".join(cl))

    # Shuffle I/O (ground truth)
    if bi.shuffle_bytes_written_total > 0 or bi.shuffle_remote_bytes_read_total > 0:
        sl = ["### Shuffle I/O"]
        sl.append(f"- Written: {format_bytes(bi.shuffle_bytes_written_total)}")
        sl.append(f"- Remote Read: {format_bytes(bi.shuffle_remote_bytes_read_total)}")
        sl.append(f"- Local Read: {format_bytes(bi.shuffle_local_bytes_read_total)}")
        sections.append("\n".join(sl))

    # Write I/O (ground truth)
    if qm.write_remote_bytes > 0:
        wl = ["### Write I/O"]
        wl.append(f"- Bytes: {format_bytes(qm.write_remote_bytes)}")
        wl.append(f"- Files: {qm.write_remote_files:,}")
        wl.append(f"- Rows: {qm.write_remote_rows:,}")
        if qm.write_remote_files > 0:
            wl.append(
                f"- Avg File Size: {format_bytes(int(qm.write_remote_bytes / qm.write_remote_files))}"
            )
        sections.append("\n".join(wl))

    # Structured alerts (ground truth for reviewer)
    if bi.alerts:
        if lang == "ja":
            alert_lines = ["### 構造化アラート（検証基準）"]
        else:
            alert_lines = ["### Structured Alerts (verification baseline)"]
        for alert in bi.alerts:
            alert_lines.append(
                f"- [{alert.severity.value.upper()}] [{alert.category}] {alert.message}"
            )
            if alert.current_value and alert.threshold:
                alert_lines.append(f"  current={alert.current_value}, threshold={alert.threshold}")
            if alert.recommendation:
                alert_lines.append(f"  action: {alert.recommendation}")
        sections.append("\n".join(alert_lines))

    # Join info
    if analysis.join_info:
        join_lines = ["### Join Types"]
        for ji in analysis.join_info:
            photon_str = "Photon" if ji.is_photon else "non-Photon"
            join_lines.append(f"- {ji.node_name}: {ji.join_type.display_name} ({photon_str})")
        sections.append("\n".join(join_lines))

    # Shuffle metrics
    if analysis.shuffle_metrics:
        shuffle_lines = ["### Shuffle Operations"]
        for sm in analysis.shuffle_metrics:
            shuffle_lines.append(
                f"- Node {sm.node_id}: {sm.partition_count:,} partitions, "
                f"{sm.memory_per_partition_mb:.1f} MB/partition, "
                f"priority={sm.optimization_priority.value}"
            )
        sections.append("\n".join(shuffle_lines))

    # Evidence
    if analysis.evidence_bundle and analysis.evidence_bundle.items:
        evidence_text = format_evidence_for_prompt(analysis.evidence_bundle, lang)
        if evidence_text:
            sections.append(evidence_text)

    return "\n\n".join(sections)


def create_review_prompt(
    analysis: ProfileAnalysis, llm_analysis: str, primary_model: str, lang: str | None = None
) -> str:
    """Create prompt for reviewing the analysis.

    Includes a comprehensive Fact Pack so the reviewer can cross-check
    every claim against the original metrics, alerts, and evidence.
    """
    if lang is None:
        lang = get_language()

    fact_pack = _build_review_fact_pack(analysis, lang)

    if lang == "ja":
        return f"""以下は{primary_model}が作成したクエリプロファイル分析レポートです。
このレポートの妥当性を、元のメトリクスとアラートに照らし合わせてレビューしてください。

## 元のメトリクス・アラート情報（検証基準）

{fact_pack}

---

## {primary_model}による分析レポート

{llm_analysis}

---

上記の分析レポートを、元のメトリクスとアラートに照らし合わせてレビューしてください。
特に以下を確認してください:
- レポート内の数値が元のメトリクスと一致しているか
- 推奨事項がアラートの内容と整合しているか
- ナレッジに記載のない設定値やパラメータを断定していないか
"""
    else:
        return f"""Below is the query profile analysis report created by {primary_model}.
Please review its validity against the original metrics and alerts.

## Original Metrics & Alerts (verification baseline)

{fact_pack}

---

## Analysis Report by {primary_model}

{llm_analysis}

---

Review the above analysis report against the original metrics and alerts.
Specifically check:
- Whether numbers cited in the report match the original metrics
- Whether recommendations are consistent with the alert content
- Whether any Spark parameters or thresholds are asserted without knowledge base support
"""


# =============================================================================
# Refine prompts
# =============================================================================


@_append_korean_directive
def create_refine_system_prompt(
    tuning_knowledge: str,
    lang: str | None = None,
    is_serverless: bool = False,
    is_streaming: bool = False,
) -> str:
    """Create system prompt for refinement based on JSON review issues."""
    if lang is None:
        lang = get_language()

    serverless_refine_ja = """
6. **unsupported_config**タイプのissueがある場合: Serverless SQL Warehouseで使用できないSET文を削除し、
   代わりにクエリ書き換え（CTE事前集約、BROADCAST/SHUFFLE_HASH/REPARTITIONヒント等）に置き換える。
   Databricks SQLでSET可能なのは以下の7パラメータのみ:
   spark.sql.ansi.enabled, spark.sql.legacy.timeParserPolicy, spark.sql.files.maxPartitionBytes,
   spark.sql.session.timeZone, spark.databricks.execution.timeout, spark.databricks.io.cache.enabled,
   spark.databricks.sql.readOnlyExternalMetastore"""

    serverless_refine_en = """
6. For **unsupported_config** issues: Remove SET statements not available on Serverless SQL Warehouse
   and replace with query rewrites (CTE pre-aggregation, BROADCAST/SHUFFLE_HASH/REPARTITION hints, etc.).
   Only these 7 parameters can be SET in Databricks SQL:
   spark.sql.ansi.enabled, spark.sql.legacy.timeParserPolicy, spark.sql.files.maxPartitionBytes,
   spark.sql.session.timeZone, spark.databricks.execution.timeout, spark.databricks.io.cache.enabled,
   spark.databricks.sql.readOnlyExternalMetastore"""

    if lang == "ja":
        return f"""あなたはDatabricks SQLのパフォーマンスチューニングの専門家です。
初期の分析レポートと、レビュー結果（JSON形式のissueリストまたはMarkdown）を受け取り、
指摘箇所のみを修正した最終版の分析レポートを作成してください。

以下はDatabricks SQLチューニングのベストプラクティスです：

{tuning_knowledge}

最終レポートの作成方針：
1. 初期分析の構成と内容をベースにする
2. **指摘された箇所だけを修正し、それ以外は変えない**
3. issueのtype別の修正方法:
   - hallucination: 根拠のない記述を削除するか、正確な情報に置き換える
   - wrong_value: ナレッジから正しいパラメータ値を引用して修正する
   - missing_evidence: 対応するアラートやメトリクス値の引用を追加する
   - inconsistent_with_alert: アラートの内容と整合する分析に修正する{serverless_refine_ja if is_serverless else ""}
4. additionsに記載された見落としポイントがあれば適切なセクションに追記する
5. 「最適化済みSQL」セクションが初期分析に含まれている場合は必ず最終版にも含める

出力は初期分析と同じ形式（日本語のMarkdown）で出力してください。
レビューへの言及は不要です。最終的な分析結果のみを出力してください。
ただし最適化SQLは変更箇所のみ抜粋し、未変更部分は `-- ... (変更なし)` で省略すること。
**ただし、オプションA/B/Cのように複数候補を提示する場合は、各オプション内に最低 1 行の具体的な SQL ステートメント（ALTER / SET / SELECT 等）を必ず含めること。オプションヘッダと `-- ... (変更なし)` だけの空の提示は禁止。DDL例（CLUSTER BY / SET TBLPROPERTIES 等）は完全な 1 行を省略せず記載。**
{"" if not is_streaming else _streaming_constraints_block("ja")}
"""
    else:
        return f"""You are a Databricks SQL performance tuning expert.
Receive the initial analysis report and review result (JSON issue list or Markdown),
and create an improved final version fixing ONLY the identified issues.

Below are the Databricks SQL tuning best practices:

{tuning_knowledge}

Guidelines for creating the final report:
1. Use the structure and content of the initial analysis as a base
2. **Fix ONLY the identified issues — do not change anything else**
3. Fix method by issue type:
   - hallucination: Remove unsupported claims or replace with accurate information
   - wrong_value: Quote the correct parameter value from the knowledge base
   - missing_evidence: Add citation of the corresponding alert or metric value
   - inconsistent_with_alert: Correct analysis to be consistent with alert content{serverless_refine_en if is_serverless else ""}
4. If additions list mentions overlooked points, add them to appropriate sections
5. If the initial analysis includes an "Optimized SQL" section, it MUST be preserved

Output the final version in the same format as the initial analysis (Markdown in English).
Do not reference the review. Output only the final analysis results.
Show only the changed parts of the SQL, abbreviating unchanged sections with `-- ... (no changes)`.
**However, when presenting multiple candidates (Option A/B/C), each option MUST contain at least one concrete SQL statement (ALTER / SET / SELECT etc.). Option headers with only `-- ... (no changes)` are FORBIDDEN. DDL examples (CLUSTER BY / SET TBLPROPERTIES) must be shown as complete one-line commands without abbreviation.**
{"" if not is_streaming else _streaming_constraints_block("en")}
"""


def create_refine_prompt(
    initial_analysis: str,
    review_comments: str,
    primary_model: str,
    review_model: str,
    lang: str | None = None,
    analysis: "ProfileAnalysis | None" = None,
) -> str:
    """Create prompt for refinement.

    If analysis is provided, includes a Fact Pack so the refiner can
    verify corrections against original metrics.
    """
    if lang is None:
        lang = get_language()

    formatted_review = format_review_for_refine(review_comments)

    # Build evidence section if analysis is available
    evidence_section = ""
    if analysis is not None:
        fact_pack = _build_review_fact_pack(analysis, lang)
        if lang == "ja":
            evidence_section = f"""
---

## 元のメトリクス・アラート（修正時の参照用）

{fact_pack}
"""
        else:
            evidence_section = f"""
---

## Original Metrics & Alerts (reference for corrections)

{fact_pack}
"""

    if lang == "ja":
        return f"""以下の初期分析とレビュー結果を元に、指摘箇所のみを修正した最終版の分析レポートを作成してください。

## 初期分析（{primary_model}による）

{initial_analysis}

---

## レビュー結果（{review_model}による）

{formatted_review}
{evidence_section}
---

上記のレビュー指摘箇所のみを修正し、それ以外は変更しないでください。
修正後、レポート末尾に以下の形式で変更サマリを追記してください:
<!-- CHANGES: 変更点1; 変更点2; ... -->
"""
    else:
        return f"""Based on the following initial analysis and review result, create an improved final version fixing ONLY the identified issues.

## Initial Analysis (by {primary_model})

{initial_analysis}

---

## Review Result (by {review_model})

{formatted_review}
{evidence_section}
---

Fix ONLY the issues identified in the review. Do not change anything else.
After the report, append a change summary in this format:
<!-- CHANGES: change1; change2; ... -->
"""


# =============================================================================
# Report review / refine prompts
# =============================================================================


def create_report_review_system_prompt(tuning_knowledge: str, lang: str | None = None) -> str:
    """Create system prompt for report review."""
    if lang is None:
        lang = get_language()

    if lang == "ja":
        return f"""あなたはDatabricks SQLのパフォーマンスチューニングの専門家であり、技術レポートの査読者です。
与えられたMarkdownレポートをレビューし、以下の観点でチェックしてください：
1. 事実誤認や不正確な記述
2. 数値/閾値の不整合
3. 根拠不足の推奨事項
4. 見落としている重要な観点
5. 改善可能な推奨の具体性不足

以下はDatabricks SQLチューニングのベストプラクティスです：

{tuning_knowledge}

出力はMarkdownで、必ず次の構成にしてください：

### 妥当性チェック
- レポート内容の正確性を評価

### 不足/不明点
- 追加で確認すべき情報や不足しているデータ

### 改善提案
- レポートの改善案や追加すべき分析

### 総合評価
- Overall: Good / Warning / Critical のいずれか1つ

注意:
- レポート本文を書き換えず「追記レビュー」として出力する
- レポート内に存在しない情報を断定しない（推測は推測と明記）
- 簡潔に要点をまとめる
"""
    else:
        return f"""You are a Databricks SQL performance tuning expert and a technical report reviewer.
Review the provided Markdown report and check the following aspects:
1. Factual errors or inaccuracies
2. Metric/threshold inconsistencies
3. Recommendations lacking evidence
4. Missing important perspectives
5. Lack of actionable specificity in recommendations

Below are the Databricks SQL tuning best practices:

{tuning_knowledge}

Output Markdown with exactly this structure:

### Validity Checks
- Evaluate the accuracy of report content

### Missing / Unclear Items
- Information that needs additional confirmation or missing data

### Improvement Suggestions
- Proposals for report improvements or additional analysis

### Overall Assessment
- Overall: Good / Warning / Critical (choose exactly one)

Notes:
- Do not rewrite the report; output an append-only review block
- Do not assert facts not present in the report (label speculation explicitly)
- Be concise and focus on key points
"""


def create_report_review_prompt(
    report_markdown: str,
    report_context: dict | None = None,
    lang: str | None = None,
) -> str:
    """Create prompt for report review."""
    if lang is None:
        lang = get_language()

    context_str = ""
    if report_context:
        context_items = []
        if report_context.get("query_id"):
            context_items.append(f"Query ID: {report_context['query_id']}")
        if report_context.get("primary_model"):
            context_items.append(f"Primary Model: {report_context['primary_model']}")
        if context_items:
            context_str = "\n".join(context_items)

    if lang == "ja":
        prompt = "以下のMarkdownはツールが生成した「クエリパフォーマンスレポート」です。\n"
        prompt += "このレポート全体を査読し、レビューを作成してください。\n\n"
        if context_str:
            prompt += f"コンテキスト情報:\n{context_str}\n\n"
        prompt += f"--- レポート本文 ---\n{report_markdown}\n--- ここまで ---"
    else:
        prompt = "The following Markdown is a 'Query Performance Report' generated by the tool.\n"
        prompt += "Please review the entire report and create a review.\n\n"
        if context_str:
            prompt += f"Context:\n{context_str}\n\n"
        prompt += f"--- Report Content ---\n{report_markdown}\n--- End of Report ---"

    return prompt


def create_report_refine_system_prompt(tuning_knowledge: str, lang: str | None = None) -> str:
    """Create system prompt for report refinement."""
    if lang is None:
        lang = get_language()

    if lang == "ja":
        return f"""あなたはDatabricks SQLのパフォーマンスチューニング専門家であり、技術レポートの編集者です。
入力として「既存のMarkdownレポート」と「そのレポートに対するレビューコメント（Markdown）」が与えられます。

あなたのタスク:
- レビュー指摘を反映して、レポート本文を改善した"最終版レポート全文（Markdown）"を生成すること。

以下はDatabricks SQLチューニングのベストプラクティスです：

{tuning_knowledge}

制約:
- 出力はレポート全文のみ（前置き、解説、箇条書きのメタコメント、JSONは禁止）
- 元レポートに無い事実を断定して追加しない。推測が必要なら「推測」と明記し、根拠を示す
- 数値/閾値/設定値は、元レポートまたは入力データに明示がある場合のみ断定する
- 文章の重複や冗長さを削り、見出し構造を整え、推奨事項は"実行可能な手順"として具体化する
- レビューのうち、誤り/根拠薄弱/適用不能な指摘は採用せず、その理由をレポート内の該当箇所に短く反映する

優先順位:
1) 正確性
2) 明確性（根拠→結論→アクション）
3) 実行可能性（誰が何をどう直すか）
"""
    else:
        return f"""You are a Databricks SQL performance tuning expert and a technical report editor.
You will receive (1) an existing Markdown report and (2) review comments in Markdown.

Task:
- Produce an improved FINAL version of the full report (Markdown), incorporating valid review feedback.

Below are the Databricks SQL tuning best practices:

{tuning_knowledge}

Constraints:
- Output ONLY the full report content in Markdown (no preface, no meta commentary, no JSON)
- Do not invent facts not present in the report/review context. If you must speculate, label it explicitly
- Do not assert numeric thresholds/config values unless explicitly present in the inputs
- Remove redundancy, improve structure/headings, and make recommendations actionable with concrete steps
- If a review comment is incorrect or not applicable, do NOT apply it; instead reflect the nuance briefly

Priorities:
1) Correctness
2) Clarity (evidence -> conclusion -> actions)
3) Actionability
"""


def create_report_refine_prompt(
    report_markdown: str,
    report_review_markdown: str,
    lang: str | None = None,
) -> str:
    """Create prompt for report refinement."""
    if lang is None:
        lang = get_language()

    if lang == "ja":
        return f"""以下に「元のレポート」と「レビュー」を示します。
レビューを踏まえて、改善された最終版レポート全文（Markdown）を出力してください。

改善のポイント:
- 不正確な表現の修正
- 不足観点の補強（ただし断定追加はしない）
- 推奨事項の具体化（手順・確認方法・リスク）
- 見出し/表/箇条書きの整理

--- 元のレポート ---
{report_markdown}
--- ここまで ---

--- レビュー ---
{report_review_markdown}
--- ここまで ---
"""
    else:
        return f"""Below are the original report and the review comments.
Please output the improved final report in Markdown (full content).

Improvement points:
- Fix inaccurate expressions
- Reinforce missing perspectives (without asserting unconfirmed facts)
- Make recommendations actionable (steps, verification methods, risks)
- Organize headings/tables/lists

--- Original Report ---
{report_markdown}
--- End ---

--- Review Comments ---
{report_review_markdown}
--- End ---
"""


# =============================================================================
# Structured report prompts
# =============================================================================


@_append_korean_directive
def create_structured_system_prompt(
    tuning_knowledge: str,
    lang: str | None = None,
    is_serverless: bool = False,
    is_streaming: bool = False,
    is_federation: bool = False,
) -> str:
    """Create system prompt for PDF-style structured report generation."""
    if lang is None:
        lang = get_language()

    if lang == "ja":
        return f"""あなたはDatabricks SQLのパフォーマンスチューニングの専門家です。
クエリプロファイルの分析メトリクスとアラートを元に、構造化されたレポートの特定セクションを作成してください。

以下はDatabricks SQLチューニングのベストプラクティスです：

{tuning_knowledge}

以下の4セクションを出力してください。各セクションは必ず## ヘッダーで始めてください。

## 1. エグゼクティブサマリー
マネージャーが3秒で状況を把握できる要約を、自然な文章で作成してください。
「ヘッドライン:」「事実:」などのラベルは付けず、以下の4要素を自然な段落として書いてください：

1段落目: このクエリの性能状態を太字の一文で判定（例: **重大な性能問題があり優先対応が必要です。**）
2段落目: ステータス、実行時間、読み取り量、アラート件数を簡潔に1-2文で
3段落目: 根本原因の推定、改善方向性、および十分な根拠がある場合は目標実行時間の範囲（例:「~10分から2-4分に短縮可能」）を2-3文で。高確度ボトルネックが総時間の大部分を占める場合のみ目標時間を記載し、それ以外は「目標時間は適用する修正に依存 — 実装後に計測」と記載すること
4段落目: 次に見るべきセクションを一文で（例: Action PlanのP0から着手してください。）

アラートの個別列挙は不要です（サマリー末尾の主要アラート / 付録の全アラート一覧に委譲）。

## 4. 根本原因分析
### 4.1 直接原因
直接的な症状（OOM、タイムアウト、低性能など）を記述
### 4.2 根本原因
因果チェーンを [原因] → [中間影響] → [最終症状] の形式で記述。
提供されたアラートとメトリクスを必ず引用すること。
### 4.3 Disk Spill分析（スピルが検出された場合のみ）
スピルの規模、影響を受けるオペレータ、対策を記述

## 7. 推奨アクション
各項目には必ず「ボトルネックの説明」と「具体的な対応策」の両方を含めること。
Fact Packに「Detected Bottlenecks」が含まれている場合、それをベースに詳細な分析と具体的な修正案を付加すること。ボトルネックの説明と対応策を別々のサブセクションに分けないこと。

{_recommendation_format_block("ja")}

## 8. 結論
根本原因の再確認と最優先アクションの要約（3文以内）

### シグナルベースの重要度判定
Fact Packに「Detected Signals」セクションが含まれます。
各シグナルの実際の重要度は以下を考慮して**あなたが判断**してください：
- シグナルのコンテキスト比率（例: spill_ratio_of_read）を重視し、絶対値だけで判断しない
- クエリの規模（短時間 vs 長時間、小データ vs 大データ）に対する影響度
- 複数シグナルの相関（例: spill + 高shuffle → 単独より深刻）
- 絶対的な影響が小さいシグナルをCRITICALと判定しない
例: spill_detected で spill_ratio_of_read=0.005（0.5%）の場合、情報レベルであり危機的ではない。

### 改善効果見積りルール
- 改善効果の見積りは、理論上の最大値ではなく、シグナルのコンテキスト比率に基づくこと。
  例: shuffle_impact_ratio=0.15の場合、シャッフル排除による最大改善は総時間の15%であり、50-70%ではない。
  spill_ratio_of_read、photon_ratio、remote_read_ratio等にも同じ原則を適用すること。
- 各ボトルネックの改善上限は、総実行時間に対するそのボトルネックの比率。

{_constraints_block("ja")}
- 提供されたメトリクス、アラート、および検出シグナルに基づいて分析すること
- 存在しない情報を推測で断定しないこと
- 最適化SQLが必要な場合は推奨事項内に含めること

{_action_plan_json_schema("ja")}
{"" if not is_serverless else _serverless_constraints_block("ja")}
{"" if not is_streaming else _streaming_constraints_block("ja")}
{"" if not is_federation else _federation_constraints_block("ja")}
{_v6_canonical_output_directive("ja")}
"""
    else:
        return f"""You are a Databricks SQL performance tuning expert.
Based on the provided query profile metrics and alerts, create specific sections of a structured report.

Below are the Databricks SQL tuning best practices:

{tuning_knowledge}

Output the following 4 sections. Each section MUST start with a ## header.

## 1. Executive Summary
Write a summary that a manager can grasp in 3 seconds, as natural prose paragraphs.
Do NOT use labels like "Headline:", "Facts:", "Insight:", "Navigation:". Write flowing text with these 4 elements:

Paragraph 1: One bold sentence stating the overall performance verdict (e.g., **Critical performance issue requiring immediate action.**)
Paragraph 2: Status, execution time, data read, alert counts in 1-2 concise sentences
Paragraph 3: 2-3 sentences on the likely root cause, expected improvement direction, and (if enough evidence) a target execution time range (e.g., "could reduce from ~10 min to 2-4 min"). Only state a target when high-confidence bottlenecks account for a significant fraction of total time. Otherwise write: "Target time depends on applied fixes — measure after implementation."
Paragraph 4: One sentence on what to look at next (e.g., Start with the P0 actions in the Action Plan.)

Do NOT list individual alerts (those go in the compact "Key Alerts" subsection of the Executive Summary, plus the full Appendix alerts list).

## 4. Root Cause Analysis
### 4.1 Direct Cause
Describe the immediate symptom (OOM, timeout, poor performance, etc.)
### 4.2 Root Cause
Describe the causal chain in [Cause] → [Intermediate Effect] → [Final Symptom] format.
MUST cite the provided alerts and metrics.
### 4.3 Disk Spill Analysis (only if spill detected)
Describe spill magnitude, affected operators, and countermeasures

## 7. Recommended Actions
Each item MUST include BOTH the bottleneck description AND the specific remediation.
If "Detected Bottlenecks" are provided in the Fact Pack, use them as the basis and enrich with detailed analysis and concrete fixes. Do NOT separate bottleneck description and remediation into different subsections.

{_recommendation_format_block("en")}

## 8. Conclusion
Restate root cause and summarize top-priority action (max 3 sentences)

### Signal-Based Severity Determination
The Fact Pack includes a "Detected Signals" section with factual observations.
YOU must determine the actual severity of each signal by considering:
- The signal's context ratios (e.g., spill_ratio_of_read), not just absolute values
- Whether the signal is significant given the query's scale (short vs long, small vs large data)
- Correlations between multiple signals (e.g., spill + high shuffle → more severe than spill alone)
- A single signal with small absolute impact should NOT be rated as critical
Example: spill_detected with spill_ratio_of_read=0.005 (0.5%) is informational, not critical.

### Improvement Estimation Rules
- When estimating improvement, anchor to the signal's context ratio, not theoretical maximums.
  Example: if shuffle_impact_ratio=0.15, maximum improvement from eliminating shuffle is 15% of total time — NOT 50-70%.
  Apply the same principle to spill_ratio_of_read, photon_ratio, remote_read_ratio, etc.
- Each bottleneck's improvement upper bound is its ratio of total execution time.

{_constraints_block("en")}
- Base analysis on provided metrics, alerts, AND detected signals
- Do not assert facts not present in the data
- If SQL optimization is needed, include it within Recommendations

{_action_plan_json_schema("en")}
{"" if not is_serverless else _serverless_constraints_block("en")}
{"" if not is_streaming else _streaming_constraints_block("en")}
{"" if not is_federation else _federation_constraints_block("en")}
{_v6_canonical_output_directive("en")}
"""


def create_structured_analysis_prompt(analysis: ProfileAnalysis, lang: str | None = None) -> str:
    """Create analysis prompt with Fact Pack for structured report generation."""
    if lang is None:
        lang = get_language()

    qm = analysis.query_metrics
    bi = analysis.bottleneck_indicators

    sections = []

    # Fact Pack Summary: machine-readable overview placed first for LLM focus
    summary_block = _build_fact_pack_summary(analysis, lang)
    if summary_block:
        sections.append(summary_block)

    is_streaming = (
        analysis.streaming_context is not None and analysis.streaming_context.is_streaming
    )
    cumulative_label = " (Cumulative Snapshot — NOT per-batch)" if is_streaming else ""
    sections.append(f"""## Query Information{cumulative_label}
- Query ID: {qm.query_id}
- Status: {qm.status}
- Total Time: {format_time_ms(qm.total_time_ms)}{" (query uptime, not batch latency)" if is_streaming else ""}
- Execution Time: {format_time_ms(qm.execution_time_ms)}
- Compilation Time: {format_time_ms(qm.compilation_time_ms)}
- Task Total Time (CPU): {format_time_ms(qm.task_total_time_ms)}
- Photon Total Time: {format_time_ms(qm.photon_total_time_ms)}""")

    # Cost estimation — minimum viable size is inferred from parallelism
    # when warehouse API could not provide it. See
    # dbsql_cost._infer_size_from_parallelism for the rationale and
    # confidence tiers. This is a lower bound, not a normative
    # recommendation (the recommendation is produced separately).
    cost = estimate_query_cost(qm, analysis.warehouse_info)
    if cost:
        cost_lines = ["## Cost Estimation", f"- Billing Model: {cost.billing_model}"]
        if cost.cluster_size:
            cost_lines.append(f"- Cluster Size: {cost.cluster_size}")
        if cost.dbu_per_hour:
            cost_lines.append(f"- DBU/hour: {cost.dbu_per_hour}")
        cost_lines.extend(
            [
                f"- DBU Unit Price: {format_cost_usd(cost.dbu_unit_price)}/DBU",
                f"- Estimated DBU: {cost.estimated_dbu:.4f}",
                f"- Estimated Cost: {format_cost_usd(cost.estimated_cost_usd)}",
            ]
        )
        if cost.parallelism_ratio > 0:
            cost_lines.append(f"- Parallelism Ratio: {cost.parallelism_ratio:.1f}x")
        if cost.is_estimated_size:
            cost_lines.append(
                "- Note: Minimum viable size is inferred from parallelism. For "
                "low-parallelism queries this is a *minimum-viable-size* lower "
                "bound; the actual provisioned warehouse may be larger "
                "(over-provisioned workload). This is NOT a normative "
                "right-sizing recommendation."
            )
        if not cost.is_per_query:
            cost_lines.append(
                "- Note: Classic/Pro billed per uptime, not per query (this is a share estimate)"
            )
        if cost.reference_costs:
            cost_lines.append("- Reference costs by size:")
            for ref in cost.reference_costs:
                cost_lines.append(
                    f"  - {ref.cluster_size} ({ref.dbu_per_hour} DBU/h): "
                    f"{format_cost_usd(ref.estimated_cost_usd)}"
                )
        sections.append("\n".join(cost_lines))

    # Always include queue signals (available from profile JSON even without WH API)
    sizing_lines = [
        "## Warehouse Sizing Signals",
        f"- queued_overload_time: {qm.queued_overload_time_ms}ms",
        f"- queued_provisioning_time: {qm.queued_provisioning_time_ms}ms",
    ]
    if analysis.warehouse_info:
        wh = analysis.warehouse_info
        sizing_lines.extend(
            [
                f"- warehouse_type: {wh.warehouse_type}",
                f"- cluster_size: {wh.cluster_size}",
                f"- max_clusters: {wh.max_num_clusters}",
                f"- is_serverless: {wh.is_serverless}",
            ]
        )
    if True:  # always emit sizing signals
        sections.append("\n".join(sizing_lines))

    io_block = f"""## I/O Metrics
- Read Bytes: {format_bytes(qm.read_bytes)}
- Read Remote Bytes: {format_bytes(qm.read_remote_bytes)}
- Read Cache Bytes: {format_bytes(qm.read_cache_bytes)}
- Cache Hit Ratio: {bi.cache_hit_ratio:.1%} ({bi.cache_severity.value})
- Spill to Disk: {format_bytes(qm.spill_to_disk_bytes)} ({bi.spill_severity.value})
- Rows Read: {qm.rows_read_count:,}
- Rows Produced: {qm.rows_produced_count:,}
- Photon Utilization: {bi.photon_ratio:.1%} ({bi.photon_severity.value})
- Shuffle Impact: {bi.shuffle_impact_ratio:.1%} ({bi.shuffle_severity.value})"""
    ns_funcs = collect_non_sargable_filter_functions(analysis.node_metrics)
    if ns_funcs:
        io_block += "\n- Non-sargable filters detected: " + ", ".join(f"{f}(...)" for f in ns_funcs)
    sections.append(io_block)

    # Queue & Result Delivery Summary
    total_queue = qm.queued_provisioning_time_ms + qm.queued_overload_time_ms
    if total_queue > 0 or qm.result_from_cache or qm.result_fetch_time_ms > 0:
        delivery_lines = ["## Queue & Result Delivery"]
        if qm.result_from_cache:
            delivery_lines.append(
                "- **Result Cache Hit**: true (execution was skipped — "
                "performance metrics do NOT represent actual execution)"
            )
        if total_queue > 0:
            delivery_lines.append(
                f"- Queue Time: {format_time_ms(total_queue)} "
                f"(provisioning={format_time_ms(qm.queued_provisioning_time_ms)}, "
                f"overload={format_time_ms(qm.queued_overload_time_ms)})"
            )
            if qm.total_time_ms > 0:
                delivery_lines.append(f"- Queue % of Total: {total_queue / qm.total_time_ms:.1%}")
        if qm.result_fetch_time_ms > 0:
            delivery_lines.append(f"- Result Fetch Time: {format_time_ms(qm.result_fetch_time_ms)}")
        sections.append("\n".join(delivery_lines))

    # Compilation Summary
    if qm.compilation_time_ms > 5000 or qm.metadata_time_ms > 0 or qm.planning_phases:
        comp_lines = ["## Compilation Summary"]
        comp_lines.append(f"- Compilation Time: {format_time_ms(qm.compilation_time_ms)}")
        if qm.metadata_time_ms > 0:
            comp_lines.append(f"- Metadata Resolution: {format_time_ms(qm.metadata_time_ms)}")
        if qm.planning_phases:
            top_phases = sorted(
                qm.planning_phases, key=lambda p: p.get("duration_ms", 0), reverse=True
            )[:3]
            for p in top_phases:
                comp_lines.append(
                    f"- Phase {p.get('phase', '?')}: {format_time_ms(p.get('duration_ms', 0))}"
                )
        sections.append("\n".join(comp_lines))

    # Shuffle I/O Summary
    if bi.shuffle_bytes_written_total > 0 or bi.shuffle_remote_bytes_read_total > 0:
        shuf_lines = ["## Shuffle I/O Summary"]
        shuf_lines.append(
            f"- Shuffle Bytes Written: {format_bytes(bi.shuffle_bytes_written_total)}"
        )
        shuf_lines.append(
            f"- Shuffle Remote Read: {format_bytes(bi.shuffle_remote_bytes_read_total)}"
        )
        shuf_lines.append(
            f"- Shuffle Local Read: {format_bytes(bi.shuffle_local_bytes_read_total)}"
        )
        total_shuf_read = bi.shuffle_remote_bytes_read_total + bi.shuffle_local_bytes_read_total
        if total_shuf_read > 0:
            remote_pct = bi.shuffle_remote_bytes_read_total / total_shuf_read
            shuf_lines.append(f"- Remote Shuffle Ratio: {remote_pct:.1%}")
        if qm.read_bytes > 0:
            shuf_lines.append(
                f"- Shuffle Write / Read Ratio: {bi.shuffle_bytes_written_total / qm.read_bytes:.1%}"
            )
        sections.append("\n".join(shuf_lines))

    # Write I/O Summary
    if qm.write_remote_bytes > 0:
        write_lines = ["## Write I/O Summary"]
        write_lines.append(f"- Write Bytes: {format_bytes(qm.write_remote_bytes)}")
        write_lines.append(f"- Write Files: {qm.write_remote_files:,}")
        write_lines.append(f"- Write Rows: {qm.write_remote_rows:,}")
        if qm.write_remote_files > 0:
            avg_size = qm.write_remote_bytes / qm.write_remote_files
            write_lines.append(f"- Avg File Size: {format_bytes(int(avg_size))}")
        if qm.read_bytes > 0:
            write_lines.append(f"- Write/Read Ratio: {qm.write_remote_bytes / qm.read_bytes:.2f}x")
        sections.append("\n".join(write_lines))

    if bi.alerts:
        alert_lines = ["## Structured Alerts (MUST reference in analysis)"]
        for alert in sorted(bi.alerts, key=lambda a: _severity_order(a.severity)):
            section_ref = get_knowledge_section_refs(alert.category)
            ref_str = f" {section_ref}" if section_ref else ""
            alert_lines.append(
                f"- [{alert.severity.value.upper()}] [{alert.category}] "
                f"{alert.message} (current={alert.current_value}, threshold={alert.threshold})"
                f"{ref_str}"
            )
            if alert.recommendation:
                alert_lines.append(f"  Action: {alert.recommendation}")
        sections.append("\n".join(alert_lines))

    # Detected signals — factual observations for LLM-driven severity determination
    if bi.detected_signals:
        sig_lines = [
            "## Detected Signals (factual observations — YOU determine severity)",
            "Each signal is a factual observation with context. Use the combination",
            "of signals, their context ratios, and reference thresholds to determine",
            "actual severity. A single signal alone does NOT imply high severity.",
            "",
        ]
        for sig in bi.detected_signals:
            sig_lines.append(f"### {sig.signal_id} [{sig.category}]")
            sig_lines.append(f"- Description: {sig.description}")
            sig_lines.append(f"- Observed: {sig.observed_value}")
            sig_lines.append(f"- Reference: {sig.reference_value}")
            if sig.context:
                ctx_parts = [f"{k}={v}" for k, v in sig.context.items()]
                sig_lines.append(f"- Context: {', '.join(ctx_parts)}")
            sig_lines.append("")
        sections.append("\n".join(sig_lines))

    if analysis.data_flow:
        flow_lines = ["## Data Flow (row count through operators)"]
        for entry in analysis.data_flow:
            keys_str = f" [keys: {entry.join_keys}]" if entry.join_keys else ""
            flow_lines.append(
                f"- {entry.operation}: {entry.output_rows:,} rows, "
                f"{format_time_ms(entry.duration_ms)}, "
                f"{format_bytes(entry.peak_memory_bytes)}{keys_str}"
            )
        sections.append("\n".join(flow_lines))

    active_stages = [s for s in analysis.stage_info if s.status != "SKIPPED"]
    if active_stages:
        stage_lines = ["## Stage Execution"]
        for s in sorted(active_stages, key=lambda x: x.duration_ms, reverse=True)[:10]:
            stage_lines.append(
                f"- Stage {s.stage_id}: {s.status} "
                f"duration={format_time_ms(s.duration_ms)} "
                f"tasks={s.num_tasks} done={s.num_complete_tasks} "
                f"failed={s.num_failed_tasks}"
            )
        sections.append("\n".join(stage_lines))

    if analysis.join_info:
        join_lines = ["## Join Types"]
        for ji in analysis.join_info:
            photon_str = "Photon" if ji.is_photon else "non-Photon"
            join_lines.append(f"- {ji.node_name}: {ji.join_type.display_name} ({photon_str})")
        sections.append("\n".join(join_lines))

    if bi.spill_operators:
        spill_lines = ["## Top Spill Operators"]
        for op in bi.spill_operators:
            spill_lines.append(
                f"- {op.node_name} (node {op.node_id}): "
                f"spill={format_bytes(op.spill_bytes)} ({op.spill_share_percent:.1f}%)"
            )
        sections.append("\n".join(spill_lines))

    # Aggregate expressions with implicit CAST detection
    # When decimal(38,0) columns participate in arithmetic (*, +, etc.),
    # the engine implicitly casts to decimal(38,18) which is slower than INT/BIGINT ops.
    agg_exprs = []
    for nm in analysis.node_metrics:
        if nm.aggregate_expressions:
            for expr in nm.aggregate_expressions:
                agg_exprs.append((nm.node_id, nm.node_name, expr))
    if agg_exprs:
        # Check if any aggregate references columns from tables with decimal(38,0) issues
        decimal38_columns = set()
        if analysis.top_scanned_tables:
            for tsm in analysis.top_scanned_tables:
                if tsm.current_clustering_keys:
                    for ck in tsm.current_clustering_keys:
                        decimal38_columns.add(ck.lower())
        # Also collect from sql_analysis column info if available
        if analysis.sql_analysis and analysis.sql_analysis.columns:
            for col in analysis.sql_analysis.columns:
                col_name = getattr(col, "column_name", "") or getattr(col, "name", "")
                if col_name:
                    decimal38_columns.add(col_name.lower())

        agg_lines = ["## Aggregate Expressions (from query profile)"]
        has_decimal_arithmetic = False
        for node_id, node_name, expr in agg_exprs[:10]:
            # Check if expression involves multiplication/addition with column references
            # that might be decimal(38,0) → triggers implicit CAST to decimal(38,18)
            expr.lower()
            involves_arithmetic = "*" in expr or "+" in expr or "-" in expr
            if involves_arithmetic:
                has_decimal_arithmetic = True
                agg_lines.append(
                    f"- Node {node_id} ({node_name}): `{expr}` ⚠ arithmetic on decimal columns may cause implicit CAST to DECIMAL(38,18)"
                )
            else:
                agg_lines.append(f"- Node {node_id} ({node_name}): `{expr}`")
        if has_decimal_arithmetic:
            agg_lines.append("")
            agg_lines.append(
                "**Note:** If participating columns are DECIMAL(38,0) but contain only integer values, "
                "converting them to INT/BIGINT eliminates implicit CAST overhead and enables faster SIMD operations."
            )
        sections.append("\n".join(agg_lines))

    # SQL Query — prefer formatted_sql, fall back to raw query_text
    sql_text = ""
    if analysis.sql_analysis and analysis.sql_analysis.formatted_sql:
        sql_text = analysis.sql_analysis.formatted_sql
    elif qm.query_text:
        sql_text = qm.query_text
    if sql_text:
        if len(sql_text) > 3000:
            sql_text = sql_text[:3000] + "\n-- ... (truncated)"
        sections.append(f"## SQL Query\n```sql\n{sql_text}\n```")

    # Target Table Configuration — INSERT/CTAS/MERGE write target's
    # Delta/Parquet format, clustering columns, hierarchical clustering,
    # and key delta.* properties. Null for SELECT-only queries.
    tt_lines = _format_target_table_config(analysis)
    if tt_lines:
        sections.append("## Target Table Configuration\n" + "\n".join(tt_lines))

    # Table Scan Info — clustering keys (from JSON) + column types (from
    # EXPLAIN). Emit even without EXPLAIN because clustering keys alone
    # prevent the LLM from guessing structure from table-name suffixes.
    scan_lines = _format_table_scan_info(analysis)
    if scan_lines:
        sections.append("## Table Scan Info\n" + "\n".join(scan_lines))

    # Shuffle Details (per-node) — partitioning keys, memory per partition,
    # spill events. The old aggregate summary hid which key the worst
    # shuffle used, so LLMs could not name the bottleneck key.
    shuf_lines = _format_shuffle_details(analysis)
    if shuf_lines:
        sections.append("## Shuffle Details (top shuffles by memory)\n" + "\n".join(shuf_lines))

    # EXPLAIN-derived insights (only when EXPLAIN was attached)
    if analysis.explain_analysis:
        explain_lines = ["## EXPLAIN Analysis Insights"]
        ea = analysis.explain_analysis
        if ea.optimizer_statistics:
            os_ = ea.optimizer_statistics
            if os_.full_tables and not os_.missing_tables and not os_.partial_tables:
                explain_lines.append(
                    "- **Optimizer Statistics**: All tables have FULL statistics "
                    "(ANALYZE TABLE is NOT needed — DO NOT recommend it). "
                    "If hash_table_resize_count is still high, the cause is data/structural "
                    "(row explosion, duplicate GROUP BY, skew, NULL concentration, "
                    "JOIN key type mismatch, or DECIMAL key), NOT stale statistics. "
                    "Predictive optimization may also be maintaining statistics "
                    "automatically — recommending re-analysis would be misleading."
                )
            else:
                if os_.missing_tables:
                    explain_lines.append(
                        f"- **Missing Statistics**: {', '.join(os_.missing_tables)}"
                    )
                if os_.partial_tables:
                    explain_lines.append(
                        f"- **Partial Statistics**: {', '.join(os_.partial_tables)}"
                    )
                if os_.full_tables:
                    explain_lines.append(
                        f"- **Full Statistics**: {', '.join(sorted(set(os_.full_tables)))}"
                    )
        if ea.photon_explanation:
            if ea.photon_explanation.fully_supported:
                explain_lines.append("- **Photon**: Fully supported")
            else:
                blockers = [item.expression for item in ea.photon_explanation.unsupported_items[:5]]
                explain_lines.append(
                    f"- **Photon Blockers**: {', '.join(blockers) if blockers else 'partial support'}"
                )
        if ea.exchanges:
            shuffle_count = sum(1 for e in ea.exchanges if e.ensure_requirements)
            explain_lines.append(
                f"- **Exchanges**: {len(ea.exchanges)} total ({shuffle_count} shuffle)"
            )

        # v2 insights — richer signals for analysis accuracy and rewrite correctness.
        v2_lines = _format_explain_v2_insights(ea)
        if v2_lines:
            explain_lines.append("\n### EXPLAIN v2 Insights")
            explain_lines.extend(v2_lines)

        if len(explain_lines) > 1:
            sections.append("\n".join(explain_lines))

    if analysis.evidence_bundle and analysis.evidence_bundle.items:
        evidence_text = format_evidence_for_prompt(analysis.evidence_bundle, lang)
        if evidence_text:
            sections.append(evidence_text)

    # Include rule-based action cards as context for LLM recommendations
    if analysis.action_cards:
        card_lines = ["## Detected Bottlenecks (rule-based, use as context for Recommendations)"]
        for i, card in enumerate(
            sorted(analysis.action_cards, key=lambda c: c.priority_score, reverse=True)[:10], 1
        ):
            card_lines.append(f"### {i}. {card.problem}")
            card_lines.append(f"- Impact: {card.expected_impact}, Effort: {card.effort}")
            if card.evidence:
                card_lines.append(f"- Evidence: {'; '.join(card.evidence[:3])}")
            if card.likely_cause:
                card_lines.append(f"- Likely cause: {card.likely_cause}")
            if card.fix:
                card_lines.append(f"- Suggested fix: {card.fix}")
            card_lines.append("")
        sections.append("\n".join(card_lines))

    # Streaming context (micro-batch statistics)
    if analysis.streaming_context and analysis.streaming_context.is_streaming:
        from ..extractors import compute_batch_statistics

        ctx = analysis.streaming_context
        batch_stats = compute_batch_statistics(ctx)
        stream_lines = ["## Streaming / Micro-Batch Context (PRIMARY — use for analysis)"]
        stream_lines.append("- Query Type: REFRESH STREAMING TABLE")
        if ctx.target_table:
            stream_lines.append(f"- Target Table: {ctx.target_table}")
        if ctx.entry_point:
            stream_lines.append(f"- Entry Point: {ctx.entry_point}")
        stream_lines.append(f"- Micro-Batch Count: {batch_stats['batch_count']}")
        stream_lines.append(f"- Finished Batches: {batch_stats['finished_count']}")
        if batch_stats.get("running_count", 0) > 0:
            stream_lines.append(f"- Running Batches: {batch_stats['running_count']}")
        if batch_stats["batch_count"] > 0:
            stream_lines.append(f"- Duration Min: {format_time_ms(batch_stats['duration_min_ms'])}")
            stream_lines.append(
                f"- Duration Avg: {format_time_ms(int(batch_stats['duration_avg_ms']))}"
            )
            if batch_stats.get("duration_p95_ms", 0) > 0:
                stream_lines.append(
                    f"- Duration P95: {format_time_ms(batch_stats['duration_p95_ms'])}"
                )
            stream_lines.append(f"- Duration Max: {format_time_ms(batch_stats['duration_max_ms'])}")
            if batch_stats.get("duration_cv", 0) > 0:
                stream_lines.append(f"- Duration CV: {batch_stats['duration_cv']:.2f}")
            stream_lines.append(
                f"- Read Bytes Min: {format_bytes(batch_stats.get('read_bytes_min', 0))}"
            )
            stream_lines.append(
                f"- Read Bytes Avg: {format_bytes(batch_stats.get('read_bytes_avg', 0))}"
            )
            stream_lines.append(
                f"- Read Bytes Max: {format_bytes(batch_stats.get('read_bytes_max', 0))}"
            )
            stream_lines.append(f"- Rows Read Avg: {batch_stats.get('rows_avg', 0):,.0f}")
        if batch_stats.get("slow_batches"):
            stream_lines.append(
                f"- Slow Batches (>2x avg): {len(batch_stats['slow_batches'])} / {batch_stats['batch_count']}"
            )
            stream_lines.append("")
            stream_lines.append("### Slowest Micro-Batches")
            for sb in batch_stats["slow_batches"]:
                stream_lines.append(
                    f"- plan_id={sb['plan_id']}, duration={format_time_ms(sb['duration_ms'])}"
                    f"{', read_bytes=' + format_bytes(sb.get('read_bytes', 0)) if sb.get('read_bytes') else ''}"
                )
        sections.append("\n".join(stream_lines))

    prompt_header = (
        "以下のFact Packを元に、レポートの4セクションを生成してください。\n\n"
        if lang == "ja"
        else "Based on the following Fact Pack, generate the 4 report sections.\n\n"
    )

    result = prompt_header + "\n\n".join(sections)

    # Enforce user prompt size budget
    from ..llm_client import MAX_PROMPT_CHARS

    if len(result) > MAX_PROMPT_CHARS:
        logger.warning(
            "User prompt %d chars exceeds budget %d, truncating",
            len(result),
            MAX_PROMPT_CHARS,
        )
        result = result[:MAX_PROMPT_CHARS] + "\n\n<!-- prompt truncated -->"

    return result


# =============================================================================
# Clustering prompts
# =============================================================================


def create_clustering_prompt(
    target_table: str,
    candidate_columns: list[dict],
    top_scanned_tables: list[dict],
    filter_rate: float,
    read_files_count: int,
    pruned_files_count: int,
    explain_summary: str = "",
    lang: str | None = None,
    shuffle_metrics=None,
    query_sql: str = "",  # Deprecated (v5.16.23): no longer embedded in prompt
) -> tuple[str, str]:
    """Create system and user prompts for clustering recommendation.

    Phase 3 (v5.16.23): ``query_sql`` is no longer used — the LC LLM
    receives all information via structured inputs
    (``candidate_columns``, ``top_scanned_tables``, ``shuffle_metrics``,
    etc.). The parameter is retained for backward compatibility with
    existing callers but is silently ignored.

    ``shuffle_metrics`` is the list of ``ShuffleMetrics`` from the profile.
    When supplied, notable shuffles (GiB-scale writes or memory-inefficient)
    are rendered as a ``## Shuffle Details`` section so the LC LLM can
    recognize runtime shuffle keys as LC candidates — clustering on the
    dominant shuffle key reduces shuffle volume for repeat queries with
    the same GROUP BY/JOIN shape.
    """
    if lang is None:
        lang = get_language()

    if lang == "ja":
        system_prompt = """あなたはDatabricksのLiquid Clusteringの専門家です。
クエリパターンとメトリクスを分析し、最適なクラスタリングキーを推奨してください。

## 判断基準
1. **範囲フィルタ** (BETWEEN, >=, <=): 時系列カラムを優先
2. **等価フィルタ** (=, IN): カーディナリティが適度なディメンションキーを優先
3. **JOINキー**: 大きなfactテーブルの結合キーを優先（小さいテーブルがBROADCASTされる場合は優先度を下げる）
4. **GROUP BY/ORDER BY**: スキャンのプルーニングに直接寄与する場合のみ候補に
5. **支配的シャッフルキー（Shuffle Details より）**: 当該テーブルのカラムで、GiB 級の書き込み量または memory-inefficient なシャッフルの partitioning key として現れるものは LC 候補として評価する。クラスタリングにより同値データが同一ファイルに集約されるため、同じ GROUP BY/JOIN を持つ繰り返しクエリのシャッフル量が削減される。ただし (a) プルーニングに寄与する filter/join 列を優先、(b) カーディナリティが極端に低い（<10）なら単独キーは避け、他キーと組み合わせるか Hierarchical Clustering を検討

## 出力形式
必ず以下のJSON形式で回答してください（他の説明文は不要）:
```json
{
  "target_table": "テーブル名",
  "recommended_keys": ["key1", "key2"],
  "workload_pattern": "olap|oltp|timeseries|unknown",
  "rationale": "推奨理由（日本語で1-2文）",
  "confidence": 0.0-1.0,
  "alternatives": ["代替キー候補"]
}
```"""
        user_template = """## ターゲットテーブル
{target_table}

## 候補カラム
{candidate_columns_str}

## I/O指標
- フィルタ効率: {filter_rate:.1%}
- 読み取りファイル数: {read_files_count:,}
- プルーニングファイル数: {pruned_files_count:,}

## スキャン量上位テーブル
{top_tables_str}

{shuffle_section}{explain_section}
最適なクラスタリングキーをJSON形式で推奨してください。"""
    else:
        system_prompt = """You are a Databricks Liquid Clustering expert.
Analyze the query pattern and metrics to recommend optimal clustering keys.

## Evaluation Criteria
1. **Range filters** (BETWEEN, >=, <=): Prioritize time-series columns
2. **Equality filters** (=, IN): Prioritize dimension keys with moderate cardinality
3. **JOIN keys**: Prioritize join keys of large fact tables (lower priority if small table is broadcast)
4. **GROUP BY/ORDER BY**: Include only if they directly contribute to scan pruning
5. **Dominant shuffle key (from Shuffle Details)**: When a column of THIS table appears as the partitioning key of a GiB-scale or memory-inefficient shuffle, evaluate it as an LC candidate. Clustering co-locates same-value rows into the same file, which shrinks shuffle volume for repeat queries with the same GROUP BY/JOIN. Caveats: (a) filter/join columns that contribute to pruning take precedence, (b) if cardinality is extremely low (<10 distinct values), avoid using it as a sole key — combine with another key or consider Hierarchical Clustering.

## Output Format
Always respond in the following JSON format (no additional explanation needed):
```json
{
  "target_table": "table_name",
  "recommended_keys": ["key1", "key2"],
  "workload_pattern": "olap|oltp|timeseries|unknown",
  "rationale": "Recommendation reason (1-2 sentences)",
  "confidence": 0.0-1.0,
  "alternatives": ["alternative_key_candidates"]
}
```"""
        user_template = """## Target Table
{target_table}

## Candidate Columns
{candidate_columns_str}

## I/O Metrics
- Filter efficiency: {filter_rate:.1%}
- Files read: {read_files_count:,}
- Files pruned: {pruned_files_count:,}

## Top Scanned Tables
{top_tables_str}

{shuffle_section}{explain_section}
Please recommend optimal clustering keys in JSON format."""

    if candidate_columns:
        candidate_columns_str = "\n".join(
            f"- {c.get('column', 'unknown')}: context={c.get('context', 'unknown')}, operator={c.get('operator', '-')}"
            for c in candidate_columns
        )
    else:
        candidate_columns_str = "- (no candidates extracted)"

    if top_scanned_tables:
        lines = []
        for t in top_scanned_tables:
            line = f"- {t.get('table_name', 'unknown')}: {t.get('bytes_read', 0):,} bytes read"
            keys = t.get("current_clustering_keys")
            card_map = t.get("clustering_key_cardinality") or {}
            if keys:
                parts = []
                for k in keys:
                    cls = card_map.get(k, "unknown")
                    if cls == "low":
                        parts.append(f"{k} [low-card]")
                    elif cls == "high":
                        parts.append(f"{k} [high-card]")
                    else:
                        parts.append(f"{k} [unknown-card]")
                line += f" (current clustering keys: {', '.join(parts)})"
            else:
                line += " (no clustering keys configured)"
            lines.append(line)
        top_tables_str = "\n".join(lines)
    else:
        top_tables_str = "- (no table scan metrics)"

    explain_section = ""
    if explain_summary:
        explain_section = f"## EXPLAIN Summary\n{explain_summary}\n\n"

    # Shuffle Details — only notable shuffles, mirroring the gate used
    # by _format_shuffle_details so we do not inflate the prompt with
    # trivial shuffles. Keys are surfaced verbatim so the LC LLM can
    # decide whether they are LC candidates for this table.
    shuffle_section = ""
    notable_shuffles = []
    for sm in shuffle_metrics or []:
        if _is_notable_shuffle(sm) and sm.shuffle_attributes:
            notable_shuffles.append(sm)
    if notable_shuffles:
        notable_shuffles.sort(key=lambda s: s.peak_memory_bytes or 0, reverse=True)
        if lang == "ja":
            lines = ["## シャッフル詳細（notableのみ、peak_memory 降順）"]
        else:
            lines = ["## Shuffle Details (notable only, sorted by peak memory)"]
        for sm in notable_shuffles[:5]:
            peak_gb = (sm.peak_memory_bytes or 0) / 1024**3
            written_gb = (sm.sink_bytes_written or 0) / 1024**3
            mpp = sm.memory_per_partition_mb
            keys = ", ".join(f"`{k}`" for k in sm.shuffle_attributes)
            lines.append(
                f"- Node #{sm.node_id}: partitioning key(s) {keys}, "
                f"peak {peak_gb:.1f} GB ({mpp:.0f} MB/part), written {written_gb:.1f} GB"
            )
        shuffle_section = "\n".join(lines) + "\n\n"

    # Phase 3 (v5.16.23): SQL body is no longer included in the LC LLM
    # user prompt. All information needed for clustering-key selection
    # is already structured (Candidate Columns with operator + context,
    # I/O Metrics, Top Scanned Tables with cardinality, Shuffle Details).
    # The raw SQL was previously truncated to 2000 chars which could
    # silently lose WHERE columns on long queries. ``query_sql`` is
    # kept as a parameter for backward compat but unused in the prompt.
    user_prompt = user_template.format(
        target_table=target_table,
        candidate_columns_str=candidate_columns_str,
        filter_rate=filter_rate,
        read_files_count=read_files_count,
        pruned_files_count=pruned_files_count,
        top_tables_str=top_tables_str,
        shuffle_section=shuffle_section,
        explain_section=explain_section,
    )

    return system_prompt, user_prompt


# =============================================================================
# Query Rewrite Prompts
# =============================================================================


@_append_korean_directive
def create_rewrite_system_prompt(
    knowledge: str,
    lang: str,
    *,
    is_serverless: bool = False,
    token_constrained: bool = False,
) -> str:
    """Build system prompt for SQL query rewrite based on bottleneck analysis."""
    if lang == "ja":
        base = (
            "あなたは Databricks SQL クエリリライターです。\n"
            "ボトルネック分析結果に基づいて元の SQL を最適化してください。\n\n"
            "## 厳守ルール\n"
            "- 元クエリと**完全に同じ結果セット**を返すこと\n"
            "- テーブル名・カラム名・エイリアスを元クエリからそのまま使うこと\n"
            "- 実行不可能な変更は提案しないこと\n"
            "- 改善の余地がない場合は「リライト不要」とだけ返すこと\n"
        )
        fmt = (
            "\n## 出力フォーマット（厳守）\n"
            "ボトルネック分析、根拠メトリクス、期待効果などの説明文は**出力しない**。\n"
            "以下の構成のみ出力すること:\n\n"
            "1行目: `アクションプラン #1, #3, #5 を適用しました。` の形式（番号はユーザープロンプト内のアクションカード番号に対応）\n"
            "2行目: `適用内容:「XXX」「YYY」` の形式（各アクションの具体的な改善名）\n\n"
            "（任意）分析結果に基づき、リライト前に1回実行すべき補助コマンド（ANALYZE TABLE、OPTIMIZE FULL、ALTER TABLE CLUSTER BY 等）がある場合:\n"
            "**注意:** ALTER TABLE CLUSTER BY の後の OPTIMIZE には必ず FULL オプションをつけること（OPTIMIZE FULL）。FULL がないと新規レコードしかクラスタリングされない。\n"
            "### 事前実行\n"
            "```sql で囲んだ補助コマンド（複数の場合はセミコロン区切り）\n```\n\n"
            "### リライト後 SQL\n"
            "```sql で囲んだ**完全なメインクエリ**（省略禁止・placeholder 禁止・コメント禁止）\n\n"
            "補助コマンドが不要な場合は「事前実行」セクションを省略し、リライト後 SQL のみ出力すること。\n"
        )
    else:
        base = (
            "You are a Databricks SQL query rewriter.\n"
            "Optimize the original SQL based on bottleneck analysis results.\n\n"
            "## Strict Rules\n"
            "- MUST return the **exact same result set** as the original\n"
            "- Use table names, column names, and aliases exactly as in the original\n"
            "- Do not suggest changes that cannot be executed\n"
            "- If no rewrite is needed, respond only with 'No rewrite necessary'\n"
        )
        fmt = (
            "\n## Output Format (STRICT)\n"
            "Do NOT output bottleneck analysis, metric citations, expected impact, or any explanation.\n"
            "Output ONLY the following structure:\n\n"
            "Line 1: `Applied action plans #1, #3, #5.` format (numbers correspond to action card numbers in the user prompt)\n"
            'Line 2: `Changes: "XXX", "YYY"` format (specific improvement names for each action)\n\n'
            "(Optional) If analysis results indicate auxiliary commands to run once before the rewrite (ANALYZE TABLE, OPTIMIZE FULL, ALTER TABLE CLUSTER BY, etc.):\n"
            "**IMPORTANT:** After ALTER TABLE CLUSTER BY, OPTIMIZE must include the FULL option (OPTIMIZE FULL). Without FULL, only new records are clustered.\n"
            "### Pre-execution\n"
            "```sql block with auxiliary commands (semicolon-separated if multiple)\n```\n\n"
            "### Rewritten SQL\n"
            "```sql block with the **complete main query** (no omissions, no placeholders, no comments)\n\n"
            "If no auxiliary commands are needed, omit the Pre-execution section and output only Rewritten SQL.\n"
        )

    serverless_block = ""
    if is_serverless:
        if lang == "ja":
            serverless_block = (
                "\n## Serverless 制約\n"
                "Serverless SQL Warehouse では SET パラメータ変更が制限されています。\n"
                "SQL リライトのみで最適化してください:\n"
                "- CTE での事前集約（JOIN 前にデータ削減）\n"
                "- 早期 WHERE フィルタ（shuffle 前にデータ削減）\n"
                "- JOIN ヒント（JOINがあるSELECT句に `/*+ BROADCAST(エイリアス) */` を配置。CTE内のJOINにはそのCTEのSELECTに記述）\n"
                "- 相関サブクエリの JOIN 書き換え\n"
                "- UNION ALL vs UNION\n"
                "- SELECT * の回避（必要カラムのみ指定）\n"
            )
        else:
            serverless_block = (
                "\n## Serverless Constraints\n"
                "Serverless SQL Warehouse does not allow SET parameter changes.\n"
                "Optimize using SQL rewrites only:\n"
                "- CTE pre-aggregation (reduce data before JOINs)\n"
                "- Early WHERE filters (reduce data before shuffle)\n"
                "- JOIN hints (place `/*+ BROADCAST(alias) */` in the SELECT of the query block containing the JOIN; use alias not full table name)\n"
                "- Rewrite correlated subqueries as JOINs\n"
                "- UNION ALL instead of UNION\n"
                "- Avoid SELECT * (specify needed columns)\n"
            )

    truncation_block = ""
    if token_constrained:
        if lang == "ja":
            truncation_block = (
                "\n## 重要: 出力トークン制限\n"
                "元クエリが非常に長いため、出力トークン上限内で完全な SQL を再現できない可能性があります。\n"
                "以下の戦略で可能な限り完全に近い SQL を出力してください:\n\n"
                "1. **変更がある箇所のみ差分で示す**: 変更のない部分は `-- ... (original lines N-M unchanged) ...` で省略可\n"
                "2. **完全な差分パッチ形式で出力**: ユーザーが元クエリに適用できる形にする\n"
                "3. **変更箇所の前後 2-3 行のコンテキスト**を含め、適用位置が明確になるようにする\n"
                "4. 最後に「手動マージ手順」を箇条書きで記載する\n\n"
                "出力フォーマットを以下に変更:\n"
                "### リライト後 SQL（差分形式）\n"
                "```sql\n-- 変更箇所1: (行番号付近の説明)\n-- 元:\n...元の行...\n-- 変更後:\n...新しい行...\n```\n\n"
                "### 手動マージ手順\n"
                "1. 元クエリの行 N を ... に置き換える\n"
            )
        else:
            truncation_block = (
                "\n## IMPORTANT: Output Token Limit\n"
                "The original query is very long and may exceed the output token limit.\n"
                "Use the following strategy to produce the most complete rewrite possible:\n\n"
                "1. **Show only changed sections as diffs**: Use `-- ... (original lines N-M unchanged) ...` for unchanged parts\n"
                "2. **Output as a complete diff patch**: Users will apply changes to the original query\n"
                "3. **Include 2-3 lines of context** around each change so the apply location is clear\n"
                "4. End with a **Manual Merge Steps** checklist\n\n"
                "Change the output format to:\n"
                "### Rewritten SQL (diff format)\n"
                "```sql\n-- Change 1: (description near line N)\n-- Original:\n...original lines...\n-- Rewritten:\n...new lines...\n```\n\n"
                "### Manual Merge Steps\n"
                "1. Replace line N with ...\n"
            )

    knowledge_block = ""
    if knowledge:
        knowledge_block = f"\n## Tuning Knowledge\n{knowledge}\n"

    return base + serverless_block + truncation_block + knowledge_block + fmt


def create_rewrite_fix_system_prompt(lang: str) -> str:
    """Build system prompt for fixing a previously rewritten SQL based on user feedback."""
    if lang == "ja":
        return (
            "あなたは Databricks SQL クエリ修正者です。\n"
            "前回リライトした SQL にユーザーからフィードバック（文法エラー、結果の相違、等）がありました。\n\n"
            "## 厳守ルール\n"
            "- ユーザーが指摘した箇所**のみ**修正すること。他の最適化部分は変更しない\n"
            "- 修正後も元クエリと**完全に同じ結果セット**を返すこと\n"
            "- 修正後の SQL は**そのままコピーして即実行できる完全な SQL** であること\n\n"
            "## 出力フォーマット（厳守）\n"
            "出力は**以下の2つだけ**。\n\n"
            "1行目: `修正内容: XXX` の形式（何を修正したか1行で）\n\n"
            "2行目以降: ```sql で囲んだ**完全な SQL**\n"
        )
    else:
        return (
            "You are a Databricks SQL query fixer.\n"
            "The previously rewritten SQL has feedback from the user (syntax error, incorrect results, etc.).\n\n"
            "## Strict Rules\n"
            "- Fix ONLY the issue the user reported. Do not change other optimizations\n"
            "- The fixed SQL MUST return the **exact same result set** as the original query\n"
            "- The fixed SQL MUST be **complete and copy-paste-ready**\n\n"
            "## Output Format (STRICT)\n"
            "Output **ONLY the following two parts**.\n\n"
            "Line 1: `Fix applied: XXX` format (what was fixed, 1 line)\n\n"
            "Line 2+: ```sql block with the **complete fixed SQL**\n"
        )


def create_rewrite_fix_user_prompt(
    original_sql: str,
    previous_rewrite: str,
    feedback: str,
    lang: str,
) -> str:
    """Build user prompt for fixing a rewritten SQL."""
    parts: list[str] = []

    parts.append(f"## Original SQL\n```sql\n{original_sql}\n```\n")
    parts.append(f"## Previous Rewrite\n```sql\n{previous_rewrite}\n```\n")

    if lang == "ja":
        parts.append(f"## ユーザーからのフィードバック\n{feedback}\n")
    else:
        parts.append(f"## User Feedback\n{feedback}\n")

    return "\n".join(parts)


def create_rewrite_user_prompt(analysis: ProfileAnalysis, lang: str) -> str:
    """Build user prompt for SQL query rewrite from analysis data."""
    parts: list[str] = []

    # Original SQL — never truncate (LLM needs the full query to produce a runnable rewrite)
    sql = analysis.query_metrics.query_text or ""
    parts.append(f"## Original SQL\n```sql\n{sql}\n```\n")

    # Top alerts
    alerts = sorted(
        analysis.bottleneck_indicators.alerts,
        key=lambda a: _severity_order(a.severity),
    )[:7]
    if alerts:
        if lang == "ja":
            parts.append("## ボトルネックアラート\n")
        else:
            parts.append("## Bottleneck Alerts\n")
        for a in alerts:
            parts.append(
                f"- [{a.severity.value.upper()}][{a.category}] {a.message}"
                f" (current={a.current_value}, threshold={a.threshold})\n"
            )
        parts.append("")

    # Action cards — numbered so LLM can reference them in output
    all_cards = analysis.action_cards or []
    if all_cards:
        if lang == "ja":
            parts.append("## アクションプラン（分析レポートより）\n")
        else:
            parts.append("## Action Plans (from analysis report)\n")
        for i, c in enumerate(all_cards, 1):
            parts.append(f"### #{i}: {c.problem}\n")
            parts.append(f"- Fix: {c.fix}\n")
            if c.fix_sql:
                parts.append(f"- SQL: `{c.fix_sql}`\n")
            if c.expected_impact:
                parts.append(f"- Impact: {c.expected_impact}\n")
            parts.append("")

    # Hot operators
    hot_ops = (analysis.hot_operators or [])[:5]
    if hot_ops:
        if lang == "ja":
            parts.append("## ホットオペレータ Top 5\n")
        else:
            parts.append("## Hot Operators Top 5\n")
        for op in hot_ops:
            parts.append(
                f"- {op.node_name}: {format_time_ms(op.duration_ms)} ({op.rows_out:,} rows)\n"
            )
        parts.append("")

    # Table scan info — rich format with clustering keys + column types so
    # the rewrite LLM doesn't guess structure from table-name suffixes or
    # ask for schema verification when the data is already present.
    rich_scan_lines = _format_table_scan_info(analysis)
    if rich_scan_lines:
        header = "## テーブルスキャン情報\n" if lang == "ja" else "## Table Scan Info\n"
        parts.append(header)
        parts.append("\n".join(rich_scan_lines))
        parts.append("")

    # Key metrics
    m = analysis.query_metrics
    b = analysis.bottleneck_indicators
    if lang == "ja":
        parts.append("## 主要メトリクス\n")
    else:
        parts.append("## Key Metrics\n")
    parts.append(f"- Total time: {format_time_ms(m.total_time_ms)}\n")
    parts.append(f"- Read: {format_bytes(m.read_bytes)}\n")
    if b.spill_bytes:
        parts.append(f"- Spill: {format_bytes(b.spill_bytes)}\n")
    if b.photon_ratio is not None:
        parts.append(f"- Photon utilization: {b.photon_ratio:.1%}\n")
    parts.append("")

    return "\n".join(parts)


# =============================================================================
# Helpers
# =============================================================================


def _severity_order(severity: Severity) -> int:
    """Sort order for severity (CRITICAL first)."""
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3, "info": 4, "ok": 5}
    return order.get(severity.value, 99)


def create_rerank_prompt(cards: list, lang: str | None = None) -> str:
    """Create Top-5 rerank prompt."""
    if lang is None:
        lang = get_language()
    header = (
        "ユーザー影響が最大になるよう最大5件を選んでください。重大アラートは尊重しつつ、同じ根本原因に対する冗長な施策は避けてください。優先度スコアの順序を基本とし、カバレッジや緊急度が明確に改善する場合のみ補正してください。"
        if lang == "ja"
        else "Select up to 5 actions that maximize user impact, preserve critical alerts, and avoid redundant actions addressing the same root cause. Use the provided priority score as the default ordering. Only override it when doing so clearly improves coverage or urgency."
    )
    lines = [header, "", "Candidate actions:" if lang != "ja" else "候補アクション一覧:"]
    for idx, card in enumerate(cards):
        lines.append(
            f"- id={idx} | problem={card.problem} | impact={card.expected_impact} | effort={card.effort} | priority_score={card.priority_score:.3f} | root_cause_group={card.root_cause_group or '-'} | coverage_category={card.coverage_category or '-'} | is_preserved={getattr(card, 'is_preserved', False)}"
        )
    lines.extend(
        [
            "",
            "Return JSON only:",
            "{",
            '  "selected_ids": [3, 7, 1, 12, 5],',
            '  "selection_rationale": {',
            '    "overall": "...",',
            '    "3": "..."',
            "  }",
            "}",
        ]
    )
    return "\n".join(lines)
