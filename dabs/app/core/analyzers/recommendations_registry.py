"""Spark Perf-style static-priority action card registry.

Design (v5.16.11, Phase 1 pilot): each rule-based ActionCard is
represented as a ``CardDef`` entry in the ``CARDS`` tuple with:

  - ``card_id``       — stable identifier (used for dedup vs legacy if-blocks)
  - ``priority_rank`` — static integer; higher surfaces first
  - ``detect(ctx)``   — bool predicate, absorbs all gating logic
  - ``build(ctx)``    — returns ``list[ActionCard]`` (empty = nothing
                        to emit; list because some cards generate
                        multiple instances, e.g., per-blocker Photon)

Emission is Spark Perf-like: iterate ``CARDS`` sorted by
``priority_rank`` descending, call each ``detect`` then ``build`` when
true, emit in that order. No dynamic priority_score calculation, no
Top-N cap at the registry layer (legacy selection still runs downstream
during Phase 1 for safety), no preservation-marker coupling (card titles
remain identical so existing markers still work).

Priority tiers (reflecting typical time-reduction magnitude, expert
judgment, matching the Spark Perf approach):

  Tier 1 (90-100) — direct high-magnitude time waste
     100  disk_spill              (2-5x speedup by eliminating spill)
      95  shuffle_dominant        (30-50% reduction via broadcast/rerank)
      90  shuffle_lc              (shuffle key → LC co-location benefit)
      85  data_skew               (AQE skew join / salting)

  Tier 2 (60-84) — moderate, condition-dependent
      80  low_file_pruning        (LC for pruning — requires scan_impact)
      75  low_cache               (re-run benefit only)
      70  photon_blocker          (per-blocker Photon rewrite, 2-4x)
      68  photon_low              (generic Photon utilization, 1.5-2x)
      65  scan_hot                (dominant scan operator)
      60  non_photon_join         (switch to broadcast / SHUFFLE_HASH)

  Tier 3 (40-59) — structural, limited effect
      55  hierarchical_cluster    (LC key refinement)
      50  hash_resize             (statistics / broadcast, 10-30%)
      45  aqe_absorbed            (AQE already handled; layout fix)
      40  cte_multi_ref           (CTE re-computation elimination)

  Tier 4 (20-39) — diagnostic / advisory
      38  investigate_dist        (diagnose first, then fix)
      35  stats_fresh             (rule out stale stats first)
      30  rescheduled_scan        (infra-origin, harder to fix via SQL)

Phase 1 pilot migrates 5 straightforward cards (disk_spill,
shuffle_dominant, low_cache, photon_low, rescheduled_scan) to
demonstrate the pattern end-to-end. Remaining cards are migrated in
subsequent phases.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from ..constants import THRESHOLDS
from ..i18n import gettext as _
from ..models import (
    ActionCard,
    BottleneckIndicators,
    JoinInfo,
    OperatorHotspot,
    QueryMetrics,
    ShuffleMetrics,
    SQLAnalysis,
    TableScanMetrics,
)
from ..utils import format_bytes

# =============================================================================
# Context — snapshot of all inputs needed by any card's detect/build
# =============================================================================


@dataclass
class Context:
    """Inputs available to every CardDef.

    Constructed once at the top of ``generate_action_cards`` and passed
    by reference to each card's ``detect`` and ``build``. Pure data —
    no side effects.

    Precomputed fields (``filter_columns``, ``llm_clustering_result``,
    ``lc_target_table_override``) are populated by the host before
    ``generate_from_registry`` is called, so ``build`` functions for
    cards that depend on those states (currently low_file_pruning)
    can access them uniformly.
    """

    indicators: BottleneckIndicators
    query_metrics: QueryMetrics
    hot_operators: list[OperatorHotspot] = field(default_factory=list)
    shuffle_metrics: list[ShuffleMetrics] = field(default_factory=list)
    join_info: list[JoinInfo] = field(default_factory=list)
    sql_analysis: SQLAnalysis | None = None
    top_scanned_tables: list[TableScanMetrics] | None = None
    llm_clustering_config: dict | None = None
    is_serverless: bool = False

    # Precomputed fields for cards that depend on complex derived state.
    # The host (``generate_action_cards``) fills these in before invoking
    # ``generate_from_registry`` — kept as mutable fields (not
    # properties) so the same Context instance can be handed to the
    # legacy code path too.
    filter_columns: list[str] = field(default_factory=list)
    llm_clustering_result: dict | None = None
    lc_target_table_override: str | None = None
    # EXPLAIN EXTENDED result (``ExplainExtended`` from explain_parser).
    # Populated when the user attached EXPLAIN. Used for type-based
    # and per-column-stats cardinality classification (v5.16.17 A+C+D).
    explain_analysis: Any = None

    @property
    def table_names(self) -> list[str]:
        if not self.sql_analysis or not self.sql_analysis.tables:
            return []
        return [t.full_name or t.table for t in self.sql_analysis.tables if t.full_name or t.table]

    @property
    def primary_table(self) -> str:
        names = self.table_names
        return names[0] if names else "<table_name>"

    @property
    def lc_target_table(self) -> str:
        """Target table used for Liquid Clustering recommendations.

        Priority: LC LLM override > top-scanned table (highest
        bytes_read) > primary table from SQL parse. Mirrors the legacy
        computation at the top of ``generate_action_cards``.
        """
        if self.lc_target_table_override:
            return self.lc_target_table_override
        if self.top_scanned_tables:
            return self.top_scanned_tables[0].table_name or self.primary_table
        return self.primary_table

    @property
    def lc_target_table_norm(self) -> str:
        from .recommendations import normalize_table_ref

        return normalize_table_ref(self.lc_target_table)

    def _lookup_col_type_and_stats(self, table_name: str, col: str):
        """Return ``(col_type, distinct_count)`` from explain_analysis."""
        col_type = None
        distinct_count = None
        ea = self.explain_analysis
        if ea is not None:
            # Type from ReadSchema (always populated when EXPLAIN attached)
            schemas = getattr(ea, "scan_schemas", None) or {}
            col_type = schemas.get(table_name, {}).get(col)
            # Per-column stats from ANALYZE TABLE FOR ALL COLUMNS
            stats = getattr(ea, "scan_column_stats", None) or {}
            cs = stats.get(table_name, {}).get(col)
            if cs is not None:
                distinct_count = cs.distinct_count
        return col_type, distinct_count

    def cluster_class_for_ts(self, ts, col: str) -> str:
        """Cardinality class for ``col`` in the context of scan table ``ts``.

        Priority: stats (from EXPLAIN) > bounds > type > name. Used by
        ``_hier_candidates`` so Hierarchical Clustering detection picks
        up low-cardinality keys even when the name heuristic misses
        (e.g. ``MYCLOUD_STARTMONTH`` / ``MYCLOUD_STARTYEAR``).
        """
        from ..extractors import estimate_clustering_key_cardinality

        cached = ts.clustering_key_cardinality.get(col, "unknown")
        col_type, distinct_count = self._lookup_col_type_and_stats(ts.table_name or "", col)
        rows = ts.rows_scanned or 0
        recomputed = estimate_clustering_key_cardinality(
            col, None, None, rows, col_type=col_type, distinct_count=distinct_count
        )
        # If extra signals (type/stats) upgrade from "unknown" to a
        # definitive class, prefer that. Otherwise keep the cached
        # (bounds-aware) classification.
        if recomputed in ("low", "high") and cached == "unknown":
            return recomputed
        return cached if cached != "unknown" else recomputed

    def cluster_class_for(self, col: str) -> str:
        """Cardinality class ('low' / 'high' / 'unknown') for a column.

        Matches the legacy ``_cluster_class_for_recommended_column``
        closure. Prioritizes stats/type/bounds/name in descending
        confidence. Used for recommended (not necessarily already-LC)
        columns — selects the best-matching scan table.
        """
        from ..extractors import estimate_clustering_key_cardinality
        from .recommendations import normalize_table_ref

        tsm_match = None
        if self.top_scanned_tables:
            target_norm = self.lc_target_table_norm
            for tt in self.top_scanned_tables:
                if normalize_table_ref(tt.table_name) == target_norm:
                    tsm_match = tt
                    break
            if tsm_match is None:
                tsm_match = self.top_scanned_tables[0]
        if tsm_match:
            return self.cluster_class_for_ts(tsm_match, col)
        return estimate_clustering_key_cardinality(col, None, None, 0)


# =============================================================================
# CardDef — registry entry for one rule-based ActionCard kind
# =============================================================================


@dataclass(frozen=True)
class CardDef:
    """Self-contained card description — one entry per rule-based card.

    ``build`` returns ``list[ActionCard]`` (not a single ActionCard)
    because some cards generate multiple instances (e.g., one per
    Photon blocker or per hot scan operator).
    """

    card_id: str
    priority_rank: int
    detect: Callable[[Context], bool]
    build: Callable[[Context], list[ActionCard]]


# =============================================================================
# Pilot card implementations (v5.16.11)
# =============================================================================


def _priority_from_rank(rank: int) -> float:
    """Convert priority_rank to the legacy priority_score scale.

    Phase 2e (v5.16.19): ``priority_rank`` is now the authoritative
    ordering value on ``ActionCard``. This helper is retained so each
    ``build`` function can populate the legacy ``priority_score`` field
    (float) for any consumer that still reads it.
    """
    return rank / 10.0


def _assign_priority(card, rank: int, boost: float = 0.0) -> None:
    """Set both ``priority_rank`` (authoritative) and ``priority_score``
    (legacy float) on a card. Use this instead of setting
    ``priority_score`` directly so new code can be rank-first."""
    card.priority_rank = rank
    card.priority_score = _priority_from_rank(rank) + boost


# ---------------------------------------------------------------------------
# 100  disk_spill
# ---------------------------------------------------------------------------


def _detect_disk_spill(ctx: Context) -> bool:
    return bool(ctx.indicators.spill_bytes and ctx.indicators.spill_bytes > 0)


def _build_disk_spill(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    spill_gb = bi.spill_bytes / 1024**3
    size_str = f"{spill_gb:.2f} GB" if spill_gb >= 0.1 else format_bytes(bi.spill_bytes)

    evidence: list[str] = [_("Spill to disk: {size}").format(size=size_str)]
    if bi.spill_operators:
        top = bi.spill_operators[0]
        evidence.append(
            _("Top spill operator: Node #{nid} ({name}) — {share:.1f}% of spill").format(
                nid=top.node_id,
                name=top.node_name[:40],
                share=top.spill_share_percent,
            )
        )

    impact = "high" if spill_gb >= THRESHOLDS["spill_high_gb"] else "medium"

    card = ActionCard(
        problem=_("I/O delay due to disk spill"),
        evidence=evidence,
        likely_cause=_(
            "Operator memory is insufficient for the working set. Spill writes "
            "intermediate data to disk and re-reads it, typically causing 2-5x "
            "slowdown on affected tasks."
        ),
        fix=_(
            "Reduce memory pressure: increase broadcast threshold to avoid "
            "shuffling large joins, enable AQE skew handling, or scale up the "
            "warehouse. Verify via the Spill operator graph that the target node "
            "stops spilling after the change."
        ),
        fix_sql=(
            "-- " + _("Increase broadcast threshold (avoids large shuffles)") + "\n"
            "SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB\n\n"
            "-- " + _("Enable AQE skew handling") + "\n"
            "SET spark.sql.adaptive.skewJoin.enabled = true;\n"
        ),
        expected_impact=impact,
        effort="low",
        validation_metric="spill_to_disk_bytes = 0",
        risk="low",
        risk_reason=_("Setting changes are reversible; validate on dev first"),
        verification_steps=[
            {"metric": "spill_to_disk_bytes", "expected": "0"},
            {"metric": "total_time_ms", "expected": _("Reduced")},
        ],
        root_cause_group="spill_memory_pressure",
        coverage_category="MEMORY",
        severity="HIGH" if impact == "high" else "MEDIUM",
    )
    _assign_priority(card, 100)
    return [card]


# ---------------------------------------------------------------------------
# 95  shuffle_dominant
# ---------------------------------------------------------------------------


def _detect_shuffle_dominant(ctx: Context) -> bool:
    return (ctx.indicators.shuffle_impact_ratio or 0) > 0.2


def _build_shuffle_dominant(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    ratio_pct = (bi.shuffle_impact_ratio or 0) * 100
    impact = "high" if (bi.shuffle_impact_ratio or 0) >= 0.4 else "medium"

    evidence = [
        _("Shuffle accounts for {pct:.1f}% of total task time").format(pct=ratio_pct),
    ]
    if ctx.shuffle_metrics:
        sm = max(ctx.shuffle_metrics, key=lambda s: s.peak_memory_bytes or 0)
        evidence.append(
            _("Largest shuffle: Node #{nid} ({mpp:.0f} MB/partition)").format(
                nid=sm.node_id, mpp=sm.memory_per_partition_mb
            )
        )

    # Serverless: SET spark.sql.autoBroadcastJoinThreshold is not
    # available, so recommend CTE pre-filter + BROADCAST hint + REPARTITION
    # hint rewrites instead.
    small_table = ctx.table_names[1] if len(ctx.table_names) > 1 else "<small_table>"
    if ctx.is_serverless:
        fix_sql_lines = [
            f"-- {_('Pre-aggregate or filter in CTE to reduce shuffle data volume')}",
            "WITH filtered AS (",
            f"  SELECT * FROM {ctx.primary_table}",
            f"  WHERE <filter_condition>  -- {_('Add early filter to reduce data volume')}",
            ")",
            f"SELECT /*+ BROADCAST({small_table}) */ *",
            "FROM filtered",
            f"JOIN {small_table} ON ...;",
            "",
            f"-- {_('Or use REPARTITION hint to optimize shuffle')}",
            "SELECT /*+ REPARTITION(200, join_key) */ *",
            f"FROM {ctx.primary_table} JOIN {small_table} ON ...;",
            "",
            f"-- {_('Avoid unnecessary DISTINCT — use UNION ALL instead of UNION if duplicates are acceptable')}",
        ]
        fix_text = _("Use CTE pre-filtering, BROADCAST hint, or REPARTITION to reduce shuffle")
    else:
        fix_sql_lines = [
            "-- " + _("Increase broadcast threshold"),
            "SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB",
            "",
            "-- " + _("Or use BROADCAST hint for small tables"),
            f"SELECT /*+ BROADCAST({small_table}) */ *",
            f"FROM {ctx.primary_table}",
            f"JOIN {small_table} ON ...;",
            "",
            "-- " + _("Enable AQE skew handling"),
            "SET spark.sql.adaptive.skewJoin.enabled = true;",
        ]
        fix_text = _(
            "Expand broadcast threshold for small-side joins, or use REPARTITION "
            "hint to match the dominant partitioning key. Verify via shuffle "
            "metrics that total shuffle bytes written decreases."
        )

    card = ActionCard(
        problem=_("Shuffle operations are dominant"),
        evidence=evidence,
        likely_cause=_("Joins between large tables or insufficient broadcast threshold"),
        fix=fix_text,
        fix_sql="\n".join(fix_sql_lines),
        expected_impact=impact,
        effort="low",
        validation_metric="shuffle_impact_ratio < 20%",
        risk="low",
        risk_reason=_("BROADCAST hint is safe for small tables; verify table size first"),
        verification_steps=[
            {"metric": "shuffle_impact_ratio", "expected": "< 20%"},
            {"metric": "total_time_ms", "expected": _("Significant reduction")},
        ],
        root_cause_group="shuffle_overhead",
        coverage_category="PARALLELISM",
        severity="HIGH" if impact == "high" else "MEDIUM",
    )
    _assign_priority(card, 95)
    return [card]


# ---------------------------------------------------------------------------
# 75  low_cache
# ---------------------------------------------------------------------------


def _detect_low_cache(ctx: Context) -> bool:
    # Same multi-gate as legacy: cache_hit_ratio low AND scan_impact at
    # least in the mid-band. Prevents compute-bound queries from
    # surfacing a cache recommendation that cannot move the needle.
    return (
        ctx.indicators.cache_hit_ratio < 0.3
        and ctx.indicators.scan_impact_ratio >= THRESHOLDS["scan_impact_mid"]
    )


def _build_low_cache(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    qm = ctx.query_metrics
    demoted = bi.scan_impact_ratio < THRESHOLDS["scan_impact_dominant"]

    evidence = [_("Cache hit ratio: {ratio}").format(ratio=f"{bi.cache_hit_ratio:.1%}")]
    if qm.read_bytes > 0:
        evidence.append(_("Read data size: {size}").format(size=format_bytes(qm.read_bytes)))

    if demoted:
        impact = "low"
    else:
        impact = "medium" if qm.read_bytes > 100 * 1024**2 else "low"

    card = ActionCard(
        problem=_("Low cache hit ratio"),
        evidence=evidence,
        likely_cause=_("First execution, insufficient cache size, or cluster restart"),
        fix=_("Verify cache effect by re-running same query, or scale up"),
        expected_impact=impact,
        effort="medium",
        validation_metric="cache_hit_ratio >= 30%",
        risk="low",
        risk_reason=_("Cache behavior depends on cluster state; re-run to confirm"),
        verification_steps=[
            {"metric": "cache_hit_ratio", "expected": ">= 30%"},
            {"metric": "bytes_read_from_cache_percentage", "expected": _("> 30% on re-run")},
        ],
        root_cause_group="cache_utilization",
        coverage_category="COMPUTE",
        severity="MEDIUM" if impact != "low" else "LOW",
    )
    _assign_priority(card, 75)
    return [card]


# ---------------------------------------------------------------------------
# 70  photon_blocker — per-blocker dynamic emission from EXPLAIN EXTENDED
# ---------------------------------------------------------------------------


def _detect_photon_blocker(ctx: Context) -> bool:
    return bool(ctx.indicators.photon_blockers)


def _build_photon_blocker(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    cards: list[ActionCard] = []
    for blocker in bi.photon_blockers or []:
        evidence = [
            _("Photon utilization: {ratio}").format(ratio=f"{bi.photon_ratio:.1%}"),
            _("Unsupported operation: {reason}").format(reason=blocker.reason),
        ]
        if blocker.unsupported_expression:
            expr = blocker.unsupported_expression[:80]
            if len(blocker.unsupported_expression) > 80:
                expr += "..."
            evidence.append(_("Expression: {expr}").format(expr=expr))
        if blocker.detail_message:
            evidence.append(_("Detail: {detail}").format(detail=blocker.detail_message))

        impact = blocker.impact.lower() if blocker.impact else "medium"

        card = ActionCard(
            problem=_("Photon-unsupported operation: {reason}").format(reason=blocker.reason),
            evidence=evidence,
            likely_cause=blocker.detail_message or _("This operation is not supported by Photon"),
            fix=blocker.action or _("Consider query rewrite for Photon compatibility"),
            fix_sql=blocker.sql_rewrite_example or "",
            expected_impact=impact,
            effort="medium" if blocker.sql_rewrite_example else "low",
            validation_metric="photon_ratio >= 80%",
            risk="medium",
            risk_reason=_("Rewriting SQL may change query semantics; verify results"),
            verification_steps=[
                {"metric": "photon_ratio", "expected": ">= 80%"},
            ],
            root_cause_group="photon_compatibility",
            coverage_category="COMPUTE",
            severity="HIGH" if impact == "high" else "MEDIUM",
        )
        # Legacy: impact*4/effort (with SQL example) or impact*3/effort.
        # Our static rank 70 (=7.0) lands near that range; bump when an
        # SQL example is attached so concrete rewrites outrank generic.
        boost = 0.5 if blocker.sql_rewrite_example else 0.0
        _assign_priority(card, 70, boost=boost)
        cards.append(card)
    return cards


# ---------------------------------------------------------------------------
# 68  photon_low (generic Photon utilization card — fires only when no
#     specific blockers are detected)
# ---------------------------------------------------------------------------


def _detect_photon_low(ctx: Context) -> bool:
    # Photon blocker path takes precedence; generic photon_low fires
    # only when no specific blockers are detected AND the query is
    # non-trivial. Serverless and non-serverless both flow through —
    # the build function branches on ``ctx.is_serverless`` for the SQL
    # template (SHUFFLE_HASH/BROADCAST/EXISTS rewrite vs. plain SET).
    return (
        not (ctx.indicators.photon_blockers or [])
        and ctx.indicators.photon_ratio < 0.8
        and ctx.query_metrics.task_total_time_ms >= THRESHOLDS["photon_tiny_query_ms"]
    )


def _build_photon_low(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    qm = ctx.query_metrics
    demoted = qm.task_total_time_ms < THRESHOLDS["photon_small_query_ms"]

    if demoted:
        impact = "low"
    else:
        impact = "high" if bi.photon_ratio < 0.5 else "medium"

    smj_joins = [j for j in ctx.join_info if j.join_type.name == "SORT_MERGE"]
    evidence = [_("Photon utilization: {ratio}").format(ratio=f"{bi.photon_ratio:.1%}")]
    if smj_joins:
        evidence.append(_("Sort-Merge joins: {count} places").format(count=len(smj_joins)))

    other_table = ctx.table_names[1] if len(ctx.table_names) > 1 else "<other_table>"

    # Serverless: SET spark.sql.join.preferSortMergeJoin is not available
    # → recommend SHUFFLE_HASH hint + CTE pre-filter + EXISTS rewrite.
    if ctx.is_serverless:
        photon_fix_sql = (
            f"-- {_('Run EXPLAIN EXTENDED to identify specific Photon blockers')}\n"
            "-- EXPLAIN EXTENDED <your_query>\n"
            "\n"
            f"-- {_('Use SHUFFLE_HASH hint to avoid Sort-Merge Join')}\n"
            f"SELECT /*+ SHUFFLE_HASH({ctx.primary_table}) */ *\n"
            f"FROM {ctx.primary_table}\n"
            f"JOIN {other_table} ON ...;\n"
            "\n"
            f"-- {_('Or pre-filter in CTE to make table small enough for BROADCAST')}\n"
            "WITH filtered AS (\n"
            f"  SELECT * FROM {ctx.primary_table}\n"
            f"  WHERE <filter_condition>  -- {_('Reduce data volume')}\n"
            ")\n"
            f"SELECT /*+ BROADCAST(filtered) */ *\n"
            f"FROM filtered JOIN {other_table} ON ...;"
        )
        photon_fix_text = _(
            "Run EXPLAIN EXTENDED to identify Photon blockers, use query hints to change join type"
        )
    else:
        photon_fix_sql = (
            "-- " + _("Run EXPLAIN EXTENDED to find specific Photon blockers") + "\n"
            "SET spark.sql.join.preferSortMergeJoin = false;\n"
            "SET spark.databricks.adaptive.joinFallback = true;\n"
        )
        photon_fix_text = _(
            "Run EXPLAIN EXTENDED to identify Photon blockers, or apply the "
            "settings below to prefer SHUFFLE_HASH over SMJ."
        )

    card = ActionCard(
        problem=_("Low Photon utilization"),
        evidence=evidence,
        likely_cause=_("Using Sort-Merge joins or Photon-unsupported functions"),
        fix=photon_fix_text,
        fix_sql=photon_fix_sql,
        expected_impact=impact,
        effort="low",
        validation_metric="photon_ratio >= 80%",
        risk="low",
        risk_reason=_("Setting changes are low-risk and reversible"),
        verification_steps=[{"metric": "photon_ratio", "expected": ">= 80%"}],
        root_cause_group="photon_compatibility",
        coverage_category="COMPUTE",
        severity="MEDIUM" if impact != "low" else "LOW",
    )
    _assign_priority(card, 68)
    return [card]


# ---------------------------------------------------------------------------
# 30  rescheduled_scan
# ---------------------------------------------------------------------------


def _detect_rescheduled_scan(ctx: Context) -> bool:
    return (ctx.indicators.rescheduled_scan_ratio or 0) >= THRESHOLDS["scan_locality_critical"]


def _build_rescheduled_scan(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    ratio_pct = (bi.rescheduled_scan_ratio or 0) * 100

    evidence = [
        _("Rescheduled scan ratio: {ratio:.1f}%").format(ratio=ratio_pct),
        _("Local tasks: {loc} / Non-local (rescheduled): {nl}").format(
            loc=bi.local_scan_tasks_total, nl=bi.non_local_scan_tasks_total
        ),
    ]

    card = ActionCard(
        problem=_("High rescheduled scan ratio indicates scan locality degradation"),
        evidence=evidence,
        likely_cause=_(
            "Tasks repeatedly scheduled away from cached data blocks — usually "
            "cold cluster start, scale-out event, or autoscaler churn."
        ),
        fix=_(
            "Often infra-origin and self-resolves after warmup. If persistent, "
            "check autoscaler settings and cluster assignment stability. Look "
            "for correlation with cluster resize events."
        ),
        expected_impact="medium",
        effort="medium",
        validation_metric="rescheduled_scan_ratio < 1%",
        risk="low",
        risk_reason=_(
            "Infra-level signal; SQL-side changes rarely remediate directly. "
            "Monitor across multiple runs before acting."
        ),
        verification_steps=[{"metric": "rescheduled_scan_ratio", "expected": "< 1%"}],
        root_cause_group="scan_efficiency",
        coverage_category="DATA",
        severity="MEDIUM",
    )
    _assign_priority(card, 30)
    return [card]


# ---------------------------------------------------------------------------
# 85  data_skew
# ---------------------------------------------------------------------------


def _detect_data_skew(ctx: Context) -> bool:
    return bool(ctx.indicators.has_data_skew)


def _build_data_skew(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    skew_shuffles = [sm for sm in ctx.shuffle_metrics if sm.aqe_skewed_partitions > 0]
    evidence = [_("Skewed partition count: {count}").format(count=bi.skewed_partitions)]
    skew_keys: list[str] = []
    for sm in skew_shuffles[:2]:
        if sm.shuffle_attributes:
            skew_keys.extend(sm.shuffle_attributes)
            evidence.append(_("Skewed key: {keys}").format(keys=", ".join(sm.shuffle_attributes)))

    # Serverless: AQE settings cannot be tweaked, so recommend
    # CTE pre-aggregation + REPARTITION hint rewrites instead.
    if ctx.is_serverless:
        skew_key = skew_keys[0] if skew_keys else "<skew_key>"
        other_table = ctx.table_names[1] if len(ctx.table_names) > 1 else "<other_table>"
        fix_sql_lines = [
            f"-- {_('Pre-aggregate in CTE to reduce data volume before JOIN')}",
            "WITH pre_agg AS (",
            f"  SELECT {skew_key}, COUNT(*) AS cnt, SUM(amount) AS total",
            f"  FROM {ctx.primary_table}",
            f"  GROUP BY {skew_key}",
            ")",
            f"SELECT * FROM pre_agg JOIN {other_table} "
            f"ON pre_agg.{skew_key} = {other_table}.{skew_key};",
            "",
            f"-- {_('Alternative: Use REPARTITION hint to redistribute data')}",
            f"SELECT /*+ REPARTITION({skew_key}) */ *",
            f"FROM {ctx.primary_table} JOIN {other_table} "
            f"ON {ctx.primary_table}.{skew_key} = {other_table}.{skew_key};",
        ]
        fix_text = _("Pre-aggregate in CTE or use REPARTITION hint for key distribution")
    else:
        fix_sql_lines = [
            f"-- {_('Enable AQE skew join handling')}",
            "SET spark.sql.adaptive.skewJoin.enabled = true;",
            "SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 536870912;",
        ]
        fix_text = _("Enable AQE skew join for key distribution")

    card = ActionCard(
        problem=_("Processing imbalance due to data skew"),
        evidence=evidence,
        likely_cause=_("Imbalance in join or aggregation key value distribution"),
        fix=fix_text,
        fix_sql="\n".join(fix_sql_lines),
        expected_impact="high",
        effort="low",
        validation_metric="aqe_skewed_partitions = 0",
        risk="high" if bi.skewed_partitions > 10 else "medium",
        risk_reason=_("Skewed partitions cause task stragglers and potential OOM on executors"),
        verification_steps=[
            {"metric": "aqe_skewed_partitions", "expected": "0"},
            {"metric": "task_duration_variance", "expected": _("Even distribution across tasks")},
        ],
        root_cause_group="data_skew",
        coverage_category="PARALLELISM",
        severity="HIGH",
    )
    _assign_priority(card, 85)
    return [card]


# ---------------------------------------------------------------------------
# 45  aqe_absorbed
# ---------------------------------------------------------------------------


def _detect_aqe_absorbed(ctx: Context) -> bool:
    bi = ctx.indicators
    return (
        bool(getattr(bi, "aqe_self_repartition_seen", False))
        and bi.hash_table_resize_count >= THRESHOLDS["hash_resize_high"]
    )


def _build_aqe_absorbed(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    ratio = getattr(bi, "max_aqe_partition_growth_ratio", 0.0) or 0.0
    bytes_total = getattr(bi, "shuffle_bytes_written_total", 0) or 0
    bytes_gb = bytes_total / 1024**3

    # First hotspot column for the DDL template
    hot_col = ""
    hot_fqn = ""
    for h in bi.hash_resize_hotspots or []:
        for k in h.keys or []:
            s = str(k).strip()
            if "." in s and "↔" not in s:
                hot_fqn, hot_col = s.rsplit(".", 1)
                break
            if "↔" in s:
                left = s.split("↔")[0].strip()
                if "." in left:
                    hot_fqn, hot_col = left.rsplit(".", 1)
                    break
        if hot_col:
            break

    fix_sql_parts = [
        f"-- {_('Review data types for hot columns (DECIMAL(38,0) where INTEGER fits is expensive at this scale)')}",
        f"DESCRIBE TABLE {hot_fqn or '<table>'};",
        "",
        f"-- {_('Example: if the value is actually integer (no fractional digits), migrate to BIGINT')}",
        f"-- ALTER TABLE {hot_fqn or '<table>'} "
        f"ALTER COLUMN {hot_col or '<col>'} TYPE BIGINT;  -- dry-run in a copy first",
        "",
        f"-- {_('Cluster on the hot column(s) so the grouping/join does not require a full shuffle')}",
        f"ALTER TABLE {hot_fqn or '<table>'} CLUSTER BY ({hot_col or '<col>'});",
        f"OPTIMIZE {hot_fqn or '<table>'} FULL;",
    ]
    evidence = [
        _("Shuffle bytes written: {gb:.1f} GB").format(gb=bytes_gb),
        _("AQE self-repartitioned with growth ratio ×{ratio:.0f}").format(ratio=ratio),
        _("Shuffle spill: 0 (AQE handled the volume at runtime)"),
    ]

    card = ActionCard(
        problem=_("Large shuffle absorbed by AQE — improve physical layout + review data types"),
        evidence=evidence,
        likely_cause=_(
            "AQE self-triggered a ×{ratio:.0f} repartition at runtime because the "
            "initial partition count was too coarse for the data volume, and no "
            "shuffle spilled. That rules out key skew (AQE handles skew separately) "
            "and rules out memory pressure. The sustainable causes are: "
            "(a) the table is not clustered on the hot column — every run has to "
            "re-shuffle {gb:.1f} GB; (b) data types may be oversized — DECIMAL(38,0) "
            "keys are ~2x-5x more expensive than BIGINT at this volume (hash, "
            "compare, row memory). Even without schema access the toolkit asks you "
            "to DESCRIBE and review, because at > 10 GB shuffle the impact is "
            "always meaningful"
        ).format(ratio=ratio, gb=bytes_gb),
        fix=_(
            "1) Apply Liquid Clustering on the hot column(s) to eliminate the "
            "full shuffle; 2) DESCRIBE the table and review each hot column's "
            "type — common wastes are DECIMAL(38,0) where INTEGER fits, STRING "
            "for numeric/date values, oversized VARCHAR; 3) pre-aggregate "
            "upstream if the query runs repeatedly"
        ),
        fix_sql="\n".join(fix_sql_parts),
        expected_impact="high",
        effort="low",
        validation_metric=_(
            "Shuffle bytes written drop substantially; AQE self-repartition count "
            "returns to 0 (or drops significantly); hash_table_resize_count decreases"
        ),
        risk="low",
        risk_reason=_(
            "Liquid Clustering is non-destructive (ALTER + OPTIMIZE FULL rewrites "
            "files in place). Data-type migration must be tested on a copy first"
        ),
        verification_steps=[
            {"metric": "shuffle_bytes_written_total", "expected": "significantly lower"},
            {"metric": "aqe_self_repartition_count", "expected": "0 or dropped"},
        ],
        root_cause_group="shuffle_overhead",
        coverage_category="DATA",
        severity="HIGH",
    )
    _assign_priority(card, 45)
    return [card]


# ---------------------------------------------------------------------------
# 35  stats_fresh
# ---------------------------------------------------------------------------


def _detect_stats_fresh(ctx: Context) -> bool:
    bi = ctx.indicators
    return (
        bool(getattr(bi, "statistics_confirmed_fresh", False))
        and bi.hash_table_resize_count >= THRESHOLDS["hash_resize_high"]
    )


def _build_stats_fresh(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    # Wording mirrors the legacy card verbatim — existing tests assert
    # specific substrings ("Do NOT", alternative-cause order).
    card = ActionCard(
        problem=_("Statistics are up-to-date — consider alternative causes for hash resize"),
        evidence=[
            _("Hash table resize count: {count}").format(count=bi.hash_table_resize_count),
            _("EXPLAIN confirms optimizer statistics are full (all tables)"),
        ],
        likely_cause=_(
            "Hash table resize is high despite fresh optimizer statistics. "
            "Re-running ANALYZE TABLE will not help because the statistics are "
            "already fresh. The alternative root causes, in typical order of "
            "likelihood: (1) data-level row explosion from a missing filter or "
            "wrong JOIN predicate producing duplicates, (2) duplicate aggregation "
            "nodes recomputing the same GROUP BY, (3) key-value skew / heavy "
            "hitters / NULL concentration, (4) JOIN key type mismatch forcing "
            "implicit CAST, (5) hash collision from DECIMAL keys with very high "
            "precision, (6) memory pressure (spill + fallback)"
        ),
        fix=_(
            "Do NOT re-run ANALYZE TABLE. Investigate in this order: (1) verify the "
            "result row count matches expectations (row explosion check); (2) look "
            "for duplicate GROUP BY across UNION branches / un-materialized CTEs; "
            "(3) run the diagnostic SQL from the 'Investigate data distribution' "
            "card to confirm skew/NULL; (4) check JOIN key types for mismatch; "
            "(5) if none apply, enable AQE skew join handling or broadcast the "
            "smaller side"
        ),
        fix_sql="",
        expected_impact="high",
        effort="low",
        validation_metric=_("Root cause identified before applying a fix"),
        risk="low",
        risk_reason=_("Diagnostic only; no changes applied"),
        verification_steps=[],
        root_cause_group="statistics_freshness",
        coverage_category="DATA",
        severity="MEDIUM",
    )
    # Legacy applied a +2 boost ((impact*3/effort) + 2 ≈ 17.0 for high/low).
    # We match that with a larger static value here.
    _assign_priority(card, 35, boost=2.0)
    return [card]


# ---------------------------------------------------------------------------
# 90  shuffle_lc — shuffle key → Liquid Clustering candidate
# ---------------------------------------------------------------------------


def _detect_shuffle_lc(ctx: Context) -> bool:
    from .recommendations import _shuffle_keys_on_scanned_table

    return bool(
        _shuffle_keys_on_scanned_table(
            ctx.shuffle_metrics,
            ctx.sql_analysis,
            ctx.top_scanned_tables,
            ctx.lc_target_table_norm,
        )
    )


def _build_shuffle_lc(ctx: Context) -> list[ActionCard]:
    from .recommendations import _shuffle_keys_on_scanned_table, normalize_table_ref

    pairs = _shuffle_keys_on_scanned_table(
        ctx.shuffle_metrics, ctx.sql_analysis, ctx.top_scanned_tables, ctx.lc_target_table_norm
    )
    if not pairs:
        return []

    _primary_col, primary_sm = pairs[0]
    peak_gb = (primary_sm.peak_memory_bytes or 0) / 1024**3
    written_gb = (primary_sm.sink_bytes_written or 0) / 1024**3
    mpp_mb_int = int(primary_sm.memory_per_partition_mb or 0)

    sh_target_table = ctx.lc_target_table
    if ctx.top_scanned_tables:
        matched = False
        for ts in ctx.top_scanned_tables:
            if ts.table_name and normalize_table_ref(ts.table_name) == ctx.lc_target_table_norm:
                sh_target_table = ts.table_name
                matched = True
                break
        if not matched:
            sh_target_table = ctx.top_scanned_tables[0].table_name or sh_target_table

    shuffle_impact_pct = (ctx.indicators.shuffle_impact_ratio or 0) * 100
    sh_impact = "high" if (ctx.indicators.shuffle_impact_ratio or 0) >= 0.4 else "medium"

    all_shuffle_cols: list[str] = []
    seen_cols: set[str] = set()
    for col, _sm in pairs:
        key = col.lower()
        if key in seen_cols:
            continue
        seen_cols.add(key)
        all_shuffle_cols.append(col)
    cluster_keys_str = ", ".join(all_shuffle_cols[:4])

    sh_evidence = [
        _("Shuffle impact: {pct:.1f}% of total task time").format(pct=shuffle_impact_pct),
        _(
            "Top shuffle Node #{nid}: {written:.1f} GB written, peak {peak:.1f} GB ({mpp} MB/part)"
        ).format(nid=primary_sm.node_id or "?", written=written_gb, peak=peak_gb, mpp=mpp_mb_int),
        _("Shuffle partitioning key(s): {cols}").format(cols=cluster_keys_str),
    ]

    sh_fix_sql = (
        f"-- {_('Add the dominant shuffle key(s) to Liquid Clustering')}\n"
        f"ALTER TABLE {sh_target_table} CLUSTER BY ({cluster_keys_str});\n"
        f"-- {_('FULL is required to re-cluster existing data')}\n"
        f"OPTIMIZE {sh_target_table} FULL;\n\n"
        f"-- {_('If a key has very low cardinality (<10 distinct values), use Hierarchical Clustering:')}\n"
        f"-- {_('First add a high-cardinality column to CLUSTER BY, then designate the low-card column as hierarchical')}\n"
        f"ALTER TABLE {sh_target_table} CLUSTER BY (<high_card_key>, {all_shuffle_cols[0]});\n"
        f"ALTER TABLE {sh_target_table} SET TBLPROPERTIES (\n"
        f"  'delta.liquid.hierarchicalClusteringColumns' = '{all_shuffle_cols[0]}'\n"
        f");\n"
        f"OPTIMIZE {sh_target_table} FULL;  -- {_('DBR 17.1+ required')}"
    )

    card = ActionCard(
        problem=_("Shuffle-dominant: consider adding shuffle key to Liquid Clustering"),
        evidence=sh_evidence,
        likely_cause=_(
            "The dominant shuffle partitioning key is a column of a scanned "
            "table but is not currently clustered on. Clustering on the shuffle "
            "key co-locates same-value rows, shrinking shuffle volume for "
            "repeat queries with the same GROUP BY / JOIN shape."
        ),
        fix=_(
            "Add the dominant shuffle key(s) to the target table's Liquid Clustering. "
            "If cardinality is very low (<10 distinct values), combine with a "
            "higher-cardinality key or use Hierarchical Clustering."
        ),
        fix_sql=sh_fix_sql,
        expected_impact=sh_impact,
        effort="high",
        validation_metric=_("shuffle_impact_ratio reduced; shuffle bytes written shrinks"),
        risk="medium",
        risk_reason=_(
            "Clustering changes require OPTIMIZE FULL; verify on dev first. "
            "If the shuffle key is not also filtered/joined, the improvement "
            "is bounded to shuffle reduction — no pruning benefit."
        ),
        verification_steps=[
            {"metric": "shuffle_impact_ratio", "expected": _("Reduced")},
            {"metric": "shuffle_bytes_written", "expected": _("Reduced")},
        ],
        root_cause_group="shuffle_overhead",
        coverage_category="DATA",
        severity="HIGH" if sh_impact == "high" else "MEDIUM",
    )
    _assign_priority(card, 90)
    return [card]


# ---------------------------------------------------------------------------
# 40  cte_multi_ref — per-CTE dynamic
# ---------------------------------------------------------------------------


def _iter_cte_multi_refs(ctx: Context):
    """Yield (name, total_occurrences) for CTEs referenced more than once."""
    from ..sql_patterns import analyze_cte_multi_references

    sql_text = ctx.query_metrics.query_text or ""
    if not sql_text:
        return
    yield from analyze_cte_multi_references(sql_text)


def _detect_cte_multi_ref(ctx: Context) -> bool:
    return any(True for _ in _iter_cte_multi_refs(ctx))


def _build_cte_multi_ref(ctx: Context) -> list[ActionCard]:
    cards: list[ActionCard] = []
    for name, total_occ in _iter_cte_multi_refs(ctx):
        refs = total_occ - 1
        card = ActionCard(
            problem=_(
                'CTE "{name}" is referenced {n} times — Spark may re-execute it each time'
            ).format(name=name, n=total_occ),
            evidence=[_("CTE identifier occurrences in SQL: {n}").format(n=total_occ)],
            likely_cause=_(
                "Each extra reference can force repeated evaluation unless the optimizer caches "
                "inlines or AQE inserts a ReusedExchange node"
            ),
            fix=_(
                "Do NOT rely on CREATE TEMP VIEW for this — a TEMP VIEW is only a catalog alias "
                "and does not guarantee materialization. Eliminate the duplicate work by either "
                "(a) persisting the shared result with CTAS / Delta table, or (b) rewriting the "
                "query so the CTE body runs once and is joined back (e.g. pre-aggregate, then "
                "join). Confirm the optimizer reused it via a ReusedExchange node in EXPLAIN."
            ),
            expected_impact=(
                "high" if refs >= 3 and ctx.query_metrics.read_bytes > 1_000_000_000 else "medium"
            ),
            effort="medium",
            validation_metric="ReusedExchange present in EXPLAIN for this CTE; single scan",
            risk="low",
            risk_reason=_(
                "CTAS / Delta table needs cleanup; a query rewrite is reversible"
            ),
            verification_steps=[
                {"metric": "scan_count", "expected": _("Reduced re-scans")},
                {"metric": "explain_reused_exchange", "expected": _("ReusedExchange present")},
            ],
            root_cause_group="sql_pattern",
            coverage_category="QUERY",
            severity="HIGH" if refs >= 3 else "MEDIUM",
        )
        _assign_priority(card, 40)
        cards.append(card)
    return cards


# ---------------------------------------------------------------------------
# 38  investigate_dist — diagnostic SQL for hash-resize hotspots
# ---------------------------------------------------------------------------


def _investigate_targets(ctx: Context) -> list[tuple[str, str]]:
    """Pick up to 3 distinct (fqn, col) targets from hash-resize hotspots."""
    hotspots = [h for h in (ctx.indicators.hash_resize_hotspots or []) if h.keys]
    if not hotspots:
        return []
    seen: set[tuple[str, str]] = set()
    targets: list[tuple[str, str]] = []
    for h in hotspots:
        for k in h.keys or []:
            s = str(k)
            parts = [p.strip() for p in s.split("↔")] if "↔" in s else [s.strip()]
            for part in parts:
                if "." not in part:
                    continue
                bits = part.rsplit(".", 1)
                if len(bits) != 2:
                    continue
                fqn, col = bits[0], bits[1]
                if not fqn or not col:
                    continue
                key = (fqn.lower(), col.lower())
                if key in seen:
                    continue
                seen.add(key)
                targets.append((fqn, col))
                if len(targets) >= 3:
                    return targets
    return targets


def _detect_investigate_dist(ctx: Context) -> bool:
    return ctx.indicators.hash_table_resize_count >= THRESHOLDS["hash_resize_high"] and bool(
        _investigate_targets(ctx)
    )


def _build_investigate_dist(ctx: Context) -> list[ActionCard]:
    targets = _investigate_targets(ctx)
    if not targets:
        return []

    blocks: list[str] = []
    for fqn, col in targets:
        blocks.append(
            f"-- [{fqn}.{col}] " + _("Cardinality check: distinct vs total and NULL count") + "\n"
            f"SELECT\n"
            f"  COUNT(DISTINCT {col}) AS distinct_values,\n"
            f"  COUNT(*)              AS total_rows,\n"
            f"  COUNT(*) - COUNT({col}) AS null_count,\n"
            f"  ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT {col}), 0), 1) AS avg_rows_per_value\n"
            f"FROM {fqn};\n\n"
            f"-- [{fqn}.{col}] "
            + _("Top 20 values by row count (skew / heavy-hitter detection)")
            + "\n"
            f"SELECT {col}, COUNT(*) AS n\n"
            f"FROM {fqn}\n"
            f"GROUP BY 1\n"
            f"ORDER BY n DESC\n"
            f"LIMIT 20;\n"
        )
    fix_sql = "\n".join(blocks)
    evidence = [
        _("Hash table resize count: {count}").format(count=ctx.indicators.hash_table_resize_count),
        _("Top contributor columns: {cols}").format(
            cols=", ".join(f"{t[0]}.{t[1]}" for t in targets)
        ),
    ]

    card = ActionCard(
        problem=_("Investigate data distribution on hot join/grouping columns"),
        evidence=evidence,
        likely_cause=_(
            "What hash resize means: Photon doubled and rebuilt its in-memory "
            "hash table this many times because row-count estimates were wrong. "
            "Many resizes signal a data/structural issue, not just slowness. "
            "Common root causes are (a) skewed key values, (b) high NULL "
            "concentration, or (c) duplicate rows from an upstream transformation. "
            "Running these diagnostics confirms which applies before choosing "
            "between AQE skew handling, broadcast, pre-aggregation, or fixing data"
        ),
        fix=_(
            "Run the diagnostics below and compare against your expectation. "
            "If distinct_values is much lower than expected or null_count is large, "
            "the data itself is the problem — fix upstream. Otherwise proceed with "
            "skew-handling settings"
        ),
        fix_sql=fix_sql,
        expected_impact="high",
        effort="low",
        validation_metric=_("Confirm skew/NULL/duplicate rows hypothesis before applying fixes"),
        risk="low",
        risk_reason=_("Read-only diagnostics; no data or config changes"),
        verification_steps=[
            {
                "sql": f"SELECT COUNT(DISTINCT {col}), COUNT(*) FROM {fqn}",
                "expected": _("distinct_values close to expected cardinality"),
            }
            for (fqn, col) in targets[:2]
        ],
        root_cause_group="data_skew",
        coverage_category="PARALLELISM",
        severity="HIGH",
    )
    # Legacy boosted priority by +1 so investigation sits above fix cards.
    # Static base rank 38 + 1.0 boost keeps the same relative ordering.
    _assign_priority(card, 38, boost=1.0)
    return [card]


# ---------------------------------------------------------------------------
# 55  hierarchical_cluster — existing LC has low-cardinality keys
# ---------------------------------------------------------------------------


def _hier_candidates(ctx: Context) -> list[tuple[Any, str, list[str]]]:
    """Return (TableScanMetrics, low_card_key, other_keys) triples for
    tables whose current clustering includes a low-cardinality column.

    v5.16.17: uses ``ctx.cluster_class_for_ts`` which consults EXPLAIN
    per-column stats and types in addition to name heuristics and
    bounds — so names like ``MYCLOUD_STARTMONTH`` / ``MYCLOUD_STARTYEAR``
    that miss the underscore-prefix heuristic are still detected.
    """
    if not ctx.top_scanned_tables:
        return []
    out: list[tuple[Any, str, list[str]]] = []
    seen: set[str] = set()
    for ts in ctx.top_scanned_tables:
        if not ts.current_clustering_keys or ts.table_name in seen:
            continue
        lows = [c for c in ts.current_clustering_keys if ctx.cluster_class_for_ts(ts, c) == "low"]
        if not lows:
            continue
        seen.add(ts.table_name)
        low_c = lows[0]
        highs = [c for c in ts.current_clustering_keys if c != low_c]
        out.append((ts, low_c, highs))
    return out


def _detect_hier_clustering(ctx: Context) -> bool:
    return bool(_hier_candidates(ctx))


def _build_hier_clustering(ctx: Context) -> list[ActionCard]:
    cards: list[ActionCard] = []
    for ts, low_c, highs in _hier_candidates(ctx):
        # Canonical Databricks Liquid Hierarchical Clustering syntax
        # (Field Guide, 2025). Requirements:
        #   1. The hierarchical columns must ALREADY be part of the
        #      table's CLUSTER BY set.
        #   2. Only the TBLPROPERTIES form is supported — no CLUSTER BY
        #      ``WITH (HIERARCHICAL CLUSTERING (...))`` clause exists.
        #   3. Run OPTIMIZE on DBR 17.1+ to apply to existing data.
        check_line = (
            f"-- {_('First, check if Hierarchical Clustering is already configured:')}\n"
            f"SHOW TBLPROPERTIES {ts.table_name};\n"
            f"-- {_('If delta.liquid.hierarchicalClusteringColumns is already set, skip this step.')}\n\n"
        )
        # Ensure low_c (and highs) are already in CLUSTER BY — these keys
        # were detected from current_clustering_keys, so this is a no-op
        # for the target table; the ALTER here is explicit documentation.
        full_keys = [low_c] + list(highs) if highs else [low_c]
        cluster_by_line = f"ALTER TABLE {ts.table_name} CLUSTER BY ({', '.join(full_keys)});"
        fix_h = check_line + (
            f"-- {_('Step 1: Ensure the hierarchical column is part of CLUSTER BY')}\n"
            f"{cluster_by_line}\n\n"
            f"-- {_('Step 2: Designate the low-cardinality column as the hierarchical key')}\n"
            f"ALTER TABLE {ts.table_name} SET TBLPROPERTIES (\n"
            f"  'delta.liquid.hierarchicalClusteringColumns' = '{low_c}'\n"
            f");\n\n"
            f"-- {_('Step 3: Apply to existing data (DBR 17.1+ required)')}\n"
            f"OPTIMIZE {ts.table_name} FULL;"
        )

        card = ActionCard(
            problem=_("Hierarchical Clustering candidate detected"),
            evidence=[
                _("Table: {t}").format(t=ts.table_name),
                _("Low-cardinality clustering key: {c}").format(c=low_c),
            ],
            likely_cause=_(
                "Low-cardinality clustering key {c} can benefit from hierarchical clustering"
            ).format(c=low_c),
            fix=_(
                "Use Hierarchical Clustering to nest low-cardinality keys; "
                "TPC-DS style benchmarks report roughly 22-31% improvement in some scenarios. "
                "Check SHOW TBLPROPERTIES first — skip if already configured."
            ),
            fix_sql=fix_h,
            expected_impact="high",
            effort="low",
            validation_metric=_("After OPTIMIZE FULL, confirm file pruning and scan locality"),
            risk="low",
            risk_reason=_("Table properties and CLUSTER BY are reversible; validate in dev first"),
            verification_steps=[
                {"metric": "filter_rate", "expected": _("Improved or stable")},
                {"metric": "bytes_read", "expected": _("Reduced on repeated filters")},
            ],
            severity="MEDIUM",
            root_cause_group="scan_efficiency",
            coverage_category="DATA",
        )
        _assign_priority(card, 55)
        cards.append(card)
    return cards


# ---------------------------------------------------------------------------
# 50  hash_resize — 4 wording variants depending on join/group hotspots
# ---------------------------------------------------------------------------


def _detect_hash_resize(ctx: Context) -> bool:
    bi = ctx.indicators
    has_join = bool(ctx.join_info) or (
        ctx.sql_analysis and ctx.sql_analysis.structure.join_count > 0
    )
    has_group_hotspot = any((h.key_kind == "group") for h in (bi.hash_resize_hotspots or []))
    return (has_join or has_group_hotspot) and (
        bi.hash_table_resize_count >= THRESHOLDS["hash_resize_high"]
        or bi.avg_hash_probes_per_row >= THRESHOLDS["hash_probes_high"]
    )


def _build_hash_resize(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators

    evidence: list[str] = []
    if bi.hash_table_resize_count >= THRESHOLDS["hash_resize_high"]:
        evidence.append(
            _("Hash table resize count: {count}").format(count=bi.hash_table_resize_count)
        )
    if bi.avg_hash_probes_per_row >= THRESHOLDS["hash_probes_high"]:
        evidence.append(
            _("Avg hash probes per row: {probes}").format(
                probes=f"{bi.avg_hash_probes_per_row:.1f}"
            )
        )
    if bi.hash_build_time_total_ms > 0:
        evidence.append(_("Hash build time: {time}ms").format(time=bi.hash_build_time_total_ms))

    impact = (
        "high"
        if bi.hash_table_resize_count >= THRESHOLDS["hash_resize_critical"]
        or bi.avg_hash_probes_per_row >= THRESHOLDS["hash_probes_critical"]
        else "medium"
    )

    # Wording variants by hotspot kind (matches legacy behavior).
    top_kinds = [h.key_kind for h in (bi.hash_resize_hotspots or [])[:3]]
    has_join_hs = any(k == "join" for k in top_kinds)
    has_group_hs = any(k == "group" for k in top_kinds)
    if has_join_hs and has_group_hs:
        problem = _("Mitigate hash resize on JOIN and GROUP BY hot columns")
    elif has_group_hs:
        problem = _("Mitigate hash resize on hot GROUP BY columns")
    elif has_join_hs:
        problem = _("Mitigate hash resize on hot JOIN keys")
    else:
        problem = _("Mitigate hash table resize / high probe count")

    # Serverless: broadcast threshold setting not available → use
    # CTE pre-aggregation + BROADCAST hint rewrites instead.
    if ctx.is_serverless:
        other_table = ctx.table_names[1] if len(ctx.table_names) > 1 else "<other_table>"
        fix_sql = (
            f"-- {_('Pre-aggregate in CTE to reduce data volume before JOIN')}\n"
            "WITH pre_agg AS (\n"
            f"  SELECT join_key, COUNT(*) AS cnt FROM {ctx.primary_table}\n"
            f"  GROUP BY join_key\n"
            ")\n"
            f"SELECT /*+ BROADCAST(pre_agg) */ *\n"
            f"FROM pre_agg JOIN {other_table} ON pre_agg.join_key = {other_table}.join_key;"
        )
        fix_text = _(
            "For JOIN skew: pre-aggregate the build side, increase broadcast threshold, "
            "or enable AQE skew join handling. "
            "For GROUP BY skew: verify no unintended cardinality (row explosion / duplicates) "
            "and pre-aggregate upstream"
        )
    else:
        fix_sql = (
            "-- Increase broadcast threshold\n"
            "SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB\n\n"
            "-- Enable AQE skew handling\n"
            "SET spark.sql.adaptive.skewJoin.enabled = true;\n\n"
            "-- Encourage partial aggregation\n"
            "SET spark.sql.adaptive.enabled = true;"
        )
        fix_text = _(
            "For JOIN skew: increase broadcast threshold or enable AQE skew join handling. "
            "For GROUP BY skew: verify no unintended cardinality and pre-aggregate upstream"
        )

    card = ActionCard(
        problem=problem,
        evidence=evidence,
        likely_cause=_(
            "This card directly addresses the top [hash_table_resize_count] alert. "
            "Hash table resize + high probe count indicates that Photon's row-count "
            "estimate was wrong repeatedly — typically caused by skewed keys, "
            "unexpected cardinality from upstream row explosion, or memory pressure"
        ),
        fix=fix_text,
        fix_sql=fix_sql,
        expected_impact=impact,
        effort="low",
        validation_metric="hash_table_resize_count < 100, avg_hash_probes_per_row < 10",
        risk="low",
        risk_reason=_("Setting changes are reversible; broadcast threshold increase is safe"),
        verification_steps=[
            {"metric": "hash_table_resize_count", "expected": "< 100"},
            {"metric": "avg_hash_probes_per_row", "expected": "< 10"},
        ],
        root_cause_group="spill_memory_pressure",
        coverage_category="MEMORY",
        severity="HIGH" if impact == "high" else "MEDIUM",
    )
    # Legacy boost: +0.5 over the base impact*3/effort score. We match
    # that by adding 0.5 to the static rank-derived score.
    _assign_priority(card, 50, boost=0.5)
    return [card]


# ---------------------------------------------------------------------------
# 80  low_file_pruning — LC recommendation gated on scan_impact_mid
# ---------------------------------------------------------------------------


def _detect_low_file_pruning(ctx: Context) -> bool:
    # 3-band gate mirrors the legacy bottleneck.py I/O alert gate:
    #   scan_impact >= 25% → full card
    #   10% <= scan_impact < 25% → demoted card (impact=low)
    #   scan_impact < 10% → suppressed
    return (
        ctx.indicators.filter_rate < 0.3
        and ctx.indicators.scan_impact_ratio >= THRESHOLDS["scan_impact_mid"]
    )


def _build_low_file_pruning(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    qm = ctx.query_metrics
    demoted = bi.scan_impact_ratio < THRESHOLDS["scan_impact_dominant"]

    evidence = [_("Filter efficiency: {ratio}").format(ratio=f"{bi.filter_rate:.1%}")]
    total_files = qm.read_files_count + qm.pruned_files_count
    if total_files > 0:
        evidence.append(
            _("Read file count: {read} / {total}").format(
                read=f"{qm.read_files_count:,}", total=f"{total_files:,}"
            )
        )

    # LLM clustering rationale (when the LC LLM was consulted)
    llm_res = ctx.llm_clustering_result
    if llm_res:
        if llm_res.get("rationale"):
            evidence.append(
                _("LLM recommendation: {rationale}").format(rationale=llm_res["rationale"])
            )
        if llm_res.get("workload_pattern") and llm_res["workload_pattern"] != "unknown":
            evidence.append(
                _("Workload pattern: {pattern}").format(pattern=llm_res["workload_pattern"])
            )
        if llm_res.get("confidence"):
            evidence.append(_("Confidence: {conf:.0%}").format(conf=llm_res["confidence"]))

    filter_columns = ctx.filter_columns or []
    filter_cols_str = ", ".join(filter_columns) if filter_columns else "<filter_columns>"
    target = ctx.lc_target_table

    fix_sql_lines = [
        f"-- {_('Apply Liquid Clustering (recommended)')}",
        f"ALTER TABLE {target} CLUSTER BY ({filter_cols_str});",
        f"-- {_('FULL is required to re-cluster existing data (not just new records)')}",
        f"OPTIMIZE {target} FULL;",
        "",
        f"-- {_('Z-Order (only if Liquid Clustering is not available)')}",
        f"OPTIMIZE {target} ZORDER BY ({filter_cols_str});",
    ]
    low_recommended = [c for c in filter_columns if c and ctx.cluster_class_for(c) == "low"]
    if low_recommended:
        low_c = low_recommended[0]
        highs = [c for c in filter_columns if c and c != low_c]
        # Canonical Hierarchical Clustering syntax (Databricks Liquid
        # Hierarchical Clustering Field Guide, 2025): TBLPROPERTIES
        # ``delta.liquid.hierarchicalClusteringColumns`` only. No
        # CLUSTER BY ... WITH (HIERARCHICAL CLUSTERING (...)) clause.
        full_keys = [low_c] + list(highs) if highs else [low_c]
        fix_sql_lines.extend(
            [
                "",
                f"-- {_('Hierarchical Clustering (low-cardinality clustering key)')}",
            ]
        )
        if not highs:
            fix_sql_lines.append(
                f"-- {_('Add a high-cardinality CLUSTER BY column alongside the low-cardinality key')}"
            )
        fix_sql_lines.append(f"ALTER TABLE {target} CLUSTER BY ({', '.join(full_keys)});")
        fix_sql_lines.append(f"ALTER TABLE {target} SET TBLPROPERTIES (")
        fix_sql_lines.append(f"  'delta.liquid.hierarchicalClusteringColumns' = '{low_c}'")
        fix_sql_lines.append(");")
        fix_sql_lines.append(f"OPTIMIZE {target} FULL;")

    card = ActionCard(
        problem=_("Low file pruning efficiency"),
        evidence=evidence,
        likely_cause=_(
            "Liquid Clustering not configured, or clustering keys mismatch with filter conditions"
        ),
        fix=_("Apply Liquid Clustering (Z-Order only if Liquid is unavailable)"),
        fix_sql="\n".join(fix_sql_lines),
        expected_impact="low" if demoted else "medium",
        effort="high",
        validation_metric="filter_rate >= 50%",
        risk="medium",
        risk_reason=_("Clustering changes require OPTIMIZE run; may take time on large tables"),
        verification_steps=[
            {"metric": "filter_rate", "expected": ">= 50%"},
            {"metric": "pruned_files_count", "expected": _("Majority of files pruned")},
        ],
        root_cause_group="scan_efficiency",
        coverage_category="DATA",
        severity="HIGH" if not demoted else "MEDIUM",
    )
    _assign_priority(card, 80)
    return [card]


# ---------------------------------------------------------------------------
# 65  scan_hot — hot scan operator with >=30% time share
# ---------------------------------------------------------------------------


def _hot_ops_matching(ctx: Context, kind: str, require_non_photon: bool = False):
    """Return the top-3 hot_operators matching ``bottleneck_type == kind``
    and time_share >= 30%. For ``kind == 'join'``, optionally require
    ``not op.is_photon`` (matches legacy non_photon_join gate)."""
    out = []
    for op in ctx.hot_operators[:3]:
        if op.time_share_percent < 30:
            continue
        if op.bottleneck_type != kind:
            continue
        if require_non_photon and op.is_photon:
            continue
        out.append(op)
    return out


def _detect_scan_hot(ctx: Context) -> bool:
    return bool(_hot_ops_matching(ctx, "scan"))


def _build_scan_hot(ctx: Context) -> list[ActionCard]:
    cards: list[ActionCard] = []
    select_columns: list[str] = []
    if ctx.sql_analysis and ctx.sql_analysis.columns:
        select_columns = list(
            {
                c.column_name
                for c in ctx.sql_analysis.columns
                if c.context == "select" and c.column_name
            }
        )[:5]
    cols_str = ", ".join(select_columns) if select_columns else "col1, col2, col3"

    for op in _hot_ops_matching(ctx, "scan"):
        fix_sql_lines = [
            f"-- {_('Avoid SELECT * - specify only needed columns')}",
            f"SELECT {cols_str}",
            f"FROM {ctx.primary_table}",
            f"WHERE <partition_column> = <value>;  -- {_('Add partition filter')}",
        ]
        evidence = [
            _("Execution time share: {share}%").format(share=f"{op.time_share_percent:.1f}"),
            _("Operator: {op}").format(op=op.node_name[:50]),
        ]
        card = ActionCard(
            problem=_("Scan operation accounts for {share}% of total time").format(
                share=f"{op.time_share_percent:.0f}"
            ),
            evidence=evidence,
            likely_cause=_("Large data read or insufficient column pruning"),
            fix=_("SELECT only required columns or add partition filter"),
            fix_sql="\n".join(fix_sql_lines),
            expected_impact="high",
            effort="low",
            validation_metric=_("time_share of Node {id} < 20%").format(id=op.node_id),
            risk="low",
            risk_reason=_("Column selection and filter changes are safe and reversible"),
            verification_steps=[
                {"metric": _("time_share of scan node"), "expected": "< 20%"},
            ],
            root_cause_group="scan_efficiency",
            coverage_category="DATA",
            severity="HIGH",
        )
        _assign_priority(card, 65)
        cards.append(card)
    return cards


# ---------------------------------------------------------------------------
# 60  non_photon_join — hot non-Photon join operator with >=30% time share
# ---------------------------------------------------------------------------


def _detect_non_photon_join(ctx: Context) -> bool:
    return bool(_hot_ops_matching(ctx, "join", require_non_photon=True))


def _build_non_photon_join(ctx: Context) -> list[ActionCard]:
    cards: list[ActionCard] = []
    other_table = ctx.table_names[1] if len(ctx.table_names) > 1 else "<other_table>"
    for op in _hot_ops_matching(ctx, "join", require_non_photon=True):
        evidence = [
            _("Execution time share: {share}%").format(share=f"{op.time_share_percent:.1f}"),
            _("Operator: {op}").format(op=op.node_name[:50]),
            "Photon: X",
        ]
        # Serverless: SET spark.sql.join.preferSortMergeJoin is not
        # available, so recommend query hints (SHUFFLE_HASH, BROADCAST)
        # + EXISTS rewrite patterns instead.
        if ctx.is_serverless:
            join_fix_sql = (
                f"-- {_('Use SHUFFLE_HASH hint to change join type')}\n"
                f"SELECT /*+ SHUFFLE_HASH({ctx.primary_table}) */ *\n"
                f"FROM {ctx.primary_table} JOIN {other_table} ON ...;\n"
                "\n"
                f"-- {_('Or pre-filter in CTE to reduce table size for BROADCAST')}\n"
                "WITH filtered AS (\n"
                f"  SELECT * FROM {ctx.primary_table} WHERE <filter_condition>\n"
                ")\n"
                f"SELECT /*+ BROADCAST(filtered) */ * FROM filtered JOIN ...;\n"
                "\n"
                f"-- {_('Consider using EXISTS for semi-join patterns')}\n"
                f"SELECT * FROM {ctx.primary_table} t1\n"
                f"WHERE EXISTS (SELECT 1 FROM {other_table} t2 WHERE t1.key = t2.key);"
            )
            join_fix_text = _("Use query hints (SHUFFLE_HASH/BROADCAST) or rewrite JOIN pattern")
        else:
            join_fix_sql = "SET spark.sql.join.preferSortMergeJoin = false;"
            join_fix_text = _("Change join type (Hash Join recommended)")

        card = ActionCard(
            problem=_("Non-Photon join accounts for {share}% of total time").format(
                share=f"{op.time_share_percent:.0f}"
            ),
            evidence=evidence,
            likely_cause=_("Sort-Merge join or unsupported join type"),
            fix=join_fix_text,
            fix_sql=join_fix_sql,
            expected_impact="high",
            effort="low",
            validation_metric=_("Photon execution on Node {id}").format(id=op.node_id),
            risk="low",
            risk_reason=_("Join type setting is reversible"),
            verification_steps=[
                {"metric": "photon_ratio", "expected": ">= 80%"},
            ],
            root_cause_group="photon_compatibility",
            coverage_category="COMPUTE",
            severity="HIGH",
        )
        _assign_priority(card, 60)
        cards.append(card)
    return cards


# ---------------------------------------------------------------------------
# 72  compilation_overhead — driver-side compile/prune dominates wall clock
# ---------------------------------------------------------------------------


def _detect_compilation_overhead(ctx: Context) -> bool:
    bi = ctx.indicators
    return bool(bi.compilation_pruning_heavy and bi.compilation_time_ratio >= 0.30)


def _build_compilation_overhead(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    qm = ctx.query_metrics
    ratio_pct = bi.compilation_time_ratio * 100
    compile_s = qm.compilation_time_ms / 1000
    metadata_s = qm.metadata_time_ms / 1000
    pruned = qm.pruned_files_count
    impact = "high" if bi.compilation_time_ratio >= 0.50 else "medium"

    evidence = [
        _("Compilation time: {secs:.1f}s ({pct:.0f}% of total)").format(
            secs=compile_s, pct=ratio_pct
        ),
    ]
    if metadata_s >= 0.5:
        evidence.append(_("Metadata resolution: {secs:.1f}s").format(secs=metadata_s))
    if pruned >= 1000:
        evidence.append(_("Files pruned: {n:,}").format(n=pruned))

    table = ctx.lc_target_table or "<table>"
    fix_sql = (
        f"-- {_('Compact small files to shorten metadata and pruning work')}\n"
        f"OPTIMIZE {table};\n\n"
        f"-- {_('Shorten Delta transaction log (requires retention period review)')}\n"
        f"VACUUM {table} RETAIN 168 HOURS;\n\n"
        f"-- {_('Enable auto compaction / predictive optimization if available')}\n"
        "ALTER TABLE " + table + " SET TBLPROPERTIES (\n"
        "  'delta.autoOptimize.optimizeWrite' = 'true',\n"
        "  'delta.autoOptimize.autoCompact' = 'true'\n"
        ");\n"
    )

    card = ActionCard(
        problem=_("Compilation and file pruning dominate execution"),
        evidence=evidence,
        likely_cause=_(
            "Driver-side work (SQL parse, Catalyst, Delta log replay, file-stats "
            "pruning) is large relative to execution. Typical root causes: "
            "small-file proliferation, long-unvacuumed Delta log, cold warehouse "
            "cache, or excessive partitioning."
        ),
        fix=_(
            "Compact files with OPTIMIZE, shorten the Delta log via VACUUM, "
            "enable Predictive Optimization, and re-run to rule out cold cache. "
            "Review partition design if partitionCount is very high."
        ),
        fix_sql=fix_sql,
        expected_impact=impact,
        effort="low",
        validation_metric="compilation_time_ratio < 30%",
        risk="low",
        risk_reason=_("OPTIMIZE/VACUUM are standard Delta maintenance operations"),
        verification_steps=[
            {"metric": "compilation_time_ms", "expected": _("Reduced")},
            {"metric": "metadata_time_ms", "expected": _("Reduced")},
        ],
        root_cause_group="compilation_overhead",
        coverage_category="QUERY",
        severity="HIGH" if impact == "high" else "MEDIUM",
    )
    card.priority_score = _priority_from_rank(72)
    return [card]


# ---------------------------------------------------------------------------
# 32  driver_overhead — queue wait + scheduling + waiting-for-compute
# ---------------------------------------------------------------------------


def _detect_driver_overhead(ctx: Context) -> bool:
    sev = ctx.indicators.driver_overhead_severity
    return getattr(sev, "value", "ok") != "ok"


def _build_driver_overhead(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    queue_s = bi.queue_wait_ms / 1000
    sched_s = bi.scheduling_compute_wait_ms / 1000
    total_s = bi.driver_overhead_ms / 1000
    pct = bi.driver_overhead_ratio * 100
    impact = "high" if getattr(bi.driver_overhead_severity, "value", "") == "high" else "medium"

    evidence = [
        _("Driver overhead: {total:.1f}s ({pct:.0f}% of total)").format(total=total_s, pct=pct),
    ]
    qm = ctx.query_metrics
    if queue_s >= 0.1:
        prov = qm.queued_provisioning_time_ms / 1000
        ovl = qm.queued_overload_time_ms / 1000
        evidence.append(
            _("Queue wait: {q:.1f}s (provisioning={p:.1f}s, overload={o:.1f}s)").format(
                q=queue_s, p=prov, o=ovl
            )
        )
    if sched_s >= 0.1:
        evidence.append(_("Scheduling + waiting-for-compute: {s:.1f}s").format(s=sched_s))

    queue_dominant = queue_s >= sched_s
    if queue_dominant:
        likely_cause = _(
            "Query waited in the warehouse queue before compilation started. "
            "Provisioning-heavy waits indicate Serverless cold start; "
            "overload-heavy waits indicate concurrent-query pressure."
        )
        fix = _(
            "Reduce queue time: enable Serverless warm pools, extend the "
            "auto-stop idle timeout, or increase the warehouse max clusters. "
            "For overload-dominated waits, stagger concurrent query launches."
        )
    else:
        likely_cause = _(
            "Pre-execution driver work (task scheduling, waiting for compute "
            "to become ready) dominates. Typical causes: many concurrent "
            "queries contending on the driver, or a warehouse that has to "
            "ramp up before it can dispatch tasks."
        )
        fix = _(
            "Reduce concurrent query load on this warehouse, or pin the "
            "workload to a warehouse sized to stay warm. Verify via repeated "
            "runs that scheduling time shrinks once the warehouse is hot."
        )

    card = ActionCard(
        problem=_("Driver-side wait dominates query time"),
        evidence=evidence,
        likely_cause=likely_cause,
        fix=fix,
        fix_sql="",  # No SQL remediation — warehouse/infra config only
        expected_impact=impact,
        effort="low",
        validation_metric="driver_overhead_ratio < 10%",
        risk="low",
        risk_reason=_("Warehouse configuration changes are reversible"),
        verification_steps=[
            {"metric": "queued_provisioning_time_ms", "expected": _("Reduced")},
            {"metric": "queued_overload_time_ms", "expected": _("Reduced")},
        ],
        root_cause_group="driver_overhead",
        coverage_category="COMPUTE",
        severity="HIGH" if impact == "high" else "MEDIUM",
    )
    card.priority_score = _priority_from_rank(32)
    return [card]


# =============================================================================
# CARDS registry — the Spark Perf-style ordered list
# =============================================================================


def _detect_federation_query(ctx: Context) -> bool:
    return bool(getattr(ctx.query_metrics, "is_federation_query", False))


def _build_federation_query(ctx: Context) -> list[ActionCard]:
    """Explain the federation query shape and point the reader at the
    concrete levers that matter (pushdown verification, fetchSize,
    source-side pre-aggregation). v5.18.0.
    """
    qm = ctx.query_metrics
    tables = qm.federation_tables or []
    src = (qm.federation_source_type or "").strip()
    src_label = src.upper() if src else _("external source")

    evidence: list[str] = []
    if tables:
        shown = ", ".join(tables[:3])
        if len(tables) > 3:
            shown += f" (+{len(tables) - 3})"
        evidence.append(_("Federated tables: {tables}").format(tables=shown))
    if src:
        evidence.append(_("Likely source type: {src}").format(src=src_label))
    evidence.append(
        _(
            "Scan node tag = ROW_DATA_SOURCE_SCAN_EXEC (Lakehouse Federation; "
            "data is read from the remote engine, not from Delta files)"
        )
    )

    fix_text = _(
        "Focus on pushdown: run `EXPLAIN FORMATTED` and inspect the "
        "`EXTERNAL ENGINE QUERY` / `PushedFilters` / `PushedJoins` "
        "blocks to confirm filters and joins reach the remote engine. "
        "Unsupported predicates are evaluated in Databricks after a "
        "full fetch — rewrite or split them so the heavy reduction "
        "happens on the source side. For JDBC sources set `WITH "
        "('fetchSize' 100000)` on large result sets; for BigQuery "
        "joins consider the materialization mode; for Snowflake tune "
        "`partition_size_in_mb`. Databricks-side tunings (Liquid "
        "Clustering, disk cache, shuffle partitions) generally do NOT "
        "help federated scans and are suppressed from the Top "
        "recommendations."
    )

    fix_sql = (
        f"-- {_('1. Confirm which predicates pushed down to the source')}\n"
        "EXPLAIN FORMATTED\n"
        + (f"SELECT ... FROM {tables[0]} WHERE ...;\n" if tables else "SELECT ...;\n")
        + "\n"
        + f"-- {_('2. (JDBC sources) tune batch size')}\n"
        + (
            f"SELECT * FROM {tables[0]} WITH ('fetchSize' 100000) WHERE ...;\n"
            if tables
            else "SELECT * FROM <catalog.schema.table> WITH ('fetchSize' 100000) WHERE ...;\n"
        )
        + "\n"
        + f"-- {_('3. (High-freq dashboards) materialize into Delta to avoid repeated federation cost')}\n"
        "CREATE TABLE main.analytics.<target_table> AS\n"
        + (
            f"SELECT ... FROM {tables[0]} WHERE ...;\n"
            if tables
            else "SELECT ... FROM <catalog.schema.table> WHERE ...;\n"
        )
    )

    card = ActionCard(
        problem=_(
            "Lakehouse Federation query — remote engine ({src}) is the true bottleneck"
        ).format(src=src_label),
        evidence=evidence,
        likely_cause=_(
            "Query reads via Lakehouse Federation; network transfer and "
            "remote engine execution dominate. Databricks-side storage "
            "optimizations (Liquid Clustering, disk cache, Photon "
            "compatibility) do not apply."
        ),
        fix=fix_text,
        fix_sql=fix_sql,
        expected_impact="high",
        effort="medium",
        validation_metric=_(
            "PushedFilters/PushedJoins contains the heavy predicates; "
            "bytes returned from source drops; wall-clock time drops."
        ),
        risk="low",
        risk_reason=_(
            "All three steps are safe — EXPLAIN is read-only, "
            "fetchSize is a connector hint, CTAS runs once and can be "
            "dropped if not useful."
        ),
        verification_steps=[
            {"metric": "read_remote_bytes", "expected": _("Significant reduction")},
            {"metric": "total_time_ms", "expected": _("Significant reduction")},
        ],
    )
    card.priority_score = 30.0  # between disk_spill (10) and everything-else; rank 96
    card.root_cause_group = "federation"
    card.coverage_category = "DATA"
    return [card]


# ---------------------------------------------------------------------------
# 28  cluster_underutilization — variant-aware
# ---------------------------------------------------------------------------


_CLUSTER_UNDERUTIL_VARIANT_CONFIG = {
    "external_contention": {
        "likely_cause": _(
            "Scan tasks were rescheduled to different nodes because another "
            "query saturated the local CPU slots. This loses disk-cache "
            "locality and forces remote reads — classic shared-warehouse "
            "contention."
        ),
        "fix": _(
            "Isolate this workload on its own warehouse, stagger concurrent "
            "query launches, or raise max clusters so the warehouse can "
            "scale horizontally under load."
        ),
    },
    "driver_overhead": _(
        "Many AQE re-plans, subqueries, or broadcast-hash joins force the "
        "driver to do heavy coordination between stages; executors wait on "
        "the driver instead of running tasks."
    ),
    "serial_plan": _(
        "The plan itself is topologically narrow — stages run one after the "
        "other with few tasks each — so the warehouse runs idle even "
        "without any contention."
    ),
}


def _detect_cluster_underutilization(ctx: Context) -> bool:
    return (ctx.indicators.cluster_underutilization_variant or "") != ""


def _build_cluster_underutilization(ctx: Context) -> list[ActionCard]:
    bi = ctx.indicators
    qm = ctx.query_metrics
    variant = bi.cluster_underutilization_variant
    impact = "high" if bi.cluster_underutilization_severity.value == "high" else "medium"
    exec_s = qm.execution_time_ms / 1000

    evidence: list[str] = [
        _("Effective parallelism: {par:.1f}x over {secs:.0f}s execution").format(
            par=bi.effective_parallelism, secs=exec_s
        ),
    ]
    if variant == "external_contention":
        resched_pct = (bi.rescheduled_scan_ratio or 0) * 100
        evidence.append(_("Rescheduled scan tasks: {pct:.1f}%").format(pct=resched_pct))
        if bi.cache_hit_ratio:
            evidence.append(_("Cache hit ratio: {pct:.1f}%").format(pct=bi.cache_hit_ratio * 100))
        likely_cause = _(
            "Scan tasks were rescheduled to different nodes because another "
            "query saturated the local CPU slots. This loses disk-cache "
            "locality and forces remote reads — classic shared-warehouse "
            "contention."
        )
        fix = _(
            "Isolate this workload on its own warehouse, stagger concurrent "
            "query launches, or raise max clusters so the warehouse can "
            "scale horizontally under load."
        )
    elif variant == "driver_overhead":
        evidence.append(
            _("AQE re-plans: {a}, subqueries: {s}, broadcast joins: {b}, total nodes: {n}").format(
                a=qm.aqe_replan_count,
                s=qm.subquery_count,
                b=qm.broadcast_hash_join_count,
                n=qm.total_plan_node_count,
            )
        )
        likely_cause = _(
            "Many AQE re-plans, subqueries, or broadcast-hash joins force the "
            "driver to do heavy coordination between stages; executors wait "
            "on the driver instead of running tasks."
        )
        fix = _(
            "Simplify the plan: replace multi-step subqueries with JOINs, "
            "persist multi-referenced CTE results with CTAS / Delta or "
            "rewrite to remove duplicate evaluation (a TEMP VIEW does NOT "
            "materialize them), and avoid broadcasting large sides. For "
            "Pro/Classic warehouses consider upgrading the driver node size."
        )
    else:  # serial_plan
        evidence.append(
            _("Plan appears narrow: {n} plan nodes, {s} shuffle stages").format(
                n=qm.total_plan_node_count, s=len(ctx.shuffle_metrics)
            )
        )
        likely_cause = _(
            "The plan itself is topologically narrow — stages run one after "
            "the other with few tasks each — so the warehouse runs idle even "
            "without any contention."
        )
        fix = _(
            "Widen stages with REPARTITION(n) hints on dominant scans, "
            "pre-aggregate to collapse serial stages, or review join strategy "
            "(switch SHJ → SMJ if build is oversized)."
        )

    card = ActionCard(
        problem=_("Cluster underutilized during execution"),
        evidence=evidence,
        likely_cause=likely_cause,
        fix=fix,
        fix_sql="",  # remediation is warehouse/plan level, not SQL-replacement
        expected_impact=impact,
        effort="low" if variant == "external_contention" else "medium",
        validation_metric="effective_parallelism",
        risk="low",
        risk_reason=_("Warehouse separation / REPARTITION hints are reversible"),
        verification_steps=[
            {"metric": "effective_parallelism", "expected": _("Increased")},
            {"metric": "total_time_ms", "expected": _("Reduced")},
        ],
        root_cause_group="cluster_underutilization",
        coverage_category="COMPUTE",
        severity="HIGH" if impact == "high" else "MEDIUM",
    )
    card.priority_score = _priority_from_rank(28)
    return [card]


# ---------------------------------------------------------------------------
# 25  compilation_absolute_heavy — INFO-level advisory
# ---------------------------------------------------------------------------


def _detect_compilation_absolute_heavy(ctx: Context) -> bool:
    return bool(ctx.indicators.compilation_absolute_heavy)


def _build_compilation_absolute_heavy(ctx: Context) -> list[ActionCard]:
    qm = ctx.query_metrics
    compile_s = qm.compilation_time_ms / 1000
    table = ctx.lc_target_table or "<table>"

    card = ActionCard(
        problem=_("Compilation took {secs:.1f}s (absolute threshold)").format(secs=compile_s),
        evidence=[
            _("Compilation: {secs:.1f}s").format(secs=compile_s),
            _("Pruned files: {n:,}").format(n=qm.pruned_files_count),
        ],
        likely_cause=_(
            "Driver-side compilation exceeds a normal 3-5s envelope even "
            "though it is a small fraction of total time. Long-lived tables "
            "with many small files or a long Delta log commit history "
            "typically drive this."
        ),
        fix=_(
            "Schedule routine OPTIMIZE + VACUUM maintenance on the scanned "
            "table(s). For UC-managed tables enable Predictive Optimization "
            "to automate this."
        ),
        fix_sql=(
            f"-- {_('Compact small files')}\n"
            f"OPTIMIZE {table};\n\n"
            f"-- {_('Shorten Delta log (verify retention policy first)')}\n"
            f"VACUUM {table} RETAIN 168 HOURS;\n"
        ),
        expected_impact="low",
        effort="low",
        validation_metric="compilation_time_ms",
        risk="low",
        risk_reason=_("OPTIMIZE/VACUUM are standard Delta maintenance"),
        verification_steps=[
            {"metric": "compilation_time_ms", "expected": _("Reduced")},
        ],
        root_cause_group="compilation_absolute",
        coverage_category="QUERY",
        severity="INFO",
    )
    card.priority_score = _priority_from_rank(25)
    return [card]


CARDS: tuple[CardDef, ...] = (
    CardDef("disk_spill", 100, _detect_disk_spill, _build_disk_spill),
    # v5.18.0: federation card owns the highest non-spill rank so it
    # always lands near the top of the Top-N for federated queries.
    CardDef("federation_query", 97, _detect_federation_query, _build_federation_query),
    CardDef("shuffle_dominant", 95, _detect_shuffle_dominant, _build_shuffle_dominant),
    CardDef("shuffle_lc", 90, _detect_shuffle_lc, _build_shuffle_lc),
    CardDef("data_skew", 85, _detect_data_skew, _build_data_skew),
    CardDef("low_file_pruning", 80, _detect_low_file_pruning, _build_low_file_pruning),
    CardDef("low_cache", 75, _detect_low_cache, _build_low_cache),
    CardDef(
        "compilation_overhead",
        72,
        _detect_compilation_overhead,
        _build_compilation_overhead,
    ),
    CardDef("photon_blocker", 70, _detect_photon_blocker, _build_photon_blocker),
    CardDef("photon_low", 68, _detect_photon_low, _build_photon_low),
    CardDef("scan_hot", 65, _detect_scan_hot, _build_scan_hot),
    CardDef("non_photon_join", 60, _detect_non_photon_join, _build_non_photon_join),
    CardDef("hier_clustering", 55, _detect_hier_clustering, _build_hier_clustering),
    CardDef("hash_resize", 50, _detect_hash_resize, _build_hash_resize),
    CardDef("aqe_absorbed", 45, _detect_aqe_absorbed, _build_aqe_absorbed),
    CardDef("cte_multi_ref", 40, _detect_cte_multi_ref, _build_cte_multi_ref),
    CardDef("investigate_dist", 38, _detect_investigate_dist, _build_investigate_dist),
    CardDef("stats_fresh", 35, _detect_stats_fresh, _build_stats_fresh),
    CardDef("driver_overhead", 32, _detect_driver_overhead, _build_driver_overhead),
    CardDef("rescheduled_scan", 30, _detect_rescheduled_scan, _build_rescheduled_scan),
    CardDef(
        "cluster_underutilization",
        28,
        _detect_cluster_underutilization,
        _build_cluster_underutilization,
    ),
    CardDef(
        "compilation_absolute_heavy",
        25,
        _detect_compilation_absolute_heavy,
        _build_compilation_absolute_heavy,
    ),
)


# =============================================================================
# Emission driver
# =============================================================================


# v5.18.0: Cards that are meaningless or misleading for Lakehouse
# Federation queries — these emit Liquid Clustering, disk-cache, file-
# pruning, stats-freshness, or Photon-blocker advice that all assume a
# Delta/Parquet scan. On a federated query the "scan" is an external
# engine (BigQuery, Snowflake, …) call, so the advice either can't be
# acted on or attacks the wrong layer entirely.
_FEDERATION_SUPPRESSED_CARDS: frozenset[str] = frozenset(
    {
        "shuffle_lc",
        "low_file_pruning",
        "low_cache",
        "photon_blocker",
        "scan_hot",
        "hier_clustering",
        "stats_fresh",
        "rescheduled_scan",
    }
)


def generate_from_registry(ctx: Context) -> tuple[list[ActionCard], frozenset[str]]:
    """Emit cards in priority_rank descending order.

    Spark Perf-style: iterate CARDS sorted by priority_rank, call each
    ``detect`` then ``build``, accumulate ActionCards in that order.
    Later downstream (preservation, diversity rerank, Top-N) still runs
    during Phase 1 — this function only replaces the rule-based
    emission, not the selection pipeline.

    Federation gate (v5.18.0): when ``query_metrics.is_federation_query``
    is True, cards in ``_FEDERATION_SUPPRESSED_CARDS`` are skipped
    because their advice (LC, disk cache, file pruning, stats fresh,
    Photon blockers, …) does not apply to external-source scans.

    Returns:
        ``(cards, fired_ids)`` — the emitted ActionCards, and the set of
        ``card_id``s that actually emitted at least one card. The legacy
        if-blocks use ``fired_ids`` (not ``migrated_card_ids``) so that
        a card which is registered but whose detect returned False on
        this specific context can still fall through to legacy handling
        (relevant when legacy has context-specific branches not yet
        ported — e.g. the serverless Photon card).
    """
    is_federation = bool(getattr(ctx.query_metrics, "is_federation_query", False))
    cards: list[ActionCard] = []
    fired: set[str] = set()
    for card_def in sorted(CARDS, key=lambda c: -c.priority_rank):
        if is_federation and card_def.card_id in _FEDERATION_SUPPRESSED_CARDS:
            continue
        if card_def.detect(ctx):
            new_cards = card_def.build(ctx)
            if new_cards:
                cards.extend(new_cards)
                fired.add(card_def.card_id)
    return cards, frozenset(fired)


def migrated_card_ids() -> frozenset[str]:
    """Return the set of ``card_id``s currently registered.

    Kept for introspection / tests. Legacy emission now uses the
    ``fired_ids`` set returned by :func:`generate_from_registry`.
    """
    return frozenset(c.card_id for c in CARDS)
