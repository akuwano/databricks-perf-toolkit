"""Profile-derived evidence aggregator (Codex (b) recommendation).

Computes case-independent signals from a `ProfileAnalysis` so multiple
scorers (L2 invariants, future Q3 enrichment) can share one expensive
walk over the analysis tree. Returns a small dataclass with derived
booleans and lists, NOT raw analysis fields — callers should never
re-walk node_metrics / shuffle_metrics themselves.

Signals are designed to map onto V5/V6 retention failure modes that
surfaced during the Q23 review:
- ``decimal_arithmetic_in_heavy_agg``: motivates "verify DECIMAL
  precision" recommendation. Already a rule-based card (commit 8ecebe9);
  this signal is for the L2 invariant that re-checks LLM coverage even
  if the card weren't fired.
- ``dominant_shuffle_keys_outside_lc``: motivates "consider adding
  shuffle key to Liquid Clustering". Mirrors the existing shuffle_lc
  card's intent at the analysis layer.
- ``cte_multi_reference_signals``: CTE referenced N>=2 times without
  ReusedExchange evidence.
- ``spill_dominant``: total spill > threshold; expects warehouse-size /
  CLUSTER BY hint family in remediation.

L2 invariants consume these signals; they do not regenerate them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Threshold constants — keep aligned with bottleneck.py / recommendations.py
# so a single source of truth (eventually) can drive both detector and
# invariant. For now we mirror the values in each file with a comment.
_HEAVY_AGG_PEAK_BYTES = 100 * (1024**3)  # 100 GB; mirror bottleneck.py
_DOMINANT_SHUFFLE_WRITTEN_BYTES = 10 * (1024**3)  # 10 GB
_DOMINANT_SHUFFLE_PARTITION_MB = 256  # >256 MB/partition is "dominant"
_SPILL_DOMINANT_BYTES = 100 * (1024**3)  # 100 GB cumulative spill
_CTE_MULTI_REF_THRESHOLD = 2  # 2+ external references → potential recompute


@dataclass
class ProfileEvidence:
    """Derived signals from one ProfileAnalysis.

    Each field is a boolean *or* a list of supporting tuples so an
    invariant can both decide "fire?" and quote evidence in its message.
    """

    # ---- DECIMAL arithmetic in heavy aggregate ----
    decimal_arithmetic_in_heavy_agg: bool = False
    decimal_arithmetic_examples: list[tuple[str, str]] = field(default_factory=list)

    # ---- Dominant shuffle keys not yet in Liquid Clustering ----
    dominant_shuffle_keys_outside_lc: bool = False
    dominant_shuffle_outside_lc_columns: list[tuple[str, str]] = field(
        default_factory=list
    )  # [(table, column), ...]

    # ---- CTE multi-reference (probable recompute) ----
    cte_multi_reference: bool = False
    cte_multi_reference_names: list[tuple[str, int]] = field(default_factory=list)

    # ---- Spill dominant (heavy memory pressure) ----
    spill_dominant: bool = False
    spill_total_bytes: int = 0


def _collect_decimal_arithmetic_in_heavy_agg(
    node_metrics: list[Any],
) -> list[tuple[str, str]]:
    import re
    pattern = re.compile(r"[*+\-/]")
    found: list[tuple[str, str]] = []
    for nm in node_metrics or []:
        if (getattr(nm, "peak_memory_bytes", 0) or 0) < _HEAVY_AGG_PEAK_BYTES:
            continue
        for expr in (getattr(nm, "aggregate_expressions", None) or []):
            if expr and pattern.search(expr):
                found.append((getattr(nm, "node_id", "?") or "?", expr.strip()[:120]))
                break
    return found


def _collect_dominant_shuffle_keys_outside_lc(
    shuffle_metrics: list[Any],
    sql_analysis: Any,
    top_scanned_tables: list[Any],
) -> list[tuple[str, str]]:
    """Return (table_name, shuffle_key_column) pairs where the dominant
    shuffle key is NOT among the table's current_clustering_keys.

    Borrows the column→table resolution from
    ``recommendations._shuffle_keys_on_scanned_table`` but does the
    "is column already clustered?" filter here so the caller doesn't
    have to wire LC target context.
    """
    if not shuffle_metrics or not top_scanned_tables:
        return []

    by_short_name: dict[str, Any] = {}
    by_full_name: dict[str, Any] = {}
    for ts in top_scanned_tables:
        name = (getattr(ts, "table_name", "") or "").lower()
        if not name:
            continue
        by_full_name[name] = ts
        by_short_name[name.split(".")[-1]] = ts

    alias_to_table: dict[str, str] = {}
    columns_by_name: dict[str, list] = {}
    if sql_analysis and getattr(sql_analysis, "columns", None):
        for c in sql_analysis.columns:
            cn = getattr(c, "column_name", None)
            if cn:
                columns_by_name.setdefault(cn.lower(), []).append(c)
        for t in (getattr(sql_analysis, "tables", None) or []):
            alias = getattr(t, "alias", "")
            if alias:
                alias_to_table[alias.lower()] = (
                    getattr(t, "full_name", "") or getattr(t, "table", "")
                ).lower()

    results: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for sm in shuffle_metrics:
        written = getattr(sm, "sink_bytes_written", 0) or 0
        mpp = getattr(sm, "memory_per_partition_mb", 0) or 0
        if written < _DOMINANT_SHUFFLE_WRITTEN_BYTES and mpp < _DOMINANT_SHUFFLE_PARTITION_MB:
            continue
        for attr in (getattr(sm, "shuffle_attributes", None) or []):
            if not attr:
                continue
            parts = attr.split(".")
            col_name = parts[-1].strip()
            col_key = col_name.lower()

            resolved_table = ""
            # Case A: 3+ parts → "catalog.schema.table.column" — match the
            # everything-before-last as a fully-qualified table name.
            if len(parts) >= 3:
                fqn = ".".join(parts[:-1]).lower()
                if fqn in by_full_name:
                    resolved_table = fqn
                elif parts[-2].strip().lower() in by_short_name:
                    resolved_table = parts[-2].strip().lower()
            # Case B: 2 parts → "alias.column"
            if not resolved_table and len(parts) == 2:
                col_alias = parts[0].strip()
                if col_alias:
                    resolved_table = alias_to_table.get(col_alias.lower(), "")
            # Case C: bare column name → resolve via SQL analysis if unambiguous
            if not resolved_table and col_key in columns_by_name:
                candidates: set[str] = set()
                for cr in columns_by_name[col_key]:
                    tn = (getattr(cr, "table_name", "") or "").lower()
                    if not tn and getattr(cr, "table_alias", ""):
                        tn = alias_to_table.get(cr.table_alias.lower(), "")
                    if tn:
                        candidates.add(tn)
                if len(candidates) == 1:
                    resolved_table = next(iter(candidates))

            if not resolved_table:
                continue
            ts = by_full_name.get(resolved_table) or by_short_name.get(
                resolved_table.split(".")[-1]
            )
            if ts is None:
                continue
            existing = {
                k.lower() for k in (getattr(ts, "current_clustering_keys", None) or [])
            }
            if col_key in existing:
                continue
            key = (getattr(ts, "table_name", "") or "", col_name)
            if key in seen:
                continue
            seen.add(key)
            results.append(key)
    return results


def _collect_cte_multi_reference(query_text: str | None) -> list[tuple[str, int]]:
    if not query_text:
        return []
    try:
        from core.sql_patterns import analyze_cte_multi_references
    except ImportError:
        return []
    return [
        (name, refs - 1)
        for name, refs in analyze_cte_multi_references(query_text)
        if refs >= _CTE_MULTI_REF_THRESHOLD
    ]


def collect_profile_evidence(analysis: Any) -> ProfileEvidence:
    """Build a ProfileEvidence from a ProfileAnalysis.

    Tolerates partial inputs — None / missing attributes flow through
    as the default empty values so a single missing field does not
    cascade.
    """
    if analysis is None:
        return ProfileEvidence()

    node_metrics = getattr(analysis, "node_metrics", None) or []
    shuffle_metrics = getattr(analysis, "shuffle_metrics", None) or []
    top_scanned = getattr(analysis, "top_scanned_tables", None) or []
    sql_analysis = getattr(analysis, "sql_analysis", None)
    qm = getattr(analysis, "query_metrics", None)
    bi = getattr(analysis, "bottleneck_indicators", None)

    decimal_examples = _collect_decimal_arithmetic_in_heavy_agg(node_metrics)
    dominant_outside_lc = _collect_dominant_shuffle_keys_outside_lc(
        shuffle_metrics, sql_analysis, top_scanned
    )
    cte_refs = _collect_cte_multi_reference(getattr(qm, "query_text", "") if qm else "")
    spill_total = getattr(bi, "spill_bytes", 0) or 0

    return ProfileEvidence(
        decimal_arithmetic_in_heavy_agg=bool(decimal_examples),
        decimal_arithmetic_examples=decimal_examples[:3],
        dominant_shuffle_keys_outside_lc=bool(dominant_outside_lc),
        dominant_shuffle_outside_lc_columns=dominant_outside_lc[:5],
        cte_multi_reference=bool(cte_refs),
        cte_multi_reference_names=cte_refs[:5],
        spill_dominant=spill_total >= _SPILL_DOMINANT_BYTES,
        spill_total_bytes=int(spill_total),
    )
