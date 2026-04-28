"""Post-LLM safety net: fill ``ActionCard.fix_sql`` only when the
binding to a unique table + column set is unambiguous.

Codex 2026-04-26 review (Path B). The prompt contract (Iter 1) and
eval invariant (Iter 2) handle the LLM-cooperation case. This module
catches the residual case where the LLM still leaves ``fix_sql``
empty for a clearly SQL-shaped recommendation. Per Codex, the safety
net must be **conservative**:

  - Fill only when the target table is unique from structured
    evidence (``TableScanMetrics.recommended_clustering_keys``).
  - Use canonical syntax (the same forms surfaced as the prompt
    allowlist in Iter 1).
  - Never parse natural-language column names — the columns must
    come from the analyzer's structured output.

When the binding is ambiguous, leave ``fix_sql`` empty — Codex:
"writing a wrong table/column is worse than writing nothing".
``explain_skip_reason`` exposes the reason so eval / logging can
surface it.

Iter 3 scope: CLUSTER BY only. OPTIMIZE / SET TBLPROPERTIES require
either an additional structured input or the LLM's cooperation;
they remain in the "leave empty" bucket until a future iter adds
similarly safe bindings.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from .models import ActionCard, TableScanMetrics


_CLUSTER_BY_HINTS: tuple[str, ...] = ("CLUSTER BY", "Liquid Clustering")
_SQL_ACTION_HINTS: tuple[str, ...] = (
    "CLUSTER BY",
    "Liquid Clustering",
    "ALTER TABLE",
    "OPTIMIZE",
    "SET TBLPROPERTIES",
    "ANALYZE TABLE",
)


def _mentions_any(text: str, needles: Iterable[str]) -> bool:
    if not text:
        return False
    upper = text.upper()
    return any(n.upper() in upper for n in needles)


def _unique_clustering_target(
    top_scanned_tables: list[TableScanMetrics] | None,
) -> TableScanMetrics | None:
    """Return the unique TableScan with non-empty recommended keys.

    None when no candidate exists OR multiple candidates exist —
    both cases fail the "unambiguous binding" gate.
    """
    if not top_scanned_tables:
        return None
    candidates = [t for t in top_scanned_tables if t.recommended_clustering_keys]
    if len(candidates) != 1:
        return None
    return candidates[0]


def fill_missing_fix_sql(
    card: ActionCard,
    *,
    top_scanned_tables: list[TableScanMetrics] | None = None,
) -> ActionCard:
    """Return a card with ``fix_sql`` populated when safe, else
    return the original card unchanged.

    Currently handles only CLUSTER BY recommendations bound to a
    unique top-scanned table with structured ``recommended_clustering_keys``.
    """
    if card.fix_sql.strip():
        return card
    if not _mentions_any(card.fix, _CLUSTER_BY_HINTS):
        return card
    target = _unique_clustering_target(top_scanned_tables)
    if target is None:
        return card

    cols_str = ", ".join(target.recommended_clustering_keys)
    sql = f"ALTER TABLE {target.table_name} CLUSTER BY ({cols_str});"
    return replace(card, fix_sql=sql)


def explain_skip_reason(
    card: ActionCard,
    *,
    top_scanned_tables: list[TableScanMetrics] | None = None,
) -> str:
    """Diagnostic counterpart of ``fill_missing_fix_sql``.

    Returns a short reason string when an SQL-shaped action remains
    without ``fix_sql``, or empty string when no fill was expected.
    Used by eval / logging to surface why the safety net declined to
    act.
    """
    if card.fix_sql.strip():
        return ""
    if not _mentions_any(card.fix, _SQL_ACTION_HINTS):
        return ""
    if not _mentions_any(card.fix, _CLUSTER_BY_HINTS):
        return (
            "fix_sql omitted: action type outside the Iter 3 allowlist "
            "(CLUSTER BY only). Other patterns rely on prompt cooperation."
        )
    if not top_scanned_tables:
        return "fix_sql omitted: no top-scanned tables available for binding."
    candidates = [t for t in top_scanned_tables if t.recommended_clustering_keys]
    if not candidates:
        return (
            "fix_sql omitted: no table has structured "
            "recommended_clustering_keys — binding is ambiguous."
        )
    if len(candidates) > 1:
        names = ", ".join(t.table_name for t in candidates)
        return (
            f"fix_sql omitted: multiple candidate tables ({names}) — "
            "binding is ambiguous, refusing to pick one."
        )
    return ""  # candidate exists; fill_missing_fix_sql would have populated.
