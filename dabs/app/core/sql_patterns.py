"""SQL-level pattern detectors shared across analyzers.

Lives above the `analyzers/` layer so both `bottleneck` and `recommendations`
can import without cross-module coupling.
"""

import re

from .models import NodeMetrics

# Non-sargable predicates on columns (functions prevent file pruning / pushdown)
_NON_SARGABLE_FN_RE = re.compile(
    r"\b(YEAR|MONTH|DAY|DATE|CAST|UPPER|LOWER|TRIM|SUBSTRING|CONCAT|COALESCE)\s*\(",
    re.IGNORECASE,
)


def collect_non_sargable_filter_functions(node_metrics: list[NodeMetrics]) -> list[str]:
    """Return sorted unique function names found in scan filter_conditions (profile metadata)."""
    found: set[str] = set()
    for nm in node_metrics:
        for cond in nm.filter_conditions or []:
            for m in _NON_SARGABLE_FN_RE.finditer(cond):
                found.add(m.group(1).upper())
    return sorted(found)


def analyze_cte_multi_references(query_text: str) -> list[tuple[str, int]]:
    """Return (cte_name, total_identifier_occurrences) when a CTE is used 2+ times beyond WITH.

    Total counts all ``name`` token occurrences in the query (including ``WITH name AS``).
    Reference count = total - 1; we emit when reference count >= 2 (total >= 3).
    """
    from .sql_analyzer import remove_comments

    text = remove_comments(query_text or "")
    if not text.strip():
        return []
    try:
        import sqlglot
    except ImportError:
        return []
    tree = None
    for dialect in ("databricks", "spark", None):
        try:
            tree = sqlglot.parse_one(text, dialect=dialect) if dialect else sqlglot.parse_one(text)
            break
        except Exception:
            continue
    if tree is None:
        return []
    ctes = getattr(tree, "ctes", None) or []
    if not ctes:
        return []
    out: list[tuple[str, int]] = []
    for cte in ctes:
        name = (getattr(cte, "alias", None) or "").strip()
        if not name:
            continue
        pat = re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE)
        total = len(pat.findall(text))
        refs = total - 1
        if refs >= 2:
            out.append((name, total))
    return out
