"""Query family identification for grouping semantically similar queries.

Generates a ``purpose_signature`` that groups queries with the same
intent (same tables, joins, aggregations, projections) even when
the SQL differs in hints, JOIN order, CTE structure, or literal values.

Hierarchy:
    query_fingerprint  — exact normalized SQL match
    purpose_signature  — same purpose / intent (this module)
    variant_type       — what differs within the same purpose
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

PURPOSE_SIGNATURE_VERSION = "v1"


def extract_purpose_features(sql: str) -> dict[str, Any]:
    """Extract structural features that define a query's purpose.

    These features are stable across hint changes, JOIN reordering,
    CTE rewrites, alias changes, and literal value differences.

    Returns:
        Dict with sorted feature lists for deterministic hashing.
    """
    if not sql or not sql.strip():
        return {}

    try:
        return _extract_features_sqlglot(sql)
    except Exception as e:
        logger.debug("sqlglot feature extraction failed, using regex: %s", e)
        return _extract_features_regex(sql)


def generate_purpose_signature(sql: str) -> str:
    """Generate a purpose-based signature hash.

    Returns:
        Hex-encoded SHA-256 of the purpose features.
        Empty string if extraction fails.
    """
    features = extract_purpose_features(sql)
    if not features:
        return ""

    # Deterministic JSON serialization
    canonical = json.dumps(features, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def detect_variant_type(
    baseline_sql: str,
    candidate_sql: str,
    baseline_warehouse: str = "",
    candidate_warehouse: str = "",
) -> str:
    """Detect what kind of variant this is compared to baseline.

    Returns one of:
        same_sql_diff_warehouse, diff_hint, diff_filter,
        diff_join_order, diff_projection, diff_cte, diff_other
    """
    if not baseline_sql or not candidate_sql:
        return "unknown"

    # Check hint difference first (before normalizing, since normalize strips hints)
    b_hints = _extract_hints(baseline_sql)
    c_hints = _extract_hints(candidate_sql)

    # Compare normalized SQL (hints already stripped by normalize)
    from .fingerprint import normalize_sql

    b_norm = normalize_sql(baseline_sql)
    c_norm = normalize_sql(candidate_sql)

    if b_norm == c_norm:
        # SQL is identical after normalization
        if b_hints != c_hints:
            return "diff_hint"
        if baseline_warehouse != candidate_warehouse:
            return "same_sql_diff_warehouse"
        return "same_sql"

    b_feat = extract_purpose_features(baseline_sql)
    c_feat = extract_purpose_features(candidate_sql)

    if not b_feat or not c_feat:
        return "diff_other"

    # Hint difference with same tables
    if b_hints != c_hints and b_feat.get("tables") == c_feat.get("tables"):
        return "diff_hint"

    # Check filter difference
    if (
        b_feat.get("tables") == c_feat.get("tables")
        and b_feat.get("join_keys") == c_feat.get("join_keys")
        and b_feat.get("group_by") == c_feat.get("group_by")
        and b_feat.get("filter_columns") != c_feat.get("filter_columns")
    ):
        return "diff_filter"

    # Check JOIN order (same tables, same join keys, different join edges order)
    if (
        b_feat.get("tables") == c_feat.get("tables")
        and set(b_feat.get("join_keys", [])) == set(c_feat.get("join_keys", []))
        and b_feat.get("aggregates") == c_feat.get("aggregates")
    ):
        return "diff_join_order"

    # Check projection difference
    if (
        b_feat.get("tables") == c_feat.get("tables")
        and b_feat.get("group_by") == c_feat.get("group_by")
        and b_feat.get("aggregates") != c_feat.get("aggregates")
    ):
        return "diff_projection"

    # CTE structure difference
    if b_feat.get("tables") == c_feat.get("tables") and b_feat.get("cte_count", 0) != c_feat.get(
        "cte_count", 0
    ):
        return "diff_cte"

    return "diff_other"


# ---------------------------------------------------------------------------
# Feature extraction: sqlglot (primary)
# ---------------------------------------------------------------------------


def _extract_features_sqlglot(sql: str) -> dict[str, Any]:
    """Extract features using sqlglot AST parsing."""
    import sqlglot
    from sqlglot import exp

    # Remove hints before parsing to avoid hint args being treated as tables
    clean_sql = re.sub(r"/\*\+.*?\*/", "", sql, flags=re.DOTALL)

    # Parse with Databricks dialect, fallback to Spark
    tree = None
    for dialect in ("databricks", "spark", None):
        try:
            parsed = sqlglot.parse(clean_sql, dialect=dialect)
            if parsed:
                tree = parsed[0]
                break
        except Exception:
            continue

    if tree is None:
        return _extract_features_regex(sql)

    # Tables (sorted, deduplicated)
    tables = sorted(
        {
            t.name.lower()
            for t in tree.find_all(exp.Table)
            if t.name and t.name.upper() not in _NON_TABLE_KEYWORDS
        }
    )

    # JOIN keys
    join_keys = []
    for join in tree.find_all(exp.Join):
        on_clause = join.find(exp.EQ)
        if on_clause:
            cols = [c.name.lower() for c in on_clause.find_all(exp.Column)]
            if cols:
                join_keys.append("=".join(sorted(cols)))
    join_keys = sorted(set(join_keys))

    # Aggregate functions
    aggregates = sorted({node.key.upper() for node in tree.find_all(exp.AggFunc)})

    # GROUP BY columns
    group_by = []
    for gb in tree.find_all(exp.Group):
        for col in gb.find_all(exp.Column):
            group_by.append(col.name.lower())
    group_by = sorted(set(group_by))

    # WHERE/filter columns (column names only, not values)
    filter_columns = []
    for where in tree.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            filter_columns.append(col.name.lower())
    filter_columns = sorted(set(filter_columns))

    # Window functions
    window_funcs = sorted({node.key.upper() for node in tree.find_all(exp.Window)})

    # CTE count
    cte_count = len(list(tree.find_all(exp.CTE)))

    # Has DISTINCT, UNION, ORDER BY, LIMIT
    has_distinct = bool(list(tree.find_all(exp.Distinct)))
    has_union = bool(list(tree.find_all(exp.Union)))
    has_order = bool(list(tree.find_all(exp.Order)))
    has_limit = bool(list(tree.find_all(exp.Limit)))

    return {
        "tables": tables,
        "join_keys": join_keys,
        "aggregates": aggregates,
        "group_by": group_by,
        "filter_columns": filter_columns,
        "window_funcs": window_funcs,
        "cte_count": cte_count,
        "has_distinct": has_distinct,
        "has_union": has_union,
        "has_order": has_order,
        "has_limit": has_limit,
    }


_NON_TABLE_KEYWORDS = frozenset(
    [
        "VALUES",
        "RANGE",
        "EXPLODE",
        "EXPLODE_OUTER",
        "POSEXPLODE",
        "INLINE",
        "INLINE_OUTER",
        "STACK",
        "UNNEST",
        "LATERAL",
        "DUAL",
    ]
)


# ---------------------------------------------------------------------------
# Feature extraction: regex fallback
# ---------------------------------------------------------------------------


def _extract_features_regex(sql: str) -> dict[str, Any]:
    """Extract features using regex (fallback when parser fails)."""
    sql.upper()

    # Remove hints and comments
    clean = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    clean = re.sub(r"--[^\n]*", " ", clean)
    upper_clean = clean.upper()

    # Tables from FROM/JOIN
    tables = sorted(
        set(
            m.group(1).lower().split(".")[-1]
            for m in re.finditer(r"(?:FROM|JOIN)\s+(\S+)", upper_clean, re.IGNORECASE)
            if m.group(1).upper() not in _NON_TABLE_KEYWORDS and not m.group(1).startswith("(")
        )
    )

    # Aggregates
    agg_funcs = ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT_LIST", "COLLECT_SET"]
    aggregates = sorted(f for f in agg_funcs if re.search(rf"\b{f}\s*\(", upper_clean))

    # GROUP BY columns
    gb_match = re.search(
        r"GROUP\s+BY\s+(.+?)(?:HAVING|ORDER|LIMIT|UNION|$)", upper_clean, re.DOTALL
    )
    group_by = []
    if gb_match:
        group_by = sorted(
            set(
                c.strip().lower().split(".")[-1]
                for c in gb_match.group(1).split(",")
                if c.strip() and not c.strip().startswith("(")
            )
        )

    # WHERE columns
    where_match = re.search(
        r"WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|UNION|HAVING|$)", upper_clean, re.DOTALL
    )
    filter_columns = []
    if where_match:
        filter_columns = sorted(
            set(
                m.group(1).lower().split(".")[-1]
                for m in re.finditer(
                    r"(\w+)\s*(?:=|<|>|<=|>=|!=|<>|IN|LIKE|BETWEEN|IS)", where_match.group(1)
                )
            )
        )

    # JOIN keys
    join_keys = sorted(
        set(
            "=".join(sorted([m.group(1).lower().split(".")[-1], m.group(2).lower().split(".")[-1]]))
            for m in re.finditer(r"ON\s+(\S+)\s*=\s*(\S+)", upper_clean, re.IGNORECASE)
        )
    )

    cte_count = len(re.findall(r"\bAS\s*\(", upper_clean))

    return {
        "tables": tables,
        "join_keys": join_keys,
        "aggregates": aggregates,
        "group_by": group_by,
        "filter_columns": filter_columns,
        "window_funcs": [],
        "cte_count": cte_count,
        "has_distinct": bool(re.search(r"\bDISTINCT\b", upper_clean)),
        "has_union": bool(re.search(r"\bUNION\b", upper_clean)),
        "has_order": bool(re.search(r"\bORDER\s+BY\b", upper_clean)),
        "has_limit": bool(re.search(r"\bLIMIT\b", upper_clean)),
    }


# ---------------------------------------------------------------------------
# Hint extraction
# ---------------------------------------------------------------------------


def _extract_hints(sql: str) -> set[str]:
    """Extract SQL hints (e.g., BROADCAST, SHUFFLE_HASH)."""
    hints = set()
    for m in re.finditer(r"/\*\+\s*(.+?)\s*\*/", sql, re.DOTALL):
        hints.add(m.group(1).strip().upper())
    return hints
