"""Profile JSON redaction for L5 customer feedback bundles (2026-04-26).

Goal: produce a "reduced-sensitivity" copy of a profiler-uploaded JSON
payload that we can safely include in bundles sent to the vendor.
**NOT** a full anonymization — schema/object names and operational
metadata remain. Customers MUST be told this is reduced-sensitivity,
not anonymized (Codex (e) recommendation).

What gets stripped:
- SQL literals (numbers + strings) inside SQL/expression strings
- SQL comments (could carry "-- 顧客名: ABC" hints)
- File paths (S3 / ABFS keys can contain customer identifiers)
- Error messages (Spark exception text often includes table values)
- Prepared-statement parameter values
- min/max/bounds raw values (clustering_key_bounds, stat extremes)

What stays:
- Table / column names (schema-level identifiers)
- Operator metadata (peak_memory, shuffle bytes, durations)
- Aggregate expression *structure* (operands stripped to placeholders)
- File counts, partition counts, statistics aggregates

The fail-safe: if SQL parsing fails, we emit ``<UNPARSEABLE_SQL>``
rather than the original — never leak raw SQL on parser failure.
``RedactStats`` records how many failures and where, so the vendor
can prioritize parser improvements.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Key-name policies (case-insensitive)
# ---------------------------------------------------------------------------

# Keys whose string value is SQL — parse + strip literals + emit.
_SQL_KEYS_LOWER = frozenset(
    s.lower()
    for s in (
        "sql",
        "query",
        "queryText",
        "query_text",
        "explain",
        "filter",
        "filter_condition",
        "filter_conditions",
        "predicate",
        "condition",
        "having_clause",
        "where_clause",
        "join_condition",
        "agg_expression",
        "aggregate_expression",
        "aggregate_expressions",
        "grouping_expression",
        "grouping_expressions",
        "partition_filter",
        "partition_filters",
    )
)

# Keys whose string value is treated as opaque secret-bearing data —
# replace with a fixed marker. We never try to "preserve structure" on
# these because they often inline customer identifiers (paths, error
# blobs with table values, etc.).
_OPAQUE_KEYS_LOWER = frozenset(
    s.lower()
    for s in (
        "path",
        "file",
        "filePath",
        "file_path",
        "files",
        "location",
        "locations",
        "error",
        "errorMessage",
        "error_message",
        "stackTrace",
        "stack_trace",
        "exception",
        "parameter",
        "parameters",
        "boundParameters",
        "stmtParameters",
        "session_parameters",
        "queryTags",
        "query_tags",
        "tags",
        "user",
        "userName",
        "user_name",
    )
)

# Keys representing range bounds — drop scalar value but keep the key
# so the consumer can see the field existed.
_BOUNDS_KEYS_LOWER = frozenset(
    s.lower()
    for s in (
        "min",
        "max",
        "minValue",
        "maxValue",
        "bounds",
        "lowerBound",
        "upperBound",
        "lower_bound",
        "upper_bound",
        "clustering_key_bounds",
    )
)

_REDACTED_OPAQUE_VALUE = "<REDACTED>"
_UNPARSEABLE_MARKER = "<UNPARSEABLE_SQL>"
_LITERAL_PLACEHOLDER_STR = "?"


@dataclass
class RedactStats:
    """Summary of redaction work for the metadata.json header.

    The vendor uses this to gauge how much profile content was lost to
    parser failure (worth investigating) vs successfully redacted.
    """

    sql_redacted_count: int = 0
    parse_failures: int = 0
    unparseable_sql_paths: list[str] = field(default_factory=list)
    opaque_redacted_count: int = 0
    bounds_redacted_count: int = 0
    comments_stripped_count: int = 0


# ---------------------------------------------------------------------------
# SQL redaction (string → string)
# ---------------------------------------------------------------------------

# SQL line/block comment regex — used to strip BEFORE handing off to
# sqlglot, both because comments often carry sensitive context and
# because sqlglot's comment handling is dialect-dependent.
_LINE_COMMENT_RE = re.compile(r"--[^\n\r]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _strip_comments(sql: str) -> tuple[str, int]:
    """Remove SQL comments. Returns (stripped, count)."""
    count = len(_LINE_COMMENT_RE.findall(sql)) + len(_BLOCK_COMMENT_RE.findall(sql))
    if count == 0:
        return sql, 0
    sql = _BLOCK_COMMENT_RE.sub(" ", sql)
    sql = _LINE_COMMENT_RE.sub("", sql)
    return sql, count


def redact_sql_literals(sql: str) -> tuple[str, bool, int]:
    """Strip literals + comments from one SQL string.

    Returns ``(redacted, parsed_ok, comments_stripped)``. On parser
    failure returns ``(<UNPARSEABLE_SQL>, False, comments_stripped)``
    — never the original text.
    """
    if not sql or not sql.strip():
        return sql, True, 0

    pre_stripped, comment_count = _strip_comments(sql)

    try:
        import sqlglot
        from sqlglot import expressions as exp
    except ImportError:
        # sqlglot is required by the project — but be defensive.
        return _UNPARSEABLE_MARKER, False, comment_count

    try:
        ast = sqlglot.parse_one(pre_stripped, read="databricks")
    except Exception:
        return _UNPARSEABLE_MARKER, False, comment_count
    if ast is None:
        return _UNPARSEABLE_MARKER, False, comment_count

    try:
        # Replace every Literal with a placeholder. We avoid touching
        # Identifier / Column / Table nodes — schema-level names are
        # intentionally preserved.
        for lit in list(ast.find_all(exp.Literal)):
            if lit.is_string:
                lit.replace(exp.Literal.string(_LITERAL_PLACEHOLDER_STR))
            else:
                lit.replace(exp.Literal.number(0))
        # Boolean / NULL constants are usually safe but harmless to
        # neutralize the booleans for parity.
        for b in list(ast.find_all(exp.Boolean)):
            b.replace(exp.Literal.number(0))
        return ast.sql(dialect="databricks"), True, comment_count
    except Exception:
        return _UNPARSEABLE_MARKER, False, comment_count


# ---------------------------------------------------------------------------
# Deep-walk redaction
# ---------------------------------------------------------------------------


def _redact_string_at_key(
    key_lower: str,
    value: str,
    stats: RedactStats,
    path: str,
) -> str:
    if key_lower in _SQL_KEYS_LOWER:
        redacted, ok, ccount = redact_sql_literals(value)
        if ccount:
            stats.comments_stripped_count += ccount
        if ok:
            if redacted != value:
                stats.sql_redacted_count += 1
            return redacted
        stats.parse_failures += 1
        if len(stats.unparseable_sql_paths) < 50:
            stats.unparseable_sql_paths.append(path)
        return _UNPARSEABLE_MARKER
    if key_lower in _OPAQUE_KEYS_LOWER:
        stats.opaque_redacted_count += 1
        return _REDACTED_OPAQUE_VALUE
    if key_lower in _BOUNDS_KEYS_LOWER:
        stats.bounds_redacted_count += 1
        return _REDACTED_OPAQUE_VALUE
    return value


def _walk_redact(obj: Any, stats: RedactStats, path: str = "$") -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            child_path = f"{path}.{k}"
            kl = str(k).lower()
            if isinstance(v, str):
                out[k] = _redact_string_at_key(kl, v, stats, child_path)
            elif isinstance(v, list):
                # Lists under SQL keys: each string item is SQL.
                if kl in _SQL_KEYS_LOWER:
                    new_items: list[Any] = []
                    for i, item in enumerate(v):
                        if isinstance(item, str):
                            new_items.append(
                                _redact_string_at_key(kl, item, stats, f"{child_path}[{i}]")
                            )
                        else:
                            new_items.append(_walk_redact(item, stats, f"{child_path}[{i}]"))
                    out[k] = new_items
                # Lists under opaque/bounds keys: redact every string.
                elif kl in _OPAQUE_KEYS_LOWER or kl in _BOUNDS_KEYS_LOWER:
                    new_items = []
                    for i, item in enumerate(v):
                        if isinstance(item, str):
                            new_items.append(_REDACTED_OPAQUE_VALUE)
                            stats.opaque_redacted_count += 1
                        else:
                            new_items.append(_walk_redact(item, stats, f"{child_path}[{i}]"))
                    out[k] = new_items
                else:
                    out[k] = [
                        _walk_redact(item, stats, f"{child_path}[{i}]")
                        for i, item in enumerate(v)
                    ]
            elif isinstance(v, dict):
                # If the parent key implies bounds OR opaque metadata,
                # sweep every leaf to <REDACTED> regardless of the
                # child key (which is often customer-defined). Codex (g)
                # call-out: parameters / queryTags etc.
                if kl in _BOUNDS_KEYS_LOWER:
                    out[k] = _redact_bounds_dict(v, stats)
                elif kl in _OPAQUE_KEYS_LOWER:
                    out[k] = _redact_opaque_dict(v, stats)
                else:
                    out[k] = _walk_redact(v, stats, child_path)
            else:
                out[k] = v
        return out
    if isinstance(obj, list):
        return [_walk_redact(item, stats, f"{path}[{i}]") for i, item in enumerate(obj)]
    return obj


def _redact_opaque_dict(d: dict, stats: RedactStats) -> dict:
    """Replace every leaf in an opaque dict with <REDACTED>.

    Used when the parent key is one of ``_OPAQUE_KEYS_LOWER`` and the
    value is a dict (e.g., ``parameters: {user_var: ..., session_id: ...}``).
    """
    out: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = _redact_opaque_dict(v, stats)
        elif isinstance(v, list):
            new_items = []
            for item in v:
                if isinstance(item, str):
                    new_items.append(_REDACTED_OPAQUE_VALUE)
                    stats.opaque_redacted_count += 1
                else:
                    new_items.append(item)
            out[k] = new_items
        elif isinstance(v, str):
            out[k] = _REDACTED_OPAQUE_VALUE
            stats.opaque_redacted_count += 1
        else:
            # numbers under opaque parents are uncommon but redact for safety
            out[k] = _REDACTED_OPAQUE_VALUE
            stats.opaque_redacted_count += 1
    return out


def _redact_bounds_dict(d: dict, stats: RedactStats) -> dict:
    """Replace every leaf in a bounds dict with <REDACTED>, preserving keys."""
    out: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = _redact_bounds_dict(v, stats)
        elif isinstance(v, list):
            out[k] = [_REDACTED_OPAQUE_VALUE for _ in v]
            stats.bounds_redacted_count += len(v)
        else:
            out[k] = _REDACTED_OPAQUE_VALUE
            stats.bounds_redacted_count += 1
    return out


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------


def redact_profile(payload: Any) -> tuple[Any, RedactStats]:
    """Redact a parsed profile JSON object.

    Args:
        payload: parsed JSON object (typically a dict from
            ``json.loads(profile_json_text)``).

    Returns:
        ``(redacted_payload, stats)`` — both are safe to serialize.
        Caller is expected to NOT include the original payload alongside
        unless the customer explicitly opts in.
    """
    stats = RedactStats()
    return _walk_redact(payload, stats), stats
