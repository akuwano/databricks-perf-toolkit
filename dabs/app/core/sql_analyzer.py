"""
SQL analysis module using sqlparse.
"""

import logging
import re
from typing import Any

import sqlglot
import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis
from sqlparse.tokens import Keyword

from .constants import UsageType
from .models import ColumnReference, JoinEdge, QueryStructure, SQLAnalysis, TableReference

# sqlparse grouping stage has an internal DoS guard (MAX_GROUPING_TOKENS=10000)
# that rejects medium-large SQL. Raise it moderately and enforce our own length
# guards below for very large inputs.
_SQLPARSE_GROUPING_MAX_TOKENS = 50_000
_SQLPARSE_COMMENT_MAX_CHARS = 50_000
_SQLPARSE_FORMAT_MAX_CHARS = 50_000

try:
    from sqlparse.engine import grouping as _sqlparse_grouping

    _current_max = getattr(_sqlparse_grouping, "MAX_GROUPING_TOKENS", None)
    if _current_max is not None:
        _sqlparse_grouping.MAX_GROUPING_TOKENS = max(_current_max, _SQLPARSE_GROUPING_MAX_TOKENS)
except Exception:
    pass

# Keywords that look like tables but are not (function tables, table-valued functions)
NON_TABLE_KEYWORDS = frozenset(
    [
        "VALUES",
        "RANGE",
        "EXPLODE",
        "EXPLODE_OUTER",
        "POSEXPLODE",
        "POSEXPLODE_OUTER",
        "INLINE",
        "INLINE_OUTER",
        "STACK",
        "JSON_TUPLE",
        "PARSE_URL_TUPLE",
        "UNNEST",
        "LATERAL",
        "GENERATE_SERIES",
        "SEQUENCE",
        "DUAL",
    ]
)

logger = logging.getLogger(__name__)


# Skip sqlglot formatting for very large SQL (performance guard)
_SQLGLOT_FORMAT_MAX_CHARS = 200_000


def format_sql(sql: str) -> str:
    """Format SQL with consistent indentation and style.

    Uses a multi-tier approach:
    1. sqlglot pretty-print (dialect=databricks) — best for Databricks syntax
    2. sqlglot pretty-print (dialect=spark) — fallback for Spark-only syntax
    3. sqlparse formatting — fallback for unsupported syntax
    4. Original SQL — last resort

    Args:
        sql: Raw SQL string

    Returns:
        Formatted SQL string
    """
    if not sql or not sql.strip():
        return ""

    # Skip sqlglot for very large SQL to avoid performance issues
    if len(sql) <= _SQLGLOT_FORMAT_MAX_CHARS:
        result = _format_sql_with_sqlglot(sql)
        if result:
            return result

    return _format_sql_with_sqlparse(sql)


def _format_sql_with_sqlglot(sql: str) -> str | None:
    """Try to format SQL using sqlglot AST pretty-print.

    Returns formatted SQL or None if parsing fails or quality degrades
    (e.g., single-line comments converted to block comments).
    """
    original_block_count = sql.count("/*")

    for dialect in ("databricks", "spark"):
        try:
            ast = sqlglot.parse_one(sql, dialect=dialect)
            formatted = ast.sql(pretty=True, dialect=dialect)
            if not formatted:
                continue
            # Reject if sqlglot significantly increased block comments
            # (indicates -- line comments were converted to /* */ blocks)
            new_block_count = formatted.count("/*")
            if new_block_count > original_block_count + 2:
                continue
            return formatted
        except Exception:
            continue
    return None


def _format_sql_with_sqlparse(sql: str) -> str:
    """Format SQL using sqlparse (fallback)."""
    if len(sql) > _SQLPARSE_FORMAT_MAX_CHARS:
        logger.warning(
            "sqlparse formatting skipped",
            extra={
                "query_length": len(sql),
                "path": "_format_sql_with_sqlparse",
                "exception_type": "LengthGuard",
            },
        )
        return sql

    try:
        return sqlparse.format(
            sql,
            reindent=True,
            keyword_case="upper",
            indent_width=4,
            wrap_after=80,
        )
    except Exception as e:
        logger.warning(
            "sqlparse formatting failed",
            extra={
                "query_length": len(sql),
                "path": "_format_sql_with_sqlparse",
                "exception_type": type(e).__name__,
            },
        )
        return sql


def _strip_comments_lightweight(sql: str) -> str:
    """Remove SQL comments with a 1-pass scanner that respects string literals.

    Handles ``--`` line comments, ``/* */`` block comments, and single/double
    quoted literals. Comment markers inside string literals are preserved.
    ANSI-style doubled quotes (``''`` / ``""``) are treated as literal escapes,
    matching Databricks SQL semantics (no backslash escape dependency).
    """
    if not sql:
        return ""

    result: list[str] = []
    length = len(sql)
    index = 0
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False

    while index < length:
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < length else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                result.append(char)
            index += 1
            continue

        if in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment = False
                index += 2
            else:
                if char == "\n":
                    result.append(char)
                index += 1
            continue

        if in_single_quote:
            result.append(char)
            if char == "'":
                if next_char == "'":
                    result.append(next_char)
                    index += 2
                    continue
                in_single_quote = False
            index += 1
            continue

        if in_double_quote:
            result.append(char)
            if char == '"':
                if next_char == '"':
                    result.append(next_char)
                    index += 2
                    continue
                in_double_quote = False
            index += 1
            continue

        if char == "-" and next_char == "-":
            in_line_comment = True
            index += 2
            continue

        if char == "/" and next_char == "*":
            in_block_comment = True
            index += 2
            continue

        if char == "'":
            in_single_quote = True
            result.append(char)
            index += 1
            continue

        if char == '"':
            in_double_quote = True
            result.append(char)
            index += 1
            continue

        result.append(char)
        index += 1

    return "".join(result).strip()


def remove_comments(sql: str) -> str:
    """Remove SQL comments from query.

    Args:
        sql: SQL string with potential comments

    Returns:
        SQL string with comments removed
    """
    if not sql:
        return ""

    if len(sql) <= _SQLPARSE_COMMENT_MAX_CHARS:
        try:
            return sqlparse.format(sql, strip_comments=True).strip()
        except Exception as e:
            logger.warning(
                "sqlparse comment removal fallback",
                extra={
                    "query_length": len(sql),
                    "path": "remove_comments",
                    "exception_type": type(e).__name__,
                },
            )
    else:
        logger.warning(
            "sqlparse comment removal skipped",
            extra={
                "query_length": len(sql),
                "path": "remove_comments",
                "exception_type": "LengthGuard",
            },
        )

    try:
        return _strip_comments_lightweight(sql)
    except Exception as e:
        logger.warning(
            "lightweight comment removal failed",
            extra={
                "query_length": len(sql),
                "path": "remove_comments",
                "exception_type": type(e).__name__,
            },
        )
        return sql


def _parse_table_name(identifier: str) -> TableReference:
    """Parse a table identifier into catalog, schema, table parts.

    Args:
        identifier: Table identifier (e.g., "catalog.schema.table" or "table AS t")

    Returns:
        TableReference with parsed components
    """
    # Remove quotes and clean up
    clean_id = identifier.strip().strip("`").strip('"').strip("'")

    # Handle alias
    alias = ""
    alias_match = re.match(r"(.+?)\s+(?:AS\s+)?(\w+)$", clean_id, re.IGNORECASE)
    if alias_match:
        clean_id = alias_match.group(1).strip()
        alias = alias_match.group(2)

    # Split by dots
    parts = clean_id.split(".")
    parts = [p.strip().strip("`").strip('"').strip("'") for p in parts]

    catalog = ""
    schema = ""
    table = ""

    if len(parts) == 3:
        catalog, schema, table = parts
    elif len(parts) == 2:
        schema, table = parts
    elif len(parts) == 1:
        table = parts[0]

    full_name = ".".join(filter(None, [catalog, schema, table]))

    return TableReference(
        catalog=catalog,
        schema=schema,
        table=table,
        alias=alias,
        full_name=full_name,
        usage_type=UsageType.SOURCE,
    )


def _is_non_table_identifier(name: str) -> bool:
    """Check if the identifier is a non-table keyword (e.g., VALUES, RANGE).

    Args:
        name: Identifier name to check

    Returns:
        True if this is not a real table reference
    """
    upper_name = name.upper().strip()
    return upper_name in NON_TABLE_KEYWORDS


def _extract_table_from_identifier(identifier: Identifier) -> TableReference | None:
    """Extract table reference from a sqlparse Identifier.

    Args:
        identifier: sqlparse Identifier object

    Returns:
        TableReference or None if not a table reference
    """
    real_name = identifier.get_real_name()
    if not real_name:
        return None

    # Skip derived tables/subqueries: FROM (...) alias
    for token in identifier.tokens:
        if isinstance(token, Parenthesis):
            return None

    # Skip CTE definitions: WITH cte_name AS ( ... )
    upper_text = str(identifier).upper()
    if " AS " in upper_text and "(" in upper_text:
        return None

    # Skip non-table identifiers (VALUES, RANGE, etc.)
    if _is_non_table_identifier(real_name):
        return None

    # Build full name from parent tokens
    parts = []
    for token in identifier.tokens:
        if token.ttype is not None:
            if str(token.ttype) in ("Token.Name", "Token.Literal.String.Symbol"):
                parts.append(str(token).strip("`").strip('"'))
        elif isinstance(token, Identifier):
            parts.append(str(token.get_real_name() or "").strip("`").strip('"'))

    # Get alias
    alias = identifier.get_alias() or ""

    # Prefer get_real_name() (table) and build full name from tokens to avoid
    # accidentally using the alias (Identifier.get_name() can return the alias).
    full_name_str = str(identifier).strip()

    # Skip if the table name itself is a non-table keyword
    table_ref = _parse_table_name(full_name_str)
    if _is_non_table_identifier(table_ref.table):
        return None

    if alias:
        table_ref.alias = alias

    return table_ref


def extract_tables(sql: str) -> list[TableReference]:
    """Extract table references from SQL query.

    Args:
        sql: SQL query string

    Returns:
        List of TableReference objects
    """
    if not sql:
        return []

    try:
        tables: list[TableReference] = []
        seen_tables: set[tuple[str, str]] = set()

        # Clean SQL first
        clean_sql = remove_comments(sql)
        parsed = sqlparse.parse(clean_sql)
    except Exception as e:
        logger.warning(f"Failed to parse SQL for table extraction: {e}")
        return []

    for statement in parsed:
        # Track if we're in FROM/JOIN context
        from_seen = False
        join_seen = False
        cte_names = set()

        # Track CTE names (WITH clause) using regex.
        upper_sql = clean_sql.upper()
        cte_pattern = r"\bWITH\b\s+(\w+)\s+AS\s*\("
        for match in re.finditer(cte_pattern, upper_sql, re.IGNORECASE):
            cte_names.add(match.group(1).strip("`").strip('"').lower())
        additional_cte_pattern = r",\s*(\w+)\s+AS\s*\("
        for match in re.finditer(additional_cte_pattern, upper_sql, re.IGNORECASE):
            cte_names.add(match.group(1).strip("`").strip('"').lower())

        # Use token tree to find tables
        def process_tokens(token_list: Any, cte_names_set: set[str]) -> None:
            """Process tokens recursively to find tables."""
            nonlocal from_seen, join_seen, tables, seen_tables

            i = 0
            tokens = list(token_list.tokens) if hasattr(token_list, "tokens") else []

            while i < len(tokens):
                token = tokens[i]
                token_str = str(token).upper().strip()

                # Track FROM/JOIN keywords
                if token.ttype is Keyword:
                    if token_str == "FROM":
                        from_seen = True
                        join_seen = False
                    elif "JOIN" in token_str:
                        join_seen = True
                        from_seen = False
                    elif token_str in (
                        "WHERE",
                        "GROUP",
                        "ORDER",
                        "HAVING",
                        "LIMIT",
                        "UNION",
                    ):
                        from_seen = False
                        join_seen = False
                    elif token_str == "INTO":
                        # Next identifier is target table
                        if i + 1 < len(tokens):
                            next_token = tokens[i + 1]
                            if isinstance(next_token, Identifier):
                                table_ref = _extract_table_from_identifier(next_token)
                                if table_ref and table_ref.table.lower() not in cte_names_set:
                                    table_ref.usage_type = UsageType.TARGET
                                    key = (table_ref.full_name, table_ref.alias)
                                    if key not in seen_tables:
                                        seen_tables.add(key)
                                        tables.append(table_ref)

                # Handle identifiers after FROM/JOIN
                # Note: Identifier may include Parenthesis for derived table / CTE definition.
                if (from_seen or join_seen) and isinstance(token, Identifier):
                    table_ref = _extract_table_from_identifier(token)
                    if table_ref and table_ref.table.lower() not in cte_names_set:
                        key = (table_ref.full_name, table_ref.alias)
                        if key not in seen_tables:
                            seen_tables.add(key)
                            tables.append(table_ref)

                # Always recurse into Identifier to process CTE bodies / derived tables
                # (CTE definitions like "cte AS (...)" are not after FROM/JOIN but contain subqueries)
                if isinstance(token, Identifier) and hasattr(token, "tokens"):
                    for inner_token in token.tokens:
                        if isinstance(inner_token, Parenthesis):
                            saved_from = from_seen
                            saved_join = join_seen
                            from_seen = False
                            join_seen = False
                            process_tokens(inner_token, cte_names_set)
                            from_seen = saved_from
                            join_seen = saved_join

                # Handle identifier lists (multiple tables or CTE definitions)
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        if isinstance(identifier, Identifier):
                            # Only extract as table if after FROM/JOIN
                            if from_seen or join_seen:
                                table_ref = _extract_table_from_identifier(identifier)
                                if table_ref and table_ref.table.lower() not in cte_names_set:
                                    key = (table_ref.full_name, table_ref.alias)
                                    if key not in seen_tables:
                                        seen_tables.add(key)
                                        tables.append(table_ref)

                            # Always recurse into derived tables / CTE bodies
                            if hasattr(identifier, "tokens"):
                                for inner_token in identifier.tokens:
                                    if isinstance(inner_token, Parenthesis):
                                        saved_from = from_seen
                                        saved_join = join_seen
                                        from_seen = False
                                        join_seen = False
                                        process_tokens(inner_token, cte_names_set)
                                        from_seen = saved_from
                                        join_seen = saved_join

                # Recurse into subqueries / parenthesis groups
                # Reset from_seen/join_seen for subqueries so they can find their own FROM clauses
                if isinstance(token, Parenthesis):
                    saved_from = from_seen
                    saved_join = join_seen
                    from_seen = False
                    join_seen = False
                    process_tokens(token, cte_names_set)
                    from_seen = saved_from
                    join_seen = saved_join

                i += 1

        process_tokens(statement, cte_names)

    return tables


def _split_qualified_column(raw: str) -> tuple[str, str]:
    """Split "alias.column" into (alias, column).

    Returns ("", "") if the input is not a qualified column.
    """
    cleaned = raw.strip().strip("`").strip('"')
    if "." not in cleaned:
        return "", ""

    left, right = cleaned.rsplit(".", 1)
    left = left.strip().strip("`").strip('"')
    right = right.strip().strip("`").strip('"')
    if not left or not right:
        return "", ""

    # Ignore things like schema.table (2+ parts) by taking the last part as alias.
    table_alias = left.split(".")[-1]
    return table_alias, right


def _infer_table_alias_from_unqualified_column(
    column_name: str,
    alias_to_table: dict[str, str],
) -> str:
    """Infer table alias for an unqualified column reference.

    Heuristics:
    - TPC-DS style prefixes (ss_, cs_, ws_, d_, i_) map to canonical tables.
    - If the inferred table is present in FROM/JOIN aliases, return its alias.
    - If there is a single table in the query, assume it belongs to it.

    Returns "" when no safe inference is possible.
    """

    if not column_name:
        return ""

    lower_col = column_name.lower()
    prefix_to_table = {
        "ss_": "store_sales",
        "cs_": "catalog_sales",
        "ws_": "web_sales",
        "d_": "date_dim",
        "i_": "item",
    }

    inferred_table = ""
    for prefix, table in prefix_to_table.items():
        if lower_col.startswith(prefix):
            inferred_table = table
            break

    if inferred_table:
        for alias, table_name in alias_to_table.items():
            if table_name == inferred_table:
                return alias

    if len(alias_to_table) == 1:
        return next(iter(alias_to_table.keys()))

    return ""


def _build_alias_to_table_map(sql: str) -> dict[str, str]:
    """Build alias->table map from the query's table references."""
    alias_to_table: dict[str, str] = {}
    for table_ref in extract_tables(sql):
        # Use alias if present, otherwise the table name itself.
        alias = (table_ref.alias or table_ref.table or "").strip()
        table_name = (table_ref.table or "").strip()
        if not alias or not table_name:
            continue
        alias_to_table[alias.lower()] = table_name.lower()
    return alias_to_table


def _detect_operator_near_column(clause_body: str, match_end: int) -> str:
    """Detect the comparison operator near a column reference.

    Looks for operators after the column (e.g., col = value, col > 10).

    Args:
        clause_body: The SQL clause text
        match_end: Position after the column match

    Returns:
        Operator string ("=", "<", ">", "<=", ">=", "<>", "!=", "BETWEEN", "IN", "LIKE")
        or empty string if not found
    """
    # Look at the text after the column match
    after_text = clause_body[match_end : match_end + 20].strip().upper()

    # Check for operators in order of specificity
    if after_text.startswith("<="):
        return "<="
    elif after_text.startswith(">="):
        return ">="
    elif after_text.startswith("<>"):
        return "<>"
    elif after_text.startswith("!="):
        return "!="
    elif after_text.startswith("="):
        return "="
    elif after_text.startswith("<"):
        return "<"
    elif after_text.startswith(">"):
        return ">"
    elif after_text.startswith("BETWEEN"):
        return "BETWEEN"
    elif after_text.startswith("IN"):
        return "IN"
    elif after_text.startswith("LIKE"):
        return "LIKE"
    elif after_text.startswith("IS"):
        return "IS"

    return ""


def _extract_columns_from_clause(
    sql: str, clause_keyword: str, context: str
) -> list[ColumnReference]:
    """Extract column references from a clause using simple heuristics.

    This focuses on Databricks-style analytic queries where clustering keys tend
    to be referenced in WHERE and JOIN ... ON conditions.
    """
    if not sql:
        return []

    alias_to_table = _build_alias_to_table_map(sql)

    clean_sql = remove_comments(sql)
    upper_sql = clean_sql.upper()
    kw = clause_keyword.upper()
    start = upper_sql.find(kw)
    if start < 0:
        return []

    clause_body = clean_sql[start + len(kw) :]
    # Stop at the next major clause boundary.
    stop_keywords = [
        " WHERE ",
        " GROUP ",
        " ORDER ",
        " HAVING ",
        " LIMIT ",
        " UNION ",
        " INTERSECT ",
        " EXCEPT ",
        " QUALIFY ",
        " WINDOW ",
    ]
    upper_body = clause_body.upper()
    stop_pos = len(clause_body)
    for stop_kw in stop_keywords:
        pos = upper_body.find(stop_kw)
        if pos >= 0:
            stop_pos = min(stop_pos, pos)
    clause_body = clause_body[:stop_pos]

    # Grab qualified columns like ss.ss_item_sk or `ss`.`ss_item_sk`.
    # Avoid numeric literals and function calls by requiring an identifier on both sides.
    pattern = r"\b([A-Za-z_][\w$]*)\s*\.\s*([A-Za-z_][\w$]*)\b"
    seen: set[tuple[str, str, str]] = set()
    out: list[ColumnReference] = []
    for m in re.finditer(pattern, clause_body):
        table_alias, col = m.group(1), m.group(2)
        key = (context, table_alias.lower(), col.lower())
        if key in seen:
            continue
        seen.add(key)
        operator = _detect_operator_near_column(clause_body, m.end())
        out.append(
            ColumnReference(
                column_name=col,
                table_alias=table_alias,
                context=context,
                operator=operator,
            )
        )

    # Extract unqualified column references and try to infer their table.
    # Keep this conservative to avoid accidentally recommending wrong keys.
    # - only identifiers that look like columns (letters/underscore start)
    # - exclude SQL keywords
    # - exclude function calls (name followed by '(')
    # - exclude anything already matched as qualified
    unqualified_pattern = r"\b([A-Za-z_][\w$]*)\b(?!\s*\()"
    excluded = {
        "and",
        "or",
        "not",
        "in",
        "is",
        "null",
        "like",
        "between",
        "exists",
        "case",
        "when",
        "then",
        "else",
        "end",
        "as",
        "on",
        "join",
        "inner",
        "left",
        "right",
        "full",
        "outer",
        "where",
        "group",
        "order",
        "having",
        "limit",
        "union",
        "intersect",
        "except",
        "qualify",
        "true",
        "false",
    }

    for m in re.finditer(unqualified_pattern, clause_body):
        col = m.group(1)

        # Skip if this token is part of a qualified reference.
        before = clause_body[m.start() - 1] if m.start() > 0 else ""
        after = clause_body[m.end()] if m.end() < len(clause_body) else ""
        if before == "." or after == ".":
            continue

        lower_col = col.lower()
        if lower_col in excluded:
            continue

        inferred_alias = _infer_table_alias_from_unqualified_column(col, alias_to_table)
        if not inferred_alias:
            continue

        key = (context, inferred_alias.lower(), lower_col)
        if key in seen:
            continue
        seen.add(key)
        operator = _detect_operator_near_column(clause_body, m.end())
        out.append(
            ColumnReference(
                column_name=col,
                table_alias=inferred_alias,
                context=context,
                operator=operator,
            )
        )
    return out


def _classify_join_type_from_sql(join_text: str) -> str:
    """Classify join type from SQL text.

    Args:
        join_text: JOIN clause text

    Returns:
        Join type string
    """
    upper_text = join_text.upper()

    if "CROSS" in upper_text:
        return "CROSS JOIN"
    elif "FULL OUTER" in upper_text or "FULL JOIN" in upper_text:
        return "FULL OUTER JOIN"
    elif "LEFT OUTER" in upper_text or "LEFT JOIN" in upper_text:
        return "LEFT JOIN"
    elif "RIGHT OUTER" in upper_text or "RIGHT JOIN" in upper_text:
        return "RIGHT JOIN"
    elif "INNER" in upper_text:
        return "INNER JOIN"
    elif "JOIN" in upper_text:
        return "INNER JOIN"

    return "UNKNOWN"


def _extract_join_edges(sql: str) -> list[JoinEdge]:
    """Extract JOIN edges (table pairs) from SQL query.

    Args:
        sql: SQL query string

    Returns:
        List of JoinEdge objects representing each JOIN operation
    """
    if not sql:
        return []

    try:
        clean_sql = remove_comments(sql)
        parsed = sqlparse.parse(clean_sql)
    except Exception as e:
        logger.warning(f"Failed to parse SQL for join edge extraction: {e}")
        return []

    join_edges: list[JoinEdge] = []

    for statement in parsed:
        # Track CTE names to exclude them
        cte_names: set[str] = set()

        # First pass: collect CTE names
        for token in statement.flatten():
            upper_val = str(token).upper().strip()
            if upper_val == "WITH":
                continue

        # Extract CTE names using regex (simpler approach)
        upper_sql = clean_sql.upper()
        cte_pattern = r"\bWITH\b\s+(\w+)\s+AS\s*\("
        for match in re.finditer(cte_pattern, upper_sql, re.IGNORECASE):
            cte_names.add(match.group(1).lower())
        additional_cte_pattern = r",\s*(\w+)\s+AS\s*\("
        for match in re.finditer(additional_cte_pattern, upper_sql, re.IGNORECASE):
            cte_names.add(match.group(1).lower())

        # Track current "left side" (accumulated tables from FROM/previous JOINs)
        left_table = ""
        left_alias = ""

        def get_table_info(identifier: Identifier) -> tuple[str, str]:
            """Extract table name and alias from identifier."""
            # Get alias first
            alias = identifier.get_alias() or ""

            # Get full table name by removing alias part from string representation
            full_str = str(identifier).strip()
            if alias:
                # Remove "AS alias" or just "alias" from the end
                full_str = re.sub(
                    rf"\s+(?:AS\s+)?{re.escape(alias)}\s*$",
                    "",
                    full_str,
                    flags=re.IGNORECASE,
                ).strip()

            # Clean up quotes
            full_str = full_str.strip("`").strip('"').strip("'")

            return full_str, alias

        def process_tokens_for_joins(token_list: Any, cte_names_local: set[str]) -> None:
            """Process tokens to extract JOIN edges."""
            nonlocal left_table, left_alias

            tokens = list(token_list.tokens) if hasattr(token_list, "tokens") else []
            i = 0

            while i < len(tokens):
                token = tokens[i]
                token_str = str(token).upper().strip()

                # Handle FROM clause - set initial left table
                if token.ttype is Keyword and token_str == "FROM":
                    # Look for next identifier
                    j = i + 1
                    while j < len(tokens):
                        next_token = tokens[j]
                        if isinstance(next_token, Identifier):
                            name, alias = get_table_info(next_token)
                            if name:
                                # Include CTE names as valid left tables for JOIN
                                left_table = name
                                left_alias = alias
                            break
                        elif isinstance(next_token, IdentifierList):
                            # FROM a, b - use first table as left
                            for ident in next_token.get_identifiers():
                                if isinstance(ident, Identifier):
                                    name, alias = get_table_info(ident)
                                    if name:
                                        # Include CTE names as valid left tables
                                        left_table = name
                                        left_alias = alias
                                        break
                            break
                        elif isinstance(next_token, Parenthesis):
                            # Subquery - mark as SUBQUERY
                            left_table = "(SUBQUERY)"
                            left_alias = ""
                            break
                        elif not next_token.is_whitespace:
                            break
                        j += 1

                # Handle JOIN clause
                elif token.ttype is Keyword and "JOIN" in token_str:
                    join_type = _classify_join_type_from_sql(token_str)

                    # Look for right table
                    j = i + 1
                    right_table = ""
                    right_alias = ""
                    while j < len(tokens):
                        next_token = tokens[j]
                        if isinstance(next_token, Identifier):
                            name, alias = get_table_info(next_token)
                            if name:
                                right_table = name
                                right_alias = alias
                            break
                        elif isinstance(next_token, Parenthesis):
                            # JOIN (subquery)
                            right_table = "(SUBQUERY)"
                            right_alias = ""
                            break
                        elif not next_token.is_whitespace:
                            # Check if it's a compound keyword like "LEFT OUTER"
                            if next_token.ttype is Keyword:
                                j += 1
                                continue
                            break
                        j += 1

                    # Create join edge if we have both sides
                    if left_table and right_table:
                        # Skip if right table is CTE
                        if right_table.lower() not in cte_names_local:
                            join_edges.append(
                                JoinEdge(
                                    join_type=join_type,
                                    left_table=left_table,
                                    left_alias=left_alias,
                                    right_table=right_table,
                                    right_alias=right_alias,
                                )
                            )
                            # Update left side for next JOIN
                            left_table = right_table
                            left_alias = right_alias

                # Reset on certain keywords (new query context)
                elif token.ttype is Keyword and token_str in ("UNION", "INTERSECT", "EXCEPT"):
                    left_table = ""
                    left_alias = ""

                # Recurse into parentheses (subqueries) and identifiers (CTEs)
                if isinstance(token, Parenthesis):
                    # Save current state
                    saved_left = left_table
                    saved_alias = left_alias
                    process_tokens_for_joins(token, cte_names_local)
                    # Restore state after subquery
                    left_table = saved_left
                    left_alias = saved_alias
                elif isinstance(token, Identifier):
                    # CTE definitions are parsed as Identifier containing Parenthesis
                    # Recurse into Identifier to find nested Parenthesis (CTE body)
                    if hasattr(token, "tokens"):
                        for inner_token in token.tokens:
                            if isinstance(inner_token, Parenthesis):
                                saved_left = left_table
                                saved_alias = left_alias
                                process_tokens_for_joins(inner_token, cte_names_local)
                                left_table = saved_left
                                left_alias = saved_alias

                i += 1

        process_tokens_for_joins(statement, cte_names)

    return join_edges


def analyze_structure(sql: str) -> QueryStructure:
    """Analyze the structure of a SQL query.

    Args:
        sql: SQL query string

    Returns:
        QueryStructure with analysis results
    """
    if not sql:
        return QueryStructure()

    structure = QueryStructure()

    try:
        clean_sql = remove_comments(sql)
        upper_sql = clean_sql.upper()
        parsed = sqlparse.parse(clean_sql)

        if not parsed:
            return structure

        statement = parsed[0]

        # Determine statement type
        stmt_type = statement.get_type()
        structure.statement_type = stmt_type or "UNKNOWN"
    except Exception as e:
        logger.warning(f"Failed to parse SQL for structure analysis: {e}")
        # Try to extract basic info from raw SQL using regex
        upper_sql = sql.upper()
        if "SELECT" in upper_sql:
            structure.statement_type = "SELECT"
        elif "INSERT" in upper_sql:
            structure.statement_type = "INSERT"
        elif "UPDATE" in upper_sql:
            structure.statement_type = "UPDATE"
        elif "DELETE" in upper_sql:
            structure.statement_type = "DELETE"
        else:
            structure.statement_type = "UNKNOWN"

    # Count joins
    structure.join_count = len(re.findall(r"\bJOIN\b", upper_sql, re.IGNORECASE))

    # Extract join types
    join_type_pattern = r"((?:LEFT|RIGHT|FULL|INNER|CROSS)(?:\s+OUTER)?\s+JOIN|JOIN)"
    for match in re.finditer(join_type_pattern, upper_sql, re.IGNORECASE):
        join_type = _classify_join_type_from_sql(match.group(1))
        if join_type not in structure.join_types:
            structure.join_types.append(join_type)

    # Extract join edges (table pairs)
    structure.join_edges = _extract_join_edges(sql)

    # Count subqueries (SELECT within parentheses, not CTEs)
    # Simple heuristic: count SELECT after opening parenthesis
    structure.subquery_count = len(re.findall(r"\(\s*SELECT\b", upper_sql, re.IGNORECASE))

    # Extract CTEs (WITH clause)
    cte_pattern = r"\bWITH\b\s+(.*?)\s+AS\s*\("
    cte_matches = re.findall(cte_pattern, upper_sql, re.IGNORECASE | re.DOTALL)
    for cte_name in cte_matches:
        clean_name = cte_name.strip().split()[-1]  # Get last word (the name)
        if clean_name and clean_name not in ("RECURSIVE",):
            structure.cte_names.append(clean_name.lower())

    # Also find additional CTEs after commas
    additional_cte_pattern = r",\s*(\w+)\s+AS\s*\("
    for match in re.finditer(additional_cte_pattern, upper_sql, re.IGNORECASE):
        cte_name = match.group(1).lower()
        if cte_name not in structure.cte_names:
            structure.cte_names.append(cte_name)

    structure.cte_count = len(structure.cte_names)

    # Extract aggregate functions
    agg_functions = ["COUNT", "SUM", "AVG", "MIN", "MAX", "COLLECT_LIST", "COLLECT_SET"]
    for func in agg_functions:
        if re.search(rf"\b{func}\s*\(", upper_sql, re.IGNORECASE):
            structure.aggregate_functions.append(func)

    # Extract window functions
    if re.search(r"\bOVER\s*\(", upper_sql, re.IGNORECASE):
        window_funcs = [
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "NTILE",
            "LAG",
            "LEAD",
            "FIRST_VALUE",
            "LAST_VALUE",
        ]
        for func in window_funcs:
            if re.search(rf"\b{func}\s*\(", upper_sql, re.IGNORECASE):
                structure.window_functions.append(func)

    # Check for various clauses
    structure.has_distinct = bool(re.search(r"\bDISTINCT\b", upper_sql, re.IGNORECASE))
    structure.has_group_by = bool(re.search(r"\bGROUP\s+BY\b", upper_sql, re.IGNORECASE))
    structure.has_order_by = bool(re.search(r"\bORDER\s+BY\b", upper_sql, re.IGNORECASE))
    structure.has_limit = bool(re.search(r"\bLIMIT\b", upper_sql, re.IGNORECASE))
    structure.has_union = bool(re.search(r"\bUNION\b", upper_sql, re.IGNORECASE))

    # Calculate complexity score
    complexity = 0
    complexity += structure.join_count * 2
    complexity += structure.subquery_count * 3
    complexity += structure.cte_count * 2
    complexity += len(structure.aggregate_functions)
    complexity += len(structure.window_functions) * 2
    complexity += 1 if structure.has_distinct else 0
    complexity += 1 if structure.has_group_by else 0
    complexity += 1 if structure.has_order_by else 0
    complexity += 2 if structure.has_union else 0

    structure.complexity_score = complexity

    return structure


def analyze_sql(sql: str) -> SQLAnalysis:
    """Perform complete SQL analysis.

    Args:
        sql: Raw SQL query string

    Returns:
        SQLAnalysis with all analysis results
    """
    if not sql or not sql.strip():
        return SQLAnalysis()

    columns: list[ColumnReference] = []
    try:
        # WHERE clause columns
        columns.extend(_extract_columns_from_clause(sql, "WHERE", context="where"))

        # JOIN ... ON clause columns (collect all ON occurrences)
        clean_sql = remove_comments(sql)
        for match in re.finditer(r"\bON\b", clean_sql, re.IGNORECASE):
            on_body = clean_sql[match.end() :]
            upper_body = on_body.upper()
            stop_keywords = [
                " JOIN ",
                " WHERE ",
                " GROUP ",
                " ORDER ",
                " HAVING ",
                " LIMIT ",
                " UNION ",
                " INTERSECT ",
                " EXCEPT ",
                " QUALIFY ",
            ]
            stop_pos = len(on_body)
            for stop_kw in stop_keywords:
                pos = upper_body.find(stop_kw)
                if pos >= 0:
                    stop_pos = min(stop_pos, pos)
            on_clause = on_body[:stop_pos]
            # Reuse the same extraction logic (qualified + inferred unqualified).
            # Wrap the ON clause to make it a "WHERE"-like chunk.
            columns.extend(
                _extract_columns_from_clause(
                    f"SELECT 1 WHERE {on_clause}",
                    "WHERE",
                    context="join",
                )
            )

        # Dedupe
        seen: set[tuple[str, str, str]] = set()
        unique: list[ColumnReference] = []
        for c in columns:
            key = (c.context, c.table_alias.lower(), c.column_name.lower())
            if key in seen:
                continue
            seen.add(key)
            unique.append(c)
        columns = unique
    except Exception as e:
        logger.warning(f"Failed to extract columns: {e}")
        columns = []

    return SQLAnalysis(
        raw_sql=sql,
        formatted_sql=format_sql(sql),
        tables=extract_tables(sql),
        columns=columns,
        structure=analyze_structure(sql),
    )
