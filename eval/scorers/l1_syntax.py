"""L1: Syntax validity scorer for ActionCard fix_sql."""

from __future__ import annotations

import re

import sqlglot

from ..models import L1Score

# Evaluation rubric: supported Spark configs on Serverless SQL Warehouse.
# Owned by eval/ (not core/) because this is a scoring criterion, not analysis logic.
# https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-parameters
SERVERLESS_SUPPORTED_SPARK_CONFIGS = frozenset({
    "spark.sql.ansi.enabled",
    "spark.sql.legacy.timeParserPolicy",
    "spark.sql.files.maxPartitionBytes",
    "spark.sql.session.timeZone",
    "spark.databricks.execution.timeout",
    "spark.databricks.io.cache.enabled",
    "spark.databricks.sql.readOnlyExternalMetastore",
})

_SET_PATTERN = re.compile(r"^\s*SET\s+(spark\.\S+)\s*=", re.IGNORECASE)
_COMMENT_PATTERN = re.compile(r"^\s*--")


def score_l1(card, is_serverless: bool = False) -> L1Score:
    """Score an ActionCard's fix_sql for syntax validity and serverless compliance.

    - Empty fix_sql is not penalized (parses_ok=True, serverless_compliant=True).
    - Multi-statement fix_sql (semicolon-separated) is split and each checked.
    - SET statements are checked against SERVERLESS_SUPPORTED_SPARK_CONFIGS.
    - SQL statements are parsed with sqlglot (databricks → spark fallback).
    """
    if not card.fix_sql or not card.fix_sql.strip():
        return L1Score(
            card_index=0,
            has_fix_sql=False,
            parses_ok=True,
            serverless_compliant=True,
        )

    statements = _split_statements(card.fix_sql)
    if not statements:
        return L1Score(card_index=0, has_fix_sql=True, parses_ok=True, serverless_compliant=True)

    all_parse_ok = True
    parse_errors: list[str] = []
    unsupported_configs: list[str] = []

    for stmt in statements:
        # Check SET statements
        set_match = _SET_PATTERN.match(stmt)
        if set_match:
            config_key = set_match.group(1).rstrip(";").strip()
            if is_serverless and config_key not in SERVERLESS_SUPPORTED_SPARK_CONFIGS:
                unsupported_configs.append(config_key)
            continue  # SET statements are not SQL — skip parse check

        # Check ALTER TABLE (sqlglot handles this)
        # Check regular SQL
        parsed = _try_parse(stmt)
        if not parsed:
            all_parse_ok = False
            parse_errors.append(f"Parse error: {stmt[:80]}...")

    return L1Score(
        card_index=0,
        has_fix_sql=True,
        parses_ok=all_parse_ok,
        parse_error="; ".join(parse_errors) if parse_errors else "",
        serverless_compliant=len(unsupported_configs) == 0,
        unsupported_configs=unsupported_configs,
    )


def _split_statements(fix_sql: str) -> list[str]:
    """Split fix_sql into individual statements, filtering comments and blanks.

    Handles semicolons inside string literals by tracking quote state.
    """
    raw_stmts = _split_on_semicolons(fix_sql)
    results = []
    for raw in raw_stmts:
        # Strip and remove comment-only lines
        lines = []
        for line in raw.strip().splitlines():
            if not _COMMENT_PATTERN.match(line) and line.strip():
                lines.append(line)
        stmt = "\n".join(lines).strip()
        if stmt:
            results.append(stmt)
    return results


def _split_on_semicolons(sql: str) -> list[str]:
    """Split SQL on semicolons, respecting string literals (single/double quotes)."""
    parts = []
    current: list[str] = []
    in_quote: str | None = None
    i = 0
    while i < len(sql):
        ch = sql[i]
        if in_quote:
            current.append(ch)
            if ch == in_quote:
                # Check for escaped quote (doubled)
                if i + 1 < len(sql) and sql[i + 1] == in_quote:
                    current.append(sql[i + 1])
                    i += 2
                    continue
                in_quote = None
        elif ch in ("'", '"'):
            in_quote = ch
            current.append(ch)
        elif ch == ";":
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    if current:
        parts.append("".join(current))
    return parts


def _try_parse(sql: str) -> bool:
    """Try parsing SQL with sqlglot (databricks → spark fallback)."""
    for dialect in ("databricks", "spark"):
        try:
            sqlglot.parse_one(sql, dialect=dialect)
            return True
        except Exception:
            continue
    return False
