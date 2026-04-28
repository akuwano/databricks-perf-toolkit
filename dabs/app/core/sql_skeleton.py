"""V6 SQL skeleton extraction (Week 5 Day 2).

Replaces the v5.19 ``prompts.py:2285`` blind 3000-char truncate with a
sqlglot-based structural summary that preserves CTE / JOIN / GROUP BY
shape but drops literal values and expression bodies.

Strategy (TODO.md "SQL スケルトン抽出" + W5 Day 1 design):

1. Apply skeleton only when SQL is "large + structured":
     len(sql) > 3000  AND  (cte_count >= 2 OR join_count >= 3 OR union_count >= 1 OR subquery_count >= 2)

2. Skeleton preserves:
     - WITH/CTE name + reference graph
     - FROM/JOIN type + counterpart table
     - WHERE/ON predicate *shape* (eq/range/like/in/exists/or-heavy)
     - GROUP BY / ORDER BY / HAVING column names
     - SELECT column count
     - DISTINCT / LIMIT / UNION arity

3. Skeleton drops:
     - literal values
     - column lists (replaced with "<N cols>")
     - complex expression bodies
     - function call arguments

4. Fallback:
     - sqlglot parse failed   -> head+tail (1500 chars each)
     - head+tail too short    -> legacy truncate (warn)

5. MERGE / CREATE VIEW AS / INSERT...SELECT bypass skeletonization
   in W5 (initial scope). They go straight to head+tail or full SQL.

The output is a `SkeletonResult` dataclass that downstream callers
(canonical Action.fix_sql_skeleton, scorers) consume directly.

See: docs/v6/sql_skeleton_design.md
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_BUDGET_CHARS = 2500
DEFAULT_FULLSQL_THRESHOLD = 3000  # below this = use full SQL verbatim


@dataclass
class SkeletonResult:
    """Outcome of attempting to build a SQL skeleton."""

    skeleton: str
    method: str  # "sqlglot" | "head_tail" | "truncate" | "fullsql" | "bypass"
    parse_success: bool
    cte_count: int = 0
    join_count: int = 0
    union_count: int = 0
    subquery_count: int = 0
    select_column_count: int = 0
    original_chars: int = 0
    skeleton_chars: int = 0
    bypass_reason: str = ""
    notes: list[str] = field(default_factory=list)

    @property
    def compression_ratio(self) -> float:
        if self.original_chars <= 0:
            return 1.0
        return round(self.skeleton_chars / self.original_chars, 4)


# ---------------------------------------------------------------------------
# Bypass / shortcut detection
# ---------------------------------------------------------------------------


_BYPASS_RE = re.compile(
    r"^\s*(MERGE\s+INTO|CREATE\s+(OR\s+REPLACE\s+)?(MATERIALIZED\s+)?VIEW|"
    r"INSERT\s+(OVERWRITE\s+)?(INTO\s+)?|UPDATE\s+|DELETE\s+FROM)",
    re.IGNORECASE,
)


def _bypass_reason(sql: str) -> str | None:
    """Return reason string if SQL should bypass skeletonization, else None.

    V6.1: when `feature_flags.sql_skeleton_extended()` is on, MERGE /
    CREATE VIEW / INSERT branches are NO LONGER bypassed — they go to
    structure extractors instead. UPDATE / DELETE remain bypassed in
    V6.1 (low-priority, kept for V6.2+).
    """
    if not sql:
        return "empty"

    try:
        from core import feature_flags as _ff  # noqa: WPS433
        ext = _ff.sql_skeleton_extended()
    except (ImportError, AttributeError):
        ext = False

    m = _BYPASS_RE.match(sql)
    if not m:
        return None

    head = m.group(1).split()[0].lower()
    if ext and head in {"merge", "create", "insert"}:
        # The dedicated extractor branches handle these.
        return None
    return f"unsupported_statement_type:{head}"


# ---------------------------------------------------------------------------
# Cheap heuristics (used to decide whether skeleton is worthwhile)
# ---------------------------------------------------------------------------


_CTE_RE = re.compile(r"\bWITH\b\s+|\b\w+\s+AS\s*\(", re.IGNORECASE)
_JOIN_RE = re.compile(r"\bJOIN\b", re.IGNORECASE)
_UNION_RE = re.compile(r"\bUNION\s+(ALL\s+)?", re.IGNORECASE)


def _heuristic_counts(sql: str) -> tuple[int, int, int]:
    """Cheap regex counts of CTE/JOIN/UNION (for applicability gate)."""
    cte = max(0, len(_CTE_RE.findall(sql)) - 1)  # -1 for the leading WITH itself
    join = len(_JOIN_RE.findall(sql))
    union = len(_UNION_RE.findall(sql))
    return cte, join, union


def _is_skeleton_worthwhile(sql: str) -> bool:
    """True when SQL is large enough AND structurally complex enough."""
    if len(sql) <= DEFAULT_FULLSQL_THRESHOLD:
        return False
    cte, join, union = _heuristic_counts(sql)
    return cte >= 2 or join >= 3 or union >= 1


# ---------------------------------------------------------------------------
# sqlglot-based skeleton
# ---------------------------------------------------------------------------


def _try_parse(sql: str):
    """Try sqlglot parse with databricks→spark fallback. Returns AST root or None."""
    try:
        import sqlglot  # noqa: WPS433
    except ImportError:
        return None
    for dialect in ("databricks", "spark"):
        try:
            return sqlglot.parse_one(sql, dialect=dialect)
        except Exception:  # noqa: BLE001 — sqlglot raises various
            continue
    return None


def _classify_predicate(expr) -> str:
    """Classify a boolean expression into a coarse shape label."""
    try:
        from sqlglot import exp  # noqa: WPS433
    except ImportError:
        return "unknown"
    if expr is None:
        return "none"
    # OR-heavy if there are 3+ ORs at the top level
    or_count = sum(1 for _ in expr.find_all(exp.Or))
    if or_count >= 3:
        return "or_heavy"
    # Specific kinds
    if list(expr.find_all(exp.Like)):
        return "like"
    if list(expr.find_all(exp.In)):
        return "in"
    if list(expr.find_all(exp.Exists)):
        return "exists"
    if list(expr.find_all(exp.Between)) or any(expr.find_all(exp.GT)) or any(expr.find_all(exp.LT)):
        return "range"
    if list(expr.find_all(exp.EQ)):
        return "eq"
    return "complex"


def _column_name(node) -> str:
    """Best-effort short name for a column / identifier node."""
    try:
        return node.alias_or_name
    except Exception:  # noqa: BLE001
        return str(node)[:40]


def _table_qualified(node) -> str:
    """Return a 'db.schema.table' form when available, else table name."""
    try:
        from sqlglot import exp  # noqa: WPS433
    except ImportError:
        return str(node)
    if isinstance(node, exp.Table):
        parts = [str(p) for p in (node.args.get("catalog"), node.args.get("db"), node.this) if p is not None]
        return ".".join(parts) or str(node.this)
    return str(node)[:40]


_MERGE_TOPLEVEL_RE = re.compile(r"^\s*MERGE\s+INTO", re.IGNORECASE)
_CREATE_VIEW_RE = re.compile(r"^\s*CREATE\s+(OR\s+REPLACE\s+)?(MATERIALIZED\s+)?VIEW", re.IGNORECASE)
_INSERT_TOPLEVEL_RE = re.compile(r"^\s*INSERT\s+(OVERWRITE\s+)?(INTO\s+)?", re.IGNORECASE)


def _is_merge(sql: str) -> bool:
    return bool(_MERGE_TOPLEVEL_RE.match(sql or ""))


def _is_create_view(sql: str) -> bool:
    return bool(_CREATE_VIEW_RE.match(sql or ""))


def _is_insert(sql: str) -> bool:
    return bool(_INSERT_TOPLEVEL_RE.match(sql or ""))


def _build_merge_skeleton(ast, sql: str, budget: int) -> str:
    """V6.1: structured skeleton for MERGE INTO ...

    Uses sqlglot AST when available; falls back to a regex-driven
    summary when AST traversal yields nothing.
    """
    try:
        from sqlglot import exp  # noqa: WPS433
    except ImportError:
        return ""

    lines: list[str] = []
    merge = ast if isinstance(ast, exp.Merge) else ast.find(exp.Merge)
    if merge is not None:
        # target
        target = _table_qualified(merge.this) if merge.this is not None else "?"
        lines.append(f"MERGE INTO {target}")
        # source
        using = merge.args.get("using")
        if using is not None:
            if isinstance(using, exp.Table):
                lines.append(f"USING {_table_qualified(using)}")
            else:
                inner_tbls = list(using.find_all(exp.Table)) if hasattr(using, "find_all") else []
                if inner_tbls:
                    names = [_table_qualified(t) for t in inner_tbls[:3]]
                    lines.append(f"USING (SELECT FROM {', '.join(names)})")
                else:
                    lines.append("USING (subquery)")
        # ON
        on = merge.args.get("on")
        if on is not None:
            lines.append(f"ON [{_classify_predicate(on)}]")
        # WHEN clauses
        whens = merge.args.get("whens")
        if whens is not None:
            try:
                for w in whens.expressions:
                    matched = w.args.get("matched")
                    not_matched_by_source = w.args.get("source")
                    action_kind = "?"
                    body = w.args.get("then")
                    if isinstance(body, exp.Update):
                        action_kind = "UPDATE"
                    elif isinstance(body, exp.Insert):
                        action_kind = "INSERT"
                    elif isinstance(body, exp.Delete):
                        action_kind = "DELETE"
                    when_kind = "MATCHED"
                    if not matched:
                        when_kind = "NOT MATCHED"
                    if not_matched_by_source:
                        when_kind = "NOT MATCHED BY SOURCE"
                    cond = w.args.get("condition")
                    cond_shape = _classify_predicate(cond) if cond is not None else "always"
                    lines.append(f"WHEN {when_kind} [{cond_shape}] THEN {action_kind}")
            except Exception:  # noqa: BLE001
                lines.append("WHEN ... (parse partial)")

    if not lines:
        # Fallback line: regex-based scan
        target_match = re.match(r"\s*MERGE\s+INTO\s+([\w.`]+)", sql, re.IGNORECASE)
        if target_match:
            lines.append(f"MERGE INTO {target_match.group(1).strip('`')}")
        when_count = len(re.findall(r"\bWHEN\s+(MATCHED|NOT\s+MATCHED)", sql, re.IGNORECASE))
        if when_count:
            lines.append(f"WHEN clauses: {when_count}")

    skeleton = "\n".join(lines)
    if len(skeleton) > budget:
        skeleton = skeleton[: budget - 5] + "\n…"
    return skeleton


def _build_view_skeleton(ast, sql: str, budget: int) -> str:
    """V6.1: structured skeleton for CREATE [OR REPLACE] [MATERIALIZED] VIEW."""
    try:
        from sqlglot import exp  # noqa: WPS433
    except ImportError:
        return ""

    lines: list[str] = []
    head_match = _CREATE_VIEW_RE.match(sql)
    or_replace = "OR REPLACE" if (head_match and head_match.group(1)) else ""
    materialized = "MATERIALIZED" if (head_match and head_match.group(2)) else ""

    name_match = re.match(
        r"\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+([\w.`]+)",
        sql,
        re.IGNORECASE,
    )
    name = name_match.group(1).strip("`") if name_match else "?"
    head = " ".join(p for p in ["CREATE", or_replace, materialized, "VIEW", name] if p)
    lines.append(head)

    # Inner SELECT — reuse the standard sqlglot skeleton on the AST
    select = ast.find(exp.Select)
    if select is not None:
        # build_sqlglot_skeleton was designed for a top-level AST; we hand
        # it the Select sub-tree by wrapping in a small synthetic walker.
        inner = _build_sqlglot_skeleton(select, max(0, budget - len(head) - 20))
        if inner:
            lines.append("AS")
            lines.append(inner)

    skeleton = "\n".join(lines)
    if len(skeleton) > budget:
        skeleton = skeleton[: budget - 5] + "\n…"
    return skeleton


def _build_insert_skeleton(ast, sql: str, budget: int) -> str:
    """V6.1: structured skeleton for INSERT [OVERWRITE] INTO ..."""
    try:
        from sqlglot import exp  # noqa: WPS433
    except ImportError:
        return ""

    lines: list[str] = []
    head_match = _INSERT_TOPLEVEL_RE.match(sql)
    overwrite = "OVERWRITE" if (head_match and head_match.group(1)) else ""

    name_match = re.match(
        r"\s*INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?([\w.`]+)",
        sql,
        re.IGNORECASE,
    )
    target = name_match.group(1).strip("`") if name_match else "?"
    head = " ".join(p for p in ["INSERT", overwrite, "INTO", target] if p)
    lines.append(head)

    select = ast.find(exp.Select)
    if select is not None:
        inner = _build_sqlglot_skeleton(select, max(0, budget - len(head) - 20))
        if inner:
            lines.append("AS")
            lines.append(inner)

    skeleton = "\n".join(lines)
    if len(skeleton) > budget:
        skeleton = skeleton[: budget - 5] + "\n…"
    return skeleton


def _build_sqlglot_skeleton(ast, budget: int) -> str:
    """Walk an sqlglot AST and emit a compact text skeleton."""
    try:
        from sqlglot import exp  # noqa: WPS433
    except ImportError:
        return ""

    lines: list[str] = []

    # CTE block — sqlglot may put With either on the root (.args['with'])
    # or as a wrapping node. Search descendants too as a safety net.
    with_node = ast.args.get("with")
    if with_node is None:
        with_node = ast.find(exp.With)
    if with_node is not None:
        cte_names = []
        for cte in with_node.expressions:
            cte_names.append(cte.alias_or_name)
        if cte_names:
            lines.append(f"WITH {', '.join(cte_names)}")
            for cte in with_node.expressions:
                # one-line summary per CTE: name -> tables/joins
                inner = cte.this
                tbls = [_table_qualified(t) for t in inner.find_all(exp.Table)]
                tbls = list(dict.fromkeys(tbls))[:5]  # dedupe + cap
                join_kinds = []
                for j in inner.find_all(exp.Join):
                    kind = (j.args.get("kind") or "INNER").upper() if hasattr(j, "args") else "JOIN"
                    join_kinds.append(kind)
                if tbls:
                    lines.append(
                        f"  {cte.alias_or_name}: from={', '.join(tbls)}"
                        + (f" joins=[{', '.join(join_kinds)}]" if join_kinds else "")
                    )

    # Top-level SELECT
    select = ast.find(exp.Select)
    if select is not None:
        cols = select.args.get("expressions") or []
        lines.append(f"SELECT <{len(cols)} cols>{' DISTINCT' if select.args.get('distinct') else ''}")

        # FROM
        from_node = select.args.get("from")
        if from_node is not None:
            tbls = [_table_qualified(t) for t in from_node.find_all(exp.Table)]
            if tbls:
                lines.append(f"FROM {', '.join(tbls[:3])}{' …' if len(tbls) > 3 else ''}")

        # JOINs
        joins = list(select.find_all(exp.Join))
        for j in joins[:6]:  # cap to first 6
            kind = (j.args.get("kind") or "INNER").upper() if hasattr(j, "args") else "JOIN"
            target = _table_qualified(j.this)
            on = j.args.get("on")
            shape = _classify_predicate(on)
            lines.append(f"{kind} JOIN {target} ON [{shape}]")
        if len(joins) > 6:
            lines.append(f"... +{len(joins) - 6} more joins")

        # WHERE
        where = select.args.get("where")
        if where is not None:
            lines.append(f"WHERE [{_classify_predicate(where.this)}]")

        # GROUP BY / HAVING / ORDER BY
        group = select.args.get("group")
        if group is not None:
            cols = [_column_name(c) for c in group.expressions]
            lines.append(f"GROUP BY {', '.join(cols[:8])}{' …' if len(cols) > 8 else ''}")
        having = select.args.get("having")
        if having is not None:
            lines.append(f"HAVING [{_classify_predicate(having.this)}]")
        order = select.args.get("order")
        if order is not None:
            cols = [_column_name(o) for o in order.expressions]
            lines.append(f"ORDER BY {', '.join(cols[:6])}{' …' if len(cols) > 6 else ''}")

        # LIMIT
        limit = select.args.get("limit")
        if limit is not None:
            lines.append("LIMIT *")

    # UNION arity
    unions = list(ast.find_all(exp.Union))
    if unions:
        lines.append(f"UNION (×{len(unions)})")

    skeleton = "\n".join(lines)
    if len(skeleton) > budget:
        skeleton = skeleton[: budget - 5] + "\n…"
    return skeleton


# ---------------------------------------------------------------------------
# Fallback methods
# ---------------------------------------------------------------------------


def _head_tail_fallback(sql: str, head_chars: int = 1250, tail_chars: int = 1250) -> str:
    """Codex W5 review: 1500/1500 default exceeded the 2500 char_budget,
    risking tail being truncated by later budget enforcement. Default to
    1250/1250 so head+tail+marker fits inside the typical budget.
    """
    if len(sql) <= head_chars + tail_chars:
        return sql
    return f"{sql[:head_chars]}\n\n-- … truncated middle …\n\n{sql[-tail_chars:]}"


def _legacy_truncate(sql: str, budget: int) -> str:
    return sql[:budget] + "\n-- truncated"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_sql_skeleton(
    sql: str,
    *,
    char_budget: int = DEFAULT_BUDGET_CHARS,
    fallback_head_tail: bool = True,
) -> SkeletonResult:
    """Produce a SQL skeleton for prompt injection / canonical Action.

    Behavior:
      - Empty/whitespace SQL -> empty skeleton, method="fullsql"
      - Bypass statement types (MERGE, CREATE VIEW AS, INSERT, UPDATE, DELETE)
        -> method="bypass", skeleton = head+tail or full SQL
      - SQL not worth skeletonizing (short/simple) -> method="fullsql"
      - sqlglot parse OK -> method="sqlglot"
      - sqlglot parse fail + fallback enabled -> method="head_tail"
      - else -> method="truncate"
    """
    sql = sql or ""
    original_chars = len(sql)

    if not sql.strip():
        return SkeletonResult(
            skeleton="",
            method="fullsql",
            parse_success=True,
            original_chars=original_chars,
            skeleton_chars=0,
        )

    bypass = _bypass_reason(sql)
    if bypass:
        # Use head+tail for very long bypass statements; full SQL otherwise
        skeleton = (
            _head_tail_fallback(sql)
            if original_chars > char_budget * 2
            else sql
        )
        return SkeletonResult(
            skeleton=skeleton,
            method="bypass",
            parse_success=False,
            original_chars=original_chars,
            skeleton_chars=len(skeleton),
            bypass_reason=bypass,
            notes=[f"bypass:{bypass}"],
        )

    # Short/simple SQL: pass full text through.
    # V6.1: when extended-skeleton flag is on AND the statement is a
    # MERGE / CREATE VIEW / INSERT, skip the worthwhile gate so we
    # always emit the structured shape (the value of the structured
    # form is independent of length).
    try:
        from core import feature_flags as _ff_short  # noqa: WPS433
        ext_short = _ff_short.sql_skeleton_extended()
    except (ImportError, AttributeError):
        ext_short = False
    is_special = ext_short and (_is_merge(sql) or _is_create_view(sql) or _is_insert(sql))
    if not is_special and not _is_skeleton_worthwhile(sql):
        return SkeletonResult(
            skeleton=sql,
            method="fullsql",
            parse_success=True,
            original_chars=original_chars,
            skeleton_chars=original_chars,
            cte_count=_heuristic_counts(sql)[0],
            join_count=_heuristic_counts(sql)[1],
            union_count=_heuristic_counts(sql)[2],
        )

    # Try sqlglot
    ast = _try_parse(sql)
    if ast is not None:
        try:
            from sqlglot import exp  # noqa: WPS433

            cte_count = 0
            with_node = ast.args.get("with") or ast.find(exp.With)
            if with_node is not None:
                cte_count = len(with_node.expressions)
            join_count = sum(1 for _ in ast.find_all(exp.Join))
            union_count = sum(1 for _ in ast.find_all(exp.Union))
            subq_count = sum(1 for _ in ast.find_all(exp.Subquery))
            select_top = ast.find(exp.Select)
            select_cols = (
                len(select_top.args.get("expressions") or [])
                if select_top is not None
                else 0
            )
        except Exception:  # noqa: BLE001
            cte_count = join_count = union_count = subq_count = select_cols = 0

        # V6.1: MERGE / VIEW / INSERT structured extraction (only when
        # the extended flag is on and the SQL matches the relevant head).
        method = "sqlglot"
        skeleton_text = ""
        try:
            from core import feature_flags as _ff  # noqa: WPS433
            ext = _ff.sql_skeleton_extended()
        except (ImportError, AttributeError):
            ext = False

        if ext and _is_merge(sql):
            skeleton_text = _build_merge_skeleton(ast, sql, char_budget)
            method = "merge" if skeleton_text else "sqlglot"
        elif ext and _is_create_view(sql):
            skeleton_text = _build_view_skeleton(ast, sql, char_budget)
            method = "view" if skeleton_text else "sqlglot"
        elif ext and _is_insert(sql):
            skeleton_text = _build_insert_skeleton(ast, sql, char_budget)
            method = "insert" if skeleton_text else "sqlglot"

        if not skeleton_text:
            skeleton_text = _build_sqlglot_skeleton(ast, char_budget)
            method = "sqlglot"

        return SkeletonResult(
            skeleton=skeleton_text,
            method=method,
            parse_success=True,
            cte_count=cte_count,
            join_count=join_count,
            union_count=union_count,
            subquery_count=subq_count,
            select_column_count=select_cols,
            original_chars=original_chars,
            skeleton_chars=len(skeleton_text),
        )

    # Fallback path. Reserve ~50 chars for the truncation marker so the
    # produced skeleton stays inside char_budget.
    if fallback_head_tail:
        per_side = max(200, (char_budget - 50) // 2)
        skeleton = _head_tail_fallback(sql, head_chars=per_side, tail_chars=per_side)
        method = "head_tail"
    else:
        skeleton = _legacy_truncate(sql, char_budget)
        method = "truncate"

    return SkeletonResult(
        skeleton=skeleton,
        method=method,
        parse_success=False,
        original_chars=original_chars,
        skeleton_chars=len(skeleton),
        notes=["sqlglot_parse_failed"],
    )


def aggregate_parse_metrics(results: list[SkeletonResult]) -> dict[str, Any]:
    """Aggregate batch skeleton metrics across many SQLs.

    Returns: {
        parse_success_rate, skeleton_used_rate, avg_compression_ratio,
        method_distribution
    }
    """
    if not results:
        return {
            "parse_success_rate": 1.0,
            "skeleton_used_rate": 0.0,
            "avg_compression_ratio": 1.0,
            "method_distribution": {},
        }
    total = len(results)
    succ = sum(1 for r in results if r.parse_success)
    skel = sum(1 for r in results if r.method == "sqlglot")
    method_counts: dict[str, int] = {}
    for r in results:
        method_counts[r.method] = method_counts.get(r.method, 0) + 1
    avg_compression = sum(r.compression_ratio for r in results) / total
    return {
        "parse_success_rate": round(succ / total, 4),
        "skeleton_used_rate": round(skel / total, 4),
        "avg_compression_ratio": round(avg_compression, 4),
        "method_distribution": {k: round(v / total, 4) for k, v in method_counts.items()},
    }
