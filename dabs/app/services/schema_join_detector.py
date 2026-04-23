"""JOIN implicit CAST detection using past profiler_analysis_raw rows (Delta)."""

from __future__ import annotations

import base64
import gzip
import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_DECIMAL_RE = re.compile(r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)


def list_recent_analyses(
    conn: Any, limit: int = 20
) -> list[tuple[str, str | None, str | None, Any, str | None]]:
    """Load recent rows: raw JSON plus header analyzed_at / query_text (TableWriterConfig).

    Returns list of (analysis_id, query_id, analysis_json_compressed, analyzed_at, query_text).
    """
    from core.sql_safe import safe_fqn

    from services.table_writer import TableWriterConfig

    cfg = TableWriterConfig.from_env()
    if not cfg.http_path:
        return []
    lim = max(1, min(int(limit), 500))
    raw_fqn = safe_fqn(cfg.catalog, cfg.schema, "profiler_analysis_raw")
    hdr_fqn = safe_fqn(cfg.catalog, cfg.schema, "profiler_analysis_header")
    sql = f"""
SELECT r.analysis_id, r.query_id, r.analysis_json, h.analyzed_at, h.query_text
FROM {raw_fqn} r
LEFT JOIN {hdr_fqn} h ON r.analysis_id = h.analysis_id
ORDER BY h.analyzed_at DESC NULLS LAST
LIMIT {lim}
"""
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall() or []
        out: list[tuple[str, str | None, str | None, Any, str | None]] = []
        for row in rows:
            if not row:
                continue
            aid = str(row[0]) if row[0] is not None else ""
            qid = str(row[1]) if len(row) > 1 and row[1] is not None else None
            raw = row[2] if len(row) > 2 else None
            analyzed_at = row[3] if len(row) > 3 else None
            qtext = row[4] if len(row) > 4 else None
            if aid:
                out.append(
                    (
                        aid,
                        qid,
                        raw if raw is None else str(raw),
                        analyzed_at,
                        None if qtext is None else str(qtext),
                    )
                )
        return out
    except Exception:
        logger.info(
            "profiler tables not available or query failed; skipping JOIN cast scan",
            exc_info=True,
        )
        return []


def decode_analysis_json(raw: str | bytes | None) -> dict[str, Any] | None:
    """Decode gzip+base64 compressed JSON from profiler_analysis_raw (see table_writer._compress_json)."""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        s = raw.decode("ascii", errors="ignore")
    else:
        s = str(raw).strip()
    if not s:
        return None
    try:
        decoded = base64.b64decode(s)
        text = gzip.decompress(decoded).decode("utf-8")
        return json.loads(text)
    except Exception:
        try:
            return json.loads(s)
        except Exception:
            return None


def extract_join_pairs_from_sql(query_text: str) -> list[tuple[str, str, str, str]]:
    """Parse ON-clause equalities: (left_alias_or_table, left_col, right_alias_or_table, right_col)."""
    if not (query_text or "").strip():
        return []
    try:
        import sqlglot
        from sqlglot import exp
    except Exception:
        return []

    out: list[tuple[str, str, str, str]] = []
    try:
        parsed = sqlglot.parse_one(query_text, dialect="databricks")
    except Exception:
        try:
            parsed = sqlglot.parse_one(query_text, dialect="spark")
        except Exception:
            try:
                parsed = sqlglot.parse_one(query_text)
            except Exception:
                return []

    def is_simple_col(e: exp.Expression) -> bool:
        return isinstance(e, exp.Column)

    def col_parts(e: exp.Column) -> tuple[str, str]:
        tbl = str(e.table) if e.table else ""
        name = str(e.name)
        return (tbl, name)

    def walk_on(expr: exp.Expression | None) -> None:
        if expr is None:
            return
        if isinstance(expr, exp.And):
            walk_on(expr.left)
            walk_on(expr.right)
            return
        if isinstance(expr, exp.Paren):
            walk_on(expr.this)
            return
        if isinstance(expr, exp.EQ):
            left, right = expr.this, expr.expression
            if isinstance(left, exp.Cast) or isinstance(right, exp.Cast):
                return
            if not (is_simple_col(left) and is_simple_col(right)):
                return
            a1, c1 = col_parts(left)
            a2, c2 = col_parts(right)
            if not c1 or not c2:
                return
            out.append((a1, c1, a2, c2))
            return

    for join in parsed.find_all(exp.Join):
        on_expr = join.args.get("on")
        walk_on(on_expr)

    return out


def resolve_aliases(
    join_pairs: list[tuple[str, str, str, str]],
    tables_info: list[dict[str, Any]],
) -> list[tuple[str, str, str, str]]:
    """Map table aliases to lowercase full table names using sql_analysis.tables."""
    alias_map: dict[str, str] = {}
    for t in tables_info:
        if not isinstance(t, dict):
            continue
        alias = (t.get("alias") or "").strip().lower()
        fn = (t.get("full_name") or "").strip()
        if not fn:
            cat = (t.get("catalog") or "").strip()
            sch = (t.get("schema") or "").strip()
            tb = (t.get("table") or "").strip()
            if tb:
                fn = f"{cat}.{sch}.{tb}".strip()
        fn_l = fn.lower() if fn else ""
        if alias and fn_l:
            alias_map[alias] = fn_l
        tb = (t.get("table") or "").strip().lower()
        if tb and fn_l:
            alias_map[tb] = fn_l

    def res_tbl(x: str) -> str:
        xl = (x or "").strip().lower()
        return alias_map.get(xl, xl)

    resolved: list[tuple[str, str, str, str]] = []
    for a, ca, b, cb in join_pairs:
        t1 = res_tbl(a)
        t2 = res_tbl(b)
        resolved.append((t1, ca.strip(), t2, cb.strip()))
    return resolved


def _norm_dt(t: str) -> str:
    return re.sub(r"\s+", "", (t or "").lower())


def _is_string(t: str) -> bool:
    return _norm_dt(t) == "string"


def _is_integral(t: str) -> bool:
    u = _norm_dt(t)
    return u in ("int", "bigint", "smallint", "tinyint") or u.startswith("int(")


def _is_numeric_family(t: str) -> bool:
    u = _norm_dt(t)
    if _is_integral(t):
        return True
    if "decimal" in u or "double" in u or "float" in u or "numeric" in u:
        return True
    return False


def _decimal_prec_scale(t: str) -> tuple[int | None, int | None]:
    m = _DECIMAL_RE.match((t or "").strip())
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


def _severity_and_message(left_type: str, right_type: str) -> tuple[str, str]:
    a, b = left_type, right_type
    if _is_string(a) and _is_numeric_family(b):
        return (
            "CRITICAL",
            "STRING joined to numeric type causes implicit cast and poor join performance.",
        )
    if _is_string(b) and _is_numeric_family(a):
        return (
            "CRITICAL",
            "Numeric type joined to STRING causes implicit cast and poor join performance.",
        )
    pa, sa = _decimal_prec_scale(a)
    pb, sb = _decimal_prec_scale(b)
    if pa == 38 and sa == 0 and _is_integral(b):
        return (
            "HIGH",
            "DECIMAL(38,0) joined to integer family may cause unnecessary cast; prefer matching INTEGER types.",
        )
    if pb == 38 and sb == 0 and _is_integral(a):
        return "HIGH", "Integer joined to DECIMAL(38,0) may cause implicit cast; align types."
    if pa is not None and pb is not None and (pa, sa) != (pb, sb):
        return (
            "MEDIUM",
            "DECIMAL precision/scale differs across join keys; engine may insert casts.",
        )
    na, nb = _norm_dt(a), _norm_dt(b)
    if na in ("int",) and nb in ("bigint",) or nb in ("int",) and na in ("bigint",):
        return "LOW", "INT vs BIGINT join may cast the INT side; align types if possible."
    return "MEDIUM", "Join key types differ; implicit casts may hurt performance."


def _serialize_analyzed_at(at: Any) -> str | None:
    if at is None:
        return None
    if hasattr(at, "isoformat"):
        try:
            return at.isoformat()
        except Exception:
            pass
    s = str(at).strip()
    return s or None


def _trunc_query120(text: str | None) -> str:
    if not text:
        return ""
    s = " ".join(str(text).split())
    if len(s) <= 120:
        return s
    return s[:117] + "..."


def _tables_referenced_in_scope(
    tables_info: list[Any],
    user_catalog: str,
    user_schema: str,
    analyzed_tables: list[str],
) -> list[str]:
    """Short table names from sql_analysis.tables that belong to the analyzed schema scope."""
    uc = user_catalog.strip().lower()
    us = user_schema.strip().lower()
    analyzed_lower = {t.strip().lower() for t in analyzed_tables}
    out: list[str] = []
    seen: set[str] = set()
    for t in tables_info:
        if not isinstance(t, dict):
            continue
        cat = (t.get("catalog") or "").strip().lower()
        sch = (t.get("schema") or "").strip().lower()
        tb = (t.get("table") or "").strip()
        if not tb:
            continue
        if cat != uc or sch != us:
            continue
        tbn = tb.lower()
        if tbn not in analyzed_lower:
            continue
        if tbn not in seen:
            seen.add(tbn)
            out.append(tb)
    out.sort(key=str.lower)
    return out


def detect_type_mismatches(
    resolved_pairs: list[tuple[str, str, str, str]],
    type_map: dict[str, dict[str, str]],
    query_id: str,
) -> list[dict[str, Any]]:
    """Compare resolved join column types using type_map from schema analysis."""
    issues: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()

    def lookup_type(table_fqn: str, col: str) -> str | None:
        t = table_fqn.lower().strip()
        c = col.lower().strip()
        if t in type_map and c in type_map[t]:
            return type_map[t][c]
        short = t.split(".")[-1] if "." in t else t
        if short in type_map and c in type_map[short]:
            return type_map[short][c]
        return None

    for t1, c1, t2, c2 in resolved_pairs:
        key = (t1.lower(), c1.lower(), t2.lower(), c2.lower())
        if key in seen:
            continue
        seen.add(key)
        lt = lookup_type(t1, c1)
        rt = lookup_type(t2, c2)
        if lt is None or rt is None:
            continue
        if _norm_dt(lt) == _norm_dt(rt):
            continue
        sev, msg = _severity_and_message(lt, rt)
        issues.append(
            {
                "query_id": query_id,
                "left_table": t1,
                "left_column": c1,
                "left_type": lt,
                "right_table": t2,
                "right_column": c2,
                "right_type": rt,
                "severity": sev,
                "message": msg,
                "recommendation": (
                    "Cast explicitly to one shared type in ETL or align table DDL so join keys match."
                ),
            }
        )
    return issues


def collect_join_cast_issues(
    conn: Any,
    *,
    user_catalog: str,
    user_schema: str,
    analyzed_tables: list[str],
    type_map: dict[str, dict[str, str]],
    tables_filter: list[str] | None,
    limit: int = 20,
) -> tuple[list[dict[str, Any]], int, list[dict[str, Any]]]:
    """Run full pipeline; returns (issues, queries_scanned, scanned_analyses). Never raises."""
    rows = list_recent_analyses(conn, limit=limit)
    scanned = len(rows)
    if not rows:
        return [], 0, []

    analyzed_fqns = {f"{user_catalog}.{user_schema}.{t}".lower() for t in analyzed_tables}
    selected: set[str] | None = None
    if tables_filter is not None:
        selected = {f"{user_catalog}.{user_schema}.{t}".lower() for t in tables_filter}

    issues_out: list[dict[str, Any]] = []
    scanned_analyses: list[dict[str, Any]] = []

    for analysis_id, query_id, raw_json, analyzed_at, header_query_text in rows:
        try:
            data = decode_analysis_json(raw_json)
            if not data:
                continue
            sa = data.get("sql_analysis") or {}
            if not isinstance(sa, dict):
                continue
            tables_info = sa.get("tables") or []
            if not isinstance(tables_info, list):
                tables_info = []

            tables_ref = _tables_referenced_in_scope(
                tables_info, user_catalog, user_schema, analyzed_tables
            )
            if not tables_ref:
                continue

            sql_text = (sa.get("formatted_sql") or sa.get("raw_sql") or "").strip()
            qid = query_id or ""
            if not qid:
                qm = data.get("query_metrics") or {}
                if isinstance(qm, dict):
                    qid = str(qm.get("query_id") or analysis_id)
                else:
                    qid = analysis_id

            row_issues: list[dict[str, Any]] = []
            if sql_text:
                pairs = extract_join_pairs_from_sql(sql_text)
                if pairs:
                    resolved = resolve_aliases(pairs, tables_info)
                    mismatches = detect_type_mismatches(resolved, type_map, qid)
                    for m in mismatches:
                        lt = m.get("left_table") or ""
                        rt = m.get("right_table") or ""
                        ll = lt.lower()
                        rr = rt.lower()
                        in_analyzed = ll in analyzed_fqns or rr in analyzed_fqns
                        if not in_analyzed:
                            continue
                        if selected is not None:
                            if not (ll in selected and rr in selected):
                                continue
                        row_issues.append(m)
                    issues_out.extend(row_issues)

            display_src = (header_query_text or "").strip() or sql_text
            scanned_analyses.append(
                {
                    "analysis_id": analysis_id,
                    "query_id": qid,
                    "query_text": _trunc_query120(display_src),
                    "analyzed_at": _serialize_analyzed_at(analyzed_at),
                    "join_cast_issue_count": len(row_issues),
                    "tables_referenced": tables_ref,
                }
            )
        except Exception:
            logger.debug("Skipping one profiler row for JOIN cast scan", exc_info=True)
            continue

    return issues_out, scanned, scanned_analyses
