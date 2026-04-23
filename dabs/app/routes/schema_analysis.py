"""Schema analysis routes: /schema-analysis, /api/v1/schema/*."""

from __future__ import annotations

import logging
import re
from typing import Any

from core.i18n import gettext as _
from flask import Blueprint, jsonify, render_template, request

logger = logging.getLogger(__name__)

bp = Blueprint("schema_analysis", __name__)

_DECIMAL_RE = re.compile(r"^decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", re.IGNORECASE)


def _decimal_storage_bytes(precision: int) -> int:
    """Approximate on-disk size for DECIMAL(p, s) in Spark/Delta (by precision)."""
    if precision <= 9:
        return 4
    if precision <= 18:
        return 8
    return 16


def _serialize_value(val: Any) -> Any:
    """Convert Decimal/date/etc to JSON-safe types."""
    if val is None:
        return None
    import datetime
    import decimal

    if isinstance(val, decimal.Decimal):
        return str(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return str(val)
    return val


def _strip_host(raw: str) -> str:
    h = raw
    if h.startswith("https://"):
        h = h[len("https://") :]
    if h.startswith("http://"):
        h = h[len("http://") :]
    return h.rstrip("/")


def _get_warehouse_connection():
    """SQL Warehouse connection using the same auth pattern as TableWriter."""
    from databricks import sql as dbsql
    from services import _sdk_credentials_provider
    from services.table_writer import TableWriterConfig

    config = TableWriterConfig.from_env()
    if not config.http_path:
        raise ValueError("SQL Warehouse HTTP path is not configured (http_path)")

    host = _strip_host(config.databricks_host)

    if config.databricks_token:
        return dbsql.connect(
            server_hostname=host,
            http_path=config.http_path,
            access_token=config.databricks_token,
        )

    from databricks.sdk.core import Config

    sdk_cfg = Config()
    effective_host = host or _strip_host(sdk_cfg.host or "")
    return dbsql.connect(
        server_hostname=effective_host,
        http_path=config.http_path,
        credentials_provider=_sdk_credentials_provider(sdk_cfg),
    )


def _parse_describe_table_extended(
    rows: list[tuple[Any, ...]],
) -> tuple[list[dict[str, str]], list[tuple[Any, ...]]]:
    """Parse column definitions and retain metadata rows (from first # header onward)."""
    cols: list[dict[str, str]] = []
    meta_rows: list[tuple[Any, ...]] = []
    seen_hash = False
    for row in rows:
        col_name = row[0]
        if col_name is None and not seen_hash:
            continue
        name = str(col_name).strip() if col_name is not None else ""
        if name.startswith("#"):
            seen_hash = True
            meta_rows.append(row)
            continue
        if seen_hash:
            meta_rows.append(row)
            continue
        data_type = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
        comment = str(row[2]).strip() if len(row) > 2 and row[2] is not None else ""
        cols.append({"name": name, "data_type": data_type, "comment": comment})
    return cols, meta_rows


def _parse_bracket_list(val: Any) -> list[str]:
    """Parse [a, b] or list/tuple into string identifiers."""
    if val is None:
        return []
    if isinstance(val, (list, tuple)):
        return [str(x).strip() for x in val if x is not None and str(x).strip()]
    s = str(val).strip()
    if not s or s == "[]":
        return []
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if not inner:
            return []
        parts = [p.strip().strip('"').strip("'") for p in inner.split(",")]
        return [p for p in parts if p]
    return [s]


def _norm_sql_type(t: str) -> str:
    return re.sub(r"\s+", "", (t or "").lower())


def _extract_clustering_columns(meta_rows: list[tuple[Any, ...]]) -> list[str]:
    """Find clustering column names from DESCRIBE TABLE EXTENDED metadata rows."""
    for row in meta_rows:
        k = str(row[0] or "").strip().lower()
        if "clustering" in k and ("column" in k or "key" in k):
            val = row[1] if len(row) > 1 else None
            return _parse_bracket_list(val)
    for row in meta_rows:
        k = str(row[0] or "").strip()
        if k.replace(" ", "").lower() == "clusteringcolumns":
            val = row[1] if len(row) > 1 else None
            return _parse_bracket_list(val)
    return []


def _is_join_key_column_name(name: str) -> bool:
    """Column names that often participate in joins across fact/dimension tables."""
    u = name.lower()
    if u.endswith("_sk") or u.endswith("_id") or u.endswith("_key"):
        return True
    if u.endswith("_date_sk"):
        return True
    return False


def _is_date_like_partition_name(name: str) -> bool:
    n = name.lower()
    return (
        "date" in n
        or n.endswith("_dt")
        or n.endswith("_day")
        or n.endswith("_month")
        or n.endswith("_year")
    )


def _is_high_cardinality_partition_key_name(name: str) -> bool:
    """Partition keys that are typically high-cardinality (IDs, surrogate keys)."""
    s = (name or "").strip()
    if not s:
        return False
    sl = s.lower()
    if sl.endswith("_id") or sl.endswith("_sk") or sl.endswith("_key"):
        return True
    return False


def _is_high_cardinality_name(name: str) -> bool:
    """Backward-compatible alias for partition design checks."""
    return _is_high_cardinality_partition_key_name(name)


def _partition_design_issues(
    table_name: str,
    partition_columns: list[str],
    col_type_by_lower: dict[str, str],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for pc in partition_columns:
        key = pc.strip()
        lk = key.lower()
        dt_raw = col_type_by_lower.get(lk, "")
        dtu = dt_raw.upper()
        if _is_high_cardinality_partition_key_name(key):
            issues.append(
                {
                    "table_name": table_name,
                    "column_name": key,
                    "category": "partition_design",
                    "severity": "HIGH",
                    "current_type": dt_raw,
                    "recommended_type": "lower-cardinality column (e.g. date) or Liquid Clustering",
                    "current_bytes": 0,
                    "recommended_bytes": 0,
                    "savings_per_row": 0,
                    "reason": _(
                        "High-cardinality column {col} used as partition key — causes small file "
                        "proliferation. Migrate to Liquid Clustering."
                    ).format(col=key),
                    "confirmed": False,
                }
            )
            continue
        if dtu == "STRING":
            sev = "HIGH" if _is_date_like_partition_name(key) else "MEDIUM"
            rec = (
                "DATE or INT"
                if _is_date_like_partition_name(key)
                else "narrower type or date/int for time-based data"
            )
            issues.append(
                {
                    "table_name": table_name,
                    "column_name": key,
                    "category": "partition_design",
                    "severity": sev,
                    "current_type": dt_raw,
                    "recommended_type": rec,
                    "current_bytes": 0,
                    "recommended_bytes": 0,
                    "savings_per_row": 0,
                    "reason": (
                        "STRING partition column; prefer DATE or INT for date-based partitions."
                        if sev == "HIGH"
                        else "STRING partition; consider a typed date/int key for pruning efficiency."
                    ),
                    "confirmed": False,
                }
            )
        elif "TIMESTAMP" in dtu:
            issues.append(
                {
                    "table_name": table_name,
                    "column_name": key,
                    "category": "partition_design",
                    "severity": "MEDIUM",
                    "current_type": dt_raw,
                    "recommended_type": "DATE or INT (daily)",
                    "current_bytes": 0,
                    "recommended_bytes": 0,
                    "savings_per_row": 0,
                    "reason": "TIMESTAMP partition is often too granular and creates excessive partitions.",
                    "confirmed": False,
                }
            )
    return issues


def _has_hierarchical_clustering_meta(
    meta_rows: list[tuple[Any, ...]],
    *,
    cursor: Any = None,
    fqn: str = "",
) -> bool:
    """True if hierarchical clustering is configured on this table.

    Checks DESCRIBE EXTENDED metadata first, then falls back to
    SHOW TBLPROPERTIES if cursor is available.
    """
    # Check DESCRIBE EXTENDED metadata rows
    for row in meta_rows:
        blob = " ".join(str(x or "") for x in row).lower()
        if "hierarchical" in blob:
            return True

    # Fallback: SHOW TBLPROPERTIES for delta.liquid.hierarchicalClusteringColumns
    if cursor and fqn:
        try:
            cursor.execute(f"SHOW TBLPROPERTIES {fqn}")
            for row in cursor.fetchall() or []:
                key = str(row[0] or "").strip().lower()
                val = str(row[1] or "").strip()
                if "hierarchicalclusteringcolumns" in key and val:
                    return True
        except Exception:
            pass  # Table may not support TBLPROPERTIES

    return False


def _clustering_key_issues(
    table_name: str,
    clustering_columns: list[str],
    col_type_by_lower: dict[str, str],
    *,
    meta_rows: list[tuple[Any, ...]] | None = None,
    table_fqn: str | None = None,
    cursor: Any = None,
) -> list[dict[str, Any]]:
    from core.extractors import estimate_clustering_key_cardinality

    issues: list[dict[str, Any]] = []
    for cc in clustering_columns:
        key = cc.strip()
        lk = key.lower()
        dt_raw = col_type_by_lower.get(lk, "")
        dtu = dt_raw.upper()
        if dtu == "STRING":
            issues.append(
                {
                    "table_name": table_name,
                    "column_name": key,
                    "category": "clustering_key",
                    "severity": "HIGH",
                    "current_type": dt_raw,
                    "recommended_type": "numeric or typed key",
                    "current_bytes": 0,
                    "recommended_bytes": 0,
                    "savings_per_row": 0,
                    "reason": "STRING clustering key; numeric comparison and data skipping work better with typed keys.",
                    "confirmed": False,
                }
            )
            continue
        m = _DECIMAL_RE.match(dt_raw.strip())
        if m and int(m.group(1)) == 38 and int(m.group(2)) == 0:
            issues.append(
                {
                    "table_name": table_name,
                    "column_name": key,
                    "category": "clustering_key",
                    "severity": "HIGH",
                    "current_type": dt_raw,
                    "recommended_type": "BIGINT",
                    "current_bytes": 16,
                    "recommended_bytes": 8,
                    "savings_per_row": 8,
                    "reason": "decimal(38,0) clustering key; BIGINT compares faster and improves skipping.",
                    "confirmed": False,
                }
            )
    if (
        meta_rows is not None
        and table_fqn
        and clustering_columns
        and not _has_hierarchical_clustering_meta(meta_rows, cursor=cursor, fqn=table_fqn or "")
    ):
        for cc in clustering_columns:
            key = cc.strip()
            if estimate_clustering_key_cardinality(key, None, None, 0) != "low":
                continue
            issues.append(
                {
                    "table_name": table_name,
                    "column_name": key,
                    "category": "clustering_hierarchy",
                    "severity": "MEDIUM",
                    "current_type": col_type_by_lower.get(key.lower(), ""),
                    "recommended_type": "Hierarchical Clustering",
                    "current_bytes": 0,
                    "recommended_bytes": 0,
                    "savings_per_row": 0,
                    "reason": (
                        f"Low-cardinality clustering key {key} would benefit from Hierarchical Clustering"
                    ),
                    "confirmed": False,
                    "recommendation": (
                        f"ALTER TABLE {table_fqn} SET TBLPROPERTIES "
                        "('delta.feature.hierarchicalClustering' = 'supported'); "
                        f"-- CLUSTER BY (<high-cardinality cols>) WITH (HIERARCHICAL CLUSTERING ({key})); "
                        f"OPTIMIZE {table_fqn} FULL;"
                    ),
                }
            )
    return issues


def _cross_table_type_mismatches(
    table_column_types: list[tuple[str, str, str]],
) -> list[dict[str, Any]]:
    """table_column_types: (table_name, column_name, data_type)."""
    by_col: dict[str, list[tuple[str, str]]] = {}
    for table_name, col_name, data_type in table_column_types:
        if not _is_join_key_column_name(col_name):
            continue
        by_col.setdefault(col_name, []).append((table_name, data_type))

    out: list[dict[str, Any]] = []
    for col_name, pairs in by_col.items():
        tables_seen = {t for t, _ in pairs}
        if len(tables_seen) < 2:
            continue
        norm_types = {_norm_sql_type(dt) for _, dt in pairs}
        if len(norm_types) <= 1:
            continue
        occ = [{"table_name": t, "data_type": dt} for t, dt in sorted(pairs, key=lambda x: x[0])]
        types_summary = "; ".join(f"{t}: {dt}" for t, dt in sorted(pairs, key=lambda x: x[0]))
        out.append(
            {
                "category": "type_mismatch",
                "severity": "HIGH",
                "column_name": col_name,
                "occurrences": occ,
                "recommendation": "Align join-key column types across tables (prefer one canonical type, e.g. BIGINT for surrogate keys).",
                "message": f"Join key {col_name!r} has mismatched types: {types_summary}.",
            }
        )
    return out


def _is_surrogate_key_name(name: str) -> bool:
    u = name.upper()
    return u.endswith("_SK") or u.endswith("_ID") or u.endswith("_KEY")


def _is_qty_or_count_name(name: str) -> bool:
    u = name.upper()
    if u.endswith("_COUNT") or u.startswith("COUNT_"):
        return True
    if "QTY" in u or "QUANTITY" in u:
        return True
    if u.endswith("_QTY") or u.startswith("QTY_"):
        return True
    return False


def _string_pattern_issue(col_name: str) -> tuple[str, str] | None:
    """If STRING column name suggests date/number semantics, return (recommended, kind)."""
    u = col_name.upper()
    if u.endswith("_DATE") or u.endswith("_DT"):
        return "DATE", "date"
    if u.endswith("_TS") or u.endswith("_TIMESTAMP") or u.endswith("_AT"):
        return "TIMESTAMP", "timestamp"
    if u.endswith("_ID"):
        return "BIGINT", "id"
    return None


def _analyze_column(table_name: str, col_name: str, data_type: str) -> dict[str, Any] | None:
    """Return one issue dict or None."""
    dt = (data_type or "").strip()
    if not dt:
        return None

    m = _DECIMAL_RE.match(dt)
    if m:
        prec, scale = int(m.group(1)), int(m.group(2))
        cur_bytes = _decimal_storage_bytes(prec)
        if prec == 38 and scale == 0:
            if _is_surrogate_key_name(col_name):
                rec = "BIGINT"
                rec_bytes = 8
                return {
                    "table_name": table_name,
                    "category": "column_type",
                    "column_name": col_name,
                    "current_type": dt,
                    "recommended_type": rec,
                    "severity": "CRITICAL",
                    "current_bytes": cur_bytes,
                    "recommended_bytes": rec_bytes,
                    "savings_per_row": cur_bytes - rec_bytes,
                    "reason": "decimal(38,0) for a surrogate-style key column; BIGINT is sufficient.",
                    "confirmed": False,
                }
            if _is_qty_or_count_name(col_name):
                rec = "INT"
                rec_bytes = 4
                return {
                    "table_name": table_name,
                    "category": "column_type",
                    "column_name": col_name,
                    "current_type": dt,
                    "recommended_type": rec,
                    "severity": "HIGH",
                    "current_bytes": cur_bytes,
                    "recommended_bytes": rec_bytes,
                    "savings_per_row": cur_bytes - rec_bytes,
                    "reason": "decimal(38,0) for quantity/count-like column; INT often suffices.",
                    "confirmed": False,
                }
            rec = "BIGINT"
            rec_bytes = 8
            return {
                "table_name": table_name,
                "category": "column_type",
                "column_name": col_name,
                "current_type": dt,
                "recommended_type": rec,
                "severity": "MEDIUM",
                "current_bytes": cur_bytes,
                "recommended_bytes": rec_bytes,
                "savings_per_row": cur_bytes - rec_bytes,
                "reason": "decimal(38,0) wastes space vs BIGINT for whole numbers.",
                "confirmed": False,
            }

        if prec > 18:
            if scale > 18:
                return None
            new_prec = 18
            rec = f"DECIMAL({new_prec},{scale})"
            rec_bytes = _decimal_storage_bytes(new_prec)
            if rec_bytes >= cur_bytes:
                return None
            return {
                "table_name": table_name,
                "category": "column_type",
                "column_name": col_name,
                "current_type": dt,
                "recommended_type": rec,
                "severity": "MEDIUM",
                "current_bytes": cur_bytes,
                "recommended_bytes": rec_bytes,
                "savings_per_row": cur_bytes - rec_bytes,
                "reason": f"Precision {prec} is wider than needed; {rec} reduces storage.",
                "confirmed": False,
            }
        return None

    if dt.upper() == "STRING":
        pat = _string_pattern_issue(col_name)
        if not pat:
            return None
        rec, kind = pat
        cur_bytes = 28  # heuristic average variable string width for sizing
        if kind == "date":
            rec_bytes = 4
        elif kind == "timestamp":
            rec_bytes = 8
        else:
            rec_bytes = 8
        return {
            "table_name": table_name,
            "category": "column_type",
            "column_name": col_name,
            "current_type": dt,
            "recommended_type": rec,
            "severity": "LOW",
            "current_bytes": cur_bytes,
            "recommended_bytes": rec_bytes,
            "savings_per_row": cur_bytes - rec_bytes,
            "reason": f"STRING column name suggests {kind}; prefer {rec} (validate with samples).",
            "confirmed": False,
        }

    return None


def _safe_catalog_schema_sql(catalog: str, schema: str) -> str:
    """Return `catalog`.`schema` for use in SHOW TABLES (identifiers validated)."""
    from core.sql_safe import validate_identifier

    validate_identifier(catalog, "catalog")
    validate_identifier(schema, "schema")
    return f"`{catalog}`.`{schema}`"


def _describe_detail_snapshot(
    cursor: Any, fqn: str
) -> tuple[int | None, list[str], int | None, int | None]:
    """numRecords, partitionColumns, numFiles, sizeInBytes from DESCRIBE DETAIL (no table scan)."""
    row_count: int | None = None
    part_cols: list[str] = []
    num_files: int | None = None
    size_in_bytes: int | None = None
    try:
        cursor.execute(f"DESCRIBE DETAIL {fqn}")
        desc = cursor.description
        rows = cursor.fetchall()
        if not rows:
            return None, [], None, None

        def _set_from_norm(norm: str, val: Any) -> None:
            nonlocal row_count, part_cols, num_files, size_in_bytes
            if val is None:
                return
            if norm in ("numrecords", "numrows"):
                row_count = int(val)
            elif norm == "partitioncolumns":
                part_cols = _parse_bracket_list(val)
            elif norm in ("numfiles",):
                num_files = int(val)
            elif norm in ("sizeinbytes", "totalsizeinbytes"):
                size_in_bytes = int(val)

        if desc and rows[0] is not None:
            row0 = rows[0]
            names = [d[0] for d in desc if d is not None]
            for i, name in enumerate(names):
                if name is None:
                    continue
                norm = str(name).lower().replace("_", "")
                val = row0[i]
                _set_from_norm(norm, val)

        if rows and len(rows[0]) >= 2:
            for row in rows:
                prop = str(row[0]).strip().lower() if row[0] is not None else ""
                pn = prop.replace(" ", "").replace("_", "")
                val = row[1] if len(row) > 1 else None
                if pn in ("numrecords", "numrows") and val is not None and row_count is None:
                    row_count = int(val)
                if pn == "partitioncolumns" and val is not None and not part_cols:
                    part_cols = _parse_bracket_list(val)
                if pn in ("numfiles",) and val is not None and num_files is None:
                    num_files = int(val)
                if (
                    pn in ("sizeinbytes", "totalsizeinbytes")
                    and val is not None
                    and size_in_bytes is None
                ):
                    size_in_bytes = int(val)
    except Exception:
        logger.exception("DESCRIBE DETAIL failed for %s", fqn)
    return row_count, part_cols, num_files, size_in_bytes


def _list_tables(cursor: Any, catalog: str, schema: str) -> list[str]:
    ref = _safe_catalog_schema_sql(catalog, schema)
    cursor.execute(f"SHOW TABLES IN {ref}")
    rows = cursor.fetchall()
    names: list[str] = []
    for row in rows:
        if not row:
            continue
        # (database, tableName, isTemporary) or similar
        table_name = row[1] if len(row) > 1 else row[0]
        if table_name is not None:
            names.append(str(table_name))
    return names


def _table_row_count_and_partitions(
    cursor: Any, catalog: str, schema: str, table: str
) -> tuple[int | None, list[str], int | None, int | None]:
    from core.sql_safe import safe_fqn

    fqn = safe_fqn(catalog, schema, table)
    return _describe_detail_snapshot(cursor, fqn)


_FILE_TOO_SMALL_BYTES = 32 * 1024**2  # 32 MiB
_FILE_TOO_LARGE_BYTES = 2 * 1024**3  # 2 GiB
_SMALL_TABLE_BYTES = 1024**3  # 1 GiB
_MAX_FILES_WARN = 10000


def _file_size_issues(
    table_name: str,
    num_files: int | None,
    size_in_bytes: int | None,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if num_files is not None and num_files > _MAX_FILES_WARN:
        issues.append(
            {
                "table_name": table_name,
                "column_name": "",
                "category": "file_layout",
                "severity": "MEDIUM",
                "current_type": "",
                "recommended_type": f"OPTIMIZE {table_name} to reduce file count and metadata overhead",
                "current_bytes": 0,
                "recommended_bytes": 0,
                "savings_per_row": 0,
                "reason": (
                    f"Table has {num_files:,} files — consider OPTIMIZE to reduce metadata overhead"
                ),
                "confirmed": False,
            }
        )
    if num_files is not None and size_in_bytes is not None and num_files > 0:
        avg_b = size_in_bytes // num_files
        if avg_b < _FILE_TOO_SMALL_BYTES:
            avg_mb = avg_b / (1024**2)
            issues.append(
                {
                    "table_name": table_name,
                    "column_name": "",
                    "category": "file_layout",
                    "severity": "HIGH",
                    "current_type": "",
                    "recommended_type": f"OPTIMIZE {table_name} to target 128MB-1GB files",
                    "current_bytes": 0,
                    "recommended_bytes": 0,
                    "savings_per_row": 0,
                    "reason": (
                        f"Average file size is {avg_mb:.1f}MB — too many small files. Run OPTIMIZE to compact."
                    ),
                    "confirmed": False,
                }
            )
        elif avg_b > _FILE_TOO_LARGE_BYTES:
            avg_gb = avg_b / (1024**3)
            issues.append(
                {
                    "table_name": table_name,
                    "column_name": "",
                    "category": "file_layout",
                    "severity": "MEDIUM",
                    "current_type": "",
                    "recommended_type": (
                        "Set spark.databricks.delta.optimize.maxFileSize or repartition before write"
                    ),
                    "current_bytes": 0,
                    "recommended_bytes": 0,
                    "savings_per_row": 0,
                    "reason": (
                        f"Average file size is {avg_gb:.2f}GB — files too large, reducing parallelism"
                    ),
                    "confirmed": False,
                }
            )
    return issues


def _small_table_issues(
    table_name: str,
    size_in_bytes: int | None,
    partition_columns: list[str],
    clustering_columns: list[str],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if size_in_bytes is None or size_in_bytes >= _SMALL_TABLE_BYTES:
        return issues
    size_mb = size_in_bytes / (1024**2)
    if partition_columns:
        issues.append(
            {
                "table_name": table_name,
                "column_name": "",
                "category": "table_design",
                "severity": "MEDIUM",
                "current_type": "",
                "recommended_type": "Consider removing partitioning or growing the table before partitioning",
                "current_bytes": 0,
                "recommended_bytes": 0,
                "savings_per_row": 0,
                "reason": (
                    f"Table is only {size_mb:.1f}MB but has partitioning — partitioning is unnecessary "
                    "for small tables and may cause small file issues"
                ),
                "confirmed": False,
            }
        )
    if clustering_columns:
        issues.append(
            {
                "table_name": table_name,
                "column_name": "",
                "category": "table_design",
                "severity": "LOW",
                "current_type": "",
                "recommended_type": "Liquid Clustering is optional for small tables",
                "current_bytes": 0,
                "recommended_bytes": 0,
                "savings_per_row": 0,
                "reason": (
                    f"Table is only {size_mb:.1f}MB — Liquid Clustering benefit is minimal for small tables"
                ),
                "confirmed": False,
            }
        )
    return issues


def _lc_migration_issues(
    table_name: str,
    partition_columns: list[str],
    clustering_columns: list[str],
    size_in_bytes: int | None,
    table_fqn: str,
) -> list[dict[str, Any]]:
    if (
        not partition_columns
        or clustering_columns
        or size_in_bytes is None
        or size_in_bytes <= _SMALL_TABLE_BYTES
    ):
        return []
    size_gb = size_in_bytes / (1024**3)
    hc_cols = [
        pc.strip()
        for pc in partition_columns
        if pc.strip() and _is_high_cardinality_partition_key_name(pc.strip())
    ]
    if hc_cols:
        cluster_by = ", ".join(hc_cols)
        primary = hc_cols[0]
        return [
            {
                "table_name": table_name,
                "column_name": primary,
                "category": "table_design",
                "severity": "HIGH",
                "current_type": "",
                "recommended_type": _(
                    "ALTER TABLE {fqn} CLUSTER BY ({cols}); DROP PARTITION — no data rewrite needed with LC"
                ).format(fqn=table_fqn, cols=cluster_by),
                "current_bytes": 0,
                "recommended_bytes": 0,
                "savings_per_row": 0,
                "reason": _(
                    "High-cardinality column {col} used as partition key — causes small file "
                    "proliferation. Migrate to Liquid Clustering."
                ).format(col=primary),
                "confirmed": False,
            }
        ]
    return [
        {
            "table_name": table_name,
            "column_name": "",
            "category": "table_design",
            "severity": "HIGH",
            "current_type": "",
            "recommended_type": (
                f"ALTER TABLE {table_fqn} CLUSTER BY (col1, col2); OPTIMIZE {table_fqn} FULL;"
            ),
            "current_bytes": 0,
            "recommended_bytes": 0,
            "savings_per_row": 0,
            "reason": (
                f"Large table ({size_gb:.2f}GB) uses partitioning but no Liquid Clustering — "
                "consider migrating to LC"
            ),
            "confirmed": False,
        }
    ]


def _extract_three_part_table_refs(query_text: str) -> list[tuple[str, str, str]]:
    """Parse SQL and return (catalog, schema, table) for fully qualified table refs only."""
    if not (query_text or "").strip():
        return []
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return []

    parsed = None
    for dialect in ("databricks", "spark", None):
        try:
            if dialect:
                parsed = sqlglot.parse_one(query_text, dialect=dialect)
            else:
                parsed = sqlglot.parse_one(query_text)
            break
        except Exception:
            continue
    if parsed is None:
        return []

    seen: set[tuple[str, str, str]] = set()
    out: list[tuple[str, str, str]] = []
    for t in parsed.find_all(exp.Table):
        cat = (str(t.catalog).strip() if t.catalog else "").strip('`"')
        sch = (str(t.db).strip() if t.db else "").strip('`"')
        name = (str(t.name).strip() if t.name else "").strip('`"')
        if not cat or not sch or not name:
            continue
        key = (cat.lower(), sch.lower(), name.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append((cat, sch, name))
    return out


@bp.route("/schema-analysis")
def schema_analysis_page():
    """Schema analysis UI."""
    from app import APP_VERSION, get_locale

    return render_template(
        "schema_analysis.html",
        current_lang=get_locale(),
        app_version=APP_VERSION,
    )


@bp.route("/api/v1/schema/past-schemas", methods=["GET"])
def schema_past_schemas():
    """Discover catalog.schema and tables from recent profiler_analysis_header rows (sqlglot)."""
    from datetime import datetime

    from core.sql_safe import safe_fqn
    from services.table_writer import TableWriterConfig

    cfg = TableWriterConfig.from_env()
    if not cfg.http_path:
        return jsonify({"ok": True, "schemas": []})

    hdr_fqn = safe_fqn(cfg.catalog, cfg.schema, "profiler_analysis_header")
    sql = f"""
SELECT query_id, query_text, analyzed_at
FROM {hdr_fqn}
WHERE query_text IS NOT NULL AND TRIM(query_text) <> ''
ORDER BY analyzed_at DESC NULLS LAST
LIMIT 50
"""
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    try:
        conn = _get_warehouse_connection()
        with conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall() or []
        for row in rows:
            if not row or len(row) < 3:
                continue
            qtext = row[1]
            analyzed_at = row[2]
            if qtext is None:
                continue
            fqns = _extract_three_part_table_refs(str(qtext))
            if not fqns:
                continue
            keys_this_row: set[tuple[str, str]] = set()
            for cat, sch, tbl in fqns:
                key = (cat.lower(), sch.lower())
                keys_this_row.add(key)
                if key not in groups:
                    groups[key] = {
                        "catalog": cat,
                        "schema": sch,
                        "tables": set(),
                        "query_count": 0,
                        "last_analyzed": None,
                    }
                groups[key]["tables"].add(tbl)
            for key in keys_this_row:
                g = groups[key]
                g["query_count"] += 1
                if analyzed_at is not None:
                    prev = g["last_analyzed"]
                    if prev is None or analyzed_at > prev:
                        g["last_analyzed"] = analyzed_at
    except Exception:
        logger.info("past-schemas: profiler header query or parse failed", exc_info=True)
        return jsonify({"ok": True, "schemas": []})

    items = list(groups.values())
    items.sort(
        key=lambda g: g["last_analyzed"] if g["last_analyzed"] is not None else datetime.min,
        reverse=True,
    )
    out: list[dict[str, Any]] = []
    for g in items:
        tables_sorted = sorted(g["tables"], key=str.lower)
        out.append(
            {
                "catalog": g["catalog"],
                "schema": g["schema"],
                "tables": tables_sorted,
                "query_count": g["query_count"],
                "last_analyzed": _serialize_value(g["last_analyzed"]),
            }
        )
    return jsonify({"ok": True, "schemas": out})


@bp.route("/api/v1/schema/analyze", methods=["POST"])
def schema_analyze():
    """Analyze table schemas for suboptimal types."""
    from core.sql_safe import safe_fqn, validate_identifier

    try:
        payload = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid JSON body"}), 400

    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "JSON object expected"}), 400

    catalog = payload.get("catalog") or ""
    schema = payload.get("schema") or ""
    tables_in = payload.get("tables")

    try:
        validate_identifier(catalog, "catalog")
        validate_identifier(schema, "schema")
    except ValueError:
        logger.warning("Invalid catalog or schema in schema analyze request")
        return jsonify(
            {
                "ok": False,
                "error": "Invalid catalog or schema name.",
            }
        ), 400

    if tables_in is not None:
        if not isinstance(tables_in, list):
            return jsonify({"ok": False, "error": "tables must be a list of strings"}), 400
        for t in tables_in:
            if not isinstance(t, str):
                return jsonify({"ok": False, "error": "tables must be strings"}), 400
            try:
                validate_identifier(t, "table name")
            except ValueError:
                logger.warning("Invalid table name in schema analyze request")
                return jsonify({"ok": False, "error": "Invalid table name."}), 400

    try:
        conn = _get_warehouse_connection()
    except ValueError:
        return jsonify({"ok": False, "error": "SQL Warehouse HTTP path is not configured."}), 400
    except Exception:
        logger.exception("Warehouse connection failed")
        return jsonify({"ok": False, "error": "Failed to connect to SQL Warehouse."}), 500

    tables: list[str]
    try:
        with conn:
            cursor = conn.cursor()
            if tables_in:
                tables = list(tables_in)
            else:
                tables = _list_tables(cursor, catalog, schema)

            all_issues: list[dict[str, Any]] = []
            table_summaries: list[dict[str, Any]] = []
            table_column_types: list[tuple[str, str, str]] = []

            for table in tables:
                fqn = safe_fqn(catalog, schema, table)
                cursor.execute(f"DESCRIBE TABLE EXTENDED {fqn}")
                rows = cursor.fetchall()
                cols, meta_rows = _parse_describe_table_extended(rows)
                col_type_by_lower = {c["name"].lower(): c["data_type"] for c in cols}
                for c in cols:
                    table_column_types.append((table, c["name"], c["data_type"]))

                clustering_cols = _extract_clustering_columns(meta_rows)
                table_issues: list[dict[str, Any]] = []
                for c in cols:
                    issue = _analyze_column(table, c["name"], c["data_type"])
                    if issue:
                        table_issues.append(issue)

                row_count, partition_columns, num_files, size_in_bytes = (
                    _table_row_count_and_partitions(cursor, catalog, schema, table)
                )
                table_issues.extend(
                    _partition_design_issues(table, partition_columns, col_type_by_lower)
                )
                table_issues.extend(
                    _clustering_key_issues(
                        table,
                        clustering_cols,
                        col_type_by_lower,
                        meta_rows=meta_rows,
                        table_fqn=fqn,
                        cursor=cursor,
                    )
                )
                table_issues.extend(_file_size_issues(table, num_files, size_in_bytes))
                table_issues.extend(
                    _small_table_issues(table, size_in_bytes, partition_columns, clustering_cols)
                )
                table_issues.extend(
                    _lc_migration_issues(
                        table, partition_columns, clustering_cols, size_in_bytes, fqn
                    )
                )

                est_table_savings = None
                if row_count is not None:
                    est_table_savings = (
                        sum(int(i.get("savings_per_row") or 0) for i in table_issues) * row_count
                    )
                table_summaries.append(
                    {
                        "table_name": table,
                        "row_count": row_count,
                        "partition_columns": partition_columns,
                        "clustering_columns": clustering_cols,
                        "num_files": num_files,
                        "size_in_bytes": size_in_bytes,
                        "columns": [{"name": c["name"], "data_type": c["data_type"]} for c in cols],
                        "issues": table_issues,
                        "estimated_savings_bytes": est_table_savings,
                    }
                )
                for i in table_issues:
                    issue_out = dict(i)
                    issue_out["row_count"] = row_count
                    spr = int(issue_out.get("savings_per_row") or 0)
                    if row_count is not None:
                        issue_out["estimated_savings_bytes"] = spr * row_count
                    else:
                        issue_out["estimated_savings_bytes"] = None
                    all_issues.append(issue_out)

            cross_table_issues = _cross_table_type_mismatches(table_column_types)

            total_estimated_savings = sum(
                (t["estimated_savings_bytes"] or 0) for t in table_summaries
            )

            type_map: dict[str, dict[str, str]] = {}
            for summ in table_summaries:
                tname = summ["table_name"]
                fq = f"{catalog}.{schema}.{tname}".lower()
                cmap: dict[str, str] = {}
                for c in summ.get("columns") or []:
                    if isinstance(c, dict) and c.get("name"):
                        cmap[str(c["name"]).lower()] = str(c.get("data_type") or "")
                type_map[tname.lower()] = cmap
                type_map[fq] = cmap

            join_cast_issues: list[dict[str, Any]] = []
            join_analysis_queries_scanned = 0
            scanned_analyses: list[dict[str, Any]] = []
            try:
                from services.schema_join_detector import collect_join_cast_issues

                tf = list(tables_in) if tables_in else None
                join_cast_issues, join_analysis_queries_scanned, scanned_analyses = (
                    collect_join_cast_issues(
                        conn,
                        user_catalog=catalog,
                        user_schema=schema,
                        analyzed_tables=list(tables),
                        type_map=type_map,
                        tables_filter=tf,
                        limit=20,
                    )
                )
            except Exception:
                logger.info("JOIN implicit cast scan skipped", exc_info=True)

            return jsonify(
                {
                    "ok": True,
                    "catalog": catalog,
                    "schema": schema,
                    "tables_analyzed": tables,
                    "issues": all_issues,
                    "by_table": table_summaries,
                    "cross_table_issues": cross_table_issues,
                    "join_cast_issues": join_cast_issues,
                    "join_analysis_queries_scanned": join_analysis_queries_scanned,
                    "scanned_analyses": scanned_analyses,
                    "total_estimated_savings_bytes": total_estimated_savings,
                }
            )
    except Exception:
        logger.exception("schema analyze failed")
        return jsonify({"ok": False, "error": "Failed to analyze schema."}), 500


# ---------------------------------------------------------------------------
# Async sample — background thread + polling (avoids Databricks Apps 60s proxy timeout)
# ---------------------------------------------------------------------------
import time
import uuid
from threading import Thread

_sample_tasks: dict[str, dict] = {}
_SAMPLE_TASK_TTL = 600  # 10 min


def _purge_stale_sample_tasks() -> None:
    now = time.monotonic()
    stale = [k for k, v in _sample_tasks.items() if now - v.get("_ts", 0) > _SAMPLE_TASK_TTL]
    for k in stale:
        _sample_tasks.pop(k, None)


def _run_sample_task(task_id: str, fqn: str, table: str, columns: list[str]) -> None:
    """Background worker for column sampling."""
    try:
        conn = _get_warehouse_connection()
        parts: list[str] = []
        for col in columns:
            parts.append(f"MIN(`{col}`) AS `__min_{col}`")
            parts.append(f"MAX(`{col}`) AS `__max_{col}`")

        sql = f"SELECT {', '.join(parts)} FROM {fqn}"

        with conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()

        out_cols: list[dict[str, Any]] = []
        if row:
            idx = 0
            flat = list(row)
            for col in columns:
                if idx + 1 < len(flat):
                    out_cols.append(
                        {
                            "column": col,
                            "min": _serialize_value(flat[idx]),
                            "max": _serialize_value(flat[idx + 1]),
                        }
                    )
                    idx += 2
                else:
                    out_cols.append({"column": col, "min": None, "max": None})

        confirmed_updates = [
            {"table_name": table, "column_name": col, "confirmed": True} for col in columns
        ]
        _sample_tasks[task_id] = {
            "status": "completed",
            "result": {
                "ok": True,
                "table": table,
                "columns": out_cols,
                "confirmed_updates": confirmed_updates,
            },
        }
    except Exception as e:
        logger.exception("schema sample task failed")
        _sample_tasks[task_id] = {"status": "failed", "error": str(e)[:200]}


@bp.route("/api/v1/schema/sample", methods=["POST"])
def schema_sample():
    """Start async MIN/MAX sampling for columns."""
    from core.sql_safe import safe_fqn, validate_identifier

    try:
        payload = request.get_json(force=True, silent=False)
    except Exception:
        return jsonify({"ok": False, "error": "Invalid JSON body"}), 400

    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "JSON object expected"}), 400

    catalog = payload.get("catalog") or ""
    schema = payload.get("schema") or ""
    table = payload.get("table") or ""
    columns = payload.get("columns")

    try:
        validate_identifier(catalog, "catalog")
        validate_identifier(schema, "schema")
        validate_identifier(table, "table name")
    except ValueError:
        logger.warning("Invalid identifier in schema sample request")
        return jsonify({"ok": False, "error": "Invalid catalog, schema, or table name."}), 400

    if not isinstance(columns, list) or not columns:
        return jsonify({"ok": False, "error": "columns must be a non-empty list"}), 400

    for c in columns:
        if not isinstance(c, str):
            return jsonify({"ok": False, "error": "column names must be strings"}), 400
        try:
            validate_identifier(c, "column name")
        except ValueError:
            return jsonify({"ok": False, "error": "Invalid column name."}), 400

    fqn = safe_fqn(catalog, schema, table)
    task_id = str(uuid.uuid4())[:8]
    _purge_stale_sample_tasks()
    _sample_tasks[task_id] = {"status": "running", "_ts": time.monotonic()}

    thread = Thread(
        target=_run_sample_task,
        args=(task_id, fqn, table, columns),
        daemon=True,
    )
    thread.start()

    return jsonify({"ok": True, "task_id": task_id})


@bp.route("/api/v1/schema/sample/<task_id>")
def schema_sample_status(task_id):
    """Poll sample task status."""
    task = _sample_tasks.get(task_id)
    if not task:
        return jsonify({"ok": False, "error": "Task not found"}), 404

    status = task.get("status", "unknown")
    if status == "running":
        return jsonify({"ok": True, "status": "running"})
    if status == "failed":
        _sample_tasks.pop(task_id, None)
        return jsonify(
            {"ok": False, "status": "failed", "error": task.get("error", "Unknown error")}
        ), 502

    # Completed
    result = task.get("result", {})
    _sample_tasks.pop(task_id, None)
    return jsonify({"status": "completed", **result})
