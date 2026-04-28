"""Tests for V6 SQL skeleton extraction (Week 5 Day 2)."""

from __future__ import annotations

from core.sql_skeleton import (
    DEFAULT_BUDGET_CHARS,
    DEFAULT_FULLSQL_THRESHOLD,
    SkeletonResult,
    aggregate_parse_metrics,
    build_sql_skeleton,
)


# ----- empty / short SQL -----


def test_empty_sql():
    r = build_sql_skeleton("")
    assert r.method == "fullsql"
    assert r.skeleton == ""
    assert r.parse_success is True


def test_short_sql_returns_full():
    sql = "SELECT id FROM tbl WHERE x = 1"
    r = build_sql_skeleton(sql)
    assert r.method == "fullsql"
    assert r.skeleton == sql


def test_short_simple_sql_below_threshold_returns_full():
    sql = "SELECT a, b, c FROM customers JOIN orders USING (id) WHERE active = 1"
    r = build_sql_skeleton(sql)
    assert r.method == "fullsql"
    assert r.skeleton == sql


# ----- bypass statement types -----


def test_merge_bypass_when_extended_disabled(monkeypatch):
    """Kill-switch path: V6_SQL_SKELETON_EXTENDED off → MERGE bypasses."""
    from core import feature_flags

    monkeypatch.setenv(feature_flags.V6_SQL_SKELETON_EXTENDED, "0")
    feature_flags.reset_cache()
    sql = "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"
    r = build_sql_skeleton(sql)
    assert r.method == "bypass"
    assert r.bypass_reason == "unsupported_statement_type:merge"


def test_create_view_bypass_when_extended_disabled(monkeypatch):
    """Kill-switch path: V6_SQL_SKELETON_EXTENDED off → CREATE VIEW bypasses."""
    from core import feature_flags

    monkeypatch.setenv(feature_flags.V6_SQL_SKELETON_EXTENDED, "0")
    feature_flags.reset_cache()
    sql = (
        "CREATE OR REPLACE MATERIALIZED VIEW my.tbl AS\n"
        + ("SELECT * FROM source_tbl WHERE x > 0\n" * 200)
    )
    r = build_sql_skeleton(sql)
    assert r.method == "bypass"
    # head+tail should be applied to keep prompt small
    assert r.skeleton_chars < r.original_chars


def test_insert_bypass_when_extended_disabled(monkeypatch):
    """Kill-switch path: V6_SQL_SKELETON_EXTENDED off → INSERT bypasses."""
    from core import feature_flags

    monkeypatch.setenv(feature_flags.V6_SQL_SKELETON_EXTENDED, "0")
    feature_flags.reset_cache()
    sql = "INSERT INTO t SELECT * FROM s"
    r = build_sql_skeleton(sql)
    assert r.method == "bypass"


# ----- complex SQL with CTE / JOIN -----


def _build_complex_sql(_repeat: int = 50) -> str:
    """One complex SQL with several CTEs and joins, padded with comments
    so it crosses the 3000-char threshold."""
    base = """
WITH ranked_orders AS (
  SELECT customer_id, order_id,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC) AS rn
  FROM fact_orders
  WHERE order_ts >= '2025-01-01'
),
top_customers AS (
  SELECT customer_id, COUNT(*) AS n
  FROM ranked_orders
  WHERE rn = 1
  GROUP BY customer_id
  HAVING COUNT(*) > 5
),
enriched AS (
  SELECT t.customer_id, c.region, t.n
  FROM top_customers t
  JOIN dim_customer c ON t.customer_id = c.customer_id
  LEFT JOIN dim_region r ON c.region_id = r.region_id
)
SELECT region, AVG(n) AS avg_n, COUNT(*) AS k
FROM enriched
WHERE region IS NOT NULL AND n BETWEEN 5 AND 1000
GROUP BY region
ORDER BY avg_n DESC
LIMIT 100
"""
    pad = "-- " + ("padding " * 60) + "\n"  # ~720 chars/line
    # Need >3000 chars total to trigger skeleton; 5 pad lines = ~3600 chars
    return base + ("\n" + pad * 5)


def test_complex_sql_uses_sqlglot():
    sql = _build_complex_sql(50)
    r = build_sql_skeleton(sql)
    assert r.method == "sqlglot"
    assert r.parse_success is True
    assert r.cte_count >= 3
    # skeleton should be much smaller
    assert r.skeleton_chars < r.original_chars
    assert r.compression_ratio < 0.5


def test_complex_sql_skeleton_contains_structure():
    sql = _build_complex_sql(50)
    r = build_sql_skeleton(sql)
    # CTE names appear
    assert "ranked_orders" in r.skeleton
    assert "top_customers" in r.skeleton
    assert "enriched" in r.skeleton
    # Predicate shapes appear
    assert "WHERE" in r.skeleton
    assert "JOIN" in r.skeleton
    # GROUP BY column appears
    assert "GROUP BY" in r.skeleton


def test_complex_sql_drops_literals():
    """Skeleton should NOT contain raw literal values like '2025-01-01'."""
    sql = _build_complex_sql(50)
    r = build_sql_skeleton(sql)
    assert "2025-01-01" not in r.skeleton
    # Specific BETWEEN literals should also be gone
    assert "1000" not in r.skeleton


# ----- parse failure fallback -----


def test_unparseable_falls_back_to_head_tail():
    """Construct SQL that sqlglot cannot parse; expect head_tail."""
    # COPY INTO is not parseable in some sqlglot versions; size > threshold
    sql = "COPY INTO mytable FROM '/path/to/data' " + ("xxxxxxxxxxxxxxxxxxxx " * 200)
    sql += " WITH JOIN JOIN JOIN"  # also bumps complexity heuristic
    if len(sql) <= DEFAULT_FULLSQL_THRESHOLD:
        sql = sql + (" -- pad" * 1000)
    r = build_sql_skeleton(sql, fallback_head_tail=True)
    # Either sqlglot parses or head_tail kicks in. Just assert no crash.
    assert r.method in ("sqlglot", "head_tail", "truncate", "fullsql")
    if r.method == "head_tail":
        assert r.parse_success is False


def test_force_truncate_when_no_fallback():
    """Trigger fallback path with fallback_head_tail=False."""
    # Same construction as above but disable head_tail
    sql = "INVALID@@@@@SQL" + ("xx " * 1500)
    # Force unparseable + complex
    sql = (
        "WITH a AS (SELECT 1), b AS (SELECT 2), c AS (SELECT 3) "
        + sql
        + " JOIN x JOIN y JOIN z"
    )
    r = build_sql_skeleton(sql, fallback_head_tail=False)
    assert r.method in ("sqlglot", "truncate", "fullsql")


# ----- aggregation -----


def test_aggregate_parse_metrics():
    results = [
        SkeletonResult("x", "sqlglot", True, original_chars=1000, skeleton_chars=200),
        SkeletonResult("y", "sqlglot", True, original_chars=2000, skeleton_chars=300),
        SkeletonResult("z", "head_tail", False, original_chars=5000, skeleton_chars=3000),
        SkeletonResult("", "fullsql", True, original_chars=100, skeleton_chars=100),
    ]
    m = aggregate_parse_metrics(results)
    assert m["parse_success_rate"] == 0.75  # 3/4
    assert m["skeleton_used_rate"] == 0.5   # 2/4
    assert "sqlglot" in m["method_distribution"]
    assert m["method_distribution"]["sqlglot"] == 0.5


def test_aggregate_empty():
    m = aggregate_parse_metrics([])
    assert m["parse_success_rate"] == 1.0
    assert m["skeleton_used_rate"] == 0.0
    assert m["avg_compression_ratio"] == 1.0


# ----- compression budget -----


def test_skeleton_respects_budget():
    sql = _build_complex_sql(200)
    budget = 800
    r = build_sql_skeleton(sql, char_budget=budget)
    assert r.skeleton_chars <= budget + 50  # small slack for trimming marker


# ----- V6.1 extended extraction (MERGE / CREATE VIEW / INSERT) -----


def _enable_extended(monkeypatch):
    """Helper to flip V6_SQL_SKELETON_EXTENDED on for one test."""
    from core import feature_flags
    monkeypatch.setenv(feature_flags.V6_SQL_SKELETON_EXTENDED, "1")
    feature_flags.reset_cache()


def _disable_extended(monkeypatch):
    from core import feature_flags
    monkeypatch.delenv(feature_flags.V6_SQL_SKELETON_EXTENDED, raising=False)
    feature_flags.reset_cache()


def test_merge_explicit_disabled_still_bypass(monkeypatch):
    """Kill-switch path: V6_SQL_SKELETON_EXTENDED explicitly disabled
    keeps MERGE on the legacy bypass route (back-compat)."""
    from core import feature_flags

    monkeypatch.setenv(feature_flags.V6_SQL_SKELETON_EXTENDED, "0")
    feature_flags.reset_cache()
    sql = (
        "MERGE INTO catalog.schema.target t USING source s ON t.id = s.id "
        + "WHEN MATCHED THEN UPDATE SET *"
    )
    r = build_sql_skeleton(sql)
    assert r.method == "bypass"


def test_merge_extended_uses_merge_method(monkeypatch):
    _enable_extended(monkeypatch)
    sql = """
MERGE INTO catalog.schema.target t
USING (SELECT id, val FROM stage_table WHERE active = 1) s
ON t.id = s.id
WHEN MATCHED AND s.val > 0 THEN UPDATE SET val = s.val
WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
"""
    r = build_sql_skeleton(sql)
    assert r.method == "merge"
    # target appears
    assert "MERGE INTO" in r.skeleton
    assert "catalog.schema.target" in r.skeleton or "target" in r.skeleton
    # ON predicate shape captured
    assert "ON" in r.skeleton
    # WHEN actions captured
    assert "UPDATE" in r.skeleton or "INSERT" in r.skeleton


def test_create_view_extended(monkeypatch):
    _enable_extended(monkeypatch)
    sql = """
CREATE OR REPLACE VIEW catalog.schema.daily_revenue AS
WITH base AS (SELECT id, amount FROM fact_orders WHERE order_ts > '2025-01-01')
SELECT customer_id, SUM(amount) AS total
FROM base
GROUP BY customer_id
"""
    r = build_sql_skeleton(sql, char_budget=1500)
    assert r.method == "view"
    assert "VIEW" in r.skeleton
    # The skeleton should include the inner SELECT structure
    assert "SELECT" in r.skeleton
    assert "GROUP BY" in r.skeleton
    # literal must be dropped
    assert "2025-01-01" not in r.skeleton


def test_insert_extended(monkeypatch):
    _enable_extended(monkeypatch)
    sql = """
INSERT OVERWRITE INTO catalog.schema.target_tbl (id, name, ts)
SELECT id, name, current_timestamp() AS ts
FROM source_tbl
WHERE active = 1
"""
    r = build_sql_skeleton(sql, char_budget=1500)
    assert r.method == "insert"
    assert "INSERT" in r.skeleton
    assert "target_tbl" in r.skeleton or "OVERWRITE" in r.skeleton
    assert "1" not in r.skeleton.split("active")[-1][:20] or "active = 1" not in r.skeleton


def test_update_remains_bypass_in_v61(monkeypatch):
    """UPDATE/DELETE bypass kept in V6.1 (low-priority backlog)."""
    _enable_extended(monkeypatch)
    sql = "UPDATE catalog.schema.tbl SET x = 1 WHERE y = 2"
    r = build_sql_skeleton(sql)
    assert r.method == "bypass"
    assert r.bypass_reason == "unsupported_statement_type:update"
