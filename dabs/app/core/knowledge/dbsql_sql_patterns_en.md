# SQL Rewrite Patterns for Query Optimization

## SQL Rewrite Patterns
<!-- section_id: sql_patterns -->

Rewriting SQL can dramatically improve query performance. Below are proven optimization patterns.

---

### 1. UNION ALL vs UNION

`UNION` performs sort and deduplication, which can be **10-100x slower** on large datasets.

**Anti-pattern:**
```sql
-- UNION when dedup is unnecessary (sort + dedup overhead)
SELECT material_id FROM query1
UNION
SELECT material_id FROM query2
UNION
SELECT material_id FROM query3;
```

**Recommended:**
```sql
-- UNION ALL + final DISTINCT only if needed
SELECT DISTINCT material_id FROM (
    SELECT material_id FROM query1
    UNION ALL
    SELECT material_id FROM query2
    UNION ALL
    SELECT material_id FROM query3
);
```

---

### 2. EXISTS vs IN (Large Datasets)

`IN` evaluates the entire inner table, while `EXISTS` stops at first match.

**Anti-pattern:**
```sql
SELECT * FROM orders
WHERE customer_id IN (
    SELECT customer_id FROM customers WHERE status = 'ACTIVE'
);
```

**Recommended:**
```sql
SELECT * FROM orders o
WHERE EXISTS (
    SELECT 1 FROM customers c
    WHERE c.customer_id = o.customer_id
    AND c.status = 'ACTIVE'
);
```

---

### 3. Filter Early

> **Note on TEMP VIEW usage in this section:** The examples below use `CREATE OR REPLACE TEMP VIEW` purely to structure SQL for readability and step-wise composition. Like CTEs, a TEMP VIEW does **not** guarantee materialization or eliminate re-computation — it is a catalog alias only. To remove duplicate work, persist the shared result with `CTAS` / a Delta table, or rewrite the query so the shared subplan runs once. Verify reuse via `ReusedExchange` in `EXPLAIN` under AQE.

Apply filters **before** JOINs to reduce data volume.

**ES Ticket Pattern:** Filter Early is the most reproducible win. Applying selective predicates before JOINs, MERGE, and aggregates cuts downstream shuffle, spill, and OOM risk together.

**Anti-pattern:**
```sql
SELECT * FROM large_table1
JOIN large_table2 ON ...
JOIN large_table3 ON ...
WHERE large_table1.status = 'ACTIVE'
  AND large_table2.type = 'VALID';
```

**Recommended:**
```sql
CREATE OR REPLACE TEMP VIEW filtered_table1 AS
SELECT * FROM large_table1 WHERE status = 'ACTIVE';

CREATE OR REPLACE TEMP VIEW filtered_table2 AS
SELECT * FROM large_table2 WHERE type = 'VALID';

SELECT * FROM filtered_table1
JOIN filtered_table2 ON ...;
```

---

### 4. Eliminate Correlated Subqueries

Correlated subqueries execute for each outer row and are extremely slow.

**Anti-pattern:**
```sql
SELECT material_id,
    (SELECT MAX(price) FROM prices p
     WHERE p.material_id = m.material_id) as max_price
FROM materials m;
```

**Recommended:**
```sql
SELECT m.material_id, p.max_price
FROM materials m
LEFT JOIN (
    SELECT material_id, MAX(price) as max_price
    FROM prices GROUP BY material_id
) p ON m.material_id = p.material_id;
```

---

### 5. Remove Unnecessary DISTINCT

`DISTINCT` involves sort and deduplication; using it in intermediate steps adds unnecessary cost.

**Anti-pattern:**
```sql
CREATE OR REPLACE TEMP VIEW step1 AS
SELECT DISTINCT * FROM table1 WHERE ...;

CREATE OR REPLACE TEMP VIEW step2 AS
SELECT DISTINCT * FROM step1 JOIN table2 ...;
```

**Recommended:**
```sql
CREATE OR REPLACE TEMP VIEW step1 AS
SELECT * FROM table1 WHERE ...;

CREATE OR REPLACE TEMP VIEW step2 AS
SELECT * FROM step1 JOIN table2 ...;

-- DISTINCT only at final output
SELECT DISTINCT * FROM step2;
```

---

### 6. Decompose Complex JOIN Conditions

CASE expressions in JOIN conditions prevent the optimizer from choosing optimal strategies.

**Anti-pattern:**
```sql
SELECT * FROM table1 a
JOIN table2 b ON
    CASE WHEN a.type = 'A' THEN a.id_a
         WHEN a.type = 'B' THEN a.id_b
         ELSE a.id_c END = b.id;
```

**Recommended:**
```sql
CREATE OR REPLACE TEMP VIEW normalized_table1 AS
SELECT
    CASE WHEN type = 'A' THEN id_a
         WHEN type = 'B' THEN id_b
         ELSE id_c END as join_id, *
FROM table1;

SELECT * FROM normalized_table1 a
JOIN table2 b ON a.join_id = b.id;
```

---

### 7. Sargable Filter Conditions

Applying functions to columns disables index and Data Skipping.

**ES Ticket Pattern:** Non-sargable predicates weaken not only Data Skipping but **statistics-driven optimization overall**. The most common fix is rewriting `WHERE YEAR(col) = 2024` to a range on the column. Avoid functions on the column; use ranges or pre-normalized columns.

**Anti-pattern:**
```sql
WHERE YEAR(order_date) = 2024
WHERE UPPER(customer_name) = 'JOHN'
```

**Recommended:**
```sql
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
```

---

### 8. Pre-JOIN Aggregation and Deduplication
<!-- section_id: pre_join_agg -->

Aggregating or deduplicating before JOIN greatly reduces Data Explosion and shuffle volume.

**ES Ticket Pattern:** Many-to-many JOINs are a leading cause of query failures from Data Explosion. When post-JOIN row count exceeds the sum of inputs, duplicate join keys are usually the cause.

**Anti-pattern:**
```sql
-- JOINing tables with duplicate keys as-is -> row count explodes
SELECT * FROM orders o JOIN items i ON o.order_id = i.order_id;
```

**Recommended:**
```sql
-- Aggregate one side before JOIN to turn 1:N toward 1:1
WITH order_summary AS (
  SELECT order_id, SUM(amount) AS total_amount
  FROM orders GROUP BY order_id
)
SELECT s.order_id, s.total_amount, i.*
FROM order_summary s JOIN items i ON s.order_id = i.order_id;
```

**Rule of thumb:** If a JOIN node’s `rows output` far exceeds total input rows, add pre-JOIN aggregation.

---

### Real-World Results: SQL Rewrite Impact

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Execution Time | 2 hours | 2 minutes | **60x faster** |
| Data Scanned | 500 GB | 100 GB | **5x reduction** |
| Memory Usage | 32 GB | 8 GB | **4x reduction** |

**Patterns applied:** Filter Early + UNION ALL + Remove unnecessary DISTINCT + Temp Views
