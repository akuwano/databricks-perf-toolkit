# Databricks SQL Tuning Guide

## Overview
<!-- section_id: overview -->

This document summarizes best practices for Databricks SQL performance tuning. It explains specific optimization techniques based on query profiler analysis results.

---

## 1. I/O Optimization
<!-- section_id: io -->

### 1.1 Liquid Clustering (Recommended)

Liquid Clustering is a new data layout optimization feature that replaces traditional partitioning and Z-Order. **When creating new tables or redesigning tables, consider Liquid Clustering first.**

**Recommendations:**
- Specify columns frequently used in WHERE clauses or JOIN conditions (up to about 4 columns) as clustering keys
- Effective at reducing Shuffle operations
- Particularly effective for high memory usage (100GB or more)
- Data layout is automatically optimized, resulting in low operational overhead
- Simpler design compared to the partitioning + Z-Order combination

**Configuration example:**
```sql
-- When creating a new table
CREATE TABLE my_table (...)
CLUSTER BY (col1, col2);

-- Applying to an existing table
ALTER TABLE my_table
CLUSTER BY (col1, col2);
-- FULL option required to re-cluster existing data (mandatory)
-- Without FULL, only new records are clustered
OPTIMIZE my_table FULL;
```

### 1.1.1 Hierarchical Clustering

Standard Liquid Clustering treats all columns equally. Hierarchical Clustering **fully clusters by low-cardinality columns first**, then processes the remaining columns. This achieves performance equivalent to the traditional "Partitioning + ZORDER" pattern within Liquid Clustering.

**Benchmark results (TPC-DS SF10k):**
- OPTIMIZE time: **31% reduction**
- Write amplification: **26% reduction**
- Query time: **22% reduction**

**When to use:**
- Workloads where **low-cardinality column filters dominate** (dates with ~thousands of values)
- Migrating from Partition + ZORDER to Liquid Clustering
- Multi-tenant/region tables (`WHERE region = 'JP'` is always present)

**When NOT to use:**
- All clustering columns are high-cardinality (e.g., user_id)
- Filter patterns are evenly distributed across all clustering columns
- Small tables (clustering benefit is minimal)

**Caveats:**
- Queries filtering only on non-hierarchical columns may see degraded performance
- Selecting high-cardinality columns as hierarchical increases OPTIMIZE time

**Configuration example:**
```sql
-- Set date column as hierarchical (low cardinality → ideal for hierarchy)
ALTER TABLE my_table
CLUSTER BY (date_col, id_col);

ALTER TABLE my_table SET TBLPROPERTIES(
  'delta.liquid.hierarchicalClusteringColumns' = 'date_col'
);

OPTIMIZE my_table FULL;
```

**Fast writes (Eager Clustering Fast Path):**
- DBR 17.3+: enabled by default
- DBR 17.1/17.2: set table property `delta.liquid.eagerClusteringFastPathMode = 'forceEnabled'`

**Auto-enablement roadmap:**
- DBR 17.3: shadow evaluation (auto-detects low-cardinality columns)
- Future: `CLUSTER BY AUTO` will automatically choose hierarchical clustering

### 1.1.2 Disabling Eager Clustering (Write Overhead Mitigation)
<!-- section_id: eager_clustering_disable -->

When INSERT/MERGE into a Liquid-Clustering table incurs a heavy pre-write shuffle and spill (the "ClusterOnWrite" overhead), you can **disable eager clustering alone** instead of dropping CLUSTER BY entirely. Re-clustering happens later via OPTIMIZE.

**When to apply:**
- A one-shot bulk INSERT/MERGE where shuffle-spill dominates the runtime
- Dropping CLUSTER BY outright is too destructive because you want to keep read-side file pruning
- Regular ETL batches where write latency is the primary SLO

**Configuration:**
```sql
ALTER TABLE <target-table> SET TBLPROPERTIES (
  'delta.liquid.forceDisableEagerClustering' = 'True'
);

-- Subsequent INSERT / MERGE runs now skip the write-time re-shuffle
INSERT INTO <target-table> SELECT ... FROM <source>;

-- Re-cluster afterwards to restore read-side pruning
OPTIMIZE <target-table> FULL;
```

**Trade-offs:**
- ✅ Substantial reduction in write-time shuffle and spill (workloads with ~1 TB spill typically see it disappear)
- ⚠️ Read-side file pruning is degraded until OPTIMIZE runs
- ⚠️ On high-QPS production tables, a scheduled OPTIMIZE is mandatory

**Decision criteria:**
- You want to **keep** the CLUSTER BY definition (metadata used at read time)
- The write cost outweighs `write_frequency × read_degradation_duration`

### 1.2 Partitioning (Limited Use Cases)

Partitioning is recommended only for the following **limited cases**.

**Recommended cases:**
- When **data lifecycle management is the primary goal** (e.g., date-based data deletion or archiving)
- When there is a single coarse-grained filtering axis used at very high frequency (e.g., date), and the number of partitions can be properly managed

**Notes:**
- If the goal is query performance optimization, prioritize Liquid Clustering
- Performance degrades when the number of partitions exceeds several thousand
- Keep the number of partitions to a maximum of about 1000-2000
- Run Optimize per partition unit to avoid many small file configurations

**ES Ticket Pattern (63 tickets):** High-cardinality partitioning (user_id, order_id) is the top cause of small file proliferation, metadata overhead, and parallelism degradation. Prioritize partitioning for data lifecycle management over scan reduction, and limit to coarse axes like date.

### 1.3 Z-Order (Limited Use Cases)

Consider Z-Order **only when Liquid Clustering cannot be used**.

**Use cases (limited):**
- When Liquid Clustering cannot be used for technical reasons (feature constraints, compatibility issues, etc.)
- When you want to improve multi-column filter locality while maintaining existing partition designs

**Configuration notes:**
- Aim for about 4 columns
- For tables with many columns, place Z-Order target columns within the first 32 columns
- Requires periodic execution of `OPTIMIZE ... ZORDER BY` (higher operational overhead than Liquid Clustering)

**ES Ticket Pattern (42 tickets):** Z-Order is unreliable on columns without stats or on frequently updated tables. Effectiveness diminishes sharply beyond three columns. Prefer Liquid Clustering when available; treat Z-Order as a legacy-table complement only.

**Reference:** https://kb.databricks.com/delta/zordering-ineffective-column-stats

### 1.3.1 Statistics Freshness and Data Skipping

Data Skipping, AQE, and broadcast decisions **depend on fresh column statistics**. When performance degrades, review not only the execution plan but also stale statistics and skewed file layout.

**ES Ticket Pattern (68 tickets):** The top causes of disabled Data Skipping are **functions on filter columns** and **missing statistics**. `WHERE YEAR(date_col) = 2024` does not push down — rewrite to `WHERE date_col >= '2024-01-01'`. Use `SHOW TBLPROPERTIES` to verify min/max column statistics.

### 1.4 Selecting Clustering Key Candidates Using the Query Profile

#### Identifying clustering key candidates
- Filter conditions from the profiler are displayed in the Scan section of each table
- The columns used there are candidates for Liquid Clustering clustering keys
- **Note:** When filter conditions are not directly applied to the target table (e.g., in join processing), Filter conditions may not appear in the profiler. Always review the query itself

#### Checking filter conditions from the WHERE clause
- When filter conditions are not directly applied to the target table due to join processing, Filter conditions may not appear in the profiler during query analysis
- Even in such cases, runtime filters still function, so review the WHERE clause of the query to select columns

### 1.5 Verifying I/O Reduction Effects

You can verify this from the runtime metrics information in the profiler:

| Metric | Description |
|--------|-------------|
| Files pruned | Number of files skipped from READ |
| Files read | Number of files READ |
| Files to read before dynamic pruning | Number of files before dynamic pruning |
| Partitions read | Number of partitions READ |
| Size of files pruned | Size of pruned files |
| Size of files read | Size of files read |
| Size of data read with io requests | Actual data size read with I/O requests |

**How to calculate filter rate:**
```
Pruning efficiency = Size of files pruned / (Size of files read + Size of files pruned)
```
A higher value indicates that I/O reduction is functioning efficiently.

### 1.6 Verifying Predictive I/O Effects

Check the following items:
- `data filters - batches skipped`
- `data filters - rows skipped`

---

## 2. Execution Plan Improvement
<!-- section_id: execution_plan -->

### 2.1 Overview of Join Types

Check the join processing plan in the query profile. While it depends on conditions such as data volume, in most cases it is desirable to have only the two plans processable by the Photon engine: BROADCAST and SHUFFLE_HASH.

> **Note:** While SHUFFLE_HASH is listed as Photon-supported, actual Photon execution depends on
> the join type (INNER vs OUTER), key shape, and full plan context. LEFT OUTER JOIN with SHUFFLE_HASH
> may still fall back to non-Photon in some DBR versions. After applying SHUFFLE_HASH hints,
> always verify Photon execution in the query profile.

**Performance ranking (faster toward the right):**
```
Shuffle-nested-loop join < Sort-merge join < Shuffle-hash join < Broadcast join
```

Reference: https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-hints#join-hint-types

#### Broadcast Join
- Requires one side of the data to be smaller than the threshold
- No shuffle or sort required
- Very fast
- **Photon engine supported**

#### Shuffle-Hash Join
- Requires shuffle but no sort
- Can handle large tables
- Selects the smaller side (based on statistics)
- May cause out-of-memory errors (OOM) with significant data skew
- **Photon engine supported**

#### Sort-Merge Join
- Most robust but requires shuffle and sort, consuming large amounts of resources
- Handles any data size
- Can be slower for small table sizes
- **Not supported by Photon engine**

#### Shuffle-Nested-Loop Join
- May reduce resource usage in some cases
- Row retrieval starts quickly but may take time to complete the entire process
- Not used for equi-joins
- **Photon engine supported**

### 2.2 Slow Query Plan Example: Sort-Merge Join

Symptoms where Sort-Merge joins cause massive disk spill and Photon engine is not used, resulting in a very low Photon task processing ratio:
- Slow Sort-Merge join is selected
- Low proportion of task processing by the Photon engine (ideally Photon should handle the majority)
- Massive disk spill occurring (ideally spill should be 0 bytes)

### 2.3 Tuning Steps for Optimal Execution Plan Generation

#### Running ANALYZE
Retrieve the latest statistics for all tables used in each query, including small master tables.

```sql
ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;
```

AQE (Adaptive Query Execution) optimizes query plans based on runtime statistics collected during query execution, but the Photon engine also uses table statistics simultaneously.

#### Spark Config Parameter Changes

Representative parameters for adjusting join processing execution plans:

##### spark.sql.autoBroadcastJoinThreshold
- **Default:** 10MB
- **Recommended:** 200MB

Threshold for table size to execute broadcast joins. Be cautious as larger sizes can lead to OOM errors. Increasing the DBSQL cluster size can resolve OOM errors, but performance may degrade for values above 400MB.

##### spark.sql.join.preferSortMergeJoin
- **Default:** true
- **Recommended:** false

Specifies whether to use sort-merge as the default join strategy for large datasets. With `spark.sql.adaptive.enabled=true` (default), faster BROADCAST/SHUFFLE_HASH tends to be selected, but setting this to false can change the plan when SortMerge plans persist.

##### spark.databricks.adaptive.joinFallback
- **Default:** false
- **Recommended:** true

A Databricks-specific parameter. Instead of raising an error when the broadcast join threshold is exceeded, it selects SHUFFLE_HASH. The threshold can also be set with `spark.databricks.adaptive.joinFallback.threshold`, but the value of `spark.sql.autoBroadcastJoinThreshold` appears to take precedence.

### 2.4 Healthy Query Plan Example: Broadcast/Shuffled-Hash Join

It is desirable to have Broadcast or Shuffled-Hash joins with no disk spill and a high Photon task processing ratio:
- A fast join plan is selected
- The proportion of task processing by the Photon engine should constitute the majority
- Ideally disk spill is 0 bytes

If spill still occurs after improving the join plan, consider scaling up the cluster size. Additionally, increasing the `spark.sql.shuffle.partitions` parameter subdivides tasks, reducing memory usage per task and potentially improving spill, but this is normally not necessary.

---

## 2A. Causes of Frequent Hash Table Resize
<!-- section_id: hash_resize_causes -->

**What hash resize means**: Photon pre-allocates a hash table for hash-based operations (JOIN build side, GROUP BY, DISTINCT, etc.) based on row-count estimates. When the estimate is wrong and the table overflows, Photon doubles the capacity and rebuilds — each cycle is a "resize". Normal queries have < 10. 100+ is a warning, 1,000+ is abnormal, and tens of thousands means the row-count estimate is systematically wrong — a data/structural issue, not "just slow".

**Why "Run ANALYZE TABLE" is often a misleading fix**: Stale statistics is only one of many causes. In the following cases, re-running ANALYZE will not help:
- ANALYZE TABLE has already been run recently
- **Predictive Optimization** is enabled and maintains statistics automatically
- EXPLAIN `Optimizer Statistics` shows all tables are `full`

Recommending ANALYZE in these cases is misleading. **Instead, investigate these 8 causes in order**:

#### Recommended investigation order

1. **Row explosion** — Missing filter or wrong JOIN predicate producing more rows than expected
   - Compare result row count against business expectations
   - `EXPLAIN` estimated rows vs actual rows_output per node
   - Verify post-JOIN row count is sensible vs the sum/product of input tables

2. **Duplicate GROUP BY / Re-computed aggregation** — Same-key aggregation repeated across CTEs or UNION branches
   - Check `EXPLAIN` for `ReusedExchange`
   - Consolidate identical `GROUP BY <col>` patterns into a single CTE
   - For parallel GROUP BY across UNION branches, aggregate once upstream

3. **Key value skew** — Heavy hitters (one value concentrating many rows)
   ```sql
   SELECT <key_col>, COUNT(*) AS n
   FROM <table>
   GROUP BY 1
   ORDER BY n DESC
   LIMIT 20;
   ```
   - If the top value is >= 5% of total rows, apply skew mitigation (AQE skew join handling, salting, pre-aggregation)

4. **NULL concentration** — Large null counts on JOIN/GROUP keys
   ```sql
   SELECT COUNT(*) - COUNT(<key_col>) AS null_count,
          COUNT(*)                    AS total
   FROM <table>;
   ```
   - NULLs collapse into a single hash partition — a special case of skew
   - Push `WHERE <key_col> IS NOT NULL` upstream, or use null-safe JOIN predicates

5. **JOIN key type mismatch** — Different types on left/right force implicit CAST, causing hash collisions
   - e.g. `decimal(10,0) ↔ bigint`, `string ↔ int`
   - Align types at the DDL layer (preferred) or with pre-JOIN projections
   - Physically same values may hash into different buckets under different types

6. **High-precision DECIMAL keys / inappropriate data types** — `DECIMAL(38,0)` is heavier than BIGINT
   - If values are actually integer, consider `ALTER TABLE ... ALTER COLUMN <col> TYPE BIGINT`
   - Reduces hash cost, row memory footprint, and comparison cost
   - Apply the change on both sides of the JOIN to avoid introducing a type mismatch
   - **For large shuffle / aggregation workloads (> 10 GB), always review data-type appropriateness regardless of whether schema can be fetched.** Run:
     ```sql
     DESCRIBE TABLE <fqn>;
     ```
     Common wastes:
     - DECIMAL(38,0) storing INTEGER-range values → migrate to BIGINT (2-5x cost reduction)
     - STRING holding numeric or date values → migrate to numeric / DATE / TIMESTAMP
     - Oversized VARCHAR where actual max length is small
   - At large volumes the per-row cost difference compounds significantly, dominating total cost

7. **UDF / Non-deterministic predicates** — Optimizer cannot estimate row count, hash table sized too small
   - UDFs produce unpredictable row reductions/expansions
   - Predicates with `rand()`, `current_timestamp()`, etc. break push-down and estimation
   - Replace UDFs with built-in functions; move non-deterministic expressions into a pre-computed column

8. **Memory pressure** — Other operators consuming memory force the hash table to rebuild repeatedly
   - Check `spill_bytes` / `num_spills_to_disk` / OOM fallback
   - Scale up the cluster, or raise `spark.sql.shuffle.partitions` to reduce per-task memory usage

#### Diagnostic SQL (requires SELECT on the target tables)

Cardinality / NULL / heavy-hitter checks on a hot column:
```sql
-- Cardinality check
SELECT
  COUNT(DISTINCT <key_col>) AS distinct_values,
  COUNT(*)                  AS total_rows,
  COUNT(*) - COUNT(<key_col>) AS null_count,
  ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT <key_col>), 0), 1) AS avg_rows_per_value
FROM <fqn>;

-- Top 20 values (skew detection)
SELECT <key_col>, COUNT(*) AS n
FROM <fqn>
GROUP BY 1
ORDER BY n DESC
LIMIT 20;
```

#### Decision flow

```
hash_table_resize_count is high
  ├─ EXPLAIN EXTENDED Optimizer Statistics? ──┐
  │                                            │
  │  only full (no missing/partial)            │
  │  ↓                                          │
  │  → ANALYZE is ineffective. Investigate 8 causes
  │                                            │
  │  missing/partial present                   │
  │  ↓                                          │
  │  → Run ANALYZE first; if no improvement, 8 causes
  │                                            │
  └─ No EXPLAIN attached                       │
     ↓                                          │
     → If predictive optimization is enabled, 8 causes first.
       Otherwise try ANALYZE, then fall back to 8 causes.
```

---

## 3. Shuffle Optimization
<!-- section_id: shuffle -->

### 3.1 Memory Efficiency Criteria

Criteria for evaluating memory efficiency of Shuffle operations:

| Metric | Threshold | Description |
|--------|-----------|-------------|
| Memory per partition | ≤512MB | Optimization needed if exceeded |
| High memory usage threshold | 100GB | Liquid Clustering recommended |
| Long-running threshold | 300 seconds | Review data distribution strategy |

### 3.2 Shuffle Optimization Priority

| Memory/Partition | Priority | Recommended Action |
|-----------------|----------|-------------------|
| >2GB | High | Scale up cluster or significantly increase partition count |
| 1GB-2GB | High | Increase partition count, adjust AQE settings |
| 512MB-1GB | Medium | Partition count adjustment recommended |
| ≤512MB | Low | Efficient state |

### 3.3 Using REPARTITION Hints

When Shuffle operations occur in SQL queries, set the following hints appropriately:

```sql
-- Standard repartition
SELECT /*+ REPARTITION(100, column1, column2) */ ...

-- When using Window functions
SELECT /*+ REPARTITION_BY_RANGE(column1) */ ...
```

### 3.4 AQE (Adaptive Query Execution) Settings

AQE dynamically optimizes based on runtime statistics collected during query execution.

**Recommended Spark parameters:**

```sql
-- Target partition size (512MB or less recommended)
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 536870912;

-- Enable partition coalescing
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;
SET spark.sql.adaptive.coalescePartitions.maxBatchSize = 100;

-- Enable skew join
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 536870912;
```

### 3.5 Checking AQEShuffleRead Metrics

Check the following metrics in the profiler:

| Metric | Description |
|--------|-------------|
| AQEShuffleRead - Number of partitions | Number of partitions |
| AQEShuffleRead - Partition data size | Partition data size |
| AQEShuffleRead - Number of skewed partitions | Number of skewed partitions |

**Calculating average partition size:**
```
Average partition size = Partition data size / Number of partitions
```

A skew warning is triggered when it exceeds 512MB.

### 3.5 Auto Shuffle Partitions (Pro/Classic only)

For Pro and Classic SQL Warehouses (NOT Serverless), setting `spark.sql.shuffle.partitions = auto` enables AQE to dynamically determine the optimal partition count based on actual data sizes at each shuffle stage. This is often preferable to a fixed value (default 200) when query data volumes vary significantly.

```sql
SET spark.sql.shuffle.partitions = auto;
```

**Note:** On Serverless SQL Warehouses, `auto` is already set internally and this parameter cannot be changed by users.

---

## 3A. Consider Dominant Shuffle Keys as Liquid Clustering Candidates
<!-- section_id: lc_shuffle_key_candidate -->

When the profiler's Shuffle Details section shows a shuffle with **GiB-scale writes or memory-inefficiency (>128 MB/partition)** and its `partitioning key` is a column of the target table, evaluate that column as a **Liquid Clustering key candidate**. Clustering co-locates same-value rows into the same file, which shrinks shuffle volume for repeat queries sharing the same GROUP BY / JOIN pattern (co-located aggregation).

**Decision criteria:**
1. Columns that contribute to **pruning** (WHERE filters) take precedence — LC's primary purpose is I/O reduction.
2. If the shuffle key's cardinality is **extremely low (<10 distinct values)**, do NOT use it as a sole key. Use Hierarchical Clustering (`delta.feature.hierarchicalClustering`) to place the low-cardinality key at the lower level, or combine with another higher-cardinality key.
3. If the shuffle is small (<1 GiB written, memory-efficient), skip it — a REPARTITION hint is sufficient.

**Acceptable example:**
- `ce.lineitem_usagetype` as the GROUP BY shuffle key with 146 GB written + 981 GB peak memory, cardinality 20–50 → adopt `CLUSTER BY (usage_date, lineitem_usagetype)` combining with a date column.

**Unacceptable examples:**
- Shuffle key is `synthetic_partition_id` (expression-based virtual column) → not an LC candidate.
- Cardinality 3 `region_type` alone → use as the lower tier of Hierarchical Clustering or combine with another key.

---

## 4. Spill (Disk Spill) Detection and Remediation
<!-- section_id: spill -->

### 4.1 Spill Detection Metrics

Check the following metrics in the profiler:

| Metric | Description |
|--------|-------------|
| Num bytes spilled to disk due to memory pressure | Disk spill caused by memory pressure |
| Sink - Num bytes spilled to disk due to memory pressure | Spill at Sink nodes |

### 4.2 Remediation for Spill

1. **Emergency measures (high priority)**
   - Scale up cluster size (increase worker node count)
   - Switch to high-memory instance types

2. **Short-term measures**
   - `spark.sql.adaptive.coalescePartitions.enabled = true`
   - `spark.sql.adaptive.skewJoin.enabled = true`
   - Adjust partition count

3. **Medium to long-term measures**
   - Explicitly specify partition count (`.repartition()`)
   - Optimize JOIN strategy (leverage broadcast JOINs)
   - Implement Liquid Clustering
   - Optimize table design

### 4.3 Spill Severity Assessment

| Spill Amount | Severity | Action |
|-------------|----------|--------|
| >5GB | Critical | Memory configuration and partition strategy review required |
| >1GB | Important | Optimization strongly recommended |
| >0 | Attention | Monitor and consider improvement |
| 0 | Ideal | Optimal state |

---

## 5. Improving Photon Utilization
<!-- section_id: photon -->

### 5.1 Checking Photon Efficiency

How to verify that processing is efficiently executed by the Photon engine:
- Check from query history
- Visual inspection in Spark UI (recommended)

**Photon efficiency calculation:**
```
Photon efficiency = photon_total_time_ms / task_total_time_ms
```

### 5.2 Photon Efficiency Criteria

| Efficiency | Rating | Description |
|-----------|--------|-------------|
| >80% | High | Good state |
| 50-80% | Medium | Room for improvement |
| <50% | Low | Optimization needed |

### 5.3 Identifying Photon-Unsupported Operations

Identify SQL operations not supported by the Photon engine from plan detail information and consider improvement options.

> Checked: 2026/02/15 / DBR 18.0

#### Photon Support Status by JOIN Type

| JOIN Type | Photon Support |
|-----------|---------------|
| Broadcast join | Supported |
| Shuffle-hash join | Supported |
| Sort-merge join | **Not supported** |
| Shuffle-nested-loop join | Supported |

#### Photon-Unsupported SQL Functions (Unimplemented)

The following functions are not supported by the Photon engine as of DBR 18.0. When these functions are used in queries, Photon acceleration is not applied.

**Aggregate Functions:**

| SQL Function | Description |
|-------------|-------------|
| `percentile_cont` | Continuous percentile |
| `percentile_disc` | Discrete percentile |
| `listagg`, `string_agg` | String aggregation |
| `bool_and`, `every` | Logical AND aggregation |
| `bool_or`, `any`, `some` | Logical OR aggregation |
| `collect_set` | Deduplicated list aggregation (*`collect_list`/`array_agg` supported since DBR 9.0*) |
| `count_if` | Conditional count |
| `count_min_sketch` | Count-min sketch |
| `covar_pop`, `covar_samp` | Covariance |
| `try_avg` | try_average |
| `try_sum` | try_sum |
| `var_pop` | Population variance (*`var_samp` supported since DBR 10.1*) |
| `regr_count`, `regr_r2`, `regr_sxx`, `regr_sxy`, `regr_syy` | Regression analysis functions |
| `measure` | Measure |

**Array Functions:**

| SQL Function | Description |
|-------------|-------------|
| `aggregate`, `reduce` | Array folding |
| `array_append` | Append to end of array |
| `array_prepend` | Prepend to beginning of array |
| `array_compact` | Remove NULLs |
| `array_insert` | Insert into array |
| `shuffle` | Array shuffle |
| `zip_with` | Zip two arrays |

**String Functions:**

| SQL Function | Description |
|-------------|-------------|
| `left` | Get N characters from left |
| `right` | Get N characters from right |
| `search`, `isearch` | Text search |

**Numeric Functions:**

| SQL Function | Description |
|-------------|-------------|
| `e` | Euler's number |
| `positive` | Unary plus |
| `try_divide` | Safe division |
| `try_mod`, `try_remainder` | Safe modulo |
| `try_multiply` | Safe multiplication |

**Map Functions:**

| SQL Function | Description |
|-------------|-------------|
| `map_contains_key` | Key existence check |
| `map_filter` | Map filter |
| `map_zip_with` | Map zip |
| `transform_keys` | Key transformation |
| `transform_values` | Value transformation |

**Date/Time Functions:**

| SQL Function | Description |
|-------------|-------------|
| `localtimestamp` | Local timestamp |
| `make_timestamp_ltz`, `try_make_timestamp_ltz` | LTZ timestamp generation |
| `make_timestamp_ntz`, `try_make_timestamp_ntz` | NTZ timestamp generation |
| `try_to_timestamp` | Safe timestamp conversion |
| `to_time`, `try_to_time` | TIME type conversion |
| `try_to_date` | Safe date conversion |
| `current_time`, `make_time` | TIME type related |
| `session_window`, `window`, `window_time` | Window functions |

**Operators:**

| SQL Function | Description |
|-------------|-------------|
| `between` | Range operator |
| `ilike` | Case-insensitive LIKE |

**CSV/XML/JSON Functions:**

| SQL Function | Description |
|-------------|-------------|
| `from_csv`, `to_csv`, `schema_of_csv` | CSV conversion |
| `from_xml`, `to_xml`, `schema_of_xml` | XML conversion |
| `schema_of_json`, `schema_of_json_agg` | JSON schema inference |

**XPath Functions:** `xpath`, `xpath_boolean`, `xpath_double`, `xpath_float`, `xpath_int`, `xpath_long`, `xpath_short`, `xpath_string` — all unsupported

**AI Functions:** `ai_gen`, `ai_query`, `ai_classify`, `ai_similarity`, `ai_summarize`, `ai_translate`, `ai_extract`, `ai_mask`, `ai_fix_grammar`, `ai_analyze_sentiment`, `ai_generate_text`, `ai_complete`, `ai_embed`, `ai_parse_document` — all unsupported (executed on CPU)

**Geospatial (unsupported only):**

| SQL Function | Description |
|-------------|-------------|
| `h3_getpentagoncellids` | Get H3 pentagon cell IDs |
| `h3_tessellateaswkb`, `h3_try_tessellateaswkb` | Tessellation |
| `st_buffer`, `st_difference`, `st_distance` | Spatial operations |
| `st_envelope_agg`, `st_union_agg` | Spatial aggregation |
| `st_intersection`, `st_union` | Spatial union |
| `st_simplify` | Spatial simplification |

**Miscellaneous:**

| SQL Function | Description |
|-------------|-------------|
| `java_method`, `reflect`, `try_reflect` | Java invocation |
| `assert_true` | Assertion |
| `typeof` | Type information |
| `current_version` | Version retrieval |
| `grouping`, `grouping_id` | GROUPING SETS related |
| `zstd_compress`, `zstd_decompress`, `try_zstd_decompress` | Compression/decompression |
| `to_avro`, `from_avro` | Avro serialization |
| `to_protobuf`, `from_protobuf` | Protobuf serialization |
| `uniform` | Uniform distribution random number |

#### Major Functions Added by DBR Version for Photon Support

| DBR | Major Functions Added |
|-----|---------------------|
| 8.3 | Basic arithmetic (`+`, `-`, `*`, `/`), comparison operators, `cast`, `count`, `sum`, `avg`, `min`, `max`, `concat`, `substr`, `lower`, `upper`, `trim`, date/time functions (`date_add`, `date_diff`, `to_timestamp`, etc.) |
| 8.4 | `map`, `explode`, `sqrt`, `exp`, `cbrt`, `log2` |
| 9.0 | `collect_list`/`array_agg`, `width_bucket` |
| 9.1 | `regexp_replace`, `base64`, `unbase64`, `hex`, `unhex`, `posexplode`, trigonometric functions (`atan`, `atan2`, `tan`) |
| 10.0-10.1 | `array_distinct`, `array_except`, `array_intersect`, `array_union`, `var_samp`, `chr`, `levenshtein`, `soundex` |
| 10.4 | Window functions (`row_number`, `rank`, `dense_rank`, `lead`, `lag`, `nth_value`, `ntile`), `percentile`, `transform`, `filter`, `md5`, `sha1`, `sha2`, `aes_encrypt`/`aes_decrypt`, trigonometric functions (`sin`, `cos`, `asin`, `acos`) |
| 11.1-11.3 | `approx_count_distinct`, `from_json`, `to_json`, `get_json_object`, `map_keys`, `map_values`, `map_entries`, `map_from_entries`, `flatten`, `split_part`, `initcap`, hyperbolic functions, `try_add`, `try_subtract` |
| 13.1-13.3 | `approx_percentile`, `format_string`, `to_number`, `parse_url`, `sort_array`, `array_sort`, `mask`, `luhn_check`, HLL functions |
| 14.0-14.3 | `max_by`, `min_by`, `corr`, `skewness`, `kurtosis`, Geospatial (most `st_*` functions), `parse_json`, `variant_get`, Bitmap functions |
| 15.0-15.4 | `collate`, `collation`, `nullifzero`, `zeroifnull`, `spark_partition_id`, UTF-8 validation functions, `convert_timezone`, Variant related |
| 16.0-16.4 | `elt`, `st_contains`, `st_covers`, `st_within`, `st_intersects`, `st_transform`, `try_parse_url`, `dayname` |
| 17.1-17.3 | `hll_union_agg`, `bitmap_and_agg`, `st_dump`, `st_dwithin`, `st_exteriorring`, `st_interiorringn`, `st_numinteriorrings`, `try_url_decode` |
| 18.0 | `randstr`, `approx_top_k`, Geospatial (`st_azimuth`, `st_boundary`, `st_closestpoint`, `st_geogfromewkt`, `st_geomfromewkt`, `st_isvalid`, `st_makeline`, `st_makepolygon`) |

#### Workarounds for Photon-Unsupported Functions

| # | Unsupported Function/Syntax | Photon-Compatible Alternative | SQL Rewrite Example | Notes |
|---|---------------------------|------------------------------|-------------------|-------|
| 1 | `collect_set(col)` | `array_distinct(collect_list(col))` | `SELECT array_distinct(collect_list(col)) FROM t GROUP BY key` | `collect_list` supported since DBR 9.0. Result order is not guaranteed |
| 2 | `percentile_cont(0.5)` / `percentile_disc(0.5)` | `percentile(col, 0.5)` or `approx_percentile(col, 0.5)` | `SELECT percentile(salary, 0.5) FROM employees` | `percentile` supported since DBR 10.4, `approx_percentile` since DBR 13.1. approx is approximate but faster |
| 3 | `left(str, n)` | `substr(str, 1, n)` | `SELECT substr(name, 1, 3) FROM t` | `substr` supported since DBR 8.3 |
| 4 | `right(str, n)` | `substr(str, -n)` | `SELECT substr(name, -3) FROM t` | Same as above |
| 5 | `ilike` | `lower(col) LIKE lower(pattern)` | `WHERE lower(name) LIKE lower('%tokyo%')` | `lower` and `LIKE` supported since DBR 8.3 |
| 6 | `between` | `col >= low AND col <= high` | `WHERE price >= 100 AND price <= 500` | Comparison operators supported since DBR 8.3 |
| 7 | Sort-merge join | Broadcast JOIN / Shuffle Hash JOIN | `SELECT /*+ BROADCAST(small_t) */ ... FROM large_t JOIN small_t` | Also see tuning steps in section 2.3 |
| 8 | `count_if(cond)` | `count(CASE WHEN cond THEN 1 END)` | `SELECT count(CASE WHEN status = 'active' THEN 1 END) FROM t` | `count`+`CASE` supported since DBR 8.3 |
| 9 | `bool_and(col)` / `every(col)` | `min(CAST(col AS INT)) = 1` | `SELECT min(CAST(is_valid AS INT)) = 1 FROM t GROUP BY key` | Alternative for logical AND. Semantically equivalent |
| 10 | `bool_or(col)` / `any(col)` | `max(CAST(col AS INT)) = 1` | `SELECT max(CAST(is_active AS INT)) = 1 FROM t GROUP BY key` | Alternative for logical OR |
| 11 | `listagg(col, ',')` / `string_agg(col, ',')` | `array_join(collect_list(col), ',')` | `SELECT array_join(collect_list(name), ', ') FROM t GROUP BY dept` | Combination of `collect_list` (DBR 9.0) + `array_join`. Use ORDER BY clause for ordering |
| 12 | `try_divide(a, b)` | `CASE WHEN b != 0 THEN a / b ELSE NULL END` | `SELECT CASE WHEN cnt != 0 THEN total / cnt ELSE NULL END FROM t` | Explicit branching for division-by-zero avoidance |
| 13 | `try_multiply(a, b)` | `a * b` (when overflow is acceptable) | Direct multiplication as alternative. When overflow detection is not needed | Handle overflow detection at the application layer if needed |
| 14 | `var_pop(col)` | `var_samp(col) * (count(col) - 1) / count(col)` | Calculate population variance from sample variance | `var_samp` supported since DBR 10.1. Approximately equivalent for large samples |
| 15 | `from_csv(str, schema)` | CTE with `split` + `CAST` combination | `WITH parsed AS (SELECT split(csv_col, ',') AS cols FROM t) SELECT CAST(cols[0] AS INT), cols[1] FROM parsed` | Effective only for fixed schemas. Not suitable for dynamic schemas |
| 16 | `map_filter(map, func)` | `map_from_entries(filter(map_entries(map), e -> cond))` | `SELECT map_from_entries(filter(map_entries(m), x -> x.value > 0)) FROM t` | `filter` supported since DBR 10.4, `map_entries`/`map_from_entries` since DBR 11.1 |
| 17 | Complex Window Frame | Rewrite to ROWS frame | Change to `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | RANGE frame + non-standard boundaries may be unsupported by Photon |
| 18 | Python/Scala UDF | Rewrite using built-in SQL functions | Decompose UDF logic into SQL expressions | UDFs always run on JVM/Python. This is the most impactful workaround |

### 5.4 Representative Use Cases for Photon Optimization

The following are concrete examples of Photon optimization frequently encountered in actual query patterns.

#### Case 1: SortMergeJoin to Broadcast/ShuffleHash

**Symptoms:**
- Low Photon utilization (<50%)
- Massive disk spill occurring
- `SortMergeJoin` displayed in the profile

**Signs in EXPLAIN:**
```
== Photon Explanation ==
Query is not fully supported by Photon.
Unsupported node: SortMergeJoin
Reference node: SortMergeJoin [...]
```

**Remediation SQL:**
```sql
-- Before: SortMergeJoin is selected
SELECT o.*, c.name
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- After: Force with Broadcast hint
SELECT /*+ BROADCAST(c) */ o.*, c.name
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- Or, avoid SortMerge globally with Spark parameters
SET spark.sql.join.preferSortMergeJoin = false;
SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB
```

**Expected effects:**
- Photon utilization improves to over 80%
- Disk spill approaches 0
- Execution time is often reduced by 30-70%

#### Case 2: Avoiding PIVOT/Unsupported Aggregate Functions

**Symptoms:**
- Medium Photon utilization (50-80%)
- Only specific aggregate operations fall back to JVM execution

**Signs in EXPLAIN:**
```
== Photon Explanation ==
Query is not fully supported by Photon.
Unsupported aggregation function: pivotfirst(...)
Reference node: HashAggregate [...]
```

**Remediation SQL:**
```sql
-- Before: PIVOT + collect_set
SELECT *
FROM (SELECT dept, status, employee_id FROM employees)
PIVOT (collect_set(employee_id) FOR status IN ('active', 'inactive'));

-- After: Conditional aggregation + array_distinct + collect_list
SELECT
  dept,
  array_distinct(collect_list(CASE WHEN status = 'active' THEN employee_id END))
    AS active_employees,
  array_distinct(collect_list(CASE WHEN status = 'inactive' THEN employee_id END))
    AS inactive_employees
FROM employees
GROUP BY dept;
```

**Expected effects:**
- Decomposing PIVOT into conditional aggregation enables Photon processing
- `collect_set` to `array_distinct(collect_list(...))` enables Photon support

#### Case 3: Simplifying Complex Window Frames

**Symptoms:**
- Photon fallback on specific Window functions
- Caused by RANGE frames or complex boundary specifications

**Signs in EXPLAIN:**
```
== Photon Explanation ==
Query is not fully supported by Photon.
Window frame not supported: RANGE BETWEEN ...
```

**Remediation SQL:**
```sql
-- Before: RANGE frame (may be unsupported by Photon)
SELECT
  id,
  SUM(amount) OVER (
    ORDER BY event_date
    RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
  ) AS rolling_7d
FROM events;

-- After: Approximate with ROWS frame + date filter
SELECT
  e.id,
  (SELECT SUM(e2.amount)
   FROM events e2
   WHERE e2.event_date BETWEEN DATE_SUB(e.event_date, 7) AND e.event_date
  ) AS rolling_7d
FROM events e;

-- Or, pre-partition and convert to a simple ROWS frame
SELECT
  id,
  SUM(amount) OVER (
    PARTITION BY date_trunc('month', event_date)
    ORDER BY event_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_monthly
FROM events;
```

**Expected effects:**
- Window processing becomes executable by Photon
- Converting complex RANGE frames to ROWS also improves processing efficiency

#### Case 4: Replacing UDFs with Built-in Functions

**Symptoms:**
- Very low Photon utilization (<30%)
- Heavy use of Python UDFs or Scala UDFs
- Majority of task processing time consumed by JVM/Python

**Remediation:**
```sql
-- Before: String processing with Python UDF
-- Using parse_address(addr) defined with @udf
SELECT parse_address(address) AS parsed FROM customers;

-- After: Decompose using built-in functions
SELECT
  regexp_extract(address, '^(\\d+)', 1) AS house_number,
  regexp_extract(address, '\\d+\\s+(.*),', 1) AS street,
  regexp_extract(address, ',\\s*([^,]+)$', 1) AS city
FROM customers;
```

**Expected effects:**
- Complete elimination of UDFs dramatically improves Photon utilization (from 30% to over 90% possible)
- Python/JVM serialization overhead is eliminated
- This is the pattern that yields the most dramatic improvement

---

## 6. Cache Efficiency
<!-- section_id: cache -->

### 6.1 Delta Cache Hit Rate

Check the following metrics in the profiler:

| Metric | Description |
|--------|-------------|
| read_bytes | Total bytes read |
| read_cache_bytes | Bytes read from cache |
| read_remote_bytes | Bytes read from remote |

**Cache hit rate calculation:**
```
Cache hit rate = read_cache_bytes / read_bytes
```

### 6.2 Cache Efficiency Criteria

| Hit Rate | Rating | Action |
|----------|--------|--------|
| >80% | High | Good state |
| 50-80% | Medium | Consider scaling up |
| <50% | Low | Cache strategy review needed |

**ES Ticket Pattern (38 tickets):** Caching helps with repeated access and a stable working set, but adds little for one-off large scans. On Serverless, only in-memory cache applies (Delta cache not available). Before blaming insufficient cache, fix scan volume and file layout (whether `OPTIMIZE` has been applied).

### 6.3 Checking Scan Locality

When queries are slow despite good cache hit rates, deterioration of **scan locality** may be the cause. Check the following metrics displayed in verbose mode.

**Metrics to check:**

| Metric | Description |
|--------|-------------|
| Number of local scan tasks | Number of scan tasks where the Executor could read data locally at its initially assigned location |
| Number of non-local (rescheduled) scan tasks | Number of scan tasks where data was not local to the initially assigned Executor and was rescheduled to another Executor |
| Cache hits size | Amount of data read from cache at the node level |
| Cache misses size | Amount of data that resulted in cache misses at the node level |
| Cloud storage request count | Cloud storage request count at the node level |

**Reschedule rate calculation:**
```
Reschedule rate = non-local scan tasks / (local scan tasks + non-local scan tasks)
```

**Criteria:**

| Reschedule Rate | Rating | Action |
|----------------|--------|--------|
| 0-1% | Good | Optimal state |
| 1-5% | Watch | Continue monitoring |
| >5% | Action needed | Root cause identification required (see below) |

#### 6.3.1 Importance of Per-Node Analysis

**Always check the breakdown by Scan node, not just the overall reschedule rate.**

Typical pattern:
```
Table A: local=10, non-local=0  (0%)   <- Normal
Table B: local=15, non-local=26 (63%)  <- Problem
Table C: local=4,  non-local=0  (0%)   <- Normal
Overall: rescheduled=41.9%  <- Dragged down by Table B
```

In this case, the problem is **specific to Table B's Scan**, not the entire cluster.

#### 6.3.2 Root Cause Decision Flow

Non-local scans can have multiple causes, and they are **distinguished by correlation with per-node cache hit rates**:

| Pattern | Non-local Rate | Node Cache Hit Rate | Estimated Cause |
|---------|---------------|-------------------|----------------|
| **Cold node placement** | High | **Very low (<20%)** | No cache on newly added nodes from scale-out |
| **CPU contention rescheduling** | High | Medium to low | Rescheduled to another node due to CPU slot shortage from concurrent queries |
| **File placement issue** | High | Medium to high | Cache exists but file layout is distributed |
| **Dynamic Scan reorganization** | High | Low | Locality lost after task redistribution by Dynamic Scan Coalescing |

**Important decision rules:**
- High non-local + very low cache hits -> **Cold node (scale-out/cold start) is the most likely cause**
- High non-local + other tables normal + large Cloud storage requests only for the affected table -> Evidence of recent scale-out
- High non-local + equally bad across all tables -> Possible excessive Executor count

#### 6.3.3 Typical Patterns in Serverless SQL Warehouse

Scan Locality tends to degrade in Serverless SQL Warehouses for the following reasons:

1. **Frequent scale-out**: Clusters are automatically added based on load. New clusters have no cache, so queries assigned to them must fetch everything from cloud storage
2. **CPU contention from concurrent execution**: When there are many concurrent queries, CPU slots on the preferred location node are full, and queries are rescheduled to another node without cache
3. **Cold start**: After scale-down and subsequent scale-up, previous cache is lost

**How to read the evidence:**
```
[Problematic Scan node]
Number of non-local (rescheduled) scan tasks: 26  <- Not assigned to preferred location
Cache hits size: 1.3 GB                            <- Cache is nearly empty
Cache misses size: 7.3 GB                          <- Massive cache misses
Cloud storage request count: 1081                   <- Full fetch from cloud storage

[Normal Scan node]
Number of non-local (rescheduled) scan tasks: 0    <- Executed at preferred location
Cache hits size: 623 MB                            <- Read from cache
Cache misses size: 0                               <- No cache misses
Cloud storage request count: 3                      <- Minimal requests
```

This contrast is definitive evidence of "assignment to a cold node."

#### 6.3.4 Recommended Actions

Remediation differs depending on the cause:

**For cold node placement (caused by scale-out):**
- Consider a warmup strategy for Serverless WH (pre-cache key tables with dummy queries)
- Distribute concurrent execution peaks to suppress sudden scale-out
- Check scale event frequency in the Warehouse Event Log
- If possible, set the minimum cluster count to 1 or more for Pro Warehouses

**For CPU contention (caused by concurrent execution):**
- Review concurrent query scheduling
- Distribute execution times of heavy queries
- Adjust Warehouse scale-out thresholds

**For file layout issues:**
- **Run OPTIMIZE**: When there are many small files, tasks become fragmented and locality misses increase
- **Apply Liquid Clustering**: Optimizes file layout and improves locality efficiency

**Notes:**
- "Reducing Executor count" or "shrinking cluster size" is not effective when cold node placement or CPU contention is the cause
- Determine remediation after confirming the correlation between per-node cache hit rates and non-local rates
- In environments with high concurrency or auto-scaling, it is recommended to collect profiles during both single-execution and concurrent-execution scenarios for comparison

---

## 7. Cloud Storage Limit Expansion
<!-- section_id: cloud_storage -->

### 7.1 Investigating Cloud Resource (Storage I/O) Bottlenecks

If expected performance is not achieved after implementing I/O optimization and join plan improvements, check the profiler metrics in verbose mode to determine whether cloud storage access is the bottleneck.

**Metrics to check:**

| Metric | Description |
|--------|-------------|
| Cloud storage request count | Storage request count |
| Cloud storage request duration | Request duration |
| Cloud storage retry count | Retry count |
| Cloud storage retry duration | Retry duration |

**Assessment:**
- If retries show very high values, cloud storage access is likely the bottleneck
- Ideally retries should be 0

---

## 7A. Compilation / File-Pruning Overhead
<!-- section_id: compilation_overhead -->

### Detection Criteria

The phase the Databricks Query Profile UI labels as "Optimizing query & pruning files" aggregates driver-side work:

- SQL parse / Catalyst logical optimization
- Delta log replay (active file list construction)
- Per-file min/max data skipping
- Partition / Liquid Clustering static pruning
- Photon code generation / physical plan finalization

When this phase dominates wall-clock, the bottleneck is **metadata processing**, not data processing.

### Typical Signals

- `compilationTimeMs / totalTimeMs >= 30%`
- `prunedFilesCount` reaches tens of thousands (driver evaluated many file stats)
- `metadataTimeMs` is on the order of seconds
- `readFilesCount` is small (actual reads tiny despite heavy pruning work)

### Causes and Remedies

| Cause | Remedy |
|------|------|
| Too many small files | `OPTIMIZE <table>` to compact. Combine with ZORDER / Liquid Clustering |
| Delta log bloat | `VACUUM <table> RETAIN 168 HOURS`. Enable Auto Checkpoint |
| Excessive partitioning | Review partition columns, consider Liquid Clustering migration |
| Cold warehouse cache | Run the same query a second time and compare compilation time |
| Stale statistics | `ANALYZE TABLE <table> COMPUTE STATISTICS FOR ALL COLUMNS` |

### Recommended SQL

```sql
-- Compact small files
OPTIMIZE catalog.schema.table;

-- Shorten Delta log (verify retention policy)
VACUUM catalog.schema.table RETAIN 168 HOURS;

-- Enable auto-optimization
ALTER TABLE catalog.schema.table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
```

Predictive Optimization on Unity Catalog managed tables automates most of the above.

---

## 7B. Driver-side Wait (Queue / Scheduling / Waiting for Compute)
<!-- section_id: driver_overhead -->

### What this captures

The Databricks Query Profile UI shows three bars — "Scheduling", "Waiting for compute", and "Waiting in queue" — whose sum is the **pre-execution driver-side wait time**. This is time spent waiting for compute resources, not processing data.

| UI bar | Source | Typical cause |
|---|---|---|
| Waiting in queue | `queuedProvisioningTimeMs` + `queuedOverloadTimeMs` | Serverless cold start / over-subscription |
| Waiting for compute | Time until warehouse becomes task-dispatch ready | Warehouse warmup, scaling up |
| Scheduling | Task scheduling overhead | Driver contention from concurrent queries |

When explicit fields are absent, derive via timestamps:

```
queue_ms   = queuedProvisioningTimeMs + queuedOverloadTimeMs
pre_compile_gap = queryCompilationStartTimestamp
                - (overloadingQueueStartTimestamp
                   or provisioningQueueStartTimestamp
                   or queryStartTimeMs)
sched_compute_ms = max(0, pre_compile_gap - queue_ms)
driver_overhead_ms = queue_ms + sched_compute_ms
```

### Detection Thresholds

- Queue alone: ≥ 5s or ≥ 10% of total
- Scheduling + compute wait: ≥ 3s or ≥ 15% of total
- Combined: ≥ 5s AND ≥ 10% of total

Severity HIGH: combined ≥ 30s or ≥ 30%

### Remedy by Dominant Component

| Dominant | Typical remedy |
|---|---|
| Provisioning queue | Enable Serverless warm pools; extend auto-stop idle timeout |
| Overload queue | Raise warehouse max clusters; stagger concurrent query launches |
| Scheduling | Spread concurrent queries across warehouses; avoid peak windows |
| Waiting for compute | Scale up warehouse, or use always-on / warm pool |

Not addressable via SQL rewrites — treat as a **warehouse configuration / concurrency-management** operational issue.

---

## 7C. Cluster Underutilization
<!-- section_id: cluster_underutilization -->

### What this captures

`effective_parallelism = task_total_time_ms / execution_time_ms` is the average number of CPUs actively doing useful work during the query. On a Medium warehouse (32-64 cores), values below 20x indicate clear under-utilization.

### Detection Thresholds

- `execution_time_ms >= 60_000` (short queries excluded)
- `effective_parallelism < 20`
- Queue wait is zero (queue waits belong to the `driver_overhead` section)

### Three Variants

| Variant | Signal | Remedy direction |
|---|---|---|
| **external_contention** | `rescheduled_scan_ratio >= 10%` | Isolate workload, stagger concurrency, raise max clusters |
| **driver_overhead** | `aqe_replan_count >= 5` OR `subquery_count >= 3` OR (`broadcast_hash_join_count >= 5` AND exec >= 120s) | Simplify SQL structure, reduce broadcasts, upgrade driver |
| **serial_plan** | None of the above | Use REPARTITION hints, pre-aggregate, revise join strategy |

### Decision Flow

```
1. rescheduled_scan >= 10% ?
   YES → external_contention (another query stole CPU)
   NO  → next
2. AQE replans >= 5 OR subqueries >= 3 OR many BHJs ?
   YES → driver_overhead (driver saturated)
   NO  → next
3. → serial_plan (plan is inherently narrow)
```

### Guidance

- **external_contention** can't be fixed by rewriting SQL — it's a workload-management problem.
- **driver_overhead** often yields big wins from SQL restructuring — especially flattening multi-step subqueries and folding multi-referenced CTEs.
- **serial_plan** typically responds well to REPARTITION hints (`/*+ REPARTITION(32, col) */`).

---

## 7D. Compilation Absolute-Heavy (Advisory)
<!-- section_id: compilation_absolute -->

### What this captures

`compilation_time_ms >= 5s` but the **ratio is small** (e.g. a 5-minute query with 10s compile = 3%). The 7A `compilation_overhead` card gates on the 30% ratio, so this variant exists to catch absolute-value anomalies as INFO-level advisories.

### Detection Thresholds

- `compilation_time_ms >= 5000` (absolute)
- `pruned_files_count >= 1000` OR `metadata_time_ms >= 500` (evidence required)
- The `compilation_overhead` card did not fire (dedup)

Severity: **INFO only**. Not a call to action, but a signal to consider OPTIMIZE/VACUUM maintenance scheduling at the workload level.

### Remedy

Same as 7A (OPTIMIZE / VACUUM / Predictive Optimization) — just at lower action priority. When multiple queries on the same table keep surfacing this advisory, it's a signal to add that table to a maintenance cadence.

---

## 8. Cluster Size Adjustment
<!-- section_id: cluster -->

**Note:** Performance can also degrade when the cluster size is too large. If queries are slow despite good cache hit rates, refer to [6.3 Checking Scan Locality](#63-checking-scan-locality) and check the reschedule rate. Scaling down may be effective in addition to scaling up.

### 8.1 Scale Out

Suitable when there are many concurrent queries and significant queue waits.

**Recommended cases:**
- No resource-related issues (CPU/memory/disk spill) when running a single SQL query
- Performance degradation occurs as the number of concurrent queries increases

**Notes:**
- The number of SQL queries each DBSQL cluster can execute concurrently is 10 (fixed, cannot be changed)
- Determine the maximum cluster count in auto-scale settings based on the number of queued queries

### 8.2 Scale Up

Suitable when performance degradation is caused by resource-intensive queries.

**Recommended cases:**
- Resource-related issues (CPU/memory/disk spill) occur when running a single SQL query
- Delta cache hit rate is low

**Effect:**
- Scaling up can reduce the number of clusters comprising the SQL endpoint, enabling more effective use of Delta cache

---

## 9. Bottleneck Metric Summary
<!-- section_id: bottleneck_summary -->

### 9.1 Key Bottleneck Metrics

| Metric | Threshold | Description |
|--------|-----------|-------------|
| Cache hit rate | <30% | Low cache efficiency |
| Remote read ratio | >80% | High remote reads |
| Photon efficiency | <50% | Low Photon efficiency |
| Spill occurrence | >0 | Memory spill occurring |
| Filter efficiency | <20% | Low filter efficiency |
| Shuffle impact ratio | ≥40% | Critical shuffle bottleneck |
| Shuffle impact ratio | 20-40% | Moderate shuffle bottleneck |
| Reschedule rate | >5% | Scan locality degradation (cold node placement/CPU contention/file placement issues) |

### 9.2 About Time Calculations

The profiler metrics include multiple time indicators:

| Metric | Description | Usage |
|--------|-------------|-------|
| total_time_ms | Total query execution time | Basic execution time |
| execution_time_ms | Execution time | Excluding compilation time |
| task_total_time_ms | Cumulative execution time of all tasks | Evaluating parallel execution |
| compilation_time_ms | Compilation time | Query optimization time |

**Important:** Simple summation of times from nodes executed in parallel may exceed 100%. Use `task_total_time_ms` as the baseline for bottleneck analysis.

---

## 10. Recommended Spark Parameters Summary
<!-- section_id: spark_params -->

### 10.1 Classic / Pro SQL Warehouse

```sql
-- Join optimization
SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB
SET spark.sql.join.preferSortMergeJoin = false;
SET spark.databricks.adaptive.joinFallback = true;

-- AQE settings
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;
SET spark.sql.adaptive.coalescePartitions.maxBatchSize = 100;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 536870912;  -- 512MB
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 536870912;  -- 512MB
```

### 10.2 Serverless SQL Warehouse
<!-- section_id: serverless_optimization -->

Serverless SQL Warehouses only support the following 6 Spark configs:

| Config | Default | Description |
|--------|---------|-------------|
| `spark.sql.shuffle.partitions` | 200 | Number of shuffle partitions |
| `spark.sql.ansi.enabled` | false | ANSI SQL mode |
| `spark.sql.session.timeZone` | UTC | Session timezone |
| `spark.sql.legacy.timeParserPolicy` | EXCEPTION | Time parser behavior |
| `spark.sql.files.maxPartitionBytes` | 128MB | Max partition bytes for file scan |
| `spark.databricks.execution.timeout` | 0 | Query execution timeout |

**For all other optimizations, use query rewrites instead of config changes.**

| Classic/Pro Config | Serverless Alternative (Query Rewrite) |
|-------------------|----------------------------------------|
| `autoBroadcastJoinThreshold` | `/*+ BROADCAST(table) */` hint, or pre-aggregate in CTE to make table small enough |
| `preferSortMergeJoin = false` | `/*+ SHUFFLE_HASH(table) */` hint |
| `adaptive.joinFallback` | `/*+ SHUFFLE_HASH(table) */` hint |
| `adaptive.skewJoin.enabled` | CTE pre-aggregation to reduce data volume before JOIN, or `/*+ REPARTITION(N) */` hint |
| `adaptive.coalescePartitions.*` | `/*+ COALESCE(N) */` hint |
| `adaptive.advisoryPartitionSizeInBytes` | `/*+ REPARTITION(N) */` hint |

See the **Query Rewrite Patterns** section in the Appendix for concrete before/after examples.

---

## Appendix: Query Optimization Hints
<!-- section_id: appendix -->

### JOIN Hints

```sql
-- Force broadcast join
SELECT /*+ BROADCAST(small_table) */ ...

-- Force shuffle join
SELECT /*+ SHUFFLE_HASH(table) */ ...

-- Force merge join
SELECT /*+ MERGE(table) */ ...
```

### Data Distribution Hints

```sql
-- Specify partition count
SELECT /*+ REPARTITION(200) */ ...

-- Repartition by columns
SELECT /*+ REPARTITION(200, col1, col2) */ ...

-- Range partitioning (for Window functions)
SELECT /*+ REPARTITION_BY_RANGE(col1) */ ...

-- Partition coalescing
SELECT /*+ COALESCE(10) */ ...
```

### Query Rewrite Patterns for Serverless Optimization
<!-- section_id: query_rewrite_patterns -->

#### Pattern 1: Pre-aggregate in CTE before JOIN

Reduces shuffle data volume by aggregating before joining.

```sql
-- Before: Large table joined directly
SELECT o.*, c.name
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- After: Pre-aggregate to reduce JOIN data volume
WITH order_summary AS (
  SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total
  FROM orders
  GROUP BY customer_id
)
SELECT /*+ BROADCAST(order_summary) */ os.*, c.name
FROM order_summary os JOIN customers c ON os.customer_id = c.id;
```

#### Pattern 2: Pre-filter in CTE

Apply WHERE filters early to minimize shuffle.

```sql
-- Before: Filter applied after JOIN
SELECT o.*, p.name FROM orders o
JOIN products p ON o.product_id = p.id
WHERE o.order_date >= '2024-01-01';

-- After: Filter in CTE before JOIN
WITH recent_orders AS (
  SELECT * FROM orders WHERE order_date >= '2024-01-01'
)
SELECT ro.*, p.name
FROM recent_orders ro JOIN products p ON ro.product_id = p.id;
```

#### Pattern 3: CTE pre-aggregation for data skew

Reduces data volume before JOIN to mitigate skew impact. AQE/AOS handles skew automatically, so focus on reducing data volume.

```sql
-- Before: Skewed join on popular_key
SELECT * FROM fact_table f
JOIN dim_table d ON f.popular_key = d.key;

-- After: Pre-aggregate in CTE to reduce data volume
WITH pre_agg AS (
  SELECT popular_key, COUNT(*) AS cnt, SUM(amount) AS total
  FROM fact_table
  GROUP BY popular_key
)
SELECT pre_agg.*, d.*
FROM pre_agg JOIN dim_table d ON pre_agg.popular_key = d.key;
```

#### Pattern 4: EXISTS / IN conversion

Replace correlated subqueries with semi-joins.

```sql
-- Before: Correlated subquery
SELECT * FROM orders o
WHERE o.customer_id IN (SELECT id FROM customers WHERE region = 'US');

-- After: EXISTS for correlated pattern
SELECT * FROM orders o
WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.region = 'US');
```

#### Pattern 5: UNION ALL instead of UNION

Avoid unnecessary sort/distinct when duplicates are acceptable.

```sql
-- Before: UNION forces sort + distinct
SELECT id, name FROM table_a
UNION
SELECT id, name FROM table_b;

-- After: UNION ALL skips sort if duplicates are acceptable
SELECT id, name FROM table_a
UNION ALL
SELECT id, name FROM table_b;
```

#### Pattern 6: Column pruning

Select only needed columns to reduce scan and memory.

```sql
-- Before: SELECT * reads all columns
SELECT * FROM large_table WHERE partition_col = 'value';

-- After: Select only required columns
SELECT col1, col2, col3 FROM large_table WHERE partition_col = 'value';
```

#### Pattern 7: Photon-compatible rewrite

Rewrite Photon-incompatible functions.

```sql
-- Before: Window RANGE frame (may not use Photon)
SUM(amount) OVER (ORDER BY event_date RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW)

-- After: ROWS frame with date filter (Photon-compatible)
SUM(amount) OVER (ORDER BY event_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)

-- Before: Python UDF
SELECT my_udf(col) FROM table;

-- After: Built-in function
SELECT UPPER(col) FROM table;  -- Use built-in equivalent
```

---

## 19. Lakehouse Federation query tuning
<!-- section_id: federation -->

When a query uses Lakehouse Federation to read from an external source (BigQuery / Snowflake / Postgres / MySQL / Redshift, …) via Unity Catalog, the execution cost is dominated by **the remote engine** and **network transfer**. Databricks-side storage optimizations (Liquid Clustering, disk cache, Photon compatibility) do not apply. Work through the following in order instead.

### 19.1 Check the pushdown status (mandatory)

```sql
EXPLAIN FORMATTED
SELECT ... FROM <federated_catalog>.<schema>.<table>
WHERE ...;
```

- `EXTERNAL ENGINE QUERY` block: the exact SQL sent to the remote engine
- `PushedFilters`: predicates evaluated remotely
- `PushedJoins`: whether joins pushed down (DBR 17.3+)
- If predicates aren't pushed, the cause is usually an unsupported function (e.g., `ILIKE` on MySQL), a function on the left-hand side, or ANSI-mode interaction.

BigQuery reads via the Storage API, so `EXTERNAL ENGINE QUERY` is absent by design — that's normal. However, **join pushdown requires materialization mode**.

### 19.2 Rewrite predicates so pushdown goes through

```sql
-- Bad: function on the partition column defeats BigQuery partition pruning
WHERE DATE(created_at) = '2026-04-20'

-- Good: range condition lets pruning kick in
WHERE created_at >= TIMESTAMP('2026-04-20')
  AND created_at <  TIMESTAMP('2026-04-21')
```

- Functions on the LHS (`DATE()`, `CAST()`, `UPPER()`, `SUBSTRING()`, …) are the classic pushdown blocker
- Non-sargable `LIKE '%...%'` cannot be pushed either

### 19.3 JDBC connectors: fetchSize and parallel reads

For large result sets:

```sql
SELECT * FROM cat.schema.tbl WITH ('fetchSize' 100000) WHERE ...;
```

- Default is "fetch all at once", which OOMs easily
- Applies to MySQL / Postgres / SQL Server / Oracle / Redshift / Synapse / Teradata

Parallel reads (requires a numeric, indexed column):

```sql
SELECT * FROM cat.schema.tbl
  WITH ('numPartitions' 8,
        'partitionColumn' 'id',
        'lowerBound' '1',
        'upperBound' '10000000')
WHERE ...;
```

- Does not work on federated views — create the view on the source side instead.

### 19.4 Snowflake: partition_size_in_mb

```sql
SELECT * FROM cat.schema.tbl WITH ('partition_size_in_mb' 1000) WHERE ...;
```

- The default produces too many small partitions on large tables
- 500 MB – 2 GB is typical depending on volume

### 19.5 BigQuery: billing and materialization

- **Billing**: federation pays both for DBSQL compute and BigQuery on-demand (per TB scanned) — double-billed
- Partition pruning is mandatory. If the BQ table is partitioned by `_PARTITIONDATE` or `DATE(ts)`, apply the rewrite in 19.2
- Join pushdown is only active in "materialization" mode — enable for large joins, skip for small point queries (overhead > benefit)

### 19.6 OLTP sources (MySQL / Postgres) specifics

- Predicates that miss the index trigger full scans, which pressures the primary DB
- Point the federation connection at a read replica
- Watch connection pool limits — federation opens short-lived connections frequently

### 19.7 LIMIT pushdown is suppressed under UC governance

Tables with column masks or row-level filters have **LIMIT pushdown disabled for correctness**. If users report "LIMIT 10 is still slow", this is often the reason.

### 19.8 Should we federate every time?

For frequently-run dashboards / jobs:

```sql
-- Materialize into Delta once, query Delta many times
CREATE OR REPLACE TABLE main.analytics.sales_daily AS
SELECT ... FROM pococha_bq_prod.source.db_reincarnation_device_histories
WHERE created_at >= TIMESTAMP(CURRENT_DATE() - INTERVAL 30 DAYS);
```

- Dashboards point at the Delta table
- Refresh nightly via a job
- Use federation for ad-hoc / exploratory work; move steady-state queries to Delta.

### 19.9 Alert priority for federation queries

For federation queries, alerts related to Liquid Clustering / disk cache / file pruning / stats freshness / Photon blockers are suppressed (v5.18.0) because they don't apply. Focus on:

1. Federation Query card (this section)
2. Driver overhead (connection setup / scheduling wait)
3. Compilation overhead (remote metadata fetch / planning)
4. shuffle / spill / hash resize (when a DBSQL-side JOIN is heavy)

---

## References
<!-- section_id: references -->

- [Databricks SQL Join Hints](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-hints#join-hint-types)
- [Z-Ordering Ineffective Column Stats](https://kb.databricks.com/delta/zordering-ineffective-column-stats)
