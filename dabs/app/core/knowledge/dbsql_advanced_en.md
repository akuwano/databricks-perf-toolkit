# Shuffle, Spill, Skew & Data Explosion Advanced Tuning

## Shuffle Partition Advanced Tuning
<!-- section_id: shuffle_advanced -->

### Manual Shuffle Partition Calculation

When AOS is unavailable or its estimates are inaccurate, manual tuning is needed.

**Formula:**
```
T = total worker cores
B = total shuffled data in stage (MB)
M = ceiling(B / 128 / T)  <- multiplier
N = M * T                 <- shuffle partition count
```

**Target: 128MB-200MB per task** (verify in Spark UI Shuffle Stage metrics)

```sql
-- Manual setting
SET spark.sql.shuffle.partitions = <N>;

-- Quick rule of thumb (no tuning)
SET spark.sql.shuffle.partitions = <2 * total_worker_cores>;
```

### Partition Count Bounds

| Setting | Default | Description |
|---------|---------|-------------|
| `spark.databricks.adaptive.autoOptimizeShuffle.maxPartitionNumber` | 20480 | AOS upper bound |
| AOS v2 lower bound | `2 * vCPUs` (DBSQL) | Initial partition count |

**Note:** Too many partitions can also cause memory issues (Map Status OOM). Balance is critical for large shuffles.

---

## Data Explosion Detection and Mitigation
<!-- section_id: data_explosion -->

### What is Data Explosion?

A sudden increase in data volume after specific transformations. Main causes:

1. **Explode function**: Flattening arrays/maps inflates data
2. **JOIN operations**: Producing more rows than expected (Row Explosion)

**ES Ticket Pattern:** The most common causes of Data Explosion are **many-to-many JOINs, duplicate join keys, and late filters**. When post-JOIN row counts far exceed the sum of inputs, first verify join predicates and **pre-JOIN aggregation or deduplication**. Pushing selective filters before the JOIN also helps a lot.

### Detection

- **Spark UI**: Check `rows output` in `Generate` (Explode) or `SortMergeJoin`/`ShuffleHashJoin` nodes
- If 128MB input partitions balloon to several GB, data explosion is occurring

### Mitigation

**For Explode-caused explosion:**
```sql
-- Reduce input partition size
SET spark.sql.files.maxPartitionBytes = 16777216;  -- 16MB (default 128MB)
```

**For JOIN-caused explosion:**
```sql
-- Increase shuffle partition count
SET spark.sql.shuffle.partitions = <larger_value>;
```

---

## Data Skew Detection and Mitigation
<!-- section_id: skew_advanced -->

### Detection

1. **Spark UI**: Most tasks complete but 1-2 hang for a long time -> Skew
2. **Summary Metrics**: Large difference between min/max Shuffle Read Size
3. **Direct check**:
```sql
SELECT column_name, COUNT(*) as cnt
FROM table
GROUP BY column_name
ORDER BY cnt DESC
LIMIT 20;
```

**ES Ticket Pattern:** AQE skew join mitigation helps in many cases, but extreme hot keys (e.g. more than half of rows on one value) still need SQL-side fixes. Before skew hints or key salting, prioritize **filter pushdown and shrinking one side** first.

### Mitigation (in priority order)

**1. Filter skewed values**
```sql
-- If NULL values cause skew
SELECT * FROM table WHERE join_key IS NOT NULL;
```

**2. Skew hints**
```sql
SELECT /*+ SKEW('table', 'column_name', (value1, value2)) */ *
FROM table;
```

**3. AQE Skew Optimization (enabled by default)**
```sql
-- Default: partitions >= 256MB and >= 5x average are considered skewed
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = <bytes>;
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = <value>;

-- AQE cannot detect skew when partitions exceed 2000
SET spark.shuffle.minNumPartitionsToHighlyCompress = <value_above_partition_count>;
```

**4. CTE Pre-aggregation for Skew**
Pre-aggregate skewed keys in a CTE to reduce data volume before JOIN. This avoids the complexity of manual key manipulation and works well with AQE.

---

## Broadcast JOIN Advanced Configuration
<!-- section_id: broadcast_advanced -->

### Auto-Broadcast Thresholds

```sql
-- Spark auto-broadcast threshold (default 10MB)
SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB

-- AQE adaptive broadcast threshold (default 30MB)
SET spark.databricks.adaptive.autoBroadcastJoinThreshold = 209715200;  -- 200MB
```

### Important Constraints

| Constraint | Value | Description |
|-----------|-------|-------------|
| Broadcast hard limit | 8GB | Spark hard limit |
| Recommended limit | 1GB | Driver memory constraint |
| Compression caveat | - | Parquet disk size != memory size (20-40x difference possible) |

**ES Ticket Pattern:** Broadcast is often the fastest join, but **stale statistics can mis-size the build side** and broadcast a table that is too large, causing OOM. Optimizing for speed is not enough — **validate the actual build-side size**. Sizes often inflate from outdated stats or upstream row amplification.

### Explicit Broadcast Recommended

```sql
-- Explicit hint skips shuffle before AQE can intervene
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table JOIN small_table ON ...;
```

**With Photon:** Executor-side broadcast is available, so `spark.driver.maxResultSize` adjustment is unnecessary.

---

## Delta MERGE Performance Optimization
<!-- section_id: merge_advanced -->

### MERGE Bottlenecks

MERGE internally JOINs using the ON clause condition and rewrites matched files.

**ES Ticket Pattern (42 tickets, ~24% bug-related):** MERGE performance depends not only on predicates but on **target file layout, layout quality, and statistics freshness**. Poor locality of updated rows increases unnecessary file reads. **Deletion Vectors** (`enableDeletionVectors=true`) often help dramatically. If MERGE returns wrong results, check for **known issues** — verify DBR version and ES tickets.

**Problem:** Overly broad ON conditions cause excessive file rewrites

### Optimization Techniques

**1. Target table file size tuning**
```sql
-- Smaller file sizes recommended for MERGE-heavy tables (16-64MB)
ALTER TABLE target_table SET TBLPROPERTIES (
  'delta.targetFileSize' = '33554432'  -- 32MB
);
```

**2. Include partition filters in ON clause**
```sql
MERGE INTO target t
USING source s
ON t.date = s.date  -- Partition column for pruning
  AND t.id = s.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**3. Low Shuffle Merge (default on DBR 10.4+)**
- Preserves data layout (Z-Order clustering etc.) for unmodified rows
- Only modified rows are reorganized
