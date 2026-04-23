# Serverless SQL Warehouse Tuning

## Serverless-Specific Tuning
<!-- section_id: serverless -->

Serverless SQL Warehouses have distinct performance characteristics and optimization points compared to Classic/Pro.

---

### 1. Cold Start and Warm Pools

First queries on a Serverless WH incur **6-13 seconds** of compute spin-up overhead.

**Mitigation:**
- **Warm pool**: Set appropriate `Auto Stop` time (5-10 min) for dashboards with regular access
- **Pre-warming**: Send lightweight queries (`SELECT 1`) before critical dashboard views
- If the first batch of queries is slower than subsequent ones (e.g., 13s vs 2s), cold start is the cause

---

### 2. AOS (Auto Optimized Shuffle)

Automatic shuffle partition optimization is available on Serverless.

**ES Ticket Pattern:** AOS improves shuffle partitioning but does **not** fix root causes such as data skew, data explosion, or oversized broadcasts. On Serverless, SQL and data design still come first.

#### AOS v1 (DBR 11.3+)
- `spark.sql.shuffle.partitions = auto` is set internally by Serverless (users cannot SET this parameter)
- Estimates partition count based on catalog stats or file size
- May produce incorrect estimates (especially for highly compressed tables)
- Target size tuning:
```sql
-- Default 64MB. Reduce if partitions are too large
SET spark.databricks.adaptive.autoOptimizeShuffle.preshufflePartitionSizeInBytes = 16777216;  -- 16MB
```

#### AOS v2 (DBR 16.4+, rolled out on DBSQL PREVIEW channel)
- Does not rely on stats estimation; uses **sampling and extrapolation**
- Target partition size: **256MB-1GB** (uncompressed)
- Initial partition count: `2 * vCPUs`
- Automatically re-shuffles during execution if needed

#### Spill Fallback (DBR 17.1+)
- Add-on feature for AOS v2
- Detects spilling and long-running stages, automatically retries with higher parallelism

---

### 3. Concurrent Query Queuing

Serverless WHs have concurrency limits (~10 queries); excess queries are queued.

**Diagnosis:**
- Long `Scheduling Time` in query profile indicates queuing
- Typical pattern: first batch of concurrent dashboard queries is slow

**Mitigation:**
- Scale up WH size (increases concurrency slots)
- Optimize queries (shorter execution -> shorter slot occupation)
- Eliminate unnecessary dashboard queries

---

### 4. Partition Size Guidelines for Serverless

| Use Case | Recommended Size (uncompressed) | Notes |
|----------|--------------------------------|-------|
| Shuffle partitions | 256MB-1GB | AOS v2 default target |
| Highly compressed tables | 16MB-64MB (tune via `preshufflePartitionSizeInBytes`) | When using AOS v1 |
| Downstream of exploding stage | Smaller partitions | Let Spill Fallback handle or tune manually |

---

### 5. Statistics on Serverless

`ANALYZE TABLE` remains effective on Serverless. AQE uses statistics for better execution plans.

**ES Ticket Pattern:** Statistics freshness matters on Serverless too. **Stale stats** degrade AQE, broadcast, and Data Skipping decisions. Before blaming the execution engine, check statistics state.

```sql
ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;
```

- **Predictive Optimization** auto-updates statistics when enabled
- Manual execution recommended immediately after table recreation/bulk updates

---

### 6. Serverless vs Classic/Pro Key Differences

| Aspect | Serverless | Classic/Pro |
|--------|-----------|-------------|
| Default shuffle partitions | `2 * vCPUs` | 200 |
| AOS | v2 rolled out (PREVIEW) | Not available |
| Photon | Enabled by default | Depends on WH type |
| Cold start | Yes (6-13s) | Depends on WH size |
| Scaling | Seconds | Minutes |
| Spark config changes | Limited | Flexible |
