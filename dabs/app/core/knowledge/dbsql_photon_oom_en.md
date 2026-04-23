# Photon OOM Diagnosis and Mitigation

## Photon OOM Troubleshooting
<!-- section_id: photon_oom -->

Systematic diagnosis flow and mitigation for Photon Out of Memory (OOM) errors.

---

### Step 1: Is Photon the Largest Memory Consumer?

Compare `task: allocated [...] MiB` with `Total task memory` in the OOM error.

- **Photon is largest** -> Step 2
- **ShuffleExternalSorter is largest** -> Upgrade to DBR 16.3+ (known fix)
- **BytesToBytesMap is largest** -> Spark Hash Aggregation starving Photon. Rewrite query to run more in Photon
- **UnsafeExternalSorter is largest** -> File ES ticket

---

### Step 2: Identify the Failed Photon Operator

In `Photon failed to reserve [...] MiB for [...], in [...]`, the second-to-last tracker in the list is usually the most important.

#### FileWriterNode OOM

**Cause:** Wide schema (>1000 columns) or highly compressible data

**Mitigation:**
```sql
-- Reduce page.size (rule of thumb: 256MB / num_columns)
SET parquet.page.size = <value>;
```

#### AggNode / GroupingAggNode OOM

**Cause:** `collect_list`, `collect_set`, `percentile` collect all data in memory

**ES Ticket Pattern:** High-cardinality `GROUP BY`, unnecessary `DISTINCT`, and bloated intermediate stages in multi-level aggregates are the main drivers. First reduce row counts before aggregation and revisit grain.

**Mitigation:**
- Use `approx_percentile` instead of `percentile`
- Avoid non-grouping aggregation or skewed group sizes with `collect_list/set`
- Use instances with higher memory-per-core ratio

#### BroadcastHashedRelation OOM

**Cause:** Broadcast table too large to fit in memory

**ES Ticket Pattern:** Beyond misuse of broadcast hints, **stale statistics or upstream row amplification** often inflate the build side. Prefer pre-aggregation on one side, filter pushdown, and refreshing stats — not only removing the hint.

**Mitigation:**
1. AQE BHJ OOM fallback is enabled by default on DBR 13.3+ (auto-converts to shuffle join)
2. For Null Aware Anti Join (NAAJ):
```sql
-- Rewrite NOT IN as NOT EXISTS
-- Before
SELECT * FROM t WHERE val NOT IN (SELECT val FROM sub);
-- After
SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM sub WHERE sub.val = t.val);
```
3. Run `ANALYZE TABLE` to update statistics
4. Apply shuffle join hint:
```sql
SELECT /*+ SHUFFLE_HASH(t2) */ * FROM t1 JOIN t2 ON ...;
```

#### BroadcastBufferedRelation OOM (Broadcast Nested Loop Join)

**Cause:** Missing join conditions or large cross joins

**Mitigation:**
- Verify join conditions are complete (could become hash join)
- For range joins: use `/*+ RANGE_JOIN(t, 10) */` hint
- Specify broadcast side: `/*+ BROADCAST(smaller_table) */`

#### FileReader OOM

**Cause:** Large JSON strings or nested arrays causing huge Parquet pages

**Mitigation:**
- Reduce selected columns (avoid `SELECT *`)
- For JSON scan OOM: `SET spark.databricks.photon.jsonScan.enabled = false`

#### ShuffleExchangeSinkNode OOM

**Cause:** Bloom filter memory overuse or large row data

**Mitigation (Bloom filters):**
```sql
SET spark.databricks.photon.outputHashCodeForBloomFilter.enabled = false;
```

**Mitigation (row data):**
- Upgrade to DBR 15.4+ (auto batch sizing enabled)

---

### Step 3: Check for Spill Pinning

If `output batch var len data` or `spilled var-len chunks` in the OOM error is >= 1 GiB, spill pinning is the issue.

**Mitigation:**
- Upgrade to DBR 16.3+ (retry-based spill feature)
- Reduce batch size:
```sql
SET spark.databricks.photon.autoBatchSize.targetSize = 16777216;  -- 16MB
```

---

### Generic Mitigations (Last Resort)

1. Select worker instances with higher memory-per-core ratio
2. Reduce `spark.executor.cores` (e.g., set to 2 on 4-core instance -> doubles memory per task)
3. Disable Photon: `SET spark.databricks.photon.enabled = false` (last resort)
