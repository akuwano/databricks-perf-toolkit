# Databricks Spark Performance Optimization Knowledge Base

<!-- section_id: spark_overview -->
## Optimization Principles

### Optimize Only What's Necessary
- Define goals before starting (cost targets, SLA/performance targets)
- Start with high-impact, simple changes (80/20 rule)
- Benchmark and iterate (record results to track progress)
- Know when to stop tuning

### Understand the Processing Layers
Optimization targets span 3 layers:
- **Query layer**: SQL/DataFrame logic
- **Processing engine layer**: Spark/Photon, optimizer
- **Infrastructure layer**: VM/Serverless, instance selection, scaling, disk cache

### 7 Key Optimization Points
1. Define what you are optimizing for
2. Start with easy wins (Platform → Data → Query)
3. Leverage latest compute and features (Serverless, Photon, Predictive Optimization, Liquid Clustering)
4. Scale vertically or horizontally based on workload
5. Use streaming for incremental processing where possible
6. Use monitoring tools to measure optimization effectiveness
7. Determine when to stop long-running optimization efforts

---

<!-- section_id: spark_compute -->
## Compute Optimization

### Photon Engine
**Workloads that benefit most:**
- Heavy computation on large data (joins, aggregations)
- Delta Lake Merge operations
- Wide table reads/writes
- DLT and Auto Loader

**When Photon is not used:**
- Unsupported APIs (e.g., collect_set() → use collect_list(distinct))
- UDF / RDD / Typed Dataset → avoid where possible
- Check Photon Explanation in Spark UI Details

### Adaptive Query Execution (AQE)
- Automatically adapts query plans at runtime based on accurate metrics
- Features:
  - Dynamic switch from SortMergeJoin (SMJ) to BroadcastHashJoin (BHJ)
  - Shuffle partition optimization
  - Data skew handling

### SQL Warehouse Sizing
- **Vertical scaling (cluster size)**: Improve speed for large queries, resolve disk spill
- **Horizontal scaling (cluster count)**: Handle concurrent queries
- Too many queued queries → increase number of clusters
- Queries taking too long → increase cluster size

### Classic Compute Configuration
- **CPU:RAM ratio**: Allocate sufficient memory per core
- **Processor type**: ARM-based chips offer excellent performance
- **Local storage**: Disk cache effective for repeated access
- **Spot availability**: Stability more important for long-running jobs
- **Auto-scaling**: Achieve high cluster utilization, reduce costs

### Executor Configuration
- **spark.executor.cores**: Start with 4-5
- **Memory formula**: (Node memory / Executor count) × (1 - 0.1875)
- **Driver**: Default 2GB sufficient for most cases

---

<!-- section_id: spark_data_layout -->
## Data Layout Optimization

### Data Layout Principles
Organize data to avoid reading unnecessary files:
- **Partition pruning**: Exclude file groups by partition key
- **File skipping**: Skip files based on statistics (min/max)

### Liquid Clustering (Recommended)
Replaces Z-Order and traditional partitioning:
- **Fast**: Automatic clustering during writes
- **Self-tuning**: AI-based automatic clustering key selection
- **Skew-resistant**: OPTIMIZE maintains consistent file sizes
- **Flexible**: Change clustering columns without affecting existing data

### Predictive Optimization
- No manual scheduling of optimization needed
- Auto-executes: Vacuum, Optimize, Analyze, Liquid Clustering key selection

### File Size
- **Small files**: Better filter efficiency, faster updates, more files
- **Large files**: Less filter efficiency, slower updates, fewer files
- **Target file size**: 128MB to 1GB

---

<!-- section_id: spark_code -->
## Code Optimization

### Basic Principles
- Minimize count(), display(), collect() in production jobs
- Avoid single-threaded Python/Pandas/Scala → use Pandas API on Spark
- Avoid Python UDFs → use PySpark native functions or Pandas UDF (vectorized)
- Use DataFrame over RDD (RDD cannot leverage CBO/Photon)

### UDF Types and Performance
- **Python UDF (@F.udf)**: Row-level IPC, System CPU spike, no Photon (slowest)
- **Pandas UDF (SCALAR)**: Batch transfer, ArrowEvalPython, no Photon
- **Pandas UDF (GROUPED_MAP)**: Group-level, FlatMapGroupsInPandas, no Photon
- **mapInArrow**: Partition-level, no Arrow conversion (fastest UDF alternative)
- **SparkSQL / PySpark native functions**: JVM-native, Photon-compatible (recommended)

### Broadcast Join
- Most performant join type
- Controlled by spark.sql.autoBroadcastJoinThreshold (default 30MB)
- Prerequisite for Dynamic File Pruning (DFP)

---

<!-- section_id: spark_data_skew -->
## Bottleneck: Data Skew (DATA_SKEW)

**Symptom:** Imbalanced data sizes across partitions. High task_skew_ratio.

### JOIN Skew Remediation (AQE skewJoin)
```
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256m
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5  -- lower to 3 if skew not detected
spark.sql.adaptive.advisoryPartitionSizeInBytes=64m
```
- **Important**: skewJoin only applies to SortMergeJoin. Does NOT work for ShuffledHashJoin, BroadcastHashJoin, or GroupBy/Aggregate
- Skew hint: `SELECT /*+ SKEW('table', 'col') */`

### GroupBy/Aggregate Skew Remediation
- Salting: Add random value to key, distribute across partitions, then re-aggregate
- Two-stage aggregation: First aggregate by partial key, then by final key
- Increase spark.sql.shuffle.partitions to reduce data per partition

### Common Remediation
- Enable AQE: spark.sql.adaptive.enabled=true
- spark.sql.adaptive.coalescePartitions.enabled=true
- Explicit repartition() for even data distribution
- Liquid Clustering to reduce data distribution skew

---

<!-- section_id: spark_disk_spill -->
## Bottleneck: Disk Spill (DISK_SPILL)

**Symptom:** Temporary files written to disk due to insufficient memory.

### Recommended Actions
```
spark.executor.memory=<increase>  -- Formula: (node memory / executor count) × (1 - 0.1875)
spark.memory.fraction=0.6  -- Default. Increase if needed
spark.sql.shuffle.partitions=auto  -- Try auto first. Manual: input data MB / 128
spark.sql.files.maxPartitionBytes=<adjust>
spark.executor.cores=4  -- Ensure sufficient memory per core
```
- If skew is the root cause, address skew first
- For SQL Warehouses, scale up cluster size (more RAM)

### GC Tuning (when GC overhead is high)
```
-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35
```
- Target: GC time ≤ 1% of task execution time
- Warning threshold: GC ≥ 25% indicates serious performance issue

---

<!-- section_id: spark_heavy_shuffle -->
## Bottleneck: Heavy Shuffle (HEAVY_SHUFFLE)

**Symptom:** Large volumes of data moved between worker nodes.

### Recommended Actions
```
spark.sql.autoBroadcastJoinThreshold=30m  -- Broadcast Join threshold
spark.sql.shuffle.partitions=auto  -- Try auto first. Manual: input data MB / 128
```
- Reduce shuffled data volume (column pruning, pre-filtering)
- Align partition count with cluster total cores (multiples)
- Use fewer, larger workers to reduce inter-node transfers
- Tip: Don't obsess over eliminating all shuffles — focus on operations more costly than shuffle

---

<!-- section_id: spark_small_files -->
## Bottleneck: Small Files (SMALL_FILES)

**Symptom:** High overhead from many small files (per-task read < 10MB).

### Recommended Actions
```sql
OPTIMIZE <table_name>;
```
```
spark.databricks.delta.optimizeWrite.enabled=true  -- Adjust write size to 128MB
spark.databricks.delta.autoCompact.enabled=auto  -- Improve file size after writes
```
- Enable Predictive Optimization for automatic OPTIMIZE / Vacuum
- Vacuum to remove old versions and clean up metadata
- Target file size: 128MB to 1GB

---

<!-- section_id: spark_serialization -->
## Bottleneck: Serialization (SERIALIZATION)

**Symptom:** Delays from data/code conversion and transfer. System CPU exceeds User CPU.

### Recommended Actions
- Minimize UDF usage → prefer PySpark native or SparkSQL functions
- Rewrite `@F.udf` (Python UDF) → `@pandas_udf` (batch transfer for better IPC efficiency)
- Where possible, rewrite to SparkSQL / PySpark native functions (Photon-compatible)

### Kryo Serializer
```
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryo.registrationRequired=true
spark.kryoserializer.buffer=1024k
spark.kryoserializer.buffer.max=1024m
```

---

<!-- section_id: spark_photon -->
## Bottleneck: Photon Fallback (PHOTON_FALLBACK)

**Symptom:** Photon engine not used, falling back to Classic Spark.

### Photon-Unsupported Operations

**Command operations (DDL/control flow) — internal data processing may still use Photon:**
- AtomicReplaceTableAsSelect / AtomicCreateTableAsSelect
- Execute / WriteFiles / AddJarsCommand
- Photon Explanation: "Commands are not directly photonized" → check subsequent jobs

**Unsupported operators (UDF/non-native processing):**
- FlatMapGroupsInPandas (applyInPandas) → rewrite with Window functions + groupBy
- ArrowEvalPython (Pandas UDF) → rewrite with Spark native functions
- BatchEvalPython (Python UDF) → rewrite with SparkSQL / PySpark native functions
- SortMergeJoin → switch to Broadcast Hash Join (adjust autoBroadcastJoinThreshold)
- ColumnarToRow → identify and rewrite upstream unsupported operator

---

<!-- section_id: spark_spot_loss -->
## Bottleneck: Spot Instance Loss (SPOT_LOSS)

**Symptom:** Executor loss due to Spot instance preemption. Shuffle data loss and recomputation.

### Recommended Settings
```
spark.decommission.enabled=true
spark.storage.decommission.enabled=true
spark.storage.decommission.shuffleBlocks.enabled=true
spark.storage.decommission.rddBlocks.enabled=true
spark.decommission.graceful.timeout=120s
spark.speculation=true
```
- Cluster configuration: Driver on On-Demand, Workers on Spot + On-Demand fallback
- AWS: capacity-optimized allocation strategy, multiple instance types for risk distribution

### Caching Strategy (Spot Environments)
- cache() / persist() / localCheckpoint() → local storage, lost on Spot preemption
- checkpoint() / Delta table write → remote storage, Spot-resilient
- Use checkpoint() or Delta table write for intermediate results in long-running jobs

---

<!-- section_id: spark_shuffle_params -->
## Shuffle Partition Tuning

### Default Value Issues
- Default spark.sql.shuffle.partitions=200 is inadequate for >20GB data
- Oversized partitions → OOM, undersized → task overhead

### Optimal Partition Size Calculation
- **Target partition size**: 100-200MB (128MB recommended)
- **Formula**: Partition count = Stage input data (MB) / Target size (MB)
- **Example**: 210GB shuffle data → 210,000MB / 128MB = 1,640 partitions

### Core Count Alignment
- Ensure partition count ≥ total cluster cores
- Round partition count to multiple of core count

### AQE Automatic Optimization (Try First)
**Always try `auto` first.** Manual tuning should only be considered if `auto` doesn't help.
```
spark.sql.shuffle.partitions=auto  -- Try this first (AQE auto-optimizes)
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```
- Setting `auto` lets AQE automatically adjust shuffle partition count at runtime
- When specifying a manual number, it acts as an upper limit — set it generously
- Only consider manual calculation (input data MB ÷ 128) if `auto` proves insufficient

---

<!-- section_id: spark_diagnostics -->
## Performance Diagnostics

### Key Items to Check in Query Profile
- Confirm Photon utilization is close to 100%
- Check disk cache hit ratio
- Verify row counts are reasonable
- Spill indicates insufficient warehouse RAM
- Check if file/partition counts are too high

### Cache Strategy Selection
**Recommended for Photon environments:**
1. persist(DISK_ONLY) — Photon-compatible reads + Lineage preservation
2. Delta table write — Photon-compatible reads + Lineage cutoff + Spot resilience
3. cache() — Not Photon-compatible for reads but convenient. Effective for 3+ accesses
4. Parquet IO Cache — Auto-managed by Databricks. Faster than explicit cache() in some cases

### SQL Warehouse Diagnostics
- Compute wait → use Serverless / start more clusters
- Queue wait → increase max cluster count
- Long optimization/pruning time → statistics & file optimization needed
