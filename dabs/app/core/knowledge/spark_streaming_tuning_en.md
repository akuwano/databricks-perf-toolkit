# Structured Streaming Performance Optimization Knowledge Base

<!-- section_id: spark_streaming -->
## Structured Streaming Optimization

### Trigger Modes
- **Default (micro-batch)**: Starts next batch immediately after previous completes. Use when low latency is required
- **processingTime**: Fixed interval execution (e.g., `trigger(processingTime='10 seconds')`). Use to control resource consumption
- **availableNow**: Processes all available data then stops (`trigger(availableNow=True)`). Cost-effective for batch-like streaming workloads
- **continuous** (experimental): Millisecond latency. Only supports map-like operations. Not recommended for production

### Throughput Optimization
- **maxFilesPerTrigger / maxBytesPerTrigger**: Control per-batch processing volume
  - Auto Loader: `cloudFiles.maxFilesPerTrigger`, `cloudFiles.maxBytesPerTrigger`
  - Kafka: `maxOffsetsPerTrigger`
- **spark.sql.shuffle.partitions**: Important for streaming too. AQE is disabled by default for streaming, so manual tuning is required
- **Batch size optimization**: Check batch duration breakdown (addBatch, queryPlanning, commit, etc.) to identify bottleneck location

### State Management
- **RocksDB State Backend**: Recommended for large state
  - `spark.sql.streaming.stateStore.providerClass=com.databricks.sql.streaming.state.RocksDBStateStoreProvider`
  - Benefits: Disk-based to avoid OOM, faster checkpointing
- **Watermark**: Use `withWatermark()` to automatically drop stale state. Set based on acceptable late data tolerance
- **State TTL**: `spark.sql.streaming.stateTTL` for global TTL setting (Databricks extension)
- **State memory monitoring**: Check `stateOperators.memoryUsedBytes` regularly. If continuously increasing, review watermark settings

### Backlog Remediation
- Use `trigger(availableNow=True)` to process backlog in one shot
- Temporarily scale up cluster size for catch-up
- Increase `maxFilesPerTrigger` for larger batch ingestion
- Monitor `numFilesOutstanding` / `numBytesOutstanding` metrics

### Commit Overhead Remediation
- RocksDB state backend has fast checkpointing (incremental checkpoints)
- Delta sink log compaction: Address log file growth from frequent commits with OPTIMIZE
- Use default `spark.sql.streaming.commitProtocolClass` (change not recommended)

### Planning Overhead Remediation
- Keep queries simple (avoid complex UDFs, multi-stage joins)
- Watch for schema evolution when using Delta CDF (Change Data Feed)
- Investigate if "Query Planning" time exceeds 30% of trigger execution time in Spark UI

### Monitoring
- **StreamingQueryListener**: Use for custom metrics collection
- **Spark UI**: Check per-batch processing time and throughput in the Structured Streaming tab
- **query.lastProgress**: Programmatic metrics retrieval
- Key metrics: `batchDuration`, `inputRowsPerSecond`, `processedRowsPerSecond`, `stateOperators.memoryUsedBytes`
