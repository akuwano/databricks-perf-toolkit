# Databricks notebook source
# DBTITLE 1,Pipeline Overview
# MAGIC %md
# MAGIC # Spark Job Performance Analysis — PySpark Pipeline
# MAGIC
# MAGIC DLT パイプライン (`pipeline.py`) と同等の処理を標準 PySpark で実行します。
# MAGIC
# MAGIC **アーキテクチャ:**
# MAGIC - **Bronze (1 table):** Raw テキスト行
# MAGIC - **Silver (9 tables):** イベント種別ごとにパース・フラット化
# MAGIC - **Gold (9 tables):** 集計・分析・ボトルネック判定
# MAGIC
# MAGIC **合計 19 テーブル** を書き込みます。Bronze/Silver は `overwrite`、Gold (13テーブル) は `MERGE` (upsert) で過去の分析結果を保持します。

# COMMAND ----------

# DBTITLE 1,Configuration — Widgets
# ==============================================================================
# CONFIGURATION
# ==============================================================================
dbutils.widgets.text("log_root",      "/Volumes/main/base/data/cluster_logs/cluster02", "Log Root Path")
dbutils.widgets.text("cluster_id",    "your-cluster-id",                                "Cluster ID")
dbutils.widgets.text("catalog",       "main",                                            "Catalog")
dbutils.widgets.text("schema",        "base2",                                           "Schema")
dbutils.widgets.text("table_prefix",  "PERF_",                                          "Table Name Prefix")

_LOG_ROOT    = dbutils.widgets.get("log_root").rstrip("/")
CLUSTER_ID   = dbutils.widgets.get("cluster_id")
CATALOG      = dbutils.widgets.get("catalog")
_SCHEMA      = dbutils.widgets.get("schema")
SCHEMA       = f"{CATALOG}.{_SCHEMA}"
TABLE_PREFIX = dbutils.widgets.get("table_prefix")
EVENT_LOG_PATH = f"{_LOG_ROOT}/{CLUSTER_ID}/eventlog/"

print(f"Log Root      : {_LOG_ROOT}")
print(f"Cluster ID    : {CLUSTER_ID}")
print(f"Event Path    : {EVENT_LOG_PATH}")
print(f"Schema        : {SCHEMA}")
print(f"Table Prefix  : {TABLE_PREFIX}")

# COMMAND ----------

# DBTITLE 1,Schema Definitions & Helpers
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

# ==============================================================================
# SCHEMA DEFINITIONS
# ==============================================================================

_ACCUM_ITEM = StructType([
    StructField("ID",                   LongType()),
    StructField("Name",                 StringType()),
    StructField("Value",                StringType()),
    StructField("Internal",             BooleanType()),
    StructField("Count Failed Values",  BooleanType()),
])

_STAGE_INFO = StructType([
    StructField("Stage ID",                 IntegerType()),
    StructField("Stage Attempt ID",         IntegerType()),
    StructField("Stage Name",               StringType()),
    StructField("Number of Tasks",          IntegerType()),
    StructField("Submission Time",          LongType()),
    StructField("First Task Launched Time", LongType()),
    StructField("Completion Time",          LongType()),
    StructField("Failure Reason",           StringType()),
    StructField("Accumulables",             ArrayType(_ACCUM_ITEM)),
])

_TASK_INFO = StructType([
    StructField("Task ID",              LongType()),
    StructField("Index",                IntegerType()),
    StructField("Attempt",              IntegerType()),
    StructField("Launch Time",          LongType()),
    StructField("Executor ID",          StringType()),
    StructField("Host",                 StringType()),
    StructField("Locality",             StringType()),
    StructField("Speculative",          BooleanType()),
    StructField("Getting Result Time",  LongType()),
    StructField("Finish Time",          LongType()),
])

_TASK_METRICS = StructType([
    StructField("Executor Deserialize Time",        LongType()),
    StructField("Executor Deserialize CPU Time",    LongType()),
    StructField("Executor Run Time",                LongType()),
    StructField("Executor CPU Time",                LongType()),
    StructField("Peak Execution Memory",            LongType()),
    StructField("Result Size",                      LongType()),
    StructField("JVM GC Time",                      LongType()),
    StructField("Result Serialization Time",        LongType()),
    StructField("Memory Bytes Spilled",             LongType()),
    StructField("Disk Bytes Spilled",               LongType()),
    StructField("Shuffle Read Metrics", StructType([
        StructField("Remote Blocks Fetched",    LongType()),
        StructField("Local Blocks Fetched",     LongType()),
        StructField("Fetch Wait Time",          LongType()),
        StructField("Remote Bytes Read",        LongType()),
        StructField("Remote Bytes Read To Disk",LongType()),
        StructField("Local Bytes Read",         LongType()),
        StructField("Total Records Read",       LongType()),
    ])),
    StructField("Shuffle Write Metrics", StructType([
        StructField("Shuffle Bytes Written",    LongType()),
        StructField("Shuffle Write Time",       LongType()),
        StructField("Shuffle Records Written",  LongType()),
    ])),
    StructField("Input Metrics", StructType([
        StructField("Bytes Read",       LongType()),
        StructField("Records Read",     LongType()),
    ])),
    StructField("Output Metrics", StructType([
        StructField("Bytes Written",    LongType()),
        StructField("Records Written",  LongType()),
    ])),
])

_TASK_END_EVENT = StructType([
    StructField("Event",            StringType()),
    StructField("Stage ID",         IntegerType()),
    StructField("Stage Attempt ID", IntegerType()),
    StructField("Task Type",        StringType()),
    StructField("Task End Reason",  StructType([StructField("Reason", StringType())])),
    StructField("Task Info",        _TASK_INFO),
    StructField("Task Metrics",     _TASK_METRICS),
])


# ==============================================================================
# HELPER: Accumulables から指定メトリクス名の値を取得
# ==============================================================================
def _accum(arr_col, metric_name: str):
    """
    Stage Info の Accumulables 配列から内部メトリクスを抽出。
    該当なしの場合は 0 を返す。
    """
    matched = F.filter(arr_col, lambda x: x["Name"] == metric_name)
    return F.when(
        F.size(matched) > 0,
        matched[0]["Value"].cast(LongType())
    ).otherwise(F.lit(0).cast(LongType()))


# ==============================================================================
# テーブル名接頭辞 (ウィジェットから取得済み)
# ==============================================================================

def _save(df, table_name, reread=False, merge_keys=None):
    """DataFrame を SCHEMA 配下のテーブルに保存する。テーブル名には TABLE_PREFIX を付与。

    merge_keys が指定された場合は Delta MERGE (upsert) を使い、
    既存レコードを保持しつつ該当キーのレコードのみ上書きする。
    merge_keys が None の場合は overwrite モードで全件上書きする。
    reread=True の場合、保存後にテーブルから再読み込みして返す。
    """
    full_name = f"{SCHEMA}.{TABLE_PREFIX}{table_name}"

    if merge_keys and spark.catalog.tableExists(full_name):
        from delta.tables import DeltaTable
        # Auto-add new columns if source has columns not in target
        try:
            existing_cols = {c.name.lower() for c in spark.table(full_name).schema}
            new_cols = [f for f in df.schema if f.name.lower() not in existing_cols]
            if new_cols:
                for f in new_cols:
                    spark.sql(f"ALTER TABLE {full_name} ADD COLUMNS (`{f.name}` {f.dataType.simpleString()})")
                    print(f"    + Added column: {f.name}")
        except Exception as e:
            print(f"    ⚠ Schema migration skipped: {e}")
        condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in merge_keys])
        DeltaTable.forName(spark, full_name).alias("t").merge(
            df.alias("s"), condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)

    saved_df = spark.read.table(full_name) if reread else df
    print(f"  \u2713 {full_name} ({saved_df.count():,} rows){' [MERGE]' if merge_keys else ''}")
    return saved_df

# COMMAND ----------

# DBTITLE 1,Bronze Layer
# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC
# MAGIC S3 / UC Volume から Spark event log を読み込み、生テキスト行として取り込みます。

# COMMAND ----------

# DBTITLE 1,bronze_raw_events
# ==============================================================================
# BRONZE: bronze_raw_events
# ==============================================================================
print("[Bronze] bronze_raw_events ...")

# --- Step 1: 全イベントを読み込み、セッションID（ファイルパスベース）を付与 ---
_raw_df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("ignoreMissingFiles", "true")
    .option("ignoreCorruptFiles", "true")
    .text(EVENT_LOG_PATH)
    .withColumn("source_file",  F.col("_metadata.file_path"))
    .withColumn("_session_id",
        F.coalesce(
            F.nullif(
                F.regexp_extract(F.col("source_file"), r"/eventlog/([^/]+)/", 1),
                F.lit("")
            ),
            F.nullif(
                F.regexp_extract(F.col("source_file"), r"(application_\d+_\d+)", 1),
                F.lit("")
            ),
        )
    )
    .withColumn("event_type",   F.get_json_object(F.col("value"), "$.Event"))
    .withColumn("cluster_id",   F.lit(CLUSTER_ID))
    .withColumn("ingested_at",  F.current_timestamp())
    .filter(F.col("event_type").isNotNull())
)

# --- Step 2: セッションID → 実際の Spark App ID のマッピングを構築 ---
# SparkListenerApplicationStart イベントから App ID を取得
_session_to_app = (
    _raw_df
    .filter(F.col("event_type") == "SparkListenerApplicationStart")
    .withColumn("_real_app_id", F.get_json_object(F.col("value"), "$['App ID']"))
    .withColumn("_app_name",    F.get_json_object(F.col("value"), "$['App Name']"))
    .withColumn("_start_ms",    F.get_json_object(F.col("value"), "$.Timestamp").cast("long"))
    .withColumn("_start_ts", F.to_timestamp(F.col("_start_ms") / 1000))
    .select("_session_id", "_real_app_id", "_app_name", "_start_ms", "_start_ts")
    .dropDuplicates(["_session_id"])
)

print("[Bronze] セッション → App ID マッピング:")
_session_to_app.select("_session_id", "_real_app_id", "_app_name", "_start_ts").orderBy("_start_ts").show(truncate=False)

# --- Step 3: セッションID を実際の App ID に置換 ---
bronze_df = (
    _raw_df
    .join(F.broadcast(_session_to_app), on="_session_id", how="left")
    .withColumn("app_id", F.coalesce(F.col("_real_app_id"), F.col("_session_id")))
    .withColumn("app_name", F.col("_app_name"))
    .withColumn("app_start_ms", F.col("_start_ms"))
    .drop("_session_id", "_real_app_id", "_app_name", "_start_ms")
)

bronze_df = _save(bronze_df, "bronze_raw_events", reread=True)

# セッション一覧を表示
print("\n[Bronze] 検出されたアプリケーション:")
bronze_df.select("app_id", "app_name", F.to_timestamp(F.col("app_start_ms") / 1000).alias("start_ts")).dropDuplicates(["app_id"]).orderBy("start_ts").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Silver Layer
# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC
# MAGIC イベント種別ごとにパース・フラット化します（9 テーブル）。

# COMMAND ----------

# DBTITLE 1,silver_application_events
# ==============================================================================
# SILVER 1: silver_application_events
# ==============================================================================
print("[Silver] silver_application_events ...")

j = lambda p: F.get_json_object(F.col("value"), p)

silver_app_df = (
    bronze_df
    .filter(F.col("event_type").isin(
        "SparkListenerApplicationStart",
        "SparkListenerApplicationEnd"
    ))
    .withColumn("app_name",     j("$['App Name']"))
    .withColumn("spark_user",   j("$['Spark User']"))
    .withColumn("timestamp_ms", j("$.Timestamp").cast("long"))
    .withColumn("timestamp_ts", F.to_timestamp(F.col("timestamp_ms") / 1000))
    .select(
        "cluster_id", "app_id", "event_type",
        "app_name", "spark_user",
        "timestamp_ts", "timestamp_ms",
        "source_file", "ingested_at",
    )
    # expect_or_drop → filter
    .filter(F.expr("app_id IS NOT NULL AND app_id != ''"))
)

silver_app_df = _save(silver_app_df, "silver_application_events", reread=True)

# COMMAND ----------

# DBTITLE 1,silver_job_events
# ==============================================================================
# SILVER 2: silver_job_events
# ==============================================================================
print("[Silver] silver_job_events ...")

j = lambda p: F.get_json_object(F.col("value"), p)

silver_job_df = (
    bronze_df
    .filter(F.col("event_type").isin(
        "SparkListenerJobStart",
        "SparkListenerJobEnd"
    ))
    .withColumn("job_id",           j("$['Job ID']").cast("int"))
    .withColumn("submission_ms",    j("$['Submission Time']").cast("long"))
    .withColumn("completion_ms",    j("$['Completion Time']").cast("long"))
    .withColumn("job_result",       j("$['Job Result']['Result']"))
    .withColumn("stage_ids",        j("$['Stage IDs']"))
    .withColumn("submission_ts",    F.to_timestamp(F.col("submission_ms") / 1000))
    .withColumn("completion_ts",    F.to_timestamp(F.col("completion_ms") / 1000))
    # SparkListenerJobStart の Properties から SQL Execution ID を抽出
    .withColumn("_sql_exec_str",
        F.regexp_extract(F.col("value"), r'"spark\.sql\.execution\.id":"(\d+)"', 1)
    )
    .withColumn("sql_execution_id",
        F.when(F.col("_sql_exec_str") != "", F.col("_sql_exec_str").cast("long"))
    )
    .drop("_sql_exec_str")
    .select(
        "cluster_id", "app_id", "event_type", "job_id",
        "submission_ts", "completion_ts",
        "submission_ms", "completion_ms",
        "job_result", "stage_ids", "sql_execution_id",
        "source_file", "ingested_at",
    )
    # expect_or_drop → filter
    .filter(F.expr("job_id IS NOT NULL"))
)

silver_job_df = _save(silver_job_df, "silver_job_events", reread=True)

# COMMAND ----------

# DBTITLE 1,silver_stage_events
# ==============================================================================
# SILVER 3: silver_stage_events
# ==============================================================================
print("[Silver] silver_stage_events ...")

_stage_event_schema = StructType([
    StructField("Event",      StringType()),
    StructField("Stage Info", _STAGE_INFO),
])

_stage_parsed = (
    bronze_df
    .filter(F.col("event_type") == "SparkListenerStageCompleted")
    .withColumn("p",   F.from_json(F.col("value"), _stage_event_schema))
    .withColumn("si",  F.col("p.Stage Info"))
    .withColumn("acc", F.col("si.Accumulables"))
)

silver_stage_df = (
    _stage_parsed
    # Stage Info 基本フィールド
    .withColumn("stage_id",         F.col("si.Stage ID"))
    .withColumn("attempt_id",       F.col("si.Stage Attempt ID"))
    .withColumn("stage_name",       F.col("si.Stage Name"))
    .withColumn("num_tasks",        F.col("si.Number of Tasks"))
    .withColumn("submission_ms",    F.col("si.Submission Time"))
    .withColumn("first_task_ms",    F.col("si.First Task Launched Time"))
    .withColumn("completion_ms",    F.col("si.Completion Time"))
    .withColumn("failure_reason",   F.col("si.Failure Reason"))
    .withColumn("status",
        F.when(
            F.col("failure_reason").isNotNull() & (F.col("failure_reason") != ""),
            F.lit("FAILED")
        ).otherwise(F.lit("COMPLETED"))
    )
    # タイムスタンプ変換
    .withColumn("submission_ts",    F.to_timestamp(F.col("submission_ms") / 1000))
    .withColumn("first_task_ts",    F.to_timestamp(F.col("first_task_ms") / 1000))
    .withColumn("completion_ts",    F.to_timestamp(F.col("completion_ms") / 1000))
    .withColumn("duration_ms",      F.col("completion_ms") - F.col("submission_ms"))
    .withColumn("scheduling_delay_ms",
        F.when(
            F.col("first_task_ms").isNotNull(),
            F.col("first_task_ms") - F.col("submission_ms")
        ).otherwise(F.lit(0))
    )
    # --- Accumulables: 実行時間・CPU ---
    .withColumn("executor_run_time_ms",
        _accum(F.col("acc"), "internal.metrics.executorRunTime"))
    .withColumn("executor_cpu_time_ns",
        _accum(F.col("acc"), "internal.metrics.executorCpuTime"))
    .withColumn("jvm_gc_time_ms",
        _accum(F.col("acc"), "internal.metrics.jvmGarbageCollectionTime"))
    .withColumn("deserialize_ms",
        _accum(F.col("acc"), "internal.metrics.executorDeserializeTime"))
    .withColumn("result_serialize_ms",
        _accum(F.col("acc"), "internal.metrics.resultSerializationTime"))
    .withColumn("result_size_bytes",
        _accum(F.col("acc"), "internal.metrics.resultSize"))
    .withColumn("peak_exec_memory_bytes",
        _accum(F.col("acc"), "internal.metrics.peakExecutionMemory"))
    # --- Accumulables: Spill ---
    .withColumn("memory_bytes_spilled",
        _accum(F.col("acc"), "internal.metrics.memoryBytesSpilled"))
    .withColumn("disk_bytes_spilled",
        _accum(F.col("acc"), "internal.metrics.diskBytesSpilled"))
    # --- Accumulables: I/O ---
    .withColumn("input_bytes",
        _accum(F.col("acc"), "internal.metrics.input.bytesRead"))
    .withColumn("input_records",
        _accum(F.col("acc"), "internal.metrics.input.recordsRead"))
    .withColumn("output_bytes",
        _accum(F.col("acc"), "internal.metrics.output.bytesWritten"))
    .withColumn("output_records",
        _accum(F.col("acc"), "internal.metrics.output.recordsWritten"))
    # --- Accumulables: Shuffle Read ---
    .withColumn("shuffle_remote_bytes",
        _accum(F.col("acc"), "internal.metrics.shuffle.read.remoteBytesRead"))
    .withColumn("shuffle_local_bytes",
        _accum(F.col("acc"), "internal.metrics.shuffle.read.localBytesRead"))
    .withColumn("shuffle_read_records",
        _accum(F.col("acc"), "internal.metrics.shuffle.read.recordsRead"))
    .withColumn("shuffle_fetch_wait_ms",
        _accum(F.col("acc"), "internal.metrics.shuffle.read.fetchWaitTime"))
    .withColumn("shuffle_remote_blocks",
        _accum(F.col("acc"), "internal.metrics.shuffle.read.remoteBlocksFetched"))
    .withColumn("shuffle_local_blocks",
        _accum(F.col("acc"), "internal.metrics.shuffle.read.localBlocksFetched"))
    # --- Accumulables: Shuffle Write ---
    .withColumn("shuffle_write_bytes",
        _accum(F.col("acc"), "internal.metrics.shuffle.write.bytesWritten"))
    .withColumn("shuffle_write_records",
        _accum(F.col("acc"), "internal.metrics.shuffle.write.recordsWritten"))
    .withColumn("shuffle_write_time_ns",
        _accum(F.col("acc"), "internal.metrics.shuffle.write.writeTime"))
    # --- 派生: Shuffle Read 合計 ---
    .withColumn("shuffle_read_bytes",
        F.col("shuffle_remote_bytes") + F.col("shuffle_local_bytes"))
    # --- 派生: GC オーバーヘッド率 ---
    .withColumn("gc_overhead_pct",
        F.when(
            F.col("executor_run_time_ms") > 0,
            F.col("jvm_gc_time_ms") / F.col("executor_run_time_ms") * 100
        ).otherwise(F.lit(0.0))
    )
    # --- 派生: CPU 効率 ---
    .withColumn("cpu_efficiency_pct",
        F.when(
            F.col("executor_run_time_ms") > 0,
            (F.col("executor_cpu_time_ns") / 1e6) / F.col("executor_run_time_ms") * 100
        ).otherwise(None)
    )
    .select(
        "cluster_id", "app_id", "stage_id", "attempt_id",
        "stage_name", "status", "failure_reason",
        "num_tasks",
        "submission_ts", "first_task_ts", "completion_ts",
        "duration_ms", "scheduling_delay_ms",
        "executor_run_time_ms", "executor_cpu_time_ns",
        "jvm_gc_time_ms", "gc_overhead_pct", "cpu_efficiency_pct",
        "deserialize_ms", "result_serialize_ms", "result_size_bytes",
        "peak_exec_memory_bytes",
        "memory_bytes_spilled", "disk_bytes_spilled",
        "input_bytes", "input_records",
        "output_bytes", "output_records",
        "shuffle_read_bytes", "shuffle_read_records",
        "shuffle_remote_bytes", "shuffle_local_bytes",
        "shuffle_remote_blocks", "shuffle_local_blocks",
        "shuffle_fetch_wait_ms",
        "shuffle_write_bytes", "shuffle_write_records", "shuffle_write_time_ns",
        "source_file", "ingested_at",
    )
    # expect_or_drop → filter
    .filter(F.expr("stage_id IS NOT NULL"))
)

silver_stage_df = _save(silver_stage_df, "silver_stage_events", reread=True)

# COMMAND ----------

# DBTITLE 1,silver_task_events
# ==============================================================================
# SILVER 4: silver_task_events
# ==============================================================================
print("[Silver] silver_task_events ...")

_task_parsed = (
    bronze_df
    .filter(F.col("event_type") == "SparkListenerTaskEnd")
    .withColumn("p", F.from_json(F.col("value"), _TASK_END_EVENT))
)

ti = F.col("p.Task Info")
tm = F.col("p.Task Metrics")
sr = F.col("p.Task Metrics.Shuffle Read Metrics")
sw = F.col("p.Task Metrics.Shuffle Write Metrics")
im = F.col("p.Task Metrics.Input Metrics")
om = F.col("p.Task Metrics.Output Metrics")

silver_task_df = (
    _task_parsed
    .withColumn("stage_id",         F.col("p.Stage ID"))
    .withColumn("attempt_id",       F.col("p.Stage Attempt ID"))
    .withColumn("task_type",        F.col("p.Task Type"))
    .withColumn("task_result",      F.col("p.Task End Reason.Reason"))
    # Task Info
    .withColumn("task_id",          ti["Task ID"])
    .withColumn("index",            ti["Index"])
    .withColumn("executor_id",      ti["Executor ID"])
    .withColumn("host",             ti["Host"])
    .withColumn("locality",         ti["Locality"])
    .withColumn("speculative",      ti["Speculative"])
    .withColumn("launch_ms",        ti["Launch Time"])
    .withColumn("finish_ms",        ti["Finish Time"])
    .withColumn("task_duration_ms", ti["Finish Time"] - ti["Launch Time"])
    .withColumn("launch_ts",        F.to_timestamp(ti["Launch Time"] / 1000))
    # Task Metrics
    .withColumn("executor_run_time_ms",     tm["Executor Run Time"])
    .withColumn("executor_cpu_time_ns",     tm["Executor CPU Time"])
    .withColumn("deserialize_ms",           tm["Executor Deserialize Time"])
    .withColumn("result_serialize_ms",      tm["Result Serialization Time"])
    .withColumn("gc_time_ms",               tm["JVM GC Time"])
    .withColumn("result_size_bytes",        tm["Result Size"])
    .withColumn("peak_exec_memory_bytes",   tm["Peak Execution Memory"])
    .withColumn("memory_bytes_spilled",     tm["Memory Bytes Spilled"])
    .withColumn("disk_bytes_spilled",       tm["Disk Bytes Spilled"])
    # Shuffle Read
    .withColumn("shuffle_remote_blocks",    sr["Remote Blocks Fetched"])
    .withColumn("shuffle_local_blocks",     sr["Local Blocks Fetched"])
    .withColumn("shuffle_fetch_wait_ms",    sr["Fetch Wait Time"])
    .withColumn("shuffle_remote_bytes",     sr["Remote Bytes Read"])
    .withColumn("shuffle_local_bytes",      sr["Local Bytes Read"])
    .withColumn("shuffle_read_records",     sr["Total Records Read"])
    .withColumn("shuffle_read_bytes",
        F.coalesce(sr["Remote Bytes Read"], F.lit(0)) +
        F.coalesce(sr["Local Bytes Read"],  F.lit(0))
    )
    # Shuffle Write
    .withColumn("shuffle_write_bytes",      sw["Shuffle Bytes Written"])
    .withColumn("shuffle_write_time_ns",    sw["Shuffle Write Time"])
    .withColumn("shuffle_write_records",    sw["Shuffle Records Written"])
    # Input / Output
    .withColumn("input_bytes",              im["Bytes Read"])
    .withColumn("input_records",            im["Records Read"])
    .withColumn("output_bytes",             om["Bytes Written"])
    .withColumn("output_records",           om["Records Written"])
    # 派生: GC オーバーヘッド率
    .withColumn("gc_overhead_pct",
        F.when(
            F.col("task_duration_ms") > 0,
            F.col("gc_time_ms") / F.col("task_duration_ms") * 100
        ).otherwise(F.lit(0.0))
    )
    # 派生: CPU 効率
    .withColumn("cpu_efficiency_pct",
        F.when(
            F.col("executor_run_time_ms") > 0,
            (F.col("executor_cpu_time_ns") / 1e6) / F.col("executor_run_time_ms") * 100
        ).otherwise(None)
    )
    .select(
        "cluster_id", "app_id", "stage_id", "attempt_id",
        "task_id", "index", "task_type", "task_result",
        "executor_id", "host", "locality", "speculative",
        "launch_ts", "task_duration_ms",
        "executor_run_time_ms", "executor_cpu_time_ns",
        "deserialize_ms", "result_serialize_ms",
        "gc_time_ms", "gc_overhead_pct", "cpu_efficiency_pct",
        "result_size_bytes", "peak_exec_memory_bytes",
        "memory_bytes_spilled", "disk_bytes_spilled",
        "shuffle_read_bytes", "shuffle_read_records",
        "shuffle_remote_bytes", "shuffle_local_bytes",
        "shuffle_remote_blocks", "shuffle_local_blocks",
        "shuffle_fetch_wait_ms",
        "shuffle_write_bytes", "shuffle_write_records", "shuffle_write_time_ns",
        "input_bytes", "input_records",
        "output_bytes", "output_records",
        "ingested_at",
    )
    # expect_or_drop → filter
    .filter(F.expr("task_id IS NOT NULL"))
)

silver_task_df = _save(silver_task_df, "silver_task_events", reread=True)

# COMMAND ----------

# DBTITLE 1,silver_executor_events
# ==============================================================================
# SILVER 5: silver_executor_events
# ==============================================================================
print("[Silver] silver_executor_events ...")

j = lambda p: F.get_json_object(F.col("value"), p)

silver_exec_df = (
    bronze_df
    .filter(F.col("event_type").isin(
        "SparkListenerExecutorAdded",
        "SparkListenerExecutorRemoved"
    ))
    .withColumn("executor_id",        j("$['Executor ID']"))
    .withColumn("timestamp_ms",       j("$.Timestamp").cast("long"))
    .withColumn("host",               j("$['Executor Info']['Host']"))
    .withColumn("total_cores",        j("$['Executor Info']['Total Cores']").cast("int"))
    .withColumn("removed_reason",     j("$['Removed Reason']"))
    .withColumn("resource_profile_id",j("$['Executor Info']['Resource Profile Id']").cast("int"))
    .withColumn("timestamp_ts",       F.to_timestamp(F.col("timestamp_ms") / 1000))
    .select(
        "cluster_id", "app_id", "event_type", "executor_id",
        "timestamp_ts", "timestamp_ms", "host", "total_cores", "removed_reason",
        "resource_profile_id", "ingested_at",
    )
)

silver_exec_df = _save(silver_exec_df, "silver_executor_events", reread=True)

# COMMAND ----------

# DBTITLE 1,silver_resource_profiles
# ==============================================================================
# SILVER 5b: silver_resource_profiles
# ==============================================================================
print("[Silver] silver_resource_profiles ...")

j = lambda p: F.get_json_object(F.col("value"), p)

silver_rp_df = (
    bronze_df
    .filter(F.col("event_type") == "SparkListenerResourceProfileAdded")
    .withColumn("resource_profile_id",
        j("$['Resource Profile Id']").cast("int"))
    .withColumn("onheap_memory_mb",
        j("$['Executor Resource Requests']['memory']['Amount']").cast("double"))
    .withColumn("offheap_memory_mb",
        j("$['Executor Resource Requests']['offHeap']['Amount']").cast("double"))
    .withColumn("task_cpus",
        j("$['Task Resource Requests']['cpus']['Amount']").cast("double"))
    .select(
        "cluster_id", "app_id", "resource_profile_id",
        "onheap_memory_mb", "offheap_memory_mb", "task_cpus",
        "ingested_at",
    )
)

silver_rp_df = _save(silver_rp_df, "silver_resource_profiles", reread=True)

# COMMAND ----------

# DBTITLE 1,silver_spark_config
# ==============================================================================
# SILVER 6: silver_spark_config
# ==============================================================================
print("[Silver] silver_spark_config ...")

_props_schema = MapType(StringType(), StringType())

silver_config_df = (
    bronze_df
    .filter(F.col("event_type") == "SparkListenerEnvironmentUpdate")
    .withColumn("spark_props",
        F.from_json(
            F.get_json_object(F.col("value"), "$['Spark Properties']"),
            _props_schema
        )
    )
    .select("*", F.explode("spark_props").alias("config_key", "config_value"))
    .filter(
        F.col("config_key").rlike(
            r"^spark\.(sql\.adaptive|executor|driver|memory|cores"
            r"|shuffle|sql\.shuffle|sql\.broadcast"
            r"|databricks\.|sql\.files"
            r"|decommission|speculation|storage\.decommission"
            r"|sql\.autoBroadcastJoinThreshold"
            r"|locality)"
        )
    )
    .select("cluster_id", "app_id", "config_key", "config_value", "ingested_at")
    .dropDuplicates(["cluster_id", "app_id", "config_key"])
)

silver_config_df = _save(silver_config_df, "silver_spark_config", reread=True)

# COMMAND ----------

# DBTITLE 1,silver_sql_executions
# ==============================================================================
# SILVER 7: silver_sql_executions
# ==============================================================================
print("[Silver] silver_sql_executions ...")

j = lambda p: F.get_json_object(F.col("value"), p)

_sql_starts = (
    bronze_df
    .filter(F.col("event_type") ==
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart")
    .withColumn("execution_id",  j("$.executionId").cast("long"))
    .withColumn("description",   j("$.description"))
    .withColumn("physical_plan", j("$.physicalPlanDescription"))
    .withColumn("spark_plan_json", j("$.sparkPlanInfo"))
    .withColumn("start_time_ms", j("$.time").cast("long"))
    .select("cluster_id", "app_id", "execution_id",
            "description", "physical_plan", "spark_plan_json", "start_time_ms", "ingested_at")
)

_sql_ends = (
    bronze_df
    .filter(F.col("event_type") ==
            "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd")
    .withColumn("execution_id", j("$.executionId").cast("long"))
    .withColumn("end_time_ms",  j("$.time").cast("long"))
    .select("cluster_id", "app_id", "execution_id", "end_time_ms")
)

silver_sql_df = _sql_starts.join(_sql_ends, on=["cluster_id", "app_id", "execution_id"], how="left")

silver_sql_df = _save(silver_sql_df, "silver_sql_executions", reread=True)

# COMMAND ----------

# DBTITLE 1,silver_streaming_events
# ==============================================================================
# SILVER 8: silver_streaming_events
# ==============================================================================
print("[Silver] silver_streaming_events ...")

j = lambda p: F.get_json_object(F.col("value"), p)

_streaming_raw = (
    bronze_df
    .filter(F.col("event_type").like("%StreamingQueryListener%"))
)

_streaming_count = _streaming_raw.count()
print(f"  Streaming events found: {_streaming_count}")

if _streaming_count > 0:
    silver_streaming_df = (
        _streaming_raw
        # ── 識別 ──
        .withColumn("query_id",   F.coalesce(j("$.id"), j("$.progress.id")))
        .withColumn("run_id",     F.coalesce(j("$.runId"), j("$.progress.runId")))
        .withColumn("query_name", F.coalesce(j("$.name"), j("$.progress.name")))
        # ── バッチ処理時間内訳 ──
        .withColumn("batch_id",            j("$.progress.batchId").cast("long"))
        .withColumn("batch_duration_ms",   j("$.progress.batchDuration").cast("long"))
        .withColumn("trigger_execution_ms", j("$.progress.durationMs.triggerExecution").cast("long"))
        .withColumn("query_planning_ms",   j("$.progress.durationMs.queryPlanning").cast("long"))
        .withColumn("get_batch_ms",        j("$.progress.durationMs.getBatch").cast("long"))
        .withColumn("add_batch_ms",        j("$.progress.durationMs.addBatch").cast("long"))
        .withColumn("latest_offset_ms",    j("$.progress.durationMs.latestOffset").cast("long"))
        .withColumn("commit_offsets_ms",   j("$.progress.durationMs.commitOffsets").cast("long"))
        .withColumn("commit_batch_ms",     j("$.progress.durationMs.commitBatch").cast("long"))
        .withColumn("wal_commit_ms",       j("$.progress.durationMs.walCommit").cast("long"))
        # ── スループット（最初のソース）──
        .withColumn("num_input_rows",        j("$.progress.sources[0].numInputRows").cast("long"))
        .withColumn("input_rows_per_sec",    j("$.progress.sources[0].inputRowsPerSecond").cast("double"))
        .withColumn("processed_rows_per_sec", j("$.progress.sources[0].processedRowsPerSecond").cast("double"))
        # ── ソース/シンク ──
        .withColumn("source_description",          j("$.progress.sources[0].description"))
        .withColumn("source_num_files_outstanding", j("$.progress.sources[0].metrics.numFilesOutstanding"))
        .withColumn("source_num_bytes_outstanding", j("$.progress.sources[0].metrics.numBytesOutstanding"))
        .withColumn("sink_description",    j("$.progress.sink.description"))
        .withColumn("sink_num_output_rows", j("$.progress.sink.numOutputRows").cast("long"))
        # ── 状態ストア（最初の stateOperator）──
        .withColumn("state_num_rows_total",          j("$.progress.stateOperators[0].numRowsTotal").cast("long"))
        .withColumn("state_num_rows_updated",        j("$.progress.stateOperators[0].numRowsUpdated").cast("long"))
        .withColumn("state_memory_used_bytes",       j("$.progress.stateOperators[0].memoryUsedBytes").cast("long"))
        .withColumn("state_rows_dropped_by_watermark", j("$.progress.stateOperators[0].numRowsDroppedByWatermark").cast("long"))
        .withColumn("state_all_updates_time_ms",     j("$.progress.stateOperators[0].allUpdatesTimeMs").cast("long"))
        .withColumn("state_all_removals_time_ms",    j("$.progress.stateOperators[0].allRemovalsTimeMs").cast("long"))
        .withColumn("state_commit_time_ms",          j("$.progress.stateOperators[0].commitTimeMs").cast("long"))
        # ── ウォーターマーク ──
        .withColumn("watermark", j("$.progress.eventTime.watermark"))
        # ── 終了情報 ──
        .withColumn("exception", j("$.exception"))
        # ── タイムスタンプ ──
        .withColumn("event_timestamp",
            F.coalesce(
                F.to_timestamp(j("$.progress.timestamp")),
                F.to_timestamp(j("$.timestamp")),
            )
        )
        # ── 派生 ──
        .withColumn("is_stateful",
            F.coalesce(j("$.progress.stateOperators[0].numRowsTotal"), F.lit(None)).isNotNull()
        )
        .select(
            "cluster_id", "app_id", "event_type",
            "query_id", "run_id", "query_name",
            "batch_id", "batch_duration_ms",
            "trigger_execution_ms", "query_planning_ms", "get_batch_ms",
            "add_batch_ms", "latest_offset_ms", "commit_offsets_ms",
            "commit_batch_ms", "wal_commit_ms",
            "num_input_rows", "input_rows_per_sec", "processed_rows_per_sec",
            "source_description", "source_num_files_outstanding", "source_num_bytes_outstanding",
            "sink_description", "sink_num_output_rows",
            "state_num_rows_total", "state_num_rows_updated", "state_memory_used_bytes",
            "state_rows_dropped_by_watermark", "state_all_updates_time_ms",
            "state_all_removals_time_ms", "state_commit_time_ms",
            "watermark", "exception", "event_timestamp", "is_stateful",
            "source_file", "ingested_at",
        )
    )
    silver_streaming_df = _save(silver_streaming_df, "silver_streaming_events", reread=True)
else:
    silver_streaming_df = None
    print("  → No streaming events detected, skipping silver_streaming_events")

# COMMAND ----------

# DBTITLE 1,Gold Layer
# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC
# MAGIC 集計・分析・ボトルネック判定（9 テーブル）。

# COMMAND ----------

# DBTITLE 1,gold_application_summary
# ==============================================================================
# GOLD 1: gold_application_summary
# ==============================================================================
print("[Gold] gold_application_summary ...")

app_start = (
    silver_app_df
    .filter(F.col("event_type") == "SparkListenerApplicationStart")
    .select(
        "cluster_id", "app_id", "app_name", "spark_user",
        F.col("timestamp_ts").alias("start_ts"),
        F.col("timestamp_ms").alias("start_ms"),
    )
)
app_end = (
    silver_app_df
    .filter(F.col("event_type") == "SparkListenerApplicationEnd")
    .select(
        "app_id",
        F.col("timestamp_ts").alias("end_ts"),
        F.col("timestamp_ms").alias("end_ms"),
    )
)
# ── ステージ集計: Completed/Failed メトリクス（silver_stage_df から）──────────
_stage_metric_agg = (
    silver_stage_df
    .groupBy("cluster_id", "app_id")
    .agg(
        F.count("stage_id").alias("completed_stages_raw"),
        F.sum(F.when(F.col("status") == "COMPLETED", 1).otherwise(0)).alias("completed_stages"),
        F.sum(F.when(F.col("status") == "FAILED",    1).otherwise(0)).alias("failed_stages"),
        F.sum("num_tasks").alias("total_tasks"),
        F.sum("input_bytes").alias("total_input_bytes"),
        F.sum("shuffle_read_bytes").alias("total_shuffle_read_bytes"),
        F.sum("shuffle_write_bytes").alias("total_shuffle_write_bytes"),
        F.sum("disk_bytes_spilled").alias("total_disk_spilled_bytes"),
        F.sum("memory_bytes_spilled").alias("total_memory_spilled_bytes"),
        F.sum("jvm_gc_time_ms").alias("total_gc_time_ms"),
        F.sum("executor_run_time_ms").alias("total_exec_run_ms"),
        F.count(F.when(F.col("disk_bytes_spilled") > 0, 1)).alias("stages_with_disk_spill"),
    )
)
# ── 全ステージ数: ジョブの宣言 stage_ids から（Skipped 含む）────────────────
_total_stages_agg = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobStart")
    .withColumn("_sid", F.explode(F.from_json(F.col("stage_ids"), ArrayType(IntegerType()))))
    .groupBy("cluster_id", "app_id")
    .agg(F.countDistinct("_sid").alias("total_stages"))
)
stage_agg = (
    _stage_metric_agg
    .join(_total_stages_agg, on=["cluster_id", "app_id"], how="left")
)
job_agg = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobEnd")
    .groupBy("cluster_id", "app_id")
    .agg(
        F.count("job_id").alias("total_jobs"),
        F.sum(F.when(F.col("job_result") == "JobSucceeded", 1).otherwise(0)).alias("succeeded_jobs"),
        F.sum(F.when(F.col("job_result") != "JobSucceeded", 1).otherwise(0)).alias("failed_jobs"),
        F.max("completion_ms").alias("last_job_end_ms"),
    )
)

# ── クラスタ情報: silver_spark_config から主要タグを取得 ─────────────────
_cluster_tags = [
    ("clusterName",        "cluster_name"),
    ("clusterNodeType",    "worker_node_type"),
    ("driverNodeType",     "driver_node_type"),
    ("clusterMinWorkers",  "min_workers"),
    ("clusterMaxWorkers",  "max_workers"),
    ("sparkVersion",       "dbr_version"),
    ("clusterAvailability","cluster_availability"),
    ("region",             "region"),
]
_cluster_info = (
    silver_config_df
    .filter(F.col("config_key").startswith("spark.databricks.clusterUsageTags."))
    .withColumn("_tag", F.regexp_extract(F.col("config_key"), r"clusterUsageTags\.(.+)$", 1))
    .groupBy("cluster_id", "app_id")
    .pivot("_tag", [t[0] for t in _cluster_tags])
    .agg(F.first("config_value"))
)
for _orig, _alias in _cluster_tags:
    _cluster_info = _cluster_info.withColumnRenamed(_orig, _alias)

# ── ストリーミングクエリ集計 ──────────────────────────────────────────────
if silver_streaming_df is not None:
    _streaming_agg = (
        silver_streaming_df
        .filter(F.col("event_type").contains("QueryStartedEvent"))
        .groupBy("cluster_id", "app_id")
        .agg(
            F.countDistinct("query_id").alias("streaming_query_count"),
        )
        .withColumn("has_streaming_queries", F.lit(True))
    )
else:
    _streaming_agg = None

gold_app_df = (
    app_start
    .join(app_end,   on="app_id",                 how="left")
    .join(stage_agg, on=["cluster_id", "app_id"], how="left")
    .join(job_agg,   on=["cluster_id", "app_id"], how="left")
    .join(_cluster_info, on=["cluster_id", "app_id"], how="left")
    # end_ts / end_ms が NULL の場合（ApplicationEnd 欠落）はジョブ完了時刻から推定
    .withColumn("duration_ms",
        F.coalesce(
            F.col("end_ms") - F.col("start_ms"),
            F.col("last_job_end_ms") - F.col("start_ms"),
        )
    )
    .withColumn("duration_min",     F.col("duration_ms") / 60_000)
    .withColumn("end_ts",
        F.coalesce(
            F.col("end_ts"),
            F.to_timestamp(F.col("last_job_end_ms") / 1000)
        )
    )
    .withColumn("job_success_rate",
        F.when(F.col("total_jobs") > 0,
            F.round(F.col("succeeded_jobs") / F.col("total_jobs") * 100, 1)
        ).otherwise(None)
    )
    .withColumn("gc_overhead_pct",
        F.when(F.col("total_exec_run_ms") > 0,
            F.round(F.col("total_gc_time_ms") / F.col("total_exec_run_ms") * 100, 2)
        ).otherwise(0.0)
    )
    .withColumn("total_input_gb",   F.round(F.col("total_input_bytes")        / 1024**3, 3))
    .withColumn("total_shuffle_gb", F.round(F.col("total_shuffle_read_bytes") / 1024**3, 3))
    .withColumn("total_spill_gb",   F.round(
        (F.col("total_disk_spilled_bytes") + F.col("total_memory_spilled_bytes")) / 1024**3, 3
    ))
)

# ストリーミング集計を LEFT JOIN
if _streaming_agg is not None:
    gold_app_df = (
        gold_app_df
        .join(_streaming_agg, on=["cluster_id", "app_id"], how="left")
        .withColumn("has_streaming_queries",
            F.coalesce(F.col("has_streaming_queries"), F.lit(False)))
        .withColumn("streaming_query_count",
            F.coalesce(F.col("streaming_query_count"), F.lit(0)))
    )
else:
    gold_app_df = (
        gold_app_df
        .withColumn("has_streaming_queries", F.lit(False))
        .withColumn("streaming_query_count", F.lit(0))
    )

gold_app_df = (
    gold_app_df
    .select(
        "cluster_id", "app_id", "app_name", "spark_user",
        "cluster_name", "worker_node_type", "driver_node_type",
        "min_workers", "max_workers", "dbr_version",
        "cluster_availability", "region",
        "start_ts", "end_ts", "duration_ms", "duration_min",
        "total_jobs", "succeeded_jobs", "failed_jobs", "job_success_rate",
        "total_stages", "completed_stages", "failed_stages", "total_tasks",
        "total_input_gb", "total_shuffle_gb", "total_spill_gb",
        "stages_with_disk_spill",
        "total_gc_time_ms", "gc_overhead_pct",
        "total_exec_run_ms",
        "has_streaming_queries", "streaming_query_count",
    )
)

gold_app_df = gold_app_df.withColumn("etl_loaded_at", F.current_timestamp())

# Add placeholder cost columns so _save's whenMatchedUpdateAll() does not fail
# when the target table already has these columns from a prior enrichment run.
# Actual values are filled later by the Cost Estimation Enrichment section.
gold_app_df = (
    gold_app_df
    .withColumn("estimated_total_dbu",    F.lit(None).cast(DoubleType()))
    .withColumn("estimated_dbu_per_hour", F.lit(None).cast(DoubleType()))
    .withColumn("estimated_total_usd",    F.lit(None).cast(DoubleType()))
)

gold_app_df = _save(gold_app_df, "gold_application_summary", merge_keys=["cluster_id", "app_id"])

# COMMAND ----------

# DBTITLE 1,gold_job_performance
# ==============================================================================
# GOLD 2: gold_job_performance
# ==============================================================================
print("[Gold] gold_job_performance ...")

job_start = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobStart")
    .select(
        "cluster_id", "app_id", "job_id", "stage_ids", "sql_execution_id",
        F.col("submission_ts").alias("submit_ts"),
        F.col("submission_ms").alias("submit_ms"),
    )
)
job_end = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobEnd")
    .select(
        "app_id", "job_id", "job_result",
        F.col("completion_ts").alias("complete_ts"),
        F.col("completion_ms").alias("complete_ms"),
    )
)

gold_job_df = (
    job_start
    .join(job_end, on=["app_id", "job_id"], how="left")
    .withColumn("duration_ms",  F.col("complete_ms") - F.col("submit_ms"))
    .withColumn("duration_min", F.round(F.col("duration_ms") / 60_000, 2))
    .withColumn("status",
        F.when(F.col("job_result") == "JobSucceeded", F.lit("SUCCEEDED"))
         .when(F.col("job_result").isNotNull(),        F.lit("FAILED"))
         .otherwise(F.lit("RUNNING"))
    )
    .select(
        "cluster_id", "app_id", "job_id", "status",
        "submit_ts", "complete_ts", "duration_ms", "duration_min",
        "job_result", "stage_ids", "sql_execution_id",
    )
    .orderBy("submit_ts")
)

gold_job_df = _save(gold_job_df, "gold_job_performance", merge_keys=["cluster_id", "app_id", "job_id"])

# COMMAND ----------

# gold_job_detail は gold_bottleneck_report の後に作成（正確なボトルネック情報を使用するため）

# COMMAND ----------

# DBTITLE 1,gold_stage_performance
# ==============================================================================
# GOLD 3: gold_stage_performance
# ==============================================================================
print("[Gold] gold_stage_performance ...")

# タスク単位の分布統計 (成功タスクのみ)
task_dist = (
    silver_task_df
    .filter(F.col("task_result") == "Success")
    .groupBy("cluster_id", "app_id", "stage_id", "attempt_id")
    .agg(
        F.count("task_id").alias("task_count"),
        F.min("task_duration_ms").alias("task_min_ms"),
        F.max("task_duration_ms").alias("task_max_ms"),
        F.avg("task_duration_ms").alias("task_avg_ms"),
        F.percentile_approx("task_duration_ms", 0.50).alias("task_p50_ms"),
        F.percentile_approx("task_duration_ms", 0.75).alias("task_p75_ms"),
        F.percentile_approx("task_duration_ms", 0.95).alias("task_p95_ms"),
        F.percentile_approx("task_duration_ms", 0.99).alias("task_p99_ms"),
        F.percentile_approx("gc_time_ms",        0.95).alias("gc_p95_ms"),
        F.percentile_approx("peak_exec_memory_bytes", 0.95).alias("peak_mem_p95_bytes"),
        F.count(F.when(F.col("speculative"),              True)).alias("speculative_tasks"),
        F.count(F.when(F.col("disk_bytes_spilled") > 0,  True)).alias("tasks_with_disk_spill"),
        F.count(F.when(F.col("task_result") != "Success", True)).alias("failed_tasks"),
        F.min("shuffle_read_bytes").alias("task_shuffle_min_bytes"),
        F.max("shuffle_read_bytes").alias("task_shuffle_max_bytes"),
        F.percentile_approx("shuffle_read_bytes", 0.50).alias("task_shuffle_p50_bytes"),
    )
)

# ── Task-derived stage aggregate ──────────────────────────────────────────────
# SparkListenerStageCompleted が欠落したステージ（AQE によるステージ書き換え、
# 前ジョブのシャッフル出力再利用によるスキップ等）をタスクデータから復元し、
# 全ステージを分析対象に含める。
_task_stage_agg = (
    silver_task_df
    .groupBy("cluster_id", "app_id", "stage_id", "attempt_id")
    .agg(
        F.count("task_id").alias("_t_num_tasks"),
        F.min(F.unix_timestamp("launch_ts")).alias("_t_first_epoch"),
        F.max(
            F.unix_timestamp("launch_ts") + F.col("task_duration_ms") / 1000.0
        ).alias("_t_last_epoch"),
        F.sum("executor_run_time_ms").alias("_t_executor_run_time_ms"),
        F.sum("executor_cpu_time_ns").alias("_t_executor_cpu_time_ns"),
        F.sum("gc_time_ms").alias("_t_jvm_gc_time_ms"),
        F.sum("memory_bytes_spilled").alias("_t_memory_bytes_spilled"),
        F.sum("disk_bytes_spilled").alias("_t_disk_bytes_spilled"),
        F.sum("input_bytes").alias("_t_input_bytes"),
        F.sum("output_bytes").alias("_t_output_bytes"),
        F.sum("shuffle_read_bytes").alias("_t_shuffle_read_bytes"),
        F.sum("shuffle_write_bytes").alias("_t_shuffle_write_bytes"),
        F.sum("shuffle_fetch_wait_ms").alias("_t_shuffle_fetch_wait_ms"),
        F.sum("deserialize_ms").alias("_t_deserialize_ms"),
        F.sum("result_serialize_ms").alias("_t_result_serialize_ms"),
        F.sum("result_size_bytes").alias("_t_result_size_bytes"),
        F.max("peak_exec_memory_bytes").alias("_t_peak_exec_memory_bytes"),
        F.sum(F.when(F.col("task_result") != "Success", 1).otherwise(0)).alias("_t_failed"),
    )
    .withColumn("_t_duration_ms",
        F.round((F.col("_t_last_epoch") - F.col("_t_first_epoch")) * 1000).cast("long"))
    .withColumn("_t_first_task_ts", F.to_timestamp(F.col("_t_first_epoch")))
    .withColumn("_t_completion_ts", F.to_timestamp(F.col("_t_last_epoch")))
    .withColumn("_t_status",
        F.when(F.col("_t_failed") > F.col("_t_num_tasks") / 2, F.lit("FAILED"))
         .otherwise(F.lit("COMPLETED")))
    .withColumn("_t_gc_overhead_pct",
        F.when(F.col("_t_executor_run_time_ms") > 0,
            F.col("_t_jvm_gc_time_ms") / F.col("_t_executor_run_time_ms") * 100
        ).otherwise(0.0))
    .withColumn("_t_cpu_efficiency_pct",
        F.when(F.col("_t_executor_run_time_ms") > 0,
            (F.col("_t_executor_cpu_time_ns") / 1e6) / F.col("_t_executor_run_time_ms") * 100
        ).otherwise(None))
)

# ── ステージイベント + タスク派生メトリクスを統合 ─────────────────────────────
# FULL OUTER JOIN で、StageCompleted が欠落したステージもタスクデータから復元
_stage_base = (
    _task_stage_agg
    .join(silver_stage_df,
          on=["cluster_id", "app_id", "stage_id", "attempt_id"],
          how="full_outer")
    # COALESCE: StageCompleted データを優先、欠落時はタスク派生値にフォールバック
    .withColumn("stage_name",           F.coalesce(F.col("stage_name"), F.lit("(task-derived)")))
    .withColumn("status",               F.coalesce(F.col("status"), F.col("_t_status")))
    .withColumn("num_tasks",            F.coalesce(F.col("num_tasks"), F.col("_t_num_tasks")))
    .withColumn("submission_ts",        F.coalesce(F.col("submission_ts"), F.col("_t_first_task_ts")))
    .withColumn("first_task_ts",        F.coalesce(F.col("first_task_ts"), F.col("_t_first_task_ts")))
    .withColumn("completion_ts",        F.coalesce(F.col("completion_ts"), F.col("_t_completion_ts")))
    .withColumn("duration_ms",          F.coalesce(F.col("duration_ms"), F.col("_t_duration_ms")))
    .withColumn("scheduling_delay_ms",  F.coalesce(F.col("scheduling_delay_ms"), F.lit(0).cast("long")))
    .withColumn("executor_run_time_ms", F.coalesce(F.col("executor_run_time_ms"), F.col("_t_executor_run_time_ms")))
    .withColumn("executor_cpu_time_ns", F.coalesce(F.col("executor_cpu_time_ns"), F.col("_t_executor_cpu_time_ns")))
    .withColumn("jvm_gc_time_ms",       F.coalesce(F.col("jvm_gc_time_ms"), F.col("_t_jvm_gc_time_ms")))
    .withColumn("gc_overhead_pct",      F.coalesce(F.col("gc_overhead_pct"), F.col("_t_gc_overhead_pct")))
    .withColumn("cpu_efficiency_pct",   F.coalesce(F.col("cpu_efficiency_pct"), F.col("_t_cpu_efficiency_pct")))
    .withColumn("memory_bytes_spilled", F.coalesce(F.col("memory_bytes_spilled"), F.col("_t_memory_bytes_spilled")))
    .withColumn("disk_bytes_spilled",   F.coalesce(F.col("disk_bytes_spilled"), F.col("_t_disk_bytes_spilled")))
    .withColumn("input_bytes",          F.coalesce(F.col("input_bytes"), F.col("_t_input_bytes")))
    .withColumn("output_bytes",         F.coalesce(F.col("output_bytes"), F.col("_t_output_bytes")))
    .withColumn("shuffle_read_bytes",   F.coalesce(F.col("shuffle_read_bytes"), F.col("_t_shuffle_read_bytes")))
    .withColumn("shuffle_write_bytes",  F.coalesce(F.col("shuffle_write_bytes"), F.col("_t_shuffle_write_bytes")))
    .withColumn("shuffle_fetch_wait_ms",F.coalesce(F.col("shuffle_fetch_wait_ms"), F.col("_t_shuffle_fetch_wait_ms")))
    .withColumn("peak_exec_memory_bytes", F.coalesce(F.col("peak_exec_memory_bytes"), F.col("_t_peak_exec_memory_bytes")))
    .withColumn("deserialize_ms", F.coalesce(F.col("_t_deserialize_ms"), F.lit(0)))
    .withColumn("result_serialize_ms", F.coalesce(F.col("_t_result_serialize_ms"), F.lit(0)))
    .withColumn("result_size_mb", F.round(F.coalesce(F.col("_t_result_size_bytes"), F.lit(0)) / 1024.0 / 1024.0, 2))
    .withColumn("serialization_pct",
        F.when(F.col("executor_run_time_ms") > 0,
            F.round((F.col("deserialize_ms") + F.col("result_serialize_ms"))
                    / F.col("executor_run_time_ms") * 100, 1)
        ).otherwise(F.lit(0.0))
    )
)

_recovered = _task_stage_agg.join(
    silver_stage_df.select("cluster_id", "app_id", "stage_id").distinct(),
    on=["cluster_id", "app_id", "stage_id"], how="left_anti"
).count()
print(f"  Recovered {_recovered} stages from task data (missing StageCompleted events)")

gold_stage_df = (
    _stage_base
    .join(task_dist, on=["cluster_id", "app_id", "stage_id", "attempt_id"], how="left")
    # タスクスキュー比率 (max / p50)
    .withColumn("task_skew_ratio",
        F.when(F.col("task_p50_ms") > 0,
            F.round(F.col("task_max_ms").cast("double") / F.col("task_p50_ms"), 2)
        ).otherwise(None)
    )
    # 処理時間スキューギャップ (最大 − 中央値)
    .withColumn("time_skew_gap_ms",
        F.when(F.col("task_p50_ms").isNotNull(),
            F.col("task_max_ms") - F.col("task_p50_ms")
        ).otherwise(None)
    )
    # タスク単位データ量 MB 換算
    .withColumn("task_shuffle_min_mb",
        F.round(F.col("task_shuffle_min_bytes") / 1024**2, 2))
    .withColumn("task_shuffle_max_mb",
        F.round(F.col("task_shuffle_max_bytes") / 1024**2, 2))
    .withColumn("task_shuffle_p50_mb",
        F.round(F.col("task_shuffle_p50_bytes") / 1024**2, 2))
    # データ量スキュー比率 (max / p50)
    .withColumn("shuffle_skew_ratio",
        F.when(F.col("task_shuffle_p50_bytes") > 0,
            F.round(F.col("task_shuffle_max_bytes").cast("double") / F.col("task_shuffle_p50_bytes"), 2)
        ).otherwise(None)
    )
    # データ量スキューギャップ (最大 − 最小 per task)
    .withColumn("data_skew_gap_mb",
        F.round((F.col("task_shuffle_max_bytes") - F.col("task_shuffle_min_bytes")) / 1024**2, 2)
    )
    # データ量スキューギャップ (最大 − 中央値 per task)
    .withColumn("data_skew_gap_p50_mb",
        F.round((F.col("task_shuffle_max_bytes") - F.col("task_shuffle_p50_bytes")) / 1024**2, 2)
    )
    # MB 換算
    .withColumn("input_mb",         F.round(F.col("input_bytes")          / 1024**2, 1))
    .withColumn("output_mb",        F.round(F.col("output_bytes")         / 1024**2, 1))
    .withColumn("shuffle_read_mb",  F.round(F.col("shuffle_read_bytes")   / 1024**2, 1))
    .withColumn("shuffle_write_mb", F.round(F.col("shuffle_write_bytes")  / 1024**2, 1))
    .withColumn("disk_spill_mb",    F.round(F.col("disk_bytes_spilled")   / 1024**2, 1))
    .withColumn("memory_spill_mb",  F.round(F.col("memory_bytes_spilled") / 1024**2, 1))
    .withColumn("peak_mem_p95_mb",  F.round(F.col("peak_mem_p95_bytes")   / 1024**2, 1))
    .withColumn("has_disk_spill",   F.col("disk_bytes_spilled")   > 0)
    .withColumn("has_memory_spill", F.col("memory_bytes_spilled") > 0)
    # ─── ボトルネック分類（5S + 追加）───
    # 優先度順: FAILURE > SPILL > GC > SKEW > SHUFFLE > SMALL_FILES > SERIALIZATION > MEMORY_SPILL > MODERATE_GC
    .withColumn("bottleneck_type",
        F.when(F.col("status") == "FAILED",
            F.lit("STAGE_FAILURE"))
         .when(F.col("disk_bytes_spilled") > 0,
            F.lit("DISK_SPILL"))
         .when(F.col("gc_overhead_pct") > 20,
            F.lit("HIGH_GC"))
         .when(F.col("task_skew_ratio") > 5,
            F.lit("DATA_SKEW"))
         .when(F.col("shuffle_read_bytes") > 5 * 1024**3,
            F.lit("HEAVY_SHUFFLE"))
         .when(
            (F.col("input_bytes") > 10 * 1024**2) &
            (F.col("input_bytes") / F.greatest(F.col("num_tasks"), F.lit(1)) < 10 * 1024**2),
            F.lit("SMALL_FILES"))
         .when(F.col("memory_bytes_spilled") > 0,
            F.lit("MEMORY_SPILL"))
         .when(F.col("gc_overhead_pct") > 10,
            F.lit("MODERATE_GC"))
         .otherwise(F.lit("OK"))
    )
    .withColumn("severity",
        F.when(F.col("bottleneck_type").isin("STAGE_FAILURE", "DISK_SPILL"), F.lit("HIGH"))
         .when(F.col("bottleneck_type").isin("HIGH_GC", "DATA_SKEW", "SMALL_FILES"), F.lit("MEDIUM"))
         .when(F.col("bottleneck_type") == "OK",                             F.lit("NONE"))
         .otherwise(F.lit("LOW"))
    )
    # ─── 推奨アクション ───
    .withColumn("recommendation",
        F.when(F.col("bottleneck_type") == "STAGE_FAILURE",
            "failure_reason を確認。OOM の場合は executor memory を増加。")
         .when(F.col("bottleneck_type") == "DISK_SPILL",
            "executor memory を増加。AQE を有効化: spark.sql.adaptive.enabled=true")
         .when(F.col("bottleneck_type") == "HIGH_GC",
            "UDF・collect の見直し。オブジェクト生成を削減。G1GC チューニングを検討。")
         .when(F.col("bottleneck_type") == "DATA_SKEW",
            "AQE スキュー結合を有効化: spark.sql.adaptive.skewJoin.enabled=true または JOIN キーのサルティング。")
         .when(F.col("bottleneck_type") == "HEAVY_SHUFFLE",
            "Broadcast Join を検討 (spark.sql.autoBroadcastJoinThreshold)。shuffle partitions 数を調整。")
         .when(F.col("bottleneck_type") == "SMALL_FILES",
            "OPTIMIZE でファイルをコンパクト化。spark.databricks.delta.optimizeWrite.enabled=true, autoCompact.enabled=auto。Predictive Optimization の有効化を検討。")
         .when(F.col("bottleneck_type") == "MEMORY_SPILL",
            "spark.memory.fraction を増加。repartition() でパーティションサイズを削減。")
         .when(F.col("bottleneck_type") == "MODERATE_GC",
            "GC 傾向を監視。executor memory の増加を検討。")
         .otherwise("ボトルネックなし。")
    )
    .select(
        "cluster_id", "app_id", "stage_id", "attempt_id",
        "stage_name", "status", "failure_reason",
        "num_tasks", "task_count", "failed_tasks",
        "submission_ts", "first_task_ts", "completion_ts",
        "duration_ms", "scheduling_delay_ms",
        "executor_run_time_ms", "gc_overhead_pct", "cpu_efficiency_pct",
        "jvm_gc_time_ms",
        "input_mb", "output_mb",
        "shuffle_read_mb", "shuffle_write_mb", "shuffle_fetch_wait_ms",
        "disk_spill_mb", "memory_spill_mb",
        "has_disk_spill", "has_memory_spill",
        "peak_exec_memory_bytes",
        "task_min_ms", "task_avg_ms",
        "task_p50_ms", "task_p75_ms", "task_p95_ms", "task_p99_ms", "task_max_ms",
        "task_skew_ratio", "time_skew_gap_ms",
        "task_shuffle_min_mb", "task_shuffle_p50_mb", "task_shuffle_max_mb",
        "shuffle_skew_ratio", "data_skew_gap_mb", "data_skew_gap_p50_mb",
        "gc_p95_ms", "peak_mem_p95_mb",
        "speculative_tasks", "tasks_with_disk_spill",
        "deserialize_ms", "result_serialize_ms", "serialization_pct",
        "result_size_mb",
        "bottleneck_type", "severity", "recommendation",
    )
)

# ── Skipped ステージを追加 ─────────────────────────────────────────────────────
# SparkListenerJobStart の Stage IDs に含まれるが、StageCompleted もタスクも
# 存在しないステージは Spark が「Skipped」（シャッフル出力再利用）としたもの。
# Spark UI の Skipped Stages と一致させるために SKIPPED レコードを追加する。
_STAGE_IDS_SCHEMA_SK = ArrayType(IntegerType())
_all_declared_stages = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobStart")
    .withColumn("_sk_stage_id", F.explode(
        F.from_json(F.col("stage_ids"), _STAGE_IDS_SCHEMA_SK)
    ))
    .select(
        "cluster_id", "app_id",
        F.col("_sk_stage_id").alias("stage_id"),
        "job_id",
    )
    .dropDuplicates(["cluster_id", "app_id", "stage_id"])
)

_existing_stages = gold_stage_df.select("cluster_id", "app_id", "stage_id").distinct()

_skipped_stages = (
    _all_declared_stages
    .join(_existing_stages, on=["cluster_id", "app_id", "stage_id"], how="left_anti")
    .withColumn("attempt_id",       F.lit(0))
    .withColumn("stage_name",       F.lit("(skipped)"))
    .withColumn("status",           F.lit("SKIPPED"))
    .withColumn("failure_reason",   F.lit(None).cast("string"))
    .withColumn("num_tasks",        F.lit(0))
    .withColumn("task_count",       F.lit(0))
    .withColumn("failed_tasks",     F.lit(0))
    .withColumn("submission_ts",    F.lit(None).cast("timestamp"))
    .withColumn("first_task_ts",    F.lit(None).cast("timestamp"))
    .withColumn("completion_ts",    F.lit(None).cast("timestamp"))
    .withColumn("duration_ms",      F.lit(0).cast("long"))
    .withColumn("scheduling_delay_ms", F.lit(0).cast("long"))
    .withColumn("executor_run_time_ms", F.lit(0).cast("long"))
    .withColumn("gc_overhead_pct",  F.lit(0.0))
    .withColumn("cpu_efficiency_pct", F.lit(None).cast("double"))
    .withColumn("jvm_gc_time_ms",   F.lit(0).cast("long"))
    .withColumn("input_mb",         F.lit(0.0))
    .withColumn("output_mb",        F.lit(0.0))
    .withColumn("shuffle_read_mb",  F.lit(0.0))
    .withColumn("shuffle_write_mb", F.lit(0.0))
    .withColumn("shuffle_fetch_wait_ms", F.lit(0).cast("long"))
    .withColumn("disk_spill_mb",    F.lit(0.0))
    .withColumn("memory_spill_mb",  F.lit(0.0))
    .withColumn("has_disk_spill",   F.lit(False))
    .withColumn("has_memory_spill", F.lit(False))
    .withColumn("peak_exec_memory_bytes", F.lit(0).cast("long"))
    .withColumn("task_min_ms",      F.lit(None).cast("long"))
    .withColumn("task_avg_ms",      F.lit(None).cast("double"))
    .withColumn("task_p50_ms",      F.lit(None).cast("long"))
    .withColumn("task_p75_ms",      F.lit(None).cast("long"))
    .withColumn("task_p95_ms",      F.lit(None).cast("long"))
    .withColumn("task_p99_ms",      F.lit(None).cast("long"))
    .withColumn("task_max_ms",      F.lit(None).cast("long"))
    .withColumn("task_skew_ratio",  F.lit(None).cast("double"))
    .withColumn("time_skew_gap_ms", F.lit(None).cast("long"))
    .withColumn("task_shuffle_min_mb", F.lit(None).cast("double"))
    .withColumn("task_shuffle_p50_mb", F.lit(None).cast("double"))
    .withColumn("task_shuffle_max_mb", F.lit(None).cast("double"))
    .withColumn("shuffle_skew_ratio",  F.lit(None).cast("double"))
    .withColumn("data_skew_gap_mb",    F.lit(None).cast("double"))
    .withColumn("data_skew_gap_p50_mb",F.lit(None).cast("double"))
    .withColumn("gc_p95_ms",        F.lit(None).cast("long"))
    .withColumn("peak_mem_p95_mb",  F.lit(None).cast("double"))
    .withColumn("speculative_tasks",    F.lit(0).cast("long"))
    .withColumn("tasks_with_disk_spill",F.lit(0).cast("long"))
    .withColumn("deserialize_ms",   F.lit(0).cast("long"))
    .withColumn("result_serialize_ms", F.lit(0).cast("long"))
    .withColumn("serialization_pct", F.lit(0.0))
    .withColumn("result_size_mb",   F.lit(0.0))
    .withColumn("bottleneck_type",  F.lit("SKIPPED"))
    .withColumn("severity",         F.lit("NONE"))
    .withColumn("recommendation",   F.lit("Skipped: 前ジョブのシャッフル出力を再利用。"))
)

_skipped_count = _skipped_stages.count()
print(f"  Skipped stages: {_skipped_count}")

gold_stage_df = gold_stage_df.unionByName(
    _skipped_stages.select(gold_stage_df.columns)
)

gold_stage_df = _save(gold_stage_df, "gold_stage_performance", reread=True, merge_keys=["cluster_id", "app_id", "stage_id", "attempt_id"])

# COMMAND ----------

# DBTITLE 1,gold_executor_analysis
# ==============================================================================
# GOLD 4: gold_executor_analysis
# ==============================================================================
print("[Gold] gold_executor_analysis ...")

task_by_exec = (
    silver_task_df
    .filter(F.col("task_result") == "Success")
    .groupBy("cluster_id", "app_id", "executor_id", "host")
    .agg(
        F.count("task_id").alias("total_tasks"),
        F.sum("task_duration_ms").alias("total_task_ms"),
        F.avg("task_duration_ms").alias("avg_task_ms"),
        F.sum("gc_time_ms").alias("total_gc_ms"),
        F.avg("gc_overhead_pct").alias("avg_gc_pct"),
        F.avg("cpu_efficiency_pct").alias("avg_cpu_efficiency_pct"),
        F.sum("executor_run_time_ms").alias("total_exec_run_ms"),
        F.sum("executor_cpu_time_ns").alias("total_cpu_time_ns"),
        F.sum("input_bytes").alias("total_input_bytes"),
        F.sum("shuffle_read_bytes").alias("total_shuffle_read_bytes"),
        F.sum("shuffle_write_bytes").alias("total_shuffle_write_bytes"),
        F.sum("memory_bytes_spilled").alias("total_memory_spilled"),
        F.sum("disk_bytes_spilled").alias("total_disk_spilled"),
        F.max("peak_exec_memory_bytes").alias("peak_memory_bytes"),
        F.sum("deserialize_ms").alias("total_deserialize_ms"),
        F.sum("result_serialize_ms").alias("total_result_serialize_ms"),
        F.count(F.when(F.col("speculative"), True)).alias("speculative_tasks"),
        F.count(F.when(F.col("disk_bytes_spilled") > 0, True)).alias("tasks_with_disk_spill"),
        F.count(F.when(F.col("memory_bytes_spilled") > 0, True)).alias("tasks_with_memory_spill"),
    )
)

# アプリ内の Executor 平均・標準偏差（正規化用）
app_norm = (
    task_by_exec
    .groupBy("cluster_id", "app_id")
    .agg(
        F.avg("total_task_ms").alias("app_avg_task_ms"),
        F.stddev("total_task_ms").alias("app_stddev_task_ms"),
    )
)

exec_lifecycle = (
    silver_exec_df
    .groupBy("cluster_id", "app_id", "executor_id")
    .agg(
        F.max(F.when(F.col("event_type") == "SparkListenerExecutorAdded",
                     F.col("total_cores"))).alias("total_cores"),
        F.min(F.when(F.col("event_type") == "SparkListenerExecutorAdded",
                     F.col("timestamp_ts"))).alias("add_ts"),
        F.max(F.when(F.col("event_type") == "SparkListenerExecutorRemoved",
                     F.col("timestamp_ts"))).alias("remove_ts"),
        F.max(F.when(F.col("event_type") == "SparkListenerExecutorRemoved",
                     F.col("removed_reason"))).alias("removed_reason"),
        F.max(F.when(F.col("event_type") == "SparkListenerExecutorAdded",
                     F.col("resource_profile_id"))).alias("resource_profile_id"),
    )
)

base = (
    task_by_exec
    .join(app_norm,       on=["cluster_id", "app_id"],                 how="left")
    .join(exec_lifecycle, on=["cluster_id", "app_id", "executor_id"],  how="left")
)

gold_exec_df = (
    base
    .join(silver_rp_df,
          on=["cluster_id", "app_id", "resource_profile_id"],
          how="left"
    )
    .withColumn("load_vs_avg",
        F.when(F.col("app_avg_task_ms") > 0,
            F.round(F.col("total_task_ms") / F.col("app_avg_task_ms"), 2)
        ).otherwise(None)
    )
    .withColumn("z_score",
        F.when(F.col("app_stddev_task_ms") > 0,
            F.round(
                (F.col("total_task_ms") - F.col("app_avg_task_ms")) / F.col("app_stddev_task_ms"),
                2
            )
        ).otherwise(None)
    )
    .withColumn("is_straggler",     F.col("load_vs_avg") > 1.5)
    .withColumn("is_underutilized", F.col("load_vs_avg") < 0.5)
    .withColumn("input_gb",         F.round(F.col("total_input_bytes")        / 1024**3, 3))
    .withColumn("shuffle_read_gb",  F.round(F.col("total_shuffle_read_bytes") / 1024**3, 3))
    .withColumn("shuffle_write_gb", F.round(F.col("total_shuffle_write_bytes")/ 1024**3, 3))
    .withColumn("peak_memory_mb",   F.round(F.col("peak_memory_bytes")        / 1024**2, 1))
    .withColumn("memory_spill_mb",  F.round(F.col("total_memory_spilled")     / 1024**2, 1))
    .withColumn("disk_spill_mb",    F.round(F.col("total_disk_spilled")       / 1024**2, 1))
    .withColumn("total_gc_sec",     F.round(F.col("total_gc_ms") / 1000.0, 1))
    .withColumn("total_task_sec",   F.round(F.col("total_task_ms") / 1000.0, 1))
    .withColumn("gc_pct",
        F.when(F.col("total_task_ms") > 0,
            F.round(F.col("total_gc_ms") / F.col("total_task_ms") * 100, 1)
        ).otherwise(0.0)
    )
    .withColumn("cpu_utilization_pct",
        F.when(F.col("total_exec_run_ms") > 0,
            F.round(F.col("total_cpu_time_ns") / 1e6 / F.col("total_exec_run_ms") * 100, 1)
        ).otherwise(None)
    )
    # ── リソース診断 ──────────────────────────────────────────────────────────
    .withColumn("_diag_list", F.array(
        # メモリ不足: ディスクスピルが発生
        F.when(F.col("total_disk_spilled") > 0,
            F.concat(
                F.lit("MEMORY_PRESSURE: ディスクスピル "),
                F.round(F.col("total_disk_spilled") / 1024**2, 0).cast("string"),
                F.lit("MB ("),
                F.col("tasks_with_disk_spill").cast("string"),
                F.lit("タスク) → executor memory 増加を検討")
            )
        ),
        # メモリスピル
        F.when((F.col("total_memory_spilled") > 0) & (F.col("total_disk_spilled") == 0),
            F.concat(
                F.lit("MEMORY_SPILL: メモリスピル "),
                F.round(F.col("total_memory_spilled") / 1024**2, 0).cast("string"),
                F.lit("MB → spark.memory.fraction の調整を検討")
            )
        ),
        # GC 高負荷: タスク時間の20%以上がGC
        F.when((F.col("total_task_ms") > 0) & (F.col("total_gc_ms") / F.col("total_task_ms") > 0.2),
            F.concat(
                F.lit("HIGH_GC: GC時間 "),
                F.round(F.col("total_gc_ms") / 1000.0, 1).cast("string"),
                F.lit("秒 ("),
                F.round(F.col("total_gc_ms") / F.col("total_task_ms") * 100, 1).cast("string"),
                F.lit("%) → UDF削減・メモリ増加・G1GCチューニングを検討")
            )
        ),
        # CPU低効率: CPU利用率50%未満
        F.when((F.col("total_exec_run_ms") > 0) &
               (F.col("total_cpu_time_ns") / 1e6 / F.col("total_exec_run_ms") < 0.5),
            F.concat(
                F.lit("LOW_CPU: CPU効率 "),
                F.round(F.col("total_cpu_time_ns") / 1e6 / F.col("total_exec_run_ms") * 100, 1).cast("string"),
                F.lit("% → I/O待ち・GC・ロック競合の可能性")
            )
        ),
        # ストラグラー
        F.when(F.col("load_vs_avg") > 1.5,
            F.concat(
                F.lit("STRAGGLER: 負荷が平均の "),
                F.col("load_vs_avg").cast("string"),
                F.lit("倍 → データスキューまたはノード性能差の可能性")
            )
        ),
        # 低稼働
        F.when(F.col("load_vs_avg") < 0.5,
            F.concat(
                F.lit("UNDERUTILIZED: 負荷が平均の "),
                F.col("load_vs_avg").cast("string"),
                F.lit("倍 → ワーカー数の削減を検討")
            )
        ),
        # シリアライズオーバーヘッド
        F.when((F.col("total_task_ms") > 0) &
               ((F.col("total_deserialize_ms") + F.col("total_result_serialize_ms")) / F.col("total_task_ms") > 0.1),
            F.concat(
                F.lit("SERIALIZATION: シリアライズ時間 "),
                F.round((F.col("total_deserialize_ms") + F.col("total_result_serialize_ms")) / 1000.0, 1).cast("string"),
                F.lit("秒 ("),
                F.round((F.col("total_deserialize_ms") + F.col("total_result_serialize_ms")) / F.col("total_task_ms") * 100, 1).cast("string"),
                F.lit("%) → UDF・RDD使用の削減を検討")
            )
        ),
    ))
    # NULL を除去して結合
    .withColumn("resource_diagnosis",
        F.concat_ws(
            "\n",
            F.filter(F.col("_diag_list"), lambda x: x.isNotNull())
        )
    )
    .withColumn("resource_diagnosis",
        F.when(F.col("resource_diagnosis") == "", F.lit("OK: リソース問題なし"))
         .otherwise(F.col("resource_diagnosis"))
    )
    .withColumn("has_resource_issue",
        F.when(F.col("resource_diagnosis") == "OK: リソース問題なし", F.lit("NO"))
         .otherwise(F.lit("YES"))
    )
    .withColumn("serialization_pct",
        F.when(F.col("total_task_ms") > 0,
            F.round((F.col("total_deserialize_ms") + F.col("total_result_serialize_ms"))
                    / F.col("total_task_ms") * 100, 1)
        ).otherwise(F.lit(0.0))
    )
    .drop("_diag_list")
    .select(
        "cluster_id", "app_id", "executor_id", "host",
        "total_cores", "add_ts", "remove_ts", "removed_reason",
        "resource_profile_id", "onheap_memory_mb", "offheap_memory_mb", "task_cpus",
        "total_tasks", "total_task_ms", "total_task_sec", "avg_task_ms",
        "total_gc_ms", "total_gc_sec", "gc_pct", "avg_gc_pct", "avg_cpu_efficiency_pct",
        "cpu_utilization_pct",
        "input_gb", "shuffle_read_gb", "shuffle_write_gb",
        "total_memory_spilled", "total_disk_spilled",
        "memory_spill_mb", "disk_spill_mb",
        "peak_memory_mb", "speculative_tasks",
        "tasks_with_disk_spill", "tasks_with_memory_spill",
        "app_avg_task_ms", "load_vs_avg", "z_score",
        "is_straggler", "is_underutilized",
        "has_resource_issue", "resource_diagnosis",
        "total_deserialize_ms", "total_result_serialize_ms", "serialization_pct",
    )
)

gold_exec_df = _save(gold_exec_df, "gold_executor_analysis", merge_keys=["cluster_id", "app_id", "executor_id"])

# COMMAND ----------

# DBTITLE 1,gold_spot_instance_analysis
# ==============================================================================
# GOLD 4b: gold_spot_instance_analysis
#
# Spot インスタンスのロスト（予期しない Executor 削除）を検知し、
# 影響範囲と推奨設定を生成する。
#
# 検知ロジック:
#   - SparkListenerExecutorRemoved の removed_reason を解析
#   - "lost", "LostExecutor", "Decommission", "spot" 等のキーワードで分類
#   - タスクの再実行やステージの再試行との相関を分析
# ==============================================================================
print("[Gold] gold_spot_instance_analysis ...")

# Executor のライフサイクル分析
exec_added = (
    silver_exec_df
    .filter(F.col("event_type") == "SparkListenerExecutorAdded")
    .select(
        "cluster_id", "app_id", "executor_id", "host",
        F.col("timestamp_ts").alias("added_ts"),
        F.col("timestamp_ms").alias("added_ms"),
        "total_cores",
    )
)

exec_removed = (
    silver_exec_df
    .filter(F.col("event_type") == "SparkListenerExecutorRemoved")
    .select(
        "cluster_id", "app_id", "executor_id",
        F.col("timestamp_ts").alias("removed_ts"),
        F.col("timestamp_ms").alias("removed_ms"),
        "removed_reason",
    )
)

exec_lifecycle_full = (
    exec_added
    .join(exec_removed, on=["cluster_id", "app_id", "executor_id"], how="left")
    .withColumn("lifetime_sec",
        F.when(F.col("removed_ms").isNotNull(),
            (F.col("removed_ms") - F.col("added_ms")) / 1000.0
        )
    )
    # removed_reason を解析して分類
    .withColumn("_reason_lower", F.lower(F.coalesce(F.col("removed_reason"), F.lit(""))))
    .withColumn("removal_type",
        F.when(
            F.col("_reason_lower").rlike("lost|losttworker|lostexecutor|heartbeat"),
            F.lit("NODE_LOST")
        )
        .when(
            F.col("_reason_lower").rlike("spot|preempt|evict"),
            F.lit("SPOT_PREEMPTION")
        )
        .when(
            F.col("_reason_lower").rlike("decommission"),
            F.lit("DECOMMISSIONED")
        )
        .when(
            F.col("_reason_lower").rlike("cluster termination|shutdown"),
            F.lit("CLUSTER_SHUTDOWN")
        )
        .when(
            F.col("_reason_lower").rlike("idle|unused"),
            F.lit("IDLE_TIMEOUT")
        )
        .when(F.col("removed_reason").isNotNull(), F.lit("OTHER"))
        .otherwise(F.lit("STILL_RUNNING"))
    )
    .withColumn("is_unexpected_loss",
        F.col("removal_type").isin("NODE_LOST", "SPOT_PREEMPTION")
    )
    .drop("_reason_lower")
)

# Executor ごとのタスク統計（失敗タスク・タスク実行時間）
tasks_per_exec = (
    silver_task_df
    .groupBy("cluster_id", "app_id", "executor_id")
    .agg(
        F.count("task_id").alias("total_tasks_assigned"),
        F.sum(F.when(F.col("task_result") != "Success", 1).otherwise(0)).alias("failed_tasks"),
        F.count(F.when(F.col("speculative"), True)).alias("speculative_tasks"),
        # 失敗タスクの合計実行時間（再実行が必要な時間の推定）
        F.sum(F.when(F.col("task_result") != "Success", F.col("task_duration_ms")).otherwise(0))
            .alias("failed_task_duration_ms"),
        # このExecutorのタスク平均実行時間
        F.avg("task_duration_ms").alias("avg_task_duration_ms"),
        # このExecutorが保持していたシャッフル出力量
        F.sum("shuffle_write_bytes").alias("total_shuffle_write_bytes"),
    )
)

# アプリ全体のタスク平均実行時間（Executor 再取得後の再実行時間推定用）
app_task_avg = (
    silver_task_df
    .filter(F.col("task_result") == "Success")
    .groupBy("cluster_id", "app_id")
    .agg(
        F.avg("task_duration_ms").alias("app_avg_task_ms"),
        # シャッフル再計算の速度推定（MB/sec）
        F.sum("shuffle_write_bytes").alias("app_total_shuffle_write"),
        F.sum("executor_run_time_ms").alias("app_total_run_ms"),
    )
    .withColumn("shuffle_write_rate_mb_per_sec",
        F.when(F.col("app_total_run_ms") > 0,
            (F.col("app_total_shuffle_write") / 1024**2) / (F.col("app_total_run_ms") / 1000.0)
        ).otherwise(F.lit(1.0))
    )
)

# Executor イベントからノード再取得時間を推定
# （同一 app 内で NODES_LOST 後に次の ExecutorAdded までの時間）
# Step 1: Window で前回の追加時刻を取得し、差分を計算
_exec_add_with_gap = (
    silver_exec_df
    .filter(F.col("event_type") == "SparkListenerExecutorAdded")
    .withColumn("_prev_add_ts",
        F.lag(F.unix_timestamp("timestamp_ts"))
         .over(Window.partitionBy("cluster_id", "app_id").orderBy("timestamp_ts"))
    )
    .withColumn("_gap_sec",
        F.unix_timestamp("timestamp_ts") - F.col("_prev_add_ts")
    )
    .filter(F.col("_gap_sec").isNotNull())
)
# Step 2: 平均を集約
_exec_add_times = (
    _exec_add_with_gap
    .groupBy("cluster_id", "app_id")
    .agg(
        F.round(F.avg("_gap_sec"), 1).alias("avg_exec_add_interval_sec"),
    )
)

_app_dur = (
    spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}gold_application_summary")
    .select("cluster_id", "app_id", (F.col("duration_ms") / 1000.0).alias("app_duration_sec"))
)

gold_spot_df = (
    exec_lifecycle_full
    .join(tasks_per_exec, on=["cluster_id", "app_id", "executor_id"], how="left")
    .join(app_task_avg,    on=["cluster_id", "app_id"],                how="left")
    .join(_exec_add_times, on=["cluster_id", "app_id"],                how="left")
    .join(_app_dur,        on=["cluster_id", "app_id"],                how="left")
    .withColumn("lifetime_min", F.round(F.col("lifetime_sec") / 60.0, 1))
    # ─── 遅延予想時間の計算 ───
    # (1) タスク再実行遅延: 失敗タスク数 × アプリ平均タスク時間
    .withColumn("_task_retry_delay_ms",
        F.when(F.col("is_unexpected_loss"),
            F.coalesce(F.col("failed_tasks"), F.lit(0)) * F.col("app_avg_task_ms")
        ).otherwise(F.lit(0))
    )
    # (2) シャッフル再計算遅延:
    # ロストした Shuffle データは残りの Executor で並列に再計算される。
    # 推定: 消失データ量 / (アプリ全体の Shuffle スループット × 並列度)
    # アプリ全体の Shuffle スループット = 全 Shuffle Write / アプリ実行時間（壁時計時間）
    # ただし上限はアプリ実行時間の 10%（Spot ロスト1回の影響はアプリ全体の一部）
    # アプリ全体の Shuffle スループット = 全 Shuffle Write / アプリ壁時計時間
    .withColumn("_app_dur_sec",
        F.coalesce(F.col("app_duration_sec"), F.lit(600)))  # fallback 10分
    .withColumn("_app_shuffle_throughput_mb_per_sec",
        F.when(F.col("_app_dur_sec") > 0,
            (F.col("app_total_shuffle_write") / 1024**2) / F.col("_app_dur_sec")
        ).otherwise(F.lit(100.0))
    )
    .withColumn("_shuffle_recompute_delay_ms",
        F.when(
            F.col("is_unexpected_loss") & (F.col("_app_shuffle_throughput_mb_per_sec") > 0),
            F.least(
                # 消失データ / アプリ全体スループット
                (F.col("total_shuffle_write_bytes") / 1024**2)
                / F.col("_app_shuffle_throughput_mb_per_sec")
                * 1000,
                # 上限: アプリ実行時間の 10%
                F.col("_app_dur_sec") * 100
            )
        ).otherwise(F.lit(0))
    )
    # (3) Executor 再取得遅延: ノード起動待ち時間（平均 Executor 追加間隔で近似）
    .withColumn("_exec_acquire_delay_ms",
        F.when(F.col("is_unexpected_loss"),
            F.coalesce(F.col("avg_exec_add_interval_sec"), F.lit(60)) * 1000  # デフォルト60秒
        ).otherwise(F.lit(0))
    )
    # 合計遅延予想
    .withColumn("estimated_delay_ms",
        F.when(F.col("is_unexpected_loss"),
            F.col("_task_retry_delay_ms")
            + F.col("_shuffle_recompute_delay_ms")
            + F.col("_exec_acquire_delay_ms")
        ).otherwise(F.lit(None))
    )
    .withColumn("estimated_delay_sec", F.round(F.col("estimated_delay_ms") / 1000.0, 1))
    .withColumn("estimated_delay_min", F.round(F.col("estimated_delay_ms") / 60000.0, 1))
    # 遅延内訳
    .withColumn("delay_breakdown",
        F.when(F.col("is_unexpected_loss"),
            F.concat(
                F.lit("タスク再実行: "), F.round(F.col("_task_retry_delay_ms") / 1000, 1).cast("string"), F.lit("秒"),
                F.lit(" + シャッフル再計算: "), F.round(F.col("_shuffle_recompute_delay_ms") / 1000, 1).cast("string"), F.lit("秒"),
                F.lit(" + Executor取得: "), F.round(F.col("_exec_acquire_delay_ms") / 1000, 1).cast("string"), F.lit("秒"),
            )
        )
    )
    .withColumn("shuffle_lost_mb",
        F.when(F.col("is_unexpected_loss"),
            F.round(F.col("total_shuffle_write_bytes") / 1024**2, 1)
        )
    )
    # 推奨設定
    .withColumn("recommendation",
        F.when(F.col("removal_type") == "NODE_LOST",
            F.lit(
                "Spot ノードロストを検知。以下の設定を推奨:\n"
                "  1. spark.decommission.enabled=true (Graceful Decommission 有効化)\n"
                "  2. spark.storage.decommission.enabled=true (シャッフルデータの事前退避)\n"
                "  3. spark.storage.decommission.shuffleBlocks.enabled=true\n"
                "  4. spark.storage.decommission.rddBlocks.enabled=true\n"
                "  5. spark.decommission.graceful.timeout=120s\n"
                "  6. spark.speculation=true (投機実行で遅延タスクをカバー)\n"
                "  7. spark.speculation.multiplier=1.5\n"
                "  8. クラスタ設定: Spot/On-Demand 混在構成を検討\n"
                "     (Driver は On-Demand、Worker は Spot + フォールバック On-Demand)"
            )
        )
        .when(F.col("removal_type") == "SPOT_PREEMPTION",
            F.lit(
                "Spot プリエンプションを検知。以下の設定を推奨:\n"
                "  1. spark.decommission.enabled=true\n"
                "  2. spark.storage.decommission.enabled=true\n"
                "  3. spark.storage.decommission.shuffleBlocks.enabled=true\n"
                "  4. クラスタポリシー: 'First On-Demand, then Spot' を設定\n"
                "  5. AWS: Spot の capacity-optimized 割り当て戦略を使用\n"
                "  6. 複数のインスタンスタイプを指定して中断リスクを分散"
            )
        )
        .when(F.col("removal_type") == "DECOMMISSIONED",
            F.lit("Graceful Decommission が動作。シャッフルデータが退避されている可能性あり。")
        )
        .otherwise(F.lit(None))
    )
    .select(
        "cluster_id", "app_id", "executor_id", "host",
        "total_cores", "added_ts", "removed_ts",
        "lifetime_sec", "lifetime_min",
        "removed_reason", "removal_type", "is_unexpected_loss",
        "total_tasks_assigned", "failed_tasks", "speculative_tasks",
        "shuffle_lost_mb",
        "estimated_delay_sec", "estimated_delay_min", "delay_breakdown",
        "recommendation",
    )
)

# Deduplicate: keep latest removal per executor (handles executor ID reuse)
gold_spot_df = gold_spot_df.dropDuplicates(["cluster_id", "app_id", "executor_id"])

gold_spot_df = _save(gold_spot_df, "gold_spot_instance_analysis", reread=True, merge_keys=["cluster_id", "app_id", "executor_id"])

# サマリー表示
_spot_summary = (
    gold_spot_df
    .groupBy("cluster_id", "app_id", "removal_type")
    .agg(
        F.count("executor_id").alias("executor_count"),
        F.round(F.avg("lifetime_min"), 1).alias("avg_lifetime_min"),
        F.sum("failed_tasks").alias("total_failed_tasks"),
        F.round(F.sum("estimated_delay_sec"), 1).alias("total_estimated_delay_sec"),
        F.round(F.sum("shuffle_lost_mb"), 0).alias("total_shuffle_lost_mb"),
    )
    .orderBy("app_id", "removal_type")
)
print("\n[Spot Analysis] Executor 削除タイプ別サマリー:")
_spot_summary.show(truncate=False)

_unexpected_df = gold_spot_df.filter("is_unexpected_loss = true")
_unexpected = _unexpected_df.count()
if _unexpected > 0:
    _total_delay = _unexpected_df.agg(F.sum("estimated_delay_sec")).collect()[0][0] or 0
    _total_shuffle = _unexpected_df.agg(F.sum("shuffle_lost_mb")).collect()[0][0] or 0
    print(f"⚠ 予期しない Executor ロスト (NODE_LOST / SPOT_PREEMPTION): {_unexpected} 件")
    print(f"  推定遅延合計    : {_total_delay:.1f} 秒 ({_total_delay/60:.1f} 分)")
    print(f"  消失Shuffle合計 : {_total_shuffle:.0f} MB")
    print("  → gold_spot_instance_analysis テーブルで詳細と推奨設定を確認してください")
else:
    print("✅ 予期しない Executor ロストは検出されませんでした")

# COMMAND ----------

# DBTITLE 1,gold_bottleneck_report
# ==============================================================================
# GOLD 5: gold_bottleneck_report
# ==============================================================================
print("[Gold] gold_bottleneck_report ...")

_STAGE_IDS_SCHEMA = ArrayType(IntegerType())

job_stage_map = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobStart")
    .withColumn("stage_id", F.explode(
        F.from_json(F.col("stage_ids"), _STAGE_IDS_SCHEMA)
    ))
    .select("cluster_id", "app_id", "job_id", "stage_id")
)

gold_bn_df = (
    gold_stage_df
    .filter(~F.col("bottleneck_type").isin("OK", "SKIPPED"))
    .join(job_stage_map, on=["cluster_id", "app_id", "stage_id"], how="left")
    .withColumn("severity_order",
        F.when(F.col("severity") == "HIGH",   F.lit(1))
         .when(F.col("severity") == "MEDIUM",  F.lit(2))
         .otherwise(F.lit(3))
    )
    .select(
        "cluster_id", "app_id", "job_id",
        "stage_id", "stage_name", "status",
        "severity", "bottleneck_type",
        "duration_ms", "num_tasks",
        "task_skew_ratio", "gc_overhead_pct",
        "disk_spill_mb", "memory_spill_mb",
        "shuffle_read_mb",
        "task_p95_ms", "task_p99_ms",
        "recommendation", "failure_reason",
    )
    .orderBy("severity_order", F.col("duration_ms").desc())
)

gold_bn_df = _save(gold_bn_df, "gold_bottleneck_report", reread=True, merge_keys=["cluster_id", "app_id", "job_id", "stage_id"])

# COMMAND ----------

# DBTITLE 1,gold_job_detail (ダッシュボード用事前結合)
# ==============================================================================
# GOLD 5b: gold_job_detail
# gold_job_performance + タスク成功/失敗数 + gold_bottleneck_report を事前結合。
# gold_bottleneck_report の後に配置し、正確なボトルネック判定を使用する。
# ==============================================================================
print("[Gold] gold_job_detail ...")

# テーブルから再読み込み（ambiguous self-join 回避）
_gold_job_fresh = spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}gold_job_performance")

# job → stage マッピング
_STAGE_IDS_SCHEMA_JD = ArrayType(IntegerType())
_job_stage_map_jd = (
    _gold_job_fresh
    .withColumn("_stage_id", F.explode(F.from_json(F.col("stage_ids"), _STAGE_IDS_SCHEMA_JD)))
    .select("cluster_id", "app_id", "job_id", "_stage_id")
)

# タスク成功/失敗数をジョブ単位で集計
_tk = silver_task_df.alias("tk")
_jm = _job_stage_map_jd.alias("jm")
_task_counts = (
    _tk
    .join(_jm,
          (F.col("tk.cluster_id") == F.col("jm.cluster_id")) &
          (F.col("tk.app_id") == F.col("jm.app_id")) &
          (F.col("tk.stage_id") == F.col("jm._stage_id")),
          "inner")
    .groupBy(F.col("jm.cluster_id").alias("cluster_id"),
             F.col("jm.app_id").alias("app_id"),
             F.col("jm.job_id").alias("job_id"))
    .agg(
        F.count(F.col("tk.task_id")).alias("total_tasks_all"),
        F.sum(F.when(F.col("tk.task_result") == "Success", 1).otherwise(0)).alias("succeeded_tasks"),
        F.sum(F.when(F.col("tk.task_result") != "Success", 1).otherwise(0)).alias("failed_tasks"),
    )
)

# ボトルネック情報をジョブ単位で集約（gold_bottleneck_report から取得 — 正確な判定）
_bn_by_job = (
    gold_bn_df
    .groupBy("cluster_id", "app_id", "job_id")
    .agg(
        F.count("*").alias("bottleneck_count"),
        F.concat_ws("; ", F.collect_set(
            F.concat(F.col("bottleneck_type"), F.lit("(S"), F.col("stage_id").cast("string"), F.lit(")"))
        )).alias("bottleneck_summary"),
    )
)

gold_job_detail_df = (
    _gold_job_fresh
    .join(_task_counts, on=["cluster_id", "app_id", "job_id"], how="left")
    .join(_bn_by_job, on=["cluster_id", "app_id", "job_id"], how="left")
    .withColumn("task_status",
        F.concat(
            F.coalesce(F.col("succeeded_tasks").cast("string"), F.lit("?")),
            F.lit("/"),
            F.coalesce(F.col("total_tasks_all").cast("string"), F.lit("?")),
            F.when(F.col("failed_tasks") > 0,
                F.concat(F.lit(" ("), F.col("failed_tasks").cast("string"), F.lit(" failed)"))
            ).otherwise(F.lit(""))
        )
    )
    .withColumn("has_bottleneck",
        F.when(F.col("bottleneck_count") > 0, F.lit("YES")).otherwise(F.lit("NO"))
    )
    .withColumn("bottleneck_summary", F.coalesce(F.col("bottleneck_summary"), F.lit("")))
    .select(
        "cluster_id", "app_id", "job_id", "status",
        "submit_ts", "complete_ts", "duration_ms",
        F.round(F.col("duration_ms") / 1000.0, 2).alias("duration_sec"),
        "duration_min", "job_result", "stage_ids", "sql_execution_id",
        "total_tasks_all", "succeeded_tasks", "failed_tasks", "task_status",
        "has_bottleneck", "bottleneck_summary",
    )
)

gold_job_detail_df = _save(gold_job_detail_df, "gold_job_detail", merge_keys=["cluster_id", "app_id", "job_id"])

# COMMAND ----------

# DBTITLE 1,gold_job_concurrency
# ==============================================================================
# GOLD 6: gold_job_concurrency
# ==============================================================================
print("[Gold] gold_job_concurrency ...")

jobs = spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}gold_job_performance")

# 自己結合: 全カラムに j2_ プレフィックスを付けた別 DataFrame
j2 = jobs.toDF(*[f"j2_{c}" for c in jobs.columns])

concurrency = (
    jobs
    .filter(F.col("submit_ts").isNotNull())
    .join(
        j2.filter(F.col("j2_submit_ts").isNotNull()),
        on=(
            (F.col("cluster_id")   == F.col("j2_cluster_id")) &
            (F.col("app_id")       == F.col("j2_app_id")) &
            (F.col("job_id")       != F.col("j2_job_id")) &
            (F.col("j2_submit_ts")   <= F.col("submit_ts")) &
            (
                F.col("j2_complete_ts").isNull() |
                (F.col("j2_complete_ts") >= F.col("submit_ts"))
            )
        ),
        how="left"
    )
    .groupBy(
        "cluster_id", "app_id", "job_id",
        "status", "job_result",
        "submit_ts", "complete_ts",
        "duration_ms", "duration_min",
    )
    .agg(F.count("j2_job_id").alias("concurrent_jobs_at_start"))
)

# ── ジョブ単位の CPU 指標 ──
_STAGE_IDS_SCHEMA = ArrayType(IntegerType())
job_stage_map_2 = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobStart")
    .withColumn("stage_id", F.explode(
        F.from_json(F.col("stage_ids"), _STAGE_IDS_SCHEMA)
    ))
    .select("cluster_id", "app_id", "job_id", "stage_id")
)

task_cpu = (
    silver_task_df
    .filter(F.col("task_result") == "Success")
    .join(job_stage_map_2, on=["cluster_id", "app_id", "stage_id"], how="inner")
    .groupBy("cluster_id", "app_id", "job_id")
    .agg(
        F.count("task_id").alias("job_total_tasks"),
        F.round(F.sum("executor_cpu_time_ns") / 1e9,    2).alias("total_cpu_time_sec"),
        F.round(F.sum("executor_run_time_ms") / 1000.0, 1).alias("total_exec_run_time_sec"),
        F.round(
            F.sum("executor_cpu_time_ns") / 1e6
            / F.greatest(F.sum("executor_run_time_ms"), F.lit(1)) * 100,
            1
        ).alias("job_cpu_efficiency_pct"),
        F.round(F.avg("executor_cpu_time_ns") / 1e6, 1).alias("avg_task_cpu_time_ms"),
        F.round(F.sum("gc_time_ms") / 1000.0, 1).alias("total_gc_time_sec"),
    )
)

gold_jc_df = (
    concurrency
    .join(task_cpu, on=["cluster_id", "app_id", "job_id"], how="left")
    .withColumn("job_id_str",   F.col("job_id").cast("string"))
    .withColumn("duration_sec", F.round(F.col("duration_ms") / 1000.0, 1))
    .select(
        "cluster_id", "app_id", "job_id", "job_id_str",
        "status", "job_result",
        "submit_ts", "complete_ts",
        "duration_ms", "duration_sec", "duration_min",
        "concurrent_jobs_at_start",
        "job_total_tasks",
        "total_cpu_time_sec",
        "total_exec_run_time_sec",
        "job_cpu_efficiency_pct",
        "avg_task_cpu_time_ms",
        "total_gc_time_sec",
    )
)

gold_jc_df = _save(gold_jc_df, "gold_job_concurrency", merge_keys=["cluster_id", "app_id", "job_id"])

# ── クロス app_id 並列実行検出 ──────────────────────────────────────────────
# 同一クラスタ上で、分析対象 app_id の実行期間中に他の app_id のジョブが
# 走っていたかを検出する。別セッションのワークロードによる CPU・メモリ・
# I/O リソース競合の有無を判定する。
print("[Gold] gold_cross_app_concurrency ...")

# 全 app_id のジョブ（Start + End を結合して時間範囲を取得）
_all_job_start = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobStart")
    .select("cluster_id", "app_id", "job_id",
            F.col("submission_ts").alias("submit_ts"),
            F.col("submission_ms").alias("submit_ms"))
)
_all_job_end = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobEnd")
    .select("app_id", "job_id",
            F.col("completion_ts").alias("complete_ts"),
            F.col("completion_ms").alias("complete_ms"))
)
_all_jobs = (
    _all_job_start
    .join(_all_job_end, on=["app_id", "job_id"], how="left")
)

# 分析対象 app のジョブごとに、他の app_id で同時実行中だったジョブ数をカウント
_other_jobs = _all_jobs.toDF(*[f"o_{c}" for c in _all_jobs.columns])
_target = spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}gold_job_performance").alias("t")

_cross_concurrency = (
    _target
    .filter(F.col("t.submit_ts").isNotNull())
    .join(
        _other_jobs.filter(F.col("o_submit_ts").isNotNull()),
        on=(
            (F.col("t.cluster_id")   == F.col("o_cluster_id")) &
            (F.col("t.app_id")       != F.col("o_app_id")) &
            (F.col("o_submit_ts")    <= F.coalesce(F.col("t.complete_ts"), F.current_timestamp())) &
            (
                F.col("o_complete_ts").isNull() |
                (F.col("o_complete_ts") >= F.col("t.submit_ts"))
            )
        ),
        how="left"
    )
    .groupBy(
        F.col("t.cluster_id").alias("cluster_id"),
        F.col("t.app_id").alias("app_id"),
        F.col("t.job_id").alias("job_id"),
        F.col("t.submit_ts").alias("submit_ts"),
        F.col("t.complete_ts").alias("complete_ts"),
        F.col("t.duration_ms").alias("duration_ms"),
    )
    .agg(
        F.count("o_job_id").alias("cross_app_concurrent_jobs"),
        F.collect_set("o_app_id").alias("concurrent_app_ids"),
    )
    .withColumn("concurrent_app_list",
        F.array_join(F.col("concurrent_app_ids"), ", ")
    )
    .withColumn("has_cross_app_concurrency",
        F.when(F.col("cross_app_concurrent_jobs") > 0, F.lit("YES")).otherwise(F.lit("NO"))
    )
    .select(
        "cluster_id", "app_id", "job_id",
        "submit_ts", "complete_ts", "duration_ms",
        "cross_app_concurrent_jobs", "concurrent_app_list",
        "has_cross_app_concurrency",
    )
)

_cross_concurrency = _save(_cross_concurrency, "gold_cross_app_concurrency", merge_keys=["cluster_id", "app_id", "job_id"])

# サマリー表示
_cross_summary = _cross_concurrency.filter("has_cross_app_concurrency = 'YES'")
_cross_count = _cross_summary.count()
if _cross_count > 0:
    _cross_apps = _cross_concurrency.select(F.explode(F.split("concurrent_app_list", ", "))).distinct().count()
    print(f"⚠ クロスアプリ並列実行検出: {_cross_count} ジョブが他の {_cross_apps} アプリと重複")
else:
    print("✅ 他のアプリケーションとの並列実行は検出されませんでした")

# COMMAND ----------

# DBTITLE 1,gold_spark_config_analysis
# ==============================================================================
# GOLD: gold_spark_config_analysis
# ==============================================================================
print("[Gold] gold_spark_config_analysis ...")

# パフォーマンスチューニングに関連する主要パラメータとデフォルト値
# Spark Properties に存在 = 明示的に設定済み、存在しない = デフォルトのまま
_PERF_CONFIG_DEFAULTS = {
    # ── AQE ──
    "spark.sql.adaptive.enabled":                          ("true",      "AQE",           "Adaptive Query Execution を有効化"),
    "spark.sql.adaptive.coalescePartitions.enabled":       ("true",      "AQE",           "AQE パーティション結合"),
    "spark.sql.adaptive.skewJoin.enabled":                 ("true",      "AQE",           "AQE スキュー結合"),
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor":   ("5",         "AQE",           "スキュー判定倍率"),
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": ("256m", "AQE",        "スキュー判定閾値"),
    "spark.sql.adaptive.advisoryPartitionSizeInBytes":     ("64m",       "AQE",           "AQE 推奨パーティションサイズ"),
    # ── Shuffle / パーティション ──
    "spark.sql.shuffle.partitions":                        ("200",       "Shuffle",       "Shuffle パーティション数"),
    "spark.sql.files.maxPartitionBytes":                   ("128m",      "I/O",           "ファイル読み込み最大パーティションサイズ"),
    "spark.sql.files.openCostInBytes":                     ("4194304",   "I/O",           "ファイルオープンコスト（小ファイル結合閾値）"),
    "spark.sql.autoBroadcastJoinThreshold":                ("10485760",  "Join",          "Broadcast Join 自動適用閾値"),
    # ── Photon ──
    "spark.databricks.photon.enabled":                     ("true",      "Photon",        "Photon エンジン有効化"),
    # ── メモリ ──
    "spark.memory.fraction":                               ("0.6",       "Memory",        "Spark 実行/ストレージメモリ比率"),
    "spark.memory.storageFraction":                        ("0.5",       "Memory",        "ストレージメモリの予約比率"),
    "spark.memory.offHeap.enabled":                        ("false",     "Off-Heap",      "Off-Heap メモリ有効化"),
    "spark.memory.offHeap.size":                           ("0",         "Off-Heap",      "Off-Heap メモリサイズ"),
    "spark.executor.memoryOverhead":                       ("auto",      "Memory",        "Executor メモリオーバーヘッド"),
    "spark.executor.memoryOverheadFactor":                 ("0.1",       "Memory",        "Executor メモリオーバーヘッド比率"),
    # ── Executor / Driver ──
    "spark.executor.memory":                               ("auto",      "Executor",      "Executor メモリ"),
    "spark.executor.cores":                                ("auto",      "Executor",      "Executor コア数"),
    "spark.driver.memory":                                 ("auto",      "Driver",        "Driver メモリ"),
    "spark.driver.maxResultSize":                          ("1g",        "Driver",        "Driver 最大結果サイズ"),
    # ── Speculation ──
    "spark.speculation":                                   ("false",     "Speculation",   "投機的実行（遅いタスクの再実行）"),
    "spark.speculation.multiplier":                        ("1.5",       "Speculation",   "投機的実行のタスク遅延倍率"),
    "spark.speculation.quantile":                          ("0.75",      "Speculation",   "投機的実行の発動閾値（完了率）"),
    # ── Decommission（Spot 耐性）──
    "spark.decommission.enabled":                          ("false",     "Decommission",  "Executor デコミッション有効化"),
    "spark.storage.decommission.enabled":                  ("false",     "Decommission",  "ストレージデコミッション有効化"),
    "spark.storage.decommission.shuffleBlocks.enabled":    ("false",     "Decommission",  "Shuffle ブロックのデコミッション移行"),
    "spark.storage.decommission.rddBlocks.enabled":       ("false",     "Decommission",  "RDD ブロックのデコミッション移行"),
    "spark.storage.decommission.fallbackStorage.path":    ("",          "Decommission",  "デコミッション時のフォールバックストレージパス"),
    # ── Shuffle Service ──
    "spark.shuffle.service.enabled":                       ("false",     "Shuffle",       "External Shuffle Service 有効化"),
    "spark.shuffle.memoryFraction":                        ("0.2",       "Shuffle",       "Shuffle メモリ比率"),
    "spark.storage.memoryFraction":                        ("0.6",       "Memory",        "ストレージメモリ比率"),
    # ── Locality ──
    "spark.locality.wait":                                 ("3s",        "Scheduler",     "データローカリティ待機時間"),
}

# silver_spark_config から全アプリの設定を取得し、デフォルト値と比較
_config_ref = spark.createDataFrame(
    [(k, v[0], v[1], v[2]) for k, v in _PERF_CONFIG_DEFAULTS.items()],
    ["config_key", "default_value", "category", "description"]
)

# アプリごとの設定値（存在するもの = 明示設定済み）
_app_configs = (
    spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}silver_spark_config")
    .select("cluster_id", "app_id", "config_key", "config_value")
)

# FULL OUTER JOIN: デフォルト一覧と実際の設定をアプリごとに突合
# まず全アプリ × 全デフォルトキーのクロス結合を作成
_all_apps = _app_configs.select("cluster_id", "app_id").distinct()
_all_combos = _all_apps.crossJoin(_config_ref)

gold_config_df = (
    _all_combos
    .join(
        _app_configs,
        on=["cluster_id", "app_id", "config_key"],
        how="left"
    )
    .withColumn("is_set", F.when(F.col("config_value").isNotNull(), F.lit("YES")).otherwise(F.lit("NO")))
    .withColumn("actual_value", F.coalesce(F.col("config_value"), F.col("default_value")))
    .withColumn("is_changed",
        F.when(
            (F.col("config_value").isNotNull()) &
            (F.col("default_value") != "auto") &
            (F.col("config_value") != F.col("default_value")),
            F.lit("YES")
        ).when(
            (F.col("config_value").isNotNull()) &
            (F.col("default_value") == "auto"),
            F.lit("SET")  # auto デフォルトの場合は "SET" で明示設定を示す
        ).otherwise(F.lit("NO"))
    )
    .select(
        "cluster_id", "app_id", "category", "config_key", "description",
        "default_value", "actual_value", "is_set", "is_changed",
    )
)

gold_config_df = _save(gold_config_df, "gold_spark_config_analysis", reread=True, merge_keys=["cluster_id", "app_id", "config_key"])

# サマリー表示
_changed = gold_config_df.filter("is_changed IN ('YES', 'SET')").select("config_key", "actual_value", "default_value", "is_changed").distinct()
_changed_count = _changed.count()
if _changed_count > 0:
    print(f"⚠ デフォルトから変更/明示設定された設定: {_changed_count} 件")
    for row in _changed.orderBy("config_key").collect():
        tag = "変更" if row["is_changed"] == "YES" else "明示設定"
        print(f"  [{tag}] {row['config_key']}: {row['actual_value']} (default: {row['default_value']})")
else:
    print("✅ パフォーマンス関連設定はすべてデフォルト値です")

# COMMAND ----------

# DBTITLE 1,silver_sql_scan_metrics (Scan ノード accumulator 突合)
# ==============================================================================
# Scan ノードのキャッシュヒット率・フィルタレート等を sparkPlanInfo + DriverAccumUpdates から取得
# ==============================================================================
print("[Gold] sql_scan_metrics ...")

# --- Step 1: sparkPlanInfo から Scan ノードの accumulator ID を再帰的に抽出 ---
# PySpark UDF で sparkPlanInfo JSON を再帰パースし、Scan ノードのメトリクスを抽出する
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType

_scan_metrics_schema = ArrayType(StructType([
    StructField("scan_node", StringType()),
    StructField("metric_name", StringType()),
    StructField("acc_id", LongType()),
]))

@F.udf(_scan_metrics_schema)
def _extract_scan_acc_ids(plan_json):
    """sparkPlanInfo JSON を再帰的に辿り、Scan ノードのメトリクスを抽出"""
    if not plan_json:
        return []
    import json as _json
    try:
        plan = _json.loads(plan_json)
    except Exception:
        return []
    results = []
    _target_metrics = {
        "cache hits size", "cache misses size",
        "cache writes size",
        "number of output rows", "number of files read", "number of files pruned",
        "number of bytes pruned", "size of files read",
        "filesystem read data size", "filesystem read time",
        "scan time", "number of scanned columns", "number of columns in the relation",
        "cloud storage request count", "cloud storage request duration",
        # キャッシュ I/O 待ち時間
        "executor time IO wait - cache read columns",
        "executor time IO wait - cache read footers",
        "executor time IO wait - cache write columns",
        "executor time IO wait - cache write footers",
        "cache async file status fetch waiting time",
    }
    def _walk(node):
        name = node.get("nodeName", "")
        if "Scan" in name:
            for m in node.get("metrics", []):
                if m.get("name") in _target_metrics:
                    results.append({
                        "scan_node": name,
                        "metric_name": m["name"],
                        "acc_id": m["accumulatorId"],
                    })
        for child in node.get("children", []):
            _walk(child)
    _walk(plan)
    return results

# --- Step 2: Accumulator 値の取得 ---
# DriverAccumUpdates（Driver-side: planning metrics）
_driver_accum = (
    bronze_df
    .filter(F.col("event_type") ==
            "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates")
    .withColumn("execution_id",
        F.expr("try_cast(get_json_object(value, '$.executionId') AS BIGINT)"))
    .filter(F.col("execution_id").isNotNull())
    .withColumn("_updates",
        F.from_json(
            F.get_json_object(F.col("value"), "$.accumUpdates"),
            ArrayType(ArrayType(StringType()))
        )
    )
    .withColumn("_update", F.explode("_updates"))
    .withColumn("acc_id",    F.expr("try_cast(`_update`[0] AS BIGINT)"))
    .withColumn("acc_value", F.expr("try_cast(`_update`[1] AS BIGINT)"))
    .filter(F.col("acc_id").isNotNull() & F.col("acc_value").isNotNull())
    .select("cluster_id", "app_id", "execution_id", "acc_id", "acc_value")
)

# StageCompleted の Accumulables（Task-side 実測値の集約値）
# StageCompleted は 5,855 件程度なので TaskEnd（130万件）より軽量
# StageCompleted の accumulables を SQL で安全に抽出
# StageCompleted には sql.execution.id がないため、
# stage_id → silver_job_events.stage_ids → sql_execution_id で紐づけ
_stage_completed_sql = spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}bronze_raw_events").filter(
    F.col("event_type") == "SparkListenerStageCompleted"
)
_stage_completed_sql.createOrReplaceTempView("_tmp_stage_completed")

# stage_id → sql_execution_id のマッピング（silver_job_events から構築）
_stage_to_exec = (
    silver_job_df
    .filter(F.col("event_type") == "SparkListenerJobStart")
    .filter(F.col("sql_execution_id").isNotNull())
    .withColumn("_sid", F.explode(F.from_json(F.col("stage_ids"), ArrayType(IntegerType()))))
    .select("cluster_id", "app_id", F.col("_sid").alias("stage_id"), "sql_execution_id")
    .dropDuplicates(["cluster_id", "app_id", "stage_id"])
)
_stage_to_exec.createOrReplaceTempView("_tmp_stage_to_exec")

_stage_accum = spark.sql(f"""
    SELECT
        j.cluster_id, j.app_id,
        j.execution_id,
        try_cast(acc.ID AS BIGINT) AS acc_id,
        try_cast(acc.Value AS BIGINT) AS acc_value
    FROM (
        SELECT sc.cluster_id, sc.app_id, se.sql_execution_id AS execution_id, sc.value
        FROM _tmp_stage_completed sc
        INNER JOIN _tmp_stage_to_exec se
            ON sc.cluster_id = se.cluster_id
            AND sc.app_id = se.app_id
            AND try_cast(get_json_object(sc.value, "$['Stage Info']['Stage ID']") AS INT) = se.stage_id
    ) j
    LATERAL VIEW EXPLODE(
        from_json(
            get_json_object(j.value, "$['Stage Info'].Accumulables"),
            'ARRAY<STRUCT<ID: STRING, Name: STRING, Value: STRING>>'
        )
    ) t AS acc
    WHERE try_cast(acc.ID AS BIGINT) IS NOT NULL
      AND try_cast(acc.Value AS BIGINT) IS NOT NULL
""")

# Driver + Stage の accumulator を統合
_accum_updates = _driver_accum.unionByName(_stage_accum)

# --- Step 3: sparkPlanInfo から Scan ノードの acc_id を取得 ---
_scan_acc = (
    silver_sql_df
    .withColumn("_scan_metrics", _extract_scan_acc_ids(F.col("spark_plan_json")))
    .withColumn("_sm", F.explode_outer("_scan_metrics"))
    .filter(F.col("_sm").isNotNull())
    .select(
        "cluster_id", "app_id", "execution_id",
        F.col("_sm.scan_node").alias("scan_node"),
        F.col("_sm.metric_name").alias("metric_name"),
        F.col("_sm.acc_id").alias("acc_id"),
    )
)

# --- Step 4: acc_id で突合して値を取得 ---
_scan_values = (
    _scan_acc
    .join(_accum_updates, on=["cluster_id", "app_id", "execution_id", "acc_id"], how="left")
    .groupBy("cluster_id", "app_id", "execution_id")
    .pivot("metric_name")
    .agg(F.sum("acc_value"))
)

# --- Step 5: キャッシュヒット率・フィルタレート等を計算 ---
# pivot 後に存在しないメトリクスカラムをリテラル 0 で補完
_pivot_cols = set(_scan_values.columns)
_expected_metrics = [
    "cache hits size", "cache misses size", "cache writes size",
    "number of output rows", "number of files read", "number of files pruned",
    "number of bytes pruned", "size of files read",
    "filesystem read data size", "filesystem read time",
    "scan time", "number of scanned columns", "number of columns in the relation",
    "cloud storage request count", "cloud storage request duration",
    "executor time IO wait - cache read columns",
    "executor time IO wait - cache read footers",
    "executor time IO wait - cache write columns",
    "executor time IO wait - cache write footers",
    "cache async file status fetch waiting time",
]
for _m in _expected_metrics:
    if _m not in _pivot_cols:
        _scan_values = _scan_values.withColumn(_m, F.lit(0).cast("bigint"))

_scan_metrics_df = (
    _scan_values
    .withColumn("cache_hit_bytes",  F.coalesce(F.col("`cache hits size`"), F.lit(0)))
    .withColumn("cache_miss_bytes", F.coalesce(F.col("`cache misses size`"), F.lit(0)))
    .withColumn("cache_hit_pct",
        F.when(
            (F.col("cache_hit_bytes") + F.col("cache_miss_bytes")) > 0,
            F.round(F.col("cache_hit_bytes") / (F.col("cache_hit_bytes") + F.col("cache_miss_bytes")) * 100, 1)
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn("files_read",   F.coalesce(F.col("`number of files read`"), F.lit(0)))
    .withColumn("files_pruned", F.coalesce(F.col("`number of files pruned`"), F.lit(0)))
    .withColumn("file_pruning_pct",
        F.when(
            (F.col("files_read") + F.col("files_pruned")) > 0,
            F.round(F.col("files_pruned") / (F.col("files_read") + F.col("files_pruned")) * 100, 1)
        ).otherwise(F.lit(None).cast("double"))
    )
    .withColumn("bytes_pruned",    F.coalesce(F.col("`number of bytes pruned`"), F.lit(0)))
    .withColumn("scan_output_rows", F.col("`number of output rows`"))
    .withColumn("files_read_size_mb",
        F.round(F.coalesce(F.col("`size of files read`"), F.lit(0)) / 1024.0 / 1024.0, 1))
    .withColumn("fs_read_size_mb",
        F.round(F.coalesce(F.col("`filesystem read data size`"), F.lit(0)) / 1024.0 / 1024.0, 1))
    .withColumn("fs_read_time_ms",
        F.round(F.coalesce(F.col("`filesystem read time`"), F.lit(0)), 0))  # StageCompleted: 既にms
    .withColumn("scan_time_ms",
        F.round(F.coalesce(F.col("`scan time`"), F.lit(0)), 0))  # StageCompleted: 既にms
    .withColumn("cloud_request_count",  F.col("`cloud storage request count`"))
    .withColumn("cloud_request_dur_ms",
        F.round(F.coalesce(F.col("`cloud storage request duration`"), F.lit(0)), 0))  # StageCompleted: 既にms
    # キャッシュ I/O 待ち時間（タスク累計、ナノ秒→ミリ秒変換）
    .withColumn("cache_read_wait_ms",
        F.round(
            (F.coalesce(F.col("`executor time IO wait - cache read columns`"), F.lit(0))
            + F.coalesce(F.col("`executor time IO wait - cache read footers`"), F.lit(0)))
            / 1000000.0,
        1))
    .withColumn("cache_write_wait_ms",
        F.round(
            (F.coalesce(F.col("`executor time IO wait - cache write columns`"), F.lit(0))
            + F.coalesce(F.col("`executor time IO wait - cache write footers`"), F.lit(0)))
            / 1000000.0,
        1))
    .withColumn("cache_write_bytes",
        F.coalesce(F.col("`cache writes size`"), F.lit(0)))
    # --- 整合性チェック: Driver-side の推定値のみで Task-side 実測値がない場合は NULL にする ---
    # files_read=0 かつ files_read_size_mb>0 の場合、files_read_size_mb は planning 時の推定値
    .withColumn("files_read_size_mb",
        F.when((F.col("files_read") == 0) & (F.col("files_read_size_mb") > 0),
            F.lit(None).cast("double")
        ).otherwise(F.col("files_read_size_mb"))
    )
    # files_read=0 の場合、file_pruning_pct も意味がない
    .withColumn("file_pruning_pct",
        F.when(F.col("files_read") == 0, F.lit(None).cast("double"))
         .otherwise(F.col("file_pruning_pct"))
    )
    .select(
        "cluster_id", "app_id", "execution_id",
        "cache_hit_bytes", "cache_miss_bytes", "cache_hit_pct",
        "cache_write_bytes", "cache_read_wait_ms", "cache_write_wait_ms",
        "files_read", "files_pruned", "file_pruning_pct", "bytes_pruned",
        "scan_output_rows", "files_read_size_mb",
        "fs_read_size_mb", "fs_read_time_ms", "scan_time_ms",
        "cloud_request_count", "cloud_request_dur_ms",
    )
    # Deduplicate: aggregate per execution_id to avoid 1:N join with gold_sql_df
    .groupBy("cluster_id", "app_id", "execution_id")
    .agg(
        F.sum("cache_hit_bytes").alias("cache_hit_bytes"),
        F.sum("cache_miss_bytes").alias("cache_miss_bytes"),
        F.round(
            F.when((F.sum("cache_hit_bytes") + F.sum("cache_miss_bytes")) > 0,
                   F.sum("cache_hit_bytes") / (F.sum("cache_hit_bytes") + F.sum("cache_miss_bytes")) * 100
            ).otherwise(F.lit(None).cast("double")), 1
        ).alias("cache_hit_pct"),
        F.sum("cache_write_bytes").alias("cache_write_bytes"),
        F.round(F.sum("cache_read_wait_ms"), 1).alias("cache_read_wait_ms"),
        F.round(F.sum("cache_write_wait_ms"), 1).alias("cache_write_wait_ms"),
        F.sum("files_read").alias("files_read"),
        F.sum("files_pruned").alias("files_pruned"),
        F.round(
            F.when((F.sum("files_read") + F.sum("files_pruned")) > 0,
                   F.sum("files_pruned") / (F.sum("files_read") + F.sum("files_pruned")) * 100
            ).otherwise(F.lit(None).cast("double")), 1
        ).alias("file_pruning_pct"),
        F.sum("bytes_pruned").alias("bytes_pruned"),
        F.sum("scan_output_rows").alias("scan_output_rows"),
        F.round(F.sum("files_read_size_mb"), 1).alias("files_read_size_mb"),
        F.round(F.sum("fs_read_size_mb"), 1).alias("fs_read_size_mb"),
        F.round(F.sum("fs_read_time_ms"), 0).alias("fs_read_time_ms"),
        F.round(F.sum("scan_time_ms"), 0).alias("scan_time_ms"),
        F.sum("cloud_request_count").alias("cloud_request_count"),
        F.round(F.sum("cloud_request_dur_ms"), 0).alias("cloud_request_dur_ms"),
    )
)

# COMMAND ----------

# DBTITLE 1,gold_sql_photon_analysis
# ==============================================================================
# GOLD 7: gold_sql_photon_analysis
# ==============================================================================
print("[Gold] gold_sql_photon_analysis ...")

gold_sql_df = (
    silver_sql_df
    # 全オペレーター名リスト: "(N) OperatorName" パターンを抽出
    .withColumn("all_operators",
        F.expr(r"regexp_extract_all(physical_plan, '\\((\\d+)\\)\\s+(\\w+)', 2)"))
    .withColumn("total_operators",  F.size(F.col("all_operators")))
    .withColumn("photon_operators",
        F.expr("size(filter(all_operators, x -> x like 'Photon%'))"))
    .withColumn("photon_pct",
        F.when(F.col("total_operators") > 0,
            F.round(F.col("photon_operators") / F.col("total_operators") * 100, 1)
        ).otherwise(None))
    .withColumn("is_photon",    F.col("photon_operators") > 0)
    .withColumn("duration_ms",  F.col("end_time_ms") - F.col("start_time_ms"))
    .withColumn("duration_sec", F.round(F.col("duration_ms") / 1000.0, 2))
    .withColumn("start_ts",     F.to_timestamp(F.col("start_time_ms") / 1000))
    .withColumn("description_short",
        F.regexp_replace(F.trim(F.col("description")), r"\s+", " "))
    # ─── ジョイン種別カウント ───
    .withColumn("bhj_count",
        F.expr("size(filter(all_operators, x -> x = 'BroadcastHashJoin'))"))
    .withColumn("photon_bhj_count",
        F.expr("size(filter(all_operators, x -> x = 'PhotonBroadcastHashJoin'))"))
    .withColumn("smj_count",
        F.expr("size(filter(all_operators, x -> x = 'SortMergeJoin'))"))
    .withColumn("total_join_count",
        F.col("bhj_count") + F.col("photon_bhj_count") + F.col("smj_count"))
    # ─── 非Photon演算子リスト ───
    .withColumn("non_photon_op_list",
        F.array_join(
            F.expr("array_distinct(filter(all_operators, x -> x NOT like 'Photon%'))"),
            ", "
        ))
    # ─── Photon Explanation（Spark UI と同じ内容）───
    # physical_plan の末尾に "== Photon Explanation ==" 以降のテキストがある
    .withColumn("photon_explanation",
        F.when(
            F.col("physical_plan").contains("Photon Explanation"),
            F.trim(F.expr(
                "SUBSTRING(physical_plan, LOCATE('== Photon Explanation ==', physical_plan) + LENGTH('== Photon Explanation =='))"
            ))
        ).otherwise(F.lit(None).cast("string"))
    )
    # ─── スキャン対象テーブル抽出（"Scan parquet catalog.schema.table" パターン）───
    .withColumn("_scan_tables_arr",
        F.expr(r"regexp_extract_all(physical_plan, 'Scan \\w+\\s+(\\w+\\.\\w+\\.\\w+)', 1)"))
    .withColumn("scan_tables",
        F.array_join(F.array_distinct(F.col("_scan_tables_arr")), ", "))
    .drop("_scan_tables_arr")
    # ─── スキャンフォーマット（Scan parquet / Scan json / Scan csv / InMemoryTableScan 等）───
    .withColumn("_scan_formats_arr",
        F.expr(r"regexp_extract_all(physical_plan, '(Scan \\w+|InMemoryTableScan|FileScan \\w+)', 0)"))
    .withColumn("scan_formats",
        F.array_join(F.array_distinct(F.col("_scan_formats_arr")), ", "))
    .drop("_scan_formats_arr")
    # ─── ストレージパス（Location: から抽出）───
    .withColumn("_scan_paths_arr",
        F.expr(r"regexp_extract_all(physical_plan, 'Location:\\s*\\w+\\s*\\[([^\\]]+)\\]', 1)"))
    .withColumn("scan_paths",
        F.array_join(F.array_distinct(F.col("_scan_paths_arr")), ", "))
    .drop("_scan_paths_arr")
    # ─── ReadSchema のカラム数 ───
    .withColumn("_schema_strs",
        F.expr(r"regexp_extract_all(physical_plan, 'ReadSchema:\\s*struct<([^>]+)>', 1)"))
    .withColumn("scan_column_count",
        F.when(F.size("_schema_strs") > 0,
            F.expr("aggregate(_schema_strs, 0, (acc, s) -> acc + size(split(s, ',')))")
        ).otherwise(F.lit(0)))
    .drop("_schema_strs")
    # ─── フィルタ条件抽出（PushedFilters / DataFilters / PartitionFilters）───
    .withColumn("_pushed",
        F.expr(r"regexp_extract_all(physical_plan, 'PushedFilters:\\s*\\[([^\\]]*)\\]', 1)"))
    .withColumn("_data_filters",
        F.expr(r"regexp_extract_all(physical_plan, 'DataFilters:\\s*\\[([^\\]]*)\\]', 1)"))
    .withColumn("_partition_filters",
        F.expr(r"regexp_extract_all(physical_plan, 'PartitionFilters:\\s*\\[([^\\]]*)\\]', 1)"))
    .withColumn("scan_filters",
        F.trim(F.concat_ws(
            " | ",
            F.when(F.size("_data_filters") > 0,
                F.concat(F.lit("DataFilters: "), F.array_join(F.array_distinct(F.col("_data_filters")), "; "))),
            F.when(F.size("_partition_filters") > 0,
                F.concat(F.lit("PartitionFilters: "), F.array_join(F.array_distinct(F.col("_partition_filters")), "; "))),
            F.when((F.size("_data_filters") == 0) & (F.size("_partition_filters") == 0) & (F.size("_pushed") > 0),
                F.concat(F.lit("PushedFilters: "), F.array_join(F.array_distinct(F.col("_pushed")), "; "))),
        ))
    )
    .withColumn("scan_filters",
        F.when(F.col("scan_filters") == "", F.lit(None).cast("string"))
         .otherwise(F.col("scan_filters"))
    )
    .drop("_pushed", "_data_filters", "_partition_filters")
)

# Scan メトリクス（キャッシュヒット率・プルーニング率等）を JOIN
gold_sql_df = (
    gold_sql_df
    .join(_scan_metrics_df, on=["cluster_id", "app_id", "execution_id"], how="left")
    .select(
        "cluster_id", "app_id", "execution_id",
        "description_short", "start_ts", "duration_sec",
        "total_operators", "photon_operators", "photon_pct",
        F.col("is_photon").cast("string").alias("is_photon"),
        "bhj_count", "photon_bhj_count", "smj_count", "total_join_count",
        "non_photon_op_list", "photon_explanation",
        "scan_tables", "scan_formats", "scan_paths", "scan_column_count", "scan_filters",
        # Scan メトリクス
        "cache_hit_pct", "cache_hit_bytes", "cache_miss_bytes",
        "cache_write_bytes", "cache_read_wait_ms", "cache_write_wait_ms",
        "files_read", "files_pruned", "file_pruning_pct",
        "scan_output_rows", "files_read_size_mb",
        "fs_read_size_mb", "fs_read_time_ms", "scan_time_ms",
        "cloud_request_count", "cloud_request_dur_ms",
    )
)

gold_sql_df = _save(gold_sql_df, "gold_sql_photon_analysis", merge_keys=["cluster_id", "app_id", "execution_id"])

# COMMAND ----------

# DBTITLE 1,gold_autoscale_timeline
# ==============================================================================
# GOLD 8: gold_autoscale_timeline
# ==============================================================================
# Correlate executor scaling events with active stages.
# Includes: autoscale, fixed-size clusters, Spot loss/recovery.
# Provides duration_sec per segment for accurate cost calculation.
print("[Gold] gold_autoscale_timeline ...")

from pyspark.sql.window import Window

# --- Step 1: Build all executor add/remove events ---
_exec_events = (
    gold_exec_df
    .select("cluster_id", "app_id", "executor_id", "host", "total_cores",
            "add_ts", "remove_ts", "removed_reason")
    .filter(F.col("add_ts").isNotNull())
)

# Classify removal reason
def _classify_reason(col):
    return (
        F.when(col.isNull(), F.lit(None))
        .when(col.contains("kill request"), F.lit("AUTOSCALE_IN"))
        .when(col.contains("cluster termination"), F.lit("CLUSTER_STOP"))
        .when(col.contains("spot"), F.lit("SPOT_PREEMPTION"))
        .when(col.contains("node lost") | col.contains("heartbeat"), F.lit("NODE_LOST"))
        .otherwise(F.lit("OTHER"))
    )

# Add events
_add_events = (
    _exec_events
    .select(
        "cluster_id", "app_id",
        F.col("add_ts").alias("event_ts"),
        F.lit("EXECUTOR_ADDED").alias("event_type"),
        F.col("executor_id"), F.col("host"), F.col("total_cores"),
        F.lit(None).cast("string").alias("event_reason"),
    )
)

# Remove events with classified reason
_remove_events = (
    _exec_events
    .filter(F.col("remove_ts").isNotNull())
    .select(
        "cluster_id", "app_id",
        F.col("remove_ts").alias("event_ts"),
        F.lit("EXECUTOR_REMOVED").alias("event_type"),
        F.col("executor_id"), F.col("host"), F.col("total_cores"),
        _classify_reason(F.col("removed_reason")).alias("event_reason"),
    )
)

# Union and compute running worker count
_all_events = _add_events.unionByName(_remove_events)
_w_app = Window.partitionBy("cluster_id", "app_id").orderBy("event_ts")
_all_events = (
    _all_events
    .withColumn("_delta", F.when(F.col("event_type") == "EXECUTOR_ADDED", 1).otherwise(-1))
    .withColumn("worker_count_after", F.sum("_delta").over(_w_app))
    .withColumn("worker_count_before",
                F.coalesce(F.lag("worker_count_after").over(_w_app), F.lit(0)))
    .drop("_delta")
)

# --- Step 2: Compute duration to next event (for cost calculation) ---
_all_events = (
    _all_events
    .withColumn("next_event_ts", F.lead("event_ts").over(_w_app))
    .withColumn("segment_duration_sec",
                F.when(F.col("next_event_ts").isNotNull(),
                       F.round((F.unix_timestamp("next_event_ts") - F.unix_timestamp("event_ts")), 1))
                .otherwise(None))
)

# --- Step 3: Join active stages at each event ---
_with_stages = (
    _all_events.alias("e")
    .join(
        gold_stage_df.alias("s"),
        on=[
            F.col("e.cluster_id") == F.col("s.cluster_id"),
            F.col("e.app_id") == F.col("s.app_id"),
            F.col("e.event_ts").between(F.col("s.submission_ts"), F.col("s.completion_ts")),
        ],
        how="left",
    )
    .select(
        F.col("e.cluster_id"), F.col("e.app_id"),
        F.col("e.event_ts"), F.col("e.event_type"), F.col("e.event_reason"),
        F.col("e.executor_id"), F.col("e.host"), F.col("e.total_cores"),
        F.col("e.worker_count_before"), F.col("e.worker_count_after"),
        F.col("e.segment_duration_sec"),
        F.col("s.stage_id"), F.col("s.stage_name"), F.col("s.num_tasks"),
        F.col("s.duration_ms").alias("stage_duration_ms"),
        F.col("s.disk_spill_mb").alias("stage_disk_spill_mb"),
        F.col("s.shuffle_read_mb").alias("stage_shuffle_read_mb"),
        F.col("s.bottleneck_type").alias("stage_bottleneck_type"),
        F.col("s.severity").alias("stage_severity"),
    )
)

# --- Step 4: Aggregate per event ---
gold_autoscale_df = (
    _with_stages
    .groupBy(
        "cluster_id", "app_id", "event_ts", "event_type", "event_reason",
        "executor_id", "host", "total_cores",
        "worker_count_before", "worker_count_after", "segment_duration_sec",
    )
    .agg(
        F.count("stage_id").alias("active_stage_count"),
        F.sum("num_tasks").alias("total_active_tasks"),
        F.collect_set("stage_id").alias("_stage_ids"),
        F.collect_set("stage_name").alias("_stage_names"),
        F.collect_set("stage_bottleneck_type").alias("_bn_types"),
        F.max("stage_severity").alias("max_stage_severity"),
        F.sum("stage_disk_spill_mb").alias("active_spill_mb"),
        F.sum("stage_shuffle_read_mb").alias("active_shuffle_mb"),
    )
    .withColumn("active_stage_ids", F.concat_ws(",", F.col("_stage_ids").cast("array<string>")))
    .withColumn("active_stage_names", F.concat_ws(", ", F.col("_stage_names")))
    .withColumn("active_bottleneck_types", F.concat_ws(", ", F.col("_bn_types")))
    .drop("_stage_ids", "_stage_names", "_bn_types")
    .select(
        "cluster_id", "app_id", "event_ts", "event_type", "event_reason",
        "executor_id", "host", "total_cores",
        "worker_count_before", "worker_count_after", "segment_duration_sec",
        "active_stage_count", "total_active_tasks",
        "active_stage_ids", "active_stage_names",
        "active_bottleneck_types", "max_stage_severity",
        "active_spill_mb", "active_shuffle_mb",
    )
    .orderBy("cluster_id", "app_id", "event_ts")
)

gold_autoscale_df = _save(gold_autoscale_df, "gold_autoscale_timeline", merge_keys=["cluster_id", "app_id", "event_ts", "executor_id"])

# COMMAND ----------

# DBTITLE 1,Cost Estimation Enrichment
# ==============================================================================
# COST ENRICHMENT: DBU / USD 推定値を gold_application_summary に追加
# ==============================================================================
# dabs/app/core/dbu_pricing.py の最小インライン版。定数は同ファイルと一致必須。
# tests/test_notebook_cost_drift.py が notebook と app 側の定数一致を検証する。
# ==============================================================================
print("[Cost] enriching gold_application_summary with cost columns ...")

import re as _cost_re
from datetime import datetime as _cost_dt

# ---- Pricing constants (MUST match dabs/app/core/dbu_pricing.py) --------------
_COST_PHOTON_MULTIPLIER = 2.0
_COST_DBU_PRICE_USD = 0.15
_COST_DBU_PRICE_USD_PHOTON = 0.30
_COST_FALLBACK_VCPUS = 4

_COST_AWS_SIZE_VCPUS = {
    "large": 2, "xlarge": 4, "2xlarge": 8, "4xlarge": 16,
    "8xlarge": 32, "9xlarge": 36, "12xlarge": 48, "16xlarge": 64,
    "24xlarge": 96, "metal": 96,
}
_COST_AWS_FAMILY_CATEGORY = {
    "c": "compute", "m": "general", "r": "memory",
    "i": "storage", "d": "storage", "g": "gpu", "p": "gpu",
}
_COST_AZURE_KNOWN = {
    "Standard_DS3_v2": (4, "general"), "Standard_DS4_v2": (8, "general"),
    "Standard_DS5_v2": (16, "general"),
    "Standard_D4s_v3": (4, "general"), "Standard_D8s_v3": (8, "general"),
    "Standard_D16s_v3": (16, "general"), "Standard_D32s_v3": (32, "general"),
    "Standard_D4s_v5": (4, "general"), "Standard_D8s_v5": (8, "general"),
    "Standard_D16s_v5": (16, "general"), "Standard_D32s_v5": (32, "general"),
    "Standard_E4s_v3": (4, "memory"), "Standard_E8s_v3": (8, "memory"),
    "Standard_E16s_v3": (16, "memory"), "Standard_E32s_v3": (32, "memory"),
    "Standard_E4s_v5": (4, "memory"), "Standard_E8s_v5": (8, "memory"),
    "Standard_E16s_v5": (16, "memory"), "Standard_E32s_v5": (32, "memory"),
    "Standard_F4s_v2": (4, "compute"), "Standard_F8s_v2": (8, "compute"),
    "Standard_F16s_v2": (16, "compute"), "Standard_F32s_v2": (32, "compute"),
    "Standard_L8s_v2": (8, "storage"), "Standard_L16s_v2": (16, "storage"),
    "Standard_L8s_v3": (8, "storage"), "Standard_L16s_v3": (16, "storage"),
    "Standard_NC6s_v3": (6, "gpu"), "Standard_NC12s_v3": (12, "gpu"),
    "Standard_NC24s_v3": (24, "gpu"),
}
_COST_INSTANCE_PRICING = {
    # AWS general (m5)
    "m5.xlarge": (0.28, 0.192), "m5.2xlarge": (0.56, 0.384),
    "m5.4xlarge": (1.12, 0.768), "m5.8xlarge": (2.24, 1.536),
    "m5.12xlarge": (3.36, 2.304), "m5.16xlarge": (4.48, 3.072),
    "m5d.xlarge": (0.28, 0.226), "m5d.2xlarge": (0.56, 0.452),
    "m5d.4xlarge": (1.12, 0.904),
    # AWS general (m6, Graviton and Intel)
    "m6i.xlarge": (0.28, 0.192), "m6i.2xlarge": (0.56, 0.384),
    "m6i.4xlarge": (1.12, 0.768), "m6i.8xlarge": (2.24, 1.536),
    "m6id.xlarge": (0.28, 0.237), "m6id.2xlarge": (0.56, 0.475),
    "m6id.4xlarge": (1.12, 0.949),
    "m6gd.xlarge": (0.28, 0.181), "m6gd.2xlarge": (0.56, 0.362),
    "m6gd.4xlarge": (1.12, 0.724), "m6gd.8xlarge": (2.24, 1.448),
    # AWS general (m7, latest)
    "m7g.xlarge": (0.28, 0.163), "m7g.2xlarge": (0.56, 0.326),
    "m7g.4xlarge": (1.12, 0.653), "m7g.8xlarge": (2.24, 1.306),
    "m7gd.xlarge": (0.28, 0.216), "m7gd.2xlarge": (0.56, 0.432),
    "m7gd.4xlarge": (1.12, 0.864), "m7gd.8xlarge": (2.24, 1.729),
    "m7i.xlarge": (0.28, 0.201), "m7i.2xlarge": (0.56, 0.403),
    "m7i.4xlarge": (1.12, 0.806), "m7i.8xlarge": (2.24, 1.613),
    # AWS compute (c5/c6/c7)
    "c5.xlarge": (0.28, 0.170), "c5.2xlarge": (0.56, 0.340),
    "c5.4xlarge": (1.12, 0.680), "c5.9xlarge": (2.52, 1.530),
    "c6i.xlarge": (0.28, 0.170), "c6i.2xlarge": (0.56, 0.340),
    "c6i.4xlarge": (1.12, 0.680),
    "c6id.xlarge": (0.28, 0.215), "c6id.2xlarge": (0.56, 0.430),
    "c6gd.xlarge": (0.28, 0.154), "c6gd.2xlarge": (0.56, 0.307),
    "c6gd.4xlarge": (1.12, 0.614),
    "c7g.xlarge": (0.28, 0.145), "c7g.2xlarge": (0.56, 0.289),
    "c7g.4xlarge": (1.12, 0.579),
    "c7gd.xlarge": (0.28, 0.191), "c7gd.2xlarge": (0.56, 0.382),
    "c7i.xlarge": (0.28, 0.178), "c7i.2xlarge": (0.56, 0.357),
    # AWS memory (r5/r6/r7)
    "r5.xlarge": (0.28, 0.252), "r5.2xlarge": (0.56, 0.504),
    "r5.4xlarge": (1.12, 1.008), "r5.8xlarge": (2.24, 2.016),
    "r5d.xlarge": (0.28, 0.288), "r5d.2xlarge": (0.56, 0.576),
    "r5d.4xlarge": (1.12, 1.152),
    "r6i.xlarge": (0.28, 0.252), "r6i.2xlarge": (0.56, 0.504),
    "r6i.4xlarge": (1.12, 1.008),
    "r6id.xlarge": (0.28, 0.303), "r6id.2xlarge": (0.56, 0.605),
    "r6id.4xlarge": (1.12, 1.210), "r6id.8xlarge": (2.24, 2.419),
    "r6gd.xlarge": (0.28, 0.227), "r6gd.2xlarge": (0.56, 0.454),
    "r6gd.4xlarge": (1.12, 0.907),
    "r7g.xlarge": (0.28, 0.214), "r7g.2xlarge": (0.56, 0.428),
    "r7g.4xlarge": (1.12, 0.857),
    "r7gd.xlarge": (0.28, 0.271), "r7gd.2xlarge": (0.56, 0.543),
    "r7gd.4xlarge": (1.12, 1.085), "r7gd.8xlarge": (2.24, 2.170),
    "r7i.xlarge": (0.28, 0.266), "r7i.2xlarge": (0.56, 0.531),
    "r7iz.xlarge": (0.28, 0.372), "r7iz.2xlarge": (0.56, 0.745),
    # AWS storage (i3/i3en/i4i/i4g)
    "i3.xlarge": (0.28, 0.312), "i3.2xlarge": (0.56, 0.624),
    "i3.4xlarge": (1.12, 1.248), "i3.8xlarge": (2.24, 2.496),
    "i3.16xlarge": (4.48, 4.992),
    "i3en.xlarge": (0.28, 0.452), "i3en.2xlarge": (0.56, 0.904),
    "i4i.xlarge": (0.28, 0.341), "i4i.2xlarge": (0.56, 0.682),
    "i4i.4xlarge": (1.12, 1.364),
    "i4g.xlarge": (0.28, 0.277), "i4g.2xlarge": (0.56, 0.554),
    # AWS GPU
    "g4dn.xlarge": (1.00, 0.526), "g5.xlarge": (1.00, 1.006),
    "g5.2xlarge": (1.00, 1.212), "g5.4xlarge": (1.00, 1.624),
    "g5g.xlarge": (1.00, 0.420),
    "g6.xlarge": (1.00, 0.805), "g6.2xlarge": (1.00, 0.977),
    "p3.2xlarge": (2.00, 3.060),
    # Azure general (D series)
    "Standard_DS3_v2": (0.28, 0.229), "Standard_DS4_v2": (0.56, 0.458),
    "Standard_DS5_v2": (1.12, 0.916),
    "Standard_D4s_v3": (0.28, 0.192), "Standard_D8s_v3": (0.56, 0.384),
    "Standard_D16s_v3": (1.12, 0.768), "Standard_D32s_v3": (2.24, 1.536),
    "Standard_D4s_v5": (0.28, 0.192), "Standard_D8s_v5": (0.56, 0.384),
    "Standard_D16s_v5": (1.12, 0.768), "Standard_D32s_v5": (2.24, 1.536),
    # Azure memory (E series)
    "Standard_E4s_v3": (0.28, 0.252), "Standard_E8s_v3": (0.56, 0.504),
    "Standard_E16s_v3": (1.12, 1.008), "Standard_E32s_v3": (2.24, 2.016),
    "Standard_E4s_v5": (0.28, 0.252), "Standard_E8s_v5": (0.56, 0.504),
    "Standard_E16s_v5": (1.12, 1.008), "Standard_E32s_v5": (2.24, 2.016),
    # Azure compute (F series)
    "Standard_F4s_v2": (0.28, 0.169), "Standard_F8s_v2": (0.56, 0.338),
    "Standard_F16s_v2": (1.12, 0.677), "Standard_F32s_v2": (2.24, 1.354),
    # Azure storage (L series)
    "Standard_L8s_v2": (0.56, 0.572), "Standard_L16s_v2": (1.12, 1.144),
    "Standard_L8s_v3": (0.56, 0.624), "Standard_L16s_v3": (1.12, 1.248),
    # Azure GPU (NC series)
    "Standard_NC6s_v3": (1.00, 3.060), "Standard_NC12s_v3": (1.00, 6.120),
    "Standard_NC24s_v3": (1.00, 12.240),
    # GCP general (n2-standard)
    "n2-standard-2": (0.14, 0.097), "n2-standard-4": (0.28, 0.194),
    "n2-standard-8": (0.56, 0.389), "n2-standard-16": (1.12, 0.777),
    "n2-standard-32": (2.24, 1.555),
    # GCP memory (n2-highmem)
    "n2-highmem-2": (0.14, 0.131), "n2-highmem-4": (0.28, 0.262),
    "n2-highmem-8": (0.56, 0.524), "n2-highmem-16": (1.12, 1.048),
    "n2-highmem-32": (2.24, 2.096),
    # GCP compute (n2-highcpu)
    "n2-highcpu-4": (0.28, 0.143), "n2-highcpu-8": (0.56, 0.285),
    "n2-highcpu-16": (1.12, 0.571), "n2-highcpu-32": (2.24, 1.142),
    # GCP AMD EPYC (n2d-standard)
    "n2d-standard-4": (0.28, 0.169), "n2d-standard-8": (0.56, 0.338),
    "n2d-standard-16": (1.12, 0.676), "n2d-standard-32": (2.24, 1.352),
    # GCP cost-optimized (e2)
    "e2-standard-2": (0.14, 0.067), "e2-standard-4": (0.28, 0.134),
    "e2-standard-8": (0.56, 0.268), "e2-standard-16": (1.12, 0.536),
    # GCP latest compute (c3)
    "c3-standard-4": (0.28, 0.209), "c3-standard-8": (0.56, 0.418),
    "c3-standard-22": (1.54, 1.150),
}
_COST_DBU_PER_VCPU_HOUR = {"general": 0.07, "compute": 0.07, "memory": 0.07,
                            "storage": 0.07, "gpu": 0.25, "unknown": 0.07}
_COST_USD_PER_VCPU_HOUR = {"general": 0.048, "compute": 0.042, "memory": 0.063,
                            "storage": 0.078, "gpu": 0.50, "unknown": 0.048}
_COST_REGION_MULTIPLIER = {
    "us-east-1": 1.0, "us-east-2": 1.0, "us-west-2": 1.0,
    "us-west-1": 1.05, "ca-central-1": 1.05,
    "eu-west-1": 1.10, "eu-west-2": 1.12, "eu-central-1": 1.12, "eu-north-1": 1.10,
    "ap-northeast-1": 1.15, "ap-northeast-2": 1.12, "ap-northeast-3": 1.15,
    "ap-southeast-1": 1.10, "ap-southeast-2": 1.12,
    "ap-south-1": 1.05, "sa-east-1": 1.20,
    "eastus": 1.0, "eastus2": 1.0, "westus2": 1.0, "westus3": 1.0,
    "centralus": 1.0, "northcentralus": 1.0, "southcentralus": 1.0,
    "westeurope": 1.10, "northeurope": 1.10, "uksouth": 1.12,
    "japaneast": 1.15, "japanwest": 1.15,
    "southeastasia": 1.10, "eastasia": 1.12,
    "australiaeast": 1.12, "brazilsouth": 1.20,
    "us-central1": 1.0, "us-east1": 1.0, "us-east4": 1.02,
    "us-west1": 1.0, "us-west4": 1.05,
    "europe-west1": 1.10, "europe-west4": 1.12,
    "asia-northeast1": 1.15, "asia-southeast1": 1.10,
}


def _cost_parse_instance(raw):
    raw = (raw or "").strip()
    if not raw:
        return (_COST_FALLBACK_VCPUS, "unknown", "unknown")
    m = _cost_re.match(r"^([a-z]\w*?)\.(\w+)$", raw, _cost_re.IGNORECASE)
    if m:
        family = m.group(1).lower()
        size = m.group(2).lower()
        vcpus = _COST_AWS_SIZE_VCPUS.get(size, _COST_FALLBACK_VCPUS)
        cat_key = family[0] if family else ""
        category = _COST_AWS_FAMILY_CATEGORY.get(cat_key, "general")
        return (vcpus, category, "aws")
    if raw.startswith("Standard_"):
        if raw in _COST_AZURE_KNOWN:
            vcpus, category = _COST_AZURE_KNOWN[raw]
            return (vcpus, category, "azure")
        nums = _cost_re.findall(r"(\d+)", raw)
        vcpus = int(nums[0]) if nums else _COST_FALLBACK_VCPUS
        return (vcpus, "general", "azure")
    m = _cost_re.match(r"^(\w+)-(\w+)-(\d+)$", raw)
    if m:
        type_ = m.group(2).lower()
        if "highmem" in type_:
            category = "memory"
        elif "highcpu" in type_:
            category = "compute"
        else:
            category = "general"
        return (int(m.group(3)), category, "gcp")
    return (_COST_FALLBACK_VCPUS, "unknown", "unknown")


def _cost_region_mult(region):
    return _COST_REGION_MULTIPLIER.get((region or "").strip().lower(), 1.0)


def _cost_dbu_rate(instance_type, photon):
    key = (instance_type or "").strip().lower()
    mult = _COST_PHOTON_MULTIPLIER if photon else 1.0
    for k, (dbu, _usd) in _COST_INSTANCE_PRICING.items():
        if k.lower() == key:
            return round(dbu * mult, 4)
    vcpus, category, cloud = _cost_parse_instance(instance_type)
    if cloud == "unknown":
        rate = _COST_FALLBACK_VCPUS * _COST_DBU_PER_VCPU_HOUR["unknown"]
    else:
        rate = vcpus * _COST_DBU_PER_VCPU_HOUR.get(category, 0.07)
    return round(rate * mult, 4)


def _cost_compute_rate(instance_type, region):
    key = (instance_type or "").strip().lower()
    region_mult = _cost_region_mult(region)
    for k, (_dbu, usd) in _COST_INSTANCE_PRICING.items():
        if k.lower() == key:
            return round(usd * region_mult, 4)
    vcpus, category, cloud = _cost_parse_instance(instance_type)
    if cloud == "unknown":
        rate = _COST_FALLBACK_VCPUS * _COST_USD_PER_VCPU_HOUR["unknown"]
    else:
        rate = vcpus * _COST_USD_PER_VCPU_HOUR.get(category, 0.048)
    return round(rate * region_mult, 4)


def _cost_estimate(worker_node_type, driver_node_type, duration_min,
                    autoscale_cost, min_workers, max_workers, photon_enabled, region):
    """Return (total_dbu, dbu_per_hour, total_usd)."""
    if not duration_min or duration_min <= 0:
        return (0.0, 0.0, 0.0)
    dbu_unit_price = _COST_DBU_PRICE_USD_PHOTON if photon_enabled else _COST_DBU_PRICE_USD
    hours = duration_min / 60.0
    driver_dbu_rate = _cost_dbu_rate(driver_node_type, photon_enabled)
    worker_dbu_rate = _cost_dbu_rate(worker_node_type, photon_enabled)
    driver_dbu = driver_dbu_rate * hours

    # Fixed-size cluster: ignore autoscale_cost (often incomplete for fixed clusters)
    _fixed_cluster = min_workers > 0 and min_workers == max_workers

    if autoscale_cost and not _fixed_cluster:
        worker_dbu = sum(
            int(e.get("worker_count", 0)) * worker_dbu_rate * float(e.get("cumulative_min", 0)) / 60.0
            for e in autoscale_cost
        )
    else:
        wc = min_workers if min_workers > 0 else max_workers
        worker_dbu = wc * worker_dbu_rate * hours

    total_dbu = driver_dbu + worker_dbu
    dbu_per_hour = total_dbu / hours if hours > 0 else 0.0
    dbu_usd = total_dbu * dbu_unit_price
    driver_compute_rate = _cost_compute_rate(driver_node_type, region)
    worker_compute_rate = _cost_compute_rate(worker_node_type, region)
    driver_compute_usd = driver_compute_rate * hours
    if autoscale_cost and not _fixed_cluster:
        worker_compute_usd = sum(
            int(e.get("worker_count", 0)) * worker_compute_rate * float(e.get("cumulative_min", 0)) / 60.0
            for e in autoscale_cost
        )
    else:
        wc = min_workers if min_workers > 0 else max_workers
        worker_compute_usd = wc * worker_compute_rate * hours
    total_usd = dbu_usd + driver_compute_usd + worker_compute_usd
    return (round(total_dbu, 3), round(dbu_per_hour, 3), round(total_usd, 3))


# ---- App runtime windows (for clipping autoscale events) ------------------
_app_windows = {
    (_r["cluster_id"], _r["app_id"]): (_r["start_ts"], _r["end_ts"])
    for _r in (
        spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}gold_application_summary")
        .select("cluster_id", "app_id", "start_ts", "end_ts")
        .collect()
    )
}

# ---- Autoscale timeline → per-app worker_count distribution ---------------
# Clip events to each app's [start_ts, end_ts] window so post-termination
# events (driver shutdown, cluster idle) do not inflate the distribution.
# Append a synthetic end event at end_ts to capture the steady state.
_autoscale_rows = (
    spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}gold_autoscale_timeline")
    .select("cluster_id", "app_id", "event_ts", "worker_count_after")
    .orderBy("cluster_id", "app_id", "event_ts")
    .collect()
)

# Group events by (cluster_id, app_id), clip, add synthetic end
_events_by_app: dict[tuple, list] = {}
for _r in _autoscale_rows:
    _key = (_r["cluster_id"], _r["app_id"])
    _ts = _r["event_ts"]
    _wc = int(_r["worker_count_after"] or 0)
    if _ts is None:
        continue
    _win = _app_windows.get(_key)
    if _win and _win[0] is not None and _win[1] is not None:
        if _ts < _win[0] or _ts > _win[1]:
            continue  # outside app runtime window
    _events_by_app.setdefault(_key, []).append((_ts, _wc))

_autoscale_by_app = {}  # (cluster_id, app_id) -> {wc: total_sec}
for _key, _evs in _events_by_app.items():
    _evs.sort()
    _win = _app_windows.get(_key)
    # Append synthetic end event at app's end_ts so the final wc segment counts
    if _win and _win[1] is not None and _evs and _evs[-1][0] < _win[1]:
        _evs.append((_win[1], _evs[-1][1]))
    _prev_ts = None
    _prev_wc = 0
    _bucket: dict[int, float] = {}
    for _ts, _wc in _evs:
        if _prev_ts is not None:
            _dur_sec = (_ts - _prev_ts).total_seconds()
            if _dur_sec > 0:
                _bucket[_prev_wc] = _bucket.get(_prev_wc, 0) + _dur_sec
        _prev_ts = _ts
        _prev_wc = _wc
    if _bucket:
        _autoscale_by_app[_key] = _bucket

_autoscale_cost_dict = {
    _key: [{"worker_count": _wc, "cumulative_min": round(_sec / 60.0, 1)}
           for _wc, _sec in sorted(_wc_to_sec.items())]
    for _key, _wc_to_sec in _autoscale_by_app.items()
}

# ---- Photon enabled flag per app ------------------------------------------
_photon_set = set()
try:
    _photon_rows = (
        silver_config_df
        .filter(F.col("config_key") == "spark.databricks.photon.enabled")
        .filter(F.lower(F.col("config_value")) == "true")
        .select("cluster_id", "app_id").distinct().collect()
    )
    _photon_set = {(_r["cluster_id"], _r["app_id"]) for _r in _photon_rows}
except Exception as _e:
    print(f"  ⚠ Photon lookup skipped: {_e}")

# ---- Compute cost per app -------------------------------------------------
_gold_app_rows = (
    spark.read.table(f"{SCHEMA}.{TABLE_PREFIX}gold_application_summary")
    .select(
        "cluster_id", "app_id", "worker_node_type", "driver_node_type",
        "duration_min", "min_workers", "max_workers", "region",
    )
    .collect()
)

_cost_rows = []
for _r in _gold_app_rows:
    _key = (_r["cluster_id"], _r["app_id"])
    _total_dbu, _dbu_per_hour, _total_usd = _cost_estimate(
        worker_node_type=_r["worker_node_type"] or "",
        driver_node_type=_r["driver_node_type"] or "",
        duration_min=float(_r["duration_min"] or 0),
        autoscale_cost=_autoscale_cost_dict.get(_key, []),
        min_workers=int(_r["min_workers"] or 0) if _r["min_workers"] else 0,
        max_workers=int(_r["max_workers"] or 0) if _r["max_workers"] else 0,
        photon_enabled=(_key in _photon_set),
        region=_r["region"] or "",
    )
    _cost_rows.append((
        _r["cluster_id"],
        _r["app_id"],
        float(_total_dbu),
        float(_dbu_per_hour),
        float(_total_usd),
    ))

# ---- MERGE into gold_application_summary ----------------------------------
if _cost_rows:
    _cost_schema = StructType([
        StructField("cluster_id",              StringType(), True),
        StructField("app_id",                  StringType(), True),
        StructField("estimated_total_dbu",     DoubleType(), True),
        StructField("estimated_dbu_per_hour",  DoubleType(), True),
        StructField("estimated_total_usd",     DoubleType(), True),
    ])
    _cost_df = spark.createDataFrame(_cost_rows, schema=_cost_schema)
    print(f"  _cost_df schema: {_cost_df.schema.fieldNames()}")
    print(f"  _cost_df rows: {_cost_df.count()}")

    _app_fqn = f"{SCHEMA}.{TABLE_PREFIX}gold_application_summary"

    # Add missing cost columns via ALTER TABLE
    _existing_cols = {c.name.lower() for c in spark.table(_app_fqn).schema}
    _altered = False
    for _col in ("estimated_total_dbu", "estimated_dbu_per_hour", "estimated_total_usd"):
        if _col not in _existing_cols:
            spark.sql(f"ALTER TABLE {_app_fqn} ADD COLUMNS ({_col} DOUBLE)")
            print(f"  + Added column: {_col}")
            _altered = True

    # Force metadata cache refresh so MERGE sees the new columns
    if _altered:
        spark.sql(f"REFRESH TABLE {_app_fqn}")
        spark.catalog.refreshTable(_app_fqn)
    _post_alter_cols = [c.name for c in spark.table(_app_fqn).schema]
    print(f"  gold_app columns post-alter: {_post_alter_cols}")

    # Rebuild via join + insertInto (avoids MERGE schema-caching quirks)
    # Left-join cost onto gold_app, then overwrite the table.
    _existing = spark.read.table(_app_fqn)
    for _col in ("estimated_total_dbu", "estimated_dbu_per_hour", "estimated_total_usd"):
        if _col in _existing.columns:
            _existing = _existing.drop(_col)
    _enriched = _existing.join(_cost_df, on=["cluster_id", "app_id"], how="left")
    (
        _enriched
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(_app_fqn)
    )

    print(f"  ✓ Cost enriched for {len(_cost_rows)} application(s)")
else:
    print("  (no applications to enrich)")

# COMMAND ----------

# DBTITLE 1,gold_streaming_query_summary
# ==============================================================================
# GOLD 8: gold_streaming_query_summary
# ==============================================================================
if silver_streaming_df is not None and _streaming_count > 0:
    print("[Gold] gold_streaming_query_summary ...")

    # ── Progress イベントからバッチ統計を集計 ──
    _progress_df = (
        silver_streaming_df
        .filter(F.col("event_type").contains("QueryProgressEvent"))
    )

    _batch_agg = (
        _progress_df
        .groupBy("cluster_id", "app_id", "query_id")
        .agg(
            F.max("run_id").alias("run_id"),
            F.first(F.when(F.col("query_name").isNotNull(), F.col("query_name"))).alias("query_name"),
            F.count("batch_id").alias("total_batches"),
            F.avg("batch_duration_ms").alias("avg_batch_duration_ms"),
            F.max("batch_duration_ms").alias("max_batch_duration_ms"),
            F.expr("percentile_approx(batch_duration_ms, 0.95)").alias("p95_batch_duration_ms"),
            F.avg("trigger_execution_ms").alias("avg_trigger_execution_ms"),
            F.avg("add_batch_ms").alias("avg_add_batch_ms"),
            F.avg("query_planning_ms").alias("avg_query_planning_ms"),
            F.avg("latest_offset_ms").alias("avg_latest_offset_ms"),
            F.avg(F.col("commit_offsets_ms") + F.col("commit_batch_ms")).alias("avg_commit_ms"),
            F.sum("num_input_rows").alias("total_input_rows"),
            # inputRowsPerSecond: batch 0 は常に 0 なので batch_id > 0 を優先、
            # ただし batch_id=0 しかない場合は全バッチの平均にフォールバック
            F.coalesce(
                F.avg(F.when(F.col("batch_id") > 0, F.col("input_rows_per_sec"))),
                F.avg("input_rows_per_sec"),
            ).alias("avg_input_rows_per_sec"),
            # processedRowsPerSecond: batch 0 でも有効値を持つため全バッチで計算
            F.avg("processed_rows_per_sec").alias("avg_processed_rows_per_sec"),
            F.first("source_description").alias("source_description"),
            F.max("source_num_files_outstanding").alias("last_files_outstanding"),
            F.first("sink_description").alias("sink_description"),
            # 状態ストア
            F.max("state_memory_used_bytes").alias("max_state_memory_bytes"),
            F.max("state_num_rows_total").alias("max_state_rows_total"),
            F.sum("state_rows_dropped_by_watermark").alias("total_rows_dropped_by_watermark"),
            # stateful 判定
            F.max(F.col("is_stateful").cast("int")).alias("_is_stateful_int"),
        )
    )

    # ── Started/Terminated イベントからライフサイクル情報を取得 ──
    _lifecycle_df = (
        silver_streaming_df
        .filter(F.col("event_type").contains("QueryStartedEvent") | F.col("event_type").contains("QueryTerminatedEvent"))
        .groupBy("cluster_id", "app_id", "query_id")
        .agg(
            F.min(F.when(F.col("event_type").contains("QueryStartedEvent"), F.col("event_timestamp"))).alias("start_ts"),
            F.max(F.when(F.col("event_type").contains("QueryTerminatedEvent"), F.col("event_timestamp"))).alias("end_ts"),
            F.first(F.when(F.col("event_type").contains("QueryTerminatedEvent"), F.col("exception"))).alias("exception"),
        )
    )

    gold_streaming_summary_df = (
        _batch_agg
        .join(_lifecycle_df, on=["cluster_id", "app_id", "query_id"], how="left")
        .withColumn("is_stateful", F.col("_is_stateful_int") > 0)
        .drop("_is_stateful_int")
        .withColumn("duration_ms",
            F.when(F.col("end_ts").isNotNull() & F.col("start_ts").isNotNull(),
                (F.unix_timestamp("end_ts") - F.unix_timestamp("start_ts")) * 1000
            ).otherwise(None)
        )
        .withColumn("terminated_normally", F.col("exception").isNull() & F.col("end_ts").isNotNull())
        # ── ソースタイプ抽出 ──
        .withColumn("source_type",
            F.when(F.col("source_description").contains("CloudFilesSource"), F.lit("CloudFiles"))
             .when(F.col("source_description").contains("Kafka"), F.lit("Kafka"))
             .when(F.col("source_description").contains("Rate"), F.lit("Rate"))
             .when(F.col("source_description").contains("Socket"), F.lit("Socket"))
             .when(F.col("source_description").contains("EventHubs"), F.lit("EventHubs"))
             .when(F.col("source_description").isNotNull(), F.lit("Other"))
             .otherwise(F.lit("Unknown"))
        )
        .withColumn("sink_type",
            F.when(F.col("sink_description").contains("DeltaSink"), F.lit("Delta"))
             .when(F.col("sink_description").contains("ForeachBatchSink"), F.lit("ForeachBatch"))
             .when(F.col("sink_description").contains("Console"), F.lit("Console"))
             .when(F.col("sink_description").contains("Kafka"), F.lit("Kafka"))
             .when(F.col("sink_description").isNotNull(), F.lit("Other"))
             .otherwise(F.lit("Unknown"))
        )
        # ─── ボトルネック分類 ───
        # 優先度順: EXCEPTION > BACKLOG > SLOW_BATCH > STATE_GROWTH > WATERMARK_DROP >
        #           PLANNING_OVERHEAD > COMMIT_OVERHEAD > LOW_THROUGHPUT > OK
        .withColumn("bottleneck_type",
            F.when(F.col("exception").isNotNull(),
                F.lit("STREAM_EXCEPTION"))
             .when(
                (F.coalesce(F.col("last_files_outstanding").cast("long"), F.lit(0)) > 0) &
                (F.col("avg_processed_rows_per_sec") < F.col("avg_input_rows_per_sec")),
                F.lit("STREAM_BACKLOG"))
             .when(F.col("p95_batch_duration_ms") > 60000,
                F.lit("STREAM_SLOW_BATCH"))
             .when(
                (F.col("is_stateful") == True) &
                (F.coalesce(F.col("max_state_memory_bytes"), F.lit(0)) > 1024 * 1024 * 1024),
                F.lit("STREAM_STATE_GROWTH"))
             .when(
                (F.col("total_input_rows") > 0) &
                (F.coalesce(F.col("total_rows_dropped_by_watermark"), F.lit(0)) / F.col("total_input_rows") > 0.05),
                F.lit("STREAM_WATERMARK_DROP"))
             .when(
                (F.col("avg_trigger_execution_ms") > 0) &
                (F.col("avg_query_planning_ms") / F.col("avg_trigger_execution_ms") > 0.3),
                F.lit("STREAM_PLANNING_OVERHEAD"))
             .when(
                (F.col("avg_trigger_execution_ms") > 0) &
                (F.col("avg_commit_ms") / F.col("avg_trigger_execution_ms") > 0.2),
                F.lit("STREAM_COMMIT_OVERHEAD"))
             .when(
                (F.col("total_batches") > 5) &
                (F.coalesce(F.col("avg_processed_rows_per_sec"), F.lit(0)) < 10),
                F.lit("STREAM_LOW_THROUGHPUT"))
             .otherwise(F.lit("STREAM_OK"))
        )
        .withColumn("severity",
            F.when(F.col("bottleneck_type").isin("STREAM_EXCEPTION", "STREAM_BACKLOG"), F.lit("HIGH"))
             .when(F.col("bottleneck_type").isin("STREAM_SLOW_BATCH", "STREAM_STATE_GROWTH", "STREAM_WATERMARK_DROP"), F.lit("MEDIUM"))
             .when(F.col("bottleneck_type") == "STREAM_OK", F.lit("NONE"))
             .otherwise(F.lit("LOW"))
        )
        .withColumn("recommendation",
            F.when(F.col("bottleneck_type") == "STREAM_EXCEPTION",
                "例外メッセージを確認。一般的な原因: ソーススキーマ変更、権限エラー、一時的なネットワーク障害。")
             .when(F.col("bottleneck_type") == "STREAM_BACKLOG",
                "処理が取り込み速度に追いつけていない。クラスタサイズの拡大、処理ロジックの最適化、trigger(availableNow=True) でのキャッチアップを検討。")
             .when(F.col("bottleneck_type") == "STREAM_SLOW_BATCH",
                "バッチ処理が遅い。ソースデータのスキュー確認、変換処理の最適化、spark.sql.shuffle.partitions の増加を検討。")
             .when(F.col("bottleneck_type") == "STREAM_STATE_GROWTH",
                "状態ストアが大きい。ウォーターマーク設定の見直し、State TTL の短縮、RocksDB バックエンドの使用を検討。")
             .when(F.col("bottleneck_type") == "STREAM_WATERMARK_DROP",
                "大量のレコードが遅延到着により破棄されている。ウォーターマーク閾値の拡大、上流の遅延調査を検討。")
             .when(F.col("bottleneck_type") == "STREAM_PLANNING_OVERHEAD",
                "クエリプランニングがトリガー時間の大部分を占めている。クエリの簡素化、Delta CDF/スキーマ進化の確認を検討。")
             .when(F.col("bottleneck_type") == "STREAM_COMMIT_OVERHEAD",
                "コミットオーバーヘッドが高い。stateful クエリには RocksDB バックエンド、Delta シンクのログコンパクション設定を確認。")
             .when(F.col("bottleneck_type") == "STREAM_LOW_THROUGHPUT",
                "スループットが低い。リソース競合、バッチサイズの小ささ、トリガー間隔の設定ミスを確認。")
             .otherwise("ETLレベルのボトルネック未検出。トリガーラグ・スパイクはレポートのセクションFで検出。")
        )
        .withColumn("etl_loaded_at", F.current_timestamp())
        .select(
            "cluster_id", "app_id", "query_id", "run_id", "query_name",
            "source_type", "sink_type", "is_stateful",
            "total_batches", "start_ts", "end_ts", "duration_ms",
            "total_input_rows",
            "avg_input_rows_per_sec", "avg_processed_rows_per_sec",
            "avg_batch_duration_ms", "max_batch_duration_ms", "p95_batch_duration_ms",
            "avg_trigger_execution_ms", "avg_add_batch_ms", "avg_query_planning_ms",
            "avg_latest_offset_ms", "avg_commit_ms",
            "max_state_memory_bytes", "max_state_rows_total",
            "total_rows_dropped_by_watermark",
            "exception", "terminated_normally",
            "bottleneck_type", "severity", "recommendation",
            "etl_loaded_at",
        )
    )

    gold_streaming_summary_df = _save(
        gold_streaming_summary_df, "gold_streaming_query_summary",
        merge_keys=["cluster_id", "app_id", "query_id"]
    )
else:
    print("[Gold] gold_streaming_query_summary → skipped (no streaming events)")

# COMMAND ----------

# DBTITLE 1,gold_streaming_batch_detail
# ==============================================================================
# GOLD 9: gold_streaming_batch_detail
# ==============================================================================
if silver_streaming_df is not None and _streaming_count > 0:
    print("[Gold] gold_streaming_batch_detail ...")

    gold_streaming_batch_df = (
        silver_streaming_df
        .filter(F.col("event_type").contains("QueryProgressEvent"))
        .select(
            "cluster_id", "app_id", "query_id", "batch_id",
            "batch_duration_ms", "trigger_execution_ms",
            "query_planning_ms", "get_batch_ms", "add_batch_ms",
            "latest_offset_ms", "commit_offsets_ms", "commit_batch_ms", "wal_commit_ms",
            "num_input_rows", "input_rows_per_sec", "processed_rows_per_sec",
            "state_memory_used_bytes", "state_num_rows_total",
            "state_num_rows_updated", "state_rows_dropped_by_watermark",
            "state_commit_time_ms",
            "watermark",
            "event_timestamp",
        )
        .orderBy("cluster_id", "app_id", "query_id", "batch_id")
    )

    gold_streaming_batch_df = _save(
        gold_streaming_batch_df, "gold_streaming_batch_detail",
        merge_keys=["cluster_id", "app_id", "query_id", "batch_id"]
    )
else:
    print("[Gold] gold_streaming_batch_detail → skipped (no streaming events)")

# COMMAND ----------

# DBTITLE 1,アプリケーション別サマリーレポート
# ==============================================================================
# 複数セッション（アプリケーション）が検出された場合、
# 各アプリケーションごとの分析サマリーを表示
# ==============================================================================
print("=" * 80)
print("  アプリケーション別 分析サマリーレポート")
print("=" * 80)

_report_app  = spark.table(f"{SCHEMA}.{TABLE_PREFIX}gold_application_summary")
_report_bn   = spark.table(f"{SCHEMA}.{TABLE_PREFIX}gold_bottleneck_report")
_report_job  = spark.table(f"{SCHEMA}.{TABLE_PREFIX}gold_job_performance")
_report_jc   = spark.table(f"{SCHEMA}.{TABLE_PREFIX}gold_job_concurrency")
_report_spot = spark.table(f"{SCHEMA}.{TABLE_PREFIX}gold_spot_instance_analysis")
_report_stage = spark.table(f"{SCHEMA}.{TABLE_PREFIX}gold_stage_performance")

_apps = _report_app.orderBy("start_ts").collect()
print(f"\n検出されたアプリケーション数: {len(_apps)}")

for idx, app in enumerate(_apps, 1):
    app_id = app["app_id"]

    # ══════════════════════════════════════════════════════════════════════
    # 概要セクション
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'━' * 80}")
    print(f"  [{idx}/{len(_apps)}] App ID: {app_id}")
    print(f"{'━' * 80}")
    print(f"  App Name    : {app['app_name'] or 'N/A'}")
    print(f"  User        : {app['spark_user'] or 'N/A'}")
    print(f"  Start       : {app['start_ts']}")
    print(f"  End         : {app['end_ts']}")
    _dur = app['duration_min']
    print(f"  Duration    : {f'{_dur:.1f} min' if _dur is not None else 'N/A (実行中 or EndEvent なし)'}")
    print(f"  Stages      : {app['total_stages'] or 0} (完了: {app['completed_stages'] or 0}, 失敗: {app['failed_stages'] or 0})")
    print(f"  Tasks       : {app['total_tasks'] or 0}")
    print(f"  Shuffle     : {app['total_shuffle_gb'] or 0} GB")
    print(f"  Spill       : {app['total_spill_gb'] or 0} GB")
    print(f"  GC Overhead : {app['gc_overhead_pct'] or 0}%")

    # ══════════════════════════════════════════════════════════════════════
    # ジョブ分析セクション
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n  {'─' * 70}")
    print(f"  ■ ジョブ分析")
    print(f"  {'─' * 70}")

    # gold_job_performance と gold_job_concurrency を結合して
    # 実行時間順のジョブ一覧を作成
    _jobs = (
        _report_job
        .filter(F.col("app_id") == app_id)
        .join(
            _report_jc.select("app_id", "job_id", "job_total_tasks",
                              "concurrent_jobs_at_start",
                              "job_cpu_efficiency_pct", "total_gc_time_sec"),
            on=["app_id", "job_id"], how="left"
        )
        .filter(F.col("duration_ms").isNotNull())
        .orderBy(F.desc("duration_ms"))
        .collect()
    )

    _total_jobs = app['total_jobs'] or 0
    _succeeded = app['succeeded_jobs'] or 0
    _failed = app['failed_jobs'] or 0
    print(f"  合計: {_total_jobs} ジョブ (成功: {_succeeded}, 失敗: {_failed})")

    if _jobs:
        # ヘッダー
        print(f"\n  {'Job ID':>8s} | {'実行時間':>10s} | {'タスク数':>8s} | {'同時実行':>8s} | {'CPU効率':>7s} | {'GC(秒)':>7s} | {'結果'}")
        print(f"  {'─'*8}─┼─{'─'*10}─┼─{'─'*8}─┼─{'─'*8}─┼─{'─'*7}─┼─{'─'*7}─┼─{'─'*12}")
        for j in _jobs:
            dur_sec = (j["duration_ms"] or 0) / 1000.0
            tasks = j["job_total_tasks"]
            concurrent = j["concurrent_jobs_at_start"]
            cpu_eff = j["job_cpu_efficiency_pct"]
            gc_sec = j["total_gc_time_sec"]
            result = j["job_result"] or "N/A"
            # 長いジョブ・低CPU効率をマーク
            dur_str = f"{dur_sec:>8.1f} 秒"
            tasks_str = f"{tasks:>8d}" if tasks is not None else f"{'N/A':>8s}"
            conc_str = f"{concurrent:>8d}" if concurrent is not None else f"{'N/A':>8s}"
            cpu_str = f"{cpu_eff:>6.1f}%" if cpu_eff is not None else f"{'N/A':>7s}"
            gc_str = f"{gc_sec:>7.1f}" if gc_sec is not None else f"{'N/A':>7s}"
            # 警告マーク
            warn = ""
            if dur_sec > 60:
                warn += " ⏱"
            if cpu_eff is not None and cpu_eff < 50:
                warn += " ⚠CPU"
            if result != "JobSucceeded":
                warn += " ❌"
            print(f"  {j['job_id']:>8d} | {dur_str} | {tasks_str} | {conc_str} | {cpu_str} | {gc_str} | {result}{warn}")

        # ジョブサマリー統計
        dur_list = [(j["duration_ms"] or 0) / 1000.0 for j in _jobs]
        if dur_list:
            import statistics
            print(f"\n  ジョブ実行時間統計:")
            print(f"    合計: {sum(dur_list):.1f} 秒 | 平均: {statistics.mean(dur_list):.1f} 秒 | "
                  f"中央値: {statistics.median(dur_list):.1f} 秒 | 最大: {max(dur_list):.1f} 秒 | 最小: {min(dur_list):.1f} 秒")
            # 実行時間が突出しているジョブを検出
            if len(dur_list) > 2:
                _median = statistics.median(dur_list)
                _outliers = [j for j in _jobs if (j["duration_ms"] or 0) / 1000.0 > _median * 5 and _median > 0]
                if _outliers:
                    print(f"    ⚠ 中央値の5倍以上のジョブ: {len(_outliers)} 件 → チューニング対象")
    else:
        print(f"  ジョブデータなし")

    # ══════════════════════════════════════════════════════════════════════
    # ステージ分析セクション
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n  {'─' * 70}")
    print(f"  ■ ステージ分析（ボトルネック検出）")
    print(f"  {'─' * 70}")

    bn_rows = (
        _report_bn
        .filter(F.col("app_id") == app_id)
        .groupBy("bottleneck_type", "severity")
        .count()
        .orderBy(
            F.when(F.col("severity") == "HIGH", 1)
             .when(F.col("severity") == "MEDIUM", 2)
             .otherwise(3),
            F.desc("count")
        )
        .collect()
    )
    if bn_rows:
        print(f"  検出されたボトルネック:")
        for bn in bn_rows:
            print(f"    {bn['severity']:8s} | {bn['bottleneck_type']:25s} | {bn['count']} ステージ")
    else:
        print(f"  ✅ ボトルネックなし")

    # ボトルネックがあるステージの詳細 TOP5
    _worst_stages = (
        _report_stage
        .filter(F.col("app_id") == app_id)
        .filter(F.col("bottleneck_type") != "OK")
        .orderBy(F.desc("duration_ms"))
        .limit(5)
        .collect()
    )
    if _worst_stages:
        print(f"\n  ボトルネック ステージ TOP5:")
        print(f"  {'Stage':>7s} | {'実行時間':>10s} | {'タスク数':>8s} | {'スキュー比':>10s} | {'ボトルネック':20s} | {'推奨アクション'}")
        print(f"  {'─'*7}─┼─{'─'*10}─┼─{'─'*8}─┼─{'─'*10}─┼─{'─'*20}─┼─{'─'*30}")
        for s in _worst_stages:
            dur = (s["duration_ms"] or 0) / 1000.0
            tasks = s["num_tasks"] or 0
            skew = s["task_skew_ratio"]
            skew_str = f"{skew:>10.1f}" if skew is not None else f"{'N/A':>10s}"
            rec = (s["recommendation"] or "")[:50]
            print(f"  {s['stage_id']:>7d} | {dur:>8.1f} 秒 | {tasks:>8d} | {skew_str} | {s['bottleneck_type']:20s} | {rec}")

    # ══════════════════════════════════════════════════════════════════════
    # Spot インスタンスロスト セクション
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n  {'─' * 70}")
    print(f"  ■ Spot / ノードロスト分析")
    print(f"  {'─' * 70}")

    spot_rows = (
        _report_spot
        .filter(F.col("app_id") == app_id)
        .filter(F.col("is_unexpected_loss") == True)
        .orderBy("removed_ts")
        .collect()
    )
    if spot_rows:
        total_delay = sum((s['estimated_delay_sec'] or 0) for s in spot_rows)
        print(f"  ⚠ 予期しないロスト: {len(spot_rows)} 件  |  推定遅延合計: {total_delay:.1f} 秒 ({total_delay/60:.1f} 分)")
        for s in spot_rows:
            delay = s['estimated_delay_sec'] or 0
            shuffle_mb = s['shuffle_lost_mb'] or 0
            print(f"    Executor {s['executor_id']:4s} | {s['host'] or '?':15s} | "
                  f"稼働 {s['lifetime_min'] or 0:.1f}分 | {s['removal_type']} | "
                  f"影響タスク: {s['failed_tasks'] or 0} | "
                  f"Shuffle消失: {shuffle_mb:.0f}MB | "
                  f"推定遅延: {delay:.1f}秒")
            if s['delay_breakdown']:
                print(f"      内訳: {s['delay_breakdown']}")
        if spot_rows[0]["recommendation"]:
            print(f"\n  推奨設定:")
            for line in spot_rows[0]["recommendation"].split("\n"):
                print(f"    {line}")
    else:
        spot_all = (
            _report_spot
            .filter(F.col("app_id") == app_id)
            .filter(F.col("removal_type") != "STILL_RUNNING")
            .count()
        )
        if spot_all > 0:
            print(f"  ✅ Executor 削除 {spot_all} 件（すべて正常終了）")
        else:
            print(f"  ✅ ノードロストなし")

    # ══════════════════════════════════════════════════════════════════════
    # ストリーミング分析セクション
    # ══════════════════════════════════════════════════════════════════════
    _has_streaming = bool(app["has_streaming_queries"]) if "has_streaming_queries" in app.__fields__ else False
    if _has_streaming:
        print(f"\n  {'─' * 70}")
        print(f"  ■ ストリーミング分析")
        print(f"  {'─' * 70}")
        _report_streaming = spark.table(f"{SCHEMA}.{TABLE_PREFIX}gold_streaming_query_summary")
        _sq_rows = (
            _report_streaming
            .filter(F.col("app_id") == app_id)
            .orderBy(F.desc("avg_batch_duration_ms"))
            .collect()
        )
        if _sq_rows:
            print(f"  ストリーミングクエリ: {len(_sq_rows)} 件")
            print(f"\n  {'Query ID':>40s} | {'ソース':>12s} | {'シンク':>10s} | {'バッチ数':>8s} | {'平均処理時間':>12s} | {'ボトルネック':>20s}")
            print(f"  {'─'*40}─┼─{'─'*12}─┼─{'─'*10}─┼─{'─'*8}─┼─{'─'*12}─┼─{'─'*20}")
            for sq in _sq_rows:
                avg_dur = sq["avg_batch_duration_ms"]
                avg_dur_str = f"{avg_dur:>10.0f}ms" if avg_dur is not None else f"{'N/A':>12s}"
                print(f"  {sq['query_id'][:40]:>40s} | {(sq['source_type'] or 'N/A'):>12s} | {(sq['sink_type'] or 'N/A'):>10s} | {sq['total_batches']:>8d} | {avg_dur_str} | {sq['bottleneck_type']:>20s}")
        else:
            print(f"  ストリーミングクエリデータなし")

print(f"\n{'━' * 80}")

# アプリケーション間の比較テーブルも表示
if len(_apps) > 1:
    print("\n\n=== アプリケーション間比較 ===")
    display(
        _report_app
        .select(
            "app_id", "app_name", "start_ts", "end_ts",
            F.round("duration_min", 1).alias("duration_min"),
            "total_jobs", "succeeded_jobs", "failed_jobs",
            "total_stages", "total_tasks",
            "total_shuffle_gb", "total_spill_gb", "gc_overhead_pct",
        )
        .orderBy("start_ts")
    )

# COMMAND ----------

# DBTITLE 1,Pipeline Complete
# MAGIC %md
# MAGIC ## ✅ 完了
# MAGIC
# MAGIC 全 19 テーブルの書き込みが完了しました（ストリーミングイベントが含まれない場合は 16 テーブル）。
# MAGIC
# MAGIC | レイヤー | テーブル数 | テーブル名 |
# MAGIC | --- | --- | --- |
# MAGIC | Bronze | 1 | `PERF_bronze_raw_events` |
# MAGIC | Silver | 8-9 | `PERF_silver_application_events`, `PERF_silver_job_events`, `PERF_silver_stage_events`, `PERF_silver_task_events`, `PERF_silver_executor_events`, `PERF_silver_resource_profiles`, `PERF_silver_spark_config`, `PERF_silver_sql_executions`, *`PERF_silver_streaming_events`* |
# MAGIC | Gold | 8-10 | `PERF_gold_application_summary`, `PERF_gold_job_performance`, `PERF_gold_stage_performance`, `PERF_gold_executor_analysis`, `PERF_gold_spot_instance_analysis`, `PERF_gold_bottleneck_report`, `PERF_gold_job_concurrency`, `PERF_gold_sql_photon_analysis`, *`PERF_gold_streaming_query_summary`*, *`PERF_gold_streaming_batch_detail`* |
# MAGIC
# MAGIC *イタリック* のテーブルはストリーミングイベントが検出された場合のみ作成されます。
# MAGIC
# MAGIC 出力先スキーマ: `{SCHEMA}` (ウィジェットで変更可能)