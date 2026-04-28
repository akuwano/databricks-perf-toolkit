# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Performance — AI サマリー生成
# MAGIC
# MAGIC Gold テーブルのメトリクスを読み込み、選択したLLMモデルで分析テキストを生成して
# MAGIC ダッシュボードの「処理概要」「改善インパクト TOP3」テキストウィジェットを自動更新します。
# MAGIC
# MAGIC **実行タイミング:** DLT パイプライン完了後に Databricks Workflow からトリガーするか、手動で実行してください。

# COMMAND ----------

# MAGIC %pip install openai typing_extensions --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,ナレッジベース読み込み
import requests, base64

OPTIMIZATION_KNOWLEDGE_BASE = ""
BOTTLENECK_RECOMMENDATIONS = {}

try:
    _kb_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    _kb_token = _kb_ctx.apiToken().get()
    _kb_host = _kb_ctx.apiUrl().get().rstrip("/")
    _kb_path = "/Users/<your-email>/spark-perf-job/optimization_knowledge_base"

    _kb_resp = requests.get(
        f"{_kb_host}/api/2.0/workspace/export",
        headers={"Authorization": f"Bearer {_kb_token}"},
        params={"path": _kb_path, "format": "SOURCE"}
    )
    if _kb_resp.status_code == 200:
        _kb_code = base64.b64decode(_kb_resp.json()["content"]).decode("utf-8")
        # Databricks notebook のマジックコメントを除去して exec
        _kb_clean = "\n".join(
            line for line in _kb_code.split("\n")
            if not line.startswith("# Databricks notebook source")
            and not line.startswith("# MAGIC")
            and not line.startswith("# COMMAND ----------")
            and not line.startswith("# DBTITLE")
        )
        exec(_kb_clean, globals())
        print(f"✅ ナレッジベース読み込み完了 ({len(OPTIMIZATION_KNOWLEDGE_BASE)} chars, {len(BOTTLENECK_RECOMMENDATIONS)} ボトルネックタイプ)")
    else:
        print(f"⚠ ナレッジベース取得失敗: {_kb_resp.status_code}")
except Exception as e:
    print(f"⚠ ナレッジベースの読み込みに失敗: {e}")
    print("  LLM分析はナレッジなしで実行されます。")

# COMMAND ----------

# DBTITLE 1,Configuration — Widgets
# ┌─────────────────────────────────────────────────────────────────────────┐
# │  CONFIGURATION — ウィジェットで動的に変更できます                         │
# └─────────────────────────────────────────────────────────────────────────┘

dbutils.widgets.text("catalog",        "main",                              "Catalog")
dbutils.widgets.text("schema",         "base2",                             "Schema")
dbutils.widgets.text("table_prefix",   "PERF_",                             "Table Name Prefix")
# dashboard_id は 03_create_dashboard_notebook に移動済み
# ──────────────────────────────────────────────────────────────────────────
# 使用するモデルの Databricks Model Serving エンドポイント名を指定してください。
#
# Databricks Hosted (Pay-per-token) の主要モデル:
#   "databricks-claude-sonnet-4"      ← Claude Sonnet 4    (精度とコストのバランス・推奨)
#   "databricks-claude-opus-4"        ← Claude Opus 4      (最高精度)
#   "databricks-meta-llama-3-3-70b-instruct"  ← Llama 3.3 70B (OSS)
#
# External Model エンドポイント（自前で登録した場合）:
#   登録したエンドポイント名をそのまま指定してください。
# ──────────────────────────────────────────────────────────────────────────
dbutils.widgets.text("model_endpoint", "databricks-claude-sonnet-4",        "LLM Model Endpoint")
dbutils.widgets.dropdown("output_lang", "en", ["ja", "en"],                 "Output Language")
dbutils.widgets.text("experiment_id", "", "Experiment ID")
dbutils.widgets.text("variant", "", "Variant")

CATALOG        = dbutils.widgets.get("catalog")
_SCHEMA        = dbutils.widgets.get("schema")
SCHEMA         = f"{CATALOG}.{_SCHEMA}"
TABLE_PREFIX   = dbutils.widgets.get("table_prefix")
# DASHBOARD_ID は 03 で管理
MODEL_ENDPOINT = dbutils.widgets.get("model_endpoint")
OUTPUT_LANG    = dbutils.widgets.get("output_lang")
EXPERIMENT_ID  = dbutils.widgets.get("experiment_id")
VARIANT        = dbutils.widgets.get("variant")

# ──────────────────────────────────────────────────────────────────────────
# 分析対象アプリケーション選択
# gold_application_summary から検出されたアプリケーション一覧を取得し、
# ドロップダウンで選択できるようにする。デフォルトは最新の App_ID。
# ──────────────────────────────────────────────────────────────────────────
_app_rows = spark.sql(f"""
    SELECT app_id, app_name, cluster_id, start_ts,
           ROUND(duration_min, 1) AS duration_min,
           total_jobs
    FROM {SCHEMA}.{TABLE_PREFIX}gold_application_summary
    ORDER BY start_ts
""").collect()

_app_choices = [r["app_id"] for r in _app_rows]
_default_app = _app_rows[-1]["app_id"] if _app_rows else _app_choices[0]
dbutils.widgets.dropdown("app_id", _default_app, _app_choices, "Analysis Target App ID")

APP_ID = dbutils.widgets.get("app_id")

print("検出されたアプリケーション:")
for r in _app_rows:
    marker = " ← 選択中" if r["app_id"] == APP_ID else ""
    dur = f"{r['duration_min']}分" if r["duration_min"] else "実行中/不明"
    jobs = r["total_jobs"] or "?"
    cid = r["cluster_id"] or "N/A"
    print(f"  {r['app_id']}  cluster: {cid}  ({r['app_name'] or 'N/A'})  started: {r['start_ts']}  duration: {dur}  jobs: {jobs}{marker}")
# app_id フィルタ条件（SQL用）
APP_FILTER = f"AND app_id = '{APP_ID}'"
# app_id ラベル（レポート表示用）
APP_LABEL = APP_ID

# 生成履歴を保存するテーブル（自動作成されます）
HISTORY_TABLE = f"{SCHEMA}.{TABLE_PREFIX}gold_narrative_summary"

# COMMAND ----------

import json
import requests
from datetime import datetime, timezone
from openai import OpenAI
from pyspark.sql import functions as F

# 認証情報を Databricks コンテキストから自動取得
ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
host  = ctx.apiUrl().get().rstrip("/")

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# OpenAI 互換クライアント（Databricks Model Serving）
client = OpenAI(
    api_key=token,
    base_url=f"{host}/serving-endpoints"
)

print(f"Model  : {MODEL_ENDPOINT}")
print(f"Schema : {SCHEMA}")
print(f"Host   : {host}")
print(f"Experiment: {EXPERIMENT_ID or '(none)'}")
print(f"Variant   : {VARIANT or '(none)'}")

# COMMAND ----------

# MAGIC %md ## 1. Gold テーブルからメトリクスを収集

# COMMAND ----------

from decimal import Decimal

from datetime import datetime as _dt, date as _date

def _json_safe(v):
    """Decimal / datetime / date → JSON シリアライズ可能な型に変換"""
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, _dt):
        return v.isoformat()
    if isinstance(v, _date):
        return v.isoformat()
    return v

def df_to_dict_list(df, limit=5):
    return [{k: _json_safe(v) for k, v in row.asDict().items()}
            for row in df.limit(limit).collect()]

print(f"分析対象: {APP_LABEL}")

# アプリケーションサマリー
# ALL の場合は全アプリの合算、個別の場合は該当アプリのみ
app_summary = spark.sql(f"""
    SELECT
        app_id, app_name, cluster_id,
        cluster_name, worker_node_type, driver_node_type,
        min_workers, max_workers, dbr_version,
        cluster_availability, region,
        CAST(start_ts AS STRING) AS start_ts,
        CAST(end_ts AS STRING) AS end_ts,
        total_jobs, succeeded_jobs, failed_jobs,
        ROUND(job_success_rate, 1)   AS job_success_rate,
        total_stages, completed_stages, failed_stages, total_tasks,
        ROUND(duration_min, 1)       AS duration_min,
        ROUND(duration_ms / 1000.0, 1) AS duration_sec,
        ROUND(total_shuffle_gb, 2)   AS total_shuffle_gb,
        ROUND(total_spill_gb, 2)     AS total_spill_gb,
        ROUND(gc_overhead_pct, 1)    AS gc_overhead_pct,
        ROUND(total_exec_run_ms / 3600000.0, 1) AS total_exec_run_hours
    FROM {SCHEMA}.{TABLE_PREFIX}gold_application_summary
    WHERE 1=1 {APP_FILTER}
    ORDER BY start_ts
""").collect()
app_rows_list = [{k: _json_safe(v) for k, v in r.asDict().items()} for r in app_summary]
# プロンプト用にはサマリー（ALL なら全アプリ、個別なら1つ）
app_row = app_rows_list[0] if len(app_rows_list) == 1 else {
    "app_count": len(app_rows_list),
    "apps": app_rows_list,
}

# ボトルネック件数サマリー
bn_summary = spark.sql(f"""
    SELECT bottleneck_type, severity, COUNT(*) AS cnt
    FROM {SCHEMA}.{TABLE_PREFIX}gold_bottleneck_report
    WHERE 1=1 {APP_FILTER}
    GROUP BY bottleneck_type, severity
    ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END, cnt DESC
""")
bn_rows = df_to_dict_list(bn_summary, 20)

# I/O サマリー（ステージ全体の Input/Output/Shuffle 集計）
io_summary = spark.sql(f"""
    SELECT ROUND(SUM(input_mb) / 1024.0, 2)          AS total_input_gb,
           ROUND(SUM(output_mb) / 1024.0, 2)         AS total_output_gb,
           ROUND(SUM(shuffle_read_mb) / 1024.0, 2)   AS total_shuffle_read_gb,
           ROUND(SUM(shuffle_write_mb) / 1024.0, 2)  AS total_shuffle_write_gb,
           ROUND(SUM(disk_spill_mb) / 1024.0, 2)     AS total_disk_spill_gb,
           ROUND(SUM(memory_spill_mb) / 1024.0, 2)   AS total_memory_spill_gb,
           SUM(num_tasks) AS total_tasks,
           COUNT(*) AS completed_stage_count
    FROM {SCHEMA}.{TABLE_PREFIX}gold_stage_performance
    WHERE status NOT IN ('SKIPPED') {APP_FILTER}
""")
io_summary_rows = df_to_dict_list(io_summary, 1)

# Scan メトリクスサマリー（キャッシュヒット率・プルーニング率）
scan_metrics_summary = spark.sql(f"""
    SELECT ROUND(AVG(cache_hit_pct), 1) AS avg_cache_hit_pct,
           ROUND(SUM(cache_hit_bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS total_cache_hit_gb,
           ROUND(SUM(cache_miss_bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS total_cache_miss_gb,
           ROUND(AVG(file_pruning_pct), 1) AS avg_file_pruning_pct,
           SUM(files_read) AS total_files_read,
           SUM(files_pruned) AS total_files_pruned,
           ROUND(SUM(files_read_size_mb) / 1024.0, 2) AS total_files_read_gb,
           ROUND(SUM(scan_time_ms) / 1000.0, 1) AS total_scan_time_sec,
           SUM(cloud_request_count) AS total_cloud_requests,
           ROUND(SUM(cloud_request_dur_ms) / 1000.0, 1) AS total_cloud_request_sec
    FROM {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis
    WHERE cache_hit_pct IS NOT NULL OR files_read > 0 {'' if not APP_FILTER else APP_FILTER}
""")
scan_metrics_rows = df_to_dict_list(scan_metrics_summary, 1)

# ジョブ特性（処理パターンの傾向 — データ生成型 vs I/O型 vs Shuffle型）
job_characteristics = spark.sql(f"""
    SELECT j.job_id,
           ROUND(j.duration_ms / 1000.0, 1) AS duration_sec,
           j.stage_ids,
           COALESCE(d.has_bottleneck, 'NO') AS has_bottleneck,
           COALESCE(d.bottleneck_summary, '') AS bottleneck_summary,
           d.total_tasks_all,
           ROUND(COALESCE(s.job_input_mb, 0), 1)  AS input_mb,
           ROUND(COALESCE(s.job_output_mb, 0), 1)  AS output_mb,
           ROUND(COALESCE(s.job_shuffle_read_mb, 0), 1)  AS shuffle_read_mb,
           ROUND(COALESCE(s.job_shuffle_write_mb, 0), 1) AS shuffle_write_mb,
           ROUND(COALESCE(s.job_disk_spill_mb, 0), 1) AS disk_spill_mb
    FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance j
    LEFT JOIN {SCHEMA}.{TABLE_PREFIX}gold_job_detail d
      ON j.app_id = d.app_id AND j.job_id = d.job_id
    LEFT JOIN (
      SELECT jm.cluster_id, jm.app_id, jm.job_id,
             SUM(sp.input_mb)         AS job_input_mb,
             SUM(sp.output_mb)        AS job_output_mb,
             SUM(sp.shuffle_read_mb)  AS job_shuffle_read_mb,
             SUM(sp.shuffle_write_mb) AS job_shuffle_write_mb,
             SUM(sp.disk_spill_mb)    AS job_disk_spill_mb
      FROM (
        SELECT cluster_id, app_id, job_id, CAST(sid AS INT) AS stage_id
        FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance
        LATERAL VIEW EXPLODE(FROM_JSON(stage_ids, 'ARRAY<INT>')) t AS sid
        WHERE 1=1 {APP_FILTER}
      ) jm
      INNER JOIN {SCHEMA}.{TABLE_PREFIX}gold_stage_performance sp
        ON jm.cluster_id = sp.cluster_id AND jm.app_id = sp.app_id AND jm.stage_id = sp.stage_id
      GROUP BY jm.cluster_id, jm.app_id, jm.job_id
    ) s ON j.cluster_id = s.cluster_id AND j.app_id = s.app_id AND j.job_id = s.job_id
    WHERE j.duration_ms IS NOT NULL {APP_FILTER.replace('app_id', 'j.app_id')}
    ORDER BY j.duration_ms DESC
    LIMIT 5
""")
job_char_rows = df_to_dict_list(job_characteristics, 5)

# 遅いジョブ TOP5
slow_jobs = spark.sql(f"""
    SELECT app_id, job_id, ROUND(duration_ms / 1000.0, 1) AS duration_sec, job_result, stage_ids
    FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance
    WHERE 1=1 {APP_FILTER}
    ORDER BY duration_ms DESC NULLS LAST
    LIMIT 5
""")
slow_job_rows = df_to_dict_list(slow_jobs)

# ボトルネック件数サマリー（種別ごとの件数）
bn_count_summary = spark.sql(f"""
    SELECT bottleneck_type, severity, COUNT(*) AS cnt,
           ROUND(SUM(duration_ms) / 1000.0, 1) AS total_sec
    FROM {SCHEMA}.{TABLE_PREFIX}gold_stage_performance
    WHERE bottleneck_type NOT IN ('OK', 'SKIPPED') {APP_FILTER}
    GROUP BY bottleneck_type, severity
    ORDER BY total_sec DESC
""")
bn_count_summary_rows = df_to_dict_list(bn_count_summary, 20)

# ボトルネック検出ステージ TOP5（実行時間順、SKIPPED 除外）
# job_id は gold_job_performance の stage_ids から逆引き
_stage_query = f"""
    SELECT s.app_id, s.stage_id, jm.job_id, s.stage_name, s.bottleneck_type, s.severity,
           ROUND(s.duration_ms/1000.0, 1) AS duration_sec,
           s.num_tasks,
           ROUND(s.task_skew_ratio, 1)    AS task_skew_ratio,
           ROUND(s.shuffle_read_mb, 0)    AS shuffle_read_mb,
           ROUND(s.shuffle_write_mb, 0)   AS shuffle_write_mb,
           ROUND(s.gc_overhead_pct, 1)    AS gc_overhead_pct,
           ROUND(s.cpu_efficiency_pct, 1) AS cpu_efficiency_pct,
           ROUND(s.disk_spill_mb, 0)      AS disk_spill_mb,
           ROUND(s.memory_spill_mb, 0)    AS memory_spill_mb,
           ROUND(s.input_mb, 1)           AS input_mb,
           s.recommendation,
           jd.sql_execution_id,
           sq.scan_tables,
           sq.scan_filters
    FROM {SCHEMA}.{TABLE_PREFIX}gold_stage_performance s
    LEFT JOIN (
      SELECT cluster_id, app_id, job_id, CAST(sid AS INT) AS stage_id
      FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance
      LATERAL VIEW EXPLODE(FROM_JSON(stage_ids, 'ARRAY<INT>')) t AS sid
      WHERE 1=1 {APP_FILTER}
    ) jm ON s.cluster_id = jm.cluster_id AND s.app_id = jm.app_id AND s.stage_id = jm.stage_id
    LEFT JOIN {SCHEMA}.{TABLE_PREFIX}gold_job_detail jd
      ON jm.app_id = jd.app_id AND jm.job_id = jd.job_id
    LEFT JOIN {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis sq
      ON jd.app_id = sq.app_id AND jd.sql_execution_id = sq.execution_id
"""
# ボトルネックステージ（上位5件）
bn_stages = spark.sql(f"""
    {_stage_query}
    WHERE s.bottleneck_type NOT IN ('OK', 'SKIPPED') {APP_FILTER.replace('app_id', 's.app_id')}
    ORDER BY s.duration_ms DESC NULLS LAST
    LIMIT 5
""")
bn_stage_rows = df_to_dict_list(bn_stages, 5)

# 正常ステージ（上位5件、実行時間順）
ok_stages = spark.sql(f"""
    {_stage_query}
    WHERE s.bottleneck_type = 'OK' {APP_FILTER.replace('app_id', 's.app_id')}
    ORDER BY s.duration_ms DESC NULLS LAST
    LIMIT 5
""")
ok_stage_rows = df_to_dict_list(ok_stages, 5)

worst_stage_rows = bn_stage_rows + ok_stage_rows

# CPU・並列実行（効率が低いジョブ TOP5）
cpu_rows = spark.sql(f"""
    SELECT app_id, job_id,
           ROUND(duration_sec, 1)           AS duration_sec,
           concurrent_jobs_at_start,
           ROUND(job_cpu_efficiency_pct, 1) AS cpu_efficiency_pct,
           ROUND(total_gc_time_sec, 1)      AS gc_time_sec
    FROM {SCHEMA}.{TABLE_PREFIX}gold_job_concurrency
    WHERE 1=1 {APP_FILTER}
    ORDER BY job_cpu_efficiency_pct ASC NULLS LAST
    LIMIT 5
""")
cpu_job_rows = df_to_dict_list(cpu_rows)

# 並列実行サマリー
concurrency_summary = spark.sql(f"""
    SELECT
        MAX(concurrent_jobs_at_start) AS max_concurrent,
        ROUND(AVG(concurrent_jobs_at_start), 1) AS avg_concurrent,
        SUM(CASE WHEN concurrent_jobs_at_start > 0 THEN 1 ELSE 0 END) AS jobs_with_concurrency,
        COUNT(*) AS total_jobs,
        ROUND(AVG(CASE WHEN concurrent_jobs_at_start > 0 THEN job_cpu_efficiency_pct END), 1) AS avg_cpu_when_concurrent,
        ROUND(AVG(CASE WHEN concurrent_jobs_at_start = 0 THEN job_cpu_efficiency_pct END), 1) AS avg_cpu_when_solo,
        ROUND(AVG(CASE WHEN concurrent_jobs_at_start > 0 THEN duration_sec END), 1) AS avg_dur_when_concurrent,
        ROUND(AVG(CASE WHEN concurrent_jobs_at_start = 0 THEN duration_sec END), 1) AS avg_dur_when_solo
    FROM {SCHEMA}.{TABLE_PREFIX}gold_job_concurrency
    WHERE 1=1 {APP_FILTER}
""")
concurrency_summary_rows = df_to_dict_list(concurrency_summary, 1)

# 並列実行時に影響を受けたジョブ（concurrent > 0 のジョブ）
concurrent_jobs = spark.sql(f"""
    SELECT job_id, concurrent_jobs_at_start,
           ROUND(duration_sec, 1) AS duration_sec,
           ROUND(job_cpu_efficiency_pct, 1) AS cpu_efficiency_pct,
           ROUND(total_gc_time_sec, 1) AS gc_time_sec,
           job_total_tasks
    FROM {SCHEMA}.{TABLE_PREFIX}gold_job_concurrency
    WHERE concurrent_jobs_at_start > 0 {APP_FILTER}
    ORDER BY concurrent_jobs_at_start DESC, duration_sec DESC
    LIMIT 5
""")
concurrent_job_rows = df_to_dict_list(concurrent_jobs, 5)

# クロスアプリ並列実行サマリー
cross_app_summary = spark.sql(f"""
    SELECT
        SUM(CASE WHEN has_cross_app_concurrency = 'YES' THEN 1 ELSE 0 END) AS jobs_with_cross_app,
        COUNT(*) AS total_jobs,
        MAX(cross_app_concurrent_jobs) AS max_cross_app_concurrent,
        COLLECT_SET(
            CASE WHEN has_cross_app_concurrency = 'YES' THEN concurrent_app_list END
        ) AS concurrent_apps
    FROM {SCHEMA}.{TABLE_PREFIX}gold_cross_app_concurrency
    WHERE 1=1 {APP_FILTER}
""")
cross_app_summary_rows = df_to_dict_list(cross_app_summary, 1)

# クロスアプリ並列ジョブ詳細（上位5件）
cross_app_jobs = spark.sql(f"""
    SELECT job_id, cross_app_concurrent_jobs, concurrent_app_list,
           CAST(submit_ts AS STRING) AS submit_ts,
           CAST(complete_ts AS STRING) AS complete_ts,
           ROUND(duration_ms / 1000.0, 1) AS duration_sec
    FROM {SCHEMA}.{TABLE_PREFIX}gold_cross_app_concurrency
    WHERE has_cross_app_concurrency = 'YES' {APP_FILTER}
    ORDER BY cross_app_concurrent_jobs DESC
    LIMIT 5
""")
cross_app_job_rows = df_to_dict_list(cross_app_jobs, 5)

# Shuffle パーティション分析（タスクあたりのデータ量 — 128MB目標）
shuffle_partition_rows = spark.sql(f"""
    SELECT stage_id, num_tasks,
           ROUND(shuffle_read_mb, 0) AS shuffle_read_mb,
           ROUND(shuffle_write_mb, 0) AS shuffle_write_mb,
           ROUND(shuffle_read_mb / NULLIF(num_tasks, 0), 1) AS shuffle_read_per_task_mb,
           ROUND(shuffle_write_mb / NULLIF(num_tasks, 0), 1) AS shuffle_write_per_task_mb,
           CASE
             WHEN shuffle_read_mb / NULLIF(num_tasks, 0) < 10 AND shuffle_read_mb > 100
               THEN 'OVER_PARTITIONED'
             WHEN shuffle_read_mb / NULLIF(num_tasks, 0) > 200
               THEN 'UNDER_PARTITIONED'
             ELSE 'OK'
           END AS partition_sizing,
           CAST(ROUND(shuffle_read_mb / 128.0, 0) AS INT) AS recommended_partitions
    FROM {SCHEMA}.{TABLE_PREFIX}gold_stage_performance
    WHERE (shuffle_read_mb > 10 OR shuffle_write_mb > 10) {APP_FILTER}
      AND status != 'SKIPPED'
    ORDER BY shuffle_read_mb DESC
    LIMIT 5
""")
shuffle_partition_data = df_to_dict_list(shuffle_partition_rows, 5)

# Small Files 推定（Read ステージのタスクあたり読み取り量）
small_files_rows = spark.sql(f"""
    SELECT stage_id, num_tasks,
           ROUND(input_mb, 1) AS input_mb,
           ROUND(input_mb / NULLIF(num_tasks, 0), 1) AS input_per_task_mb,
           CASE
             WHEN input_mb > 10 AND input_mb / NULLIF(num_tasks, 0) < 10
               THEN 'SMALL_FILES: タスクあたり読み取りが小さい（目標128MB）。ファイルが細分化されている可能性'
             WHEN input_mb > 10 AND input_mb / NULLIF(num_tasks, 0) > 256
               THEN 'LARGE_FILES: タスクあたり読み取りが大きすぎる。パーティション数増加を検討'
             ELSE 'OK'
           END AS file_sizing
    FROM {SCHEMA}.{TABLE_PREFIX}gold_stage_performance
    WHERE input_mb > 0 {APP_FILTER}
      AND status != 'SKIPPED'
    ORDER BY input_mb DESC
    LIMIT 5
""")
small_files_data = df_to_dict_list(small_files_rows, 5)

# Photon 利用率サマリー
photon_summary = spark.sql(f"""
    SELECT ROUND(AVG(photon_pct), 1) AS avg_photon_pct,
           COUNT(*) AS total_sql_count,
           SUM(CASE WHEN photon_pct < 50 THEN 1 ELSE 0 END) AS low_photon_count,
           SUM(photon_operators) AS total_photon_ops,
           SUM(total_operators) AS total_ops
    FROM {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis
    WHERE duration_sec IS NOT NULL {APP_FILTER}
""")
photon_summary_rows = df_to_dict_list(photon_summary, 1)

# Photon 利用率（低いSQL TOP5、Photon Explanation ありを優先）+ 関連ジョブID
photon_rows = spark.sql(f"""
    SELECT p.execution_id,
           ROUND(p.duration_sec, 1) AS duration_sec,
           ROUND(p.photon_pct, 1)   AS photon_pct,
           p.total_operators, p.photon_operators,
           p.non_photon_op_list,
           p.photon_explanation,
           p.scan_tables,
           j.job_ids
    FROM {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis p
    LEFT JOIN (
        SELECT app_id, sql_execution_id,
               CONCAT_WS(', ', COLLECT_SET(CAST(job_id AS STRING))) AS job_ids
        FROM {SCHEMA}.{TABLE_PREFIX}gold_job_detail
        WHERE sql_execution_id IS NOT NULL {APP_FILTER}
        GROUP BY app_id, sql_execution_id
    ) j ON p.app_id = j.app_id AND p.execution_id = j.sql_execution_id
    WHERE p.duration_sec IS NOT NULL AND p.photon_pct < 50 {APP_FILTER.replace('app_id', 'p.app_id')}
    ORDER BY
        CASE WHEN p.photon_explanation IS NOT NULL AND p.photon_explanation != '' THEN 0 ELSE 1 END,
        p.duration_sec DESC NULLS LAST
    LIMIT 5
""")
photon_job_rows = df_to_dict_list(photon_rows)

# ジョブ分析（実行時間順、タスク数・CPU効率付き）
job_detail_rows_df = spark.sql(f"""
    SELECT j.app_id, j.job_id, j.status, j.job_result,
           ROUND(j.duration_ms / 1000.0, 1) AS duration_sec,
           ROUND(j.duration_min, 2) AS duration_min,
           j.stage_ids,
           c.job_total_tasks,
           c.concurrent_jobs_at_start,
           ROUND(c.job_cpu_efficiency_pct, 1) AS cpu_efficiency_pct,
           ROUND(c.total_gc_time_sec, 1)      AS gc_time_sec,
           ROUND(c.total_cpu_time_sec, 1)     AS cpu_time_sec,
           ROUND(c.total_exec_run_time_sec,1) AS exec_run_time_sec,
           t.succeeded_tasks, t.failed_tasks, t.total_tasks_all,
           COALESCE(bn.bottleneck_summary, 'NONE') AS bottleneck_summary
    FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance j
    LEFT JOIN {SCHEMA}.{TABLE_PREFIX}gold_job_concurrency c
      ON j.app_id = c.app_id AND j.job_id = c.job_id
    LEFT JOIN (
      SELECT tk.cluster_id, tk.app_id, sm.job_id,
        COUNT(*) AS total_tasks_all,
        SUM(CASE WHEN tk.task_result = 'Success' THEN 1 ELSE 0 END) AS succeeded_tasks,
        SUM(CASE WHEN tk.task_result != 'Success' THEN 1 ELSE 0 END) AS failed_tasks
      FROM {SCHEMA}.{TABLE_PREFIX}silver_task_events tk
      INNER JOIN (
        SELECT cluster_id, app_id, job_id, CAST(stage_id_val AS INT) AS stage_id
        FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance
        LATERAL VIEW EXPLODE(FROM_JSON(stage_ids, 'ARRAY<INT>')) t AS stage_id_val
      ) sm ON tk.cluster_id = sm.cluster_id AND tk.app_id = sm.app_id AND tk.stage_id = sm.stage_id
      GROUP BY tk.cluster_id, tk.app_id, sm.job_id
    ) t ON j.cluster_id = t.cluster_id AND j.app_id = t.app_id AND j.job_id = t.job_id
    LEFT JOIN (
      SELECT b.cluster_id, b.app_id, b.job_id,
        CONCAT_WS('; ', COLLECT_SET(
          CONCAT(b.bottleneck_type, '(S', CAST(b.stage_id AS STRING), ')')
        )) AS bottleneck_summary
      FROM {SCHEMA}.{TABLE_PREFIX}gold_bottleneck_report b
      GROUP BY b.cluster_id, b.app_id, b.job_id
    ) bn ON j.cluster_id = bn.cluster_id AND j.app_id = bn.app_id AND j.job_id = bn.job_id
    WHERE j.duration_ms IS NOT NULL {APP_FILTER.replace('app_id', 'j.app_id')}
    ORDER BY j.duration_ms DESC
    LIMIT 20
""")
job_detail_rows = df_to_dict_list(job_detail_rows_df, 20)

# ノード（Executor）分析 — リソース情報・ライフサイクル・Spot ロスト・リソース診断
node_analysis_rows_df = spark.sql(f"""
    SELECT e.app_id, e.executor_id, e.host,
           e.total_cores, e.add_ts, e.remove_ts, e.removed_reason,
           ROUND(e.avg_task_ms / 1000.0, 1)    AS avg_task_sec,
           e.total_tasks,
           ROUND(e.total_task_ms / 1000.0, 1)  AS total_task_sec,
           ROUND(e.avg_gc_pct, 1)               AS avg_gc_pct,
           ROUND(e.avg_cpu_efficiency_pct, 1)   AS avg_cpu_efficiency_pct,
           ROUND(e.cpu_utilization_pct, 1)      AS cpu_utilization_pct,
           ROUND(e.gc_pct, 1)                   AS gc_pct,
           ROUND(e.input_gb, 2)                 AS input_gb,
           ROUND(e.shuffle_read_gb, 2)          AS shuffle_read_gb,
           ROUND(e.shuffle_write_gb, 2)         AS shuffle_write_gb,
           ROUND(e.peak_memory_mb, 0)           AS peak_memory_mb,
           ROUND(e.memory_spill_mb, 1)          AS memory_spill_mb,
           ROUND(e.disk_spill_mb, 1)            AS disk_spill_mb,
           e.onheap_memory_mb, e.offheap_memory_mb, e.task_cpus,
           CAST(e.is_straggler AS STRING)       AS is_straggler,
           CAST(e.is_underutilized AS STRING)   AS is_underutilized,
           ROUND(e.load_vs_avg, 2)              AS load_vs_avg,
           ROUND(e.z_score, 2)                  AS z_score,
           e.has_resource_issue,
           e.resource_diagnosis
    FROM {SCHEMA}.{TABLE_PREFIX}gold_executor_analysis e
    WHERE 1=1 {APP_FILTER.replace('app_id', 'e.app_id')}
    ORDER BY e.total_task_ms DESC
""")
node_analysis_rows = df_to_dict_list(node_analysis_rows_df, 30)

# Spot ロスト分析
spot_analysis_rows_df = spark.sql(f"""
    SELECT app_id, executor_id, host, removal_type,
           is_unexpected_loss,
           ROUND(lifetime_min, 1) AS lifetime_min,
           ROUND(estimated_delay_sec, 1) AS estimated_delay_sec,
           ROUND(shuffle_lost_mb, 0) AS shuffle_lost_mb,
           failed_tasks, total_tasks_assigned,
           delay_breakdown
    FROM {SCHEMA}.{TABLE_PREFIX}gold_spot_instance_analysis
    WHERE 1=1 {APP_FILTER}
    ORDER BY is_unexpected_loss DESC, lifetime_min ASC
""")
spot_analysis_rows = df_to_dict_list(spot_analysis_rows_df, 30)

# Executor 数の変動（ノード増減の時系列）— gold_autoscale_timeline から取得
try:
    node_timeline_df = spark.sql(f"""
        SELECT event_ts, event_type, event_reason,
               executor_id, host, total_cores,
               worker_count_before, worker_count_after,
               ROUND(segment_duration_sec, 1) AS segment_duration_sec,
               active_stage_count, total_active_tasks,
               active_stage_ids, active_stage_names,
               active_bottleneck_types, max_stage_severity,
               ROUND(active_spill_mb, 1) AS active_spill_mb,
               ROUND(active_shuffle_mb, 1) AS active_shuffle_mb
        FROM {SCHEMA}.{TABLE_PREFIX}gold_autoscale_timeline
        WHERE 1=1 {APP_FILTER}
        ORDER BY event_ts
    """)
    node_timeline_rows = df_to_dict_list(node_timeline_df, 50)
except Exception:
    # Fallback to silver events if gold_autoscale_timeline doesn't exist yet
    node_timeline_df = spark.sql(f"""
        SELECT event_type, executor_id, host,
               CAST(timestamp_ts AS STRING) AS event_ts,
               COALESCE(removed_reason, '') AS removed_reason
        FROM {SCHEMA}.{TABLE_PREFIX}silver_executor_events
        WHERE 1=1 {APP_FILTER}
        ORDER BY timestamp_ts
    """)
    node_timeline_rows = df_to_dict_list(node_timeline_df, 50)

# プラン最適化時間分析（ジョブ実行時間 vs ステージ実行時間の差分）
plan_opt_summary = spark.sql(f"""
    SELECT
      ROUND(SUM(j.duration_ms)/1000.0, 1) AS total_job_dur_sec,
      ROUND(MAX(a.duration_min) * 60, 1) AS total_app_dur_sec,
      COUNT(DISTINCT j.job_id) AS total_jobs
    FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance j
    CROSS JOIN (
      SELECT duration_min FROM {SCHEMA}.{TABLE_PREFIX}gold_application_summary
      WHERE 1=1 {APP_FILTER} LIMIT 1
    ) a
    WHERE j.duration_ms IS NOT NULL {APP_FILTER.replace('app_id', 'j.app_id')}
""")
plan_opt_summary_rows = df_to_dict_list(plan_opt_summary, 1)

# ジョブごとのオーバーヘッド（上位5件）
plan_opt_jobs = spark.sql(f"""
    SELECT j.job_id,
      ROUND(j.duration_ms/1000.0, 1) AS job_dur_sec,
      ROUND(COALESCE(s.stage_dur_ms, 0)/1000.0, 1) AS stage_dur_sec,
      ROUND((j.duration_ms - COALESCE(s.stage_dur_ms, 0))/1000.0, 1) AS overhead_sec,
      s.completed_stages, s.skipped_stages
    FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance j
    LEFT JOIN (
      SELECT jm.cluster_id, jm.app_id, jm.job_id,
        SUM(CASE WHEN sp.status != 'SKIPPED' THEN sp.duration_ms ELSE 0 END) AS stage_dur_ms,
        COUNT(CASE WHEN sp.status != 'SKIPPED' THEN 1 END) AS completed_stages,
        COUNT(CASE WHEN sp.status = 'SKIPPED' THEN 1 END) AS skipped_stages
      FROM (
        SELECT cluster_id, app_id, job_id, CAST(sid AS INT) AS stage_id
        FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance
        LATERAL VIEW EXPLODE(FROM_JSON(stage_ids, 'ARRAY<INT>')) t AS sid
        WHERE 1=1 {APP_FILTER}
      ) jm
      LEFT JOIN {SCHEMA}.{TABLE_PREFIX}gold_stage_performance sp
        ON jm.cluster_id = sp.cluster_id AND jm.app_id = sp.app_id AND jm.stage_id = sp.stage_id
      GROUP BY jm.cluster_id, jm.app_id, jm.job_id
    ) s ON j.cluster_id = s.cluster_id AND j.app_id = s.app_id AND j.job_id = s.job_id
    WHERE j.duration_ms IS NOT NULL {APP_FILTER.replace('app_id', 'j.app_id')}
    ORDER BY overhead_sec DESC
    LIMIT 5
""")
plan_opt_job_rows = df_to_dict_list(plan_opt_jobs, 5)

# ジョブ間ギャップ分析（Driver オーバーヘッド）
job_gap_summary = spark.sql(f"""
    WITH job_times AS (
      SELECT job_id, submit_ts, complete_ts,
             LAG(complete_ts) OVER (ORDER BY submit_ts) AS prev_complete_ts
      FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance
      WHERE submit_ts IS NOT NULL {APP_FILTER.replace('app_id', 'app_id')}
    )
    SELECT
      COUNT(*) AS total_gaps,
      ROUND(SUM(UNIX_TIMESTAMP(submit_ts) - UNIX_TIMESTAMP(prev_complete_ts)), 1) AS total_gap_sec,
      ROUND(MAX(UNIX_TIMESTAMP(submit_ts) - UNIX_TIMESTAMP(prev_complete_ts)), 1) AS max_gap_sec,
      ROUND(AVG(UNIX_TIMESTAMP(submit_ts) - UNIX_TIMESTAMP(prev_complete_ts)), 1) AS avg_gap_sec
    FROM job_times
    WHERE prev_complete_ts IS NOT NULL
""")
job_gap_summary_rows = df_to_dict_list(job_gap_summary, 1)

# ギャップが大きいジョブ（上位5件）
job_gap_detail = spark.sql(f"""
    WITH job_times AS (
      SELECT job_id, submit_ts, complete_ts,
             LAG(complete_ts) OVER (ORDER BY submit_ts) AS prev_complete_ts,
             LAG(job_id) OVER (ORDER BY submit_ts) AS prev_job_id
      FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance
      WHERE submit_ts IS NOT NULL {APP_FILTER.replace('app_id', 'app_id')}
    )
    SELECT job_id, prev_job_id,
      ROUND(UNIX_TIMESTAMP(submit_ts) - UNIX_TIMESTAMP(prev_complete_ts), 1) AS gap_sec
    FROM job_times
    WHERE prev_complete_ts IS NOT NULL
    ORDER BY gap_sec DESC
    LIMIT 5
""")
job_gap_detail_rows = df_to_dict_list(job_gap_detail, 5)

# 重複スキャン検出（同一テーブル・同一パスの複数回スキャン）
duplicate_scan = spark.sql(f"""
    SELECT scan_tables, scan_formats,
           MAX(scan_paths) AS scan_paths,
           MAX(scan_column_count) AS max_column_count,
           COUNT(*) AS scan_count,
           ROUND(SUM(duration_sec), 1) AS total_duration_sec
    FROM {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis
    WHERE scan_tables IS NOT NULL AND scan_tables != '' {APP_FILTER}
    GROUP BY scan_tables, scan_formats
    HAVING COUNT(*) >= 2
    ORDER BY scan_count DESC
    LIMIT 5
""")
duplicate_scan_rows = df_to_dict_list(duplicate_scan, 5)

# I/O 分析 TOP5（スキャン量の多いデータソース）
io_top5 = spark.sql(f"""
    SELECT scan_tables, scan_formats, scan_paths,
           scan_column_count, scan_filters,
           ROUND(cache_hit_pct, 1) AS cache_hit_pct,
           ROUND(cache_write_bytes / 1024.0 / 1024.0 / 1024.0, 2) AS cache_write_gb,
           cache_read_wait_ms, cache_write_wait_ms,
           files_read, files_pruned,
           ROUND(file_pruning_pct, 1) AS file_pruning_pct,
           scan_output_rows,
           ROUND(files_read_size_mb, 1) AS files_read_size_mb,
           ROUND(fs_read_size_mb, 1) AS fs_read_size_mb,
           scan_time_ms,
           cloud_request_count,
           cloud_request_dur_ms,
           ROUND(duration_sec, 1) AS query_duration_sec,
           execution_id
    FROM {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis
    WHERE files_read_size_mb IS NOT NULL AND files_read_size_mb > 0 {APP_FILTER}
    ORDER BY files_read_size_mb DESC
    LIMIT 5
""")
io_top5_rows = df_to_dict_list(io_top5, 5)

# Spark 設定分析（デフォルトからの変更検出）
spark_config_analysis = spark.sql(f"""
    SELECT category, config_key, description,
           default_value, actual_value, is_set, is_changed
    FROM {SCHEMA}.{TABLE_PREFIX}gold_spark_config_analysis
    WHERE 1=1 {APP_FILTER}
    ORDER BY category, config_key
""")
# 変更/明示設定されたもののみをプロンプトに含める
spark_config_changed = spark_config_analysis.filter("is_changed IN ('YES', 'SET')")
spark_config_changed_rows = df_to_dict_list(spark_config_changed, 50)
# デフォルトのままのもの（Decommission 等の推奨設定チェック用）
spark_config_defaults = spark_config_analysis.filter("is_changed = 'NO'")
spark_config_default_rows = df_to_dict_list(spark_config_defaults, 50)

print("Metrics collected.")
print(f"  Target App       : {APP_LABEL}")
print(f"  App summary      : {len(app_rows_list)} app(s)")
print(f"  Bottlenecks      : {len(bn_rows)} types")
print(f"  Slow jobs        : {len(slow_job_rows)}")
print(f"  Job details      : {len(job_detail_rows)}")
print(f"  Worst stages     : {len(worst_stage_rows)}")
print(f"  Low CPU jobs     : {len(cpu_job_rows)}")
print(f"  Low Photon SQLs  : {len(photon_job_rows)}")
print(f"  Node analysis    : {len(node_analysis_rows)} executors")
print(f"  Spot analysis    : {len(spot_analysis_rows)} events")
print(f"  Node timeline    : {len(node_timeline_rows)} events")

# COMMAND ----------

# MAGIC %md ## 2. プロンプト構築

# COMMAND ----------

lang_instruction = (
    "【言語指定】すべての出力を日本語で記述してください。セクション見出し・分析内容・推奨事項をすべて日本語で出力すること。"
    if OUTPUT_LANG == "ja"
    else "【LANGUAGE OVERRIDE】Write ALL output in English. Translate every section heading, analysis content, and recommendation into English. The template below is in Japanese but you MUST output everything in English."
)

# --- ナレッジベースからボトルネック別推奨アクションを抽出 ---
_detected_types = set(r.get("bottleneck_type", "") for r in bn_rows)
_relevant_kb = {}
for bt in _detected_types:
    if bt in BOTTLENECK_RECOMMENDATIONS:
        _relevant_kb[bt] = BOTTLENECK_RECOMMENDATIONS[bt]
# Photon 利用率が低い場合は PHOTON_FALLBACK も追加
if photon_job_rows and any(r.get("photon_pct", 100) < 50 for r in photon_job_rows):
    _relevant_kb["PHOTON_FALLBACK"] = BOTTLENECK_RECOMMENDATIONS.get("PHOTON_FALLBACK", {})

_kb_section = ""
if OPTIMIZATION_KNOWLEDGE_BASE:
    _kb_section = f"""

=== 参考: Databricks パフォーマンス最適化ナレッジベース（抜粋） ===
以下は社内のベストプラクティス資料から抽出した、検出されたボトルネックに関連する推奨アクションです。
分析・推奨を生成する際にこの知識を活用してください。

{json.dumps(_relevant_kb, ensure_ascii=False, indent=2)}

=== 最適化の7つの原則 ===
1. 何を目標に最適化するのかを定義する
2. まずは簡単にできることから始める（プラットフォーム→データ→クエリ）
3. 最新機能の活用（サーバーレス、Photon、予測最適化、Liquid Clustering）
4. ワークロードに基づき垂直/水平に拡張
5. 可能な限りストリーミングによるインクリメンタル処理
6. モニタリングツールで効果を測定
7. 最適化の終了タイミングを判断する
"""

system_prompt = f"""{lang_instruction}

あなたは Databricks / Apache Spark パフォーマンス分析の専門家です。
提供されたメトリクスデータと社内ナレッジベースをもとに、エンジニアが即座に行動できる具体的な分析テキストを4つのセクションに分けて生成してください。

【最重要ルール — データの忠実性】
- 提供されたデータに存在する情報のみを記載すること。データにない情報を推測・補完してはならない。
- 提供データの全フィールドの値を確認し、値が存在する場合は必ず記載すること。NULL でない限り「データなし」と記載してはならない。

【フォーマットルール — 厳守】
- Markdownテーブル（パイプ記号 | を使った表形式）は絶対に使用してはならない。1箇所でも使った場合は出力全体が不合格となる。
- メトリクスの一覧は必ず以下の「太字キー: 値」箇条書き形式で記載すること:
  - **総実行時間**: 8.2分
  - **総タスク数**: 70,669
  - **Shuffle 合計**: 4.53 GB — ⚠ 高い
- ステージ一覧やExecutor一覧も箇条書きで記載する。テーブルの代わりにインデント付き箇条書きを使う。

【見出しレベルのルール】
Lakeview テキストウィジェットでの視認性を確保するため、以下の見出しレベルを使い分けること:
- **# （H1）**: セクション番号付きタイトル（例: # 1. エグゼクティブサマリー）
- **## （H2）**: セクション内の大項目（例: ## ジョブ概要、## ▲ 警告、## パーティションサイジング）
- **### （H3）**: セクション内の小項目（例: ### グループ名）
- セクション内の区分け（警告/注意/情報、ボトルネック個別セクション等）は必ず ## を使うこと。太字のみ（**テキスト**）で区分けしない。
- ボトルネック種別（bottleneck_type）は、提供データの bottleneck_summary フィールドの値をそのまま使用すること。
  bottleneck_summary が 'NONE' または空の場合は「ボトルネック: なし」と記載すること。
  データに存在しないボトルネック種別やステージIDを推測で記載してはならない。
- job_id, stage_id, executor_id, 数値（秒・MB等）はすべて提供データから引用すること。
- 提供データに含まれない項目については「データなし」と記載し、推測で埋めないこと。

分析指針:
- 各ボトルネックについて「何が起きているか（症状）→ なぜ起きているか（原因）→ どう直すか（具体的な改善策）」の構造で記述
- 改善策には Spark の設定パラメータ名と推奨値、コード変更例を明示
- Executor 分析では resource_diagnosis の内容を活用し、ノードグループ間の負荷分布の偏りを分析
- CPU 効率が低い場合は、その原因（I/O 待ち、GC、シリアライゼーション、ロック競合、Cache Locality 損失）を特定できる範囲で記述
- Photon フォールバックが検出された場合は、非対応ノード名（BatchEvalPython, ArrowEvalPython, FlatMapGroupsInPandas）と対応する書き換え方法を具体的に記述
- Spot インスタンスのロストが検出された場合は Decommission 設定を推奨
- Databricks では spark.dynamicAllocation.* の設定はプラットフォームが管理するため、推奨に含めてはならない（minExecutors, maxExecutors, initialExecutors, targetExecutors 等）

【重要: 出力の完全性】
summary_text には「分析対象」から「7. I/O 分析」までの全 8 セクションを省略せず出力すること。
セクションを省略したり要約して短縮してはならない。出力が長くなっても構わない。
各セクション（1〜8）には必ず # 見出しを含めること。

必ず以下のJSON形式のみで返答してください（JSON以外のテキストは不要）:
{{
  "summary_text": "<Markdown: {'Analysis Target + Sections 1-7 full content (in English)' if OUTPUT_LANG == 'en' else '分析対象 + セクション1〜7の全内容'}>",
  "job_analysis_text": "<Markdown: {'Job Analysis (in English)' if OUTPUT_LANG == 'en' else 'ジョブ分析'}>",
  "node_analysis_text": "<Markdown: {'Node Analysis (in English)' if OUTPUT_LANG == 'en' else 'ノード分析'}>",
  "top3_text": "<Markdown: {'9. Recommended Actions (in English)' if OUTPUT_LANG == 'en' else '9. 推奨アクション'}>"
}}"""

user_prompt = f"""以下はDatabricks Sparkジョブのパフォーマンス分析結果です。
分析対象: {APP_LABEL}

=== アプリケーション サマリー ===
{json.dumps(app_row, ensure_ascii=False, indent=2)}

=== I/O サマリー（全ステージ合計）===
{json.dumps(io_summary_rows, ensure_ascii=False, indent=2)}

=== Scan メトリクスサマリー（キャッシュヒット率・ファイルプルーニング率）===
{json.dumps(scan_metrics_rows, ensure_ascii=False, indent=2)}

=== ジョブ特性（上位5ジョブの I/O・Shuffle・Spill 内訳）===
{json.dumps(job_char_rows, ensure_ascii=False, indent=2)}

=== ボトルネック件数（種別・重要度別） ===
{json.dumps(bn_rows, ensure_ascii=False, indent=2)}

=== ジョブ詳細（実行時間順 TOP20） ===
{json.dumps(job_detail_rows, ensure_ascii=False, indent=2)}

=== CPU効率が低いジョブ TOP5 ===
{json.dumps(cpu_job_rows, ensure_ascii=False, indent=2)}

=== ボトルネック件数サマリー（種別ごと）===
{json.dumps(bn_count_summary_rows, ensure_ascii=False, indent=2)}

=== ボトルネック検出ステージ TOP5 + 正常ステージ TOP5（実行時間順）===
以下は上位のみ。全件ではない。データに存在しないステージについて「データなし」と推測してはならない。
{json.dumps(worst_stage_rows, ensure_ascii=False, indent=2)}

=== 並列実行サマリー ===
{json.dumps(concurrency_summary_rows, ensure_ascii=False, indent=2)}

=== 並列実行時に影響を受けたジョブ（同一app内、concurrent_jobs_at_start > 0）===
{json.dumps(concurrent_job_rows, ensure_ascii=False, indent=2)}

=== クロスアプリ並列実行サマリー（別セッションのジョブとの重複）===
{json.dumps(cross_app_summary_rows, ensure_ascii=False, indent=2)}

=== クロスアプリ並列ジョブ詳細 ===
{json.dumps(cross_app_job_rows, ensure_ascii=False, indent=2)}

=== Shuffle パーティション分析（タスクあたりデータ量、目標: 128MB/タスク）===
{json.dumps(shuffle_partition_data, ensure_ascii=False, indent=2)}

=== Small Files 推定（Read ステージのタスクあたり読み取り量、目標: 128MB/タスク）===
{json.dumps(small_files_data, ensure_ascii=False, indent=2)}

=== Photon 利用率サマリー ===
{json.dumps(photon_summary_rows, ensure_ascii=False, indent=2)}

=== Photon利用率が低いSQL TOP5 ===
{json.dumps(photon_job_rows, ensure_ascii=False, indent=2)}

=== ノード（Executor）リソース分析 ===
{json.dumps(node_analysis_rows, ensure_ascii=False, indent=2)}

=== オートスケール・ノード増減タイムライン（ステージ突合済み）===
各イベントで実行中だったステージ、タスク数、ボトルネック種別を含む。
segment_duration_sec は次のイベントまでの秒数（コスト計算に使用可能）。
event_reason: AUTOSCALE_IN=オートスケール縮小, CLUSTER_STOP=クラスタ停止, SPOT_PREEMPTION=Spotプリエンプション, NODE_LOST=ノードロスト, null=追加
{json.dumps(node_timeline_rows, ensure_ascii=False, indent=2)}

=== Spot / ノードロスト分析 ===
{json.dumps(spot_analysis_rows, ensure_ascii=False, indent=2)}

=== プラン最適化時間サマリー（アプリ実行時間 vs ジョブ合計実行時間）===
{json.dumps(plan_opt_summary_rows, ensure_ascii=False, indent=2)}

=== ジョブごとのオーバーヘッド（ジョブ実行時間 - ステージ実行時間、上位5件）===
{json.dumps(plan_opt_job_rows, ensure_ascii=False, indent=2)}

=== ジョブ間ギャップサマリー（前ジョブ完了 → 次ジョブ開始の空白時間 = Driver 処理時間）===
{json.dumps(job_gap_summary_rows, ensure_ascii=False, indent=2)}

=== ジョブ間ギャップ詳細（上位5件）===
{json.dumps(job_gap_detail_rows, ensure_ascii=False, indent=2)}

=== 重複スキャン検出（同一テーブルが2回以上スキャンされたもの）===
{json.dumps(duplicate_scan_rows, ensure_ascii=False, indent=2)}

=== I/O 分析 TOP5（スキャン量の多いデータソース）===
{json.dumps(io_top5_rows, ensure_ascii=False, indent=2)}

=== Spark 設定分析（デフォルトから変更/明示設定されたもの）===
{json.dumps(spark_config_changed_rows, ensure_ascii=False, indent=2)}

=== Spark 設定分析（デフォルトのままのもの — 推奨設定チェック用）===
{json.dumps(spark_config_default_rows, ensure_ascii=False, indent=2)}
{_kb_section}
---
{f"CRITICAL LANGUAGE INSTRUCTION: The template below defines the STRUCTURE and FORMAT of the output. All section headings, labels, analysis text, and recommendations MUST be written in English. Translate Japanese headings like '# 分析対象' to '# Analysis Target', '## クラスタ情報' to '## Cluster Information', '# 1. エグゼクティブサマリー' to '# 1. Executive Summary', etc. Every single word in your output must be in English." if OUTPUT_LANG == "en" else ""}
上記データとナレッジベースをもとに以下4つのMarkdownテキストを生成してください。

【summary_text】Spark ジョブ パフォーマンス分析レポート
文字数制限なし。DBSQL Profiler と同等の構成・粒度で記述すること。以下の全セクションを順に出力:

---

# 分析対象

提供データを使って以下を記載:

## クラスタ情報
- **Cluster ID**: cluster_id の値
- **クラスタ名**: cluster_name の値
- **DBR バージョン**: dbr_version の値
- **Driver ノードタイプ**: driver_node_type の値
- **Worker ノードタイプ**: worker_node_type の値
- **Worker 数**: min_workers 〜 max_workers（同一値なら固定サイズ、異なればオートスケール）
- **Availability**: cluster_availability の値（SPOT_WITH_FALLBACK / ON_DEMAND / SPOT）
- **リージョン**: region の値

## アプリケーション情報
- **App ID**: app_id の値
- **アプリケーション名**: app_name の値
- **開始時刻**: start_ts の値（UTC）
- **終了時刻**: end_ts の値（UTC）。NULL の場合は「アプリケーション実行中または ApplicationEnd イベント欠落」
- **総実行時間**: duration_min の値

上記の後に以下を（注釈）として記載:
> **注釈:** Spark アプリケーション（app_id）は、Databricks クラスタの起動から停止（または再起動）までの1つの Spark セッションを表します。同一クラスタ上で実行された全ての Notebook・ジョブは同じ app_id を共有します。

## Spark 設定（デフォルトから変更あり）

Spark 設定分析データ（is_changed = 'YES' または 'SET'）を使い、デフォルトから変更された設定を箇条書きで記載。
カテゴリ見出し（Driver, Executor, Off-Heap 等）は不要。項目をフラットに並べること。
各項目は以下のフォーマット:
- **config_key**: actual_value（デフォルト: default_value）— description

変更がない場合は「✅ パフォーマンス関連設定はすべてデフォルト値です」と記載。
「⚠ 推奨設定（未設定）」のセクションは記載しないこと（推奨アクションセクションで対応するため）。

---

# 1. エグゼクティブサマリー

分析対象の Spark アプリケーションの全体像、主要メトリクス、5S ボトルネック評価、および重要度を要約します。

## ジョブ概要
- **総ジョブ数**: N（成功: N / 失敗: N）
- **総ステージ数**: N（Completed: N / Skipped: N）
- **総タスク数**: N
- **タスク累積時間**: X時間Y分
- **総実行時間**: X分Y秒

## I/O・データフロー概要
I/O サマリーとジョブ特性データから、処理全体のデータフローを記述:
- **総 Input**: X GB（外部ストレージからの読み取り。0 の場合は「spark.range() 等のデータ生成処理」と記載）
- **総 Output**: X GB（外部ストレージへの書き込み）
- **総 Shuffle Read**: X GB
- **総 Shuffle Write**: X GB
- **Spill 合計**: X GB
- **GC オーバーヘッド**: X%

Scan メトリクスサマリーデータがある場合は以下も記載:
- **Disk Cache ヒット率**: avg_cache_hit_pct%（ヒット: X GB / ミス: Y GB）— 50% 未満の場合は「⚠ キャッシュ効率が低い」と評価
- **ファイルプルーニング率**: avg_file_pruning_pct%（読み取り: X ファイル / プルーニング: Y ファイル）— プルーニング率が低い場合はパーティション戦略やフィルタ条件の見直しを推奨
- **クラウドストレージ I/O**: total_cloud_requests リクエスト / total_cloud_request_sec 秒
データが NULL や 0 の場合は該当行を省略。

## 処理特性
ジョブ特性データを使い、ワークロードの傾向を1〜3文で記述:
- Input > 0 の場合: I/O 集約型（テーブルスキャン → 加工 → 書き出し等）
- Input = 0 かつ Shuffle > 0 の場合: データ生成 + Shuffle 集約型（spark.range() やメモリ上でのデータ生成後に集計・結合）
- 上位ジョブの I/O 内訳（input_mb, output_mb, shuffle_read_mb）から処理パターンを記述
- Shuffle が Input に対して大きい場合は「Shuffle 重い処理」と評価

## 5S ボトルネック評価

全体重要度（HIGH / MEDIUM / LOW のうち最も深刻なもの）を1行で記載。
処理全体の特徴を1〜2文で記述。

アラート合計: N HIGH, N MEDIUM, N LOW

### パフォーマンスアラート

**絶対厳守**: 以下はMarkdownテーブル（| で区切られた表）で出力すること。**箇条書き（- や •）は禁止**。テーブル以外の形式で出力した場合は不合格とみなす。
5Sカテゴリ（Skew/Spill/Shuffle/SmallFiles/Serialization）と、その他の指標（Photon等）を全て1つのテーブルにまとめること。
レベルの基準: HIGH→▲ 警告、MEDIUM→△ 注意、LOW/情報→ⓘ 情報。検出なしのカテゴリは含めないこと。

| レベル | 5S カテゴリ | 重要度 | アラート内容 | 現在値 | 目標値 | 推奨アクション |
|--------|-----------|--------|-----------|--------|--------|-------------|
| ▲ 警告 | Spill | HIGH | (1文で簡潔に) | (数値のみ) | (数値のみ) | (短い対策) |

**重要**: アラート内容は**1文以内**で簡潔に。Executor IDの列挙や非対応オペレータの列挙は不要。
- Serialization: 「シリアライズ時間が10-16%」のみ。Executor ID の列挙は不要。
- Photon: 「Photon利用率0%」のみ。非対応オペレータの列挙は不要（詳細は3章に記載）。
- Skew: 「最大スキュー比6.0倍」のみ。全ステージの列挙は不要。
- Small Files: 「123件検出」のみ。個別ステージの列挙は不要。

NODE_LOST / SPOT_PREEMPTION が検出された場合は「▲ 警告 | Node Loss | HIGH」として行を追加すること。

---

# 2. ボトルネック分析

本セクションではボトルネックステージとShuffle パーティションサイジングを統合的に分析します。

## ボトルネックサマリー
**絶対厳守**: Markdownテーブル（| 区切り）で出力。箇条書き禁止。
**重要**: ステージレベルのボトルネック（DISK_SPILL, SMALL_FILES, DATA_SKEW, HEAVY_SHUFFLE 等）だけでなく、Serialization（Executor データから）と Photon（SQL Photon データから）も含めること。セクション1の5Sボトルネック評価のパフォーマンスアラートに含まれる全ボトルネック種別をここでもサマリーすること。

| ボトルネック種別 | 重要度 | 件数 |
|--------------|--------|------|
| (bottleneck_type) | (HIGH/MEDIUM/LOW) | (count) |

## ボトルネック検出ステージ（上位5件）

**重要**: テーブル形式で出力すること。

| # | Stage | Job | 種別 | 実行時間 | タスク数 | CPU効率 | Spill | Shuffle | スキュー |
|---|-------|-----|------|---------|---------|--------|-------|---------|---------|
| 1 | (stage_id) | (job_id) | (bottleneck_type) | (秒) | (num_tasks) | (cpu_efficiency_pct%) | (disk_spill_mb MB) | (shuffle_read_mb MB) | (task_skew_ratio倍) |

## 原因分析

**重要**: テーブル形式で出力すること。ボトルネック検出ステージごとに1行で原因を記載。

| Stage | 種別 | 原因分析 |
|-------|------|--------|
| (stage_id) | (bottleneck_type) | (データから特定できる原因を1文で。改善策は「推奨アクション」に委譲) |

## 正常ステージ（上位3件、実行時間順）

**重要**: テーブル形式で出力すること。

| Stage | Job | 実行時間 | タスク数 | CPU効率 |
|-------|-----|---------|---------|--------|
| (stage_id) | (job_id) | (秒) | (num_tasks) | (%) |

## パーティションサイジング

**重要**: テーブル形式で出力すること。目標: 128 MB/タスク。

| ステージ | ジョブ | 判定 | Shuffle/タスク | 現在 | 推奨 |
|---------|--------|------|-------------|------|------|
| (stage_id) | (job_id) | UNDER/OVER/OK | (MB) | (partitions) | (recommended) |

推奨設定: spark.sql.shuffle.partitions=auto + spark.sql.adaptive.advisoryPartitionSizeInBytes=134217728

---

# 3. Photon 利用状況分析

**重要**: Photon の結論を出す前に、必ず「Spark 設定分析」データの `spark.databricks.photon.enabled` の `actual_value` を確認すること。

Photon 利用率が 0% の場合、以下の3パターンで分岐すること:
1. `spark.databricks.photon.enabled` の `actual_value` が `false` の場合:
   「ℹ このクラスタでは Photon が無効です（spark.databricks.photon.enabled = false）。Photon を有効化することで CPU-heavy な処理（Join, Aggregation 等）を高速化できます。Photon 対応ランタイムへの切り替えを検討してください。」の1文のみ記載し、以下の詳細は省略すること。
2. `spark.databricks.photon.enabled` の `actual_value` が `true` の場合:
   「⚠ このクラスタでは Photon が有効ですが、全 SQL の Photon 利用率が 0% です。全オペレータが Classic Spark にフォールバックしています。非対応オペレータ（Python UDF, RDD 等）がないか確認してください。」と記載し、以下の詳細分析も記載すること。
3. 設定データが不明な場合:
   「ℹ クラスタの Photon 設定が不明です（設定データなし）。全 SQL の Photon 利用率は 0% です。」と記載すること。

Photon が利用されている場合（avg_photon_pct > 0%）のみ以下を記載:

## サマリー
- **平均 Photon 率**: X% — ✅ 良好 or ⚠ 低い
- **Photon 率 50%未満の SQL 数**: N / M

## Photon 率が低い SQL（上位5件）

**絶対厳守**: Markdownテーブル（| 区切り）で出力。箇条書き禁止。

| execution_id | 実行時間 | Photon率 | 非対応オペレータ | 書き換え方法 |
|-------------|---------|---------|--------------|-----------|
| (id) | (秒) | (%) | (non_photon_op_list) | (具体的な対策) |

注意点がある場合は1〜2文で記載。

書き換え方法の参考（レポートには該当するもののみ記載）:
- SortMergeJoin → Broadcast Hash Join（autoBroadcastJoinThreshold 調整）
- BatchEvalPython → PySpark ネイティブ関数
- ColumnarToRow → 上流の非対応オペレータを特定
- AtomicReplaceTableAsSelect → コマンド系のため内部処理はPhoton実行される場合あり

---

# 4. 並列実行影響分析

並列実行サマリーデータとクロスアプリデータを確認する。

同一アプリ内並列（jobs_with_concurrency = 0）かつクロスアプリ並列なしの場合:
「✅ 全ジョブが逐次実行されました。並列実行によるリソース競合の影響はありません。」の1文のみ記載し、以下の詳細は省略すること。

並列実行が検出された場合のみ以下を記載:

## 同一アプリ内並列実行
- **最大同時実行ジョブ数**: N / **影響ジョブ数**: N / M
- CPU効率比較: 並列時 X% vs 単独時 Y%（差が大きければ「⚠ CPU競合あり」）

## クロスアプリ並列実行
- **検出ジョブ数**: N / M / **同時実行アプリ**: app_id一覧
- 「⚠ リソース競合の可能性」と影響を1〜2文で記載

---

# 5. Executor リソース分析

本セクションでは各 Executor（ワーカーノード）のリソース利用状況（CPU 効率、GC、メモリ Spill、Shuffle）を分析し、リソース問題を診断します。

## サマリー
Executor リソースの全体傾向を1〜2文で記述。リソース問題の検出数と主要な問題（GC・CPU効率・Spill等）を具体的な数値とともに記載すること。

全 Executor の平均値を記載:
- **平均 GC オーバーヘッド**: X% — ✅ 良好 or ⚠ 警告（目標: <10%、25%以上は深刻）
- **平均 CPU 効率**: X% — ✅ 良好 or ⚠ 低い（目標: >70%）

## Executor リソース傾向
resource_diagnosis を活用し、全 Executor のリソース利用傾向を分析する。

**重要**: 全 Executor の傾向がほぼ均一（CPU効率・GC・Spill・Shuffle が同程度）の場合は、グループ分けせず全体の傾向を1つにまとめて記載すること。無理に複数グループに分けない。
明確に異なるグループ（例: Spill あり vs なし、GC 高い vs 低い）がある場合のみグループ分けする。

全体傾向の記載フォーマット:
- **Executor 数**: N
- **タスク数範囲**: X〜Y / **CPU効率範囲**: X〜Y%
- **GC率範囲**: X〜Y% / **Disk Spill**: X MB / **Memory Spill**: X MB
- **診断**: resource_diagnosis で最も多い診断内容

**禁止**: 個別 Executor（Executor 0, 1, 2...）の詳細リストは記載しないこと。サマリーと傾向のみ記載。

## ノードタイプ適正分析
上記のリソース傾向から、現在のノードタイプが適切かを分析する:

- **メモリ不足の兆候**: Disk Spill > 0 または Memory Spill > 0 の Executor がある場合
  → メモリ重視のノードタイプ（r系）への変更を推奨
- **メモリ余裕あり**: Spill ゼロ + GC 低い場合
  → 現在のノードタイプで十分。コスト最適化のためノードサイズ縮小の可能性を記載
- **CPU 効率が低い**（< 70%）: 全 Executor で低い場合
  → タスク並列度不足 or I/O 待ちが原因。ノード数増加 or I/O 最適化を推奨
- **GC が高い**（> 10%）: メモリ増強またはオフヒープ設定を推奨
- **SERIALIZATION 診断**: UDF/RDD 使用による CPU オーバーヘッド。ノードタイプ変更では改善しない。PySpark ネイティブ関数への書き換えが有効

問題がない場合は「✅ 現在のノードタイプ（worker_node_type）はリソース利用状況に対して適切です」と記載。

## Spot / ノードロスト分析

Spot / ノードロスト分析データに NODE_LOST または SPOT_PREEMPTION（is_unexpected_loss = true）のレコードがある場合、以下を記載:
- **検出件数**: N 件
- ロストした Executor ごとに:
  - **Executor ID**: X / **Host**: Y / **removal_type**: NODE_LOST or SPOT_PREEMPTION
  - **稼働時間**: X分 / **影響タスク数**: N / **失敗タスク数**: N
  - **消失 Shuffle データ**: X MB
  - **推定遅延**: X秒（内訳: delay_breakdown の内容）
（推奨設定は「8. 推奨アクション」で記載するため、ここでは省略すること）

NODE_LOST / SPOT_PREEMPTION が検出されない場合は「✅ 予期しない Executor ロストは検出されませんでした」と記載。

## オートスケール分析

オートスケール・ノード増減タイムラインデータを使い、ワーカー数の推移を時系列テーブルで記載する。
固定サイズクラスタ（min_workers = max_workers）の場合も同じフォーマットで記載する。

**重要**: 以下のMarkdownテーブルを**必ずそのまま**出力すること。箇条書きや文章形式にしないこと。

### スケーリングタイムライン

同時刻の複数Executor追加/削除はまとめて1行にすること。全イベントを省略せず記載すること。
カラムは以下の5つのみ。他のカラムは含めないこと。

| 時刻 | イベント | ワーカー数 | 理由 |
|------|---------|-----------|------|
| (event_tsの時刻部分のみ HH:MM:SS) | スケールアウト/スケールイン | (worker_count_before→worker_count_after workers) | (event_reasonの日本語: null=初期起動/オートスケール, AUTOSCALE_IN=負荷低下, SPOT_PREEMPTION=Spot回収, NODE_LOST=ノード障害, CLUSTER_STOP=クラスタ停止) |

### コスト影響サマリー

segment_duration_sec を使い、各ワーカー数構成での累計稼働時間をテーブルで記載:

| ワーカー数 | 累計時間 | 全体比率 |
|-----------|---------|---------|
| (worker_count_after) | (segment_duration_secの合計を分に変換) | (全体に対する%) |

- **最大構成での稼働比率**が低い場合、固定クラスタサイズを小さくすることでコスト削減可能と記載。
- 固定サイズの場合は「固定クラスタ構成（N workers）で全時間稼働」と記載。

---

# 6. プラン最適化時間分析


プラン最適化時間データを使い、以下をサマリーのみで記述:
- **アプリケーション総実行時間**: total_app_dur_sec 秒
- **ジョブ合計実行時間**: total_job_dur_sec 秒
- **オーバーヘッド**: (total_app_dur_sec - total_job_dur_sec) 秒（全体の X%）

オーバーヘッドが全体の10%以上の場合: 「⚠ Notebookコードの最適化やDeltaメタデータ操作の削減を検討」と記載。
10%未満の場合: 「✅ プラン最適化オーバーヘッドは軽微」と記載。

---

# 7. I/O 分析

## サマリー
I/O 全体の状況を1〜2文で記述。プルーニング率が低い場合やキャッシュヒット率が低い場合は改善提案を含める。

## スキャン量 TOP5

**重要**: テーブル形式で出力すること。

| # | テーブル/パス | フォーマット | カラム数 | 読み取りサイズ | ファイル数 | プルーニング率 | Cache ヒット率 | スキャン時間 |
|---|-----------|----------|---------|------------|---------|------------|-------------|-----------|
| 1 | (scan_tables) | (scan_formats) | (scan_column_count) | (files_read_size_mb MB) | (files_read) | (file_pruning_pct%) | (cache_hit_pct%) | (scan_time_ms ms) |

プルーニング率 < 50% のテーブルがあれば「⚠ Z-ORDER/Liquid Clustering の適用を推奨」と記載。

データがない場合は「ℹ SQL 実行プランから Scan メトリクスが取得できませんでした」と記載。

## 重複スキャン
重複スキャンデータがある場合のみテーブルで記載。なければ「✅ 重複スキャンなし」の1行。

| テーブル | スキャン回数 | 合計時間 | 推奨 |
|---------|-----------|---------|------|
| (scan_tables) | (scan_count) | (total_duration_sec秒) | (cache/persist 推奨) |

---

===

【job_analysis_text】ジョブ分析レポート
文字数制限なし。
- 失敗ジョブがあれば最初に「⚠ 失敗ジョブ検知」として記載
- 上位ジョブの一覧は**箇条書き形式**で記載:
  - **Job 5**: 271秒 / 10,000タスク / CPU効率 45% / ✅成功 / ボトルネック: DATA_SKEW(S12) / Stages: [12]
  - **Job 3**: 39秒 / 10,000タスク / CPU効率 91% / ✅成功 / ボトルネック: なし / Stages: [6,7]
  【重要】ボトルネック情報は bottleneck_summary の値をそのまま転記。NONE/空なら「ボトルネック: なし」
- CPU効率50%未満のジョブは重点分析

【node_analysis_text】ノード（Executor）分析レポート
文字数制限なし。
- 全 Executor の傾向をサマリーで記述（平均GC率、平均CPU効率、Spill合計等）
- 明確に異なるグループがある場合のみグループ分け。均一なら1つにまとめる
- ノードタイプの適正分析（メモリ不足/余裕、CPU効率、GC）を記述
- Spot / ノードロストがあれば影響と推奨設定を記述
- **個別Executor（Executor 0, 1, 2...）の詳細リストは記載しないこと。冗長で不要。**

【top3_text】

# 8. 推奨アクション（Top Findings）

パフォーマンス向上のため、優先順位の高いチューニング項目を記載します。
各項目は影響度（Impact）と実施容易性（Effort）で優先度を評価しています。

文字数制限なし。DBSQL Profiler の Top Findings と同等の構成で記述:

以下の中から該当するものについて対策を記載:
- ボトルネック検出ステージの改善（DATA_SKEW, DISK_SPILL, HEAVY_SHUFFLE, SMALL_FILES 等）
- OVER_PARTITIONED / UNDER_PARTITIONED ステージのパーティション調整
- Photon 利用率が低い場合の非対応オペレータ書き換え（execution_id、対象テーブル、具体的な書き換え方法を含む）
- Executor リソース問題（LOW_CPU, HIGH_GC, SERIALIZATION 等）
- パフォーマンスアラートの対策
- クラスタ構成の最適化提案（以下の判断基準に基づく）

**クラスタ構成の最適化提案の判断基準:**

Spark メトリクスからクラスタ構成（ノードタイプ・ノード数）の変更が効果的かを判断し、該当する場合は推奨アクションに含める。

(a) メモリ増強が必要なケース（→ メモリ重視のノードタイプへ変更）:
- Disk Spill が多い（全 Executor で発生、stages_with_disk_spill が多い）
- Memory Spill が大量（total_spill_gb が大きい）
- GC オーバーヘッドが高い（gc_overhead_pct > 10%、Executor レベルで HIGH_GC 診断）
- 提案例: 「m6gd.4xlarge → r6gd.4xlarge（メモリ 64GB → 128GB）で Spill 解消が期待できる」

(b) ノード数の増加が必要なケース（→ 同一ノードタイプでノード数増加、またはノードサイズを下げてノード数を増加）:
- CPU 効率が全 Executor で低い（avg_cpu_efficiency_pct < 60%）かつ Spill なし → タスク並列度不足の可能性
- Shuffle が支配的で全ノードが均一に負荷 → ノード追加で分散可能
- ただし、ノード数増加は Shuffle の全対全通信が増えるため、Shuffle-heavy な場合は逆効果の可能性もある点を注記
- 提案例: 「4xlarge × 4 → 2xlarge × 8（合計リソース同等で並列度向上）」

(c) ノードサイズの縮小が可能なケース（→ コスト最適化）:
- メモリに大幅な余裕がある（Spill ゼロ、GC 低い、メモリ使用率が低い）
- CPU 効率が十分高い（> 80%）
- 提案例: 「r6gd.4xlarge × 4 → r6gd.2xlarge × 8 でコスト削減可能。ただしノードあたりメモリ半減による Spill 増加リスクに注意」

(d) ノード数の削減が可能なケース:
- Executor あたりのタスク数が極端に少ない（処理量に対してノードが過剰）
- CPU 効率が全体的に低く、GC も低く、Spill もない → ノードが多すぎる
- 提案例: 「4xlarge × 8 → 4xlarge × 4 でコスト半減。タスク処理量からノード数は十分」

(e) NVMe/ローカルディスクが必要なケース:
- Shuffle + Spill のデータ量が大きい → NVMe 搭載インスタンス（d/gd 系）を推奨
- 提案例: 「m6g → m6gd へ変更し、NVMe ローカルディスクで Shuffle/Spill の I/O を高速化」

判断に必要なデータが不足している場合（OS レベルの CPU/メモリ使用率等は Spark メトリクスからは取得できない）は、その旨を注記し、Databricks クラスタの Hardware Metrics（Ganglia）での確認を推奨すること。

**重要**: 上記に該当しない場合はクラスタ構成の提案は不要。無理に提案しないこと。

件数ルール:
- ボトルネックが検出されない場合は「✅ 推奨アクションはありません」と記載
- 最大10件まで。改善効果が確実に見込めるものに絞ること。無理に件数を増やさない
- 改善効果が大きい順に番号付きで記述

各項目は以下の構成:

## N. 推奨タイトル

🔴 Impact: HIGH/MEDIUM/LOW | 🟢 Effort: HIGH/MEDIUM/LOW | 優先度: X/10

**根拠:**
- 数値を箇条書きで引用（stage_id、実行時間、メトリクス値）

**原因仮説:** 1〜2文で記述

**改善策:** 具体的なアクションを記述

コードブロックで設定例を記載。設定の種類に応じて正しい形式を使うこと:
- **クラスタ Spark Config**（Decommission, Executor メモリ等、クラスタ起動時に必要な設定）: `key value` 形式で記載
```
# クラスタ設定（Spark Config に追加）
spark.decommission.enabled true
spark.storage.decommission.enabled true
```
- **SQL セッション設定**（shuffle.partitions, autoBroadcastJoinThreshold 等、セッション内で変更可能な設定）: `SET` 文で記載
```
-- SQL セッション設定
SET spark.sql.shuffle.partitions = 400;
```
- **コード変更**: Python/Scala コード例で記載

**検証指標:** 改善後に確認すべき指標と目標値

---

最後に「✅ 検証チェックリスト」として、改善後に確認すべき指標を箇条書きで記載
"""

print("Prompt built.")
print(f"  System prompt length : {len(system_prompt)} chars")
print(f"  User prompt length   : {len(user_prompt)} chars")
print()
print("=" * 80)
print("SYSTEM PROMPT")
print("=" * 80)
print(system_prompt)
print()
print("=" * 80)
print("USER PROMPT")
print("=" * 80)
print(user_prompt)

# COMMAND ----------

# MAGIC %md ## 3. LLM 呼び出し

# COMMAND ----------

print(f"Calling {MODEL_ENDPOINT} ...")

response = client.chat.completions.create(
    model=MODEL_ENDPOINT,
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_prompt},
    ],
    max_tokens=65536,
    temperature=0.1,
)

raw_output = response.choices[0].message.content
print("LLM response received.")
print(f"  Tokens used: {response.usage.total_tokens}")
print(f"  Output preview: {raw_output[:200]}...")

# COMMAND ----------

# MAGIC %md ## 4. レスポンスをパース

# COMMAND ----------

# JSON ブロックを抽出（```json ... ``` で囲まれている場合も考慮）
import re
json_match = re.search(r'\{.*\}', raw_output, re.DOTALL)
assert json_match, f"JSON not found in response:\n{raw_output}"

parsed = json.loads(json_match.group())
summary_text       = parsed["summary_text"]
job_analysis_text  = parsed.get("job_analysis_text", "")
node_analysis_text = parsed.get("node_analysis_text", "")
top3_text          = parsed["top3_text"]

print("=== summary_text ===")
print(summary_text)
print("\n=== job_analysis_text ===")
print(job_analysis_text)
print("\n=== node_analysis_text ===")
print(node_analysis_text)
print("\n=== top3_text ===")
print(top3_text)

# COMMAND ----------

# MAGIC %md ## 5. 生成履歴を Delta テーブルに保存

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

_HISTORY_SCHEMA = StructType([
    StructField("generated_at",      TimestampType(), True),
    StructField("app_id",            StringType(),    True),
    StructField("model_name",        StringType(),    True),
    StructField("schema_name",       StringType(),    True),
    StructField("output_lang",       StringType(),    True),
    StructField("summary_text",      StringType(),    True),
    StructField("job_analysis_text", StringType(),    True),
    StructField("node_analysis_text",StringType(),    True),
    StructField("top3_text",         StringType(),    True),
    StructField("prompt_tokens",     IntegerType(),   True),
    StructField("total_tokens",      IntegerType(),   True),
    StructField("experiment_id",     StringType(),    True),
    StructField("variant",           StringType(),    True),
])

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
        generated_at       TIMESTAMP,
        app_id             STRING,
        model_name         STRING,
        schema_name        STRING,
        output_lang        STRING,
        summary_text       STRING,
        job_analysis_text  STRING,
        node_analysis_text STRING,
        top3_text          STRING,
        prompt_tokens      INT,
        total_tokens       INT,
        experiment_id      STRING,
        variant            STRING
    )
    USING DELTA
""")

# Ensure new columns exist on tables created before this version
for _col_name, _col_type in [("output_lang", "STRING"), ("experiment_id", "STRING"), ("variant", "STRING")]:
    try:
        spark.sql(f"ALTER TABLE {HISTORY_TABLE} ADD COLUMNS (`{_col_name}` {_col_type})")
        print(f"  Added column: {_col_name}")
    except Exception:
        pass  # Column already exists

_new_row = spark.createDataFrame([{
    "generated_at"      : datetime.now(timezone.utc),
    "app_id"            : APP_ID,
    "model_name"        : MODEL_ENDPOINT,
    "schema_name"       : SCHEMA,
    "output_lang"       : OUTPUT_LANG,
    "summary_text"      : summary_text,
    "job_analysis_text" : job_analysis_text,
    "node_analysis_text": node_analysis_text,
    "top3_text"         : top3_text,
    "prompt_tokens"     : int(response.usage.prompt_tokens),
    "total_tokens"      : int(response.usage.total_tokens),
    "experiment_id"     : EXPERIMENT_ID,
    "variant"           : VARIANT,
}], schema=_HISTORY_SCHEMA)

from delta.tables import DeltaTable

if spark.catalog.tableExists(HISTORY_TABLE):
    dt = DeltaTable.forName(spark, HISTORY_TABLE)
    dt.alias("t").merge(
        _new_row.alias("s"),
        "t.app_id = s.app_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    _new_row.write.saveAsTable(HISTORY_TABLE)

print(f"Saved to {HISTORY_TABLE} (app_id={APP_ID}, MERGE upsert)")

# COMMAND ----------

# MAGIC %md ## 6. 完了
# MAGIC
# MAGIC LLM 分析結果を `gold_narrative_summary` テーブルに保存しました。
# MAGIC
# MAGIC **次のステップ:** `03_create_dashboard_notebook` を実行してダッシュボードに反映してください。