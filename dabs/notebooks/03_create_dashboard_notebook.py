# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Job Performance Analysis — ダッシュボード作成
# MAGIC
# MAGIC このノートブックを実行するとLakeviewダッシュボードを作成・公開します。
# MAGIC
# MAGIC **手順:**
# MAGIC 1. 下の「CONFIGURATION」セルを環境に合わせて変更する
# MAGIC 2. 「Run All」でノートブック全体を実行する
# MAGIC
# MAGIC **更新時:** `EXISTING_DASHBOARD_ID` は通常 `None` のままでOK。同名ダッシュボードを自動検索してPATCHで上書きするため、URLは変わりません。

# COMMAND ----------

# ┌─────────────────────────────────────────────────────────────────────────┐
# │  CONFIGURATION — ウィジェットで動的に変更できます                         │
# └─────────────────────────────────────────────────────────────────────────┘

# Gold テーブルが格納されているカタログ.スキーマ
dbutils.widgets.text("catalog",       "main",                                                     "Catalog")
dbutils.widgets.text("schema",        "base2",                                                     "Schema")
# テーブル名プレフィックス（01_Spark Perf Pipeline の table_prefix と合わせてください）
dbutils.widgets.text("table_prefix",  "PERF_",                                                   "Table Name Prefix")
# SQL ウェアハウス ID（Databricks UI → SQL Warehouses → 接続の詳細タブで確認）
dbutils.widgets.text("warehouse_id",  "your-warehouse-id",                                       "SQL Warehouse ID")
# ダッシュボードファイルの保存先ワークスペースパス
dbutils.widgets.text("parent_path",   "/Users/your-user@example.com/spark-perf-dlt",              "Dashboard Parent Path")
# ダッシュボード表示名
dbutils.widgets.text("dash_name",     "Spark Job Performance Analysis",                           "Dashboard Display Name")
# ──────────────────────────────────────────────────────────────────────────
# 更新オプション（通常は空のまま）
#   空文字      : dash_name と parent_path で同名ダッシュボードを自動検索し、
#                 存在すれば PATCH で中身を上書き（IDとURLを引き継ぐ）
#                 見つからなければ新規作成する
#   "<dashboard_id>" : 指定IDのダッシュボードを PATCH で上書きする
# ──────────────────────────────────────────────────────────────────────────
dbutils.widgets.text("existing_dashboard_id", "",                                                 "Existing Dashboard ID (optional)")

CATALOG               = dbutils.widgets.get("catalog")
_SCHEMA               = dbutils.widgets.get("schema")
SCHEMA                = f"{CATALOG}.{_SCHEMA}"
TABLE_PREFIX          = dbutils.widgets.get("table_prefix")
WAREHOUSE_ID          = dbutils.widgets.get("warehouse_id")
PARENT_PATH           = dbutils.widgets.get("parent_path")
DASH_NAME             = dbutils.widgets.get("dash_name")
_existing_id          = dbutils.widgets.get("existing_dashboard_id").strip()
EXISTING_DASHBOARD_ID = _existing_id if _existing_id else None

# ── 分析対象アプリケーション選択 ─────────────────────────────────────────────
_app_rows = spark.sql(f"""
    SELECT app_id, app_name, cluster_id, start_ts,
           ROUND(duration_min, 1) AS duration_min, total_jobs
    FROM {SCHEMA}.{TABLE_PREFIX}gold_application_summary
    ORDER BY start_ts
""").collect()
_app_choices = [r["app_id"] for r in _app_rows]
_default_app = _app_rows[-1]["app_id"] if _app_rows else _app_choices[0]
dbutils.widgets.dropdown("app_id", _default_app, _app_choices, "Target App ID")

APP_ID = dbutils.widgets.get("app_id")
APP_FILTER_SQL = f"WHERE app_id = '{APP_ID}'" if APP_ID != "ALL" else ""
APP_AND_SQL    = f"AND app_id = '{APP_ID}'"   if APP_ID != "ALL" else ""

print(f"Target App: {APP_ID}")
for r in _app_rows:
    marker = " ← 選択中" if r["app_id"] == APP_ID else ""
    cid = r["cluster_id"] or "N/A"
    dur = f"{r['duration_min']}分" if r["duration_min"] else "実行中/不明"
    jobs = r["total_jobs"] or "?"
    print(f"  {r['app_id']}  cluster: {cid}  ({r['app_name'] or 'N/A'})  started: {r['start_ts']}  duration: {dur}  jobs: {jobs}{marker}")

# COMMAND ----------

# ── インポート & LakeviewDashboard クラス（依存ライブラリなし） ──────────────
import json, uuid, requests
from typing import Optional, List, Dict, Any

class LakeviewDashboard:
    DEFAULT_COLORS = ["#FFAB00","#00A972","#FF3621","#8BCAE7","#AB4057","#99DDB4","#FCA4A1","#919191","#BF7080"]

    def __init__(self, name: str = "New Dashboard"):
        self.name = name
        self.datasets: List[Dict] = []
        self.pages: List[Dict] = []
        self._current_page: Optional[Dict] = None
        self.add_page("Overview")

    @staticmethod
    def _generate_id() -> str:
        return uuid.uuid4().hex[:8]

    def add_dataset(self, name, display_name, query):
        self.datasets.append({"name": name, "displayName": display_name, "queryLines": [query]})
        return name

    def add_page(self, display_name):
        page_id = self._generate_id()
        page = {"name": page_id, "displayName": display_name, "pageType": "PAGE_TYPE_CANVAS", "layout": []}
        self.pages.append(page)
        self._current_page = page
        return page_id

    def _add_widget(self, widget, position):
        self._current_page["layout"].append({"widget": widget, "position": {
            "x": position.get("x", 0), "y": position.get("y", 0),
            "width": position.get("width", 2), "height": position.get("height", 3)
        }})

    def add_counter(self, dataset_name, value_field, value_agg="SUM", title=None, position=None):
        wid = self._generate_id()
        if value_agg == "COUNT":
            vname, vexpr = "count(*)", "COUNT(`*`)"
        else:
            vname, vexpr = f"{value_agg.lower()}({value_field})", f"{value_agg}(`{value_field}`)"
        widget = {
            "name": wid,
            "queries": [{"name": "main_query", "query": {
                "datasetName": dataset_name,
                "fields": [{"name": vname, "expression": vexpr}],
                "disaggregated": True
            }}],
            "spec": {
                "version": 2, "widgetType": "counter",
                "encodings": {"value": {"fieldName": vname, "displayName": title or vname}},
                "frame": {"showTitle": title is not None, "title": title or ""}
            }
        }
        self._add_widget(widget, position or {"x": 0, "y": 0, "width": 1, "height": 2})
        return wid

    def to_dict(self):
        return {
            "datasets": self.datasets,
            "pages": self.pages,
            "uiSettings": {"theme": {"widgetHeaderAlignment": "ALIGNMENT_UNSPECIFIED"}, "applyModeEnabled": False}
        }

    def to_json(self):
        return json.dumps(self.to_dict())

    def get_api_payload(self, warehouse_id, parent_path):
        return {"display_name": self.name, "warehouse_id": warehouse_id,
                "parent_path": parent_path, "serialized_dashboard": self.to_json()}

# COMMAND ----------

# ── ヘルパー関数 ──────────────────────────────────────────────────────────────
def uid():
    return uuid.uuid4().hex[:8]

BN_COLORS     = ["#00A972","#FFAB00","#FF3621","#AB4057","#8BCAE7","#FCA4A1","#99DDB4","#919191"]
STATUS_COLORS = ["#00A972","#FF3621","#FFAB00"]

def add_widget(db, page_idx, widget, x, y, w, h):
    db.pages[page_idx]["layout"].append({
        "widget": widget,
        "position": {"x": x, "y": y, "width": w, "height": h}
    })

def agg_bar(ds_name, x_f, y_f, y_agg, color_f, title, colors=None, sort_x=None):
    wid = uid()
    y_name = f"{y_agg.lower()}({y_f})"
    y_expr = f"{y_agg}(`{y_f}`)"
    x_scale = {"type": "categorical"}
    if sort_x:
        x_scale["sort"] = {"by": sort_x}
    spec = {
        "version": 3, "widgetType": "bar",
        "encodings": {
            "x":     {"fieldName": x_f,    "scale": x_scale,                  "displayName": x_f},
            "y":     {"fieldName": y_name, "scale": {"type": "quantitative"}, "displayName": y_f},
            "color": {"fieldName": color_f, "scale": {"type": "categorical"}, "displayName": color_f},
            "label": {"show": True},
        },
        "frame": {"showTitle": True, "title": title},
    }
    if colors:
        spec["mark"] = {"colors": colors}
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": ds_name,
            "fields": [
                {"name": x_f,    "expression": f"`{x_f}`"},
                {"name": y_name, "expression": y_expr},
                {"name": color_f,"expression": f"`{color_f}`"},
            ],
            "disaggregated": False
        }}],
        "spec": spec,
    }

def raw_scatter(ds_name, x_f, x_label, y_f, y_label, color_f, title, colors=None):
    wid = uid()
    spec = {
        "version": 3, "widgetType": "scatter",
        "encodings": {
            "x":     {"fieldName": x_f,     "scale": {"type": "quantitative"}, "displayName": x_label},
            "y":     {"fieldName": y_f,     "scale": {"type": "quantitative"}, "displayName": y_label},
            "color": {"fieldName": color_f, "scale": {"type": "categorical"},  "displayName": color_f},
        },
        "frame": {"showTitle": True, "title": title},
    }
    if colors:
        spec["mark"] = {"colors": colors}
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": ds_name,
            "fields": [
                {"name": x_f,     "expression": f"`{x_f}`"},
                {"name": y_f,     "expression": f"`{y_f}`"},
                {"name": color_f, "expression": f"`{color_f}`"},
            ],
            "disaggregated": True,
        }}],
        "spec": spec,
    }

def make_text_widget(text_md):
    return {"name": uid(), "textbox_spec": text_md}

def make_table(ds_name, field_names, title, max_rows=1000):
    wid = uid()
    return {
        "name": wid,
        "queries": [{"name": "main_query", "query": {
            "datasetName": ds_name,
            "fields": [{"name": f, "expression": f"`{f}`"} for f in field_names],
            "disaggregated": True,
        }}],
        "spec": {
            "version": 2, "widgetType": "table",
            "encodings": {
                "columns": [
                    {"fieldName": f, "displayName": f} for f in field_names
                ]
            },
            "frame": {"showTitle": True, "title": title},
        },
        "overrides": {
            "queries": [{"query": {"limit": max_rows}}],
        },
    }

# COMMAND ----------

# MAGIC %md ## ダッシュボード構築

# COMMAND ----------

# ── ダッシュボード & データセット ─────────────────────────────────────────────
db = LakeviewDashboard(DASH_NAME)

db.add_dataset("app_ds", "Application Summary",
    f"SELECT cluster_id, app_id, app_name, spark_user, cluster_name, worker_node_type, driver_node_type, min_workers, max_workers, dbr_version, cluster_availability, region, start_ts, end_ts, duration_ms, duration_min, total_jobs, succeeded_jobs, failed_jobs, job_success_rate, total_stages, completed_stages, failed_stages, total_tasks, total_input_gb, total_shuffle_gb, total_spill_gb, stages_with_disk_spill, total_gc_time_ms, gc_overhead_pct, total_exec_run_ms FROM {SCHEMA}.{TABLE_PREFIX}gold_application_summary {APP_FILTER_SQL}")

db.add_dataset("job_ds", "Job Performance",
    f"SELECT * FROM {SCHEMA}.{TABLE_PREFIX}gold_job_detail "
    f"{APP_FILTER_SQL} "
    f"ORDER BY duration_ms DESC NULLS LAST")

db.add_dataset("stage_ds", "Stage Performance",
    f"SELECT s.cluster_id, s.app_id, s.stage_id, s.attempt_id, s.stage_name, s.status, s.failure_reason, "
    f"s.num_tasks, s.task_count, s.failed_tasks, s.submission_ts, s.first_task_ts, s.completion_ts, "
    f"s.duration_ms, s.scheduling_delay_ms, ROUND(s.scheduling_delay_ms / 1000.0, 2) AS scheduling_delay_sec, "
    f"s.gc_overhead_pct, s.cpu_efficiency_pct, s.shuffle_read_mb, s.shuffle_write_mb, "
    f"s.shuffle_fetch_wait_ms, ROUND(s.shuffle_fetch_wait_ms / 1000.0, 2) AS shuffle_fetch_wait_sec, "
    f"s.disk_spill_mb, s.memory_spill_mb, "
    f"s.task_min_ms, s.task_avg_ms, s.task_p50_ms, s.task_p75_ms, s.task_p95_ms, s.task_p99_ms, s.task_max_ms, "
    f"s.task_skew_ratio, s.time_skew_gap_ms, "
    f"ROUND(s.task_p50_ms / 1000.0, 3) AS task_p50_sec, ROUND(s.task_max_ms / 1000.0, 3) AS task_max_sec, "
    f"ROUND(s.time_skew_gap_ms / 1000.0, 3) AS time_skew_gap_sec, "
    f"s.task_shuffle_min_mb, s.task_shuffle_p50_mb, s.task_shuffle_max_mb, "
    f"s.shuffle_skew_ratio, s.data_skew_gap_mb, "
    f"s.bottleneck_type, s.severity, s.recommendation, "
    f"jm.job_id "
    f"FROM {SCHEMA}.{TABLE_PREFIX}gold_stage_performance s "
    f"LEFT JOIN ("
    f"  SELECT cluster_id, app_id, job_id, CAST(sid AS INT) AS stage_id "
    f"  FROM (SELECT * FROM {SCHEMA}.{TABLE_PREFIX}gold_job_performance {APP_FILTER_SQL}) jp "
    f"  LATERAL VIEW EXPLODE(FROM_JSON(stage_ids, 'ARRAY<INT>')) t AS sid"
    f") jm ON s.cluster_id = jm.cluster_id AND s.app_id = jm.app_id AND s.stage_id = jm.stage_id "
    f"{'WHERE s.app_id = ' + chr(39) + APP_ID + chr(39) if APP_ID != 'ALL' else ''}")

db.add_dataset("exec_ds", "Executor Analysis",
    f"SELECT cluster_id, app_id, executor_id, host, total_cores, resource_profile_id, "
    f"onheap_memory_mb, offheap_memory_mb, task_cpus, add_ts, remove_ts, removed_reason, "
    f"total_tasks, total_task_ms, total_task_sec, avg_task_ms, avg_task_ms / 1000.0 AS avg_task_sec, "
    f"total_gc_ms, total_gc_sec, gc_pct, avg_gc_pct, avg_cpu_efficiency_pct, cpu_utilization_pct, "
    f"input_gb, shuffle_read_gb, shuffle_write_gb, "
    f"total_memory_spilled, total_disk_spilled, memory_spill_mb, disk_spill_mb, "
    f"peak_memory_mb, speculative_tasks, tasks_with_disk_spill, tasks_with_memory_spill, "
    f"app_avg_task_ms, load_vs_avg, z_score, "
    f"CAST(is_straggler AS STRING) AS is_straggler_str, "
    f"CAST(is_underutilized AS STRING) AS is_underutilized_str, "
    f"has_resource_issue, resource_diagnosis "
    f"FROM {SCHEMA}.{TABLE_PREFIX}gold_executor_analysis {APP_FILTER_SQL}")

db.add_dataset("jc_ds", "Job Concurrency",
    f"SELECT cluster_id, app_id, job_id, job_id_str, status, job_result, submit_ts, complete_ts, duration_ms, duration_sec, duration_min, concurrent_jobs_at_start, job_total_tasks, total_cpu_time_sec, total_exec_run_time_sec, job_cpu_efficiency_pct, avg_task_cpu_time_ms, total_gc_time_sec FROM {SCHEMA}.{TABLE_PREFIX}gold_job_concurrency {APP_FILTER_SQL} ORDER BY submit_ts")

db.add_dataset("bn_ds", "Bottleneck Report",
    f"SELECT cluster_id, app_id, job_id, stage_id, stage_name, status, severity, bottleneck_type, duration_ms, num_tasks, task_skew_ratio, gc_overhead_pct, disk_spill_mb, memory_spill_mb, shuffle_read_mb, task_p95_ms, task_p99_ms, recommendation, failure_reason FROM {SCHEMA}.{TABLE_PREFIX}gold_bottleneck_report {APP_FILTER_SQL}")

db.add_dataset("sql_ds", "SQL Photon Analysis",
    f"SELECT cluster_id, app_id, execution_id, description_short, start_ts, duration_sec, total_operators, photon_operators, photon_pct, is_photon, bhj_count, photon_bhj_count, smj_count, total_join_count, non_photon_op_list, photon_explanation FROM {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis {APP_FILTER_SQL} ORDER BY duration_sec DESC NULLS LAST")

db.add_dataset("sql_top5_ds", "SQL Top 5% Photon",
    f"SELECT AVG(photon_pct) AS avg_photon_pct_top5 FROM ("
    f"  SELECT photon_pct, NTILE(20) OVER (ORDER BY duration_sec DESC) AS ventile"
    f"  FROM {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis WHERE duration_sec IS NOT NULL {APP_AND_SQL}"
    f") WHERE ventile = 1")

db.add_dataset("sql_top10_ds", "SQL Top 10% Photon",
    f"SELECT AVG(photon_pct) AS avg_photon_pct_top10 FROM ("
    f"  SELECT photon_pct, NTILE(10) OVER (ORDER BY duration_sec DESC) AS decile"
    f"  FROM {SCHEMA}.{TABLE_PREFIX}gold_sql_photon_analysis WHERE duration_sec IS NOT NULL {APP_AND_SQL}"
    f") WHERE decile = 1")

# Spot / ノードロスト分析データセット
db.add_dataset("spot_ds", "Spot Instance Analysis",
    f"SELECT cluster_id, app_id, executor_id, host, total_cores, "
    f"CAST(added_ts AS STRING) AS added_ts, CAST(removed_ts AS STRING) AS removed_ts, "
    f"ROUND(lifetime_min, 1) AS lifetime_min, removal_type, "
    f"CAST(is_unexpected_loss AS STRING) AS is_unexpected_loss, "
    f"total_tasks_assigned, failed_tasks, "
    f"ROUND(shuffle_lost_mb, 0) AS shuffle_lost_mb, "
    f"ROUND(estimated_delay_sec, 1) AS estimated_delay_sec, "
    f"delay_breakdown "
    f"FROM {SCHEMA}.{TABLE_PREFIX}gold_spot_instance_analysis {APP_FILTER_SQL} "
    f"ORDER BY is_unexpected_loss DESC, lifetime_min ASC")

# Executor タイムライン（ノード増減）
db.add_dataset("exec_timeline_ds", "Executor Timeline",
    f"SELECT event_type, executor_id, host, "
    f"CAST(timestamp_ts AS STRING) AS event_ts, "
    f"COALESCE(removed_reason, '') AS removed_reason "
    f"FROM {SCHEMA}.{TABLE_PREFIX}silver_executor_events {APP_FILTER_SQL} "
    f"ORDER BY timestamp_ts")

# COMMAND ----------

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: 概要
# ══════════════════════════════════════════════════════════════════════════════
db.pages[0]["displayName"] = "パフォーマンス分析レポート"

# ── gold_narrative_summary から選択した app_id の LLM 生成テキストを取得 ────────
_PLACEHOLDER = "*サマリーが未生成です。`02_generate_summary_notebook` を先に実行してください。*"
try:
    _narr_filter = f"AND app_id = '{APP_ID}'" if APP_ID != "ALL" else ""
    _narr = spark.sql(f"""
        SELECT summary_text, job_analysis_text, node_analysis_text, top3_text
        FROM {SCHEMA}.{TABLE_PREFIX}gold_narrative_summary
        WHERE schema_name = '{SCHEMA}' {_narr_filter}
        ORDER BY generated_at DESC
        LIMIT 1
    """).collect()
    if _narr:
        _SUMMARY_TEXT       = _narr[0]["summary_text"] or _PLACEHOLDER
        _JOB_ANALYSIS_TEXT  = _narr[0]["job_analysis_text"] or _PLACEHOLDER
        _NODE_ANALYSIS_TEXT = _narr[0]["node_analysis_text"] or _PLACEHOLDER
        _TOP3_TEXT          = _narr[0]["top3_text"] or _PLACEHOLDER
        print(f"Loaded narrative from {SCHEMA}.{TABLE_PREFIX}gold_narrative_summary (app_id={APP_ID})")
    else:
        raise ValueError("No rows found")
except Exception as e:
    print(f"⚠ gold_narrative_summary の取得に失敗 ({e})")
    print("  → 02_generate_summary_notebook を先に実行してください")
    _SUMMARY_TEXT       = f"## 処理概要\n\n{_PLACEHOLDER}"
    _JOB_ANALYSIS_TEXT  = f"## ジョブ分析\n\n{_PLACEHOLDER}"
    _NODE_ANALYSIS_TEXT = f"## ノード分析\n\n{_PLACEHOLDER}"
    _TOP3_TEXT          = f"## 改善インパクト TOP 3\n\n{_PLACEHOLDER}"

# 概要ページ: 分析レポート + 推奨事項を1つのテキストに統合
_FULL_REPORT = _SUMMARY_TEXT + "\n\n---\n\n" + _TOP3_TEXT
add_widget(db, 0, make_text_widget(_FULL_REPORT), 0, 0, 6, 24)

print(f"\nTarget App: {APP_ID}")
print("ダッシュボード構築完了（概要ページ）")
print("※ 別の App ID のレポートを表示するには、app_id ウィジェットを変更して再実行してください")

# COMMAND ----------

# MAGIC %md ## 作成 & 公開

# COMMAND ----------

# ── 認証情報を Databricks コンテキストから自動取得 ────────────────────────────
ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
host  = ctx.apiUrl().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ── 既存ダッシュボードを検索 ───────────────────────────────────────────────────
target_id = EXISTING_DASHBOARD_ID

if not target_id:
    # ワークスペースAPIでファイルパスを直接指定して resource_id (=dashboard_id) を取得
    dashboard_path = f"{PARENT_PATH}/{DASH_NAME}.lvdash.json"
    r = requests.get(f"{host}/api/2.0/workspace/get-status",
                     headers=headers, params={"path": dashboard_path})
    if r.status_code == 200 and r.json().get("object_type") == "DASHBOARD":
        target_id = r.json().get("resource_id")
        print(f"Found existing dashboard: {target_id}")

# ── parent_path のフォルダが存在しない場合は作成 ────────────────────────────
_pp_check = requests.get(f"{host}/api/2.0/workspace/get-status",
                         headers=headers, params={"path": PARENT_PATH})
if _pp_check.status_code != 200:
    print(f"Parent path '{PARENT_PATH}' does not exist — creating ...")
    _pp_mk = requests.post(f"{host}/api/2.0/workspace/mkdirs",
                           headers=headers, json={"path": PARENT_PATH})
    assert _pp_mk.status_code == 200, f"MKDIRS ERROR: {_pp_mk.status_code} {_pp_mk.text}"
    print(f"Created folder: {PARENT_PATH}")

# ── ダッシュボードを作成 or 更新（IDを引き継いでURLを固定） ───────────────────
payload = db.get_api_payload(WAREHOUSE_ID, PARENT_PATH)
payload["display_name"] = DASH_NAME

if target_id:
    # 既存IDに対して PATCH → URLが変わらない
    r = requests.patch(f"{host}/api/2.0/lakeview/dashboards/{target_id}", headers=headers, json=payload)
    assert r.status_code == 200, f"UPDATE ERROR: {r.status_code} {r.text}"
    dashboard_id = target_id
    print(f"Updated: {dashboard_id}")
else:
    # 初回作成
    r = requests.post(f"{host}/api/2.0/lakeview/dashboards", headers=headers, json=payload)
    assert r.status_code == 200, f"CREATE ERROR: {r.status_code} {r.text}"
    dashboard_id = r.json()["dashboard_id"]
    print(f"Created: {dashboard_id}")

# ── 公開 ──────────────────────────────────────────────────────────────────────
r = requests.post(
    f"{host}/api/2.0/lakeview/dashboards/{dashboard_id}/published",
    headers=headers,
    json={"warehouse_id": WAREHOUSE_ID}
)
assert r.status_code == 200, f"PUBLISH ERROR: {r.status_code} {r.text}"

workspace_host = host.rstrip("/")
print("Published!")
print(f"URL: {workspace_host}/dashboardsv3/{dashboard_id}/published")