# API Reference

Databricks Performance Toolkit exposes 55+ endpoints across 9 Flask Blueprints.
Auto-generated docs are also available at `/api/docs` in the running app.

## Authentication

When deployed as a Databricks App, authentication is handled automatically via Service Principal (SP).
No API keys or tokens are required for the Web UI or API calls within the app context.

## Error Responses

All API errors return JSON:

```json
{"error": "Error message here"}
```

| Status | Meaning |
|--------|---------|
| 400 | Bad request (missing/invalid parameters) |
| 404 | Resource not found |
| 503 | Service unavailable (warehouse/config not set) |

---

## Core

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Home page (upload UI) |
| GET | `/health` | Health check |

---

## Analysis (`routes/analysis.py`)

### POST `/api/v1/analyze`

Upload a query profile JSON and start analysis.

**Content-Type**: `multipart/form-data`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file` | file | Yes | Query profile JSON |
| `skip_llm` | string | No | `"true"` to skip LLM analysis |
| `primary_model` | string | No | Primary LLM model (default: `databricks-claude-opus-4-6`) |
| `review_model` | string | No | Review LLM model |
| `refine_model` | string | No | Refine LLM model (default: `databricks-gpt-5-4`) |
| `enable_report_review` | string | No | `"on"` to enable report review step |
| `enable_report_refine` | string | No | `"on"` to enable report refine step |
| `enable_table_write` | string | No | `"on"` to persist results to Delta |
| `profiler_catalog` | string | No | Target catalog for persistence |
| `profiler_schema` | string | No | Target schema for persistence |
| `profiler_http_path` | string | No | SQL Warehouse HTTP path |
| `experiment_id` | string | No | Experiment ID for tracking |
| `variant` | string | No | Variant name (`"baseline"` marks as baseline) |
| `explain_text` | string | No | EXPLAIN EXTENDED output |
| `explain_file` | file | No | EXPLAIN file upload |

**Response**:
```json
{
  "id": "analysis-uuid",
  "status": "pending",
  "validation_warnings": [],
  "is_verbose": true
}
```

### GET `/api/v1/analyze/<analysis_id>/status`

Poll analysis progress.

**Response**:
```json
{
  "id": "analysis-uuid",
  "status": "pending|processing|completed|failed",
  "stage": "extracting|analyzing|llm_analysis|report|done",
  "redirect_url": "/result/analysis-uuid",
  "error": "..."
}
```

### GET `/api/v1/analyze/<analysis_id>`

Get full analysis result.

### GET `/api/v1/analyze/<analysis_id>/download`

Download report as `.md` file.

### GET `/result/<analysis_id>`

Render analysis result page (HTML).

---

## History (`routes/history.py`)

### GET `/api/v1/history`

List past analyses from Delta tables.

| Parameter | Type | Description |
|-----------|------|-------------|
| `fingerprint` | string | Filter by query fingerprint |
| `experiment_id` | string | Filter by experiment |
| `variant` | string | Filter by variant |
| `limit` | int | Max results (1-200, default: 50) |

**Response**: Array of analysis summaries.

### POST `/api/v1/history/delete`

Delete analyses by IDs.

**Body**: `{"analysis_ids": ["id1", "id2"]}`

### GET `/history`

History page (HTML).

---

## Compare (`routes/compare.py`)

### POST `/api/v1/compare`

Compare two analyses.

**Body**: `{"before_id": "...", "after_id": "...", "enable_llm": true, "model": "..."}`

### GET `/compare`

Compare page (HTML).

---

## Spark Performance (`routes/spark_perf.py`)

All Spark endpoints require Spark Perf HTTP path to be configured. Returns 503 if not set.

### GET `/api/v1/spark-perf/applications`

List Spark applications from Gold tables.

### GET `/api/v1/spark-perf/report`

Generate full Markdown report for a Spark application.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `app_id` | string | Yes | Spark application ID |
| `lang` | string | No | Language (`ja`/`en`, default: `ja`) |

### GET `/api/v1/spark-perf/summary`

Get application summary + bottleneck report. Requires `app_id`.

### GET `/api/v1/spark-perf/stages`

Get stage performance. Requires `app_id`.

### GET `/api/v1/spark-perf/executors`

Get executor analysis. Requires `app_id`.

### GET `/api/v1/spark-perf/jobs`

Get job performance. Requires `app_id`.

### GET `/api/v1/spark-perf/concurrency`

Get job concurrency analysis. Requires `app_id`.

### GET `/api/v1/spark-perf/narrative`

Get LLM narrative summary, optionally translated.

### GET `/api/v1/spark-perf/spot`

Get spot instance / node loss analysis. Requires `app_id`.

### GET `/api/v1/spark-perf/sql-photon`

Get SQL/Photon analysis. Requires `app_id`.

### GET `/spark-perf`

Spark Performance page (HTML).

### POST `/api/v1/spark-perf/etl-runs`

Trigger Spark Perf ETL pipeline job.

**Body**: `{"log_root": "/Volumes/...", "cluster_id": "0322-..."}`

**Response**: `{"run_id": 12345, "status": "PENDING", "run_page_url": "..."}`

Requires `spark_perf_job_id` to be configured.

### GET `/api/v1/spark-perf/etl-runs/<run_id>/status`

Poll ETL job run status.

**Response**: `{"run_id": 12345, "state": "RUNNING|TERMINATED", "result_state": "SUCCESS|FAILED", "run_page_url": "..."}`

---

## Settings (`routes/settings.py`)

### GET `/api/v1/settings`

Get current DBSQL profiler settings.

**Response**:
```json
{
  "catalog": "main",
  "schema": "profiler",
  "http_path": "/sql/1.0/warehouses/...",
  "table_write_enabled": false
}
```

### POST `/api/v1/settings`

Save settings to local config file.

**Body**: `{"catalog": "...", "schema": "...", "http_path": "...", "table_write_enabled": "true"}`

### GET `/api/v1/debug/config`

Show effective settings with source information for debugging.

**Response**:
```json
{
  "settings": {
    "catalog": {"value": "main", "source": "runtime_config"},
    "schema": {"value": "profiler", "source": "default"},
    "http_path": {"value": "/sql/...", "source": "env"}
  },
  "config_paths": {
    "user_config": "~/.dbsql_profiler_config.json",
    "runtime_config": "/app/runtime-config.json"
  }
}
```

Source values: `env`, `user_config`, `runtime_config`, `default`.

### POST `/api/v1/lang`

Set language preference. Body: `{"lang": "ja"}`. Sets cookie.

### GET/POST `/api/v1/spark-perf/settings`

Get/save Spark Perf settings (catalog, schema, table_prefix, http_path).

---

## Report (`routes/report.py`)

### GET `/report`

Report upload page (HTML).

### POST `/api/v1/report/upload`

Upload a Markdown report file for viewing.

### GET `/report/<report_id>`

View uploaded report (HTML).

### GET `/api/v1/report/<report_id>/download`

Download uploaded report as `.md`.

---

## Share (`routes/share.py`)

### GET `/shared/<analysis_id>`

Persistent shared result page. Falls back from in-memory to Delta table.

### GET `/api/v1/shared/<analysis_id>/summary`

Plain-text Slack summary for an analysis.

| Parameter | Type | Description |
|-----------|------|-------------|
| `base_url` | string | Base URL for links (from `window.location.origin`) |

---

## Workload (`routes/workload.py`)

### GET `/workload`

Cross-analysis page: DBSQL + Spark side-by-side (HTML).

### GET `/api/v1/workload/pairs`

List saved DBSQL ↔ Spark app pairs.

### POST `/api/v1/workload/pairs`

Save a pair. Body: `{"analysis_id": "...", "app_id": "...", "label": "..."}`

### DELETE `/api/v1/workload/pairs`

Delete a pair. Body: `{"analysis_id": "...", "app_id": "..."}`

### GET `/api/v1/workload/report`

Fetch DBSQL report markdown. Requires `analysis_id` query parameter.

### POST `/api/v1/workload/cross-analyze`

LLM-powered cross-analysis of DBSQL and Spark reports. [Experimental]

**Body**: `{"dbsql_report": "...", "spark_report": "...", "model": "databricks-claude-opus-4-6"}`

**Response**: `{"analysis": "## Cross Analysis\n..."}`

---

## Models

### GET `/api/v1/models`

List available LLM chat models from Databricks Serving Endpoints.

**Response**: `[{"name": "databricks-claude-opus-4-6", "display_name": "Claude Opus 4.6"}, ...]`
