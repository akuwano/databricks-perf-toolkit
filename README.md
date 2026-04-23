# Databricks Performance Toolkit

A tool for analyzing Databricks SQL query profile JSON and generating performance reports. It uses LLM (Databricks Foundation Model APIs) to identify bottlenecks and provide specific improvement recommendations.

[日本語版 README](README.ja.md)

## Features

### Core Analysis
- **Query Profile Analysis**: Automatically extract metrics from query profile JSON
- **Bottleneck Detection**: Automatic calculation and evaluation of bottleneck indicators
  - Cache hit ratio (>80% good, <30% critical)
  - Photon utilization (>80% good, <50% critical)
  - Disk spill detection (>5GB critical, >1GB important)
  - Shuffle efficiency (512MB/partition threshold)
  - Filter efficiency (pruning rate)
- **EXPLAIN Analysis**: Parse EXPLAIN EXTENDED output for detailed execution plan insights
- **Join Type Classification**: Classify join types and determine Photon compatibility
- **LLM-Powered Recommendations**: 3-stage AI analysis (Initial → Review → Refine) using Databricks Foundation Model APIs
- **Actionable Reports**: Generate reports with Top Findings, Hot Operators, and Validation Checklists
- **Multi-language Support**: English (default) and Japanese

### v3: Comparison & Experimentation
- **Profile Comparison**: Before/after comparison with 15 direction-aware metrics (lower-is-better, higher-is-better) and LLM-generated comparison summary
- **Query Family Grouping**: Identify same-purpose queries across hint/JOIN/filter changes using `purpose_signature`
- **SQL Fingerprint**: Normalized SQL hash for tracking the same query across executions
- **Experiment & Variant Tracking**: Tag analyses with `--experiment-id` and `--variant` for A/B testing workflows
- **Variant Ranking**: Weighted scoring view with disqualification guardrails for variant selection

### v3: Knowledge & Persistence
- **Knowledge Base**: Auto-generated documents from analyses and comparisons, searchable by tags
- **Delta Table Persistence**: Store all analysis results, comparisons, and knowledge in 11 Delta tables
- **Persistent Settings**: Catalog/schema/warehouse saved to `~/.dbsql_profiler_config.json`
- **Curated SQL Views**: 7 views for Genie (Text2SQL) integration including regression detection and recommendations

### v3: Web UI Additions
- **Analysis History** (`/history`): Browse past analyses with filtering and search
- **Side-by-Side Comparison** (`/compare`): Visual diff of two analyses with metric direction indicators
- **Settings Persistence**: Catalog, schema, and HTTP path saved across sessions

### v4: Spark Performance Analysis
- **Spark Perf ETL Notebooks**: 4 Databricks notebooks (`dabs/notebooks/`) — Pipeline v2 (17 tables, Gold MERGE upsert), LLM Summary (4 sections), Knowledge Base, Knowledge Manager
- **Spark Perf Web UI** (`/spark-perf`): Markdown report with LLM narrative integration, ETL job trigger from UI
- **Workload Cross-Analysis** (`/workload`): DBSQL + Spark side-by-side comparison with LLM cross-analysis [Experimental]
- **9 Bottleneck Types**: STAGE_FAILURE, DISK_SPILL, HIGH_GC, DATA_SKEW, HEAVY_SHUFFLE, MEMORY_SPILL, MODERATE_GC, SPOT_LOSS, PHOTON_FALLBACK, SKEW_SHUFFLE_PARALLELISM
- **LLM Narrative Summary**: 4-section AI analysis (summary, job analysis, node analysis, top 3) with EN/JA auto-translation
- **Baseline Auto-Comparison**: New analyses automatically compared with baseline of same query family, regression/improvement shown on result page
- **Use-Case Quick Start**: 3 guided cards on top page (Analyze Query, Compare & Optimize, Spark Job Analysis)
- **Gold MERGE (Upsert)**: Historical retention via MERGE instead of overwrite for Gold tables (v4.13.5)
- **Knowledge Base i18n**: section_id based routing (decoupled from Japanese headings), bilingual knowledge files (ja/en) (v4.13)
- **Bilingual Spark Perf Reports**: ~200 labels with ja/en support, "Japanese Report" toggle, LLM model selector (v4.13)
- **Schema Validation**: Settings save validates schema for both DBSQL and Spark Perf configurations (v4.13.5)
- **Table Auto-Init**: Delta tables auto-initialized on DBSQL settings save with CREATE TABLE privilege check and GRANT guidance (v4.13.5)
- **Applications Pagination**: 20 per page for Spark Perf application list (v4.13.5)
- **LLM Narrative Direct Display**: Skip reporter generation, display narrative directly (v4.13.5)
- **Evidence-Constrained Recommendations**: 7-field format with HARD RULES and Confidence Criteria (v4.12.5)
- **Structured Fact Pack Summary**: YAML block with top_alerts, dominant_operations, confidence_notes (v4.12.5)
- **Compare Analysis LLM Improvements**: C1-C9 enhancements including causal graph constraints and counter-evidence checks (v4.12.5)
- **Genie Chat Panel**: Genie Conversation API chat on DBSQL, Spark Perf, and comparison pages with SP auth and Space auto-recreation (v4.14)
- **Spark Comparison**: Before/after comparison of Spark applications with history tab, report view, delete, Experiment/Variant tracking, and 5-level verdict (v4.14)
- **Comparison History Persistence**: Comparison results persisted to Delta with column sorting (v4.14)
- **Inline Experiment/Variant Editing**: Edit experiment/variant directly on result page with cascade update (v4.14)
- **OBO Authentication**: On-Behalf-Of auth for reads/LLM, Service Principal auth for writes/Genie/jobs (v4.15)
- **Deploy Automation**: Auto-create catalogs/schemas, SP write grants, job CAN_MANAGE_RUN, reset to deploy defaults (v4.15)
- **Post-Deploy Smoke Tests**: API 31 checks + UI 12 checks (Playwright), auto-run after deploy, --full-test flag (v4.16)
- **Spark Perf App-Side LLM Report**: 2-call strategy generating sections 1-7 + Recommended Actions with Spark tuning knowledge base, longest-prefix model max_tokens auto-adjustment (v4.26)
- **SQL Accuracy Evaluation Framework**: 4-axis scoring (L1 syntax, L2 evidence grounding, L3 diagnosis accuracy via LLM-as-judge, L4 fix effectiveness) with `--diff-from` before/after comparison using git worktrees (v4.26)
- **DBSQL Cost Estimation**: Per-query cost estimation based on warehouse size (2X-Small to 4X-Large), billing model (Serverless/Pro/Classic), and DBU pricing. Parallelism-ratio fallback when warehouse API is unavailable, with reference cost table showing nearest T-shirt sizes (v4.28)
- **Streaming Query Support**: Detect and analyze DLT/SDP streaming profiles (`REFRESH STREAMING TABLE`). Micro-batch statistics (min/avg/max/p95 duration, read bytes, rows), batch-oriented report sections, slow batch detection, and LLM prompt integration (v4.29)
- **SQL Query Rewrite**: LLM-powered SQL optimization with Rewrite button, automatic EXPLAIN/sqlglot validation, and iterative Refine flow for fine-tuning results (v4.38)
- **Schema Analysis** (`/schema-analysis`): Detect suboptimal data types (decimal(38,0)→INT/BIGINT), cross-table type mismatches for JOIN keys, partition design anti-patterns, clustering key type issues, implicit CAST detection from aggregate expressions, and JOIN type mismatch from past analyses with migration DML generation (v5.0)
- **Enhanced EXPLAIN Analysis**: When EXPLAIN EXTENDED is attached, use optimizer statistics to confirm/deny stale stats, detect Photon blockers by name, extract DFP selectivity and Runtime Filters (v4.41)
- **Improved Alert Quality**: Aggregated shuffle alerts with severity aligned to optimization_priority, Serverless scan locality downgraded, hash join alerts with 3-tier skew detection, cloud storage retry suppressed when duration=0 (v4.42)
- **Scan Locality Per-Node**: Per-scan-node local/non-local + cache hit table with cold node pattern detection
- **History Management**: Checkbox selection + batch deletion of past analyses
- **Databricks Asset Bundles**: Serverless jobs + apps deployment with `local-overrides.yml` pattern

## Requirements

- Python 3.11+
- uv (recommended) or pip
- Databricks Workspace (for LLM analysis)

## Installation

```bash
# Clone the repository
git clone https://github.com/akuwano/databricks-perf-toolkit.git
cd dbsql_profiler_analysis_tool

# Install dependencies (with uv)
uv sync

# Or with pip
pip install -e .
```

## Usage

### Step 1: Get Query Profile JSON

1. Run a query in Databricks SQL Warehouse
2. Select the target query from Query History
3. Open the "Query Profile" tab
4. Select "Download profile" from the "..." menu in the upper right
5. **Important: Select "Verbose" mode** before downloading to include detailed per-node metrics (memory, spill, I/O, scan locality)
6. Download the JSON file

> **Note:** Both DBSQL profiles and Spark Connect profiles (`entryPoint=SPARK_CONNECT`) are automatically detected and analyzed. **Verbose mode is strongly recommended** — without it, many advanced metrics (peak memory, cloud storage retries, data filter statistics, scan locality) will be unavailable.

### Step 1.5: Get EXPLAIN EXTENDED (Recommended)

To enable detailed Photon blocker detection and SQL rewrite suggestions, obtain the EXPLAIN EXTENDED output:

1. Run the following in DBSQL Query Editor:
   ```sql
   EXPLAIN EXTENDED <your query>
   ```
2. Save the result as a text file (e.g., `explain.txt`)
3. Pass it with the `--explain` option

### Step 2: Set Environment Variables (CLI only)

> **Note:** If using the **Web UI (Databricks Apps)**, skip this step — authentication is handled automatically via service principal.

For CLI usage with LLM analysis:

```bash
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-token>"
```

### Step 3: Generate Report

```bash
cd dabs/app

# Basic usage (with LLM analysis)
uv run python -m cli.main <profile.json> -o report.md

# Specify model
uv run python -m cli.main <profile.json> --model databricks-claude-opus-4-6 -o report.md

# Skip LLM analysis (metrics only)
uv run python -m cli.main <profile.json> --no-llm -o report.md

# Output to stdout
uv run python -m cli.main <profile.json>
```

### Step 4: Review Report and Take Action

Generated reports include (conclusion-first structured layout):

1. **Executive Summary** (LLM) - Severity assessment, key findings, and impact at a glance
2. **Query Overview** - Query ID, execution time, status, formatted SQL with structure analysis
3. **Performance Metrics** - Bottleneck indicators, I/O metrics, Hot Operators, spill analysis
4. **Data Flow** - Data volume flow through query stages
5. **Alerts** - Automatically detected critical issues and warnings
6. **Stage Execution** - Execution timeline and stage-level details
7. **Root Cause Analysis** (LLM) - Direct cause and underlying root cause identification
8. **Recommendations** (LLM + Rule-based) - Prioritized improvement actions with Action Cards
9. **Optimized SQL** (LLM) - Rewritten SQL with BROADCAST hints, CTE optimization, Photon-compatible functions
10. **Conclusion** (LLM) - Summary and next steps
- **Appendix A**: Validation Checklist

#### Understanding Bottleneck Indicators

| Indicator | Description | Notes |
|-----------|-------------|-------|
| Cache Hit Ratio | Ratio of data read from cache vs total | Low on first run is expected |
| Remote Read Ratio | Ratio of data read from remote storage | Warning level, not critical |
| Photon Utilization | Percentage of time spent in Photon | Check Photon Blockers if low |
| Rescheduled Scan Ratio | Non-local scan tasks / total scan tasks | Shown only when scan task data exists |
| Disk Spill | Amount of data spilled to disk | Indicates memory pressure |
| Shuffle Impact Ratio | Shuffle time / total task time | High ratio suggests join optimization |
| Filter Efficiency | Files pruned / total files | Low indicates partitioning issues |

### Profile Comparison

Compare two analyses to measure the impact of query changes:

```bash
cd databricks-apps

# 1. Analyze the "before" profile
uv run python -m cli.main before.json --persist --experiment-id exp001 --variant baseline -o before.md

# 2. Analyze the "after" profile
uv run python -m cli.main after.json --persist --experiment-id exp001 --variant optimized -o after.md

# 3. Compare the two (using analysis IDs from step 1 and 2)
uv run python -m cli.main after.json --compare-with <before-analysis-id> --persist -o comparison.md
```

The comparison report includes 15 direction-aware metrics, a delta summary, and LLM-generated insights highlighting regressions and improvements.

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `profile` | Path to query profile JSON file | (required) |
| `-o, --output` | Output file path | stdout |
| `--model` | LLM model to use | databricks-claude-opus-4-6 |
| `--tuning-file` | Path to dbsql_tuning.md file or directory | auto-detect |
| `--no-llm` | Skip LLM analysis (metrics and rule-based analysis only) | false |
| `--explain` | Path to EXPLAIN EXTENDED output file (enables Photon blocker detection with SQL rewrite suggestions) | - |
| `--lang` | Output language (`en` or `ja`) | en |
| `--persist` | Save analysis results to Delta tables | false |
| `--experiment-id` | Experiment identifier for A/B testing | - |
| `--variant` | Variant label (e.g., `baseline`, `optimized`) | - |
| `--compare-with` | Analysis ID to compare against (before profile) | - |
| `--tags` | Comma-separated tags for the analysis | - |

> **Note:** The `--explain` option is highly recommended for accurate Photon blocker detection. Without it, only basic heuristics are used.

## Available LLM Models

Models available via Databricks Foundation Model APIs:

- `databricks-claude-opus-4-6` (default)
- `databricks-claude-sonnet-4-6`
- `databricks-claude-sonnet-4`
- `databricks-gpt-5-4`
- `databricks-meta-llama-4-maverick`
- `databricks-meta-llama-3-3-70b-instruct`

## Bottleneck Indicator Thresholds

| Indicator | Good | Needs Improvement | Critical |
|-----------|------|-------------------|----------|
| Cache Hit Ratio | >80% | 50-80% | <30% |
| Photon Utilization | >80% | 50-80% | <50% |
| Disk Spill | 0 | <1GB | >5GB |
| Shuffle Impact Ratio | <20% | 20-40% | >40% |
| Memory/Partition | <512MB | - | >512MB |

## Spark Perf ETL (Notebooks)

The `dabs/notebooks/` directory contains 2 Databricks notebooks that run as scheduled jobs to process Spark event logs:

| Notebook | Description |
|----------|-------------|
| `01_Spark Perf Pipeline PySpark.py` | Main ETL: reads Spark event logs, generates 16 tables in Bronze/Silver/Gold medallion architecture. Parameters: `log_root`, `cluster_id`, `schema`, `table_prefix`. |
| `02_generate_summary_notebook.py` | LLM Summary: reads Gold tables, calls LLM (Claude/Llama) to generate natural language summary, writes to `gold_narrative_summary` table. |

**Gold Tables (7)** read by the Web UI:

| Table | Description |
|-------|-------------|
| `application_summary` | Application-level metrics and duration |
| `job_performance` | Job-level duration and task counts |
| `stage_performance` | Stage-level metrics with bottleneck classification |
| `executor_analysis` | Executor resource utilization and straggler detection |
| `bottleneck_report` | Classified bottlenecks with severity and recommendations |
| `job_concurrency` | Job concurrency, CPU efficiency, and scheduling delays |
| `sql_photon_analysis` | SQL execution Photon utilization and operator analysis |

**Bottleneck Classification:**

| Type | Severity | Condition |
|------|----------|-----------|
| STAGE_FAILURE | HIGH | Stage status == FAILED |
| DISK_SPILL | HIGH | disk_bytes_spilled > 0 |
| HIGH_GC | MEDIUM | gc_overhead_pct > 20% |
| DATA_SKEW | MEDIUM | task_skew_ratio > 5 |
| HEAVY_SHUFFLE | LOW | shuffle_read_bytes > 10GB |
| MEMORY_SPILL | LOW | memory_bytes_spilled > 0 |
| MODERATE_GC | LOW | gc_overhead_pct > 10% |

## Spark Perf Web UI

The Spark Perf page (`/spark-perf`) provides:

- **Markdown Report**: Unified report with 8 numbered body sections + Appendix (same architecture as DBSQL)
- **LLM Narrative**: Auto-generated natural language summary integrated into the report (app-side 2-call LLM strategy in v4.26)
- **App-Side LLM Report**: 2-call LLM strategy — Call 1: sections 1-2 + Recommended Actions, Call 2: sections 3-7 — with Spark tuning knowledge base for context-aware recommendations (v4.26)
- **ETL Job Trigger**: Run ETL pipeline directly from the UI (Volume path + Cluster ID → Job API)
- **Application Selector**: Browse and select from Gold table data

## Workload Cross-Analysis

The Workload page (`/workload`) enables side-by-side analysis:

- **Manual Pairing**: Select a DBSQL analysis and a Spark app to view reports side by side
- **LLM Cross-Analysis**: AI-powered correlation analysis identifying common bottlenecks [Experimental]
- **Pair Persistence**: Save linked pairs for quick recall

**Spark Perf Configuration** (stored in `~/.dbsql_profiler_config.json`):

| Key | Environment Variable | Description |
|-----|---------------------|-------------|
| `spark_perf_catalog` | `SPARK_PERF_CATALOG` | Unity Catalog name |
| `spark_perf_schema` | `SPARK_PERF_SCHEMA` | Schema name |
| `spark_perf_table_prefix` | `SPARK_PERF_TABLE_PREFIX` | Table name prefix |
| `spark_perf_http_path` | `SPARK_PERF_HTTP_PATH` | SQL Warehouse HTTP path |

## Project Structure

```
dbsql_profiler_analysis_tool/
├── README.md                 # This file
├── README.ja.md              # Japanese README
├── pyproject.toml            # Project settings
├── dabs/                     # Databricks Asset Bundles
│   ├── databricks.yml        # Bundle config (variables + dev/prod targets)
│   ├── resources/
│   │   ├── jobs.yml          # Spark Perf Pipeline job (serverless)
│   │   └── apps.yml          # Web UI app deployment
│   ├── notebooks/            # ETL notebooks
│   │   ├── 01_Spark Perf Pipeline PySpark.py
│   │   └── 02_generate_summary_notebook.py
│   └── app/                  # Flask Web UI + CLI
│       ├── app.py            # Flask application (Blueprint architecture)
│       ├── app.yaml          # Databricks Apps configuration
│       ├── routes/           # Flask Blueprints
│       ├── cli/              # CLI entry point
│       ├── core/             # Analysis logic
│       ├── services/         # Delta table readers/writers
│       ├── templates/        # HTML templates
│       └── tests/            # Unit tests (1078+)
├── eval/                     # SQL accuracy evaluation framework (v4.26)
│   ├── cli.py                # CLI entry point (python -m eval)
│   ├── runner.py             # Pipeline execution + scoring
│   ├── diff_runner.py        # --diff-from before/after comparison (git worktree)
│   ├── scorers/              # L1 syntax, L2 evidence, L3/L4 LLM-as-judge
│   ├── fixtures/             # Profile JSON test data
│   └── tests/                # Eval unit tests (44+)
├── scripts/
│   ├── deploy_views.py       # Deploy SQL views for Genie
│   └── eval_models.py        # LLM model evaluation
├── docs/
│   ├── v3-detailed-design.md
│   ├── genie-space-setup.md
│   └── llm-model-evaluation-results.md
├── TODO.md                   # Improvement backlog
└── CLAUDE.md                 # Development guidelines
```

## Deploy SQL Views

Deploy curated SQL views for Genie (Text2SQL) integration:

```bash
cd scripts

# Deploy views to a specific catalog/schema
uv run python deploy_views.py --catalog my_catalog --schema profiler

# Reset tables and redeploy
uv run python deploy_views.py --catalog my_catalog --schema profiler --reset-tables
```

This creates 7 SQL views:

| View | Description |
|------|-------------|
| `vw_latest_analysis_by_fingerprint` | Latest analysis per unique SQL fingerprint |
| `vw_comparison_diff` | Metric deltas between compared profiles |
| `vw_regression_candidates` | Queries with performance regressions |
| `vw_genie_profile_summary` | Simplified profile summary for Genie |
| `vw_genie_comparison_summary` | Simplified comparison summary for Genie |
| `vw_genie_recommendations` | Actionable recommendations for Genie |
| `vw_variant_ranking` | Weighted variant ranking with guardrails |

## Genie Integration

For setting up a Genie Space to query analysis results with natural language, see [docs/genie-space-setup.md](docs/genie-space-setup.md).

## Deployment (Databricks Asset Bundles)

All resources are deployed via [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) using the `deploy.sh` script.

### Quick Start

```bash
# 1. Copy and edit local-overrides.yml (one-time setup)
cp dabs/local-overrides.yml.sample dabs/local-overrides.yml
# Edit dabs/local-overrides.yml with your warehouse_id, catalog, schema, etc.

# 2. Deploy (generates config, deploys bundle, starts app, grants permissions)
./scripts/deploy.sh dev       # dev environment
./scripts/deploy.sh staging   # staging environment
./scripts/deploy.sh prod      # production environment
```

`deploy.sh` performs the following steps automatically:
1. Generates `runtime-config.json` from `local-overrides.yml`
2. Generates `app.yaml` with SQL Warehouse resource declaration
3. Auto-creates catalogs/schemas and grants SP write permissions (v4.15)
4. Deploys the Databricks Asset Bundle
5. Starts the app
6. Grants `CAN_USE` permission on the SQL Warehouse to the app's service principal
7. Runs post-deploy smoke tests (API 31 checks + UI 12 checks) (v4.16)

> **All configuration is done in `dabs/local-overrides.yml` only.** Files like `runtime-config.json` and `app.yaml` are auto-generated and should not be edited manually.

### Run Resources Individually

```bash
cd dabs

# Run Spark Perf ETL pipeline (01_Pipeline → 02_Summary)
databricks bundle run spark_perf_pipeline

# Start the Web UI app (without full deploy)
databricks bundle run profiler_app
```

### Configuration Variables

All variables are set in `dabs/local-overrides.yml` per target:

| Variable | Description | Example |
|----------|-------------|---------|
| `sparkperf_catalog` | Spark Perf catalog | `main` |
| `sparkperf_schema` | Spark Perf schema | `base2` |
| `dbsql_catalog` | DBSQL Profiler catalog | `my_catalog` |
| `dbsql_schema` | DBSQL Profiler schema | `dbsql_profiler` |
| `warehouse_id` | SQL Warehouse ID | `your-warehouse-id` |
| `log_root` | Spark event log path | `/Volumes/main/base/data/...` |
| `cluster_id` | Cluster ID for logs | `your-cluster-id` |
| `app_name` | Databricks App name | `your-app-name` |

> **Note:** No secret configuration is required. Databricks Apps automatically provides service principal authentication (SDK auth). The deploy script automatically grants the app's service principal `CAN_USE` access to the configured SQL Warehouse.

### Web UI Features

- **File Upload**: Drag & drop or file selection to upload JSON
- **Markdown Report Viewer**: Upload previously generated Markdown reports for viewing/sharing
- **LLM Analysis Options**:
  - Primary/Review/Refine model selection (Web UI supports 3-stage model selection)
  - Skip LLM analysis (metrics only)
- **Analysis Results**:
  - Bottleneck indicator visualization
  - List of detected issues
  - LLM analysis report
- **Analysis History** (`/history`): Browse and search past analyses with filtering (v3)
- **Side-by-Side Comparison** (`/compare`): Visual diff with direction-aware metric indicators (v3)
- **Settings Persistence**: Catalog, schema, and HTTP path saved to `~/.dbsql_profiler_config.json` (v3)
- **Spark Performance Analysis** (`/spark-perf`): 7-tab dashboard with Chart.js charts, KPI cards, bottleneck classification, LLM narrative summaries, bilingual reports (ja/en), LLM model selector, and applications pagination (v4/v4.13)
- **Genie Chat** (`/genie-chat`): Genie Conversation API chat panel on DBSQL, Spark Perf, and comparison pages (v4.14)
- **Spark Comparison**: Before/after Spark app comparison with 5-level verdict, history, and report view (v4.14)
- **Inline Editing**: Edit experiment/variant directly on result page with cascade update (v4.14)
- **OBO Authentication**: On-Behalf-Of auth for reads/LLM; SP auth for writes/Genie/jobs (v4.15)
- **Schema Validation**: Settings save validates required fields for both DBSQL and Spark Perf (v4.13.5)
- **Export**: Download as Markdown, print/save as PDF
- **Dark Mode**: Auto-detect + manual toggle
- **Language Switch**: Click **EN/JA** toggle in the header
- **Navigation**: Analyze | View Report | Compare | **Spark Perf** | EN/JA | Theme
- **Security**: Markdown output is sanitized to prevent XSS attacks

### Running Locally

For development and testing:

```bash
# Set environment variables
export DATABRICKS_HOST="https://xxx.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-token>"

# Start Flask app
cd dabs/app
uv run flask --app app.py run --host 0.0.0.0 --port 8000

# Access http://localhost:8000 in your browser
```

## Tuning Guide

For detailed tuning methods, see [dbsql_tuning.md](databricks-apps/core/knowledge/dbsql_tuning.md).

Main topics:
- I/O efficiency (partitioning, Z-Order, Liquid Clustering)
- Execution plan improvement (join types, Spark parameters)
- Shuffle optimization (AQE settings, REPARTITION hints)
- Spill countermeasures
- Improving Photon utilization
- Cluster size adjustment

## DBSQL Cost Estimation

The tool estimates per-query cost based on warehouse configuration and DBU pricing:

| Cluster Size | DBU/hour | Serverless ($0.70) | Pro ($0.55) | Classic ($0.22) |
|-------------|----------|-------------------|-------------|-----------------|
| 2X-Small | 2 | $1.40/h | $1.10/h | $0.44/h |
| X-Small | 4 | $2.80/h | $2.20/h | $0.88/h |
| Small | 8 | $5.60/h | $4.40/h | $1.76/h |
| Medium | 16 | $11.20/h | $8.80/h | $3.52/h |
| Large | 32 | $22.40/h | $17.60/h | $7.04/h |
| X-Large | 64 | $44.80/h | $35.20/h | $14.08/h |
| 2X-Large | 128 | $89.60/h | $70.40/h | $28.16/h |
| 3X-Large | 256 | $179.20/h | $140.80/h | $56.32/h |
| 4X-Large | 512 | $358.40/h | $281.60/h | $112.64/h |

> Prices are Premium tier, us-west-2, Pay-As-You-Go. Serverless is per-query billing; Classic/Pro show estimated query share of hourly cost.

When the warehouse API is unavailable, cost is estimated from the parallelism ratio (task_total_time_ms / execution_time_ms) with a reference table showing nearest T-shirt sizes.

## Supported Input Files

| File Type | Description | Required |
|-----------|-------------|----------|
| Query Profile JSON | Downloaded from DBSQL Query Profile | Yes |
| EXPLAIN EXTENDED | Text output from `EXPLAIN EXTENDED <query>` | Recommended |
| Markdown Report (Web UI only) | Previously generated report for viewing | No |

> **Note:** Both DBSQL and Spark Connect profile formats are supported.

## Known Limitations

- **Rescheduled Scan Ratio**: Only displayed when scan task locality data exists in the profile
- **Cache Hit Ratio**: May show low values on first query execution (cold cache)
- **Photon Blockers**: Full detection requires EXPLAIN EXTENDED; basic heuristics used otherwise
- **SQL Parsing**: Very large or complex SQL may exceed token limits for formatting

## Troubleshooting

### LLM Analysis is Skipped

```
Warning: DATABRICKS_HOST and DATABRICKS_TOKEN not set, skipping LLM analysis
```

**Solution:** Set the environment variables (see Step 2)

### dbsql_tuning.md not found

```
Warning: dbsql_tuning.md not found, analysis will proceed without tuning guidelines
```

**Solution:** Specify the file or directory with the `--tuning-file` option. Usually located at `databricks-apps/core/knowledge/dbsql_tuning.md`

### JSON Parse Error

**Solution:** Verify that the downloaded JSON file is in the correct format. Use a file downloaded directly from Databricks Query Profile.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting changes.

**Team rules (strict):**
- **No direct push to `main`** — always go through a PR
- **No self-merge** — at least one human review required (Codex review alone is not sufficient)
- **No merge when CI is red** — all 5 jobs (Lint / Build / Type Check / Validate / Test) must be green

Bug reports and feature requests: [Issues](https://github.com/akuwano/databricks-perf-toolkit/issues).
