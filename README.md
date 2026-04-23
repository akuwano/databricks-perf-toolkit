# DBSQL Profiler Analysis Tool

A practical toolkit for analyzing Databricks SQL query profiles and Spark job performance, generating actionable optimization guidance, and comparing tuning experiments through CLI and Web UI.

[日本語版 / Japanese README](README.ja.md)

## What It Does

- Analyzes Databricks SQL query profile JSON and optional `EXPLAIN` output to detect bottlenecks, classify root causes, and generate concrete action plans.
- Processes Spark event-log ETL outputs into curated Delta tables, LLM-written narratives, and dashboard-ready datasets for performance investigation.
- Compares DBSQL queries and Spark workloads across experiments, variants, and query families to measure regressions and improvements.
- Adds a multilingual knowledge layer, Web UI, and SQL accuracy evaluation workflow to support iterative optimization work.

## Features

### DBSQL Query Profile Analysis

The DBSQL analysis pipeline is the core of this repository. It combines deterministic signal extraction, a rule-based ActionCard registry, and a three-stage LLM workflow.

**Highlights**

- **22 ActionCards in a single registry**
  - Rule-based recommendations are generated from a canonical 22-card registry.
  - Includes recent cards such as `federation_query`, `cluster_underutilization`, and `compilation_absolute_heavy`.
  - Cards are ranked and emitted by priority rather than by legacy version-specific logic.
- **3-stage LLM pipeline**
  - Stage 1: initial structured analysis
  - Stage 2: review
  - Stage 3: refine
  - The LLM layer complements the registry and is deduplicated by root-cause grouping.
- **Optional `EXPLAIN` integration**
  - Accepts `EXPLAIN EXTENDED` text to enrich physical-plan interpretation.
  - Adds plan-structure evidence beyond the profile JSON alone.
- **Implicit `CAST` on join-key detection**
  - Detects join-key `CAST(...)` patterns from profile metadata and/or `EXPLAIN`.
  - Surfaces hidden compatibility and performance issues in joins.
- **Lakehouse Federation awareness**
  - Detects federation scans from profile operators.
  - Applies federation-specific suppression so the tool avoids irrelevant recommendations for federated workloads.
- **Streaming awareness**
  - Detects streaming context (DLT/SDP streaming tables) and routes analysis/reporting accordingly.
- **Serverless awareness**
  - Detects serverless query execution and filters recommendations where appropriate.
- **Cost estimation**
  - Computes DBU-based cost estimates using pricing helpers and query metrics.

**What the DBSQL analyzer covers**

- Query-level metrics and bottleneck indicators
- Physical operator hot spots
- Shuffle-heavy and skew-related issues
- Spill and memory pressure
- Photon blockers and low Photon usage
- Scan efficiency and file pruning quality
- Compilation overhead, including absolute-heavy compilation cases
- Driver / queue / scheduling overhead
- Federation-specific handling
- Clustering-related recommendations
- Statistics freshness and SQL-pattern issues

**ActionCard registry (22 cards)**

`disk_spill`, `federation_query`, `shuffle_dominant`, `shuffle_lc`, `data_skew`, `low_file_pruning`, `low_cache`, `compilation_overhead`, `photon_blocker`, `photon_low`, `scan_hot`, `non_photon_join`, `hier_clustering`, `hash_resize`, `aqe_absorbed`, `cte_multi_ref`, `investigate_dist`, `stats_fresh`, `driver_overhead`, `rescheduled_scan`, `cluster_underutilization`, `compilation_absolute_heavy`

**Root-cause grouping**

The analyzer uses a 16-group taxonomy to deduplicate and organize recommendations, including categories such as spill/memory pressure, shuffle overhead, data skew, scan efficiency, cache utilization, Photon compatibility, SQL pattern, statistics freshness, driver overhead, federation, cluster underutilization, and compilation overhead. This keeps the final action plan focused even when both rule-based and LLM-generated suggestions are present.

### Spark Job Performance Analysis

A Spark performance pipeline built around Databricks notebooks, curated Delta tables, and LLM-generated summaries.

**Highlights**

- **ETL notebooks**
  - Ingests Spark event-log data and writes curated Bronze/Silver/Gold Delta tables.
- **LLM narrative summaries**
  - Generates readable performance narratives on top of raw metrics.
- **Lakeview dashboard support**
  - Includes a dashboard-building notebook on top of Gold tables.
- **Cost columns**
  - Gold outputs include DBU and cost-oriented fields to support efficiency analysis.

**Spark analysis scope**

- application/job/stage/executor summaries
- concurrency and workload views
- spot / SQL / Photon / streaming-related views
- narrative generation and update flows
- comparison history and comparison reports
- ETL-run and summary-run orchestration endpoints in the Web UI

### Comparison & Experimentation

Comparison workflows for both DBSQL and Spark.

**Capabilities**

- **DBSQL and Spark comparisons**
  - Compare analysis runs side by side with direction-aware metric changes.
- **Experiment and variant tracking**
  - Tag analyses with experiment IDs and variants such as `baseline`, `candidate_a`, or `optimized`.
- **Family grouping**
  - Groups related queries/workloads into families to support meaningful before/after comparison.
- **Persisted comparison history**
  - Stores comparison results for later inspection in the UI and downstream tables.

### Knowledge Base

A multilingual knowledge layer injected into prompts and routed by topic.

- **7 topics in English and Japanese**
  - DBSQL tuning, DBSQL advanced, DBSQL SQL patterns, DBSQL serverless, DBSQL Photon OOM, Spark tuning, Spark streaming tuning
- **`section_id` routing**
  - Only relevant sections are injected into prompts rather than loading entire documents.
- **Knowledge-assisted analysis**
  - Grounds recommendations in curated guidance rather than free-form generation alone.

### Web UI

A Flask-based Web UI is included for interactive use.

- Analysis upload and result browsing (`/analyze`, `/history`, `/report`)
- Side-by-side comparison (`/compare`, `/workload`)
- Spark performance pages (`/spark-perf`) with ETL trigger, report view, comparisons, and run status
- Schema analysis (`/schema-analysis`)
- Settings management for catalog/schema/warehouse
- Genie Chat panel and Rewrite/Refine flows for SQL optimization

### SQL Accuracy Evaluation

An evaluation workflow under `eval/` for SQL quality and report quality analysis.

- **4-axis scoring**: L1 syntax, L2 evidence grounding, L3 diagnosis accuracy (LLM-as-judge), L4 fix effectiveness
- **Diff runner**: compare outputs across git refs using worktrees
- **Scorers and fixtures**: iterative validation of prompt/model changes

## Requirements

- Python `3.11` or later
- Databricks workspace access for LLM-backed workflows
  - `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
- Optional: Databricks SQL warehouse / catalog / schema for persistence, dashboard, or Databricks Apps deployment

Core dependencies: `openai`, `sqlparse`, `sqlglot`, `requests`, `pyyaml`.
Web UI adds: `flask`, `flask-babel`, `markdown`.
Dev/test groups: `pytest`, `pytest-cov`, `mypy`, `playwright`, `babel`.

## Quick Start

This repository is designed to be deployed as a Databricks App and also used locally through the CLI.

### 1. Clone and install

```bash
git clone https://github.com/akuwano/databricks-perf-toolkit.git
cd databricks-perf-toolkit

python -m venv .venv
source .venv/bin/activate

pip install -e ".[web]"
```

For dev/test/lint extras:

```bash
pip install -e ".[web,test,lint,ui-smoke,dev]"
```

### 2. Set environment variables

For LLM-backed analysis:

```bash
export DATABRICKS_HOST="https://<your-workspace>"
export DATABRICKS_TOKEN="<your-token>"
```

Optional output language (default `en`):

```bash
export DBSQL_LANG="ja"
```

### 3. Databricks Apps deployment inputs

For Apps deployment, configure at minimum:

- a Databricks workspace where the app runs
- auth credentials (`DATABRICKS_HOST` / `DATABRICKS_TOKEN`)
- catalog / schema / warehouse settings for persistence and Web UI
- notebook / job wiring for Spark ETL and dashboard flows

Deployment assets live under `dabs/`, `scripts/`, and `docs/`.

### 4. Launch the Web UI locally

```bash
python dabs/app/app.py
# then open http://localhost:8000
```

### 5. Run a first analysis via CLI

```bash
profiler-analyzer path/to/profile.json --no-llm
```

Or with `EXPLAIN` and LLM stages:

```bash
profiler-analyzer path/to/profile.json \
  --explain path/to/explain.txt \
  --model databricks-claude-opus-4-6 \
  --review-model databricks-claude-opus-4-6
```

## CLI Usage

The CLI analyzes a query profile JSON and optionally persists or compares the result.

```bash
# Basic analysis
profiler-analyzer profile.json

# Metrics-only (skip LLM)
profiler-analyzer profile.json --no-llm

# With EXPLAIN
profiler-analyzer profile.json --explain explain.txt

# Japanese output
profiler-analyzer profile.json --lang ja

# Customize LLM stages
profiler-analyzer profile.json \
  --model databricks-claude-opus-4-6 \
  --review-model databricks-claude-opus-4-6 \
  --refine-model databricks-claude-opus-4-6 \
  --verbose

# Tag with experiment / variant
profiler-analyzer profile.json \
  --experiment-id exp_2026_04 --variant baseline

# Persist results to Delta tables
profiler-analyzer profile.json --persist

# Compare against a prior analysis
profiler-analyzer profile.json --compare-with <analysis-id>

# Add structured tags
profiler-analyzer profile.json --tags '{"env":"prod","team":"analytics"}'

# Review & refine the generated report
profiler-analyzer profile.json --report-review --refine-report
```

## Architecture

### Main directories

```
dabs/app/              # Flask Web UI + CLI entry
├── core/              # data models, extractors, analyzers, reporters,
│                      # LLM clients/prompts, comparison/family logic,
│                      # DBU pricing/cost, multilingual knowledge
├── services/          # table readers/writers, Spark perf readers/writers,
│                      # schema-join detection, job launcher, Genie client
├── routes/            # Flask blueprints
├── templates/         # HTML templates
├── translations/      # JA/EN po/mo
├── cli/               # profiler-analyzer entry point
└── tests/             # application tests
dabs/notebooks/        # Spark ETL, summary generation, dashboard, KB mgmt
docs/                  # design docs (analysis pipeline, action plan, API, ops)
eval/                  # SQL accuracy evaluation framework (L1–L4 scorers)
scripts/               # deploy, smoke test, view deployment, runtime config
```

### DBSQL analysis flow

1. Load query profile JSON
2. Extract query metrics, node metrics, and bottleneck indicators
3. Generate rule-based ActionCards from the 22-card registry
4. Apply environment-aware filters (serverless, federation)
5. Optionally parse `EXPLAIN`
6. Run LLM stages: initial → review → refine
7. Deduplicate LLM suggestions against rule-based cards by root-cause group
8. Render Markdown report
9. Optionally persist analysis and comparisons to Delta tables

### Delta tables

- **DBSQL persistence layer**: 11 tables for analysis, comparison, and knowledge (headers, actions, table scans, hot operators, stages, raw, comparison pairs/metrics, knowledge docs/tags, metric directions).
- **Spark performance pipeline**: Bronze / Silver / Gold Delta tables produced by `dabs/notebooks/01_Spark Perf Pipeline PySpark.py`, with Gold-layer outputs consumed by the Web UI and Lakeview dashboards.

See `docs/analysis-pipeline.md` and `docs/action-plan-generation.md` for details.

## Development

```bash
# Install dev extras
pip install -e ".[web,test,lint,ui-smoke,dev]"

# Run tests
pytest                       # full suite
pytest dabs/app/tests        # app only
pytest eval/tests            # eval only

# Type-check
mypy dabs/app

# Lint / format (ruff)
ruff check .
ruff format .

# UI smoke test
python scripts/ui_smoke_test.py
```

## License

Apache License 2.0. See `LICENSE`.
