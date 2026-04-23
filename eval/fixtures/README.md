# Evaluation Fixtures

Place profile JSON files here for evaluation.

## Quick Start

Copy from existing profiles:
```bash
cp ../../json/query-profile_*.json .
```

## Collecting TPC-DS Profiles

1. Run TPC-DS queries against a SQL Warehouse
2. Capture profile JSONs via the DBSQL Profile API
3. Save as `tpcds_qNN.json`

## Running Evaluation

```bash
cd /path/to/dbsql_profiler_analysis_tool

# Full LLM evaluation
PYTHONPATH=dabs/app:. python -m eval eval/fixtures/ --model databricks-claude-sonnet-4

# With judge scoring
PYTHONPATH=dabs/app:. python -m eval eval/fixtures/ --judge-model databricks-claude-opus-4

# JSON output
PYTHONPATH=dabs/app:. python -m eval eval/fixtures/ --json > eval_results.json
```
