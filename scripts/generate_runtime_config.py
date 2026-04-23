#!/usr/bin/env python3
"""Generate runtime-config.json from local-overrides.yml for Databricks App.

Usage:
    python scripts/generate_runtime_config.py [--target dev]

Reads DABs variables from local-overrides.yml and writes
dabs/app/runtime-config.json with the resolved settings.
"""

import argparse
import json
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("pyyaml is required: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

DABS_DIR = Path(__file__).parent.parent / "dabs"
LOCAL_OVERRIDES = DABS_DIR / "local-overrides.yml"
OUTPUT_PATH = DABS_DIR / "app" / "runtime-config.json"
APP_YAML_PATH = DABS_DIR / "app" / "app.yaml"


def main():
    parser = argparse.ArgumentParser(description="Generate runtime-config.json")
    parser.add_argument("--target", default="dev", help="DABs target (default: dev)")
    args = parser.parse_args()

    if not LOCAL_OVERRIDES.exists():
        print(f"Error: {LOCAL_OVERRIDES} not found.", file=sys.stderr)
        print("Copy local-overrides.yml.sample to local-overrides.yml and edit it.", file=sys.stderr)
        sys.exit(1)

    with open(LOCAL_OVERRIDES, encoding="utf-8") as f:
        overrides = yaml.safe_load(f)

    variables = (
        overrides.get("targets", {}).get(args.target, {}).get("variables", {})
    )

    warehouse_id = variables.get("warehouse_id", "")
    http_path = f"/sql/1.0/warehouses/{warehouse_id}" if warehouse_id else ""

    config = {
        "catalog": variables.get("dbsql_catalog", ""),
        "schema": variables.get("dbsql_schema", ""),
        "http_path": http_path,
        "table_write_enabled": "true",
        "spark_perf_catalog": variables.get("sparkperf_catalog", ""),
        "spark_perf_schema": variables.get("sparkperf_schema", ""),
        "spark_perf_table_prefix": variables.get("table_prefix", "PERF_"),
        "spark_perf_http_path": http_path,
        "spark_perf_etl_job_id": variables.get("spark_perf_etl_job_id", ""),
        "spark_perf_summary_job_id": variables.get("spark_perf_summary_job_id", ""),
    }

    # Remove empty values
    config = {k: v for k, v in config.items() if v}

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)

    print(f"Generated {OUTPUT_PATH}")
    for k, v in config.items():
        print(f"  {k}: {v}")

    # Generate app.yaml with warehouse resource for SP auto-permission
    app_yaml = {
        "command": ["flask", "--app", "app.py", "run", "--host", "0.0.0.0", "--port", "8000"],
    }
    if warehouse_id:
        app_yaml["resources"] = [
            {
                "name": "sql-warehouse",
                "sql_warehouse": {
                    "id": warehouse_id,
                    "permission": "CAN_USE",
                },
            }
        ]

    with open(APP_YAML_PATH, "w", encoding="utf-8") as f:
        yaml.dump(app_yaml, f, default_flow_style=False, sort_keys=False)

    print(f"Generated {APP_YAML_PATH}")
    if warehouse_id:
        print(f"  sql-warehouse: {warehouse_id} (CAN_USE)")


if __name__ == "__main__":
    main()
