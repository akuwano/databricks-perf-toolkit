#!/bin/bash
# Deploy DBSQL Profiler Analyzer to Databricks Apps
#
# Usage:
#   ./scripts/deploy.sh [target]
#   ./scripts/deploy.sh dev    # default
#   ./scripts/deploy.sh prod
#   ./scripts/deploy.sh dev --full-test  # includes full analysis smoke test
set -euo pipefail

TARGET="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Helper: run Python with project deps only (no test/lint/ui-smoke groups)
_run_py() {
    (cd "${ROOT_DIR}" && uv run --offline --no-group test --no-group lint --no-group ui-smoke python3 "$@")
}

echo "=== Generating runtime-config.json for target: ${TARGET} ==="
cd "${ROOT_DIR}/dabs/app"
_run_py "${SCRIPT_DIR}/generate_runtime_config.py" --target "${TARGET}"

echo ""
echo "=== Injecting version into app.py ==="
APP_VERSION=$(python3 -c "
import re
with open('${ROOT_DIR}/pyproject.toml') as f:
    m = re.search(r'^version\s*=\s*\"(.+?)\"', f.read(), re.MULTILINE)
    print(m.group(1) if m else 'unknown')
")
sed -i.bak "s/APP_VERSION = .*/APP_VERSION = \"${APP_VERSION}\"/" "${ROOT_DIR}/dabs/app/app.py" && rm -f "${ROOT_DIR}/dabs/app/app.py.bak"
echo "  Version: ${APP_VERSION}"

echo ""
echo "=== Deploying bundle (target: ${TARGET}) ==="
cd "${ROOT_DIR}/dabs"
databricks bundle deploy -t "${TARGET}"

echo ""
echo "=== Resolving Job IDs from bundle summary ==="
JOB_IDS=$(cd "${ROOT_DIR}/dabs" && databricks bundle summary -t "${TARGET}" --output json 2>/dev/null \
    | python3 -c "
import json, sys
data = json.load(sys.stdin)
jobs = data.get('resources', {}).get('jobs', {})
etl_id = jobs.get('spark_perf_etl', {}).get('id', '')
summary_id = jobs.get('spark_perf_summary', {}).get('id', '')
print(f'{etl_id} {summary_id}')
" 2>/dev/null || echo "")

read -r ETL_JOB_ID SUMMARY_JOB_ID <<< "${JOB_IDS}"

if [ -n "${ETL_JOB_ID}" ] || [ -n "${SUMMARY_JOB_ID}" ]; then
    echo "  ETL Job ID: ${ETL_JOB_ID:-not found}"
    echo "  Create Report Job ID: ${SUMMARY_JOB_ID:-not found}"
    # Inject job IDs into runtime-config.json
    python3 -c "
import json
config_path = '${ROOT_DIR}/dabs/app/runtime-config.json'
with open(config_path) as f:
    config = json.load(f)
etl_id = '${ETL_JOB_ID}'
summary_id = '${SUMMARY_JOB_ID}'
if etl_id:
    config['spark_perf_etl_job_id'] = etl_id
if summary_id:
    config['spark_perf_summary_job_id'] = summary_id
with open(config_path, 'w') as f:
    json.dump(config, f, indent=2, ensure_ascii=False)
print('  ✓ Job IDs injected into runtime-config.json')
"
    # Re-sync the updated config to workspace
    cd "${ROOT_DIR}/dabs"
    databricks bundle deploy -t "${TARGET}"
else
    echo "  ⚠ Could not resolve job IDs from bundle summary"
fi

echo ""
echo "=== Starting app ==="
databricks bundle run profiler_app -t "${TARGET}"

echo ""
echo "=== Granting SQL Warehouse permissions to App SP ==="
# Read app_name and warehouse_id from local-overrides.yml via Python
read -r APP_NAME WAREHOUSE_ID < <(_run_py -c "
import yaml, sys
with open('${ROOT_DIR}/dabs/local-overrides.yml') as f:
    o = yaml.safe_load(f)
v = o.get('targets',{}).get('${TARGET}',{}).get('variables',{})
print(v.get('app_name',''), v.get('warehouse_id',''))
")

if [ -n "${APP_NAME}" ] && [ -n "${WAREHOUSE_ID}" ]; then
    SP_CLIENT_ID=$(databricks apps get "${APP_NAME}" --output json 2>/dev/null \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('service_principal_client_id',''))" 2>/dev/null || echo "")

    if [ -n "${SP_CLIENT_ID}" ]; then
        echo "  App: ${APP_NAME}"
        echo "  SP:  ${SP_CLIENT_ID}"

        # --- deploy.sh explicitly handles: WH CAN_USE, catalog/schema, SP write ---

        # Grant SP CAN_USE on SQL Warehouse (app.yaml resources is unreliable)
        echo "  Granting WH CAN_USE to SP ..."
        databricks api patch "/api/2.0/permissions/sql/warehouses/${WAREHOUSE_ID}" --json "{
            \"access_control_list\": [{
                \"service_principal_name\": \"${SP_CLIENT_ID}\",
                \"permission_level\": \"CAN_USE\"
            }]
        }" > /dev/null 2>&1 && echo "  ✓ WH CAN_USE granted" || echo "  ⚠ WH CAN_USE grant failed"

        # Read catalog/schema config
        read -r SP_CATALOG SP_SCHEMA DBSQL_CATALOG DBSQL_SCHEMA < <(_run_py -c "
import yaml, sys
with open('${ROOT_DIR}/dabs/local-overrides.yml') as f:
    o = yaml.safe_load(f)
v = o.get('targets',{}).get('${TARGET}',{}).get('variables',{})
print(v.get('sparkperf_catalog',''), v.get('sparkperf_schema',''), v.get('dbsql_catalog',''), v.get('dbsql_schema',''))
")

        # Run a SQL statement via the statements API and return 0 only if the
        # SQL actually SUCCEEDED (not just "HTTP 200"). The older pattern of
        # `databricks api post ... > /dev/null && echo ready` masked real
        # failures (e.g. CREATE CATALOG denied by metastore) as success.
        _run_sql() {
            local stmt="$1" timeout="${2:-30s}"
            local response rc
            response=$(databricks api post /api/2.0/sql/statements --json "{
                \"warehouse_id\": \"${WAREHOUSE_ID}\",
                \"statement\": \"${stmt}\",
                \"wait_timeout\": \"${timeout}\"
            }" 2>&1)
            rc=$?
            if [ "${rc}" -ne 0 ]; then
                echo "    [http_error] ${response}" >&2
                return 1
            fi
            python3 - "${response}" <<'PY' || return 1
import json, sys
resp = json.loads(sys.argv[1])
state = resp.get("status", {}).get("state", "")
if state == "SUCCEEDED":
    sys.exit(0)
err = resp.get("status", {}).get("error", {}) or {}
code = err.get("error_code", "")
msg = err.get("message", "")
sys.stderr.write(f"    [state={state or '?'}] {code}: {msg}\n".strip() + "\n")
sys.exit(1)
PY
        }

        # Create catalogs and schemas (requires deployer admin privileges)
        _ensure_catalog() {
            local catalog="$1"
            [ -z "${catalog}" ] && return 0
            echo "  Ensuring catalog ${catalog} exists ..."
            if _run_sql "CREATE CATALOG IF NOT EXISTS \`${catalog}\`"; then
                echo "  ✓ Catalog ${catalog} ready"
                return 0
            fi
            echo "  ✗ Catalog ${catalog} creation FAILED (see error above)"
            return 1
        }
        _ensure_schema() {
            local catalog="$1" schema="$2"
            [ -z "${catalog}" ] || [ -z "${schema}" ] && return 0
            _ensure_catalog "${catalog}" || return 1
            echo "  Ensuring ${catalog}.${schema} exists ..."
            if _run_sql "CREATE SCHEMA IF NOT EXISTS \`${catalog}\`.\`${schema}\`"; then
                echo "  ✓ ${catalog}.${schema} ready"
                return 0
            fi
            echo "  ✗ ${catalog}.${schema} creation FAILED (see error above)"
            return 1
        }
        _ensure_schema "${SP_CATALOG}" "${SP_SCHEMA}" || true
        if [ "${DBSQL_CATALOG}.${DBSQL_SCHEMA}" != "${SP_CATALOG}.${SP_SCHEMA}" ]; then
            _ensure_schema "${DBSQL_CATALOG}" "${DBSQL_SCHEMA}" || true
        fi

        # Grant SP write permissions (CREATE TABLE + MODIFY for table_writer)
        # Also SELECT (all SQL connections use SP auth)
        _grant_sp_write() {
            local catalog="$1" schema="$2" include_select="$3"
            [ -z "${catalog}" ] || [ -z "${schema}" ] && return 0
            echo "  Granting SP write on ${catalog}.${schema} ..."
            local stmts=(
                "GRANT USE CATALOG ON CATALOG \`${catalog}\` TO \`${SP_CLIENT_ID}\`"
                "GRANT USE SCHEMA ON SCHEMA \`${catalog}\`.\`${schema}\` TO \`${SP_CLIENT_ID}\`"
                "GRANT CREATE TABLE ON SCHEMA \`${catalog}\`.\`${schema}\` TO \`${SP_CLIENT_ID}\`"
                "GRANT MODIFY ON SCHEMA \`${catalog}\`.\`${schema}\` TO \`${SP_CLIENT_ID}\`"
            )
            if [ "${include_select}" = "true" ]; then
                stmts+=("GRANT SELECT ON SCHEMA \`${catalog}\`.\`${schema}\` TO \`${SP_CLIENT_ID}\`")
            fi
            local failed=0
            for stmt in "${stmts[@]}"; do
                if ! _run_sql "${stmt}" "10s"; then
                    failed=$((failed + 1))
                fi
            done
            if [ "${failed}" -eq 0 ]; then
                echo "  ✓ ${catalog}.${schema} SP write granted"
            else
                echo "  ✗ ${catalog}.${schema} SP write: ${failed} of ${#stmts[@]} grants FAILED"
                return 1
            fi
        }
        # Spark Perf: write + SELECT (SELECT needed for Genie Space creation)
        _grant_sp_write "${SP_CATALOG}" "${SP_SCHEMA}" "true" || true
        # DBSQL: write + SELECT (all SQL connections use SP auth)
        if [ "${DBSQL_CATALOG}.${DBSQL_SCHEMA}" != "${SP_CATALOG}.${SP_SCHEMA}" ]; then
            _grant_sp_write "${DBSQL_CATALOG}" "${DBSQL_SCHEMA}" "true" || true
        fi

        # Grant CAN_MANAGE_RUN on jobs to App SP
        if [ -n "${ETL_JOB_ID}" ]; then
            databricks api patch "/api/2.0/permissions/jobs/${ETL_JOB_ID}" --json "{
                \"access_control_list\": [{
                    \"service_principal_name\": \"${SP_CLIENT_ID}\",
                    \"permission_level\": \"CAN_MANAGE_RUN\"
                }]
            }" > /dev/null 2>&1 && echo "  ✓ ETL Job CAN_MANAGE_RUN granted" || echo "  ⚠ ETL Job permission grant failed"
        fi
        if [ -n "${SUMMARY_JOB_ID}" ]; then
            databricks api patch "/api/2.0/permissions/jobs/${SUMMARY_JOB_ID}" --json "{
                \"access_control_list\": [{
                    \"service_principal_name\": \"${SP_CLIENT_ID}\",
                    \"permission_level\": \"CAN_MANAGE_RUN\"
                }]
            }" > /dev/null 2>&1 && echo "  ✓ Create Report Job CAN_MANAGE_RUN granted" || echo "  ⚠ Create Report Job permission grant failed"
        fi
    else
        echo "  ⚠ Could not resolve SP for app '${APP_NAME}'"
    fi
else
    echo "  ⚠ Skipped (app_name or warehouse_id not set in local-overrides.yml)"
fi

echo ""
echo "=== Running smoke test ==="
APP_URL=$(databricks apps get "${APP_NAME}" --output json 2>/dev/null \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || echo "")

if [ -n "${APP_URL}" ]; then
    # Get token from Databricks CLI for smoke test auth
    SMOKE_TOKEN=$(databricks auth token -p DEFAULT 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null || echo "")
    SMOKE_ARGS="--skip-analysis"
    if [ "${2:-}" = "--full-test" ]; then
        SMOKE_ARGS=""
        echo "  (Full test mode: includes analysis flow)"
    fi
    _run_py "${SCRIPT_DIR}/smoke_test.py" "${APP_URL}" --token "${SMOKE_TOKEN}" ${SMOKE_ARGS} -v \
        && echo "  ✓ API smoke test passed" \
        || echo "  ⚠ API smoke test failed (see above)"

    # UI smoke test (Playwright — only if ui-smoke group is available)
    if (cd "${ROOT_DIR}" && uv run --offline --group ui-smoke python3 -c "import playwright") >/dev/null 2>&1; then
        echo ""
        (cd "${ROOT_DIR}" && uv run --offline --group ui-smoke python3 "${SCRIPT_DIR}/ui_smoke_test.py" "${APP_URL}" --token "${SMOKE_TOKEN}") \
            && echo "  ✓ UI smoke test passed" \
            || echo "  ⚠ UI smoke test failed (see above)"
    else
        echo "  ⏭ UI smoke test skipped (playwright not installed — run: uv sync --group ui-smoke && uv run --group ui-smoke python -m playwright install chromium)"
    fi
else
    echo "  ⚠ Skipped (could not resolve app URL)"
fi
