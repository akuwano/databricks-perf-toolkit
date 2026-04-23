"""Blueprint for settings routes: /api/v1/settings, /api/v1/spark-perf/settings, /api/v1/lang."""

import logging
from typing import Any

from flask import Blueprint, jsonify, make_response, request

logger = logging.getLogger(__name__)

bp = Blueprint("settings", __name__)


@bp.route("/api/v1/lang", methods=["POST"])
def set_language():
    """Set the language preference via cookie."""
    from flask import current_app

    lang = request.json.get("lang", "en")
    if lang not in current_app.config["BABEL_SUPPORTED_LOCALES"]:
        lang = "en"

    response = make_response(jsonify({"lang": lang}))
    response.set_cookie(
        "lang", lang, max_age=365 * 24 * 60 * 60, samesite="Lax", httponly=True, secure=True
    )
    return response


@bp.route("/api/v1/settings", methods=["GET"])
def get_settings():
    """Get current profiler settings."""
    from core.config_store import get_setting

    return jsonify(
        {
            "catalog": get_setting("catalog", "main"),
            "schema": get_setting("schema", "profiler"),
            "http_path": get_setting("http_path", ""),
            "table_write_enabled": get_setting("table_write_enabled", "false").lower()
            in ("true", "1", "yes"),
        }
    )


@bp.route("/api/v1/settings", methods=["POST"])
def save_settings():
    """Save profiler settings to local config file with schema validation."""
    from app import UserInputError
    from core.config_store import save_config

    data = request.get_json()
    if not data:
        raise UserInputError("JSON body required")

    # Validate catalog.schema exists before saving
    catalog = data.get("catalog", "").strip()
    schema = data.get("schema", "").strip()
    http_path = data.get("http_path", "").strip()
    if catalog and schema and http_path:
        _validate_schema_exists(catalog, schema, http_path=http_path, check_write=True)

    config = {}
    for key in ("catalog", "schema", "http_path", "table_write_enabled"):
        if key in data:
            config[key] = str(data[key])

    save_config(config)

    # Clear DBSQL Genie Space if data source config changed
    if any(k in data for k in ("catalog", "schema")):
        save_config({"dbsql_genie_space_id": ""})

    logger.info("Settings saved: %s", config)

    # Initialize tables after successful save
    tables_created = _ensure_all_tables()

    result = {"status": "saved", "settings": config}
    if tables_created is not None:
        result["tables_initialized"] = tables_created
    return jsonify(result)


@bp.route("/api/v1/settings/reset", methods=["POST"])
def reset_settings():
    """Reset DBSQL profiler settings to deploy-time defaults."""
    from core.config_store import reset_keys

    reset_keys(["catalog", "schema", "http_path", "table_write_enabled", "dbsql_genie_space_id"])
    logger.info("DBSQL settings reset to defaults")

    from core.config_store import get_setting

    return jsonify(
        {
            "status": "reset",
            "settings": {
                "catalog": get_setting("catalog", "main"),
                "schema": get_setting("schema", "profiler"),
                "http_path": get_setting("http_path", ""),
                "table_write_enabled": get_setting("table_write_enabled", "false").lower()
                in ("true", "1", "yes"),
            },
        }
    )


@bp.route("/api/v1/spark-perf/settings/reset", methods=["POST"])
def reset_spark_perf_settings():
    """Reset Spark Perf settings to deploy-time defaults."""
    from core.config_store import reset_keys

    reset_keys(
        [
            "spark_perf_catalog",
            "spark_perf_schema",
            "spark_perf_table_prefix",
            "spark_perf_http_path",
            "spark_perf_etl_job_id",
            "spark_perf_summary_job_id",
            "genie_space_id",
        ]
    )
    logger.info("Spark Perf settings reset to defaults")

    from core.config_store import get_setting

    return jsonify(
        {
            "status": "reset",
            "settings": {
                "catalog": get_setting("spark_perf_catalog", "main"),
                "schema": get_setting("spark_perf_schema", "default"),
                "table_prefix": get_setting("spark_perf_table_prefix", "PERF_"),
                "http_path": get_setting("spark_perf_http_path", ""),
                "etl_job_id": get_setting("spark_perf_etl_job_id", ""),
                "summary_job_id": get_setting("spark_perf_summary_job_id", ""),
            },
        }
    )


def _ensure_all_tables() -> list[str] | None:
    """Create all profiler tables if they don't exist. Returns list of table names or None on skip."""
    try:
        from services.table_writer import TableWriter, TableWriterConfig

        tw_config = TableWriterConfig.from_env()
        if not tw_config.http_path:
            return None

        writer = TableWriter(tw_config)
        conn = writer._get_connection()
        created = []
        with conn:
            with conn.cursor() as cursor:
                from services.table_writer import _TABLE_DDLS

                for table_name in _TABLE_DDLS:
                    writer._ensure_table(cursor, table_name)
                    created.append(table_name)
        logger.info(
            "Tables initialized: %d tables in %s.%s",
            len(created),
            tw_config.catalog,
            tw_config.schema,
        )
        return created
    except Exception as e:
        logger.warning("Table initialization skipped: %s", e)
        return None


@bp.route("/api/v1/debug/config")
def debug_config():
    """Return effective settings with source info for debugging."""
    from core.config_store import get_config_paths, get_setting_with_source

    keys_defaults = {
        "catalog": "main",
        "schema": "profiler",
        "http_path": "",
        "table_write_enabled": "false",
        "spark_perf_catalog": "main",
        "spark_perf_schema": "default",
        "spark_perf_table_prefix": "PERF_",
        "spark_perf_http_path": "",
        "spark_perf_etl_job_id": "0",
        "spark_perf_summary_job_id": "0",
        "genie_space_id": "",
        "dbsql_genie_space_id": "",
    }

    settings = {k: get_setting_with_source(k, d) for k, d in keys_defaults.items()}
    return jsonify({"settings": settings, "config_paths": get_config_paths()})


@bp.route("/api/v1/spark-perf/settings", methods=["GET"])
def get_spark_perf_settings():
    """Get Spark Perf settings."""
    from core.config_store import get_setting

    return jsonify(
        {
            "catalog": get_setting("spark_perf_catalog", "main"),
            "schema": get_setting("spark_perf_schema", "default"),
            "table_prefix": get_setting("spark_perf_table_prefix", "PERF_"),
            "http_path": get_setting("spark_perf_http_path", ""),
            "etl_job_id": get_setting("spark_perf_etl_job_id", ""),
            "summary_job_id": get_setting("spark_perf_summary_job_id", ""),
            "genie_space_id": get_setting("genie_space_id", ""),
        }
    )


@bp.route("/api/v1/spark-perf/settings", methods=["POST"])
def save_spark_perf_settings():
    """Save Spark Perf settings with schema validation."""
    from app import UserInputError
    from core.config_store import save_config

    data = request.get_json()
    if not data:
        raise UserInputError("JSON body required")

    # Validate catalog.schema exists before saving
    catalog = data.get("catalog", "").strip()
    schema = data.get("schema", "").strip()
    sp_http_path = data.get("http_path", "").strip()
    if catalog and schema:
        _validate_schema_exists(catalog, schema, http_path=sp_http_path)

    config = {}
    key_map = {
        "catalog": "spark_perf_catalog",
        "schema": "spark_perf_schema",
        "table_prefix": "spark_perf_table_prefix",
        "http_path": "spark_perf_http_path",
        "etl_job_id": "spark_perf_etl_job_id",
        "summary_job_id": "spark_perf_summary_job_id",
        "genie_space_id": "genie_space_id",
    }
    for form_key, config_key in key_map.items():
        if form_key in data:
            config[config_key] = str(data[form_key])

    save_config(config)

    # Clear Genie Space if data source config changed
    if any(k in data for k in ("catalog", "schema", "table_prefix")):
        save_config({"genie_space_id": ""})

    return jsonify({"status": "saved", "settings": config})


def _validate_schema_exists(
    catalog: str, schema: str, http_path: str = "", check_write: bool = False
) -> None:
    """Check that catalog.schema exists via SQL. Raises 400 if not.

    Args:
        catalog: Catalog name.
        schema: Schema name.
        http_path: SQL warehouse HTTP path. If empty, tries to resolve from config.
        check_write: If True, also verify CREATE TABLE privilege (for DBSQL tables).
    """
    from core.config_store import get_setting
    from core.sql_safe import validate_identifier

    validate_identifier(catalog, "catalog")
    validate_identifier(schema, "schema")

    # Resolve HTTP path: explicit > DBSQL setting > Spark Perf setting
    if not http_path:
        http_path = get_setting("http_path", "") or get_setting("spark_perf_http_path", "")
    if not http_path:
        return  # Can't validate without connection

    from werkzeug.exceptions import HTTPException

    try:
        from services import get_sp_sql_connection

        conn = get_sp_sql_connection(http_path)

        with conn:
            with conn.cursor() as cursor:
                # 1. Check schema exists using SHOW SCHEMAS (works regardless of current catalog)
                cursor.execute(f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'")
                if not cursor.fetchone():
                    from flask import abort

                    abort(400, description=f"Schema '{catalog}.{schema}' does not exist.")

                # 2. Check write privilege (DBSQL only — Spark Perf writes via jobs)
                if check_write:
                    _check_create_table_privilege(cursor, catalog, schema)
    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Schema validation skipped: %s", e)


def _check_create_table_privilege(cursor: Any, catalog: str, schema: str) -> None:
    """Verify CREATE TABLE privilege by creating and dropping a test table.

    If the CREATE TABLE fails, abort with 400 and show GRANT commands.
    """
    from flask import abort

    test_table = f"`{catalog}`.`{schema}`.__profiler_permission_test__"
    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {test_table} (_test INT) USING DELTA")
        cursor.execute(f"DROP TABLE IF EXISTS {test_table}")
    except Exception as e:
        error_msg = str(e)
        logger.warning(
            "CREATE TABLE privilege check failed for %s.%s: %s", catalog, schema, error_msg
        )
        grant_commands = (
            f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `<principal>`;\n"
            f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{schema}` TO `<principal>`;\n"
            f"GRANT CREATE TABLE ON SCHEMA `{catalog}`.`{schema}` TO `<principal>`;\n"
            f"GRANT SELECT ON SCHEMA `{catalog}`.`{schema}` TO `<principal>`;"
        )
        abort(
            400,
            description=(
                f"Cannot create tables in '{catalog}.{schema}'. "
                f"Run the following SQL as an admin (replace <principal> with your SP or user):\n\n"
                f"{grant_commands}"
            ),
        )
