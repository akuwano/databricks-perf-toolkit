#!/usr/bin/env python3
"""
Flask application for Databricks SQL Query Profile Analyzer Web Interface.
"""

import logging
import os
import traceback
from collections import OrderedDict

from core import (
    LLMConfig,
    PipelineOptions,
    get_stage_messages,
)
from core import (
    set_language as set_core_language,
)
from core.serving_client import list_chat_models
from flask import Flask, Response, jsonify, render_template, request
from flask_babel import Babel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Reduce noisy library logs
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("sqlglot").setLevel(logging.ERROR)

# Overwritten by deploy.sh from pyproject.toml at deploy time
APP_VERSION = "5.17.0"

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB limit

# i18n Configuration
app.config["BABEL_DEFAULT_LOCALE"] = "en"
app.config["BABEL_SUPPORTED_LOCALES"] = ["en", "ja"]


def get_locale():
    """Determine the best locale for the request.

    Priority order:
    1. 'lang' query parameter
    2. 'lang' cookie
    3. Accept-Language header
    4. Default: 'en'
    """
    # Query parameter has highest priority
    lang = request.args.get("lang")
    if lang in app.config["BABEL_SUPPORTED_LOCALES"]:
        return lang

    # Cookie has second priority
    lang = request.cookies.get("lang")
    if lang in app.config["BABEL_SUPPORTED_LOCALES"]:
        return lang

    # Accept-Language header
    return request.accept_languages.best_match(app.config["BABEL_SUPPORTED_LOCALES"], default="en")


babel = Babel(app, locale_selector=get_locale)

# In-memory storage for analysis results (LRU, max 100 entries)
# Structure: {id: {status, stage, analysis, llm_result, report, filename, error}}

import threading

_MAX_STORE_SIZE = 100


class _LRUStore(OrderedDict):
    """Thread-safe OrderedDict with max size eviction.

    Processing entries (status != completed/failed) are protected from eviction.
    Uses RLock (reentrant) to avoid deadlock from nested calls.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = threading.RLock()

    def __setitem__(self, key, value):
        with self._lock:
            if super().__contains__(key):
                self.move_to_end(key)
            super().__setitem__(key, value)
            while len(self) > _MAX_STORE_SIZE:
                for k in list(self.keys()):
                    entry = super().__getitem__(k)
                    status = entry.get("status", "") if isinstance(entry, dict) else ""
                    if status in ("completed", "failed", ""):
                        super().__delitem__(k)
                        break
                else:
                    break

    def __getitem__(self, key):
        with self._lock:
            return super().__getitem__(key)

    def __contains__(self, key):
        with self._lock:
            return super().__contains__(key)


analysis_store: dict[str, dict] = _LRUStore()

# In-memory storage for uploaded reports (LRU, max 100 entries)
report_store: dict[str, dict] = _LRUStore()


# =============================================================================
# Custom Exceptions
# =============================================================================


class AppError(Exception):
    """Base exception for application errors."""

    def __init__(self, message: str, status_code: int = 500):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class UserInputError(AppError):
    """Error caused by invalid user input (4xx)."""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message, status_code)


class ServerError(AppError):
    """Internal server error (5xx)."""

    def __init__(self, message: str = "Internal server error", status_code: int = 500):
        super().__init__(message, status_code)


class NotFoundError(AppError):
    """Resource not found error (404)."""

    def __init__(self, message: str = "Resource not found"):
        super().__init__(message, 404)


# =============================================================================
# Error Handlers
# =============================================================================


@app.errorhandler(AppError)
def handle_app_error(error: AppError):
    """Handle custom application errors."""
    logger.warning(f"AppError: {error.message} (status={error.status_code})")
    return jsonify({"error": error.message}), error.status_code


@app.errorhandler(413)
def handle_file_too_large(error):
    """Handle file size exceeded error."""
    logger.warning("File upload exceeded size limit")
    return jsonify({"error": "File size exceeds 10MB limit"}), 413


@app.errorhandler(404)
def handle_not_found(error):
    """Handle 404 errors gracefully."""
    logger.debug(f"404 Not Found: {request.url}")
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def handle_internal_error(error):
    """Handle unexpected internal errors."""
    logger.error(f"Internal error: {error}\n{traceback.format_exc()}")
    return jsonify({"error": "Internal server error"}), 500


@app.errorhandler(Exception)
def handle_unexpected_error(error):
    """Handle any unexpected exceptions."""
    from werkzeug.exceptions import HTTPException

    if isinstance(error, HTTPException):
        return jsonify({"error": error.description or str(error)}), error.code
    logger.error(f"Unexpected error: {type(error).__name__}: {error}\n{traceback.format_exc()}")
    return jsonify({"error": "An unexpected error occurred"}), 500


# =============================================================================
# Helper Functions
# =============================================================================


def get_databricks_credentials() -> tuple[str | None, str | None]:
    """Get Databricks credentials using SP auth.

    Priority:
    1. DATABRICKS_TOKEN environment variable (PAT / CLI)
    2. None (falls back to SDK auto-detection in downstream code)
    """
    host = os.environ.get("DATABRICKS_HOST")
    return host, os.environ.get("DATABRICKS_TOKEN")


def validate_json_structure(data: dict) -> None:
    """Validate that the JSON has expected query profile structure.

    Args:
        data: Parsed JSON data

    Raises:
        UserInputError: If the JSON structure is invalid
    """
    # Check for minimum required fields
    has_query_data = "query" in data or "id" in data or "metrics" in data or "graphs" in data

    if not has_query_data:
        raise UserInputError(
            "Invalid query profile format. Expected 'query', 'id', 'metrics', or 'graphs' field."
        )


def run_analysis_background(
    analysis_id: str,
    data: dict,
    filename: str,
    primary_model: str,
    review_model: str,
    refine_model: str,
    skip_llm: bool,
    explain_text: str | None = None,
    lang: str = "en",
    enable_report_review: bool = False,
    enable_report_refine: bool = False,
    enable_table_write: bool = False,
    profiler_catalog: str = "main",
    profiler_schema: str = "profiler",
    profiler_http_path: str = "",
    experiment_id: str = "",
    variant: str = "",
    baseline_flag: bool = False,
):
    """Run analysis in background thread using the shared pipeline."""
    try:
        # Set language for this thread (required for translations in background)
        set_core_language(lang)

        databricks_host, databricks_token = get_databricks_credentials()
        logger.info(
            "Credentials check: host_set=%s, token_set=%s",
            bool(databricks_host),
            bool(databricks_token),
        )

        llm_config = LLMConfig(
            primary_model=primary_model,
            review_model=review_model,
            refine_model=refine_model,
            databricks_host=databricks_host or "",
            databricks_token=databricks_token or "",
            lang=lang,
        )

        options = PipelineOptions(
            skip_llm=skip_llm,
            enable_report_review=enable_report_review,
            enable_report_refine=enable_report_refine,
            explain_text=explain_text,
            lang=lang,
        )

        # Update status
        analysis_store[analysis_id]["status"] = "processing"

        def on_stage(stage: str) -> None:
            analysis_store[analysis_id]["stage"] = stage

        # Use v3 pipeline with auto fingerprint/family generation + optional persistence
        writer = None
        table_write_error = None
        if enable_table_write and profiler_http_path:
            try:
                from services.table_writer import TableWriter, TableWriterConfig

                tw_config = TableWriterConfig(
                    catalog=profiler_catalog,
                    schema=profiler_schema,
                    databricks_host=databricks_host or "",
                    databricks_token=databricks_token or "",
                    http_path=profiler_http_path,
                    enabled=True,
                )
                writer = TableWriter(tw_config)
            except Exception as e:
                table_write_error = f"Table connection failed: {e}"
                logger.warning("TableWriter init failed: %s", e)
        elif enable_table_write and not profiler_http_path:
            table_write_error = (
                "SQL Warehouse HTTP Path is not configured. Configure it in Table Output Settings."
            )

        from core.models import AnalysisContext
        from core.usecases import run_analysis_and_persist_pipeline

        analysis_context = None
        if experiment_id or variant:
            analysis_context = AnalysisContext(
                experiment_id=experiment_id,
                variant=variant,
                baseline_flag=baseline_flag,
            )

        # Run analysis WITHOUT writer first, then persist separately to capture errors
        result = run_analysis_and_persist_pipeline(
            data,
            llm_config,
            options,
            writer=None,  # Don't pass writer here — we'll write manually below
            analysis_context=analysis_context,
            on_stage=on_stage,
        )

        # Store results
        analysis_store[analysis_id]["analysis"] = result.analysis
        analysis_store[analysis_id]["llm_result"] = {
            "llm_analysis": result.llm_analysis,
            "review_analysis": result.review_analysis,
            "refined_analysis": result.refined_analysis,
            "primary_model": primary_model,
            "review_model": review_model,
            "refine_model": refine_model,
            "llm_enabled": result.llm_enabled,
            "llm_errors": result.llm_errors,
        }
        analysis_store[analysis_id]["report"] = result.report
        analysis_store[analysis_id]["baseline_comparison"] = result.baseline_comparison

        # Persist separately to capture write errors
        persisted = False
        if writer is not None and not table_write_error:
            import json as _json

            try:
                write_result = writer.write(
                    result.analysis,
                    report=result.report,
                    raw_profile_json=_json.dumps(data, ensure_ascii=False, default=str),
                    lang=lang,
                )
                if write_result:
                    persisted = True
                else:
                    table_write_error = (
                        "Failed to save analysis to Delta tables. Check Settings and try again."
                    )
            except Exception as e:
                table_write_error = f"Table write failed: {e}"
                logger.exception("Table write failed")

        # Mark as completed
        analysis_store[analysis_id]["status"] = "completed"
        analysis_store[analysis_id]["stage"] = "done"
        analysis_store[analysis_id]["persisted"] = persisted
        if table_write_error:
            analysis_store[analysis_id]["table_write_error"] = table_write_error
        logger.info(
            "Analysis completed: id=%s, query_id=%s",
            analysis_id,
            result.analysis.query_metrics.query_id,
        )

    except Exception as e:
        logger.error(
            "Background analysis failed: %s: %s\n%s",
            type(e).__name__,
            e,
            traceback.format_exc(),
        )
        analysis_store[analysis_id]["status"] = "failed"
        analysis_store[analysis_id]["error"] = str(e)


# =============================================================================
# Register Blueprints
# =============================================================================

from routes.analysis import bp as analysis_bp
from routes.compare import bp as compare_bp
from routes.genie_chat import bp as genie_chat_bp
from routes.history import bp as history_bp
from routes.report import bp as report_bp
from routes.schema_analysis import bp as schema_analysis_bp
from routes.settings import bp as settings_bp
from routes.share import bp as share_bp
from routes.spark_perf import bp as spark_perf_bp
from routes.workload import bp as workload_bp

app.register_blueprint(analysis_bp)
app.register_blueprint(history_bp)
app.register_blueprint(compare_bp)
app.register_blueprint(spark_perf_bp)
app.register_blueprint(schema_analysis_bp)
app.register_blueprint(settings_bp)
app.register_blueprint(report_bp)
app.register_blueprint(share_bp)
app.register_blueprint(workload_bp)
app.register_blueprint(genie_chat_bp)


# =============================================================================
# Routes (kept in app.py: index, health, favicon, api docs)
# =============================================================================


def _collect_endpoints() -> list[dict]:
    """Introspect Flask routes and return structured endpoint list."""
    endpoints = []
    seen = set()
    for rule in sorted(app.url_map.iter_rules(), key=lambda r: r.rule):
        if rule.rule in seen:
            continue
        seen.add(rule.rule)
        # Skip static files and internal endpoints
        if rule.rule.startswith("/static") or rule.endpoint == "static":
            continue
        methods = sorted(rule.methods - {"HEAD", "OPTIONS"})
        if not methods:
            continue
        # Get docstring from view function
        view_func = app.view_functions.get(rule.endpoint)
        doc = (view_func.__doc__ or "").strip().split("\n")[0] if view_func else ""
        # Determine blueprint
        bp_name = rule.endpoint.split(".")[0] if "." in rule.endpoint else "app"
        endpoints.append(
            {
                "path": rule.rule,
                "methods": methods,
                "description": doc,
                "blueprint": bp_name,
            }
        )
    return endpoints


@app.route("/api/docs")
def api_docs_page():
    """Auto-generated API documentation page."""
    endpoints = _collect_endpoints()
    # Group by blueprint
    groups: dict[str, list] = {}
    for ep in endpoints:
        groups.setdefault(ep["blueprint"], []).append(ep)
    return render_template("api_docs.html", groups=groups, total=len(endpoints))


@app.route("/api/docs.json")
def api_docs_json():
    """Machine-readable endpoint list."""
    return jsonify({"endpoints": _collect_endpoints()})


@app.route("/")
def index():
    """Home page — landing with 3 feature cards."""
    return render_template("index.html", current_lang=get_locale(), app_version=APP_VERSION)


@app.route("/analyze")
def analyze_page():
    """Query profile upload page."""
    databricks_host, _ = get_databricks_credentials()
    models = list_chat_models(host=databricks_host)

    from core.config_store import get_setting

    profiler_catalog = get_setting("catalog", "main")
    profiler_schema = get_setting("schema", "profiler")
    profiler_http_path = get_setting("http_path", "")
    table_write_enabled = get_setting("table_write_enabled", "false").lower() in (
        "true",
        "1",
        "yes",
    )

    return render_template(
        "analyze.html",
        stage_messages=get_stage_messages(),
        current_lang=get_locale(),
        app_version=APP_VERSION,
        available_models=models,
        profiler_catalog=profiler_catalog,
        profiler_schema=profiler_schema,
        profiler_http_path=profiler_http_path,
        table_write_enabled=table_write_enabled,
    )


@app.route("/api/v1/models")
def api_models():
    """List available LLM chat models."""
    databricks_host, _ = get_databricks_credentials()
    models = list_chat_models(host=databricks_host)
    return jsonify([{"name": m.name, "display_name": m.display_name} for m in models])


@app.route("/health")
def health():
    """Health check endpoint."""
    databricks_host, databricks_token = get_databricks_credentials()
    return jsonify(
        {
            "status": "ok",
            "llm_available": bool(databricks_host and databricks_token),
        }
    )


@app.route("/favicon.ico")
def favicon():
    """Return empty favicon to prevent 404 errors."""
    return Response(status=204)


if __name__ == "__main__":
    # Use environment variable for debug mode (default: False for security)
    debug_mode = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    app.run(debug=debug_mode, host="0.0.0.0", port=8000)
