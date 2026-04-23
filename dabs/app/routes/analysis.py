"""Blueprint for analysis routes: /api/v1/analyze, /result, file upload analysis."""

import json
import logging
import os
import uuid
from threading import Thread

from flask import Blueprint, jsonify, render_template, request
from flask_babel import gettext as _
from werkzeug.exceptions import ClientDisconnected

logger = logging.getLogger(__name__)

bp = Blueprint("analysis", __name__)


@bp.route("/api/v1/analyze", methods=["POST"])
def analyze():
    """Start analysis of uploaded JSON file (async).

    Returns:
        JSON response with analysis ID for polling
    """
    from app import (
        UserInputError,
        analysis_store,
        get_locale,
        run_analysis_background,
        validate_json_structure,
    )

    # Handle client disconnection during file upload
    try:
        files = request.files
    except ClientDisconnected:
        logger.warning("Client disconnected during file upload")
        return jsonify({"error": "Upload interrupted - please try again"}), 400

    # Validate file presence
    if "file" not in files:
        raise UserInputError("No file provided")

    file = files["file"]
    if file.filename == "":
        raise UserInputError("No file selected")

    if not file.filename.endswith(".json"):
        raise UserInputError("File must be JSON format")

    # Parse and validate JSON
    try:
        data = json.load(file)
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON uploaded: {e}")
        raise UserInputError("Invalid JSON file: malformed JSON syntax") from None

    # Validate JSON structure + verbose check
    try:
        validate_json_structure(data)
    except UserInputError:
        raise
    except Exception as e:
        logger.error(f"JSON validation error: {e}")
        raise UserInputError("Invalid query profile format") from None

    from core.profile_validator import validate_profile

    validation = validate_profile(data)
    if not validation.valid:
        raise UserInputError("; ".join(validation.errors))

    # Get LLM options from request
    primary_model = request.form.get("primary_model", "databricks-claude-opus-4-6")
    review_model = request.form.get("review_model", "databricks-claude-opus-4-6")
    refine_model = request.form.get("refine_model", "databricks-gpt-5-4")
    skip_llm = request.form.get("skip_llm", "false").lower() == "true"

    # Get report review/refine options (default: off)
    # Checkbox sends "on" when checked, nothing when unchecked
    enable_report_review = request.form.get("enable_report_review", "").lower() in (
        "true",
        "on",
        "1",
        "yes",
    )
    enable_report_refine = request.form.get("enable_report_refine", "").lower() in (
        "true",
        "on",
        "1",
        "yes",
    )

    # Get table output options from form
    enable_table_write = request.form.get("enable_table_write", "").lower() in (
        "true",
        "on",
        "1",
        "yes",
    )
    profiler_catalog = request.form.get("profiler_catalog", "").strip() or os.environ.get(
        "PROFILER_CATALOG", "main"
    )
    profiler_schema = request.form.get("profiler_schema", "").strip() or os.environ.get(
        "PROFILER_SCHEMA", "profiler"
    )
    profiler_http_path = request.form.get("profiler_http_path", "").strip() or os.environ.get(
        "PROFILER_WAREHOUSE_HTTP_PATH", ""
    )

    # Get EXPLAIN text (from file or textarea)
    explain_text = None
    if "explain_file" in files:
        explain_file = files["explain_file"]
        if explain_file.filename:
            try:
                explain_text = explain_file.read().decode("utf-8")
            except Exception as e:
                logger.warning(f"Failed to read EXPLAIN file: {e}")
    if not explain_text:
        explain_text = request.form.get("explain_text", "")

    # Create analysis entry
    analysis_id = str(uuid.uuid4())
    analysis_store[analysis_id] = {
        "status": "pending",
        "stage": "queued",
        "filename": file.filename,
        "analysis": None,
        "llm_result": None,
        "report": None,
        "error": None,
    }

    # Get locale in request context before starting background thread
    lang = get_locale()

    # Get experiment/variant from form
    experiment_id = request.form.get("experiment_id", "").strip()
    variant_name = request.form.get("variant", "").strip()
    baseline_flag = variant_name.lower() == "baseline"

    # Start background processing
    thread = Thread(
        target=run_analysis_background,
        args=(
            analysis_id,
            data,
            file.filename,
            primary_model,
            review_model,
            refine_model,
            skip_llm,
            explain_text,
            lang,
            enable_report_review,
            enable_report_refine,
            enable_table_write,
            profiler_catalog,
            profiler_schema,
            profiler_http_path,
            experiment_id,
            variant_name,
            baseline_flag,
        ),
    )
    thread.daemon = True
    thread.start()

    logger.info(f"Analysis started: id={analysis_id}, filename={file.filename}")

    return jsonify(
        {
            "id": analysis_id,
            "status": "pending",
            "validation_warnings": validation.warnings,
            "is_verbose": validation.is_verbose,
        }
    )


@bp.route("/api/v1/analyze/<analysis_id>/status")
def get_analysis_status(analysis_id: str):
    """Get analysis status for polling."""
    from app import NotFoundError, analysis_store

    if analysis_id not in analysis_store:
        raise NotFoundError("Analysis not found")

    stored = analysis_store[analysis_id]

    response = {
        "id": analysis_id,
        "status": stored["status"],
        "stage": stored.get("stage", "unknown"),
    }

    if stored["status"] == "completed":
        response["redirect_url"] = f"/result/{analysis_id}"
        if stored.get("table_write_error"):
            response["table_write_error"] = stored["table_write_error"]
        llm_result = stored.get("llm_result", {})
        if llm_result and llm_result.get("llm_errors"):
            response["llm_errors"] = llm_result["llm_errors"]
    elif stored["status"] == "failed":
        response["error"] = stored.get("error", "Unknown error")

    return jsonify(response)


@bp.route("/api/v1/analyze/<analysis_id>")
def get_analysis(analysis_id: str):
    """Get analysis result by ID."""
    from app import NotFoundError, UserInputError, analysis_store

    if analysis_id not in analysis_store:
        raise NotFoundError("Analysis not found")

    stored = analysis_store[analysis_id]

    if stored["status"] != "completed":
        raise UserInputError(f"Analysis not ready. Status: {stored['status']}")

    analysis = stored["analysis"]
    qm = analysis.query_metrics
    bi = analysis.bottleneck_indicators

    return jsonify(
        {
            "id": analysis_id,
            "filename": stored["filename"],
            "query_metrics": {
                "query_id": qm.query_id,
                "status": qm.status,
                "total_time_ms": qm.total_time_ms,
                "read_bytes": qm.read_bytes,
            },
            "bottleneck_indicators": {
                "cache_hit_ratio": bi.cache_hit_ratio,
                "photon_ratio": bi.photon_ratio,
                "spill_bytes": bi.spill_bytes,
                "shuffle_impact_ratio": bi.shuffle_impact_ratio,
                "critical_issues": bi.critical_issues,
                "warnings": bi.warnings,
            },
            "llm_enabled": stored["llm_result"].get("llm_enabled", False),
        }
    )


@bp.route("/api/v1/analyze/<analysis_id>/download")
def download_report(analysis_id: str):
    """Download report as Markdown."""
    from app import NotFoundError, UserInputError, analysis_store

    if analysis_id not in analysis_store:
        raise NotFoundError("Analysis not found")

    stored = analysis_store[analysis_id]

    if stored["status"] != "completed":
        raise UserInputError(f"Analysis not ready. Status: {stored['status']}")

    return (
        stored["report"],
        200,
        {
            "Content-Type": "text/markdown; charset=utf-8",
            "Content-Disposition": f"attachment; filename=report_{analysis_id[:8]}.md",
        },
    )


@bp.route("/result/<analysis_id>")
def result_page(analysis_id: str):
    """Display analysis result page."""
    from app import analysis_store, get_locale

    if analysis_id not in analysis_store:
        return render_template(
            "error.html",
            error=_("Analysis not found"),
            current_lang=get_locale(),
        ), 404

    stored = analysis_store[analysis_id]

    if stored["status"] != "completed":
        return render_template(
            "error.html",
            error=_("Analysis not ready. Status: {status}").format(status=stored["status"]),
            current_lang=get_locale(),
        ), 400

    # Re-localize report headers to match the current UI language
    from core.i18n import relocalize_report

    current_lang = get_locale()
    report = relocalize_report(stored["report"], current_lang)

    return render_template(
        "result.html",
        analysis_id=analysis_id,
        analysis=stored["analysis"],
        llm_result=stored["llm_result"],
        report=report,
        filename=stored["filename"],
        current_lang=current_lang,
        baseline_comparison=stored.get("baseline_comparison"),
        persisted=stored.get("persisted", False),
    )
