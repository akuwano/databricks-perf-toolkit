"""Blueprint for report routes: /report, /api/v1/report/*."""

import logging
import uuid

from flask import Blueprint, jsonify, render_template, request
from flask_babel import gettext as _
from werkzeug.utils import secure_filename

logger = logging.getLogger(__name__)

bp = Blueprint("report", __name__)


@bp.route("/report")
def report_upload_page():
    """Page for uploading saved Markdown reports."""
    from app import get_locale

    return render_template(
        "report_upload.html",
        current_lang=get_locale(),
    )


@bp.route("/api/v1/report/upload", methods=["POST"])
def upload_report():
    """Upload a Markdown report for viewing.

    Returns:
        JSON response with report ID and redirect URL
    """
    from app import UserInputError, report_store

    if "file" not in request.files:
        raise UserInputError("No file provided")

    file = request.files["file"]
    if file.filename == "":
        raise UserInputError("No file selected")

    if not file.filename.endswith((".md", ".markdown")):
        raise UserInputError("File must be Markdown format (.md)")

    try:
        content = file.read().decode("utf-8")
    except UnicodeDecodeError:
        raise UserInputError("File must be valid UTF-8 text") from None

    if not content.strip():
        raise UserInputError("File is empty")

    # Create report entry
    report_id = str(uuid.uuid4())
    report_store[report_id] = {
        "filename": file.filename,
        "content": content,
    }

    logger.info(f"Report uploaded: id={report_id}, filename={file.filename}")

    return jsonify(
        {
            "id": report_id,
            "redirect_url": f"/report/{report_id}",
        }
    )


@bp.route("/report/<report_id>")
def report_view_page(report_id: str):
    """Display uploaded Markdown report."""
    from app import get_locale, report_store

    if report_id not in report_store:
        return render_template(
            "error.html",
            error=_("Report not found"),
            current_lang=get_locale(),
        ), 404

    stored = report_store[report_id]

    from core.i18n import relocalize_report

    current_lang = get_locale()
    report = relocalize_report(stored["content"], current_lang)

    return render_template(
        "report_view.html",
        report_id=report_id,
        report=report,
        filename=stored["filename"],
        current_lang=current_lang,
    )


@bp.route("/api/v1/report/<report_id>/download")
def download_uploaded_report(report_id: str):
    """Download uploaded report as Markdown."""
    from app import NotFoundError, report_store

    if report_id not in report_store:
        raise NotFoundError("Report not found")

    stored = report_store[report_id]

    return (
        stored["content"],
        200,
        {
            "Content-Type": "text/markdown; charset=utf-8",
            "Content-Disposition": f"attachment; filename={secure_filename(stored['filename']) or 'report.md'}",
        },
    )
