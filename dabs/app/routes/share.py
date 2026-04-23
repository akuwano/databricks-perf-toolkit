"""Blueprint for share routes: /shared/<id>, /api/v1/shared/<id>/summary."""

import logging
from dataclasses import dataclass
from typing import Any

from flask import Blueprint, Response, abort, render_template, request

logger = logging.getLogger(__name__)

bp = Blueprint("share", __name__)


@dataclass
class _SharedData:
    """Resolved data for a shared analysis."""

    analysis: Any
    report_markdown: str = ""
    source: str = ""  # 'memory' or 'delta'
    warehouse_name: str = ""
    warehouse_size: str = ""
    action_card_count: int = 0
    critical_alert_count: int = 0
    warning_alert_count: int = 0


def _load_from_delta(analysis_id: str):
    """Try to load analysis from Delta tables. Returns AnalysisWithReport or None."""
    try:
        from services.table_reader import TableReader
        from services.table_writer import TableWriterConfig

        config = TableWriterConfig.from_env()
        if not config.http_path:
            logger.warning("Delta lookup skipped: http_path not configured")
            return None
        reader = TableReader(config)
        return reader.get_analysis_with_report(analysis_id)
    except Exception:
        logger.exception("Failed to load from Delta: %s", analysis_id)
        return None


def _load_analysis(analysis_id: str) -> _SharedData | None:
    """Load analysis from in-memory store or Delta."""
    from app import analysis_store

    entry = analysis_store.get(analysis_id)
    if entry and entry.get("status") == "completed":
        analysis = entry.get("analysis")
        bi = analysis.bottleneck_indicators if analysis else None
        return _SharedData(
            analysis=analysis,
            report_markdown=entry.get("report", ""),
            source="memory",
            action_card_count=(len(bi.critical_issues) + len(bi.warnings)) if bi else 0,
            critical_alert_count=len(bi.critical_issues) if bi else 0,
        )

    result = _load_from_delta(analysis_id)
    if result:
        return _SharedData(
            analysis=result.analysis,
            report_markdown=result.report_markdown,
            source="delta",
            warehouse_name=result.warehouse_name,
            warehouse_size=result.warehouse_size,
            action_card_count=result.action_card_count,
            critical_alert_count=result.critical_alert_count,
            warning_alert_count=result.high_alert_count + result.medium_alert_count,
        )

    return None


@bp.route("/shared/<analysis_id>")
def shared_result_page(analysis_id: str):
    """Shared analysis result page — persistent URL."""
    from app import get_locale

    data = _load_analysis(analysis_id)
    if data is None:
        abort(404)

    from core.i18n import relocalize_report
    from core.serving_client import list_chat_models

    current_lang = get_locale()
    report = relocalize_report(data.report_markdown, current_lang)

    return render_template(
        "shared_result.html",
        analysis_id=analysis_id,
        analysis=data.analysis,
        report=report,
        source=data.source,
        action_card_count=data.action_card_count,
        critical_alert_count=data.critical_alert_count,
        current_lang=current_lang,
        available_models=list_chat_models(),
    )


@bp.route("/api/v1/shared/<analysis_id>/summary")
def get_slack_summary(analysis_id: str):
    """Return plain-text Slack summary for an analysis."""
    from core.summary_builder import build_slack_summary

    data = _load_analysis(analysis_id)
    if data is None:
        abort(404)

    summary = build_slack_summary(
        analysis_id=analysis_id,
        query_metrics=data.analysis.query_metrics,
        bottleneck_indicators=data.analysis.bottleneck_indicators,
        action_count=data.action_card_count,
        base_url=request.args.get("base_url", "").rstrip("/"),
        warehouse_name=data.warehouse_name,
        warehouse_size=data.warehouse_size,
        critical_count=data.critical_alert_count,
        warning_count=data.warning_alert_count,
    )

    return Response(summary, mimetype="text/plain")
