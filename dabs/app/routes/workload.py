"""Blueprint for workload routes: /workload, /api/v1/workload/*."""

import logging

from flask import Blueprint, jsonify, render_template, request

logger = logging.getLogger(__name__)

bp = Blueprint("workload", __name__)


@bp.route("/workload")
def workload_page():
    """Cross-analysis workload page (DBSQL + Spark side-by-side)."""
    from app import get_locale

    return render_template("workload.html", current_lang=get_locale())


# ---------------------------------------------------------------------------
# Workload pairs: lightweight DBSQL ↔ Spark app linking
# ---------------------------------------------------------------------------


def _load_pairs() -> list[dict]:
    """Load saved workload pairs from config store."""
    from core.config_store import load_config

    config = load_config()
    return config.get("workload_pairs", [])


def _save_pairs(pairs: list[dict]) -> None:
    """Save workload pairs to config store."""
    from core.config_store import save_config

    save_config({"workload_pairs": pairs})


@bp.route("/api/v1/workload/pairs", methods=["GET"])
def list_pairs():
    """List saved DBSQL ↔ Spark app pairs."""
    return jsonify({"pairs": _load_pairs()})


@bp.route("/api/v1/workload/pairs", methods=["POST"])
def save_pair():
    """Save a DBSQL ↔ Spark app pair."""
    from app import UserInputError

    data = request.get_json()
    if not data:
        raise UserInputError("JSON body required")

    analysis_id = data.get("analysis_id", "").strip()
    app_id = data.get("app_id", "").strip()
    if not analysis_id or not app_id:
        raise UserInputError("analysis_id and app_id are required")

    pair = {
        "analysis_id": analysis_id,
        "app_id": app_id,
        "label": data.get("label", ""),
    }

    pairs = _load_pairs()
    # Avoid duplicates
    pairs = [p for p in pairs if not (p["analysis_id"] == analysis_id and p["app_id"] == app_id)]
    pairs.insert(0, pair)
    _save_pairs(pairs)

    return jsonify({"status": "saved", "pair": pair})


@bp.route("/api/v1/workload/pairs", methods=["DELETE"])
def delete_pair():
    """Delete a DBSQL ↔ Spark app pair."""
    from app import UserInputError

    data = request.get_json()
    if not data:
        raise UserInputError("JSON body required")

    analysis_id = data.get("analysis_id", "").strip()
    app_id = data.get("app_id", "").strip()

    pairs = _load_pairs()
    pairs = [p for p in pairs if not (p["analysis_id"] == analysis_id and p["app_id"] == app_id)]
    _save_pairs(pairs)

    return jsonify({"status": "deleted"})


# ---------------------------------------------------------------------------
# Report proxy: fetch DBSQL report markdown by analysis_id
# ---------------------------------------------------------------------------


@bp.route("/api/v1/workload/report")
def workload_report():
    """Fetch DBSQL report markdown for side-by-side display."""
    from app import UserInputError

    analysis_id = request.args.get("analysis_id")
    if not analysis_id:
        raise UserInputError("analysis_id is required")

    # Try in-memory store first, then Delta
    from app import analysis_store

    stored = analysis_store.get(analysis_id)
    if stored and stored.get("report"):
        from app import get_locale
        from core.i18n import relocalize_report

        report = relocalize_report(stored["report"], get_locale())
        return jsonify({"report": report})

    # Fallback to Delta
    from services.table_reader import TableReader
    from services.table_writer import TableWriterConfig

    config = TableWriterConfig.from_env()
    if not config.http_path:
        return jsonify({"error": "Warehouse not configured"}), 503

    reader = TableReader(config)
    result = reader.get_analysis_with_report(analysis_id)
    if result is None:
        return jsonify({"error": "Analysis not found"}), 404

    return jsonify({"report": result.report_markdown})


# ---------------------------------------------------------------------------
# Cross-analysis: LLM-powered DBSQL × Spark insight generation
# ---------------------------------------------------------------------------

_CROSS_ANALYZE_PROMPT = """You are a Databricks performance expert. You are given two performance reports:

1. **DBSQL Query Profile Report** — analysis of a Databricks SQL query
2. **Spark Job Performance Report** — analysis of a Spark application/job

Your task: Identify cross-cutting insights by comparing both reports. Write a concise Markdown analysis.

## Output Structure (write in the same language as the input reports):

### 1. Correlation Summary
- Are these likely the same workload? (time overlap, similar data volumes, etc.)
- Key shared characteristics

### 2. Common Bottlenecks
- Issues that appear in BOTH reports (e.g., shuffle, skew, spill)
- For each: severity from DBSQL side vs Spark side

### 3. Root Cause Hypothesis
- What is the most likely root cause connecting both sides?
- Which layer (SQL optimizer, Spark execution, infrastructure) is the primary source?

### 4. Unified Recommendations (Top 3)
- Prioritized actions that address issues visible in both reports
- For each: expected impact, which side benefits most

Keep it concise (300-500 words). Focus on actionable insights, not restating what each report says."""


@bp.route("/api/v1/workload/cross-analyze", methods=["POST"])
def cross_analyze():
    """Generate LLM cross-analysis from DBSQL and Spark reports."""
    from app import UserInputError, get_databricks_credentials
    from core.llm_client import call_llm_with_retry, create_openai_client

    data = request.get_json(silent=True)
    if not data:
        raise UserInputError("JSON body required")

    dbsql_report = (data.get("dbsql_report") or "").strip()
    spark_report = (data.get("spark_report") or "").strip()

    if not dbsql_report or not spark_report:
        raise UserInputError("Both dbsql_report and spark_report are required")

    host, token = get_databricks_credentials()
    if not host:
        return jsonify({"error": "Databricks credentials not available"}), 503

    client = create_openai_client(host, token or "")

    user_content = (
        "## DBSQL Query Profile Report\n\n"
        + dbsql_report[:8000]
        + "\n\n---\n\n## Spark Job Performance Report\n\n"
        + spark_report[:8000]
    )

    messages = [
        {"role": "system", "content": _CROSS_ANALYZE_PROMPT},
        {"role": "user", "content": user_content},
    ]

    model = (data.get("model") or "databricks-claude-opus-4-7").strip()

    result = call_llm_with_retry(
        client=client,
        model=model,
        messages=messages,
        max_tokens=2048,
        temperature=0.2,
    )

    return jsonify({"analysis": result})
