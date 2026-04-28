"""Step 2 of ``enrich_llm_canonical``: rebuild the ``context`` block.

The schema requires a ``context`` object describing the execution
environment (warehouse type/size, serverless / streaming / federation
flags, query duration, rows produced). The LLM has no reliable way to
know any of these — they live on the analysis. So even if the LLM
emitted a partial context, this step rebuilds it from the analysis as
the authoritative source.

The same builder is also reused by the V5→V6 normalizer adapter
(``normalizer.build_canonical_report``) so context layout stays
identical between the two paths.
"""

from __future__ import annotations

from typing import Any

from core.models import ProfileAnalysis


def build_context(qm, analysis: ProfileAnalysis, language: str) -> dict[str, Any]:
    """Pure helper — returns a fresh dict, does not mutate inputs."""
    is_serverless = bool(
        qm.query_typename == "LakehouseSqlQuery"
        or (analysis.warehouse_info and analysis.warehouse_info.is_serverless)
    )
    ctx: dict[str, Any] = {
        "is_serverless": is_serverless,
        "is_streaming": analysis.streaming_context is not None,
        "is_federation": bool(qm.is_federation_query),
        "result_from_cache": bool(qm.result_from_cache),
        "language": language if language in ("en", "ja") else "en",
    }
    wh = getattr(analysis, "warehouse_info", None)
    if wh:
        # WarehouseInfo uses cluster_size + warehouse_type (Day 4 v1
        # incorrectly used .size/.type — fixed Day 5).
        ctx["warehouse_size"] = getattr(wh, "cluster_size", None) or None
        ctx["warehouse_type"] = getattr(wh, "warehouse_type", None) or None
    else:
        ctx["warehouse_size"] = None
        ctx["warehouse_type"] = None
    ctx["duration_ms"] = qm.total_time_ms or None
    ctx["rows_produced"] = qm.rows_produced_count or None
    return ctx


def rebuild_context(
    out: dict[str, Any], analysis: ProfileAnalysis, language: str
) -> dict[str, Any]:
    """Always overwrite ``out['context']`` from the analysis since the
    analysis is authoritative. Mutates and returns ``out``."""
    qm = analysis.query_metrics
    if qm is not None:
        out["context"] = build_context(qm, analysis, language)
    return out
