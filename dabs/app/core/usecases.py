"""
Use-case orchestrators for the analysis pipeline.

Provides a single entry point for the full analysis workflow
(metrics extraction -> EXPLAIN -> LLM analysis -> report generation)
so that CLI, Web, and future interfaces share the same logic.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .analyzers import (
    analyze_from_dict,
    enhance_bottleneck_with_explain,
    generate_action_cards,
)
from .explain_parser import parse_explain_extended
from .llm import (
    analyze_with_llm,
    load_tuning_knowledge,
    parse_llm_sections,
    refine_report_with_llm,
    refine_with_llm,
    review_report_with_llm,
    review_with_llm,
    select_top_actions_with_llm,
)
from .models import (
    AnalysisContext,
    ComparisonRequest,
    ComparisonResult,
    ProfileAnalysis,
)
from .reporters import generate_report
from .warehouse_client import WarehouseClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration dataclasses
# ---------------------------------------------------------------------------


@dataclass
class LLMConfig:
    """LLM-related configuration."""

    primary_model: str = "databricks-claude-opus-4-6"
    review_model: str = "databricks-claude-opus-4-6"
    refine_model: str = "databricks-gpt-5-4"
    databricks_host: str = ""
    databricks_token: str = ""
    tuning_file: str | None = None
    lang: str = "en"

    @property
    def is_available(self) -> bool:
        """Check if LLM credentials are available.

        Supports two modes:
        1. PAT token: both host and token must be set
        2. SDK auth: host is set, token is empty but SDK credentials exist
           (DATABRICKS_CLIENT_ID/SECRET auto-injected in Databricks Apps)
        """
        if self.databricks_host and self.databricks_token:
            return True
        # Check for Databricks SDK auth (service principal in Apps)
        if self.databricks_host:
            try:
                import os

                return bool(
                    os.environ.get("DATABRICKS_CLIENT_ID")
                    and os.environ.get("DATABRICKS_CLIENT_SECRET")
                )
            except Exception:
                pass
        return False


@dataclass
class PipelineOptions:
    """Options controlling which pipeline stages run."""

    skip_llm: bool = False
    skip_review: bool = False
    skip_refine: bool = False
    enable_report_review: bool = False
    enable_report_refine: bool = False
    verbose: bool = False
    explain_text: str | None = None
    lang: str = "en"


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    """Result of a full analysis pipeline run."""

    analysis: ProfileAnalysis = field(default_factory=ProfileAnalysis)
    report: str = ""
    llm_analysis: str = ""
    review_analysis: str = ""
    refined_analysis: str = ""
    llm_enabled: bool = False
    llm_errors: list[str] = field(default_factory=list)
    # v4.6: Baseline comparison (auto-populated when persist + baseline found)
    baseline_comparison: ComparisonResult | None = None


# ---------------------------------------------------------------------------
# Stage progress callback
# ---------------------------------------------------------------------------

# Callable that receives the current stage name, e.g. "metrics", "llm_initial"
StageCallback = Callable[[str], None]


def _noop_stage(_stage: str) -> None:
    pass


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------


def run_analysis_pipeline(
    data: dict[str, Any],
    llm_config: LLMConfig,
    options: PipelineOptions,
    on_stage: StageCallback | None = None,
) -> PipelineResult:
    """Run the full analysis pipeline.

    Stages (in order):
        1. metrics   – extract metrics from profile JSON
        2. warehouse – fetch SQL warehouse info (if available)
        3. explain   – parse EXPLAIN EXTENDED (if provided)
        4. llm_initial / llm_review / llm_refine – LLM 3-stage analysis
        5. report    – generate Markdown report
        6. report_review / report_refine – optional report QA

    Args:
        data: Raw query profile JSON (dict).
        llm_config: LLM credentials and model names.
        options: Feature flags controlling which stages run.
        on_stage: Optional callback invoked when a new stage starts.

    Returns:
        PipelineResult with analysis object, report text, and LLM outputs.
    """
    notify = on_stage or _noop_stage
    result = PipelineResult()

    # ---- 1. Metrics extraction ----
    notify("metrics")
    llm_clustering_config = _build_clustering_config(llm_config, options)
    result.analysis = analyze_from_dict(data, llm_clustering_config=llm_clustering_config)

    # ---- 2. Warehouse info ----
    _fetch_warehouse_info(result.analysis, llm_config)

    # ---- 2b. Apply serverless filter to action cards from step 1 ----
    # Detect serverless: profile __typename (reliable) > warehouse API (fallback)
    is_serverless = result.analysis.query_metrics.query_typename == "LakehouseSqlQuery" or (
        result.analysis.warehouse_info.is_serverless if result.analysis.warehouse_info else False
    )
    if is_serverless and result.analysis.action_cards:
        _apply_serverless_filter(result.analysis)

    # ---- 2c. Detect streaming ----
    is_streaming = (
        result.analysis.streaming_context is not None
        and result.analysis.streaming_context.is_streaming
    )

    # ---- 2d. Detect Lakehouse Federation (v5.18.0) ----
    is_federation = bool(getattr(result.analysis.query_metrics, "is_federation_query", False))

    # ---- 3. EXPLAIN EXTENDED ----
    if options.explain_text and options.explain_text.strip():
        notify("explain")
        _apply_explain(result.analysis, options.explain_text, llm_clustering_config, is_serverless)

    # ---- 4. LLM analysis (3-stage) ----
    if not options.skip_llm and llm_config.is_available:
        result.llm_enabled = True
        tuning_knowledge = load_tuning_knowledge(llm_config.tuning_file, lang=options.lang)
        _run_llm_stages(
            result,
            llm_config,
            options,
            tuning_knowledge,
            notify,
            is_serverless=is_serverless,
            is_streaming=is_streaming,
            is_federation=is_federation,
        )
    elif not options.skip_llm:
        logger.info(
            "LLM credentials not configured, skipping LLM analysis "
            "(host_set=%s, secret_set=%s, is_available=%s, skip_llm=%s)",
            bool(llm_config.databricks_host),
            bool(llm_config.databricks_token),
            llm_config.is_available,
            options.skip_llm,
        )

    # ---- 5. Extract Action Plan from LLM output ----
    best_llm_output = result.refined_analysis or result.llm_analysis
    if best_llm_output and result.analysis.action_cards is not None:
        _merge_llm_action_plan(result.analysis, best_llm_output)

    _select_top_action_cards(result.analysis)
    if result.analysis.selected_action_cards and not options.skip_llm and llm_config.is_available:
        rerank = select_top_actions_with_llm(
            result.analysis.action_cards,
            llm_config.review_model,
            llm_config.databricks_host,
            llm_config.databricks_token,
            lang=options.lang,
        )
        if rerank:
            selected = []
            rationales = rerank.get("selection_rationale", {})
            for idx in rerank.get("selected_ids", [])[:5]:
                if 0 <= idx < len(result.analysis.action_cards):
                    card = result.analysis.action_cards[idx]
                    card.selected_because = str(rationales.get(str(idx), ""))
                    selected.append(card)
            if selected:
                result.analysis.selected_action_cards = selected

    # ---- 5b. Safety net: filter LLM-generated action cards for serverless ----
    if is_serverless and result.analysis.action_cards:
        _apply_serverless_filter(result.analysis)

    # ---- 6. Report generation ----
    notify("report")
    llm_sections = parse_llm_sections(best_llm_output) if best_llm_output else {}

    result.report = generate_report(
        analysis=result.analysis,
        llm_sections=llm_sections,
        primary_model=llm_config.primary_model if best_llm_output else "",
        verbose=options.verbose,
        raw_llm_analysis=best_llm_output or "",
        lang=options.lang,
    )

    # ---- 7. Report review / refine ----
    if (options.enable_report_review or options.enable_report_refine) and result.llm_enabled:
        tuning_knowledge = load_tuning_knowledge(llm_config.tuning_file, lang=options.lang)
        _run_report_review(result, llm_config, options, tuning_knowledge, notify)

    notify("done")
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _apply_serverless_filter(analysis: ProfileAnalysis) -> None:
    """Post-process action cards to remove non-serverless SET statements."""
    from .analyzers.recommendations import _filter_fix_sql_for_serverless

    for card in analysis.action_cards:
        if card.fix_sql:
            card.fix_sql = _filter_fix_sql_for_serverless(card.fix_sql)


def _merge_llm_action_plan(analysis: ProfileAnalysis, llm_text: str) -> None:
    """Parse LLM-generated Action Plan into ``analysis.llm_action_cards``.

    Phase 2a (v5.16.19): replaces the old preservation-marker / substring
    hybrid dedup with a narrower group-level equivalence check.
    Rule-based cards stay in ``analysis.action_cards`` (from the
    registry); LLM-generated cards go into ``llm_action_cards`` so the
    reporter can render them in a separate "LLM 独自提案" section.

    Dedup policy (``classify_root_cause_group`` + ``groups_overlap``):
      - LLM card whose classified group matches or overlaps any
        rule-based card's ``root_cause_group`` → drop the LLM entry
        (rule wins: its fix_sql is already template-tested).
      - LLM card whose group is empty / unclassified → keep it
        (fail-open — we can't prove equivalence).

    The ANALYZE TABLE filter is unchanged:
      - If stats are confirmed fresh, drop LLM ``ANALYZE TABLE``
        recommendations (they would be no-ops).
      - If stats state is unknown, append an alternative-causes caveat
        to each ``ANALYZE TABLE`` action so readers see the fallback
        root causes without needing to re-run with EXPLAIN attached.
    """
    from .action_classify import classify_root_cause_group, groups_overlap
    from .llm_prompts import parse_action_plan_from_llm
    from .models import ActionCard

    llm_actions = parse_action_plan_from_llm(llm_text)
    if not llm_actions:
        return  # Nothing to merge — rule-based cards remain untouched

    # Whether stats are confirmed fresh — used to filter out LLM-suggested
    # ANALYZE TABLE actions that would be no-ops in this state.
    _stats_fresh = bool(
        getattr(analysis.bottleneck_indicators, "statistics_confirmed_fresh", False)
    )

    def _is_analyze_table_action(item: dict[str, Any]) -> bool:
        """Detect ANALYZE TABLE recommendations to drop when stats are fresh."""
        blob = " ".join(str(item.get(k, "")) for k in ("problem", "fix", "fix_sql")).lower()
        return "analyze table" in blob or "テーブル統計の更新" in blob or "統計情報の更新" in blob

    # Caveat appended to ANALYZE TABLE recommendations when the stats state
    # is UNKNOWN (EXPLAIN not attached). Predictive optimization / a recent
    # ANALYZE run may already keep stats current, in which case re-running
    # ANALYZE is a no-op and the real cause is elsewhere. We list the
    # alternative causes explicitly so the reader does not stop at ANALYZE.
    _analyze_caveat = (
        "\n\n**注意**: 既に ANALYZE TABLE 実行済み、または予測最適化 "
        "(Predictive Optimization) が有効な環境では再実行しても改善しません。"
        "その場合、ハッシュリサイズ多発の別要因を以下の順で調査してください:\n"
        "1. **行数爆発** — フィルタ漏れや誤った JOIN 述語で想定以上に行が増えていないか（結果件数を期待値と照合）\n"
        "2. **重複 GROUP BY / 集約の再計算** — 同じキーでの集約が CTE や UNION 分岐で複数回実行されていないか（EXPLAIN で ReusedExchange 確認）\n"
        "3. **キー値スキュー** — ヘビーヒッター（1 値に行が集中）がないか（`SELECT col, COUNT(*) ... ORDER BY 2 DESC`）\n"
        "4. **NULL 集中** — JOIN/GROUP キーに NULL が大量にないか（`null_count` 確認）\n"
        "5. **JOIN キーの型不一致** — 左右の型が異なると暗黙 CAST でハッシュ衝突が起きる\n"
        "6. **DECIMAL 高精度キー** — DECIMAL(38,0) 等は BIGINT より重い。整数値のみなら BIGINT へ\n"
        "7. **UDF / 非決定的述語** — オプティマイザが行数推定不能となり、ハッシュテーブル初期容量が小さすぎる\n"
        "8. **メモリ圧迫** — 他オペレータが大量消費してハッシュテーブルが繰り返し再構築される（spill / fallback を確認）\n"
        "\n*EXPLAIN EXTENDED を添付して再分析すると、統計情報が最新かどうかが自動判定され、"
        "ANALYZE が不要と確認できた場合は本推奨は抑制されます。*"
    )

    # Pre-compute the set of root_cause_groups already covered by
    # registry-emitted rule-based cards so we can drop LLM duplicates.
    rule_groups: set[str] = {
        c.root_cause_group for c in (analysis.action_cards or []) if c.root_cause_group
    }

    llm_cards: list[ActionCard] = []
    dropped_analyze = 0
    dropped_dedup = 0
    injected_caveat = 0
    for item in llm_actions:
        is_analyze = _is_analyze_table_action(item)
        if _stats_fresh and is_analyze:
            dropped_analyze += 1
            continue
        fix_text = item.get("fix", "")
        # When stats state is unknown (no EXPLAIN), append caveat to ANALYZE
        # actions so the reader sees alternative causes without needing to
        # re-run the analysis.
        if is_analyze and not _stats_fresh and _analyze_caveat not in fix_text:
            fix_text = fix_text.rstrip() + _analyze_caveat
            injected_caveat += 1
        # Group-level dedup against rule-based cards. Classify from the
        # combined problem/fix/fix_sql text; empty group => fail-open.
        blob = " ".join(str(item.get(k, "")) for k in ("problem", "fix", "fix_sql"))
        llm_group = classify_root_cause_group(blob)
        if llm_group and any(groups_overlap(llm_group, rg) for rg in rule_groups):
            dropped_dedup += 1
            continue
        card = ActionCard(
            problem=item.get("problem", ""),
            fix=fix_text,
            fix_sql=item.get("fix_sql", ""),
            expected_impact=item.get("expected_impact", "medium"),
            effort=item.get("effort", "medium"),
            risk=item.get("risk", ""),
            risk_reason=item.get("risk_reason", ""),
            verification_steps=item.get("verification", []),
        )
        card.root_cause_group = llm_group
        card.priority_score = card.impact_score * 3 / card.effort_score
        llm_cards.append(card)

    analysis.llm_action_cards = llm_cards
    logger.info(
        "LLM action plan: %d cards kept (dropped %d ANALYZE-TABLE, "
        "%d group-overlap dupes, injected %d caveats). "
        "Rule-based groups present: %s",
        len(llm_cards),
        dropped_analyze,
        dropped_dedup,
        injected_caveat,
        sorted(rule_groups),
    )


def _select_top_action_cards(analysis: ProfileAnalysis, limit: int = 20):
    """Phase 2c (v5.16.19): Top-N cap + diversity rerank removed.

    The registry now emits rule-based cards in static ``priority_rank``
    order (Spark Perf-style). There is no need for a secondary selection
    pass — just expose ``analysis.action_cards`` as the selected list
    (optionally capped at ``limit`` for pathological cases).

    Kept as a thin shim so existing callers (reporters, tests) continue
    to work; ``limit=20`` is a safety bound, not an intentional cap.
    """
    analysis.selected_action_cards = list(analysis.action_cards or [])[:limit]
    return analysis.selected_action_cards


def _build_clustering_config(
    llm_config: LLMConfig, options: PipelineOptions
) -> dict[str, Any] | None:
    if options.skip_llm or not llm_config.is_available:
        return None
    return {
        "model": llm_config.review_model,
        "databricks_host": llm_config.databricks_host,
        "databricks_token": llm_config.databricks_token,
        "lang": options.lang,
    }


def _resolve_token(llm_config: LLMConfig) -> str:
    """Resolve an auth token from LLMConfig or Databricks SDK."""
    if llm_config.databricks_token:
        return llm_config.databricks_token
    try:
        from databricks.sdk.core import Config

        cfg = Config()
        headers = cfg.authenticate()
        return str(headers.get("Authorization", "")).replace("Bearer ", "")
    except Exception as e:
        logger.warning("Failed to resolve auth from SDK: %s", e)
        return ""


def _fetch_warehouse_info(analysis: ProfileAnalysis, llm_config: LLMConfig) -> None:
    if not analysis.endpoint_id or not llm_config.is_available:
        return
    logger.info("Fetching warehouse info for endpoint: %s", analysis.endpoint_id)
    token = _resolve_token(llm_config)
    host = llm_config.databricks_host
    if host and not host.startswith("https://") and not host.startswith("http://"):
        host = f"https://{host}"
    client = WarehouseClient(host=host, token=token)
    analysis.warehouse_info = client.get_warehouse(analysis.endpoint_id)
    if analysis.warehouse_info:
        logger.info(
            "Warehouse: %s (%s)",
            analysis.warehouse_info.name,
            analysis.warehouse_info.size_description,
        )


def _apply_explain(
    analysis: ProfileAnalysis,
    explain_text: str,
    llm_clustering_config: dict[str, Any] | None,
    is_serverless: bool = False,
) -> None:
    try:
        analysis.explain_analysis = parse_explain_extended(explain_text)
        analysis.bottleneck_indicators = enhance_bottleneck_with_explain(
            analysis.bottleneck_indicators,
            analysis.explain_analysis,
        )
        analysis.action_cards = generate_action_cards(
            analysis.bottleneck_indicators,
            analysis.hot_operators,
            analysis.query_metrics,
            analysis.shuffle_metrics,
            analysis.join_info,
            analysis.sql_analysis,
            analysis.top_scanned_tables,
            llm_clustering_config,
            is_serverless=is_serverless,
            explain_analysis=analysis.explain_analysis,
        )
        logger.info("EXPLAIN analysis completed successfully")
    except Exception as e:
        logger.warning("EXPLAIN parsing failed: %s", e)


def _run_llm_stages(
    result: PipelineResult,
    llm_config: LLMConfig,
    options: PipelineOptions,
    tuning_knowledge: str,
    notify: StageCallback,
    is_serverless: bool = False,
    is_streaming: bool = False,
    is_federation: bool = False,
) -> None:
    host = llm_config.databricks_host
    token = llm_config.databricks_token
    lang = options.lang

    # Stage 1: Initial analysis
    notify("llm_initial")
    logger.info("Starting LLM analysis with model: %s", llm_config.primary_model)
    try:
        result.llm_analysis = analyze_with_llm(
            result.analysis,
            llm_config.primary_model,
            host,
            token,
            tuning_knowledge,
            lang=lang,
            is_serverless=is_serverless,
            is_streaming=is_streaming,
            is_federation=is_federation,
        )
    except Exception as e:
        from .llm_client import LLMTimeoutError

        if isinstance(e, LLMTimeoutError):
            result.llm_errors.append(
                f"LLM analysis timed out. Try a faster model (e.g. Sonnet) or a smaller query profile. ({e})"
            )
        else:
            result.llm_errors.append(f"LLM analysis failed: {e}")
        logger.warning("LLM analysis exception: %s", e)
        result.llm_analysis = ""
    if not result.llm_analysis:
        if not result.llm_errors:
            result.llm_errors.append("Initial analysis failed")
        logger.warning("Initial LLM analysis returned empty result")
        return

    # Stage 2: Review
    if not options.skip_review:
        notify("llm_review")
        logger.info("Starting LLM review with model: %s", llm_config.review_model)
        result.review_analysis = review_with_llm(
            result.analysis,
            result.llm_analysis,
            llm_config.primary_model,
            llm_config.review_model,
            host,
            token,
            tuning_knowledge,
            lang=lang,
            is_serverless=is_serverless,
            is_streaming=is_streaming,
        )
        if not result.review_analysis:
            result.llm_errors.append("Review analysis failed")
            logger.warning("LLM review returned empty result")
            return

    # Stage 3: Refine
    if result.review_analysis and not options.skip_refine:
        notify("llm_refine")
        refine_model = llm_config.refine_model
        logger.info("Starting LLM refinement with model: %s", refine_model)
        result.refined_analysis = refine_with_llm(
            result.analysis,
            result.llm_analysis,
            result.review_analysis,
            refine_model,
            host,
            token,
            tuning_knowledge,
            llm_config.primary_model,
            llm_config.review_model,
            lang=lang,
            is_serverless=is_serverless,
            is_streaming=is_streaming,
        )
        if not result.refined_analysis:
            result.llm_errors.append("Refinement failed")
            logger.warning("LLM refinement returned empty result")


def _run_report_review(
    result: PipelineResult,
    llm_config: LLMConfig,
    options: PipelineOptions,
    tuning_knowledge: str,
    notify: StageCallback,
) -> None:
    host = llm_config.databricks_host
    token = llm_config.databricks_token
    lang = options.lang
    alerts = result.analysis.bottleneck_indicators.alerts

    report_review = ""

    # Step 1: Review
    if options.enable_report_review or options.enable_report_refine:
        notify("report_review")
        logger.info("Reviewing report with LLM: %s", llm_config.review_model)
        report_review = review_report_with_llm(
            result.report,
            llm_config.review_model,
            host,
            token,
            tuning_knowledge,
            lang=lang,
            report_context={
                "query_id": result.analysis.query_metrics.query_id,
                "primary_model": llm_config.primary_model,
            },
            alerts=alerts,
        )

    # Step 2: Refine
    if report_review and options.enable_report_refine:
        notify("report_refine")
        refine_model = llm_config.refine_model
        logger.info("Refining report with LLM: %s", refine_model)
        refined_report = refine_report_with_llm(
            result.report,
            report_review,
            refine_model,
            host,
            token,
            tuning_knowledge,
            lang=lang,
            alerts=alerts,
        )
        if refined_report:
            result.report = refined_report

    # Append review for transparency
    if report_review and options.enable_report_review:
        if lang == "ja":
            result.report += "\n\n---\n\n## 🔍 レポートレビュー（LLM）\n\n"
        else:
            result.report += "\n\n---\n\n## 🔍 Report Review (LLM)\n\n"
        result.report += report_review


# ---------------------------------------------------------------------------
# v3: Comparison / Knowledge orchestrators
# ---------------------------------------------------------------------------


def run_analysis_and_persist_pipeline(
    data: dict[str, Any],
    llm_config: LLMConfig,
    options: PipelineOptions,
    writer: Any | None = None,
    analysis_context: AnalysisContext | None = None,
    on_stage: StageCallback | None = None,
) -> PipelineResult:
    """Run the full analysis pipeline and optionally persist results.

    This wraps ``run_analysis_pipeline`` with:
    - Automatic ``AnalysisContext`` attachment
    - Automatic ``query_fingerprint`` generation
    - Optional Delta table persistence via ``TableWriter``

    Args:
        data: Raw query profile JSON (dict).
        llm_config: LLM credentials and model names.
        options: Feature flags controlling which stages run.
        writer: Optional TableWriter instance for persistence.
        analysis_context: Optional context (experiment, variant, tags).
        on_stage: Optional callback invoked when a new stage starts.

    Returns:
        PipelineResult with analysis object, report text, and LLM outputs.
    """
    import json as _json

    from .family import extract_purpose_features, generate_purpose_signature
    from .fingerprint import generate_fingerprint, normalize_sql

    result = run_analysis_pipeline(data, llm_config, options, on_stage=on_stage)

    # Attach context
    if analysis_context:
        result.analysis.analysis_context = analysis_context

    # Record prompt version
    from core.llm_prompts import PROMPT_VERSION

    result.analysis.analysis_context.prompt_version = PROMPT_VERSION

    # Auto-generate fingerprint and family if not already set
    ctx = result.analysis.analysis_context
    sql_text = result.analysis.query_metrics.query_text
    if sql_text:
        if not ctx.query_fingerprint:
            ctx.query_text_normalized = normalize_sql(sql_text)
            ctx.query_fingerprint = generate_fingerprint(sql_text)
        if not ctx.purpose_signature:
            ctx.purpose_signature = generate_purpose_signature(sql_text)
            ctx.query_family_id = ctx.purpose_signature[:16] if ctx.purpose_signature else ""
            features = extract_purpose_features(sql_text)
            if features:
                ctx.feature_json = _json.dumps(features, ensure_ascii=False)

    # Persist
    if writer is not None:
        writer.write(
            result.analysis,
            report=result.report,
            raw_profile_json=_json.dumps(data, ensure_ascii=False, default=str),
            lang=options.lang,
        )

        # Auto-compare with baseline if query_family_id exists and this is NOT a baseline
        if ctx.query_family_id and not ctx.baseline_flag:
            try:
                from services.table_reader import TableReader
                from services.table_writer import TableWriterConfig

                reader_config = TableWriterConfig.from_env()
                reader = TableReader(reader_config)
                baseline = reader.find_baseline(
                    ctx.query_family_id, experiment_id=ctx.experiment_id or None
                )
                if baseline:
                    from .comparison import ComparisonService

                    comparison_req = ComparisonRequest(
                        baseline_analysis_id="baseline",
                        candidate_analysis_id="current",
                        request_source="auto",
                    )
                    result.baseline_comparison = ComparisonService().compare_analyses(
                        baseline, result.analysis, comparison_req
                    )
                    logger.info(
                        "Auto-compared with baseline: regression=%s, severity=%s",
                        result.baseline_comparison.regression_detected,
                        result.baseline_comparison.regression_severity,
                    )
            except Exception as e:
                logger.warning("Baseline auto-comparison failed: %s", e)

    return result


def run_comparison_pipeline(
    baseline: ProfileAnalysis,
    candidate: ProfileAnalysis,
    request: ComparisonRequest,
) -> ComparisonResult:
    """Compare two analyses and return a ComparisonResult."""
    from .comparison import ComparisonService

    return ComparisonService().compare_analyses(baseline, candidate, request)


def run_comparison_and_knowledge_pipeline(
    baseline: ProfileAnalysis,
    candidate: ProfileAnalysis,
    request: ComparisonRequest,
    writer: Any | None = None,
) -> ComparisonResult:
    """Compare two analyses, persist results, and generate a knowledge entry.

    Args:
        baseline: The baseline ProfileAnalysis.
        candidate: The candidate ProfileAnalysis.
        request: Comparison request metadata.
        writer: Optional TableWriter for persistence.

    Returns:
        ComparisonResult with metric diffs and regression detection.
    """
    from .knowledge import KnowledgeService

    comparison = run_comparison_pipeline(baseline, candidate, request)

    if writer is not None:
        writer.write_comparison_result(comparison)
        knowledge = KnowledgeService().build_from_comparison(comparison)
        writer.write_knowledge_document(knowledge)

    return comparison
