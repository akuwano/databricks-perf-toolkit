"""Spark Perf LLM prompt construction.

Builds system and user prompts for app-side Spark Performance report generation.
LLM is called twice:
  Call 1: Sections 1-2 (Executive Summary + Bottleneck Analysis with Recommended Actions)
  Call 2: Sections 3-7 (Photon / Concurrency / Executor / SQL/DataFrame / I/O)
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from services.spark_perf_reader import SparkPerfReader

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fact Pack assembly
# ---------------------------------------------------------------------------


def assemble_spark_perf_fact_pack(reader: SparkPerfReader, app_id: str) -> dict[str, Any]:
    """Collect all Gold table data for a Spark app into a Fact Pack dict."""
    summary = reader.get_application_summary(app_id) or {}
    bottlenecks = reader.get_bottleneck_report(app_id)
    stages = reader.get_stage_performance(app_id)
    executors = reader.get_executor_analysis(app_id)
    jobs = reader.get_job_performance(app_id)
    concurrency = reader.get_job_concurrency(app_id)
    sql_photon = reader.get_sql_photon_analysis(app_id)
    spark_config = reader.get_spark_config_analysis(app_id)
    spot = reader.get_spot_instance_analysis(app_id)
    autoscale = reader.get_autoscale_timeline(app_id)
    io_summary = reader.get_io_summary(app_id)
    bn_summary = reader.get_bottleneck_summary(app_id)
    config_changed = reader.get_spark_config_changed(app_id)
    config_defaults = [c for c in spark_config if c.get("is_changed") == "NO"]
    cross_app = reader.get_cross_app_concurrency(app_id)
    job_detail = reader.get_job_detail(app_id)
    scan_metrics = reader.get_scan_metrics_summary(app_id)
    io_top5 = reader.get_io_top5(app_id)
    duplicate_scans = reader.get_duplicate_scans(app_id)
    sql_plan_top5 = reader.get_sql_plan_top_n(app_id, limit=5)
    concurrency_summary = reader.get_concurrency_summary(app_id)
    executor_summary = reader.get_executor_summary(app_id)
    autoscale_cost = reader.get_autoscale_cost_summary(app_id)
    scaling_event_counts = reader.get_scaling_event_counts(app_id)
    serialization_summary = reader.get_serialization_summary(app_id)
    udf_analysis = reader.get_udf_analysis(app_id)
    high_ser_jobs = reader.get_high_serialization_jobs(app_id)
    skew_analysis = reader.get_skew_analysis(app_id)
    driver_risk = reader.get_driver_risk_analysis(app_id)
    aqe_diagnosis = reader.get_aqe_diagnosis(app_id)
    scaling_quality = reader.get_scaling_quality(app_id)
    memory_analysis = reader.get_memory_analysis(app_id)
    parallelism = reader.get_parallelism_analysis(app_id)
    streaming_deep = reader.get_streaming_deep_analysis(app_id)

    # DBU cost estimate (computed from already-fetched data, no extra queries)
    from core.dbu_pricing import estimate_dbu_cost as _estimate_dbu

    photon_enabled = any(
        c.get("config_key") == "spark.databricks.photon.enabled"
        and str(c.get("actual_value", "")).lower() == "true"
        for c in spark_config
    )
    dbu_estimate = _estimate_dbu(
        worker_node_type=summary.get("worker_node_type", ""),
        driver_node_type=summary.get("driver_node_type", ""),
        duration_min=float(summary.get("duration_min", 0) or 0),
        autoscale_cost=autoscale_cost,
        min_workers=int(summary.get("min_workers", 0) or 0),
        max_workers=int(summary.get("max_workers", 0) or 0),
        photon_enabled=photon_enabled,
        region=summary.get("region", ""),
    )

    worst_stages = sorted(
        [
            s
            for s in stages
            if s.get("bottleneck_type") and s["bottleneck_type"] not in ("OK", "SKIPPED")
        ],
        key=lambda s: float(s.get("duration_ms", 0) or 0),
        reverse=True,
    )[:10]

    normal_stages = sorted(
        [s for s in stages if not s.get("bottleneck_type") or s.get("bottleneck_type") == "OK"],
        key=lambda s: float(s.get("duration_ms", 0) or 0),
        reverse=True,
    )[:5]

    slow_jobs = sorted(jobs, key=lambda j: float(j.get("duration_ms", 0) or 0), reverse=True)[:20]

    photon_pcts = [float(s.get("photon_pct", 0) or 0) for s in sql_photon]
    avg_photon = sum(photon_pcts) / max(len(photon_pcts), 1) if photon_pcts else 0
    low_photon = [s for s in sql_photon if float(s.get("photon_pct", 0) or 0) < 50]
    concurrent_jobs = [c for c in concurrency if int(c.get("concurrent_jobs_at_start", 0) or 0) > 0]

    # Photon config from spark_config (not just from environment)
    photon_config = {}
    for c in spark_config:
        key = c.get("config_key", "")
        if "photon" in key.lower():
            photon_config[key] = c.get("actual_value", "")

    pack: dict[str, Any] = {
        "app_summary": summary,
        "io_summary": io_summary,
        "bottlenecks": bottlenecks,
        "bottleneck_summary": bn_summary,
        "worst_stages": worst_stages,
        "normal_stages": normal_stages,
        "stages": stages,
        "executors": executors,
        "slow_jobs": slow_jobs,
        "jobs": jobs,
        "job_detail": job_detail,
        "concurrency": concurrency,
        "concurrent_jobs": concurrent_jobs,
        "cross_app_concurrency": cross_app,
        "sql_photon": sql_photon,
        "photon_summary": {
            "avg_photon_pct": round(avg_photon, 1),
            "total_sql_count": len(sql_photon),
            "low_photon_count": len(low_photon),
            "photon_config": photon_config,
        },
        "low_photon_sqls": sorted(
            low_photon, key=lambda s: float(s.get("duration_sec", 0) or 0), reverse=True
        )[:5],
        "spark_config_changed": config_changed,
        "spark_config_defaults": config_defaults,
        "spot": spot,
        "autoscale": autoscale,
        "scan_metrics": scan_metrics,
        "io_top5": io_top5,
        "duplicate_scans": duplicate_scans,
        "sql_plan_top5": sql_plan_top5,
        "concurrency_summary": concurrency_summary,
        "executor_summary": executor_summary,
        "autoscale_cost": autoscale_cost,
        "scaling_event_counts": scaling_event_counts,
        "dbu_estimate": dbu_estimate,
        "serialization_summary": serialization_summary,
        "udf_analysis": udf_analysis,
        "high_ser_jobs": high_ser_jobs,
        "skew_analysis": skew_analysis,
        "driver_risk": driver_risk,
        "aqe_diagnosis": aqe_diagnosis,
        "scaling_quality": scaling_quality,
        "memory_analysis": memory_analysis,
        "parallelism": parallelism,
        "streaming_deep": streaming_deep,
        # Streaming (fetch once, reuse in section F generation)
        "streaming_queries": reader.get_streaming_query_summary(app_id),
        "streaming_summary": reader.get_streaming_summary(app_id),
        "streaming_bottlenecks": reader.get_streaming_bottleneck_summary(app_id),
        "streaming_batches": reader.get_streaming_batch_detail(app_id),
        "streaming_idle_events": reader.get_streaming_idle_events(app_id),
    }

    # Pre-compute alerts from already-fetched data (no duplicate queries)
    from core.spark_perf_markdown import compute_streaming_alerts

    pack["streaming_alerts"] = compute_streaming_alerts(
        pack["streaming_batches"],
        pack["streaming_idle_events"],
    )

    # Pre-compute sizing recommendations (no duplicate queries)
    from core.dbu_pricing import generate_sizing_recommendations

    pack["sizing_recommendations"] = generate_sizing_recommendations(
        executor_summary=pack.get("executor_summary", {}),
        app_summary=pack.get("app_summary", {}),
        bottleneck_summary=pack.get("bottleneck_summary", []),
        autoscale_cost=pack.get("autoscale_cost", []),
        scaling_event_counts=pack.get("scaling_event_counts", {}),
        region=summary.get("region", ""),
    )
    return pack


# ---------------------------------------------------------------------------
# System prompts — Call 1 (sections 1-2 + recommended actions)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT_JA = """あなたは Databricks / Apache Spark パフォーマンス分析の専門家です。
提供されたメトリクスデータとナレッジベースをもとに、エンジニアが即座に行動できる具体的な分析テキストを4つのセクションに分けて生成してください。

【最重要ルール — データの忠実性】
- 提供されたデータに存在する情報のみを記載すること。データにない情報を推測・補完してはならない。
- 提供データの全フィールドの値を確認し、値が存在する場合は必ず記載すること。

【フォーマットルール】
- メトリクスの一覧、ステージ一覧、Executor一覧、ボトルネック根拠は **Markdownテーブル（| 区切り）** で出力すること
- 箇条書き（- や •）は原因分析や改善策の説明文のみに使用し、データの列挙には使わないこと
- テーブルの列数は3〜6列に抑え、横に長くなりすぎないようにすること
- 例: 根拠テーブル

| ステージ | Spill (MB) | Shuffle (MB) | タスク数 | CPU効率 |
|---------|-----------|-------------|---------|--------|
| S126 | 495,511 | 596,032 | 200 | 32.8% |

【見出しレベルのルール】
- # (H1): セクション番号付きタイトル
- ## (H2): セクション内の大項目
- ### (H3): セクション内の小項目

【分析指針】
- 各ボトルネックについて「症状 → 原因 → 改善策」の構造で記述
- 改善策には Spark の設定パラメータ名と推奨値、コード変更例を明示
- Executor 分析では resource_diagnosis の内容を活用
- Databricks では spark.dynamicAllocation.* の設定はプラットフォームが管理するため、推奨に含めないこと

【ストリーミング分析指針】
アプリケーションに Structured Streaming クエリが含まれる場合、セクション1のボトルネック評価テーブルにストリーミングボトルネックも統合すること。
ストリーミングボトルネックタイプ: STREAM_EXCEPTION, STREAM_BACKLOG, STREAM_SLOW_BATCH, STREAM_STATE_GROWTH, STREAM_WATERMARK_DROP, STREAM_PLANNING_OVERHEAD, STREAM_COMMIT_OVERHEAD, STREAM_LOW_THROUGHPUT, STREAM_TRIGGER_LAG, STREAM_DURATION_SPIKE
- STREAM_TRIGGER_LAG: バッチ処理時間が推定トリガー間隔を超過。「ストリーミングアラート」データに検出結果あり
- STREAM_DURATION_SPIKE: バッチ処理時間が平均の3倍を超過するスパイク。「ストリーミングアラート」データに検出結果あり
分析のポイント:
- バッチ処理時間の内訳（addBatch, queryPlanning, commit 等）からボトルネック箇所を特定
- inputRowsPerSecond vs processedRowsPerSecond からバックログの有無を判定
- 状態ストアのメモリ使用量の推移とウォーターマーク設定の適切性を評価
- ソースタイプ（CloudFiles, Kafka等）に応じた最適化策を提示

【Photon 判定指針】
spark.databricks.photon.enabled=true でも Photon が実際に動作しているとは限らない。
- photon_operators > 0 → Photon が実際に動作
- photon_operators == 0 かつ spark.databricks.photon.enabled=true → 設定はあるがランタイムが非対応の可能性あり（クラスタで Photon acceleration が有効でない可能性）
必ず photon_operators の実数値を根拠にすること。設定値だけで「Photon が有効」と断定してはならない。

【レポートタイトルとエグゼクティブサマリー】
レポートの最初に以下の構成で一枚もののエグゼクティブサマリーを配置すること:

# Sparkパフォーマンスレポート

## エグゼクティブサマリー

（2〜3段落の簡潔なナラティブで以下を記述）
- 1段落目: 全体の深刻度判定（問題なし/要改善/重大な問題あり）と主要メトリクス（実行時間、スピル量、Photon利用率、アラート件数）
- 2段落目: 根本原因の要約（何が主因で性能劣化が起きているか）と最も影響の大きいボトルネック
- 3段落目: 最初に着手すべきアクション（Priority最上位の改善策の1行要約）

※テーブルは使わず、テキストのみで記述すること。印刷して1ページに収まる分量にすること。

---

【セクション1. ボトルネック分析サマリーの構成】
# 1. ボトルネック分析サマリー 内に以下のサブセクションを順に配置すること:
1. ジョブ概要
2. I/O・データフロー概要
3. 処理特性
4. **ボトルネック評価** — 以下のテーブル1つに統合すること（「5S ボトルネック評価」と「パフォーマンスアラート」を別々に出さないこと）:

## ボトルネック評価

| # | アラート | 値 | 閾値 | Impact | Effort | Priority |
|---|--------|-----|------|--------|--------|----------|
| 1 | [ボトルネック名] | [現在値] | [閾値] | 🔴 HIGH | 🟢 LOW | 10/10 |
| 2 | [ボトルネック名] | [現在値] | [閾値] | 🔴 HIGH | 🟡 MEDIUM | 9/10 |
| ... | 正常項目も含めて全項目を記載 | ... | ... | ... | ... | ... |

Impactのアイコン: 🔴 HIGH, 🟡 MEDIUM, 🟢 LOW
Effortのアイコン: 🟢 LOW, 🟡 MEDIUM, 🔴 HIGH
※データ量の単位はGB統一（MB→GBに変換して記載すること。例: 495,511 MB → 484.0 GB）

- Impact/Effort/Priorityはセクション2の推奨アクションと同じスコアを使うこと（一貫性を保つ）
- **Priority降順（高い順）にソートすること**（10/10が最上位、正常項目は最下位）
- 正常な項目（ディスクスピル=0、GC正常等）もPriority=-で記載すること
- 詳細はセクション2を参照。

※「5S ボトルネック評価」と「パフォーマンスアラート」を別々のテーブルで出力してはならない。上記1テーブルに統合すること。

【セクション2. 推奨アクション — ボトルネック分析との統合（上限10件）】
セクション2はボトルネック分析と推奨アクションを統合したセクションとする。
**推奨アクションは最大10件まで**とし、Priority上位10件を記載すること。11件以上ある場合はボトルネック評価テーブルを参照と記載。
各ボトルネックについて、以下の形式で「問題の説明 + 具体的な対応策」を一体化して記述すること:

### N. [問題タイトル]
🔴 Impact: HIGH | 🟢 Effort: LOW | Priority: X/10

**根拠**（テーブル形式で出力すること）

| ステージ | 主要メトリクス | 値 | 閾値 |
|---------|------------|-----|------|
| S126 | disk_spill_mb | 495,511 | 0 |

**原因分析:** なぜこの問題が起きているかの因果仮説（1-3文）

**改善策:** 具体的なアクション。設定変更が必要な場合はパラメータ名と値を明示:
```
spark.xxx.yyy=value
```

**検証指標:** 改善後に確認すべきメトリクス

Impact/Effort/Priorityの基準:
- Impact: **実行時間への影響度で判断すること**
  - HIGH=該当ボトルネックのtotal_duration_msがアプリ全体のduration_msの20%以上を占める場合
  - MEDIUM=5%〜20%の場合
  - LOW=5%未満、または修正しても実行時間にほぼ影響しない場合
  - 例: PHOTON_FALLBACKでもUDFの実行時間が全体の1%未満ならLOW
- Effort: LOW=設定変更のみ, MEDIUM=コード変更必要, HIGH=テーブル再設計必要
- Priority: 1-10のスコア（Impact高×Effort低=高スコア）

【出力範囲】
summary_text には**エグゼクティブサマリー + セクション1（ボトルネック分析サマリー）+ セクション2（推奨アクション）**を出力すること。
Appendix（Photon分析、SQL/DataFrame分析、I/O分析、シリアライゼーション分析、Executor分析、並列実行分析）はCall 2で生成するため、LLMでは出力しないこと。

【Priorityの定義】
- 問題がある項目: Priority = 1〜10のスコア（Impact高×Effort低=高スコア）
- 正常な項目（問題なし）: Priority = 「-」（ハイフン。スコアなし）

【出力品質】
- 推奨アクションはPriority降順（高い順）に出力すること（ボトルネック評価テーブルと同じ順序にすること）
- 出力の最後に「※ 推奨アクションは優先度順に記載。Appendixの詳細分析も参照。」と記載すること

{knowledge_section}

必ず以下のJSON形式のみで返答してください（JSON以外のテキストは不要）:
{{
  "summary_text": "<Markdown: エグゼクティブサマリー + セクション1 + セクション2>",
  "job_analysis_text": "",
  "node_analysis_text": "",
  "top3_text": ""
}}"""

_SYSTEM_PROMPT_EN = """You are a Databricks / Apache Spark performance analysis expert.
Based on the provided metrics data and knowledge base, generate specific analysis text in 4 sections that engineers can immediately act upon.

【Critical Rule — Data Fidelity】
- Only include information present in the provided data. Do not infer or fabricate.
- Check all field values and include them when present.

【Format Rules】
- Use **Markdown tables (| delimited)** for metrics, stage lists, executor lists, and bottleneck evidence
- Use bullet lists ONLY for cause analysis and improvement descriptions (prose text), NOT for data enumeration
- Keep tables to 3-6 columns to avoid excessive width
- Example: Evidence table

| Stage | Spill (MB) | Shuffle (MB) | Tasks | CPU Eff |
|-------|-----------|-------------|-------|---------|
| S126 | 495,511 | 596,032 | 200 | 32.8% |

【Heading Levels】
- # (H1): Section number + title
- ## (H2): Major subsection
- ### (H3): Minor subsection

【Analysis Guidelines】
- For each bottleneck: Symptom → Cause → Remediation
- Include specific Spark parameter names and recommended values
- Leverage resource_diagnosis for Executor analysis
- Do NOT recommend spark.dynamicAllocation.* settings (platform-managed on Databricks)

【Streaming Analysis Guidelines】
If the application contains Structured Streaming queries, include streaming bottlenecks in the Section 1 Bottleneck Evaluation table.
Streaming bottleneck types: STREAM_EXCEPTION, STREAM_BACKLOG, STREAM_SLOW_BATCH, STREAM_STATE_GROWTH, STREAM_WATERMARK_DROP, STREAM_PLANNING_OVERHEAD, STREAM_COMMIT_OVERHEAD, STREAM_LOW_THROUGHPUT, STREAM_TRIGGER_LAG, STREAM_DURATION_SPIKE
- STREAM_TRIGGER_LAG: Batch duration exceeds estimated trigger interval. Detection results in "Streaming Alerts" data
- STREAM_DURATION_SPIKE: Batch duration spikes exceeding 3x average. Detection results in "Streaming Alerts" data
Key analysis points:
- Identify bottleneck location from batch duration breakdown (addBatch, queryPlanning, commit, etc.)
- Determine backlog presence by comparing inputRowsPerSecond vs processedRowsPerSecond
- Evaluate state store memory usage trends and watermark configuration adequacy
- Provide optimization strategies appropriate to the source type (CloudFiles, Kafka, etc.)

【Photon Assessment Guidelines】
spark.databricks.photon.enabled=true does NOT guarantee Photon is actually running.
- photon_operators > 0 → Photon is actually executing
- photon_operators == 0 AND spark.databricks.photon.enabled=true → Configuration exists but Photon runtime may not be enabled on the cluster (Photon acceleration checkbox may be unchecked)
Always base Photon assessment on actual photon_operators count. Never conclude "Photon is active" from the config setting alone.

【Report Title and Executive Summary】
Place a one-page executive summary at the very beginning of the report:

# Spark Performance Report

## Executive Summary

(2-3 concise narrative paragraphs covering:)
- Paragraph 1: Overall severity assessment (no issues / needs improvement / critical issues) with key metrics (execution time, spill, Photon utilization, alert count)
- Paragraph 2: Root cause summary (what is the primary cause of performance degradation) and the highest-impact bottleneck
- Paragraph 3: First action to take (1-line summary of the highest-Priority improvement)

Use text only, no tables. Keep it short enough to fit on one printed page.

---

【Section 1. Bottleneck Analysis Summary Structure】
# 1. Bottleneck Analysis Summary must contain these subsections in order:
1. Job Overview
2. I/O & Data Flow Overview
3. Processing Characteristics
4. **Bottleneck Evaluation** — merge into ONE table (do NOT output "5S Bottleneck Evaluation" and "Performance Alerts" as separate tables):

## Bottleneck Evaluation

| # | Alert | Value | Threshold | Impact | Effort | Priority |
|---|-------|-------|-----------|--------|--------|----------|
| 1 | [Bottleneck name] | [Current value] | [Threshold] | 🔴 HIGH | 🟢 LOW | 10/10 |
| 2 | [Bottleneck name] | [Current value] | [Threshold] | 🔴 HIGH | 🟡 MEDIUM | 9/10 |
| ... | Include all items including healthy ones | ... | ... | ... | ... | ... |

Impact icons: 🔴 HIGH, 🟡 MEDIUM, 🟢 LOW
Effort icons: 🟢 LOW, 🟡 MEDIUM, 🔴 HIGH
All data sizes must use GB (convert MB to GB, e.g. 495,511 MB → 484.0 GB)

- Impact/Effort/Priority MUST use the same scores as Section 2 recommended actions (consistency)
- **Sort by Priority descending** (10/10 at top, healthy items at bottom)
- Include healthy items (disk spill=0, GC normal, etc.) with Priority=-
- See Section 2 for details.

Do NOT output "5S Bottleneck Evaluation" and "Performance Alerts" as separate tables. Merge into the single table above.

【Section 2. Recommended Actions — Merged with Bottleneck Analysis (max 10 items)】
Section 2 unifies bottleneck analysis and recommended actions.
**List at most 10 recommended actions**, prioritized by Priority score. If more than 10 exist, refer to Bottleneck Evaluation table.
For each bottleneck, describe the problem and specific remediation together:

### N. [Problem Title]
🔴 Impact: HIGH | 🟢 Effort: LOW | Priority: X/10

**Rationale** (output as table)

| Stage | Key Metric | Value | Threshold |
|-------|-----------|-------|-----------|
| S126 | disk_spill_mb | 495,511 | 0 |

**Cause Analysis:** Causal hypothesis for why this problem occurs (1-3 sentences)

**Improvement:** Specific action. Include parameter names and values:
```
spark.xxx.yyy=value
```

**Verification Metric:** Metrics to check after fix

Impact/Effort/Priority criteria:
- Impact: **Must be based on actual execution time impact**
  - HIGH=bottleneck's total_duration_ms is ≥20% of app's total duration_ms
  - MEDIUM=5%–20% of total duration
  - LOW=<5%, or fixing it would have negligible effect on execution time
  - Example: PHOTON_FALLBACK with UDF execution <1% of total time → LOW
- Effort: LOW=config change only, MEDIUM=code change needed, HIGH=table redesign
- Priority: 1-10 score (high Impact × low Effort = high score)

【Output Scope】
summary_text must contain **Executive Summary + Section 1 (Bottleneck Analysis Summary) + Section 2 (Recommended Actions)**.
Appendix (Photon, SQL/DataFrame, I/O, Serialization, Executor, Concurrency) is generated by Call 2 — do NOT output them.

【Priority Definition】
- Items with issues: Priority = 1-10 score (high Impact × low Effort = high score)
- Healthy items (no issues): Priority = "-" (hyphen, no score)

【Output Quality】
- Recommended Actions MUST be sorted by Priority descending (same order as the Bottleneck Evaluation table)
- End the output with: "※ Recommended Actions listed in priority order. See Appendix for detailed analysis."

{knowledge_section}

Respond ONLY in the following JSON format (no other text):
{{
  "summary_text": "<Markdown: Executive Summary + Section 1 + Section 2>",
  "job_analysis_text": "",
  "node_analysis_text": "",
  "top3_text": ""
}}"""


def create_spark_perf_system_prompt(knowledge: str, lang: str = "ja") -> str:
    """Build system prompt for LLM call 1 (sections 1-2 + actions)."""
    knowledge_section = ""
    if knowledge:
        knowledge_section = (
            "以下はパフォーマンス最適化ナレッジベースです。分析・推奨に活用してください:\n\n"
            if lang == "ja"
            else "Below is the performance optimization knowledge base. Use it for analysis and recommendations:\n\n"
        ) + knowledge

    template = _SYSTEM_PROMPT_JA if lang == "ja" else _SYSTEM_PROMPT_EN
    return template.format(knowledge_section=knowledge_section)


# ---------------------------------------------------------------------------
# System prompts — Call 2 (sections 3-7)
# ---------------------------------------------------------------------------

_SYSTEM_BASE_JA = """あなたは Databricks / Apache Spark パフォーマンス分析の専門家です。
セクション1（ボトルネック分析サマリー）とセクション2（推奨アクション）は別途生成済みです。
あなたの役割は**Appendix（詳細分析: A〜F、ただしDはシステム自動生成）のみ**を生成することです。
セクション1, 2や推奨アクション、ボトルネック分析サマリーは絶対に出力しないでください。

【最重要ルール — データの忠実性】
- 提供されたデータに存在する情報のみを記載すること。データにない情報を推測・補完してはならない。
- 提供データの全フィールドの値を確認し、値が存在する場合は必ず記載すること。

【フォーマットルール】
- メトリクスの一覧やステージ一覧は **Markdownテーブル（| 区切り）** で出力すること
- 箇条書き（- や •）は分析コメントの説明文のみに使用し、データの列挙には使わないこと
- テーブルの列数は3〜8列に抑えること

【見出しレベルのルール】
- # (H1): セクション番号付きタイトル（# 3. 〜 # 7. のみ）
- ## (H2): セクション内の大項目
- ### (H3): セクション内の小項目

【出力範囲の厳守】
- Appendix（A, B, C, E, F）のみを出力すること（Section Dはシステムが自動生成）
- セクション1, 2 や「ボトルネック分析サマリー」「推奨アクション」「スロージョブ分析」は出力禁止
- Section D（シリアライゼーション分析）はシステムが自動生成するため出力しないこと

{knowledge_section}"""

_SYSTEM_BASE_EN = """You are a Databricks / Apache Spark performance analysis expert.
Sections 1 (Bottleneck Analysis Summary) and 2 (Recommended Actions) have already been generated.
Your role is to generate **only the Appendix (detailed analysis: A-C, E-F)**. Section D is auto-generated by the system.
Do NOT output Section 1, 2, D, or any recommended actions / bottleneck analysis summary.

【Critical Rule — Data Fidelity】
- Only include information present in the provided data. Do not infer or fabricate.
- Check all field values and include them when present.

【Format Rules】
- Use **Markdown tables (| delimited)** for metrics, stage lists, executor lists
- Use bullet lists ONLY for analysis comments and descriptions
- Keep tables to 3-8 columns

【Heading Levels】
- # (H1): Section number + title (# 3. through # 7. only)
- ## (H2): Major subsection
- ### (H3): Minor subsection

【Output Scope — Strict】
- Output ONLY Appendix (A, B, C, E, F) — Section D is auto-generated by the system
- Do NOT output Section 1, 2, D, or any "Bottleneck Analysis Summary", "Recommended Actions", "Slow Job Analysis"

{knowledge_section}"""


def _build_call2_system_prompt(knowledge: str, lang: str) -> str:
    """Build base system prompt for Call 2 with knowledge injection."""
    knowledge_section = ""
    if knowledge:
        label = (
            "以下はパフォーマンス最適化ナレッジベースです:\n\n"
            if lang == "ja"
            else "Performance optimization knowledge base:\n\n"
        )
        knowledge_section = label + knowledge
    template = _SYSTEM_BASE_JA if lang == "ja" else _SYSTEM_BASE_EN
    return template.format(knowledge_section=knowledge_section)


def create_spark_perf_sections_system_prompt(knowledge: str, lang: str = "ja") -> str:
    """Build system prompt for LLM call 2 (sections 3-7)."""
    return _build_call2_system_prompt(knowledge, lang)


# ---------------------------------------------------------------------------
# User prompts — shared helpers
# ---------------------------------------------------------------------------


def _json_section(title: str, data: Any, max_items: int = 0) -> str:
    if not data:
        return ""
    if max_items and isinstance(data, list):
        data = data[:max_items]
    return f"=== {title} ===\n{json.dumps(data, ensure_ascii=False, indent=2, default=str)}\n"


def _build_call2_data_sections(fact_pack: dict[str, Any]) -> str:
    """Build data sections for Call 2 (sections 3-7 only).

    Only includes data relevant to Photon, Concurrency, Executor, SQL/DataFrame, I/O.
    Excludes bottleneck/stage/job data that belongs to sections 1-2.
    """
    sections = []
    # App summary (needed for plan optimization time calculation)
    sections.append(_json_section("Application Summary", fact_pack.get("app_summary")))
    # I/O (section 7)
    sections.append(_json_section("I/O Summary", fact_pack.get("io_summary")))
    # Concurrency (section 4) — summary only, no per-job detail
    sections.append(_json_section("Concurrency Summary", fact_pack.get("concurrency_summary")))
    # Photon (section 3)
    sections.append(_json_section("Photon Summary", fact_pack.get("photon_summary")))
    if fact_pack.get("low_photon_sqls"):
        sections.append(
            _json_section("Low Photon SQLs (Top 5)", fact_pack.get("low_photon_sqls"), max_items=5)
        )
    # SQL Plan Analysis (Section E) — top 5 heaviest SQLs with join/operator breakdown
    if fact_pack.get("sql_plan_top5"):
        sections.append(
            _json_section(
                "SQL Plan Analysis (Top 5 by Duration)",
                fact_pack.get("sql_plan_top5"),
                max_items=10,
            )
        )
    # Executor (section 5) — summary only, no per-executor detail
    sections.append(_json_section("Executor Diagnosis Summary", fact_pack.get("executor_summary")))
    # Autoscale + Spot/Node events (section 5) — statistical summary
    autoscale_cost = fact_pack.get("autoscale_cost", [])
    if autoscale_cost:
        import statistics

        worker_counts = [int(e.get("worker_count", 0)) for e in autoscale_cost]
        total_min = sum(float(e.get("cumulative_min", 0)) for e in autoscale_cost)
        sorted_wc = sorted(worker_counts)

        # Worker Count Statistics (Min/Median/Avg/P95/Max)
        p95_idx = max(0, int(len(sorted_wc) * 0.95) - 1)
        wc_stats = {
            "min": sorted_wc[0] if sorted_wc else 0,
            "median": round(statistics.median(sorted_wc), 0) if sorted_wc else 0,
            "avg": round(statistics.mean(sorted_wc), 1) if sorted_wc else 0,
            "p95": sorted_wc[p95_idx] if sorted_wc else 0,
            "max": sorted_wc[-1] if sorted_wc else 0,
            "total_time_min": round(total_min, 1),
            "scaling_steps": len(autoscale_cost),
        }

        # Cost-Weighted Distribution: group into ranges
        max_wc = sorted_wc[-1] if sorted_wc else 1
        if max_wc <= 10:
            boundaries = [0, 5, 10]
        elif max_wc <= 50:
            boundaries = [0, 10, 25, 50]
        elif max_wc <= 100:
            boundaries = [0, 25, 50, 100]
        else:
            step = max_wc // 3
            boundaries = [0, step, step * 2, max_wc]
        # Ensure max is included
        if boundaries[-1] < max_wc:
            boundaries.append(max_wc)

        ranges = []
        for i in range(len(boundaries) - 1):
            lo, hi = boundaries[i], boundaries[i + 1]
            range_entries = [
                e
                for e in autoscale_cost
                if lo < int(e.get("worker_count", 0)) <= hi
                or (i == 0 and int(e.get("worker_count", 0)) == 0)
            ]
            range_min = sum(float(e.get("cumulative_min", 0)) for e in range_entries)
            range_pct = (range_min / total_min * 100) if total_min > 0 else 0
            if range_entries:
                label = f"{lo + 1}-{hi}" if lo > 0 else f"0-{hi}"
                ranges.append(
                    {
                        "range": label,
                        "time_min": round(range_min, 1),
                        "pct_of_total": round(range_pct, 1),
                    }
                )

        summary = {
            "worker_count_statistics": wc_stats,
            "cost_weighted_distribution": ranges,
        }
        sections.append(_json_section("Worker Count Distribution (Statistical Summary)", summary))
    if fact_pack.get("scaling_event_counts"):
        sections.append(
            _json_section("Scaling / Removal Event Counts", fact_pack.get("scaling_event_counts"))
        )
    # Spark config (sections 3, 5)
    sections.append(
        _json_section("Spark Config (Changed)", fact_pack.get("spark_config_changed"), max_items=30)
    )
    # I/O scan analysis (section 7)
    sections.append(_json_section("Scan Metrics Summary", fact_pack.get("scan_metrics")))
    sections.append(_json_section("I/O Scan Volume TOP 5", fact_pack.get("io_top5"), max_items=5))
    if fact_pack.get("duplicate_scans"):
        sections.append(
            _json_section("Duplicate Scans", fact_pack.get("duplicate_scans"), max_items=5)
        )
    # Streaming data is NOT included in Call 2 — section F is generated
    # separately by spark_perf_markdown.py to avoid LLM misplacement.
    return "".join(s for s in sections if s)


# ---------------------------------------------------------------------------
# User prompt — Call 1 (sections 1-2)
# ---------------------------------------------------------------------------


def create_spark_perf_analysis_prompt(fact_pack: dict[str, Any], lang: str = "ja") -> str:
    """Build user prompt for LLM call 1: sections 1-2 + recommended actions.

    Assembles all Gold table data into labeled JSON sections,
    matching the notebook 02 prompt structure.
    """
    header = (
        "以下はDatabricks Sparkジョブのパフォーマンス分析結果です。\n"
        "このデータをもとに分析レポートを生成してください。\n\n"
        if lang == "ja"
        else "Below are the Databricks Spark job performance analysis results.\n"
        "Generate an analysis report based on this data.\n\n"
    )

    sections = [header]

    # --- Data needed for Executive Summary + Section 1 + Section 2 only ---
    # App overview
    sections.append(
        _json_section(
            "アプリケーション サマリー" if lang == "ja" else "Application Summary",
            fact_pack.get("app_summary"),
        )
    )
    sections.append(
        _json_section(
            "I/O サマリー" if lang == "ja" else "I/O Summary",
            fact_pack.get("io_summary"),
        )
    )
    # Bottleneck data (core of sections 1-2)
    sections.append(
        _json_section(
            "ボトルネック件数サマリー" if lang == "ja" else "Bottleneck Summary",
            fact_pack.get("bottleneck_summary"),
        )
    )
    sections.append(
        _json_section(
            "ボトルネック検出ステージ" if lang == "ja" else "Bottleneck Stages",
            fact_pack.get("worst_stages"),
            max_items=10,
        )
    )
    sections.append(
        _json_section(
            "正常ステージ（上位5件）" if lang == "ja" else "Normal Stages (Top 5)",
            fact_pack.get("normal_stages"),
            max_items=5,
        )
    )
    # Jobs — top 10 only (sufficient for section 2 evidence)
    sections.append(
        _json_section(
            "ジョブ詳細（実行時間順 TOP10）"
            if lang == "ja"
            else "Job Details (Top 10 by Duration)",
            fact_pack.get("slow_jobs"),
            max_items=10,
        )
    )
    # Summaries only (no per-record detail)
    sections.append(
        _json_section(
            "Photon 利用率サマリー" if lang == "ja" else "Photon Utilization Summary",
            fact_pack.get("photon_summary"),
        )
    )
    sections.append(
        _json_section(
            "並列実行サマリー" if lang == "ja" else "Concurrency Summary",
            fact_pack.get("concurrency_summary"),
        )
    )
    sections.append(
        _json_section(
            "Executor 診断サマリー" if lang == "ja" else "Executor Diagnosis Summary",
            fact_pack.get("executor_summary"),
        )
    )
    # Spark config (changed only — defaults not needed for sections 1-2)
    sections.append(
        _json_section(
            "Spark 設定分析（変更あり）"
            if lang == "ja"
            else "Spark Config (Changed from Defaults)",
            fact_pack.get("spark_config_changed"),
            max_items=30,
        )
    )

    # DBU cost estimate
    if (
        fact_pack.get("dbu_estimate")
        and fact_pack["dbu_estimate"].get("estimated_total_dbu", 0) > 0
    ):
        sections.append(
            _json_section(
                "DBU コスト推定" if lang == "ja" else "DBU Cost Estimate",
                fact_pack["dbu_estimate"],
            )
        )

    # Cluster sizing recommendations
    sizing_recs = fact_pack.get("sizing_recommendations", [])
    if sizing_recs:
        from dataclasses import asdict

        sections.append(
            _json_section(
                "クラスタサイジング推奨" if lang == "ja" else "Cluster Sizing Recommendations",
                [asdict(r) for r in sizing_recs],
            )
        )

    # Serialization analysis
    ser_summary = fact_pack.get("serialization_summary", {})
    if ser_summary and float(ser_summary.get("total_serialization_ms", 0) or 0) > 0:
        sections.append(
            _json_section(
                "シリアライゼーション分析" if lang == "ja" else "Serialization Analysis",
                ser_summary,
            )
        )
    if fact_pack.get("udf_analysis"):
        sections.append(
            _json_section(
                "UDF検出" if lang == "ja" else "UDF Detection",
                fact_pack["udf_analysis"],
                max_items=10,
            )
        )

    # Skew analysis
    skew = fact_pack.get("skew_analysis", {})
    if skew and skew.get("skew_stages"):
        sections.append(
            _json_section(
                "データスキュー分析" if lang == "ja" else "Data Skew Analysis",
                skew,
            )
        )

    # Driver risk analysis
    dr = fact_pack.get("driver_risk", {})
    if dr and (
        dr.get("oom_stages") or dr.get("large_result_stages") or dr.get("collect_operators")
    ):
        sections.append(
            _json_section(
                "ドライバーリスク分析" if lang == "ja" else "Driver Risk Analysis",
                dr,
            )
        )

    # AQE diagnosis
    aqe = fact_pack.get("aqe_diagnosis", {})
    if aqe and aqe.get("aqe_configs"):
        sections.append(
            _json_section(
                "AQE活用状況診断" if lang == "ja" else "AQE Utilization Diagnosis",
                aqe,
            )
        )

    # Scaling quality (dynamic allocation diagnosis)
    sq = fact_pack.get("scaling_quality", {})
    if sq and (
        sq.get("recommendations") or sq.get("overprovisioned") or sq.get("underprovisioned")
    ):
        sections.append(
            _json_section(
                "動的アロケーション品質" if lang == "ja" else "Dynamic Allocation Quality",
                sq,
            )
        )

    # Memory analysis
    mem = fact_pack.get("memory_analysis", {})
    if mem and mem.get("total_mb"):
        sections.append(
            _json_section(
                "メモリ構成分析" if lang == "ja" else "Memory Configuration Analysis",
                mem,
            )
        )

    # Parallelism analysis
    par = fact_pack.get("parallelism", {})
    if par and par.get("issues"):
        sections.append(
            _json_section(
                "並列度分析" if lang == "ja" else "Parallelism Analysis",
                par,
            )
        )

    # Streaming data (conditional — only if streaming queries exist)
    streaming_summary = fact_pack.get("streaming_summary")
    if streaming_summary and streaming_summary.get("query_count", 0) > 0:
        sections.append(
            _json_section(
                "ストリーミングサマリー" if lang == "ja" else "Streaming Summary",
                streaming_summary,
            )
        )
        sections.append(
            _json_section(
                "ストリーミングボトルネック" if lang == "ja" else "Streaming Bottlenecks",
                fact_pack.get("streaming_bottlenecks"),
            )
        )
        sections.append(
            _json_section(
                "ストリーミングクエリ（上位10件）"
                if lang == "ja"
                else "Streaming Queries (Top 10)",
                fact_pack.get("streaming_queries"),
                max_items=10,
            )
        )
        streaming_alerts = fact_pack.get("streaming_alerts")
        if streaming_alerts:
            sections.append(
                _json_section(
                    "ストリーミングアラート（トリガーラグ・スパイク検出）"
                    if lang == "ja"
                    else "Streaming Alerts (Trigger Lag & Spike Detection)",
                    streaming_alerts,
                )
            )

    # Streaming deep analysis (State growth, Watermark) — for bottleneck evaluation
    streaming_deep = fact_pack.get("streaming_deep", {})
    if streaming_deep:
        sections.append(
            _json_section(
                "ストリーミングState/Watermark分析"
                if lang == "ja"
                else "Streaming State/Watermark Analysis",
                streaming_deep,
            )
        )

    result = "".join(s for s in sections if s)

    from ..llm_client import MAX_PROMPT_CHARS

    if len(result) > MAX_PROMPT_CHARS:
        logger.warning(
            "Call 1 prompt %d chars exceeds budget %d, truncating",
            len(result),
            MAX_PROMPT_CHARS,
        )
        result = result[:MAX_PROMPT_CHARS] + "\n\n<!-- prompt truncated -->"
    return result


# ---------------------------------------------------------------------------
# User prompt — Call 2 (sections 3-7)
# ---------------------------------------------------------------------------


def create_spark_perf_sections_prompt(fact_pack: dict[str, Any], lang: str = "ja") -> str:
    """Build user prompt for LLM call 2: sections 3-7."""
    data = _build_call2_data_sections(fact_pack)

    if lang == "ja":
        instructions = """以下のデータをもとに、Appendix（詳細分析）のMarkdownのみを生成してください。
出力はMarkdownのみ（JSON不要）。各セクションの分析コメントを必ず含めること。
**セクション1, 2や「ボトルネック分析サマリー」「推奨アクション」「スロージョブ分析」は絶対に出力しないこと。**

# Appendix: 詳細分析

## A. Photon 利用状況分析
（Photon設定確認（photon_configのspark.databricks.photon.enabledの値を確認し、true/falseを明記）→サマリー→低Photon SQL上位5件テーブル→書き換え方法→分析コメント）
※Photonの有効/無効はDBRバージョンやノードタイプではなく、photon_configの設定値で判断すること。

## B. SQL/DataFrame 分析
※SQL実行一覧テーブル（execution_id, Duration, DataSourceInfo, Type, Operators, Joins）はシステムが自動生成するため、LLMでは出力しないこと。
LLMは以下の分析コメントのみ出力すること:
（各SQLの物理プランから推定されるボトルネック（SortMergeJoin、フィルタ未適用等）を分析→ジョブ間の実行間隔が長い箇所がある場合は上位5件を特定し、プラン最適化オーバーヘッドの可能性を評価→改善策の提示）
※Photon利用率・非Photonオペレータはセクション A で分析済みのため、ここでは省略すること。
※重複スキャン分析はセクション C. I/O 分析に記載するため、ここでは省略すること。
※詳細なクエリプランの確認は Spark UI の SQL タブを参照。ここではメトリクスから推定可能な範囲でボトルネックを指摘すること。

## C. I/O 分析
（サマリー（キャッシュヒット率/プルーニング率/クラウドI/O）→スキャン量TOP5テーブル（テーブル名/フォーマット/カラム数/読み取りサイズ/ファイル数/プルーニング率/キャッシュヒット率）→重複スキャンテーブル（テーブル名/スキャン回数/合計時間/推奨）→分析コメント）

※Section D（シリアライゼーション分析）はシステムが自動生成するため出力しないこと。

## E. Executor リソース分析
（**診断結果を最初にテキストで記載**（例: 「✅ リソース問題なし」「⚠ MEMORY_PRESSURE検出: 10/12 Executor」等）→メモリ設定→診断詳細テーブル（CPU効率/GC/Spill/Serialization/Straggler/Underutilized）→ノードタイプ適正分析→ワーカー数統計サマリー（Min/Median/Avg/P95/Max + レンジ別コスト配分）→イベント種別カウント（Scale Out/AUTOSCALE_IN/SPOT_PREEMPTION/NODE_LOST等の発生回数）→分析コメント）
※ワーカー数は統計サマリー形式で出力すること（個別ワーカー数ごとのレコードは出力しない）:
  - Worker Count Statistics テーブル: Min, Median, Avg, P95, Max, Total Time, Scaling Steps
  - Cost-Weighted Distribution テーブル: レンジ別の稼働時間と割合
※診断結果（正常/異常の判定）はテーブルの前にテキストで記載すること。テーブルの最下行ではなく冒頭に置く。
※全Executorの個別リストは出力しないこと。診断サマリーのみ記載。
※時系列の全イベントリストは出力しないこと。種別ごとの発生回数のみ記載。

## F. 並列実行影響分析
（要約統計値のみ: 最大同時実行数/並列実行ジョブ数/クロスアプリ検出数/CPU効率比較(並列vs単独)→並列実行起因のパフォーマンス劣化兆候の有無を判定→分析コメント）
※個別ジョブのレコード一覧は出力しないこと。要約統計と劣化判定のみ記載。

出力は「# Appendix: 詳細分析」で始まり ## F. で終わること。Section D は出力しないこと。"""
    else:
        instructions = """Based on the data below, generate Markdown for the Appendix (detailed analysis) ONLY.
Output Markdown only (no JSON). Include analysis comments for each section.
**Do NOT output Section 1, 2 or any "Bottleneck Analysis Summary", "Recommended Actions", "Slow Job Analysis".**

# Appendix: Detailed Analysis

## A. Photon Utilization Analysis
(Config check (check spark.databricks.photon.enabled value from photon_config, state true/false explicitly) → Summary → Low Photon SQL Top 5 table → rewrite methods → analysis)
Determine Photon enabled/disabled from photon_config setting, NOT from DBR version or node type.

## B. SQL/DataFrame Analysis
Note: The SQL execution list table (execution_id, Duration, DataSourceInfo, Type, Operators, Joins) is auto-generated by the system — do NOT output it.
LLM should output ONLY the analysis commentary below:
(Analyze estimated bottlenecks from each SQL's physical plan (SortMergeJoin, missing filters, etc.) → If long inter-job gaps exist, identify top 5 and assess plan optimization overhead → Suggest improvements)
Note: Photon utilization and non-Photon operators are already covered in Section A — do NOT repeat here.
Note: Duplicate scan analysis belongs in Section C (I/O Analysis) — do NOT include it here.
Note: For detailed query plan inspection, refer to the Spark UI SQL tab. Here, identify bottlenecks to the extent estimable from metrics.

## C. I/O Analysis
(Summary (cache hit rate/pruning rate/cloud I/O) → Scan Volume TOP 5 table (table name/format/columns/read size/files/pruning %/cache hit %) → Duplicate Scans table (table/scan count/total duration/recommendation) → analysis)

Section D (Serialization Analysis) is auto-generated by the system — do NOT output Section D.

## E. Executor Resource Analysis
(**Diagnosis result as text first** (e.g., "✅ No resource issues" or "⚠ MEMORY_PRESSURE detected: 10/12 Executors") → Memory config → Diagnosis detail table (CPU efficiency/GC/Spill/Serialization/Straggler/Underutilized) → Node type suitability → Worker count statistical summary (Min/Median/Avg/P95/Max + cost-weighted range distribution) → Event type counts (Scale Out/AUTOSCALE_IN/SPOT_PREEMPTION/NODE_LOST counts) → analysis)
Worker count MUST be output as a statistical summary (do NOT list individual worker count records):
  - Worker Count Statistics table: Min, Median, Avg, P95, Max, Total Time, Scaling Steps
  - Cost-Weighted Distribution table: time and percentage by worker count range
Diagnosis result (normal/abnormal verdict) MUST be placed as text BEFORE the table, not as a row at the bottom.
Do NOT output per-executor detail tables. Only diagnosis summary.
Do NOT output chronological event lists. Only event type counts.

## F. Concurrent Execution Impact Analysis
(Summary stats only: max concurrent jobs / jobs with concurrency / cross-app detected / CPU efficiency comparison (concurrent vs solo) → determine if concurrency-induced performance degradation exists → analysis)
Do NOT output per-job record tables. Only summary stats and degradation assessment.

Output must start with "# Appendix: Detailed Analysis" and end with ## F. Do not output Section D."""

    result = f"{data}\n\n{instructions}"

    from ..llm_client import MAX_PROMPT_CHARS

    if len(result) > MAX_PROMPT_CHARS:
        logger.warning("Call 2 prompt %d chars exceeds budget %d", len(result), MAX_PROMPT_CHARS)
        result = result[:MAX_PROMPT_CHARS] + "\n\n<!-- truncated -->"
    return result


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


def _extract_json_from_text(text: str) -> dict | None:
    """Extract JSON object from text by brace-depth counting.

    Handles cases where LLM wraps JSON in markdown fences or
    appends extra text after the closing brace.
    """
    stripped = text.strip()

    # Strip ```json ... ``` wrapper
    if stripped.startswith("```"):
        lines = stripped.split("\n")
        if lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()

    # Quick check: does this look like JSON with summary_text?
    start = stripped.find("{")
    if start < 0:
        return None

    # Find matching closing brace by counting depth
    depth = 0
    in_string = False
    escape_next = False
    for i in range(start, len(stripped)):
        ch = stripped[i]
        if escape_next:
            escape_next = False
            continue
        if ch == "\\" and in_string:
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    parsed: dict[Any, Any] = json.loads(stripped[start : i + 1])
                    return parsed
                except json.JSONDecodeError:
                    return None
    return None


def parse_spark_perf_response(response_text: str) -> dict[str, str]:
    """Parse LLM JSON response into narrative dict.

    Uses brace-depth counting to reliably extract JSON even when
    the LLM wraps it in markdown fences or appends extra text.
    Always returns clean Markdown in summary_text, never raw JSON.
    """
    # Try standard JSON parse first
    text = response_text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].strip().startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    try:
        data = json.loads(text)
        return {
            "summary_text": data.get("summary_text", ""),
            "job_analysis_text": data.get("job_analysis_text", ""),
            "node_analysis_text": data.get("node_analysis_text", ""),
            "top3_text": data.get("top3_text", ""),
        }
    except json.JSONDecodeError:
        pass

    # Fallback: brace-depth extraction for malformed responses
    data = _extract_json_from_text(response_text)
    if data and data.get("summary_text"):
        logger.info("Extracted JSON via brace-depth parsing")
        return {
            "summary_text": data.get("summary_text", ""),
            "job_analysis_text": data.get("job_analysis_text", ""),
            "node_analysis_text": data.get("node_analysis_text", ""),
            "top3_text": data.get("top3_text", ""),
        }

    logger.warning("Failed to parse LLM response as JSON, using as summary_text")
    return {
        "summary_text": response_text,
        "job_analysis_text": "",
        "node_analysis_text": "",
        "top3_text": "",
    }
