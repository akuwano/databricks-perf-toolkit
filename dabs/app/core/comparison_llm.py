"""LLM-powered comparison analysis generation.

Takes a ComparisonResult and generates a detailed natural language analysis
explaining what changed, root causes, implications, and concrete next actions.

v4.11: Evidence-constrained prompts with structured fact pack,
       observation/speculation separation, confidence criteria,
       and counter-evidence requirements.
"""

from __future__ import annotations

import logging

from .comparison import (
    _IO_DEPENDENT_METRICS,
    _METRIC_WEIGHTS,
    _NOISE_FLOOR,
    COMPARABLE_METRICS,
)
from .llm_client import call_llm_with_retry, create_openai_client
from .models import ComparisonResult, MetricDiff

logger = logging.getLogger(__name__)

# Allowed causal links: (cause_metric, effect_metric) pairs
# LLM should only propose causality along these links.
_CAUSAL_LINKS: list[tuple[str, str]] = [
    ("read_bytes", "total_time_ms"),
    ("read_bytes", "execution_time_ms"),
    ("read_remote_bytes", "total_time_ms"),
    ("spill_to_disk_bytes", "total_time_ms"),
    ("spill_to_disk_bytes", "execution_time_ms"),
    ("spill_bytes", "total_time_ms"),
    ("shuffle_impact_ratio", "total_time_ms"),
    ("shuffle_impact_ratio", "spill_to_disk_bytes"),
    ("photon_ratio", "execution_time_ms"),
    ("read_cache_bytes", "read_remote_bytes"),
    ("bytes_read_from_cache_percentage", "remote_read_ratio"),
    ("bytes_pruning_ratio", "read_bytes"),
    ("oom_fallback_count", "photon_ratio"),
    ("oom_fallback_count", "spill_to_disk_bytes"),
]


def _build_decision_drivers(result: ComparisonResult) -> dict:
    """Pre-compute weighted decision drivers from metric diffs.

    Returns a dict with top_regressions, top_improvements,
    suppressed_regressions, and net_score for LLM context.
    """
    weighted: list[tuple[str, float, str, MetricDiff]] = []
    suppressed: list[str] = []

    for md in result.metric_diffs:
        if not md.changed_flag:
            continue
        w = _METRIC_WEIGHTS.get(md.metric_name, 1.0)
        impact = w * abs(md.relative_diff_ratio) if md.relative_diff_ratio is not None else 0.0

        if md.regression_flag:
            weighted.append((md.metric_name, impact, "regression", md))
        elif md.improvement_flag:
            weighted.append((md.metric_name, impact, "improvement", md))

    # Detect suppressed regressions (IO-dependent metrics suppressed by comparison.py)
    for md in result.metric_diffs:
        if md.metric_name in _IO_DEPENDENT_METRICS and md.changed_flag and not md.regression_flag:
            # If value worsened but regression_flag is False, it was suppressed
            if md.relative_diff_ratio is not None:
                increase_effect = COMPARABLE_METRICS.get(md.metric_name, ("", "WORSENS"))[1]
                if (md.relative_diff_ratio > 0 and increase_effect == "WORSENS") or (
                    md.relative_diff_ratio < 0 and increase_effect == "IMPROVES"
                ):
                    suppressed.append(md.metric_name)

    regressions = sorted(
        [(n, i, md) for n, i, t, md in weighted if t == "regression"],
        key=lambda x: x[1],
        reverse=True,
    )[:5]
    improvements = sorted(
        [(n, i, md) for n, i, t, md in weighted if t == "improvement"],
        key=lambda x: x[1],
        reverse=True,
    )[:5]

    return {
        "top_regressions": regressions,
        "top_improvements": improvements,
        "suppressed_regressions": suppressed,
    }


def _build_comparison_fact_pack(result: ComparisonResult, lang: str = "en") -> str:
    """Build a structured fact pack for LLM comparison analysis.

    Includes decision drivers, metric changes, suppression context,
    and allowed causal links. Fully bilingual (en/ja).
    """
    ja = lang == "ja"
    drivers = _build_decision_drivers(result)
    lines: list[str] = []

    # Section 1: Decision Drivers (weighted)
    lines.append(
        "## 判定ドライバー（重要度順）" if ja else "## Decision Drivers (weighted by importance)"
    )
    if drivers["top_regressions"]:
        lines.append("### 主要な悪化" if ja else "### Top Regressions")
        for name, impact, md in drivers["top_regressions"]:
            pct = f"{md.relative_diff_ratio * 100:+.1f}%" if md.relative_diff_ratio else "N/A"
            lines.append(
                f"- {name}: {md.baseline_value} -> {md.candidate_value} ({pct}) "
                f"[重み={_METRIC_WEIGHTS.get(name, 1.0)}, 影響スコア={impact:.2f}]"
                if ja
                else f"- {name}: {md.baseline_value} -> {md.candidate_value} ({pct}) "
                f"[weight={_METRIC_WEIGHTS.get(name, 1.0)}, impact_score={impact:.2f}]"
            )
    if drivers["top_improvements"]:
        lines.append("### 主要な改善" if ja else "### Top Improvements")
        for name, impact, md in drivers["top_improvements"]:
            pct = f"{md.relative_diff_ratio * 100:+.1f}%" if md.relative_diff_ratio else "N/A"
            lines.append(
                f"- {name}: {md.baseline_value} -> {md.candidate_value} ({pct}) "
                f"[重み={_METRIC_WEIGHTS.get(name, 1.0)}, 影響スコア={impact:.2f}]"
                if ja
                else f"- {name}: {md.baseline_value} -> {md.candidate_value} ({pct}) "
                f"[weight={_METRIC_WEIGHTS.get(name, 1.0)}, impact_score={impact:.2f}]"
            )

    # Section 2: Suppressed Regressions
    if drivers["suppressed_regressions"]:
        lines.append("")
        lines.append(
            "## 抑制された悪化（判定から除外済み）"
            if ja
            else "## Suppressed Regressions (excluded from verdict)"
        )
        lines.append(
            "以下のメトリクスは悪化しましたが、総I/O（read_bytes）が30%以上改善したため抑制されました:"
            if ja
            else "The following metrics worsened but were suppressed because "
            "total I/O (read_bytes) improved by >30%:"
        )
        for name in drivers["suppressed_regressions"]:
            lines.append(f"- {name}")

    # Section 3: All Metric Changes
    lines.append("")
    lines.append("## 全メトリクス変化" if ja else "## All Metric Changes")
    lines.append(
        f"- ベースライン: {result.baseline_variant or 'N/A'}"
        if ja
        else f"- Baseline variant: {result.baseline_variant or 'N/A'}"
    )
    lines.append(
        f"- 候補: {result.candidate_variant or 'N/A'}"
        if ja
        else f"- Candidate variant: {result.candidate_variant or 'N/A'}"
    )
    lines.append(
        f"- 実験: {result.experiment_id or 'N/A'}"
        if ja
        else f"- Experiment: {result.experiment_id or 'N/A'}"
    )
    verdict = "性能劣化あり" if ja else "REGRESSION"
    ok_str = "問題なし" if ja else "OK"
    lines.append(f"- 総合判定: {verdict if result.regression_detected else ok_str}")
    lines.append(
        f"- 劣化深刻度: {result.regression_severity}"
        if ja
        else f"- Regression severity: {result.regression_severity}"
    )
    lines.append("")

    regression_label = " [悪化]" if ja else " [REGRESSION]"
    improved_label = " [改善]" if ja else " [IMPROVED]"
    for md in result.metric_diffs:
        if not md.changed_flag:
            continue
        flag = ""
        if md.regression_flag:
            flag = regression_label
        elif md.improvement_flag:
            flag = improved_label

        pct = ""
        if md.relative_diff_ratio is not None:
            pct = f" ({md.relative_diff_ratio * 100:+.1f}%)"

        abs_diff = ""
        if md.absolute_diff is not None:
            abs_diff_label = "絶対差分" if ja else "abs_diff"
            abs_diff = f" {abs_diff_label}={md.absolute_diff}"

        if ja:
            direction = f"(増加時={'悪化' if md.direction_when_increase == 'WORSENS' else '改善'})"
        else:
            direction = (
                f"(increase={'worsens' if md.direction_when_increase == 'WORSENS' else 'improves'})"
            )
        weight = _METRIC_WEIGHTS.get(md.metric_name, 1.0)
        weight_label = "重み" if ja else "weight"
        lines.append(
            f"- {md.metric_name}: {md.baseline_value} -> {md.candidate_value}"
            f"{pct}{abs_diff}{flag} {direction} [{weight_label}={weight}]"
        )

    # Unchanged metrics
    unchanged = [md for md in result.metric_diffs if not md.changed_flag]
    if unchanged:
        lines.append("")
        lines.append("## 変化なしのメトリクス" if ja else "## Unchanged Metrics")
        no_change = "変化なし" if ja else "no change"
        for md in unchanged:
            lines.append(f"- {md.metric_name}: {md.baseline_value} ({no_change})")

    # Section 4: Context
    lines.append("")
    lines.append("## 分析の前提条件" if ja else "## Analysis Context")
    if ja:
        lines.append("- 単一実行の比較（繰り返し計測なし）")
        lines.append("- 同一クエリフィンガープリント（データ量は異なる可能性あり）")
        lines.append("- ウォーム/コールドキャッシュ状態は不明")
    else:
        lines.append("- Single-run comparison (no repeated measurement)")
        lines.append("- Same query fingerprint (data volume may differ)")
        lines.append("- Warm/cold cache state unknown")
    if _NOISE_FLOOR:
        noise_label = "ノイズ閾値" if ja else "Noise thresholds"
        lines.append(
            f"- {noise_label}: " + ", ".join(f"{m} < {v}" for m, v in _NOISE_FLOOR.items())
        )

    # Section 5: Causal Links (reference)
    lines.append("")
    lines.append(
        "## 既知の因果関係（推論の参考）"
        if ja
        else "## Known Causal Links (reference for reasoning)"
    )
    for cause, effect in _CAUSAL_LINKS:
        lines.append(f"- {cause} -> {effect}")

    return "\n".join(lines)


def _build_comparison_system_prompt(lang: str, context: str = "dbsql") -> str:
    """Build system prompt balancing substantive analysis with evidence grounding.

    Args:
        lang: Output language ("en" or "ja").
        context: "dbsql" for DBSQL query comparison, "spark" for Spark application comparison.
    """
    if context == "spark":
        expert_ja = "Databricks / Apache Spark のパフォーマンスチューニング専門家"
        target_ja = "2つのSparkアプリケーションのBefore/After比較データ"
        expert_en = "a Databricks / Apache Spark performance tuning expert"
        target_en = "Before/After comparison data of two Spark applications"
    else:
        expert_ja = "Databricks SQLのパフォーマンスチューニング専門家"
        target_ja = "2つのクエリプロファイルのBefore/After比較データ"
        expert_en = "a Databricks SQL performance tuning expert"
        target_en = "the Before/After comparison data"

    if lang == "ja":
        return f"""あなたは{expert_ja}です。
{target_ja}を元に、**具体的で実用的な分析**を日本語で提供してください。

最も重要なのは「何がどう変わったのか」「なぜそうなったのか」「次に何をすべきか」を明確に説明することです。

## 出力構造

### 1. 変化の要約
- 最も重要な改善と悪化を、数値を引用しながら具体的に説明してください
- Decision Drivers（重み付き）の上位メトリクスを中心に分析してください
- 絶対値が小さい変化（例: 0.2% → 0.3%）はノイズの可能性があることを指摘してください
- 抑制された悪化（Suppressed Regressions）があれば、なぜ無視してよいか説明してください

### 2. 原因の考察
- 改善・悪化の原因として考えられる仮説を提示してください
- メトリクス間の相関を分析してください（例: 実行時間短縮とキャッシュ改善の関係）
- Allowed Causal Linksを参考にしつつ、自然な推論を行ってください
- 矛盾するメトリクスがあれば指摘してください
- 確信度が低い推論は「可能性がある」「考えられる」等の表現を使ってください

### 3. 推奨アクション
- 次に試すべきチューニングを優先度順に3件以内でリストしてください
- 各アクションにDatabricks SQLの具体的な設定値やヒントを含めてください
- 確度（high/medium/needs_verification）を付けてください

### 4. 総合判定
- **Go**: candidateを本番適用して問題ないか
- **Hold**: 追加検証が必要か
- **Rollback**: baselineに戻すべきか
- 判定の根拠を数値で説明してください

### ルール
- 提供されたメトリクスデータに基づいて分析すること
- SQL本文、実行プラン、クラスタ設定は未知のため断定しないこと
- 数値を引用する際はFact Packの値を正確に使うこと

Markdown形式で出力してください。"""

    else:
        return f"""You are {expert_en}.
Analyze {target_en} and provide **specific, substantive, actionable analysis**.

The most important thing is to clearly explain: what changed, why it likely changed, and what to do next.

## Output Structure

### 1. Change Summary
- Explain the most important improvements and regressions with specific numbers
- Focus on the top Decision Drivers (weighted metrics)
- Flag changes with small absolute values (e.g., 0.2% → 0.3%) as potential noise
- If Suppressed Regressions exist, explain why they can be safely ignored

### 2. Root Cause Hypotheses
- Propose likely causes for the observed improvements and regressions
- Analyze correlations between metrics (e.g., execution time improvement + cache improvement)
- Use the Allowed Causal Links as guidance for reasoning
- Point out contradicting metrics if any
- Use hedging language ("likely", "suggests", "possibly") for low-confidence inferences

### 3. Recommended Actions
- List up to 3 next tuning actions in priority order
- Include specific Databricks SQL settings, hints, or configurations
- Mark confidence: high / medium / needs_verification

### 4. Overall Verdict
- **Go**: Candidate is safe for production
- **Hold**: Needs additional verification
- **Rollback**: Revert to baseline
- Justify with specific metric numbers

### Rules
- Base analysis on the provided metric data
- SQL text, execution plan, and cluster config are unknown — do not assert them
- Quote numbers accurately from the Fact Pack

Output in Markdown format."""


def _build_comparison_prompt(result: ComparisonResult, lang: str = "en") -> str:
    """Build a detailed prompt for LLM comparison analysis."""
    fact_pack = _build_comparison_fact_pack(result, lang)

    if lang == "ja":
        header = "以下のFact Packを元に、比較分析の4セクションを日本語で生成してください。\n\n"
    else:
        header = (
            "Based on the following Fact Pack, generate the 4 comparison analysis sections.\n\n"
        )

    return header + fact_pack


def generate_comparison_llm_summary(
    result: ComparisonResult,
    model: str,
    databricks_host: str,
    databricks_token: str,
    lang: str = "en",
    context: str = "dbsql",
) -> str:
    """Generate an LLM-powered detailed analysis of a comparison.

    Args:
        result: ComparisonResult with metric diffs.
        model: LLM model name (e.g., databricks-claude-sonnet-4-5).
        databricks_host: Databricks workspace host.
        databricks_token: Authentication token.
        lang: Output language ("en" or "ja").

    Returns:
        Detailed analysis string in Markdown. Empty string on failure.
    """
    if not databricks_host:
        logger.warning("No Databricks host configured, skipping LLM comparison analysis")
        return ""

    try:
        client = create_openai_client(databricks_host, databricks_token)
        system_msg = _build_comparison_system_prompt(lang, context=context)
        user_prompt = _build_comparison_prompt(result, lang)

        messages = [
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_prompt},
        ]

        summary = call_llm_with_retry(
            client=client,
            model=model,
            messages=messages,
            max_tokens=16384,
            temperature=0.3,
        )
        logger.info("LLM comparison analysis generated (%d chars)", len(summary))
        if not summary:
            logger.warning("LLM returned empty content (possible max_tokens truncation)")
        return summary

    except Exception as e:
        logger.warning("LLM comparison analysis failed: %s", e)
        return ""
