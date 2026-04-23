"""Build Markdown sections from Gold table data (no LLM involved).

Used to append deterministic, data-driven sections to LLM-generated reports.
"""

from __future__ import annotations

import statistics
from typing import Any


def _percentile(values: list[float], p: float) -> float:
    """Simple percentile calculation (linear interpolation)."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    k = (n - 1) * p
    f = int(k)
    c = f + 1
    if c >= n:
        return sorted_vals[-1]
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


def _fmt_num(v: float | None, decimals: int = 0) -> str:
    if v is None:
        return "-"
    if decimals == 0:
        return f"{round(v):,}"
    return f"{v:,.{decimals}f}"


# ---------------------------------------------------------------------------
# i18n labels
# ---------------------------------------------------------------------------

_LABELS = {
    "ja": {
        "title": "G. ストリーミング分析",
        "streaming_queries_h": "ストリーミングクエリ",
        "metric": "メトリクス",
        "value": "値",
        "streaming_queries": "ストリーミングクエリ数",
        "total_batches": "合計バッチ数",
        "avg_batch_duration": "平均バッチ処理時間",
        "avg_process_rate": "平均処理レート",
        "max_state_memory": "最大状態メモリ",
        "stateful_queries": "ステートフルクエリ数",
        "source_types": "ソースタイプ",
        "sink_types": "シンクタイプ",
        "exceptions": "例外",
        "estimated_trigger": "推定トリガー間隔",
        "inferred_from": "QueryIdleEventから推定",
        "yes": "あり",
        "no": "なし",
        "query_id": "クエリ ID",
        "source": "ソース",
        "sink": "シンク",
        "batches": "バッチ数",
        "avg_dur": "平均バッチ処理時間 (ms)",
        "process_rate": "処理レート (rows/s)",
        "bottleneck": "ボトルネック",
        "severity": "重大度",
        "batch_summary": "バッチサマリーメトリクス",
        "min": "Min",
        "median": "Median",
        "avg": "Avg",
        "p95": "P95",
        "max": "Max",
        "duration_ms": "処理時間 (ms)",
        "add_batch_ms": "addBatch (ms)",
        "planning_ms": "プランニング (ms)",
        "offset_ms": "latestOffset (ms)",
        "commit_ms": "コミット (ms)",
        "input_rows": "入力行数",
        "process_rate_col": "処理レート (rows/s)",
        "bn_eval_h": "ストリーミングボトルネック評価",
        "alert": "アラート",
        "threshold": "閾値",
        "recommendation": "推奨アクション",
        "trigger_lag_detail": "トリガーラグ詳細",
        "spike_detail": "処理時間スパイク詳細",
        "batches_exceeded_trigger": "{n}バッチ が推定トリガー間隔 ({sec}秒) を超過:",
        "batches_exceeded_avg": "{n}バッチ が平均の3倍 ({avg} ms) を超過:",
        "no_trigger_lag": "トリガーラグなし — 全バッチがトリガー間隔内に完了。",
        "trigger_lag_rec": "クラスタサイズ拡大、maxFilesPerTrigger 削減、処理ロジック最適化。addBatch がボトルネックの場合は Photon 有効化や shuffle partitions 調整を検討。",
        "spike_rec": "データ到着パターンの平準化（maxBytesPerTrigger 設定）、スパイク時のデータ量を確認。大量ファイル到着時は trigger(availableNow=True) での分離処理を検討。",
        "trigger_col": "トリガー間隔",
        "lag_col": "ラグ",
        "spike_col": "スパイク",
        "problem_detail_h": "問題クエリ詳細",
        "verdict": "判定",
        "batch_col": "バッチ",
        "duration_col": "所要時間",
        "ratio_col": "倍率",
        "na": "N/A",
        "lag_exceeded": "バッチが推定間隔 ({sec}s) を超過",
        "spike_exceeded": "バッチが平均の3倍 ({avg} ms) を超過",
        "trigger_lag_n": "トリガーラグ {n}/{total} バッチ",
        "spike_n": "スパイク {n}/{total} バッチ",
        "overview_h": "概要",
        "avg_batch_duration_val": "平均バッチ処理時間",
        "avg_process_rate_val": "平均処理レート",
        "analysis_summary_h": "分析サマリー",
    },
    "en": {
        "title": "G. Streaming Analysis",
        "streaming_queries_h": "Streaming Queries",
        "metric": "Metric",
        "value": "Value",
        "streaming_queries": "Streaming Queries",
        "total_batches": "Total Batches",
        "avg_batch_duration": "Avg Batch Duration",
        "avg_process_rate": "Avg Process Rate",
        "max_state_memory": "Max State Memory",
        "stateful_queries": "Stateful Queries",
        "source_types": "Source Types",
        "sink_types": "Sink Types",
        "exceptions": "Exceptions",
        "estimated_trigger": "Estimated Trigger Interval",
        "inferred_from": "inferred from QueryIdleEvent",
        "yes": "Yes",
        "no": "No",
        "query_id": "Query ID",
        "source": "Source",
        "sink": "Sink",
        "batches": "Batches",
        "avg_dur": "Avg Batch Duration (ms)",
        "process_rate": "Process Rate (rows/s)",
        "bottleneck": "Bottleneck",
        "severity": "Severity",
        "batch_summary": "Batch Summary Metrics",
        "min": "Min",
        "median": "Median",
        "avg": "Avg",
        "p95": "P95",
        "max": "Max",
        "duration_ms": "Duration (ms)",
        "add_batch_ms": "addBatch (ms)",
        "planning_ms": "Planning (ms)",
        "offset_ms": "latestOffset (ms)",
        "commit_ms": "Commit (ms)",
        "input_rows": "Input Rows",
        "process_rate_col": "Process Rate (rows/s)",
        "bn_eval_h": "Streaming Bottleneck Evaluation",
        "alert": "Alert",
        "threshold": "Threshold",
        "recommendation": "Recommendation",
        "trigger_lag_detail": "Trigger Lag Detail",
        "spike_detail": "Duration Spike Detail",
        "batches_exceeded_trigger": "**{n} batches** exceeded estimated trigger interval ({sec}s):",
        "batches_exceeded_avg": "**{n} batches** exceeded 3x average ({avg} ms):",
        "no_trigger_lag": "No trigger lag detected — all batches completed within the trigger interval.",
        "trigger_lag_rec": "Scale up cluster, reduce maxFilesPerTrigger, optimize processing logic. If addBatch is the bottleneck, consider enabling Photon or tuning shuffle partitions.",
        "spike_rec": "Normalize data arrival pattern (set maxBytesPerTrigger), check data volume during spikes. For large file arrivals, consider separate processing with trigger(availableNow=True).",
        "trigger_col": "Trigger",
        "lag_col": "Lag",
        "spike_col": "Spike",
        "problem_detail_h": "Problem Query Details",
        "verdict": "Verdict",
        "batch_col": "Batch",
        "duration_col": "Duration",
        "ratio_col": "Ratio",
        "na": "N/A",
        "lag_exceeded": "batches exceeded estimated interval ({sec}s)",
        "spike_exceeded": "batches exceeded 3x average ({avg} ms)",
        "trigger_lag_n": "Trigger lag in {n}/{total} batches",
        "spike_n": "Duration spike in {n}/{total} batches",
        "overview_h": "Overview",
        "avg_batch_duration_val": "Avg Batch Duration",
        "avg_process_rate_val": "Avg Process Rate",
        "analysis_summary_h": "Analysis Summary",
    },
}


def _l(lang: str) -> dict[str, str]:
    return _LABELS.get(lang, _LABELS["en"])


# ---------------------------------------------------------------------------
# Analysis comment generation (data-driven, no LLM)
# ---------------------------------------------------------------------------


def _build_overview_comment(
    streaming_summary: dict[str, Any],
    trigger_interval_ms: int | None,
    lang: str,
) -> str:
    """Generate overview analysis paragraph from summary metrics."""
    qc = streaming_summary.get("query_count", 0)
    batches = streaming_summary.get("total_batches", 0)
    avg_dur = streaming_summary.get("avg_batch_duration_ms", 0) or 0
    avg_rate = streaming_summary.get("avg_throughput_rows_per_sec", 0) or 0
    stateful = streaming_summary.get("stateful_query_count", 0)
    has_exc = streaming_summary.get("has_exceptions", False)
    sources = streaming_summary.get("source_types", [])

    if lang == "ja":
        parts = []
        parts.append(f"{qc}件のストリーミングクエリが検出され、合計{batches}バッチが処理された。")
        parts.append(
            f"平均バッチ処理時間は {round(avg_dur)} ms、平均処理レートは {avg_rate:.1f} rows/sec である。"
        )
        if trigger_interval_ms:
            trigger_sec = trigger_interval_ms / 1000
            if avg_dur > trigger_interval_ms:
                parts.append(
                    f"推定トリガー間隔 {trigger_sec:.0f}秒 に対して平均処理時間が超過しており、恒常的な遅延が発生している。"
                )
            elif avg_dur > trigger_interval_ms * 0.7:
                parts.append(
                    f"推定トリガー間隔 {trigger_sec:.0f}秒 に対して平均処理時間が70%以上を占めており、余裕が少ない。"
                )
            else:
                parts.append(f"推定トリガー間隔 {trigger_sec:.0f}秒 に対して十分な余裕がある。")
        if stateful > 0:
            parts.append(f"ステートフルクエリが{stateful}件あり、状態ストアの監視が推奨される。")
        if has_exc:
            parts.append("例外による終了が検出されており、エラー原因の調査が必要。")
        src_str = ", ".join(sources)
        parts.append(f"ソースタイプ: {src_str}。")
        return " ".join(parts)
    else:
        parts = []
        parts.append(
            f"{qc} streaming {'query' if qc == 1 else 'queries'} detected, processing {batches} total batches."
        )
        parts.append(
            f"Average batch duration is {round(avg_dur)} ms with a process rate of {avg_rate:.1f} rows/sec."
        )
        if trigger_interval_ms:
            trigger_sec = trigger_interval_ms / 1000
            if avg_dur > trigger_interval_ms:
                parts.append(
                    f"Average duration exceeds the estimated trigger interval of {trigger_sec:.0f}s, indicating persistent lag."
                )
            elif avg_dur > trigger_interval_ms * 0.7:
                parts.append(
                    f"Average duration uses >70% of the estimated trigger interval ({trigger_sec:.0f}s), leaving limited headroom."
                )
            else:
                parts.append(
                    f"Sufficient headroom against the estimated trigger interval of {trigger_sec:.0f}s."
                )
        if stateful > 0:
            parts.append(
                f"{stateful} stateful {'query' if stateful == 1 else 'queries'} detected; state store monitoring is recommended."
            )
        if has_exc:
            parts.append("Exception-terminated queries detected; investigate error causes.")
        src_str = ", ".join(sources)
        parts.append(f"Source types: {src_str}.")
        return " ".join(parts)


def _build_batch_comment(
    streaming_batches: list[dict[str, Any]],
    trigger_interval_ms: int | None,
    lang: str,
) -> str:
    """Generate batch analysis comment from batch detail metrics."""
    if not streaming_batches:
        return ""

    durations = [float(b.get("batch_duration_ms", 0) or 0) for b in streaming_batches]
    add_batches = [float(b.get("add_batch_ms", 0) or 0) for b in streaming_batches]
    plannings = [float(b.get("query_planning_ms", 0) or 0) for b in streaming_batches]
    commits = [
        float(b.get("commit_offsets_ms", 0) or 0) + float(b.get("commit_batch_ms", 0) or 0)
        for b in streaming_batches
    ]
    offsets = [float(b.get("latest_offset_ms", 0) or 0) for b in streaming_batches]

    avg_dur = statistics.mean(durations) if durations else 0
    avg_add = statistics.mean(add_batches) if add_batches else 0
    avg_plan = statistics.mean(plannings) if plannings else 0
    avg_commit = statistics.mean(commits) if commits else 0
    avg_offset = statistics.mean(offsets) if offsets else 0

    # Identify dominant phase
    phases = [
        ("addBatch", avg_add),
        ("queryPlanning", avg_plan),
        ("latestOffset", avg_offset),
        ("commit", avg_commit),
    ]
    dominant = max(phases, key=lambda x: x[1])
    dominant_pct = (dominant[1] / avg_dur * 100) if avg_dur > 0 else 0

    p95 = _percentile(durations, 0.95)
    max_dur = max(durations)
    variance_ratio = max_dur / avg_dur if avg_dur > 0 else 0

    if lang == "ja":
        parts = []
        parts.append(
            f"バッチ処理時間の支配的フェーズは **{dominant[0]}** で、平均処理時間の {dominant_pct:.1f}% を占めている。"
        )
        if dominant[0] == "addBatch" and dominant_pct > 80:
            parts.append(
                "データ処理自体がボトルネック。Photon有効化、パーティション調整、処理ロジックの最適化を検討。"
            )
        elif dominant[0] == "latestOffset" and dominant_pct > 30:
            parts.append("ソースからのオフセット取得に時間がかかっている。ソースの応答性能を確認。")
        elif dominant[0] == "commit" and dominant_pct > 20:
            parts.append(
                "コミットオーバーヘッドが大きい。RocksDB状態バックエンドやDeltaログコンパクションを検討。"
            )
        elif dominant[0] == "queryPlanning" and dominant_pct > 20:
            parts.append(
                "クエリプランニングに時間がかかっている。クエリの簡素化やスキーマ進化の確認を検討。"
            )
        if variance_ratio > 5:
            parts.append(
                f"P95 ({_fmt_num(p95)} ms) と最大値 ({_fmt_num(max_dur)} ms) の差が大きく、処理時間にばらつきがある。"
            )
        return " ".join(parts)
    else:
        parts = []
        parts.append(
            f"The dominant processing phase is **{dominant[0]}**, accounting for {dominant_pct:.1f}% of average batch duration."
        )
        if dominant[0] == "addBatch" and dominant_pct > 80:
            parts.append(
                "Data processing itself is the bottleneck. Consider enabling Photon, tuning partitions, or optimizing processing logic."
            )
        elif dominant[0] == "latestOffset" and dominant_pct > 30:
            parts.append("Offset retrieval from source is slow. Check source responsiveness.")
        elif dominant[0] == "commit" and dominant_pct > 20:
            parts.append(
                "Commit overhead is significant. Consider RocksDB state backend or Delta log compaction."
            )
        elif dominant[0] == "queryPlanning" and dominant_pct > 20:
            parts.append(
                "Query planning is consuming significant time. Consider simplifying queries or checking schema evolution."
            )
        if variance_ratio > 5:
            parts.append(
                f"P95 ({_fmt_num(p95)} ms) and max ({_fmt_num(max_dur)} ms) show significant variance in batch duration."
            )
        return " ".join(parts)


def _build_summary_comment(
    streaming_summary: dict[str, Any],
    streaming_batches: list[dict[str, Any]] | None,
    lag_batches: list[tuple],
    spike_batches: list[tuple],
    trigger_interval_ms: int | None,
    lang: str,
) -> str:
    """Generate concluding summary comment for the entire section F."""
    avg_dur = streaming_summary.get("avg_batch_duration_ms", 0) or 0
    avg_rate = streaming_summary.get("avg_throughput_rows_per_sec", 0) or 0
    qc = streaming_summary.get("query_count", 0)
    has_exc = streaming_summary.get("has_exceptions", False)
    n_batches = len(streaming_batches) if streaming_batches else 0
    n_lag = len(lag_batches)
    n_spike = len(spike_batches)

    # Determine overall health
    issues = []
    if has_exc:
        issues.append("exception" if lang == "en" else "例外終了")
    if n_lag > 0:
        issues.append(
            f"trigger lag ({n_lag} batches)" if lang == "en" else f"トリガーラグ ({n_lag}バッチ)"
        )
    if n_spike > 0:
        issues.append(
            f"duration spike ({n_spike} batches)"
            if lang == "en"
            else f"処理時間スパイク ({n_spike}バッチ)"
        )
    if trigger_interval_ms and avg_dur > trigger_interval_ms:
        issues.append("chronic lag" if lang == "en" else "恒常的遅延")

    if lang == "ja":
        parts = []
        if not issues:
            parts.append(
                f"全体として、ストリーミング処理は安定して動作している。{qc}件のクエリが{n_batches}バッチを処理し、重大なボトルネックは検出されなかった。"
            )
        else:
            issue_str = "、".join(issues)
            parts.append(
                f"ストリーミング処理に **{len(issues)}件の問題** が検出された: {issue_str}。"
            )
            if n_lag > 0 and trigger_interval_ms:
                lag_pct = round(n_lag / max(n_batches, 1) * 100)
                parts.append(
                    f"全{n_batches}バッチ中{n_lag}バッチ ({lag_pct}%) がトリガー間隔を超過しており、データ量の増加に対してクラスタリソースが不足している可能性がある。"
                )
            if n_spike > 0:
                parts.append(
                    "処理時間のスパイクは、大量データの突発的到着またはリソース競合が原因の可能性がある。maxBytesPerTrigger の設定でバッチサイズを制限することを検討。"
                )
        parts.append(
            f"平均処理レート {avg_rate:.1f} rows/sec はワークロードの要件に対して十分かを確認すること。"
        )
        return " ".join(parts)
    else:
        parts = []
        if not issues:
            parts.append(
                f"Overall, streaming processing is operating stably. {qc} {'query' if qc == 1 else 'queries'} processed {n_batches} batches with no critical bottlenecks detected."
            )
        else:
            issue_str = ", ".join(issues)
            parts.append(
                f"**{len(issues)} issue(s)** detected in streaming processing: {issue_str}."
            )
            if n_lag > 0 and trigger_interval_ms:
                lag_pct = round(n_lag / max(n_batches, 1) * 100)
                parts.append(
                    f"{n_lag} of {n_batches} batches ({lag_pct}%) exceeded the trigger interval, suggesting insufficient cluster resources for the data volume."
                )
            if n_spike > 0:
                parts.append(
                    "Duration spikes may indicate sudden large data arrivals or resource contention. Consider setting maxBytesPerTrigger to limit batch size."
                )
        parts.append(
            f"Verify that the average process rate of {avg_rate:.1f} rows/sec meets workload requirements."
        )
        return " ".join(parts)


# ---------------------------------------------------------------------------
# Trigger interval estimation
# ---------------------------------------------------------------------------


def estimate_trigger_interval_ms(
    idle_events: list[dict[str, Any]] | None,
    batches: list[dict[str, Any]] | None,
) -> int | None:
    """Estimate trigger interval from QueryIdleEvent timestamps.

    Returns interval in ms, or None if not estimable.
    """
    if not idle_events or len(idle_events) < 2:
        return None

    from datetime import datetime

    timestamps = []
    for e in idle_events:
        ts_raw = e.get("event_timestamp") or e.get("timestamp") or ""
        if not ts_raw:
            continue
        ts_str = str(ts_raw)
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                timestamps.append(datetime.strptime(ts_str[:26].rstrip("Z"), fmt))
                break
            except (ValueError, TypeError):
                continue

    if len(timestamps) < 2:
        return None

    timestamps.sort()
    gaps_ms = []
    for i in range(1, len(timestamps)):
        gap = (timestamps[i] - timestamps[i - 1]).total_seconds() * 1000
        if gap > 0:
            gaps_ms.append(gap)

    if not gaps_ms:
        return None

    min_gap = min(gaps_ms)
    common_intervals = [1000, 2000, 5000, 10000, 15000, 30000, 60000, 120000, 300000]
    closest = min(common_intervals, key=lambda x: abs(x - min_gap))
    if abs(closest - min_gap) / max(min_gap, 1) < 0.5:
        return closest
    return round(min_gap)


# ---------------------------------------------------------------------------
# Pre-compute streaming alerts for LLM Fact Pack
# ---------------------------------------------------------------------------


def compute_streaming_alerts(
    streaming_batches: list[dict[str, Any]] | None,
    idle_events: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Pre-compute trigger lag and spike alerts for inclusion in LLM Fact Pack.

    Returns a dict with trigger_interval_ms, lag/spike counts, and details.
    """
    result: dict[str, Any] = {}
    if not streaming_batches:
        return result

    durations = [float(b.get("batch_duration_ms", 0) or 0) for b in streaming_batches]
    trigger_interval_ms = estimate_trigger_interval_ms(idle_events, streaming_batches)
    result["trigger_interval_ms"] = trigger_interval_ms
    n_total = len(streaming_batches)

    # Trigger lag
    if trigger_interval_ms and trigger_interval_ms > 0:
        lag_batches = []
        for b in streaming_batches:
            dur = float(b.get("batch_duration_ms", 0) or 0)
            if dur > trigger_interval_ms:
                lag_batches.append(
                    {
                        "batch_id": b.get("batch_id", "?"),
                        "duration_ms": dur,
                        "ratio": round(dur / trigger_interval_ms, 1),
                    }
                )
        result["trigger_lag"] = {
            "count": len(lag_batches),
            "total_batches": n_total,
            "pct": round(len(lag_batches) / max(n_total, 1) * 100),
            "severity": "HIGH"
            if len(lag_batches) > n_total / 2
            else ("MEDIUM" if lag_batches else "NONE"),
            "worst": sorted(lag_batches, key=lambda x: -x["duration_ms"])[:5],
        }

    # Duration spike
    if len(durations) >= 3:
        avg_dur = statistics.mean(durations)
        spike_batches = []
        for b in streaming_batches:
            dur = float(b.get("batch_duration_ms", 0) or 0)
            if dur > avg_dur * 3:
                spike_batches.append(
                    {
                        "batch_id": b.get("batch_id", "?"),
                        "duration_ms": dur,
                        "ratio": round(dur / avg_dur, 1),
                    }
                )
        result["duration_spike"] = {
            "count": len(spike_batches),
            "avg_duration_ms": round(avg_dur),
            "threshold_ms": round(avg_dur * 3),
            "severity": "MEDIUM" if spike_batches else "NONE",
            "worst": sorted(spike_batches, key=lambda x: -x["duration_ms"])[:5],
        }

    return result


# ---------------------------------------------------------------------------
# Section F Markdown builder
# ---------------------------------------------------------------------------


def build_streaming_section(
    streaming_queries: list[dict[str, Any]],
    streaming_summary: dict[str, Any],
    streaming_batches: list[dict[str, Any]] | None = None,
    idle_events: list[dict[str, Any]] | None = None,
    lang: str = "ja",
) -> str:
    """Build G. Streaming Analysis section as Markdown.

    Always returns a section — shows 'not detected' message when no data.
    """
    L = _l(lang)

    if (
        not streaming_queries
        or not streaming_summary
        or not streaming_summary.get("query_count", 0)
    ):
        heading = f"## {L['title']}"
        msg = (
            "ストリーミングジョブは検出されませんでした。"
            if lang == "ja"
            else "No streaming jobs detected."
        )
        return f"---\n\n{heading}\n\n{msg}\n"

    query_count = streaming_summary.get("query_count", 0)
    lines: list[str] = []
    lines.append("---\n")
    lines.append(f"## {L['title']}\n")

    # --- Overview table ---
    def _fmt_mem(b):
        return f"{b / 1024 / 1024:.1f} MB" if b and b > 0 else "-"

    def _fmt_rate(r):
        return f"{r:.1f} rows/sec" if r else "0"

    ja = lang == "ja"

    # --- Per-query batch grouping ---
    batches_by_query: dict[str, list[dict]] = {}
    for b in streaming_batches or []:
        qid = b.get("query_id", "__all__")
        batches_by_query.setdefault(qid, []).append(b)

    # --- Per-query trigger interval estimation ---
    query_trigger: dict[str, float | None] = {}
    for q in streaming_queries:
        qid = q.get("query_id", "")
        q_batches = batches_by_query.get(qid, [])
        query_trigger[qid] = estimate_trigger_interval_ms(idle_events, q_batches)

    # Global trigger (fallback)
    global_trigger = estimate_trigger_interval_ms(idle_events, streaming_batches)

    # --- Per-query lag/spike detection ---
    query_lag: dict[str, list[tuple]] = {}
    query_spike: dict[str, list[tuple]] = {}
    for q in streaming_queries:
        qid = q.get("query_id", "")
        q_batches = batches_by_query.get(qid, [])
        ti = query_trigger.get(qid) or global_trigger

        lags: list[tuple] = []
        if ti and ti > 0:
            for b in q_batches:
                dur = float(b.get("batch_duration_ms", 0) or 0)
                if dur > ti:
                    lags.append((b.get("batch_id", "?"), dur, dur / ti))
        query_lag[qid] = lags

        spikes: list[tuple] = []
        durs = [float(b.get("batch_duration_ms", 0) or 0) for b in q_batches]
        if len(durs) >= 3:
            avg_d = statistics.mean(durs)
            for b in q_batches:
                dur = float(b.get("batch_duration_ms", 0) or 0)
                if dur > avg_d * 3:
                    spikes.append((b.get("batch_id", "?"), dur, dur / avg_d))
        query_spike[qid] = spikes

    # --- Determine which queries need detail ---
    def _query_has_issue(q: dict) -> bool:
        qid = q.get("query_id", "")
        bn = q.get("bottleneck_type", "STREAM_OK")
        return (
            bn != "STREAM_OK"
            or len(query_lag.get(qid, [])) > 0
            or len(query_spike.get(qid, [])) > 0
        )

    # --- Overview table (restore full metrics) ---
    avg_dur_val = streaming_summary.get("avg_batch_duration_ms", 0) or 0
    avg_rate_val = streaming_summary.get("avg_throughput_rows_per_sec", 0) or 0
    max_state = streaming_summary.get("max_state_memory_bytes", 0) or 0
    src_types = ", ".join(streaming_summary.get("source_types", [])) or "-"
    sink_types = ", ".join(streaming_summary.get("sink_types", [])) or "-"

    overview_rows = [
        (L["streaming_queries"], str(query_count)),
        (L["total_batches"], str(streaming_summary.get("total_batches", 0))),
        (L["avg_batch_duration_val"], f"{round(avg_dur_val)} ms"),
        (L["avg_process_rate_val"], _fmt_rate(avg_rate_val)),
        (L["max_state_memory"], _fmt_mem(max_state)),
        (L["stateful_queries"], str(streaming_summary.get("stateful_query_count", 0))),
        (L["source_types"], src_types),
        (L["sink_types"], sink_types),
        (L["exceptions"], L["yes"] if streaming_summary.get("has_exceptions") else L["no"]),
    ]

    # Add global trigger interval to overview if available
    if global_trigger:
        trigger_sec = global_trigger / 1000
        overview_rows.append((L["estimated_trigger"], f"{trigger_sec:.0f}s ({L['inferred_from']})"))

    lines.append(f"| {L['metric']} | {L['value']} |")
    lines.append("|--------|-------|")
    for label, value in overview_rows:
        lines.append(f"| {label} | {value} |")
    lines.append("")

    # --- Query list table (all queries, with per-query trigger interval) ---
    lines.append(f"### {L['streaming_queries_h']}\n")
    lines.append(
        f"| {L['query_id']} | {L['source']} | {L['sink']} | {L['trigger_col']} | {L['batches']} | {L['avg_dur']} | {L['process_rate']} | {L['lag_col']} | {L['spike_col']} | {L['bottleneck']} |"
    )
    lines.append(
        "|----------|--------|------|---------|---------|----------|----------|---:|---:|----------|"
    )
    for q in streaming_queries:
        qid_full = q.get("query_id") or ""
        qid = (qid_full[:20] + "...") if len(qid_full) > 20 else qid_full
        src = q.get("source_type") or "-"
        sink = q.get("sink_type") or "-"
        batches = q.get("total_batches", 0) or 0
        avg_dur = round(q.get("avg_batch_duration_ms") or 0)
        rate = (
            f"{q['avg_processed_rows_per_sec']:.1f}" if q.get("avg_processed_rows_per_sec") else "-"
        )
        bn = q.get("bottleneck_type") or "STREAM_OK"
        ti = query_trigger.get(qid_full)
        ti_str = f"{ti / 1000:.0f}s" if ti else "-"
        n_lag = len(query_lag.get(qid_full, []))
        n_spike = len(query_spike.get(qid_full, []))
        bn_display = f"**{bn}**" if bn != "STREAM_OK" else bn
        lines.append(
            f"| `{qid}` | {src} | {sink} | {ti_str} | {batches} | {avg_dur} ms | {rate} | {n_lag} | {n_spike} | {bn_display} |"
        )
    lines.append("")

    # --- Detail for problem queries only ---
    problem_queries = [q for q in streaming_queries if _query_has_issue(q)]

    if problem_queries:
        lines.append(f"### {L['problem_detail_h']}\n")

    for q in problem_queries:
        qid_full = q.get("query_id") or ""
        qid_short = (qid_full[:20] + "...") if len(qid_full) > 20 else qid_full
        src = q.get("source_type") or "-"
        sink = q.get("sink_type") or "-"
        bn = q.get("bottleneck_type") or "STREAM_OK"
        ti = query_trigger.get(qid_full) or global_trigger
        q_batches = batches_by_query.get(qid_full, [])
        n_batches = len(q_batches)
        lags = query_lag.get(qid_full, [])
        spikes = query_spike.get(qid_full, [])

        ti_str = f"{ti / 1000:.0f}s" if ti else L["na"]
        lines.append(f"#### `{qid_short}` ({src} → {sink}, {L['estimated_trigger']}: {ti_str})\n")

        # Batch summary for this query
        if q_batches:

            def _qcol(key: str, _batches: list = q_batches) -> list[float]:
                return [float(b.get(key, 0) or 0) for b in _batches]

            def _qcol_sum2(k1: str, k2: str, _batches: list = q_batches) -> list[float]:
                return [float(b.get(k1, 0) or 0) + float(b.get(k2, 0) or 0) for b in _batches]

            q_metrics = [
                (L["duration_ms"], _qcol("batch_duration_ms")),
                (L["add_batch_ms"], _qcol("add_batch_ms")),
                (L["planning_ms"], _qcol("query_planning_ms")),
                (L["commit_ms"], _qcol_sum2("commit_offsets_ms", "commit_batch_ms")),
                (L["input_rows"], _qcol("num_input_rows")),
            ]
            lines.append(
                f"| {L['metric']} | {L['min']} | {L['median']} | {L['avg']} | {L['p95']} | {L['max']} |"
            )
            lines.append("|--------|-----|--------|-----|-----|-----|")
            for label, vals in q_metrics:
                if not vals or all(v == 0 for v in vals):
                    lines.append(f"| {label} | - | - | - | - | - |")
                    continue
                lines.append(
                    f"| {label} | {_fmt_num(min(vals))} | {_fmt_num(statistics.median(vals))} | {_fmt_num(statistics.mean(vals))} | {_fmt_num(_percentile(vals, 0.95))} | {_fmt_num(max(vals))} |"
                )
            lines.append("")

        # Trigger lag for this query
        if lags:
            trigger_sec = ti / 1000 if ti else 0
            lines.append(
                f"**{L['trigger_lag_detail']}**: {len(lags)}/{n_batches} "
                + L["lag_exceeded"].format(sec=f"{trigger_sec:.0f}")
                + "\n"
            )
            lines.append(f"| {L['batch_col']} | {L['duration_col']} | {L['ratio_col']} |")
            lines.append("|---:|---:|---:|")
            for bid, dur, ratio in sorted(lags, key=lambda x: -x[1])[:5]:
                lines.append(f"| #{bid} | {_fmt_num(dur)} ms | {ratio:.1f}x |")
            lines.append("")

        # Spike for this query
        if spikes:
            q_durs = [float(b.get("batch_duration_ms", 0) or 0) for b in q_batches]
            avg_d = statistics.mean(q_durs) if q_durs else 0
            lines.append(
                f"**{L['spike_detail']}**: {len(spikes)}/{n_batches} "
                + L["spike_exceeded"].format(avg=_fmt_num(avg_d * 3))
                + "\n"
            )
            lines.append(f"| {L['batch_col']} | {L['duration_col']} | {L['ratio_col']} |")
            lines.append("|---:|---:|---:|")
            for bid, dur, ratio in sorted(spikes, key=lambda x: -x[1])[:5]:
                lines.append(f"| #{bid} | {_fmt_num(dur)} ms | {ratio:.1f}x |")
            lines.append("")

        # Per-query verdict
        verdict_parts = []
        if bn != "STREAM_OK":
            rec = q.get("recommendation") or ""
            verdict_parts.append(f"**{bn}**: {rec}" if rec else f"**{bn}**")
        if lags:
            verdict_parts.append(L["trigger_lag_n"].format(n=len(lags), total=n_batches))
        if spikes:
            verdict_parts.append(L["spike_n"].format(n=len(spikes), total=n_batches))
        if verdict_parts:
            sep = "。" if ja else ". "
            lines.append(f"**{L['verdict']}**: " + sep.join(verdict_parts))
            lines.append("")

        lines.append("---\n")

    # --- Summary analysis comment ---
    all_lags = []
    all_spikes = []
    for v in query_lag.values():
        all_lags.extend(v)
    for v in query_spike.values():
        all_spikes.extend(v)

    # Note: Analysis comment is generated separately by build_streaming_analysis_comment()
    # and appended after State/Watermark deep sections for proper ordering.

    return "\n".join(lines)


def build_streaming_deep_section(
    deep_analysis: dict[str, Any],
    lang: str = "ja",
) -> str:
    """Build additional sub-sections for Section G: state growth, watermark."""
    if not deep_analysis:
        return ""

    ja = lang == "ja"
    lines: list[str] = []

    # State growth
    sg = deep_analysis.get("state_growth")
    if sg:
        h = "### State成長分析" if ja else "### State Growth Analysis"
        lbl_val = "値" if ja else "Value"
        first = sg.get("first_state_mb", 0)
        last = sg.get("last_state_mb", 0)
        growth = sg.get("growth_mb_per_batch", 0)
        batches = sg.get("batches_analyzed", 0)
        eviction = sg.get("eviction_ratio", 0)
        dropped = sg.get("total_rows_dropped_by_watermark", 0)

        lines.extend(
            [
                h,
                "",
                f"| | {lbl_val} |",
                "|---|---|",
                f"| {'初回State' if ja else 'First State'} | {first:.2f} MB |",
                f"| {'最新State' if ja else 'Last State'} | {last:.2f} MB |",
                f"| {'成長率' if ja else 'Growth Rate'} | {growth:.4f} MB/batch ({batches} batches) |",
                f"| {'Watermark除去行数' if ja else 'Rows Dropped by Watermark'} | {dropped:,} |",
                f"| {'除去効率' if ja else 'Eviction Ratio'} | {eviction:.1%} |",
                "",
            ]
        )

        if growth > 0 and last > 0:
            # Simple linear projection: batches until 2x current
            batches_to_double = int(last / growth) if growth > 0 else 0
            note = (
                f"> State成長率 {growth:.4f} MB/batch。このペースが続く場合、約{batches_to_double}バッチで現在の2倍に到達。"
                if ja
                else f"> State growing at {growth:.4f} MB/batch. At this rate, ~{batches_to_double} batches to double current size."
            )
            lines.extend([note, ""])

    # Watermark
    wm = deep_analysis.get("watermark")
    if wm:
        h = "### Watermark進行" if ja else "### Watermark Progression"
        lbl_val = "値" if ja else "Value"
        lines.extend(
            [
                h,
                "",
                f"| | {lbl_val} |",
                "|---|---|",
                f"| {'最初' if ja else 'First'} | {wm.get('first_watermark', '-')} |",
                f"| {'最新' if ja else 'Last'} | {wm.get('last_watermark', '-')} |",
                f"| {'更新回数' if ja else 'Updates'} | {wm.get('total_watermark_updates', 0)} |",
                "",
            ]
        )

    return "\n".join(lines)


def build_streaming_analysis_comment(
    streaming_summary: dict[str, Any],
    streaming_batches: list[dict[str, Any]] | None,
    streaming_queries: list[dict[str, Any]] | None,
    deep_analysis: dict[str, Any] | None,
    trigger_interval_ms: float | None,
    lang: str = "ja",
) -> str:
    """Build the analysis comment for Section G, placed after all sub-sections.

    Includes State growth/Watermark risk assessment.
    """
    ja = lang == "ja"
    _summary_h = "分析コメント" if ja else "Analysis Summary"
    comments: list[str] = []

    # Delegate to existing _build_summary_comment for base analysis
    all_lags: list[tuple] = []
    all_spikes: list[tuple] = []
    # Re-derive lag/spike from batches (simplified)
    if streaming_batches and trigger_interval_ms and trigger_interval_ms > 0:
        for b in streaming_batches:
            dur = float(b.get("batch_duration_ms", 0) or 0)
            if dur > trigger_interval_ms:
                all_lags.append((b.get("batch_id", "?"), dur, dur / trigger_interval_ms))
    if streaming_batches and len(streaming_batches) >= 3:
        durs = [float(b.get("batch_duration_ms", 0) or 0) for b in streaming_batches]
        avg_d = statistics.mean(durs)
        for b in streaming_batches:
            dur = float(b.get("batch_duration_ms", 0) or 0)
            if dur > avg_d * 3:
                all_spikes.append((b.get("batch_id", "?"), dur, dur / avg_d))

    base_comment = _build_summary_comment(
        streaming_summary,
        streaming_batches,
        all_lags,
        all_spikes,
        int(trigger_interval_ms) if trigger_interval_ms is not None else None,
        lang,
    )
    if base_comment:
        comments.append(base_comment)

    # State growth risk assessment
    if deep_analysis:
        sg = deep_analysis.get("state_growth")
        if sg:
            growth = sg.get("growth_mb_per_batch", 0)
            last_mb = sg.get("last_state_mb", 0)
            eviction = sg.get("eviction_ratio", 0)
            if growth > 0 and last_mb > 100:
                batches_to_double = int(last_mb / growth) if growth > 0 else 0
                comments.append(
                    f"**State成長リスク**: 現在 {last_mb:.1f} MB、成長率 {growth:.4f} MB/batch。"
                    f"約{batches_to_double}バッチで2倍に到達見込み。"
                    if ja
                    else f"**State Growth Risk**: Current {last_mb:.1f} MB, growth rate {growth:.4f} MB/batch. "
                    f"~{batches_to_double} batches to double."
                )
            total_dropped = sg.get("total_rows_dropped_by_watermark", 0)
            state_growing = growth > 0 and last_mb > 10

            if eviction < 0.01 and total_dropped == 0 and state_growing:
                # No eviction + state growing → accumulation risk
                comments.append(
                    "**Watermark除去なし**: Stateが蓄積し続けています。Watermark設定またはState TTLの追加を推奨。"
                    if ja
                    else "**No Watermark Eviction**: State is accumulating without eviction. Consider adding watermark or state TTL."
                )
            elif eviction > 0.5 and state_growing:
                # High eviction but still growing → eviction not keeping up
                comments.append(
                    f"**State蓄積リスク**: Watermark除去率 {eviction:.0%} だがStateが増加中（{growth:.4f} MB/batch）。"
                    "除去が追いついていません。Watermark閾値の調整またはState TTLの追加を推奨。"
                    if ja
                    else f"**State Accumulation Risk**: Eviction rate {eviction:.0%} but state still growing ({growth:.4f} MB/batch). "
                    "Eviction not keeping up. Adjust watermark threshold or add state TTL."
                )
            elif eviction > 0.5 and not state_growing:
                # High eviction + state stable/shrinking → watermark working correctly
                comments.append(
                    f"**Watermark正常動作**: 除去率 {eviction:.0%}（{total_dropped:,}行除去）。"
                    "Stateは安定しており、Watermarkが期限切れデータを適切にクリーンアップしています。"
                    if ja
                    else f"**Watermark Operating Normally**: Eviction rate {eviction:.0%} ({total_dropped:,} rows dropped). "
                    "State is stable — watermark is properly cleaning up expired data."
                )

        # Watermark progression
        wm = deep_analysis.get("watermark")
        if wm:
            updates = wm.get("total_watermark_updates", 0)
            if updates == 0:
                comments.append(
                    "**Watermark未進行**: Watermarkが更新されていません。Watermark設定を確認してください。"
                    if ja
                    else "**Watermark Not Advancing**: No watermark updates detected. Verify watermark configuration."
                )

    if not comments:
        return ""

    lines = [f"### {_summary_h}", ""]
    for c in comments:
        lines.append(c)
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section D — Serialization Analysis (Python-generated, no LLM)
# ---------------------------------------------------------------------------


_UDF_TYPE_LABELS: dict[str, dict[str, str]] = {
    "BatchEvalPython": {"ja": "Python UDF", "en": "Python UDF"},
    "ArrowEvalPython": {"ja": "Pandas UDF (Arrow)", "en": "Pandas UDF (Arrow)"},
    "FlatMapGroupsInPandas": {
        "ja": "Pandas UDF (applyInPandas)",
        "en": "Pandas UDF (applyInPandas)",
    },
    "ExistingRDD": {"ja": "RDD使用", "en": "RDD Usage"},
}


def _detect_udf_types(op_list: str, lang: str) -> str:
    """Extract human-readable UDF type labels from non_photon_op_list."""
    found = []
    for key, labels in _UDF_TYPE_LABELS.items():
        if key in (op_list or ""):
            found.append(labels.get(lang, labels["en"]))
    return ", ".join(found) if found else ""


def _classify_serialization_cause(
    stage_name_lower: str, sql_exec_id: Any, udf_sql_ids: set, ja: bool
) -> tuple[str, str]:
    """Classify the cause of high serialization and suggest remediation."""
    # Check if this SQL execution uses UDFs
    try:
        sid = int(sql_exec_id) if sql_exec_id else -1
    except (TypeError, ValueError):
        sid = -1

    if sid in udf_sql_ids:
        return (
            ("Python UDF", "Pandas UDF (Arrow) に移行")
            if ja
            else ("Python UDF", "Migrate to Pandas UDF (Arrow)")
        )

    if "executecollect" in stage_name_lower or (
        "collect" in stage_name_lower and "action" not in stage_name_lower
    ):
        return (
            ("collect()", "limit() + display() に置換")
            if ja
            else ("collect()", "Replace with limit() + display()")
        )

    if "withaction" in stage_name_lower:
        return (
            ("アクション (collect/count/take)", "collect()の削減、limit()の活用")
            if ja
            else ("Action (collect/count/take)", "Reduce collect(), use limit()")
        )

    if "broadcast" in stage_name_lower:
        return (
            ("Broadcast", "Broadcast サイズの確認・削減")
            if ja
            else ("Broadcast", "Review and reduce broadcast size")
        )

    if "withthreadlocal" in stage_name_lower:
        return (
            ("Python-JVM転送", "UDF/RDDの削減、DataFrame APIへ移行")
            if ja
            else ("Python-JVM Transfer", "Reduce UDF/RDD, migrate to DataFrame API")
        )

    if "wrapper" in stage_name_lower or "ipykernel" in stage_name_lower:
        return (
            ("Notebook実行", "display()/show()の使用を最小化")
            if ja
            else ("Notebook Exec", "Minimize display()/show() usage")
        )

    return (
        ("その他", "Spark UIでステージ詳細を確認")
        if ja
        else ("Other", "Check stage details in Spark UI")
    )


def build_serialization_section(
    serialization_summary: dict[str, Any],
    udf_analysis: list[dict[str, Any]],
    high_ser_jobs: list[dict[str, Any]] | None = None,
    lang: str = "ja",
) -> str:
    """Build Markdown Section D: Serialization Analysis.

    Always returns a section — shows 'not detected' message when no data.
    """
    ja = lang == "ja"
    heading = "## D. シリアライゼーション分析" if ja else "## D. Serialization Analysis"

    has_ser = (
        serialization_summary
        and float(serialization_summary.get("total_serialization_ms", 0) or 0) > 0
    )
    has_udf = bool(udf_analysis)
    has_jobs = bool(high_ser_jobs)

    if not has_ser and not has_udf and not has_jobs:
        msg = (
            "シリアライゼーションの問題は検出されませんでした。"
            if ja
            else "No serialization issues detected."
        )
        return f"{heading}\n\n{msg}\n"

    lines = [heading, ""]

    # Overview table
    if has_ser:
        total_ms = float(serialization_summary.get("total_serialization_ms", 0) or 0)
        deser_ms = float(serialization_summary.get("total_deserialize_ms", 0) or 0)
        result_ser_ms = float(serialization_summary.get("total_result_serialize_ms", 0) or 0)
        ser_pct = float(serialization_summary.get("serialization_pct", 0) or 0)
        high_stages = int(serialization_summary.get("stages_with_high_ser", 0) or 0)

        lbl_overview = "### 概要" if ja else "### Overview"
        lbl_total = "**シリアライズ合計**" if ja else "**Total Serialization**"
        lbl_deser = "デシリアライズ" if ja else "Deserialize"
        lbl_result = "結果シリアライズ" if ja else "Result Serialize"
        lbl_high = "高シリアライズステージ数" if ja else "High-serialization stages"
        lbl_val = "値" if ja else "Value"

        lines.extend(
            [
                lbl_overview,
                "",
                f"| | {lbl_val} |",
                "|---|---|",
                f"| {lbl_total} | {total_ms:,.0f} ms ({ser_pct:.1f}%) |",
                f"| {lbl_deser} | {deser_ms:,.0f} ms |",
                f"| {lbl_result} | {result_ser_ms:,.0f} ms |",
                f"| {lbl_high} | {high_stages} (> 10%) |",
                "",
            ]
        )

    # UDF detection table (always shown)
    lbl_udf = "### UDF検出" if ja else "### UDF Detection"
    if has_udf:
        h_id = "SQL ID"
        h_desc = "説明" if ja else "Description"
        h_dur = "実行時間" if ja else "Duration"
        h_type = "検出タイプ" if ja else "Detected Type"

        lines.extend(
            [
                lbl_udf,
                "",
                f"| {h_id} | {h_desc} | {h_dur} | {h_type} |",
                "|---:|---|---:|---|",
            ]
        )
        for row in udf_analysis:
            eid = row.get("execution_id", "")
            desc = (row.get("description_short", "") or "")[:60]
            dur = float(row.get("duration_sec", 0) or 0)
            udf_type = _detect_udf_types(row.get("non_photon_op_list", ""), lang)
            lines.append(f"| {eid} | {desc} | {dur:.1f} sec | {udf_type} |")
        lines.append("")

        note = (
            "> Python UDFはシリアライゼーションコストが高くなります。Pandas UDF (Arrow) への移行で改善が見込めます。"
            if ja
            else "> Python UDFs have high serialization overhead. Consider migrating to Pandas UDF (Arrow) for better performance."
        )
        lines.append(note)
        lines.append("")
    else:
        # UDF not detected — show explicit message
        lines.append(lbl_udf)
        lines.append("")
        if has_ser and ser_pct > 10:
            # High serialization but no UDF detected — explain possible causes
            msg = (
                "SQLプランからUDFオペレータ（BatchEvalPython, ArrowEvalPython等）は検出されませんでした。\n"
                "シリアライゼーションオーバーヘッドが高い原因として、以下が考えられます：\n\n"
                "- PySpark RDD操作（map, flatMap等）によるPython-JVM間のデータ転送\n"
                "- 大量のタスク結果のDriverへの転送\n"
                "- Broadcast変数のシリアライゼーション\n"
                if ja
                else "No UDF operators (BatchEvalPython, ArrowEvalPython, etc.) detected in SQL plans.\n"
                "Possible causes of high serialization overhead:\n\n"
                "- PySpark RDD operations (map, flatMap, etc.) causing Python-JVM data transfer\n"
                "- Large task result transfers to driver\n"
                "- Broadcast variable serialization\n"
            )
        else:
            msg = (
                "SQLプランからUDFオペレータは検出されませんでした。"
                if ja
                else "No UDF operators detected in SQL plans."
            )
        lines.append(msg)
        lines.append("")

    # High-serialization jobs table
    lbl_jobs = (
        "### ジョブ別高シリアライズステージ (Top 5)"
        if ja
        else "### High-Serialization Stages by Job (Top 5)"
    )
    if has_jobs and high_ser_jobs is not None:
        h_job = "Job ID"
        h_sql = "SQL ID"
        h_stage = "Stage"
        h_name = "ステージ名" if ja else "Stage Name"
        h_ser = "シリアライズ%" if ja else "Ser %"
        h_cause = "原因" if ja else "Cause"
        h_fix = "改善方法" if ja else "Remediation"

        # Build UDF SQL ID set for cross-reference
        udf_sql_ids = set()
        if udf_analysis:
            for u in udf_analysis:
                udf_sql_ids.add(int(u.get("execution_id", -1)))

        h_sql = "SQL ID"
        h_dur = "実行時間" if ja else "Duration"

        lines.extend(
            [
                lbl_jobs,
                "",
                f"| {h_job} | {h_sql} | {h_stage} | {h_name} | {h_dur} | {h_ser} | {h_cause} | {h_fix} |",
                "|---:|---:|---:|---|---:|---:|---|---|",
            ]
        )
        for row in high_ser_jobs:
            job_id = row.get("job_id", "")
            sql_id = row.get("sql_execution_id", "")
            sql_display = str(sql_id) if sql_id else "-"
            stage_id = row.get("stage_id", "")
            name = (row.get("stage_name", "") or "").lower()
            s_pct = float(row.get("serialization_pct", 0) or 0)
            dur_ms = float(row.get("duration_ms", 0) or 0)
            dur_str = f"{dur_ms / 1000:.1f}s" if dur_ms >= 1000 else f"{dur_ms:.0f}ms"

            # Classify cause and remediation
            cause, fix = _classify_serialization_cause(name, sql_id, udf_sql_ids, ja)

            display_name = (row.get("stage_name", "") or "")[:35]
            lines.append(
                f"| {job_id} | {sql_display} | {stage_id} | {display_name} | {dur_str} | {s_pct:.1f}% | {cause} | {fix} |"
            )
        lines.append("")
    else:
        lines.extend([lbl_jobs, ""])
        msg = (
            "高シリアライズステージとジョブの対応は取得できませんでした。"
            if ja
            else "High-serialization stage-to-job mapping is not available."
        )
        lines.extend([msg, ""])

    # Analysis comment
    lbl_analysis = "### 分析コメント" if ja else "### Analysis"
    lines.append(lbl_analysis)
    lines.append("")

    # Build analysis based on available data
    comments: list[str] = []
    if has_ser:
        if ser_pct > 50:
            comments.append(
                f"シリアライゼーションオーバーヘッドが極めて高い状態です（{ser_pct:.1f}%）。"
                if ja
                else f"Serialization overhead is extremely high ({ser_pct:.1f}%)."
            )
        elif ser_pct > 10:
            comments.append(
                f"シリアライゼーションオーバーヘッドが高い状態です（{ser_pct:.1f}%）。"
                if ja
                else f"Serialization overhead is elevated ({ser_pct:.1f}%)."
            )

    if has_udf:
        udf_types = set()
        for row in udf_analysis:
            t = _detect_udf_types(row.get("non_photon_op_list", ""), "en")
            if t:
                udf_types.update(t.split(", "))
        if "Python UDF" in udf_types:
            comments.append(
                "Python UDF（BatchEvalPython）が検出されました。Pandas UDF (Arrow) への移行でシリアライゼーションコストを大幅に削減できます。"
                if ja
                else "Python UDF (BatchEvalPython) detected. Migrating to Pandas UDF (Arrow) can significantly reduce serialization cost."
            )
        if "Pandas UDF (Arrow)" in udf_types or "Pandas UDF (applyInPandas)" in udf_types:
            comments.append(
                "Pandas UDF (Arrow/applyInPandas) が使用されています。Python UDFよりは効率的ですが、ネイティブSpark関数への置換が可能か検討してください。"
                if ja
                else "Pandas UDF (Arrow/applyInPandas) in use. More efficient than Python UDFs, but consider replacing with native Spark functions where possible."
            )
        if "RDD Usage" in udf_types or "RDD使用" in udf_types:
            comments.append(
                "ExistingRDD（RDD操作）が検出されました。DataFrame APIへの移行でシリアライゼーションを回避できます。"
                if ja
                else "ExistingRDD (RDD operations) detected. Migrating to DataFrame API can avoid serialization overhead."
            )

    if has_jobs and has_ser and high_ser_jobs is not None:
        # Cause breakdown from job table
        cause_counts: dict[str, int] = {}
        for row in high_ser_jobs:
            name_l = (row.get("stage_name", "") or "").lower()
            sql_id = row.get("sql_execution_id", "")
            cause, _ = _classify_serialization_cause(
                name_l, sql_id, udf_sql_ids if has_udf else set(), ja
            )
            cause_counts[cause] = cause_counts.get(cause, 0) + 1

        if cause_counts:
            breakdown_parts = [
                f"{cause}: {cnt}件" if ja else f"{cause}: {cnt}"
                for cause, cnt in sorted(cause_counts.items(), key=lambda x: -x[1])
            ]
            comments.append(
                f"高シリアライズステージの原因内訳: {', '.join(breakdown_parts)}"
                if ja
                else f"High-serialization stage cause breakdown: {', '.join(breakdown_parts)}"
            )

    if not comments:
        comments.append(
            "シリアライゼーションオーバーヘッドは低い水準です。"
            if ja
            else "Serialization overhead is at a low level."
        )

    for c in comments:
        lines.append(f"- {c}")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section H — Cost Estimate (Python-generated, no LLM)
# (was Section G before serialization section was added)
# ---------------------------------------------------------------------------


def build_cost_section(
    dbu_estimate: dict[str, Any],
    lang: str = "ja",
) -> str:
    """Build Markdown Section H: Cost Estimate.

    Always returns a section — shows 'not available' message when no data.
    """
    if not dbu_estimate or not dbu_estimate.get("estimated_total_usd"):
        heading = "## H. コスト推定" if lang == "ja" else "## H. Cost Estimate"
        msg = (
            "コスト推定に必要なクラスタ情報が取得できませんでした。"
            if lang == "ja"
            else "Cluster information required for cost estimation is not available."
        )
        return f"{heading}\n\n{msg}\n"

    total_usd = float(dbu_estimate.get("estimated_total_usd", 0) or 0)

    total_dbu = dbu_estimate.get("estimated_total_dbu", 0)
    dbu_usd = dbu_estimate.get("estimated_dbu_usd", 0)
    compute_usd = dbu_estimate.get("estimated_compute_usd", 0)
    dbu_per_hour = dbu_estimate.get("estimated_dbu_per_hour", 0)
    photon_mult = dbu_estimate.get("photon_multiplier", 1.0)
    dbu_estimate.get("pricing_note", "")
    region = dbu_estimate.get("region", "")

    # Per-hour USD
    duration_hours = total_dbu / dbu_per_hour if dbu_per_hour > 0 else 0
    per_hour_usd = total_usd / duration_hours if duration_hours > 0 else 0

    if lang == "ja":
        heading = "## H. コスト推定"
        lbl_total = "**推定総コスト**"
        lbl_dbu = "DBU"
        lbl_compute = "クラウドコンピュート"
        lbl_per_hour = "時間単価"
        lbl_photon = "Photon"
    else:
        heading = "## H. Cost Estimate"
        lbl_total = "**Estimated Total**"
        lbl_dbu = "DBU"
        lbl_compute = "Cloud Compute"
        lbl_per_hour = "Per Hour"
        lbl_photon = "Photon"

    photon_str = "ON" if photon_mult > 1.0 else "OFF"

    # Breakdown fields
    sku = dbu_estimate.get("sku", "Jobs Compute")
    worker_type = dbu_estimate.get("worker_node_type", "(unknown)")
    driver_type = dbu_estimate.get("driver_node_type", "(unknown)")
    worker_vcpus = dbu_estimate.get("worker_vcpus", "?")
    driver_vcpus = dbu_estimate.get("driver_vcpus", "?")
    worker_count_label = dbu_estimate.get("worker_count_label", "?")
    duration_m = dbu_estimate.get("duration_min", 0)
    dbu_unit_price = dbu_estimate.get("dbu_unit_price", 0)
    worker_dbu_rate = dbu_estimate.get("worker_dbu_rate", 0)
    driver_dbu_rate = dbu_estimate.get("driver_dbu_rate", 0)
    worker_compute_rate = dbu_estimate.get("worker_compute_rate", 0)
    driver_compute_rate = dbu_estimate.get("driver_compute_rate", 0)
    region_mult = dbu_estimate.get("region_multiplier", 1.0)
    driver_dbu_val = dbu_estimate.get("driver_dbu", 0)
    worker_dbu_val = dbu_estimate.get("worker_dbu", 0)

    if lang == "ja":
        lbl_breakdown = "### 内訳"
        lbl_role = "役割"
        lbl_inst = "インスタンス"
        lbl_vcpu = "vCPU"
        lbl_count = "台数"
        lbl_dbu_rate = "DBU/hr"
        lbl_usd_rate = "$/hr"
        lbl_subtotal_dbu = "DBU小計"
        lbl_assumptions = "### 前提条件"
    else:
        lbl_breakdown = "### Breakdown"
        lbl_role = "Role"
        lbl_inst = "Instance"
        lbl_vcpu = "vCPU"
        lbl_count = "Count"
        lbl_dbu_rate = "DBU/hr"
        lbl_usd_rate = "$/hr"
        lbl_subtotal_dbu = "DBU Subtotal"
        lbl_assumptions = "### Assumptions"

    lines = [
        heading,
        "",
        "| | Value |",
        "|---|---|",
        f"| {lbl_total} | **${total_usd:.3f}** |",
        f"| {lbl_dbu} | {total_dbu:.3f} DBU (${dbu_usd:.3f}) |",
        f"| {lbl_compute} | ${compute_usd:.3f} |",
        f"| {lbl_per_hour} | ${per_hour_usd:.3f}/hr |",
        f"| {lbl_photon} | {photon_str} ({photon_mult}x) |",
        "",
        lbl_breakdown,
        "",
        f"| {lbl_role} | {lbl_inst} | {lbl_vcpu} | {lbl_count} | {lbl_dbu_rate} | {lbl_usd_rate} | {lbl_subtotal_dbu} |",
        "|---|---|---:|---:|---:|---:|---:|",
        f"| Driver | {driver_type} | {driver_vcpus} | 1 | {driver_dbu_rate} | ${driver_compute_rate:.3f} | {driver_dbu_val:.3f} |",
        f"| Worker | {worker_type} | {worker_vcpus} | {worker_count_label} | {worker_dbu_rate} | ${worker_compute_rate:.3f} | {worker_dbu_val:.3f} |",
        "",
        lbl_assumptions,
        "",
    ]

    if lang == "ja":
        lines.extend(
            [
                f"- **コンピュート種別**: {sku}",
                f"- **DBU単価**: ${dbu_unit_price}/DBU (PAYGO定価)",
                f"- **クラウド単価**: On-Demand定価 ({region or 'us-east-1'}",
                f"  {'× ' + str(region_mult) if region_mult != 1.0 else ''})",
                f"- **実行時間**: {duration_m:.1f} min",
                "- **計算式**: 総コスト = DBUコスト + クラウドコンピュートコスト",
                f"  - DBUコスト = 総DBU × ${dbu_unit_price}/DBU",
                "  - クラウドコスト = Σ (ノード数 × $/hr × 稼働時間)",
                "",
                "> 実際の請求額は契約形態（Commit/Enterprise）やReserved Instancesにより異なります。",
            ]
        )
    else:
        lines.extend(
            [
                f"- **SKU**: {sku}",
                f"- **DBU Unit Price**: ${dbu_unit_price}/DBU (PAYGO list price)",
                f"- **Cloud Pricing**: On-Demand list price ({region or 'us-east-1'}"
                f"{'  × ' + str(region_mult) if region_mult != 1.0 else ''})",
                f"- **Duration**: {duration_m:.1f} min",
                "- **Formula**: Total = DBU Cost + Cloud Compute Cost",
                f"  - DBU Cost = Total DBU × ${dbu_unit_price}/DBU",
                "  - Cloud Cost = Σ (node count × $/hr × duration)",
                "",
                "> Actual billing may differ based on contract terms (Commit/Enterprise) and Reserved Instances.",
            ]
        )

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section H — Cluster Right-Sizing Recommendations (Python-generated)
# ---------------------------------------------------------------------------


def build_sizing_section(
    recommendations: list[Any],
    current_config: dict[str, Any],
    lang: str = "ja",
) -> str:
    """Build Markdown Section I: Cluster Right-Sizing Recommendations.

    Always returns a section — shows 'well-sized' message when no recommendations.
    """
    if not recommendations:
        heading = (
            "## I. クラスタサイジング推奨"
            if lang == "ja"
            else "## I. Cluster Right-Sizing Recommendations"
        )
        msg = (
            "サイジングの問題は検出されませんでした。現在の構成は適正です。"
            if lang == "ja"
            else "No sizing issues detected. Current configuration appears well-sized."
        )
        return f"{heading}\n\n{msg}\n"

    worker_type = current_config.get("worker_node_type", "(unknown)")
    min_w = current_config.get("min_workers", "?")
    max_w = current_config.get("max_workers", "?")
    workers_label = f"{min_w}→{max_w}" if min_w != max_w else str(min_w)

    if lang == "ja":
        heading = "## I. クラスタサイジング推奨"
        lbl_current = "### 現在の構成"
        lbl_recs = "### 推奨事項"
        h_signal = "シグナル"
        h_sev = "重要度"
        h_dir = "方向"
        h_change = "変更内容"
        h_cost = "コスト影響"
        h_rationale = "根拠"
    else:
        heading = "## I. Cluster Right-Sizing Recommendations"
        lbl_current = "### Current Configuration"
        lbl_recs = "### Recommendations"
        h_signal = "Signal"
        h_sev = "Severity"
        h_dir = "Direction"
        h_change = "Change"
        h_cost = "Cost Impact"
        h_rationale = "Rationale"

    dir_labels = {
        "UP": "スケールアップ" if lang == "ja" else "Scale UP",
        "DOWN": "スケールダウン" if lang == "ja" else "Scale DOWN",
        "CONSOLIDATE": "統合" if lang == "ja" else "Consolidate",
        "HORIZONTAL": "水平拡張" if lang == "ja" else "Horizontal",
        "SCALE_LIMIT": "スケール上限" if lang == "ja" else "Scale Limit",
        "SPOT": "可用性変更" if lang == "ja" else "Availability",
    }

    lines = [
        heading,
        "",
        lbl_current,
        f"{worker_type} × {workers_label} workers",
        "",
        lbl_recs,
        "",
        f"| {h_signal} | {h_sev} | {h_dir} | {h_change} | {h_cost} | {h_rationale} |",
        "|---|:---:|---|---|---|---|",
    ]

    for r in recommendations:
        change = f"{r.current_instance} → {r.recommended_instance}"
        if r.current_instance == r.recommended_instance:
            change = dir_labels.get(r.direction, r.direction)

        sign = "+" if r.cost_delta_pct > 0 else ""
        cost_str = f"{sign}{r.cost_delta_pct:.0f}% (${r.recommended_usd_per_hr:.3f}/hr)"

        # Truncate rationale for table readability
        rationale = r.rationale
        if len(rationale) > 120:
            rationale = rationale[:117] + "..."

        lines.append(
            f"| {r.signal} | {r.severity} | {r.direction} | {change} | {cost_str} | {rationale} |"
        )

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Section B preamble — SQL Execution Top N table (Python-generated, no LLM)
# ---------------------------------------------------------------------------


def build_sql_execution_table(
    sql_plan_top5: list[dict[str, Any]],
    lang: str = "ja",
) -> str:
    """Build deterministic Markdown table for SQL/DataFrame execution top N.

    Inserted at the beginning of Appendix B before LLM-generated analysis.
    """
    ja = lang == "ja"

    if not sql_plan_top5:
        return ""

    h_num = "#"
    h_id = "execution_id"
    h_dur = "Duration" if not ja else "実行時間"
    h_ds = "DataSourceInfo"
    h_type = "Type"
    h_ops = "Operators"
    h_joins = "Joins"

    lines = [
        f"| {h_num} | {h_id} | {h_dur} | {h_ds} | {h_type} | {h_ops} | {h_joins} |",
        "|---:|---:|---:|---|---|---:|---|",
    ]

    has_sql_type = False
    for i, row in enumerate(sql_plan_top5, 1):
        eid = row.get("execution_id", "")
        dur = float(row.get("duration_sec", 0) or 0)
        ds_info = (row.get("data_source_info", "") or "")[:80]
        rtype = row.get("type") or "DataFrame"
        ops = int(row.get("total_operators", 0) or 0)

        # Build join summary
        bhj = int(row.get("bhj_count", 0) or 0)
        pbhj = int(row.get("photon_bhj_count", 0) or 0)
        smj = int(row.get("smj_count", 0) or 0)
        join_parts = []
        if smj > 0:
            join_parts.append(f"{smj} SMJ")
        if bhj > 0:
            join_parts.append(f"{bhj} BHJ")
        if pbhj > 0:
            join_parts.append(f"{pbhj} PBHJ")
        join_str = ", ".join(join_parts) if join_parts else "-"

        if rtype.startswith("SQL"):
            has_sql_type = True

        lines.append(f"| {i} | {eid} | {dur:.1f}s | {ds_info} | {rtype} | {ops} | {join_str} |")

    lines.append("")

    if has_sql_type:
        tip = (
            "💡 Type=SQL の実行は DBSQL Profiler で詳細分析・SQLリライトが可能です。"
            if ja
            else "💡 SQL-type executions can be analyzed in detail and rewritten using DBSQL Profiler."
        )
        lines.append(tip)
        lines.append("")

    return "\n".join(lines)
