"""Spark Perf Markdown report generation from Gold table data.

Generates a structured 8-section report matching the Lakeview dashboard layout.
Sections: 分析対象, 1.エグゼクティブサマリー（5Sボトルネック評価+アラート含む）,
2.推奨アクション（ボトルネック分析+推奨アクション統合）, 3.Shuffle分析,
4.Photon利用状況分析, 5.並列実行影響分析, 6.Executorリソース分析,
7.プラン最適化時間分析, 8.I/O分析
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Bilingual label dictionary (ja / en)
# ---------------------------------------------------------------------------
_LABELS: dict[str, dict[str, str]] = {
    "ja": {
        # Section titles
        "analysis_target": "分析対象",
        "cluster_info": "クラスタ情報",
        "cluster_name": "クラスタ名",
        "dbr_version": "DBR バージョン",
        "driver_node_type": "Driver ノードタイプ",
        "worker_node_type": "Worker ノードタイプ",
        "worker_count": "Worker 数",
        "fixed_size": "固定サイズ",
        "autoscale": "オートスケール",
        "region": "リージョン",
        "app_info": "アプリケーション情報",
        "app_name": "アプリケーション名",
        "start_time": "開始時刻",
        "end_time": "終了時刻",
        "total_exec_time": "総実行時間",
        "executive_summary": "エグゼクティブサマリー",
        "performance_alerts": "パフォーマンスアラート",
        "stage_exec_bottleneck_analysis": "ボトルネック分析",
        "shuffle_analysis": "Shuffle 分析",
        "photon_utilization": "Photon 利用状況分析",
        "concurrent_exec_analysis": "並列実行影響分析",
        "io_analysis": "I/O 分析",
        "executor_resource_analysis": "Executor リソース分析",
        "plan_optimization_analysis": "プラン最適化時間分析",
        "recommended_actions": "推奨アクション（Top Findings）",
        # Executive summary
        "exec_summary_desc": "分析対象の Spark アプリケーションの全体像、主要メトリクス、5S ボトルネック評価、および重要度を要約します。",
        "job_overview": "ジョブ概要",
        "total_jobs": "総ジョブ数",
        "success": "成功",
        "failure": "失敗",
        "total_stages": "総ステージ数",
        "total_tasks": "総タスク数",
        "task_cumulative_time": "タスク累積時間",
        "hours": "時間",
        "minutes_unit": "分",
        "io_dataflow_overview": "I/O・データフロー概要",
        "total_input": "総 Input",
        "total_output": "総 Output",
        "total_shuffle_read": "総 Shuffle Read",
        "total_shuffle_write": "総 Shuffle Write",
        "spill_total": "Spill 合計",
        "gc_overhead": "GC オーバーヘッド",
        "5s_bottleneck_eval": "5S ボトルネック評価・重要度",
        "severity_level": "重要度レベル",
        # 5S labels
        "skew": "Skew（スキュー）",
        "skew_detected_fmt": "{count}件検出。最大スキュー比率{ratio:.1f}倍（Stage {stage}）",
        "not_detected": "検出なし",
        "spill_label": "Spill（スピル）",
        "spill_detected_fmt": "総スピル: {spill}",
        "shuffle_label": "Shuffle（シャッフル）",
        "shuffle_heavy_fmt": "{count}件のHEAVY_SHUFFLE検出。総Shuffle量{amount}",
        "shuffle_moderate": "中程度。総Shuffle: {amount}",
        "shuffle_good": "良好。総Shuffle: {amount}",
        "small_files_label": "Small Files（スモールファイル）",
        "small_files_detected_fmt": "{count}件検出",
        "small_files_not_applicable": "判定対象外",
        "serialization_label": "Serialization（シリアライゼーション）",
        "serialization_high_fmt": "{count}件のExecutorでシリアライズ時間が高い（{min:.1f}〜{max:.1f}%）",
        "serialization_gc_fmt": "GCオーバーヘッド: {pct:.1f}%",
        "good": "良好",
        "alert_total_fmt": "アラート合計: {high} HIGH, {med} MEDIUM, {low} LOW",
        # Performance alerts
        "alert_section_desc": "本セクションでは各パフォーマンス指標を目標値と比較し、警告・注意・情報に分類します。",
        "summary": "サマリー",
        "warnings_count_fmt": "\u25b2 {w} 警告 | \u25b3 {c} 注意 | \u24d8 {i} 情報",
        "warnings_header": "\u25b2 警告",
        "cautions_header": "\u25b3 注意",
        "infos_header": "\u24d8 情報",
        # Alert detail templates
        "small_files_alert_fmt": "[Small Files] {count}ステージで小ファイル問題検出（{range}）",
        "small_files_ipt_fmt": "タスクあたり読み取り: {min:.1f}〜{max:.1f}MB | 目標値: 128MB",
        "small_files_rec": "OPTIMIZE でファイルをコンパクト化。spark.databricks.delta.optimizeWrite.enabled=true, autoCompact.enabled=auto。Predictive Optimization の有効化を検討",
        "data_skew_alert_fmt": "[Data Skew] {count}ステージでデータスキュー検出（最大スキュー比率: {ratio:.1f}倍 | 目標値: <5倍）",
        "data_skew_rec": "AQE スキュー結合を有効化: spark.sql.adaptive.skewJoin.enabled=true または REPARTITION ヒントでデータを再分散",
        "heavy_shuffle_alert_fmt": "[Heavy Shuffle] {count}ステージで大量Shuffle検出（{amount}）",
        "heavy_shuffle_rec": "Broadcast Join を検討 (spark.sql.autoBroadcastJoinThreshold)。shuffle partitions 数を調整",
        "generic_alert_fmt": "[{type}] {count}ステージで検出",
        "ser_alert_fmt": "[Serialization] {count}件のExecutorでシリアライズ時間高い（{min:.1f}〜{max:.1f}% | 目標値: <5%）",
        "ser_alert_rec": "UDF・RDD使用の削減を検討",
        "photon_alert_fmt": "[Photon] 平均Photon利用率低い（{pct:.1f}% | 目標値: >80%）",
        "photon_alert_rec": "BatchEvalPython（Python UDF）をPySparkネイティブ関数に書き換え",
        "photon_alert_disabled_fmt": "[Photon] クラスタで Photon が無効（spark.databricks.photon.enabled = false）",
        "photon_alert_disabled_rec": "Photon 対応ランタイムへの切り替えを検討してください",
        "partition_sizing_alert_fmt": "[Partition Sizing] {count}ステージでパーティション不足（タスクあたり{min:.1f}〜{max:.1f}MB | 目標値: 100〜200MB）",
        "partition_sizing_rec_fmt": "spark.sql.shuffle.partitions を{count}に調整",
        # Stage execution & bottleneck analysis (merged)
        "stage_exec_desc": "本セクションではボトルネックのサマリー、検出されたステージの詳細分析（上位5件）、およびボトルネックなしの正常ステージ上位5件を表示します。",
        "total_count_fmt": "合計",
        "count_unit": "件",
        "count_unit_fmt": "{count}件（合計 {dur:,.1f}秒）",
        "bn_stages_header": "ボトルネック検出ステージ 詳細分析（上位5件、実行時間順）",
        "bn_detail_fmt": "Stage {sid} (Job {jid}): {dur:.1f}秒 / {tasks:,}タスク / ボトルネック: {bt} / CPU効率: {cpu:.1f}%",
        "normal_stages_header": "実行時間上位ステージ（ボトルネックなし、上位5件）",
        "normal_detail_fmt": "Stage {sid} (Job {jid}): {dur:.1f}秒 / {tasks:,}タスク / ボトルネック: なし / CPU効率: {cpu:.1f}%",
        "bn_shuffle_input_data": "Shuffle/入力データ",
        "bn_disk_spill": "Disk Spill",
        "bn_memory_spill": "Memory Spill",
        "task_skew_ratio": "タスクスキュー比率",
        "threshold": "閾値",
        "cpu_efficiency": "CPU効率",
        "exec_time": "実行時間",
        "task_count": "タスク数",
        "per_task_fmt": "タスクあたり{val:.1f}MB",
        "input_data": "入力データ",
        "cause_analysis": "原因分析",
        "improvement": "改善策",
        # Small files cause
        "small_files_cause_fmt": "タスクあたりの読み取り量が{per_task:.1f}MBと目標の128MBを大幅に下回っており、大量の小さなファイルによる高いオーバーヘッドが発生しています。",
        "small_files_improvement_1": "OPTIMIZE でファイルをコンパクト化・クラスタ化",
        "small_files_improvement_2": "spark.databricks.delta.optimizeWrite.enabled=true",
        "small_files_improvement_3": "spark.databricks.delta.autoCompact.enabled=auto",
        "small_files_improvement_4": "Predictive Optimization の有効化を検討",
        # Data skew cause
        "data_skew_cause_fmt": "タスクスキュー比率が{ratio:.1f}倍と高く、データの分布に偏りがあります。特定のパーティションにデータが集中しています。",
        "data_skew_improvement_1": "AQE スキュー結合を有効化: spark.sql.adaptive.skewJoin.enabled=true",
        "data_skew_improvement_2": "スキューパーティション係数を調整: spark.sql.adaptive.skewJoin.skewedPartitionFactor=3",
        "data_skew_improvement_3": "CTE で事前集約し JOIN 前にデータ量を削減",
        "data_skew_improvement_4": "明示的な repartition() でデータを均等に分割",
        # Heavy shuffle cause
        "heavy_shuffle_cause_fmt": "大量のShuffleデータ（{amount}）がワーカーノード間で移動しており、タスクあたりのShuffle Read量が{per_task:.1f}MBと目標の128MBを上回っています。",
        "heavy_shuffle_improvement_1": "Broadcast Join を検討: spark.sql.autoBroadcastJoinThreshold を調整",
        "heavy_shuffle_improvement_2_fmt": "Shuffle パーティション数の最適化: spark.sql.shuffle.partitions={count}",
        "heavy_shuffle_improvement_3": "シャッフルされるデータ量を減らす（列の絞り込み、事前フィルタリング）",
        "no_bottleneck_detected": "ボトルネックは検出されませんでした。",
        # Shuffle analysis
        "shuffle_analysis_desc": "本セクションではノード間のデータ移動（Shuffle）の量とパーティションサイジングを分析します。",
        "appropriate": "適正",
        "high": "多い",
        "top3_shuffle_stages": "Shuffle が多いステージ TOP3",
        "partition_sizing": "パーティションサイジング",
        "partition_target": "目標",
        "partition_target_val": "100〜200MB/タスク（128MB推奨）",
        "under_partitioned_header": "パーティション不足（UNDER_PARTITIONED）:",
        "under_partitioned_fmt": "Stage {sid}: タスクあたり{per_task:.1f}MB \u2192 パーティション不足。推奨パーティション数: {rec:,}",
        "over_partitioned_header": "パーティション過剰（OVER_PARTITIONED）:",
        "over_partitioned_fmt": "Stage {sid}: タスクあたり{per_task:.1f}MB \u2192 パーティション過剰。推奨パーティション数: {rec:,}",
        "partition_ok": "パーティションサイジングに問題は検出されませんでした。",
        # Photon analysis
        "photon_desc": "本セクションでは Photon エンジンの利用状況を分析します。Photon が利用されていないクエリでは Classic Spark にフォールバックし、処理速度が低下します。",
        "photon_important_note": "重要: Photon が 100% 実行されても効果が薄い処理特性がある。",
        "photon_cpu_heavy": "Photon は CPU-heavy な処理（Join, Aggregation 等）を高速化するエンジンであり、以下の特性を持つワークロードでは改善が小さい:",
        "photon_io_bound": "**I/O bound な処理**: 単純 ETL（Read \u2192 Filter \u2192 Write）",
        "photon_deep_lineage": "**Deep DataFrame Lineage**: withColumn ループ \u00d7 数百回",
        "photon_short_query": "**短時間クエリ（< 2秒）**: Photon 初期化オーバーヘッドが相対的に大きい",
        "photon_ser_heavy": "**Serialization-heavy**: Python UDF の serialization は Photon で改善されない",
        "avg_photon_rate": "平均 Photon 率",
        "low": "低い",
        "photon_below_50_fmt": "Photon 率 50%未満の SQL 数",
        "photon_detail_desc": "本セクションでは Photon Explanation（非対応理由）が記録されているクエリを優先し、上位10件を表示します。実行時間が0.5秒未満のクエリは影響が小さいため省略します。",
        "photon_rate": "Photon率",
        "target_tables": "対象テーブル",
        "non_photon_ops": "非対応オペレータ",
        "rewrite_method": "書き換え方法",
        "no_photon_data": "Photon データなし",
        "photon_cluster_disabled": "ℹ このクラスタでは Photon が無効です（spark.databricks.photon.enabled = false）。Photon を有効化することで CPU-heavy な処理（Join, Aggregation 等）を高速化できます。Photon 対応ランタイムへの切り替えを検討してください。",
        "photon_enabled_but_zero": "⚠ このクラスタでは Photon が有効ですが、全 SQL の Photon 利用率が 0% です。全オペレータが Classic Spark にフォールバックしています。非対応オペレータ（Python UDF, RDD 等）がないか確認してください。",
        "photon_config_unknown": "ℹ クラスタの Photon 設定が不明です（設定データなし）。全 SQL の Photon 利用率は 0% です。DBR バージョンが Photon 対応かどうかを確認し、Photon 対応ランタイムへの切り替えを検討してください。",
        # Photon rewrite suggestions
        "rewrite_udf": "BatchEvalPython（Python UDF）をPySparkネイティブ関数に書き換え",
        "rewrite_rdd": "RDD スキャンは Photon 非対応です。DataFrame API または SparkSQL を使用してテーブルから直接読み込むように書き換え",
        "rewrite_ctas": "コマンド系処理（CREATE OR REPLACE TABLE AS SELECT）は制御フローのため Photon 対象外ですが、内部のデータ処理は Photon で実行される場合があります",
        "rewrite_cmd_flow": "コマンド系処理は制御フローのため Photon 対象外ですが、内部のデータ処理は Photon で実行される場合があります。後続ジョブのPhoton利用状況を確認してください。",
        "non_photon_ops_fmt": "非対応オペレータ: {ops}",
        # Concurrent execution analysis
        "concurrent_desc": "本セクションでは同一アプリケーション内および別セッション（クロスアプリ）のジョブ並列実行がパフォーマンスに与えた影響を分析します。",
        "intra_app_concurrency": "同一アプリケーション内並列実行（同一セッションでのリソース競合）",
        "max_concurrent_jobs": "最大同時実行ジョブ数",
        "parallel_jobs_fmt": "並列実行が発生したジョブ数",
        "all_jobs_fmt": "全{total} ジョブ",
        "sequential_info": "同一アプリケーション内では全ジョブが逐次実行されました",
        "cross_app_concurrency": "クロスアプリケーション並列実行（別セッションとのリソース競合）",
        "cross_app_detected_fmt": "クロスアプリ並列が検出されたジョブ数",
        "max_cross_concurrent": "最大同時実行数（他アプリのジョブ数）",
        "concurrent_apps": "同時実行していたアプリ",
        "cross_app_warning": "同一クラスタ上で別セッションのジョブが同時実行されていました。CPU・メモリ・I/O・Disk Cache のリソース競合により、Cache Locality の損失やスケジューリングオーバーヘッドの増大が発生している可能性があります。",
        "affected_jobs_example": "影響を受けたジョブの例:",
        "affected_job_fmt": "Job {jid}: 重複ジョブ数 {dup}、実行時間 {dur:.1f}秒",
        "no_concurrency_data": "並列実行データなし",
        # Executor resource analysis
        "executor_desc": "本セクションでは各 Executor（ワーカーノード）のリソース利用状況を分析し、リソース問題を診断します。",
        "avg_gc_overhead": "平均 GC オーバーヘッド",
        "avg_cpu_efficiency": "平均 CPU 効率",
        "target_fmt": "目標: {val}",
        "executor_group_analysis": "Executor グループ分析",
        "high_load_group": "高負荷グループ",
        "mid_load_group": "中負荷グループ",
        "low_load_group": "低負荷グループ",
        "task_count_label": "タスク数",
        "task_time_label": "タスク時間",
        "cpu_eff_label": "CPU効率",
        "gc_rate_label": "GC率",
        "diagnosis": "診断",
        "ser_diagnosis_fmt": "SERIALIZATION: シリアライズ時間 {min:.1f}〜{max:.1f}秒 ({min_pct:.1f}〜{max_pct:.1f}%) \u2192 UDF・RDD使用の削減を検討",
        "ser_diagnosis_pct_fmt": "SERIALIZATION: シリアライズ時間 {min:.1f}〜{max:.1f}% \u2192 UDF・RDD使用の削減を検討",
        "load_diff_minor_fmt": "グループ間の負荷差は軽微（最大差約{pct:.0f}%）で、全Executorで均等にタスクが分散されています。",
        "load_diff_skewed_fmt": "グループ間の負荷差が{pct:.0f}%あり、タスク分散に偏りがあります。",
        # Spot / Node loss
        "spot_node_loss": "Spot / ノードロスト分析",
        "detected_count": "検出件数",
        "uptime": "稼働時間",
        "affected_tasks": "影響タスク数",
        "failed_tasks": "失敗タスク数",
        "lost_shuffle_data": "消失 Shuffle データ",
        "task_reexec": "タスク再実行",
        "shuffle_recomp": "シャッフル再計算",
        "executor_acquire": "Executor取得",
        "estimated_delay": "推定遅延",
        "breakdown": "内訳",
        "recommended_settings": "推奨設定",
        # Plan optimization analysis
        "plan_opt_desc": "本セクションではジョブの実行時間のうち、Spark のクエリプラン最適化・コンパイル・スケジューリングに消費された時間を分析します。",
        "app_total_exec_time": "アプリケーション総実行時間",
        "job_total_exec_time": "ジョブ合計実行時間",
        "job_gap_overhead": "ジョブ間オーバーヘッド",
        "job_internal_overhead": "ジョブ内オーバーヘッド（プラン最適化・コード生成）",
        "plan_overhead_ok": "ジョブ内のプラン最適化オーバーヘッドは軽微です。",
        "plan_overhead_warn_fmt": "最大プラン最適化オーバーヘッド: {val:.1f}秒",
        "job_gap_analysis": "ジョブ間ギャップ分析（Driver 処理時間）",
        "total_gap": "合計ギャップ",
        "max_gap": "最大ギャップ",
        "max_gap_fmt": "{gap:.0f}秒（Job {jid} \u2192 Job {next_jid} 間）",
        "avg_gap": "平均ギャップ",
        "top_gap_pairs": "最大ギャップが発生したジョブペア（上位5件）:",
        "driver_overhead_warn_fmt": "Driver 処理時間がアプリケーション実行時間の {pct:.1f}% を占めており、Notebook コードの最適化や Delta メタデータ操作の削減を検討してください。",
        "dup_scan_analysis": "重複スキャン分析とキャッシュ戦略",
        "dup_scan_desc": "同一テーブル/パスが複数回スキャンされている場合、キャッシュ戦略により処理時間を削減できる可能性がある。",
        "scan_count_fmt": "{count}回スキャン / 合計実行時間: {dur:,.1f}秒",
        "photon_cache_priority": "Photon 環境での推奨優先順位:",
        "cache_opt_1": "**Delta テーブル書き出し** \u2014 読み込み Photon \u2705 + Spot ロスト耐性 \u2705",
        "cache_opt_2": "**persist(DISK_ONLY)** \u2014 読み込み Photon \u2705 + Lineage 保持",
        "cache_opt_3": "**cache()** \u2014 読み込み Photon \u274c だが手軽。Spot ロスト時は Lineage から再計算",
        "cache_opt_4": "**checkpoint()** \u2014 Lineage 切断 + Spot ロスト耐性",
        # I/O analysis
        "io_analysis_desc": "本セクションではスキャン（テーブル読み取り）の I/O パターンを分析します。",
        "io_scan_volume_top5": "スキャンボリューム TOP5（読み取りサイズ順）",
        "io_table": "テーブル",
        "io_format": "フォーマット",
        "io_storage_path": "ストレージパス",
        "io_column_count": "カラム数",
        "io_wide_schema": "Wide Schema",
        "io_filters": "フィルタ",
        "io_files_read": "読み取りファイル数",
        "io_files_pruned": "プルーニング済みファイル数",
        "io_file_pruning_pct": "ファイルプルーニング率",
        "io_files_read_size_mb": "読み取りファイルサイズ",
        "io_fs_read_size_mb": "FS 読み取りサイズ",
        "io_cache_hit_pct": "キャッシュヒット率",
        "io_cache_hit_good": "良好",
        "io_cache_hit_low": "低い",
        "io_cache_write_gb": "キャッシュ書き込み",
        "io_cache_read_wait_ms": "キャッシュ読み取り待ち",
        "io_cache_write_wait_ms": "キャッシュ書き込み待ち",
        "io_scan_time_ms": "スキャン時間",
        "io_cloud_request_count": "クラウドリクエスト数",
        "io_cloud_request_dur_ms": "クラウドリクエスト時間",
        "io_duration_sec": "クエリ実行時間",
        "io_execution_id": "Execution ID",
        "io_dup_scan_analysis": "重複スキャン分析",
        "io_dup_scan_desc": "同一テーブルが複数回スキャンされている場合、キャッシュ戦略により処理時間を削減できる可能性があります。",
        "io_dup_total_scan_fmt": "合計スキャン回数: {count}回 / 合計実行時間: {dur:,.1f}秒",
        "io_dup_max_columns": "最大カラム数",
        "io_dup_format_breakdown": "フォーマット内訳",
        "io_dup_format_fmt": "{fmt}: {count}回 / {dur:.1f}秒",
        "io_no_scan_data": "スキャンデータなし",
        # Recommended actions
        "actions_priority_header": "推奨アクション（優先度順）",
        "actions_desc": "パフォーマンス向上のため、優先順位の高いチューニング項目を記載します。",
        "actions_desc2": "各項目は影響度（Impact）と実施容易性（Effort）で優先度を評価しています。",
        "action_spot_decommission": "Spot インスタンス Decommission 設定の有効化",
        "action_spot_priority": "優先度: 10/10",
        "action_rationale": "根拠:",
        "action_spot_detected_fmt": "{count}件のExecutorロスト（NODE_LOST: {nl}件、SPOT_PREEMPTION: {sp}件）が検出",
        "action_total_delay_fmt": "合計推定遅延: {delay:,.0f}秒（{hours:.1f}時間）",
        "action_lost_shuffle_fmt": "消失Shuffleデータ: {amount}",
        "action_spot_hypothesis": "Spot インスタンスのプリエンプションやノード障害時に、Shuffleデータが消失し、上流ステージからの再計算が必要になっている。Decommission機能が無効のため、Executorロスト前にShuffleデータの退避が行われていない。",
        "action_spot_improvement": "Decommission関連設定を有効化",
        "action_decommission_comment": "Decommission設定（最優先）",
        "action_speculation_comment": "投機的実行（遅いタスクの早期再実行）",
        "action_spot_verify_fmt": "Executorロスト時の遅延時間が大幅短縮（{delay:.0f}分 \u2192 数分以内）",
        "cause_hypothesis": "原因仮説:",
        "verification_metric": "検証指標:",
        "action_small_files": "小ファイル問題の解決（Delta テーブル最適化）",
        "action_small_files_priority": "優先度: 9/10",
        "action_sf_detected_fmt": "{count}ステージで小ファイル問題検出（合計実行時間: {dur:,.0f}秒）",
        "action_sf_ipt_fmt": "タスクあたり読み取り量: {min:.1f}〜{max:.1f}MB（目標: 128MB）",
        "action_sf_stage_fmt": "最も影響の大きいステージ: Stage {sid}（{dur:.1f}秒）",
        "action_sf_hypothesis": "Delta テーブルのファイルが細分化されており、大量の小さなファイルによるメタデータオーバーヘッドとタスク起動コストが処理効率を低下させている。",
        "action_sf_improvement": "Delta テーブルの最適化とファイルサイズ制御",
        "action_sf_comment_optimize": "既存テーブルの最適化",
        "action_sf_comment_auto": "書き込み時の自動最適化設定",
        "action_sf_verify": "タスクあたり読み取り量が128MB以上に改善、小ファイル検出ステージ数の削減",
        "action_data_skew": "データスキューの解決（AQE スキュー結合の最適化）",
        "action_ds_priority": "優先度: 8/10",
        "action_ds_detected_fmt": "{count}ステージでデータスキュー検出（合計実行時間: {dur:,.0f}秒）",
        "action_ds_max_fmt": "最大スキュー比率: {ratio:.1f}倍（Stage {sid}）",
        "action_ds_hypothesis": "特定のパーティションにデータが集中している。現在のAQE設定では、スキューパーティションの検出・分割が十分に機能していない。",
        "action_ds_improvement": "AQE スキュー結合の設定最適化",
        "action_ds_verify": "タスクスキュー比率が5倍以下に改善、データスキュー検出ステージ数の削減",
        "action_photon": "Photon 利用率向上（非対応オペレータの書き換え）",
        "action_photon_priority": "優先度: 7/10",
        "action_photon_avg_fmt": "平均Photon利用率: {pct:.1f}%（目標: >80%）",
        "action_photon_low_fmt": "Photon率50%未満のSQL: {low}/{total}件",
        "action_photon_causes_fmt": "主要な非対応原因: {causes}",
        "action_photon_hypothesis": "Python UDFやRDD操作の使用により、Photonエンジンが利用できずClassic Sparkにフォールバックしている。",
        "action_photon_improvement": "非対応オペレータの段階的書き換え",
        "action_photon_comment_udf": "Python UDF の書き換え例",
        "action_photon_comment_after": "After: PySparkネイティブ関数",
        "action_photon_comment_rdd": "RDD → DataFrame 書き換え例",
        "action_photon_verify": "平均Photon利用率が80%以上に改善、BatchEvalPython/ExistingRDD検出数の削減",
        "action_photon_enable_runtime": "Photon 対応ランタイム（DBR Photon Runtime）に切り替え、spark.databricks.photon.enabled = true を設定してください",
        "action_shuffle_partitions": "Shuffle パーティション数の最適化",
        "action_sp_priority": "優先度: 6/10",
        "action_sp_under_fmt": "Stage {sid}: タスクあたり{per_task:.1f}MB（目標: 100-200MB）\u2192 パーティション不足",
        "action_sp_over_fmt": "Stage {sid}: タスクあたり{per_task:.1f}MB \u2192 パーティション過剰",
        "action_sp_hypothesis": "デフォルトのshuffle partitionsが処理データ量に対して不適切。",
        "action_sp_improvement": "ステージ特性に応じたパーティション数調整",
        "action_sp_verify": "タスクあたりShuffle Read量が100-200MBの範囲に改善",
        "action_serialization": "シリアライゼーション問題の解決",
        "action_ser_priority": "優先度: 5/10",
        "action_ser_detected_fmt": "{count}件のExecutorでシリアライズ時間が{min:.1f}-{max:.1f}%",
        "action_ser_cause": "主要原因: UDF・RDD使用によるオブジェクトシリアライゼーション",
        "action_ser_hypothesis": "Python UDFやRDD操作により、JVMとPython間のオブジェクトシリアライゼーションが頻発している。",
        "action_ser_improvement": "UDF・RDD使用の削減（Photon対策と連動）",
        "action_ser_comment1": "1. Python UDF → ネイティブ関数（Photon対策と同じ）",
        "action_ser_comment2": "2. RDD → DataFrame API",
        "action_ser_comment3": "3. broadcast変数の活用",
        "action_ser_verify": "Executorあたりシリアライズ時間が5%以下に改善",
        "action_dup_scan": "重複スキャンのキャッシュ戦略",
        "action_dup_priority": "優先度: 4/10",
        "action_dup_scan_fmt": "{tbl}: {count}回スキャン、合計{dur:,.0f}秒",
        "action_dup_cause": "重複スキャンによる無駄な I/O とCPU使用",
        "action_dup_hypothesis": "同一テーブルが複数回読み込まれているが、キャッシュ戦略が適用されていない。",
        "action_dup_improvement": "Spot環境に適したキャッシュ戦略の実装",
        "action_dup_comment_delta": "Spot環境での推奨: Delta テーブル書き出し",
        "action_dup_comment_persist": "または persist(DISK_ONLY) - Photon対応",
        "action_dup_comment_unpersist": "使用後は必ずunpersist",
        "action_dup_verify": "重複スキャン回数の削減、総I/O量の削減",
        "no_major_bottleneck": "重大なボトルネックは検出されませんでした。",
        # Verification checklist
        "verification_checklist": "検証チェックリスト",
        "checklist_desc": "改善実施後に以下の指標を確認してください：",
        "check_spot_fmt": "Executorロスト時の遅延時間（目標: {delay:.0f}分 \u2192 5分以内）",
        "check_sf_count_fmt": "小ファイル検出ステージ数（目標: {count}件 \u2192 5件以下）",
        "check_sf_ipt": "タスクあたり読み取り量（目標: 128MB以上）",
        "check_skew_count_fmt": "データスキュー検出ステージ数（目標: {count}件 \u2192 5件以下）",
        "check_skew_ratio_fmt": "最大タスクスキュー比率（目標: {ratio:.1f}倍 \u2192 5倍以下）",
        "check_photon_fmt": "平均Photon利用率（目標: {pct:.1f}% \u2192 80%以上）",
        "check_photon_ops": "BatchEvalPython/ExistingRDD検出数（目標: 大幅削減）",
        "check_shuffle": "タスクあたりShuffle Read量（目標: 100-200MB）",
        "check_ser_fmt": "Executorシリアライズ時間（目標: {min:.1f}-{max:.1f}% \u2192 5%以下）",
        "check_dup_fmt": "重複スキャン回数（目標: {count}回 \u2192 10回以下）",
        "check_no_issues": "重大な問題は検出されていません",
        # Footer
        "footer": "このレポートはDatabricks Spark Performance Toolkitにより生成されました。",
        # Skew/CPU icons
        "abnormal": "異常",
        "caution": "注意",
        "normal": "正常",
    },
    "en": {
        # Section titles
        "analysis_target": "Analysis Target",
        "cluster_info": "Cluster Information",
        "cluster_name": "Cluster Name",
        "dbr_version": "DBR Version",
        "driver_node_type": "Driver Node Type",
        "worker_node_type": "Worker Node Type",
        "worker_count": "Worker Count",
        "fixed_size": "Fixed Size",
        "autoscale": "Autoscale",
        "region": "Region",
        "app_info": "Application Information",
        "app_name": "Application Name",
        "start_time": "Start Time",
        "end_time": "End Time",
        "total_exec_time": "Total Execution Time",
        "executive_summary": "Executive Summary",
        "performance_alerts": "Performance Alerts",
        "stage_exec_bottleneck_analysis": "Bottleneck Analysis",
        "shuffle_analysis": "Shuffle Analysis",
        "photon_utilization": "Photon Usage Analysis",
        "concurrent_exec_analysis": "Parallel Execution Impact Analysis",
        "io_analysis": "I/O Analysis",
        "executor_resource_analysis": "Executor Resource Analysis",
        "plan_optimization_analysis": "Plan Optimization Time Analysis",
        "recommended_actions": "Recommended Actions (Top Findings)",
        # Executive summary
        "exec_summary_desc": "Summarizes the overall picture, key metrics, 5S bottleneck evaluation, and severity of the target Spark application.",
        "job_overview": "Job Overview",
        "total_jobs": "Total Jobs",
        "success": "Succeeded",
        "failure": "Failed",
        "total_stages": "Total Stages",
        "total_tasks": "Total Tasks",
        "task_cumulative_time": "Task Cumulative Time",
        "hours": "hours",
        "minutes_unit": "min",
        "io_dataflow_overview": "I/O & Data Flow Overview",
        "total_input": "Total Input",
        "total_output": "Total Output",
        "total_shuffle_read": "Total Shuffle Read",
        "total_shuffle_write": "Total Shuffle Write",
        "spill_total": "Spill Total",
        "gc_overhead": "GC Overhead",
        "5s_bottleneck_eval": "5S Bottleneck Evaluation & Severity",
        "severity_level": "Severity Level",
        # 5S labels
        "skew": "Skew",
        "skew_detected_fmt": "{count} detected. Max skew ratio {ratio:.1f}x (Stage {stage})",
        "not_detected": "Not detected",
        "spill_label": "Spill",
        "spill_detected_fmt": "Total spill: {spill}",
        "shuffle_label": "Shuffle",
        "shuffle_heavy_fmt": "{count} HEAVY_SHUFFLE detected. Total shuffle {amount}",
        "shuffle_moderate": "Moderate. Total shuffle: {amount}",
        "shuffle_good": "Good. Total shuffle: {amount}",
        "small_files_label": "Small Files",
        "small_files_detected_fmt": "{count} detected",
        "small_files_not_applicable": "Not applicable",
        "serialization_label": "Serialization",
        "serialization_high_fmt": "{count} Executors with high serialization time ({min:.1f}-{max:.1f}%)",
        "serialization_gc_fmt": "GC overhead: {pct:.1f}%",
        "good": "Good",
        "alert_total_fmt": "Alert total: {high} HIGH, {med} MEDIUM, {low} LOW",
        # Performance alerts
        "alert_section_desc": "This section compares performance metrics against target values and classifies them as warnings, cautions, or informational.",
        "summary": "Summary",
        "warnings_count_fmt": "\u25b2 {w} Warnings | \u25b3 {c} Cautions | \u24d8 {i} Info",
        "warnings_header": "\u25b2 Warnings",
        "cautions_header": "\u25b3 Cautions",
        "infos_header": "\u24d8 Info",
        # Alert detail templates
        "small_files_alert_fmt": "[Small Files] Small file issue detected in {count} stages ({range})",
        "small_files_ipt_fmt": "Per-task read: {min:.1f}-{max:.1f}MB | Target: 128MB",
        "small_files_rec": "Compact files with OPTIMIZE. spark.databricks.delta.optimizeWrite.enabled=true, autoCompact.enabled=auto. Consider enabling Predictive Optimization",
        "data_skew_alert_fmt": "[Data Skew] Data skew detected in {count} stages (Max skew ratio: {ratio:.1f}x | Target: <5x)",
        "data_skew_rec": "Enable AQE skew join: spark.sql.adaptive.skewJoin.enabled=true or use REPARTITION hints to redistribute data",
        "heavy_shuffle_alert_fmt": "[Heavy Shuffle] Heavy shuffle detected in {count} stages ({amount})",
        "heavy_shuffle_rec": "Consider Broadcast Join (spark.sql.autoBroadcastJoinThreshold). Adjust shuffle partition count",
        "generic_alert_fmt": "[{type}] Detected in {count} stages",
        "ser_alert_fmt": "[Serialization] High serialization time in {count} Executors ({min:.1f}-{max:.1f}% | Target: <5%)",
        "ser_alert_rec": "Consider reducing UDF/RDD usage",
        "photon_alert_fmt": "[Photon] Low average Photon utilization ({pct:.1f}% | Target: >80%)",
        "photon_alert_rec": "Rewrite BatchEvalPython (Python UDF) to PySpark native functions",
        "photon_alert_disabled_fmt": "[Photon] Photon is disabled on this cluster (spark.databricks.photon.enabled = false)",
        "photon_alert_disabled_rec": "Consider switching to a Photon-enabled runtime",
        "partition_sizing_alert_fmt": "[Partition Sizing] Under-partitioned in {count} stages (Per-task {min:.1f}-{max:.1f}MB | Target: 100-200MB)",
        "partition_sizing_rec_fmt": "Adjust spark.sql.shuffle.partitions to {count}",
        # Stage execution & bottleneck analysis (merged)
        "stage_exec_desc": "This section shows bottleneck summary, detailed analysis of top 5 bottleneck stages, and top 5 normal stages without bottlenecks.",
        "total_count_fmt": "Total",
        "count_unit": "items",
        "count_unit_fmt": "{count} items (total {dur:,.1f}s)",
        "bn_stages_header": "Bottleneck Stages Detailed Analysis (Top 5, by Execution Time)",
        "bn_detail_fmt": "Stage {sid} (Job {jid}): {dur:.1f}s / {tasks:,} tasks / Bottleneck: {bt} / CPU Efficiency: {cpu:.1f}%",
        "normal_stages_header": "Top Stages by Execution Time (No Bottleneck, Top 5)",
        "normal_detail_fmt": "Stage {sid} (Job {jid}): {dur:.1f}s / {tasks:,} tasks / Bottleneck: None / CPU Efficiency: {cpu:.1f}%",
        "bn_shuffle_input_data": "Shuffle/Input Data",
        "bn_disk_spill": "Disk Spill",
        "bn_memory_spill": "Memory Spill",
        "task_skew_ratio": "Task Skew Ratio",
        "threshold": "Threshold",
        "cpu_efficiency": "CPU Efficiency",
        "exec_time": "Execution Time",
        "task_count": "Task Count",
        "per_task_fmt": "{val:.1f}MB per task",
        "input_data": "Input Data",
        "cause_analysis": "Root Cause Analysis",
        "improvement": "Improvement",
        # Small files cause
        "small_files_cause_fmt": "Per-task read volume of {per_task:.1f}MB is significantly below the target of 128MB, causing high overhead from numerous small files.",
        "small_files_improvement_1": "Compact and cluster files with OPTIMIZE",
        "small_files_improvement_2": "spark.databricks.delta.optimizeWrite.enabled=true",
        "small_files_improvement_3": "spark.databricks.delta.autoCompact.enabled=auto",
        "small_files_improvement_4": "Consider enabling Predictive Optimization",
        # Data skew cause
        "data_skew_cause_fmt": "Task skew ratio of {ratio:.1f}x is high, indicating uneven data distribution. Data is concentrated in specific partitions.",
        "data_skew_improvement_1": "Enable AQE skew join: spark.sql.adaptive.skewJoin.enabled=true",
        "data_skew_improvement_2": "Adjust skew partition factor: spark.sql.adaptive.skewJoin.skewedPartitionFactor=3",
        "data_skew_improvement_3": "Pre-aggregate in CTE to reduce data volume before JOIN",
        "data_skew_improvement_4": "Use explicit repartition() for even data distribution",
        # Heavy shuffle cause
        "heavy_shuffle_cause_fmt": "Large shuffle data ({amount}) is being moved between worker nodes, with per-task Shuffle Read of {per_task:.1f}MB exceeding the 128MB target.",
        "heavy_shuffle_improvement_1": "Consider Broadcast Join: adjust spark.sql.autoBroadcastJoinThreshold",
        "heavy_shuffle_improvement_2_fmt": "Optimize shuffle partition count: spark.sql.shuffle.partitions={count}",
        "heavy_shuffle_improvement_3": "Reduce shuffled data volume (column pruning, pre-filtering)",
        "no_bottleneck_detected": "No bottlenecks were detected.",
        # Shuffle analysis
        "shuffle_analysis_desc": "This section analyzes the volume of data movement (shuffle) between nodes and partition sizing.",
        "appropriate": "Appropriate",
        "high": "High",
        "top3_shuffle_stages": "Top 3 Shuffle-Heavy Stages",
        "partition_sizing": "Partition Sizing",
        "partition_target": "Target",
        "partition_target_val": "100-200MB/task (128MB recommended)",
        "under_partitioned_header": "Under-Partitioned (UNDER_PARTITIONED):",
        "under_partitioned_fmt": "Stage {sid}: {per_task:.1f}MB per task \u2192 Under-partitioned. Recommended partition count: {rec:,}",
        "over_partitioned_header": "Over-Partitioned (OVER_PARTITIONED):",
        "over_partitioned_fmt": "Stage {sid}: {per_task:.1f}MB per task \u2192 Over-partitioned. Recommended partition count: {rec:,}",
        "partition_ok": "No partition sizing issues detected.",
        # Photon analysis
        "photon_desc": "This section analyzes Photon engine utilization. Queries not using Photon fall back to Classic Spark with reduced processing speed.",
        "photon_important_note": "Important: Even with 100% Photon execution, some workload characteristics yield minimal benefit.",
        "photon_cpu_heavy": "Photon accelerates CPU-heavy operations (Join, Aggregation, etc.). Workloads with these characteristics see smaller improvements:",
        "photon_io_bound": "**I/O bound operations**: Simple ETL (Read \u2192 Filter \u2192 Write)",
        "photon_deep_lineage": "**Deep DataFrame Lineage**: withColumn loops \u00d7 hundreds of times",
        "photon_short_query": "**Short queries (< 2s)**: Photon initialization overhead is relatively large",
        "photon_ser_heavy": "**Serialization-heavy**: Python UDF serialization is not improved by Photon",
        "avg_photon_rate": "Average Photon Rate",
        "low": "Low",
        "photon_below_50_fmt": "SQL queries with Photon rate below 50%",
        "photon_detail_desc": "This section prioritizes queries with Photon Explanation (non-support reasons) and shows the top 10. Queries under 0.5 seconds are omitted due to minimal impact.",
        "photon_rate": "Photon Rate",
        "target_tables": "Target Tables",
        "non_photon_ops": "Non-Photon Operators",
        "rewrite_method": "Rewrite Method",
        "no_photon_data": "No Photon data",
        "photon_cluster_disabled": "ℹ Photon is disabled on this cluster (spark.databricks.photon.enabled = false). Enabling Photon can accelerate CPU-heavy operations (Join, Aggregation, etc.). Consider switching to a Photon-enabled runtime.",
        "photon_enabled_but_zero": "⚠ Photon is enabled on this cluster, but Photon utilization is 0% across all SQL executions. All operators are falling back to Classic Spark. Check for unsupported operators (Python UDF, RDD, etc.).",
        "photon_config_unknown": "ℹ Photon cluster configuration is unknown (no config data available). Photon utilization is 0% across all SQL executions. Verify whether the DBR version supports Photon and consider switching to a Photon-enabled runtime.",
        # Photon rewrite suggestions
        "rewrite_udf": "Rewrite BatchEvalPython (Python UDF) to PySpark native functions",
        "rewrite_rdd": "RDD scan is not supported by Photon. Rewrite to read directly from tables using DataFrame API or SparkSQL",
        "rewrite_ctas": "Command operations (CREATE OR REPLACE TABLE AS SELECT) are control flow and not Photon targets, but internal data processing may be executed by Photon",
        "rewrite_cmd_flow": "Command operations are control flow and not Photon targets, but internal data processing may be executed by Photon. Check Photon utilization of subsequent jobs.",
        "non_photon_ops_fmt": "Non-supported operators: {ops}",
        # Concurrent execution analysis
        "concurrent_desc": "This section analyzes the impact of concurrent job execution within the same application and across sessions (cross-app) on performance.",
        "intra_app_concurrency": "Intra-Application Concurrency (Resource Contention within Same Session)",
        "max_concurrent_jobs": "Max Concurrent Jobs",
        "parallel_jobs_fmt": "Jobs with Parallel Execution",
        "all_jobs_fmt": "All {total} Jobs",
        "sequential_info": "All jobs were executed sequentially within the application",
        "cross_app_concurrency": "Cross-Application Concurrency (Resource Contention with Other Sessions)",
        "cross_app_detected_fmt": "Jobs with Cross-App Concurrency Detected",
        "max_cross_concurrent": "Max Concurrent Count (Other App Jobs)",
        "concurrent_apps": "Concurrently Running Apps",
        "cross_app_warning": "Jobs from other sessions were running concurrently on the same cluster. CPU, memory, I/O, and Disk Cache resource contention may be causing Cache Locality loss and increased scheduling overhead.",
        "affected_jobs_example": "Examples of affected jobs:",
        "affected_job_fmt": "Job {jid}: Overlapping jobs {dup}, Execution time {dur:.1f}s",
        "no_concurrency_data": "No concurrency data",
        # Executor resource analysis
        "executor_desc": "This section analyzes resource utilization of each Executor (worker node) and diagnoses resource issues.",
        "avg_gc_overhead": "Average GC Overhead",
        "avg_cpu_efficiency": "Average CPU Efficiency",
        "target_fmt": "Target: {val}",
        "executor_group_analysis": "Executor Group Analysis",
        "high_load_group": "High Load Group",
        "mid_load_group": "Medium Load Group",
        "low_load_group": "Low Load Group",
        "task_count_label": "Tasks",
        "task_time_label": "Task Time",
        "cpu_eff_label": "CPU Efficiency",
        "gc_rate_label": "GC Rate",
        "diagnosis": "Diagnosis",
        "ser_diagnosis_fmt": "SERIALIZATION: Serialization time {min:.1f}-{max:.1f}s ({min_pct:.1f}-{max_pct:.1f}%) \u2192 Consider reducing UDF/RDD usage",
        "ser_diagnosis_pct_fmt": "SERIALIZATION: Serialization time {min:.1f}-{max:.1f}% \u2192 Consider reducing UDF/RDD usage",
        "load_diff_minor_fmt": "Load difference between groups is minor (max diff ~{pct:.0f}%), tasks are evenly distributed across all Executors.",
        "load_diff_skewed_fmt": "Load difference between groups is {pct:.0f}%, indicating uneven task distribution.",
        # Spot / Node loss
        "spot_node_loss": "Spot / Node Loss Analysis",
        "detected_count": "Detected Count",
        "uptime": "Uptime",
        "affected_tasks": "Affected Tasks",
        "failed_tasks": "Failed Tasks",
        "lost_shuffle_data": "Lost Shuffle Data",
        "task_reexec": "Task Re-execution",
        "shuffle_recomp": "Shuffle Recomputation",
        "executor_acquire": "Executor Acquisition",
        "estimated_delay": "Estimated Delay",
        "breakdown": "Breakdown",
        "recommended_settings": "Recommended Settings",
        # Plan optimization analysis
        "plan_opt_desc": "This section analyzes the time consumed by Spark query plan optimization, compilation, and scheduling within job execution time.",
        "app_total_exec_time": "Application Total Execution Time",
        "job_total_exec_time": "Job Total Execution Time",
        "job_gap_overhead": "Inter-Job Overhead",
        "job_internal_overhead": "Intra-Job Overhead (Plan Optimization & Code Generation)",
        "plan_overhead_ok": "Intra-job plan optimization overhead is minimal.",
        "plan_overhead_warn_fmt": "Max plan optimization overhead: {val:.1f}s",
        "job_gap_analysis": "Inter-Job Gap Analysis (Driver Processing Time)",
        "total_gap": "Total Gap",
        "max_gap": "Max Gap",
        "max_gap_fmt": "{gap:.0f}s (between Job {jid} \u2192 Job {next_jid})",
        "avg_gap": "Average Gap",
        "top_gap_pairs": "Job pairs with largest gaps (top 5):",
        "driver_overhead_warn_fmt": "Driver processing time accounts for {pct:.1f}% of application execution time. Consider optimizing Notebook code and reducing Delta metadata operations.",
        "dup_scan_analysis": "Duplicate Scan Analysis & Cache Strategy",
        "dup_scan_desc": "When the same table/path is scanned multiple times, caching strategies can potentially reduce processing time.",
        "scan_count_fmt": "{count} scans / Total execution time: {dur:,.1f}s",
        "photon_cache_priority": "Recommended priority for Photon environments:",
        "cache_opt_1": "**Delta table write** \u2014 Read Photon \u2705 + Spot loss resilience \u2705",
        "cache_opt_2": "**persist(DISK_ONLY)** \u2014 Read Photon \u2705 + Lineage preserved",
        "cache_opt_3": "**cache()** \u2014 Read Photon \u274c but easy. Recomputed from Lineage on Spot loss",
        "cache_opt_4": "**checkpoint()** \u2014 Lineage cut + Spot loss resilience",
        # I/O analysis
        "io_analysis_desc": "This section analyzes scan (table read) I/O patterns.",
        "io_scan_volume_top5": "Scan Volume TOP5 (by Read Size)",
        "io_table": "Table",
        "io_format": "Format",
        "io_storage_path": "Storage Path",
        "io_column_count": "Column Count",
        "io_wide_schema": "Wide Schema",
        "io_filters": "Filters",
        "io_files_read": "Files Read",
        "io_files_pruned": "Files Pruned",
        "io_file_pruning_pct": "File Pruning %",
        "io_files_read_size_mb": "Files Read Size",
        "io_fs_read_size_mb": "FS Read Size",
        "io_cache_hit_pct": "Cache Hit %",
        "io_cache_hit_good": "Good",
        "io_cache_hit_low": "Low",
        "io_cache_write_gb": "Cache Write",
        "io_cache_read_wait_ms": "Cache Read Wait",
        "io_cache_write_wait_ms": "Cache Write Wait",
        "io_scan_time_ms": "Scan Time",
        "io_cloud_request_count": "Cloud Request Count",
        "io_cloud_request_dur_ms": "Cloud Request Duration",
        "io_duration_sec": "Query Execution Time",
        "io_execution_id": "Execution ID",
        "io_dup_scan_analysis": "Duplicate Scan Analysis",
        "io_dup_scan_desc": "When the same table is scanned multiple times, caching strategies can potentially reduce processing time.",
        "io_dup_total_scan_fmt": "Total scan count: {count} / Total execution time: {dur:,.1f}s",
        "io_dup_max_columns": "Max Column Count",
        "io_dup_format_breakdown": "Format Breakdown",
        "io_dup_format_fmt": "{fmt}: {count} times / {dur:.1f}s",
        "io_no_scan_data": "No scan data",
        # Recommended actions
        "actions_priority_header": "Recommended Actions (by Priority)",
        "actions_desc": "High-priority tuning items for performance improvement are listed below.",
        "actions_desc2": "Each item is evaluated by Impact and Effort for prioritization.",
        "action_spot_decommission": "Enable Spot Instance Decommission Settings",
        "action_spot_priority": "Priority: 10/10",
        "action_rationale": "Rationale:",
        "action_spot_detected_fmt": "{count} Executor losses detected (NODE_LOST: {nl}, SPOT_PREEMPTION: {sp})",
        "action_total_delay_fmt": "Total estimated delay: {delay:,.0f}s ({hours:.1f} hours)",
        "action_lost_shuffle_fmt": "Lost shuffle data: {amount}",
        "action_spot_hypothesis": "When Spot instances are preempted or nodes fail, shuffle data is lost and upstream stages require recomputation. With Decommission disabled, shuffle data is not evacuated before Executor loss.",
        "action_spot_improvement": "Enable Decommission-related settings",
        "action_decommission_comment": "Decommission settings (highest priority)",
        "action_speculation_comment": "Speculative execution (early re-execution of slow tasks)",
        "action_spot_verify_fmt": "Executor loss delay significantly reduced ({delay:.0f}min \u2192 within a few minutes)",
        "cause_hypothesis": "Hypothesis:",
        "verification_metric": "Verification Metric:",
        "action_small_files": "Resolve Small File Problem (Delta Table Optimization)",
        "action_small_files_priority": "Priority: 9/10",
        "action_sf_detected_fmt": "Small file issue detected in {count} stages (Total execution time: {dur:,.0f}s)",
        "action_sf_ipt_fmt": "Per-task read volume: {min:.1f}-{max:.1f}MB (Target: 128MB)",
        "action_sf_stage_fmt": "Most impacted stage: Stage {sid} ({dur:.1f}s)",
        "action_sf_hypothesis": "Delta table files are fragmented, causing processing efficiency degradation from metadata overhead and task launch costs of numerous small files.",
        "action_sf_improvement": "Delta table optimization and file size control",
        "action_sf_comment_optimize": "Optimize existing tables",
        "action_sf_comment_auto": "Auto-optimization settings for writes",
        "action_sf_verify": "Per-task read volume improved to 128MB+, reduction in small file detection stages",
        "action_data_skew": "Resolve Data Skew (AQE Skew Join Optimization)",
        "action_ds_priority": "Priority: 8/10",
        "action_ds_detected_fmt": "Data skew detected in {count} stages (Total execution time: {dur:,.0f}s)",
        "action_ds_max_fmt": "Max skew ratio: {ratio:.1f}x (Stage {sid})",
        "action_ds_hypothesis": "Data is concentrated in specific partitions. Current AQE settings are not effectively detecting/splitting skewed partitions.",
        "action_ds_improvement": "AQE skew join configuration optimization",
        "action_ds_verify": "Task skew ratio improved to 5x or less, reduction in data skew detection stages",
        "action_photon": "Improve Photon Utilization (Rewrite Non-Supported Operators)",
        "action_photon_priority": "Priority: 7/10",
        "action_photon_avg_fmt": "Average Photon utilization: {pct:.1f}% (Target: >80%)",
        "action_photon_low_fmt": "SQL with Photon rate below 50%: {low}/{total}",
        "action_photon_causes_fmt": "Primary non-support causes: {causes}",
        "action_photon_hypothesis": "Python UDF and RDD operations prevent Photon engine usage, falling back to Classic Spark.",
        "action_photon_improvement": "Gradual rewrite of non-supported operators",
        "action_photon_comment_udf": "Python UDF rewrite example",
        "action_photon_comment_after": "After: PySpark native functions",
        "action_photon_comment_rdd": "RDD → DataFrame rewrite example",
        "action_photon_verify": "Average Photon utilization improved to 80%+, reduction in BatchEvalPython/ExistingRDD detections",
        "action_photon_enable_runtime": "Switch to a Photon-enabled runtime (DBR Photon Runtime) and set spark.databricks.photon.enabled = true",
        "action_shuffle_partitions": "Optimize Shuffle Partition Count",
        "action_sp_priority": "Priority: 6/10",
        "action_sp_under_fmt": "Stage {sid}: {per_task:.1f}MB per task (Target: 100-200MB) \u2192 Under-partitioned",
        "action_sp_over_fmt": "Stage {sid}: {per_task:.1f}MB per task \u2192 Over-partitioned",
        "action_sp_hypothesis": "Default shuffle partitions are inappropriate for the data volume being processed.",
        "action_sp_improvement": "Adjust partition count based on stage characteristics",
        "action_sp_verify": "Per-task Shuffle Read volume improved to 100-200MB range",
        "action_serialization": "Resolve Serialization Issues",
        "action_ser_priority": "Priority: 5/10",
        "action_ser_detected_fmt": "{count} Executors with serialization time at {min:.1f}-{max:.1f}%",
        "action_ser_cause": "Primary cause: Object serialization from UDF/RDD usage",
        "action_ser_hypothesis": "Python UDF and RDD operations cause frequent object serialization between JVM and Python.",
        "action_ser_improvement": "Reduce UDF/RDD usage (aligned with Photon improvements)",
        "action_ser_comment1": "1. Python UDF → Native functions (same as Photon improvement)",
        "action_ser_comment2": "2. RDD → DataFrame API",
        "action_ser_comment3": "3. Leverage broadcast variables",
        "action_ser_verify": "Per-Executor serialization time improved to 5% or less",
        "action_dup_scan": "Implement Cache Strategy for Duplicate Scans",
        "action_dup_priority": "Priority: 4/10",
        "action_dup_scan_fmt": "{tbl}: {count} scans, total {dur:,.0f}s",
        "action_dup_cause": "Wasted I/O and CPU usage from duplicate scans",
        "action_dup_hypothesis": "The same table is being read multiple times without a caching strategy applied.",
        "action_dup_improvement": "Implement caching strategy suitable for Spot environments",
        "action_dup_comment_delta": "Recommended for Spot environments: Delta table write",
        "action_dup_comment_persist": "Or persist(DISK_ONLY) - Photon compatible",
        "action_dup_comment_unpersist": "Always unpersist after use",
        "action_dup_verify": "Reduction in duplicate scan count and total I/O volume",
        "no_major_bottleneck": "No major bottlenecks were detected.",
        # Verification checklist
        "verification_checklist": "Verification Checklist",
        "checklist_desc": "Please verify the following metrics after implementing improvements:",
        "check_spot_fmt": "Executor loss delay time (Target: {delay:.0f}min \u2192 within 5 min)",
        "check_sf_count_fmt": "Small file detection stage count (Target: {count} \u2192 5 or less)",
        "check_sf_ipt": "Per-task read volume (Target: 128MB+)",
        "check_skew_count_fmt": "Data skew detection stage count (Target: {count} \u2192 5 or less)",
        "check_skew_ratio_fmt": "Max task skew ratio (Target: {ratio:.1f}x \u2192 5x or less)",
        "check_photon_fmt": "Average Photon utilization (Target: {pct:.1f}% \u2192 80%+)",
        "check_photon_ops": "BatchEvalPython/ExistingRDD detection count (Target: significant reduction)",
        "check_shuffle": "Per-task Shuffle Read volume (Target: 100-200MB)",
        "check_ser_fmt": "Per-Executor serialization time (Target: {min:.1f}-{max:.1f}% \u2192 5% or less)",
        "check_dup_fmt": "Duplicate scan count (Target: {count} \u2192 10 or less)",
        "check_no_issues": "No major issues detected",
        # Footer
        "footer": "This report was generated by the Databricks Spark Performance Toolkit.",
        # Skew/CPU icons
        "abnormal": "Abnormal",
        "caution": "Caution",
        "normal": "Normal",
    },
}


def _L(key: str, lang: str) -> str:
    """Return the label for *key* in the given language."""
    return _LABELS.get(lang, _LABELS["ja"]).get(key, key)


def _n(v, default=0):
    """Coerce None to default numeric value."""
    return v if v is not None else default


def _fmt_ms(ms) -> str:
    """Format milliseconds to human-readable time."""
    ms = _n(ms)
    if ms >= 60000:
        return f"{ms / 60000:.1f} min"
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    return f"{ms:.0f}ms" if ms else "0s"


def _fmt_mb(mb) -> str:
    """Format MB to human-readable size."""
    mb = _n(mb)
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    return f"{mb:.0f} MB" if mb else "0 MB"


def _fmt_gb(gb) -> str:
    gb = _n(gb)
    return f"{gb:.2f} GB" if gb else "0 GB"


def _severity_icon(severity: str | None) -> str:
    s = (severity or "").upper()
    if s == "HIGH" or s == "CRITICAL":
        return "\U0001f534"
    if s == "MEDIUM":
        return "\u26a0"
    if s == "LOW" or s == "INFO":
        return "\u2139"
    return "\u2705"


def _skew_icon(ratio: float, lang: str = "ja") -> tuple[str, str]:
    """Return (icon, label) for task skew ratio."""
    if ratio > 5:
        return ("\U0001f534", _L("abnormal", lang))
    if ratio > 3:
        return ("\u26a0", _L("caution", lang))
    return ("\u2705", _L("normal", lang))


def _cpu_icon(pct: float, lang: str = "ja") -> tuple[str, str]:
    """Return (icon, label) for CPU efficiency."""
    if pct < 70:
        return ("\u26a0", _L("low", lang))
    return ("\u2705", _L("good", lang))


def _photon_rewrite_suggestion(op_list: str, lang: str = "ja") -> str:
    """Categorize non-photon operators and return rewrite suggestion."""
    ops = (op_list or "").strip()
    if not ops:
        return ""
    if "BatchEvalPython" in ops:
        return _L("rewrite_udf", lang)
    if "ExistingRDD" in ops or "Scan ExistingRDD" in ops:
        return _L("rewrite_rdd", lang)
    if "AtomicReplaceTableAsSelect" in ops:
        return _L("rewrite_ctas", lang)
    return _L("non_photon_ops_fmt", lang).format(ops=ops)


def _is_photon_enabled(spark_config: list[dict[str, Any]]) -> bool | None:
    """Check if Photon is enabled from spark config analysis data.

    Returns True if enabled, False if disabled, None if unknown (no config data).
    """
    for row in spark_config:
        if row.get("config_key") == "spark.databricks.photon.enabled":
            actual = (row.get("actual_value") or "").strip().lower()
            if actual == "true":
                return True
            if actual == "false":
                return False
            return None
    return None


def _ms_to_sec(ms) -> float:
    return float(_n(ms)) / 1000.0


def _ms_to_min(ms) -> float:
    return float(_n(ms)) / 60000.0


def generate_spark_perf_report(
    summary: dict[str, Any] | None = None,
    bottlenecks: list[dict[str, Any]] | None = None,
    stages: list[dict[str, Any]] | None = None,
    executors: list[dict[str, Any]] | None = None,
    jobs: list[dict[str, Any]] | None = None,
    concurrency: list[dict[str, Any]] | None = None,
    sql_photon: list[dict[str, Any]] | None = None,
    spot: list[dict[str, Any]] | None = None,
    narrative: dict[str, str] | None = None,
    spark_config: list[dict[str, Any]] | None = None,
    lang: str = "ja",
    skip_actions: bool = False,
) -> str:
    """Generate Markdown report from Spark Perf Gold table data."""
    summary = summary or {}
    bottlenecks = bottlenecks or []
    stages = stages or []
    executors = executors or []
    jobs = jobs or []
    concurrency = concurrency or []
    sql_photon = sql_photon or []
    spot = spot or []
    spark_config = spark_config or []

    parts: list[str] = []

    # =====================================================================
    # 分析対象
    # =====================================================================
    parts.append(f"# {_L('analysis_target', lang)}\n")

    parts.append(f"## {_L('cluster_info', lang)}\n")
    cluster_fields = [
        ("Cluster ID", "cluster_id"),
        (_L("cluster_name", lang), "cluster_name"),
        (_L("dbr_version", lang), "dbr_version"),
        (_L("driver_node_type", lang), "driver_node_type"),
        (_L("worker_node_type", lang), "worker_node_type"),
    ]
    for label, key in cluster_fields:
        val = summary.get(key, "")
        if val:
            parts.append(f"- **{label}**: {val}\n")

    min_w = summary.get("min_workers")
    max_w = summary.get("max_workers")
    if min_w is not None or max_w is not None:
        if min_w == max_w or max_w is None:
            parts.append(
                f"- **{_L('worker_count', lang)}**: {_n(min_w)}（{_L('fixed_size', lang)}）\n"
            )
        else:
            parts.append(
                f"- **{_L('worker_count', lang)}**: {_n(min_w)}〜{_n(max_w)}（{_L('autoscale', lang)}）\n"
            )

    avail = summary.get("cluster_availability", "")
    if avail:
        parts.append(f"- **Availability**: {avail}\n")
    region = summary.get("region", "")
    if region:
        parts.append(f"- **{_L('region', lang)}**: {region}\n")
    parts.append("")

    parts.append(f"## {_L('app_info', lang)}\n")
    app_fields = [
        ("App ID", "app_id"),
        (_L("app_name", lang), "app_name"),
        (_L("start_time", lang), "start_ts"),
        (_L("end_time", lang), "end_ts"),
    ]
    for label, key in app_fields:
        val = summary.get(key, "")
        if val:
            parts.append(f"- **{label}**: {val}\n")
    dur_min = summary.get("duration_min", 0)
    if dur_min:
        parts.append(
            f"- **{_L('total_exec_time', lang)}**: {dur_min:.1f}{_L('minutes_unit', lang)}\n"
        )
    spark_user = summary.get("spark_user", "")
    if spark_user:
        parts.append(f"- **Spark User**: {spark_user}\n")
    parts.append("")

    # =====================================================================
    # 1. エグゼクティブサマリー
    # =====================================================================
    parts.append("---\n")
    parts.append(f"# 1. {_L('executive_summary', lang)}\n")

    if narrative and narrative.get("summary_text"):
        import re

        text = narrative["summary_text"]
        # Extract Section 1 content from narrative.
        # Matches both old "Executive Summary" and new "ボトルネック分析サマリー/Bottleneck Analysis Summary".
        exec_match = re.search(
            r"(?:^|\n)#+ *1[.\s].*?(?:エグゼクティブサマリー|Executive Summary|ボトルネック分析サマリー|Bottleneck Analysis Summary).*?\n(.*?)(?=\n#+ *(?:2[.\s]|\d+[.\s])|$)",
            text,
            flags=re.DOTALL,
        )
        if exec_match:
            text = exec_match.group(1).strip()
        else:
            # Fallback: strip report title, executive summary preamble, and sections 2+
            text = re.sub(
                r"^#+ .*(?:分析対象|Analysis Target).*?(?=^#+ .*(?:エグゼクティブサマリー|Executive Summary|ボトルネック分析サマリー|Bottleneck Analysis Summary)|$)",
                "",
                text,
                flags=re.MULTILINE | re.DOTALL,
            )
            text = re.sub(
                r"^#+ .*(?:エグゼクティブサマリー|Executive Summary|ボトルネック分析サマリー|Bottleneck Analysis Summary).*\n*",
                "",
                text,
                flags=re.MULTILINE,
            )
            text = re.sub(
                r"^#+ .*(?:Spark.*パフォーマンス|Spark.*Performance).*\n*",
                "",
                text,
                flags=re.MULTILINE,
            )
            # Remove sections 2+ and Appendix if present
            text = re.sub(r"\n#+ *(?:\d+[.\s]|Appendix).*$", "", text, flags=re.DOTALL)
            text = text.strip()
        if text:
            parts.append(text)
            parts.append("")
    else:
        parts.append(f"{_L('exec_summary_desc', lang)}\n")

        # Job overview
        parts.append(f"## {_L('job_overview', lang)}\n")
        total_jobs = _n(summary.get("total_jobs", 0))
        succeeded = _n(summary.get("succeeded_jobs", 0))
        failed = _n(summary.get("failed_jobs", 0))
        total_stages = _n(summary.get("total_stages", 0))
        completed_stages = _n(summary.get("completed_stages", 0))
        failed_stages = _n(summary.get("failed_stages", 0))
        total_tasks = _n(summary.get("total_tasks", 0))
        total_exec_ms = _n(summary.get("total_exec_run_ms", 0))

        parts.append(
            f"- **{_L('total_jobs', lang)}**: {total_jobs}（{_L('success', lang)}: {succeeded} / {_L('failure', lang)}: {failed}）\n"
        )
        stages_line = (
            f"- **{_L('total_stages', lang)}**: {total_stages}（Completed: {completed_stages}"
        )
        if failed_stages:
            stages_line += f" / Failed: {failed_stages}"
        skipped = total_stages - completed_stages - failed_stages
        if skipped > 0:
            stages_line += f" / Skipped: {skipped}"
        stages_line += "）\n"
        parts.append(stages_line)
        parts.append(f"- **{_L('total_tasks', lang)}**: {total_tasks:,}\n")
        if total_exec_ms:
            hours = total_exec_ms / 3_600_000
            parts.append(
                f"- **{_L('task_cumulative_time', lang)}**: {hours:.1f}{_L('hours', lang)}\n"
            )
        parts.append(
            f"- **{_L('total_exec_time', lang)}**: {dur_min:.1f}{_L('minutes_unit', lang)}\n"
        )
        parts.append("")

        # I/O overview
        parts.append(f"## {_L('io_dataflow_overview', lang)}\n")
        total_input = _n(summary.get("total_input_gb", 0))
        total_output = _n(summary.get("total_output_gb", 0))
        total_shuffle_r = _n(summary.get("total_shuffle_gb", 0))
        total_shuffle_w = _n(
            summary.get("total_shuffle_write_gb", summary.get("total_shuffle_gb", 0))
        )
        total_spill = _n(summary.get("total_spill_gb", 0))
        gc_pct = _n(summary.get("gc_overhead_pct", 0))

        parts.append(f"- **{_L('total_input', lang)}**: {_fmt_gb(total_input)}\n")
        if total_output:
            parts.append(f"- **{_L('total_output', lang)}**: {_fmt_gb(total_output)}\n")
        parts.append(f"- **{_L('total_shuffle_read', lang)}**: {_fmt_gb(total_shuffle_r)}\n")
        parts.append(f"- **{_L('total_shuffle_write', lang)}**: {_fmt_gb(total_shuffle_w)}\n")
        parts.append(f"- **{_L('spill_total', lang)}**: {_fmt_gb(total_spill)}\n")
        parts.append(f"- **{_L('gc_overhead', lang)}**: {gc_pct:.1f}%\n")
        parts.append("")

        # 5S evaluation
        parts.append(f"## {_L('5s_bottleneck_eval', lang)}\n")

        # Collect bottleneck type info
        bn_by_type: dict[str, list[dict]] = {}
        for b in bottlenecks:
            bt = (b.get("bottleneck_type") or "").upper()
            bn_by_type.setdefault(bt, []).append(b)

        skew_list = bn_by_type.get("DATA_SKEW", [])
        spill_stages_list = [s for s in stages if _n(s.get("disk_spill_mb", 0)) > 0]
        shuffle_list = bn_by_type.get("HEAVY_SHUFFLE", [])
        small_files_list = bn_by_type.get("SMALL_FILES", [])

        # Serialization check from executors
        high_ser_execs = [e for e in executors if _n(e.get("serialization_pct", 0)) > 5]

        # Determine overall severity
        high_bn = [
            b for b in bottlenecks if (b.get("severity") or "").upper() in ("HIGH", "CRITICAL")
        ]
        if high_bn or failed > 0:
            sev_label = "HIGH"
        elif bottlenecks:
            sev_label = "MEDIUM"
        else:
            sev_label = "LOW"
        parts.append(f"**{_L('severity_level', lang)}: {sev_label}**\n")

        # Skew
        if skew_list:
            max_skew = max(_n(b.get("task_skew_ratio", 0)) for b in skew_list)
            max_skew_stage = max(skew_list, key=lambda b: _n(b.get("task_skew_ratio", 0)))
            parts.append(
                f"- **{_L('skew', lang)}**: 【MEDIUM】 "
                + _L("skew_detected_fmt", lang).format(
                    count=len(skew_list), ratio=max_skew, stage=max_skew_stage.get("stage_id", "?")
                )
                + "\n"
            )
        else:
            parts.append(f"- **{_L('skew', lang)}**: \u2705 {_L('not_detected', lang)}\n")

        # Spill
        if total_spill > 0 or spill_stages_list:
            parts.append(
                f"- **{_L('spill_label', lang)}**: 【MEDIUM】 "
                + _L("spill_detected_fmt", lang).format(spill=_fmt_gb(total_spill))
                + "\n"
            )
        else:
            parts.append(f"- **{_L('spill_label', lang)}**: \u2705 {_L('not_detected', lang)}\n")

        # Shuffle
        if shuffle_list:
            parts.append(
                f"- **{_L('shuffle_label', lang)}**: 【LOW】 "
                + _L("shuffle_heavy_fmt", lang).format(
                    count=len(shuffle_list), amount=_fmt_gb(total_shuffle_r)
                )
                + "\n"
            )
        elif total_shuffle_r > 1:
            parts.append(
                f"- **{_L('shuffle_label', lang)}**: \u26a0 "
                + _L("shuffle_moderate", lang).format(amount=_fmt_gb(total_shuffle_r))
                + "\n"
            )
        else:
            parts.append(
                f"- **{_L('shuffle_label', lang)}**: \u2705 "
                + _L("shuffle_good", lang).format(amount=_fmt_gb(total_shuffle_r))
                + "\n"
            )

        # Small Files
        if small_files_list:
            parts.append(
                f"- **{_L('small_files_label', lang)}**: 【MEDIUM】 "
                + _L("small_files_detected_fmt", lang).format(count=len(small_files_list))
                + "\n"
            )
        elif total_input == 0:
            parts.append(
                f"- **{_L('small_files_label', lang)}**: \u2139 {_L('small_files_not_applicable', lang)}\n"
            )
        else:
            parts.append(
                f"- **{_L('small_files_label', lang)}**: \u2705 {_L('not_detected', lang)}\n"
            )

        # Serialization
        if high_ser_execs:
            ser_pcts = [_n(e.get("serialization_pct", 0)) for e in high_ser_execs]
            parts.append(
                f"- **{_L('serialization_label', lang)}**: 【MEDIUM】 "
                + _L("serialization_high_fmt", lang).format(
                    count=len(high_ser_execs), min=min(ser_pcts), max=max(ser_pcts)
                )
                + "\n"
            )
        elif gc_pct > 5:
            parts.append(
                f"- **{_L('serialization_label', lang)}**: \u26a0 "
                + _L("serialization_gc_fmt", lang).format(pct=gc_pct)
                + "\n"
            )
        else:
            parts.append(f"- **{_L('serialization_label', lang)}**: \u2705 {_L('good', lang)}\n")
        parts.append("")

        # Alert count summary
        sev_counts: dict[str, int] = {}
        for b in bottlenecks:
            s = (b.get("severity") or "UNKNOWN").upper()
            sev_counts[s] = sev_counts.get(s, 0) + 1
        high_c = sev_counts.get("HIGH", 0) + sev_counts.get("CRITICAL", 0)
        med_c = sev_counts.get("MEDIUM", 0)
        low_c = sev_counts.get("LOW", 0) + sev_counts.get("INFO", 0)
        parts.append(_L("alert_total_fmt", lang).format(high=high_c, med=med_c, low=low_c) + "\n")
    parts.append("")

    # Performance alerts (integrated into 5S bottleneck evaluation)
    parts.append(f"### {_L('performance_alerts', lang)}\n")

    # Classify bottlenecks
    warnings: list[tuple[str, str]] = []  # (category, detail + recommendation)
    cautions: list[tuple[str, str]] = []
    infos: list[tuple[str, str]] = []

    # Group bottlenecks by type for alert generation
    bn_type_groups: dict[str, list[dict]] = {}
    for b in bottlenecks:
        bt = (b.get("bottleneck_type") or "UNKNOWN").upper()
        bn_type_groups.setdefault(bt, []).append(b)

    for bt, items in sorted(bn_type_groups.items()):
        sev_set = {(b.get("severity") or "").upper() for b in items}
        total_dur = sum(_n(b.get("duration_ms", 0)) for b in items) / 1000
        count = len(items)

        if bt == "SMALL_FILES":
            # Calculate per-task input
            input_per_task_vals = []
            for b in items:
                nt = _n(b.get("num_tasks", 1))
                inp = _n(b.get("input_mb", 0))
                if nt > 0 and inp > 0:
                    input_per_task_vals.append(inp / nt)
            ipt_range = ""
            if input_per_task_vals:
                ipt_range = _L("small_files_ipt_fmt", lang).format(
                    min=min(input_per_task_vals), max=max(input_per_task_vals)
                )
            detail = _L("small_files_alert_fmt", lang).format(count=count, range=ipt_range)
            rec = _L("small_files_rec", lang)
            if "HIGH" in sev_set or "CRITICAL" in sev_set:
                warnings.append((detail, rec))
            else:
                cautions.append((detail, rec))

        elif bt == "DATA_SKEW":
            max_skew = max(_n(b.get("task_skew_ratio", 0)) for b in items)
            detail = _L("data_skew_alert_fmt", lang).format(count=count, ratio=max_skew)
            rec = _L("data_skew_rec", lang)
            if "HIGH" in sev_set or "CRITICAL" in sev_set:
                warnings.append((detail, rec))
            else:
                cautions.append((detail, rec))

        elif bt == "HEAVY_SHUFFLE":
            total_shuffle_mb = sum(_n(b.get("shuffle_read_mb", 0)) for b in items)
            detail = _L("heavy_shuffle_alert_fmt", lang).format(
                count=count, amount=_fmt_mb(total_shuffle_mb)
            )
            rec = _L("heavy_shuffle_rec", lang)
            if "HIGH" in sev_set or "CRITICAL" in sev_set:
                warnings.append((detail, rec))
            else:
                infos.append((detail, rec))

        else:
            detail = _L("generic_alert_fmt", lang).format(type=bt, count=count)
            rec = items[0].get("recommendation", "") if items else ""
            if "HIGH" in sev_set or "CRITICAL" in sev_set:
                warnings.append((detail, rec))
            elif "MEDIUM" in sev_set:
                cautions.append((detail, rec))
            else:
                infos.append((detail, rec))

    # Serialization alert from executors
    if executors:
        high_ser = [e for e in executors if _n(e.get("serialization_pct", 0)) > 5]
        if high_ser:
            ser_pcts = [_n(e.get("serialization_pct", 0)) for e in high_ser]
            detail = _L("ser_alert_fmt", lang).format(
                count=len(high_ser), min=min(ser_pcts), max=max(ser_pcts)
            )
            rec = _L("ser_alert_rec", lang)
            cautions.append((detail, rec))

    # Photon alert
    if sql_photon:
        avg_photon = sum(_n(s.get("photon_pct", 0)) for s in sql_photon) / max(len(sql_photon), 1)
        if avg_photon < 50:
            photon_enabled = _is_photon_enabled(spark_config)
            if photon_enabled is False:
                detail = _L("photon_alert_disabled_fmt", lang)
                rec = _L("photon_alert_disabled_rec", lang)
            else:
                detail = _L("photon_alert_fmt", lang).format(pct=avg_photon)
                rec = _L("photon_alert_rec", lang)
            cautions.append((detail, rec))

    # Partition sizing alert
    under_part = [
        s
        for s in stages
        if _n(s.get("shuffle_read_mb", 0)) > 0
        and _n(s.get("num_tasks", 1)) > 0
        and (_n(s.get("shuffle_read_mb", 0)) / _n(s.get("num_tasks", 1))) > 200
    ]
    if under_part:
        per_task_vals = [
            _n(s.get("shuffle_read_mb", 0)) / max(_n(s.get("num_tasks", 1)), 1) for s in under_part
        ]
        # Recommend partition count for largest stage
        largest = max(under_part, key=lambda s: _n(s.get("shuffle_read_mb", 0)))
        rec_parts = max(1, int(_n(largest.get("shuffle_read_mb", 0)) / 128))
        detail = _L("partition_sizing_alert_fmt", lang).format(
            count=len(under_part), min=min(per_task_vals), max=max(per_task_vals)
        )
        rec = _L("partition_sizing_rec_fmt", lang).format(count=rec_parts)
        cautions.append((detail, rec))

    # Summary line
    parts.append(f"## {_L('summary', lang)}\n")
    parts.append(
        _L("warnings_count_fmt", lang).format(w=len(warnings), c=len(cautions), i=len(infos)) + "\n"
    )

    if warnings:
        parts.append(f"\n## {_L('warnings_header', lang)}\n")
        for detail, rec in warnings:
            parts.append(f"- {detail}\n")
            if rec:
                parts.append(f"  - {rec}\n")

    if cautions:
        parts.append(f"\n## {_L('cautions_header', lang)}\n")
        for detail, rec in cautions:
            parts.append(f"- {detail}\n")
            if rec:
                parts.append(f"  - {rec}\n")

    if infos:
        parts.append(f"\n## {_L('infos_header', lang)}\n")
        for detail, rec in infos:
            parts.append(f"- {detail}\n")
            if rec:
                parts.append(f"  - {rec}\n")
    parts.append("")

    # =====================================================================
    # 2. 推奨アクション（ボトルネック分析統合）
    # =====================================================================
    parts.append("---\n")
    parts.append(f"# 2. {_L('recommended_actions', lang)}\n")
    parts.append(f"{_L('stage_exec_desc', lang)}\n")

    # Bottleneck stages (exclude OK/SKIPPED)
    bn_stages = [
        s
        for s in stages
        if s.get("bottleneck_type") and s.get("bottleneck_type") not in ("OK", "SKIPPED")
    ]
    bn_stages_sorted = sorted(bn_stages, key=lambda s: _n(s.get("duration_ms", 0)), reverse=True)

    # Normal stages (no bottleneck, with tasks)
    normal_stages = sorted(
        [
            s
            for s in stages
            if (not s.get("bottleneck_type") or s.get("bottleneck_type") in ("OK",))
            and _n(s.get("num_tasks", 0)) > 0
            and s.get("status") != "SKIPPED"
        ],
        key=lambda s: _n(s.get("duration_ms", 0)),
        reverse=True,
    )[:5]

    # Type summary — use bottlenecks data (per-stage × severity) for accurate counts,
    # plus Serialization (from executors) and Photon (from sql_photon).
    bn_type_summary: dict[str, tuple[int, str]] = {}  # {type: (count, severity)}
    for b in bottlenecks:
        bt = (b.get("bottleneck_type") or "UNKNOWN").upper()
        sev = (b.get("severity") or "UNKNOWN").upper()
        cnt, prev_sev = bn_type_summary.get(bt, (0, "LOW"))
        sev_rank = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1, "INFO": 0}
        best_sev = sev if sev_rank.get(sev, 0) > sev_rank.get(prev_sev, 0) else prev_sev
        bn_type_summary[bt] = (cnt + 1, best_sev)

    # Add Serialization if detected
    if executors:
        high_ser = [e for e in executors if _n(e.get("serialization_pct", 0)) > 5]
        if high_ser:
            ser_pcts = [_n(e.get("serialization_pct", 0)) for e in high_ser]
            max_ser = max(ser_pcts)
            sev = "HIGH" if max_ser > 20 else "MEDIUM"
            bn_type_summary["SERIALIZATION"] = (len(high_ser), sev)

    # Add Photon if low utilization
    if sql_photon:
        avg_photon_s = sum(_n(s.get("photon_pct", 0)) for s in sql_photon) / max(len(sql_photon), 1)
        if avg_photon_s < 50:
            low_p_count = len([s for s in sql_photon if _n(s.get("photon_pct", 0)) < 50])
            bn_type_summary["PHOTON"] = (low_p_count, "MEDIUM")

    if bn_type_summary:
        parts.append(f"## {_L('summary', lang)}\n")
        sev_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFO": 4}
        for bt, (cnt, sev) in sorted(
            bn_type_summary.items(), key=lambda x: (sev_order.get(x[1][1], 9), -x[1][0])
        ):
            unit = _L("count_unit", lang)
            sep = "" if lang == "ja" else " "
            parts.append(f"- **{bt}** ({sev}): {cnt}{sep}{unit}\n")
        total_bn = sum(c for c, _ in bn_type_summary.values())
        parts.append(f"- **{_L('total_count_fmt', lang)}**: {total_bn}\n")
        parts.append("")

    # Top 5 bottleneck stages with full detail analysis
    if bn_stages_sorted:
        parts.append(f"## {_L('bn_stages_header', lang)}\n")
        for idx, s in enumerate(bn_stages_sorted[:5], 1):
            bt = s.get("bottleneck_type", "UNKNOWN")
            stage_id = s.get("stage_id", "?")
            job_id = s.get("job_id", "?")
            dur_sec = _n(s.get("duration_ms", 0)) / 1000
            num_tasks = _n(s.get("num_tasks", 0))
            skew_ratio = _n(s.get("task_skew_ratio", 0))
            cpu_eff = _n(s.get("cpu_efficiency_pct", 0))
            shuffle_r = _n(s.get("shuffle_read_mb", 0))
            shuffle_w = _n(s.get("shuffle_write_mb", 0))
            input_mb = _n(s.get("input_mb", 0))
            disk_spill = _n(s.get("disk_spill_mb", 0))
            mem_spill = _n(s.get("memory_spill_mb", 0))

            parts.append(f"### {idx}. {bt} \u2014 Stage {stage_id}（Job {job_id}）\n")

            parts.append(f"- **{_L('exec_time', lang)}**: {dur_sec:.1f}s\n")
            parts.append(f"- **{_L('task_count', lang)}**: {num_tasks:,}\n")

            # CPU efficiency
            cpu_ic, cpu_label = _cpu_icon(cpu_eff, lang)
            parts.append(
                f"- **{_L('cpu_efficiency', lang)}**: {cpu_eff:.1f}%（{_L('threshold', lang)}: >70%）\u2014 {cpu_ic} {cpu_label}\n"
            )

            # Skew ratio with threshold evaluation
            skew_ic, skew_label = _skew_icon(skew_ratio, lang)
            parts.append(
                f"- **{_L('task_skew_ratio', lang)}**: {skew_ratio:.1f}x（{_L('threshold', lang)}: <5）\u2014 {skew_ic} {skew_label}\n"
            )

            # Shuffle/Input data
            if shuffle_r > 0:
                parts.append(f"- **Shuffle Read**: {_fmt_mb(shuffle_r)}")
                if num_tasks > 0:
                    parts.append(
                        f"（{_L('per_task_fmt', lang).format(val=shuffle_r / num_tasks)}）"
                    )
                parts.append("\n")
            if shuffle_w > 0:
                parts.append(f"- **Shuffle Write**: {_fmt_mb(shuffle_w)}\n")
            if input_mb > 0:
                parts.append(f"- **{_L('input_data', lang)}**: {_fmt_mb(input_mb)}")
                if num_tasks > 0:
                    parts.append(f"（{_L('per_task_fmt', lang).format(val=input_mb / num_tasks)}）")
                parts.append("\n")

            # Disk / Memory spill
            if disk_spill > 0:
                parts.append(f"- **{_L('bn_disk_spill', lang)}**: {_fmt_mb(disk_spill)}\n")
            if mem_spill > 0:
                parts.append(f"- **{_L('bn_memory_spill', lang)}**: {_fmt_mb(mem_spill)}\n")
            parts.append("")

            # Root cause analysis paragraph
            if bt == "SMALL_FILES":
                per_task = input_mb / max(num_tasks, 1) if input_mb > 0 else 0
                parts.append(
                    f"**{_L('cause_analysis', lang)}**: {_L('small_files_cause_fmt', lang).format(per_task=per_task)}\n"
                )
                parts.append(f"\n**{_L('improvement', lang)}**: \n")
                parts.append(f"- {_L('small_files_improvement_1', lang)}\n")
                parts.append(f"- {_L('small_files_improvement_2', lang)}\n")
                parts.append(f"- {_L('small_files_improvement_3', lang)}\n")
                parts.append(f"- {_L('small_files_improvement_4', lang)}\n")

            elif bt == "DATA_SKEW":
                parts.append(
                    f"**{_L('cause_analysis', lang)}**: {_L('data_skew_cause_fmt', lang).format(ratio=skew_ratio)}\n"
                )
                parts.append(f"\n**{_L('improvement', lang)}**: \n")
                parts.append(f"- {_L('data_skew_improvement_1', lang)}\n")
                parts.append(f"- {_L('data_skew_improvement_2', lang)}\n")
                parts.append(f"- {_L('data_skew_improvement_3', lang)}\n")
                parts.append(f"- {_L('data_skew_improvement_4', lang)}\n")

            elif bt == "HEAVY_SHUFFLE":
                per_task_sh = shuffle_r / max(num_tasks, 1) if shuffle_r > 0 else 0
                parts.append(
                    f"**{_L('cause_analysis', lang)}**: {_L('heavy_shuffle_cause_fmt', lang).format(amount=_fmt_mb(shuffle_r), per_task=per_task_sh)}\n"
                )
                parts.append(f"\n**{_L('improvement', lang)}**: \n")
                parts.append(f"- {_L('heavy_shuffle_improvement_1', lang)}\n")
                rec_p = max(1, int(shuffle_r / 128))
                parts.append(
                    f"- {_L('heavy_shuffle_improvement_2_fmt', lang).format(count=rec_p)}\n"
                )
                parts.append(f"- {_L('heavy_shuffle_improvement_3', lang)}\n")

            else:
                rec = s.get("recommendation", "")
                if rec:
                    parts.append(f"**{_L('improvement', lang)}**: {rec}\n")
            parts.append("")

    else:
        parts.append(f"{_L('no_bottleneck_detected', lang)}\n")

    # Top 5 normal stages
    if normal_stages:
        parts.append(f"## {_L('normal_stages_header', lang)}\n")
        h_s = "Stage" if lang == "en" else "ステージ"
        h_j = "Job" if lang == "en" else "ジョブ"
        h_d = "Duration" if lang == "en" else "実行時間"
        h_t = "Tasks" if lang == "en" else "タスク数"
        h_c = "CPU Eff" if lang == "en" else "CPU効率"
        parts.append(f"\n| {h_s} | {h_j} | {h_d} | {h_t} | {h_c} |\n")
        parts.append("|-------|-----|---------|-------|--------|\n")
        for s in normal_stages:
            dur_sec = _n(s.get("duration_ms", 0)) / 1000
            parts.append(
                f"| S{s.get('stage_id', '?')} | J{s.get('job_id', '?')} | {dur_sec:.1f}s | {int(_n(s.get('num_tasks', 0))):,} | {_n(s.get('cpu_efficiency_pct', 0)):.1f}% |\n"
            )
        parts.append("")
    parts.append("")

    # =====================================================================
    # 3. Shuffle 分析
    # =====================================================================
    parts.append("---\n")
    parts.append(f"# 3. {_L('shuffle_analysis', lang)}\n")
    parts.append(f"{_L('shuffle_analysis_desc', lang)}\n")

    total_shuffle_r_gb = _n(summary.get("total_shuffle_gb", 0))
    total_shuffle_w_gb = _n(
        summary.get("total_shuffle_write_gb", summary.get("total_shuffle_gb", 0))
    )

    parts.append(f"## {_L('summary', lang)}\n")
    shuffle_ok = (
        f"\u2705 {_L('appropriate', lang)}"
        if total_shuffle_r_gb < 1000
        else f"\u26a0 {_L('high', lang)}"
    )

    metric_col = "Metric" if lang == "en" else "メトリクス"
    value_col = "Value" if lang == "en" else "値"
    status_col = "Status" if lang == "en" else "状態"
    parts.append(f"\n| {metric_col} | {value_col} | {status_col} |\n")
    parts.append("|---------|-------|--------|\n")
    parts.append(
        f"| {_L('total_shuffle_read', lang)} | {_fmt_gb(total_shuffle_r_gb)} | {shuffle_ok} |\n"
    )
    parts.append(
        f"| {_L('total_shuffle_write', lang)} | {_fmt_gb(total_shuffle_w_gb)} | {shuffle_ok} |\n"
    )

    # Top 3 shuffle-heavy stages
    shuffle_stages = sorted(
        [s for s in stages if _n(s.get("shuffle_read_mb", 0)) > 0],
        key=lambda s: _n(s.get("shuffle_read_mb", 0)),
        reverse=True,
    )[:3]
    if shuffle_stages:
        top_str = ", ".join(
            f"S{s.get('stage_id', '?')} ({_fmt_mb(s.get('shuffle_read_mb', 0))})"
            for s in shuffle_stages
        )
        parts.append(f"| {_L('top3_shuffle_stages', lang)} | {top_str} | |\n")
    parts.append("\n")

    # Partition sizing analysis
    parts.append(f"## {_L('partition_sizing', lang)}\n")
    parts.append(f"**{_L('partition_target', lang)}**: {_L('partition_target_val', lang)}\n")

    # Stages with shuffle data for partition analysis
    partition_stages = [
        s for s in stages if _n(s.get("shuffle_read_mb", 0)) > 0 and _n(s.get("num_tasks", 0)) > 0
    ]

    under_partitioned = []
    over_partitioned = []
    for s in partition_stages:
        per_task = _n(s.get("shuffle_read_mb", 0)) / max(_n(s.get("num_tasks", 1)), 1)
        if per_task > 200:
            rec_count = max(1, int(_n(s.get("shuffle_read_mb", 0)) / 128))
            under_partitioned.append((s, per_task, rec_count))
        elif per_task < 10:
            rec_count = max(1, int(_n(s.get("shuffle_read_mb", 0)) / 128))
            over_partitioned.append((s, per_task, rec_count))

    has_partition_issues = under_partitioned or over_partitioned
    if has_partition_issues:
        header_stage = "Stage" if lang == "en" else "ステージ"
        header_judgment = "Judgment" if lang == "en" else "判定"
        header_per_task = "MB/Task" if lang == "en" else "MB/タスク"
        header_current = "Current" if lang == "en" else "現在"
        header_recommended = "Recommended" if lang == "en" else "推奨"
        parts.append(
            f"\n| {header_stage} | {header_judgment} | {header_per_task} | {header_current} | {header_recommended} |\n"
        )
        parts.append("|---------|------|---------|---------|--------|\n")
        for s, per_task, rec_count in sorted(under_partitioned, key=lambda x: -x[1])[:10]:
            sid = s.get("stage_id", "?")
            cur = int(_n(s.get("num_tasks", 0)))
            parts.append(f"| S{sid} | UNDER | {per_task:.1f} | {cur:,} | {rec_count:,} |\n")
        for s, per_task, rec_count in sorted(over_partitioned, key=lambda x: x[1])[:10]:
            sid = s.get("stage_id", "?")
            cur = int(_n(s.get("num_tasks", 0)))
            parts.append(f"| S{sid} | OVER | {per_task:.1f} | {cur:,} | {max(rec_count, 1):,} |\n")
        parts.append("")
    else:
        parts.append(f"\n\u2705 {_L('partition_ok', lang)}\n")
    parts.append("")

    # =====================================================================
    # 4. Photon 利用状況分析
    # =====================================================================
    parts.append("---\n")
    parts.append(f"# 4. {_L('photon_utilization', lang)}\n")
    parts.append(f"{_L('photon_desc', lang)}\n")

    # Determine Photon cluster status from spark_config
    photon_enabled = _is_photon_enabled(spark_config)

    if sql_photon:
        avg_photon = sum(_n(s.get("photon_pct", 0)) for s in sql_photon) / max(len(sql_photon), 1)
        low_photon = [s for s in sql_photon if _n(s.get("photon_pct", 0)) < 50]

        # If avg_photon is 0%, branch based on cluster Photon config
        if avg_photon == 0:
            if photon_enabled is False:
                parts.append(f"\n{_L('photon_cluster_disabled', lang)}\n")
            elif photon_enabled is True:
                parts.append(f"\n{_L('photon_enabled_but_zero', lang)}\n")
            else:
                parts.append(f"\n{_L('photon_config_unknown', lang)}\n")
        else:
            # Photon is being used (avg > 0%) — show full analysis
            parts.append(f"**{_L('photon_important_note', lang)}**\n")
            parts.append(f"{_L('photon_cpu_heavy', lang)}\n")
            parts.append(f"- {_L('photon_io_bound', lang)}\n")
            parts.append(f"- {_L('photon_deep_lineage', lang)}\n")
            parts.append(f"- {_L('photon_short_query', lang)}\n")
            parts.append(f"- {_L('photon_ser_heavy', lang)}\n")
            parts.append("")

            parts.append(f"## {_L('summary', lang)}\n")
            photon_icon = (
                f"\u26a0 {_L('low', lang)}" if avg_photon < 50 else f"\u2705 {_L('good', lang)}"
            )
            parts.append(
                f"- **{_L('avg_photon_rate', lang)}**: {avg_photon:.1f}% \u2014 {photon_icon}\n"
            )
            parts.append(
                f"- **{_L('photon_below_50_fmt', lang)}**: {len(low_photon)} / {len(sql_photon)}\n"
            )
            parts.append("")

            # Top 5 SQL queries by duration, skip < 0.5s
            significant_sql = sorted(
                [s for s in sql_photon if _n(s.get("duration_sec", 0)) >= 0.5],
                key=lambda s: _n(s.get("duration_sec", 0)),
                reverse=True,
            )[:5]

            if significant_sql:
                parts.append(f"{_L('photon_detail_desc', lang)}\n")

                for s in significant_sql:
                    exec_id = s.get("execution_id", "?")
                    dur_sec = _n(s.get("duration_sec", 0))
                    photon_pct = _n(s.get("photon_pct", 0))
                    non_photon_ops = s.get("non_photon_op_list", "")
                    photon_expl = s.get("photon_explanation", "")
                    tables = s.get("target_tables", s.get("scan_tables", ""))

                    parts.append(f"\n### execution_id: {exec_id}\n")
                    parts.append(f"- **{_L('exec_time', lang)}**: {dur_sec:.1f}s\n")
                    parts.append(f"- **{_L('photon_rate', lang)}**: {photon_pct:.1f}%\n")
                    if tables:
                        parts.append(f"- **{_L('target_tables', lang)}**: {tables}\n")
                    if non_photon_ops:
                        parts.append(f"- **{_L('non_photon_ops', lang)}**: {non_photon_ops}\n")
                    if photon_expl:
                        parts.append(f"- **Photon Explanation**: {photon_expl}\n")

                    # Rewrite suggestion
                    suggestion = _photon_rewrite_suggestion(non_photon_ops, lang)
                    if not suggestion and photon_expl:
                        # Try to derive from explanation
                        if "BatchEvalPython" in photon_expl:
                            suggestion = _L("rewrite_udf", lang)
                        elif "ExistingRDD" in photon_expl:
                            suggestion = _L("rewrite_rdd", lang)
                        elif "Commands are not directly photonized" in photon_expl:
                            suggestion = _L("rewrite_cmd_flow", lang)
                    if suggestion:
                        parts.append(f"- **{_L('rewrite_method', lang)}**: {suggestion}\n")
    else:
        parts.append(f"\n{_L('no_photon_data', lang)}\n")
    parts.append("")

    # =====================================================================
    # 5. 並列実行影響分析
    # =====================================================================
    parts.append("---\n")
    parts.append(f"# 5. {_L('concurrent_exec_analysis', lang)}\n")
    parts.append(f"{_L('concurrent_desc', lang)}\n")

    if concurrency:
        # Intra-app concurrency
        parts.append(f"## {_L('intra_app_concurrency', lang)}\n")
        max_conc = max((_n(c.get("concurrent_jobs_at_start", 0)) for c in concurrency), default=0)
        parallel_jobs = [c for c in concurrency if _n(c.get("concurrent_jobs_at_start", 0)) > 0]
        total_conc_jobs = len(concurrency)

        parts.append(f"- **{_L('max_concurrent_jobs', lang)}**: {max_conc}\n")
        parts.append(
            f"- **{_L('parallel_jobs_fmt', lang)}**: {len(parallel_jobs)} / {_L('all_jobs_fmt', lang).format(total=total_conc_jobs)}\n"
        )

        if not parallel_jobs:
            parts.append(f"\n\u2139 {_L('sequential_info', lang)}\n")
        parts.append("")

        # Cross-app concurrency
        cross_app_jobs = [c for c in concurrency if _n(c.get("cross_app_concurrent_jobs", 0)) > 0]
        if cross_app_jobs:
            parts.append(f"## {_L('cross_app_concurrency', lang)}\n")
            max_cross = max(
                (_n(c.get("cross_app_concurrent_jobs", 0)) for c in cross_app_jobs), default=0
            )
            # Collect other app IDs
            other_apps: set[str] = set()
            for c in cross_app_jobs:
                other_app = c.get("other_app_id", "")
                if other_app:
                    other_apps.add(other_app)

            parts.append(
                f"- **{_L('cross_app_detected_fmt', lang)}**: {len(cross_app_jobs)} / {_L('all_jobs_fmt', lang).format(total=total_conc_jobs)}\n"
            )
            parts.append(f"- **{_L('max_cross_concurrent', lang)}**: {max_cross}\n")
            if other_apps:
                parts.append(
                    f"- **{_L('concurrent_apps', lang)}**: {', '.join(sorted(other_apps))}\n"
                )
            parts.append("")

            parts.append(f"\u26a0 {_L('cross_app_warning', lang)}\n")

            # Top affected jobs
            affected_top = sorted(
                cross_app_jobs, key=lambda c: _n(c.get("duration_sec", 0)), reverse=True
            )[:3]
            if affected_top:
                parts.append(f"\n{_L('affected_jobs_example', lang)}\n")
                for c in affected_top:
                    parts.append(
                        "- **"
                        + _L("affected_job_fmt", lang).format(
                            jid=c.get("job_id", "?"),
                            dup=_n(c.get("cross_app_concurrent_jobs", 0)),
                            dur=_n(c.get("duration_sec", 0)),
                        )
                        + "**\n"
                    )
            parts.append("")
    else:
        parts.append(f"{_L('no_concurrency_data', lang)}\n")
    parts.append("")

    # =====================================================================
    # 6. Executor リソース分析
    # =====================================================================
    parts.append("---\n")
    parts.append(f"# 6. {_L('executor_resource_analysis', lang)}\n")
    parts.append(f"{_L('executor_desc', lang)}\n")

    if executors:
        avg_gc = sum(_n(e.get("gc_pct", 0)) for e in executors) / max(len(executors), 1)
        avg_cpu = sum(_n(e.get("cpu_efficiency_pct", 0)) for e in executors) / max(
            len(executors), 1
        )

        parts.append(f"## {_L('summary', lang)}\n")
        gc_icon = f"\u2705 {_L('good', lang)}" if avg_gc < 10 else f"\u26a0 {_L('high', lang)}"
        cpu_icon_str = (
            f"\u2705 {_L('good', lang)}" if avg_cpu >= 70 else f"\u26a0 {_L('low', lang)}"
        )
        parts.append(
            f"- **{_L('avg_gc_overhead', lang)}**: {avg_gc:.1f}% \u2014 {gc_icon}（{_L('target_fmt', lang).format(val='<10%')}）\n"
        )
        parts.append(
            f"- **{_L('avg_cpu_efficiency', lang)}**: {avg_cpu:.1f}% \u2014 {cpu_icon_str}（{_L('target_fmt', lang).format(val='>70%')}）\n"
        )
        parts.append("")

        # Group executors into 3 tiers by total_task_ms
        if len(executors) >= 3:
            sorted_execs = sorted(
                executors, key=lambda e: _n(e.get("total_task_ms", 0)), reverse=True
            )
            n = len(sorted_execs)
            tier_size = max(n // 3, 1)
            high_tier = sorted_execs[:tier_size]
            mid_tier = sorted_execs[tier_size : 2 * tier_size]
            low_tier = sorted_execs[2 * tier_size :]

            parts.append(f"## {_L('executor_group_analysis', lang)}\n")

            for tier_label, tier in [
                (_L("high_load_group", lang), high_tier),
                (_L("mid_load_group", lang), mid_tier),
                (_L("low_load_group", lang), low_tier),
            ]:
                if not tier:
                    continue
                sample_ids = ", ".join(str(e.get("executor_id", "?")) for e in tier[:5])
                task_counts = [_n(e.get("total_tasks", 0)) for e in tier]
                task_times = [_n(e.get("total_task_ms", 0)) / 1000 for e in tier]
                cpu_effs = [_n(e.get("cpu_efficiency_pct", 0)) for e in tier]
                gc_rates = [_n(e.get("gc_pct", 0)) for e in tier]
                disk_spills = [_n(e.get("disk_spill_mb", 0)) for e in tier]
                mem_spills = [_n(e.get("memory_spill_mb", 0)) for e in tier]
                shuffle_writes = [_n(e.get("shuffle_write_gb", 0)) for e in tier]

                parts.append(f"\n#### {tier_label}（Executor {sample_ids}）\n")
                parts.append(
                    f"- **{_L('task_count_label', lang)}**: {min(task_counts):,}〜{max(task_counts):,} / "
                    f"**{_L('task_time_label', lang)}**: {min(task_times):,.0f}〜{max(task_times):,.0f}s / "
                    f"**{_L('cpu_eff_label', lang)}**: {min(cpu_effs):.1f}〜{max(cpu_effs):.1f}%\n"
                )
                parts.append(
                    f"- **{_L('gc_rate_label', lang)}**: {min(gc_rates):.1f}〜{max(gc_rates):.1f}% / "
                    f"**Disk Spill**: {max(disk_spills):.0f} MB / "
                    f"**Memory Spill**: {max(mem_spills):.0f} MB"
                )
                if any(sw > 0 for sw in shuffle_writes):
                    parts.append(
                        f" / **Shuffle Write**: {min(shuffle_writes):.2f}〜{max(shuffle_writes):.2f} GB"
                    )
                parts.append("\n")

                # Serialization diagnosis
                ser_pcts = [_n(e.get("serialization_pct", 0)) for e in tier]
                ser_times = [_n(e.get("serialization_time_sec", 0)) for e in tier]
                if max(ser_pcts) > 5:
                    if any(st > 0 for st in ser_times):
                        parts.append(
                            f"- **{_L('diagnosis', lang)}**: "
                            + _L("ser_diagnosis_fmt", lang).format(
                                min=min(ser_times),
                                max=max(ser_times),
                                min_pct=min(ser_pcts),
                                max_pct=max(ser_pcts),
                            )
                            + "\n"
                        )
                    else:
                        parts.append(
                            f"- **{_L('diagnosis', lang)}**: "
                            + _L("ser_diagnosis_pct_fmt", lang).format(
                                min=min(ser_pcts), max=max(ser_pcts)
                            )
                            + "\n"
                        )

            # Load difference assessment
            if high_tier and low_tier:
                high_avg = sum(_n(e.get("total_task_ms", 0)) for e in high_tier) / len(high_tier)
                low_avg = sum(_n(e.get("total_task_ms", 0)) for e in low_tier) / max(
                    len(low_tier), 1
                )
                if low_avg > 0:
                    diff_pct = ((high_avg - low_avg) / low_avg) * 100
                    if diff_pct < 20:
                        parts.append(
                            "\n" + _L("load_diff_minor_fmt", lang).format(pct=diff_pct) + "\n"
                        )
                    else:
                        parts.append(
                            "\n\u26a0 "
                            + _L("load_diff_skewed_fmt", lang).format(pct=diff_pct)
                            + "\n"
                        )
            parts.append("")

    # Spot / Node Loss (top 5 by estimated delay)
    if spot:
        spot_sorted = sorted(spot, key=lambda s: _n(s.get("estimated_delay_sec", 0)), reverse=True)[
            :5
        ]
        parts.append(f"## {_L('spot_node_loss', lang)}\n")
        parts.append(f"- **{_L('detected_count', lang)}**: {len(spot)}\n")

        for s in spot_sorted:
            removal_type = s.get("removal_type", "UNKNOWN")
            exec_id = s.get("executor_id", "?")
            host = s.get("host", "")
            lifetime = _n(s.get("lifetime_min", 0))
            total_assigned = _n(s.get("total_tasks_assigned", 0))
            failed_tasks = _n(s.get("failed_tasks", 0))
            shuffle_lost = _n(s.get("shuffle_lost_mb", 0))
            est_delay = _n(s.get("estimated_delay_sec", 0))
            task_reexec = _n(s.get("task_reexecution_sec", 0))
            shuffle_recomp = _n(s.get("shuffle_recomputation_sec", 0))
            exec_acquire = _n(s.get("executor_acquire_sec", 0))

            parts.append(f"\n**Executor {exec_id}**: {removal_type}\n")
            parts.append(
                f"- **Host**: {host} / **{_L('uptime', lang)}**: {lifetime:.1f}{_L('minutes_unit', lang)} / "
                f"**{_L('affected_tasks', lang)}**: {total_assigned:,} / **{_L('failed_tasks', lang)}**: {failed_tasks}\n"
            )
            parts.append(f"- **{_L('lost_shuffle_data', lang)}**: {_fmt_mb(shuffle_lost)}\n")
            delay_breakdown = f"{_L('task_reexec', lang)}: {task_reexec:.1f}s"
            if shuffle_recomp > 0:
                delay_breakdown += f" + {_L('shuffle_recomp', lang)}: {shuffle_recomp:.1f}s"
            if exec_acquire > 0:
                delay_breakdown += f" + {_L('executor_acquire', lang)}: {exec_acquire:.1f}s"
            parts.append(
                f"- **{_L('estimated_delay', lang)}**: {est_delay:,.0f}s（{_L('breakdown', lang)}: {delay_breakdown}）\n"
            )

        parts.append(f"\n**{_L('recommended_settings', lang)}**:\n")
        parts.append("- spark.decommission.enabled=true\n")
        parts.append("- spark.storage.decommission.enabled=true\n")
        parts.append("- spark.storage.decommission.shuffleBlocks.enabled=true\n")
        parts.append("- spark.speculation=true\n")
    parts.append("")

    # =====================================================================
    # 7. プラン最適化時間分析
    # =====================================================================
    # Check if we have job gap data from concurrency
    has_gap_data = any(
        c.get("gap_to_next_sec") is not None or c.get("plan_overhead_sec") is not None
        for c in concurrency
    )
    if has_gap_data or (concurrency and jobs):
        parts.append("---\n")
        parts.append(f"# 7. {_L('plan_optimization_analysis', lang)}\n")
        parts.append(f"{_L('plan_opt_desc', lang)}\n")

        app_total_sec = dur_min * 60
        job_total_sec = sum(_n(c.get("duration_sec", 0)) for c in concurrency)
        overhead_sec = app_total_sec - job_total_sec if app_total_sec > job_total_sec else 0

        parts.append(f"## {_L('summary', lang)}\n")
        parts.append(f"- **{_L('app_total_exec_time', lang)}**: {app_total_sec:,.0f}s\n")
        parts.append(f"- **{_L('job_total_exec_time', lang)}**: {job_total_sec:,.0f}s\n")
        parts.append(f"- **{_L('job_gap_overhead', lang)}**: {overhead_sec:,.0f}s\n")
        parts.append("")

        # Job internal overhead
        plan_overheads = [
            _n(c.get("plan_overhead_sec", 0))
            for c in concurrency
            if c.get("plan_overhead_sec") is not None
        ]
        if plan_overheads:
            max_plan = max(plan_overheads)
            parts.append(f"## {_L('job_internal_overhead', lang)}\n")
            if max_plan < 1:
                parts.append(f"\u2705 {_L('plan_overhead_ok', lang)}\n")
            else:
                parts.append(f"\u26a0 {_L('plan_overhead_warn_fmt', lang).format(val=max_plan)}\n")
            parts.append("")

        # Job gap analysis
        gaps = [
            (c.get("job_id", "?"), c.get("next_job_id", "?"), _n(c.get("gap_to_next_sec", 0)))
            for c in concurrency
            if c.get("gap_to_next_sec") is not None and _n(c.get("gap_to_next_sec", 0)) > 0
        ]

        if gaps:
            total_gap = sum(g for _, _, g in gaps)
            max_gap_item = max(gaps, key=lambda x: x[2])
            avg_gap = total_gap / max(len(gaps), 1)

            parts.append(f"## {_L('job_gap_analysis', lang)}\n")
            parts.append(f"- **{_L('total_gap', lang)}**: {total_gap:,.0f}s\n")
            parts.append(
                f"- **{_L('max_gap', lang)}**: {_L('max_gap_fmt', lang).format(gap=max_gap_item[2], jid=max_gap_item[0], next_jid=max_gap_item[1])}\n"
            )
            parts.append(f"- **{_L('avg_gap', lang)}**: {avg_gap:.1f}s\n")
            parts.append("")

            # Top 5 gap pairs
            top_gaps = sorted(gaps, key=lambda x: -x[2])[:5]
            parts.append(f"{_L('top_gap_pairs', lang)}\n")
            for jid, next_jid, gap in top_gaps:
                parts.append(f"- **Job {jid} \u2192 Job {next_jid}**: {gap:.0f}s\n")

            if app_total_sec > 0:
                gap_pct = (total_gap / app_total_sec) * 100
                if gap_pct > 5:
                    parts.append(
                        f"\n\u26a0 {_L('driver_overhead_warn_fmt', lang).format(pct=gap_pct)}\n"
                    )
            parts.append("")
    parts.append("")

    # =====================================================================
    # 8. I/O 分析
    # =====================================================================
    parts.append("---\n")
    parts.append(f"# 8. {_L('io_analysis', lang)}\n")
    parts.append(f"{_L('io_analysis_desc', lang)}\n")

    if sql_photon:
        # --- Scan Volume TOP5 ---
        # Sort by files_read_size_mb descending
        scans_with_size = [s for s in sql_photon if _n(s.get("files_read_size_mb", 0)) > 0]
        scans_sorted = sorted(
            scans_with_size, key=lambda s: _n(s.get("files_read_size_mb", 0)), reverse=True
        )[:5]

        if scans_sorted:
            parts.append(f"## {_L('io_scan_volume_top5', lang)}\n")
            for idx_s, sc in enumerate(scans_sorted, 1):
                table_name = (
                    sc.get("scan_tables")
                    or sc.get("scan_paths")
                    or sc.get("description_short")
                    or "?"
                )
                scan_fmt = sc.get("scan_formats", "")
                scan_path = sc.get("scan_paths", "")
                col_count = _n(sc.get("scan_column_count", 0))
                scan_filters = sc.get("scan_filters", "")
                files_read = _n(sc.get("files_read", 0))
                files_pruned = _n(sc.get("files_pruned", 0))
                file_pruning_pct = _n(sc.get("file_pruning_pct", 0))
                files_read_size_mb = _n(sc.get("files_read_size_mb", 0))
                fs_read_size_mb = _n(sc.get("fs_read_size_mb", 0))
                cache_hit = _n(sc.get("cache_hit_pct", 0))
                cache_write_bytes = _n(sc.get("cache_write_bytes", 0))
                cache_write_gb = cache_write_bytes / (1024**3) if cache_write_bytes else 0
                cache_read_wait = _n(sc.get("cache_read_wait_ms", 0))
                cache_write_wait = _n(sc.get("cache_write_wait_ms", 0))
                scan_time = _n(sc.get("scan_time_ms", 0))
                cloud_req_count = _n(sc.get("cloud_request_count", 0))
                cloud_req_dur = _n(sc.get("cloud_request_dur_ms", 0))
                dur_sec_io = _n(sc.get("duration_sec", 0))
                exec_id_io = sc.get("execution_id", "?")

                parts.append(f"\n### {idx_s}. {table_name}\n")
                if scan_fmt:
                    parts.append(f"- **{_L('io_format', lang)}**: {scan_fmt}\n")
                if scan_path and scan_path != table_name:
                    parts.append(f"- **{_L('io_storage_path', lang)}**: {scan_path}\n")
                col_extra = f" ({_L('io_wide_schema', lang)})" if col_count >= 100 else ""
                if col_count > 0:
                    parts.append(f"- **{_L('io_column_count', lang)}**: {col_count}{col_extra}\n")
                if scan_filters:
                    parts.append(f"- **{_L('io_filters', lang)}**: {scan_filters}\n")
                parts.append(
                    f"- **{_L('io_files_read', lang)}**: {files_read:,} / "
                    f"**{_L('io_files_pruned', lang)}**: {files_pruned:,} / "
                    f"**{_L('io_file_pruning_pct', lang)}**: {file_pruning_pct:.1f}%\n"
                )
                parts.append(
                    f"- **{_L('io_files_read_size_mb', lang)}**: {_fmt_mb(files_read_size_mb)} / "
                    f"**{_L('io_fs_read_size_mb', lang)}**: {_fmt_mb(fs_read_size_mb)}\n"
                )
                # Cache hit evaluation
                if cache_hit >= 90:
                    cache_eval = f"\u2705 {_L('io_cache_hit_good', lang)}"
                elif cache_hit < 50:
                    cache_eval = f"\u26a0 {_L('io_cache_hit_low', lang)}"
                else:
                    cache_eval = ""
                parts.append(f"- **{_L('io_cache_hit_pct', lang)}**: {cache_hit:.1f}%")
                if cache_eval:
                    parts.append(f" \u2014 {cache_eval}")
                parts.append("\n")
                if cache_write_gb > 0:
                    parts.append(
                        f"- **{_L('io_cache_write_gb', lang)}**: {cache_write_gb:.3f} GB\n"
                    )
                if cache_read_wait > 0 or cache_write_wait > 0:
                    parts.append(
                        f"- **{_L('io_cache_read_wait_ms', lang)}**: {_fmt_ms(cache_read_wait)} / "
                        f"**{_L('io_cache_write_wait_ms', lang)}**: {_fmt_ms(cache_write_wait)}\n"
                    )
                if scan_time > 0:
                    parts.append(f"- **{_L('io_scan_time_ms', lang)}**: {_fmt_ms(scan_time)}\n")
                if cloud_req_count > 0:
                    parts.append(
                        f"- **{_L('io_cloud_request_count', lang)}**: {cloud_req_count:,} / "
                        f"**{_L('io_cloud_request_dur_ms', lang)}**: {_fmt_ms(cloud_req_dur)}\n"
                    )
                parts.append(
                    f"- **{_L('io_duration_sec', lang)}**: {dur_sec_io:.1f}s / "
                    f"**{_L('io_execution_id', lang)}**: {exec_id_io}\n"
                )
            parts.append("")

        # --- Duplicate Scan Analysis ---
        # Group by scan_tables where same table scanned 2+ times
        scan_table_groups: dict[str, list[dict]] = {}
        for s in sql_photon:
            tbl = s.get("scan_tables", "")
            if tbl:
                scan_table_groups.setdefault(tbl, []).append(s)

        dup_tables = {
            tbl: entries for tbl, entries in scan_table_groups.items() if len(entries) >= 2
        }
        if dup_tables:
            parts.append(f"## {_L('io_dup_scan_analysis', lang)}\n")
            parts.append(f"{_L('io_dup_scan_desc', lang)}\n")

            for tbl in sorted(
                dup_tables, key=lambda t: -sum(_n(e.get("duration_sec", 0)) for e in dup_tables[t])
            ):
                entries = dup_tables[tbl]
                total_scan_count = len(entries)
                total_dur = sum(_n(e.get("duration_sec", 0)) for e in entries)
                max_cols = max((_n(e.get("scan_column_count", 0)) for e in entries), default=0)

                parts.append(f"\n### {tbl}\n")
                parts.append(
                    f"- {_L('io_dup_total_scan_fmt', lang).format(count=total_scan_count, dur=total_dur)}\n"
                )
                if max_cols > 0:
                    parts.append(f"- **{_L('io_dup_max_columns', lang)}**: {max_cols}\n")

                # Format breakdown
                fmt_groups: dict[str, tuple[int, float]] = {}
                for e in entries:
                    fmt = str(e.get("scan_formats") or "unknown")
                    fg_count, fg_dur = fmt_groups.get(fmt, (0, 0.0))
                    fmt_groups[fmt] = (fg_count + 1, fg_dur + _n(e.get("duration_sec", 0)))

                if fmt_groups:
                    parts.append(f"- **{_L('io_dup_format_breakdown', lang)}**: ")
                    fmt_parts = [
                        _L("io_dup_format_fmt", lang).format(fmt=f, count=fg_count, dur=fg_dur)
                        for f, (fg_count, fg_dur) in sorted(
                            fmt_groups.items(), key=lambda x: -x[1][1]
                        )
                    ]
                    parts.append(" / ".join(fmt_parts) + "\n")
            parts.append("")
    else:
        parts.append(f"\n{_L('io_no_scan_data', lang)}\n")
    parts.append("")

    # =====================================================================
    # 推奨アクション（セクション2に統合、優先度順）
    # skip_actions=True の場合はLLMが生成済みのため省略
    # =====================================================================
    if skip_actions:
        return "\n".join(parts)

    parts.append(f"\n## {_L('actions_priority_header', lang)}\n")
    parts.append(f"{_L('actions_desc', lang)}\n")
    parts.append(f"{_L('actions_desc2', lang)}\n")

    unexpected_spot = [
        s
        for s in spot
        if s.get("is_unexpected_loss") or s.get("removal_type") in ("SPOT_PREEMPTION", "NODE_LOST")
    ]

    action_idx = 0
    if unexpected_spot:
        action_idx += 1
        total_delay = sum(_n(s.get("estimated_delay_sec", 0)) for s in unexpected_spot)
        total_shuffle_lost = sum(_n(s.get("shuffle_lost_mb", 0)) for s in unexpected_spot)
        node_lost_count = sum(1 for s in unexpected_spot if s.get("removal_type") == "NODE_LOST")
        spot_count = sum(1 for s in unexpected_spot if s.get("removal_type") == "SPOT_PREEMPTION")

        parts.append(f"## {action_idx}. {_L('action_spot_decommission', lang)}\n")
        parts.append(
            f"\U0001f534 Impact: HIGH | \U0001f7e2 Effort: LOW | {_L('action_spot_priority', lang)}\n"
        )
        parts.append(f"\n**{_L('action_rationale', lang)}**\n")
        parts.append(
            f"- {_L('action_spot_detected_fmt', lang).format(count=len(unexpected_spot), nl=node_lost_count, sp=spot_count)}\n"
        )
        parts.append(
            f"- {_L('action_total_delay_fmt', lang).format(delay=total_delay, hours=total_delay / 3600)}\n"
        )
        parts.append(
            f"- {_L('action_lost_shuffle_fmt', lang).format(amount=_fmt_mb(total_shuffle_lost))}\n"
        )
        parts.append(f"\n**{_L('cause_hypothesis', lang)}** {_L('action_spot_hypothesis', lang)}\n")
        parts.append(f"\n**{_L('improvement', lang)}:** {_L('action_spot_improvement', lang)}\n")
        parts.append("\n```\n")
        parts.append(f"# {_L('action_decommission_comment', lang)}\n")
        parts.append("spark.decommission.enabled=true\n")
        parts.append("spark.storage.decommission.enabled=true\n")
        parts.append("spark.storage.decommission.shuffleBlocks.enabled=true\n")
        parts.append(f"\n# {_L('action_speculation_comment', lang)}\n")
        parts.append("spark.speculation=true\n")
        parts.append("```\n")
        per_loss_delay = total_delay / max(len(unexpected_spot), 1)
        parts.append(
            f"\n**{_L('verification_metric', lang)}** {_L('action_spot_verify_fmt', lang).format(delay=per_loss_delay / 60)}\n"
        )
        parts.append("")

    # --- Action: Small Files ---
    small_files_bn = [
        b for b in bottlenecks if (b.get("bottleneck_type") or "").upper() == "SMALL_FILES"
    ]
    if small_files_bn:
        action_idx += 1
        total_sf_dur = sum(_n(b.get("duration_ms", 0)) for b in small_files_bn) / 1000
        # Find per-task input range
        ipt_vals = []
        for b in small_files_bn:
            nt = _n(b.get("num_tasks", 1))
            inp = _n(b.get("input_mb", 0))
            if nt > 0 and inp > 0:
                ipt_vals.append(inp / nt)
        top_sf = sorted(small_files_bn, key=lambda b: _n(b.get("duration_ms", 0)), reverse=True)[:2]

        parts.append(f"## {action_idx}. {_L('action_small_files', lang)}\n")
        parts.append(
            f"\U0001f534 Impact: HIGH | \U0001f7e1 Effort: MEDIUM | {_L('action_small_files_priority', lang)}\n"
        )
        parts.append(f"\n**{_L('action_rationale', lang)}**\n")
        parts.append(
            f"- {_L('action_sf_detected_fmt', lang).format(count=len(small_files_bn), dur=total_sf_dur)}\n"
        )
        if ipt_vals:
            parts.append(
                f"- {_L('action_sf_ipt_fmt', lang).format(min=min(ipt_vals), max=max(ipt_vals))}\n"
            )
        for b in top_sf:
            parts.append(
                f"- {_L('action_sf_stage_fmt', lang).format(sid=b.get('stage_id', '?'), dur=_n(b.get('duration_ms', 0)) / 1000)}\n"
            )
        parts.append(f"\n**{_L('cause_hypothesis', lang)}** {_L('action_sf_hypothesis', lang)}\n")
        parts.append(f"\n**{_L('improvement', lang)}:** {_L('action_sf_improvement', lang)}\n")
        parts.append("\n```sql\n")
        parts.append(f"-- {_L('action_sf_comment_optimize', lang)}\n")
        parts.append("OPTIMIZE <table_name>;\n")
        parts.append(f"\n-- {_L('action_sf_comment_auto', lang)}\n")
        parts.append("SET spark.databricks.delta.optimizeWrite.enabled = true;\n")
        parts.append("SET spark.databricks.delta.autoCompact.enabled = auto;\n")
        parts.append("```\n")
        parts.append(f"\n**{_L('verification_metric', lang)}** {_L('action_sf_verify', lang)}\n")
        parts.append("")

    # --- Action: Data Skew ---
    skew_bn = [b for b in bottlenecks if (b.get("bottleneck_type") or "").upper() == "DATA_SKEW"]
    if skew_bn:
        action_idx += 1
        total_skew_dur = sum(_n(b.get("duration_ms", 0)) for b in skew_bn) / 1000
        max_skew_val = max(_n(b.get("task_skew_ratio", 0)) for b in skew_bn)
        max_skew_b = max(skew_bn, key=lambda b: _n(b.get("task_skew_ratio", 0)))

        parts.append(f"## {action_idx}. {_L('action_data_skew', lang)}\n")
        parts.append(
            f"\U0001f7e1 Impact: MEDIUM | \U0001f7e1 Effort: MEDIUM | {_L('action_ds_priority', lang)}\n"
        )
        parts.append(f"\n**{_L('action_rationale', lang)}**\n")
        parts.append(
            f"- {_L('action_ds_detected_fmt', lang).format(count=len(skew_bn), dur=total_skew_dur)}\n"
        )
        parts.append(
            f"- {_L('action_ds_max_fmt', lang).format(ratio=max_skew_val, sid=max_skew_b.get('stage_id', '?'))}\n"
        )
        parts.append(f"\n**{_L('cause_hypothesis', lang)}** {_L('action_ds_hypothesis', lang)}\n")
        parts.append(f"\n**{_L('improvement', lang)}:** {_L('action_ds_improvement', lang)}\n")
        parts.append("\n```\n")
        parts.append("spark.sql.adaptive.skewJoin.enabled=true\n")
        parts.append("spark.sql.adaptive.skewJoin.skewedPartitionFactor=3\n")
        parts.append("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=128MB\n")
        parts.append("spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB\n")
        parts.append("```\n")
        parts.append(f"\n**{_L('verification_metric', lang)}** {_L('action_ds_verify', lang)}\n")
        parts.append("")

    # --- Action: Photon ---
    if sql_photon:
        avg_p = sum(_n(s.get("photon_pct", 0)) for s in sql_photon) / max(len(sql_photon), 1)
        low_p = [s for s in sql_photon if _n(s.get("photon_pct", 0)) < 50]
        if avg_p < 50:
            photon_on = _is_photon_enabled(spark_config)
            action_idx += 1
            if photon_on is False:
                # Photon disabled on cluster — recommend enabling
                parts.append(f"## {action_idx}. {_L('action_photon', lang)}\n")
                parts.append(
                    f"\U0001f7e2 Impact: HIGH | \U0001f7e2 Effort: LOW | {_L('action_photon_priority', lang)}\n"
                )
                parts.append(f"\n**{_L('action_rationale', lang)}**\n")
                parts.append(f"- {_L('photon_cluster_disabled', lang)}\n")
                parts.append(
                    f"\n**{_L('improvement', lang)}:** {_L('action_photon_enable_runtime', lang)}\n"
                )
                parts.append("")
            else:
                # Photon enabled (or unknown) but low utilization
                parts.append(f"## {action_idx}. {_L('action_photon', lang)}\n")
                parts.append(
                    f"\U0001f7e1 Impact: MEDIUM | \U0001f534 Effort: HIGH | {_L('action_photon_priority', lang)}\n"
                )
                parts.append(f"\n**{_L('action_rationale', lang)}**\n")
                parts.append(f"- {_L('action_photon_avg_fmt', lang).format(pct=avg_p)}\n")
                parts.append(
                    f"- {_L('action_photon_low_fmt', lang).format(low=len(low_p), total=len(sql_photon))}\n"
                )
                # Identify main non-photon causes
                ops_set: set[str] = set()
                for s in sql_photon:
                    npo = s.get("non_photon_op_list", "")
                    if npo:
                        for op in npo.split(","):
                            op = op.strip()
                            if op in (
                                "BatchEvalPython",
                                "ExistingRDD",
                                "AtomicReplaceTableAsSelect",
                            ):
                                ops_set.add(op)
                if ops_set:
                    parts.append(
                        f"- {_L('action_photon_causes_fmt', lang).format(causes=', '.join(sorted(ops_set)))}\n"
                    )
                parts.append(
                    f"\n**{_L('cause_hypothesis', lang)}** {_L('action_photon_hypothesis', lang)}\n"
                )
                parts.append(
                    f"\n**{_L('improvement', lang)}:** {_L('action_photon_improvement', lang)}\n"
                )
                parts.append("\n```python\n")
                parts.append(f"# {_L('action_photon_comment_udf', lang)}\n")
                parts.append("# Before: Python UDF\n")
                parts.append("classify_udf = udf(classify_and_transform, StringType())\n")
                parts.append(
                    'df = df.withColumn("result", classify_udf(col("id"), col("category")))\n'
                )
                parts.append(f"\n# {_L('action_photon_comment_after', lang)}\n")
                parts.append("from pyspark.sql.functions import when, col, lit\n")
                parts.append('df = df.withColumn("result",\n')
                parts.append('    when(col("id") % 50 == 0, lit("type_a"))\n')
                parts.append('    .otherwise(lit("type_other"))\n')
                parts.append(")\n")
                parts.append(f"\n# {_L('action_photon_comment_rdd', lang)}\n")
                parts.append(
                    "# Before: rdd_data = spark.sparkContext.parallelize(data); df = rdd_data.toDF()\n"
                )
                parts.append("# After:  df = spark.createDataFrame(data, schema)\n")
                parts.append("```\n")
                parts.append(
                    f"\n**{_L('verification_metric', lang)}** {_L('action_photon_verify', lang)}\n"
                )
                parts.append("")

    # --- Action: Shuffle partitions ---
    if under_partitioned:
        action_idx += 1
        largest_up = max(under_partitioned, key=lambda x: x[1])
        rec_count_up = largest_up[2]
        parts.append(f"## {action_idx}. {_L('action_shuffle_partitions', lang)}\n")
        parts.append(
            f"\U0001f7e1 Impact: MEDIUM | \U0001f7e2 Effort: LOW | {_L('action_sp_priority', lang)}\n"
        )
        parts.append(f"\n**{_L('action_rationale', lang)}**\n")
        for s, per_task, _rec_c in under_partitioned[:3]:
            parts.append(
                f"- {_L('action_sp_under_fmt', lang).format(sid=s.get('stage_id', '?'), per_task=per_task)}\n"
            )
        for s, per_task, _rec_c in over_partitioned[:3]:
            parts.append(
                f"- {_L('action_sp_over_fmt', lang).format(sid=s.get('stage_id', '?'), per_task=per_task)}\n"
            )
        parts.append(f"\n**{_L('cause_hypothesis', lang)}** {_L('action_sp_hypothesis', lang)}\n")
        parts.append(f"\n**{_L('improvement', lang)}:** {_L('action_sp_improvement', lang)}\n")
        parts.append("\n```\n")
        parts.append(f"spark.sql.shuffle.partitions={rec_count_up}\n")
        parts.append("spark.sql.adaptive.coalescePartitions.enabled=true\n")
        parts.append("spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB\n")
        parts.append("```\n")
        parts.append(f"\n**{_L('verification_metric', lang)}** {_L('action_sp_verify', lang)}\n")
        parts.append("")

    # --- Action: Serialization ---
    if executors:
        high_ser_all = [e for e in executors if _n(e.get("serialization_pct", 0)) > 5]
        if high_ser_all:
            action_idx += 1
            ser_pcts_all = [_n(e.get("serialization_pct", 0)) for e in high_ser_all]
            parts.append(f"## {action_idx}. {_L('action_serialization', lang)}\n")
            parts.append(
                f"\U0001f7e1 Impact: MEDIUM | \U0001f534 Effort: HIGH | {_L('action_ser_priority', lang)}\n"
            )
            parts.append(f"\n**{_L('action_rationale', lang)}**\n")
            parts.append(
                f"- {_L('action_ser_detected_fmt', lang).format(count=len(high_ser_all), min=min(ser_pcts_all), max=max(ser_pcts_all))}\n"
            )
            parts.append(f"- {_L('action_ser_cause', lang)}\n")
            parts.append(
                f"\n**{_L('cause_hypothesis', lang)}** {_L('action_ser_hypothesis', lang)}\n"
            )
            parts.append(f"\n**{_L('improvement', lang)}:** {_L('action_ser_improvement', lang)}\n")
            parts.append("\n```python\n")
            parts.append(f"# {_L('action_ser_comment1', lang)}\n")
            parts.append(f"# {_L('action_ser_comment2', lang)}\n")
            parts.append(f"# {_L('action_ser_comment3', lang)}\n")
            parts.append("broadcast_dict = spark.sparkContext.broadcast(lookup_dict)\n")
            parts.append("```\n")
            parts.append(
                f"\n**{_L('verification_metric', lang)}** {_L('action_ser_verify', lang)}\n"
            )
            parts.append("")

    # --- Action: Duplicate scans ---
    if sql_photon:
        scan_counts_act: dict[str, tuple[int, float]] = {}
        for s in sql_photon:
            tables = s.get("target_tables", s.get("scan_tables", ""))
            if tables:
                cnt, dur_sum = scan_counts_act.get(tables, (0, 0.0))
                scan_counts_act[tables] = (cnt + 1, dur_sum + _n(s.get("duration_sec", 0)))
        heavy_scans = [(t, c, d) for t, (c, d) in scan_counts_act.items() if c > 2]
        if heavy_scans:
            action_idx += 1
            heavy_scans.sort(key=lambda x: -x[2])
            top_scan = heavy_scans[0]
            parts.append(f"## {action_idx}. {_L('action_dup_scan', lang)}\n")
            parts.append(
                f"\U0001f7e1 Impact: MEDIUM | \U0001f7e1 Effort: MEDIUM | {_L('action_dup_priority', lang)}\n"
            )
            parts.append(f"\n**{_L('action_rationale', lang)}**\n")
            parts.append(
                f"- {_L('action_dup_scan_fmt', lang).format(tbl=top_scan[0], count=top_scan[1], dur=top_scan[2])}\n"
            )
            parts.append(f"- {_L('action_dup_cause', lang)}\n")
            parts.append(
                f"\n**{_L('cause_hypothesis', lang)}** {_L('action_dup_hypothesis', lang)}\n"
            )
            parts.append(f"\n**{_L('improvement', lang)}:** {_L('action_dup_improvement', lang)}\n")
            parts.append("\n```python\n")
            parts.append(f"# {_L('action_dup_comment_delta', lang)}\n")
            parts.append(
                'intermediate_df.write.mode("overwrite").saveAsTable("temp.intermediate_result")\n'
            )
            parts.append('result_df = spark.table("temp.intermediate_result")\n')
            parts.append(f"\n# {_L('action_dup_comment_persist', lang)}\n")
            parts.append("from pyspark import StorageLevel\n")
            parts.append(f'base_df = spark.table("{top_scan[0]}")\n')
            parts.append("cached_df = base_df.persist(StorageLevel.DISK_ONLY)\n")
            parts.append(f"# {_L('action_dup_comment_unpersist', lang)}\n")
            parts.append("cached_df.unpersist()\n")
            parts.append("```\n")
            parts.append(
                f"\n**{_L('verification_metric', lang)}** {_L('action_dup_verify', lang)}\n"
            )
            parts.append("")

    if action_idx == 0:
        parts.append(f"{_L('no_major_bottleneck', lang)}\n")
    parts.append("")

    # =====================================================================
    # Verification Checklist
    # =====================================================================
    parts.append("---\n")
    parts.append(f"\u2705 **{_L('verification_checklist', lang)}**\n")
    parts.append(f"{_L('checklist_desc', lang)}\n")

    checklist_items: list[str] = []

    if unexpected_spot:
        per_loss = sum(_n(s.get("estimated_delay_sec", 0)) for s in unexpected_spot) / max(
            len(unexpected_spot), 1
        )
        checklist_items.append(_L("check_spot_fmt", lang).format(delay=per_loss / 60))

    if small_files_bn:
        checklist_items.append(_L("check_sf_count_fmt", lang).format(count=len(small_files_bn)))
        checklist_items.append(_L("check_sf_ipt", lang))

    if skew_bn:
        max_skew_check = max(_n(b.get("task_skew_ratio", 0)) for b in skew_bn)
        checklist_items.append(_L("check_skew_count_fmt", lang).format(count=len(skew_bn)))
        checklist_items.append(_L("check_skew_ratio_fmt", lang).format(ratio=max_skew_check))

    if sql_photon:
        avg_p_check = sum(_n(s.get("photon_pct", 0)) for s in sql_photon) / max(len(sql_photon), 1)
        if avg_p_check < 80:
            checklist_items.append(_L("check_photon_fmt", lang).format(pct=avg_p_check))
            checklist_items.append(_L("check_photon_ops", lang))

    if under_partitioned:
        checklist_items.append(_L("check_shuffle", lang))

    if executors:
        high_ser_check = [e for e in executors if _n(e.get("serialization_pct", 0)) > 5]
        if high_ser_check:
            ser_range = [_n(e.get("serialization_pct", 0)) for e in high_ser_check]
            checklist_items.append(
                _L("check_ser_fmt", lang).format(min=min(ser_range), max=max(ser_range))
            )

    # Duplicate scans checklist item
    if sql_photon:
        _sc: dict[str, int] = {}
        for s in sql_photon:
            t = s.get("target_tables", s.get("scan_tables", ""))
            if t:
                _sc[t] = _sc.get(t, 0) + 1
        max_scan_count = max(_sc.values()) if _sc else 0
        if max_scan_count > 2:
            checklist_items.append(_L("check_dup_fmt", lang).format(count=max_scan_count))

    if not checklist_items:
        checklist_items.append(_L("check_no_issues", lang))

    for item in checklist_items:
        parts.append(f"- [ ] {item}\n")
    parts.append("")

    # Footer
    parts.append("---\n")
    parts.append(f"*{_L('footer', lang)}*\n")

    return "\n".join(parts)
