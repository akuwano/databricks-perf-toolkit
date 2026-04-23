# Key Mapping Reference

このドキュメントは、Databricks SQL Query Profile JSONの全キーと、`extractors.py`での使用状況を対応付けた一覧です。

## 概要

| カテゴリ | パス数 | 説明 |
|----------|--------|------|
| 全ユニークパス | 443 | json/サンプル14ファイルから収集 |
| 使用中 (true) | 130 | extractors.pyで明示的に参照 |
| 部分使用 (partial) | 229 | 親要素は使用、深い枝は未参照 |
| 未使用 (false) | 84 | 現在未使用 |

## 使用状況の定義

- **true**: `extractors.py`でそのパスを明示的に参照している
- **partial**: 親要素や兄弟キーは使用しているが、このパスは直接参照していない
- **false**: コード中に関連キー参照が見当たらない

---

## 1. Query - 基本情報 (使用中)

| JSONパス | 型 | マッピング先 | 説明 |
|----------|------|-------------|------|
| `query.id` | string | `QueryMetrics.query_id` | クエリID |
| `query.status` | string | `QueryMetrics.status` | 実行ステータス |
| `query.queryText` | string | `QueryMetrics.query_text` | SQLテキスト |

## 2. Query - メトリクス (使用中)

| JSONパス | 型 | マッピング先 | 説明 |
|----------|------|-------------|------|
| `query.metrics.totalTimeMs` | int | `QueryMetrics.total_time_ms` | 総実行時間 |
| `query.metrics.compilationTimeMs` | int | `QueryMetrics.compilation_time_ms` | コンパイル時間 |
| `query.metrics.executionTimeMs` | int | `QueryMetrics.execution_time_ms` | 実行時間 |
| `query.metrics.readBytes` | int | `QueryMetrics.read_bytes` | 読み取りバイト数 |
| `query.metrics.readRemoteBytes` | int | `QueryMetrics.read_remote_bytes` | リモート読み取りバイト数 |
| `query.metrics.readCacheBytes` | int | `QueryMetrics.read_cache_bytes` | キャッシュ読み取りバイト数 |
| `query.metrics.spillToDiskBytes` | int | `QueryMetrics.spill_to_disk_bytes` | ディスクスピルバイト数 |
| `query.metrics.photonTotalTimeMs` | int | `QueryMetrics.photon_total_time_ms` | Photon実行時間 |
| `query.metrics.taskTotalTimeMs` | int | `QueryMetrics.task_total_time_ms` | 累積タスク時間 |
| `query.metrics.readFilesCount` | int | `QueryMetrics.read_files_count` | 読み取りファイル数 |
| `query.metrics.prunedFilesCount` | int | `QueryMetrics.pruned_files_count` | プルーニングファイル数 |
| `query.metrics.rowsReadCount` | int | `QueryMetrics.rows_read_count` | 読み取り行数 |
| `query.metrics.rowsProducedCount` | int | `QueryMetrics.rows_produced_count` | 出力行数 |
| `query.metrics.bytesReadFromCachePercentage` | int | `QueryMetrics.bytes_read_from_cache_percentage` | キャッシュヒット率 |
| `query.metrics.writeRemoteBytes` | int | `QueryMetrics.write_remote_bytes` | 書き込みバイト数 |
| `query.metrics.writeRemoteFiles` | int | `QueryMetrics.write_remote_files` | 書き込みファイル数 |
| `query.metrics.networkSentBytes` | int | `QueryMetrics.network_sent_bytes` | ネットワーク送信バイト数 |
| `query.metrics.readPartitionsCount` | int | `QueryMetrics.read_partitions_count` | 読み取りパーティション数 |

## 3. Query - メトリクス (extra_metricsに格納)

| JSONパス | 型 | 説明 |
|----------|------|------|
| `query.metrics.planningTimeMs` | int | プランニング時間 |
| `query.metrics.queuedProvisioningTimeMs` | int | プロビジョニング待ち時間 |
| `query.metrics.queuedOverloadTimeMs` | int | オーバーロード待ち時間 |
| `query.metrics.resultFetchTimeMs` | int | 結果取得時間 |
| `query.metrics.metadataTimeMs` | int | メタデータ操作時間 |
| `query.metrics.planningPhases[]` | array | プランニングフェーズ詳細 |

## 4. Graphs - ノード (使用中)

| JSONパス | 型 | マッピング先 | 説明 |
|----------|------|-------------|------|
| `graphs[].nodes[].id` | string | `NodeMetrics.node_id` | ノードID |
| `graphs[].nodes[].name` | string | `NodeMetrics.name` | ノード名 |
| `graphs[].nodes[].tag` | string | `NodeMetrics.tag` | ノードタグ |
| `graphs[].nodes[].hidden` | bool | フィルタリング | 非表示フラグ |
| `graphs[].nodes[].keyMetrics.durationMs` | int | `NodeMetrics.duration_ms` | 実行時間 |
| `graphs[].nodes[].keyMetrics.peakMemoryBytes` | int | `NodeMetrics.peak_memory_bytes` | ピークメモリ |
| `graphs[].nodes[].keyMetrics.rowsNum` | int | `NodeMetrics.rows` | 処理行数 |

## 5. Graphs - ノードメタデータ (キーマッチングで使用)

`graphs[].nodes[].metadata[].key` の値によって分岐処理:

| metadata.key | マッピング先 | 説明 |
|--------------|-------------|------|
| `IS_PHOTON` | `NodeMetrics.is_photon` | Photon有効フラグ |
| `SCAN_CLUSTERS` | `NodeMetrics.clustering_keys` | クラスタリングキー |
| `JOIN_ALGORITHM` | `JoinInfo.join_type` | JOINアルゴリズム |
| `JOIN_TYPE` | `JoinInfo.join_type` | JOIN種別 |
| `JOIN_BUILD_SIDE` | `JoinInfo` | JOINビルド側 |
| `LEFT_KEYS` | `JoinInfo` | 左側キー |
| `RIGHT_KEYS` | `JoinInfo` | 右側キー |
| `SHUFFLE_ATTRIBUTES` | `ShuffleMetrics` | シャッフル属性 |

## 6. Graphs - ノードメトリクス (ラベルマッチングで使用)

`graphs[].nodes[].metrics[].label` の値によって分岐処理:

| metrics.label | マッピング先 | 説明 |
|---------------|-------------|------|
| `Files read` | `NodeMetrics.files_read` | 読み取りファイル数 |
| `Files pruned` | `NodeMetrics.files_pruned` | プルーニングファイル数 |
| `Size of files read` | `NodeMetrics.bytes_read` | 読み取りバイト数 |
| `Rows scanned` | `NodeMetrics.rows_scanned` | スキャン行数 |
| `Number of output rows` | `NodeMetrics.output_rows` | 出力行数 |
| `Cache hits size` | `NodeMetrics.cache_hits_bytes` | キャッシュヒットバイト数 |
| `Number of partitions` | `ShuffleMetrics.partition_count` | パーティション数 |
| `AQEShuffleRead - Number of partitions` | `ShuffleMetrics.aqe_partitions` | AQEパーティション数 |

---

## 未使用だが有用なパス

### 優先度: 高

| JSONパス | 説明 | 活用案 |
|----------|------|--------|
| `insights[].recommendation` | システム生成の改善提案 | LLMへの追加コンテキスト |
| `insights[].docUrl` | 推奨ドキュメントURL | レポートへのリンク追加 |
| `insights[].body` | インサイト詳細テキスト | 分析の根拠情報 |
| `insights[].insight.type.insightEnum` | インサイト種別 | 分類・フィルタリング |
| `query.errorMessage` | エラーメッセージ | 失敗クエリの分析 |

### 優先度: 中

| JSONパス | 説明 | 活用案 |
|----------|------|--------|
| `query.queryStartTimeMs` | クエリ開始時刻 | 時系列分析 |
| `query.queryEndTimeMs` | クエリ終了時刻 | 実行時間検証 |
| `query.statementType` | SQL文種別 | SELECT/INSERT分類 |
| `query.internalQuerySource.jobId` | ジョブID | 実行元追跡 |
| `query.internalQuerySource.notebookId` | ノートブックID | 実行元追跡 |
| `query.internalQuerySource.dashboardId` | ダッシュボードID | 実行元追跡 |
| `insights[].insight.context.tableStatsContext.tableName` | テーブル名 | テーブル特定 |

### 優先度: 低

| JSONパス | 説明 |
|----------|------|
| `query.executedAsUser.displayName` | 実行ユーザー表示名 |
| `query.channelUsed.name` | チャネル/エンドポイント名 |
| `query.channelUsed.dbsqlVersion` | DBSQLバージョン |
| `query.sparkUiUrl` | Spark UI URL |
| `query.internalQuerySource.driverInfo.driverName` | ドライバー名 |

---

## トップレベル別の使用状況サマリー

| トップレベルキー | 総パス数 | 使用 | 部分 | 未使用 |
|------------------|----------|------|------|--------|
| `graphs` | 87 | 87 | 0 | 0 |
| `query` | 165 | 43 | 122 | 0 |
| `metrics` | 39 | 0 | 39 | 0 |
| `insights` | 30 | 0 | 7 | 23 |
| `internalQuerySource` | 27 | 0 | 27 | 0 |
| `planMetadata` | 12 | 0 | 8 | 4 |
| `planMetadatas` | 18 | 0 | 9 | 9 |
| その他 | 65 | 0 | 24 | 41 |

---

## 関連ファイル

- **YAMLソース**: `databricks-apps/core/key_mapping.yaml`
- **抽出実装**: `databricks-apps/core/extractors.py`
- **データモデル**: `databricks-apps/core/models.py`
- **分析ロジック**: `databricks-apps/core/analyzers.py`

---

## 更新方法

1. JSONサンプルに新しいキーが追加された場合:
   - `json/`フォルダにサンプルを追加
   - このドキュメントと`key_mapping.yaml`を更新

2. extractors.pyで新しいキーを使用開始した場合:
   - 該当パスの`usage`を`true`に変更
   - `target`にマッピング先を記載
