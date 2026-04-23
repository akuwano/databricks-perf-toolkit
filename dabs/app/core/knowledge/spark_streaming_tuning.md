# Structured Streaming パフォーマンス最適化 ナレッジベース

<!-- section_id: spark_streaming -->
## Structured Streaming 最適化

### トリガーモード
- **Default (micro-batch)**: 前バッチ完了後即座に次バッチ開始。低レイテンシーが必要な場合に使用
- **processingTime**: 固定間隔（例: `trigger(processingTime='10 seconds')`）で実行。リソース消費を制御したい場合に使用
- **availableNow**: バックログを一括処理して停止（`trigger(availableNow=True)`）。バッチジョブライクな使い方で、コスト最適化に有効
- **continuous** (experimental): ミリ秒レイテンシー。map系処理のみ対応。本番利用は非推奨

### スループット最適化
- **maxFilesPerTrigger / maxBytesPerTrigger**: バッチあたりの処理量を制御
  - Auto Loader: `cloudFiles.maxFilesPerTrigger`, `cloudFiles.maxBytesPerTrigger`
  - Kafka: `maxOffsetsPerTrigger`
- **spark.sql.shuffle.partitions**: ストリーミングでもシャッフルパーティション数が重要。AQE はストリーミングではデフォルト無効のため、手動設定が必要
- **バッチサイズの最適化**: バッチ処理時間の内訳（addBatch, queryPlanning, commit 等）を確認し、ボトルネック箇所を特定

### 状態管理
- **RocksDB State Backend**: 大規模状態に推奨
  - `spark.sql.streaming.stateStore.providerClass=com.databricks.sql.streaming.state.RocksDBStateStoreProvider`
  - メリット: ディスクベースでOOM回避、チェックポイント高速化
- **Watermark**: `withWatermark()` で古い状態を自動削除。遅延データ許容範囲に応じて設定
- **State TTL**: `spark.sql.streaming.stateTTL` でグローバルTTL設定（Databricks 拡張）
- **状態メモリ監視**: `stateOperators.memoryUsedBytes` を定期チェック。増加し続ける場合はウォーターマーク設定を見直す

### バックログ対策
- `trigger(availableNow=True)` でバックログを一括処理
- クラスタサイズを一時的に拡大してキャッチアップ
- `maxFilesPerTrigger` を増加して一括取り込み量を増やす
- `numFilesOutstanding` / `numBytesOutstanding` メトリクスを監視

### コミットオーバーヘッド対策
- RocksDB state backend はチェックポイントが高速（増分チェックポイント）
- Delta シンクのログコンパクション: 頻繁なコミットによるログファイル増加を OPTIMIZE で対応
- `spark.sql.streaming.commitProtocolClass` のデフォルトを使用（変更非推奨）

### プランニングオーバーヘッド対策
- クエリをシンプルに保つ（複雑なUDF、多段結合を避ける）
- Delta CDF (Change Data Feed) 使用時はスキーマ進化に注意
- Spark UI の "Query Planning" 時間がトリガー時間の30%を超える場合は要調査

### モニタリング
- **StreamingQueryListener**: カスタムメトリクス収集に使用
- **Spark UI**: Structured Streaming タブでバッチごとの処理時間・スループットを確認
- **query.lastProgress**: プログラムによるメトリクス取得
- 重要メトリクス: `batchDuration`, `inputRowsPerSecond`, `processedRowsPerSecond`, `stateOperators.memoryUsedBytes`
