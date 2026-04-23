# Serverless SQL Warehouse固有のチューニング

## Serverless固有のチューニング
<!-- section_id: serverless -->

Serverless SQL Warehouseには、Classic/Proとは異なる特有のパフォーマンス特性と最適化ポイントがあります。

---

### 1. コールドスタートとウォームプール

Serverless WHの初回クエリは、コンピュートのスピンアップに**6〜13秒**のオーバーヘッドが発生します。

**対策:**
- **ウォームプール設定**: ダッシュボード等で定期アクセスがある場合、WHの`Auto Stop`時間を適切に設定（5〜10分）
- **プリウォーミング**: 重要なダッシュボードの閲覧前に軽量クエリ（`SELECT 1`）を投入
- 初回バッチのクエリが後続バッチより遅い場合（例: 13s vs 2s）、これはコールドスタートが原因

---

### 2. AOS（Auto Optimized Shuffle）

ServerlessではShuffle Partitionの自動最適化が利用可能です。

**ESチケット頻出パターン:** AOSはshuffle最適化には有効だが、data skew・data explosion・過大broadcastの根本原因までは解消しない。ServerlessでもSQL/データ設計の改善が先。

#### AOS v1（DBR 11.3+）
- `spark.sql.shuffle.partitions = auto` はServerless内部で自動設定されます（ユーザーがSETすることはできません）
- カタログ統計またはファイルサイズに基づいてパーティション数を推定
- 推定誤りが発生する可能性あり（特に高圧縮テーブル）
- ターゲットサイズ調整:
```sql
-- デフォルト64MB。パーティションが大きすぎる場合は縮小
SET spark.databricks.adaptive.autoOptimizeShuffle.preshufflePartitionSizeInBytes = 16777216;  -- 16MB
```

#### AOS v2（DBR 16.4+, DBSQL PREVIEWチャンネルで展開済み）
- 統計推定に依存せず、**サンプリングと外挿**によりパーティション数を動的決定
- ターゲットパーティションサイズ: **256MB〜1GB**（非圧縮）
- 初期パーティション数: `2 * vCPUs`
- 実行中にリパーティションが必要と判断した場合、自動的にシャッフルをやり直す

#### Spill Fallback（DBR 17.1+）
- AOS v2のアドオン機能
- スピルや長時間ステージを検出し、自動的に並列度を上げてリトライ

---

### 3. 同時実行クエリのキューイング

Serverless WHでは同時実行数に制限があり（約10クエリ）、超過するとキューに入ります。

**診断:**
- クエリプロファイルの`Scheduling Time`が長い場合、キューイングが発生
- 同時アクセスダッシュボードの初回バッチが遅い場合の典型的パターン

**対策:**
- WHサイズのスケールアップ（同時実行スロット増加）
- クエリの軽量化（実行時間短縮 → スロット占有時間短縮）
- ダッシュボードクエリの最適化（不要なクエリの排除）

---

### 4. Serverlessでのパーティションサイズ目安

| パーティション用途 | 推奨サイズ（非圧縮） | 備考 |
|---|---|---|
| Shuffleパーティション | 256MB〜1GB | AOS v2のデフォルトターゲット |
| 高圧縮テーブル | 16MB〜64MB（`preshufflePartitionSizeInBytes`で調整） | AOS v1使用時 |
| Exploding stageの下流 | より小さいパーティション | Spill Fallbackに任せるか手動調整 |

---

### 5. Serverlessでの統計情報

Serverlessでも`ANALYZE TABLE`は有効です。AQE（Adaptive Query Execution）がより良い実行計画を選択するために統計情報を活用します。

**ESチケット頻出パターン:** Serverlessでも統計鮮度は重要。stale statsはAQE/Broadcast/Data Skippingの判断精度を下げる。実行エンジン差を疑う前に、まず統計状態を確認すること。

```sql
-- テーブル統計の更新
ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;
```

- **Predictive Optimization**が有効な場合、統計情報は自動更新される
- 手動実行は、テーブル再作成/大量更新直後に推奨

---

### 6. Serverless vs Classic/Pro の主な違い

| 観点 | Serverless | Classic/Pro |
|------|-----------|-------------|
| デフォルトshuffle partitions | `2 * vCPUs` | 200 |
| AOS | v2展開済み（PREVIEW） | 非対応 |
| Photon | デフォルト有効 | WHタイプによる |
| コールドスタート | あり（6-13s） | WHサイズによる |
| スケーリング | 秒単位 | 分単位 |
| Spark設定変更 | 制限あり | 自由 |
