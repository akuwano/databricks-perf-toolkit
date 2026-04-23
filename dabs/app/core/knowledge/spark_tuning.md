# Databricks Spark パフォーマンス最適化 ナレッジベース

<!-- section_id: spark_overview -->
## 最適化の原則

### 必要な部分だけを最適化する
- 目標を定義してから始める（コスト目標、SLA/パフォーマンス目標）
- シンプルで効果の高いものから開始する（80/20の法則）
- ベンチマークと反復（結果を記録して進捗を把握）
- チューニングのやめ時を判断する

### 処理プロセスを理解する
最適化対象は3層に分かれる:
- **クエリー層**: SQL/DataFrame のロジック
- **プロセッシング・エンジン層**: Spark/Photon、オプティマイザ
- **インフラ層**: VM/Serverless、インスタンス選択、スケーリング、ディスクキャッシュ

### 最適化の7つのポイント
1. 何を目標に最適化するのかを定義する
2. まずは簡単にできることから始める（プラットフォーム→データ→クエリ）
3. 最新のコンピュートと機能の活用（サーバーレス、Photon、予測最適化、Liquid Clustering）
4. ワークロードに基づき垂直方向または水平方向に拡張する
5. 可能な限りストリーミングによるインクリメンタル処理で実装する
6. 最適化の効果を測定するためにモニタリングツールを利用する
7. 長期間にわたる最適化を回避するため、終了タイミングを判断する

---

<!-- section_id: spark_compute -->
## コンピュート最適化

### Photon エンジン
**最も恩恵を受けるワークロード:**
- 大量データに対する結合と集約などの重い計算処理
- Delta Lake での Merge 処理
- ワイドテーブルの読み書き
- DLT と Auto Loader

**Photon が利用されない場合の対処:**
- 非対応APIの使用（例: collect_set() → collect_list(distinct)）
- UDF / RDD / Typed Dataset → 可能な限り使用を避ける
- Spark UI の Details で Photon Explanation を確認

### Adaptive Query Execution (AQE)
- 正確なメトリクスに基づき、実行時に自動的にクエリプランを適応
- 機能:
  - ソートマージ結合(SMJ) → ブロードキャストハッシュ結合(BHJ) への動的切替
  - シャッフル・パーティションの最適化
  - データ・スキューへの対応

### SQL ウェアハウスのサイジング
- **垂直スケーリング（クラスターサイズ）**: 大規模クエリの処理速度向上、ディスクスピル解消
- **水平スケーリング（クラスター数）**: 同時クエリ対応
- キューにクエリが多すぎる → クラスタの個数を増やす
- クエリに時間がかかりすぎる → クラスタサイズを大きくする

### クラシック・コンピュートの設定ポイント
- **CPU:RAM比率**: 予算に応じて十分なメモリをコアに割り当て
- **プロセッサー種類**: ARMベースのチップは非常に良い性能
- **ローカル・ストレージ**: ディスク・キャッシュは繰り返しアクセスに有効
- **Spot可用性**: 長時間稼働ジョブでは安定性がより重要
- **自動スケーリング**: 高いクラスタ利用率を実現しコスト削減

### Executor 設定
- **spark.executor.cores**: 4〜5 を起点とする
- **メモリ計算式**: (ノードメモリ ÷ Executor数) × (1 - 0.1875)
- **Driver**: デフォルト 2GB で大半は十分。1,000+ Executor やcollect()で大量結果取得時のみ増加

---

<!-- section_id: spark_data_layout -->
## データレイアウト最適化

### データレイアウトの原則
不要なファイルを読み込まないようデータを整理する:
- **パーティション・プルーニング**: パーティションキーによるファイル群の除外
- **ファイル・スキッピング**: 統計情報（min/max）によるファイル単位の除外

### Liquid Clustering（推奨）
Z-Order や従来のパーティショニングを置き換える推奨手法:
- **高速**: 書き込み時の自動クラスタリング
- **セルフ・チューニング**: AIによるクラスタリングキーの自動選定
- **スキューに強い**: OPTIMIZE により一定ファイルサイズを維持
- **フレキシブル**: クラスタリングカラムを既存データに影響なく変更可能

### Predictive Optimization（予測最適化）
- 最適化の手動スケジューリングが不要
- 自動実行: Vacuum, Optimize, Analyze, Liquid Clusteringキーの自動設定

### ファイルサイズ
- **小さいファイル**: フィルタ効率高、更新高速、ファイル数増加
- **大きいファイル**: フィルタ非効率、更新低速、ファイル数削減
- **目標ファイルサイズ**: 128MB〜1GB

---

<!-- section_id: spark_code -->
## コード最適化

### 基本原則
- 本番ジョブでは count(), display(), collect() を最小限に
- シングルスレッド Python/Pandas/Scala を避ける → Spark 上の Pandas API を使用
- Python UDF を避ける → PySpark ネイティブ関数か Pandas UDF（ベクトル化UDF）
- RDD より DataFrame を使用（RDD は CBO/Photon 利用不可）

### UDF の種類と性能
- **Python UDF (@F.udf)**: 行単位IPC、System CPU高騰、Photon不可（最遅）
- **Pandas UDF (SCALAR)**: バッチ単位転送、ArrowEvalPython、Photon不可
- **Pandas UDF (GROUPED_MAP)**: グループ単位、FlatMapGroupsInPandas、Photon不可
- **mapInArrow**: パーティション単位、Arrow変換なし（最速のUDF代替）
- **SparkSQL / PySparkネイティブ関数**: JVM内完結、Photon対応（推奨）

### Broadcast Join
- 最もパフォーマンスの高い結合タイプ
- spark.sql.autoBroadcastJoinThreshold（デフォルト30MB）で制御
- Dynamic File Pruning (DFP) の前提条件

---

<!-- section_id: spark_data_skew -->
## ボトルネック: データスキュー (DATA_SKEW)

**症状:** 処理対象データサイズの不均衡。task_skew_ratio が高い。

### JOIN のスキュー対策（AQE skewJoin）
```
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256m  -- デフォルト
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5  -- デフォルト。検出されない場合は3に下げる
spark.sql.adaptive.advisoryPartitionSizeInBytes=64m  -- スキュー分割後の目標サイズ
```
- **重要**: skewJoin は SortMergeJoin のみ対象。ShuffledHashJoin, BroadcastHashJoin, GroupBy/Aggregate には効かない
- スキューヒント: `SELECT /*+ SKEW('table', 'col') */`

### GroupBy/Aggregate のスキュー対策
- ソルティング: キーにランダム値を追加して分散後、再集約
- 二段階集約: まず部分キーで集約、次に最終キーで集約
- spark.sql.shuffle.partitions の増加でパーティションあたりのデータ量を削減

### 共通の対策
- AQE を有効化: spark.sql.adaptive.enabled=true
- spark.sql.adaptive.coalescePartitions.enabled=true
- 明示的な repartition() でデータを均等に分割
- Liquid Clustering の導入でデータ分布の偏りを緩和

---

<!-- section_id: spark_disk_spill -->
## ボトルネック: ディスクスピル (DISK_SPILL)

**症状:** メモリ不足による一時ファイルのディスクへの書き込み。

### 推奨アクション
```
spark.executor.memory=<増加>  -- 計算式: (ノードメモリ÷Executor数)×(1-0.1875)
spark.memory.fraction=0.6  -- デフォルト。必要に応じて増加
spark.sql.shuffle.partitions=auto  -- まずautoを試す（AQEが自動最適化）
spark.sql.files.maxPartitionBytes=<調整>
spark.executor.cores=4  -- コアあたりのメモリを確保
```
- スキューが根本原因の場合はまずスキューに対処
- ウェアハウスの場合はクラスタサイズを拡大（より多くの RAM）

### GC チューニング（GC オーバーヘッドが高い場合）
```
-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35
```
- 目標: GC 時間 ≤ タスク実行時間の 1%
- 警告閾値: GC 25% 以上は深刻なパフォーマンス問題
- UDF・collect の見直し — オブジェクト生成を削減

---

<!-- section_id: spark_heavy_shuffle -->
## ボトルネック: ヘビーシャッフル (HEAVY_SHUFFLE)

**症状:** 大量のデータがワーカーノード間で移動。

### 推奨アクション
```
spark.sql.autoBroadcastJoinThreshold=30m  -- Broadcast Join の閾値
spark.sql.shuffle.partitions=auto  -- まずautoを試す。手動設定は入力データMB÷128
```
- シャッフルされるデータ量を減らす（列の絞り込み、事前フィルタリング）
- パーティション数をクラスタ総コア数の倍数に調整
- より少数のより大きなワーカーを使用してノード間転送を削減
- Tips: すべてのシャッフルを外すことにこだわらない — シャッフルより高コストな操作に集中

---

<!-- section_id: spark_small_files -->
## ボトルネック: スモールファイル (SMALL_FILES)

**症状:** 大量の小さなファイルによる高いオーバーヘッド（タスクあたり読み取り < 10MB）。

### 推奨アクション
```sql
OPTIMIZE <table_name>;
```
```
spark.databricks.delta.optimizeWrite.enabled=true  -- 128MBに書き込みサイズ調整
spark.databricks.delta.autoCompact.enabled=auto  -- 書き込み後にファイルサイズ改善
```
- Predictive Optimization を有効化して自動 OPTIMIZE / Vacuum
- Vacuum で古いバージョンを削除・メタデータをクリーンアップ
- 目標ファイルサイズ: 128MB〜1GB

---

<!-- section_id: spark_serialization -->
## ボトルネック: シリアライゼーション (SERIALIZATION)

**症状:** データやコードの変換・転送処理による遅延。System CPU がUser CPUを上回る。

### 推奨アクション
- UDF 使用を最小限に → PySparkネイティブ関数か SparkSQL 関数を推奨
- `@F.udf`（Python UDF）→ `@pandas_udf` への書き換え（バッチ転送で IPC 効率改善）
- 可能な限り SparkSQL / PySparkネイティブ関数に書き換え（Photon 対応）

### Kryo Serializer
```
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryo.registrationRequired=true
spark.kryoserializer.buffer=1024k
spark.kryoserializer.buffer.max=1024m
```

---

<!-- section_id: spark_photon -->
## ボトルネック: Photon フォールバック (PHOTON_FALLBACK)

**症状:** Photon エンジンが利用されず Classic Spark にフォールバック。

### Photon 非対応処理の分類

**コマンド系処理（DDL/制御系）— 内部データ処理はPhoton実行される場合あり:**
- AtomicReplaceTableAsSelect / AtomicCreateTableAsSelect
- Execute / WriteFiles / AddJarsCommand
- Photon Explanation: "Commands are not directly photonized" → 後続ジョブで確認

**非対応オペレータ（UDF/非ネイティブ処理）:**
- FlatMapGroupsInPandas (applyInPandas) → Window関数 + groupBy に書き換え
- ArrowEvalPython (Pandas UDF) → Spark ネイティブ関数に書き換え
- BatchEvalPython (Python UDF) → SparkSQL / PySparkネイティブ関数に書き換え
- SortMergeJoin → Broadcast Hash Join への切り替え（autoBroadcastJoinThreshold 調整）
- ColumnarToRow → 上流の非対応オペレータを特定して書き換え

---

<!-- section_id: spark_spot_loss -->
## ボトルネック: Spot インスタンスロスト (SPOT_LOSS)

**症状:** Spot インスタンスのロストによる Executor 消失。Shuffle データの消失と再計算。

### 推奨設定
```
spark.decommission.enabled=true
spark.storage.decommission.enabled=true
spark.storage.decommission.shuffleBlocks.enabled=true
spark.storage.decommission.rddBlocks.enabled=true
spark.decommission.graceful.timeout=120s
spark.speculation=true
```
- クラスタ構成: Driver は On-Demand、Worker は Spot + フォールバック On-Demand
- AWS: capacity-optimized 割り当て戦略、複数インスタンスタイプで中断リスク分散

### キャッシュ戦略（Spot環境）
- cache() / persist() / localCheckpoint() → ローカル保持。Spot ロストでデータ消失
- checkpoint() / Delta テーブル書き出し → リモートストレージ永続化。Spot ロスト耐性あり
- 長時間処理の中間結果には checkpoint() または Delta テーブル書き出しを推奨

---

<!-- section_id: spark_shuffle_params -->
## Shuffle パーティションチューニング

### デフォルト値の問題
- spark.sql.shuffle.partitions のデフォルト 200 は 20GB 超のデータでは不適切
- パーティションサイズが大きすぎるとメモリ不足（OOM）、小さすぎるとタスクオーバーヘッド増大

### 最適パーティションサイズの計算
- **目標パーティションサイズ**: 100〜200MB（128MB 推奨）
- **計算式**: パーティション数 = ステージ入力データ(MB) ÷ 目標サイズ(MB)
- **例**: 210GB のシャッフルデータ → 210,000MB ÷ 128MB = 1,640 パーティション

### コア数との調整
- パーティション数 ≥ クラスタ総コア数を確保
- パーティション数をコア数の倍数に丸める

### AQE による自動最適化（最優先で推奨）
**まず `auto` を試すこと。** 手動チューニングは `auto` で改善しない場合のみ検討する。
```
spark.sql.shuffle.partitions=auto  -- 最初にこれを試す（AQEが自動最適化）
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```
- `auto` を設定すると、AQE がシャッフル後のパーティション数を実行時に自動調整
- 手動で数値を指定する場合、その値は上限として機能するため大きめに設定する
- `auto` で効果が不十分な場合のみ、計算式（入力データMB÷128）で手動設定を検討

---

<!-- section_id: spark_diagnostics -->
## パフォーマンス診断

### クエリプロファイルで確認すべき項目
- Photon が 100% に近いことを確認
- ディスク・キャッシュのヒット率を確認
- 読み込んだ行数は理にかなっているか
- スピルはウェアハウスの RAM 不足を示す
- ファイルやパーティションの数が多すぎないか

### キャッシュ戦略の選択
**Photon 環境での推奨:**
1. persist(DISK_ONLY) — 読み込み Photon 対応 + Lineage 保持
2. Delta テーブル書き出し — 読み込み Photon 対応 + Lineage 切断 + Spot 耐性
3. cache() — 読み込み Photon 非対応だが手軽。3回以上のアクセスで効果的
4. Parquet IO Cache — Databricks が自動管理。明示的 cache() より高速なケースあり

### SQL ウェアハウス診断
- コンピュート待ち → サーバーレス / 起動済みクラスタを増やす
- キュー待機中 → クラスタの最大数を増やす
- 長い最適化/プルーニング時間 → 統計情報 & ファイル最適化が必要
