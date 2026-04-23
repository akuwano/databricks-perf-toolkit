# Databricks notebook source
# MAGIC %md
# MAGIC # Spark パフォーマンス最適化 ナレッジベース
# MAGIC
# MAGIC `02_generate_summary_notebook` の LLM 分析時に参照される知識ベース。
# MAGIC ソース: 「データエンジニアリング最適化のベストプラクティス」(板垣 輝広)
# MAGIC
# MAGIC **このノートブックは直接実行するものではありません。**
# MAGIC 他のノートブックから `%run` または知識テキストとして読み込んで使用します。

# COMMAND ----------

# DBTITLE 1,ナレッジベース定義
OPTIMIZATION_KNOWLEDGE_BASE = """
# Databricks Spark パフォーマンス最適化 ナレッジベース

## 1. 最適化の原則

### 1-1. 必要な部分だけを最適化する
- ① 目標を定義してから始める（コスト目標、SLA/パフォーマンス目標）
- ② シンプルで効果の高いものから開始する（80/20の法則：20%の改善ポイントが80%の成果）
- ③ ベンチマークと反復（結果を記録して進捗を把握）
- ④ チューニングのやめ時を判断する

### 1-2. 処理プロセスを理解する
最適化対象は3層に分かれる:
- **クエリー層**: SQL/DataFrame のロジック
- **プロセッシング・エンジン層**: Spark/Photon、オプティマイザ
- **インフラ層**: VM/Serverless、インスタンス選択、スケーリング、ディスクキャッシュ

## 2. コンピュート最適化

### 2-1. Photon エンジン
**最も恩恵を受けるワークロード:**
- 大量データに対する結合と集約などの重い計算処理
- Delta Lake での Merge 処理
- ワイドテーブルの読み書き
- 小数の計算
- DLT と Auto Loader
- 削除ベクターによるデータの更新と削除

**Photon が利用されない場合の対処:**
- 非対応APIの使用（例: collect_set() → collect_list(distinct) を使用）
- UDF / RDD / Typed Dataset → 可能な限り使用を避ける
- Spark UI の Details で Photon Explanation を確認
- "== Photon Explanation == The query is fully supported by Photon." と表示されれば完全対応

**Photon 非対応処理の分類:**

コマンド系処理（DDL/制御系）— コマンド自体は Photon 対象外だが、内部のデータ処理は Photon で実行される場合がある:
- AtomicReplaceTableAsSelect（CREATE OR REPLACE TABLE AS SELECT）
- AtomicCreateTableAsSelect（CREATE TABLE AS SELECT）
- Execute（ストアドプロシージャ等の制御フロー処理）
- WriteFiles（INSERT INTO / COPY INTO の書き込み制御）
- AddJarsCommand（JAR ライブラリのロード）
- Photon Explanation: "Commands are not directly photonized" → 後続ジョブで Photon 利用を確認

非対応オペレータ（UDF/非ネイティブ処理）— データ処理自体が Photon で実行できない:
- FlatMapGroupsInPandas (applyInPandas) → Window関数 + groupBy に書き換え
- ArrowEvalPython (Pandas UDF) → Spark ネイティブ関数に書き換え
- BatchEvalPython (Python UDF) → SparkSQL / PySparkネイティブ関数に書き換え
- SortMergeJoin → Broadcast Hash Join への切り替え（spark.sql.autoBroadcastJoinThreshold 調整）
- ColumnarToRow → 上流の非対応オペレータが原因、そのオペレータを特定して書き換え

### 2-2. Predictive I/O (予測 I/O)
- AIによりコンピュート・エンジンにデータ取得の最適な方法を自動決定
- 非定型クエリにおけるポイント・クエリの高速化（最大17倍）

### 2-3. Adaptive Query Execution (AQE)
- 正確なメトリクスに基づき、実行時に自動的にクエリプランを適応
- 機能:
  - ソートマージ結合(SMJ) → ブロードキャストハッシュ結合(BHJ) への動的切替
  - シャッフル・パーティションの最適化
  - データ・スキューへの対応

### 2-4. SQL ウェアハウスのサイジング
- **垂直スケーリング（クラスターサイズ）**: 大規模クエリの処理速度向上、ディスクスピル解消
- **水平スケーリング（クラスター数）**: 同時クエリ対応
- キューにクエリが多すぎる → クラスタの個数を増やす
- クエリに時間がかかりすぎる → クラスタサイズを大きくする

### 2-5. クラシック・コンピュートの設定ポイント
- **CPU:RAM比率**: 予算に応じて十分なメモリをコアに割り当て
- **プロセッサー種類**: ARMベースのチップは非常に良い性能
- **ローカル・ストレージ**: ディスク・キャッシュは繰り返しアクセスに有効
- **ドライバサイズ**: 過剰にしない（4〜8コア、16〜32GB RAMで十分）
- **Spot可用性**: 長時間稼働ジョブでは安定性がより重要
- **自動スケーリング**: 高いクラスタ利用率を実現しコスト削減

### 2-6. クラスタ選定ガイドライン
- **ジョブ・クラスタ**: ETLジョブ用。実行中のみ課金
- **インタラクティブ・クラスタ**: 開発用。オートスケール/オートポーズ。サブセットで開発推奨
- **シェアードSQL ウェアハウス**: アドホック分析用。サーバーレスで即時起動
- **専用SQLウェアハウス**: BIレポーティング用。適切なサイズ設定で競合回避

## 3. データレイアウト最適化

### 3-1. データレイアウトの原則
不要なファイルを読み込まないようデータを整理する:
- **パーティション・プルーニング**: パーティションキーによるファイル群の除外
- **ファイル・スキッピング**: 統計情報（min/max）によるファイル単位の除外

### 3-2. パーティション使用判断
**使用すべきケース（限定的）:**
- テーブルサイズ > 100TB
- データ隔離のためのスキーマ分離
- パーティション全体の削除が必要なガバナンスユースケース

**ベストプラクティス:**
- 低カーディナリティのカラム（地域、年など）をキーにする
- パーティションサイズは 1GB〜1TB
- 理由が明確でない限りパーティションを使用しない

### 3-3. Liquid Clustering（推奨）
Z-Order や従来のパーティショニングを置き換える推奨手法:
- **高速**: 書き込み時の自動クラスタリング（オーバーヘッド軽微）
- **セルフ・チューニング**: AIによるクラスタリングキーの自動選定（Predictive Optimization）
- **スキューに強い**: OPTIMIZE により一定ファイルサイズを維持
- **フレキシブル**: クラスタリングカラムを既存データに影響なく変更可能

**恩恵を受けるシナリオ:**
- 高カーディナリティ列で頻繁にフィルタリング
- データ分布が偏っているテーブル
- 短期間でサイズが肥大するテーブル
- 同時に複数の書き込みが発生するテーブル
- アクセスパターンが変化するテーブル

### 3-4. Predictive Optimization（予測最適化）
- 最適化の手動スケジューリングが不要
- 自動実行: Vacuum, Optimize, Analyze, Liquid Clusteringキーの自動設定
- AIが費用対効果を優先して自動実行
- サーバーレスコンピュートで実行（クラスタ管理不要）
- システムテーブルで操作・コスト・影響を確認可能

### 3-5. Deletion Vector（削除ベクター）
- ファイル書き換えコストの削減
- DELETE: 削除フラグをセット（ファイル更新なし）
- UPDATE: 削除フラグ + 新規ファイル追加（ファイル全体のリライト不要）

### 3-6. 統計情報とデータスキッピング
- Delta テーブルは最初の32列の統計情報を収集（dataSkippingNumIndexedCols=32）
- Timestamp型とString型は必ずしも有用ではない
- 長い文字列を含む列は32列の外に配置するか、収集列数を減らす
- メタデータのみのクエリ（例: select max(col)）はファイルを見ずにDeltaログだけで回答可能

### 3-7. ファイルサイズ
- **小さいファイル**: フィルタ効率高、更新高速、ファイル数増加
- **大きいファイル**: フィルタ非効率、更新低速、ファイル数削減
- delta.tuneFileSizesForRewrites=true: 頻繁な書き換えテーブルのファイルサイズを小さく調整

## 4. コード最適化

### 4-1. 基本原則
- 本番ジョブでは count(), display(), collect() を最小限に
- シングルスレッド Python/Pandas/Scala を避ける → Spark 上の Pandas API を使用
- Python UDF を避ける → PySpark ネイティブ関数か Pandas UDF（ベクトル化UDF）
- RDD より DataFrame を使用（RDD は CBO/Photon 利用不可）
- Typed Dataset は Photon 利用不可

### 4-2. UDF の種類と性能比較
| 種類 | 特徴 | パフォーマンス | Photon |
|------|------|-------------|--------|
| Python UDF | JVMとの通信で遅い | 遅い | 不可 |
| Pandas(ベクトル化) UDF | Arrow で高速化 | 高速 | 不可 |
| Scala UDF | JVM内で直接実行 | 高速 | 不可 |
| SparkSQL | Catalyst最適化 | 非常に高速 | 対応 |
| PySparkネイティブ関数 | Spark最適化済み組み込み関数 | 非常に高速 | 対応 |

### 4-3. ストリーミング最適化
- 可能な場合すべてのETLにストリーミングを検討
- CDC アーキテクチャで処理時間を大幅短縮
- バッチサイズの調整: maxFilesPerTrigger, maxBytesPerTrigger

### 4-4. Broadcast Join
- 最もパフォーマンスの高い結合タイプ
- 小さいテーブルが autoBroadcastJoinThreshold（デフォルト30MB）より小さい場合にトリガ
- Dynamic File Pruning (DFP) の前提条件

## 5. 5Sボトルネックと緩和策

### 5-1. ① スキュー (Skew)
**症状:** 処理対象データサイズの不均衡
**緩和策:**

JOIN のスキュー対策（AQE skewJoin）:
- spark.sql.adaptive.skewJoin.enabled=true
- 【重要】skewJoin は SortMergeJoin のみ対象。ShuffledHashJoin、BroadcastHashJoin、GroupBy/Aggregate には効かない
- スキューパーティション閾値: spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes（デフォルト256MB）— この値を超えるパーティションがスキュー候補になる
- スキューパーティション係数: spark.sql.adaptive.skewJoin.skewedPartitionFactor（デフォルト5）— 中央値のN倍以上でスキュー判定。スキューが検出されない場合は係数を下げる（例: 3）
- advisoryPartitionSizeInBytes の調整 — スキュー分割後の目標サイズ
- スキューヒント: SELECT /*+ SKEW('table', 'col') */

GroupBy/Aggregate のスキュー対策（skewJoin は効かない）:
- ソルティング: キーにランダム値を追加して複数パーティションに分散後、再集約
- 二段階集約: まず部分キーで集約、次に最終キーで集約
- spark.sql.shuffle.partitions の増加でパーティションあたりのデータ量を削減

共通の対策:
- AQE を有効化: spark.sql.adaptive.enabled=true
- spark.sql.adaptive.coalescePartitions.enabled=true
- 明示的な repartition() でデータを分割
- Liquid Clustering の導入でデータ分布の偏りを緩和

### 5-2. ② スピル (Spill)
**症状:** メモリ不足による一時ファイルのディスクへの書き込み
**緩和策:**
- ワーカーあたりにより多くのメモリを持つクラスタを割り当て
- spark.executor.memory の調整
- spark.memory.fraction の調整
- スキューが原因の場合はまずスキューに対処
- パーティション数を増やしてパーティションサイズを小さくする
- spark.sql.shuffle.partitions の調整
- spark.sql.files.maxPartitionBytes の調整

### 5-3. ③ シャッフル (Shuffle)
**症状:** ワーカーノード間でデータを移動する処理
**緩和策:**
- より少数のより大きなワーカーを使用（ノード間シャッフル削減）
- Broadcast Join が可能か確認
- シャッフルされるデータ量を減らす（列の絞り込み、事前フィルタリング）
- spark.sql.shuffle.partitions の調整
- Tips: 大きくて少ないノード > 小さくて多いノード
- すべてのシャッフルを外すことにこだわらない（シャッフルより高コストな操作に集中）

### 5-4. ④ スモールファイル (Small Files)
**症状:** 小さなファイルによる高いオーバーヘッド
**緩和策:**
- OPTIMIZE でファイルをコンパクト化・クラスタ化
- Vacuum で古いバージョンを削除・メタデータをクリーンアップ
- Predictive Optimization で自動化
- Auto Optimize を有効化:
  - spark.databricks.delta.optimizeWrite.enabled=true（128MBに書き込みサイズ調整）
  - spark.databricks.delta.autoCompact.enabled=auto（書き込み後にファイルサイズ改善）

### 5-5. ⑤ シリアライゼーション (Serialization)
**症状:** データやコードの変換・転送処理による遅延
**緩和策:**
- UDF 使用を最小限に → PySparkネイティブ関数か SparkSQL 関数を推奨
- Python UDF は PythonとJVM間のデータ通信（シリアライズ/デシリアライズ）が必要
- UDF は Photon エンジン利用不可、Catalyst オプティマイザの内部最適化が不十分

## 6. パフォーマンス診断

### 6-1. SQL ウェアハウス診断
- **同じ実行時間 ≠ 同じパフォーマンス**
  - コンピュート待ち → サーバーレス / 起動済みクラスタを増やす
  - キュー待機中 → クラスタの最大数を増やす
- **同じ実行時間 ≠ 同じ実行プラン**
  - 長い最適化/プルーニング時間 → 統計情報 & ファイル最適化が必要
  - 長いクエリ実行時間 → プラン改善 or コンピューティングリソース追加

### 6-2. クエリプロファイルで確認すべき項目
- Photon が 100% に近いことを確認
- ディスク・キャッシュのヒット率を確認
- 読み込んだ行数は理にかなっているか
- スピルはウェアハウスの RAM 不足を示す
- ファイルやパーティションの数が多すぎないか

### 6-3. Spot インスタンスのロスト対策
**推奨設定:**
- spark.decommission.enabled=true（Graceful Decommission 有効化）
- spark.storage.decommission.enabled=true（シャッフルデータの事前退避）
- spark.storage.decommission.shuffleBlocks.enabled=true
- spark.storage.decommission.rddBlocks.enabled=true
- spark.decommission.graceful.timeout=120s
- spark.speculation=true（投機実行）
- クラスタ構成: Driver は On-Demand、Worker は Spot + フォールバック On-Demand

## 7. まとめ — 最適化の7つのポイント
1. 何を目標に最適化するのかを定義する
2. まずは簡単にできることから始める（プラットフォーム→データ→クエリ）
3. 最新のコンピュートと機能の活用（サーバーレス、Photon、予測最適化、Liquid Clustering）
4. ワークロードに基づき垂直方向または水平方向に拡張する
5. 可能な限りストリーミングによるインクリメンタル処理で実装する
6. 最適化の効果を測定するためにモニタリングツールを利用する
7. 長期間にわたる最適化を回避するため、終了タイミングを判断する

## 8. Shuffle パーティションチューニング
ソース: Fine-Tuning Shuffle Partitions in Apache Spark (blog.dataengineerthings.org)

### 8-1. デフォルト値の問題
- spark.sql.shuffle.partitions のデフォルト 200 は 20GB 超のデータでは不適切
- パーティションサイズが大きすぎるとメモリ不足（OOM）、小さすぎるとタスクオーバーヘッド増大

### 8-2. 最適パーティションサイズの計算
- **目標パーティションサイズ**: 100〜200MB（128MB 推奨）
- **計算式**: パーティション数 = ステージ入力データ(MB) ÷ 目標サイズ(MB)
- **例**: 210GB のシャッフルデータ → 210,000MB ÷ 128MB = 1,640 パーティション

### 8-3. コア数との調整
- パーティション数 ≥ クラスタ総コア数を確保（コアが遊ばないように）
- パーティション数をコア数の倍数に丸める（バッチ処理の効率化）
- 例: 1,640 パーティション、2,000 コア → 2,000 パーティションに調整

### 8-4. AQE による自動最適化（Databricks 推奨）
- spark.sql.adaptive.enabled=true（デフォルト有効）
- spark.sql.adaptive.coalescePartitions.enabled=true
- AQE はシャッフル後のパーティションを実行時に自動調整
- ただし初期パーティション数（spark.sql.shuffle.partitions）は上限として機能するため、大きめに設定しておくことが重要

## 9. キャッシュ戦略と Lineage 管理

### 9-1. キャッシュ戦略の比較
同じ DataFrame を複数回使う場合や、深い Lineage のプラン最適化オーバーヘッドを解消する場合に、
適切なキャッシュ戦略を選択する。

**Lineage 保持型（キャッシュ消失時に Lineage から再計算で回復可能）:**
- cache() = persist(MEMORY_AND_DISK): 最も手軽。書き込み Photon ✅ / 読み込み Photon ❌（InMemoryTableScan は行指向 → PhotonRowToColumnar 変換が発生）
- persist(MEMORY_ONLY): メモリのみ。小データ向け。読み込み Photon ❌（InMemoryTableScan は行指向）
- persist(DISK_ONLY): ローカルディスク。読み込み Photon ✅（ディスク列指向読み込み）
- persist(MEMORY_AND_DISK_SER): シリアライズ形式でメモリ節約。読み込み Photon ❌

**Lineage 切断型（深い Lineage のプラン最適化を回避）:**
- localCheckpoint(): メモリに実体化。読み込み Photon ❌（RDD スキャン）。Spot ロスト時回復不可
- checkpoint(): リモートストレージに永続化。読み込み Photon ❌（RDD スキャン）。Spot ロスト耐性あり
- Delta テーブル書き出し: クラウドストレージに永続化。読み込み Photon ✅（Scan parquet）。Spot ロスト耐性あり

### 9-2. Lineage 保持と切断の違い
- Lineage 保持: キャッシュ有効時はキャッシュから読み込み（Lineage は実行されない）。
  キャッシュ消失時（Executor 消失・メモリ追い出し等）に Lineage を辿って再計算で回復可能
- Lineage 切断: キャッシュ消失時に回復不可（localCheckpoint）またはストレージから回復（checkpoint/Delta）。
  深い Lineage の Catalyst オプティマイザ処理時間を解消できる

### 9-3. Photon 環境での推奨
1. persist(DISK_ONLY) — 読み込み Photon 対応 + Lineage 保持。メモリに収まらない大データ向け
2. Delta テーブル書き出し — 読み込み Photon 対応 + Lineage 切断 + Spot 耐性
3. cache() — 読み込み Photon 非対応（行→列変換オーバーヘッド）だが手軽。3回以上のアクセスで効果的
4. checkpoint() — 読み込み Photon 非対応。Spot 環境でのデータ安全性確保用
5. localCheckpoint() — Spot 環境では推奨しない

### 9-4. Spark cache vs Parquet IO Cache（Databricks）
Databricks 環境では `df.cache()` より Parquet IO Cache（ディスクキャッシュ）の方が速いケースがある:
- **Spark cache（`df.cache()`）**: 全カラムをデシリアライズして JVM ヒープに保持 → メモリ膨張、GC 負荷増大（Stop the World）、カラム pruning / フィルタ pushdown が効かない
- **Parquet IO Cache**: 圧縮 Parquet のままローカル SSD に格納 → GC 負荷なし、カラム pruning / フィルタ pushdown が有効、Photon も利用可能
- Parquet IO Cache は Databricks が自動的に管理するため、明示的な `cache()` 呼び出しが不要
- 検証結果: 同一データ・同一クエリで Spark cache より Parquet IO Cache（キャッシュなし）の方が高速だった

### 9-5. Spot インスタンス利用時の注意
- cache() / persist() / localCheckpoint() → ローカル保持。Spot ロストでデータ消失
  - cache/persist: Lineage から再計算可能（遅いが回復可能）
  - localCheckpoint: Lineage 切断済みのため回復不可（ジョブ失敗）
- checkpoint() / Delta テーブル書き出し → リモートストレージ永続化。Spot ロスト耐性あり
- Spot 環境では長時間処理の中間結果に checkpoint() または Delta テーブル書き出しを推奨

## 10. Executor 設定とクラスタ最適化
ソース: AWS EMR Best Practices - Spark Performance

### 9-1. Executor コア・メモリ設計
- **spark.executor.cores**: 4〜5 を起点とする
- **メモリ計算式**: (YARN メモリ ÷ Executor 数) × (1 - 0.1875)
  - 0.1875 = メモリオーバーヘッド率（spark.executor.memoryOverheadFactor）
- **例**: r4.8xlarge (241,664MB YARN) → 4コア Executor × 10 = 24,544MB/Executor

### 9-2. Driver 設定
- デフォルト 2GB で大半のケースは十分
- 1,000+ Executor のハートビート管理、または collect()/take() で大量結果を取得する場合のみ増加

### 9-3. シリアライゼーション
- **Kryo Serializer 推奨**: Java Serializer より 10 倍高速
- spark.serializer=org.apache.spark.serializer.KryoSerializer
- spark.kryo.registrationRequired=true（未登録クラスでエラーにして検出）
- spark.kryoserializer.buffer=1024k（デフォルト 64k から増加）
- spark.kryoserializer.buffer.max=1024m（デフォルト 64m から増加）

### 9-4. GC チューニング
- **G1GC 推奨**: デフォルトの Parallel GC より停止時間が短い
- 設定: -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35
- **目標**: GC 時間 ≤ タスク実行時間の 1%
- **警告閾値**: GC 25% 以上は深刻なパフォーマンス問題

### 9-5. API 選択
- reduceByKey() > groupByKey()（ネットワーク転送量を削減）
- coalesce() > repartition()（パーティション数を減らす場合、シャッフル不要）
- DataFrame > RDD（Catalyst オプティマイザ + Photon 利用可能）

### 9-6. 大規模クラスタ（100+ ノード）
- Blacklisting 有効化で障害ノードを自動排除:
  - spark.blacklist.killBlacklistedExecutors=true
  - spark.blacklist.application.fetchFailure.enabled=true
- Dynamic Allocation: 95% のワークロードで有効のままが最適

### 9-7. I/O 最適化
- I/O 集約型ワークロードでは HDFS を中間ストレージとして利用
- S3 への書き込みは S3DistCp でバッチ転送（S3 スロットリング回避）
- spark.speculation は EMRFS + Parquet 書き込み時のみ有効化（それ以外はデータ重複リスク）

### 9-8. ファイルフォーマットと圧縮
- **推奨**: Parquet（最もパフォーマンスが良く、コミュニティサポートが広い）
- **圧縮**: Snappy（Parquet デフォルト）、ZLIB（ORC デフォルト）— 両方 OK
- **非推奨**: GZIP（分割不可 → Executor OOM のリスク）
- **パーティションごとのファイルサイズ**: 128MB 以上を目標

## 11. System CPU 高騰パターン（PySpark UDF）

### 11-1. 現象
- PySpark UDF（`@F.udf`）を大量の行に適用すると、クラスタメトリクスで **System CPU（カーネル時間）が User CPU を上回る** 状態になる
- 実際の計算（User CPU）よりも、データの受け渡し（System CPU）の方が重くなる

### 11-2. 重要な区別
- **`@F.udf`（Python UDF）** → System CPU が高騰する（行単位の IPC）
- **`@pandas_udf`（Pandas UDF / GROUPED_MAP）** → System CPU は高騰しない（バッチ単位で効率的に転送）

### 11-3. 原因
- `@F.udf` は行単位で JVM↔Python 間の Apache Arrow シリアライズ/デシリアライズが発生
- データ転送はパイプ経由の `read()`/`write()` **システムコール**で行われ、これがカーネル空間で実行される
- `spark.sql.execution.arrow.maxRecordsPerBatch` が小さいほど syscall 回数が増加し悪化

### 11-3b. Spark UI での判別方法
Spark UI の物理プランで UDF の種類を判別できる:
- **`BatchEvalPython`** → `@F.udf`（Python UDF）— 行単位 IPC で System CPU 高騰のリスクあり
- **`ArrowEvalPython`** → `@pandas_udf`（SCALAR）— バッチ単位転送で効率的
- **`FlatMapGroupsInPandas`** → `@pandas_udf`（GROUPED_MAP）— グループ単位、groupBy 必須（シャッフル負荷あり）

### 11-3c. UDF タイプ比較表

| 方式 | スカラー/グルーピング | groupBy | shuffle | IPC | Spark UI プラン表記 |
|---|---|---|---|---|---|
| Python UDF (@F.udf) | スカラー | なし | なし | 行単位 | BatchEvalPython |
| pandas_udf (SCALAR) | スカラー | なし | なし | バッチ単位 | ArrowEvalPython |
| pandas_udf (GROUPED_MAP) | グルーピング | あり | あり | グループ単位 | FlatMapGroupsInPandas |
| mapInPandas | スカラー | なし | なし | パーティション単位 | MapInPandas |
| mapInArrow | スカラー | なし | なし | パーティション単位（Arrow変換なし） | MapInArrow |
| Spark ネイティブ | 両方 | 任意 | agg時あり | なし | WholeStageCodegen / HashAggregate 等 |

IPC の転送単位:
- 行単位: 1行ずつ Python に渡す → IPC 回数が最大 → System CPU 高騰
- バッチ単位: maxRecordsPerBatch（デフォルト10,000行）ずつまとめて渡す
- グループ単位: groupBy のキーが同じ行をまとめて渡す（グループ数分の IPC）
- パーティション単位: Spark のパーティションごとにまとめて渡す → IPC 回数が最小
- IPC 回数: 行単位 >> グループ単位 > バッチ単位 ≧ パーティション単位

### 11-3d. maxRecordsPerBatch の影響範囲
- `spark.sql.execution.arrow.maxRecordsPerBatch`（デフォルト: 10,000）
- **`@F.udf`（Python UDF）と `@pandas_udf`（SCALAR）に影響** — バッチサイズが小さいほど IPC 回数が増え System CPU が上がる
- **`@pandas_udf`（GROUPED_MAP）には効かない** — GROUPED_MAP はグループ単位でデータ転送するため、バッチサイズではなくグループサイズで IPC データ量が決まる

### 11-3e. 実測パフォーマンスランキング（4000万行、1行→7行展開処理）
1. mapInArrow — 10.3 秒（最速）
2. Spark ネイティブ — 17.6 秒
3. mapInPandas — 23.5 秒
4. pandas_udf (SCALAR) — 42.6 秒
5. Python UDF (@F.udf) — 286.8 秒（最遅、28倍）

注意: Spark ネイティブが常に最速とは限らない。「1行→複数行の展開 + 条件分岐」のような処理では、Spark ネイティブの explode + when/otherwise が冗長になり、mapInArrow で Python 側でループした方が速いケースがある。

### 11-4. 対策
- **Spark ネイティブ関数への置き換え**: JVM 内で完結し、Python との往復が不要になる（最も効果的）
- **`@F.udf` → `@pandas_udf` への書き換え**: バッチ転送で IPC 効率が改善し、System CPU 高騰を回避できる
- **バッチサイズ増加**: `spark.sql.execution.arrow.maxRecordsPerBatch` をデフォルト（10,000）以上にする
- **Photon 非対応**: ArrowEvalPython (Pandas UDF) / BatchEvalPython (Python UDF) は Photon で実行されないため、可能な限り SparkSQL / PySpark ネイティブ関数に書き換える
"""

# COMMAND ----------

# DBTITLE 1,ボトルネック別推奨アクション辞書
BOTTLENECK_RECOMMENDATIONS = {
    "DATA_SKEW": {
        "severity": "MEDIUM",
        "description": "処理対象データサイズの不均衡",
        "recommendations": [
            "【JOIN スキュー】AQE skewJoin を有効化: spark.sql.adaptive.skewJoin.enabled=true（SortMergeJoin のみ対象。ShuffledHashJoin, BroadcastHashJoin, GroupBy/Aggregate には効かない）",
            "【JOIN スキュー】スキューパーティション閾値: spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes（デフォルト256MB）— タスクあたりのShuffle Readに基づき適切な値を設定",
            "【JOIN スキュー】スキューパーティション係数: spark.sql.adaptive.skewJoin.skewedPartitionFactor（デフォルト5）— task_skew_ratio が大きい場合は係数を下げる（例: 3）",
            "【JOIN スキュー】advisoryPartitionSizeInBytes を調整（デフォルト64MB）— スキュー分割後の目標パーティションサイズ",
            "【JOIN スキュー】スキューヒント: SELECT /*+ SKEW('table', 'col') */",
            "【GroupBy スキュー】ソルティング: キーにランダム値を追加して分散後、再集約（skewJoin は効かない）",
            "【GroupBy スキュー】二段階集約: まず部分キーで集約、次に最終キーで集約",
            "【共通】spark.sql.shuffle.partitions の増加でパーティションあたりのデータ量を削減",
            "【共通】明示的な repartition() でデータを均等に分割",
            "【共通】Liquid Clustering の導入でデータ分布の偏りを緩和",
        ],
    },
    "DISK_SPILL": {
        "severity": "HIGH",
        "description": "メモリ不足によるディスクへの一時ファイル書き込み",
        "recommendations": [
            "spark.executor.memory を増加（メモリ計算式: (ノードメモリ÷Executor数)×(1-0.1875)）",
            "spark.memory.fraction を調整（デフォルト0.6）",
            "スキューが根本原因の場合はまずスキューに対処",
            "spark.sql.shuffle.partitions を増やしてパーティションサイズを128MB以下に縮小（計算式: 入力データMB÷128）",
            "spark.sql.files.maxPartitionBytes を調整",
            "ウェアハウスの場合はクラスタサイズを拡大（より多くの RAM）",
            "Executor cores を 4〜5 に設定してコアあたりのメモリを確保",
        ],
    },
    "HIGH_GC": {
        "severity": "MEDIUM",
        "description": "GC オーバーヘッドが高い（JVM ヒープ圧迫）",
        "recommendations": [
            "UDF・collect の見直し — オブジェクト生成を削減",
            "Python UDF を PySparkネイティブ関数に置き換え（Photon 対応かつ GC 不要）",
            "spark.executor.memory を増加",
            "G1GC に切り替え: -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35（目標: GC ≤ タスク時間の1%、25%以上は深刻）",
            "broadcast 変数の適切な利用でデータ複製を削減",
            "Kryo Serializer の導入でオブジェクトのシリアライズ/デシリアライズ負荷を軽減",
        ],
    },
    "HEAVY_SHUFFLE": {
        "severity": "LOW",
        "description": "大量のデータがワーカーノード間で移動",
        "recommendations": [
            "Broadcast Join を検討: spark.sql.autoBroadcastJoinThreshold を調整（デフォルト10MB）",
            "シャッフルされるデータ量を減らす（列の絞り込み、事前フィルタリング）",
            "Shuffle パーティション数の最適化: 目標128MB/パーティション、計算式=入力データ(MB)÷128",
            "パーティション数をクラスタ総コア数の倍数に調整（コアが遊ばないように）",
            "spark.sql.shuffle.partitions を調整",
            "より少数のより大きなワーカーを使用してノード間転送を削減",
            "Tips: すべてのシャッフルを外すことにこだわらない — シャッフルより高コストな操作に集中",
        ],
    },
    "STAGE_FAILURE": {
        "severity": "HIGH",
        "description": "ステージの実行失敗",
        "recommendations": [
            "failure_reason を確認 — OOM の場合は executor memory を増加",
            "FetchFailedException の場合は Spot インスタンスのロストを疑う",
            "spark.task.maxFailures を調整（デフォルト4）",
            "spark.speculation=true で投機実行を有効化",
        ],
    },
    "MEMORY_SPILL": {
        "severity": "LOW",
        "description": "メモリスピル（ディスクスピルの前段階）",
        "recommendations": [
            "spark.memory.fraction を増加",
            "repartition() でパーティションサイズを削減",
            "不要なキャッシュ（.cache()/.persist()）を解除",
        ],
    },
    "SPOT_LOSS": {
        "severity": "HIGH",
        "description": "Spot インスタンスのロストによる Executor 消失",
        "recommendations": [
            "spark.decommission.enabled=true（Graceful Decommission 有効化）",
            "spark.storage.decommission.enabled=true（シャッフルデータの事前退避）",
            "spark.storage.decommission.shuffleBlocks.enabled=true",
            "spark.storage.decommission.rddBlocks.enabled=true",
            "spark.decommission.graceful.timeout=120s",
            "spark.speculation=true（投機実行で遅延タスクをカバー）",
            "クラスタ構成: Driver は On-Demand、Worker は Spot + フォールバック On-Demand",
            "AWS: capacity-optimized 割り当て戦略、複数インスタンスタイプで中断リスク分散",
        ],
    },
    "PHOTON_FALLBACK": {
        "severity": "MEDIUM",
        "description": "Photon エンジンが利用されず Classic Spark にフォールバック",
        "recommendations": [
            "コマンド系処理（AtomicReplaceTableAsSelect等）はコマンド自体がPhoton対象外だが、内部のデータ処理はPhotonで実行される場合がある。後続ジョブのPhoton利用状況を確認",
            "applyInPandas (FlatMapGroupsInPandas) → Window関数 + groupBy + join に書き換え",
            "Python UDF (BatchEvalPython) → SparkSQL / PySparkネイティブ関数に書き換え",
            "Pandas UDF (ArrowEvalPython) → F.sqrt(), F.sin() 等のネイティブ関数に書き換え",
            "SortMergeJoin → Broadcast Hash Join への切り替え（spark.sql.autoBroadcastJoinThreshold 調整）",
            "collect_set() → collect_list(distinct) を使用",
            "RDD / Typed Dataset の使用を避ける",
            "Spark UI の Photon Explanation で非対応ノードを特定",
        ],
    },
    "SKEW_SHUFFLE_PARALLELISM": {
        "severity": "HIGH",
        "description": "データスキューによりシャッフル後のパーティションの大部分が空になり実効並列度が低下",
        "recommendations": [
            "AQE skewJoin は SortMergeJoin のみ対象 — ShuffledHashJoin, BroadcastHashJoin, groupBy/Aggregate には効かない",
            "ソルティング: キーにランダム値を追加して複数パーティションに分散後、再集約",
            "二段階集約: まず部分キーで集約、次に最終キーで集約",
            "spark.sql.shuffle.partitions を実データの分布に合わせて調整",
            "Liquid Clustering でデータ分布を事前に均等化",
        ],
    },
    "SMALL_FILES": {
        "severity": "MEDIUM",
        "description": "大量の小さなファイルによる高いオーバーヘッド（タスクあたり読み取り < 10MB）",
        "recommendations": [
            "OPTIMIZE でファイルをコンパクト化・クラスタ化",
            "spark.databricks.delta.optimizeWrite.enabled=true（128MBに書き込みサイズ調整）",
            "spark.databricks.delta.autoCompact.enabled=auto（書き込み後にファイルサイズ改善）",
            "Predictive Optimization を有効化して自動 OPTIMIZE / Vacuum",
            "Vacuum で古いバージョンを削除・メタデータをクリーンアップ",
            "目標ファイルサイズ: 128MB〜1GB",
        ],
    },
}
