# Databricks SQL チューニングガイド

## 概要
<!-- section_id: overview -->

本ドキュメントはDatabricks SQLのパフォーマンスチューニングに関するベストプラクティスをまとめたものです。クエリプロファイラの分析結果に基づいた具体的な最適化手法を解説します。

---

## 1. I/Oの効率化
<!-- section_id: io -->

### 1.1 Liquid Clustering（推奨）

Liquid Clusteringは従来のパーティショニングとZ-Orderを置き換える新しいデータレイアウト最適化機能です。**新規テーブルやテーブル再設計時には、まずLiquid Clusteringを検討してください。**

**推奨事項:**
- WHERE句やJOIN条件で頻繁に使用されるカラム（最大4カラム程度）をクラスタリングキーに指定
- Shuffle操作の削減に効果的
- 大量メモリ使用(100GB以上)の場合に特に有効
- 自動的にデータレイアウトが最適化されるため、運用負荷が低い
- パーティショニング+Z-Orderの組み合わせより設計がシンプル

**設定例:**
```sql
-- 新規テーブル作成時
CREATE TABLE my_table (...)
CLUSTER BY (col1, col2);

-- 既存テーブルへの適用
ALTER TABLE my_table
CLUSTER BY (col1, col2);
-- FULLオプションで既存データも再クラスタリング（必須）
-- FULLなしでは新規レコードのみクラスタリングされる
OPTIMIZE my_table FULL;
```

### 1.1.1 Hierarchical Clustering（階層型クラスタリング）

従来のLiquid Clusteringは全カラムを均等にクラスタリングしますが、Hierarchical Clusteringは**低カーディナリティのカラムを先に完全クラスタリング**してから、残りのカラムを処理します。従来の「パーティショニング + ZORDER」に相当する性能を、Liquid Clusteringの中で実現します。

**効果（TPC-DS SF10kベンチマーク）:**
- OPTIMIZE時間: **31%削減**
- Write Amplification: **26%削減**
- クエリ時間: **22%削減**

**適用すべきケース:**
- 日付カラムなど**低カーディナリティ（〜数千値）のフィルタが大半**のワークロード
- 元々パーティション + ZORDERだったテーブルのLiquid Clustering移行
- マルチテナント/リージョン分割テーブル（`WHERE region = 'JP'` が常にあるパターン）

**適用すべきでないケース:**
- 全クラスタリングカラムが高カーディナリティ（user_id等）
- フィルタパターンが均等で特定カラムに偏りがない
- 小テーブル（クラスタリング効果自体が薄い）

**注意事項:**
- 非階層カラムのみのクエリ性能が低下する可能性がある
- 高カーディナリティのカラムを階層指定するとOPTIMIZE時間が増加

**設定例:**
```sql
-- 日付カラムを階層化（低カーディナリティ → 階層に最適）
ALTER TABLE my_table
CLUSTER BY (date_col, id_col);

ALTER TABLE my_table SET TBLPROPERTIES(
  'delta.liquid.hierarchicalClusteringColumns' = 'date_col'
);

OPTIMIZE my_table FULL;
```

**高速書き込み（Eager Clustering Fast Path）:**
- DBR 17.3+: デフォルトで有効
- DBR 17.1/17.2: テーブルプロパティ `delta.liquid.eagerClusteringFastPathMode = 'forceEnabled'` を設定

**自動有効化ロードマップ:**
- DBR 17.3でシャドウ評価開始（低カーディナリティ自動検出）
- 将来的に`CLUSTER BY AUTO`で自動選択

### 1.1.2 Eager Clustering の無効化（書き込みオーバーヘッド対策）
<!-- section_id: eager_clustering_disable -->

Liquid Clustering 適用テーブルへの INSERT/MERGE で、書き込み前に大きな shuffle と spill（ClusterOnWrite オーバーヘッド）が発生する場合、**CLUSTER BY 自体を外す代わりに eager clustering のみ無効化**して書き込みコストを下げられます。事後 OPTIMIZE で再クラスタリングを行うモデル。

**適用シーン:**
- LC テーブルへの一度限りの大量 INSERT/MERGE で shuffle spill が支配的
- CLUSTER BY を完全撤去すると読み込み側の file pruning 効果まで失うため避けたい
- 定期的な ETL バッチで書き込み時間を優先したい

**設定:**
```sql
ALTER TABLE <target-table> SET TBLPROPERTIES (
  'delta.liquid.forceDisableEagerClustering' = 'True'
);

-- 以降の INSERT / MERGE は shuffle/spill が大幅減少
INSERT INTO <target-table> SELECT ... FROM <source>;

-- 事後に再クラスタリング（読み込み側の pruning 効果を回復）
OPTIMIZE <target-table> FULL;
```

**トレードオフ:**
- ✅ 書き込み時の shuffle/spill を大幅削減（1 TB 規模の spill が典型的なワークロードで消失）
- ⚠️ OPTIMIZE 未実行の間、file pruning 効果が低下する
- ⚠️ 高頻度クエリの本番テーブルでは定期 OPTIMIZE の運用が必須

**判断基準:**
- CLUSTER BY の定義は**保持したい**（読み込み時のメタデータは使う）
- 書き込みコスト > 書き込み頻度 × 読み込み劣化時間 の場合に選択

### 1.2 パーティショニング（限定用途）

パーティショニングは以下の**限定されたケース**でのみ推奨します。

**推奨ケース:**
- **データライフサイクル管理が主目的**の場合（日付単位でのデータ削除・アーカイブなど）
- 超高頻度で利用される単一の粗い絞り込み軸（例: 日付）があり、パーティション数が適切に管理できる場合

**注意事項:**
- クエリパフォーマンスの最適化が目的であれば、Liquid Clusteringを優先してください
- パーティション数が数千を超えるとパフォーマンスに悪影響
- パーティション数は最大でも1000-2000程度を目安とする
- 多数の小規模ファイル構成とならないようパーティション単位でOptimizeを実行する

**ESチケット頻出パターン（63件）:** 高カーディナリティ分割（user_id, order_id等）は小ファイル増加・メタデータ負荷・並列度悪化を招く最多原因。パーティションはスキャン削減よりデータライフサイクル管理を主目的とし、日付など粗い軸に限定すること。

### 1.3 Z-Order（限定用途）

Z-Orderは**Liquid Clusteringが使用できない場合のみ**検討してください。

**使用ケース（限定）:**
- Liquid Clusteringが技術的に使用できない場合（機能制約・互換性の問題など）
- 既存のパーティション設計を維持しつつ、追加で多列フィルタの局所性を向上させたい場合

**設定時の注意:**
- 4カラム程度を目安とする
- カラム数が多いテーブルでは先頭の32カラムにZ-Order対象のカラムを配置
- 定期的な`OPTIMIZE ... ZORDER BY`の実行が必要（運用負荷がLiquid Clusteringより高い）

**ESチケット頻出パターン（42件）:** Z-Orderは統計がない列や更新頻度の高い表では効果が安定しない。3カラム超で効果が急減。LCが使えるならLCを優先し、Z-Orderはレガシーテーブルの補完策として扱う。

**参考:** https://kb.databricks.com/delta/zordering-ineffective-column-stats

### 1.3.1 統計鮮度とData Skipping

Data Skipping・AQE・Broadcast判定は**列統計の鮮度に依存**します。性能劣化時は実行計画だけでなく、統計未更新や偏ったファイル分布も併せて確認してください。

**ESチケット頻出パターン（68件）:** Data Skipping無効化の最多原因は「フィルタ列に関数適用」と「統計未収集」。`WHERE YEAR(date_col) = 2024` は pushdown されない — `WHERE date_col >= '2024-01-01'` に変換すること。SHOW TBLPROPERTIESで列統計のmin/maxを確認。

### 1.4 クエリプロファイルによるクラスタリングキー候補の選定

#### クラスタリングキー候補の特定
- 各テーブルのScanセクションにプロファイラのフィルター条件が表示されます
- ここで使用されているカラムがLiquid Clusteringのクラスタリングキー候補になります
- **注意:** 結合処理などで対象テーブルに直接フィルタ条件が付与されていない場合、Filter条件がプロファイラに表示されないことがあります。クエリを必ず確認してください

#### クエリのWhere句によるフィルター条件の確認
- 結合処理などで対象テーブルに直接フィルタ条件が付与されていない場合は、クエリ解析時にFilter条件がプロファイラに表示されない場合があります
- このような場合でも実行時フィルタは機能しますので、クエリのWhere句を確認しカラムを選定してください

### 1.5 I/O削減効果の確認

プロファイラから実行時のメトリクス情報で確認できます:

| メトリクス | 説明 |
|-----------|------|
| Files pruned | READをスキップしたファイル数 |
| Files read | READしたファイル数 |
| Files to read before dynamic pruning | 動的プルーニング前のファイル数 |
| Partitions read | READ対象となったパーティション数 |
| Size of files pruned | プルーニングされたファイルのサイズ |
| Size of files read | 読み込まれたファイルのサイズ |
| Size of data read with io requests | 実際のI/Oリクエストで読み込まれたデータサイズ |

**フィルタ率の計算方法:**
```
プルーニング効率 = Size of files pruned / (Size of files read + Size of files pruned)
```
高い値ほどI/O削減が効率的に機能しています。

### 1.6 Predictive I/Oの効果確認

下記の項目を確認:
- `data filters - batches skipped`
- `data filters - rows skipped`

---

## 2. 実行プランの改善
<!-- section_id: execution_plan -->

### 2.1 結合タイプの概要

クエリプロファイルでテーブル結合処理のプランを確認してください。データ量などの条件にも依存しますが、多くの場合Photonエンジンで処理可能なBROADCAST/SHUFFLE_HASHの2プランのみとなるのが望ましいです。

> **注意:** SHUFFLE_HASHはPhoton対応と記載されていますが、実際のPhoton実行はJOIN型（INNER vs OUTER）、
> キー形状、プラン全体のコンテキストに依存します。LEFT OUTER JOINにSHUFFLE_HASHを適用しても、
> DBRバージョンによってはPhoton非対応にフォールバックする場合があります。SHUFFLE_HASHヒント適用後は、
> 必ずクエリプロファイルでPhoton実行を確認してください。

**パフォーマンス順位（右に行くほど高速）:**
```
シャッフル-ネストループ結合 < ソート-マージ結合 < シャッフル-ハッシュ結合 < ブロードキャスト結合
```

参考: https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-hints#join-hint-types

#### ブロードキャスト結合
- 片方のデータが閾値より小さいことが必要
- シャッフルやソートがない
- 非常に高速
- **Photonエンジン対応**

#### シャッフル-ハッシュ結合
- シャッフルは必要だが、ソートは不要
- 大きなテーブルを扱える
- (統計に基づいて）小さい側を選択
- データの偏りが大きいとメモリエラー(OOM)になる場合がある
- **Photonエンジン対応**

#### ソート-マージ結合
- 最も堅牢だがシャッフルとソートが必要であるためリソースを大量に消費
- あらゆるデータサイズに対応
- テーブルサイズが小さい場合には遅くなることがある
- **Photonエンジン非対応**

#### シャッフル-ネストループ結合
- リソース使用量を低減できる場合がある
- 行の取り出し開始は高速だが処理全体を完了するまでには時間がかかる場合がある
- 等価結合では使用されません
- **Photonエンジン対応**

### 2.2 低速なクエリプラン例：Sort-Merge結合

ソート-マージ結合により大量のDiskへのスピルが発生し、Photonエンジンが使用されないためPhotonでのタスク処理の比率も非常に低くなる症状:
- 低速なソートマージ結合が選択されている
- Photonエンジンでのタスク処理の割合が低い（理想はPhotonが大半を占めている状態）
- Diskスピルが大量に発生してしまっている（理想はスピルが0バイト）

### 2.3 最適な実行プラン生成のためのチューニングステップ

#### Analyze実行
各クエリで使用しているテーブルは小規模マスターテーブルを含めすべて最新の統計情報を取得してください。

```sql
ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS;
```

AQE (Adaptive Query Execution)によりクエリー実行時に収集される実行時統計情報に基づきクエリー計画の最適化が行われますが、Photonエンジンではテーブルの統計情報も同時に使用されます。

#### Spark configパラメータ変更

結合処理の実行プランを調整するための代表的なパラメータ:

##### spark.sql.autoBroadcastJoinThreshold
- **デフォルト:** 10MB
- **推奨:** 200MB

ブロードキャスト結合を実行するテーブルサイズの閾値。サイズが大きいとOOMエラーになりやすいので注意してください。DBSQLのクラスタサイズを大きくするとOOMエラーを解消できますが400MB以上では逆に遅くなるケースもあります。

##### spark.sql.join.preferSortMergeJoin
- **デフォルト:** true
- **推奨:** false

データセットが大きい場合のデフォルト結合戦略にソートマージを使用するかどうかを指定します。`spark.sql.adaptive.enabled=true`（デフォルト）においては高速なBROADCAST/SHUFFLE_HASHが選択されやすいのですが、SortMergeプランが解消されない場合にはfalseを指定することでプラン変更できる場合があります。

##### spark.databricks.adaptive.joinFallback
- **デフォルト:** false
- **推奨:** true

Databricks専用パラメータ。ブロードキャスト結合の閾値を超えた場合でもエラーとせずにSHUFFLE_HASHを選択するようにします。`spark.databricks.adaptive.joinFallback.threshold`で閾値の設定も可能ですが`spark.sql.autoBroadcastJoinThreshold`の値が優先されるようです。

### 2.4 健全なクエリプラン例：Broadcast/Shuffled-hash結合

BroadcastまたはShuffled-hash結合によりDiskスピルがなくなり、Photonでのタスク処理の比率が高い状態が望ましいです:
- 高速な結合プランが選択されている
- Photonエンジンでのタスク処理の割合が大半を占めている状態が望ましい
- 理想はDiskスピルが0バイト

結合プランの改善後もスピルが発生している場合はクラスタサイズ拡張(スケールアップ)を検討します。また、`spark.sql.shuffle.partitions`パラメータを増加させることでタスクが細分化され、それぞれのタスクで使用するメモリ量が削減されることでスピルが改善されますが通常は変更不要です。

---

## 2A. ハッシュテーブルリサイズの多発要因
<!-- section_id: hash_resize_causes -->

**ハッシュリサイズとは**: Photon がハッシュベース操作（JOIN の build side、GROUP BY、DISTINCT 等）で事前確保したハッシュテーブルが、入力行数の見積もり誤りによって容量不足に陥り、倍増・再構築された回数です。正常なクエリは通常 10 回未満、100 回で警告、1,000 回超は異常、数万回オーダーはデータ/構造の問題を示唆します（単なる「遅さ」ではなく、行数推定が体系的に外れている状態）。

**多発時の「ANALYZE TABLE 再実行」の罠**: 統計情報の陳腐化は多くの原因の 1 つに過ぎません。以下のいずれかに該当する場合、ANALYZE TABLE 再実行では改善しません:
- 既に ANALYZE TABLE を実行済み
- **予測最適化 (Predictive Optimization)** が有効で統計が自動維持されている
- EXPLAIN の `Optimizer Statistics` で全テーブルが `full` 状態

これらの状況で「テーブル統計の更新」を推奨するのはミスリードです。**代わりに以下の 8 要因を順に調査してください**:

#### 調査手順（推奨順）

1. **行数爆発** — フィルタ漏れや誤った JOIN 述語で想定以上に行が増えていないか
   - 結果件数を業務的な期待値と照合
   - `EXPLAIN` で各ノードの推定行数 vs 実行時の rows_output を比較
   - JOIN 後の行数が入力テーブル行数の和・積と比べて妥当か確認

2. **重複 GROUP BY / 集約の再計算** — 同じキーでの集約が CTE や UNION 分岐で複数回実行されていないか
   - `EXPLAIN` で `ReusedExchange` が使われているか確認
   - 同一 `GROUP BY <col>` が物理プランに複数回出現する場合、CTE 統合を検討
   - UNION ALL の各分岐で同種の集約がある場合、上流で 1 度集約

3. **キー値スキュー** — ヘビーヒッター（1 値に行が集中）がないか
   ```sql
   SELECT <key_col>, COUNT(*) AS n
   FROM <table>
   GROUP BY 1
   ORDER BY n DESC
   LIMIT 20;
   ```
   - Top 値が全体の 5% 以上を占める場合、スキュー対策（AQE skew join handling、salting、pre-aggregation）を検討

4. **NULL 集中** — JOIN/GROUP キーに NULL が大量にないか
   ```sql
   SELECT COUNT(*) - COUNT(<key_col>) AS null_count,
          COUNT(*)                    AS total
   FROM <table>;
   ```
   - NULL は単一ハッシュパーティションに集約されるため、スキューの特殊ケース
   - 上流で `WHERE <key_col> IS NOT NULL` フィルタを追加、または NULL 除外 JOIN

5. **JOIN キーの型不一致** — 左右の型が異なると暗黙 CAST でハッシュ衝突
   - 例: `decimal(10,0) ↔ bigint`, `string ↔ int`
   - DDL で型を揃えるのが理想、難しければ JOIN 前の projection でキャスト
   - 物理的には同値でも異なる型同士は別ハッシュに配分されうる

6. **DECIMAL 高精度キー / 不適切なデータ型** — DECIMAL(38,0) 等は BIGINT より重い
   - 実際に整数値のみなら `ALTER TABLE ... ALTER COLUMN <col> TYPE BIGINT` を検討
   - ハッシュ計算コスト、行メモリフットプリント、比較コストが削減される
   - JOIN の両側で同じ型変更を適用（片側だけだと型不一致で逆効果）
   - **大容量シャッフル/集約（> 10 GB）が検出された場合は、スキーマ取得の有無に関わらず常にデータ型の妥当性を確認してください**。DDL で以下を実行:
     ```sql
     DESCRIBE TABLE <fqn>;
     ```
     よくある無駄:
     - DECIMAL(38,0) で実値は INTEGER 範囲 → BIGINT へ（2-5 倍のコスト削減）
     - STRING で数値/日付を保持 → 適切な数値型 / DATE / TIMESTAMP へ
     - 過大な VARCHAR 定義（実際の最大長が小さい）
   - 大容量ワークロードほど per-row コストの差が積み上がり、総コストに大きく影響する

7. **UDF / 非決定的述語** — オプティマイザが行数推定不能となり、ハッシュテーブル初期容量が小さすぎる
   - UDF は行数の縮退・増大が予測不能
   - `rand()`, `current_timestamp()` 等の非決定関数を含む述語もプッシュダウン・推定に失敗
   - UDF を組み込み関数に置き換え、非決定関数は pre-computed 列に分離

8. **メモリ圧迫** — 他オペレータでメモリ枯渇、ハッシュテーブルが繰り返し再構築される
   - `spill_bytes` / `num_spills_to_disk` / OOM fallback を確認
   - クラスタサイズ拡張（スケールアップ）、または `spark.sql.shuffle.partitions` 増で 1 タスクあたりのメモリ使用量を下げる

#### 診断 SQL（要対象テーブルへの SELECT 権限）

ホット化したカラムに対するカーディナリティ・NULL・ヘビーヒッター確認:
```sql
-- カーディナリティ確認
SELECT
  COUNT(DISTINCT <key_col>) AS distinct_values,
  COUNT(*)                  AS total_rows,
  COUNT(*) - COUNT(<key_col>) AS null_count,
  ROUND(COUNT(*) / NULLIF(COUNT(DISTINCT <key_col>), 0), 1) AS avg_rows_per_value
FROM <fqn>;

-- Top 20 値（スキュー検出）
SELECT <key_col>, COUNT(*) AS n
FROM <fqn>
GROUP BY 1
ORDER BY n DESC
LIMIT 20;
```

#### 判定フロー

```
hash_table_resize_count が高い
  ├─ EXPLAIN EXTENDED の Optimizer Statistics は? ──┐
  │                                                  │
  │  full のみ（missing/partial 無し）               │
  │  ↓                                                │
  │  → ANALYZE TABLE は無効。上記 8 要因を調査       │
  │                                                  │
  │  missing/partial あり                            │
  │  ↓                                                │
  │  → まず ANALYZE TABLE、それでも改善しなければ 8 要因へ
  │                                                  │
  └─ EXPLAIN 未添付                                  │
     ↓                                                │
     → 予測最適化有効なら 8 要因が本命。            │
       そうでなければ ANALYZE から試して 8 要因へフォールバック
```

---

## 3. Shuffle最適化
<!-- section_id: shuffle -->

### 3.1 メモリ効率の基準

Shuffle操作のメモリ効率を判定する基準:

| 指標 | 基準値 | 説明 |
|------|--------|------|
| パーティションあたりメモリ | ≤512MB | これを超える場合は最適化が必要 |
| 高メモリ使用閾値 | 100GB | Liquid Clusteringの検討を推奨 |
| 長時間実行閾値 | 300秒 | データ分散戦略の見直しを推奨 |

### 3.2 Shuffle最適化の優先度判定

| メモリ/パーティション | 優先度 | 推奨アクション |
|---------------------|--------|---------------|
| >2GB | 高 | クラスタサイズ拡張またはパーティション数大幅増加 |
| 1GB-2GB | 高 | パーティション数増加、AQE設定調整 |
| 512MB-1GB | 中 | パーティション数調整を推奨 |
| ≤512MB | 低 | 効率的な状態 |

### 3.3 REPARTITIONヒントの活用

SQLクエリでShuffle操作が発生している場合は、以下のヒントを適切に設定してください:

```sql
-- 標準的なrepartition
SELECT /*+ REPARTITION(100, column1, column2) */ ...

-- Window関数使用時
SELECT /*+ REPARTITION_BY_RANGE(column1) */ ...
```

### 3.4 AQE (Adaptive Query Execution) 設定

AQEはクエリ実行時の統計情報に基づいて動的に最適化を行います。

**推奨Sparkパラメータ:**

```sql
-- パーティションサイズの目標値（512MB以下を推奨）
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 536870912;

-- パーティション結合の有効化
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;
SET spark.sql.adaptive.coalescePartitions.maxBatchSize = 100;

-- スキュー結合の有効化
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 536870912;
```

### 3.5 AQEShuffleReadメトリクスの確認

プロファイラで以下のメトリクスを確認:

| メトリクス | 説明 |
|-----------|------|
| AQEShuffleRead - Number of partitions | パーティション数 |
| AQEShuffleRead - Partition data size | パーティションデータサイズ |
| AQEShuffleRead - Number of skewed partitions | スキューしたパーティション数 |

**平均パーティションサイズの計算:**
```
平均パーティションサイズ = Partition data size / Number of partitions
```

512MBを超える場合はスキュー警告となります。

### 3.5 自動シャッフルパーティション（Pro/Classicのみ）

ProおよびClassic SQL Warehouse（Serverlessは対象外）では、`spark.sql.shuffle.partitions = auto`を設定することで、AQEが各シャッフルステージの実データサイズに基づいて最適なパーティション数を動的に決定します。データ量が大きく変動するクエリでは、固定値（デフォルト200）より適切な場合が多いです。

```sql
SET spark.sql.shuffle.partitions = auto;
```

**注意:** Serverless SQL Warehouseでは`auto`が内部的に設定済みのため、このパラメータの変更はできません。

---

## 3A. 支配的シャッフルキーを Liquid Clustering キー候補として検討する
<!-- section_id: lc_shuffle_key_candidate -->

プロファイラの Shuffle Details セクションに **GiB 級の書き込み量または memory-inefficient（>128 MB/partition）** なシャッフルが存在し、その `partitioning key` が当該テーブルのカラムである場合、そのカラムは **Liquid Clustering のキー候補**として評価してください。クラスタリングにより同値の行が同一ファイルに集約されるため、同じ GROUP BY/JOIN パターンを持つ繰り返しクエリのシャッフル量が削減されます（co-located aggregation 効果）。

**判定基準:**
1. プルーニング（WHERE フィルタ）に寄与する列があれば**そちらを優先**（LC の第一目的は I/O 削減）
2. シャッフルキーのカーディナリティが**極端に低い（<10 distinct 値）**場合は、単独キーとして割り当てない。`Hierarchical Clustering`（`delta.feature.hierarchicalClustering`）で低カーディナリティキーを低位に配置するか、別の高カーディナリティキーと組み合わせる
3. シャッフルが小規模（<1 GiB 書き込み、memory efficient）なら LC 対象としない — REPARTITION ヒントで十分

**例（採用可）:**
- `ce.lineitem_usagetype` が 146 GB 書き込み + 981 GB peak memory の GROUP BY シャッフルキー、カーディナリティ 20-50 → `CLUSTER BY (usage_date, lineitem_usagetype)` のように日付と組み合わせて採用

**例（採用不可）:**
- シャッフルキーが `synthetic_partition_id` のような式ベースの仮想列 → LC 対象にできない
- カーディナリティ 3 の `region_type` 単独 → Hierarchical Clustering の低位キーまたは他キーと組み合わせる

---

## 4. スピル（Disk Spill）の検出と対策
<!-- section_id: spill -->

### 4.1 スピル検出メトリクス

プロファイラで以下のメトリクスを確認:

| メトリクス | 説明 |
|-----------|------|
| Num bytes spilled to disk due to memory pressure | メモリ圧迫によるディスクスピル |
| Sink - Num bytes spilled to disk due to memory pressure | Sinkノードでのスピル |

### 4.2 スピル発生時の対策

1. **緊急対策（高優先度）**
   - クラスターサイズの拡張（ワーカーノード数増加）
   - 高メモリインスタンスタイプへの変更

2. **短期対策**
   - `spark.sql.adaptive.coalescePartitions.enabled = true`
   - `spark.sql.adaptive.skewJoin.enabled = true`
   - パーティション数の調整

3. **中長期対策**
   - パーティション数の明示的指定（`.repartition()`）
   - JOIN戦略の最適化（ブロードキャストJOINの活用）
   - Liquid Clusteringの実装
   - テーブル設計の最適化

### 4.3 スピル重要度の判定

| スピル量 | 重要度 | アクション |
|---------|--------|-----------|
| >5GB | 危機的 | メモリ構成とパーティション戦略の見直しが必須 |
| >1GB | 重要 | 最適化を強く推奨 |
| >0 | 要注意 | 監視と改善検討 |
| 0 | 理想 | 最適な状態 |

---

## 5. Photon利用率の改善
<!-- section_id: photon -->

### 5.1 Photon効率の確認

Photonエンジンで効率的に処理が実行されているかの確認方法:
- クエリヒストリからの確認
- Spark-UIでの視覚的確認（推奨）

**Photon効率の計算:**
```
Photon効率 = photon_total_time_ms / task_total_time_ms
```

### 5.2 Photon効率の判定基準

| 効率 | 評価 | 説明 |
|------|------|------|
| >80% | 高 | 良好な状態 |
| 50-80% | 中 | 改善の余地あり |
| <50% | 低 | 最適化が必要 |

### 5.3 Photon非対応処理の特定

プランDetailの情報からPhotonエンジンでサポートされていないSQL処理を特定し改善案を検討します。

> 確認日: 2026/02/15 / DBR 18.0

#### JOIN種別のPhoton対応状況

| JOIN種別 | Photon対応 |
|---------|-----------|
| ブロードキャスト結合 | 対応 |
| シャッフル-ハッシュ結合 | 対応 |
| ソート-マージ結合 | **非対応** |
| シャッフル-ネストループ結合 | 対応 |

#### Photon非対応SQL関数一覧（Unimplemented）

以下の関数はDBR 18.0時点でPhotonエンジンに未対応です。これらの関数がクエリ内で使用されている場合、Photonによる高速化が適用されません。

**集約関数（Aggregate）:**

| SQL関数 | 説明 |
|---------|------|
| `percentile_cont` | 連続パーセンタイル |
| `percentile_disc` | 離散パーセンタイル |
| `listagg`, `string_agg` | 文字列集約 |
| `bool_and`, `every` | 論理AND集約 |
| `bool_or`, `any`, `some` | 論理OR集約 |
| `collect_set` | 重複排除リスト集約（※ `collect_list`/`array_agg` はDBR 9.0で対応済み） |
| `count_if` | 条件付きカウント |
| `count_min_sketch` | カウントミンスケッチ |
| `covar_pop`, `covar_samp` | 共分散 |
| `try_avg` | try_average |
| `try_sum` | try_sum |
| `var_pop` | 母分散（※ `var_samp` はDBR 10.1で対応済み） |
| `regr_count`, `regr_r2`, `regr_sxx`, `regr_sxy`, `regr_syy` | 回帰分析関数 |
| `measure` | メジャー |

**配列関数（Array）:**

| SQL関数 | 説明 |
|---------|------|
| `aggregate`, `reduce` | 配列の畳み込み |
| `array_append` | 配列末尾に追加 |
| `array_prepend` | 配列先頭に追加 |
| `array_compact` | NULL除去 |
| `array_insert` | 配列に挿入 |
| `shuffle` | 配列シャッフル |
| `zip_with` | 2配列のzip処理 |

**文字列関数（String）:**

| SQL関数 | 説明 |
|---------|------|
| `left` | 左からN文字取得 |
| `right` | 右からN文字取得 |
| `search`, `isearch` | テキスト検索 |

**数値関数（Numeric）:**

| SQL関数 | 説明 |
|---------|------|
| `e` | オイラー数 |
| `positive` | 単項プラス |
| `try_divide` | 安全な除算 |
| `try_mod`, `try_remainder` | 安全な剰余 |
| `try_multiply` | 安全な乗算 |

**マップ関数（Map）:**

| SQL関数 | 説明 |
|---------|------|
| `map_contains_key` | キー存在チェック |
| `map_filter` | マップフィルタ |
| `map_zip_with` | マップのzip |
| `transform_keys` | キー変換 |
| `transform_values` | 値変換 |

**日時関数（Time）:**

| SQL関数 | 説明 |
|---------|------|
| `localtimestamp` | ローカルタイムスタンプ |
| `make_timestamp_ltz`, `try_make_timestamp_ltz` | LTZタイムスタンプ生成 |
| `make_timestamp_ntz`, `try_make_timestamp_ntz` | NTZタイムスタンプ生成 |
| `try_to_timestamp` | 安全なタイムスタンプ変換 |
| `to_time`, `try_to_time` | TIME型変換 |
| `try_to_date` | 安全な日付変換 |
| `current_time`, `make_time` | TIME型関連 |
| `session_window`, `window`, `window_time` | ウィンドウ関数 |

**演算子（Operators）:**

| SQL関数 | 説明 |
|---------|------|
| `between` | 範囲演算子 |
| `ilike` | 大文字小文字無視LIKE |

**CSV/XML/JSON関数:**

| SQL関数 | 説明 |
|---------|------|
| `from_csv`, `to_csv`, `schema_of_csv` | CSV変換 |
| `from_xml`, `to_xml`, `schema_of_xml` | XML変換 |
| `schema_of_json`, `schema_of_json_agg` | JSONスキーマ推論 |

**XPath関数:** `xpath`, `xpath_boolean`, `xpath_double`, `xpath_float`, `xpath_int`, `xpath_long`, `xpath_short`, `xpath_string` — すべて非対応

**AI関数:** `ai_gen`, `ai_query`, `ai_classify`, `ai_similarity`, `ai_summarize`, `ai_translate`, `ai_extract`, `ai_mask`, `ai_fix_grammar`, `ai_analyze_sentiment`, `ai_generate_text`, `ai_complete`, `ai_embed`, `ai_parse_document` — すべて非対応（CPUで実行）

**Geospatial（非対応のみ）:**

| SQL関数 | 説明 |
|---------|------|
| `h3_getpentagoncellids` | H3五角形セルID取得 |
| `h3_tessellateaswkb`, `h3_try_tessellateaswkb` | テッセレーション |
| `st_buffer`, `st_difference`, `st_distance` | 空間演算 |
| `st_envelope_agg`, `st_union_agg` | 空間集約 |
| `st_intersection`, `st_union` | 空間結合 |
| `st_simplify` | 空間簡略化 |

**その他（Misc）:**

| SQL関数 | 説明 |
|---------|------|
| `java_method`, `reflect`, `try_reflect` | Java呼び出し |
| `assert_true` | アサーション |
| `typeof` | 型情報取得 |
| `current_version` | バージョン取得 |
| `grouping`, `grouping_id` | GROUPING SETS関連 |
| `zstd_compress`, `zstd_decompress`, `try_zstd_decompress` | 圧縮・展開 |
| `to_avro`, `from_avro` | Avroシリアライズ |
| `to_protobuf`, `from_protobuf` | Protobufシリアライズ |
| `uniform` | 一様分布乱数 |

#### DBRバージョン別 Photon対応追加の主要関数

| DBR | 追加された主要関数 |
|-----|-------------------|
| 8.3 | 基本演算（`+`, `-`, `*`, `/`）、比較演算子、`cast`, `count`, `sum`, `avg`, `min`, `max`, `concat`, `substr`, `lower`, `upper`, `trim`, 日時関数（`date_add`, `date_diff`, `to_timestamp` 等） |
| 8.4 | `map`, `explode`, `sqrt`, `exp`, `cbrt`, `log2` |
| 9.0 | `collect_list`/`array_agg`, `width_bucket` |
| 9.1 | `regexp_replace`, `base64`, `unbase64`, `hex`, `unhex`, `posexplode`, 三角関数（`atan`, `atan2`, `tan`） |
| 10.0-10.1 | `array_distinct`, `array_except`, `array_intersect`, `array_union`, `var_samp`, `chr`, `levenshtein`, `soundex` |
| 10.4 | ウィンドウ関数（`row_number`, `rank`, `dense_rank`, `lead`, `lag`, `nth_value`, `ntile`）、`percentile`, `transform`, `filter`, `md5`, `sha1`, `sha2`, `aes_encrypt`/`aes_decrypt`, 三角関数（`sin`, `cos`, `asin`, `acos`） |
| 11.1-11.3 | `approx_count_distinct`, `from_json`, `to_json`, `get_json_object`, `map_keys`, `map_values`, `map_entries`, `map_from_entries`, `flatten`, `split_part`, `initcap`, 双曲線関数、`try_add`, `try_subtract` |
| 13.1-13.3 | `approx_percentile`, `format_string`, `to_number`, `parse_url`, `sort_array`, `array_sort`, `mask`, `luhn_check`, HLL関数 |
| 14.0-14.3 | `max_by`, `min_by`, `corr`, `skewness`, `kurtosis`, Geospatial（`st_*`系の大部分）、`parse_json`, `variant_get`, Bitmap関数 |
| 15.0-15.4 | `collate`, `collation`, `nullifzero`, `zeroifnull`, `spark_partition_id`, UTF-8検証関数, `convert_timezone`, Variant関連 |
| 16.0-16.4 | `elt`, `st_contains`, `st_covers`, `st_within`, `st_intersects`, `st_transform`, `try_parse_url`, `dayname` |
| 17.1-17.3 | `hll_union_agg`, `bitmap_and_agg`, `st_dump`, `st_dwithin`, `st_exteriorring`, `st_interiorringn`, `st_numinteriorrings`, `try_url_decode` |
| 18.0 | `randstr`, `approx_top_k`, Geospatial（`st_azimuth`, `st_boundary`, `st_closestpoint`, `st_geogfromewkt`, `st_geomfromewkt`, `st_isvalid`, `st_makeline`, `st_makepolygon`） |

#### Photon非対応関数の回避策

| # | 非対応関数/構文 | Photon対応の代替 | SQL書換え例 | 注意事項 |
|---|---------------|----------------|-----------|---------|
| 1 | `collect_set(col)` | `array_distinct(collect_list(col))` | `SELECT array_distinct(collect_list(col)) FROM t GROUP BY key` | `collect_list`はDBR 9.0で対応済み。結果の順序は保証されない |
| 2 | `percentile_cont(0.5)` / `percentile_disc(0.5)` | `percentile(col, 0.5)` または `approx_percentile(col, 0.5)` | `SELECT percentile(salary, 0.5) FROM employees` | `percentile`はDBR 10.4、`approx_percentile`はDBR 13.1で対応。approxは近似値だが高速 |
| 3 | `left(str, n)` | `substr(str, 1, n)` | `SELECT substr(name, 1, 3) FROM t` | `substr`はDBR 8.3で対応済み |
| 4 | `right(str, n)` | `substr(str, -n)` | `SELECT substr(name, -3) FROM t` | 同上 |
| 5 | `ilike` | `lower(col) LIKE lower(pattern)` | `WHERE lower(name) LIKE lower('%tokyo%')` | `lower`と`LIKE`はDBR 8.3で対応済み |
| 6 | `between` | `col >= low AND col <= high` | `WHERE price >= 100 AND price <= 500` | 比較演算子はDBR 8.3で対応済み |
| 7 | ソート-マージ結合 | Broadcast JOIN / Shuffle Hash JOIN | `SELECT /*+ BROADCAST(small_t) */ ... FROM large_t JOIN small_t` | セクション2.3のチューニングステップも参照 |
| 8 | `count_if(cond)` | `count(CASE WHEN cond THEN 1 END)` | `SELECT count(CASE WHEN status = 'active' THEN 1 END) FROM t` | `count`+`CASE`はDBR 8.3で対応済み |
| 9 | `bool_and(col)` / `every(col)` | `min(CAST(col AS INT)) = 1` | `SELECT min(CAST(is_valid AS INT)) = 1 FROM t GROUP BY key` | 論理ANDの代替。意味的に等価 |
| 10 | `bool_or(col)` / `any(col)` | `max(CAST(col AS INT)) = 1` | `SELECT max(CAST(is_active AS INT)) = 1 FROM t GROUP BY key` | 論理ORの代替 |
| 11 | `listagg(col, ',')` / `string_agg(col, ',')` | `array_join(collect_list(col), ',')` | `SELECT array_join(collect_list(name), ', ') FROM t GROUP BY dept` | `collect_list`(DBR 9.0)+`array_join`の組み合わせ。順序指定はORDER BY句で |
| 12 | `try_divide(a, b)` | `CASE WHEN b != 0 THEN a / b ELSE NULL END` | `SELECT CASE WHEN cnt != 0 THEN total / cnt ELSE NULL END FROM t` | ゼロ除算回避の明示的分岐 |
| 13 | `try_multiply(a, b)` | `a * b`（オーバーフロー許容時） | 直接乗算で代替可能。オーバーフロー検出が不要な場合 | オーバーフロー検出が必要な場合はアプリ層で対応 |
| 14 | `var_pop(col)` | `var_samp(col) * (count(col) - 1) / count(col)` | 母分散を標本分散から算出 | `var_samp`はDBR 10.1で対応済み。大標本では近似的に等価 |
| 15 | `from_csv(str, schema)` | CTEでの`split` + `CAST`組み合わせ | `WITH parsed AS (SELECT split(csv_col, ',') AS cols FROM t) SELECT CAST(cols[0] AS INT), cols[1] FROM parsed` | 固定スキーマの場合のみ有効。動的スキーマには不向き |
| 16 | `map_filter(map, func)` | `map_from_entries(filter(map_entries(map), e -> cond))` | `SELECT map_from_entries(filter(map_entries(m), x -> x.value > 0)) FROM t` | `filter`はDBR 10.4、`map_entries`/`map_from_entries`はDBR 11.1で対応済み |
| 17 | 複雑なWindow Frame | ROWSフレームへの書き換え | `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` に変更 | RANGEフレーム+非標準境界がPhoton非対応の場合あり |
| 18 | Python/Scala UDF | 組み込みSQL関数での書き換え | UDF内のロジックをSQL式に分解 | UDFは常にJVM/Python実行。最も効果の大きい回避策 |

### 5.4 Photon最適化の代表ユースケース

以下は実際のクエリパターンで頻出するPhoton最適化の具体例です。

#### ケース1: SortMergeJoin → Broadcast/ShuffleHash化

**症状:**
- Photon利用率が低い（<50%）
- 大量のDiskスピル発生
- プロファイルで`SortMergeJoin`が表示される

**EXPLAIN上の兆候:**
```
== Photon Explanation ==
Query is not fully supported by Photon.
Unsupported node: SortMergeJoin
Reference node: SortMergeJoin [...]
```

**対策SQL:**
```sql
-- Before: SortMergeJoinが選択される
SELECT o.*, c.name
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- After: Broadcastヒントで強制
SELECT /*+ BROADCAST(c) */ o.*, c.name
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- または、Sparkパラメータで全体的にSortMerge回避
SET spark.sql.join.preferSortMergeJoin = false;
SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB
```

**期待効果:**
- Photon利用率が80%以上に改善
- Diskスピルが0に近づく
- 実行時間が30-70%短縮される場合が多い

#### ケース2: PIVOT/非対応集約関数の回避

**症状:**
- Photon利用率が中程度（50-80%）
- 特定の集約処理のみJVM実行にフォールバック

**EXPLAIN上の兆候:**
```
== Photon Explanation ==
Query is not fully supported by Photon.
Unsupported aggregation function: pivotfirst(...)
Reference node: HashAggregate [...]
```

**対策SQL:**
```sql
-- Before: PIVOT + collect_set
SELECT *
FROM (SELECT dept, status, employee_id FROM employees)
PIVOT (collect_set(employee_id) FOR status IN ('active', 'inactive'));

-- After: 条件付き集約 + array_distinct + collect_list
SELECT
  dept,
  array_distinct(collect_list(CASE WHEN status = 'active' THEN employee_id END))
    AS active_employees,
  array_distinct(collect_list(CASE WHEN status = 'inactive' THEN employee_id END))
    AS inactive_employees
FROM employees
GROUP BY dept;
```

**期待効果:**
- PIVOTを条件付き集約に分解することでPhoton処理が可能に
- `collect_set` → `array_distinct(collect_list(...))` でPhoton対応

#### ケース3: 複雑なWindow Frameの簡素化

**症状:**
- 特定のWindow関数でPhotonフォールバック
- RANGEフレームや複雑な境界指定が原因

**EXPLAIN上の兆候:**
```
== Photon Explanation ==
Query is not fully supported by Photon.
Window frame not supported: RANGE BETWEEN ...
```

**対策SQL:**
```sql
-- Before: RANGEフレーム（Photon非対応の場合あり）
SELECT
  id,
  SUM(amount) OVER (
    ORDER BY event_date
    RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
  ) AS rolling_7d
FROM events;

-- After: ROWSフレーム + 日付フィルタで近似
SELECT
  e.id,
  (SELECT SUM(e2.amount)
   FROM events e2
   WHERE e2.event_date BETWEEN DATE_SUB(e.event_date, 7) AND e.event_date
  ) AS rolling_7d
FROM events e;

-- または、事前にパーティション化して単純なROWSフレームに変換
SELECT
  id,
  SUM(amount) OVER (
    PARTITION BY date_trunc('month', event_date)
    ORDER BY event_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_monthly
FROM events;
```

**期待効果:**
- Window処理がPhotonで実行可能に
- 複雑なRANGEフレームをROWSに変換することで処理効率も向上

#### ケース4: UDF除去による組み込み関数への置換

**症状:**
- Photon利用率が非常に低い（<30%）
- Python UDFやScala UDFが多用されている
- タスク処理時間の大半がJVM/Pythonで消費

**対策:**
```sql
-- Before: Python UDFでの文字列処理
-- @udf で定義された parse_address(addr) を使用
SELECT parse_address(address) AS parsed FROM customers;

-- After: 組み込み関数で分解
SELECT
  regexp_extract(address, '^(\\d+)', 1) AS house_number,
  regexp_extract(address, '\\d+\\s+(.*),', 1) AS street,
  regexp_extract(address, ',\\s*([^,]+)$', 1) AS city
FROM customers;
```

**期待効果:**
- UDFの完全排除でPhoton利用率が大幅に改善（30%→90%以上も可能）
- Python/JVMのシリアライゼーションオーバーヘッドが消失
- 最も劇的な改善効果が得られるパターン

---

## 6. キャッシュ効率
<!-- section_id: cache -->

### 6.1 Deltaキャッシュヒット率

プロファイラで以下のメトリクスを確認:

| メトリクス | 説明 |
|-----------|------|
| read_bytes | 総読み込みバイト数 |
| read_cache_bytes | キャッシュから読み込んだバイト数 |
| read_remote_bytes | リモートから読み込んだバイト数 |

**キャッシュヒット率の計算:**
```
キャッシュヒット率 = read_cache_bytes / read_bytes
```

### 6.2 キャッシュ効率の判定基準

| ヒット率 | 評価 | アクション |
|---------|------|-----------|
| >80% | 高 | 良好な状態 |
| 50-80% | 中 | スケールアップを検討 |
| <50% | 低 | キャッシュ戦略の見直しが必要 |

**ESチケット頻出パターン（38件）:** キャッシュは反復参照・同一ワーキングセットでは有効だが、一回限りの大規模走査では効果が薄い。Serverlessではin-memoryキャッシュのみ（Deltaキャッシュ非対応）。キャッシュ不足を疑う前に、まずスキャン量とファイル配置（OPTIMIZEの実施状況）を正すこと。

### 6.3 ローカルスキャン率（Scan Locality）の確認

キャッシュヒット率が良好にもかかわらずクエリが遅い場合、**ローカルスキャン率**の悪化が原因である可能性があります。Verboseモードで表示される以下のメトリクスを確認してください。

**確認するメトリクス:**

| メトリクス | 説明 |
|-----------|------|
| Number of local scan tasks | Executorが最初に割り当てられた場所でローカルにデータを読めたスキャンタスク数 |
| Number of non-local (rescheduled) scan tasks | 最初に割り当てられたExecutorではデータがローカルになく、別Executorに再スケジューリングされたスキャンタスク数 |
| Cache hits size | ノードレベルでキャッシュから読み取れたデータ量 |
| Cache misses size | ノードレベルでキャッシュミスしたデータ量 |
| Cloud storage request count | ノードレベルのクラウドストレージリクエスト数 |

**再スケジュール率の計算:**
```
再スケジュール率 = non-local scan tasks / (local scan tasks + non-local scan tasks)
```

**判定基準:**

| 再スケジュール率 | 評価 | アクション |
|-----------------|------|-----------|
| 0-1% | 良好 | 最適な状態 |
| 1-5% | 要観察 | 監視を継続 |
| >5% | 要対応 | 原因の特定が必要（下記参照） |

#### 6.3.1 ノード別分析の重要性

**全体の再スケジュール率だけでなく、Scanノード別の内訳を必ず確認してください。**

典型的なパターン:
```
テーブルA: local=10, non-local=0  (0%)   ← 正常
テーブルB: local=15, non-local=26 (63%)  ← 問題あり
テーブルC: local=4,  non-local=0  (0%)   ← 正常
全体: rescheduled=41.9%  ← テーブルBに引きずられている
```

この場合、問題はクラスタ全体ではなく**テーブルBのScan固有**です。

#### 6.3.2 原因の判定フロー

Non-local scanの原因は複数あり、**ノード別のキャッシュヒット率との相関**で判別します:

| パターン | non-local率 | ノードキャッシュヒット率 | 推定原因 |
|---------|------------|----------------------|---------|
| **コールドノード配置** | 高い | **極めて低い（<20%）** | スケールアウトで追加された新ノードにキャッシュがない |
| **CPU競合による再配置** | 高い | 中〜低 | 並行クエリによるCPUスロット不足で別ノードに再スケジュール |
| **ファイル配置の問題** | 高い | 中〜高 | キャッシュはあるがファイルレイアウトが分散 |
| **Dynamic Scan再編成** | 高い | 低い | Dynamic Scan Coalescingでtask再配分後にlocality喪失 |

**重要な判定ルール:**
- non-localが高い + キャッシュヒットが極めて低い → **コールドノード（スケールアウト/コールドスタート）が最有力**
- non-localが高い + 他テーブルは正常 + 該当テーブルだけCloud storageリクエスト大量 → スケールアウト直後の証拠
- non-localが高い + 全テーブルで均等に悪い → Executor数過多の可能性

#### 6.3.3 Serverless SQL Warehouse での典型パターン

Serverless SQL Warehouseでは以下の理由でScan Localityが悪化しやすいです:

1. **頻繁なスケールアウト**: 負荷に応じて自動的にクラスタが追加される。新規クラスタにはキャッシュがないため、割り振られたクエリはクラウドストレージから全量フェッチが必要
2. **多重実行によるCPU競合**: 同時実行クエリ数が多いと、preferred locationのノードのCPUスロットが埋まっており、キャッシュを持たない別ノードにrescheduledされる
3. **コールドスタート**: スケールダウン後のスケールアップでは、以前のキャッシュが失われている

**証拠の読み方:**
```
[問題のあるScanノード]
Number of non-local (rescheduled) scan tasks: 26  ← preferred locationに割り当てられなかった
Cache hits size: 1.3 GB                            ← キャッシュがほぼ空
Cache misses size: 7.3 GB                          ← 大量のキャッシュミス
Cloud storage request count: 1081                   ← クラウドストレージから全量フェッチ

[正常なScanノード]
Number of non-local (rescheduled) scan tasks: 0    ← preferred locationで実行
Cache hits size: 623 MB                            ← キャッシュから読み取り
Cache misses size: 0                               ← キャッシュミスなし
Cloud storage request count: 3                      ← 最小限のリクエスト
```

この対比が「コールドノードに割り振られた」ことの決定的証拠です。

#### 6.3.4 推奨アクション

原因に応じて対策が異なります:

**コールドノード配置（スケールアウト起因）の場合:**
- Serverless WHのウォームアップ戦略の検討（事前にダミークエリで主要テーブルをキャッシュに載せる）
- 同時実行のピークを分散させ、急激なスケールアウトを抑制
- Warehouse Event Logでスケールイベントの頻度を確認
- 可能であれば、Pro Warehouseで最小クラスタ数を1以上に設定

**CPU競合（多重実行起因）の場合:**
- 同時実行クエリのスケジューリングを見直し
- 重いクエリの実行時間帯を分散
- Warehouseのスケールアウト閾値の調整

**ファイルレイアウト起因の場合:**
- **OPTIMIZEの実行**: 小規模ファイルが多いとタスクが細分化され、localityミスが増えやすくなります
- **Liquid Clusteringの適用**: ファイルレイアウトを最適化し、locality効率を改善します

**注意事項:**
- 「Executor数を減らす」「クラスタサイズを縮小する」は、コールドノード配置やCPU競合が原因の場合は効果がありません
- ノード別のcache hit率とnon-local率の相関を確認してから対策を決定してください
- 同時実行が多い環境やオートスケール環境では、単発実行時と多重実行時の両方でプロファイルを取得して比較することを推奨します

---

## 7. クラウドストレージ制限の拡張
<!-- section_id: cloud_storage -->

### 7.1 クラウドリソース(Storage IO)ボトルネックの調査

I/O最適化/結合プランの改善を実施しても期待するパフォーマンスが得られない場合、クラウドストレージへのアクセスがボトルネックとなっているかどうかを冗長化モードのプロファイラのメトリクスから確認してください。

**確認するメトリクス:**

| メトリクス | 説明 |
|-----------|------|
| Cloud storage request count | ストレージリクエスト数 |
| Cloud storage request duration | リクエスト時間 |
| Cloud storage retry count | リトライ数 |
| Cloud storage retry duration | リトライ時間 |

**判定:**
- リトライが非常に高い値を示している場合はクラウドストレージへのアクセスがボトルネックになっている可能性が高い
- 理想はリトライが0の状態

---

## 7A. コンパイル / ファイルプルーニングのオーバーヘッド
<!-- section_id: compilation_overhead -->

### 検出基準

Databricks Query Profile の UI で「Optimizing query & pruning files」として表示されるフェーズは、ドライバ側で行われる以下の作業をまとめた指標:

- SQL パース / Catalyst 論理最適化
- Delta ログ replay（アクティブファイル一覧の構築）
- ファイル単位の統計 (min/max) によるデータスキッピング
- パーティション/Liquid Clustering による静的プルーニング
- Photon コード生成 / 物理プラン確定

このフェーズが実行時間の大部分を占めるのは、データ処理ではなく**メタデータ処理**がボトルネックになっていることを示す。

### 典型的な兆候

- `compilationTimeMs / totalTimeMs >= 30%`
- `prunedFilesCount` が数万件に達している（大量のファイル統計を driver が評価した）
- `metadataTimeMs` が秒オーダー
- `readFilesCount` が小さい（実読み込みは少ないのにプルーニング対象は大量）

### 原因と対策

| 原因 | 対策 |
|------|------|
| 小ファイル過多 | `OPTIMIZE <table>` で compact。ZORDER / Liquid Clustering と併用推奨 |
| Delta ログ肥大 | `VACUUM <table> RETAIN 168 HOURS` で古いコミット削除。Auto Checkpoint 有効化 |
| 過度のパーティション | パーティション列を見直し、Liquid Clustering 移行を検討 |
| Warehouse コールドキャッシュ | 同一クエリを 2 回目実行してコンパイル時間が短くなるか確認 |
| 統計不鮮度 | `ANALYZE TABLE <table> COMPUTE STATISTICS FOR ALL COLUMNS` |

### 推奨 SQL

```sql
-- 小ファイル compact
OPTIMIZE catalog.schema.table;

-- Delta ログ短縮（retention は要確認）
VACUUM catalog.schema.table RETAIN 168 HOURS;

-- 自動最適化を有効化
ALTER TABLE catalog.schema.table SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
```

Predictive Optimization が Unity Catalog 管理テーブルで有効になっていれば上記の多くは自動実行される。

---

## 7B. ドライバー待ち時間（Queue / Scheduling / Waiting for compute）
<!-- section_id: driver_overhead -->

### 指標の意味

Databricks Query Profile UI の「Scheduling」「Waiting for compute」「Waiting in queue」の 3 バーを合計した、**実行前のドライバ側待機時間**。クエリ処理ではなく、コンピュートリソースへの割り当て待ちが実体。

| UI バー | 実体 | 典型原因 |
|---|---|---|
| Waiting in queue | `queuedProvisioningTimeMs` + `queuedOverloadTimeMs` | Serverless コールドスタート / 過剰同時実行 |
| Waiting for compute | ウェアハウスがタスクディスパッチ可能になるまで | ウェアハウスのウォームアップ、スケールアップ中 |
| Scheduling | タスクスケジューリングオーバーヘッド | 同時実行クエリによるドライバ競合 |

JSON 内に直接の値が無い場合は以下で導出:

```
queue_ms   = queuedProvisioningTimeMs + queuedOverloadTimeMs
pre_compile_gap = queryCompilationStartTimestamp
                - (overloadingQueueStartTimestamp
                   or provisioningQueueStartTimestamp
                   or queryStartTimeMs)
sched_compute_ms = max(0, pre_compile_gap - queue_ms)
driver_overhead_ms = queue_ms + sched_compute_ms
```

### 検出基準

- Queue 単独: ≥ 5s または全体の 10% 以上
- Scheduling + compute wait: ≥ 3s または全体の 15% 以上
- 合算: ≥ 5s かつ 全体の 10% 以上

Severity HIGH: 合算 ≥ 30s もしくは ≥ 30%

### 対策の使い分け

| 支配要素 | 典型的な打ち手 |
|---|---|
| Provisioning queue | Serverless warm pool を有効化、auto-stop のアイドルタイムアウト延長 |
| Overload queue | Warehouse の max clusters を引き上げ、または同時実行クエリを間引く |
| Scheduling | 同時クエリをウェアハウス間で分散、ピーク時間帯の実行分散 |
| Waiting for compute | ウェアハウスサイズを上げるか、常時起動・ウォームプール使用 |

ランタイムで対処できるものではなく、**ウェアハウス設定やキュー管理の運用問題**として扱う。

---

## 7C. クラスタ低稼働（Cluster Underutilization）
<!-- section_id: cluster_underutilization -->

### 指標の意味

`task_total_time_ms / execution_time_ms` で求まる **effective parallelism** がクエリ実行中の平均並列度。Medium ウェアハウス（32-64 core 相当）で 20x を下回ると明確な低稼働。

### 発火条件

- `execution_time_ms >= 60_000`（短いクエリは除外）
- `effective_parallelism < 20`
- キュー待ちはゼロ（キュー案件は `driver_overhead` セクションで扱う）

### 3 つのバリアント

| バリアント | 判定シグナル | 対策の方向性 |
|---|---|---|
| **external_contention** | `rescheduled_scan_ratio >= 10%` | ワークロード分離、同時実行分散、max clusters 引き上げ |
| **driver_overhead** | `aqe_replan_count >= 5` OR `subquery_count >= 3` OR (`broadcast_hash_join_count >= 5` AND exec >= 120s) | SQL 構造単純化、broadcast 削減、driver サイズアップ |
| **serial_plan** | 上記いずれにも該当しない | REPARTITION で並列度増、事前集約、JOIN 戦略見直し |

### 判定フロー

```
1. rescheduled_scan >= 10% ?
   YES → external_contention（別クエリとの CPU 競合）
   NO  → 次へ
2. AQE re-plan >= 5 OR subquery >= 3 OR BHJ 多い ?
   YES → driver_overhead（Driver 過負荷）
   NO  → 次へ
3. → serial_plan（プラン直列度）
```

### 重要な判断指針

- **external_contention** は SQL 書き換えでは治らない。運用側のリソース管理。
- **driver_overhead** は SQL 構造で大きく改善可能。特に多段 subquery や多重参照 CTE を畳む。
- **serial_plan** は REPARTITION ヒント（`/*+ REPARTITION(32, col) */`）で大きく改善することが多い。

---

## 7D. コンパイル時間の絶対値警告（Advisory）
<!-- section_id: compilation_absolute -->

### 指標の意味

`compilation_time_ms >= 5s` だが **比率は小さい**（例: 5 分クエリで 10 秒 compile = 3%）ケース。7A の `compilation_overhead` は ratio 30% でゲートされているため、絶対値異常を別途 INFO で警告。

### 発火条件

- `compilation_time_ms >= 5000`（絶対値）
- `pruned_files_count >= 1000` OR `metadata_time_ms >= 500`（evidence）
- `compilation_overhead` カードが未発火（重複回避）

Severity: **INFO 固定**。ユーザー即アクションは求めないが、運用として OPTIMIZE/VACUUM スケジュールを検討する材料になる。

### 対策

7A と同じ（OPTIMIZE / VACUUM / Predictive Optimization）。ただし action priority は低く、複数クエリで常時出ているテーブルがあれば運用改善の契機とする、という位置付け。

---

## 8. クラスタサイズの調整
<!-- section_id: cluster -->

**注意:** クラスタサイズが大きすぎることでパフォーマンスが低下するケースもあります。キャッシュヒット率が良好にもかかわらず遅い場合は、[6.3 ローカルスキャン率（Scan Locality）の確認](#63-ローカルスキャン率scan-localityの確認)を参照し、再スケジュール率を確認してください。スケールアップだけでなくスケールダウンが有効な場合があります。

### 8.1 スケールアウト

クエリの同時実行数が多く、多数のキュー待ちが発生している場合に適しています。

**推奨ケース:**
- SQL単体での実行ではCPU/メモリ/ディスクスピルなどのリソースに起因する問題が発生していない
- 同時実行クエリ数の増加に伴いパフォーマンス劣化が発生している場合

**注意点:**
- DBSQLの各クラスタが同時に実行可能なSQL数は10（固定値のため変更不可）
- キュー待ちしているクエリ数の情報からオートスケール設定での最大クラスタ数を決定

### 8.2 スケールアップ

パフォーマンス劣化の原因が高負荷クエリに起因する場合に適しています。

**推奨ケース:**
- SQL単体での実行においてCPU/メモリ/ディスクスピルなどのリソースに起因する問題が発生している場合
- Deltaキャッシュのヒット率が低い場合

**効果:**
- スケールアップによりSQLエンドポイントを構成するクラスタ数を削減することでDeltaキャッシュを有効利用することが可能

---

## 9. ボトルネック指標サマリー
<!-- section_id: bottleneck_summary -->

### 9.1 主要なボトルネック指標

| 指標 | 閾値 | 説明 |
|------|------|------|
| キャッシュヒット率 | <30% | 低キャッシュ効率 |
| リモート読み込み率 | >80% | 高リモート読み込み |
| Photon効率 | <50% | 低Photon効率 |
| スピル発生 | >0 | メモリスピル発生 |
| フィルタ効率 | <20% | 低フィルタ効率 |
| シャッフル影響率 | ≥40% | 重大なシャッフルボトルネック |
| シャッフル影響率 | 20-40% | 中程度のシャッフルボトルネック |
| 再スケジュール率 | >5% | Scan locality低下（コールドノード配置/CPU競合/ファイル配置の問題） |

### 9.2 時間の計算について

プロファイラのメトリクスでは複数の時間指標があります:

| メトリクス | 説明 | 用途 |
|-----------|------|------|
| total_time_ms | クエリ全体の実行時間 | 基本的な実行時間 |
| execution_time_ms | 実行時間 | コンパイル時間を除く |
| task_total_time_ms | 全タスクの累積実行時間 | 並列実行の評価 |
| compilation_time_ms | コンパイル時間 | クエリ最適化時間 |

**重要:** 並列実行されるノードの時間を単純合計すると100%を超える場合があります。ボトルネック分析には`task_total_time_ms`を基準として使用してください。

---

## 10. 推奨Sparkパラメータまとめ
<!-- section_id: spark_params -->

### 10.1 Classic / Pro SQL Warehouse

```sql
-- 結合最適化
SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB
SET spark.sql.join.preferSortMergeJoin = false;
SET spark.databricks.adaptive.joinFallback = true;

-- AQE設定
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.coalescePartitions.minPartitionNum = 1;
SET spark.sql.adaptive.coalescePartitions.maxBatchSize = 100;
SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 536870912;  -- 512MB
SET spark.sql.adaptive.skewJoin.enabled = true;
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 536870912;  -- 512MB
```

### 10.2 Serverless SQL Warehouse
<!-- section_id: serverless_optimization -->

Serverless SQL Warehouseでは以下の6つのSpark設定のみSETが可能です：

| 設定 | デフォルト | 説明 |
|------|-----------|------|
| `spark.sql.shuffle.partitions` | 200 | シャッフルパーティション数 |
| `spark.sql.ansi.enabled` | false | ANSI SQLモード |
| `spark.sql.session.timeZone` | UTC | セッションタイムゾーン |
| `spark.sql.legacy.timeParserPolicy` | EXCEPTION | 時刻パーサーの動作 |
| `spark.sql.files.maxPartitionBytes` | 128MB | ファイルスキャンの最大パーティションバイト |
| `spark.databricks.execution.timeout` | 0 | クエリ実行タイムアウト |

**その他の最適化は、設定変更ではなくクエリ書き換えで対応してください。**

| Classic/Pro設定 | サーバーレス代替（クエリ書き換え） |
|----------------|--------------------------------|
| `autoBroadcastJoinThreshold` | `/*+ BROADCAST(table) */` ヒント、またはCTEで事前集約してテーブルを小さくする |
| `preferSortMergeJoin = false` | `/*+ SHUFFLE_HASH(table) */` ヒント |
| `adaptive.joinFallback` | `/*+ SHUFFLE_HASH(table) */` ヒント |
| `adaptive.skewJoin.enabled` | CTEで事前集約しJOIN前にデータ量を削減、または `/*+ REPARTITION(N) */` ヒント |
| `adaptive.coalescePartitions.*` | `/*+ COALESCE(N) */` ヒント |
| `adaptive.advisoryPartitionSizeInBytes` | `/*+ REPARTITION(N) */` ヒント |

具体的なbefore/after例は付録の**クエリ書き換えパターン集**を参照してください。

---

## 付録: クエリ最適化ヒント
<!-- section_id: appendix -->

### JOINヒント

```sql
-- ブロードキャスト結合を強制
SELECT /*+ BROADCAST(small_table) */ ...

-- シャッフル結合を強制
SELECT /*+ SHUFFLE_HASH(table) */ ...

-- マージ結合を強制
SELECT /*+ MERGE(table) */ ...
```

### データ分散ヒント

```sql
-- パーティション数指定
SELECT /*+ REPARTITION(200) */ ...

-- カラム指定でrepartition
SELECT /*+ REPARTITION(200, col1, col2) */ ...

-- 範囲パーティション（Window関数向け）
SELECT /*+ REPARTITION_BY_RANGE(col1) */ ...

-- パーティション統合
SELECT /*+ COALESCE(10) */ ...
```

### サーバーレス向けクエリ書き換えパターン集
<!-- section_id: query_rewrite_patterns -->

#### パターン1: CTEで事前集約してからJOIN

JOIN前にデータ量を削減し、シャッフルを最小化する。

```sql
-- Before: 大きなテーブルを直接JOIN
SELECT o.*, c.name
FROM orders o JOIN customers c ON o.customer_id = c.id;

-- After: CTEで事前集約してBROADCAST JOIN
WITH order_summary AS (
  SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total
  FROM orders
  GROUP BY customer_id
)
SELECT /*+ BROADCAST(order_summary) */ os.*, c.name
FROM order_summary os JOIN customers c ON os.customer_id = c.id;
```

#### パターン2: CTEでフィルタ前処理

WHERE句を早期に適用してシャッフルデータを削減する。

```sql
-- Before: JOIN後にフィルタ
SELECT o.*, p.name FROM orders o
JOIN products p ON o.product_id = p.id
WHERE o.order_date >= '2024-01-01';

-- After: CTEでフィルタしてからJOIN
WITH recent_orders AS (
  SELECT * FROM orders WHERE order_date >= '2024-01-01'
)
SELECT ro.*, p.name
FROM recent_orders ro JOIN products p ON ro.product_id = p.id;
```

#### パターン3: CTE事前集約（データスキュー対策）

JOIN前にデータ量を削減してスキューの影響を軽減する。AQE/AOSがスキューを自動処理するため、データ量の削減に注力する。

```sql
-- Before: 偏ったキーでのJOIN
SELECT * FROM fact_table f
JOIN dim_table d ON f.popular_key = d.key;

-- After: CTEで事前集約してデータ量を削減
WITH pre_agg AS (
  SELECT popular_key, COUNT(*) AS cnt, SUM(amount) AS total
  FROM fact_table
  GROUP BY popular_key
)
SELECT pre_agg.*, d.*
FROM pre_agg JOIN dim_table d ON pre_agg.popular_key = d.key;
```

#### パターン4: EXISTS / IN変換

相関サブクエリをセミジョインに変換する。

```sql
-- Before: 相関サブクエリ
SELECT * FROM orders o
WHERE o.customer_id IN (SELECT id FROM customers WHERE region = 'US');

-- After: EXISTSパターン
SELECT * FROM orders o
WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.region = 'US');
```

#### パターン5: UNION ALL化

重複除去が不要な場合、不要なソート/DISTINCTを回避する。

```sql
-- Before: UNIONはソート+重複除去を実行
SELECT id, name FROM table_a
UNION
SELECT id, name FROM table_b;

-- After: UNION ALLで不要なソートを回避
SELECT id, name FROM table_a
UNION ALL
SELECT id, name FROM table_b;
```

#### パターン6: カラムプルーニング

必要なカラムのみ選択してスキャン・メモリ使用量を削減する。

```sql
-- Before: SELECT * は全カラムを読み込む
SELECT * FROM large_table WHERE partition_col = 'value';

-- After: 必要なカラムのみ選択
SELECT col1, col2, col3 FROM large_table WHERE partition_col = 'value';
```

#### パターン7: Photon互換書き換え

Photon非対応の関数を互換な形式に書き換える。

```sql
-- Before: Window RANGE フレーム（Photon非対応の場合あり）
SUM(amount) OVER (ORDER BY event_date RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW)

-- After: ROWSフレーム + 日付フィルタ（Photon対応）
SUM(amount) OVER (ORDER BY event_date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)

-- Before: Python UDF
SELECT my_udf(col) FROM table;

-- After: 組み込み関数で代替
SELECT UPPER(col) FROM table;  -- 組み込み関数を使用
```

---

## 19. Lakehouse Federation クエリのチューニング
<!-- section_id: federation -->

Lakehouse Federation で外部データソース (BigQuery / Snowflake / Postgres / MySQL / Redshift 等) を UC 経由でクエリする場合、実行コストは **外部エンジン側** と **ネットワーク転送** が支配します。Databricks 側のストレージ最適化 (Liquid Clustering, ディスクキャッシュ, Photon 適合) は効きません。代わりに次の順で確認・対処します。

### 19.1 Pushdown 状況の確認 (必須)

```sql
EXPLAIN FORMATTED
SELECT ... FROM <federated_catalog>.<schema>.<table>
WHERE ...;
```

- `EXTERNAL ENGINE QUERY` ブロック: 外部エンジンに投げた SQL の実体
- `PushedFilters`: 外部側で評価された述語
- `PushedJoins`: JOIN が pushdown されたか (DBR 17.3+)
- 述語が push されていないなら、unsupported 関数 (例: MySQL 向けの `ILIKE`) / 左辺関数化 / ANSI モード依存が原因

BigQuery は Storage API で直接読むので `EXTERNAL ENGINE QUERY` が出ないのが正常。ただし **JOIN pushdown には materialization が必要**。

### 19.2 述語の書き換えで pushdown を通す

```sql
-- Bad: BigQuery / partition 列に関数を適用 → partition pruning が効かない
WHERE DATE(created_at) = '2026-04-20'

-- Good: 範囲条件にして pruning を通す
WHERE created_at >= TIMESTAMP('2026-04-20')
  AND created_at <  TIMESTAMP('2026-04-21')
```

- 左辺に `DATE()`, `CAST()`, `UPPER()`, `SUBSTRING()` 等を適用するのは典型的な pushdown 阻害
- 非 sargable な `LIKE '%...%'` は外部でも push しない

### 19.3 JDBC コネクタ: fetchSize / 並列読み

大きな結果セットを取る場合:

```sql
SELECT * FROM cat.schema.tbl WITH ('fetchSize' 100000) WHERE ...;
```

- デフォルトは「一括 fetch」で OOM しやすい
- MySQL / Postgres / SQL Server / Oracle / Redshift / Synapse / Teradata に適用可

並列読み (numeric でインデックス付きの列が必要):

```sql
SELECT * FROM cat.schema.tbl
  WITH ('numPartitions' 8,
        'partitionColumn' 'id',
        'lowerBound' '1',
        'upperBound' '10000000')
WHERE ...;
```

- federated view では動かないので source 側で view を作る

### 19.4 Snowflake: partition_size_in_mb

```sql
SELECT * FROM cat.schema.tbl WITH ('partition_size_in_mb' 1000) WHERE ...;
```

- デフォルトだとパーティション過多で遅くなる
- データ量に応じて 500MB〜2GB が目安

### 19.5 BigQuery: 課金と materialization

- **課金**: federation は DBSQL の compute + BQ の on-demand (スキャン TB あたり) の二重課金
- Partition pruning を必ず通すこと。BQ 側テーブルが `_PARTITIONDATE` や `DATE(ts)` partition なら、上記 19.2 の書き換え必須
- JOIN pushdown は「materialization モード」でのみ動作 — 大規模 JOIN は有効化、小クエリは overhead 増

### 19.6 OLTP source (MySQL / Postgres) 固有

- Index に乗らない predicate は full scan → primary DB に負荷
- read replica に federation 接続を向ける
- connection pool 上限に注意 (短命接続が federation で多発)

### 19.7 LIMIT pushdown の抑止

UC の列マスキング / 行レベルフィルタが適用されているテーブルでは、**正確性維持のため LIMIT は pushdown されない**。「LIMIT 10 なのに遅い」と感じる場合はこれが原因の可能性。

### 19.8 そもそも federation を毎回叩くべきか

頻繁に同じクエリを実行するダッシュボード / ジョブなら:

```sql
-- 1 回だけ federation を叩いて Delta に materialize
CREATE OR REPLACE TABLE main.analytics.sales_daily AS
SELECT ... FROM pococha_bq_prod.source.db_reincarnation_device_histories
WHERE created_at >= TIMESTAMP(CURRENT_DATE() - INTERVAL 30 DAYS);
```

- dashboards は Delta table を参照
- 夜間 ETL で refresh
- federation は「ad-hoc / exploratory」、持続クエリは Delta に寄せる

### 19.9 アラート優先順位 (federation 時)

federation クエリでは、Liquid Clustering / disk cache / file pruning / stats freshness / Photon blocker 系のアラートは意味を持たないため抑制されます (v5.18.0)。読むべきは:

1. Federation Query カード (このセクション)
2. Driver overhead (connection setup / scheduling 待ち)
3. Compilation overhead (外部 metadata 取得 / プランニング)
4. shuffle / spill / hash resize (DBSQL 側 JOIN が重い場合)

---

## 参考リンク
<!-- section_id: references -->

- [Databricks SQL Join Hints](https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-hints#join-hint-types)
- [Z-Ordering Ineffective Column Stats](https://kb.databricks.com/delta/zordering-ineffective-column-stats)
