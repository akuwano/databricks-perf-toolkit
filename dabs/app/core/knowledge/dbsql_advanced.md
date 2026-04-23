# Shuffle・Spill・Skew・Data Explosion 詳細チューニング

## Shuffleパーティション詳細チューニング
<!-- section_id: shuffle_advanced -->

### Shuffleパーティション数の手動計算

AOS無効時やAOSの推定が不正確な場合、手動でのパーティション数チューニングが必要です。

**計算式:**
```
T = ワーカー総コア数
B = シャッフルステージの総データ量（MB）
M = ceiling(B / 128 / T)  ← 乗数
N = M × T                 ← シャッフルパーティション数
```

**目安: タスクあたり128MB〜200MB**（Spark UIのShuffle Stageメトリクスで確認）

```sql
-- 手動設定
SET spark.sql.shuffle.partitions = <N>;

-- 簡易設定（チューニングなし）
SET spark.sql.shuffle.partitions = <2 × ワーカー総コア数>;
```

### パーティション数の上限・下限

| 設定 | デフォルト | 説明 |
|------|-----------|------|
| `spark.databricks.adaptive.autoOptimizeShuffle.maxPartitionNumber` | 20480 | AOS上限 |
| AOS v2下限 | `2 * vCPUs`（DBSQL） | 初期パーティション数 |

**注意:** パーティション数が多すぎる場合もメモリ問題が発生（Map Status OOM）。大規模シャッフルでは上限と下限のバランスが重要。

---

## Data Explosionの検出と対策
<!-- section_id: data_explosion -->

### Data Explosionとは

特定の変換後にデータ量が急激に増加する現象。主な原因:

1. **Explode関数**: 配列/マップをフラット化する際にデータが膨張
2. **JOIN操作**: 予想以上の行数が生成される（Row Explosion）

**ESチケット頻出パターン:** Data Explosionの最多原因は**多対多JOIN・結合キーの重複・late filter**。JOIN後の行数が入力合計を大きく超える場合は、まず結合条件の正確性と**JOIN前の事前集約・重複排除**を疑う。フィルタ条件のJOIN前への前倒しも効果が高い。

### 検出方法

- **Spark UI**: `Generate`ノード（Explode）や`SortMergeJoin`/`ShuffleHashJoin`ノードの`rows output`メトリクスを確認
- 入力128MBのパーティションが数GBに膨張している場合、Data Explosionが発生

### 対策

**Explode関数によるExplosion:**
```sql
-- 入力パーティションサイズを縮小
SET spark.sql.files.maxPartitionBytes = 16777216;  -- 16MB（デフォルト128MB）
```

**JOINによるExplosion:**
```sql
-- シャッフルパーティション数を増加
SET spark.sql.shuffle.partitions = <大きい値>;
```

---

## Data Skewの検出と対策
<!-- section_id: skew_advanced -->

### 検出方法

1. **Spark UI**: タスクの大半が完了し1〜2タスクだけが長時間実行中 → Skew
2. **Summary Metrics**: Shuffle Read Sizeのmin/maxに大きな差がある
3. **直接確認**:
```sql
SELECT column_name, COUNT(*) as cnt
FROM table
GROUP BY column_name
ORDER BY cnt DESC
LIMIT 20;
```

**ESチケット頻出パターン:** AQEのskew join最適化で緩和できるケースは多いが、極端なホットキー（全体の50%以上が1値に集中）はSQL側での分解が必要。skew hintやキー分散より前に、**フィルタ前倒しと片側の事前縮小**を検討すること。

### 対策（優先順位順）

**1. スキュー値のフィルタ**
```sql
-- NULLによるスキューが原因の場合
SELECT * FROM table WHERE join_key IS NOT NULL;
```

**2. Skewヒント**
```sql
SELECT /*+ SKEW('table', 'column_name', (value1, value2)) */ *
FROM table;
```

**3. AQEのSkew最適化（デフォルト有効）**
```sql
-- デフォルト: 256MB以上かつ平均の5倍以上のパーティションをSkewと判定
SET spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = <bytes>;
SET spark.sql.adaptive.skewJoin.skewedPartitionFactor = <value>;

-- 2000パーティション超の場合、AQEがSkewを検出できない
SET spark.shuffle.minNumPartitionsToHighlyCompress = <パーティション数以上の値>;
```

**4. ソルティング（最終手段）**
スキューしたキーにランダムなサフィックスを追加してパーティションを分散。コード変更が必要なため、ヒントやAQEが効かない場合のみ使用。

---

## ブロードキャストJOIN詳細設定
<!-- section_id: broadcast_advanced -->

### 自動ブロードキャスト閾値

```sql
-- Sparkの自動ブロードキャスト閾値（デフォルト10MB）
SET spark.sql.autoBroadcastJoinThreshold = 209715200;  -- 200MB

-- AQEの適応的ブロードキャスト閾値（デフォルト30MB）
SET spark.databricks.adaptive.autoBroadcastJoinThreshold = 209715200;  -- 200MB
```

### 重要な制約

| 制約 | 値 | 説明 |
|------|-----|------|
| ブロードキャスト上限 | 8GB | Sparkのハード制限 |
| 推奨上限 | 1GB | ドライバのメモリ制約 |
| 圧縮注意 | - | Parquetのディスクサイズとメモリサイズは異なる（20〜40倍の差あり） |

**ESチケット頻出パターン:** Broadcastは最速候補だが、統計不正確時は過大な表を誤ってbroadcastしOOMを招く。高速化だけでなく、**build sideの実サイズ確認**が必須。古い統計や前段の行増幅でサイズが膨らむケースが多い。

### 明示的ブロードキャストの推奨

```sql
-- AQEより先にシャッフルをスキップするため、明示的ヒントが効率的
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table JOIN small_table ON ...;
```

**Photon使用時:** エグゼキュータ側ブロードキャストが有効なため、`spark.driver.maxResultSize`の調整が不要。

---

## Delta MERGEパフォーマンス最適化
<!-- section_id: merge_advanced -->

### MERGE操作のボトルネック

MERGEはON句の条件で内部的にJOINを実行し、マッチしたファイルを書き換えます。

**ESチケット頻出パターン（42件、バグ率24%）:** MERGE性能は条件式だけでなく、対象表のファイルサイズ・レイアウト・統計鮮度に強く依存。更新対象の局所性が悪いと不要ファイル読込が増大。Deletion Vectors（`enableDeletionVectors=true`）で劇的に改善するケースが多い。MERGEで不正結果が出る場合は既知バグの可能性が高い — DBRバージョンとES ticketを確認。

**問題:** ON句の条件が広すぎると大量のファイルが書き換え対象になる

### 最適化テクニック

**1. ターゲットテーブルのファイルサイズ調整**
```sql
-- MERGE頻度が高いテーブルは小さいファイルサイズ推奨（16〜64MB）
ALTER TABLE target_table SET TBLPROPERTIES (
  'delta.targetFileSize' = '33554432'  -- 32MB
);
```

**2. ON句にパーティションフィルタを含める**
```sql
MERGE INTO target t
USING source s
ON t.date = s.date  -- パーティション列でプルーニング
  AND t.id = s.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**3. Low Shuffle Merge（DBR 10.4+でデフォルト有効）**
- 変更されない行のデータ配置（Z-Orderクラスタリング等）を保持
- 変更された行のみが再編成される
