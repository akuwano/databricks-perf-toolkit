# Photon OOM診断と対策

## Photon OOMトラブルシューティング
<!-- section_id: photon_oom -->

PhotonエンジンでOOM（Out of Memory）が発生した場合の体系的な診断フローと対策です。

---

### 診断ステップ1: Photonが最大メモリ消費者か確認

OOMエラーメッセージの`task: allocated [...] MiB`と`Total task memory`を比較します。

- **Photonが最大** → ステップ2へ
- **ShuffleExternalSorterが最大** → DBR 16.3+にアップグレード（既知の問題が修正済み）
- **BytesToBytesMapが最大** → Spark Hash Aggregationがメモリを占有。より多くの処理をPhotonで実行するようクエリを書き換え
- **UnsafeExternalSorterが最大** → ESチケットを作成

---

### 診断ステップ2: 失敗したPhotonオペレータを特定

`Photon failed to reserve [...] MiB for [...], in [...]`メッセージのリスト内で、末尾から2番目のトラッカーが通常最も重要です。

#### FileWriterNode OOM

**原因:** 幅広いスキーマ（1000列超）または高圧縮データ

**対策:**
```sql
-- page.sizeを削減（目安: 256MB / カラム数）
SET parquet.page.size = <value>;
```

#### AggNode / GroupingAggNode OOM

**原因:** `collect_list`, `collect_set`, `percentile`がメモリ上に全データ収集

**ESチケット頻出パターン:** 高cardinality GROUP BY、不要DISTINCT、多段集約の中間肥大化が主因。まず集約前の件数削減と粒度見直しを行う。

**対策:**
- `percentile`の代わりに`approx_percentile`を使用
- 非グルーピング集約やスキューしたグループサイズでの`collect_list/set`を回避
- メモリ/コア比の高いインスタンスを使用

#### BroadcastHashedRelation OOM

**原因:** ブロードキャストされたテーブルがメモリに収まらない

**ESチケット頻出パターン:** broadcast hintの誤用だけでなく、古い統計や前段の行増幅でbuild sideが膨張するケースが多い。対策はhint除去だけでなく、片側の事前集約・フィルタ前倒し・stats更新を優先。

**対策:**
1. AQE BHJ OOMフォールバックがDBR 13.3+でデフォルト有効（自動でShuffle Joinに切り替え）
2. Null Aware Anti Join (NAAJ) の場合:
```sql
-- NOT INをNOT EXISTSに書き換え
-- Before
SELECT * FROM t WHERE val NOT IN (SELECT val FROM sub);
-- After
SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM sub WHERE sub.val = t.val);
```
3. `ANALYZE TABLE`で統計情報を更新
4. Shuffle Joinヒントを適用:
```sql
SELECT /*+ SHUFFLE_HASH(t2) */ * FROM t1 JOIN t2 ON ...;
```

#### BroadcastBufferedRelation OOM（Broadcast Nested Loop Join）

**原因:** JOINの条件不足や大規模なクロスジョイン

**対策:**
- JOIN条件が欠落していないか確認（Hash Joinに変換可能か）
- Range Joinの場合: `/*+ RANGE_JOIN(t, 10) */` ヒントを使用
- ブロードキャスト側の指定: `/*+ BROADCAST(smaller_table) */`

#### FileReader OOM

**原因:** 巨大なJSON文字列やネストされた配列によるParquetページの肥大化

**対策:**
- 読み取りカラムを削減（`SELECT *`を避ける）
- JSONスキャンでのOOM: `SET spark.databricks.photon.jsonScan.enabled = false`

#### ShuffleExchangeSinkNode OOM

**原因:** Bloomフィルタによるメモリ過剰使用、または大きな行データ

**対策（Bloomフィルタ）:**
```sql
SET spark.databricks.photon.outputHashCodeForBloomFilter.enabled = false;
```

**対策（行データ）:**
- DBR 15.4+にアップグレード（auto batch sizing有効）

---

### 診断ステップ3: Spill Pinningの確認

OOMエラーメッセージ内の`output batch var len data`または`spilled var-len chunks`が1GB以上の場合、Spill Pinningの問題です。

**対策:**
- DBR 16.3+にアップグレード（retry-based spill機能）
- バッチサイズを縮小:
```sql
SET spark.databricks.photon.autoBatchSize.targetSize = 16777216;  -- 16MB
```

---

### 汎用的な対策（最終手段）

1. メモリ/コア比の高いワーカーインスタンスを選択
2. `spark.executor.cores`を削減（例: 4コアインスタンスで2に設定 → タスクあたりのメモリ倍増）
3. Photonを無効化: `SET spark.databricks.photon.enabled = false`（最終手段）
