# SQL書き換えパターンによるクエリ最適化

## SQL書き換えパターン
<!-- section_id: sql_patterns -->

SQLの書き方を変えるだけでクエリパフォーマンスを劇的に改善できるケースがあります。以下は実証済みの最適化パターンです。

---

### 1. UNION ALL vs UNION

`UNION`は重複排除のためにソートとデデュプリケーションを行い、大量データでは**10〜100倍遅くなる**ことがあります。

**アンチパターン:**
```sql
-- 重複排除が不要なのにUNIONを使用（ソート+デデュプ発生）
SELECT material_id FROM query1
UNION
SELECT material_id FROM query2
UNION
SELECT material_id FROM query3;
```

**推奨パターン:**
```sql
-- UNION ALLで結合し、最後にのみDISTINCTで重複排除
SELECT DISTINCT material_id FROM (
    SELECT material_id FROM query1
    UNION ALL
    SELECT material_id FROM query2
    UNION ALL
    SELECT material_id FROM query3
);
```

---

### 2. EXISTS vs IN（大規模データセット）

`IN`サブクエリは内部テーブル全体を評価しますが、`EXISTS`は最初のマッチで停止します。

**アンチパターン:**
```sql
-- INは大規模サブクエリで非効率
SELECT * FROM orders
WHERE customer_id IN (
    SELECT customer_id FROM customers WHERE status = 'ACTIVE'
);
```

**推奨パターン:**
```sql
-- EXISTSは最初のマッチで停止
SELECT * FROM orders o
WHERE EXISTS (
    SELECT 1 FROM customers c
    WHERE c.customer_id = o.customer_id
    AND c.status = 'ACTIVE'
);
```

---

### 3. Filter Early（早期フィルタリング）

> **Note on TEMP VIEW usage in this section:** 以降で `CREATE OR REPLACE TEMP VIEW` を使った例が出てきますが、これは **可読性・段階的な SQL 整理のため**の表記です。CTE と同様に、TEMP VIEW は実体化や再計算回避を保証しません（カタログ上のエイリアス）。重複計算の解消が目的の場合は、`CTAS` / Delta テーブル化 もしくはクエリの書き換えで重複サブプランを 1 回にまとめ、`EXPLAIN` で `ReusedExchange` の有無を確認してください。

JOINの**前に**フィルタを適用し、処理データ量を削減します。

**ESチケット頻出パターン:** Filter Earlyは最も再現性の高い改善策。JOIN・MERGE・集約の前で選択性の高い条件を適用すると、後段のshuffle・spill・OOMを同時に抑制できる。

**アンチパターン:**
```sql
-- JOIN後にフィルタ → 大量データをJOINしてから絞り込み
SELECT * FROM large_table1
JOIN large_table2 ON ...
JOIN large_table3 ON ...
WHERE large_table1.status = 'ACTIVE'
  AND large_table2.type = 'VALID';
```

**推奨パターン:**
```sql
-- JOIN前にフィルタ → 処理データ量を削減
CREATE OR REPLACE TEMP VIEW filtered_table1 AS
SELECT * FROM large_table1 WHERE status = 'ACTIVE';

CREATE OR REPLACE TEMP VIEW filtered_table2 AS
SELECT * FROM large_table2 WHERE type = 'VALID';

SELECT * FROM filtered_table1
JOIN filtered_table2 ON ...;
```

---

### 4. 相関サブクエリの排除

相関サブクエリは外側の各行に対して実行されるため非常に遅くなります。

**アンチパターン:**
```sql
-- 相関サブクエリ（各行で実行）
SELECT
    material_id,
    (SELECT MAX(price) FROM prices p
     WHERE p.material_id = m.material_id) as max_price
FROM materials m;
```

**推奨パターン:**
```sql
-- JOINまたはウィンドウ関数に書き換え
SELECT m.material_id, p.max_price
FROM materials m
LEFT JOIN (
    SELECT material_id, MAX(price) as max_price
    FROM prices GROUP BY material_id
) p ON m.material_id = p.material_id;
```

---

### 5. 不要なDISTINCTの排除

`DISTINCT`はソートと重複排除を伴い、中間ステップで使うと不要なコストが発生します。

**アンチパターン:**
```sql
-- 各ステップでDISTINCT（毎回ソート+デデュプ）
CREATE OR REPLACE TEMP VIEW step1 AS
SELECT DISTINCT * FROM table1 WHERE ...;

CREATE OR REPLACE TEMP VIEW step2 AS
SELECT DISTINCT * FROM step1 JOIN table2 ...;
```

**推奨パターン:**
```sql
-- 最終結果でのみDISTINCT
CREATE OR REPLACE TEMP VIEW step1 AS
SELECT * FROM table1 WHERE ...;

CREATE OR REPLACE TEMP VIEW step2 AS
SELECT * FROM step1 JOIN table2 ...;

SELECT DISTINCT * FROM step2;
```

---

### 6. 複雑なJOIN条件の分解

CASE式をJOIN条件に含めると、オプティマイザが最適なJOIN戦略を選択できません。

**アンチパターン:**
```sql
-- CASE式をJOIN条件に使用
SELECT * FROM table1 a
JOIN table2 b ON
    CASE WHEN a.type = 'A' THEN a.id_a
         WHEN a.type = 'B' THEN a.id_b
         ELSE a.id_c END = b.id;
```

**推奨パターン:**
```sql
-- 事前にJOINキーを正規化
CREATE OR REPLACE TEMP VIEW normalized_table1 AS
SELECT
    CASE WHEN type = 'A' THEN id_a
         WHEN type = 'B' THEN id_b
         ELSE id_c END as join_id, *
FROM table1;

SELECT * FROM normalized_table1 a
JOIN table2 b ON a.join_id = b.id;
```

---

### 7. Sargableなフィルタ条件

カラムに関数を適用するとインデックスやData Skippingが効かなくなります。

**ESチケット頻出パターン:** Sargableでない条件はData Skippingだけでなく、統計ベース最適化全体を弱める。`WHERE YEAR(col) = 2024` → `WHERE col >= '2024-01-01'` への変換が最多改善。列側に関数をかけず、範囲条件や正規化済み列で比較すること。

**アンチパターン:**
```sql
-- カラムに関数を適用 → Data Skipping無効
WHERE YEAR(order_date) = 2024
WHERE UPPER(customer_name) = 'JOHN'
```

**推奨パターン:**
```sql
-- 範囲指定 → Data Skipping有効
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
```

---

### 8. JOIN前集約と重複排除
<!-- section_id: pre_join_agg -->

JOINの前にデータを集約・重複排除することで、Data ExplosionとShuffle量を大幅に削減します。

**ESチケット頻出パターン:** 多対多JOINによるData Explosionはクエリ障害の最多原因の一つ。JOIN後の行数が入力合計を超える場合は、結合キーの重複が原因。

**アンチパターン:**
```sql
-- 重複キーを持つテーブル同士をそのままJOIN → 行数が爆発
SELECT * FROM orders o JOIN items i ON o.order_id = i.order_id;
```

**推奨パターン:**
```sql
-- JOIN前に片側を集約して1:N → 1:1 に変換
WITH order_summary AS (
  SELECT order_id, SUM(amount) AS total_amount
  FROM orders GROUP BY order_id
)
SELECT s.order_id, s.total_amount, i.*
FROM order_summary s JOIN items i ON s.order_id = i.order_id;
```

**判断基準:** JOINノードの`rows output`が入力行数の合計を大きく超える場合、事前集約が必要。

---

### 実績: SQL書き換えによる改善事例

| 指標 | Before | After | 改善率 |
|------|--------|-------|--------|
| 実行時間 | 2時間 | 2分 | **60倍高速化** |
| スキャンデータ量 | 500 GB | 100 GB | **5倍削減** |
| メモリ使用量 | 32 GB | 8 GB | **4倍削減** |

**適用したパターン:** Filter Early + UNION ALL化 + 不要DISTINCT排除 + Temp View活用
