# V6 SQL Skeleton Design (Week 5 Day 1)

TODO.md の既存設計「SQL スケルトン抽出 (設計済み・未着手)」を Week 5
の R8 として実装に落とす。Codex W4 review (2026-04-25) §6 の重要観点:

> SQL skeleton は「引用根拠つき・未確定値を埋めない」

を Week 5 全体の通底原則とする。

## 1. 目的

V5.19 の `prompts.py:2285` は SQL を **3000 chars で盲目的 truncate**
している。これにより:
- 86K chars の実ケースで最終 SELECT・中後段 CTE が LLM に見えない
- LLM は見えない部分を *推測で補う* → hallucination の温床

Week 5 では:
1. sqlglot AST で SQL の **構造** を抽出
2. **値・式詳細** は落とす (literal / identifier 詳細)
3. CTE 名・JOIN 種別・GROUP BY 列等の **診断に必要な構造** は残す
4. canonical Action.fix_sql には **skeleton + 全文 fallback** を統合

## 2. canonical schema との接続 (Codex W4 review §6 反映)

`schemas/report_v6.schema.json` の `Action` を拡張し、SQL skeleton と
全文の両方を表現できるようにする (W5 Day 3 で実装):

```json
{
  "action_id": "...",
  "target": "...",
  "fix_type": "configuration | rewrite | ...",
  "what": "...",
  "fix_sql": "SET spark.sql.shuffle.partitions=400;",
  "fix_sql_skeleton": "SELECT ... FROM fact_orders f JOIN dim_customer c ON f.customer_id = c.customer_id WHERE ...",
  "fix_sql_truncated": false,
  "fix_sql_chars_original": 86000,
  "fix_sql_chars_in_prompt": 2400
}
```

- `fix_sql`: 実行可能 SQL (短ければ全文、長ければ truncated 表示)
- `fix_sql_skeleton`: sqlglot 経由の構造化要約 (LLM への入力に使われる)
- `fix_sql_truncated`: skeleton で代替されたか
- `fix_sql_chars_original` / `..._in_prompt`: 元サイズ / 削減サイズ

## 3. MVP スコープ (TODO.md L519-526 + W5 で確定)

**保持する**:
- `WITH/CTE 名` + 参照関係
- `FROM/JOIN 種別/相手テーブル`、`ON` は predicate shape のみ
  (equality / range / LIKE / IN / EXISTS / OR-heavy / non-sargable)
- `WHERE` も同様に shape のみ (値は落とす)
- `GROUP BY / ORDER BY / HAVING` 列
- `UNION 分岐数`、`subquery type`、`DISTINCT` / `LIMIT`
- `SELECT 列数の集約` (`<N cols>`)
- CTE 多重参照カウント

**落とす**:
- 完全な列リスト
- 複雑な ON / WHERE 式の中身
- 式内の関数呼び出し詳細
- 値リテラル (string / number / date)

## 4. 適用条件 (TODO.md L538-540)

- `len(sql) > 3000` **かつ** `cte_count >= 2 OR union/subquery/join_count high`
- 短い単純クエリは skeleton 化コストに見合わないので適用しない (全文を渡す)
- `MERGE` / `CREATE VIEW AS` / `INSERT ... SELECT` は **初版 skeleton 化せず**、
  全文 or head+tail fallback

## 5. fallback 階層

```
1. sqlglot skeleton  (適用条件を満たすとき)
2. head+tail (1500 chars 各)  (sqlglot parse 失敗時)
3. 旧 truncate 3000 chars (両方ダメな時の最終手段、警告ログ)
```

## 6. ファイル

```
dabs/app/core/sql_skeleton.py       # NEW (Day 2 実装)
dabs/app/tests/test_v6_sql_skeleton.py  # NEW (Day 2)
schemas/report_v6.schema.json       # Action.fix_sql_skeleton 等を追加 (Day 3)
docs/v6/sql_skeleton_design.md      # 本 doc
```

## 7. API 案

```python
from core.sql_skeleton import (
    build_sql_skeleton,
    SkeletonResult,
)

result: SkeletonResult = build_sql_skeleton(
    sql_text,
    char_budget=2500,
    fallback_head_tail=True,
)
# result.skeleton: str
# result.method: "sqlglot" | "head_tail" | "truncate" | "fullsql"
# result.parse_success: bool
# result.cte_count: int
# result.join_count: int
# result.original_chars: int
# result.skeleton_chars: int
```

## 8. 観測

W5 Day 7 baseline で計測:
- `parse_success_rate` (sqlglot で構造抽出できた割合)
- `skeleton_used_rate` (skeleton 適用された SQL の割合 — 全文使われた
  case と区別)
- `compression_ratio` (skeleton_chars / original_chars 平均)
- `fallback_distribution` (sqlglot / head_tail / truncate / fullsql の
  比率)

W5 完了基準:
- parse_success_rate ≥ 90%
- compression_ratio ≤ 0.30 (skeleton は元サイズの 30% 以下に縮む)
- fallback (head_tail + truncate) は 全 case の ≤ 5%

## 9. Q4 actionability との連動 (Day 4)

action template の標準形に **fix_sql_skeleton と元 fix_sql の差分が
合理的か** を入れる:

- `fix_sql` が短く `fix_sql_skeleton` と一致 → OK
- `fix_sql` が長く `fix_sql_skeleton` が要約 → OK
- `fix_sql` が空で `fix_sql_skeleton` も空 → fix_type=investigation のみ可

Q4 actionability の 6 dim に追加で第 7 dim **citation** を導入:
- skeleton の identifier (table / column) が profile evidence の table_scans
  に存在するか

## 10. Q5 failure taxonomy との連動 (Day 5)

failure category にエントリ追加:

| category | 説明 |
|----------|------|
| `parse_failure` | sqlglot parse 失敗 (head_tail fallback) |
| `evidence_unsupported` | Action 内 reference が profile に存在しない |
| `false_positive` | suppression が必要なのに finding が出た |
| `over_recommendation` | 1 issue に対して action が多すぎる (>3) |
| `missing_critical` | must_cover_issues の critical を見逃し |

各 case の Q5 score は 1 - (failures / weighted total) で 0..1。

## 11. Day 別配分

| Day | 成果物 |
|-----|--------|
| 1 | この設計 doc |
| 2 | `core/sql_skeleton.py` + test |
| 3 | schema 拡張 + normalizer 経由で `fix_sql_skeleton` 埋め |
| 4 | Q4 actionability に skeleton + citation 追加 |
| 5 | Q5 failure taxonomy + scorer |
| 6 | golden cases に sql_skeleton_required / action_template_required |
| 7 | W5 baseline + Codex review + W6 引き継ぎ |

## 12. 後方互換

- 既存 `prompts.py:2285` の truncate は **デフォルトでは触らない**
- `feature_flags.V6_SQL_SKELETON` を新設 (default off)
- flag on のときのみ skeleton を prompt 注入 + canonical Action に格納

## 参照

- TODO.md `## SQL スケルトン抽出 (設計済み・未着手)` (L503+)
- `schemas/report_v6.schema.json` (Action 構造)
- `docs/v6/output_contract.md` §6 (fix_type)
- Codex W4 review (2026-04-25) §6
