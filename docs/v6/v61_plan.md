# V6.1 Plan (2026-04-25)

V6 リファクタリング 6 週間 + W2.5 + W3.5 完了後、Codex 最終レビュー
(2026-04-25) で持ち越しとされた 3 項目への対応。

## 1. V6.1 タスク

| # | タスク | 優先度 | 概要 |
|---|--------|--------|------|
| 1 | LLM acceptance run | High | DATABRICKS_HOST/TOKEN 込みで stage gate を実走 |
| 2 | MERGE / complex SQL extraction | High | sql_skeleton.py の bypass を構造抽出に |
| 3 | Layer B LLM judge | High | r10_quality.py の Layer B placeholder 実装 |

着手順:
- **Day 1 (本 doc)**: V6.1 計画 + MERGE 構造抽出設計
- **Day 2-3**: MERGE / CREATE VIEW / INSERT 構造抽出 → sql_skeleton.py 拡張
- **Day 4-5**: Layer B LLM judge → r10_quality.py 拡張
- **Day 6**: LLM acceptance runbook + app.py dirty tree 整理
- **Day 7**: V6.1 baseline + Codex 確認 + push

## 2. MERGE / CREATE VIEW 構造抽出 設計

### 2.1 現状 (W5)

`core/sql_skeleton.py` で MERGE / CREATE VIEW / INSERT / UPDATE /
DELETE は **すべて bypass** = 全文 or head+tail。理由は MVP として
sqlglot の AST walk を SELECT 系に絞ったため。

```python
_BYPASS_RE = re.compile(
    r"^\s*(MERGE\s+INTO|CREATE\s+(OR\s+REPLACE\s+)?(MATERIALIZED\s+)?VIEW|"
    r"INSERT\s+(OVERWRITE\s+)?(INTO\s+)?|UPDATE\s+|DELETE\s+FROM)",
    re.IGNORECASE,
)
```

問題 (Codex W5 #1):
> DBSQL workload では MERGE / CREATE VIEW が重要。一律 bypass は
> 重要ケースを丸ごと逃す。

### 2.2 V6.1 抽出スコープ (構造のみ、値は落とす)

#### MERGE INTO

```
MERGE INTO {target_table}
USING {source_alias} ON [predicate_shape]
WHEN MATCHED [AND <cond_shape>] THEN UPDATE SET <N cols>
WHEN MATCHED [AND <cond_shape>] THEN DELETE
WHEN NOT MATCHED [BY TARGET] [AND <cond_shape>] THEN INSERT <N cols>
WHEN NOT MATCHED BY SOURCE THEN ...
```

抽出する項目:
- target table 名 (qualified)
- source 種別 (table 名 or `(SELECT ...)`) と source 内のテーブル数
- ON 述語の shape (`eq` / `range` / `or_heavy` 等)
- WHEN 句の数 + 各 action 種別 (UPDATE / DELETE / INSERT)
- 各 WHEN の述語 shape

落とす:
- 列リスト (代わりに `<N cols>`)
- 値リテラル

#### CREATE VIEW AS SELECT

```
CREATE [OR REPLACE] [MATERIALIZED] VIEW {name}
AS <select_skeleton>
```

抽出:
- view 名 (qualified)
- 修飾子 (OR REPLACE / MATERIALIZED)
- AS 以降の SELECT 本体に対して既存 sqlglot 経路を再帰適用

#### INSERT INTO ... SELECT

```
INSERT [OVERWRITE] INTO {target_table} [(<N cols>)]
<select_skeleton>
```

抽出:
- target table 名 + OVERWRITE フラグ
- target 列数 (非 verbose)
- SELECT 本体に既存 sqlglot 適用

#### UPDATE / DELETE

W6 では bypass 維持 (low priority、boundary case を golden で固定)。

### 2.3 fallback

MERGE / VIEW / INSERT で sqlglot parse 失敗 → 既存 head_tail.

### 2.4 method 列の値

|  抽出成功 | method |
|-----------|--------|
| MERGE 構造抽出 OK | `merge` |
| CREATE VIEW + 内部 SELECT 抽出 OK | `view` |
| INSERT + 内部 SELECT 抽出 OK | `insert` |
| 抽出失敗 (parse fail) | `head_tail` |
| 既存 SELECT | `sqlglot` (変化なし) |

これにより `aggregate_parse_metrics` の method_distribution が
**実態を反映する** (現状は MERGE が常に bypass で見えない)。

### 2.5 W6 boundary goldens への影響

`merge_into_skeleton.yaml` の `expected_skeleton.method` を
`bypass` → `merge` に変更し、`bypass_reason` を削除。

### 2.6 実装ファイル

```
dabs/app/core/sql_skeleton.py        # MERGE/VIEW/INSERT branch 追加
dabs/app/tests/test_v6_sql_skeleton.py # 4-6 unit test 追加
eval/goldens/cases/merge_into_skeleton.yaml  # expected_skeleton 更新
```

### 2.7 後方互換

- `feature_flags.V6_SQL_SKELETON_EXTENDED` (default off)
  - off: W5 W6 と同じ動作 (MERGE = bypass)
  - on: 構造抽出を試みる
- LLM プロンプトに新 method を渡す経路は別途検証 (initial で normalizer
  output に新 method 値が混入することは canonical schema に enum 追加が
  必要 — 段階的に)

## 3. Layer B LLM judge 設計

### 3.1 現状 (W4 Day 5)

`eval/scorers/r10_quality.py` の `R10QualityScore` に:
- `layer_b_score: float | None = None`
- `layer_b_reasons: list[str] = []`

placeholder のみ実装、実 LLM コール無し。

### 3.2 V6.1 で行うこと

- `eval/scorers/r10_quality_judge.py` を新設
  - `score_layer_b(canonical_report, llm_config) → float, list[str]`
  - 既存 `eval/scorers/l3l4_judge.py` の judge 呼び出し枠組みを再利用
  - 各 Action 単位で judge_l3_l4 → score 平均
  - 1-5 スケールを 0-1 に正規化 (avg / 5)
  - LLM API 失敗時は None で fallback
- `eval/ab_runner.py` に `--with-llm-judge` flag 追加
  - 各 condition の代表 case (top 5 by R10 layer_a) を judge に投入
  - layer_b_score を summary に追加

### 3.3 制約

- LLM call が必要 = DATABRICKS_HOST/TOKEN
- コスト抑制: 全 31 cases 全部走らせると重い → top-N (default 5) のみ
  judge する
- 最終 R10 = (layer_a + layer_b) / 2 (W4 Day 4 設計通り)

## 4. LLM acceptance run runbook

`docs/eval/llm_acceptance_runbook.md` に書く内容:

```bash
# 1. 環境
export DATABRICKS_HOST=...
export DATABRICKS_TOKEN=...
# (json/ 配下に goldens.profile_path で参照される JSON が必要)

# 2. baseline (LLM 込み)
PYTHONPATH=dabs/app:. uv run python -m eval.ab_runner \
  --run-name v61_acceptance \
  --conditions baseline,canonical-direct,both \
  --with-llm-judge \
  --gate-w4-infra \
  --gate-llm-quality \
  --gate-condition both

# 3. stage gate
PYTHONPATH=dabs/app:. uv run python -m eval.stage_gate_runner \
  --current eval/baselines/v61_acceptance__both.json \
  --baseline eval/baselines/v6_w6_baseline.json \
  --on-reject-exit 1 --on-hold-exit 1
```

V6 採用判定:
- adopt → main マージ可
- hold → V6.2 で品質改善継続
- reject → 退行修正

## 5. dirty tree 整理 (Codex W5 #6)

`dabs/app/app.py` の version bump (5.19.4 → 5.19.5) は V6 ブランチ
独立の差分。V6 PR には含めず、別 PR (`chore: bump version 5.19.5`)
で main に投入する想定。

## 6. V6.1 完了基準

- MERGE / CREATE VIEW / INSERT 構造抽出 が rule-based で動作
- skeleton method_distribution に `merge` / `view` / `insert` が出現
- Layer B 統合済 (LLM API 利用時に layer_b_score が埋まる)
- LLM acceptance runbook 文書化
- 全 tests pass + canonical schema validation 100%

## 7. ファイル

```
docs/v6/v61_plan.md                       # 本 doc
dabs/app/core/sql_skeleton.py             # MERGE/VIEW/INSERT 抽出
dabs/app/tests/test_v6_sql_skeleton.py    # 拡張テスト
eval/scorers/r10_quality_judge.py         # Layer B 実装
eval/ab_runner.py                         # --with-llm-judge
docs/eval/llm_acceptance_runbook.md       # NEW (Day 6)
TODO.md                                   # V6.1 進捗
```

## 参照

- TODO.md V6.1 backlog (V6 W6 完了レポート末尾)
- Codex 2026-04-25 V6 final review
- `docs/v6/sql_skeleton_design.md`
- `docs/eval/r10_quality_addon_design.md`
