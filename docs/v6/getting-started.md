# V6 開発者 オンボーディング

V6 (`refactor/v6-quality` ブランチ、現行 v6.6.0) を**初めて触る開発者**が
迷わず動かせる状態に到達するための最短経路ドキュメント。

---

## 1. 全体像 (3 分)

V6 の目的は **「LLM レポートの品質を測れる・退行を検知できる・採用判定
できる」基盤の整備**。アプリ機能 (DBSQL プロファイル分析) は v5.19 と
ほぼ同等で、変わったのは **品質評価レイヤと内部構造**。

ざっくり 4 ブロック:

```
┌────────────────────────────────────────────────┐
│ 1. analysis pipeline      (core/usecases.py)   │  入力: profile JSON
│    + rule-based cards     (core/analyzers/)    │
│    + LLM 3-stage          (core/llm.py)        │
│    + canonical Report     (core/v6_schema/)    │ ← V6 で追加
└────────────────────────────────────────────────┘
              ↓ produces
┌────────────────────────────────────────────────┐
│ 2. report rendering       (core/reporters/)    │  Markdown
└────────────────────────────────────────────────┘
              ↓ stored in
┌────────────────────────────────────────────────┐
│ 3. Delta tables           (services/table_*)   │  履歴 / 比較 / feedback
└────────────────────────────────────────────────┘
              ↓ evaluated by
┌────────────────────────────────────────────────┐
│ 4. evaluation             (eval/)              │  ← V6 の主役
│    - scorers              (eval/scorers/)      │
│    - golden cases         (eval/goldens/)      │
│    - regression detector  (eval/regression_*)  │
│    - stage gate           (eval/stage_*)       │
│    - feedback bundle      (services/feedback_*)│  ← V6.4-1.5
└────────────────────────────────────────────────┘
```

---

## 2. 初日に読むべき順序

| # | ドキュメント | なぜ |
|---|-------------|------|
| 1 | この `getting-started.md` | 全体像を 5 分で取る |
| 2 | [`v6-spec.md`](../v6-spec.md) | コア 6 機能の 1-行サマリー |
| 3 | [`five-layer-feedback.md`](five-layer-feedback.md) | L1-L5 品質モデルの位置付け |
| 4 | [`output_contract.md`](output_contract.md) | canonical Report のスキーマ規約 |
| 5 | [`operations.md`](operations.md) | ローカル / dev で評価を回す手順 |
| 6 | [`api-endpoints.md`](api-endpoints.md) | デバッグ用 endpoint カタログ |

> 設計の経緯まで追いたい場合は最後に
> [`canonical_schema_inventory.md`](canonical_schema_inventory.md),
> [`knowledge_inventory.md`](knowledge_inventory.md),
> [`sql_skeleton_design.md`](sql_skeleton_design.md), TODO.md `### v6.0`

---

## 3. ローカル開発の最小セット

### 3.1. テスト実行

```bash
# 全テスト (約 5 秒)
cd dabs/app && uv run pytest -q --ignore=tests/smoke

# eval 系のみ (golden / scorer のテスト)
cd repo_root && uv run pytest eval/tests/ -q
```

### 3.2. golden case を rule-only で 1 周

```bash
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name local_dev --skip-judge --skip-llm
# → eval/baselines/local_dev.json と eval/reports/local_dev.md を出力
```

### 3.3. LLM 込みは acceptance runbook

```bash
# DATABRICKS_HOST/TOKEN が必要
docs/eval/llm_acceptance_runbook.md
```

---

## 4. 主要モジュール責務 早見表

### V6 で追加された / 大幅変わったもの

| モジュール | 役割 | 何で困るか |
|-----------|------|------|
| `core/feature_flags.py` | 8 V6 flags の解決 (env > runtime-config > default OFF) | 8 つの flag 名と意味を知らないと何が動いてるか分からない |
| `core/v6_schema/issue_registry.py` | canonical issue_id レジストリ (30+) | 新 alert 追加時に必ず触る |
| `core/v6_schema/normalizer.py` | ActionCard → canonical Report 変換 | LLM-direct と normalizer fallback の 2 経路 |
| `schemas/report_v6.schema.json` | canonical Report JSON Schema v6.0 | スキーマ違反は R4 scorer で検知 |
| `core/sql_skeleton.py` | SQL → 構造化 skeleton (8 method) | 長い SQL を Q3/Q4 が読めるサイズに圧縮 |
| `core/analyzers/recommendations_registry.py` | 23 ActionCard の単一定義 | rule-based 推奨を増やす時はここ |

### 既存 (V5 と共通)

| モジュール | 役割 |
|-----------|------|
| `core/usecases.py` | 分析パイプラインの top-level orchestrator |
| `core/extractors.py` | Profile JSON → NodeMetrics, ShuffleMetrics 等 |
| `core/llm.py` | 3-stage LLM (analyze → review → refine) |
| `core/reporters/__init__.py` | Markdown レポート組み立て |
| `core/llm_prompts/prompts.py` | Stage 1-3 プロンプト構築 |

### V6 で追加された evaluation

| モジュール | 役割 |
|-----------|------|
| `eval/goldens_runner.py` | golden 31 件を順番評価 |
| `eval/ab_runner.py` | 4 conditions A/B 並走 |
| `eval/regression_detector.py` | 3-tier regression (R9) |
| `eval/stage_gate.py` + `stage_gate_runner.py` | R5 2-stage acceptance |
| `eval/scorers/r4_schema.py` | canonical Report が schema に通るか |
| `eval/scorers/evidence_grounding.py` | Q3 (5 シグナル) |
| `eval/scorers/actionability.py` | Q4 (7 dim、lenient/strict) |
| `eval/scorers/failure_taxonomy.py` | Q5 (5 category) |
| `eval/scorers/r10_quality{,_judge}.py` | R10 add-on (Layer A det / Layer B LLM) |
| `eval/scorers/rule_echo_in_llm.py` | **L1**: rule emit が LLM narrative に echo されてるか |
| `eval/scorers/invariants.py` + `eval/profile_evidence.py` | **L2**: profile signature → 必須 remedy family |
| `eval/scorers/canonical_diff.py` | V5/V6 退行検知の mechanical diff |

詳細は [`five-layer-feedback.md`](five-layer-feedback.md)。

### V6.4-1.5 で追加された L5 feedback bundle

| モジュール | 役割 |
|-----------|------|
| `services/feedback_bundle.py` | per-analysis / bulk ZIP 組み立て + signed token |
| `services/profile_redactor.py` | profile JSON deep-walk redaction (SQL literal/path/error 除去) |
| `services/user_context.py` | trusted header 経由の user_email 抽出 |
| `routes/feedback.py` | POST /api/v1/feedback, /bundle/<aid>, /bundle/bulk + /feedback/export ページ |

---

## 5. 8 V6 feature flags

| flag | 意味 |
|------|------|
| `V6_CANONICAL_SCHEMA` | LLM が canonical Finding/Action JSON を直接 emit |
| `V6_REVIEW_NO_KNOWLEDGE` | review stage で knowledge を渡さない (毒抑制) |
| `V6_REFINE_MICRO_KNOWLEDGE` | refine stage で micro knowledge のみ |
| `V6_ALWAYS_INCLUDE_MINIMUM` | knowledge 必須 ALWAYS_INCLUDE を縮小 |
| `V6_SKIP_CONDENSED_KNOWLEDGE` | condensed knowledge をスキップ |
| `V6_RECOMMENDATION_NO_FORCE_FILL` | アクション数を強制充足しない |
| `V6_SQL_SKELETON_EXTENDED` | MERGE/VIEW/INSERT skeleton を有効化 |
| `V6_COMPACT_TOP_ALERTS` | `## 2. Top Alerts` を Section 1 末尾の `### Key Alerts` に統合 (v6.6.0) |

解決順: env var → runtime-config.json → default OFF

確認方法 (dev):
```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "$APP_URL/api/v1/debug/feature-flags" | jq .
```

詳細: [`api-endpoints.md`](api-endpoints.md)

---

## 6. golden case を増やす

```bash
# 1. profile JSON を準備
ls json/<my-case>.json

# 2. case 定義
cat > eval/goldens/cases/my_new_case.yaml <<EOF
case_id: my_new_case
profile_path: json/<my-case>.json
description: 何をテストしたいか 1 行
domain: dbsql
expected_severity: high

must_cover_issues:
  - id: shuffle_dominant      # canonical issue_id (issue_registry にある語彙)
    severity: high
    keywords: [shuffle, シャッフル]

forbidden_claims:
  - id: federation             # 除外したい誤認識を id で
    description: federation ではないので federation 提案禁止

must_have_actions:
  - target: my_target          # remedy family 単位 (Q4 actionability)
    type: clustering
    keyword: CLUSTER BY
    description: 期待される remedy family の一例
EOF

# 3. manifest 登録
# eval/goldens/manifest.yaml の末尾 cases リストに追加
#   - case_id: my_new_case
#     file: cases/my_new_case.yaml

# 4. 1 周回す
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name verify --skip-judge --skip-llm
```

詳細: [`output_contract.md`](output_contract.md), `eval/goldens/README.md`

---

## 7. 典型的な debug シナリオ

### A. 「dev に deploy したけど画面が古い」
1. `/spark-perf` ページで右下の `v<X>.<Y>.<Z>` を確認
2. `/api/v1/debug/feature-flags` で flag が想定通りか確認
3. `dabs/local-overrides.yml` の `v6_*` を確認

### B. 「LLM が rule-based card の出力を narrative に書いていない」
- `eval/scorers/rule_echo_in_llm.py` (L1) で測れる
- `eval/baselines/<run>.json` の `rule_echo_score` / `rule_echo_missed` を確認
- LLM プロンプトの `Fact Pack` (`core/llm_prompts/prompts.py`) に
  rule の文言が入っているか確認

### C. 「golden case に追加したい profile の仕様が不安」
- `output_contract.md` の `must_cover_issues` / `forbidden_claims` /
  `must_have_actions` の 3 メカニズムを確認
- `eval/scorers/recall.py`, `hallucination.py`, `actionability.py` で
  実際にどう照合されるか把握

### D. 「canonical schema validation でひっかかる」
- `eval/scorers/r4_schema.py` の error 列を見る
- `core/v6_schema/normalizer.py` で fallback path に流れていないか
- LLM-direct (V6_CANONICAL_SCHEMA=on) なら parse failure は
  `canonical_source=normalizer_fallback` に切り替わる

### E. 「customer から feedback ZIP が届いた、何を見るべき」
- `manifest.json` の `bundle_format_version` (1 = per-analysis, 2 = bulk)
- `feedback.json` の `category` と `free_text`
- `profile_redacted.json` の `redact_stats.parse_failures`
  (parser 失敗が多ければ自社 redactor 改善余地)
- 詳細: [`five-layer-feedback.md`](five-layer-feedback.md) §L5

---

## 8. CI / リリース運用

| 項目 | コマンド |
|------|---------|
| 全テスト | `cd dabs/app && uv run pytest -q --ignore=tests/smoke` |
| eval テスト | `cd repo_root && uv run pytest eval/tests/ -q` |
| golden runner (rule-only) | `PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner --skip-llm --skip-judge --baseline-name local` |
| LLM 込み acceptance | `docs/eval/llm_acceptance_runbook.md` 参照 |
| dev deploy | `./scripts/deploy.sh dev` (5-10 分。Background 実行推奨) |
| version bump | `pyproject.toml` + `dabs/app/app.py` の `APP_VERSION` を bump (deploy 前) |

---

## 9. もっと深く知りたい時

- 設計経緯: [`canonical_schema_inventory.md`](canonical_schema_inventory.md)
  (W2 day 1)、[`knowledge_inventory.md`](knowledge_inventory.md) (W3 day 1)、
  [`sql_skeleton_design.md`](sql_skeleton_design.md) (W5 day 1)、
  [`v61_plan.md`](v61_plan.md) (V6.1)
- 進捗ログ: ルート [`TODO.md`](../../TODO.md) `### v6.0` 以降
- 評価仕様: [`docs/eval/`](../eval/) ディレクトリ全体
- Codex レビュー履歴: TODO.md / コミットメッセージに点在 (`Codex 2026-04-`)

---

## 10. よくある質問

**Q. `V6_CANONICAL_SCHEMA=true` を切ると挙動は v5.19 同等?**
> ほぼ同等。ただし v6.x で追加された rule-based card (decimal_heavy_aggregate
> 等) は flag 関係なく出る。完全 v5 復刻は別。

**Q. `V6_COMPACT_TOP_ALERTS=true` でレポート構造が変わる、評価への影響は?**
> R3/R4 schema, Q3/Q4/Q5 への直接影響なし (canonical Report 不変)。
> Markdown レポートの section 番号がずれるので、snapshot test を持つ
> 場合は flag 別 snapshot にする。

**Q. golden を新規追加するとき issue_id を勝手に決めて良い?**
> 不可。`core/v6_schema/issue_registry.py::ISSUES` に登録された 30+ の
> id しか canonical Report に通らない。新規 issue は registry に
> 追加 PR が先。

**Q. profile_redactor は完全匿名化と説明して良い?**
> **不可**。"reduced sensitivity" 表記。table/column 名は保持される。
> 詳細: [`five-layer-feedback.md`](five-layer-feedback.md) §L5。

---

困った時は: TODO.md の `### v6.0` セクション + 各 design doc の
`参照コード` ブロックを当たる。
