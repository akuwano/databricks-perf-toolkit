# V6 リファクタリング ドキュメント索引

V6 (品質基盤リファクタ、`refactor/v6-quality` ブランチ) の設計・運用
ドキュメント索引。**現行 v6.6.0 まで反映**。

## 1. V6 とは

> 「分析品質を上げるための構造整理」を主目的に、LLM 段数削減・
> canonical schema・評価/回帰検知を一体導入するリファクタ。

評価基準は **レポート品質向上**。整理系タスク (重複ディレクトリ、
sections.py 分割、docs 整備) は品質改善ループが回り始めてから着手する。

V6 仕様の出発点:
- **初めて触る場合**: [`getting-started.md`](getting-started.md) ← 開発者 onboarding
- **品質モデル全体**: [`five-layer-feedback.md`](five-layer-feedback.md) (L1-L5)
- **コア仕様**: [`v6-spec.md`](../v6-spec.md)

## 2. 設計ドキュメント (`docs/v6/`)

| ドキュメント | 内容 | 由来 |
|-------------|------|------|
| [`getting-started.md`](getting-started.md) | **開発者 onboarding (1 ページ)** — 初日に読む順序 / 主要モジュール / typical debug | v6.6.0 docs 整理 |
| [`five-layer-feedback.md`](five-layer-feedback.md) | **5 層品質モデル (L1-L5)** — rule_echo / invariants / drift / panel / customer feedback | v6.6.0 docs 整理 |
| [`canonical_schema_inventory.md`](canonical_schema_inventory.md) | 既存 ActionCard / Alert と canonical schema のギャップ整理 | W2 Day 1 |
| [`output_contract.md`](output_contract.md) | required/optional / nullable / issue_id 30+ 語彙 / suppression rule | W2 Day 3 |
| [`knowledge_inventory.md`](knowledge_inventory.md) | 既存 knowledge 5,182 行の棚卸し + 注入経路 5 箇所 | W3 Day 1 |
| [`sql_skeleton_design.md`](sql_skeleton_design.md) | SQL skeleton の型・粒度・fallback 階層 | W5 Day 1 |
| [`v61_plan.md`](v61_plan.md) | V6.1 計画 (MERGE/VIEW/INSERT 抽出 + Layer B + runbook) | V6.1 Day 1 |
| [`operations.md`](operations.md) | V6 評価基盤の使い方 シナリオ別ワークフロー (A-F) | V6 deploy 後 |
| [`why-default-on.md`](why-default-on.md) | **ADR 2026-04-26** — V6 flag を default-on kill switch 化、retain/retire 一覧、退役条件 | v6.6.4 |
| [`alias-admission-rule.md`](alias-admission-rule.md) | **ADR 2026-04-27** — LLM-direct canonical の alias map 拡大ガード基準、v6.6.5+ refactor 計画、telemetry 追加 | v6.6.4 |
| [`query-rewrite-extraction.md`](query-rewrite-extraction.md) | **設計メモ 2026-04-27** — Query Rewrite を `genie_chat.py` から切り出す Phase 0-3 plan、入出力契約、評価軸 | v6.6.4 (Phase 1 は v6.6.5+ 別 PR) |
| [`api-endpoints.md`](api-endpoints.md) | デバッグ・運用エンドポイントリファレンス (curl 例) | V6 deploy 後 |

## 3. 評価関連ドキュメント (`docs/eval/`)

| ドキュメント | 内容 | 由来 |
|-------------|------|------|
| [`report_quality_rubric.md`](../eval/report_quality_rubric.md) | 5 品質指標 + L1-L4 採点基準 (V6 評価の正本) | W1 Day 1 |
| [`scorer_mapping.md`](../eval/scorer_mapping.md) | rubric ↔ scorer 対応表 | W1 Day 4 |
| [`ab_runner_design.md`](../eval/ab_runner_design.md) | 4 conditions A/B 並走、case 別 verdict | W4 Day 1 |
| [`r10_quality_addon_design.md`](../eval/r10_quality_addon_design.md) | R10 add-on 重み + verdict 閾値 | W4 Day 4 |
| [`regression_detector_design.md`](../eval/regression_detector_design.md) | 3-tier regression + R5 stage gate | W6 Day 1 |
| [`v6_acceptance_policy.md`](../eval/v6_acceptance_policy.md) | V6 採用判定の正本 (Stage 1+2 閾値) | W6 Day 6 |
| [`llm_acceptance_runbook.md`](../eval/llm_acceptance_runbook.md) | DATABRICKS_HOST/TOKEN 込みの adopt 判定手順 | V6.1 Day 6 |

## 4. ナレッジ関連 (`docs/knowledge/`)

| ドキュメント | 内容 | 由来 |
|-------------|------|------|
| [`v6_knowledge_policy.md`](../knowledge/v6_knowledge_policy.md) | knowledge 注入 budget / ALWAYS_INCLUDE 縮小 / 6 V6 flag | W3 Day 2 |

## 5. V6 主要モジュール

### Core (analysis pipeline 改修)

| モジュール | 役割 | 由来 |
|-----------|------|------|
| `dabs/app/core/feature_flags.py` | **8 V6 flags** (env / runtime-config / default off) | W3 Day 3 + v6.6.0 |
| `dabs/app/core/v6_schema/issue_registry.py` | issue_id 30+ 単一レジストリ (decimal_heavy_aggregate 含む) | W2.5 #5 + v6.1.0 |
| `dabs/app/core/v6_schema/normalizer.py` | ActionCard → canonical Report adapter | W2 Day 4 |
| `dabs/app/core/sql_skeleton.py` | SQL → 構造化 skeleton (5 method + extended 3) | W5 Day 2 + V6.1 |
| `schemas/report_v6.schema.json` | canonical Report JSON Schema v6.0 | W2 Day 2 |
| `dabs/app/core/analyzers/recommendations_registry.py` | **23 ActionCard 単一定義** (decimal_heavy_aggregate 等) | + v6.1.0 |
| `dabs/app/core/reporters/{__init__,summary,alert_crossref}.py` | レポート組み立て + flag 分岐 (Top Alerts compact) | + v6.6.0 |

### Evaluation

| モジュール | 役割 | 由来 |
|-----------|------|------|
| `eval/scorers/r4_schema.py` | schema validation | W2.5 |
| `eval/scorers/evidence_grounding.py` | Q3 (5 シグナル + 加重 composite) | W3 Day 5 + W3.5 #2 |
| `eval/scorers/actionability.py` | Q4 (7 dim + lenient/strict citation) | W1 + W5 + W6 |
| `eval/scorers/failure_taxonomy.py` | Q5 (5 category) | W5 Day 5 |
| `eval/scorers/r10_quality.py` + `r10_quality_judge.py` | R10 (Layer A det / Layer B LLM judge) | W4 Day 5 + V6.1 Day 4-5 |
| `eval/scorers/rule_echo_in_llm.py` | **L1**: rule emit が LLM narrative に echo か | v6.6.0 (5-layer) |
| `eval/scorers/invariants.py` + `eval/profile_evidence.py` | **L2**: profile signature → 必須 remedy family | v6.6.0 (5-layer) |
| `eval/scorers/canonical_diff.py` | **L3**: V5/V6 退行検知 mechanical diff | v6.6.0 (5-layer) |
| `eval/ab_runner.py` | 4 conditions 並走 + composite gate | W4 |
| `eval/regression_detector.py` | 3-tier regression detection | W6 Day 2 |
| `eval/stage_gate.py` + `stage_gate_runner.py` | R5 2-stage 採否 | W6 Day 4-5 |

### L5 customer feedback (v6.4-1.5)

| モジュール | 役割 |
|-----------|------|
| `dabs/app/services/feedback_bundle.py` | per-analysis / bulk ZIP 組み立て + HMAC signed token |
| `dabs/app/services/profile_redactor.py` | profile JSON deep-walk redaction (SQL literal / path / error / bounds 除去) |
| `dabs/app/services/user_context.py` | trusted header (`X-Forwarded-Email`) → user_email 抽出 |
| `dabs/app/routes/feedback.py` | POST /api/v1/feedback, /bundle/<aid>, /bundle/bulk + GET /feedback/export |

## 6. 実行コマンド

```bash
# 単条件 baseline (rule-based)
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name v6_baseline --skip-judge --skip-llm

# A/B 4 conditions
PYTHONPATH=dabs/app:. uv run python -m eval.ab_runner \
  --run-name v6_ab --skip-judge --skip-llm

# regression vs baseline
PYTHONPATH=dabs/app:. uv run python -m eval.regression_detector \
  --current eval/baselines/v6_current.json \
  --baseline eval/baselines/v6_main.json

# Stage Gate (採否判定)
PYTHONPATH=dabs/app:. uv run python -m eval.stage_gate_runner \
  --current eval/baselines/v6_current.json \
  --baseline eval/baselines/v6_main.json \
  --on-reject-exit 1
```

LLM 込み完全実走: [`docs/eval/llm_acceptance_runbook.md`](../eval/llm_acceptance_runbook.md)

## 7. 採用判定マトリクス

| Stage 1 (regression) | Stage 2 (absolute) | Verdict |
|----------------------|--------------------|---------| 
| pass | pass | **adopt** |
| pass | fail | **hold** |
| fail | (any) | **reject** |

詳細: [`v6_acceptance_policy.md`](../eval/v6_acceptance_policy.md)

## 8. V6 ロードマップ進捗

| 版 | 焦点 | 状態 |
|---|------|------|
| W1 | 品質定義 (rubric / 18 goldens / 3 scorer) | ✅ |
| W2 | canonical schema (R4 + normalizer) | ✅ |
| W2.5 | Codex 指摘 10 件対応 | ✅ |
| W3 | knowledge 整理 + Q3 evidence grounding | ✅ |
| W3.5 | Codex 指摘 5 件対応 (canonical-direct LLM 等) | ✅ |
| W4 | A/B runner + R10 add-on | ✅ |
| W5 | SQL skeleton + Q4 + Q5 | ✅ |
| W6 | R9 regression + R5 stage gate + V6 acceptance | ✅ |
| **v6.0.0** | dev deploy + 全 7 flag ON でベンチ | ✅ |
| **v6.1.0** | rule-based `decimal_heavy_aggregate` card 追加 + Q23 退行修正 + golden 拡充 | ✅ |
| **v6.2.0** | L1 rule_echo + L2 invariants + L5 feedback box (per-Action thumbs / 欠落申告) | ✅ |
| **v6.3.0** | per-action 改善要望 (各 ActionCard の💡 + dropdown) | ✅ |
| **v6.4.0** | L5 Phase 1: per-analysis ZIP export (`/shared/<id>` の📦) + signed token + redaction | ✅ |
| **v6.5.0** | L5 Phase 1.5: bulk ZIP (`/feedback/export`、admin gate、orphan_reason 必須) | ✅ |
| **v6.5.x** | UI 文言 / i18n 整理 (67 JA-msgid → EN msgid + ja.po 訳) | ✅ |
| **v6.6.0** | Top Alerts compact (Section 1 統合 + issue-tag 参照) | ✅ |
| (backlog) | L5 Phase 2: 中央 ingest pipeline (vendor_account_id + 3 schema 昇格モデル) | TODO.md `## L5 Phase 2` |
| (backlog) | strict mode redaction (table/column 名も hash) | Codex Phase 2 推奨 |
| (backlog) | UPDATE/DELETE skeleton + PR comment 自動化 | V6.2 持ち越し |

## 9. ファイル統計 (v6.6.0 時点)

- 設計 doc: 15 ファイル (本索引含む)
- 主要モジュール: 23 (core 7 + eval 11 + services/L5 5)
- テスト: 30 ファイル超 / 1855+ unit test (L5 関連 72 + 5-layer 53 含む)
- goldens: 32 cases (L5 退行検知用 cte_with_dominant_shuffle_key_q23 含む)
- 8 V6 feature flags (`docs/v6/getting-started.md` §5)

## 10. 参照

- TODO.md `### v6.0` 以降の進捗ログ (V6.6 まで)
- TODO.md `## L5 Phase 2: 顧客 Bundle 中央 ingest pipeline` (保留中の設計)
- 本ブランチ: `refactor/v6-quality` (origin に push 済)
- Codex レビュー履歴: commit log + TODO.md / 各 spec doc に時系列で残されている
