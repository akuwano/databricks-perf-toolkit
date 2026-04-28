# V6 5-layer フィードバック品質モデル

V5 → V6 の比較で「同じ profile に対して V6 が V5 より悪化したケース」が
発見された (2026-04-25 / Q23 ベンチマーク) のを契機に、Codex 設計レビュー
を経て確立した **多層品質保証モデル**の解説。

> 単独の scorer / golden 1 つでは "未知の退行" を防ぎきれない。
> 5 層を補完的に重ねて初めて、本番運用で品質を維持できる。

---

## 0. なぜ多層モデルか

### V5 と V6 の差分問題

V5 までは LLM の自由文 narrative 1 本で品質を担保していた。V6 で
**canonical schema (R4) + rule-based card emission** を導入したことで、
「LLM の出力が canonical で取得できる」反面、**LLM 自由文と canonical
の整合**を別軸で測る必要が生じた。

具体的に Q23 (TPC-DS) で観測された退行:

| 観点 | V5.19.5 出力 | V6.0.0 出力 |
|------|-------------|-------------|
| DECIMAL 精度の見直し | 推奨あり ✅ | **脱落** 🔴 |
| `ss_customer_sk` を Liquid Clustering キーに追加 | あり ✅ | **脱落** 🔴 |
| HC TBLPROPERTIES 構文 | (legacy 引用) | (canonical 引用) |
| Filter Early 提案 | あり | あり |

V5 を ground truth に修正した結果が v6.6.0。

### Codex の判断 (要約、5 layer 一括)

> **単独の scorer は最悪「測れていない劣化」を生み出す**。L1 だけでは
> "そもそも emit されてない劣化" が見えず、L2 だけでは "rule emit が
> narrative に反映されていない劣化" が見えない。L3 (self-baseline) と
> L4 (panel) も補完的、L5 (customer feedback) は他層で取れない
> ドメイン感を補う。

---

## 1. 5 層モデル概観

```
┌─────────────────────────────────────────────────────────┐
│ L1 rule_echo_in_llm                                      │
│   rule emit (canonical Finding) が LLM 自由文に echo    │
│   されているか — deterministic                            │
│   → eval/scorers/rule_echo_in_llm.py                    │
├─────────────────────────────────────────────────────────┤
│ L2 profile-signature invariants                          │
│   profile の特徴 → 期待される remedy family が含まれて   │
│   いるか — case-independent invariants                   │
│   → eval/scorers/invariants.py + profile_evidence.py    │
├─────────────────────────────────────────────────────────┤
│ L3 self-baseline drift detection                         │
│   過去 V6 出力との diff、issue_id / family 単位の脱落    │
│   → eval/scorers/canonical_diff.py                      │
│   → eval/regression_detector.py (3-tier)                │
├─────────────────────────────────────────────────────────┤
│ L4 LLM-as-judge panel (existing)                         │
│   Q3/Q4/Q5/R10 から既知品質を測る                         │
│   → eval/scorers/{evidence_grounding,actionability,     │
│      failure_taxonomy,r10_quality{,_judge}}.py          │
├─────────────────────────────────────────────────────────┤
│ L5 customer feedback loop                                │
│   実顧客が「これが抜けてる / こうしてほしかった」を       │
│   申告 → bundle ZIP → 中央集約                          │
│   → routes/feedback.py + services/feedback_bundle.py    │
└─────────────────────────────────────────────────────────┘
```

各層は **独立に判定**でき、合わせると "未知の退行" のカバレッジが
段違いに高くなる。

---

## 2. L1 — rule_echo_in_llm

### 何を測るか

rule-based card (e.g. `decimal_heavy_aggregate`) が canonical Report に
emit されたが、**LLM 自由文 (executive summary / action plan) に echo
されていない** 状態を検知する。

### なぜ必要か

V6 では canonical Report に Finding が並ぶが、レポートを読む人間は
markdown narrative を見る。**Finding が canonical にしか無く narrative
に反映されない** = 実質的に user に届いていない退行。

### スコア

```python
score = (echoed_count / rule_finding_count)  # 1.0 = 全て narrative に反映
no_op = (rule_finding_count == 0)            # rule emit 0 件は除外
```

severity `medium` 以上の Finding が echo の対象 (low は無視)。

### 実装

```
eval/scorers/rule_echo_in_llm.py
  RuleEchoScore { rule_finding_count, echoed_count, missed_issue_ids, score, no_op }
  score_rule_echo(canonical, llm_narrative)
  aggregate_rule_echo(scores)  # no_op 除外で平均
```

### よくある退行パターン

| 退行 | 検知 |
|------|------|
| canonical に `decimal_heavy_aggregate` Finding あり、narrative に DECIMAL 言及無し | `missed_issue_ids` に登場 |
| `V6_CANONICAL_SCHEMA=true` で LLM が schema 圧縮、自由文セクションが冷却 | `echoed_count` 急減 |
| narrative が単一トピックに偏る (shuffle のみ) | 他 Finding が `missed_issue_ids` |

---

## 3. L2 — profile-signature invariants

### 何を測るか

**profile の特徴を見て「ここまでは触れるべき」を auto 判定**。1 case ずつ
golden に書かなくても、profile signature ごとに必須 remedy family が
要求される。

### Codex の重要指摘

> 「specific 解法の必須 (e.g. CLUSTER BY ss_customer_sk) を要求する」
> と false positive 多発。**remedy family のいずれかを満たせば OK**
> にする。

### 4 つの初期 invariant

| invariant_id | profile signature | 期待 remedy family |
|--------------|-------------------|---------------------|
| `heavy_decimal_arithmetic` | aggregate node の peak ≥ 100 GB かつ式に算術 (* / + / -) | `type_review` (DECIMAL / DESCRIBE TABLE / 型最適化) |
| `dominant_shuffle_outside_lc` | dominant shuffle key が当該テーブルの clustering keys に含まれない | `clustering` (CLUSTER BY / Liquid Clustering / OPTIMIZE / ZORDER) |
| `cte_recompute_materialization` | 同 CTE が 2+ 回参照され ReusedExchange なし | `materialization` (CTAS / 物理化 / persist / ReusedExchange) |
| `spill_heavy_warehouse_review` | 累積 spill ≥ 100 GB | `warehouse_size` / `filter_early` / `clustering` のいずれか |

remedy family の語彙は `eval/scorers/canonical_diff.py::REMEDY_FAMILIES` と
同じ source of truth で `eval/scorers/invariants.py::REMEDY_FAMILY_KEYWORDS`
を参照。

### スコア

```python
violations = [inv for inv in fired if not _family_satisfied(inv)]
score = max(0, 1 - 0.25 * len(violations))   # 1 violation = -0.25
no_op = len(fired) == 0                       # signature 該当なしは除外
```

### profile_evidence のシグナル抽出

`eval/profile_evidence.py::collect_profile_evidence(analysis)` で:

```
ProfileEvidence
  decimal_arithmetic_in_heavy_agg: bool
  decimal_arithmetic_examples: [(node_id, expr_excerpt)]
  dominant_shuffle_keys_outside_lc: bool
  dominant_shuffle_outside_lc_columns: [(table, column)]
  cte_multi_reference: bool
  cte_multi_reference_names: [(name, ref_count)]
  spill_dominant: bool
  spill_total_bytes: int
```

shuffle_attributes の解決は `cat.schema.table.col` (full qualified) /
`alias.col` / 裸 column の 3 ケースに対応。

### 拡張

invariant 追加は:
1. `profile_evidence.py` にシグナル収集追加
2. `invariants.py::_INVARIANTS` にエントリ追加 (signature → families)
3. テストを `eval/tests/test_invariants.py` に追加

---

## 4. L3 — self-baseline drift detection

### 何を測るか

**過去の V6 出力 (baseline) と現行 V6 出力の diff** を取り、issue_id /
remedy family 単位での「脱落」を検知する。

### canonical_diff scorer

```
eval/scorers/canonical_diff.py
  CanonicalDiffScore {
      baseline_card_count, current_card_count,
      baseline_families, current_families, dropped_families, new_families,
      baseline_issue_ids, current_issue_ids,
      dropped_issue_ids, new_issue_ids,
      dropped_high_severity_issue_ids,
      score
  }
  score_canonical_diff(baseline_cards, current_cards, baseline_canonical, current_canonical)
```

### remedy family taxonomy (10 種、L2 と共有)

```
type_review, clustering, hierarchical_clustering, materialization,
filter_early, aqe_skew, broadcast, repartition, compression, warehouse_size
```

### regression_detector の 3-tier

`eval/regression_detector.py` で aggregate baseline と比較:
- minor: 1 metric が threshold 内で悪化
- major: ≥ 2 metric が悪化 / 1 metric が threshold 外
- critical: high severity issue 脱落 / canonical schema fail

stage_gate (R5): regression が pass + V6 acceptance policy が pass の
両方を満たす場合のみ "adopt"。詳細:
[`docs/eval/v6_acceptance_policy.md`](../eval/v6_acceptance_policy.md)。

---

## 5. L4 — LLM-as-judge panel (既存 Q3/Q4/Q5/R10)

V6 で導入された 4 scorer。詳細は各 design doc:

| scorer | 設計 doc |
|--------|---------|
| Q3 evidence grounding (5 シグナル) | [`docs/eval/scorer_mapping.md`](../eval/scorer_mapping.md) |
| Q4 actionability (7 dim, lenient/strict) | 同上 |
| Q5 failure taxonomy (5 category) | 同上 |
| R10 quality add-on (Layer A det / Layer B LLM) | [`docs/eval/r10_quality_addon_design.md`](../eval/r10_quality_addon_design.md) |

L1/L2 と独立して動き、**Q4 の `actionability_specific_ratio` ≥ 0.6**
が R5 acceptance gate の主要絶対指標。

---

## 6. L5 — customer feedback loop (Phase 1 + 1.5)

### 全体フロー

```
顧客 workspace 内 (Databricks Apps)
  ┌─────────────────────────────────────────────┐
  │ 1. レポート閲覧 /shared/<analysis_id>        │
  │ 2. 末尾 box から欠落申告                      │
  │    or 各 ActionCard 横の💡で per-action 改善 │
  │     POST /api/v1/feedback                    │
  │     → profiler_feedback Delta                │
  │                                              │
  │ 3a. /shared/<id> の📦 で per-analysis ZIP   │
  │ 3b. /history → /feedback/export で bulk ZIP │
  │     → ZIP を vendor へ手動送付                │
  └─────────────────────────────────────────────┘
                ↓ (ZIP transfer)
  ┌─────────────────────────────────────────────┐
  │ Vendor 中央 (Phase 2 設計済み・保留)         │
  │  ZIP → vendor_raw → vendor_curated MERGE     │
  │  monthly triage で goldens / invariants 化   │
  └─────────────────────────────────────────────┘
```

### Phase 1 (per-analysis ZIP, v6.4.0)

`/shared/<analysis_id>` ページの上部「📦 ZIP」ボタンから:

ZIP 中身:
```
metadata.json           # bundle_format_version=1, redact_stats
report.md               # 顧客が読んだ markdown
canonical_report.json   # compact
feedback.json           # user_email→hash, domain のみ
profile_redacted.json   # SQL literal/path/error/bounds 除去
checksums.json          # SHA256 manifest
README.txt              # "reduced-sensitivity" 明記
[profile.json]          # opt-in only (raw 全部)
```

3 段階 modal: 警告 → profile.json checkbox (default OFF) →
「内容を確認してから送付」checkbox。

### Phase 1.5 (bulk ZIP, v6.5.0)

`/feedback/export` ページから (workspace_admin 限定):

```
feedback_bulk_<workspace_slug>_<exported_at>.zip
├── manifest.json          # bundle_format_version=2 (bulk)
├── bundles/
│   ├── <analysis_id_1>/{report.md, canonical_report.json,
│   │                    profile_redacted.json, feedback.json}
│   ├── <analysis_id_2>/...
├── orphan_feedback.json   # orphan_reason 必須
├── checksums.json
└── README.txt
```

profile.json (raw) は **bulk では絶対含めない** (Codex (e) 安全策)。

期間 dropdown: 過去 7/30/90 日 / 全期間 (default 30 日、`limits_hit` で
上限超過時に warning)。

### 重要な設計判断 (Codex 推奨反映)

| 観点 | Codex 推奨 |
|------|-----------|
| 配置 | `/history` ボタン → 専用 `/feedback/export` ページに遷移 (権限分離) |
| 認証 | HMAC-SHA256 signed token (5 分 TTL)、`/prepare` → `/bundle` 2 step |
| user_email | salted SHA256 hash (16 hex) + domain のみ平文残す。raw email は ZIP に**絶対含めない** |
| profile redaction | "reduced sensitivity, NOT full anonymization" を README で明示 |
| bulk profile.json | 原則禁止。raw が必要なら per-analysis ZIP で個別取得 |
| schema_version | `major.minor` 管理。major unknown → quarantine |
| 監査 | 全 export を `profiler_feedback_export_log` Delta に記録 |

### Phase 2 (中央集約 ingest) は保留

`raw → landing → staged → promoted` の昇格モデル + `vendor_account_id`
明示紐付け + 3 schema 分離 (vendor_raw / vendor_curated) を Codex 推奨。
詳細: TODO.md `## L5 Phase 2`。

---

## 7. 各層のカバレッジ早見表

| 退行パターン | L1 | L2 | L3 | L4 | L5 |
|-------------|:--:|:--:|:--:|:--:|:--:|
| rule emit が narrative に echo されない | ✅ | ⚠ | - | - | - |
| profile signature 該当の remedy family 欠落 | ⚠ | ✅ | ⚠ | ⚠ | - |
| 過去より issue_id 種類が減った | - | - | ✅ | - | ⚠ |
| LLM 出力 schema 違反 | - | - | - | R4 | - |
| evidence grounding 弱い | - | - | - | Q3 | - |
| action 具体性弱い | - | - | - | Q4 | - |
| ドメイン専門家から見た「これが抜けてる」 | - | - | - | - | ✅ |

✅ = 主担当層、⚠ = 部分的にカバー

---

## 8. ローカルでの確認

### L1 / L2 単体実行

```bash
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name verify --skip-judge --skip-llm
```

出力 `eval/baselines/verify.json` の各 case に:
- `rule_echo_score`, `rule_echo_missed`, `rule_echo_no_op`
- `invariants_score`, `invariants_fired`, `invariants_satisfied`,
  `invariants_violations`, `invariants_no_op`

### L3 (regression) の典型実行

```bash
PYTHONPATH=dabs/app:. uv run python -m eval.regression_detector \
  --current eval/baselines/v6_current.json \
  --baseline eval/baselines/v6_main.json
```

### L4 LLM 込み

```bash
docs/eval/llm_acceptance_runbook.md  # DATABRICKS_HOST/TOKEN 必須
```

### L5 デモ (dev)

```
1. /shared/<id> の📦 ZIP ボタン → 個別 ZIP DL
2. /history → 全フィードバック ZIP → /feedback/export → bulk ZIP DL
```

---

## 9. 関連ファイル一覧

```
eval/scorers/rule_echo_in_llm.py        # L1
eval/scorers/invariants.py              # L2
eval/profile_evidence.py                # L2 シグナル抽出
eval/scorers/canonical_diff.py          # L3 mechanical diff
eval/regression_detector.py             # L3 3-tier
eval/scorers/{evidence_grounding,actionability,failure_taxonomy}.py  # L4
eval/scorers/r10_quality{,_judge}.py    # L4 R10
eval/stage_gate{,_runner}.py            # L3+L4 統合 acceptance gate

services/feedback_bundle.py             # L5 ZIP 組み立て + signed token
services/profile_redactor.py            # L5 SQL/path/error redact
services/user_context.py                # L5 trusted header → user_email
routes/feedback.py                      # L5 endpoint 群
templates/{shared_result,feedback_export,history}.html  # L5 UI
```

詳細は [`getting-started.md`](getting-started.md) §4 の表参照。

---

## 10. 参考

- TODO.md `### v6.0` 以降の進捗ログ
- TODO.md `## L5 Phase 2` (保留中の中央 ingest 設計)
- Codex レビュー履歴 (commit log + TODO.md に時系列で残されている)
