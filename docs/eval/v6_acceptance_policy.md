# V6 Acceptance Policy (Week 6 Day 6)

V6 リファクタリングの **本番採用判定** に使うポリシーを正式に定義する。
コード上の実体は `eval/stage_gate.py:DEFAULT_ACCEPTANCE` にあり、本書は
その背景と運用ルールをまとめる。

## 1. 目的

W1–W5 で構築したメトリクスとゲートを **単一の判定ロジック** に集約し、
PR / リリース時に「V6 を採用すべきか / 待つべきか / 退行で却下か」を
機械的に出力する。

## 2. 2-stage 判定ロジック (`eval/stage_gate.py`)

| Stage 1 | Stage 2 | Verdict | 意味 |
|---------|---------|---------|------|
| pass | pass | **adopt** | V6 を採用、本番反映可 |
| pass | fail | **hold** | v5.19 同等以上だが V6 完了基準には未達、改善継続 |
| fail | (any) | **reject** | 退行が検出された、採用不可 |

- Stage 1 = `eval/regression_detector.py` (ベースライン比較、Tier 1 = block)
- Stage 2 = `eval/stage_gate.py:evaluate_stage2` (絶対品質)

## 3. Stage 2 absolute thresholds

`eval/stage_gate.py:DEFAULT_ACCEPTANCE` で実装。

| metric | 目標 | 由来 |
|--------|------|------|
| `schema_pass` | = 100% | R4 schema が完全に通ること (W2.5) |
| `q3_composite` | ≥ 80% | Codex W3 「測定可能になった段階」を超え、品質改善できた | 
| `actionability_specific` | ≥ 80% | rubric L4 (W1) |
| `failure_taxonomy` (Q5) | ≥ 70% | parse 失敗 / 根拠不足 / false positive を抑える (W5) |
| `recall_strict` | ≥ 50% | strict false-negative metric (W2.5) |
| `hallucination_clean` | ≥ 0.85 | rubric (W1, < 5% target) |
| `ungrounded_numeric_max` | ≤ 15% | Q3 W3 + 加重 W3.5 |
| `parse_success_rate` | ≥ 90% | SQL skeleton (W5) |
| `canonical_parse_failure_max` | ≤ 5% | LLM-direct emit (W3.5 #1) |
| `case_regressions_max` | ≤ 1 | A/B 個別 case の劣化件数 (W4 + Codex W3.5) |

すべて同時に満たす必要がある (AND 条件)。

## 4. Stage 1 regression tiers (`eval/regression_detector.py`)

| Tier | 動作 | 内容 |
|------|------|------|
| **1 BLOCK** | exit 1 / `reject` | Q3/Q4/Q5/recall_strict/hallucination -3pt 超 / schema -1% 超 |
| **2 WARN** | log + exit 0 | parse_success -5pt, ungrounded_numeric/canonical_parse_failure +5pt, over_recommendation 増加 |
| **3 INFO** | 記録のみ | skeleton method 分布シフト >10pt |

## 5. CLI 運用

### PR チェック (CI)

```bash
# 1. baseline 取得
PYTHONPATH=dabs/app:. python -m eval.goldens_runner \
  --baseline-name pr_<sha> --skip-judge

# 2. main との比較 + V6 absolute 判定
python -m eval.stage_gate_runner \
  --current eval/baselines/pr_<sha>.json \
  --baseline eval/baselines/main_latest.json \
  --on-reject-exit 1 \
  --on-hold-exit 0
```

- `reject` → exit 1 で PR ブロック
- `hold` → exit 0、PR コメントで「V6 完了基準未達、改善継続」表示
- `adopt` → exit 0 + 採用候補

### Nightly / リリース

```bash
python -m eval.stage_gate_runner \
  --current eval/baselines/release_<ver>.json \
  --baseline eval/baselines/v5_19_baseline.json \
  --on-hold-exit 1 \
  --on-reject-exit 1
```

リリース時は hold も block 相当 (V6 完了基準必須)。

## 6. metric 詳細

### `schema_pass`
- 各 case の canonical Report が JSON Schema (`schemas/report_v6.schema.json`) を pass した割合
- 1.00 必須 = 1 件でも違反あれば fail

### `q3_composite`
- Q3 evidence grounding scorer の重み付き平均
- 重み: finding_support 0.35 / metric_grounded 0.30 / ungrounded_numeric_inverse 0.20 / valid_source 0.075 / valid_knowledge_id 0.075

### `actionability_specific`
- Q4 actionability の 6/7 dim を満たす Action の割合
- 7 dim: target / what / why / how / expected_effect / verification / citation
- citation = skeleton が profile identifier を含む

### `failure_taxonomy`
- Q5 failure taxonomy の score (1.0 - sum(penalty))
- 5 category: parse_failure / evidence_unsupported / false_positive / over_recommendation / missing_critical

### `recall_strict`
- canonical Finding.issue_id が golden の must_cover_issues[].id と完全一致する割合

### `hallucination_clean`
- canonical Report 全体のハルシネーション清浄度 (1.0 = clean)

### `parse_success_rate`
- SQL skeleton の method ∈ {fullsql, sqlglot, bypass} の割合

### `canonical_parse_failure_rate`
- canonical_source ∈ {missing, normalizer_fallback} の割合

## 7. acceptance を緩める/締める手続き

- **緩める**: TODO.md に変更理由を記載、Codex レビュー必須、
  `DEFAULT_ACCEPTANCE` を更新する PR をレビュー 1 名以上承認
- **締める**: 同様に Codex レビュー必須。base shift があれば
  W5 → W6 完了時の baseline と比較した「上振れ余地」を示す

## 8. 既知の制約

- **rule-based のみで自動 adopt は不可**: canonical_parse_failure 100%
  が固定値なので、必ず LLM 込みベースラインで判定する
- **Layer B (LLM judge) は未組込**: r10_quality.py に placeholder。
  本番採用判定では deterministic Layer A のみ
- **個別 case の `expected_*` フィールド**: goldens の per-case 期待値
  (skeleton method 等) は acceptance には含めない (回帰検知の info 用)

## 9. 履歴

| Date | Change | Author |
|------|--------|--------|
| 2026-04-25 | 初版 (W6 Day 6) | V6 |

## 参照

- `eval/stage_gate.py` (実装)
- `eval/regression_detector.py` (Stage 1)
- `docs/eval/regression_detector_design.md`
- `docs/eval/r10_quality_addon_design.md`
- `docs/eval/report_quality_rubric.md` (W1 由来の品質指標)
- TODO.md V6 W6 完了基準
