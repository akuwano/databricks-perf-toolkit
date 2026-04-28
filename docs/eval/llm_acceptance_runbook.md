# V6 LLM Acceptance Runbook (V6.1 Day 6)

V6 を本番採用する判定 (= Stage Gate verdict=`adopt`) を出すための
LLM 込み実走手順。Codex V6 final review (2026-04-25) で「本番採用前
の必須」とされた 3 項目をすべてカバーする。

## 1. 前提

- `refactor/v6-quality` ブランチを check out
- DATABRICKS_HOST / DATABRICKS_TOKEN が手元に設定済
- `json/` 以下に goldens manifest が参照する profile JSON が存在
  (gitignore のため、開発者個別に collect 必要)
- Python 環境 (`uv sync`) 済

## 2. 1 コマンド実行 (推奨)

```bash
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<your-token>

# Run the full V6.1 acceptance gate (LLM ON, Layer B judge ON, all gates)
PYTHONPATH=dabs/app:. uv run python -m eval.ab_runner \
  --run-name v6_acceptance_$(date +%Y%m%d) \
  --with-llm-judge \
  --judge-top-n 5 \
  --gate-w4-infra \
  --gate-llm-quality \
  --gate-condition both \
  --gate-r10-verdict pass
```

成功 → exit 0。
失敗 → exit 1 + reason ログ。

## 3. ステージ別実行 (デバッグ用)

### Step 1. baseline 取得 (LLM 込み)

V6_CANONICAL_SCHEMA を on にした `both` condition で 31 cases 走らせる:

```bash
V6_CANONICAL_SCHEMA=1 \
V6_RECOMMENDATION_NO_FORCE_FILL=1 \
V6_REVIEW_NO_KNOWLEDGE=1 \
V6_REFINE_MICRO_KNOWLEDGE=1 \
V6_ALWAYS_INCLUDE_MINIMUM=1 \
V6_SKIP_CONDENSED_KNOWLEDGE=1 \
V6_SQL_SKELETON_EXTENDED=1 \
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name v6_acceptance_both \
  --model databricks-claude-sonnet-4-6
```

数値の確認ポイント (受入基準):

| metric | target |
|--------|--------|
| schema_pass | 100% |
| q3_composite | ≥ 80% |
| metric_grounded | ≥ 70% |
| finding_support | ≥ 80% |
| ungrounded_numeric | ≤ 15% |
| recall_strict | ≥ 50% |
| hallucination | ≥ 0.85 |
| canonical_parse_failure | ≤ 5% |

`eval/reports/v6_acceptance_both.md` に出力される。

### Step 2. A/B 比較 (4 conditions)

`baseline / canonical-direct / no-force-fill / both` を **同一 LLM API**
で並走:

```bash
PYTHONPATH=dabs/app:. uv run python -m eval.ab_runner \
  --run-name v6_acceptance_ab \
  --with-llm-judge \
  --judge-top-n 5
```

`eval/ab_summary/v6_acceptance_ab.md` で確認:
- per-condition の R10 layer_a + layer_b
- regression_summary (improved / regressed / unchanged)
- canonical_parse_failure_rate (Codex W3.5 §3-5)

### Step 3. Stage Gate (採否判定)

`v5.19` ベースライン (タグ起点) と比較:

```bash
PYTHONPATH=dabs/app:. uv run python -m eval.stage_gate_runner \
  --current eval/baselines/v6_acceptance_both.json \
  --baseline eval/baselines/v5_19_baseline.json \
  --on-reject-exit 1 \
  --on-hold-exit 1
```

verdict:
- **adopt**: V6 採用、main にマージ可
- **hold**: V6 完了基準に未達 (V6.2 で改善継続)
- **reject**: 退行検出、修正後に再実走

## 4. judge コスト目安

`--judge-top-n 5` で 4 conditions:
- 4 conditions × 5 actions = 20 LLM calls / run
- Sonnet 4.6 で 1 call ≈ 2-3K tokens output → run 全体で 40-60K tokens
- 1 run あたり数 USD 程度の見込み

cost 抑制:
- `--judge-top-n 3` でさらに削減
- `--conditions baseline,both` で 2 conditions 比較に絞る

## 5. PR / リリース工程

| 段階 | 期待 verdict | 動作 |
|------|-------------|------|
| 開発者 PR | `hold` 許容 | コメントで品質レポート、merge は人間判断 |
| Nightly main | `adopt` 推奨 | 失敗時 Slack 通知 |
| Release tag | `adopt` 必須 | reject/hold で release 中止 |

## 6. canonical parse failure が高い時

`canonical_parse_failure_rate > 5%` のとき:

1. `eval/baselines/v6_acceptance_both.json` の `canonical_source_breakdown`
   を確認: `normalizer_fallback` か `missing` か
2. `normalizer_fallback` ばかり = LLM が `json:canonical_v6` block を出
   していない → prompt directive が効いていない
   - `core/llm_prompts/prompts.py:_v6_canonical_output_directive` の
     directive が prompt 末尾に append されているか確認
   - LLM 応答を 1-2 件 dump して目視
3. `missing` = LLM 起動自体が失敗 → API 設定確認

## 7. 既知の制約

- 31 cases の goldens の profile JSON が個人環境にしか無い場合あり
  → `json/` 配下に置く / 既存環境からコピー
- judge の cost が高い → `--judge-top-n 1` で最低限の sanity check
- 一部 case が profile_path missing で skip される
  → `eval/baselines/<name>.json` の `skipped_reason` を確認

## 8. V6.1 → V6.2 への bridge

LLM acceptance run の結果が hold だった場合の改善候補 (V6.2):

| 観測 | 対応 |
|------|------|
| canonical_parse_failure > 10% | prompt directive 強化 / 例の追加 |
| metric_grounded < 60% | knowledge selector の Q3 連動を強化 |
| finding_support < 70% | normalizer evidence 抽出を canonical 直接出力へ |
| over_recommendation > 5 | LLM prompt で max actions = 3 を強調 |

## 9. ファイル

- `eval/ab_runner.py` (--with-llm-judge / --gate-llm-quality)
- `eval/stage_gate_runner.py` (--on-reject-exit / --on-hold-exit)
- `eval/scorers/r10_quality_judge.py` (Layer B)
- 出力: `eval/baselines/`, `eval/ab_summary/`, `eval/stage_gate/`

## 10. 履歴

| Date | Change |
|------|--------|
| 2026-04-25 | 初版 (V6.1 Day 6) |

## 参照

- `docs/eval/v6_acceptance_policy.md` (受入基準の正本)
- `docs/eval/ab_runner_design.md`
- `docs/eval/r10_quality_addon_design.md`
- TODO.md V6 W6 完了レポート
- Codex 2026-04-25 V6 final review
