# R9 Regression Detector Design (Week 6 Day 1)

V6 リファクタの最終週、R9 (regression detection) と R5 (2-stage gate)
の設計。Codex W5 review (2026-04-25) の追加観点を反映:

> R9 regression は Q3/Q4/Q5 だけでなく、`fix_sql_skeleton_method`
> 分布、fallback 率、compression p50/p95 を退行検知対象に入れる

## 1. 目的

W4 で A/B runner、W5 で skeleton + Q4 + Q5 が揃った。R9 は **PR ごとに
品質劣化を検知して PR を block** する仕組み。

W6 完了基準 (TODO.md V6 Week 6):
- A/B 4 conditions の R10 layer_a が全部記録
- Regression detector が改善/劣化を case 単位で判定
- 2-stage 採否 (stage1 = baseline 比、stage2 = 絶対品質) で blocking gate
- V6 acceptance: schema=100%, Q3≥80%, Q4≥80%, Q5≥70%, R10≥0.80,
  parse_success≥90%, regression≤1

## 2. Regression 検知対象

| Tier | metric | 取得元 | 許容劣化幅 |
|------|--------|--------|-----------|
| **Tier 1 (block)** | Q3 composite | ab_summary.metrics_per_condition | -3pt |
| Tier 1 | Q4 actionability | 同上 | -3pt |
| Tier 1 | Q5 failure taxonomy | goldens_runner per-case | -3pt |
| Tier 1 | recall_strict | 同上 | -3pt |
| Tier 1 | hallucination_clean | 同上 | -3pt |
| Tier 1 | schema_pass | 同上 | -1% (1 case 落ちただけで block) |
| **Tier 2 (warn)** | parse_success_rate | skeleton 分布 | -5pt |
| Tier 2 | compression_p50 | skeleton 分布 | +5pt (悪化方向) |
| Tier 2 | canonical_parse_failure | ab_summary | +5pt |
| Tier 2 | over_recommendation count | failure_counts | +1 incident |
| **Tier 3 (info)** | skeleton method shift | distribution | sqlglot→head_tail rate up |
| Tier 3 | per-case verdict count | ab_summary | regressed cases > 1 |

`block` = exit 1, `warn` = log warning, `info` = informational only.

## 3. 入力

- `eval/baselines/<run_name>__baseline.json` (現行コミットの baseline)
- `eval/baselines/<run_name>__main.json` または history (比較対象)
- 任意で `--against <git-ref>` で過去 ref と worktree 比較

## 4. 出力

`eval/regression_summary/<run_name>.json`:

```json
{
  "current_run": "v6_w6_baseline",
  "compared_against": "main",
  "block_violations": [
    {"metric": "q3_composite", "current": 0.62, "baseline": 0.66, "delta": -0.04, "tier": 1}
  ],
  "warn_violations": [
    {"metric": "parse_success_rate", "current": 0.85, "baseline": 0.92, "delta": -0.07, "tier": 2}
  ],
  "info_violations": [],
  "verdict": "block | warn | clean",
  "skeleton_distribution_drift": {
    "sqlglot": {"current": 0.60, "baseline": 0.75, "delta": -0.15},
    "head_tail": {"current": 0.30, "baseline": 0.10, "delta": +0.20}
  }
}
```

`block_violations` 非空 → exit 1。
`warn_violations` 非空 → exit 0 + warning ログ。

## 5. R5 2 stage 採否 gate

`stage1` = baseline 比較 (regression detector の結果):
- block_violations が空であれば pass

`stage2` = 絶対品質 (V6 acceptance):
- schema=100% / Q3≥0.80 / Q4≥0.80 / Q5≥0.70 / R10≥0.80 / parse_success≥0.90

両方 pass → V6 採用可。stage1 pass + stage2 fail → 候補は v5.19 と
同等以上だが V6 完了基準には未達 → Hold.

## 6. R5 候補 evaluation matrix

| Stage 1 | Stage 2 | Verdict |
|---------|---------|---------|
| pass | pass | **adopt** (採用、本番反映可) |
| pass | fail | **hold** (品質改善継続) |
| fail | (any) | **reject** (退行あり、採用不可) |

Codex W5 review #5 観点: over-recommendation / false-positive 増加
ケースは個別に reject 対象とする (regression 判定に含める)。

## 7. CLI

```bash
PYTHONPATH=dabs/app:. uv run python -m eval.regression_detector \
  --current eval/baselines/v6_w6_baseline.json \
  --baseline eval/baselines/v6_w5_baseline.json \
  --output eval/regression_summary/v6_w6.json
# exit 1 if block tier breached

# Stage gate (combined)
PYTHONPATH=dabs/app:. uv run python -m eval.stage_gate_runner \
  --current v6_w6_baseline \
  --baseline v6_w5_baseline \
  --acceptance docs/eval/v6_acceptance_policy.md
```

## 8. ファイル

```
eval/
├── regression_detector.py     # NEW (Day 2)
├── stage_gate_runner.py       # NEW (Day 5)
└── regression_summary/        # 出力先 (NEW)

docs/eval/
├── regression_detector_design.md  # 本 doc
└── v6_acceptance_policy.md         # NEW (Day 6)
```

## 9. Codex W5 指摘の取り込み

| Codex 指摘 | 対応 |
|-----------|------|
| skeleton method 分布計測 | Tier 3 info (Day 2 で計測実装) |
| over-recommendation 増加を不採用に | regression のひとつとして検知 (Day 4) |
| MERGE bypass を構造抽出に | Day 5 backlog or Week 7 (W6 では構造抽出までやらず) |
| Q4 citation lenient/strict 分離 | Day 3 で別列表示 |
| Q5 parse_failure penalty 0.15 検討 | Day 4 で実データ確認後に再校正 |

## 10. 後方互換

- `eval/regression_detector` は新規、既存 ab_runner / goldens_runner に
  影響しない
- `--against` 無指定なら baseline 必須 (失敗時に skip)

## 11. Day 別配分

| Day | 成果物 |
|-----|--------|
| 1 | この設計 doc + regression metric tier 確定 |
| 2 | regression_detector.py + skeleton method/compression 計測 |
| 3 | scorer 別 blocking + Q4 lenient/strict 分離 |
| 4 | R5 2-stage gate + Q5 penalty 再校正 |
| 5 | stage_gate_runner + MERGE/CREATE VIEW goldens |
| 6 | v6_acceptance_policy.md + 通し評価 |
| 7 | 最終 baseline + Codex review + W6 wrap |

## 参照

- `docs/eval/ab_runner_design.md`
- `docs/eval/r10_quality_addon_design.md`
- TODO.md V6 W6 引き継ぎ
- Codex W5 review (2026-04-25)
