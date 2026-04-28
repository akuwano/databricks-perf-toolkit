# V6 A/B Runner Design (Week 4 Day 1)

R7 (prompt v5/v6 A/B 評価 runner) の設計ドキュメント。Codex W3.5 レビューで
指摘された 5 観点を反映する。

## 1. 目的

W3.5 までで V6 リファクタの flag/scorer/canonical-direct はすべて実装済。
Week 4 では **同一 goldens で「どの V6 設定が品質を上げるか」を機械的に
測定** できるようにする。

「品質を上げる」の定義は W1 rubric + W3 Q3 (5 指標) + W2.5 (recall/
hallucination/actionability/R4):

| 指標 | scorer | 期待方向 |
|------|--------|---------|
| R4 schema pass | r4_schema | up |
| L1 syntax | l1_syntax | up |
| Recall (strict, canonical-only) | recall.score_canonical_recall | up |
| Hallucination clean | hallucination.score_canonical_report_hallucination | up |
| Actionability | actionability.score_canonical_report_actions | up |
| Q3 composite | evidence_grounding.score_evidence_grounding | up |
| Q3 metric_grounded | (Q3 sub) | up |
| Q3 finding_support | (Q3 sub) | up |
| Q3 ungrounded_numeric | (Q3 sub) | down |

## 2. A/B 軸 (Codex 指摘 #3-1)

**4 条件を同一 29 cases で並走** する:

| Condition | env vars | 説明 |
|-----------|----------|------|
| `baseline` | (no V6 flag) | v5.19 同等 |
| `canonical-direct` | `V6_CANONICAL_SCHEMA=1` | LLM canonical JSON emit |
| `no-force-fill` | `V6_RECOMMENDATION_NO_FORCE_FILL=1` | 根拠不足省略可 |
| `both` | 上記 2 + 他の V6 flag (V6_REVIEW_NO_KNOWLEDGE / V6_REFINE_MICRO_KNOWLEDGE / V6_ALWAYS_INCLUDE_MINIMUM / V6_SKIP_CONDENSED_KNOWLEDGE) も on | 全 V6 経路 |

実装上は `goldens_runner.py::main()` を子プロセスで条件別に呼び出すか、
`os.environ` を一時的に切り替えて feature_flags.reset_cache() で再評価する。
**前者を採用** (テスト分離・並列化容易・キャッシュ汚染なし)。

## 3. 出力 (Codex 指摘 #3-2)

`eval/ab_summary/<run_name>.json` に以下を出力:

```json
{
  "run_name": "v6_w4_smoke",
  "generated_at": "2026-04-25T...",
  "cases_count": 29,
  "conditions": ["baseline", "canonical-direct", "no-force-fill", "both"],
  "metrics_per_condition": {
    "baseline":    {"q3_composite_avg": 0.6458, "metric_grounded_avg": 0.45, ...},
    "canonical-direct": {...},
    ...
  },
  "case_diff": {
    "spill_heavy_q1": {
      "baseline":    {"q3_composite": 0.50, "metric_grounded": 0.30, "verdict": null},
      "canonical-direct": {"q3_composite": 0.85, "metric_grounded": 0.80, "verdict": "improved"},
      "delta_to_baseline": {"q3_composite": +0.35, "metric_grounded": +0.50}
    },
    ...
  },
  "regression_summary": {
    "canonical-direct": {"improved": 18, "regressed": 2, "unchanged": 9},
    "no-force-fill":    {"improved": 12, "regressed": 1, "unchanged": 16},
    "both":             {"improved": 22, "regressed": 1, "unchanged": 6}
  },
  "canonical_parse_failure_rate": {
    "canonical-direct": 0.07,
    "both": 0.03
  }
}
```

加えて markdown 要約 (`eval/ab_summary/<run_name>.md`) も emit。

## 4. case 別 regression 判定 (Codex 指摘 #3-2)

per-case verdict:
- `improved`: 主要指標 (q3_composite OR metric_grounded OR finding_support)
  が baseline 比で **絶対値 +5pt 以上** 上がった
- `regressed`: いずれかの主要指標が baseline 比で **絶対値 -5pt 以上** 下がった
- `unchanged`: ±5pt 以内

判定優先順位: regressed > improved > unchanged (1 つでも regressed があれば regressed)。

## 5. canonical parse failure 計測 (Codex 指摘 #3-5)

Q3 baseline では `canonical_report_llm_direct` が None のとき normalizer
fallback している。Codex 指摘:
> parse 失敗を silent fallback にせず、A/B runner では失敗率として表に出す

実装:
- `goldens_runner.py` で各 case に `canonical_source: "llm_direct" | "normalizer_fallback"` を記録
- ab_runner で集計し `canonical_parse_failure_rate` を出力

## 6. 複合 gate (Codex 指摘 #3-3)

Week 4 完了時の合格判定 (`--gate-w4-completion` 等):

```
PASS iff:
  q3_composite >= 0.80
  AND metric_grounded >= 0.70
  AND finding_support >= 0.80
  AND ungrounded_numeric <= 0.15
  AND recall_strict >= 0.50
  AND hallucination >= 0.85
  AND schema_pass >= 1.00
  AND case_regressions <= 1
  AND canonical_parse_failure_rate <= 0.05
```

`eval/ab_runner.py` に `--gate-w4-completion` flag。
個別 gate は既存 `goldens_runner.py` のものを継承。

## 7. R10 品質評価 add-on (Codex 指摘 #3-4)

R10 は ab_runner の **後段集約 layer**:

| Layer | 内容 | 実装 |
|-------|------|------|
| Layer A — deterministic | R4 / L1 / Q3 / recall / actionability / hallucination の集計 | `eval/scorers/r10_quality.py` |
| Layer B — LLM judge | L3/L4 (既存 l3l4_judge.py) を condition 別に呼ぶ | (W4 では設計のみ) |

Codex 推奨: deterministic と LLM judge は **混ぜない**。どちらが落としたか
分かるようにレポートで分離。

## 8. LLM API 不要モード

Week 4 では DATABRICKS_HOST/TOKEN 無しでも:
- 4 条件を rule-based で並走
- 全条件で normalizer adapter 経由 → 全 condition でほぼ同じ結果

これは Week 4 の I/F 検証用。LLM API が利用可能になった環境では
`--enable-llm` で実 LLM を呼ぶ。

## 9. ファイル構成

```
eval/
├── ab_runner.py              # A/B 4 条件実行 (NEW)
├── ab_summary/               # 出力先 (NEW、gitignore 検討)
│   ├── v6_w4_smoke.json
│   └── v6_w4_smoke.md
├── goldens_runner.py         # 既存 (case 単位 runner)
├── scorers/
│   ├── r10_quality.py        # NEW (Day 5)
│   └── (既存)
└── tests/
    └── test_v6_ab_runner.py  # NEW
```

## 10. Day 別配分

| Day | 成果物 |
|-----|--------|
| 1 | この設計 doc |
| 2 | `ab_runner.py` 実装 + canonical_source 記録 + rule-based smoke |
| 3 | per-case regression detection + JSON 出力 + tests |
| 4 | `r10_quality.py` 設計 + deterministic 集約ルール |
| 5 | `r10_quality.py` 実装 + ab_runner 統合 |
| 6 | CLI ラッパー + README + 複合 gate |
| 7 | rule-based 4 条件 baseline + Codex 確認 + W5 引き継ぎ |

## 11. 後方互換

- `goldens_runner.py` は変更しない (canonical_source 列追加のみ追記互換)
- `ab_runner.py` は新規、既存 CI から切り離し
- LLM 込みの場合のみ意味のある条件 (canonical-direct 等) は rule-based では
  pass-through (parse failure 100% で記録)

## 参照

- `docs/v6/knowledge_inventory.md`
- `docs/knowledge/v6_knowledge_policy.md`
- `eval/goldens_runner.py` (W4 で statistics-only 拡張)
- TODO.md V6 W4 引き継ぎ
- Codex W3.5 review (2026-04-25)
