# R10 Quality Evaluation Add-on Design (Week 4 Day 4)

V6 リファクタの **R10** タスク (post-analysis 品質評価 add-on) の設計。
Codex W3.5 review (§3-4) の指摘:

> R10 add-on は judge 依存を分離。deterministic scorer と LLM judge を
> 混ぜず、どちらが落としたか分かるレポートにする。

## 1. 目的

ab_runner で 4 条件の数値が出るようになったが、それぞれ:
- L1 syntax pass
- L2 evidence
- R4 schema
- Q3 composite + 5 sub-signal
- recall (lenient + strict)
- hallucination
- actionability
- canonical_parse_failure

**8+5=13 個の指標** を扱っている。R10 はこれを「単一の品質スコア + 主要 finding 一覧」に集約する add-on。

## 2. 設計原則 (Codex 指摘反映)

| 原則 | 内容 |
|------|------|
| **Layer 分離** | Layer A = deterministic、Layer B = LLM judge を別 module、別 entry |
| **混ぜない** | R10 final score は両 Layer の *別々* スコア + final aggregate |
| **explainable** | どの sub-score がどれだけ落としたかを reasons[] で出力 |
| **idempotent** | 同じ canonical Report を渡せば毎回同じ deterministic score |

## 3. スコア構造

```python
@dataclass
class R10QualityScore:
    # ----- Layer A: deterministic (本Day 5で実装) -----
    schema_pass: bool                   # R4 1.0 or 0.0
    actionability_specific: float       # 0..1
    recall_strict: float                # 0..1
    hallucination_clean: float          # 0..1
    q3_composite: float                 # 0..1
    q3_finding_support: float           # 0..1
    q3_metric_grounded: float           # 0..1
    q3_ungrounded_numeric: float        # 0..1 (lower better)
    canonical_parse_ok: bool            # extracted llm_direct 否
    layer_a_score: float                # 0..1 weighted avg
    layer_a_reasons: list[str]          # "Q3 finding_support 45% < 80%" 等

    # ----- Layer B: LLM judge (Day 5 では None placeholder) -----
    layer_b_score: float | None = None
    layer_b_reasons: list[str] = []

    # ----- Aggregate -----
    overall_score: float                # 0..1
    overall_verdict: str                # pass / borderline / fail
    overall_reasons: list[str]
```

## 4. Layer A 重み (Codex W3.5 で固定した Q3 重みを継承)

| 指標 | 重み | 由来 |
|------|------|------|
| q3_composite | 0.30 | 既に内部で 5 sub の重み付け |
| recall_strict | 0.20 | false negative 検出 |
| hallucination_clean | 0.20 | false positive 検出 |
| actionability_specific | 0.15 | rubric L4 |
| schema_pass | 0.10 | R4 形式 |
| canonical_parse_ok | 0.05 | LLM-direct 経路品質 |
| **合計** | **1.00** | |

## 5. Layer A verdict 閾値

| Verdict | layer_a_score | 説明 |
|---------|-------------|------|
| `pass` | ≥ 0.80 | Week 4 完了基準 |
| `borderline` | 0.60-0.80 | レビュー必要 |
| `fail` | < 0.60 | 採用しない |

## 6. Layer B (LLM judge) — 設計のみ、Day 5+で実装

Codex 指摘: L3 (診断妥当性) / L4 (actionability) は LLM-as-judge が必要。
既存 `eval/scorers/l3l4_judge.py` を condition 別に呼び、
- Layer B score = (L3_avg + L4_avg) / 2 / 5 (1-5 scale)
- 別 dataclass で保持、final aggregate で 0.5/0.5 mix

Week 4 では Layer B は **placeholder のみ**。LLM API キーが必要なため。

## 7. Final aggregate

```
overall_score = layer_a_score              (Layer B が None)
              | (layer_a_score + layer_b_score) / 2  (Layer B が値あり)

overall_verdict:
  pass      iff overall_score >= 0.80
  borderline iff 0.60 <= overall_score < 0.80
  fail      iff overall_score < 0.60
```

## 8. reasons[] の生成ルール

各 Layer の reasons は「閾値を割った指標」をリスト化:

```
"Q3 finding_support 45% < target 80%"
"recall_strict 18% < target 50%"
"actionability_specific 60% < target 80%"
```

ab_runner の markdown report に condition 別に reasons を表示 →
品質劣化原因が一目で分かる。

## 9. ab_runner との統合 (Day 5 で実装)

`ab_runner._build_summary()` の出力に:
```json
{
  "r10_per_condition": {
    "baseline": {"layer_a_score": 0.65, "verdict": "borderline", "reasons": [...]},
    "canonical-direct": {...},
    ...
  }
}
```

`--gate-r10` flag で `overall_verdict != "pass"` のとき exit 1。

## 10. ファイル

```
eval/scorers/r10_quality.py    # Day 5 実装
eval/tests/test_v6_r10.py      # Day 5
docs/eval/r10_quality_addon_design.md  # 本 doc
```

## 11. 後方互換

R10 は ab_runner の **追加メトリクス**。既存の goldens_runner CI gate
や Q3 個別 gate は変更しない。

## 参照

- `docs/eval/ab_runner_design.md`
- `docs/eval/scorer_mapping.md`
- TODO.md V6 W4 Day 4
- Codex W3.5 review §3-4
