# Scorer ↔ Rubric Mapping (V6 Week 1 Day 4)

`docs/eval/report_quality_rubric.md` で定義した品質指標と、`eval/scorers/` 配下の実装の対応表。

## 対応表

| Rubric項目 | Scorer ファイル | 関数 | 種別 | 入力 | 出力 |
|------------|----------------|------|------|------|------|
| L1 Format/Schema | `eval/scorers/l1_syntax.py` | `score_l1(card, is_serverless)` | mechanical | ActionCard | `L1Score` |
| L2 Evidence Grounding | `eval/scorers/l2_evidence.py` | `score_l2(card, ...)` | mechanical + LLM judge | ActionCard + profile | `L2Score` |
| L3 Diagnosis Quality | `eval/scorers/l3l4_judge.py` | `judge_l3_l4(...)` | LLM-as-judge | ActionCard + profile + SQL | `L3Score` |
| L4 Actionability (LLM) | `eval/scorers/l3l4_judge.py` | `judge_l3_l4(...)` | LLM-as-judge | ActionCard + profile + SQL | `L4Score` |
| Hallucination | `eval/scorers/hallucination.py` | `score_hallucination(card, forbidden_claims, profile_evidence)` | mechanical (Week 1) → mechanical + LLM (Week 3+) | ActionCard + golden case | `HallucinationScore` |
| Action 具体性 (Q4) | `eval/scorers/actionability.py` | `score_actionability(card)` | mechanical | ActionCard | `ActionabilityScore` |
| Critical issue recall | `eval/scorers/recall.py` | `score_recall(report_text, cards, must_cover_issues)` | mechanical | report + golden case | `RecallScore` |
| Regression率 | `eval/diff_judge.py` (Week 6 で再設計) | `judge_diff(...)` | aggregation | 2つの eval result | (Week 6) |

## 採点フロー

```
Profile JSON + Golden Case (eval/goldens/cases/<case>.yaml)
    ↓
Run analysis pipeline (current branch or candidate)
    ↓
Generated report (Markdown) + ActionCards (JSON)
    ↓
┌──────────────────┬──────────────────┬───────────────────┐
│ Mechanical       │ Mechanical+LLM   │ LLM-as-judge      │
├──────────────────┼──────────────────┼───────────────────┤
│ score_l1         │ score_l2         │ judge_l3_l4       │
│ score_actionability │ score_hallucination │                │
│ score_recall     │                  │                   │
└──────────────────┴──────────────────┴───────────────────┘
    ↓
QueryEvalResult (per-query aggregation)
    ↓
EvalReport (across all golden cases)
```

## Week 1 で動作する範囲

- **Mechanical scorers (L1 + Q4 + recall)**: スタンドアロンで実行可能
- **L2 Evidence (mechanical部分)**: 動作
- **Hallucination (Week 1 stub)**: forbidden_claims だけチェック、profile_evidence integration は Week 3 (R6 + Q3)
- **L3/L4 LLM judge**: LLM API キー設定時に動作

## Week 2-6 で拡張する範囲

| Week | 拡張対象 | 内容 |
|------|---------|------|
| Week 2 | L1 拡張 | R4 canonical schema 違反検知を追加 |
| Week 3 | Hallucination 強化 | profile_evidence (Q3 evidence grounding) と統合、LLM judge 追加 |
| Week 4 | aggregation | A/B runner で同じ goldens に対する before/after 比較 |
| Week 5 | actionability 拡張 | Q4 action template 標準形 (R8 SQL skeleton 連携) |
| Week 6 | Regression | diff_judge.py を rubric ベースに再設計 |

## 既存 scorer の保証

Day 4 時点で **既存の `score_l1` / `score_l2` / `judge_l3_l4` は変更していない**。新規 scorer は `eval/scorers/__init__.py` に追加で公開。

## CLI 統合 (Week 1 Day 5-6 で実装予定)

```bash
# 現状 (Day 4 時点): 既存 runner は L1-L4 のみ実行
PYTHONPATH=dabs/app:. uv run python -m eval eval/fixtures/

# Day 5-6 後: goldens manifest 経由で全 scorer 実行
PYTHONPATH=dabs/app:. uv run python -m eval --goldens eval/goldens/manifest.yaml --baseline
```

## 参照

- `docs/eval/report_quality_rubric.md` — 品質指標の正本
- `eval/scorers/__init__.py` — エクスポート一覧
- `eval/models.py` — Score dataclass 定義
- `eval/goldens/README.md` — golden case schema
