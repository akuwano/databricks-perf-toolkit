# Report Quality Rubric (V6)

V6 リファクタリングの **評価基準ドキュメント**。Week 1 で固定した品質定義をここに集約し、以後のすべての scorer / golden case / A/B 評価はこの rubric を参照する。

## 1. 5つの品質指標

| 指標 | 定義 | 目標値 |
|------|------|--------|
| **L1-L4 総合スコア** | L1 形式 / L2 事実整合 / L3 診断妥当性 / L4 意思決定価値の加重平均 | 4段階で平均 3.3 以上 |
| **Hallucination 率** | 入力 (profile JSON / SQL / metrics) に根拠がない断定、または DBSQL ナレッジに反する記述の割合 | < 5% |
| **Action 具体性** | 推奨アクションが対象 SQL / 設定 / 変更手順 / 期待効果 を含む割合 | ≥ 80% |
| **Critical issue recall** | golden case の `must_cover_issues` を見逃さずレポートに含めた割合 | ≥ 85% |
| **Regression 率** | 前版より評価スコアが下がった golden case の割合 (A/B 比較) | < 10% |

## 2. L1-L4 の定義と採点基準

### L1: Format / Schema (1-5)

レポートの構造的妥当性を評価。

| Score | 基準 |
|-------|------|
| 5 | 全 section / 必須フィールドが揃い、JSON / Markdown が valid |
| 4 | 1-2 個の任意フィールド欠落、構造的には valid |
| 3 | 必須 section のうち 1 個が欠落、または markdown 構造が部分的に崩れている |
| 2 | 複数の必須 section 欠落、または JSON parse エラーあり |
| 1 | レポートとして読めない / schema 違反多数 |

### L2: Evidence Grounding (1-5)

レポート内の claim が入力データで裏付けられているかを評価。

| Score | 基準 |
|-------|------|
| 5 | 全 claim が profile / SQL / metrics の具体値で裏付けられ、引用が正確 |
| 4 | 1-2 個の claim で引用がやや曖昧 (具体値の代わりに「高い」等の表現) |
| 3 | 複数の claim で引用なし、または推測ベースの記述あり |
| 2 | hallucination 1 個以上 (入力にない数値や事象を断定) |
| 1 | 半分以上が hallucination または明確に誤った参照 |

### L3: Diagnosis Quality (1-5)

ボトルネック判定・原因推定・優先順位付けの妥当性を評価。

| Score | 基準 |
|-------|------|
| 5 | golden case の must_cover_issues すべてに正しい severity で言及 |
| 4 | must_cover_issues に言及するが severity が 1 段階ずれている |
| 3 | must_cover_issues のうち 1 個を見逃し、または致命的でない誤診断 |
| 2 | must_cover_issues の 2 個以上を見逃し、または致命的な誤診断 (例: federation で LC 推奨) |
| 1 | 重要 issue を全く拾えていない、または must_avoid_diagnosis に該当 |

### L4: Decision Value / Actionability (1-5)

レポートを読んだエンドユーザーが次に何をすべきか判断できるかを評価。

| Score | 基準 |
|-------|------|
| 5 | 全推奨が「対象 / 何を / なぜ / どう / 期待効果 / 検証方法」を含み、即実行可能 |
| 4 | 大半の推奨が実行可能、1-2 個に「期待効果」or 「検証方法」欠落 |
| 3 | 推奨の半分が抽象的 (例: 「クラスタリングを検討」のみで対象列なし) |
| 2 | 推奨の大半が抽象的、具体的な SQL / 設定値が不足 |
| 1 | 推奨が「最適化してください」レベルで実行不可能 |

## 3. Hallucination の検出基準

以下のいずれかに該当した場合 hallucination 1 件としてカウント:

- 入力 profile JSON / SQL / metrics に存在しない **数値の断定** (例: profile になのに「shuffle 5GB」と書く)
- 入力にない **テーブル名 / 列名** の言及
- DBSQL の事実に反する記述 (例: 「Serverless で broadcast hint を強制」など serverless で機能しない設定)
- golden case の `forbidden_claims` リストに該当する記述

`eval/scorers/hallucination.py` (Day 4 で stub) で機械検出 + LLM-as-judge ハイブリッドで実装する。

## 4. Action 具体性の評価軸

各推奨 ActionCard を以下 6 要素で採点。すべて埋まっていれば「具体的」と判定:

| 要素 | 例 |
|------|-----|
| **対象** | 「fact_orders.customer_id 列」 |
| **何を** | 「BIGINT に型変更」 |
| **なぜ** | 「join key の暗黙 CAST が発生しているため」 |
| **どう** | `ALTER TABLE fact_orders ALTER COLUMN customer_id TYPE BIGINT;` |
| **期待効果** | 「Photon 化により実行時間 30-50% 短縮見込み」 |
| **検証方法** | 「EXPLAIN で CAST が消えていること、duration 比較」 |

6 要素のうち 5 個以上を満たせば +1、4 個以下なら 0。N 件の推奨のうち +1 の割合 = Action 具体性スコア。

## 5. Critical Issue Recall の判定

各 golden case には `must_cover_issues: [list]` を定義しておき、レポートがすべての issue に言及していれば recall=1.0、半分なら 0.5。

```yaml
# eval/goldens/cases/spill_heavy.yaml の例
must_cover_issues:
  - id: spill_dominant
    severity: high
    keywords: [spill, memory, peak_memory]
  - id: shuffle_skew
    severity: medium
    keywords: [skew, partition]
```

scorer が レポートに `keywords` のいずれかが含まれているかで一次判定 (mechanical) し、曖昧ケースは LLM-as-judge へ回す。

## 6. Regression 率の判定

A/B 比較 (Week 4) で baseline と candidate を同一 golden case で実行し、各 case の総合スコア (L1-L4 + 上記 4 指標の加重) を計算。candidate が baseline より下がった case 数 / 全 case 数 = regression 率。

閾値:
- regression > 10%: 採用しない
- 5% < regression <= 10%: 手動レビュー
- regression <= 5%: 採用可

## 7. scorer ↔ rubric の対応 (Week 1 Day 4 で確定)

| Rubric 項目 | Scorer ファイル | 種別 |
|-------------|----------------|------|
| L1 Format | `eval/scorers/l1_syntax.py` (既存) | mechanical |
| L2 Evidence | `eval/scorers/l2_evidence.py` (既存) | mechanical + LLM judge |
| L3 Diagnosis | `eval/scorers/l3l4_judge.py` (既存) | LLM-as-judge |
| L4 Actionability | `eval/scorers/l3l4_judge.py` (既存) | LLM-as-judge |
| Hallucination | `eval/scorers/hallucination.py` (新規 Day 4) | mechanical + LLM judge |
| Action 具体性 | `eval/scorers/actionability.py` (新規 Day 4) | mechanical |
| Critical issue recall | `eval/scorers/recall.py` (新規 Day 4) | mechanical |
| Regression 率 | `eval/diff_judge.py` (既存、Week 6 で再設計) | aggregation |

## 8. 採点フロー

```
Profile JSON + golden case manifest
    ↓
Run analysis pipeline (current or candidate)
    ↓
Generated report (Markdown + ActionCard JSON)
    ↓
Apply scorers in parallel:
    - L1 (mechanical)
    - L2 (mechanical + judge)
    - L3, L4 (LLM judge)
    - Hallucination (mechanical + judge)
    - Actionability (mechanical)
    - Recall (mechanical)
    ↓
Aggregate to per-case score + report
    ↓
A/B compare (Week 4) → Regression detection (Week 6)
```

## 9. 改訂履歴

| Date | Author | Change |
|------|--------|--------|
| 2026-04-25 | V6 Week 1 Day 1 | Initial rubric, 5 indicators + L1-L4 1-5 scoring |

---

## 参考

- TODO.md `### v6.0 — レポート品質向上リファクタリング`
- `eval/scorers/l1_syntax.py:1`, `l2_evidence.py:1`, `l3l4_judge.py:1` (既存実装)
- Codex レビュー (2026-04-25): 「P0 は品質を測る・決める・直接上げるタスクに限定」
