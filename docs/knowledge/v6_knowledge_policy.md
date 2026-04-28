# V6 Knowledge Injection Policy (Week 3 Day 2)

V6 リファクタの **R6 (knowledge 注入整理)** で適用する公式ポリシー。
`docs/v6/knowledge_inventory.md` の棚卸しと、Codex 指摘 (TODO.md
「LLM プロンプト精度向上」+ 2026-04-25 W3 kickoff) を踏まえて確定する。

## 1. 目的

「**LLM が削減された情報で正しく答えること**」を品質ゲートにする。
余計な knowledge を入れて LLM が形式 (recommendation block) に合わせ込み、
実は profile 由来の根拠を見失っているのが現状の主因。Codex 指摘:

- Phase 2 review / Phase 3 refine から knowledge を抜いた方が **誤 reject が減る**
- ALWAYS_INCLUDE で常時注入される `spark_params` / `appendix` は **scope creep の温床**
- Recommendation format block (`prompts.py:23-94`) が **「全項目埋める」圧力**で
  根拠薄い SQL 予測を捻出している

## 2. 段階別 budget (Codex 推奨閾値)

| Phase | 目標 budget | 内容 |
|-------|-------------|------|
| **analysis** | **8-15 KB** | 主要 issue category に紐づく section のみ。appendix / 長い examples は除外 |
| **review** | **0 KB (理想) / 最大 2 KB** | 原則として knowledge 不要。review は Fact + format + evidence consistency のみ |
| **refine** | **0-4 KB** | review の findings に対する micro-knowledge のみ (evaluator が `requires_knowledge_check: true` を返した section に限定) |
| **report review / refine** | analysis と同じ | (同様の規律) |

`load_tuning_knowledge()` は約 30 KB 弱を返すので、analysis ですら
**半分以上は捨てる前提**で組み込む。

## 3. 注入ステージごとのルール

### Stage 1: Analysis (`core/llm.py:87`)

- `filter_knowledge_for_analysis()` を維持
- ALWAYS_INCLUDE は **`bottleneck_summary` のみ** に絞る (W3 #3)
  - `spark_params` は serverless で半分以上が無効化されるので除外
  - `appendix` は scope 大きく hallucination 誘発、除外
- 上限 budget = **15 KB**。超過時は priority_score 下位から trim
- 各 section に `knowledge_source_id` を付与し、引用可能な ID で渡す

### Stage 2: Review (`core/llm.py:297`)

- **knowledge を一切入れない (default)**
- review prompt は以下のみで実行:
  - canonical Report (analysis output)
  - Fact Pack summary (top alerts + dominant operations)
  - 評価軸 (evidence grounding / format / consistency)
- `feature_flags.v6_review_no_knowledge=true` で挙動切替、当面は
  shadow run で v5 と並走

### Stage 3: Refine (`core/llm.py:349`)

- knowledge は **review が指定した section のみ** に限定 (max 4 KB)
- review が `requires_knowledge_check: [section_id, ...]` を返した
  場合のみ、その section だけを refine prompt に注入
- 全 finding に knowledge を注ぐ既存挙動は廃止

## 4. ALWAYS_INCLUDE 改修ルール (W3 #3)

`core/llm_prompts/knowledge.py:199`:

```python
# Before
ALWAYS_INCLUDE_SECTION_IDS = ["bottleneck_summary", "spark_params", "appendix"]

# After (W3)
ALWAYS_INCLUDE_SECTION_IDS = ["bottleneck_summary"]
```

- `spark_params`: serverless workspace で多くが SET 不可 / W2.5 で
  Action.preconditions が representation を持つので、必要な spark conf
  推奨は finding-driven にする
- `appendix`: knowledge 自体に matrix 衝突 (classic vs serverless
  autoBroadcast、Codex #7) があり、appendix 全文投入は誤情報誘発

## 5. Recommendation format block の圧力緩和 (W3 #8)

`core/llm_prompts/prompts.py:23-94` の "全項目を埋める" 体裁を以下に
ゆるめる:

- `expected_effect_quantitative` は **明確な数値根拠がある場合のみ** 出力
- `verification` は **profile に出てきた metric を引用** できない場合は省略可
- 文末に `根拠不足のフィールドは省略してください — 推測で埋めない` を
  明示

W2.5 で追加した canonical schema の `impact_confidence: needs_verification`
が出るパスを LLM に見せ、「数値が出せない場合の正解形」を提示する。

## 6. 注入経路の重複削除 (W3 #4)

- `_summarize_sections_with_llm()` (`knowledge.py:474-492`) は
  v6 では呼ばない (二次生成で矛盾を増やすリスク)
- `feature_flags.v6_skip_condensed_knowledge=true` で停止

## 7. Knowledge matrix 衝突解消 (W3 #7)

`dbsql_tuning_en.md` の以下を分割:
- L230-242: classic / pro 向け autoBroadcastJoinThreshold
- L1083-1104: serverless 不可

→ `serverless` セクションには serverless 専用パラメータのみ、
  `dbsql_advanced` (classic/pro 向け) は autoBroadcast 等を残す。
  本格的な分割は Week 3 後半 or Week 4 に回す (今週は inventory のみ)。

## 8. Knowledge source id 引用ルール (Q3 連携)

canonical Report の `Evidence.source` に以下の値を許可:

| source 形式 | 例 |
|-------------|-----|
| `profile.<path>` | `profile.queryMetrics.spill_to_disk_bytes` |
| `node[<id>].<path>` | `node[12].operator_stats.peak_memory` |
| `alert:<category>` | `alert:memory` |
| `actioncard.evidence` | (legacy adapter のみ) |
| `knowledge:<section_id>` | `knowledge:spill` |
| `synthetic` | (合成、Q3 で grounded=false 強制) |

`knowledge:<section_id>` の `<section_id>` は `parse_knowledge_sections()`
が認識する実在 ID と一致しなければ Q3 scorer で減点。

## 9. v6 feature flag 階層 (W3 Day 3 で実装)

| flag | default | 意味 |
|------|---------|------|
| `v6_canonical_schema` | off | LLM prompt が canonical Finding/Action JSON を直接 emit |
| `v6_review_no_knowledge` | off | Stage 2 review で knowledge 注入を skip |
| `v6_refine_micro_knowledge` | off | Stage 3 refine は review 指定 section のみ (max 4 KB) |
| `v6_always_include_minimum` | off | ALWAYS_INCLUDE を `bottleneck_summary` のみに |
| `v6_skip_condensed_knowledge` | off | `_summarize_sections_with_llm()` を呼ばない |
| `v6_recommendation_no_force_fill` | off | format block で根拠不足時の省略を許可 |

すべて off の状態が v5.19 と同等動作。`v6_canonical_schema=on` だけで
最低限の V6 経路が動く。

## 10. Day 4 実装順 (本ポリシーを実コードに反映)

1. `core/feature_flags.py` 新設、上記 6 flag を env / runtime-config 経由で読み込む (Day 3)
2. `ALWAYS_INCLUDE_SECTION_IDS` を flag 切替 (Day 4)
3. `core/llm.py:297` review から knowledge 引数 None 化 (flag 切替)
4. `core/llm.py:349` refine を review.requires_knowledge_check 駆動に
5. `prompts.py:23-94` recommendation format に "根拠不足は省略" 文言追加
6. `_summarize_sections_with_llm` 呼び出しを flag で skip
7. テスト: 各 flag で baseline 取得、shadow run で v5 と差分確認

## 11. 評価 (Q3 で測る、Day 5 実装)

- evidence_citation_coverage (Codex Q3 #1)
- ungrounded_numeric_claim_rate (Codex Q3 #2)
- valid_source_taxonomy_rate (Codex Q3 #3)
- valid_knowledge_section_id_rate (Codex Q3 #4)
- finding_grounded_support_ratio (Codex Q3 #5)

Week 3 完了時の目標:
- evidence_citation_coverage ≥ 80%
- valid_source_taxonomy_rate ≥ 95%
- finding_grounded_support_ratio ≥ 90%
- (LLM 込み baseline で) hallucination clean ≥ 0.85

## 12. 後方互換 / Rollback

- すべての flag を off にすれば v5.19 同等動作 (回帰なし)
- v6 flag を on で動かしている間は **canonical Report と既存
  ActionCard の両方を生成** (Codex 地雷 #5 への対応)
- 1 週間 shadow run で eval baseline を v5 と並走、品質低下が無いか
  確認してから本番 default を on にする (Week 4-5)

## 参照

- `docs/v6/knowledge_inventory.md` (Week 3 Day 1)
- TODO.md「LLM プロンプト精度向上」(Codex 2026-04-19)
- `docs/v6/output_contract.md` §8 source taxonomy
- `dabs/app/core/v6_schema/issue_registry.py` (issue_id 30件)
