# LLM ステージ

`docs/README.md:35` を版履歴の単一ソースとして参照。本書では現行の LLM 呼び出し面だけを記述する。

## 構成

| ステージ | 主な prompt | 用途 |
|---|---|---|
| Stage 1 | `create_structured_analysis_prompt` | 初回 structured analysis |
| Stage 2 | review prompt | Stage 1 のレビュー |
| Stage 3 | refine prompt | review を反映した最終 structured analysis |
| 専用 | `create_clustering_prompt` | LC 候補推薦 |
| 専用 | rewrite prompts | optimized SQL / SQL rewrite |

## Federation thread (v5.18.0)

`run_analysis_pipeline` は `QueryMetrics.is_federation_query` を `is_federation` thread として prompt builder に渡す。`dabs/app/core/usecases.py:188`

- Stage 1 日本語 prompt は `is_federation` のとき `_federation_constraints_block("ja")` を追加する。`dabs/app/core/llm_prompts/prompts.py:2231`
- Stage 1 英語 prompt も同様に `_federation_constraints_block("en")` を追加する。`dabs/app/core/llm_prompts/prompts.py:2295`
- 制約 block は Databricks 側 tuning ではなく pushdown・remote-side aggregation・fetch size・source 固有 advice へ寄せる。`dabs/app/core/llm_prompts/prompts.py:937`

## LLM merge の現行挙動

Stage 3 までの最良出力は `refined_analysis or llm_analysis` で選ばれ、`_merge_llm_action_plan` が rule-based cards と LLM cards を group-overlap で dedup する。`dabs/app/core/usecases.py:284`

- rule-based cards は `analysis.action_cards` に保持。`dabs/app/core/usecases.py:289`
- LLM 独自案は `analysis.llm_action_cards` に保持。`dabs/app/core/usecases.py:290`
- overlap 判定は `groups_overlap` を使う。`dabs/app/core/usecases.py:307`, `dabs/app/core/action_classify.py:155`

## LC recommendation prompt

`create_clustering_prompt` は v5.16.23 以降、長い SQL body を LLM に渡さない。structured inputs のみで判断させる。`dabs/app/core/llm_prompts/prompts.py:2788`

- 候補は `candidate_columns_with_context` を含む operator metadata から供給する。`dabs/app/core/analyzers/recommendations.py:285`
- notable shuffle key があると `shuffle_lc` / LC 候補として評価される。`dabs/app/core/llm_prompts/prompts.py:2793`
- `OPTIMIZE FULL` 必須の HC / LC DDL 注意も prompt に含まれる。`dabs/app/core/llm_prompts/prompts.py:2994`

## Optimized SQL prompt の追加ルール (v5.16.22)

複数候補を Option A/B/C 形式で出す場合、各 option に最低 1 行の具体 SQL を必ず含める。空ボディは禁止。`dabs/app/core/llm_prompts/prompts.py:1832`, `dabs/app/core/llm_prompts/prompts.py:1858`

## legacy prompt の扱い

非構造化の旧 `create_analysis_prompt` は現行 pipeline からは使われない。現行は structured prompts が正規経路である。`docs/analysis-pipeline.md:1` を参照。
