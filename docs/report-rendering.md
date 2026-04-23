# レポート描画

大きな描画順序変更はない。現行 docs セットは v5.19.0 を対象とし、版履歴の詳細は `docs/README.md:35` を参照する。

## 描画の基本方針

- rule-based cards は `analysis.action_cards` として描画する。`dabs/app/core/usecases.py:289`
- LLM 独自案は `analysis.llm_action_cards` として別セクション描画する。`dabs/app/core/usecases.py:290`
- LLM merge 時点で group-overlap dedup 済みのため、renderer は重複排除ロジックを持たない。`dabs/app/core/usecases.py:293`

## version 注記

- v5.16.19 以降、preservation marker 前提の説明は obsolete。現在は group-overlap dedup が canonical。`dabs/app/core/usecases.py:287`
- v5.18.0 以降、federation query では suppress 済み cards のみが report に流れる。`dabs/app/core/analyzers/recommendations.py:42`
- v5.19.0 で card 種別は 22 枚になったが、report rendering のセクション構成自体は不変。registry 詳細は `docs/action-plan-generation.md:1` を参照。
