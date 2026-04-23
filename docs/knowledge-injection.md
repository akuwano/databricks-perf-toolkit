# Knowledge Injection

`docs/README.md:35` の版履歴を参照。本書では section_id と alert category の現行対応だけを整理する。

## ナレッジソース

現行の DBSQL tuning knowledge は日英 2 ファイルで管理される。

- 日本語: `dabs/app/core/knowledge/dbsql_tuning.md:1`
- 英語: `dabs/app/core/knowledge/dbsql_tuning_en.md:1`

## 主要 section_id

| section_id | 概要 | 追加版 |
|---|---|---|
| `lc_shuffle_key_candidate` | shuffle key → LC candidate | v5.16.10 |
| `compilation_overhead` | compile ratio 高 | v5.16.25 |
| `driver_overhead` | queue / scheduling / driver wait | v5.16.25 |
| `cluster_underutilization` | warehouse idle 気味 / effective parallelism 低 | v5.19.0 |
| `compilation_absolute` | absolute compile heavy advisory | v5.19.0 |
| `federation` | Lakehouse Federation 向け guidance | v5.18.0 |

根拠: `section_id` コメント。`dabs/app/core/knowledge/dbsql_tuning.md:484`, `dabs/app/core/knowledge/dbsql_tuning.md:1050`, `dabs/app/core/knowledge/dbsql_tuning.md:1102`, `dabs/app/core/knowledge/dbsql_tuning.md:1148`, `dabs/app/core/knowledge/dbsql_tuning.md:1189`, `dabs/app/core/knowledge/dbsql_tuning.md:1474`

## alert category → section_ids

現行 docs では category 名の説明責務を持ち、版履歴の厳密な追加時期は `docs/README.md:35` に集約する。

| alert / root cause | 主に参照する section_ids |
|---|---|
| shuffle / LC write overhead | `lc_shuffle_key_candidate` |
| compile-heavy | `compilation_overhead`, `compilation_absolute` |
| driver-side overhead | `driver_overhead`, `cluster_underutilization` |
| federation | `federation` |
| HC / LC | `lc_shuffle_key_candidate` と既存 LC / HC sections |

## 新規 section の内容メモ

- `lc_shuffle_key_candidate` は GiB 級 shuffle key を LC 候補として扱う。`dabs/app/core/knowledge/dbsql_tuning.md:484`
- `compilation_overhead` は compile ratio が高いケースを扱う。`dabs/app/core/knowledge/dbsql_tuning.md:1050`
- `driver_overhead` は queue / scheduling / compute wait を扱う。`dabs/app/core/knowledge/dbsql_tuning.md:1102`
- `cluster_underutilization` は `effective_parallelism` と 3 variants を説明する。`dabs/app/core/knowledge/dbsql_tuning.md:1148`
- `compilation_absolute` は ratio は小さいが absolute compile が重い advisory を扱う。`dabs/app/core/knowledge/dbsql_tuning.md:1189`
- `federation` は pushdown、remote-side pre-aggregation、source 側最適化を扱う。`dabs/app/core/knowledge/dbsql_tuning.md:1474`

## ルーティング上の補足

- federation query のときは prompt 側でも `_federation_constraints_block` が入り、knowledge と LLM 方針が一致する。`dabs/app/core/llm_prompts/prompts.py:2231`
- `covered_root_cause_groups` のような未導入フィールドは現行 docs には記載しない。
