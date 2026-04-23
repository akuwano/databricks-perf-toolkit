# Action Plan 生成

`docs/README.md:35` の版履歴を参照。現行の rule-based ActionCard は registry 22 枚で固定される。

## Registry ベース設計

`recommendations_registry.py` は `CardDef` と `CARDS` tuple を使う Spark Perf-style static-priority registry である。priority 順に `detect(ctx)` → `build(ctx)` を回し、host 側で suppression / LC LLM / serverless filter を追加する。`dabs/app/core/analyzers/recommendations_registry.py:3`, `dabs/app/core/analyzers/recommendations.py:24`

## 22 cards 一覧

| rank | card_id | root_cause_group | coverage_category |
|---:|---|---|---|
| 100 | `disk_spill` | `spill_memory_pressure` | `MEMORY` |
| 97 | `federation_query` | `federation` | `DATA` |
| 95 | `shuffle_dominant` | `shuffle_overhead` | `PARALLELISM` |
| 90 | `shuffle_lc` | `shuffle_overhead` | `PARALLELISM` |
| 85 | `data_skew` | `data_skew` | `PARALLELISM` |
| 80 | `low_file_pruning` | `scan_efficiency` | `DATA` |
| 75 | `low_cache` | `cache_utilization` | `COMPUTE` |
| 72 | `compilation_overhead` | `compilation_overhead` | `COMPUTE` |
| 70 | `photon_blocker` | `photon_compatibility` | `COMPUTE` |
| 68 | `photon_low` | `photon_compatibility` | `COMPUTE` |
| 65 | `scan_hot` | `scan_efficiency` | `DATA` |
| 60 | `non_photon_join` | `photon_compatibility` | `COMPUTE` |
| 55 | `hier_clustering` | `scan_efficiency` | `DATA` |
| 50 | `hash_resize` | `spill_memory_pressure` | `MEMORY` |
| 45 | `aqe_absorbed` | `shuffle_overhead` | `PARALLELISM` |
| 40 | `cte_multi_ref` | `sql_pattern` | `QUERY` |
| 38 | `investigate_dist` | `data_skew` | `PARALLELISM` |
| 35 | `stats_fresh` | `statistics_freshness` | `DATA` |
| 32 | `driver_overhead` | `driver_overhead` | `COMPUTE` |
| 30 | `rescheduled_scan` | `scan_efficiency` | `DATA` |
| 28 | `cluster_underutilization` | `cluster_underutilization` | `COMPUTE` |
| 25 | `compilation_absolute_heavy` | `compilation_absolute` | `COMPUTE` |

根拠: `CARDS` と taxonomy。`dabs/app/core/analyzers/recommendations_registry.py:872`, `dabs/app/core/action_classify.py:84`

## root_cause_groups (v5.19.0)

現行 canonical list は `ROOT_CAUSE_GROUPS`。`dabs/app/core/action_classify.py:25`

```python
ROOT_CAUSE_GROUPS = (
    "spill_memory_pressure", "data_skew", "shuffle_overhead",
    "photon_compatibility", "scan_efficiency", "join_strategy",
    "cache_utilization", "cluster_sizing", "statistics_freshness",
    "sql_pattern", "delta_write_overhead",
    "compilation_overhead", "driver_overhead",
    "federation",
    "cluster_underutilization", "compilation_absolute",
)
```

## Group-overlap dedup (v5.16.19 以降)

`_GROUP_OVERLAPS_RAW` は LLM と rule-based の post-hoc dedup に使う。現行 pair は対称展開後 11 組相当。定義元は以下。`dabs/app/core/action_classify.py:128`

| group A | overlap group B |
|---|---|
| `data_skew` | `shuffle_overhead` |
| `shuffle_overhead` | `data_skew` |
| `shuffle_overhead` | `join_strategy` |
| `join_strategy` | `shuffle_overhead` |
| `scan_efficiency` | `delta_write_overhead` |
| `scan_efficiency` | `statistics_freshness` |
| `delta_write_overhead` | `scan_efficiency` |
| `spill_memory_pressure` | `statistics_freshness` |
| `spill_memory_pressure` | `cluster_sizing` |
| `cluster_sizing` | `spill_memory_pressure` |
| `statistics_freshness` | `spill_memory_pressure` |
| `statistics_freshness` | `scan_efficiency` |

注: 実装は方向付き map と `groups_overlap()` で扱うため、表は dedup 意味論を展開して記載している。`dabs/app/core/action_classify.py:155`

## Suppression sets

### Federation suppression (v5.18.0)

federation query では Databricks-side tuning の一部を suppress する。実集合は `_FEDERATION_SUPPRESSED_CARDS` が canonical。`dabs/app/core/analyzers/recommendations.py:42`

### Serverless suppression / filtering

Serverless では `SET` 文など無効な fix SQL を `_filter_fix_sql_for_serverless` が除去する。post-process は `_apply_serverless_filter` が行う。`dabs/app/core/usecases.py:275`

## 追加カードのシグナル

| card | 新規シグナル | 根拠 |
|---|---|---|
| `compilation_overhead` | queue timestamps 4、compile ratio | `dabs/app/core/models.py:49` |
| `driver_overhead` | queue / scheduling / compute-wait derived fields | `dabs/app/core/models.py:49` |
| `federation_query` | `is_federation_query`、`federation_tables`、`federation_source_type` | `dabs/app/core/models.py:73`, `dabs/app/core/extractors.py:504` |
| `cluster_underutilization` | `effective_parallelism`、variant、severity | `dabs/app/core/models.py:453` |
| `compilation_absolute_heavy` | `compilation_absolute_heavy` | `dabs/app/core/models.py:462` |
