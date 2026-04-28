# V6 Goldenset

V6 リファクタリング (TODO.md `### v6.0 — レポート品質向上リファクタリング`) の評価基盤。
レポート品質の継続的測定 (L1-L4 / Hallucination / Action具体性 / Critical issue recall / Regression) のために、代表的な DBSQL 分析ケースを golden として固定する。

## 構成

```
eval/goldens/
├── README.md                    # このファイル
├── manifest.yaml                # 全 case の一覧 + メタデータ (Week 1 確定)
└── cases/
    ├── spill_heavy.yaml         # 1 case = 1 ファイル
    ├── shuffle_dominant.yaml
    ├── photon_blocker.yaml
    ├── scan_low_pruning.yaml
    └── cache_cold_start.yaml
```

各 case は対応する profile JSON (gitignore 対象、`json/` 配下) を参照する。

## Case Schema

```yaml
# eval/goldens/cases/<case_name>.yaml
case_id: spill_heavy_q1            # 一意ID
profile_path: json/<filename>.json # 相対パス、json/ は gitignore
description: 短い説明
domain: dbsql                      # dbsql | spark_perf
workload_type: scan_heavy          # scan_heavy | join_heavy | etl | streaming | short_query
expected_severity: high            # critical | high | medium | low

# レポートが必ず言及すべき issue
must_cover_issues:
  - id: spill_dominant
    severity: high
    keywords: [spill, memory, peak_memory_bytes, oom]
    description: ディスクスピル発生

# レポートが言うべきでない claim (hallucination 検出用)
forbidden_claims:
  - federation_recommendation         # federation でないのに federation 推奨
  - lc_for_small_table                # 小テーブルで LC 推奨

# Action 具体性のチェック対象 (Q4 で活用)
must_have_actions:
  - target: peak_memory_bytes
    type: spark_config
    keyword: spark.databricks.photon

# 期待される L3 診断スコア下限
expected_l3_min: 4

# 期待される Critical issue recall 下限
expected_recall_min: 0.85

# 補足メモ (人間用)
notes: |
  Profile は 2XL warehouse で 60GB+ shuffle、spill 8GB のクエリ。
  AQE は有効だが skew 検出に至っていない。
```

## Manifest Schema

`manifest.yaml` は以下の構造:

```yaml
version: 1
created: 2026-04-25
description: V6 quality evaluation goldenset
cases:
  - case_id: spill_heavy_q1
    file: cases/spill_heavy.yaml
    tags: [spill, memory, oom]
    priority: critical
  - case_id: shuffle_dominant_q1
    file: cases/shuffle_dominant.yaml
    tags: [shuffle, skew]
    priority: high
  ...
```

## 運用

| アクション | コマンド |
|-----------|---------|
| 全 case を baseline で評価 | `python -m eval --goldens eval/goldens/manifest.yaml --baseline` |
| 特定 tag のみ評価 | `python -m eval --goldens eval/goldens/manifest.yaml --tag spill` |
| 新しい profile を golden 化 | `cases/` に新 yaml を追加 + `manifest.yaml` に登録 |

## Week 1 範囲

- Day 2: manifest schema 確定 + seed 5 cases
- Day 3: 15-20 cases へ拡張、`must_cover_issues` / `forbidden_claims` 充実化

## 参照

- `docs/eval/report_quality_rubric.md` — 採点基準
- `eval/scorers/` — 既存 scorer (L1-L4) と Day 4 で追加する新 scorer
