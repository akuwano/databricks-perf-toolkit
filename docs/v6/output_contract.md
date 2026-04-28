# V6 Output Contract — Section Policy & Nullable Rules (Week 2 Day 3)

`schemas/report_v6.schema.json` を支える運用ルール。schema 自体に書きにくい
「いつ何を埋めるべきか」「null 許容」「validation を強める/緩める閾値」を
ここに集約する。

## 1. Required vs Optional の原則

### 1.1 何が必須か

| Layer | Required | Optional |
|-------|----------|----------|
| Report root | `schema_version`, `report_id`, `generated_at`, `query_id`, `context`, `summary`, `findings` | `pipeline_version`, `prompt_version`, `appendix_excluded_findings` |
| Context | `is_serverless`, `is_streaming`, `is_federation` | `warehouse_size`, `duration_ms`, `result_from_cache`, `language` 等 |
| Summary | `headline`, `verdict` | `key_metrics` (≤ 6) |
| Finding | `issue_id`, `category`, `severity`, `title`, `evidence` (≥1), `actions` | `confidence`, `description`, `root_cause_group`, `coverage_category`, `alert_links`, `suppressed`, `suppression_reason` |
| Evidence | `metric`, `value_display` | `value_raw`, `threshold`, `source`, `grounded` |
| Action | `action_id`, `target`, `fix_type`, `what` | `why`, `fix_sql`, `expected_effect`, `verification`, `risk`, `effort`, `priority_rank`, `selected_because` |
| Verification | `type` + (`metric` or `sql` or `explain_pattern`) + `expected` | (none) |

### 1.2 設計理由

- **Finding に evidence ≥1 を強制**: 根拠ゼロの finding は schema レベルで拒否。Week 3 hallucination 削減の前提。
- **Action に target / fix_type / what を強制**: 「何を / どこに / どう変える」が無い action は production レベルではない。Q4 actionability scorer の最低ラインを schema で担保。
- **`why`, `expected_effect`, `verification` は optional だが Q4 では加点対象**: 強制すると LLM が捏造しやすいので "あれば加点" モデル。

## 2. Verdict の定義

`summary.verdict` は以下のいずれか:

| Verdict | 条件 | severity 上限 |
|---------|------|---------------|
| `healthy` | findings = [] または severity ≤ low のみ | low |
| `informational` | severity ≤ low の findings あり、報告のみ | low |
| `needs_attention` | medium 以上の finding が 1 つ以上 | medium/high |
| `critical` | severity = critical の finding が 1 つ以上 | critical |
| `skipped_cached` | `context.result_from_cache = true` で実分析を抑制 | (適用外) |

Generator (rule-based + LLM) は **必ず一意の verdict を出力する**。複数候補が並ぶことを避けるため、生成後に `_normalize_verdict()` (Day 4 normalizer) で確定する。

## 3. Severity と Confidence のマトリクス

| Severity \ Confidence | high | medium | low | needs_verification |
|-----------------------|------|--------|-----|---------------------|
| critical | 即対応 | 要確認 | フラグのみ | 投稿しない |
| high | 通常表示 | 通常表示 | 注釈付き | needs_verification 注釈 |
| medium | 通常表示 | 通常表示 | 注釈付き | 注釈付き |
| low | 補足表示 | 補足表示 | 補足表示 | 表示しない |
| info / ok | 表示は normalizer 判断 | (info/ok は confidence 不問) | | |

「投稿しない」セルの finding は `appendix_excluded_findings` 行きにする。

## 4. Nullable 方針

JSON Schema の `null` 許容は以下に限定する。それ以外は **欠落 = フィールド未指定** で表現する。

| Field | null OK | 理由 |
|-------|---------|------|
| `context.warehouse_size` | yes | warehouse 不明 (custom endpoint 等) |
| `context.warehouse_type` | yes | 同上 |
| `context.duration_ms` | yes | streaming で N/A |
| `context.rows_produced` | yes | streaming / DDL で N/A |
| `Evidence.value_raw` | yes | 値を抽出できなかった (LLM 経路で文字列のみ等) |
| (上記以外) | no | string なら "" 空文字、list なら []、obj なら省略 |

例:
```json
// OK
"context": { "warehouse_size": null, "is_serverless": true, ... }

// NG (use empty string instead)
"summary": { "headline": null, ... }
```

## 5. issue_id の命名規則

`Finding.issue_id` は **golden case の must_cover_issues[].id と一致する語彙** を使う。
Week 1 から抽出した初期セット (Day 6 で正式化):

| issue_id | category | 説明 |
|----------|----------|------|
| `spill_dominant` | memory | スピル支配的 |
| `shuffle_volume` | shuffle | シャッフル量大 |
| `shuffle_dominant` | shuffle | シャッフル時間支配的 |
| `data_skew` | skew | パーティション偏り |
| `aqe_handled_skew` | skew | AQE 解消済 (negative) |
| `photon_partial_fallback` | photon | Photon 部分 fallback |
| `photon_blocker_via_cast` | photon | CAST が photon 阻害 |
| `cte_recompute` | sql_pattern | CTE 再計算 |
| `low_file_pruning` | scan | データスキッピング不足 |
| `large_scan_volume` | scan | 読み込みバイト大 |
| `low_cache_hit` | cache | Cache hit ratio 低 |
| `cold_node_possibility` | cache | Cold node の可能性 |
| `result_from_cache_detected` | cache | 結果キャッシュ |
| `federation_detected` | federation | Federation 認識 |
| `streaming_detected` | streaming | ストリーミング認識 |
| `driver_overhead` | driver | Driver overhead 支配 |
| `compilation_overhead` | compilation | Compilation 支配 |
| `driver_overhead_or_compilation` | other | federation で支配的になる総称 |
| `serverless_detected` | other | Serverless 認識 |
| `cluster_underutilization` | compute | 並列度活用不足 |
| `implicit_cast_on_join_key` | sql_pattern | join key 暗黙 CAST |
| `cardinality_estimate_off` | cardinality | cardinality 推定外れ |
| `hash_resize_dominant` | shuffle | hash resize 支配的 |
| `merge_join_efficiency` | join | MERGE join 効率 |
| `write_side_optimization` | other | 書き込み側最適化 |
| `row_count_explosion` | sql_pattern | join 後行数爆発 |
| `missing_join_predicate` | sql_pattern | join 条件不足 |
| `micro_batch_throughput` | streaming | マイクロバッチスループット |
| `full_scan_large_table` | scan | フルスキャン |
| `missing_clustering` | clustering | クラスタリングなし |

**新規 issue_id を追加する場合**:
1. 該当する category を schema enum から選ぶ (なければ schema を bump)
2. snake_case
3. golden case yaml 側にも同じ id を追加して対応関係を保つ

## 6. fix_type の運用

| fix_type | 例 |
|----------|-----|
| `configuration` | `SET spark.sql.shuffle.partitions=400` |
| `ddl` | `ALTER TABLE ... ALTER COLUMN ... TYPE BIGINT` |
| `rewrite` | クエリ書き換え (CTE → CTAS 等) |
| `clustering` | `ALTER TABLE ... CLUSTER BY (...)` |
| `maintenance` | `OPTIMIZE`, `ANALYZE TABLE` |
| `investigation` | "EXPLAIN を確認", "サンプルデータを見る" 等の調査タスク |
| `operational` | warm-up, warehouse 切替 等の運用作業 |
| `pattern` | パラメータ化、結果キャッシュ活用等のクエリパターン |

**ガイドライン**:
- `configuration` と `ddl` は `fix_sql` を **必ず** 持つ (持たないなら fix_type が誤り)
- `investigation` は `fix_sql` を持たなくて良いが、`why` と `verification` は埋める
- 1 つの Action は 1 つの fix_type のみ。複合は別 Action に分ける

## 7. Evidence の grounded フラグ

Week 2 時点ではすべて `grounded: true` (デフォルト) で書く。Week 3 で:
- profile JSON から機械的に確認できた値 → `grounded: true`
- LLM が補完したが確認できない値 → `grounded: false`
- `score_hallucination` は `grounded: false` を penalty 対象に追加

normalizer (Day 4) は **既存 ActionCard.evidence (list[str]) を Evidence[] に変換する際、metric 名を抽出できれば true、できなければ false** とする。

## 8. Suppression ルール

以下を満たす finding は `suppressed: true` にして `appendix_excluded_findings` に移動:

| 条件 | suppression_reason 例 |
|------|----------------------|
| `context.is_federation = true` && category in {clustering, scan, cache, photon, stats} | `federation_workload_irrelevant` |
| `context.is_serverless = true` && action.target が classic-only spark config | `serverless_unsupported_config` |
| `context.result_from_cache = true` && finding が latency/spill/shuffle 系 | `result_cache_skip` |
| `context.is_streaming = true` && finding が one-shot OPTIMIZE 系 | `streaming_inappropriate` |

これは v5.18 までの既存 suppression ロジックの canonical 表現。

## 9. Validation の段階

| Stage | 何を validate | 失敗時の挙動 |
|-------|--------------|--------------|
| Soft (Week 2) | schema 構造のみ | warning ログ + そのまま出力 |
| Strict (Week 3+) | schema + issue_id 既知性 + fix_type一致性 | 失敗 finding を除外し、`schema_violation` を appendix に記録 |
| Hard gate (Week 6) | strict + critical recall ≥ baseline | A/B で baseline 下回ったら採用しない |

## 10. 後方互換ポリシー

- v5.19 までの ActionCard 出力は **削除しない**。canonical Report は **追加** で並走。
- feature flag `v6_canonical_schema` (default off):
  - off: 既存 markdown レポート + ActionCard JSON を生成
  - on: 上記 + canonical Report JSON も生成、eval/scorer は canonical を読む
- Markdown レポーターを canonical 入力に切り替えるのは **Week 4-6** で段階的に。

## 11. 例: minimum / realistic / suppressed

### Minimum (healthy)
```json
{
  "schema_version": "v6.0",
  "report_id": "r-abc",
  "generated_at": "2026-04-25T00:00:00Z",
  "query_id": "q-xyz",
  "context": {"is_serverless": true, "is_streaming": false, "is_federation": false},
  "summary": {"headline": "問題は検出されませんでした", "verdict": "healthy"},
  "findings": []
}
```

### Realistic (1 finding + 1 action)
```json
{
  ...header...,
  "summary": {
    "headline": "Spill が支配的です",
    "verdict": "needs_attention",
    "key_metrics": [
      {"name": "duration_ms", "value_display": "180s", "value_raw": 180000, "direction": "bad"},
      {"name": "spill_bytes", "value_display": "8 GB", "value_raw": 8589934592, "direction": "bad"}
    ]
  },
  "findings": [{
    "issue_id": "spill_dominant",
    "category": "memory",
    "severity": "high",
    "confidence": "high",
    "title": "Aggregate でメモリスピル発生",
    "evidence": [
      {"metric": "peak_memory_bytes", "value_display": "12 GB", "value_raw": 12884901888, "source": "node[12].operator_stats", "grounded": true}
    ],
    "actions": [{
      "action_id": "increase_warehouse_size",
      "target": "warehouse_size",
      "fix_type": "configuration",
      "what": "L から XL に変更",
      "why": "peak_memory が L 容量を超過しているため",
      "expected_effect": "spill_bytes → 0、実行時間 30% 短縮見込み",
      "expected_effect_quantitative": {"metric": "duration_ms", "delta_pct": -30, "confidence": "medium"},
      "verification": [{"type": "metric", "metric": "spill_bytes", "expected": "0"}],
      "risk": "low",
      "effort": "low"
    }]
  }]
}
```

### Suppressed (federation case)
```json
{
  ...,
  "findings": [...],  // federation で意味のあるもの
  "appendix_excluded_findings": [{
    "issue_id": "low_file_pruning",
    "category": "scan",
    "severity": "low",
    "title": "(suppressed)",
    "evidence": [{"metric": "files_pruned_pct", "value_display": "0%"}],
    "actions": [],
    "suppressed": true,
    "suppression_reason": "federation_workload_irrelevant"
  }]
}
```

## 12. Day 3 アクション

- [x] section policy 文書化
- [x] required/optional マトリクス確定
- [x] nullable 方針確定
- [x] issue_id 初期語彙化
- [x] suppression ルール明文化
- [ ] (Day 4) ActionCard → canonical Report normalizer 実装
- [ ] (Day 5) eval/scorers/r4_schema.py で validation
- [ ] (Day 6) goldens で違反パターン洗い出し

## 参照

- `schemas/report_v6.schema.json`
- `docs/v6/canonical_schema_inventory.md`
- `docs/eval/report_quality_rubric.md`
- `dabs/app/core/models.py:307` (Alert), `:660` (ActionCard)
