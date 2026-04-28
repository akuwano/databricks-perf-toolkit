# V6 Canonical Schema — 既存出力の棚卸し (Week 2 Day 1)

V6 R4 canonical schema を設計するため、現行 v5.19 の出力構造を棚卸しし、
Week 1 で固定した rubric / golden case が要求する項目とのギャップを整理する。

## 1. 現行のレポート出力経路

```
ProfileAnalysis (dabs/app/core/models.py:1001)
├── query_metrics: QueryMetrics
├── bottleneck_indicators: BottleneckIndicators (alerts: list[Alert])
├── action_cards: list[ActionCard]              ← rule-based registry 由来
├── llm_action_cards: list[ActionCard]          ← LLM 独自提案 (v5.16.19+)
├── selected_action_cards: list[ActionCard]     ← Top-N 表示用
├── hot_operators: list[OperatorHotspot]
├── top_scanned_tables: list[TableScanMetrics]
├── stage_info / data_flow / data_flow_dag
├── explain_analysis: ExplainExtended | None
└── streaming_context / target_table_info / warehouse_info
```

最終的な Markdown は `core/reporters/__init__.py::generate_report()` が
`ProfileAnalysis` を入力に組み立てる (12 sections + Appendix A-H)。

## 2. 既存 ActionCard schema

**dabs/app/core/models.py:660-727**

| Field | Type | 用途 | 備考 |
|-------|------|------|------|
| `problem` | str | 問題の記述 | 必須 |
| `evidence` | list[str] | 閾値超過メトリクス | 文字列、構造なし |
| `likely_cause` | str | 根本原因仮説 | |
| `fix` | str | 修正方法 (自然言語) | |
| `fix_sql` | str | SQL/Config snippet | optional |
| `expected_impact` | str | "high"/"medium"/"low" | 自由記述になりがち |
| `effort` | str | "low"/"medium"/"high" | |
| `priority_rank` | int | 100 (highest) - 0 | rule-based registry が割当 |
| `priority_score` | float | legacy | 後方互換 |
| `validation_metric` | str | 検証メトリクス | 自然言語 |
| `risk` | str | "low"/"medium"/"high" | |
| `risk_reason` | str | リスク理由 | |
| `verification_steps` | list[dict] | `{metric, expected}` or `{sql, expected}` | **半構造化** |
| `severity` | str | 任意 ("MEDIUM" 等) | |
| `root_cause_group` | str | grouping key | dedup用 |
| `coverage_category` | str | "COMPUTE"/"DATA"/"QUERY"/"MEMORY"/"PARALLELISM" | |
| `selected_because` | str | LLM rerank の理由 | |
| `is_preserved` | bool | 必読アラート扱い | |

## 3. 既存 Alert schema

**dabs/app/core/models.py:307-327**

| Field | Type | 用途 |
|-------|------|------|
| `severity` | Severity (CRITICAL/HIGH/MEDIUM/INFO/OK) | enum |
| `category` | str | "cache"/"spill"/"shuffle"/etc. |
| `message` | str | 自然言語 |
| `metric_name` | str | 例: "cache_hit_ratio" |
| `current_value` | str | "25%" — **string** に丸めている |
| `threshold` | str | ">80%" — **string** |
| `recommendation` | str | 自然言語 |
| `is_actionable` | bool | informational なら false |
| `conflicts_with` | list[str] | conflicting alert IDs |

## 4. Golden case が要求する項目 (Week 1 rubric より)

`eval/goldens/cases/<case>.yaml` には以下のスキーマ要素が登場する:

| Golden field | 対応する現行フィールド | ギャップ |
|--------------|---------------------|---------|
| `must_cover_issues[].id` | (なし) — Alert/ActionCard に **issue_id 概念がない** | **追加必要** |
| `must_cover_issues[].severity` | `Alert.severity` / `ActionCard.severity` | OK |
| `must_cover_issues[].keywords` | (なし) — 自然言語からの fuzzy match | **schema化** |
| `forbidden_claims[].id` | (なし) | **追加必要** (forbidden 検出は report 後段) |
| `must_have_actions[].target` | `ActionCard.problem` 文中に埋め込まれている | **構造化** |
| `must_have_actions[].type` | `ActionCard.coverage_category` に近いが粒度が違う | **拡張** (configuration / ddl / rewrite / clustering / maintenance / investigation) |
| `must_have_actions[].keyword` | `ActionCard.fix_sql` 等から fuzzy match | **抽出** |
| `expected_l3_min` | (eval 側) | OK — schema 不要 |
| `expected_recall_min` | (eval 側) | OK — schema 不要 |

## 5. Rubric (`docs/eval/report_quality_rubric.md`) との対応

| Rubric 項目 | canonical schema での表現 |
|-------------|--------------------------|
| L1 Format | schema 自体 (validation で測れる) |
| L2 Evidence | `Finding.evidence[]` を **構造化** (raw value + source path) |
| L3 Diagnosis | `Finding.severity` + `Finding.confidence` + `Finding.root_cause` |
| L4 Actionability | `Action` の 6 dimension (target/what/why/how/expected_effect/verification) を **schema field** に分離 |
| Hallucination | `Finding.evidence[].grounded` フラグ (Week 3 で実装、schema field は確保) |
| Action 具体性 | `Action.target` / `Action.fix_type` / `Action.fix_sql` / `Action.expected_effect` / `Action.verification` を必須に |
| Critical issue recall | `Finding.issue_id` で goldens と機械対応 |
| Regression | (Week 6 で実装、schema は v番号で互換管理) |

## 6. 必要なスキーマ要素 (Day 2 で詳細定義)

新設する canonical entity:

### `Finding` (= 既存 Alert + ActionCard.problem を統合)
```
{
  "issue_id": "spill_dominant",        # NEW: golden と機械対応
  "category": "memory",
  "severity": "high",
  "title": "...",
  "description": "...",
  "evidence": [Evidence],              # 構造化
  "confidence": "high|medium|low",     # NEW
  "root_cause_group": "...",
  "actions": [Action]                  # 紐付き
}
```

### `Evidence` (= 既存 evidence: list[str] を構造化)
```
{
  "metric": "peak_memory_bytes",       # ground truth name
  "value": 12884901888,                # raw 数値
  "value_display": "12 GB",            # 表示用
  "threshold": ">8GB",
  "source": "node[12].operator_stats.peak_memory",  # JSON path 風
  "grounded": true                     # Week 3 で hallucination 検出が立つ
}
```

### `Action` (= 既存 ActionCard.fix + verification を分離)
```
{
  "action_id": "set_shuffle_partitions",
  "target": "spark.sql.shuffle.partitions",
  "fix_type": "configuration",          # configuration|ddl|rewrite|clustering|maintenance|investigation
  "what": "Increase from 200 to 400",
  "why": "shuffle is dominant ...",
  "fix_sql": "SET spark.sql.shuffle.partitions=400;",
  "expected_effect": "実行時間 30% 短縮",
  "expected_effect_quantitative": {     # optional
    "metric": "duration_ms",
    "delta_pct": -30
  },
  "verification": [Verification],
  "risk": "low|medium|high",
  "risk_reason": "..."
}
```

### `Verification` (= 既存 verification_steps の構造化)
```
{
  "type": "metric|sql|explain",
  "metric": "shuffle_bytes_written",   # type=metric の場合
  "sql": "SELECT ...",                 # type=sql の場合
  "explain_pattern": "ReusedExchange", # type=explain の場合
  "expected": "decreases >20%"
}
```

### Top-level `Report`
```
{
  "schema_version": "v6.0",
  "report_id": "<uuid>",
  "generated_at": "ISO8601",
  "query_id": "...",
  "context": {
    "warehouse_size": "...",
    "is_serverless": true,
    "is_streaming": false,
    "is_federation": false
  },
  "summary": {
    "headline": "...",
    "verdict": "needs_attention|healthy|critical"
  },
  "findings": [Finding],
  "appendix_excluded_findings": [Finding]   # forbidden_claims hit 等
}
```

## 7. 後方互換戦略

- v5.19 までの ActionCard 出力は今後も維持
- `core/v6_schema/normalizer.py` (Day 4) で **既存 ActionCard → canonical Finding/Action** へ adapter
- `feature_flag.v6_canonical_schema` で:
  - `false` (default): 既存パス
  - `true`: パイプライン末尾で canonical schema を別 attribute (`pipeline_result.canonical_report`) に格納
- レポーター (Markdown 生成) は当面 `ActionCard` ベースのまま、Week 4 で canonical schema ベースの新レポーターを別パスで追加
- eval scorer はまず canonical 経由で動かし、その後 markdown も canonical を読む

## 8. 既知の前提・リスク

| 項目 | 対応方針 |
|------|---------|
| `evidence: list[str]` の自由記述から構造化 evidence を埋めるのは loss-y | Week 3 R6 (knowledge selector) で **生成側も schema に従う** よう prompt を改修 |
| `issue_id` の網羅セットは未定義 | Day 2 で `eval/goldens/cases/` の `must_cover_issues[].id` を集合化 + Alert.category × metric の cross product で初期値 |
| `confidence` は v5.19 ActionCard にない | 当面 normalizer は priority_rank から推定、v6 LLM 出力では明示 |
| Markdown レポーターを書き換えるか並走か | **並走**。v6 schema は eval/external 連携優先、UI は Week 4-6 で段階移行 |

## 9. Day 1 アクション

- [x] 既存 ActionCard / Alert / ProfileAnalysis 棚卸し → 本ドキュメント
- [x] golden case 要求項目とのギャップ抽出 → 本ドキュメント §4
- [x] canonical entity 5 種を仮設計 → 本ドキュメント §6
- [ ] (Day 2) JSON Schema (`schemas/report_v6.schema.json`) 作成
- [ ] (Day 3) section policy + nullable rule docs 化

## 参照

- `dabs/app/core/models.py:307` (Alert), `:660` (ActionCard), `:1001` (ProfileAnalysis)
- `eval/goldens/README.md` (case schema)
- `docs/eval/report_quality_rubric.md`
- TODO.md `### v6.0 — レポート品質向上リファクタリング`
