# DBSQL Profiler Analyzer ドキュメント

v5.19.0 現行仕様のドキュメント索引。

## 設計ドキュメント (新構成)

パイプライン・LLM・レポート・ナレッジの詳細仕様：

| ドキュメント | 内容 |
|-------------|------|
| [`architecture-overview.md`](architecture-overview.md) | 全体アーキテクチャ、主要モジュール、registry 化後の ActionCard 構成 |
| [`analysis-pipeline.md`](analysis-pipeline.md) | 分析パイプライン、registry 22 cards、federation suppression、group-overlap dedup |
| [`llm-stages.md`](llm-stages.md) | 3 ステージ構造化 LLM、federation thread、LC / rewrite / rerank / review |
| [`action-plan-generation.md`](action-plan-generation.md) | ActionCard registry、priority_rank、root_cause_group、suppression / dedup |
| [`knowledge-injection.md`](knowledge-injection.md) | knowledge section_id ルーティング、alert category → section_ids、新規 section |
| [`report-rendering.md`](report-rendering.md) | `generate_report` のセクション順序、rule/LLM 分離描画 |
| [`data-flow.md`](data-flow.md) | CLI / Web UI / 比較 / Spark Perf / Share のデータフロー |

## 既存ドキュメント

| ドキュメント | 内容 |
|-------------|------|
| [`api-reference.md`](api-reference.md) | API エンドポイント詳細 |
| [`operations-guide.md`](operations-guide.md) | デプロイ手順、環境変数、運用 |
| [`genie-space-setup.md`](genie-space-setup.md) | Genie Space 作成 / 連携設定 |
| [`key-mapping.md`](key-mapping.md) | 翻訳キー / 列マッピング |
| [`llm-model-evaluation-results.md`](llm-model-evaluation-results.md) | LLM モデル評価結果 |

## 旧ドキュメント

| ドキュメント | 注記 |
|-------------|------|
| [`v3-detailed-design.md`](v3-detailed-design.md) | v3 時点の詳細設計。v4 以降の変更は反映されていないため、本ディレクトリの新構成 docs を優先参照。 |

## 対象バージョン

本 docs セットは v5.19.0 を対象とする。版履歴の単一ソースはこの節とし、各個別ドキュメントでは必要に応じて本ファイルを参照する。

| 版 | 主要変更 |
|---|---|
| v4.11+ | 3 ステージ構造化 LLM 導入 (legacy `create_analysis_prompt` から移行) |
| v4.26 | モデル別 `max_tokens` 自動調整、Spark Perf アプリ側 LLM 2 回呼び出し |
| v4.29 | ストリーミングクエリ (DLT / SDP) 対応 |
| v4.38 | SQL Rewrite UI + EXPLAIN / `sqlglot` バリデーション |
| v4.41 | EXPLAIN Insights を Stage 1-3 プロンプトに追加 |
| v5.0 | スキーマ分析 (JOIN 型不一致検出) |
| v5.11.0 | Warehouse sizing: Scale-up / Scale-out / Optimize 判定 |
| v5.15.3 | Delta LC ClusterOnWrite overhead 検出 |
| v5.16.0 | Top-N アクションプラン選定導入 (`root_cause_group` × `coverage_category`) |
| v5.16.4 | アラート ↔ アクション相互参照 (`reporters/alert_crossref.py`) |
| v5.16.5 | `scan_impact_ratio` / Photon task duration ゲート |
| v5.16.6 | Stage 1-3 プロンプトに pushed filter 情報を追加 |
| v5.16.7 | LC 推奨 LLM に shuffle 詳細を渡す |
| v5.16.8 | Top-10 化 + preserved cap 撤廃 |
| v5.16.9 | lint / format 修正 (機能変更なし) |
| v5.16.10 | `shuffle_lc` ActionCard 追加、knowledge `lc_shuffle_key_candidate` 追加 |
| v5.16.11-16 | Phase 1 registry refactor: `recommendations_registry.py`、`CardDef`、`generate_from_registry` 追加 |
| v5.16.17 | Hier Clustering cardinality 推定強化: name/type heuristic、`ColumnStat`、composite 優先順位 |
| v5.16.18 | HC canonical SQL に統一: `ALTER TABLE ... CLUSTER BY (...)` + `SET TBLPROPERTIES` + `OPTIMIZE FULL` |
| v5.16.19 | Phase 2: preservation marker / hybrid dedup / Top-N selection 削除、group-overlap dedup へ移行 |
| v5.16.20 | Phase 3: legacy `generate_action_cards` if-block 削除、registry が唯一の rule-based emission source |
| v5.16.21 | Implicit CAST on JOIN detector 追加 (`NodeMetrics.join_keys_left` / `join_keys_right` ベース、EXPLAIN と重複抑止) |
| v5.16.22 | optimized SQL prompt に「Option A/B/C の空ボディ禁止」ルール追加 |
| v5.16.23 | LC prompt から SQL body を削除、operator metadata のみで候補供給 |
| v5.16.25 | `compilation_overhead` / `driver_overhead` card 追加、Query / Bottleneck 指標拡張 |
| v5.18.0 | Lakehouse Federation 検出、suppression、専用 card / knowledge / prompt 制約追加 |
| v5.19.0 | `cluster_underutilization` / `compilation_absolute_heavy` card 追加、taxonomy / knowledge / metrics 拡張 |

## コード参照の表記

本 docs では `file_path:line_number` 形式でソースコードを参照する。現行行番号の基準は v5.19.0。

## 貢献

ドキュメント更新は、仕様変更と同じ PR で合わせて行うことを推奨する。
