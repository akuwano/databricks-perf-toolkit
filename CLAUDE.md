# CLAUDE.md


## プロダクトビジョンと目的

**ビジョン**
Databricks SQLのクエリパフォーマンスを誰でも定量的に理解・比較・改善できるようにする

**目的**
Databricks SQLのクエリプロファイルJSONを分析し、LLMを活用してボトルネック特定・改善提案・Before/After比較レポートを自動生成する。分析結果はDeltaテーブルに永続化し、Genie（Text2SQL）経由で自然言語クエリも可能にする。v4ではSparkイベントログのETL分析とWeb UIダッシュボードを追加。

---

## ターゲットユーザーと課題・ニーズ

### ターゲットユーザー

### 課題
| 優先度 | 課題 |
|--------|------|
| P0 | |

### ニーズ

---

## 成功の定義


---

## ユーザーストーリー


---

## 機能要件

### 既存機能（実装済み）

| 機能 | モジュール | 説明 |
|------|-----------|------|
| プロファイル抽出 | `core/extractors.py` | クエリプロファイルJSONからメトリクスを抽出 |
| ボトルネック分析 | `core/analyzers.py` | キャッシュヒット率、Photon利用率、スピル等の自動評価 |
| EXPLAIN解析 | `core/explain_parser.py` | EXPLAIN EXTENDED出力の構造化パース |
| LLM分析 | `core/llm.py` | 3段階AI分析（初期→レビュー→リファイン） |
| レポート生成 | `core/reporters.py` | Markdown形式のパフォーマンスレポート出力 |
| 国際化 | `core/i18n.py` | 英語/日本語の多言語対応 |
| Web UI | `app.py` + `templates/` | Flaskベースのブラウザ分析画面 |
| Delta永続化 | `services/table_writer.py` | 分析結果のDeltaテーブル書き込み |
| プロファイル比較 | `core/comparison.py` | 15メトリクスの方向認識型Before/After比較 |
| 比較レポート | `core/comparison_reporter.py` | 比較結果のMarkdown差分レポート |
| LLM比較サマリー | `core/comparison_llm.py` | LLMによる比較の自然言語サマリー |
| SQLフィンガープリント | `core/fingerprint.py` | 正規化されたSQLハッシュ生成 |
| クエリファミリー | `core/family.py` | 同目的クエリのpurpose_signatureによるグルーピング |
| ナレッジベース | `core/knowledge.py` | 分析・比較から自動生成されるドキュメント管理 |
| ナレッジi18n | `core/knowledge.py` | section_idベースルーティング、ロケール別ナレッジロード（ja/en） (v4.13) |
| 設定スキーマバリデーション | `routes/` | DBSQL/Spark Perf設定保存時のスキーマバリデーション (v4.13.5) |
| テーブル自動初期化 | `services/table_writer.py` | DBSQL設定保存時にDeltaテーブルを自動初期化 (v4.13.5) |
| 設定永続化 | `core/config_store.py` | カタログ/スキーマ/ウェアハウスの設定保存 |
| Delta読み込み | `services/table_reader.py` | Deltaテーブルからの分析結果読み出し |
| SQLビューデプロイ | `scripts/deploy_views.py` | Genie連携用の7キュレーションビュー作成 |
| Spark Perf Goldテーブル読み込み | `services/spark_perf_reader.py` | 7つのGoldテーブルからSparkパフォーマンスデータを読み出し (v4) |
| Spark Perf Web UI | `templates/spark_perf.html` | 7タブダッシュボード（Chart.js、KPI、ボトルネック分類） (v4) |
| Spark Perf ETLパイプライン | `dabs/notebooks/01_Spark Perf Pipeline PySpark.py` | Sparkイベントログ→16テーブル（Bronze/Silver/Gold）、Gold MERGE（upsert方式で履歴保持） (v4/v4.13.5) |
| LLMナラティブサマリー | `dabs/notebooks/02_generate_summary_notebook.py` | Goldテーブルから自然言語サマリーを生成 (v4) |
| Spark Perfバイリンガルレポート | `core/reporters/` | Spark Perfレポートの日英バイリンガル生成（~200ラベル） (v4.13) |
| Genie Chatパネル | `services/genie_client.py` + `routes/genie_chat.py` | Genie Conversation APIによるチャットUI（DBSQL/Spark Perf/比較画面） (v4.14) |
| Spark比較 | `core/spark_comparison.py` + `templates/spark_compare.html` | Sparkアプリ間のBefore/After比較、5段階Verdict (v4.14) |
| 比較履歴永続化 | `services/table_writer.py` | 比較結果のDelta永続化、カラムソート (v4.14) |
| Experiment/Variantインライン編集 | `routes/` + `templates/` | 分析結果のExperiment/Variantをインライン編集、カスケード更新 (v4.14) |
| OBO認証 | `app.py` + `services/` | On-Behalf-Of認証（読み取り/LLM: OBO、書き込み/Genie/ジョブ: SP） (v4.15) |
| ポストデプロイスモークテスト | `tests/smoke/` | API 31チェック + UI 12チェック（Playwright） (v4.16) |
| Spark Perfアプリ側LLMレポート | `core/spark_perf_llm.py` | 2回LLM呼び出し戦略（Call1: セクション1-2 + 推奨アクション、Call2: セクション3-7） (v4.26) |
| Spark Perfプロンプト構築 | `core/llm_prompts/spark_perf_prompts.py` | Fact Pack組立、JA/ENシステム/ユーザープロンプト生成、JSON応答パース (v4.26) |
| Sparkチューニングナレッジ | `core/knowledge/spark_tuning.md` | section_idベースのSparkチューニングガイド（JA/EN） (v4.26) |
| モデル別max_tokens自動調整 | `core/llm_client.py` | Opus/Sonnet/Haiku/GPT/Llamaモデル別の最大出力トークン設定 (v4.26) |
| SQL精度評価フレームワーク | `eval/` | 4軸評価（L1構文/L2根拠/L3診断/L4改善効果）+ `--diff-from`による前後比較 (v4.26) |
| ストリーミングクエリ対応 | `core/extractors.py` + `core/reporters/` | DLT/SDPストリーミングプロファイルの検出・マイクロバッチ統計・バッチ指向レポート・LLMプロンプト統合 (v4.29) |
| DBSQLコスト推定 | `core/dbsql_cost.py` + `core/warehouse_client.py` | ウェアハウスサイズ別DBU/hコスト推定、Serverless/Pro/Classicモデル対応、参考コストテーブル (v4.28) |
| SQLクエリリライト | `routes/genie_chat.py` + `templates/shared_result.html` | Rewriteボタン → LLM SQL最適化、EXPLAIN/sqlglot自動バリデーション、Refineボタンによる反復微調整 (v4.38) |
| スキーマ分析 | `routes/schema_analysis.py` + `services/schema_join_detector.py` | 不適切データ型検出、テーブル間型不一致、パーティション/クラスタリング設計チェック、過去分析からのJOIN型不一致、移行DML生成 (v5.0) |
| EXPLAIN分析強化 | `analyzers/explain_analysis.py` + `explain_parser.py` | 統計状態でhash joinアラート修正、DFP/RuntimeFilter抽出、LLMプロンプトにEXPLAIN Insights追加 (v4.41) |
| アラート品質改善 | `analyzers/bottleneck.py` | Shuffle集約・severity段階化、Serverlessスキャンローカリティ、3段階スキュー検出、リトライ抑制 (v4.42) |
| 暗黙CAST検出 | `extractors.py` + `llm_prompts/prompts.py` | Aggregate expressionsからdecimal演算の暗黙CAST推定、LLMプロンプトに警告追加 (v4.45) |
| Spark Perfコスト列 | `dabs/notebooks/01_...py` + `core/dbu_pricing.py` + `services/spark_perf_reader.py` | `gold_application_summary` に `estimated_total_dbu` / `estimated_dbu_per_hour` / `estimated_total_usd` を追加。autoscale イベントを app の `[start_ts, end_ts]` にクリップ、min==max 固定クラスタは autoscale_cost を無視。比較 UI のコスト比較が機能する (v5.12) |

### 新規機能（MVP必須）

| 機能 | 説明 | 状態 |
|------|------|------|
| (v4で全て実装済み) | - | - |

### 機能の依存関係

- 比較機能 → Delta永続化（`--persist`で保存された分析IDが必要）
- ナレッジベース → Delta永続化
- Genieビュー → `scripts/deploy_views.py`によるビューデプロイ
- バリアントランキング → `--experiment-id` + `--variant`による分析タグ付け
- Spark Perf Web UI → `dabs/notebooks/`ノートブックで事前生成されたGoldテーブル
- Spark Perf ナラティブ → アプリ側LLM生成（`core/spark_perf_llm.py`）またはノートブック（`02_generate_summary_notebook.py`）
- SQL精度評価 → `eval/`ディレクトリ、`json/`のProfile JSON、`--diff-from`はgit worktree使用


---

## 非機能要件

### スケール要件

### 運用要件

---

## 受け入れ条件

### MVP完了の定義

### 品質目標（MVP後）

| 指標 | 目標 | 計測開始 |
|------|------|----------|
| テストカバレッジ | 80%以上 | MVP後 |
| CI実行時間 | 5分以内 | MVP後 |

---

## 優先順位

| 優先度 | 機能 | 状態 |
|--------|------|------|

---

## Common Commands

```bash
# テスト実行（1078テスト）
cd dabs/app && uv run pytest

# 特定モジュールのテスト
uv run pytest tests/test_comparison.py -v

# ローカルでWeb UIを起動
cd dabs/app && uv run flask --app app.py run --host 0.0.0.0 --port 8000

# デプロイ（config生成 → app.yaml生成 → bundle deploy → app起動 → WH権限付与 → スモークテスト を一括実行）
./scripts/deploy.sh dev       # dev環境
./scripts/deploy.sh staging   # staging環境
./scripts/deploy.sh prod      # prod環境

# CLI: 基本分析
cd dabs/app && uv run python -m cli.main <profile.json> -o report.md

# CLI: 永続化付き分析
uv run python -m cli.main <profile.json> --persist --experiment-id exp001 --variant baseline

# CLI: 比較
uv run python -m cli.main <profile.json> --compare-with <analysis-id> --persist

# SQLビューのデプロイ
cd scripts && uv run python deploy_views.py --catalog my_catalog --schema profiler

# SQLビューのリセット付きデプロイ
uv run python deploy_views.py --catalog my_catalog --schema profiler --reset-tables

# SQL精度評価（LLM付き完全評価）
cd dabs/app && python -m eval ../../json/ --model databricks-claude-sonnet-4-6

# SQL精度評価（前後比較: git ref）
cd dabs/app && python -m eval ../../json/ --diff-from v4.25.0 --model databricks-claude-sonnet-4-6

# SQL精度評価（前後比較: JSONファイル）
cd dabs/app && python -m eval ../../json/ --diff-from baseline.json

# eval テスト実行
cd dabs/app && uv run pytest ../../eval/tests/ -v
```

### 設定の仕組み

**設定変更は `dabs/local-overrides.yml` のみで行う。** `deploy.sh` が以下を自動生成する：
- `runtime-config.json` — アプリの実行時設定
- `app.yaml` — SQL Warehouseリソース宣言付きのアプリ定義
- SQL Warehouse `CAN_USE` 権限 — アプリのSPに自動付与
- カタログ/スキーマの自動作成、SP書き込みGRANT、ジョブCAN_MANAGE_RUN (v4.15)
- ポストデプロイスモークテスト自動実行 (v4.16)

**設定の優先順位**: 環境変数 > Web UI変更（セッション中のみ） > runtime-config.json > デフォルト値

初回セットアップ:
```bash
cp dabs/local-overrides.yml.sample dabs/local-overrides.yml
# local-overrides.yml を編集して catalog/schema/warehouse_id/app_name を設定
./scripts/deploy.sh dev
```

---

## Architecture

### Key directories

```
dabs/app/
├── app.py                  # Flaskアプリケーション（Web UI）
├── cli/main.py             # CLIエントリーポイント
├── core/                   # 分析・比較・ナレッジのビジネスロジック
│   ├── analyzers/          # ボトルネック分析（bottleneck, operators, recommendations, explain_analysis）
│   ├── reporters/          # Markdownレポート生成（summary, query_metrics, sections, details, dataflow, action_plan）
│   ├── llm_prompts/        # LLMプロンプト構築（prompts, parsing, knowledge, spark_perf_prompts）
│   ├── knowledge/          # チューニングガイド（7トピック × JA/EN = 14ファイル）
│   ├── usecases.py         # メインオーケストレーター（分析パイプライン全体の制御）
│   ├── extractors.py       # プロファイルJSON抽出 + ストリーミング検出
│   ├── models.py           # 全データモデル定義
│   ├── dbsql_cost.py       # DBSQLウェアハウスコスト推定
│   ├── dbu_pricing.py      # Spark Jobsコンピュートコスト推定
│   ├── spark_perf_llm.py   # Spark Perf LLMレポート生成
│   └── warehouse_client.py # SQL Warehouse API クライアント
├── services/               # 外部サービス連携
│   ├── table_writer.py     # Delta書き込み（分析結果 + 比較結果）
│   ├── table_reader.py     # Delta読み出し
│   ├── spark_perf_reader.py        # Spark Perf Goldテーブルリーダー
│   ├── spark_comparison_writer.py  # Spark比較結果のDelta書き込み
│   ├── genie_client.py     # Genie Conversation APIクライアント
│   └── job_launcher.py     # Databricksジョブ起動
├── routes/                 # Flaskルート
│   ├── analysis.py         # /analyze — プロファイルアップロード・分析
│   ├── history.py          # /history — 分析履歴一覧
│   ├── compare.py          # /compare — Before/After比較
│   ├── spark_perf.py       # /spark-perf — Sparkジョブ分析
│   ├── genie_chat.py       # Genie Chatパネル API
│   ├── share.py            # /shared — 共有リンク
│   ├── settings.py         # /api/v1/settings — 設定API
│   ├── report.py           # /report — レポートアップロード・表示
│   └── workload.py         # /workload — ワークロード分析
├── templates/              # HTMLテンプレート（15ファイル）
├── translations/           # 翻訳ファイル（JA/EN）
└── tests/                  # テスト（54ファイル、1078テスト）
dabs/notebooks/             # Spark Perf ETLノートブック
├── 01_Spark Perf Pipeline PySpark.py  # メインETL（Bronze/Silver/Gold）
├── 02_generate_summary_notebook.py    # LLMナラティブサマリー
└── 03_create_dashboard_notebook.py    # Lakeviewダッシュボード生成
eval/                       # SQL精度評価フレームワーク (v4.26)
├── cli.py                  # 評価CLI（--diff-from対応）
├── runner.py               # パイプライン実行 + スコアリング統合
├── diff_runner.py          # git worktreeベースの前後比較
├── scorers/                # L1構文/L2根拠/L3L4 LLM-as-judge
└── tests/                  # 評価フレームワークのテスト
scripts/
├── deploy.sh               # デプロイ（config生成 → bundle deploy → app起動 → WH権限 → スモーク）
├── deploy_views.py          # Genie用SQLビューデプロイ
├── generate_runtime_config.py  # runtime-config.json生成
├── smoke_test.py            # APIスモークテスト（19チェック）
├── ui_smoke_test.py         # UIスモークテスト（Playwright、12チェック）
├── eval_models.py           # モデル評価スクリプト
└── validate_key_mapping.py  # i18nキーマッピング検証
docs/
├── v3-detailed-design.md
└── genie-space-setup.md
```

### Core modules in `dabs/app/core/`
| モジュール | 役割 |
|-----------|------|
| `usecases.py` | メインオーケストレーター — 分析パイプライン全体の制御、LLMステージ呼び出し |
| `extractors.py` | プロファイルJSONからメトリクス抽出、ストリーミング検出・マイクロバッチ統計 |
| `models.py` | 全データモデル定義（QueryMetrics, ProfileAnalysis, StreamingContext等） |
| `constants.py` | 定数定義（Severity, 閾値等） |
| `analyzers/` | ボトルネック分析パッケージ（bottleneck, operators, recommendations, explain_analysis） |
| `reporters/` | Markdownレポート生成パッケージ（summary, query_metrics, sections, details, dataflow, action_plan） |
| `llm.py` | 3段階LLM分析（analyze → review → refine） |
| `llm_client.py` | LLMクライアント — モデル別max_tokens自動調整 |
| `llm_prompts/` | LLMプロンプト構築パッケージ（prompts, parsing, knowledge, spark_perf_prompts） |
| `explain_parser.py` | EXPLAIN EXTENDED出力の構造化パース |
| `sql_analyzer.py` | SQL構文解析（sqlglot利用） |
| `sql_safe.py` | SQLサニタイズ・安全性チェック |
| `comparison.py` | 15メトリクスの方向認識型Before/After比較 |
| `comparison_reporter.py` | 比較結果のMarkdown差分レポート |
| `comparison_llm.py` | LLMによる比較サマリー生成 |
| `spark_comparison.py` | Sparkアプリ間のBefore/After比較、5段階Verdict |
| `spark_comparison_reporter.py` | Spark比較レポート生成 |
| `spark_perf_llm.py` | Spark Perf LLMレポート（2回呼び出し戦略） |
| `spark_perf_markdown.py` | Spark Perfレポートのマークダウンフォーマット |
| `spark_perf_reporter.py` | Spark Perfレポート生成 |
| `dbsql_cost.py` | DBSQLウェアハウスコスト推定（Serverless/Pro/Classic） |
| `dbu_pricing.py` | Spark Jobsコンピュートコスト推定（インスタンスタイプ別） |
| `warehouse_client.py` | SQL Warehouse API情報取得、DBU/hマッピング |
| `serving_client.py` | モデルサービングAPIクライアント |
| `fingerprint.py` | SQL正規化とハッシュ生成 |
| `family.py` | 同目的クエリのpurpose_signatureによるグルーピング |
| `knowledge.py` | ナレッジドキュメントの自動生成・検索（section_idルーティング） |
| `config_store.py` | 設定の永続化（~/.dbsql_profiler_config.json） |
| `profile_validator.py` | プロファイルJSONバリデーション |
| `evidence.py` | 分析根拠データ構造 |
| `summary_builder.py` | レポートサマリー構築 |
| `i18n.py` | 国際化対応（JA/EN） |
| `utils.py` | ユーティリティ関数（format_bytes, format_time_ms等） |

### Knowledge files in `dabs/app/core/knowledge/`
| ファイル | 内容 |
|----------|------|
| `dbsql_tuning.md` / `_en.md` | DBSQLチューニングガイド |
| `dbsql_advanced.md` / `_en.md` | DBSQL上級チューニング |
| `dbsql_photon_oom.md` / `_en.md` | Photon OOM対策 |
| `dbsql_serverless.md` / `_en.md` | Serverless設定ガイド |
| `dbsql_sql_patterns.md` / `_en.md` | SQLパターンガイド |
| `spark_tuning.md` / `_en.md` | Sparkチューニングガイド |
| `spark_streaming_tuning.md` / `_en.md` | Sparkストリーミングチューニング |

### Services modules in `dabs/app/services/`
| モジュール | 役割 |
|-----------|------|
| `table_writer.py` | Delta書き込み（分析結果 + 比較結果、テーブル自動初期化） |
| `table_reader.py` | Delta分析結果テーブルの読み出し |
| `spark_perf_reader.py` | Spark Perf Goldテーブルリーダー + composite queries |
| `spark_comparison_writer.py` | Spark比較結果のDelta書き込み |
| `genie_client.py` | Genie Conversation APIクライアント（SP認証、Space自動作成/再作成） |
| `job_launcher.py` | Databricksジョブ起動（ETL/サマリー） |

### Spark Perf ETL notebooks in `dabs/notebooks/`
| ノートブック | 役割 |
|-------------|------|
| `01_Spark Perf Pipeline PySpark.py` | Sparkイベントログ→16テーブル（Bronze 1/Silver 8/Gold 7） |
| `02_generate_summary_notebook.py` | LLMによる自然言語サマリー生成→gold_narrative_summary |
| `03_create_dashboard_notebook.py` | 7ページ30+ウィジェットのLakeviewダッシュボード生成 |

### Delta Tables (11)
| テーブル | 説明 |
|----------|------|
| `profiler_analysis_header` | 分析ヘッダー（fingerprint, family, experiment, variant列含む） |
| `profiler_analysis_actions` | 推奨アクション |
| `profiler_analysis_table_scans` | テーブルスキャン情報 |
| `profiler_analysis_hot_operators` | Hotオペレータ |
| `profiler_analysis_stages` | ステージ情報 |
| `profiler_analysis_raw` | 生データ |
| `profiler_comparison_pairs` | 比較ペア |
| `profiler_comparison_metrics` | 比較メトリクス |
| `profiler_knowledge_documents` | ナレッジドキュメント |
| `profiler_knowledge_tags` | ナレッジタグ |
| `profiler_metric_directions` | メトリクス方向定義 |

---

## Development Workflow

```bash
# ポストデプロイスモークテスト（deploy.sh が自動実行、API 19チェック + UI 12チェック）
# 手動実行する場合:
scripts/smoke_test.py <app-url> --token <token> -v
scripts/ui_smoke_test.py <app-url> --token <token>

# フル分析フロー検証付きデプロイ
./scripts/deploy.sh dev --full-test
```

---

## 実装ガイドライン

### 開発哲学

| 原則 | 説明 |
|------|------|
| 段階的な進行 | 一度に大きな変更を行わず、小さな確実なステップで前進する |
| TDD厳守 | Red→Green→Refactorサイクルを必ず守る |
| Tidy First | 機能変更の前にまずコードを整理する |
| 詳細な文書化 | 変更の理由と目的を常に記録する |
| 継続的な検証 | 各ステップで動作を確認し、問題を早期発見する |

### 技術スタック

| カテゴリ | 技術 |
|----------|------|
| 言語 | Python 3.11+ |

### コミット規約

Conventional Commits形式を採用：

```
<type>(<scope>): <description>

# 例
feat(spark): add map field support to ProtoToSparkConverter
fix(dlt): handle empty expectation list
refactor(unity): extract SQL generation to separate method
test(schema): add nested message conversion tests
docs(readme): update architecture diagram
chore(deps): update protobuf to 4.25.0
```

| type | 用途 |
|------|------|
| feat | 新機能 |
| fix | バグ修正 |
| refactor | リファクタリング |
| test | テスト追加・修正 |
| docs | ドキュメント |
| chore | その他（依存関係更新等） |

### リリース手順（バージョン更新）

バージョン番号は `pyproject.toml` の `version` が唯一の正（Single Source of Truth）。
Web UI の `APP_VERSION` は `pyproject.toml` から自動読み取りされる。

**リリース時に必ず実行:**
```bash
# 1. pyproject.toml のバージョンを更新
#    例: version = "4.1.0"

# 2. コミット
git commit -am "chore: bump version to v4.1.0"

# 3. タグを打つ
git tag v4.1.0

# 4. push
git push origin main --tags
```

**忘れがちなポイント:**
- `pyproject.toml` の `version` を更新しないと Web UI のバージョン表示が古いまま
- タグを打たないと GitHub のリリース一覧に反映されない
- pyproject.toml とタグのバージョンは一致させること

### ブランチ運用ルール

- **mainへのマージは必ずユーザーの明示的な指示を待つこと**。自己判断でマージしない
- 機能開発・修正はfeatureブランチで行い、コミット+pushまでは自由に進めてよい
- マージの判断はユーザーが行う

### TDDサイクル

各機能は以下のサイクルで実装する：

```
1. Red: 失敗するテストを書く
   - 期待する動作を明確に定義
   - 具体的な入力と期待される出力を示す
   - エッジケースと制約条件を明示

2. Green: 最小限の実装でテストを通す
   - 最もシンプルな実装を選択
   - 複雑な最適化は後回し
   - 動作することを最優先

3. Refactor: コードを改善する
   - テストが通る状態を維持
   - 一度に一つの改善に集中
   - リファクタリングの目的を明確に
```

### Tidy First（Kent Beck）

機能変更の前に、まずコードを整理（tidy）する。

#### 適用タイミング

| 状況 | アクション |
|------|----------|
| 変更対象のコードが読みにくい | Tidy First |
| 変更が簡単にできる状態 | そのまま実装 |
| Tidyingのコストが高すぎる | 機能変更後に検討 |

#### Tidyingパターン

| パターン | 説明 |
|----------|------|
| Guard Clauses | ネストを減らすために早期リターンを使う |
| Dead Code | 使われていないコードを削除 |
| Normalize Symmetries | 似た処理は同じ形式で書く |
| Extract Helper | 再利用可能な部分を関数に抽出 |
| One Pile | 散らばった関連コードを一箇所にまとめる |
| Explaining Comments | 理解しにくい箇所にコメントを追加 |
| Explaining Variables | 複雑な式を説明的な変数に分解 |

#### 重要なルール

- 構造的変更と機能的変更を分離する（tidyingは別コミット）
- 小さく整理してから変更する
- 読みやすさを優先（未来の自分のために）

### イテレーション単位

各機能を最小単位に分割し、1イテレーションで1つの機能を完成させる。

#### イテレーションの条件

- **独立して動作可能**: そのステップだけで価値を提供できる
- **検証可能**: テストで動作を具体的に確認できる
- **完結している**: 中途半端な状態で終わらない
- **1コミット**: 1イテレーション = 1コミット

