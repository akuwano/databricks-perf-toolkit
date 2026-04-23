# Databricks Performance Toolkit

Databricks SQLのクエリプロファイルJSONを分析し、パフォーマンスレポートを生成するツールです。LLM（Databricks Foundation Model APIs）を活用して、ボトルネックの特定と具体的な改善提案を行います。

[English README](README.md)

## 機能

### コア分析
- **クエリプロファイル分析**: クエリプロファイルJSONからメトリクスを自動抽出
- **ボトルネック検出**: ボトルネック指標の自動計算と評価
  - キャッシュヒット率（>80%良好, <30%危機的）
  - Photon利用率（>80%良好, <50%危機的）
  - ディスクスピル検出（>5GB危機的, >1GB重要）
  - Shuffle効率（512MB/パーティション閾値）
  - フィルタ効率（プルーニング率）
- **EXPLAIN分析**: EXPLAIN EXTENDED出力を解析し、実行プランの詳細な洞察を提供
- **結合タイプの分類**: 結合タイプを分類し、Photon対応状況を判定
- **LLMによる推奨**: Databricks Foundation Model APIを使用した3段階AI分析（初期分析→レビュー→リファイン）
- **アクション可能なレポート**: 主要発見事項、Hot Operators、検証チェックリストを含むレポートを生成
- **多言語対応**: 英語（デフォルト）と日本語

### v3: 比較と実験
- **プロファイル比較**: 方向認識型の15メトリクスによるBefore/After比較とLLM生成の比較サマリー
- **クエリファミリーグルーピング**: ヒント/JOIN/フィルタ変更を跨いで同目的のクエリを`purpose_signature`で識別
- **SQLフィンガープリント**: 正規化されたSQLハッシュによる同一クエリの実行横断追跡
- **実験・バリアント追跡**: `--experiment-id`と`--variant`でA/Bテストワークフローをタグ付け
- **バリアントランキング**: 重み付きスコアリングと失格ガードレールによるバリアント選定ビュー

### v3: ナレッジと永続化
- **ナレッジベース**: 分析と比較から自動生成されるドキュメント、タグで検索可能
- **Deltaテーブル永続化**: 全分析結果・比較・ナレッジを11のDeltaテーブルに保存
- **設定の永続化**: カタログ/スキーマ/ウェアハウスを`~/.dbsql_profiler_config.json`に保存
- **キュレーションSQLビュー**: リグレッション検出やレコメンデーションを含むGenie（Text2SQL）連携用の7ビュー

### v3: Web UI追加機能
- **分析履歴** (`/history`): フィルタリングと検索による過去の分析閲覧
- **サイドバイサイド比較** (`/compare`): メトリクス方向指標付きの2分析のビジュアル差分
- **設定の永続化**: カタログ、スキーマ、HTTPパスをセッション間で保持

### v4: Sparkパフォーマンス分析
- **Spark Perf ETLノートブック**: Sparkイベントログを処理する4つのDatabricksノートブック（`dabs/notebooks/`）、Bronze/Silver/Goldメダリオンアーキテクチャで16テーブルを生成（Gold MERGEによるupsertで履歴保持）
- **Spark Perf Web UI** (`/spark-perf`): LLMナラティブ統合のMarkdownレポート、Web UIからETLジョブ起動
- **ワークロード横断分析** (`/workload`): DBSQL + Sparkの左右並列比較、LLM横断分析 [Experimental]
- **SparkPerfReader**: ETLパイプラインで事前計算された7つのGoldテーブルをWeb UIで表示するためのリーダー
- **LLMナラティブサマリー**: Sparkパフォーマンスメトリクスの自然言語サマリーを自動生成、EN/JA自動翻訳対応
- **ボトルネック分類**: 7種類のボトルネックタイプ（STAGE_FAILURE, DISK_SPILL, HIGH_GC, DATA_SKEW, HEAVY_SHUFFLE, MEMORY_SPILL, MODERATE_GC）と重要度レベル
- **個別設定**: Spark Perfデータ用の独立したcatalog/schema/table_prefix/http_path設定（`~/.dbsql_profiler_config.json`に保存）
- **10のAPIエンドポイント**: applications、summary、stages、executors、concurrency、SQL/Photon、jobs、narrative、settingsのREST API
- **Gold MERGE（Upsert）**: Goldテーブルの書き込みをoverwriteからMERGEに変更し履歴を保持 (v4.13.5)
- **ナレッジベースi18n**: section_idベースルーティング（日本語見出しから分離）、バイリンガルナレッジファイル（ja/en） (v4.13)
- **バイリンガルSpark Perfレポート**: ~200ラベルの日英対応、「日本語レポート」トグル、LLMモデルセレクター (v4.13)
- **スキーマバリデーション**: DBSQL/Spark Perf設定保存時のスキーマバリデーション (v4.13.5)
- **テーブル自動初期化**: DBSQL設定保存時にDeltaテーブルを自動初期化、CREATE TABLE権限チェックとGRANTガイダンス (v4.13.5)
- **アプリケーションページネーション**: Spark Perfアプリケーション一覧を20件/ページに分割 (v4.13.5)
- **LLMナラティブ直接表示**: レポーター生成をスキップし、ナラティブを直接表示 (v4.13.5)
- **根拠制約型推奨フォーマット**: 7フィールド + HARD RULES + 信頼度基準（high/medium/needs_verification） (v4.12.5)
- **比較分析LLM改善**: C1-C9の改善（因果グラフ制約、反証チェック等） (v4.12.5)
- **Genieチャットパネル**: Genie Conversation APIによるDBSQL/Spark Perf/比較画面のチャット、SP認証、Space自動再作成 (v4.14)
- **Spark比較**: Sparkアプリ間のBefore/After比較、履歴タブ、レポート閲覧、削除、Experiment/Variant、5段階Verdict (v4.14)
- **比較履歴永続化**: 比較結果のDelta永続化、カラムソート (v4.14)
- **Experiment/Variantインライン編集**: 結果画面から直接編集、カスケード更新 (v4.14)
- **OBO認証**: 読み取り/LLMはOn-Behalf-Of、書き込み/Genie/ジョブはサービスプリンシパル認証 (v4.15)
- **デプロイ自動化**: カタログ/スキーマ自動作成、SP書き込みGRANT、ジョブCAN_MANAGE_RUN、デプロイデフォルトリセット (v4.15)
- **ポストデプロイスモークテスト**: API 31チェック + UI 12チェック（Playwright）、デプロイ後自動実行、--full-testフラグ (v4.16)
- **Spark PerfアプリサイドLLMレポート**: 2回呼び出し戦略でセクション1-7 + 推奨アクションを生成、Sparkチューニングナレッジベース付き、最長プレフィックスによるモデルmax_tokens自動調整 (v4.26)
- **SQL精度評価フレームワーク**: 4軸スコアリング（L1構文妥当性、L2根拠整合性、L3 LLM-as-judgeによる診断正確性、L4改善効果）、`--diff-from`によるgit worktreeを使ったBefore/After比較 (v4.26)
- **DBSQLコスト推定**: ウェアハウスサイズ（2X-Small〜4X-Large）と課金モデル（Serverless/Pro/Classic）に基づくクエリ単位のコスト推定。ウェアハウスAPI不可時はparallelism ratioでフォールバック、参考コストテーブル付き (v4.28)
- **ストリーミングクエリ対応**: DLT/SDPストリーミングプロファイル（`REFRESH STREAMING TABLE`）の検出・分析。マイクロバッチ統計（min/avg/max/p95 duration、read bytes、rows）、バッチ指向レポートセクション、遅延バッチ検出、LLMプロンプト統合 (v4.29)
- **SQLクエリリライト**: LLMによるSQL最適化。Rewriteボタン → EXPLAIN/sqlglot自動バリデーション → Refineボタンによる反復的な微調整 (v4.38)
- **スキーマ分析** (`/schema-analysis`): 不適切なデータ型の検出（decimal(38,0)→INT/BIGINT）、テーブル間JOINキー型不一致、パーティション設計アンチパターン、クラスタリングキー型、集約式の暗黙CAST検出、過去分析からのJOIN型不一致検出、移行DML生成 (v5.0)
- **EXPLAIN分析強化**: EXPLAIN EXTENDED添付時にオプティマイザ統計状態で統計不足を確定/否定、Photonブロッカー名特定、DFP選択率・Runtime Filter抽出 (v4.41)
- **アラート品質改善**: Shuffleアラート集約・severity段階化、Serverlessスキャンローカリティ低下、hash joinの3段階スキュー検出、クラウドストレージリトライduration=0時の抑制 (v4.42)

## 必要条件

- Python 3.11+
- uv（推奨）または pip
- Databricks Workspace（LLM分析を使用する場合）

## インストール

```bash
# リポジトリのクローン
git clone https://github.com/akuwano/databricks-perf-toolkit.git
cd dbsql_profiler_analysis_tool

# 依存関係のインストール（uvの場合）
uv sync

# または pip の場合
pip install -e .
```

## 使用方法

### Step 1: クエリプロファイルJSONの取得

1. Databricks SQL Warehouseでクエリを実行
2. クエリ履歴から対象クエリを選択
3. 「Query Profile」タブを開く
4. 右上の「...」メニューから「Download profile」を選択
5. **重要: ダウンロード前に「Verbose」モードを選択**してください。ノードごとの詳細メトリクス（メモリ、スピル、I/O、スキャンローカリティ）が含まれます
6. JSONファイルをダウンロード

> **Note:** DBSQL経由のプロファイルに加え、Spark Connect経由（`entryPoint=SPARK_CONNECT`）のプロファイルも自動検出して解析できます。**Verboseモードを強く推奨します** — 指定しない場合、多くの高度なメトリクス（ピークメモリ、クラウドストレージリトライ、データフィルタ統計、スキャンローカリティ）が利用できません。

### Step 1.5: EXPLAIN EXTENDED の取得（推奨）

Photonブロッカーの詳細検出やSQL書き換え提案を有効にするには、EXPLAIN EXTENDEDを取得します:

1. DBSQLクエリエディタで以下を実行:
   ```sql
   EXPLAIN EXTENDED <対象のクエリ>
   ```
2. 結果をテキストファイル（例: `explain.txt`）として保存
3. `--explain` オプションで指定

### Step 2: 環境変数の設定（CLIのみ）

> **Note:** **Web UI（Databricks Apps）** を使用する場合はこのステップは不要です。サービスプリンシパルによる自動認証が行われます。

CLI でLLM分析を使用する場合：

```bash
export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-token>"
```

### Step 3: レポートの生成

```bash
cd databricks-apps

# 基本的な使用方法（LLM分析付き）
uv run python -m cli.main <profile.json> -o report.md

# モデルを指定する場合
uv run python -m cli.main <profile.json> --model databricks-claude-opus-4-6 -o report.md

# LLM分析をスキップ（メトリクスのみ）
uv run python -m cli.main <profile.json> --no-llm -o report.md

# 標準出力に表示
uv run python -m cli.main <profile.json>
```

### Step 4: レポートの確認と対応

生成されたレポートには以下が含まれます（結論先行の構造化レイアウト）:

1. **エグゼクティブサマリー**（LLM） - 重要度評価、主要な発見事項、影響の概要
2. **クエリ概要** - クエリID、実行時間、ステータス、フォーマット済みSQL・構造分析
3. **パフォーマンスメトリクス** - ボトルネック指標、I/Oメトリクス、Hot Operators、スピル分析
4. **データフロー** - クエリステージを通じたデータ量の流れ
5. **アラート** - 自動検出された危機的な問題と警告
6. **ステージ実行** - 実行タイムラインとステージレベルの詳細
7. **根本原因分析**（LLM） - 直接原因と根本原因の特定
8. **推奨事項**（LLM + ルールベース） - 優先度付き改善アクション（Action Cards付き）
9. **最適化済みSQL**（LLM） - BROADCASTヒント、CTE最適化、Photon互換関数への書き換え
10. **結論**（LLM） - まとめと次のステップ
- **Appendix A**: 検証チェックリスト

#### ボトルネック指標の見方

| 指標 | 説明 | 備考 |
|------|------|------|
| キャッシュヒット率 | キャッシュから読み取ったデータの割合 | 初回実行時は低い値が想定される |
| リモート読み取り率 | リモートストレージから読み取った割合 | 警告レベル（危機的ではない） |
| Photon利用率 | Photonで実行された時間の割合 | 低い場合はPhotonブロッカーを確認 |
| リスケジュールスキャン率 | 非ローカルスキャンタスクの割合 | スキャンタスクデータがある場合のみ表示 |
| ディスクスピル | ディスクに書き出されたデータ量 | メモリ不足を示唆 |
| シャッフル影響率 | シャッフル時間/全タスク時間 | 高い場合はJoin最適化を検討 |
| フィルタ効率 | プルーニングされたファイル/全ファイル | 低い場合はパーティショニングを見直し |

### プロファイル比較

2つの分析を比較してクエリ変更の影響を測定:

```bash
cd databricks-apps

# 1. "Before" プロファイルを分析
uv run python -m cli.main before.json --persist --experiment-id exp001 --variant baseline -o before.md

# 2. "After" プロファイルを分析
uv run python -m cli.main after.json --persist --experiment-id exp001 --variant optimized -o after.md

# 3. 2つを比較（ステップ1, 2の分析IDを使用）
uv run python -m cli.main after.json --compare-with <before-analysis-id> --persist -o comparison.md
```

比較レポートには方向認識型の15メトリクス、デルタサマリー、リグレッションと改善を強調するLLM生成のインサイトが含まれます。

## コマンドラインオプション

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `profile` | クエリプロファイルJSONファイルのパス | (必須) |
| `-o, --output` | 出力ファイルパス | stdout |
| `--model` | 使用するLLMモデル | databricks-claude-opus-4-6 |
| `--tuning-file` | dbsql_tuning.mdのファイルまたはディレクトリパス | 自動検出 |
| `--no-llm` | LLM分析をスキップ（メトリクスとルールベース分析のみ） | false |
| `--explain` | EXPLAIN EXTENDED出力ファイルのパス（PhotonブロッカーとSQL書き換え提案を有効化） | - |
| `--lang` | 出力言語（`en` または `ja`） | en |
| `--persist` | 分析結果をDeltaテーブルに保存 | false |
| `--experiment-id` | A/Bテスト用の実験識別子 | - |
| `--variant` | バリアントラベル（例: `baseline`, `optimized`） | - |
| `--compare-with` | 比較対象の分析ID（Beforeプロファイル） | - |
| `--tags` | 分析用のカンマ区切りタグ | - |

> **Note:** `--explain` オプションは正確なPhotonブロッカー検出のために推奨されます。指定しない場合は基本的なヒューリスティクスのみが使用されます。

## 利用可能なLLMモデル

Databricks Foundation Model APIsで利用可能なモデル:

- `databricks-claude-opus-4-6`（デフォルト）
- `databricks-claude-sonnet-4-6`
- `databricks-claude-sonnet-4`
- `databricks-gpt-5-4`
- `databricks-meta-llama-4-maverick`
- `databricks-meta-llama-3-3-70b-instruct`

## ボトルネック指標の閾値

| 指標 | 良好 | 要改善 | 危機的 |
|------|------|--------|--------|
| キャッシュヒット率 | >80% | 50-80% | <30% |
| Photon利用率 | >80% | 50-80% | <50% |
| ディスクスピル | 0 | <1GB | >5GB |
| シャッフル影響率 | <20% | 20-40% | >40% |
| メモリ/パーティション | <512MB | - | >512MB |

## Spark Perf ETL（ノートブック）

`dabs/notebooks/` ディレクトリには、Sparkイベントログを処理するスケジュールジョブとして実行される2つのDatabricksノートブックが含まれます:

| ノートブック | 説明 |
|-------------|------|
| `01_Spark Perf Pipeline PySpark.py` | メインETL: Sparkイベントログを読み込み、Bronze/Silver/Goldメダリオンアーキテクチャで16テーブルを生成。パラメータ: `log_root`, `cluster_id`, `schema`, `table_prefix` |
| `02_generate_summary_notebook.py` | LLMサマリー: Goldテーブルを読み込み、LLM（Claude/Llama）で自然言語サマリーを生成、`gold_narrative_summary`テーブルに書き込み |

**Goldテーブル（7）** Web UIが読み取るテーブル:

| テーブル | 説明 |
|----------|------|
| `application_summary` | アプリケーションレベルのメトリクスとduration |
| `job_performance` | ジョブレベルのdurationとタスク数 |
| `stage_performance` | ステージレベルのメトリクスとボトルネック分類 |
| `executor_analysis` | エグゼキュータのリソース使用率とストラグラー検出 |
| `bottleneck_report` | 重要度と推奨事項付きのボトルネック分類 |
| `job_concurrency` | ジョブ並行性、CPU効率、スケジューリング遅延 |
| `sql_photon_analysis` | SQL実行のPhoton利用率とオペレータ分析 |

**ボトルネック分類:**

| タイプ | 重要度 | 条件 |
|--------|--------|------|
| STAGE_FAILURE | HIGH | ステージステータス == FAILED |
| DISK_SPILL | HIGH | disk_bytes_spilled > 0 |
| HIGH_GC | MEDIUM | gc_overhead_pct > 20% |
| DATA_SKEW | MEDIUM | task_skew_ratio > 5 |
| HEAVY_SHUFFLE | LOW | shuffle_read_bytes > 10GB |
| MEMORY_SPILL | LOW | memory_bytes_spilled > 0 |
| MODERATE_GC | LOW | gc_overhead_pct > 10% |

## Spark Perf Web UI

Spark Perfページ（`/spark-perf`）:

- **Markdownレポート**: 8つの番号付き本文セクション + 付録（DBSQL と同じアーキテクチャ）
- **LLMナラティブ**: 自動生成された自然言語サマリーをレポートに統合（v4.26でアプリサイド2回呼び出し戦略に対応）
- **アプリサイドLLMレポート**: 2回呼び出し戦略 — Call 1: セクション1-2 + 推奨アクション、Call 2: セクション3-7 — Sparkチューニングナレッジベースによるコンテキスト認識型推奨 (v4.26)
- **ETLジョブ起動**: Web UIから直接ETLパイプラインを実行（Volume path + Cluster ID → Job API）
- **アプリケーション選択**: Goldテーブルからデータを閲覧・選択

## ワークロード横断分析

ワークロードページ（`/workload`）:

- **手動ペアリング**: DBSQL分析とSparkアプリを選んでレポートを左右並列表示
- **LLM横断分析**: AI による共通ボトルネック・根本原因・統合推奨の自動生成 [Experimental]
- **ペア永続化**: リンクしたペアを保存して次回すぐ呼び出し

**Spark Perf設定**（`~/.dbsql_profiler_config.json`に保存）:

| キー | 環境変数 | 説明 |
|------|----------|------|
| `spark_perf_catalog` | `SPARK_PERF_CATALOG` | Unity Catalog名 |
| `spark_perf_schema` | `SPARK_PERF_SCHEMA` | スキーマ名 |
| `spark_perf_table_prefix` | `SPARK_PERF_TABLE_PREFIX` | テーブル名プレフィックス |
| `spark_perf_http_path` | `SPARK_PERF_HTTP_PATH` | SQL WarehouseのHTTPパス |

## プロジェクト構成

```
dbsql_profiler_analysis_tool/
├── README.md                 # 英語版README
├── README.ja.md              # このファイル
├── pyproject.toml            # プロジェクト設定
├── dabs/                     # Databricks Asset Bundles
│   ├── databricks.yml        # バンドル設定（変数 + dev/prod ターゲット）
│   ├── resources/
│   │   ├── jobs.yml          # Spark Perf Pipelineジョブ（サーバーレス）
│   │   └── apps.yml          # Web UIアプリデプロイ
│   ├── notebooks/            # ETLノートブック
│   │   ├── 01_Spark Perf Pipeline PySpark.py
│   │   └── 02_generate_summary_notebook.py
│   └── app/                  # Flask Web UI + CLI
│       ├── app.py            # Flaskアプリ（Blueprint構成）
│       ├── app.yaml          # Databricks Apps設定
│       ├── routes/           # Flask Blueprints
│       ├── cli/              # CLIエントリーポイント
│       ├── core/             # 分析ロジック
│       ├── services/         # Deltaテーブルリーダー/ライター
│       ├── templates/        # HTMLテンプレート
│       └── tests/            # ユニットテスト（1078+）
├── eval/                     # SQL精度評価フレームワーク (v4.26)
│   ├── cli.py                # CLIエントリーポイント (python -m eval)
│   ├── runner.py             # パイプライン実行 + スコアリング
│   ├── diff_runner.py        # --diff-from Before/After比較（git worktree）
│   ├── scorers/              # L1構文、L2根拠、L3/L4 LLM-as-judge
│   ├── fixtures/             # プロファイルJSONテストデータ
│   └── tests/                # Evalユニットテスト（44+）
├── scripts/
│   ├── deploy_views.py       # Genie用SQLビューのデプロイ
│   └── eval_models.py        # LLMモデル評価
├── docs/                     # ドキュメント
├── TODO.md                   # 改善バックログ
└── CLAUDE.md                 # 開発ガイドライン
```

### キーマッピングリファレンス

プロファイルJSONの全キーと`extractors.py`での使用状況は [docs/key-mapping.md](docs/key-mapping.md) を参照してください。

## SQLビューのデプロイ

Genie（Text2SQL）連携用のキュレーションSQLビューをデプロイ:

```bash
cd scripts

# 指定のカタログ/スキーマにビューをデプロイ
uv run python deploy_views.py --catalog my_catalog --schema profiler

# テーブルをリセットして再デプロイ
uv run python deploy_views.py --catalog my_catalog --schema profiler --reset-tables
```

7つのSQLビューが作成されます:

| ビュー | 説明 |
|--------|------|
| `vw_latest_analysis_by_fingerprint` | SQLフィンガープリントごとの最新分析 |
| `vw_comparison_diff` | 比較プロファイル間のメトリクス差分 |
| `vw_regression_candidates` | パフォーマンスリグレッションのあるクエリ |
| `vw_genie_profile_summary` | Genie用の簡略化プロファイルサマリー |
| `vw_genie_comparison_summary` | Genie用の簡略化比較サマリー |
| `vw_genie_recommendations` | Genie用のアクション可能な推奨事項 |
| `vw_variant_ranking` | ガードレール付き重み付きバリアントランキング |

## Genie連携

Genie Spaceを設定して自然言語で分析結果をクエリする方法は [docs/genie-space-setup.md](docs/genie-space-setup.md) を参照してください。

## デプロイ（Databricks Asset Bundles）

全リソースは [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/) と `deploy.sh` スクリプトでデプロイします。

### クイックスタート

```bash
# 1. local-overrides.yml をコピーして編集（初回のみ）
cp dabs/local-overrides.yml.sample dabs/local-overrides.yml
# dabs/local-overrides.yml を編集して warehouse_id, catalog, schema 等を設定

# 2. デプロイ（設定生成 → バンドルデプロイ → アプリ起動 → 権限付与を一括実行）
./scripts/deploy.sh dev       # dev環境
./scripts/deploy.sh staging   # staging環境
./scripts/deploy.sh prod      # 本番環境
```

`deploy.sh` は以下を自動的に実行します：
1. `local-overrides.yml` から `runtime-config.json` を生成
2. SQL Warehouseリソース宣言付きの `app.yaml` を生成
3. カタログ/スキーマの自動作成、SP書き込み権限付与 (v4.15)
4. Databricks Asset Bundle をデプロイ
5. アプリを起動
6. アプリのサービスプリンシパルにSQL Warehouseの `CAN_USE` 権限を自動付与
7. ポストデプロイスモークテストを自動実行（API 31チェック + UI 12チェック） (v4.16)

> **設定変更は `dabs/local-overrides.yml` のみで行います。** `runtime-config.json` や `app.yaml` は自動生成されるため、手動編集は不要です。

### リソースの個別実行

```bash
cd dabs

# Spark Perf ETLパイプライン実行（01_Pipeline → 02_Summary）
databricks bundle run spark_perf_pipeline

# Web UIアプリの起動（フルデプロイなし）
databricks bundle run profiler_app
```

### 設定変数

全変数は `dabs/local-overrides.yml` のターゲットごとに設定します：

| 変数 | 説明 | 例 |
|------|------|-----|
| `sparkperf_catalog` | Spark Perfカタログ | `main` |
| `sparkperf_schema` | Spark Perfスキーマ | `base2` |
| `dbsql_catalog` | DBSQLプロファイラカタログ | `my_catalog` |
| `dbsql_schema` | DBSQLプロファイラスキーマ | `dbsql_profiler` |
| `warehouse_id` | SQL Warehouse ID | `your-warehouse-id` |
| `log_root` | Sparkイベントログパス | `/Volumes/main/base/data/...` |
| `cluster_id` | ログ対象のクラスタID | `your-cluster-id` |
| `app_name` | Databricks Appsアプリ名 | `your-app-name` |

> **Note:** シークレットの設定は不要です。Databricks Appsはサービスプリンシパル認証（SDK auth）を自動的に提供します。デプロイスクリプトがSQL Warehouseへの `CAN_USE` 権限を自動付与します。

### Web UIの機能

- **ファイルアップロード**: ドラッグ＆ドロップまたはファイル選択でJSONをアップロード
- **Markdownレポート閲覧**: 以前生成したMarkdownレポートをアップロードして閲覧・共有
- **LLM分析オプション**:
  - Primary/Review/Refine モデルの選択（Web UIは3段階モデル選択に対応）
  - LLM分析のスキップ（メトリクスのみ）
- **分析結果表示**:
  - ボトルネック指標のビジュアライズ
  - 検出された問題の一覧
  - LLM分析レポート
- **分析履歴** (`/history`): フィルタリングと検索による過去の分析閲覧 (v3)
- **サイドバイサイド比較** (`/compare`): メトリクス方向指標付きの2分析のビジュアル差分 (v3)
- **設定の永続化**: カタログ、スキーマ、HTTPパスを`~/.dbsql_profiler_config.json`に保存 (v3)
- **Sparkパフォーマンス分析** (`/spark-perf`): Chart.jsチャート、KPIカード、ボトルネック分類、LLMナラティブサマリー、バイリンガルレポート（ja/en）、LLMモデルセレクター、アプリケーションページネーション (v4/v4.13)
- **Genieチャット** (`/genie-chat`): Genie Conversation APIチャットパネル（DBSQL/Spark Perf/比較画面） (v4.14)
- **Spark比較**: Sparkアプリ間のBefore/After比較、5段階Verdict、履歴、レポート閲覧 (v4.14)
- **インライン編集**: 結果画面からExperiment/Variantを直接編集、カスケード更新 (v4.14)
- **OBO認証**: 読み取り/LLMはOn-Behalf-Of、書き込み/Genie/ジョブはSP認証 (v4.15)
- **スキーマバリデーション**: DBSQL/Spark Perf設定保存時の必須フィールドバリデーション (v4.13.5)
- **エクスポート**: Markdown形式でダウンロード、印刷/PDF保存
- **ダークモード**: 自動検出＋手動切り替え
- **言語切り替え**: ヘッダーの**EN/JA**トグルをクリック
- **ナビゲーション**: Analyze | View Report | Compare | **Spark Perf** | EN/JA | Theme
- **セキュリティ**: Markdown出力はXSS攻撃を防ぐためサニタイズされます

### ローカルでの実行

開発・テスト用にローカルで実行する場合:

```bash
# 環境変数の設定
export DATABRICKS_HOST="https://xxx.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-token>"

# Flaskアプリの起動
cd dabs/app
uv run flask --app app.py run --host 0.0.0.0 --port 8000

# ブラウザで http://localhost:8000 にアクセス
```

## チューニングガイド

詳細なチューニング方法については [dbsql_tuning.md](dabs/app/core/knowledge/dbsql_tuning.md) を参照してください。

主なトピック:
- I/Oの効率化（パーティショニング、Z-Order、Liquid Clustering）
- 実行プランの改善（結合タイプ、Sparkパラメータ）
- Shuffle最適化（AQE設定、REPARTITIONヒント）
- スピル対策
- Photon利用率の改善
- クラスタサイズの調整

## DBSQLコスト推定

ウェアハウス構成とDBU価格に基づくクエリ単位のコスト推定:

| クラスタサイズ | DBU/時間 | Serverless ($0.70) | Pro ($0.55) | Classic ($0.22) |
|--------------|----------|-------------------|-------------|-----------------|
| 2X-Small | 2 | $1.40/h | $1.10/h | $0.44/h |
| X-Small | 4 | $2.80/h | $2.20/h | $0.88/h |
| Small | 8 | $5.60/h | $4.40/h | $1.76/h |
| Medium | 16 | $11.20/h | $8.80/h | $3.52/h |
| Large | 32 | $22.40/h | $17.60/h | $7.04/h |
| X-Large | 64 | $44.80/h | $35.20/h | $14.08/h |
| 2X-Large | 128 | $89.60/h | $70.40/h | $28.16/h |
| 3X-Large | 256 | $179.20/h | $140.80/h | $56.32/h |
| 4X-Large | 512 | $358.40/h | $281.60/h | $112.64/h |

> 価格はPremiumティア、us-west-2、Pay-As-You-Go。Serverlessはクエリ単位課金、Classic/Proは時間当たりコストの推定クエリシェアを表示。

ウェアハウスAPIが利用不可の場合、parallelism ratio（task_total_time_ms / execution_time_ms）からコストを推定し、最も近いTシャツサイズの参考コストテーブルを表示します。

## 対応入力ファイル

| ファイルタイプ | 説明 | 必須 |
|---------------|------|------|
| クエリプロファイルJSON | DBSQLのQuery Profileからダウンロード | はい |
| EXPLAIN EXTENDED | `EXPLAIN EXTENDED <クエリ>` のテキスト出力 | 推奨 |
| Markdownレポート（Web UIのみ） | 以前生成したレポートの閲覧用 | いいえ |

> **Note:** DBSQLとSpark Connect両方のプロファイル形式に対応しています。

## 既知の制約

- **リスケジュールスキャン率**: スキャンタスクのローカリティデータがプロファイルに含まれる場合のみ表示
- **キャッシュヒット率**: 初回クエリ実行時（コールドキャッシュ）は低い値になる場合がある
- **Photonブロッカー**: 完全な検出にはEXPLAIN EXTENDEDが必要。それ以外は基本的なヒューリスティクスを使用
- **SQLパース**: 非常に大きいまたは複雑なSQLはフォーマット時にトークン制限を超える場合がある

## トラブルシューティング

### LLM分析がスキップされる

```
Warning: DATABRICKS_HOST and DATABRICKS_TOKEN not set, skipping LLM analysis
```

**対処法:** 環境変数を設定してください（Step 2参照）

### dbsql_tuning.md not found

```
Warning: dbsql_tuning.md not found, analysis will proceed without tuning guidelines
```

**対処法:** `--tuning-file` オプションでファイルまたはディレクトリを指定してください。通常は `dabs/app/core/knowledge/dbsql_tuning.md` に配置されています

### JSONパースエラー

**対処法:** ダウンロードしたJSONファイルが正しい形式か確認してください。Databricks Query Profileから直接ダウンロードしたファイルを使用してください。

## ライセンス

このプロジェクトは Apache License 2.0 の下でライセンスされています。詳細は [LICENSE](LICENSE) ファイルを参照してください。

## 貢献

変更を投稿する前に [CONTRIBUTING.md](CONTRIBUTING.md) を必ず読んでください。

**チームルール（厳守）:**
- **`main` への直接 push 禁止** — 必ず PR 経由
- **セルフマージ禁止** — 人間のレビューが最低1人必要（Codex レビューだけでは不十分）
- **CI 赤のままマージ禁止** — 5ジョブ全て（Lint / Build / Type Check / Validate / Test）が緑であること

バグ報告や機能要望は [Issues](https://github.com/akuwano/databricks-perf-toolkit/issues) へお願いします。
