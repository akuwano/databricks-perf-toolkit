# DBSQL Profiler Analysis Tool

Databricks SQL のクエリプロファイルと Spark ジョブ性能を分析し、改善アクションの提示・比較実験・Web UI を提供する実用ツールキットです。

[English README](README.md)

## What It Does

- Databricks SQL の query profile JSON と任意の `EXPLAIN` を解析し、ボトルネックと根本原因を整理して改善案を出します。
- Spark イベントログ ETL の結果を Delta テーブル、LLM ナラティブ、ダッシュボード向けデータとしてキュレーションします。
- DBSQL / Spark の両方で experiment / variant / family 単位の比較を行い、リグレッションと改善を計測します。
- 日本語 / 英語の知識ベース、Flask Web UI、SQL 精度評価フローを組み合わせ、継続的なチューニングを支援します。

## Features

### DBSQL クエリプロファイル分析

本ツールの中心機能。signal 抽出、ルールベース ActionCard registry、3 ステージ LLM を組み合わせて分析します。

**主な特徴**

- **22 ActionCards (単一 registry)**
  - ルールベース提案は 22 枚の canonical registry から生成されます。
  - `federation_query`、`cluster_underutilization`、`compilation_absolute_heavy` を含みます。
  - バージョン別ロジックではなく priority 順で発火します。
- **3 ステージ LLM**
  - Stage 1: initial structured analysis
  - Stage 2: review
  - Stage 3: refine
  - LLM 提案は root-cause grouping でルールベース提案と重複排除されます。
- **`EXPLAIN` 連携**
  - `EXPLAIN EXTENDED` を追加入力として受け取り、plan 構造の根拠を補強します。
- **JOIN key の暗黙 `CAST` 検出**
  - Profile / `EXPLAIN` から join key の `CAST(...)` を検出し、隠れた性能問題を可視化します。
- **Lakehouse Federation 対応**
  - Federation scan を検出し、federated workload に不適切な提案を suppression します。
- **Streaming 対応**
  - DLT/SDP ストリーミングの context を検出し、分析・レポートに反映します。
- **Serverless 対応**
  - Serverless 実行を検出し、提案を環境に合わせて調整します。
- **コスト推定**
  - DBU pricing helper とクエリメトリクスから概算コストを算出します。

**DBSQL 分析のカバー範囲**

- クエリレベルのメトリクスとボトルネック指標
- 物理オペレータのホットスポット
- Shuffle / skew 関連
- Spill / メモリ圧迫
- Photon ブロッカーと低 Photon 利用
- Scan 効率とファイルプルーニング
- Compilation overhead (absolute-heavy 含む)
- Driver / queue / scheduling overhead
- Federation 固有の扱い
- Clustering 関連提案
- 統計の鮮度と SQL パターン

**22 ActionCards**

`disk_spill`、`federation_query`、`shuffle_dominant`、`shuffle_lc`、`data_skew`、`low_file_pruning`、`low_cache`、`compilation_overhead`、`photon_blocker`、`photon_low`、`scan_hot`、`non_photon_join`、`hier_clustering`、`hash_resize`、`aqe_absorbed`、`cte_multi_ref`、`investigate_dist`、`stats_fresh`、`driver_overhead`、`rescheduled_scan`、`cluster_underutilization`、`compilation_absolute_heavy`

**Root-cause grouping**

16 個の root-cause group により、ルールベース提案と LLM 提案を統合的に整理します。主なカテゴリは spill / memory pressure、shuffle overhead、data skew、scan efficiency、cache utilization、Photon compatibility、SQL pattern、statistics freshness、driver overhead、federation、cluster underutilization、compilation overhead / compilation absolute など。これにより最終アクションプランが冗長にならずに済みます。

### Spark ジョブ性能分析

Databricks ノートブック、Delta テーブル、LLM サマリーによる Spark 性能分析パイプライン。

**主な特徴**

- **ETL ノートブック**
  - Spark イベントログを取り込み、Bronze / Silver / Gold の Delta テーブルを作成します。
- **LLM ナラティブサマリー**
  - 生メトリクスではなく、読みやすい性能説明を生成します。
- **Lakeview ダッシュボード対応**
  - Gold テーブルを入力とするダッシュボード生成ノートブックを含みます。
- **コスト列**
  - Gold 出力に DBU / USD のコスト関連カラムを含みます。

**Spark 側の分析対象**

- application / job / stage / executor のサマリー
- concurrency / workload ビュー
- spot / SQL / Photon / streaming 系 view
- ナラティブ生成と更新
- comparison history と comparison report
- ETL run / summary run の Web UI からのトリガーとステータス参照

### 比較と実験

DBSQL / Spark 両方で比較ワークフローが可能。

- **DBSQL / Spark 比較**: 方向認識型メトリクスで 2 つの分析結果を左右比較
- **Experiment / Variant 管理**: 分析に `baseline` や `candidate_a` などの variant を付与
- **Family grouping**: 類似クエリや関連ワークロードを family として束ね、意味のある before/after 比較を可能に
- **比較履歴の永続化**: 比較結果を Delta に保存し、UI からいつでも参照

### ナレッジベース

プロンプトに注入される topic 別のバイリンガル知識ベース。

- **7 トピック × JA / EN**
  - DBSQL tuning、DBSQL advanced、DBSQL SQL patterns、DBSQL serverless、DBSQL Photon OOM、Spark tuning、Spark streaming tuning
- **`section_id` ルーティング**
  - ドキュメント全体ではなく、必要なセクションのみを prompt に注入
- **Knowledge-assisted analysis**
  - 自由生成ではなく、整理済みガイダンスに基づいた提案

### Web UI

Flask ベースの Web UI。

- 分析アップロードと結果閲覧 (`/analyze`、`/history`、`/report`)
- 左右並列比較 (`/compare`、`/workload`)
- Spark 性能ページ (`/spark-perf`): ETL トリガー、レポート、比較、run ステータス
- スキーマ分析 (`/schema-analysis`)
- catalog / schema / warehouse の設定管理
- Genie Chat パネル、SQL 最適化の Rewrite / Refine フロー

### SQL 精度評価

`eval/` 配下に SQL 品質とレポート品質の評価ワークフローを用意。

- **4 軸スコアリング**: L1 syntax、L2 evidence grounding、L3 diagnosis accuracy (LLM-as-judge)、L4 fix effectiveness
- **Diff runner**: git ref 間で worktree を使った前後比較
- **Scorers / fixtures**: プロンプトやモデル変更の継続的な検証

## Requirements

- Python `3.11` 以上
- LLM を使う場合は Databricks workspace
  - `DATABRICKS_HOST` と `DATABRICKS_TOKEN`
- 永続化 / ダッシュボード / Databricks Apps デプロイを使う場合は SQL warehouse / catalog / schema

主要依存: `openai`、`sqlparse`、`sqlglot`、`requests`、`pyyaml`
Web UI 追加: `flask`、`flask-babel`、`markdown`
開発 / 検証用: `pytest`、`pytest-cov`、`mypy`、`playwright`、`babel`

## Quick Start

Databricks App として配備することも、ローカル CLI として使うこともできます。

### 1. Clone と install

```bash
git clone https://github.com/akuwano/databricks-perf-toolkit.git
cd databricks-perf-toolkit

python -m venv .venv
source .venv/bin/activate

pip install -e ".[web]"
```

開発 / テスト / lint も入れる場合:

```bash
pip install -e ".[web,test,lint,ui-smoke,dev]"
```

### 2. 環境変数

LLM 分析用:

```bash
export DATABRICKS_HOST="https://<your-workspace>"
export DATABRICKS_TOKEN="<your-token>"
```

出力言語 (デフォルト `en`):

```bash
export DBSQL_LANG="ja"
```

### 3. Databricks Apps デプロイの前提

最低限、以下を用意します。

- アプリが動作する Databricks workspace
- 認証情報 (`DATABRICKS_HOST` / `DATABRICKS_TOKEN`)
- 永続化 / Web UI 向けの catalog / schema / warehouse 設定
- Spark ETL / ダッシュボード向けの notebook / job 紐付け

デプロイ資産は `dabs/`、`scripts/`、`docs/` にあります。

### 4. Web UI をローカル起動

```bash
python dabs/app/app.py
# http://localhost:8000 を開く
```

### 5. CLI で最初の分析

```bash
profiler-analyzer path/to/profile.json --no-llm
```

`EXPLAIN` と LLM を併用する例:

```bash
profiler-analyzer path/to/profile.json \
  --explain path/to/explain.txt \
  --model databricks-claude-opus-4-6 \
  --review-model databricks-claude-opus-4-6
```

## CLI Usage

CLI は query profile JSON を読み、必要に応じて永続化や比較も行います。

```bash
# 基本実行
profiler-analyzer profile.json

# メトリクスのみ (LLM スキップ)
profiler-analyzer profile.json --no-llm

# EXPLAIN 付き
profiler-analyzer profile.json --explain explain.txt

# 日本語レポート
profiler-analyzer profile.json --lang ja

# LLM ステージのモデル指定
profiler-analyzer profile.json \
  --model databricks-claude-opus-4-6 \
  --review-model databricks-claude-opus-4-6 \
  --refine-model databricks-claude-opus-4-6 \
  --verbose

# Experiment / Variant の付与
profiler-analyzer profile.json \
  --experiment-id exp_2026_04 --variant baseline

# Delta への永続化
profiler-analyzer profile.json --persist

# 既存分析との比較
profiler-analyzer profile.json --compare-with <analysis-id>

# 構造化タグの付与
profiler-analyzer profile.json --tags '{"env":"prod","team":"analytics"}'

# レポートの review / refine
profiler-analyzer profile.json --report-review --refine-report
```

## Architecture

### 主要ディレクトリ

```
dabs/app/              # Flask Web UI + CLI エントリ
├── core/              # データモデル、抽出、分析、レポート、
│                      # LLM クライアント / プロンプト、比較 / family、
│                      # DBU pricing / コスト、多言語ナレッジ
├── services/          # テーブル読み書き、Spark perf リーダー / ライター、
│                      # schema-join 検出、job launcher、Genie client
├── routes/            # Flask blueprints
├── templates/         # HTML templates
├── translations/      # JA / EN po / mo
├── cli/               # profiler-analyzer エントリ
└── tests/             # アプリケーションテスト
dabs/notebooks/        # Spark ETL、サマリー生成、ダッシュボード、KB 管理
docs/                  # 設計ドキュメント (analysis pipeline、action plan、API、ops)
eval/                  # SQL 精度評価フレームワーク (L1–L4 scorers)
scripts/               # deploy、smoke test、view deploy、runtime config
```

### DBSQL 分析フロー

1. Query profile JSON をロード
2. query metrics、node metrics、bottleneck indicators を抽出
3. 22-card registry からルールベース ActionCards を生成
4. serverless / federation などの環境条件でフィルタ
5. 必要に応じて `EXPLAIN` を解析
6. LLM initial → review → refine を実行
7. root-cause group でルールベースと LLM 提案を重複排除
8. Markdown レポートを生成
9. 必要に応じて Delta へ永続化・比較

### Delta テーブル

- **DBSQL 永続化レイヤ**: 分析 / 比較 / ナレッジ用の 11 テーブル (headers、actions、table scans、hot operators、stages、raw、comparison pairs / metrics、knowledge docs / tags、metric directions)
- **Spark 性能パイプライン**: `dabs/notebooks/01_Spark Perf Pipeline PySpark.py` が生成する Bronze / Silver / Gold 構成の Delta テーブル群。Gold 層は Web UI と Lakeview ダッシュボードから参照されます。

詳細は `docs/analysis-pipeline.md` と `docs/action-plan-generation.md` を参照。

## Development

```bash
# 開発 extras のインストール
pip install -e ".[web,test,lint,ui-smoke,dev]"

# テスト
pytest                       # フル
pytest dabs/app/tests        # アプリ側のみ
pytest eval/tests            # eval のみ

# 型チェック
mypy dabs/app

# lint / format (ruff)
ruff check .
ruff format .

# UI スモーク
python scripts/ui_smoke_test.py
```

## License

Apache License 2.0. `LICENSE` を参照。
