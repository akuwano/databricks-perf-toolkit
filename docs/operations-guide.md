# Operations Guide

Databricks Performance Toolkit の運用・障害対応ガイド。

## デプロイ

### 初回セットアップ

```bash
# 1. 設定ファイルを作成
cp dabs/local-overrides.yml.sample dabs/local-overrides.yml

# 2. local-overrides.yml を編集
#    catalog, schema, warehouse_id, http_path を設定

# 3. デプロイ
./scripts/deploy.sh dev
```

### deploy.sh の動作

`deploy.sh` は3ステップを順番に実行します：

1. **runtime-config.json 生成** — `local-overrides.yml` を読んで `dabs/app/runtime-config.json` に変換
2. **bundle deploy** — Databricks Asset Bundles でワークスペースにアップロード
3. **bundle run** — アプリを起動（または再起動）

```bash
./scripts/deploy.sh dev       # dev環境
./scripts/deploy.sh prod      # prod環境
```

### 設定の優先順位

高い方が優先されます：

| 優先度 | ソース | 用途 |
|--------|--------|------|
| 1 (最高) | 環境変数 (`PROFILER_CATALOG` 等) | CI/CD、テスト |
| 2 | ユーザー設定ファイル (`~/.dbsql_profiler_config.json`) | Web UIからの変更 |
| 3 | runtime-config.json | デプロイ時のデフォルト値 |
| 4 (最低) | ハードコードデフォルト | フォールバック |

**注意**:
- Web UIで設定を変更するとユーザー設定ファイルに保存されます
- `runtime-config.json` は **デプロイのたびに上書き** されます
- ユーザー設定ファイルはコンテナ再作成で **消えます**（Databricks Apps）
- 永続的な設定変更は `local-overrides.yml` を編集して再デプロイしてください

### 設定の確認

```bash
# ブラウザまたは curl で有効な設定を確認
curl https://<app-url>/api/v1/debug/config | python -m json.tool
```

レスポンス例：
```json
{
  "settings": {
    "catalog": {"value": "my_catalog", "source": "runtime_config"},
    "http_path": {"value": "/sql/1.0/warehouses/abc123", "source": "runtime_config"},
    "table_write_enabled": {"value": "true", "source": "runtime_config"}
  },
  "config_paths": {
    "user_config": "/root/.dbsql_profiler_config.json",
    "runtime_config": "/app/runtime-config.json"
  }
}
```

---

## ログの確認

```bash
# アプリのログを表示
databricks apps logs <your-app-name> -p DEFAULT

# 直近のログだけ見る場合
databricks apps logs <your-app-name> -p DEFAULT | tail -50
```

### 注目すべきログパターン

| パターン | 意味 |
|----------|------|
| `App started successfully` | 起動成功 |
| `Loaded runtime config from` | runtime-config.json を読み込んだ |
| `Config saved to` | Web UIから設定が保存された |
| `Analysis completed: id=...` | 分析完了 |
| `Analysis failed: id=...` | 分析失敗（エラー詳細が続く） |
| `Narrative translation failed` | LLM翻訳失敗（分析自体には影響なし） |

---

## Spark Perf ETLジョブの起動

Web UIからSpark PerfのETLパイプラインを起動できます。

### セットアップ

1. **Job IDの設定**: Spark Perfページの Settings → ETL Job ID に DABsジョブのIDを入力
   - Job IDは Databricks UI の Jobs ページのURLから取得: `https://<workspace>/jobs/<job_id>`
   - または `databricks bundle summary` で確認
   - `local-overrides.yml` に `spark_perf_job_id` を設定して再デプロイでも可

2. **イベントログの準備**: Sparkクラスターのイベントログを UC Volume に配置
   - クラスター設定: `spark.eventLog.dir` → `/Volumes/<catalog>/<schema>/<volume>/cluster_logs/<cluster_id>/eventlog/`
   - または手動コピー

### 使い方

1. Spark Perfページ → 「Run ETL Pipeline」を開く
2. **Log Root**: イベントログの親ディレクトリ（例: `/Volumes/main/base/data/cluster_logs`）
3. **Cluster ID**: 分析対象のクラスターID
4. 「Run ETL」クリック → ジョブが起動、5秒ごとにステータス更新
5. 完了後、アプリ一覧が自動リロードされ分析結果が表示可能に

### 注意

- ETLジョブはノートブック2つ（ETL + LLMサマリー生成）を順次実行
- 実行時間はデータ量に依存（通常5-15分）
- 「View in Databricks」リンクでジョブ詳細を確認可能

---

## よくあるトラブルと対処

### 403 Forbidden（SQL Warehouse接続エラー）

**症状**: 分析時に `403 FORBIDDEN` エラー

**原因**: アプリの Service Principal が SQL Warehouse の CAN_USE 権限を持っていない

**対処**:
1. Databricks ワークスペースの SQL Warehouses ページを開く
2. 対象ウェアハウスの Permissions を開く
3. アプリの SP に `CAN_USE` 権限を付与

**確認**: `dabs/resources/apps.yml` に `sql_warehouse` リソース定義があることを確認

### 接続エラー（Warehouse unreachable）

**症状**: `ConnectionError` や `Timeout`

**チェックリスト**:
1. `/api/v1/debug/config` で `http_path` が設定されているか確認
2. SQL Warehouse が起動中（Running）か確認
3. `http_path` のフォーマットが `/sql/1.0/warehouses/<warehouse_id>` か確認

### 分析結果が保存されない

**症状**: 分析は完了するが履歴に表示されない

**原因**: Delta永続化が無効

**チェックリスト**:
1. 分析画面で **「Save to Delta Table」チェックボックス** がオンか確認
2. `/api/v1/debug/config` で `table_write_enabled` が `true` か確認
3. `catalog` と `schema` が存在するか確認
4. `http_path` が設定されているか確認

### Shared Linkが localhost を返す

**症状**: 共有リンクをコピーすると `http://localhost:8000/shared/...` になる

**原因**: Databricks Apps 内では `request.host_url` が `localhost` を返す

**対処**: これは仕様です。Slack Summary のコピーボタンはクライアント側の `window.location.origin` を使うため正しいURLが生成されます。

### Spark Perf が 503 を返す

**症状**: `/api/v1/spark-perf/applications` が `503 Service Unavailable`

**原因**: Spark Perf 用の HTTP path が未設定

**対処**:
1. Spark Perf ページの Settings でHTTP Path を設定
2. または `local-overrides.yml` に `spark_perf_http_path` を追加して再デプロイ

### LLM分析が空/失敗する

**症状**: レポートにLLMセクションがない、または分析が `failed` になる

**チェックリスト**:
1. Foundation Model Serving Endpoint が有効か確認
2. `databricks-claude-opus-4-6` と `databricks-gpt-5-4` エンドポイントが存在するか
3. SP に Serving Endpoint のアクセス権があるか
4. `skip_llm=true` で分析が通るか（LLM以外の問題を切り分け）

---

## バージョンアップ

```bash
# 1. pyproject.toml の version を更新
# 2. コミット
git commit -am "chore: bump version to vX.Y.Z"
# 3. タグ
git tag vX.Y.Z
# 4. push
git push origin main --tags
# 5. デプロイ
./scripts/deploy.sh dev
```

pyproject.toml の `version` が唯一の正（Single Source of Truth）。
Web UI の左下に表示されるバージョンはここから自動読み取りされます。

---

## Delta テーブル

### テーブル一覧（11テーブル）

| テーブル | 説明 |
|----------|------|
| `profiler_analysis_header` | 分析ヘッダー（メトリクス、ボトルネック指標、ウェアハウス情報） |
| `profiler_analysis_actions` | 推奨アクション（P0/P1/P2、risk、verification_steps） |
| `profiler_analysis_table_scans` | テーブルスキャン情報 |
| `profiler_analysis_hot_operators` | Hot オペレータ |
| `profiler_analysis_stages` | ステージ情報 |
| `profiler_analysis_raw` | 生プロファイルJSON |
| `profiler_comparison_pairs` | 比較ペア |
| `profiler_comparison_metrics` | 比較メトリクス（15指標） |
| `profiler_knowledge_documents` | ナレッジドキュメント |
| `profiler_knowledge_tags` | ナレッジタグ |
| `profiler_metric_directions` | メトリクス方向定義 |

### テーブルのリセット

```bash
cd scripts
uv run python deploy_views.py --catalog <catalog> --schema <schema> --reset-tables
```

**注意**: `--reset-tables` は全テーブルを DROP して再作成します。データは完全に失われます。

### Genie Space 連携

SQLビューのデプロイ：

```bash
cd scripts
uv run python deploy_views.py --catalog <catalog> --schema <schema>
```

7つのキュレーションビューが作成され、Genie Space から自然言語クエリが可能になります。

---

## マイグレーション

テーブルスキーマの変更は自動マイグレーションで対応しています：

- `_migrate_header_columns()`: `prompt_version` カラムの自動追加
- `_migrate_actions_columns()`: `risk`, `risk_reason`, `verification_steps_json` カラムの自動追加

既存テーブルに対して `ALTER TABLE ADD COLUMNS` が自動実行されるため、手動操作は不要です。
