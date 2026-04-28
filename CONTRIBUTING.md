# Contributing Guide

## 🚀 Getting Started (初日向け)

### 1. ローカルセットアップ

```bash
# repo clone
git clone https://github.com/akuwano/databricks-perf-toolkit.git
cd databricks-perf-toolkit

# 自分用の設定ファイルを作成
cp dabs/local-overrides.yml.sample dabs/local-overrides.yml
# dev環境の catalog / schema / warehouse_id / app_name を自分用に書き換える

# 依存インストール（社内 PyPI proxy 経由 — VPN必須）
uv sync --default-index https://pypi.org/simple

# テストが通ることを確認
cd dabs/app && uv run pytest
```

### 2. Databricks CLI 認証

```bash
databricks auth login --host <workspace-url>
# profile: DEFAULT で登録
```

### 3. 初回デプロイで動作確認

```bash
./scripts/deploy.sh dev
# 最後に URL が表示される → ブラウザで動作確認
```

### 4. 作業開始

```bash
git checkout main && git pull
git checkout -b feat/<topic>    # または fix/<topic>, refactor/<topic>
# コードを書く → テスト → コミット → push → PR
```

---

## 📋 PR workflow

### ブランチ命名
- `feat/<topic>` — 新機能
- `fix/<topic>` — バグ修正
- `refactor/<topic>` — リファクタ
- `chore/<topic>` — バージョン更新、CI設定等
- `test/<topic>` — テスト追加のみ

日付型（`feature/20260418-1`）は非推奨。内容がすぐ分かる名前に。

### PR サイズの目安
| サイズ | 行数 | レビュー所要時間 |
|--------|------|-----------------|
| 推奨 | < 500 | 30分以内 |
| 許容上限 | < 1000 | 1時間 |
| **分割提案** | **≥ 1000** | - |

**tidy first**: 整形とロジック変更は**別コミット**。1PR に混ぜても OK だが、レビュアーが差分を読みやすいように順序を整える。

### マージ条件
- [x] CI 全ジョブ緑（Lint / Build / Type Check / Validate / Test）
- [x] 1 人以上のレビュー（セルフマージ不可 — チームに入ってもらう）
- [x] Codex に見せる PR の場合、指摘事項は解消してから merge
- [x] main へ force-push 禁止

### ⚠️ チームルール（厳守）

本リポジトリは GitHub Free の private repo のため、branch protection は技術的に強制できません。以下は **運用ルール（gentlemanly agreement）** です:

1. **main への直接 push 禁止** — 必ず PR 経由でマージ
2. **セルフマージ禁止** — 自分の PR を自分で merge しない（Codex の review だけでは不十分、人間のレビューを1人以上）
3. **CI 赤のまま force merge 禁止** — `test` ジョブが赤いのに "Merge pull request" は押さない
4. **`main` へ `--force` push は破壊的なので禁止** — 緊急時でもリードに相談

違反を見つけたら相互に声を掛け合う。継続的に破られる場合は GitHub Pro / 別の org への移管を検討。

### Squash vs Merge
- 基本 **Merge commit**（履歴を残す）
- 小さな fix で commit を1個にまとめたい場合のみ Squash

---

## 🚢 Deploy process

| 環境 | トリガー | 担当 |
|------|---------|-----|
| **Dev** | `./scripts/deploy.sh dev` — PR merge 後に手動で | PR 作者 |
| **Staging** | `./scripts/deploy.sh staging` — smoke test 用に手動で | PR 作者 or リード |
| **Prod** | `./scripts/deploy.sh prod` — リリース時のみ | リード合意の上で |

**リリース時の手順**:
```bash
# 1. pyproject.toml の version を上げる
#    feat → minor bump (5.13.0 → 5.14.0)
#    fix  → patch bump (5.13.0 → 5.13.1)
# 2. コミット、push、merge
# 3. タグ打ち
git tag v5.14.0 && git push origin main --tags
# 4. prod deploy
./scripts/deploy.sh prod
```

---

## 認証ルール

**全API呼び出しはSP（Service Principal）認証を使用する。** OBO（On-Behalf-Of）は廃止済み（v4.29.0）。

| 操作 | 認証方式 | 理由 |
|------|---------|------|
| SQL読み取り（table_reader, spark_perf_reader） | **SP** | `services.get_sp_sql_connection()` を使用。deploy.shでSELECT権限付与 |
| SQL書き込み（table_writer） | **SP** | テーブル作成権限をSPに集約。deploy.shでGRANT |
| Genie API（Space作成、会話、ポーリング） | **SP** | GenieClient内でSDK認証を使用 |
| ジョブトリガー（run_now） | **SP** | deploy.shでCAN_MANAGE_RUN付与 |
| LLM（Foundation Model API） | **SP** | SDK Config()で認証。Serving Endpoint権限をSPに付与 |
| 設定バリデーション（SHOW SCHEMAS等） | **SP** | SPの権限で確認 |

**重要**: 新しいAPI呼び出しを追加する際は、SP認証を使用してください。必要な権限は `deploy.sh` で付与します。

## コーディングルール

### エラーハンドリング
- `except: pass` **禁止** → 必ず `logger.warning()` または `logger.exception()` でログを出す
- ユーザーに見せるエラーは具体的なメッセージを返す（`"Failed"` ではなく原因を含める）

### セキュリティ
- テーブル名・カタログ名・スキーマ名は `core.sql_safe.validate_identifier()` または `core.sql_safe.safe_fqn()` 経由で必ずバリデーション
- SQLパラメータは `:param_name` バインドを使用（f-stringでユーザー入力を埋め込まない）
- テーブルFQNの組み立ては `safe_fqn(catalog, schema, table)` を使用

### 設定管理
- モデル名のハードコード **禁止** → 設定パラメータまたはUIセレクターで渡す
- `databricks.yml` にプロファイル名・環境固有値を入れない → `local-overrides.yml` で管理
- `runtime-config.json` は `.gitignore` 対象（deploy.shが自動生成）
- ノートブックウィジェットのデフォルト値にモデル名を入れるのは許容（ジョブパラメータで上書き可能なため）
- 設定の優先順位: 環境変数 > user_config（UI Save） > runtime-config.json > デフォルト値。UIでSaveした値がdeploy時設定を上書きする点に注意（「Reset to Deploy Defaults」ボタンでリセット可能）

### スキーマ変更（テーブルカラム追加等）

#### 基本ルール
- **カラム追加のみ**。削除・リネーム・型変更は禁止
- 新カラムは **NULL許容**（既存行に影響しない）
- DDLテンプレートとマイグレーション関数は**同じPRで更新**

#### DBSQL Profiler テーブル（`table_writer.py`）
1. `table_writer.py` のDDLテンプレートを**最新スキーマに更新**（新規環境用）
2. 既存環境向けに `_migrate_*_columns()` 関数で `ALTER TABLE ADD COLUMN` を追加
3. マイグレーション関数にはバージョンコメントを付ける:
   ```python
   def _migrate_header_columns(self, cursor, fqn):
       """Add columns added in v4.14."""
       new_columns = {"experiment_id": "STRING", "variant": "STRING"}
   ```

#### Spark Comparison テーブル（`spark_comparison_writer.py`）
- DBSQL Profilerと同じパターン（DDLテンプレート + マイグレーション関数）

#### Spark Gold テーブル（ノートブック）
- Delta Schema Evolution (`mergeSchema=true`) に任せる
- **新規PRでは**ノートブック内の `ALTER TABLE ADD COLUMN` は使わない
- `MERGE` / `overwrite` 時にDataFrameのスキーマが自動反映される
- 注: 既存ノートブックに `ALTER TABLE` が残っている箇所がありますが、新規変更では使用しないでください

#### やってはいけないこと
- カラムの削除（`ALTER TABLE DROP COLUMN`）
- カラムのリネーム
- カラムの型変更（`STRING` → `INT` 等）
- DDLテンプレートを更新せずにマイグレーション関数だけ追加

### 国際化 (i18n)
- ユーザーに見えるテキストは `{{ _('English text') }}` で囲む
- 新規文字列は `translations/ja/LC_MESSAGES/messages.po` に日本語訳を追加
- `uv run pybabel compile -d translations` で `.mo` ファイルを再生成

### Spark Perfレポート
- レポーターの日本語ラベルは `_LABELS` 辞書 + `_L(key, lang)` ヘルパーを使用
- テンプレート文字列のハードコード禁止 → `_LABELS` に追加して `_L()` で参照

## デプロイ

### 環境構成
| 環境 | コマンド | アプリ名 |
|------|---------|---------|
| dev | `./scripts/deploy.sh dev` | dbsql-profiler-analyzer-dev |
| staging | `./scripts/deploy.sh staging` | databricks-perf-kit-staging |
| prod | `./scripts/deploy.sh prod` | databricks-perf-kit |

### deploy.shが自動で行うこと
1. `runtime-config.json` 生成（`local-overrides.yml` から）
2. `app.py` にバージョン注入（`pyproject.toml` から）
3. `databricks bundle deploy`
4. Job ID解決 → `runtime-config.json` に注入 → 再デプロイ
5. アプリ起動
6. カタログ/スキーマ作成（`CREATE IF NOT EXISTS`）
7. SP権限付与（USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY, SELECT）
8. ジョブ権限付与（CAN_MANAGE_RUN）

### DABが自動で行うこと（`apps.yml`）
- SQL Warehouse CAN_USE
- App CAN_USE（全ユーザー）
- OBOスコープ（sql, apps）

### 依存パッケージのキャッシュ更新

`deploy.sh` は `uv run --offline` で実行されるため、ローカルのuvキャッシュにパッケージが存在する必要がある。
以下のタイミングで **キャッシュの温め直し** が必要:

- `uv.lock` を更新した後（依存追加・更新・再生成）
- 新しいマシンで初めてデプロイする場合

```bash
# Databricks PyPI proxy経由でキャッシュを温める（VPN接続必須）
uv sync --default-index https://pypi.org/simple

# 確認: --offline で動くことをチェック
uv run --offline --no-group test --no-group lint --no-group ui-smoke python3 -c "print('ok')"
```

> **Note**: 社内ネットワークでは `pypi.org` に直接到達できない。必ず Databricks PyPI proxy を使用する。
> 利用可能なプロキシ:
> - pypi.org → `https://pypi.org/simple`
> - download.pytorch.org → `https://pypi-pytorch-proxy.dev.databricks.com`
> - pypi.nvidia.com → `https://pypi-nvidia-proxy.dev.databricks.com`

### 初回セットアップ
```bash
cp dabs/local-overrides.yml.sample dabs/local-overrides.yml
# local-overrides.yml を編集
uv sync --default-index https://pypi.org/simple
./scripts/deploy.sh dev
```

## テスト

### TDD（テスト駆動開発）

新規モジュール・機能は以下のサイクルで実装する:

1. **Red**: 失敗するテストを書く — 期待する動作を定義
2. **Green**: 最小限の実装でテストを通す
3. **Refactor**: テストが通る状態を維持してコードを改善

### テスト必須ルール

| 変更種別 | テスト要件 |
|---------|----------|
| **新規モジュール（.py）** | **必須** — テストなしでマージしない |
| **新規APIエンドポイント** | **必須** — 正常系 + エラー系のテスト |
| **既存モジュールの修正** | **推奨** — 影響範囲のテスト追加 |
| **バグ修正** | **必須** — 再発防止のリグレッションテスト |
| **テンプレート/CSS/JS変更** | 不要（手動確認） |
| **設定・ドキュメント変更** | 不要 |

### PRマージ前の必須チェック
- `cd dabs/app && uv run pytest` 全パス
- 新規テストが追加されている（上記ルールに該当する場合）

### 実行方法
```bash
cd dabs/app
uv run pytest                          # 全テスト
uv run pytest tests/test_xxx.py -v     # 特定モジュール
uv run pytest -x --tb=short            # 最初の失敗で停止
uv run pytest --co -q                  # テスト一覧（実行なし）
```

### テストの書き方
- テストファイルは `tests/test_{module_name}.py` に配置
- テストクラスは `Test{FeatureName}` で命名
- モック: 外部依存（SQL接続、LLM、Genie API）は `unittest.mock.patch` でモック
- フィクスチャ: 共通のテストデータは `conftest.py` または各テストファイル内のヘルパー関数で定義

## コミット規約

Conventional Commits形式:
```
<type>(<scope>): <description>

feat(spark-perf): add comparison page
fix(deploy): grant CREATE TABLE to SP
refactor(settings): extract validation logic
docs(readme): update architecture diagram
chore(deps): bump version to v4.15.0
```

| type | 用途 |
|------|------|
| feat | 新機能 |
| fix | バグ修正 |
| refactor | リファクタリング |
| docs | ドキュメント |
| chore | バージョン更新等 |
| test | テスト追加・修正 |

## PRのチェックリスト

- [ ] 認証方式が上記ルールに従っている
- [ ] `except: pass` がない（ログ出力あり）
- [ ] SQLインジェクション対策（`validate_identifier` / パラメータバインド）
- [ ] モデル名がハードコードされていない
- [ ] `databricks.yml` に環境固有値が入っていない
- [ ] ユーザー向けテキストが `{{ _() }}` で囲まれている
- [ ] テスト全パス
- [ ] 新規エンドポイントにテストあり

## バージョニング

- `pyproject.toml` の `version` が唯一の正（Single Source of Truth）
- deploy.shが `app.py` の `APP_VERSION` に自動注入
- タグ: `git tag v4.x.x && git push origin main --tags`
- Web UIのバージョン表示は `pyproject.toml` から自動反映
