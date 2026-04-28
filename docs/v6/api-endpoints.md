# V6 デバッグ・運用エンドポイントリファレンス

V6 で動作確認・トラブルシューティングに使う Web エンドポイントを
一箇所に集約。シナリオ別のワークフローは
[`operations.md`](operations.md) を参照。

## 1. エンドポイント一覧

### Debug / 設定

| メソッド | パス | 用途 | 認証 |
|---------|------|------|------|
| GET | [`/api/v1/debug/feature-flags`](#11-get-apiv1debugfeature-flags) | **8 V6 flag** の有効/無効 + 解決元 | Bearer (OBO/SP) |
| GET | [`/api/v1/debug/config`](#12-get-apiv1debugconfig) | catalog/schema 等の有効設定 + 解決元 | Bearer (OBO/SP) |
| GET | [`/api/v1/settings`](#13-get-apiv1settings) | DBSQL 永続化設定 (UI と同じ) | Bearer |
| GET | [`/api/v1/spark-perf/settings`](#14-get-apiv1spark-perfsettings) | Spark Perf 永続化設定 | Bearer |
| GET | `/health` | アプリ生存確認 | 不要 |

### L5 customer feedback (v6.4-1.5)

| メソッド | パス | 用途 | 認証 |
|---------|------|------|------|
| POST | `/api/v1/feedback` | 欠落申告 / per-action 改善要望投稿 | Bearer + trusted header |
| GET | `/api/v1/feedback/categories` | カテゴリ列挙 (UI 用 dropdown source) | Bearer |
| POST | `/api/v1/feedback/bundle/<analysis_id>/prepare` | per-analysis ZIP 用の HMAC signed token 発行 (5 分) | Bearer |
| GET | `/api/v1/feedback/bundle/<analysis_id>?token=&include_profile=` | per-analysis ZIP stream | signed token |
| POST | `/api/v1/feedback/bundle/bulk/prepare` | bulk ZIP token + summary (件数 / サイズ / 全期間との差分) | Bearer + admin |
| GET | `/api/v1/feedback/bundle/bulk?token=&since=&until=` | bulk ZIP stream | signed token + admin |
| GET | `/feedback/export` | bulk export ランディングページ (workspace_admin 限定) | Bearer + admin |

V6 の動作確認は **`/api/v1/debug/feature-flags`** と
**`/api/v1/debug/config`** の 2 つでほぼ完結する。
L5 feedback bundle の使い方は [`five-layer-feedback.md`](five-layer-feedback.md) §L5 参照。

---

## 1.1. GET /api/v1/debug/feature-flags

**用途**: V6 8 flag が「実際に有効か」「どこから読まれたか」を確認。

### リクエスト

```bash
APP_URL="https://dbsql-profiler-analyzer-dev-XXXXXXXX.aws.databricksapps.com"
TOKEN="$(databricks auth token --profile DEFAULT | jq -r .access_token)"

curl -s -H "Authorization: Bearer $TOKEN" \
  "$APP_URL/api/v1/debug/feature-flags" | jq .
```

ブラウザでも開ける (Databricks SSO 済セッションで):
`https://<app-url>/api/v1/debug/feature-flags`

### レスポンス例 (V6 全 flag 有効時)

```json
{
  "feature_flags": {
    "V6_CANONICAL_SCHEMA": {
      "enabled": true,
      "source": "runtime-config",
      "raw_value": "true"
    },
    "V6_REVIEW_NO_KNOWLEDGE": {
      "enabled": true,
      "source": "runtime-config",
      "raw_value": "true"
    },
    "V6_REFINE_MICRO_KNOWLEDGE": {
      "enabled": true,
      "source": "runtime-config",
      "raw_value": "true"
    },
    "V6_ALWAYS_INCLUDE_MINIMUM": {
      "enabled": true,
      "source": "runtime-config",
      "raw_value": "true"
    },
    "V6_SKIP_CONDENSED_KNOWLEDGE": {
      "enabled": true,
      "source": "runtime-config",
      "raw_value": "true"
    },
    "V6_RECOMMENDATION_NO_FORCE_FILL": {
      "enabled": true,
      "source": "runtime-config",
      "raw_value": "true"
    },
    "V6_SQL_SKELETON_EXTENDED": {
      "enabled": true,
      "source": "runtime-config",
      "raw_value": "true"
    }
  },
  "snapshot": {
    "V6_CANONICAL_SCHEMA": true,
    "V6_REVIEW_NO_KNOWLEDGE": true,
    "V6_REFINE_MICRO_KNOWLEDGE": true,
    "V6_ALWAYS_INCLUDE_MINIMUM": true,
    "V6_SKIP_CONDENSED_KNOWLEDGE": true,
    "V6_RECOMMENDATION_NO_FORCE_FILL": true,
    "V6_SQL_SKELETON_EXTENDED": true
  },
  "config_paths": {
    "runtime_config": "/app/runtime-config.json",
    "user_config": "/home/app/.dbsql_profiler_config.json"
  }
}
```

### フィールド意味

| フィールド | 値 | 意味 |
|-----------|----|------|
| `feature_flags[FLAG].enabled` | bool | 実際の真偽値 (LLM パイプラインが見るのと同じ) |
| `feature_flags[FLAG].source` | `env` / `runtime-config` / `default` | 値の出所 (優先順位順) |
| `feature_flags[FLAG].raw_value` | str | 読み取り前の生値 ("true" / "1" / "" 等) |
| `snapshot` | dict | 全 flag の `enabled` を一括 dump (デバッグ簡便用) |
| `config_paths` | dict | 設定ファイルの探索パス (どれを読みに行ったか) |

### 8 flag の意味早見表

| flag | 有効化すると |
|------|-------------|
| `V6_CANONICAL_SCHEMA` | LLM が canonical Finding/Action JSON を直接 emit |
| `V6_REVIEW_NO_KNOWLEDGE` | review stage で knowledge を渡さない (毒抑制) |
| `V6_REFINE_MICRO_KNOWLEDGE` | refine stage で micro knowledge のみ注入 |
| `V6_ALWAYS_INCLUDE_MINIMUM` | knowledge 必須 ALWAYS_INCLUDE 縮小 |
| `V6_SKIP_CONDENSED_KNOWLEDGE` | condensed knowledge をスキップ |
| `V6_RECOMMENDATION_NO_FORCE_FILL` | アクション数を強制充足しない |
| `V6_SQL_SKELETON_EXTENDED` | MERGE/VIEW/INSERT skeleton を有効化 |
| `V6_COMPACT_TOP_ALERTS` | `## 2. Top Alerts` を Section 1 末尾の `### Key Alerts` に統合 + issue-tag 参照 (v6.6.0) |

詳細: [`v6-spec.md` §5](../v6-spec.md), [`v5-vs-v6.md` §2.2](../v5-vs-v6.md)

### source の解決順序

```
env > runtime-config > default(false)
```

- `env`: コンテナ環境変数 (`V6_CANONICAL_SCHEMA=true` 等)
- `runtime-config`: `runtime-config.json` の小文字キー (`v6_canonical_schema`)
- `default`: 未設定時は `false`

`runtime-config` は `local-overrides.yml` の `v6_*` を `deploy.sh`
が変換して埋める。env を **意図的に** 設定する運用は通常無し。

### よくある異常パターン

| `enabled` | `source` | `raw_value` | 意味 |
|-----------|----------|-------------|------|
| `true` | `runtime-config` | `"true"` | ✅ 正常 (`local-overrides.yml` 経由) |
| `true` | `env` | `"1"` | ⚠ 意図せず env が効いている可能性 |
| `false` | `runtime-config` | `"false"` | 明示的に off (調査用以外では非推奨) |
| `false` | `runtime-config` | `""` | 空文字 (`local-overrides.yml` のキーを再確認) |
| `false` | `default` | `""` | local-overrides.yml に該当キーなし |

---

## 1.2. GET /api/v1/debug/config

**用途**: catalog/schema/HTTP path 等の **アプリ設定** がどこから読まれているかを確認。
V6 とは独立な設定だが、`runtime-config.json` が正しく配置されたかの最初の sanity check に有用。

### リクエスト

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "$APP_URL/api/v1/debug/config" | jq .
```

### レスポンス例

```json
{
  "settings": {
    "catalog": {"value": "main", "source": "runtime-config"},
    "schema":  {"value": "dbsql_profiler", "source": "runtime-config"},
    "http_path": {"value": "/sql/1.0/warehouses/abc...", "source": "runtime-config"},
    "table_write_enabled": {"value": "true", "source": "runtime-config"},
    "spark_perf_catalog": {"value": "main", "source": "runtime-config"},
    "spark_perf_schema":  {"value": "base2", "source": "runtime-config"},
    "spark_perf_table_prefix": {"value": "PERF_", "source": "default"},
    "spark_perf_http_path": {"value": "", "source": "default"},
    "spark_perf_etl_job_id": {"value": "0", "source": "default"},
    "spark_perf_summary_job_id": {"value": "0", "source": "default"},
    "genie_space_id": {"value": "", "source": "default"},
    "dbsql_genie_space_id": {"value": "", "source": "default"}
  },
  "config_paths": {
    "runtime_config": "/app/runtime-config.json",
    "user_config": "/home/app/.dbsql_profiler_config.json"
  }
}
```

### source 解決順序 (settings 系)

```
env > user_config > runtime_config > default
```

`user_config` は UI の「設定」画面で保存した値、`runtime_config` は
`deploy.sh` が `local-overrides.yml` から生成した値。

---

## 1.3. GET /api/v1/settings

UI の「DBSQL 設定」フォームと同じ値を返す (catalog/schema/HTTP path/Genie space 等)。
JSON の生値を見たいときに使う。

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$APP_URL/api/v1/settings" | jq .
```

## 1.4. GET /api/v1/spark-perf/settings

Spark Perf 用の永続化設定。

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$APP_URL/api/v1/spark-perf/settings" | jq .
```

---

## 2. 認証

Databricks Apps の HTTP エンドポイントは Workspace 内ユーザーの
PAT または OAuth トークンを `Authorization: Bearer ...` で受け付ける。

### PAT (CLI 経由)

```bash
TOKEN="$(databricks auth token --profile DEFAULT | jq -r .access_token)"
```

### ブラウザ (SSO セッション)

`https://<app-url>/api/v1/debug/feature-flags` をブラウザで直接開ける。
Workspace に SSO 済ならそのまま JSON が返る。

### 環境変数

```bash
DATABRICKS_TOKEN="dapi..."
DATABRICKS_HOST="https://...cloud.databricks.com"
```

`local-overrides.yml` の `workspace.profile` を切り替えれば
`databricks auth token --profile <name>` で別環境の token も取れる。

---

## 3. 典型的な使い方

### 3.1. デプロイ後 60 秒のスモーク

```bash
APP_URL="https://dbsql-profiler-analyzer-dev-...."
TOKEN="$(databricks auth token --profile DEFAULT | jq -r .access_token)"

# (a) アプリが立ち上がっているか
curl -fsS "$APP_URL/health"

# (b) V6 flag が全部 ON か (jq で false を抽出)
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$APP_URL/api/v1/debug/feature-flags" \
  | jq -r '.feature_flags | to_entries[] | select(.value.enabled==false) | .key'
# → 出力が空なら全 enabled=true

# (c) catalog/schema が runtime-config から読まれているか
curl -fsS -H "Authorization: Bearer $TOKEN" \
  "$APP_URL/api/v1/debug/config" \
  | jq '.settings.catalog, .settings.schema'
```

### 3.2. flag が効いていないときの切り分け

```bash
# どの flag が default に落ちているか
curl -s -H "Authorization: Bearer $TOKEN" \
  "$APP_URL/api/v1/debug/feature-flags" \
  | jq '.feature_flags | to_entries[] | select(.value.source=="default")'

# raw_value が空文字なら local-overrides.yml に書き忘れ
# → dabs/local-overrides.yml の該当 v6_* を確認 → ./scripts/deploy.sh dev
```

### 3.3. env 上書きが残ってないかチェック

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "$APP_URL/api/v1/debug/feature-flags" \
  | jq '.feature_flags | to_entries[] | select(.value.source=="env")'
# → 出力が空なら env override 無し (期待動作)
```

---

## 4. エラーパターン

| 症状 | 原因 | 対処 |
|------|------|------|
| `401 Unauthorized` | token なし or 期限切れ | `databricks auth login --profile <name>` で再ログイン |
| `403 Forbidden` | App の `CAN_USE` が無いユーザー | App owner に CAN_USE 権限を付与してもらう |
| `404 Not Found` | 旧バージョン (v5.x) アプリ | `/api/v1/debug/feature-flags` は v6.0+ で追加。再 deploy |
| `502 / 503` | アプリ起動失敗 | `databricks apps logs <app-name>` で確認 |
| `enabled=false` 全部 | `local-overrides.yml` の `v6_*` が全部コメントアウト | flag を有効化して再 deploy |

---

## 5. 関連ファイル

| ファイル | 役割 |
|----------|------|
| `dabs/app/routes/settings.py` | `/api/v1/debug/feature-flags` / `/api/v1/debug/config` 実装 |
| `dabs/app/core/feature_flags.py` | flag 解決ロジック (`_is_enabled`, `ALL_FLAGS`, `snapshot`) |
| `dabs/app/core/config_store.py` | settings 読み書き (`get_setting`, `get_setting_with_source`) |
| `scripts/generate_runtime_config.py` | `local-overrides.yml` → `runtime-config.json` 変換 (V6 flag forwarder 含む) |
| `dabs/local-overrides.yml.sample` | `v6_*` キーのサンプル (コメントアウト済) |

## 6. 参照

- [`operations.md`](operations.md): どのタイミングで何のためにこのエンドポイントを使うか (シナリオ A-F)
- [`v6-spec.md`](../v6-spec.md): V6 仕様総論
- [`v5-vs-v6.md`](../v5-vs-v6.md): 8 flag の機能差分
