# V6 運用ガイド (実践的ワークフロー)

V6 で導入した評価基盤 (debug endpoint / goldens_runner / ab_runner /
regression_detector / stage_gate) を **どんなタイミングで何のために
使うか** をシナリオ別にまとめる。

`docs/v6-spec.md` が「何がある」、本書が「いつ使う」。

## 0. 全体マップ

V6 の 4 つの "確認手段":

| ツール | 用途 | 所要時間 |
|--------|------|---------|
| `/api/v1/debug/feature-flags` | デプロイ後に **flag が効いているか** 確認 (Web) | 5 秒 |
| `goldens_runner` | 1 baseline で **rule-based 数値** を取る | 数秒 |
| `ab_runner` | 4 conditions の **A/B 比較** | rule-based: 数秒 / LLM 込み: 数 USD |
| `stage_gate_runner` | **採用判定** adopt/hold/reject | regression_detector + 絶対品質 |

判断する順序:
```
1. デプロイ → /api/v1/debug/feature-flags で flag 確認
2. 手元で goldens_runner → 数値が合理的か眺める
3. LLM 込み ab_runner → 4 conditions の影響を切り分け
4. stage_gate_runner → adopt なら main マージ可
```

---

## 1. シナリオ別ワークフロー

### シナリオ A: 「dev にデプロイした、ちゃんと V6 動いてる?」

**用途**: デプロイ後の動作確認 (まさに今の状態)

```bash
# 1. Web で flag 確認
curl -s -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  https://dbsql-profiler-analyzer-dev-...../api/v1/debug/feature-flags | jq .

# 全 8 flag が enabled=true か?
# source が "runtime-config" になっているか? (env override されていないか)
```

**判断基準**:
- 全 enabled=true → V6 完全有効 ✅
- 1 つでも false → `local-overrides.yml` の `v6_*` を再確認、再 deploy
- source=env が出る → env var が設定されている (意図せず)、`/home/app` の env を確認

**よくある失敗**:
- runtime-config に `v6_*` が無い → `generate_runtime_config.py` の forwarder が動いてない (V6.1+ で追加)
- source=default → そもそも `local-overrides.yml` に何も書かれていない

---

### シナリオ B: 「実プロファイルを 1 件分析、V6 の挙動を肉眼で見たい」

**用途**: V6 が「期待通り違う出力をするか」のスポット確認

```bash
# 1. 手元の json/ にプロファイルを置く
ls json/query-profile_*.json

# 2. CLI で 1 件分析 (LLM ON)
PYTHONPATH=dabs/app:. uv run python -m cli.main \
  json/query-profile_xxx.json \
  -o /tmp/v6_report.md

# 3. canonical Report が emit されたか確認
grep -A 30 "json:canonical_v6" /tmp/v6_report.md
```

**期待される変化 (V6 全 flag on)**:
- レポート末尾に ` ```json:canonical_v6 ` block がある
- `expected_effect` で根拠なき "30% 改善" が減る (recommendation_no_force_fill)
- MERGE クエリで skeleton が `merge` method (extended on)

**ログで確認**:
```bash
# deploy 後、dev のログで flag が読まれた痕跡を見る (初回 prompt 時に出力)
databricks apps logs <app-name>  | grep -E "V6_|skip.*condensed|review_no_knowledge"
```

---

### シナリオ C: 「V6 機能の品質寄与を測りたい」

**用途**: 「どの flag を on にすると何が良くなるか」を A/B で把握

```bash
# 1. rule-based で 4 conditions 比較 (数秒、cost 0)
PYTHONPATH=dabs/app:. uv run python -m eval.ab_runner \
  --run-name v6_smoke --skip-judge --skip-llm

# 結果は eval/ab_summary/v6_smoke.md に
# 4 conditions すべて同じ数値 → rule-based では flag の効果は出ない
# (V6 flag は LLM prompt の制御がメインなので)
```

```bash
# 2. LLM 込み (本番採用判定の素材)
export DATABRICKS_HOST=...
export DATABRICKS_TOKEN=...

PYTHONPATH=dabs/app:. uv run python -m eval.ab_runner \
  --run-name v6_llm --with-llm-judge --judge-top-n 5

# 結果:
# - baseline (V6 flag off) vs canonical-direct vs no-force-fill vs both
# - per-case verdict (improved / regressed / unchanged ±5pt)
# - canonical_parse_failure_rate
# - R10 layer A + B (LLM judge)
```

**読み方**:

| 結果 | 意味 |
|------|------|
| canonical-direct で improved 多 | LLM が canonical JSON を出すと品質改善する flag |
| no-force-fill で ungrounded_numeric 改善 | "全項目埋めない" 圧力緩和が効いている |
| both で全部改善 | V6 全機能 on で OK |
| both で regressed | flag の組み合わせが悪い → 個別 condition で切り分け |

---

### シナリオ D: 「PR を main にマージしていいか判定したい」

**用途**: V6 全体を main に投入する最終判定

```bash
# Step 1. main の最新 baseline を取る (V6 off でも on でもどちらかに統一)
git checkout main
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name v5_main --skip-judge

# Step 2. V6 ブランチに戻って baseline (LLM 込み V6 全 on)
git checkout refactor/v6-quality
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name v6_pr --skip-judge

# Step 3. stage gate で 2-stage 判定
PYTHONPATH=dabs/app:. uv run python -m eval.stage_gate_runner \
  --current eval/baselines/v6_pr.json \
  --baseline eval/baselines/v5_main.json \
  --on-reject-exit 1 \
  --on-hold-exit 1
```

**verdict 解釈**:

| verdict | 意味 | アクション |
|---------|------|-----------|
| **adopt** | V6 全条件 pass | main マージ可 ✅ |
| **hold** | regression なし、絶対品質未達 | hold で PR 出して継続改善 |
| **reject** | regression あり (block tier 違反) | merge 不可、原因究明 |

`hold` の場合の判断:
- `eval/stage_gate/<name>.md` の "Stage 2 violations" を見る
- 例: "q3_composite 0.65 < target 0.80" → V6.2 で knowledge selector を強化
- LLM 込みでないと届かない閾値かも (rule-based では永遠に hold)

---

### シナリオ E: 「main で品質劣化が起きた、回帰検出したい」

**用途**: nightly で品質トレンドを追う

```bash
# Cron / nightly で
PYTHONPATH=dabs/app:. uv run python -m eval.regression_detector \
  --current eval/baselines/main_today.json \
  --baseline eval/baselines/main_yesterday.json \
  --exit-on warn  # warn 以上で exit 1
```

**3-tier の使い分け**:

| tier | アクション |
|------|-----------|
| **BLOCK** (Q3/Q4/Q5/recall/halluc/schema 大幅下降) | Slack 通知 + revert 検討 |
| **WARN** (parse_success / ungrounded_numeric 微悪化) | 担当者 assign + 次 PR で修正 |
| **INFO** (skeleton method 分布シフト) | ダッシュボードで監視のみ |

`eval/regression_summary/<name>.md` で `delta` と `tier` を見れば
「何が悪化しているか」が一目で分かる。

---

### シナリオ F: 「特定の flag だけ off にしたい (緊急時の rollback)」

**用途**: V6 機能で問題発生 → 個別 flag を off

```bash
# 1. local-overrides.yml で問題の flag だけ off
# v6_canonical_schema: "true"  ← false に
v6_canonical_schema: "false"

# 2. deploy
./scripts/deploy.sh dev   # or staging / prod

# 3. /api/v1/debug/feature-flags で off になったか確認
```

**他の方法**:
- env var で個別 override も可能 (Databricks Apps の env 設定経由)
  - 一時的な hot fix に有効
  - 永続化したいなら local-overrides.yml に書く

---

## 2. リリース工程ごとの使い方

| 段階 | 使うツール | gate 設定 | 期待 verdict |
|------|----------|----------|-------------|
| **開発者 PR (CI)** | `ab_runner --gate-w4-infra` | infra 4 条件のみ | clean/warn |
| **PR レビュー** | `stage_gate_runner` (LLM ON) | hold 許容 | hold/adopt |
| **dev デプロイ後** | `/api/v1/debug/feature-flags` | (確認のみ) | flag 効いてる |
| **Nightly main** | `regression_detector --exit-on warn` | warn 以上で fail | clean |
| **Release tag** | `stage_gate_runner --on-hold-exit 1` | adopt 必須 | adopt |

---

## 3. デバッグ・トラブルシューティング

### 「flag を `true` にしたのに enabled=false」

```bash
# /api/v1/debug/feature-flags で source を確認
{
  "V6_CANONICAL_SCHEMA": {
    "enabled": false,
    "source": "default",   ← runtime-config に届いてない
    "raw_value": ""
  }
}
```

調査:
1. `local-overrides.yml` (gitignore) に `v6_canonical_schema: "true"` あるか
2. `dabs/app/runtime-config.json` (deploy.sh が生成) に key あるか
3. `databricks.yml` の variables: 配下に declare されているか

修復:
```bash
# 再 deploy (generate_runtime_config.py が runtime-config.json を作り直す)
./scripts/deploy.sh dev
```

### 「canonical_parse_failure 100%」

意味: V6_CANONICAL_SCHEMA=on なのに LLM が ` ```json:canonical_v6 ` block を出していない

原因 / 対応:
1. **LLM 起動していない** → `--skip-llm` モードの場合は当然 100%、無視 OK
2. **prompt directive が効いていない** → `core/llm_prompts/prompts.py:_v6_canonical_output_directive` が prompt 末尾に append されているか確認
3. **LLM 応答の format 違反** → 1-2 件 dump して目視

```bash
# baseline JSON で canonical_source の内訳を確認
jq '.cases | group_by(.canonical_source) | map({source: .[0].canonical_source, count: length})' \
  eval/baselines/v6_xxx.json
```

期待される breakdown:
- `llm_direct`: LLM が canonical 出した case
- `normalizer_fallback`: LLM が canonical 出さず adapter 経由
- `missing`: そもそも LLM 起動していない

### 「Stage 2 で hold が永続する」

V6 完了基準は LLM 込み前提。rule-based では一生 hold。

`docs/eval/llm_acceptance_runbook.md` で LLM 込み実走を試すか、
acceptance threshold を一時的に緩める (本番投入前にダメ):

```python
# eval/stage_gate.py の DEFAULT_ACCEPTANCE を temp で下げて確認のみ
acceptance = {
    **DEFAULT_ACCEPTANCE,
    "q3_composite": 0.50,  # 80 → 50 (調査用)
}
```

---

## 4. 「次に何を見ればいい?」決定木

```
A. デプロイ直後
  └─→ /api/v1/debug/feature-flags
       ├─ enabled=true 全部 → B へ
       └─ false あり → local-overrides.yml + 再 deploy

B. 動作確認したい
  ├─→ シナリオ B (実プロファイル 1 件)
  └─→ シナリオ C (rule-based ab_runner)

C. 品質測りたい
  ├─→ LLM API 設定済み? Yes → ab_runner --with-llm-judge
  └─→ LLM API なし → rule-based ab_runner で infra 確認

D. main マージ判断
  └─→ stage_gate_runner で adopt/hold/reject

E. 障害調査
  ├─→ regression_detector で tier 別 violations
  ├─→ canonical_parse_failure → トラブルシューティング 3-2
  └─→ flag が効かない → 3-1
```

---

## 5. 推奨運用 (ベストプラクティス)

### 日次 (Nightly)

```bash
# main の baseline を毎晩取り、前日との差分を見る
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name nightly_$(date +%Y%m%d) --skip-judge

PYTHONPATH=dabs/app:. uv run python -m eval.regression_detector \
  --current eval/baselines/nightly_$(date +%Y%m%d).json \
  --baseline eval/baselines/nightly_$(date -v-1d +%Y%m%d).json \
  --exit-on warn

# warn 以上で Slack 通知 (V6.2 backlog で自動化)
```

### PR 時

1. 開発者: `pytest` で unit test pass を確認
2. CI: `ab_runner --gate-w4-infra` で infra 健全性確認
3. レビュアー: PR 説明欄に `eval/ab_summary/<pr>.md` を添付
4. マージ前: `stage_gate_runner` を手動実走 → adopt が出れば OK

### リリース時

1. `release_<ver>_baseline` を LLM 込みで取る
2. `stage_gate_runner --on-hold-exit 1` で **adopt 必須**
3. adopt で release tag、verdict が hold/reject なら release 中止

---

## 6. よくある質問

### Q. 全 flag を一気に on にしていいの?

A. dev では OK。staging/prod は段階導入推奨:
1. まず `V6_ALWAYS_INCLUDE_MINIMUM` だけ on (knowledge 削減のみ)
2. 1 週間 nightly で regression 出ないか確認
3. 次に `V6_REVIEW_NO_KNOWLEDGE` を追加
4. 最後に `V6_CANONICAL_SCHEMA` (出力契約変更) を on

### Q. canonical_parse_failure が 100% でも問題ないの?

A. `--skip-llm` モードでは正常。LLM API ありで 100% は問題で、prompt
directive または LLM model の出力品質を確認。

### Q. R10 Layer B が None になる

A. V6.1 時点では canonical Report が baseline JSON に persist されていない
ため、ab_runner では placeholder canonical で judge を exercise している
だけ。完全な Layer B は V6.2 backlog。

### Q. レガシー報告との比較は?

A. `docs/v5-vs-v6.md` の互換マトリクスを参照。短く:
- V5 出力は `generate_report_legacy` で作っていたが V6 で削除
- V6 は `generate_report` (新形式) のみ。canonical Report JSON は追加で emit

---

## 7. 参照

- 確認 endpoint: `/api/v1/debug/feature-flags` (V6.1+)
- runbook: [`docs/eval/llm_acceptance_runbook.md`](../eval/llm_acceptance_runbook.md)
- acceptance policy: [`docs/eval/v6_acceptance_policy.md`](../eval/v6_acceptance_policy.md)
- V5 vs V6: [`docs/v5-vs-v6.md`](../v5-vs-v6.md)
- 仕様総論: [`docs/v6-spec.md`](../v6-spec.md)
