# TODO

## プロダクト方向性
> **Databricks Workload Performance Copilot（最適化アシスタント）**
> 「遅いです」で終わらず「次に何を直すか」まで一気通貫で出す
> **症状 → 原因 → 推奨修正 → 期待効果 → 検証方法** を一本化

## ロードマップ

### v4.11 — 横断分析 + 開発基盤 (完了)
- [x] **Spark/DBSQL横断ワークロード分析** — 手動並列表示 + ペア紐付け + LLM Cross-Analysis [Experimental]
- [x] **LLMプロンプトバージョニング** — 分析ヘッダーにprompt_versionを記録、品質改善PDCAの追跡基盤
- [x] **設定デバッグ用エンドポイント** — `/api/v1/debug/config` で有効設定・優先順位を一発確認
- [x] **ルートレベルE2Eテスト** — Flaskテストクライアントで API→レポート生成の結合テスト
- [x] **共有リンクの非永続化警告** — persist未設定時に「セッション後アクセス不可」を表示
- [x] **API自動ドキュメント** — `/api/docs` でFlaskルートから自動生成（48エンドポイント）
- [x] **アプリ名統一** — Performance Toolkit に全テンプレートを統一

### v4.12 — リファクタリング + Spark Perfレポート刷新 (完了)
- [x] **巨大ファイル分割** — analyzers.py/reporters.py/llm_prompts.pyをサブパッケージ化
- [x] **テンプレートCSS/JS外部化** — base.html(1128行)のインラインCSS/JSを6 CSS + 1 JSに分割
- [x] **mypy CI導入** — lenientモードで型チェックをCIに追加
- [x] **Spark Perfレポート刷新** — Lakeviewダッシュボード準拠の10セクション構成にリライト
- [x] **ETL/Summaryノートブック更新** — v2パイプライン
- [x] **deploy.sh改善** — app.yaml自動生成、WH CAN_USE自動付与、Job ID自動解決
- [x] **ETL未設定時のUX** — Job IDをbundle summaryから自動取得

### v4.13 — Spark Perf AIエージェント
- [x] **Spark Perf Copilot** — Genie Conversation APIチャットパネルで実現 (v4.14 PR#6)
  - [x] レポートコンテキスト自動注入 — Genie SpaceがGoldテーブルを参照し、Text2SQLでデータアクセス
  - [x] チャットUI — DBSQL/Spark Perfレポート画面にGenieチャットパネルを追加 (PR#6)
  - [x] ボトルネック深掘り — Genieチャットで自然言語質問が可能
  - [ ] 修正コード生成 — 検出された問題に対する具体的なSpark設定変更/SQLリライト/コード修正を生成
  - [ ] What-If分析 — 「パーティション数を200→400に変えたら？」等のシミュレーション回答
  - [x] アクションプラン精緻化 — Genieチャットでフォローアップ質問が可能
  - [x] 履歴比較アシスト — Spark比較機能 + 比較履歴永続化で実現 (PR#7, PR#9)
  - [x] Foundation Model API連携 — Genie Conversation API経由でDatabricksネイティブ連携

### v4.13.5 — ナレッジベース国際化 + ルーティング抽象化 + 運用品質改善 (完了)
- [x] **Phase 1: ルーティング脱ロケール化** — section_idマーカーベースに改修済み
- [x] **Phase 2: 英語版ナレッジ作成** — dbsql_tuning_en.md 追加済み（931行、section_id共有）
- [x] **Phase 3: ロケール別ロード** — lang=enなら英語版、lang=jaなら日本語版をロード
- [x] **Gold MERGE（Upsert）** — overwriteからupsertに変更し履歴保持
- [x] **アプリケーションページネーション** — 20件/ページ
- [x] **スキーマバリデーション** — DBSQL/Spark Perf設定保存時のバリデーション
- [x] **CREATE TABLE権限チェック** — GRANTコマンドガイダンス付き
- [x] **テーブル自動初期化** — DBSQL設定保存時にDeltaテーブルを自動初期化
- [x] **LLMナラティブ直接表示** — レポーター生成スキップ
- [x] **Photon/Spot TOP5制限** — 表示上限を設定
- [x] **Run ETL/Create Reportセクション分離** — UI改善
- [x] **Cluster Log Destination単一フィールド化** — 入力簡素化
- [x] **バージョン自動注入** — pyproject.tomlからdeploy.sh経由で注入
- [x] **不要翻訳コード削除** — Dead translation code removal
- [x] **Spark Perfバイリンガルレポート** — ~200ラベルのja/en対応
- [x] **LLMモデルセレクター** — Spark Perfレポート生成時のモデル選択
- [x] **「日本語レポート」トグル** — レポート言語切替
- [x] **PyPIプロキシ設定** — 社内環境対応
- [x] **ハードコードモデル名修正** — 設定値を参照するように変更
- [x] **OBO認証** — ジョブトリガー用のOn-Behalf-Of認証
- [x] **i18n: 73文字列翻訳** — 6テンプレートにわたる翻訳追加
- [x] **ETL/Summary/KnowledgeBaseノートブック更新** — v4.12.5対応
- [ ] **Phase 4: 品質比較評価** — 同一クエリでja→ja, ja→en, en→ja, en→en の4パターン比較（引用正確さ・トークン消費）
- [ ] **Phase 5: 正本決定** — 評価結果に基づき単一ソースに統一

### v4.13.6 — LLM判定品質向上 (Codexレビュー指摘)

#### Single Analysis プロンプト改善 (v4.11.0で#9,#10,#11対応済み)
- [x] **#9 根拠制約型フォーマット** — Symptom/Evidence/Causal Hypothesis/Confidence/Action/Expected Impact/Counter-evidence の7フィールド化 + HARD RULES
- [x] **#10 構造化Fact Pack概要** — top_alerts/dominant_operations/alert_contradictions/confidence_notes をYAMLブロックで先頭配置
- [x] **#11 信頼度判定基準** — Confidence Criteria (high/medium/needs_verification) + Action Plan JSONにconfidence/confidence_reason追加

#### Single Analysis ボトルネック判定改善 (→次期スプリント Week 1に移動)

#### Single Analysis メトリクス抽出改善 (未着手)
- [ ] **#5 演算子別ボトルネック分類** — HashAggregate/Window/Sort/UDF等の演算子別支配時間とduration_shareを抽出
- [ ] **#6 extra_metrics網羅性拡張** — sort time/spill read-back/hash probe collisions/metadata listing time等を追加
- [ ] **#7 パーティション推定の改善** — Tasks totalでなくSink - Number of partitions等を優先
- [ ] **#8 Scan抽出にフィルタ文脈追加** — pushed filters/partition filters/data skipping filtersを取得

#### Compare Analysis LLM改善 (C1-C9完了、C10-C12→次期スプリント Week 2に移動)
- [x] **C1 根拠制約の明示** — 「与えたmetric_dataのみを根拠に判断し、未知情報は"不明"と書く」をプロンプトに追加 (comparison_llm.py)
- [x] **C2 観測/推測/不明の分離** — Observed Changes / Evidence-backed Interpretation / Recommended Actions / Overall Verdict の4セクション構成に変更
- [x] **C3 重み付け済みメトリクス順位をLLMに渡す** — _METRIC_WEIGHTSからtop_regressions/top_improvements/decision_driversを事前計算してpromptに含める
- [x] **C4 絶対差分+相対差分の優先順位ルール** — 小さな母数の大きな率変化を過大評価しないよう絶対影響量を明示
- [x] **C5 ノイズ・抑制ルールのLLM共有** — _NOISE_FLOOR/_IO_DEPENDENT_METRICSによるsuppressed_regressionsを「判定から除外済み」としてLLMに伝える
- [x] **C6 比較サマリーの構造化出力** — 4セクション構造 + Go/Hold/Rollback判定 + confidence付きの構造化出力
- [x] **C7 情報十分性ベースの信頼度算出** — Confidence Criteria (high/medium/low) を定義、LLMに判定基準を明示
- [x] **C8 メトリクス因果グラフで推論を制約** — 許容因果リンク（read_bytes↓→total_time_ms↓等）を定義し、LLMの自由推論を制限
- [x] **C9 因果推論に反証チェック** — 各原因候補に支持メトリクス/反証メトリクス/未観測を併記させる

### v4.14 — Genie Chat + Spark比較 + 比較UX改善 (完了)
- [x] **Genie Conversation APIチャットパネル** — DBSQL/Spark Perfレポート画面にGenieチャットを統合 (PR#6)
- [x] **Spark比較機能** — 履歴タブ、レポート閲覧、削除、Experiment/Variant対応 (PR#7)
- [x] **ジョブステータスページリフレッシュ永続化** — localStorage使用 (PR#8)
- [x] **比較履歴永続化 + 5段階Verdict + カラムソート** (PR#9)
- [x] **ポーリング修正 + インラインプログレス + i18n修正** (PR#10)
- [x] **比較ページにGenie統合 + Space自動再作成** (PR#12)
- [x] **Experiment/Variantインライン編集 + カスケード** (PR#13)
- [ ] **比較画面のコスト比較** — system.billing.usageが取得可能な場合、Before/Afterのクエリコスト（DBU消費）を比較に含める
- [ ] **Verification Copilot** — verification_stepsを具体的な数値合格ライン + 再実行条件に強化

### v4.15 — OBO認証 + 権限自動化 + テスト拡充 (完了)
- [x] **OBO認証** — 読み取り・LLMはOBO、書き込み・Genie・ジョブはSP認証
- [x] **権限自動化** — DABがWH CAN_USE、App CAN_USE、OBOスコープを管理
- [x] **deploy.sh強化** — カタログ/スキーマ自動作成、SP書き込みGRANT、ジョブCAN_MANAGE_RUN
- [x] **Reset to Deploy Defaultsボタン** — 設定画面にデプロイデフォルトへのリセット機能
- [x] **Genie SP認証 + テーブル自動作成 + Space初期化待機**
- [x] **result.htmlからGenie削除** — 永続化データページのみに限定
- [x] **非同期レポート取得** — インラインJSON→非同期fetchに改善
- [x] **CONTRIBUTING.md** — TDDルール、テスト要件マトリクス
- [x] **151テスト追加** — spark_comparison, sql_safe, genie_client（527→678テスト）

### v4.16 — スモークテスト + デプロイ後自動検証 (完了)
- [x] **ポストデプロイスモークテストスイート** — API 31チェック + UI 12チェック（Playwright）
- [x] **deploy.sh自動スモークテスト** — デプロイ後に自動実行
- [x] **--full-testフラグ** — 分析フロー検証用

### 次期スプリント

#### Week 1: ボトルネック判定改善
- [ ] **#1 条件付き判定** — remote_read_ratioはread_bytesが十分大きい場合のみ有意、spillはpeak_memory_bytesと複合で判定
- [ ] **#2 絶対値→相対値/規模補正** — spill_critical_gb=5GB等の固定閾値をspill/read_bytes等の正規化指標に変更
- [ ] **#3 Photon判定を阻害要因ベースに** — photon_ratio低だけで警告せず、SortMerge/photon_blockers/OOM fallback実在時のみ強警告
- [ ] **#4 Shuffle/Skewを複合スコア化** — 複数指標を合成してskew_confidenceを算出
- [ ] **#12 ワークロード別閾値校正** — short query/scan-heavy/join-heavy/ETLで閾値セットを分岐、family.pyと連携

#### Week 2: Compare LLM改善 + Context & Discovery最小版
- [ ] **C10 本番適用可否をルールベースで判定** — ComparisonService側でgate条件を持ち、LLMは説明に限定
- [ ] **C11 比較前提差分のLLM入力追加** — 同一fingerprint/単回比較/サンプル数不明等の前提を明示
- [ ] **C12 期待効果の定量予測を弱める** — directional expectation onlyまたはconfidence low時は数値禁止
- [ ] **Query History** — system.query.historyから同一クエリの過去実行トレンド（Context & Discovery最小版）

### 未スケジュール — 意思決定支援
- [ ] **Pattern Memory / Optimization Playbook** — Query Family別に過去の修正→結果を学習して次回提案へ再利用
- [ ] **Regression Guard / Release Gate** — baseline比較でデグレなら赤
- [ ] **Impact Estimator** — spill/shuffle削減・Photon化の期待改善幅をレンジ提示

### v4.33+ — メトリクス抽出・分析の深化（後回し）
- [ ] **result_from_cache 時の分析抑制方針統一** — キャッシュヒット時にlatency系アラートを抑制するルール整備
- [ ] **ComparisonService に比率系派生指標追加** — read_cache_ratio, shuffle_write_ratio 等の算出済み比率を比較可能に
- [ ] **SCAN_TABLE/SCAN_DATABASE の node metadata 抽出** — データリネージ情報の基盤
- [ ] **AGGREGATE/PROJECTION expression 抽出** — 演算子別の式情報でLLM分析の精度向上

### 未スケジュール — 仕上げ
- [ ] **デザイン改善** — ライトテーマベース、Noto Sans JP + Inter、余白広め、Databricks赤アクセント
- [ ] **PDF/PNG完全自動化** — レポートの画像・PDF出力
- [ ] **CDN SRI** — base.html の外部スクリプトにintegrityハッシュ追加

### バグ・改善
- [x] **レポートh2ヘッダー重複** — generator関数にinclude_header引数を追加して根本対応
- [x] **ETL未設定時のUX** — Job IDをbundle summaryから自動取得（deploy.sh）
- [ ] **スキーマ指定の共通化** — DBSQL ProfilerとSpark Perfで別々のカタログ/スキーマ設定を持っているが、共通の1つのスキーマ設定に統一する
- [ ] **Spark Perfテーブル接頭語の廃止** — `PERF_` 等のユーザー指定を廃止し、内部で固定にする
- [ ] **i18n完全対応** — compare.html, result.html, report_view.html, shared_result.html, workload.html 等の未翻訳文字列を{{ _() }}で囲み、.poファイルに日本語訳を追加する。spark_perf.htmlは対応済み
- [ ] **比較ページの統一** — SQL比較とSpark比較で分析IDの概念が異なる（SQL: analysis_id=分析実行UUID / Spark: app_id=ジョブ実行ID）。将来的にSpark側にもanalysis_id概念を導入し、同一app_idに対して異なるLLMモデルや設定で複数回分析・比較できるようにする

### Context & Discovery（文脈発見機能）
- [ ] **Warehouse Events** — system.compute.warehouse_eventsからスケール/キュー待ち/再起動を取得、環境要因の切り分け
- [ ] **Regression Detector** — 直近7日 vs 過去30日で「最近だけ悪化」を自動検出
- [ ] **Concurrency Pressure** — 同時刻のwarehouse同時実行数・待機状況
- [ ] **Cost Context** — system.billing.usageからクエリ/ジョブ単位のコスト取得・分析（DBU消費、推定金額、コスト傾向）
- [ ] **Healthy Peer Comparison** — 同目的の"速いクエリ"との差分比較（勝ちパターン発見）
- [ ] **Table Health** — DESCRIBE DETAIL/HISTORYで関連テーブルの状態（補助情報として限定表示）

UI方針: 独立タブではなく既存分析画面に「Context」セクション統合。問いベースカードUI。

### 未スケジュール（アイデア）
- [ ] **Root Cause Graph** — 症状→根拠メトリクス→推定原因→推奨修正→検証SQLを1本のグラフで表示
- [ ] **One-click Experiment Template** — broadcast hint追加、cluster by見直し、Photon blocker解消の実験テンプレ
- [ ] **Explainability Panel** — Go/Hold判定の根拠を3-5根拠で説明（direction-aware metricsで固定化）
- [ ] **Workload Archetype** — クエリを「wide join」「aggregation-heavy」等に分類、典型改善パターン提示
- [ ] **Blast Radius** — 遅いクエリがダッシュボード/ジョブ/BI利用者にどれだけ影響しているか

## 完了
- [x] 推奨の優先度付け表示
- [x] アップロード前バリデーション強化
- [x] Flask Blueprint分割
- [x] Databricks Asset Bundles
- [x] 履歴の削除機能
- [x] ベースライン管理（自動比較）
- [x] ユースケース別テンプレ導線（3カード）
- [x] Scan Localityノード別表示
- [x] ActionCard進化（色付き優先度/10）
- [x] 共有出力（固定URL + Slack summary + Print CSS）
- [x] runtime-config.json デプロイ時設定
- [x] deploy.sh ワンコマンドデプロイ
- [x] SQL connector認証修正（credentials_provider, host stripping）
- [x] Action Plan Generator（P0/P1/P2 + Risk + Verification）
- [x] LLMプロンプト拡張（ACTION_PLAN_JSON）
- [x] レポートセクション順序整理（番号付き本文 + 📎Appendix）
- [x] Scan Locality / AQE Shuffle にInsightサマリー追加
- [x] i18n: .po一本化（_JA_TRANSLATIONS辞書廃止）
- [x] i18n: ~200件の翻訳追加
- [x] Executive Summaryプロンプト改善（管理者向け3秒要約）
- [x] Spark Perf: 9タブUI → Markdownレポート統一（v4.10）
- [x] Workload横断分析 + LLM Cross-Analysis（v4.11）
- [x] API自動ドキュメント /api/docs（v4.11）
- [x] アプリ名 Performance Toolkit に統一（v4.11）
- [x] 共有リンク非永続化警告（v4.11）
- [x] 設定デバッグ /api/v1/debug/config（v4.11）
- [x] E2Eルートテスト 18件（v4.11）
- [x] LLMプロンプトバージョニング（v4.11）
- [x] docs/api-reference.md + docs/operations-guide.md（v4.11）
- [x] Genie Conversation APIチャットパネル（v4.14 PR#6）
- [x] Spark比較 + 履歴タブ + レポート閲覧 + 削除 + Experiment/Variant（v4.14 PR#7）
- [x] ジョブステータスlocalStorage永続化（v4.14 PR#8）
- [x] 比較履歴永続化 + 5段階Verdict + カラムソート（v4.14 PR#9）
- [x] ポーリング修正 + インラインプログレス + i18n修正（v4.14 PR#10）
- [x] 比較ページGenie + Space自動再作成（v4.14 PR#12）
- [x] Experiment/Variantインライン編集 + カスケード（v4.14 PR#13）
- [x] OBO認証（読み取り/LLM: OBO、書き込み/Genie/ジョブ: SP）（v4.15）
- [x] deploy.sh: カタログ/スキーマ自動作成 + SP GRANT + CAN_MANAGE_RUN（v4.15）
- [x] 151テスト追加（527→678テスト）（v4.15）
- [x] CONTRIBUTING.md（v4.15）
- [x] ポストデプロイスモークテスト（API 31 + UI 12 Playwright）（v4.16）
- [x] deploy.sh自動スモークテスト + --full-testフラグ（v4.16）

## docs 小改修（将来・優先度 低 / 2026-04-22）

### CLI の位置づけを docs に反映
`dabs/app/cli/main.py` は実態としてテスト / README のサンプル / 開発者の手動デバッグでのみ使われており、本番ランタイム (`dabs/app/routes/*`, `eval/diff_runner.py:112`) は `run_analysis_pipeline` を直接 import している。

現行の `docs/architecture-overview.md` と `docs/data-flow.md` は CLI を Web UI と並列の第一級エントリポイントとして描いており、誤認を招く可能性がある。

**対応案:**
- Entry points 節で「CLI = 開発者向け補助」と注記
- `data-flow.md` の sequence 図に `eval/diff_runner.py` 由来の直接呼び出し経路を追加 (CLI subprocess ではない点を明示)

**優先度:** 低 — 誤動作ではなく表現上の改善。次の docs 更新 PR にまとめて取り込む。

---

## やらないことリスト
- UIタブの追加（Markdownレポートに統一済み）
- LLMレビュー段数の追加（コスト/レイテンシが先に来る）
- 汎用性能ポータル化（対象をDatabricks SQL/Sparkに絞る）
- 監視ツール化（既存observability製品と競合する）

---

## LLM プロンプト精度向上（保留中・2026-04-19 Codex レビュー）

**目的**: コスト削減ではなく **精度最大化**。余計な情報を削って hallucination / 誤 reject を減らす。

**保留理由**: 影響範囲が広く回帰リスクが大きい。A/B テストで before/after を eval スコアで比較してから段階的に着手する。

### Codex 指摘: 精度を下げている主因

一番の害は **Phase 2/3 への knowledge 注入** と **常時 include の `spark_params` / `appendix`**。
削ることで精度が上がる箇所を特定済み。

### 優先順位付き改善案

1. **Phase 2 review から knowledge 完全除去** — 誤 reject 減
   - `core/llm.py:290` review 向け `filter_knowledge_for_analysis()` 呼び出し廃止
   - `core/llm_prompts/prompts.py:835+` `create_review_system_prompt()` に `include_knowledge=False` スイッチ

2. **Phase 3 refine から knowledge 全文除去** — scope creep 阻止
   - `core/llm.py:342` refine 向け knowledge 注入廃止
   - `core/llm_prompts/prompts.py:1178+` 同様のスイッチ
   - review が `requires_knowledge_check: true` を返した section のみ micro-knowledge 添付可

3. **`ALWAYS_INCLUDE_SECTION_IDS` から spark_params / appendix を除去**
   - `core/llm_prompts/knowledge.py:180`

4. **Condensed knowledge 再注入の停止**
   - `core/llm_prompts/knowledge.py:474-492` 二次生成物で矛盾・一般化を増やすリスク

5. **Fact Pack を summary-first / anomaly-only に再編**
   - `core/llm_prompts/prompts.py:1689+`
   - 順序: (a) Top bottlenecks summary → (b) Critical/high alerts → (c) Key metrics → (d) Detailed evidence → (e) Raw query/EXPLAIN
   - OK 系・常時 0 の metric は除外
   - EXPLAIN 原文と構造化要約の重複解消

6. **Appendix rewrite patterns を analysis phase のみ optional に降格**
   - `core/knowledge/dbsql_tuning_en.md:1144-1258`

7. **Knowledge 内の matrix 衝突を解消**
   - L230-242 (classic/pro autoBroadcastJoinThreshold) vs L1083-1104 (serverless 不可)
   - serverless / classic で knowledge 分割

8. **Recommendation format block の圧力緩和**
   - `core/llm_prompts/prompts.py:23-94` 全項目埋める圧力が根拠薄い SQL/予測を捻出
   - "根拠不足なら省略" を明記

### 削りすぎ閾値 (Codex 目安)

| Phase | knowledge 量 |
|-------|-------------|
| analysis | 8-15KB (appendix / 長い examples 除く) |
| review | **0KB** (理想) / 最大 2KB |
| refine | 0-4KB (issue 解決に必要な断片のみ) |

### 着手の段階案

- **A. 最小 (1+2)**: review/refine から knowledge 除去 — 1-2時間
- **B. 中 (1-4)**: +ALWAYS_INCLUDE 縮小 + condensed 注入停止 — 2-3時間
- **C. フル (1-8)**: +Fact Pack 再編 + user prompt 順序 + knowledge 分割 — 半日〜1日

### 実装前の準備

- eval フレームワーク (`eval/`) のスコアで before/after 比較
- 代表的な 5-10 個の profile で手動検証
- prod prompt と改善 prompt を並列で走らせる A/B 環境

---

## Fact Pack の改廃（将来の整理課題）

**背景**: Codex レビュー (PR #59 時点, 2026-04-20) で「Fact Pack 過積載が悪化気味」と指摘。新機能追加の都度 `_build_fact_pack_summary()` に項目を足していくと、LLM の attention が薄まり精度低下を招く可能性。

**現在の Fact Pack に積まれているもの**:
- top_alerts (severity + category)
- dominant_operations (top 3 by duration)
- alert_contradictions
- confidence_notes
- sql_context (tables, join_count, cte_count)
- scanned tables + clustering keys + cardinality
- EXPLAIN-derived signals (v2): implicit CAST, aggregate phases, Photon fallback, CTE reuse, join build side, filter pushdown, ReadSchema column types
- aggregate_expressions (decimal arithmetic detection)
- warehouse sizing signals

**やること（将来）**:
1. **棚卸し**: 実際に LLM が参照している項目と参照していない項目を eval / サンプル分析で分類
2. **重複排除**: EXPLAIN v2 signal と既存 alert で情報が重複している箇所を統合
3. **動的 include**: 該当 signal が存在する時のみ表示（空セクションの省略は既に一部あるが徹底）
4. **段階的フェードアウト**: 2 release で使われていない項目は削除候補に

**トリガー**: LLM 精度向上タスク（上記のセクション）と合わせて実施するのが効率的。

---

## レポート品質評価機能（設計済み・未着手 / 2026-04-20 Codex 設計議論）

**目的**: 現行 Phase 2 review (LLM レビュー) は形骸化しているため削除し、代わりにナレッジベースを活用した「生成レポートの品質評価」機能を追加する。

**保留理由**: 設計固まったが実装ボリュームが大きい。LLM 精度向上タスク（上セクション）と連動させて着手するのが効率的。

### Codex 設計議論サマリー

- **Phase 2 削除は妥当**: `usecases.py:500` で review は critique 生成だけ、Phase 3 への伝わり方が不透明。refine は "差分修正" 思想で Phase 2 を活かしきれていない。
- **推奨: post-analysis add-on** (診断と品質保証を分離、in-pipeline 置換は将来 v2)
- **ナレッジ活用**: 30+ sections を観点別 rubric に正規化 + claim-to-reference verification
- **判定構成**: 2層 (軽量 rule-based → 必要箇所のみ LLM-as-judge)
- **eval/ と共通 scorer コア再利用**、rubric prompt は新設

### 評価軸 (per-report 4軸)

| 軸 | 内容 |
|---|------|
| **Groundedness** | レポート内の claim が Fact Pack / evidence で裏付けられているか |
| **Knowledge Alignment** | ナレッジベースの推奨パターンと整合しているか（例: serverless 不可 config を提案していないか） |
| **Coverage** | 検出された重要 alert / bottleneck に対して言及しているか |
| **Actionability** | 実行可能な fix（具体的な SET / SQL / ALTER TABLE 等）が提示されているか |

Extension: per-card / per-claim への細分化、hallucination 構造化検出

### 実装段階案

| フェーズ | 内容 |
|---------|------|
| **MVP** | post-analysis add-on、per-report 4軸スコア、主要 findings、knowledge sections 引用、再生成推奨フラグ |
| **v1.5** | per-card/per-claim 評価、hallucination/wrong_value 構造化検出、refine 入力に findings を利用 |
| **v2** | in-pipeline quality gate 化、閾値未満で自動 refine or 再生成 |
| **v3** | offline A/B diff と online evaluator を共通 rubric に統一 |

### Phase 2 削除の波及

- `routes/analysis.py:75` — review_model パラメータ
- `templates/analyze.html:120` — UI 文言
- `cli/main.py:177` — CLI フラグ
- `core/llm_prompts/prompts.py:1088+` `create_review_system_prompt()` + `create_review_prompt()`
- `core/llm.py:review_with_llm()`
- **要注意**: refine が Phase 2 依存 → Phase 3 残すなら refine 入力を review comments から **evaluator findings** に置換する

### 主要リスク

- Knowledge section 選択を誤ると judge 自体がぶれる
- 生成モデルと judge モデルの **バイアス共倒れ** → 別モデル推奨
- "Coverage" は gold answer 不在で主観化しやすい
- UI で絶対評価に見えやすく、**score 解釈の説明責任** が必要

### eval/ 既存との関係

- 既存: L1 (syntax) / L2 (evidence) / L3 (diagnosis accuracy) / L4 (fix effectiveness) — **ActionCard 単位の offline eval**
- 新機能: per-report 単位、online (UI 表示) もカバー
- **共通化**: `eval/scorers/l3l4_judge.py` の judge 呼び出し枠組み / 1-5 規約 / JSON パースは再利用
- **新設**: report 評価専用 rubric prompt

---

## レポート Regression 検知機能（設計済み・未着手 / 2026-04-20 Codex 設計議論）

**目的**: レポートロジック（prompt / analyzer / knowledge）を変更した時、既存プロファイルに対する出力が **デグレ** していないか開発フローで検知する。

**保留理由**: v6 品質評価 (上セクション) と強く連動する。**v6 の rubric/schema を先に作ったほうが regression 判定も安定する**ため、v6 と連続して実装するのが効率的。

### 背景: 既存 `--diff-from` はあるが呼ばれていない

- `eval/cli.py --diff-from <ref_or_JSON>` で baseline (git ref or 固定 JSON) と current の比較判定はすでに実装済
- `eval/diff_runner.py` で git worktree を使い baseline 展開、`eval/diff_judge.py` で LLM judge
- **問題**: 手動実行のため、誰も呼んでいない → regression 検知が機能していない

### Codex 診断: 既存 `diff_judge.py` は "実用レベル手前"

| 観点 | 評価 |
|------|------|
| 良い点 | temperature=0.0 + JSON 強制 / improved/regressed/unchanged + 説明文 / worktree 展開設計 |
| 弱い点 | judge prompt に SQL / profile summary なし → 「card 記述の見た目比較」に偏る |
| 弱い点 | baseline 側 L2 が sentinel `-1.0` でスコア表示上 baseline を不当に悪く見せる |
| 弱い点 | card matching なし → 重要 card が落ちても別の低価値 card 追加で相殺される |
| 弱い点 | parse failure が `unchanged` に吸われ silent false negative |

→ **PR コメント補助なら可、blocking gate はまだ早い**

### 統合オプション

| 案 | 価値 | 推奨度 |
|----|------|--------|
| **A. CI PR コメント** | 変更瞬間に見える、レビュー文脈 | **1位 (MVP)** |
| **E. 手動 CLI ラッパー** | ローカル反復、A のノイズ事前削減 | **2位 (MVP)** |
| **D. Nightly + Slack** | 蓄積的劣化検知 | 3位 (v2) |
| **B. Pre-deploy gate** | FP リスク高、warning-only から | 4位 (v3) |
| **C. UI Compare reports** | 調査用、ユーザー課題への直接解ではない | 5位 (後回し) |

### 推奨組み合わせ

- **MVP = A + E** (non-blocking PR コメント + CLI wrapper)
- **v2 = +D** (nightly Slack)
- **v3 = +B** soft gate → hard gate は v6 整備後

### ゴールデン fixture の curate 方針

**"代表サンプル" ではなく "failure mode カタログ"** に寄せる:

- 2 件: spill / shuffle heavy
- 1-2 件: file pruning / partition pruning 不足
- 1 件: cache / Photon 判定
- 1 件: join skew / large shuffle read
- 1 件: write-heavy / MERGE or INSERT
- 1 件: serverless 制約が効くケース
- 1-2 件: "ノイズ多いが改善提案は少ない" ケース

**fixture metadata 必須**: domain / workload type / expected bottleneck / **must-cover issues** / must-avoid-hallucination / severity

→ PR smoke set 5-10 件 + nightly extended set 20-30 件

### コスト / 揺れ対策

- PR 用は **relative-only lightweight mode** (baseline/current で L1/L2 のみ、L3/L4 省略、diff judge のみ走らせる)
- `changed-files` フィルタで `core/llm*`, `analyzers/*`, `knowledge/*` 変更時のみ発火
- 揺れ対策:
  - verdict を 3 値ではなく **weighted numeric delta** に集計
  - borderline を `unchanged` 扱いにする **deadband** 設定
  - **同一 commit の結果を cache**
- **diff judge 単独で止めない** (あくまで signal)

### False positive / 閾値設計

- PR コメント: 常に投稿、**絶対にブロックしない**
- soft gate: `regressed profiles >= 2` かつ severity-weighted delta <= -T
- hard gate: **absolute (v6) + relative (diff) 両方悪化** した時のみ
- 人間確認用 UI/PR コメントは "must-review regressions" と "possible regressions" を分ける

### v6 との連携戦略（重要）

- `relative diff` と `absolute quality (v6)` は**完全に補完的**
  - relative: 「前より悪くなったか」
  - absolute: 「今の品質が十分高いか」
- **v6 の rubric/schema を先に作る** → diff_judge もそのベースで再設計すると比較軸が安定
- **共通 rubric schema**:
  - `diagnosis_accuracy` / `evidence_grounding` / `fix_actionability` / `sql_safety` / `coverage` / `overall`
- `diff_judge.py` = 比較 adapter、`v6` = 単体評価 adapter、両方 **shared scorer コア** を使う

### 推奨実装タスク優先順（v6 と連動）

1. **v6 rubric / schema 定義** ← ここを最初にやる
2. **diff_judge を rubric ベースに再設計**
3. **smoke fixture curate** (metadata付き)
4. **PR comment 自動化 (A)**
5. **手動 CLI ラッパー (E)**
6. **nightly (D)**

→ 導線 (A/E) は早く入れたいが、**判定の信頼性は v6 rubric 整備後**が安全。

### 主要懸念

- 現 diff judge は "真の品質差" ではなく "文章差" を拾う
- baseline 側 L2 未評価の非対称性
- profile 数少ないと過学習
- PR ごと full L3/L4 judge はコスト過多
- hard gate を急ぐと運用不信を招く

### 実装段階案

| フェーズ | 内容 |
|---------|------|
| **MVP** | A + E、smoke fixtures 5-8 件、changed-files only、PR comment non-blocking、judge は "experimental" 表記 |
| **v1.5** | fixture metadata、must-cover issue + severity、weighted summary、同一 commit cache |
| **v2** | D (nightly Slack)、v6 absolute eval、PR は relative + absolute delta 併記、B warning-only |
| **v3** | hard gate (critical fixtures のみ)、C Compare UI (investigation 用) |

## SQL スケルトン抽出（設計済み・未着手 / 2026-04-21 Codex レビュー）

### 背景
- v5.15.2 sqlparse 大規模 SQL 修正の過程で判明: `create_structured_analysis_prompt` (`dabs/app/core/llm_prompts/prompts.py:2285`) が **SQL を 3000 chars で盲目的 truncate**
- 86K chars の実ケース（Master_table_insertion_after_optimization_2XL.json）では最終 SELECT・中後段 CTE が LLM に見えない
- レポート markdown 側は SQL 全文 `<details>` 折りたたみ保持 (71% 占有)、Rewrite はフル SQL 前提で分離済み
- LLM 本線のみ構造情報を取り損ねているのが問題

### Codex 判定: 条件付き OK
`3000 chars` 盲目的 truncate は実害が明確。ただし MVP を `SELECT/CTE/JOIN` に絞るのが前提。`MERGE`/DDL/Databricks 特有構文を初版から網羅すると 2–3 日見積はかなり楽観的。

### 方針
- 新規 `dabs/app/core/sql_skeleton.py` で sqlglot AST ベースの構造抽出
- `prompts.py:2284-2285` の truncate を置換（長大 SQL のみ適用）
- fallback 階層: `sqlglot skeleton` → `head+tail + sql_context.structure` → legacy truncate

### MVP スコープ
**保持**
- `WITH/CTE 名` + 参照関係
- `FROM/JOIN 種別/相手テーブル`、`ON` は predicate shape（equality/range/LIKE/IN/EXISTS/OR-heavy/non-sargable）のみ
- `WHERE` も同様に shape のみ（値は落とす、形は残す）
- `GROUP BY` / `ORDER BY` / `HAVING` 列
- `UNION 分岐数`、`subquery type`、`DISTINCT`/`LIMIT`
- `SELECT 列数の集約`（`<N cols>`）
- CTE 多重参照カウント（既存 `analyze_cte_multi_references` と統合）

**落とす**
- `MERGE` 詳細、DDL 詳細、hint 完全保持、`PIVOT/UNPIVOT` 詳細、window 詳細
- 完全な列リスト、複雑な ON/WHERE 式の中身、式内の関数呼び出し詳細

**例外処理**
- `MERGE` / `CREATE VIEW AS` / `INSERT ... SELECT` は初版 skeleton 化せず、フル SQL or head+tail fallback
- `LIKE/RLIKE/regexp/json/path literal` が重要な診断対象のクエリ、CASE-heavy、complex string/date business logic はフル SQL 優先（判定ロジック必要）

### 適用条件
- `len(sql) > 3000` **かつ** `cte_count >= 2 or union/subquery/join_count high`
- 短い単純クエリは skeleton 化コストに見合わないので適用しない

### 段階的圧縮（超過時）
1. SELECT 列集約
2. 式詳細除去
3. ON/WHERE を分類表現へ
4. 深いサブクエリ省略
5. CTE 本文を概要化
6. **CTE 名だけ化（最後）**

### LLM プロンプト指示（hallucination 防止）
- 「以下は原文 SQL ではなく構造化スケルトン。存在しない条件・列・ヒントを推測しない」を明示
- 「推奨は skeleton で確認できた事実に限定し、未保持情報に依存する場合は `needs_full_sql` として明示」

### 閾値
- ターゲット予算 `2000–3000 chars` の可変 budget（固定 2000 は攻めすぎ）

### sqlglot 限界・fallback
- 失敗しやすい: `CACHE/UNCACHE`、`COPY INTO`、`OPTIMIZE ... ZORDER BY`、`REFRESH`（既存 `extractors.py:824` で parse 回避済み）、`LATERAL VIEW` 周辺、ヒント付き複雑文、マルチステートメント
- AST 漏れ注意: `Subquery`, `With/CTE`, `SetOperation`, `Merge/Insert/Create`, `Join.kind`, `Hint`, `Window/Qualify`, generator 系 (`Explode`, `Unnest`)
- `find(exp.EQ)` は JOIN 条件の一部しか拾わない → `ON` 専用分類ロジック必要
- 中間 fallback = `head+tail + sql_context.structure + sql_analysis` の counts/tables 併記

### 評価・測定
- eval/ で A/B 比較: 既存 `L1/L2/L3/L4` + 追加で `hallucination/unsupported-claim` 軸
- Skeleton 品質指標:
  - `parse_success_rate`
  - `compression_ratio`
  - `coverage flags`（CTE/JOIN/GROUP/HAVING/WINDOW が保持されたか）
  - `final SELECT 可視率`（head-tail fallback との比較用）
- セマンティック保存率: **人手ラベル 20–30 件**で診断に必要な構造残存を判定（厳密化は重い）
- 回帰: `raw SQL → skeleton text` の golden snapshot、代表ケース = `SELECT/CTE/MERGE/LATERAL VIEW/parse fail`

### 期待効果
| 指標 | 現状 truncate | Skeleton |
|---|---|---|
| LLM 入力 SQL サイズ | 3000 chars (~750 tokens) | 2000–3000 chars (~500–750 tokens) |
| CTE 依存関係の可視性 | 先頭 2-3 個のみ | 全部 |
| 最終 SELECT の見え方 | 切られて見えない | 見える |
| JOIN 全体像 | 不完全 | 完全 |
| トークン節約 | 0 | 30–40% |
| L3 診断精度 eval | — | +5〜15pt 想定（要測定）|

### 工数見積もり（Codex 修正版）
| スコープ | 工数 |
|---|---|
| 実装のみ MVP（`SELECT/WITH/JOIN/GROUP/ORDER/UNION/subquery`） | **2–3 日** |
| MVP + prompt 調整 + snapshot tests | **3–4 日** |
| `MERGE`/DDL/Databricks 特有構文の厚め対応 + eval A/B | **5–7 日** |

### 代替案（検討済み）
| 案 | 判定 |
|---|---|
| `sqlfluff` | AST 要約器としては重い。第一候補は引き続き `sqlglot` |
| 自前 regex skeletonizer | 主実装は保守負債大。**fallback 用には有力** |
| LLM に skeleton 化を頼む | コスト・遅延・再現性で本線不向き。評価ベースラインには有用 |
| `head 1500 + tail 1500` | **fallback として最有力**。blind truncate の最大欠点を即解消 |
| `structure だけ渡して SQL を消す` | 極端、JOIN/CTE 名の局所文脈消失で推奨の具体性低下 |

**結論**: `sqlglot skeleton` 単独ではなく、**`skeleton + head/tail fallback` の複合案** にする

### 仕様に追加すべき項目
- `適用条件`: どの SQL に使うか
- `非適用条件`: rewrite、literal 依存診断、MERGE 等
- `出力契約`: skeleton text のフォーマットと最大長
- `保持保証`: 必ず残すもの
- `省略表示`: 何をどう省略表示するか
- `fallback 階層`: 3 段
- `hallucination 防止プロンプト`
- `テスト観点`: parse fail、巨大 CTE、多段 subquery、Databricks 方言
- `可観測性`: parse success rate / fallback rate のログ

### v6 内優先順位: 中の上
- 既存 truncate は明確な品質ボトルネック
- **単体でやるより回帰検出とセット**で投資対効果大（新しい失敗モード = hallucination を入れるので snapshot/eval が前提に近い）
- Fact Pack 統合とも相性良（Fact Pack 強化で SQL 本文は構造寄りで足りる場面増）

### 推奨実装順（v6 内）
1. **回帰検出の最低限整備**（既存「レポート Regression 検知機能」セクション参照）
2. **SQL skeleton MVP**（本セクション）
3. **LLM 評価で A/B**
4. **Fact Pack と連携調整**

### 参照コード
- `dabs/app/core/llm_prompts/prompts.py:2277-2286`（truncate 箇所）
- `dabs/app/core/llm_prompts/prompts.py:2780-2786`（rewrite、フル SQL 必須）
- `dabs/app/core/llm_prompts/prompts.py:531-555`（sql_context メタ情報のみ）
- `dabs/app/core/family.py:151`（既存 sqlglot dialect fallback 実装例）
- `dabs/app/core/sql_patterns.py:24`（既存 CTE 多重参照）
- `dabs/app/core/extractors.py:824`（REFRESH parse 回避）

## 3 ステージ LLM + レポーティングアーキテクチャ再設計（設計済み・未着手 / 2026-04-23 Codex レビュー）

### 背景
v5.19.0 時点の構造は「高品質だが過剰に重い」。3 stage LLM (analyze → review → refine)、17+ section Fact Pack、markdown / parsed sections / JSON review / ActionCard の並立する中間形式、毎 stage knowledge filtering 繰り返し、post-hoc alert↔action 結線など、変換点と LLM call が多い。

### Codex メタ判断
**「LLM を賢くする」より「LLM にやらせる範囲を減らす」方が先。**

### 優先度順 (Codex 推奨)

| # | 改善 | コスト | 効果 |
|---|------|--------|------|
| 1 | **registry と LLM recommendations の責務分離** — LLM を novel findings / 因果推論 / rule 外仮説に限定 | medium | **最大** |
| 2 | Fact Pack の選択的縮小 (section scoring + hard cap) | medium | 大 |
| 3 | 3-stage → 2-stage (analyze + judge) | medium | 大 |
| 4 | knowledge filtering の session/query-hash キャッシュ | **small** | 確実 |
| 5 | alert ↔ action を emit 時に結線 (post-hoc match 廃止) | medium | 中 |
| 6 | レポート progressive disclosure (collapsible sections) | medium | UX のみ |
| 7 | JA/EN prompt template 統合 | medium | 保守性のみ、ROI 低 |

### 見落としがちな改善余地 (Codex 追加指摘)

- **単一 canonical intermediate model**: `Finding` / `Evidence` / `Action` / `LinkedAlert` schema を中心に LLM・registry・reporter の変換点を集約
- **dedup を事前制約に**: 「rule-based で既に covered な `root_cause_group`」を prompt に渡して LLM に **その group の提案を禁止**させる (post-hoc drop より自然)

### 維持すべきもの

- rule-based registry 自体 (22 cards の deterministic layer が強み)
- reporter の rule-based evidence sections (監査性)
- review/refine の**考え方**は維持、ただし slimmer に

### 推奨着手順

1. knowledge cache (small, 即効)
2. LLM に `covered_root_cause_groups` 渡して novel-only 化 (#1)
3. Fact Pack section scoring / cap (#2)
4. 2-stage 化 (#3)
5. ActionCard に alert ownership 内包 (#5)
6. canonical schema 化 (見落とし項目)
7. report UX / JA-EN 整理

### 前提・リスク

- 着手前に **レポート Regression 検知機能** (本 TODO.md 別セクション) を稼働させておくと安全。novel-only prompting や 2-stage 化は挙動変化が大きく、回帰の自動検出が無いと退行発覚が遅れる
- canonical schema 化は既存 LLM 出力パーサ全面改修になるため、まず周辺 (cache / prompting / Fact Pack) で効果を稼いでから着手

### 参照コード

- `dabs/app/core/usecases.py::_run_llm_stages` (3 stage)
- `dabs/app/core/llm_prompts/prompts.py::create_structured_system_prompt` (Fact Pack)
- `dabs/app/core/llm_prompts/knowledge.py::filter_knowledge_for_analysis` (毎 stage 実行)
- `dabs/app/core/analyzers/recommendations_registry.py::CARDS` (22 cards, deterministic)
- `dabs/app/core/usecases.py::_merge_llm_action_plan` (post-hoc group dedup)
- `dabs/app/core/reporters/alert_crossref.py::match_card_to_alert_numbers` (post-hoc alert↔action)
- `dabs/app/core/reporters/__init__.py::generate_report` (12 sections + Appendix A-H)
