# V6 Knowledge Inventory (Week 3 Day 1)

V6 R6 (knowledge 注入整理) の前段として、現行 v5.19 の knowledge 関連
資産・注入経路・サイズを棚卸しする。Codex の「LLM プロンプト精度向上」
指摘 (TODO.md 別セクション) を踏まえ、削るべき箇所を特定する。

## 1. Knowledge ファイル一覧

`dabs/app/core/knowledge/` 配下、JA/EN ペアで 14 ファイル / 計 5,182 行。

| トピック | JA 行数 | EN 行数 | 備考 |
|---------|--------:|--------:|------|
| `dbsql_tuning` | **1,586** | **1,586** | **最大、ここを削るのが効果的** |
| `dbsql_sql_patterns` | 239 | 227 | パターン集 |
| `dbsql_advanced` | 187 | 187 | 上級チューニング (autoBroadcast 等) |
| `dbsql_photon_oom` | 116 | 116 | Photon OOM 専用 |
| `dbsql_serverless` | 99 | 98 | Serverless 制約 |
| `spark_tuning` | 324 | 323 | Spark 全般 |
| `spark_streaming_tuning` | 47 | 47 | DLT/SDP |

`dbsql_tuning.md` は **43 個の `## ` ヘッダ** を含み、`<!-- section_id: ... -->`
マーカーで section_id ベースのルーティングが効くようになっている。

## 2. section_id ルーティング (`core/llm_prompts/knowledge.py`)

主要関数 (knowledge.py から):

| 関数 | 役割 | 注意点 |
|------|------|--------|
| `load_tuning_knowledge(base_path, lang)` | dbsql_*.md を全部結合して返す | ja/en 切替 |
| `load_spark_tuning_knowledge` | spark_*.md を結合 | 同上 |
| `parse_knowledge_sections(text)` | section_id → 本文 dict | マーカー必須 |
| `filter_knowledge_by_alerts(text, alerts)` | alert に基づき関連 section を選別 | severity_rank で重み |
| `filter_knowledge_for_analysis(text, alerts, ...)` | analysis stage 向けの選別 + ALWAYS_INCLUDE 強制 | **3 stage で毎回呼ばれる** |
| `_trim_by_priority` | サイズ閾値超過時に低優先度を切る | |
| `_summarize_sections_with_llm` | 要約版を作る (二次生成) | **Codex 指摘 #4 で停止候補** |

### `ALWAYS_INCLUDE_SECTION_IDS`

```python
ALWAYS_INCLUDE_SECTION_IDS = ["bottleneck_summary", "spark_params", "appendix"]
```

問題: Codex 指摘 #3 で「`spark_params` / `appendix` を ALWAYS_INCLUDE から
除去すべき」とされている。これらは alert 不問で毎回投入されるため
誤 reject の温床。

### 注入回数 (`core/llm.py` の検索結果)

`filter_knowledge_for_analysis` が **5箇所** で呼ばれている:

| 行 | 用途 | 削除候補? |
|----|------|----------|
| `llm.py:87` | `analyze_with_llm` 主経路 | 残す (Phase 1 = 必要) |
| `llm.py:185` | (確認要) | |
| `llm.py:297` | `review_with_llm` Phase 2 | **Codex #1 で除去推奨** |
| `llm.py:349` | `refine_with_llm` Phase 3 | **Codex #2 で除去推奨** |
| `llm.py:404` | (確認要) | |
| `llm.py:450` | (確認要) | |

→ 主要 3 stage すべてに knowledge 全文が入っている = 同じ内容を 3 回 LLM
  入力に載せている = レイテンシ・コスト・誤 reject の主因。

## 3. 直近の改修ベース (TODO.md より)

TODO.md「LLM プロンプト精度向上 (保留中・2026-04-19)」セクションに、
Codex が **削るべき場所を 8 項目で特定済み**。Week 3 で実装すべき優先度:

| Codex # | 内容 | 影響範囲 | W3 優先度 |
|---------|------|---------|-----------|
| **#1** | Phase 2 review から knowledge 完全除去 | `llm.py:297`, `prompts.py:835+` | **高** (Day 4) |
| **#2** | Phase 3 refine から knowledge 全文除去 | `llm.py:349`, `prompts.py:1178+` | **高** (Day 4) |
| **#3** | `ALWAYS_INCLUDE_SECTION_IDS` から spark_params/appendix 除去 | `knowledge.py:199` | **高** (Day 2) |
| #4 | Condensed knowledge 再注入 (`_summarize_sections_with_llm`) 停止 | `knowledge.py:474-492` | 中 (Day 4) |
| #5 | Fact Pack を summary-first / anomaly-only に再編 | `prompts.py:1689+` | 中 (Day 4 後半) |
| #6 | Appendix rewrite patterns を analysis のみ optional に | `dbsql_tuning_en.md:1144-1258` | 低 (Week 3 後半) |
| #7 | Knowledge matrix 衝突解消 (classic vs serverless autoBroadcast) | L230-242 vs L1083-1104 | 中 (Day 2) |
| #8 | Recommendation format block の圧力緩和 | `prompts.py:23-94` | 低 (W3 後半) |

### 削りすぎ閾値 (Codex 推奨)

| Phase | knowledge 量目標 |
|-------|-----------------|
| analysis | 8-15KB (appendix / 長い examples 除く) |
| review | **0KB** (理想) / 最大 2KB |
| refine | 0-4KB (issue 解決に必要な断片のみ) |

## 4. canonical-direct 出力との関係 (Day 3 着手前の整理)

Week 3 の **Day 3 (LLM prompt canonical-direct 出力) と Day 4 (knowledge
注入整理)** の順序判断:

**先に Day 3 (canonical-direct 出力) を入れる理由:**
- knowledge を削った効果を Q3 evidence grounding scorer で測るには
  Finding/Action が canonical で emit されている必要がある
- normalizer 経由だと「LLM が削られた knowledge をどう補完したか」が
  evidence の grounded フラグに落ちない
- W2.5 で normalizer に SCOPE NOTICE を入れた通り、heuristic 山を
  作らない方針

**先に Day 4 (knowledge 整理) を入れる理由:**
- Phase 2/3 の knowledge を抜いた状態で baseline を取り、Day 3 で
  canonical-direct に変えたときの差分を区別しやすい

→ **結論: Day 3 で feature flag だけ先に入れる (default off)**。
  実際に Phase 2/3 の knowledge を抜くのは Day 4。Day 3 は schema
  v6 出力 mode を実装するが、Day 4 で削る対象も整理してから両方を
  プロンプト改修コミットで入れる。

## 5. 注入経路の重複・ノイズ

`core/llm.py` を読んだだけで判明する重複:

| 重複箇所 | 何が起きているか |
|---------|----------------|
| 3 stage 全部 | 同じ knowledge 全文 (max 30KB) が 3 回 LLM input に乗る |
| `ALWAYS_INCLUDE` 強制 | alert がなくても spark_params / appendix が常に入る |
| `_summarize_sections_with_llm` | 二次生成で要約版を作り、矛盾を増やす |
| `Recommendation format block` | 全項目を埋めるよう圧力をかける `prompts.py:23-94` |

これらが **「LLM が answer の中身ではなく形式と参照テンプレに
合わせ込む」** 副作用を生む。Q3 evidence grounding scorer が
ungrounded numeric を penalty 化する前提にも合う。

## 6. Q3 (Day 5) で測るべき項目

W2.5 で hallucination scorer に `grounded ratio` を入れた。Day 5 では
**knowledge ↔ Finding ↔ Evidence の三者整合性** をさらに細かく見る:

| 指標 | 計算 | データ源 |
|------|------|---------|
| `evidence_citation_coverage` | 各 Finding.evidence で `source` が profile 系 path or knowledge_section_id を指す比率 | canonical Report |
| `unsupported_claim_rate` | Action.what / why に出る数値 (% / GB / 倍 等) で profile/grounded evidence にアンカーがない比率 | canonical Report |
| `knowledge_source_consistency` | knowledge_section_id を引用した場合、その section が当該 issue の category と整合するか (registry 経由) | canonical + issue_registry |
| `grounded_evidence_per_finding` | Finding 1件あたりの `evidence[].grounded=true` 数 (Week 2.5 で導入済の集計版) | canonical Report |

## 7. Codex 確認 (2026-04-25 W3 着手前)

Codex の追加指摘を反映:

### Day 1 で確認すべき具体ファイル:行 (Codex 指摘)

- `dabs/app/core/llm.py:87` — analysis stage 注入
- `dabs/app/core/llm.py:297` — **review でも再フィルタ + 再注入 (本丸)**
- `dabs/app/core/llm.py:349` — refine でも再注入 (scope creep 主因)
- `dabs/app/core/llm_prompts/knowledge.py:199` — `ALWAYS_INCLUDE_SECTION_IDS`
- `dabs/app/core/llm_prompts/knowledge.py:390` — `filter_knowledge_for_analysis` 本体
- `dabs/app/core/llm_prompts/prompts.py:1401` — `create_review_system_prompt`
- `dabs/app/core/llm_prompts/prompts.py:1783` — `create_refine_system_prompt`
- `dabs/app/core/v6_schema/normalizer.py:256` — `Evidence.grounded` 判定

### Codex 推奨 W3 優先 4 項目

| # | 内容 | 配置 |
|---|------|------|
| 1 | Phase 2 review から knowledge 完全除去 | Day 4 中心 |
| 2 | Phase 3 refine から knowledge 全文除去 | Day 4 |
| 3 | `spark_params` / `appendix` の ALWAYS_INCLUDE 除去 | Day 2 (即効) |
| 8 | Recommendation format block の圧力緩和 | Day 4 後半 |

補欠: #4 Condensed knowledge 再注入停止。

### Day 3 canonical-direct vs Day 4 knowledge 削減 (順序確定)

**Day 3 canonical-direct 先 → Day 4 knowledge 削減後**。

理由 (Codex):
- 独立変数を分ける = 出力形式と入力 knowledge を別タイミングで切替
- canonical を先に入れれば「knowledge 削減の効果」を schema 上で純粋に測れる
- 逆だと「prompt 入力と出力 contract が同時揺れ」で Q3 失敗原因が混ざる

### Q3 scorer 必須シグナル (Codex 推奨)

1. **Evidence metric grounding** — `metric_name` / `value_raw` / `source` が profile 既知 metric と一致
2. **Ungrounded numeric claim** — evidence/action rationale の数値が profile/knowledge にアンカーあり
3. **Valid source reference** — `evidence.source` が許可 taxonomy (`alert:*`, `profile`, `knowledge:*` 等) に収まる
4. **Valid knowledge source id** — `knowledge_source_id` が実在 section_id と対応
5. **Finding-level support ratio** — 各 Finding が最低 1 件 `grounded=true` evidence を持つ (synthetic/raw text only でない)

### Week 3 地雷 (Codex)

- **review/refine 削除で refine が空転**: `prompts.py:1561` 周辺の防御 (review 出力なしを reject 扱い) を refine 入力契約と同時調整
- **複数 prompt 系統**: analysis/review/refine + report review/refine + structured + rewrite。片方だけ直すと回帰
- **ALWAYS_INCLUDE が隠れ注入**: analysis だけ削っても spark_params / appendix が常時戻る可能性
- **Evidence.grounded 過剰罰則**: raw text → False の現行判定で hallucination scorer を過罰しないよう scorer 側で吸収
- **canonical direct と normalizer fallback の二重系**: feature flag 期間中は両方を出して比較できる状態を維持、片方だけ突然 source of truth にしない

## 8. Day 1 アクション

- [x] knowledge ファイル / ALWAYS_INCLUDE / 注入関数 / 5回注入経路を棚卸し
- [x] Codex 既存指摘 (#1-#8) と W3 タスクを対応付け
- [x] Day 3 vs Day 4 の順序判断 (feature flag 先 / knowledge 削減本体は後)
- [x] Q3 で測る 4 指標を初期定義
- [ ] (Day 2) `docs/knowledge/v6_knowledge_policy.md` で公式ポリシー化
- [ ] (Day 3) `core/feature_flags.py` 新設 + LLM prompt v6 mode 追加

## 参照

- `dabs/app/core/knowledge/` (14 ファイル, 5,182 行)
- `dabs/app/core/llm_prompts/knowledge.py:35`-`631`
- `dabs/app/core/llm.py:87` / `:185` / `:297` / `:349` / `:404` / `:450`
- TODO.md 「LLM プロンプト精度向上 (保留中・2026-04-19 Codex レビュー)」
- W2.5 完了時の baseline: `eval/baselines/v6_w25_baseline.json`
- Codex 指摘 #1-#8 の詳細: TODO.md 487-512 行
