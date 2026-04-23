# Databricks notebook source
# MAGIC %md
# MAGIC # ナレッジベース管理ツール
# MAGIC
# MAGIC `optimization_knowledge_base` ノートブックに新しい知識を追加・管理するツール。
# MAGIC
# MAGIC ## 使い方
# MAGIC 1. **入力ソース** を選択（テキスト / Google Docs URL / Workspace ノートブック）
# MAGIC 2. **Run All** で実行
# MAGIC 3. LLM が入力を解析し、ナレッジベース形式に構造化
# MAGIC 4. 確認後、ナレッジベースに追記
# MAGIC
# MAGIC ## 入力ソースの例
# MAGIC - 社内ドキュメント（Google Docs URL）
# MAGIC - 技術ブログや公式ドキュメントのテキスト
# MAGIC - Databricks ワークスペースのノートブック
# MAGIC - 手動入力のテキスト

# COMMAND ----------

# MAGIC %pip install openai typing_extensions --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration
dbutils.widgets.dropdown("input_type", "text", ["text", "google_docs_url", "workspace_notebook"], "Input Source Type")
dbutils.widgets.text("input_value", "", "Input Value (URL / Path / leave empty for text cell)")
dbutils.widgets.text("knowledge_category", "", "Category (e.g. streaming, join, cache)")
dbutils.widgets.text("model_endpoint", "databricks-claude-sonnet-4", "LLM Model Endpoint")

INPUT_TYPE = dbutils.widgets.get("input_type")
INPUT_VALUE = dbutils.widgets.get("input_value").strip()
KNOWLEDGE_CATEGORY = dbutils.widgets.get("knowledge_category").strip()
MODEL_ENDPOINT = dbutils.widgets.get("model_endpoint")

# ナレッジベースの保存先
KB_NOTEBOOK_PATH = "/Users/your-user@example.com/spark-perf-job/optimization_knowledge_base"

print(f"Input Type     : {INPUT_TYPE}")
print(f"Input Value    : {INPUT_VALUE or '(text cell below)'}")
print(f"Category       : {KNOWLEDGE_CATEGORY or '(auto-detect)'}")
print(f"Model          : {MODEL_ENDPOINT}")
print(f"KB Notebook    : {KB_NOTEBOOK_PATH}")

# COMMAND ----------

# DBTITLE 1,テキスト入力（input_type="text" の場合はここに貼り付け）
# ==============================================================================
# input_type = "text" の場合、以下の INPUT_TEXT に知識ソースのテキストを貼り付けて
# ください。Google Docs URL や Workspace Notebook の場合は空のままで OK です。
# ==============================================================================
INPUT_TEXT = """
ここに知識ソースのテキストを貼り付けてください。
例:
- 技術ブログの記事
- 公式ドキュメントの抜粋
- 社内のトラブルシューティングメモ
- Spark の設定パラメータの説明
"""

# COMMAND ----------

# DBTITLE 1,入力ソースの取得
import json
import re
import requests
from datetime import datetime, timezone

ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
host  = ctx.apiUrl().get().rstrip("/")
headers_db = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

source_text = ""
source_label = ""

if INPUT_TYPE == "text":
    source_text = INPUT_TEXT
    source_label = f"手動入力テキスト ({len(source_text)} chars)"

elif INPUT_TYPE == "google_docs_url":
    assert INPUT_VALUE, "Google Docs URL を input_value ウィジェットに入力してください"
    # Google Docs API でドキュメントを取得
    doc_id_match = re.search(r'/d/([a-zA-Z0-9_-]+)', INPUT_VALUE)
    assert doc_id_match, f"Google Docs ID を抽出できません: {INPUT_VALUE}"
    doc_id = doc_id_match.group(1)

    # Slides かどうかを判定
    is_slides = "presentation" in INPUT_VALUE or "slides" in INPUT_VALUE

    try:
        import subprocess
        gtoken = subprocess.run(
            ["python3", "-c",
             "import sys; sys.path.insert(0, ''); "
             "from google_auth import get_token; print(get_token())"],
            capture_output=True, text=True, cwd="/root/.claude/plugins/cache/fe-vibe/fe-google-tools/1.1.5/skills/google-auth/resources/"
        ).stdout.strip()

        if is_slides:
            r = requests.get(
                f"https://slides.googleapis.com/v1/presentations/{doc_id}",
                headers={"Authorization": f"Bearer {gtoken}", "x-goog-user-project": "your-gcp-project"}
            )
            assert r.status_code == 200, f"Slides API error: {r.status_code}"
            pres = r.json()
            lines = [f"# {pres.get('title', 'Untitled')}\n"]
            for i, slide in enumerate(pres.get("slides", [])):
                lines.append(f"\n## Slide {i+1}")
                for elem in slide.get("pageElements", []):
                    for te in elem.get("shape", {}).get("text", {}).get("textElements", []):
                        content = te.get("textRun", {}).get("content", "").strip()
                        if content:
                            lines.append(content)
                    table = elem.get("table", {})
                    if table:
                        for row in table.get("tableRows", []):
                            cells = []
                            for cell in row.get("tableCells", []):
                                ct = ""
                                for te in cell.get("text", {}).get("textElements", []):
                                    ct += te.get("textRun", {}).get("content", "").strip()
                                cells.append(ct)
                            if any(cells):
                                lines.append("| " + " | ".join(cells) + " |")
            source_text = "\n".join(lines)
        else:
            r = requests.get(
                f"https://docs.googleapis.com/v1/documents/{doc_id}",
                headers={"Authorization": f"Bearer {gtoken}", "x-goog-user-project": "your-gcp-project"}
            )
            assert r.status_code == 200, f"Docs API error: {r.status_code}"
            doc = r.json()
            lines = [f"# {doc.get('title', 'Untitled')}\n"]
            for elem in doc.get("body", {}).get("content", []):
                if "paragraph" in elem:
                    para_text = ""
                    for e in elem["paragraph"].get("elements", []):
                        para_text += e.get("textRun", {}).get("content", "")
                    if para_text.strip():
                        lines.append(para_text.strip())
            source_text = "\n".join(lines)

        source_label = f"Google Docs: {doc_id} ({len(source_text)} chars)"
    except Exception as e:
        print(f"⚠ Google API 読み込みエラー: {e}")
        print("→ テキストを手動で INPUT_TEXT に貼り付けて input_type='text' で再実行してください")
        source_text = ""

elif INPUT_TYPE == "workspace_notebook":
    assert INPUT_VALUE, "ワークスペースのノートブックパスを input_value に入力してください"
    r = requests.get(
        f"{host}/api/2.0/workspace/export",
        headers=headers_db,
        params={"path": INPUT_VALUE, "format": "SOURCE"}
    )
    assert r.status_code == 200, f"Workspace export error: {r.status_code} {r.text}"
    import base64
    source_text = base64.b64decode(r.json()["content"]).decode("utf-8")
    source_label = f"Workspace: {INPUT_VALUE} ({len(source_text)} chars)"

assert source_text and len(source_text) > 50, "入力テキストが短すぎます。50文字以上のテキストを入力してください。"
print(f"✅ ソース取得完了: {source_label}")
print(f"   先頭200文字: {source_text[:200]}...")

# COMMAND ----------

# DBTITLE 1,LLM でナレッジを構造化
from openai import OpenAI

client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")

extract_prompt = f"""あなたは Databricks / Apache Spark パフォーマンス最適化の専門家です。
以下のテキストから、Sparkジョブのパフォーマンス分析ツールのナレッジベースに追加すべき知識を抽出・構造化してください。

出力は必ず以下のJSON形式のみで返してください（JSON以外のテキストは不要）:

{{
  "knowledge_sections": [
    {{
      "title": "セクションタイトル",
      "category": "カテゴリ（compute/data_layout/code/bottleneck/diagnosis/streaming/cache/spot 等）",
      "content": "Markdown形式の構造化テキスト（箇条書き推奨、Sparkパラメータは名前と推奨値を明記）"
    }}
  ],
  "bottleneck_entries": [
    {{
      "type": "ボトルネックタイプ（既存: DATA_SKEW, DISK_SPILL, HIGH_GC, HEAVY_SHUFFLE, STAGE_FAILURE, MEMORY_SPILL, SPOT_LOSS, PHOTON_FALLBACK, SKEW_SHUFFLE_PARALLELISM。新規タイプも可）",
      "severity": "HIGH/MEDIUM/LOW",
      "description": "1行の説明",
      "recommendations": ["推奨アクション1", "推奨アクション2"]
    }}
  ],
  "summary": "追加される知識の要約（100文字以内）"
}}

ルール:
- Sparkのパフォーマンスに関連する情報のみを抽出してください
- 一般的すぎる情報は除外し、具体的なアクション・パラメータ・閾値を優先してください
- 既存のボトルネックタイプに追加すべき推奨がある場合は bottleneck_entries に含めてください
- 全く新しいボトルネックタイプを発見した場合も bottleneck_entries に含めてください
- knowledge_sections の content は Markdown 形式で、見出し(###)、箇条書き(-)、コードブロック(```)を使ってください
"""

print(f"LLM で知識を抽出中 ({MODEL_ENDPOINT})...")

# テキストが長すぎる場合は切り詰め
_max_input = 30000
if len(source_text) > _max_input:
    print(f"  入力テキストが長いため先頭 {_max_input} 文字に切り詰めます")
    source_text = source_text[:_max_input]

response = client.chat.completions.create(
    model=MODEL_ENDPOINT,
    messages=[
        {"role": "system", "content": extract_prompt},
        {"role": "user",   "content": f"カテゴリヒント: {KNOWLEDGE_CATEGORY or '自動検出'}\n\n---\n\n{source_text}"},
    ],
    max_tokens=8000,
    temperature=0.2,
)

raw_output = response.choices[0].message.content
print(f"✅ LLM 応答受信 ({response.usage.total_tokens} tokens)")

# COMMAND ----------

# DBTITLE 1,抽出結果の確認
json_match = re.search(r'\{.*\}', raw_output, re.DOTALL)
assert json_match, f"JSON not found in response:\n{raw_output}"

extracted = json.loads(json_match.group())
knowledge_sections = extracted.get("knowledge_sections", [])
bottleneck_entries = extracted.get("bottleneck_entries", [])
summary = extracted.get("summary", "")

print(f"=== 抽出結果サマリー ===")
print(f"  {summary}")
print(f"\n=== ナレッジセクション: {len(knowledge_sections)} 件 ===")
for i, sec in enumerate(knowledge_sections, 1):
    print(f"\n  [{i}] {sec['title']}  (category: {sec['category']})")
    # 先頭3行だけ表示
    for line in sec['content'].split('\n')[:3]:
        print(f"      {line}")
    if len(sec['content'].split('\n')) > 3:
        print(f"      ... ({len(sec['content'])} chars)")

print(f"\n=== ボトルネック推奨: {len(bottleneck_entries)} 件 ===")
for i, bn in enumerate(bottleneck_entries, 1):
    print(f"  [{i}] {bn['type']} ({bn['severity']}): {bn['description']}")
    for r in bn.get('recommendations', [])[:2]:
        print(f"      - {r}")
    if len(bn.get('recommendations', [])) > 2:
        print(f"      ... (+{len(bn['recommendations'])-2} more)")

# COMMAND ----------

# DBTITLE 1,ナレッジベースに追記
# ==============================================================================
# 確認後、このセルを実行するとナレッジベースに追記されます。
# 追記をやめたい場合はこのセルをスキップしてください。
# ==============================================================================

# --- 現在のナレッジベースを取得 ---
r = requests.get(
    f"{host}/api/2.0/workspace/export",
    headers=headers_db,
    params={"path": KB_NOTEBOOK_PATH, "format": "SOURCE"}
)
assert r.status_code == 200, f"KB export error: {r.status_code} {r.text}"
import base64
current_kb = base64.b64decode(r.json()["content"]).decode("utf-8")

# --- OPTIMIZATION_KNOWLEDGE_BASE テキストに追記 ---
timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
new_sections_text = f"\n\n## 追加知識 ({timestamp})\n### ソース: {source_label}\n"
for sec in knowledge_sections:
    new_sections_text += f"\n### {sec['title']}\n{sec['content']}\n"

# 閉じ三重引用符の直前に挿入
kb_insert_marker = '"""'
# OPTIMIZATION_KNOWLEDGE_BASE の最後の """ を探す
_parts = current_kb.split('OPTIMIZATION_KNOWLEDGE_BASE = """')
assert len(_parts) == 2, "OPTIMIZATION_KNOWLEDGE_BASE の定義が見つかりません"
_header = _parts[0] + 'OPTIMIZATION_KNOWLEDGE_BASE = """'
_rest = _parts[1]
# 最初の """ で本文を終了
_kb_end_idx = _rest.index('"""')
_kb_body = _rest[:_kb_end_idx]
_after_kb = _rest[_kb_end_idx:]

updated_kb_body = _kb_body.rstrip() + "\n" + new_sections_text + "\n"
updated_content = _header + updated_kb_body + _after_kb

# --- BOTTLENECK_RECOMMENDATIONS 辞書に追記 ---
for bn in bottleneck_entries:
    bn_type = bn["type"]
    # 既存エントリの更新 or 新規追加
    entry_str = json.dumps({
        "severity": bn["severity"],
        "description": bn["description"],
        "recommendations": bn["recommendations"],
    }, ensure_ascii=False, indent=8)

    if f'"{bn_type}"' in updated_content:
        # 既存エントリ — recommendations に追記
        print(f"  既存エントリ '{bn_type}' に推奨を追加")
        # 既存の recommendations リストに新しい推奨を追加するのは複雑なので、
        # コメントとして追記
        comment = f"\n    # [{timestamp}] 追加推奨 for {bn_type}:\n"
        for r in bn["recommendations"]:
            comment += f"    #   - {r}\n"
        # BOTTLENECK_RECOMMENDATIONS の閉じ } の直前に挿入
        updated_content = updated_content.replace(
            "\n}\n",
            f"{comment}\n}}\n",
            1  # 最後の } のみ
        )
    else:
        # 新規エントリ
        print(f"  新規エントリ '{bn_type}' を追加")
        new_entry = f"""    "{bn_type}": {{
        "severity": "{bn["severity"]}",
        "description": "{bn["description"]}",
        "recommendations": {json.dumps(bn["recommendations"], ensure_ascii=False, indent=12)},
    }},\n"""
        # 最後の } の直前に挿入
        last_brace = updated_content.rfind("}")
        updated_content = updated_content[:last_brace] + new_entry + updated_content[last_brace:]

# --- ファイルに書き出してアップロード ---
_tmp_path = "/tmp/optimization_knowledge_base_updated.py"
with open(_tmp_path, "w") as f:
    f.write(updated_content)

import subprocess
result = subprocess.run(
    ["databricks", "workspace", "import", KB_NOTEBOOK_PATH,
     "--format", "SOURCE", "--language", "PYTHON", "--file", _tmp_path, "--overwrite"],
    capture_output=True, text=True
)
assert result.returncode == 0, f"Upload error: {result.stderr}"

print(f"\n✅ ナレッジベースを更新しました")
print(f"   ナレッジセクション追加: {len(knowledge_sections)} 件")
print(f"   ボトルネック推奨追加/更新: {len(bottleneck_entries)} 件")
print(f"   ソース: {source_label}")
print(f"   タイムスタンプ: {timestamp}")

# COMMAND ----------

# DBTITLE 1,更新後の確認
# 更新後のナレッジベースの統計
r = requests.get(
    f"{host}/api/2.0/workspace/export",
    headers=headers_db,
    params={"path": KB_NOTEBOOK_PATH, "format": "SOURCE"}
)
updated_text = base64.b64decode(r.json()["content"]).decode("utf-8")

# OPTIMIZATION_KNOWLEDGE_BASE のサイズ
_kb_match = re.search(r'OPTIMIZATION_KNOWLEDGE_BASE = """(.*?)"""', updated_text, re.DOTALL)
kb_size = len(_kb_match.group(1)) if _kb_match else 0

# BOTTLENECK_RECOMMENDATIONS のエントリ数
bn_count = updated_text.count('"severity"')

# 追加知識のセクション数
added_sections = len(re.findall(r'## 追加知識', updated_text))

print(f"=== ナレッジベース統計 ===")
print(f"  OPTIMIZATION_KNOWLEDGE_BASE : {kb_size:,} chars")
print(f"  BOTTLENECK_RECOMMENDATIONS  : {bn_count} エントリ")
print(f"  追加知識セクション           : {added_sections} 回分")
print(f"\n  ノートブック: {KB_NOTEBOOK_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用例
# MAGIC
# MAGIC ### 1. テキストから知識を追加
# MAGIC ```
# MAGIC input_type = "text"
# MAGIC → INPUT_TEXT セルにテキストを貼り付け
# MAGIC → Run All
# MAGIC ```
# MAGIC
# MAGIC ### 2. Google Docs から知識を追加
# MAGIC ```
# MAGIC input_type = "google_docs_url"
# MAGIC input_value = "https://docs.google.com/document/d/xxx/edit"
# MAGIC → Run All
# MAGIC ```
# MAGIC
# MAGIC ### 3. Google Slides から知識を追加
# MAGIC ```
# MAGIC input_type = "google_docs_url"
# MAGIC input_value = "https://docs.google.com/presentation/d/xxx/edit"
# MAGIC → Run All
# MAGIC ```
# MAGIC
# MAGIC ### 4. ワークスペースのノートブックから知識を追加
# MAGIC ```
# MAGIC input_type = "workspace_notebook"
# MAGIC input_value = "/Users/user@company.com/path/to/notebook"
# MAGIC → Run All
# MAGIC ```
