#!/usr/bin/env python3
"""LLM Model Evaluation: generate reports with 2 models, then 3 judges vote.

Usage:
    cd databricks-apps
    python ../scripts/eval_models.py ../json/customer/Concurrent2.json

Generates reports with Opus 4.6 and GPT 5.4, then has Opus/GPT/Gemini
evaluate both reports and vote on which is better.
"""

import json
import os
import sys
import time

# Add databricks-apps to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "databricks-apps"))

from core.llm_client import call_llm_with_retry, create_openai_client
from core.usecases import LLMConfig, PipelineOptions, run_analysis_pipeline

# Model configurations: each entry is (primary, review, refine, label)
# Edit this list to change which configurations to compare.
# The first two entries are compared against each other.
# Model configurations: each entry is (primary, review, refine, label)
# Edit this list to change which configurations to compare.
# Default: full-Opus vs full-GPT (A vs B pattern)
# Best config found: E (Opus+Opus+GPTrefine) - see docs/llm-model-evaluation-results.md
CANDIDATE_CONFIGS = [
    ("databricks-claude-opus-4-7", "databricks-claude-opus-4-7", "databricks-claude-opus-4-7", "Opus-full"),
    ("databricks-gpt-5-5", "databricks-gpt-5-5", "databricks-gpt-5-5", "GPT-full"),
]

# Models to evaluate (judges)
JUDGE_MODELS = [
    "databricks-claude-opus-4-7",
    "databricks-gpt-5-5",
    "databricks-gemini-3-1-pro",
]

EVAL_PROMPT_JA = """あなたはDatabricks SQLパフォーマンス分析のエキスパート審査員です。
以下の2つのレポート（Report AとReport B）を比較し、以下の観点で5段階評価してください。

## 評価観点
1. **正確性** (1-5): メトリクスの解釈が正しいか、誤診断がないか
2. **洞察力** (1-5): 表面的な数値の羅列ではなく、根本原因の推測や相関分析ができているか
3. **実用性** (1-5): 推奨アクションが具体的で、すぐ実行可能か
4. **網羅性** (1-5): 重要なメトリクスを見落としていないか
5. **説明力** (1-5): 非専門家にも理解できる説明になっているか

## Report A ({model_a})
{report_a}

## Report B ({model_b})
{report_b}

## 出力形式（JSON）
必ず以下のJSON形式で回答してください。JSON以外のテキストは含めないでください。
```json
{{
  "report_a_scores": {{
    "accuracy": <1-5>,
    "insight": <1-5>,
    "practicality": <1-5>,
    "coverage": <1-5>,
    "clarity": <1-5>,
    "total": <5-25>
  }},
  "report_b_scores": {{
    "accuracy": <1-5>,
    "insight": <1-5>,
    "practicality": <1-5>,
    "coverage": <1-5>,
    "clarity": <1-5>,
    "total": <5-25>
  }},
  "winner": "A" or "B" or "TIE",
  "reasoning": "<日本語で100文字以内の判定理由>"
}}
```"""


def generate_report(
    profile_path: str,
    primary_model: str,
    review_model: str,
    refine_model: str,
    label: str,
) -> str:
    """Generate a report using the specified 3-model pipeline."""
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    with open(profile_path, encoding="utf-8") as f:
        data = json.load(f)

    llm_config = LLMConfig(
        primary_model=primary_model,
        review_model=review_model,
        refine_model=refine_model,
        databricks_host=host,
        databricks_token=token,
        lang="ja",
    )
    options = PipelineOptions(
        skip_llm=False,
        skip_review=False,
        skip_refine=False,
        lang="ja",
    )
    # Suppress warehouse lookup warning
    import logging
    logging.getLogger("core.warehouse_client").setLevel(logging.ERROR)

    print(f"  [{label}] Primary={primary_model}, Review={review_model}, Refine={refine_model}", file=sys.stderr)
    start = time.time()

    def on_stage(stage):
        elapsed_so_far = time.time() - start
        print(f"    Stage: {stage} ({elapsed_so_far:.0f}s)", file=sys.stderr)

    result = run_analysis_pipeline(data, llm_config, options, on_stage=on_stage)
    elapsed = time.time() - start
    print(f"  [{label}] Done ({elapsed:.1f}s, {len(result.report)} chars)", file=sys.stderr)
    return result.report


def evaluate_reports(
    report_a: str, model_a: str,
    report_b: str, model_b: str,
    judge_model: str,
) -> dict:
    """Have a judge model evaluate two reports."""
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    # Truncate reports if too long (keep first 8000 chars each)
    max_chars = 8000
    ra = report_a[:max_chars] + "\n...(truncated)" if len(report_a) > max_chars else report_a
    rb = report_b[:max_chars] + "\n...(truncated)" if len(report_b) > max_chars else report_b

    prompt = EVAL_PROMPT_JA.format(
        model_a=model_a, model_b=model_b,
        report_a=ra, report_b=rb,
    )

    client = create_openai_client(host, token)

    print(f"  Judge: {judge_model}...", file=sys.stderr)
    start = time.time()
    response = call_llm_with_retry(
        client=client,
        model=judge_model,
        messages=[
            {"role": "system", "content": "You are an expert evaluator. Always respond in valid JSON only."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=2048,
        temperature=0.1,
    )
    elapsed = time.time() - start
    print(f"  Done ({elapsed:.1f}s)", file=sys.stderr)

    # Parse JSON from response (handles various formats)
    import re

    # Handle list responses (Gemini returns list of content blocks)
    text = response
    if isinstance(response, list):
        for item in response:
            if isinstance(item, dict) and "text" in item:
                text = item["text"]
                break
        else:
            text = str(response)
    if not isinstance(text, str):
        text = str(text)

    # Remove markdown code fences
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)

    # Try to extract JSON object
    json_match = re.search(r'\{[\s\S]*\}', text)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass

    return {"error": "Failed to parse JSON", "raw": text[:500]}


def main():
    if len(sys.argv) < 2:
        print("Usage: python eval_models.py <profile.json>", file=sys.stderr)
        sys.exit(1)

    profile_path = sys.argv[1]
    if not os.path.exists(profile_path):
        print(f"Error: File not found: {profile_path}", file=sys.stderr)
        sys.exit(1)

    configs = CANDIDATE_CONFIGS
    labels = [c[3] for c in configs]

    print("=" * 60, file=sys.stderr)
    print("LLM Model Evaluation (Full 3-Stage Pipeline)", file=sys.stderr)
    print(f"Profile: {profile_path}", file=sys.stderr)
    for primary, review, refine, label in configs:
        print(f"  {label}: Primary={primary}, Review={review}, Refine={refine}", file=sys.stderr)
    print(f"Judges: {JUDGE_MODELS}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    # Step 1: Generate reports with full 3-stage pipeline
    print("\n[Step 1] Generating reports (Primary → Review → Refine)...", file=sys.stderr)
    reports = {}
    for primary, review, refine, label in configs:
        reports[label] = generate_report(profile_path, primary, review, refine, label)

    # Save reports
    for label, report in reports.items():
        out_path = f"eval_report_{label}.md"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"  Saved: {out_path}", file=sys.stderr)

    # Step 2: Evaluate
    print("\n[Step 2] Evaluating reports...", file=sys.stderr)
    model_a = labels[0]
    model_b = labels[1]
    evaluations = {}

    for judge in JUDGE_MODELS:
        result = evaluate_reports(
            reports[model_a], model_a,
            reports[model_b], model_b,
            judge,
        )
        evaluations[judge] = result

    # Step 3: Tally votes
    print("\n[Step 3] Results", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    votes = {"A": 0, "B": 0, "TIE": 0}
    config_a = configs[0]
    config_b = configs[1]
    results_output = {
        "profile": profile_path,
        "model_a": {"label": model_a, "primary": config_a[0], "review": config_a[1], "refine": config_a[2]},
        "model_b": {"label": model_b, "primary": config_b[0], "review": config_b[1], "refine": config_b[2]},
        "judges": {},
        "final_result": {},
    }

    for judge, ev in evaluations.items():
        winner = ev.get("winner", "?")
        reasoning = ev.get("reasoning", "N/A")
        a_scores = ev.get("report_a_scores", {})
        b_scores = ev.get("report_b_scores", {})
        a_total = a_scores.get("total", "?")
        b_total = b_scores.get("total", "?")

        print(f"\nJudge: {judge}", file=sys.stderr)
        print(f"  Report A ({model_a}): {a_total}/25  {a_scores}", file=sys.stderr)
        print(f"  Report B ({model_b}): {b_total}/25  {b_scores}", file=sys.stderr)
        print(f"  Winner: {winner}", file=sys.stderr)
        print(f"  Reason: {reasoning}", file=sys.stderr)

        if winner in votes:
            votes[winner] += 1

        results_output["judges"][judge] = ev

    # Final tally
    print(f"\n{'='*60}", file=sys.stderr)
    print(f"Final Votes: A({model_a})={votes['A']}  B({model_b})={votes['B']}  TIE={votes['TIE']}", file=sys.stderr)

    if votes["A"] > votes["B"]:
        final = f"A ({model_a}) wins {votes['A']}-{votes['B']}"
    elif votes["B"] > votes["A"]:
        final = f"B ({model_b}) wins {votes['B']}-{votes['A']}"
    else:
        final = "TIE"

    print(f"Result: {final}", file=sys.stderr)
    results_output["final_result"] = {
        "votes": votes,
        "winner": final,
    }

    # Output JSON results to stdout
    print(json.dumps(results_output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
