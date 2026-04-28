# V6 Quality Evaluation Toolkit

Quality measurement infrastructure for the V6 refactor (TODO.md
`### v6.0 — レポート品質向上リファクタリング`).

## Quick start

### Single-condition baseline (existing)

`goldens_runner.py` runs all goldens once under the current environment
flags, producing per-case metrics + R4 schema validation + Q3 evidence
grounding.

```bash
PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \
  --baseline-name v6_baseline \
  --skip-judge --skip-llm
```

Outputs:
- `eval/baselines/<name>.json` — per-case metric records
- `eval/reports/<name>.md` — markdown summary

### A/B runner (Week 4)

`ab_runner.py` runs goldens_runner under 4 conditions in child processes
(env var-isolated, no feature_flags cache pollution) and aggregates a
diff/regression/R10 summary.

```bash
PYTHONPATH=dabs/app:. uv run python -m eval.ab_runner \
  --run-name v6_w4 \
  --skip-judge --skip-llm
```

Conditions:
| Name | env vars |
|------|----------|
| `baseline` | (no V6 flag) |
| `canonical-direct` | `V6_CANONICAL_SCHEMA=1` |
| `no-force-fill` | `V6_RECOMMENDATION_NO_FORCE_FILL=1` |
| `both` | all 6 V6 flags on |

Outputs:
- `eval/ab_summary/<run_name>.json`
- `eval/ab_summary/<run_name>.md`

## CI gating

### Per-signal gates (Week 2.5 + Week 3.5)

```bash
python -m eval.goldens_runner \
  --baseline-name pr_check \
  --gate-schema-pass 1.0 \
  --gate-actionability 0.9 \
  --gate-finding-support 0.55 \
  --gate-metric-grounded 0.40 \
  --gate-ungrounded-numeric-max 0.25 \
  --gate-valid-source 0.95 \
  --gate-valid-knowledge-id 0.95
```

Exit 1 when any threshold breached.

### W4 infra gate (rule-based pass)

`--gate-w4-infra` is the **infrastructure / pipeline** gate. It passes
on a rule-based-only run and is meant to keep the eval framework itself
healthy:

- All 4 conditions actually executed
- schema_pass = 100% in every condition
- regressions = 0 in every candidate condition (rule-based should be flat)
- R10 layer_a_score ≥ 0.55 in every condition

```bash
python -m eval.ab_runner --run-name pr_w4 --gate-w4-infra
```

### LLM quality gate (Week 4 substantive completion)

`--gate-llm-quality` (alias: `--gate-w4-completion`) applies the 9
substantive thresholds. **A rule-based run cannot pass this gate** —
canonical_parse_failure is 100% by design when the LLM is not invoked.
Use only when LLM API access is configured.

```bash
python -m eval.ab_runner \
  --run-name pr_w4 \
  --gate-llm-quality \
  --gate-condition both
```

Targets (from TODO.md):
- Q3 composite ≥ 80%
- metric_grounded ≥ 70%
- finding_support ≥ 80%
- ungrounded_numeric ≤ 15%
- Critical recall (strict) ≥ 50%
- Hallucination clean ≥ 0.85
- Schema pass = 100%
- Case regressions ≤ 1
- Canonical parse failure ≤ 5%

**Important** (Codex W4 review): rule-based-only baselines have
`canonical_parse_failure_rate = 100%` by construction (no LLM = no
LLM-direct emit). The 5% target only makes sense when the LLM API is
enabled. The A/B summary now also reports `canonical_source_breakdown`
so Week 5 can distinguish `normalizer_fallback` (LLM ran but didn't
emit a valid block) vs `missing` (LLM didn't run at all).

### R10 verdict gate

```bash
python -m eval.ab_runner \
  --run-name pr_w4 \
  --gate-r10-verdict pass
```

Fails if any condition's R10 layer-A verdict is below `pass`. Use
`borderline` for a more lenient gate.

## LLM-enabled mode

Without `--skip-llm`, conditions actually exercise different prompts:
- canonical-direct: LLM emits ` ```json:canonical_v6 ` block →
  parsed via `parsing.extract_v6_canonical_block` →
  `PipelineResult.canonical_report_llm_direct`
- no-force-fill: LLM allowed to omit fields lacking grounding
- both: combined + review/refine knowledge skipped

```bash
export DATABRICKS_HOST=...
export DATABRICKS_TOKEN=...
PYTHONPATH=dabs/app:. uv run python -m eval.ab_runner \
  --run-name v6_w4_llm \
  --gate-w4-completion
```

## Files

```
eval/
├── ab_runner.py              # 4 conditions A/B (W4 Day 2-3 + R10 Day 5)
├── goldens_runner.py         # single-condition runner (W1+)
├── scorers/
│   ├── actionability.py      # Q4 (W1+W2.5)
│   ├── evidence_grounding.py # Q3 (W3 + W3.5 weighted)
│   ├── hallucination.py      # (W1+W2.5)
│   ├── l1_syntax.py          # L1 (existing)
│   ├── l2_evidence.py        # L2 (existing)
│   ├── l3l4_judge.py         # L3/L4 LLM judge (existing)
│   ├── r4_schema.py          # R4 schema validation (W2.5)
│   ├── r10_quality.py        # R10 add-on (W4 Day 5)
│   └── recall.py             # Recall (W2.5 strict + lenient)
├── goldens/
│   ├── manifest.yaml         # 29 cases
│   └── cases/
│       ├── *.yaml            # main goldens (22)
│       └── evidence_grounding/  # Q3-specific (7)
└── tests/
    ├── test_v6_ab_runner.py
    ├── test_v6_evidence_grounding.py
    ├── test_v6_r10.py
    └── test_v6_scorers.py
```

## Reference

- `docs/eval/report_quality_rubric.md` — 5 quality indicators
- `docs/eval/scorer_mapping.md` — rubric ↔ scorer table
- `docs/eval/ab_runner_design.md` — A/B runner design
- `docs/eval/r10_quality_addon_design.md` — R10 add-on design
- TODO.md `### v6.0 — レポート品質向上リファクタリング`
