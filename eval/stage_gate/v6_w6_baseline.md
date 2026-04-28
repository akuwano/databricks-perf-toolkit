# v6_w6_baseline — V6 Stage Gate

Compared to baseline: v6_w5_baseline
Generated: 2026-04-25T12:52:33Z

## Verdict: 🟡 **HOLD**

- Stage 1 (regression vs baseline): **pass**
- Stage 2 (V6 absolute acceptance): **fail**

## Measured (Stage 2 inputs)

| metric | measured | target |
|--------|---------:|-------:|
| schema_pass | 100.00% | 100.00% |
| q3_composite | 64.22% | 80.00% |
| actionability_specific | 94.52% | 80.00% |
| failure_taxonomy | 17.58% | 70.00% |
| recall_strict | 30.65% | 50.00% |
| hallucination_clean | 64.23% | 85.00% |
| ungrounded_numeric_avg | 22.24% | ≤ 15.00% |
| parse_success_rate | 100.00% | 90.00% |
| canonical_parse_failure | 100.00% | ≤ 5.00% |
| case_regressions | 0 | ≤ 1 |

## Stage 2 violations (V6 acceptance)

- q3_composite: current=64.22%, target >= 80.00%
- failure_taxonomy: current=17.58%, target >= 70.00%
- recall_strict: current=30.65%, target >= 50.00%
- hallucination_clean: current=64.23%, target >= 85.00%
- ungrounded_numeric: current=22.24%, target <= 15.00%
- canonical_parse_failure: current=100.00%, target <= 5.00%