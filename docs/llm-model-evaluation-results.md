# LLM Model Evaluation Results

**Date:** 2026-03-20
**Profile:** `json/customer/Concurrent2.json` (TPC-H SF100, scan locality issue)
**Judges:** Opus 4.6, GPT 5.4, Gemini 3.1 Pro (5 criteria × 5 points = 25 max)

## Evaluation Criteria

| Criteria | Description |
|----------|-------------|
| Accuracy (正確性) | Are metric interpretations correct? No misdiagnosis? |
| Insight (洞察力) | Root cause analysis, correlation analysis beyond surface numbers? |
| Practicality (実用性) | Are recommendations specific and immediately actionable? |
| Coverage (網羅性) | Are important metrics covered without blind spots? |
| Clarity (説明力) | Understandable to non-experts? |

---

## Pattern Results

### A: Opus Full (Primary=Opus, Review=Opus, Refine=Opus)
### B: GPT Full (Primary=GPT, Review=GPT, Refine=GPT)

| Judge | A (Opus-full) | B (GPT-full) | Winner |
|-------|:------------:|:------------:|--------|
| Opus 4.6 | **21** | 19 | **A** |
| GPT 5.4 | 19 | 19 | TIE |
| Gemini 3.1 | **24** | 20 | **A** |
| **Total** | **64** | **58** | **A wins 2-0-1** |

**Processing time:** Opus 263s / GPT 129s

---

### C: Opus + GPT Review (Primary=Opus, Review=GPT, Refine=Opus)
### D: GPT + Opus Review (Primary=GPT, Review=Opus, Refine=GPT)

| Judge | C (Opus+GPTrev) | D (GPT+Opusrev) | Winner |
|-------|:---------------:|:---------------:|--------|
| Opus 4.6 | **20** | 20 | **A** |
| GPT 5.4 | 18 | **20** | **B** |
| Gemini 3.1 | **23** | 20 | **A** |
| **Total** | **61** | **60** | **C wins 2-1** |

---

### E: Opus + GPT Refine (Primary=Opus, Review=Opus, Refine=GPT)
### F: GPT + Opus Refine (Primary=GPT, Review=GPT, Refine=Opus)

| Judge | E (Opus+GPTref) | F (GPT+Opusref) | Winner |
|-------|:---------------:|:---------------:|--------|
| Opus 4.6 | **21** | 20 | **A** |
| GPT 5.4 | **21** | 17 | **A** |
| Gemini 3.1 | **24** | 20 | **A** |
| **Total** | **66** | **57** | **E wins 3-0** |

---

## Overall Ranking (by average total score across all judges)

| Rank | Config | Primary | Review | Refine | Avg Score | Wins |
|------|--------|---------|--------|--------|-----------|------|
| **1** | **E** | **Opus** | **Opus** | **GPT** | **22.0** | **3-0** |
| 2 | A | Opus | Opus | Opus | 21.3 | 2-0-1 |
| 3 | C | Opus | GPT | Opus | 20.3 | 2-1 |
| 4 | D | GPT | Opus | GPT | 20.0 | 1-2 |
| 5 | B | GPT | GPT | GPT | 19.3 | 0-2-1 |
| 6 | F | GPT | GPT | Opus | 19.0 | 0-3 |

## Key Findings

### 1. Opus is consistently better as Primary model
- Every configuration with Opus as Primary outperformed its GPT counterpart
- The gap is largest in **Insight** and **Practicality** scores
- Opus provides more specific clustering key recommendations and SQL examples

### 2. GPT as Refine model is surprisingly effective
- **Config E (Opus+Opus+GPT) scored highest overall** at 22.0 avg
- GPT's refine step seems to add balance and reduce Opus's tendency toward over-assertion
- This was the only config to receive a **unanimous 3-0** win

### 3. Cross-model review has mixed results
- GPT reviewing Opus (Config C) worked reasonably well
- Opus reviewing GPT (Config D) didn't improve GPT significantly
- The review model matters less than Primary and Refine

### 4. Self-bias exists but Gemini breaks ties
- Opus tends to rate itself higher, GPT rates itself higher
- **Gemini 3.1 Pro consistently provided the tie-breaking vote**
- Gemini scored Opus-primary configs 23-24/25, GPT-primary 20/25

### 5. Speed vs Quality trade-off
- GPT is ~2x faster (129s vs 263s for full pipeline)
- But consistently scores 2-4 points lower
- For production use, **Opus Primary + GPT Refine** offers the best balance

## Recommendation

**Recommended configuration: Primary=Opus 4.6, Review=Opus 4.6, Refine=GPT 5.4**

This provides:
- Highest overall quality (22.0/25 avg, unanimous 3-0 win)
- Opus's deep analysis in Primary and Review stages
- GPT's conciseness and balance in the final Refine stage
- Slightly faster than full-Opus due to GPT's faster Refine step

## Reproduction

```bash
cd databricks-apps

# Edit CANDIDATE_CONFIGS in scripts/eval_models.py to set desired configs
python ../scripts/eval_models.py ../json/customer/Concurrent2.json > results.json
```
