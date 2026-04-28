# Design memo: Query Rewrite extraction (Phase 0)

**Date:** 2026-04-27
**Status:** Draft (Phase 0 — boundary + eval definition before code split)
**Sources:** Codex review 2026-04-27
**Related:** [`alias-admission-rule.md`](alias-admission-rule.md),
[`routes/genie_chat.py`](../../dabs/app/routes/genie_chat.py) (current home),
[`templates/shared_result.html`](../../dabs/app/templates/shared_result.html) (current UI)

## Context

The Query Rewrite feature (v4.38) currently lives inside
`routes/genie_chat.py` as 3 routes plus ~7 helper functions, totaling
~370 lines (more than half the file). The UI sits inside the Genie
chat panel of `shared_result.html`. Investigation (2026-04-27) showed
the implementation **does not depend on Genie API, Genie Space, or
conversation state** — it shares only the Flask blueprint and the
chat-panel UI placement.

Codex review concluded the extraction is worthwhile but **not a
v6.6.4 merge blocker**. Plan for v6.6.5+ as a separate PR.

## Phase 0: define what rewrite is and how we measure it

Before splitting code, write down the contract. This memo is that
contract.

### Inputs

| Field | Source | Required | Notes |
|-------|--------|----------|-------|
| `analysis_id` | URL / API param | yes | Loads the existing `ProfileAnalysis` |
| `source_sql` | derived from `analysis.query_metrics.query_text` | yes | Raw user SQL |
| `model` | API param, env, or recommendation | yes | LLM model id |
| `feedback` | API param | optional | Triggers iterative refine mode |
| `previous_rewrite` | API param | optional (only with feedback) | Last LLM output |
| `lang` | locale | yes | "ja" or "en" |
| `is_serverless` | derived from `analysis.warehouse_info` | yes | Affects allowed hints |

### Outputs

| Field | Type | Notes |
|-------|------|-------|
| `rewrite` | string (Markdown SQL block) | The proposed SQL |
| `model_used` | string | Resolved model id |
| `validation_result` | dict (Phase 0 stub) | Currently only EXPLAIN/sqlglot |

### Contract: what counts as "good rewrite"

We currently track only **structural validity**:

- **EXPLAIN succeeds** (warehouse-side parse + plan)
- **sqlglot parses** (fallback when no warehouse access)

These confirm "the SQL runs," not "the SQL is better." That is the
gap Phase 0 names explicitly. Future work fills in:

| Quality axis | Definition | Phase to add |
|--------------|------------|--------------|
| Validity | EXPLAIN succeeds | already (Phase 0 baseline) |
| Syntactic correctness | sqlglot parses | already (Phase 0 baseline) |
| **Semantic preservation** | Returns same logical result set on representative inputs | Phase 3+ (needs sampling infra) |
| **Performance hint quality** | Hints are physically meaningful (BROADCAST size matches data, REPARTITION key has cardinality, …) | Phase 3+ (needs `EXPLAIN COST` or row count comparison) |
| **Human acceptability** | A human reviewer would accept this rewrite | Phase 0+ (lightweight thumbs-up tracking, see Phase 3 spec) |

Phase 0 contract: **the rewrite must pass EXPLAIN xor sqlglot**.
Anything beyond is opt-in evaluation.

## Phase 1: route split (next small PR after v6.6.4)

### Move target

```
routes/genie_chat.py
  ├─ Genie chat routes (10)         → keep
  ├─ Query Rewrite routes (3)       → move to routes/query_rewrite.py
  ├─ _rewrite_tasks dict            → move
  ├─ _run_rewrite_task              → move
  ├─ _validate_with_explain         → move
  ├─ _validate_with_sqlglot         → move
  └─ _load_analysis_for_rewrite     → move
```

### API compatibility

Keep paths exactly: `/api/v1/rewrite`, `/api/v1/rewrite/<task_id>`,
`/api/v1/rewrite/validate`. UI in `shared_result.html` requires no
change.

### Verification (Codex callouts)

Codex flagged "暗黙の共有" risks. Confirm before extraction:

- [ ] Decorator chain on `genie_chat.bp` — rewrite routes use any
      blueprint-level `@before_request` / auth?
- [ ] `_rewrite_tasks` task polling — TTL cleanup logic depends on
      module-level state; preserve its lifecycle
- [ ] Analysis context loader (`_load_analysis_for_rewrite`) — does
      it touch any Genie-only services?
- [ ] Logger naming — preserve `logger = logging.getLogger(__name__)`
      so log filters still match

## Phase 2: dedicated UX (`/rewrite/<analysis_id>`)

The chat bubble layout is the wrong shape for the actual UX needs:

| Need | Chat bubble | Dedicated page |
|------|-------------|----------------|
| Show source SQL | bubbled, hard to compare | side-by-side diff |
| Show validation | inline text | dedicated panel |
| Show alternatives | one-at-a-time | columns or tabs |
| Iterative refine | append messages | revision history |
| Export | manual copy | button |

Phase 2 keeps the chat panel button as a shortcut but moves the
actual experience to a dedicated route.

## Phase 3: persistence + comparison (later)

Suggested artifact (per Codex: "DB schema は急がない、概念設計だけ"):

```
RewriteArtifact:
  artifact_id     : UUID
  analysis_id     : ref ProfileAnalysis
  source_sql      : string (or hash)
  source_sql_hash : sha256 — for grouping reruns
  rewritten_sql   : string
  model           : string
  feedback        : string | null  (refine mode)
  parent_id       : UUID | null    (refine chain)
  validation      : { method: "explain" | "sqlglot", passed: bool, error: string | null }
  created_at      : timestamp
  user_feedback   : "accepted" | "rejected" | null  (Phase 3+ optional)
```

Stored append-only — every rewrite attempt persists. Multi-model
compare is then a query joining `RewriteArtifact` records sharing
`source_sql_hash` and `parent_id IS NULL`. Refine chain is a tree
walk on `parent_id`.

## Risks and mitigations

| Risk | Mitigation |
|------|-----------|
| Phase 1 breaks rewrite during merge | API contract tests + manual smoke before merging |
| Phase 2 UI doesn't match user mental model | Keep Phase 1 button as shortcut; observe usage before retiring chat-panel UX |
| Phase 3 schema premature | Do not implement persistence until at least one of `multi-model compare` or `rewrite history` is requested by an actual user |
| Eval framework drift | Add a 5-case rewrite goldenset to `eval/goldens/` even at Phase 0 — pin EXPLAIN-pass rate as the floor metric |

## Out of scope for this memo

- Rule-based rewriter (sqlglot-driven). Codex: keep as **supplementary
  validator**, not the primary engine.
- Genie space integration. Codex: Genie is a UX integration target,
  not a rewrite implementation home.
- External API exposure. Phase 3+ if at all.

## Decision log

- 2026-04-27: extraction plan adopted, Codex-suggested Phase 0
  inserted before Phase 1. Phase 1 implementation deferred to the
  first PR after v6.6.4 merge. Phase 2/3 design captured but not
  implemented.
