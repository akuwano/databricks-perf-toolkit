# ADR: V6 LLM-direct canonical alias admission rule

**Date:** 2026-04-27
**Status:** Accepted
**Sources:** Codex review (2026-04-27, two consultations during V5 vs V6 bench session)
**Related:** [`why-default-on.md`](why-default-on.md), [`canonical_schema_inventory.md`](canonical_schema_inventory.md), [`v6_schema/normalizer.py`](../../dabs/app/core/v6_schema/normalizer.py)

## Context

The V6 LLM-direct path (`V6_CANONICAL_SCHEMA=on`) asks the LLM to emit a
canonical Report JSON block. The LLM occasionally produces near-canonical
but schema-violating values (e.g., `fix_type: "query_rewrite"` instead of
`"rewrite"`, `category: "cluster"` instead of `"clustering"`,
`source: "I/O Metrics"` instead of `"profile.queryMetrics"`). The
`enrich_llm_canonical` helper carries small alias maps that translate
these variants into canonical form before downstream scoring.

Through one V5 vs V6 bench session (2026-04-26 → 27), the alias maps
grew to ~34 entries across 3 categories (`fix_type`, `category`,
`issue_id`) plus targeted reshaping for `verification` entries. Each
new alias was added in response to a specific smoke run failure. The
maintainer (the user) flagged this as a possible whack-a-mole pattern
and asked whether the architecture is durable.

Codex review concluded: this is **adapter complexity**, not a
whack-a-mole — provided the alias growth is **rule-bounded** and the
post-process is split out from the V5 normalizer adapter.

## Decision

Adopt a 3-criterion **admission rule** for new alias entries. New
maps stay frozen unless all three hold:

1. **Recurrence** — observed in 2+ smoke runs (not 1-case).
2. **Prompt-resistant** — directive strengthening doesn't suppress it.
3. **Unambiguous mapping** — exactly one canonical target, no semantic
   loss. Coercive fixes that change meaning (e.g., dropping fields)
   are rejected.

Track admission decisions in commits or a small `aliases_changelog.md`
when the next V6 series starts.

## Refactor plan (v6.6.5+)

The current `core/v6_schema/normalizer.py` mixes four responsibilities:

1. Operational metadata repair (UUID / timestamp / authoritative ids)
2. Context rebuild (profile-derived `is_serverless`, `is_streaming`, …)
3. Enum canonicalization (`issue_id`, `category`, `fix_type`)
4. Verification reshape (`type=sql` with `metric` field, …)

Codex flagged the issue as "件数より、normalizer が schema admission +
recovery + semantic repair を全部抱えている" — refactor target:

| Concern | New module |
|---------|-----------|
| operational metadata | `core/v6_schema/metadata_repair.py` |
| context rebuild | `core/v6_schema/context_rebuild.py` |
| enum canonicalization | `core/v6_schema/enum_canonicalize.py` |
| verification reshape | `core/v6_schema/verification_reshape.py` |
| alias definitions | `core/v6_schema/aliases.py` (next to `issue_registry.py`) |

The single source of truth becomes the `aliases.py` module. The
prompt's allowlist and the post-process maps both read from it, so a
new alias gets one PR touching exactly one file (plus its test).

Effort estimate: ~1-2 days. Not a v6.6.4 blocker — the current code
ships fine, the refactor is hygiene + future contributor experience.

## Telemetry to add (v6.6.5)

Codex flagged that "fallback case 限定 composite" is the real
customer-impact metric, not the raw fallback rate:

- [ ] `llm_direct_rate` — % of cases where LLM-direct emission worked
- [ ] `schema_valid_rate` — % of canonical Reports passing schema
- [ ] `fallback_case_composite_avg` — composite restricted to V5
      normalizer fallback cases (catches "we lose quality when we fall
      back" scenario)
- [ ] `alias_hit_rate` — per-alias counter, surfaces drift toward
      retirement
- [ ] `unknown_*` / invalid enum incidence — raw signal for adding a
      new alias

These plug into the existing eval baseline JSON and should appear on
the A/B summary markdown.

## Predicted drift (Codex)

- `fix_type` / `category`: saturating, ~17 / ~5 entries probably the
  ceiling.
- `issue_id`: model-version-sensitive, expect mild growth across
  Claude/GPT/Databricks FM differences.
- `source` / `verification`: still volatile, prompt-side directives
  are the primary control rather than alias maps.

> Not explosion, **局所的じわ増え** (Codex 2026-04-27).

## Consequences

### Required follow-ups (next V6 series)

- [ ] Move alias defs out of `normalizer.py` into the dedicated module
- [ ] Split `enrich_llm_canonical` into the four modules above
- [ ] Add the 5 telemetry counters
- [ ] Backfill the `aliases_changelog.md` from the v6.6.x commit
      history so the recurrence criterion has data to check against.

### Risks

- **Strictness too high** → contributors silently work around the
  admission rule with broader catch-all transforms. Mitigation: the
  rule's three criteria are explicit, and the changelog catches
  outliers in code review.
- **Strictness too low** → bloat returns. Mitigation: when a `category`
  or `fix_type` map crosses 25 entries, treat that as a signal to
  review whether the prompt allowlist is the problem.

### Rollback

If the rule proves too restrictive, relax criterion 1 (Recurrence) to
allow 1-case admission for V5 normalizer-only failures (these are
outside the V6 scope and arguably belong in a separate compat layer).

## References

- Codex consultation 2026-04-27 (alias bloat review): "これは破綻する
  whack-a-mole ではなく、境界を明確にすれば安定する adapter complexity"
- Codex consultation 2026-04-27 (n=32 V6 preview):
  "merge ゴーサインは出します。ただし条件はひとつ、alias map の原則は
  崩さない"
- Bench results: `eval/baselines/v5v6_full__baseline.json` (n=32,
  V6 LLM-direct 90.6%, composite 0.822, finding_support 0.956)
- Implementation commits: `5be869f`, `1b0f0a2`, `b84983f`, `5ac50fa`,
  `df9f710`, `420f3da` (alias map evolution)
