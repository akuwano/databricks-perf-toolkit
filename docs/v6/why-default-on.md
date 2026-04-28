# ADR: V6 feature flags become default-on kill switches

**Date:** 2026-04-26  
**Status:** Accepted  
**Sources:** Codex review (Session 4) + dev/staging adoption since v6.0.0  
**Related:** [`feature_flags.py`](../../dabs/app/core/feature_flags.py),
[`five-layer-feedback.md`](five-layer-feedback.md), [`v6-spec.md`](../v6-spec.md)

## Context

Eight V6 feature flags were introduced during the Week 3+ V6 quality
refactor as **experiment toggles** — defaults off, dev opt-in, with
the v5.19 path retained as a back-compat reference.

By v6.6.3 the situation had changed:

- All flags ran ON in dev for several months without regression.
- The V6 quality framework (canonical schema, scorers, eval invariants,
  L5 feedback bundle, normative sizing recommendation) is **structurally
  V6-shaped** — disabling V6_CANONICAL_SCHEMA leaves canonical-Report
  consumers without their input source.
- Operators in staging/prod kept getting v5.19 behavior because they
  forgot (or didn't realize) they had to enable each flag individually.
- The realistic rollback unit is **per-flag** ("V6_REVIEW_NO_KNOWLEDGE
  is producing odd reviews this week — disable it") not full V6.

The "experiment toggle" semantics no longer matched the situation.

## Decision

The V6 feature flags become **default-on kill switches**:

1. `_is_enabled(flag)` returns `True` when neither env nor
   runtime-config sets a value (was `False`).
2. The flags exist so a single V6 behavior can be *disabled* in
   production without redeploy. They are **not** experimental opt-ins.
3. Three supported off-patterns:
   - **default-on** — normal operation (no env / no runtime override)
   - **single-flag off** — one flag flipped for triage
   - **legacy full-off** — every `V6_*` set to a falsy value (V5 mode)

   Other partial combinations are **not exercised by tests** and
   should not be relied upon.
4. Per-flag retain vs retire criteria (Codex):
   - Retain — flags whose disable path has real triage value
     (data flow, output structure, grounding policy).
   - Retire — flags whose disable path is a UI-rendering preference
     and could be replicated by hand if needed.

## Flag inventory (post-decision)

| Flag | Status | Rationale |
|------|--------|-----------|
| `V6_CANONICAL_SCHEMA` | **retain** | Output structure root. Disabling reverts the LLM to legacy ActionCard JSON, which feeds the normalizer adapter. High triage value. |
| `V6_REVIEW_NO_KNOWLEDGE` | **retain** | Stage-2 review behavior. False-reject regressions could justify a quick disable. |
| `V6_REFINE_MICRO_KNOWLEDGE` | **retain** | Stage-3 grounding policy. Disabling reverts to full knowledge injection. |
| `V6_ALWAYS_INCLUDE_MINIMUM` | **retain** | Knowledge-injection policy (1 vs 3 sections). Token-cost regressions are easy to spot, easy to triage. |
| `V6_SKIP_CONDENSED_KNOWLEDGE` | **retain** | LLM-call-count behavior. Low-volume but explicit cost lever. |
| `V6_RECOMMENDATION_NO_FORCE_FILL` | **retain** | Grounding strictness directive. Quality-sensitive. |
| `V6_SQL_SKELETON_EXTENDED` | **retain (provisional)** | MERGE/CREATE VIEW/INSERT extraction. Specific SQL types could regress; flag stays at least one more release. |
| `V6_COMPACT_TOP_ALERTS` | **retired in v6.6.4** | UI-representation only. Disabling restores a section header + numbered references. The compact form is preferable in 100% of observed cases; no triage justification for the kill switch. |

Future retirement criteria:

- **2 stable releases** with no observed regression in dev + staging
  with the flag's behavior on.
- **No customer-reported issue** specifically requesting the legacy path.
- **Eval baselines** for the V6 path retained as `historical` reference.

## Consequences

### Required follow-ups (done in the same series)

- `feature_flags.py` docstring rewritten ("default-on V6 kill
  switches, not experimental opt-ins").
- `test_feature_flags.py` rewritten to assert default-on contract +
  3-pattern off support; legacy-behavior tests gated by explicit
  `monkeypatch.setenv("V6_*", "0")`.
- `local-overrides.yml.sample` flipped from "uncomment to opt in" to
  "uncomment to opt out for triage".
- `databricks.yml` comment block updated; empty-string variable
  declarations retained because `generate_runtime_config.py:69`
  strips empties before runtime-config write (default-on takes
  effect cleanly).

### Eval baseline semantics

`unset` no longer resolves to V5 behavior. Eval comparison runs that
need V5 baselines must explicitly set every `V6_*` to a falsy value
(the **legacy full-off** pattern). The existing
`eval/baselines/v6_w4_*.json` are renamed in spirit to "historical
reference"; new baselines should be captured under the **default-on**
configuration.

### Risks

- **Flag-combination matrix** — 7 retained flags = 128 combinations.
  Tests cover only the 3 supported patterns. Mitigation: any
  triage that disables flags must do so one at a time; cross-flag
  interaction debugging is not a supported workflow.
- **Documentation drift** — old runbooks may still reference the
  "set V6_X to true to enable" workflow. Mitigation: this ADR is the
  canonical reference; old docs link here.

## Rollback

If this decision proves wrong:

1. Revert the `_is_enabled` default to `False`.
2. Revert the docstring + tests to their default-off form.
3. Restore `dev` / `staging` `local-overrides.yml` flag entries.

The actual code paths the flags gate were not deleted in this
decision; they are retained as the legacy fallback. So the rollback
is purely a configuration change, not a re-implementation.

## References

- Codex review session (2026-04-26, this branch): "**Recommended
  selection: A** (8 flags default-on, env can disable individually);
  later, retire flags that have no kill-switch value."
- Implementation commits: `14b4542` (defaults flip), `6da4e8d`
  (deploy-side cleanup), `b574bae` (V6_COMPACT_TOP_ALERTS retired).
