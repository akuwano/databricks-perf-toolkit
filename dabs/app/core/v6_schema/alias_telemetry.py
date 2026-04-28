"""Alias-hit telemetry for the V6 canonical Report normalizer.

The normalizer applies three alias maps (``_FIX_TYPE_ALIASES``,
``_CATEGORY_ALIASES_LLM``, ``_ISSUE_ID_ALIASES_LLM``) to coerce
near-canonical LLM output into the schema's enum values. Each alias
that fires is a salvaged case that would otherwise have failed
schema validation; tracking how often each map kicks in tells us:

  - whether the alias map is doing real work (high hit rate → map is
    earning its keep; low hit rate → candidate for removal once the
    prompt stabilises)
  - which prompt-side directives are leaking back into the wild (a
    spike in ``fix_type`` hits means the LLM is drifting from the
    enum despite the prompt directive)
  - whether the admission-rule (``docs/v6/alias-admission-rule.md``)
    is paying off in practice

Hit counts are deliberately decoupled from the canonical Report dict
so the schema validator stays clean — callers create a tracker, pass
it through ``enrich_llm_canonical``, and consume ``to_dict()``
themselves (eval ab_runner aggregates, app-side telemetry persists).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AliasHitCounts:
    """Per-canonical-Report tally of alias-map hits.

    Each integer counts how many *individual values* the corresponding
    alias map rewrote during normalization. A canonical Report with
    five findings whose ``issue_id``s all triggered the alias map will
    record ``issue_id=5`` (not ``issue_id=1``); the design choice keeps
    the eval-side ``alias_hit_rate`` interpretable as
    "salvaged-values per case" rather than "salvaged-cases per N".
    """

    fix_type: int = 0
    category: int = 0
    issue_id: int = 0

    @property
    def total(self) -> int:
        return self.fix_type + self.category + self.issue_id

    def record(self, kind: str) -> None:
        """Increment the counter for ``kind``. Unknown kinds are a no-op
        so adding a fourth alias map later doesn't crash old callers.
        """
        if kind == "fix_type":
            self.fix_type += 1
        elif kind == "category":
            self.category += 1
        elif kind == "issue_id":
            self.issue_id += 1
        # else: silently ignore — see docstring

    def to_dict(self) -> dict[str, int]:
        return {
            "fix_type": self.fix_type,
            "category": self.category,
            "issue_id": self.issue_id,
            "total": self.total,
        }


def aggregate(trackers: list[AliasHitCounts]) -> dict[str, int | float]:
    """Aggregate a list of per-case trackers into the eval summary.

    Returns:
      {
        "fix_type_total": int,
        "category_total": int,
        "issue_id_total": int,
        "hits_total": int,
        "cases_with_any_hit": int,
        "cases": int,
        "alias_hit_rate": float,  # cases_with_any_hit / cases
        "hits_per_case_avg": float,
      }

    ``alias_hit_rate`` is the metric the v6.6.5 ADR called out: how
    often the alias map saved a case. ``hits_per_case_avg`` is the
    side metric for "intensity" — high numbers signal heavy reliance
    on coercion.
    """
    if not trackers:
        return {
            "fix_type_total": 0,
            "category_total": 0,
            "issue_id_total": 0,
            "hits_total": 0,
            "cases_with_any_hit": 0,
            "cases": 0,
            "alias_hit_rate": 0.0,
            "hits_per_case_avg": 0.0,
        }
    fx = sum(t.fix_type for t in trackers)
    ct = sum(t.category for t in trackers)
    iid = sum(t.issue_id for t in trackers)
    total = fx + ct + iid
    cases_with_any = sum(1 for t in trackers if t.total > 0)
    n = len(trackers)
    return {
        "fix_type_total": fx,
        "category_total": ct,
        "issue_id_total": iid,
        "hits_total": total,
        "cases_with_any_hit": cases_with_any,
        "cases": n,
        "alias_hit_rate": round(cases_with_any / n, 4) if n else 0.0,
        "hits_per_case_avg": round(total / n, 4) if n else 0.0,
    }
