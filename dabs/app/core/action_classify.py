"""Classification of ActionCards into root-cause groups and coverage
categories, used by the Top-5 rerank logic in ``usecases.py``.

The taxonomy was designed with Codex in PR #75's design review:
- ``root_cause_group`` answers "what underlying issue does this
  recommendation address?" — rerank avoids filling Top 5 with multiple
  cards attacking the same root cause.
- ``coverage_category`` answers "what tuning facet does it cover?" —
  rerank tries to keep ≥4 distinct categories in Top 5 so the user sees
  a spread of improvements rather than five flavors of the same issue.

Both fields are plain strings (not Enum) to stay friendly with JSON
serialization into the Delta tables and the LLM prompts. The canonical
values are centralized here so the classifier and the rerank reference
the same vocabulary.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Root-cause groups (one card per group in Top 5, except preserved cards
# may override this when their underlying alert is critical).
# ---------------------------------------------------------------------------

ROOT_CAUSE_GROUPS: tuple[str, ...] = (
    "spill_memory_pressure",
    "data_skew",
    "shuffle_overhead",
    "photon_compatibility",
    "scan_efficiency",
    "join_strategy",
    "cache_utilization",
    "cluster_sizing",
    "statistics_freshness",
    "sql_pattern",
    "delta_write_overhead",
    # v5.16.25: driver-side overhead detectors (PR #93).
    # ``compilation_overhead`` covers Catalyst + Delta log replay +
    # file-level stats pruning (typical root cause: small-file
    # proliferation); ``driver_overhead`` covers queue waits and
    # scheduling/compute-wait gaps.
    "compilation_overhead",
    "driver_overhead",
    # v5.18.0: Lakehouse Federation detection. The "scan" for
    # federated queries is an external-engine call (BigQuery /
    # Snowflake / Postgres / …), not a Delta file read, so most
    # Databricks-side tunings (LC, disk cache, shuffle partitions)
    # do not apply. Cards in this group advise on pushdown,
    # fetchSize, and source-side pre-aggregation instead.
    "federation",
    # v5.19.0 (PR #95): cluster-capacity detectors.
    # ``cluster_underutilization`` covers three variants — external
    # contention, plan-complexity driver load, and serial/topologically
    # narrow plans — that all land on "warehouse is idle but the query
    # is slow". ``compilation_absolute`` is the INFO-level sibling of
    # ``compilation_overhead`` that surfaces absolute-heavy compile
    # times where the ratio is below the HIGH/MEDIUM threshold.
    "cluster_underutilization",
    "compilation_absolute",
)

# ---------------------------------------------------------------------------
# Coverage categories (Top 5 aims for ≥4 distinct categories).
# ---------------------------------------------------------------------------

COVERAGE_COMPUTE = "COMPUTE"
COVERAGE_DATA = "DATA"
COVERAGE_QUERY = "QUERY"
COVERAGE_MEMORY = "MEMORY"
COVERAGE_PARALLELISM = "PARALLELISM"

COVERAGE_CATEGORIES: tuple[str, ...] = (
    COVERAGE_COMPUTE,
    COVERAGE_DATA,
    COVERAGE_QUERY,
    COVERAGE_MEMORY,
    COVERAGE_PARALLELISM,
)

# ---------------------------------------------------------------------------
# Root-cause group → coverage category (canonical mapping)
# ---------------------------------------------------------------------------

GROUP_TO_CATEGORY: dict[str, str] = {
    "spill_memory_pressure": COVERAGE_MEMORY,
    "data_skew": COVERAGE_PARALLELISM,
    "shuffle_overhead": COVERAGE_PARALLELISM,
    "photon_compatibility": COVERAGE_COMPUTE,
    "scan_efficiency": COVERAGE_DATA,
    "join_strategy": COVERAGE_QUERY,
    "cache_utilization": COVERAGE_COMPUTE,
    "cluster_sizing": COVERAGE_COMPUTE,
    "statistics_freshness": COVERAGE_DATA,
    "sql_pattern": COVERAGE_QUERY,
    "delta_write_overhead": COVERAGE_DATA,
    # Driver-side overhead: compilation is a compute/driver cost, and
    # queue/scheduling waits are infra (closest to COMPUTE sizing).
    "compilation_overhead": COVERAGE_COMPUTE,
    "driver_overhead": COVERAGE_COMPUTE,
    # Lakehouse Federation: remote-engine cost and network transfer —
    # closest to DATA (where the data lives drives the cost).
    "federation": COVERAGE_DATA,
    # Cluster-capacity detectors (v5.19.0, PR #95). Both are "the
    # warehouse could be doing more work for this query" stories —
    # closest to the COMPUTE facet used by cluster_sizing.
    "cluster_underutilization": COVERAGE_COMPUTE,
    "compilation_absolute": COVERAGE_COMPUTE,
}


# ---------------------------------------------------------------------------
# Equivalence mapping for LLM vs rule-based dedup (v5.16.19, Phase 2a).
#
# Some root-cause groups reach the same underlying fix from different
# framings — e.g. ``data_skew`` and ``shuffle_overhead`` both land on
# "reduce shuffle volume / rebalance partitions". When the rule-based
# registry already emitted a card in one of those groups and the LLM
# proposes a card in an equivalent group, we drop the LLM duplicate
# rather than show two near-identical entries next to each other.
#
# Semantics: ``GROUP_OVERLAPS[a]`` is the set of groups that should be
# considered equivalent to ``a`` for dedup purposes. The relation is
# declared as symmetric below so callers can compare in either
# direction without juggling keys. Entries are intentionally
# conservative — only merges where the fix text overlap is near-total.
# ---------------------------------------------------------------------------

_GROUP_OVERLAPS_RAW: dict[str, tuple[str, ...]] = {
    # Skew mitigations and shuffle-volume mitigations collapse to the
    # same "reshape the shuffle" story in practice. A BROADCAST-hint
    # recommendation classifies as ``join_strategy`` but addresses the
    # same bottleneck as a ``shuffle_overhead`` card — so both pair
    # with shuffle.
    "data_skew": ("shuffle_overhead",),
    "shuffle_overhead": ("data_skew", "join_strategy"),
    "join_strategy": ("shuffle_overhead",),
    # Scan efficiency cards include Liquid Clustering advice; so does
    # the delta-write-overhead family (eager clustering disable, etc.).
    # Stats-freshness alerts also land at "rebuild the scan" in
    # practice so we treat them as scan-equivalent for dedup purposes.
    "scan_efficiency": ("delta_write_overhead", "statistics_freshness"),
    "delta_write_overhead": ("scan_efficiency",),
    # Hash-resize spill is attributed to memory_pressure; the
    # stats-fresh investigative card is classified as
    # statistics_freshness even though both target the same alert.
    # Memory-pressure spill can also be addressed by sizing up the
    # cluster, so ``cluster_sizing`` recommendations would be
    # redundant when a spill card already covers it.
    "spill_memory_pressure": ("statistics_freshness", "cluster_sizing"),
    "cluster_sizing": ("spill_memory_pressure",),
    "statistics_freshness": ("spill_memory_pressure", "scan_efficiency"),
}


def groups_overlap(group_a: str, group_b: str) -> bool:
    """Return True when two root_cause_groups are equivalent for dedup.

    Same-name always overlaps. Mapped neighbors per
    ``_GROUP_OVERLAPS_RAW`` also overlap. Empty / unknown groups never
    overlap — callers treat that as "cannot prove equivalence, keep
    both" (fail-open).
    """
    if not group_a or not group_b:
        return False
    if group_a == group_b:
        return True
    return group_b in _GROUP_OVERLAPS_RAW.get(group_a, ())


# ---------------------------------------------------------------------------
# Keyword-based classifier for dynamic / LLM-generated cards whose
# ``problem`` / ``fix`` text is not pre-tagged. Ordering matters: the
# first matching group wins, so put more specific keywords ahead of
# broader ones (e.g. check ``hash resize`` before the bare ``shuffle``).
# ---------------------------------------------------------------------------

_CLASSIFIER_RULES: list[tuple[str, tuple[str, ...]]] = [
    # Photon comes first because "UDF" inside a Photon-fallback message is
    # about compatibility, not SQL pattern.
    (
        "photon_compatibility",
        (
            "photon-unsupported",
            "photon unsupported",
            "photon fallback",
            "non-photon",
            "udf ",
            "replace udf",
            "built-in function",
        ),
    ),
    (
        "spill_memory_pressure",
        (
            "spill",
            "oom",
            "out of memory",
            "hash resize",
            "hash table resize",
            "ハッシュリサイズ",
            "peak memory",
            "memory pressure",
        ),
    ),
    (
        "data_skew",
        (
            "data skew",
            "skew",
            "hot key",
            "hot join",
            "hot grouping",
            "imbalanced",
            "rescheduled scan",
            "スキュー",
        ),
    ),
    (
        "join_strategy",
        (
            "broadcast",
            "shuffle hash join",
            "sort merge join",
            "shuffle_hash",
            "/*+ broadcast",
            "join hint",
            "join strategy",
        ),
    ),
    (
        "delta_write_overhead",
        (
            "eager clustering",
            "clusteronwrite",
            "forcedisableeagerclustering",
            "delta.liquid",
            "merge into",
        ),
    ),
    (
        "scan_efficiency",
        (
            "file pruning",
            "data skipping",
            "predictive i/o",
            "predictive io",
            "cluster by",
            "hierarchical clustering",
            "optimize table",
            "optimize ",
            "partition by",
            "layout",
            "file layout",
        ),
    ),
    (
        "shuffle_overhead",
        (
            "shuffle.partitions",
            "repartition",
            "coalesce",
            "aqe",
            "shuffle operations are dominant",
            "large shuffle",
            "shuffle",
            "scan operation accounts for",  # still dominated by scan-side; falls back below
        ),
    ),
    (
        "statistics_freshness",
        (
            "analyze table",
            "statistics are up-to-date",
            "row count stats",
            "テーブル統計",
        ),
    ),
    (
        "sql_pattern",
        (
            "cte ",
            'cte "',
            "subquery",
            "window rows",
            "window range",
            "implicit cast",
            "select *",
            "pre-aggregate",
            "qualify",
            "union all",
        ),
    ),
    (
        "cluster_sizing",
        (
            "bigger warehouse",
            "increase cluster size",
            "scale up",
            "autoscale",
            "warehouse size",
        ),
    ),
    (
        "cache_utilization",
        (
            "cache hit ratio",
            "disk cache",
            "result cache",
            "cache",
        ),
    ),
    # v5.16.25: driver-side overhead. Keep these last so keywords that
    # also appear in SQL remediation text (e.g. ``vacuum``, ``optimize``)
    # are first handled by scan_efficiency / statistics_freshness rules.
    (
        "compilation_overhead",
        (
            "compilation time",
            "compile time",
            "file pruning dominates",
            "metadata resolution",
            "delta log",
            "small file",
            "un-vacuumed",
        ),
    ),
    (
        "driver_overhead",
        (
            "driver overhead",
            "queue wait",
            "queued in",
            "queued provisioning",
            "scheduling delay",
            "waiting for compute",
            "warm pool",
            "auto-stop",
        ),
    ),
    # v5.18.0: Lakehouse Federation. Keep after all Delta-oriented
    # groups so remediation text that mentions ``pushdown`` /
    # ``fetchSize`` only falls into federation when a more specific
    # rule hasn't already claimed the card.
    (
        "federation",
        (
            "lakehouse federation",
            "federated query",
            "federation query",
            "row data source scan",
            "external engine query",
            "pushedfilters",
            "pushedjoins",
            "fetchsize",
            "partition_size_in_mb",
            "materialization mode",
            "jdbc connector",
            "remote engine",
            "bigquery storage api",
        ),
    ),
    # v5.19.0 (PR #95): cluster-capacity detectors. Ordering matters —
    # "effective parallelism" phrases appear in other recommendation
    # bodies (e.g. shuffle_overhead remediation mentions REPARTITION),
    # so the more specific tokens for the cluster-underutilization
    # card are placed first.
    (
        "cluster_underutilization",
        (
            "cluster underutilization",
            "underutilized warehouse",
            "effective parallelism",
            "serial plan",
            "external contention",
            "idle warehouse",
            "plan complexity",
            "aqe re-plan",
            "aqe replan",
        ),
    ),
    (
        "compilation_absolute",
        (
            "absolute heavy compile",
            "absolute-heavy compile",
            "absolute heavy compilation",
            "long compile time",
            "absolute compile time",
        ),
    ),
]


def classify_root_cause_group(text: str) -> str:
    """Return the best-matching root_cause_group for free-form card text.

    ``text`` should typically be ``problem + " " + fix + " " + fix_sql``.
    Returns ``""`` when no rule matches — the rerank treats that as
    neutral (no diversity bonus or penalty).
    """
    if not text:
        return ""
    lower = text.lower()
    for group, keywords in _CLASSIFIER_RULES:
        for kw in keywords:
            if kw in lower:
                return group
    return ""


def category_for_group(group: str) -> str:
    """Return the canonical coverage_category for a root_cause_group."""
    return GROUP_TO_CATEGORY.get(group, "")
