"""Cross-reference between Top Alerts and ActionCards.

The report shows Top Alerts (severity-ranked) and Top-5 recommended
actions side by side. Without explicit linking the reader cannot tell
which action solves which alert. This module:

1. ``sort_alerts_by_severity`` — stable sort so CRITICAL surfaces first.
2. ``match_card_to_alert_numbers`` — returns 1-indexed alert positions
   that an ActionCard addresses, using root-cause group first and
   keyword fallback second.
3. ``alert_severity_rank_for_card`` — helper used by the renderer to
   re-order Top-5 so actions addressing CRITICAL alerts come first.

The matcher is intentionally conservative: an empty return means "no
alert link" and the renderer will show `(全般)` rather than
inventing a spurious reference.
"""

from __future__ import annotations

from ..constants import Severity
from ..models import ActionCard, Alert

# ---------------------------------------------------------------------------
# Severity ordering (lower rank surfaces first)
# ---------------------------------------------------------------------------

_SEVERITY_RANK: dict[Severity, int] = {
    Severity.CRITICAL: 0,
    Severity.HIGH: 1,
    Severity.MEDIUM: 2,
    Severity.LOW: 3,
    Severity.INFO: 4,
    Severity.OK: 5,
}


def sort_alerts_by_severity(alerts: list[Alert]) -> list[Alert]:
    """Stable sort alerts by severity (CRITICAL → HIGH → MEDIUM → INFO).

    Same-severity alerts preserve their input order so that detection
    sequencing within a severity band stays predictable.
    """
    return sorted(alerts, key=lambda a: _SEVERITY_RANK.get(a.severity, 99))


# ---------------------------------------------------------------------------
# Alert → root_cause_group mapping
# ---------------------------------------------------------------------------
# `alert.category` is a coarse bucket (see analyzers/_helpers.py usages).
# Most map cleanly to exactly one root_cause_group. `io` is ambiguous —
# `metric_name` disambiguates between remote-read (cache) and pruning
# (scan).

_CATEGORY_TO_GROUP: dict[str, str] = {
    "spill": "spill_memory_pressure",
    "memory": "spill_memory_pressure",
    "shuffle": "shuffle_overhead",
    "skew": "data_skew",
    "photon": "photon_compatibility",
    "cache": "cache_utilization",
    "result_cache": "cache_utilization",
    "join": "join_strategy",
    "statistics": "statistics_freshness",
    "cluster": "cluster_sizing",
    "auto_scaling": "cluster_sizing",
    "upsizing": "cluster_sizing",
    "downsizing": "cluster_sizing",
    "cloud_storage": "scan_efficiency",
    "query_pattern": "sql_pattern",
    "aggregation": "sql_pattern",
    "agg": "sql_pattern",
    "compilation": "sql_pattern",
    "execution_plan": "sql_pattern",
}

# For category="io": look at metric_name to pick the right group.
_IO_METRIC_TO_GROUP: dict[str, str] = {
    "remote_read_ratio": "cache_utilization",
    "filter_rate": "scan_efficiency",
    "bytes_pruning_ratio": "scan_efficiency",
    "pruning_ratio": "scan_efficiency",
}


def _alert_to_groups(alert: Alert) -> set[str]:
    """Best-guess root_cause_groups that this alert corresponds to.

    Returns a set because some alerts legitimately span two groups
    (e.g. spill cards also cover memory pressure).
    """
    if alert.category == "io":
        g = _IO_METRIC_TO_GROUP.get(alert.metric_name)
        if g:
            return {g}
        # Fallback: treat generic io alerts as scan efficiency.
        return {"scan_efficiency"}
    g = _CATEGORY_TO_GROUP.get(alert.category)
    return {g} if g else set()


# ---------------------------------------------------------------------------
# Keyword fallback — when an ActionCard has no `root_cause_group`
# (LLM-generated cards that bypassed classification) we scan the
# problem + fix text for keywords that identify the alert.
# ---------------------------------------------------------------------------

_KEYWORD_TO_GROUPS: tuple[tuple[tuple[str, ...], str], ...] = (
    # More specific keywords first.
    (("disk spill", "ディスクスピル", "spill"), "spill_memory_pressure"),
    (
        (
            "REPARTITION",
            "shuffle パーティション",
            "shuffleパーティション",
            "Shuffleパーティション",
            "shuffle partition",
            "shuffle",
        ),
        "shuffle_overhead",
    ),
    (("data skew", "データスキュー", "skew"), "data_skew"),
    (
        (
            "Liquid Clustering",
            "クラスタリング",
            "pruning",
            "プルーニング",
            "WHERE",
            "filter",
            "フィルタ",
        ),
        "scan_efficiency",
    ),
    (("Photon", "photon"), "photon_compatibility"),
    (("cache", "キャッシュ", "remote read", "リモート読み取り", "disk cache"), "cache_utilization"),
    (("broadcast", "BROADCAST", "join", "JOIN"), "join_strategy"),
    (("ANALYZE TABLE", "統計情報", "statistics"), "statistics_freshness"),
)


def _card_groups(card: ActionCard) -> set[str]:
    """Groups the card claims to address.

    Priority: explicit `root_cause_group` (assigned by the classifier or
    the LLM) > keyword match on problem + fix text.
    """
    if card.root_cause_group:
        return {card.root_cause_group}
    text = f"{card.problem} {card.fix}".lower()
    groups: set[str] = set()
    for keywords, group in _KEYWORD_TO_GROUPS:
        if any(kw.lower() in text for kw in keywords):
            groups.add(group)
    return groups


# ---------------------------------------------------------------------------
# Special cross-group links — actions commonly address secondary alerts
# even when their primary root cause is different. Conservatively we
# add spill ← scan_efficiency (WHERE filter cuts both bytes scanned and
# downstream data volume that spills).
# ---------------------------------------------------------------------------

_SECONDARY_LINKS: dict[str, set[str]] = {
    # A WHERE-filter action (scan_efficiency) almost always cuts
    # downstream spill and shuffle volume too.
    "scan_efficiency": set(),  # keep conservative unless card text confirms
}


def match_card_to_alert_numbers(card: ActionCard, alerts: list[Alert]) -> list[int]:
    """Return 1-indexed positions of alerts addressed by ``card``.

    ``alerts`` must already be in the order the report renders (i.e.
    sorted by :func:`sort_alerts_by_severity` before calling this).
    Empty list means "no specific alert link" — caller renders as
    ``(全般)`` so the reader knows the action is general advice.
    """
    card_grps = _card_groups(card)
    if not card_grps:
        return []

    # Text-based secondary hints: a WHERE-filter action also addresses
    # spill/shuffle volume alerts when they exist.
    text = f"{card.problem} {card.fix}".lower()
    has_where_filter = any(k in text for k in ("where", "フィルタ", "filter", "期間"))

    matched: list[int] = []
    for idx, alert in enumerate(alerts, start=1):
        alert_grps = _alert_to_groups(alert)
        if card_grps & alert_grps:
            matched.append(idx)
            continue
        # Cross-group: WHERE-filter card matches spill volume alert too.
        if (
            has_where_filter
            and "scan_efficiency" in card_grps
            and "spill_memory_pressure" in alert_grps
        ):
            matched.append(idx)
    return matched


# ---------------------------------------------------------------------------
# Reordering helper for Top-5 rendering
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Issue-tag matcher (v6.6.0): replaces "アラート #N" position references
# with stable issue tags like "shuffle / spill / data_skew" so the
# standalone Top Alerts section can disappear without breaking the
# Recommended Actions cross-reference.
# ---------------------------------------------------------------------------

# Stable, human-readable tag for each root_cause_group. Order does not
# matter — set membership is what we use.
_GROUP_TO_TAG: dict[str, str] = {
    "spill_memory_pressure": "spill",
    "shuffle_overhead": "shuffle",
    "data_skew": "data_skew",
    "photon_compatibility": "photon",
    "cache_utilization": "cache",
    "scan_efficiency": "scan",
    "join_strategy": "join",
    "statistics_freshness": "stats",
    "cluster_sizing": "cluster",
    "sql_pattern": "sql_pattern",
}


def match_card_to_issue_tags(card: ActionCard, alerts: list[Alert]) -> list[str]:
    """Return the set of issue tags an ActionCard addresses.

    Tags come from ``_GROUP_TO_TAG`` so a card whose root_cause_group is
    ``shuffle_overhead`` and which also matches the WHERE-filter
    secondary rule for ``spill`` returns ``["shuffle", "spill"]``.
    The result is ordered to match the rendered alert order so the
    summary tag list reads naturally.

    Empty list = "no specific alert link" — caller renders ``(全般)``.
    """
    card_grps = _card_groups(card)
    if not card_grps:
        return []

    text = f"{card.problem} {card.fix}".lower()
    has_where_filter = any(k in text for k in ("where", "フィルタ", "filter", "期間"))

    matched_groups: list[str] = []  # preserve alert-order
    seen: set[str] = set()
    for alert in alerts:
        alert_grps = _alert_to_groups(alert)
        for g in card_grps & alert_grps:
            if g not in seen:
                seen.add(g)
                matched_groups.append(g)
        if has_where_filter and "scan_efficiency" in card_grps and "spill_memory_pressure" in alert_grps:
            if "spill_memory_pressure" not in seen:
                seen.add("spill_memory_pressure")
                matched_groups.append("spill_memory_pressure")

    return [_GROUP_TO_TAG[g] for g in matched_groups if g in _GROUP_TO_TAG]


def alert_severity_rank_for_card(card: ActionCard, alerts: list[Alert]) -> int:
    """Return the rank of the highest-severity alert this card addresses.

    Used as the primary sort key when re-ordering Top-5 actions so that
    a CRITICAL-addressing action comes before a HIGH-addressing one.
    Cards with no alert link land at the end (rank 99).
    """
    nums = match_card_to_alert_numbers(card, alerts)
    if not nums:
        return 99
    # `alerts` is sorted by severity already, so the smallest matched
    # index holds the highest-severity matched alert.
    top_alert = alerts[min(nums) - 1]
    return _SEVERITY_RANK.get(top_alert.severity, 99)
