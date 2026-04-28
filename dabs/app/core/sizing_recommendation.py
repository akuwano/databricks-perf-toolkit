"""Normative cluster sizing recommendation from a profile JSON.

This module answers the question:

    "For the workload represented by this profile, what size cluster
    *should* be used, and what would it cost at that size?"

It is **not** an "infer the actual provisioned cluster" function — that
problem is observability-bounded and produces only a lower bound (see
``dbsql_cost._infer_size_from_parallelism``). Instead, this is a
prescription about a workload, intentionally framed independently of
whatever cluster actually ran the query.

Design (Codex 2026-04-26):

  1. **fit floor**   — parallelism + spill + scan throughput give the
                        smallest size that the workload could plausibly
                        run on without breaking.
  2. **SLA gate**    — if the user supplies a target runtime tighter
                        than the observed one, push up by the linear
                        scaling factor. Default target = observed run
                        time, so the gate is a no-op for the default
                        case ("same speed at the right size").
  3. **diminishing**  — parallelism ceiling and Photon coverage cap how
                        much benefit a larger size can produce. Below
                        this cap we say "larger than X likely adds
                        little benefit".

Output is a 3-band structure (``minimum_viable`` / ``recommended`` /
``oversized_beyond``) so the reader can see the uncertainty bracket
rather than betting everything on a single number.

Scope: this is a recommendation **for the observed run** — large
input-volume swings can change the answer. The scope notice on the
``SizingRecommendation`` makes that explicit.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .dbsql_cost import (
    DBU_PER_HOUR_BY_SIZE,
    DBU_PRICE_CLASSIC,
    DBU_PRICE_SERVERLESS,
    _ALL_SIZES,
    _SIZE_ABBREV,
    _infer_size_from_parallelism,
)
from .models import QueryMetrics


@dataclass
class SizeBand:
    """One T-shirt size + its priced cost at the recommended runtime."""

    cluster_size: str = ""
    abbrev: str = ""
    dbu_per_hour: int = 0
    estimated_cost_usd: float = 0.0


@dataclass
class SizingRecommendation:
    """3-band normative sizing recommendation for the observed workload."""

    minimum_viable: SizeBand
    recommended: SizeBand
    oversized_beyond: SizeBand | None = None
    confidence: str = "low"
    rationale: list[str] = field(default_factory=list)
    sla_target_ms: int = 0
    billing_label: str = ""
    scope_notice: str = ""


# ---------------------------------------------------------------------------
# Tunables — kept inside this module so the dbsql_cost lower-bound code
# stays free of normative-recommendation concerns.
# ---------------------------------------------------------------------------

# Parallelism below this is considered "low" — diminishing returns
# kick in early and the cap is tightened.
_LOW_PARALLELISM_THRESHOLD = 20.0

# Photon coverage at or above this is "saturated" — going larger adds
# little because the bottleneck is no longer Photon-execution capacity.
_PHOTON_SATURATED_RATIO = 0.95

# Spill of any size triggers the memory-pressure push-up (one tier).
# Sub-MB spills can occur from minor sort artifacts; gate above that.
_SPILL_PUSHUP_BYTES = 1 * 1024 * 1024  # 1 MB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _size_index(size_name: str) -> int:
    return _ALL_SIZES.index(size_name)


def _shift_size(size_name: str, delta: int) -> str:
    """Move ``delta`` tiers up (positive) or down (negative), clamping
    at the standard T-shirt range."""
    idx = _size_index(size_name)
    new_idx = max(0, min(len(_ALL_SIZES) - 1, idx + delta))
    return _ALL_SIZES[new_idx]


def _snap_to_size(implied_dbu_h: float) -> str:
    """Pick the nearest standard T-shirt size by DBU/h."""
    best = _ALL_SIZES[0]
    best_diff = float("inf")
    for size in _ALL_SIZES:
        diff = abs(DBU_PER_HOUR_BY_SIZE[size] - implied_dbu_h)
        if diff < best_diff - 1e-9:
            best_diff = diff
            best = size
    return best


def _make_band(size_name: str, target_hours: float, unit_price: float) -> SizeBand:
    dbu_h = DBU_PER_HOUR_BY_SIZE[size_name]
    cost = target_hours * dbu_h * unit_price
    return SizeBand(
        cluster_size=size_name,
        abbrev=_SIZE_ABBREV.get(size_name, size_name),
        dbu_per_hour=dbu_h,
        estimated_cost_usd=round(cost, 4),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def recommend_size(
    query_metrics: QueryMetrics,
    *,
    target_time_ms: int | None = None,
) -> SizingRecommendation | None:
    """Produce a normative size recommendation for the observed workload.

    Args:
        query_metrics: Profile-derived metrics. Must have a ``query_typename``
            so the billing model can be selected.
        target_time_ms: Optional target runtime. ``None`` means "use the
            observed execution time" — i.e. "same speed, right size".
            A tighter value triggers the SLA gate; a looser value never
            *downgrades* below the fit floor.

    Returns:
        ``SizingRecommendation`` with three bands, confidence, rationale,
        billing label, and scope notice. ``None`` only when neither
        billing model nor any signal is available (no typename).
    """
    typename = query_metrics.query_typename
    if not typename:
        return None

    is_serverless = typename == "LakehouseSqlQuery"
    unit_price = DBU_PRICE_SERVERLESS if is_serverless else DBU_PRICE_CLASSIC
    billing_label = (
        "Estimated execution cost"
        if is_serverless
        else "Runtime-equivalent cost (if dedicated)"
    )

    execution_time_ms = max(query_metrics.execution_time_ms, 0)
    task_total_time_ms = max(query_metrics.task_total_time_ms, 0)
    parallelism = (
        task_total_time_ms / execution_time_ms if execution_time_ms > 0 else 0.0
    )

    # ------- Step 1: fit floor (lower bound) -------
    floor_size, floor_dbu_h, floor_confidence = _infer_size_from_parallelism(parallelism)

    # ------- Step 2: SLA gate (linear scaling, monotone non-decreasing) -------
    target_ms = target_time_ms if target_time_ms is not None else execution_time_ms
    target_ms = max(target_ms, 1)  # avoid div-by-zero
    if execution_time_ms > 0 and target_ms < execution_time_ms:
        sla_factor = execution_time_ms / target_ms
        sla_required_dbu_h = floor_dbu_h * sla_factor
        sla_size = _snap_to_size(sla_required_dbu_h)
    else:
        sla_size = floor_size

    # ------- Step 2b: memory pressure push-up -------
    # Spill = the workload needed more RAM than the size offered. The
    # fit floor (parallelism-based) doesn't see this, so add a tier.
    if query_metrics.spill_to_disk_bytes >= _SPILL_PUSHUP_BYTES:
        memory_size = _shift_size(floor_size, +1)
    else:
        memory_size = floor_size

    # ------- Recommended = max of all push-ups, never below floor -------
    candidate_indices = [
        _size_index(floor_size),
        _size_index(sla_size),
        _size_index(memory_size),
    ]
    recommended_idx = max(candidate_indices)
    recommended_size = _ALL_SIZES[recommended_idx]

    # ------- Step 3: diminishing returns cap -------
    photon_ratio = (
        query_metrics.photon_total_time_ms / task_total_time_ms
        if task_total_time_ms > 0
        else 0.0
    )
    cap_delta = 2  # default headroom
    if parallelism > 0 and parallelism < _LOW_PARALLELISM_THRESHOLD:
        cap_delta = 1  # low parallelism = larger size adds little
    if photon_ratio >= _PHOTON_SATURATED_RATIO:
        cap_delta = min(cap_delta, 1)  # Photon already saturated

    cap_size = _shift_size(recommended_size, cap_delta)
    # If recommended is already the largest size, there is no
    # oversized_beyond — caller handles None.
    if _size_index(cap_size) > _size_index(recommended_size):
        oversized_beyond_size: str | None = cap_size
    else:
        oversized_beyond_size = None

    # ------- Cost computation at each band -------
    target_hours = target_ms / 3_600_000
    minimum_band = _make_band(floor_size, target_hours, unit_price)
    recommended_band = _make_band(recommended_size, target_hours, unit_price)
    oversized_band = (
        _make_band(oversized_beyond_size, target_hours, unit_price)
        if oversized_beyond_size
        else None
    )

    # ------- Rationale -------
    rationale: list[str] = []
    rationale.append(
        f"Fit floor: parallelism={parallelism:.1f} → {minimum_band.abbrev} "
        f"({floor_dbu_h} DBU/h, confidence={floor_confidence})"
    )
    if query_metrics.spill_to_disk_bytes >= _SPILL_PUSHUP_BYTES:
        rationale.append(
            f"Spill detected ({query_metrics.spill_to_disk_bytes / 1024 / 1024:.0f} MB) "
            "→ memory-pressure push-up by one tier"
        )
    if target_time_ms is not None and target_time_ms < execution_time_ms:
        rationale.append(
            f"SLA gate: target {target_time_ms} ms < observed {execution_time_ms} ms "
            f"→ push to {recommended_band.abbrev}"
        )
    else:
        rationale.append("SLA gate: default target = observed execution time (no-op)")
    if oversized_beyond_size is not None:
        if cap_delta == 1:
            cap_reason = (
                "low parallelism / Photon saturation"
                if photon_ratio >= _PHOTON_SATURATED_RATIO
                else "low parallelism — larger size adds little benefit"
            )
        else:
            cap_reason = "moderate workload — modest headroom available"
        rationale.append(f"Diminishing returns cap: {cap_reason}")

    return SizingRecommendation(
        minimum_viable=minimum_band,
        recommended=recommended_band,
        oversized_beyond=oversized_band,
        confidence=floor_confidence,
        rationale=rationale,
        sla_target_ms=target_ms,
        billing_label=billing_label,
        scope_notice=(
            "Recommendation for the observed run characteristics. May differ "
            "for larger or smaller input volumes."
        ),
    )
