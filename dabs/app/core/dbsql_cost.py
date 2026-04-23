"""DBSQL query cost estimation.

Estimates query execution cost based on warehouse type and DBU pricing.

Pricing model:
- Serverless: Per-query billing based on execution time.
  DBU consumed = (execution_time_ms / 3,600,000) x DBU_per_hour.
- Classic/Pro: Per-warehouse uptime billing (not per-query).
  We show an *estimated query share* of the hourly cost for context,
  but the actual cost depends on total warehouse utilization.

DBU unit prices (Premium tier, us-west-2, PAYGO):
- Serverless: $0.70/DBU
- Pro:        $0.55/DBU
- Classic:    $0.22/DBU

Fallback estimation (no warehouse API):
  When warehouse info is unavailable, we use the parallelism ratio
  (task_total_time_ms / execution_time_ms) as a proxy for effective
  DBU/hour.  Additionally, we provide a reference cost table showing
  what the cost would be at each standard T-shirt size.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import QueryMetrics
    from .warehouse_client import WarehouseInfo


# DBU unit prices (USD) — Premium tier, us-west-2, Pay-As-You-Go
DBU_PRICE_SERVERLESS = 0.70
DBU_PRICE_PRO = 0.55
DBU_PRICE_CLASSIC = 0.22

# Default cluster size when warehouse info is unavailable
_DEFAULT_CLUSTER_SIZE = "Medium"
_DEFAULT_DBU_PER_HOUR = 24  # Medium = 24 DBU/hour

# DBU per hour by T-shirt size (mirrors warehouse_client.DBU_PER_HOUR)
DBU_PER_HOUR_BY_SIZE: dict[str, int] = {
    "2X-Small": 4,
    "X-Small": 6,
    "Small": 12,
    "Medium": 24,
    "Large": 40,
    "X-Large": 80,
    "2X-Large": 144,
    "3X-Large": 272,
    "4X-Large": 528,
    "5X-Large": 1042,
}

# All sizes in ascending order (for nearest-neighbor lookup)
_ALL_SIZES = list(DBU_PER_HOUR_BY_SIZE.keys())

# Short abbreviations for display
_SIZE_ABBREV: dict[str, str] = {
    "2X-Small": "2XS",
    "X-Small": "XS",
    "Small": "S",
    "Medium": "M",
    "Large": "L",
    "X-Large": "XL",
    "2X-Large": "2XL",
    "3X-Large": "3XL",
    "4X-Large": "4XL",
    "5X-Large": "5XL",
}


def _nearest_size(dbu_per_hour: float) -> tuple[str, str]:
    """Return (full_name, abbreviation) of the nearest standard T-shirt size."""
    best_idx = 0
    best_diff = float("inf")
    for i, size in enumerate(_ALL_SIZES):
        diff = abs(DBU_PER_HOUR_BY_SIZE[size] - dbu_per_hour)
        if diff < best_diff:
            best_diff = diff
            best_idx = i
    name = _ALL_SIZES[best_idx]
    return name, _SIZE_ABBREV.get(name, name)


@dataclass
class SizeReferenceCost:
    """Cost estimate for a specific T-shirt size (reference table entry)."""

    cluster_size: str = ""
    dbu_per_hour: int = 0
    estimated_dbu: float = 0.0
    estimated_cost_usd: float = 0.0


@dataclass
class CostEstimate:
    """Estimated cost for a single query execution."""

    billing_model: str = ""  # "Serverless", "Pro", "Classic"
    cluster_size: str = ""
    dbu_per_hour: int = 0
    dbu_unit_price: float = 0.0
    execution_time_ms: int = 0
    estimated_dbu: float = 0.0
    estimated_cost_usd: float = 0.0
    is_per_query: bool = (
        False  # True for Serverless (actual), False for Classic/Pro (share estimate)
    )
    is_estimated_size: bool = False  # True when cluster size is assumed (no warehouse API)
    parallelism_ratio: float = 0.0  # task_total_time_ms / execution_time_ms (0 = not computed)
    reference_costs: list[SizeReferenceCost] = field(default_factory=list)
    note: str = ""  # Additional context about the estimate


def estimate_query_cost(
    query_metrics: QueryMetrics,
    warehouse_info: WarehouseInfo | None,
) -> CostEstimate | None:
    """Estimate the cost of a query execution.

    Works in two modes:
    1. With warehouse_info (API available): uses actual cluster size and type.
    2. Without warehouse_info (fallback): infers billing model from
       query_typename and assumes Medium cluster size.

    Args:
        query_metrics: Query metrics with execution_time_ms and query_typename.
        warehouse_info: Warehouse info with cluster size, type, and serverless flag.

    Returns:
        CostEstimate, or None only when neither warehouse_info nor
        query_typename is available.
    """
    if warehouse_info is not None:
        return _estimate_with_warehouse_info(query_metrics, warehouse_info)

    return _estimate_from_typename(query_metrics)


def _estimate_with_warehouse_info(
    query_metrics: QueryMetrics,
    warehouse_info: WarehouseInfo,
) -> CostEstimate:
    """Estimate cost using actual warehouse info from API."""
    execution_time_ms = query_metrics.execution_time_ms
    if execution_time_ms <= 0:
        return CostEstimate(
            billing_model=_billing_model_from_wh(warehouse_info),
            cluster_size=warehouse_info.cluster_size,
            dbu_per_hour=warehouse_info.estimated_dbu_per_hour,
            dbu_unit_price=_unit_price_from_wh(warehouse_info),
            execution_time_ms=0,
            estimated_dbu=0.0,
            estimated_cost_usd=0.0,
            is_per_query=warehouse_info.is_serverless,
            note="No execution time recorded.",
        )

    dbu_per_hour = warehouse_info.estimated_dbu_per_hour
    unit_price = _unit_price_from_wh(warehouse_info)
    is_serverless = warehouse_info.is_serverless
    billing = _billing_model_from_wh(warehouse_info)

    estimated_dbu = (execution_time_ms / 3_600_000) * dbu_per_hour
    estimated_cost = estimated_dbu * unit_price

    if is_serverless:
        note = "Per-query billing (Serverless). Actual cost based on execution time."
    else:
        note = (
            f"{billing} warehouse: billed per uptime, not per query. "
            f"This is an estimated query share of the hourly cost."
        )

    return CostEstimate(
        billing_model=billing,
        cluster_size=warehouse_info.cluster_size,
        dbu_per_hour=dbu_per_hour,
        dbu_unit_price=unit_price,
        execution_time_ms=execution_time_ms,
        estimated_dbu=round(estimated_dbu, 4),
        estimated_cost_usd=round(estimated_cost, 4),
        is_per_query=is_serverless,
        note=note,
    )


def _infer_size_from_parallelism(parallelism: float) -> tuple[str, int, str]:
    """Rule-based cluster size inference from average parallelism.

    Returns (size_name, dbu_per_hour, confidence).

    Approach: convert the observed average parallelism (task-CPU-seconds
    per wall-clock second) into an implied DBU/h using the empirical
    ``1 DBU ≈ 3 vCPU`` ratio for DBSQL Serverless, then pick the nearest
    standard T-shirt size by DBU/h (smaller-side preferred on ties).

    Confidence heuristic:
      - ``high``    : parallelism >= 80 (cluster was likely saturated,
                      so the average is close to the true vCPU count)
      - ``medium``  : 20 <= parallelism < 80
      - ``low``     : parallelism < 20 (workload used so few threads
                      the actual cluster could easily be much larger)

    Non-saturated queries produce a *minimum-required-size* estimate,
    not the actual provisioned size. The note explains this and the
    reference table lets the reader bracket the likely billing.
    """
    if parallelism <= 0:
        return (_DEFAULT_CLUSTER_SIZE, _DEFAULT_DBU_PER_HOUR, "low")

    implied_dbu_h = parallelism / _VCPU_HOURS_PER_DBU

    best_name = _ALL_SIZES[0]
    best_diff = float("inf")
    for size in _ALL_SIZES:
        diff = abs(DBU_PER_HOUR_BY_SIZE[size] - implied_dbu_h)
        if diff < best_diff - 1e-9:
            best_diff = diff
            best_name = size

    if parallelism >= 80:
        confidence = "high"
    elif parallelism >= 20:
        confidence = "medium"
    else:
        confidence = "low"
    return (best_name, DBU_PER_HOUR_BY_SIZE[best_name], confidence)


def _estimate_from_typename(query_metrics: QueryMetrics) -> CostEstimate | None:
    """Fallback when warehouse API is unavailable: infer a likely cluster
    size from observed parallelism, price the query at that size, and
    surface the full reference table for cross-checking.

    Primary signal is ``parallelism = task_total_time_ms / execution_time_ms``.
    Converted to an implied DBU/h via ``1 DBU ≈ 3 vCPU`` (empirically
    anchored on 2XL saturation) and snapped to the nearest standard
    T-shirt size. See ``_infer_size_from_parallelism`` for the algorithm
    and its confidence tiers.

    Also surfaces the compute-consumption figure (``task CPU-hours /
    _VCPU_HOURS_PER_DBU × unit_price``) as a secondary value so callers
    that need a pure consumption signal — e.g. to detect
    over-provisioning — still have it.

    Trade-off: saturated queries (parallelism >= 80) land within ~±20%
    of real billing because the average is close to the actual vCPU
    count. Low-parallelism queries produce a *minimum-required size*
    estimate — i.e. "a Small would have been enough" — not the actual
    provisioned size, and the note labels this clearly.
    """
    typename = query_metrics.query_typename
    if not typename:
        return None

    is_serverless = typename == "LakehouseSqlQuery"
    billing = "Serverless" if is_serverless else "Classic"
    unit_price = DBU_PRICE_SERVERLESS if is_serverless else DBU_PRICE_CLASSIC

    execution_time_ms = query_metrics.execution_time_ms
    task_total_time_ms = query_metrics.task_total_time_ms

    parallelism = 0.0
    if execution_time_ms > 0 and task_total_time_ms > 0:
        parallelism = task_total_time_ms / execution_time_ms

    exec_hours = max(execution_time_ms, 0) / 3_600_000
    inferred_size, inferred_dbu_h, confidence = _infer_size_from_parallelism(parallelism)

    # Primary cost = size-based (what the user expects to see)
    size_based_dbu = exec_hours * inferred_dbu_h
    size_based_cost = size_based_dbu * unit_price

    # Secondary: pure compute consumption (useful when the size is
    # known to be over-provisioned).
    consumed_cpu_hours = max(task_total_time_ms, 0) / 3_600_000
    consumption_dbu = consumed_cpu_hours / _VCPU_HOURS_PER_DBU
    consumption_cost = consumption_dbu * unit_price

    ref_costs = _build_full_reference_costs(exec_hours, unit_price)

    note_prefix = (
        "Per-query billing (Serverless)."
        if is_serverless
        else f"{billing} warehouse: billed per uptime, not per query."
    )
    confidence_label = {"high": "高", "medium": "中", "low": "低"}.get(confidence, confidence)
    abbrev = _SIZE_ABBREV.get(inferred_size, inferred_size)
    note_parts = [
        note_prefix,
        f"Cluster size inferred from parallelism ({parallelism:.1f}) "
        f"→ {abbrev} ({inferred_dbu_h} DBU/h). Confidence: {confidence} "
        f"({confidence_label}).",
    ]
    if confidence == "low":
        note_parts.append(
            f"Low parallelism ({parallelism:.1f}): this is a *minimum-required* "
            "size estimate. The actual provisioned warehouse may be larger "
            "(over-provisioned workload). Pure compute consumption "
            f"cost: {format_cost_usd(consumption_cost)}."
        )
    elif confidence == "medium":
        note_parts.append(
            f"Medium confidence: parallelism ({parallelism:.1f}) suggests the "
            "workload was partially saturating the inferred size; actual "
            f"billing may differ. Pure compute consumption: "
            f"{format_cost_usd(consumption_cost)}."
        )
    else:
        note_parts.append(
            f"High confidence: parallelism ({parallelism:.1f}) is high enough "
            "that the workload likely saturated the inferred size, so the "
            "estimate tracks real billing closely."
        )
    note = " ".join(note_parts)

    return CostEstimate(
        billing_model=billing,
        cluster_size=f"{inferred_size} (inferred, confidence: {confidence})",
        dbu_per_hour=inferred_dbu_h,
        dbu_unit_price=unit_price,
        execution_time_ms=execution_time_ms,
        estimated_dbu=round(size_based_dbu, 4),
        estimated_cost_usd=round(size_based_cost, 4),
        is_per_query=is_serverless,
        is_estimated_size=True,
        parallelism_ratio=round(parallelism, 2),
        reference_costs=ref_costs,
        note=note,
    )


# Below this, parallelism is treated as "likely over-provisioned / billing
# higher than consumption". The value is roughly the vCPU count of a small
# warehouse (Small ≈ 12 vCPU). Tune with real data.
_SATURATION_THRESHOLD = 40

# vCPU-hours that correspond to 1 DBU-hour for DBSQL Serverless. Derived
# empirically: a 2X-Large Serverless warehouse bills 144 DBU/h while
# showing saturated parallelism ≈ 450 task-threads, so each DBU-hour is
# roughly 3 vCPU-hours of task work.
#
# NOTE: this is a Serverless anchor. The same ratio is currently applied
# to Pro/Classic as a provisional approximation; the true vCPU:DBU ratio
# for those SKUs is almost certainly different (their DBU/h tables are
# lower, so the same task work yields more DBUs). Re-calibrate per-SKU
# once real billing-vs-profile samples are available.
_VCPU_HOURS_PER_DBU = 3.0


def _build_full_reference_costs(hours: float, unit_price: float) -> list[SizeReferenceCost]:
    """Reference cost table covering every standard T-shirt size.

    Used alongside the consumption-based estimate so the reader can
    bracket the likely billing if they know their actual warehouse size.
    """
    if hours <= 0:
        return []
    refs = []
    for size in _ALL_SIZES:
        dbu_h = DBU_PER_HOUR_BY_SIZE[size]
        est_dbu = hours * dbu_h
        est_cost = est_dbu * unit_price
        refs.append(
            SizeReferenceCost(
                cluster_size=size,
                dbu_per_hour=dbu_h,
                estimated_dbu=round(est_dbu, 4),
                estimated_cost_usd=round(est_cost, 4),
            )
        )
    return refs


def format_cost_usd(cost: float) -> str:
    """Format cost in USD with appropriate precision.

    Args:
        cost: Cost in USD.

    Returns:
        Formatted string like "$0.0012" or "$1.23".
    """
    if cost == 0:
        return "$0.000"
    return f"${cost:.3f}"


def _billing_model_from_wh(warehouse_info: WarehouseInfo) -> str:
    """Determine the billing model label from warehouse info."""
    if warehouse_info.is_serverless:
        return "Serverless"
    if warehouse_info.is_pro:
        return "Pro"
    return "Classic"


def _unit_price_from_wh(warehouse_info: WarehouseInfo) -> float:
    """Get the DBU unit price from warehouse info."""
    if warehouse_info.is_serverless:
        return DBU_PRICE_SERVERLESS
    if warehouse_info.is_pro:
        return DBU_PRICE_PRO
    return DBU_PRICE_CLASSIC
