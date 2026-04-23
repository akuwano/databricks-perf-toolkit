"""DBU and USD cost estimation for Spark Jobs Compute workloads.

Estimates Databricks Unit (DBU) consumption and dollar cost based on
instance type, worker count, duration, region, and Photon config.
This is an approximation -- actual billing uses system tables
(system.billing.usage) which may not be available.

Approach: core-count formula with family multipliers, plus a small
lookup table for the most common instance types.  Cloud compute
cost uses On-Demand list prices with region-based multipliers.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Instance type parsing
# ---------------------------------------------------------------------------

_FALLBACK_VCPUS = 4
_FALLBACK_CATEGORY = "unknown"


@dataclass(frozen=True)
class InstanceSpec:
    """Parsed instance specification."""

    cloud: str  # "aws", "azure", "gcp", "unknown"
    family: str  # e.g. "i3", "m5", "Standard_DS"
    vcpus: int
    category: str  # "compute", "memory", "storage", "general", "gpu", "unknown"
    raw: str


@dataclass(frozen=True)
class SizingRecommendation:
    """A single cluster sizing recommendation."""

    signal: str  # "DISK_SPILL", "HIGH_GC", "LOW_CPU", etc.
    severity: str  # "HIGH", "MEDIUM", "INFO"
    direction: str  # "UP", "DOWN", "CONSOLIDATE", "HORIZONTAL", "SCALE_LIMIT", "SPOT", "NONE"
    current_instance: str
    recommended_instance: str
    rationale: str
    current_usd_per_hr: float
    recommended_usd_per_hr: float
    cost_delta_pct: float  # positive = more expensive, negative = savings


# AWS size suffix -> vCPU count
_AWS_SIZE_VCPUS: dict[str, int] = {
    "large": 2,
    "xlarge": 4,
    "2xlarge": 8,
    "4xlarge": 16,
    "8xlarge": 32,
    "9xlarge": 36,
    "12xlarge": 48,
    "16xlarge": 64,
    "24xlarge": 96,
    "metal": 96,
}

# AWS family prefix -> category
_AWS_FAMILY_CATEGORY: dict[str, str] = {
    "c": "compute",
    "m": "general",
    "r": "memory",
    "i": "storage",
    "d": "storage",
    "g": "gpu",
    "p": "gpu",
}

# Azure common VM sizes -> (vcpus, category)
_AZURE_KNOWN: dict[str, tuple[int, str]] = {
    "Standard_DS3_v2": (4, "general"),
    "Standard_DS4_v2": (8, "general"),
    "Standard_DS5_v2": (16, "general"),
    "Standard_D4s_v3": (4, "general"),
    "Standard_D8s_v3": (8, "general"),
    "Standard_D16s_v3": (16, "general"),
    "Standard_D32s_v3": (32, "general"),
    "Standard_D4s_v5": (4, "general"),
    "Standard_D8s_v5": (8, "general"),
    "Standard_D16s_v5": (16, "general"),
    "Standard_D32s_v5": (32, "general"),
    "Standard_E4s_v3": (4, "memory"),
    "Standard_E8s_v3": (8, "memory"),
    "Standard_E16s_v3": (16, "memory"),
    "Standard_E32s_v3": (32, "memory"),
    "Standard_E4s_v5": (4, "memory"),
    "Standard_E8s_v5": (8, "memory"),
    "Standard_E16s_v5": (16, "memory"),
    "Standard_E32s_v5": (32, "memory"),
    "Standard_F4s_v2": (4, "compute"),
    "Standard_F8s_v2": (8, "compute"),
    "Standard_F16s_v2": (16, "compute"),
    "Standard_F32s_v2": (32, "compute"),
    "Standard_L8s_v2": (8, "storage"),
    "Standard_L16s_v2": (16, "storage"),
    "Standard_L8s_v3": (8, "storage"),
    "Standard_L16s_v3": (16, "storage"),
    "Standard_NC6s_v3": (6, "gpu"),
    "Standard_NC12s_v3": (12, "gpu"),
    "Standard_NC24s_v3": (24, "gpu"),
}


def parse_instance_type(instance_type: str) -> InstanceSpec:
    """Parse an instance type string into an InstanceSpec.

    Handles AWS (i3.xlarge), Azure (Standard_DS3_v2), and
    GCP (n2-standard-4) formats.
    """
    raw = (instance_type or "").strip()
    if not raw:
        return InstanceSpec("unknown", "", _FALLBACK_VCPUS, _FALLBACK_CATEGORY, raw)

    # --- AWS: family.size (e.g. i3.xlarge, m5d.2xlarge) ---
    m = re.match(r"^([a-z]\w*?)\.(\w+)$", raw, re.IGNORECASE)
    if m:
        family = m.group(1).lower()
        size = m.group(2).lower()
        vcpus = _AWS_SIZE_VCPUS.get(size, _FALLBACK_VCPUS)
        cat_key = family[0] if family else ""
        category = _AWS_FAMILY_CATEGORY.get(cat_key, "general")
        return InstanceSpec("aws", family, vcpus, category, raw)

    # --- Azure: Standard_XX_vN (e.g. Standard_DS3_v2) ---
    if raw.startswith("Standard_"):
        if raw in _AZURE_KNOWN:
            vcpus, category = _AZURE_KNOWN[raw]
            return InstanceSpec("azure", raw, vcpus, category, raw)
        # Try to extract number from name
        nums = re.findall(r"(\d+)", raw)
        vcpus = int(nums[0]) if nums else _FALLBACK_VCPUS
        return InstanceSpec("azure", raw, vcpus, "general", raw)

    # --- GCP: family-type-N (e.g. n2-standard-4, n2-highmem-8, n2-highcpu-16) ---
    m = re.match(r"^(\w+)-(\w+)-(\d+)$", raw)
    if m:
        family = m.group(1)
        type_ = m.group(2).lower()
        vcpus = int(m.group(3))
        if "highmem" in type_:
            category = "memory"
        elif "highcpu" in type_:
            category = "compute"
        else:
            category = "general"
        return InstanceSpec("gcp", family, vcpus, category, raw)

    # --- Unknown ---
    return InstanceSpec("unknown", "", _FALLBACK_VCPUS, _FALLBACK_CATEGORY, raw)


# ---------------------------------------------------------------------------
# Pricing tables
# ---------------------------------------------------------------------------

# Known instance type -> (DBU/hour, USD/hour On-Demand US East)
_INSTANCE_PRICING: dict[str, tuple[float, float]] = {
    # AWS general (m5)
    "m5.xlarge": (0.28, 0.192),
    "m5.2xlarge": (0.56, 0.384),
    "m5.4xlarge": (1.12, 0.768),
    "m5.8xlarge": (2.24, 1.536),
    "m5.12xlarge": (3.36, 2.304),
    "m5.16xlarge": (4.48, 3.072),
    "m5d.xlarge": (0.28, 0.226),
    "m5d.2xlarge": (0.56, 0.452),
    "m5d.4xlarge": (1.12, 0.904),
    # AWS general (m6, Graviton and Intel)
    "m6i.xlarge": (0.28, 0.192),
    "m6i.2xlarge": (0.56, 0.384),
    "m6i.4xlarge": (1.12, 0.768),
    "m6i.8xlarge": (2.24, 1.536),
    "m6id.xlarge": (0.28, 0.237),
    "m6id.2xlarge": (0.56, 0.475),
    "m6id.4xlarge": (1.12, 0.949),
    "m6gd.xlarge": (0.28, 0.181),
    "m6gd.2xlarge": (0.56, 0.362),
    "m6gd.4xlarge": (1.12, 0.724),
    "m6gd.8xlarge": (2.24, 1.448),
    # AWS general (m7, latest)
    "m7g.xlarge": (0.28, 0.163),
    "m7g.2xlarge": (0.56, 0.326),
    "m7g.4xlarge": (1.12, 0.653),
    "m7g.8xlarge": (2.24, 1.306),
    "m7gd.xlarge": (0.28, 0.216),
    "m7gd.2xlarge": (0.56, 0.432),
    "m7gd.4xlarge": (1.12, 0.864),
    "m7gd.8xlarge": (2.24, 1.729),
    "m7i.xlarge": (0.28, 0.201),
    "m7i.2xlarge": (0.56, 0.403),
    "m7i.4xlarge": (1.12, 0.806),
    "m7i.8xlarge": (2.24, 1.613),
    # AWS compute (c5/c6/c7)
    "c5.xlarge": (0.28, 0.170),
    "c5.2xlarge": (0.56, 0.340),
    "c5.4xlarge": (1.12, 0.680),
    "c5.9xlarge": (2.52, 1.530),
    "c6i.xlarge": (0.28, 0.170),
    "c6i.2xlarge": (0.56, 0.340),
    "c6i.4xlarge": (1.12, 0.680),
    "c6id.xlarge": (0.28, 0.215),
    "c6id.2xlarge": (0.56, 0.430),
    "c6gd.xlarge": (0.28, 0.154),
    "c6gd.2xlarge": (0.56, 0.307),
    "c6gd.4xlarge": (1.12, 0.614),
    "c7g.xlarge": (0.28, 0.145),
    "c7g.2xlarge": (0.56, 0.289),
    "c7g.4xlarge": (1.12, 0.579),
    "c7gd.xlarge": (0.28, 0.191),
    "c7gd.2xlarge": (0.56, 0.382),
    "c7i.xlarge": (0.28, 0.178),
    "c7i.2xlarge": (0.56, 0.357),
    # AWS memory (r5/r6/r7)
    "r5.xlarge": (0.28, 0.252),
    "r5.2xlarge": (0.56, 0.504),
    "r5.4xlarge": (1.12, 1.008),
    "r5.8xlarge": (2.24, 2.016),
    "r5d.xlarge": (0.28, 0.288),
    "r5d.2xlarge": (0.56, 0.576),
    "r5d.4xlarge": (1.12, 1.152),
    "r6i.xlarge": (0.28, 0.252),
    "r6i.2xlarge": (0.56, 0.504),
    "r6i.4xlarge": (1.12, 1.008),
    "r6id.xlarge": (0.28, 0.303),
    "r6id.2xlarge": (0.56, 0.605),
    "r6id.4xlarge": (1.12, 1.210),
    "r6id.8xlarge": (2.24, 2.419),
    "r6gd.xlarge": (0.28, 0.227),
    "r6gd.2xlarge": (0.56, 0.454),
    "r6gd.4xlarge": (1.12, 0.907),
    "r7g.xlarge": (0.28, 0.214),
    "r7g.2xlarge": (0.56, 0.428),
    "r7g.4xlarge": (1.12, 0.857),
    "r7gd.xlarge": (0.28, 0.271),
    "r7gd.2xlarge": (0.56, 0.543),
    "r7gd.4xlarge": (1.12, 1.085),
    "r7gd.8xlarge": (2.24, 2.170),
    "r7i.xlarge": (0.28, 0.266),
    "r7i.2xlarge": (0.56, 0.531),
    "r7iz.xlarge": (0.28, 0.372),
    "r7iz.2xlarge": (0.56, 0.745),
    # AWS storage (i3/i3en/i4i/i4g)
    "i3.xlarge": (0.28, 0.312),
    "i3.2xlarge": (0.56, 0.624),
    "i3.4xlarge": (1.12, 1.248),
    "i3.8xlarge": (2.24, 2.496),
    "i3.16xlarge": (4.48, 4.992),
    "i3en.xlarge": (0.28, 0.452),
    "i3en.2xlarge": (0.56, 0.904),
    "i4i.xlarge": (0.28, 0.341),
    "i4i.2xlarge": (0.56, 0.682),
    "i4i.4xlarge": (1.12, 1.364),
    "i4g.xlarge": (0.28, 0.277),
    "i4g.2xlarge": (0.56, 0.554),
    # AWS GPU
    "g4dn.xlarge": (1.00, 0.526),
    "g5.xlarge": (1.00, 1.006),
    "g5.2xlarge": (1.00, 1.212),
    "g5.4xlarge": (1.00, 1.624),
    "g5g.xlarge": (1.00, 0.420),
    "g6.xlarge": (1.00, 0.805),
    "g6.2xlarge": (1.00, 0.977),
    "p3.2xlarge": (2.00, 3.060),
    # Azure general (D series)
    "Standard_DS3_v2": (0.28, 0.229),
    "Standard_DS4_v2": (0.56, 0.458),
    "Standard_DS5_v2": (1.12, 0.916),
    "Standard_D4s_v3": (0.28, 0.192),
    "Standard_D8s_v3": (0.56, 0.384),
    "Standard_D16s_v3": (1.12, 0.768),
    "Standard_D32s_v3": (2.24, 1.536),
    "Standard_D4s_v5": (0.28, 0.192),
    "Standard_D8s_v5": (0.56, 0.384),
    "Standard_D16s_v5": (1.12, 0.768),
    "Standard_D32s_v5": (2.24, 1.536),
    # Azure memory (E series)
    "Standard_E4s_v3": (0.28, 0.252),
    "Standard_E8s_v3": (0.56, 0.504),
    "Standard_E16s_v3": (1.12, 1.008),
    "Standard_E32s_v3": (2.24, 2.016),
    "Standard_E4s_v5": (0.28, 0.252),
    "Standard_E8s_v5": (0.56, 0.504),
    "Standard_E16s_v5": (1.12, 1.008),
    "Standard_E32s_v5": (2.24, 2.016),
    # Azure compute (F series)
    "Standard_F4s_v2": (0.28, 0.169),
    "Standard_F8s_v2": (0.56, 0.338),
    "Standard_F16s_v2": (1.12, 0.677),
    "Standard_F32s_v2": (2.24, 1.354),
    # Azure storage (L series)
    "Standard_L8s_v2": (0.56, 0.572),
    "Standard_L16s_v2": (1.12, 1.144),
    "Standard_L8s_v3": (0.56, 0.624),
    "Standard_L16s_v3": (1.12, 1.248),
    # Azure GPU (NC series)
    "Standard_NC6s_v3": (1.00, 3.060),
    "Standard_NC12s_v3": (1.00, 6.120),
    "Standard_NC24s_v3": (1.00, 12.240),
    # GCP general (n2-standard)
    "n2-standard-2": (0.14, 0.097),
    "n2-standard-4": (0.28, 0.194),
    "n2-standard-8": (0.56, 0.389),
    "n2-standard-16": (1.12, 0.777),
    "n2-standard-32": (2.24, 1.555),
    # GCP memory (n2-highmem)
    "n2-highmem-2": (0.14, 0.131),
    "n2-highmem-4": (0.28, 0.262),
    "n2-highmem-8": (0.56, 0.524),
    "n2-highmem-16": (1.12, 1.048),
    "n2-highmem-32": (2.24, 2.096),
    # GCP compute (n2-highcpu)
    "n2-highcpu-4": (0.28, 0.143),
    "n2-highcpu-8": (0.56, 0.285),
    "n2-highcpu-16": (1.12, 0.571),
    "n2-highcpu-32": (2.24, 1.142),
    # GCP AMD EPYC (n2d-standard)
    "n2d-standard-4": (0.28, 0.169),
    "n2d-standard-8": (0.56, 0.338),
    "n2d-standard-16": (1.12, 0.676),
    "n2d-standard-32": (2.24, 1.352),
    # GCP cost-optimized (e2)
    "e2-standard-2": (0.14, 0.067),
    "e2-standard-4": (0.28, 0.134),
    "e2-standard-8": (0.56, 0.268),
    "e2-standard-16": (1.12, 0.536),
    # GCP latest compute (c3)
    "c3-standard-4": (0.28, 0.209),
    "c3-standard-8": (0.56, 0.418),
    "c3-standard-22": (1.54, 1.150),
}

# DBU per vCPU per hour by category (fallback formula)
_DBU_PER_VCPU_HOUR: dict[str, float] = {
    "general": 0.07,
    "compute": 0.07,
    "memory": 0.07,
    "storage": 0.07,
    "gpu": 0.25,
    "unknown": 0.07,
}

# USD per vCPU per hour by category (fallback formula, US East On-Demand)
_USD_PER_VCPU_HOUR: dict[str, float] = {
    "general": 0.048,
    "compute": 0.042,
    "memory": 0.063,
    "storage": 0.078,
    "gpu": 0.50,
    "unknown": 0.048,
}

PHOTON_MULTIPLIER = 2.0

# DBU list price — Jobs Compute PAYGO (consistent across AWS/Azure/GCP;
# the DBU *rate per instance* differs by cloud/instance but the DBU unit
# price in USD is the same).
_DBU_PRICE_USD = 0.15
_DBU_PRICE_USD_PHOTON = 0.30

# Region multiplier for cloud compute (us-east-1 = 1.0 baseline)
_REGION_MULTIPLIER: dict[str, float] = {
    # AWS
    "us-east-1": 1.0,
    "us-east-2": 1.0,
    "us-west-2": 1.0,
    "us-west-1": 1.05,
    "ca-central-1": 1.05,
    "eu-west-1": 1.10,
    "eu-west-2": 1.12,
    "eu-central-1": 1.12,
    "eu-north-1": 1.10,
    "ap-northeast-1": 1.15,
    "ap-northeast-2": 1.12,
    "ap-northeast-3": 1.15,
    "ap-southeast-1": 1.10,
    "ap-southeast-2": 1.12,
    "ap-south-1": 1.05,
    "sa-east-1": 1.20,
    # Azure
    "eastus": 1.0,
    "eastus2": 1.0,
    "westus2": 1.0,
    "westus3": 1.0,
    "centralus": 1.0,
    "northcentralus": 1.0,
    "southcentralus": 1.0,
    "westeurope": 1.10,
    "northeurope": 1.10,
    "uksouth": 1.12,
    "japaneast": 1.15,
    "japanwest": 1.15,
    "southeastasia": 1.10,
    "eastasia": 1.12,
    "australiaeast": 1.12,
    "brazilsouth": 1.20,
    # GCP
    "us-central1": 1.0,
    "us-east1": 1.0,
    "us-east4": 1.02,
    "us-west1": 1.0,
    "us-west4": 1.05,
    "europe-west1": 1.10,
    "europe-west4": 1.12,
    "asia-northeast1": 1.15,
    "asia-southeast1": 1.10,
}


def _get_region_multiplier(region: str) -> float:
    """Return region pricing multiplier (1.0 = US East baseline)."""
    key = (region or "").strip().lower()
    if key in _REGION_MULTIPLIER:
        return _REGION_MULTIPLIER[key]
    # Try case-insensitive match
    for k, v in _REGION_MULTIPLIER.items():
        if k.lower() == key:
            return v
    return 1.0


# ---------------------------------------------------------------------------
# Rate lookups
# ---------------------------------------------------------------------------


def dbu_rate_per_hour(instance_type: str, photon: bool = False) -> tuple[float, str]:
    """Return (dbu_per_hour, method) for a single node.

    method is "lookup" if exact match found, "formula" if derived from
    vCPU count, or "fallback" if instance type could not be parsed.
    """
    key = (instance_type or "").strip().lower()
    multiplier = PHOTON_MULTIPLIER if photon else 1.0

    # 1. Exact lookup
    for k, (dbu, _usd) in _INSTANCE_PRICING.items():
        if k.lower() == key:
            return (round(dbu * multiplier, 4), "lookup")

    # 2. Parse and use formula
    spec = parse_instance_type(instance_type)
    if spec.cloud == "unknown" and not spec.family:
        rate = _FALLBACK_VCPUS * _DBU_PER_VCPU_HOUR["unknown"]
        return (round(rate * multiplier, 4), "fallback")

    rate = spec.vcpus * _DBU_PER_VCPU_HOUR.get(spec.category, 0.07)
    return (round(rate * multiplier, 4), "formula")


def compute_price_per_hour(instance_type: str, region: str = "") -> tuple[float, str]:
    """Return (usd_per_hour, method) for a single node's cloud compute cost.

    Applies region multiplier to the US East baseline price.
    """
    key = (instance_type or "").strip().lower()
    region_mult = _get_region_multiplier(region)

    # 1. Exact lookup
    for k, (_dbu, usd) in _INSTANCE_PRICING.items():
        if k.lower() == key:
            return (round(usd * region_mult, 4), "lookup")

    # 2. Parse and use formula
    spec = parse_instance_type(instance_type)
    if spec.cloud == "unknown" and not spec.family:
        rate = _FALLBACK_VCPUS * _USD_PER_VCPU_HOUR["unknown"]
        return (round(rate * region_mult, 4), "fallback")

    rate = spec.vcpus * _USD_PER_VCPU_HOUR.get(spec.category, 0.048)
    return (round(rate * region_mult, 4), "formula")


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------


def estimate_dbu_cost(
    worker_node_type: str,
    driver_node_type: str,
    duration_min: float,
    autoscale_cost: list[dict[str, Any]],
    min_workers: int = 0,
    max_workers: int = 0,
    photon_enabled: bool = False,
    region: str = "",
) -> dict[str, Any]:
    """Estimate total DBU consumption and USD cost for a Spark application.

    Uses worker_node_type, duration, autoscale timeline distribution,
    region, and Photon config to compute approximate costs.
    """
    photon_mult = PHOTON_MULTIPLIER if photon_enabled else 1.0
    dbu_unit_price = _DBU_PRICE_USD_PHOTON if photon_enabled else _DBU_PRICE_USD

    if duration_min <= 0:
        return {
            "estimated_total_dbu": 0.0,
            "estimated_dbu_per_hour": 0.0,
            "driver_dbu": 0.0,
            "worker_dbu": 0.0,
            "estimated_total_usd": 0.0,
            "estimated_dbu_usd": 0.0,
            "estimated_compute_usd": 0.0,
            "photon_multiplier": photon_mult,
            "pricing_method": "n/a",
            "pricing_note": "Duration is 0 — no cost.",
            "region": region,
            "cost_display": "",
            "cost_display_en": "",
        }

    hours = duration_min / 60.0

    # Fixed-size cluster (min == max > 0): ignore autoscale timeline entirely.
    # Autoscale timelines on fixed clusters often contain only the ramp-up
    # events and miss the steady-state duration, leading to severe under- or
    # over-estimation. Using duration * worker_count is exact in this case.
    _fixed_cluster = min_workers > 0 and min_workers == max_workers

    # --- DBU ---
    driver_dbu_rate, driver_method = dbu_rate_per_hour(driver_node_type, photon=photon_enabled)
    driver_dbu = driver_dbu_rate * hours

    worker_dbu_rate, worker_method = dbu_rate_per_hour(worker_node_type, photon=photon_enabled)

    if autoscale_cost and not _fixed_cluster:
        worker_dbu = sum(
            int(entry.get("worker_count", 0))
            * worker_dbu_rate
            * float(entry.get("cumulative_min", 0))
            / 60.0
            for entry in autoscale_cost
        )
    else:
        worker_count = min_workers if min_workers > 0 else max_workers
        worker_dbu = worker_count * worker_dbu_rate * hours

    total_dbu = driver_dbu + worker_dbu
    dbu_per_hour = total_dbu / hours if hours > 0 else 0.0
    dbu_usd = total_dbu * dbu_unit_price

    # --- Cloud compute ---
    driver_compute_rate, _ = compute_price_per_hour(driver_node_type, region)
    driver_compute_usd = driver_compute_rate * hours

    worker_compute_rate, _ = compute_price_per_hour(worker_node_type, region)

    if autoscale_cost and not _fixed_cluster:
        worker_compute_usd = sum(
            int(entry.get("worker_count", 0))
            * worker_compute_rate
            * float(entry.get("cumulative_min", 0))
            / 60.0
            for entry in autoscale_cost
        )
    else:
        worker_count = min_workers if min_workers > 0 else max_workers
        worker_compute_usd = worker_count * worker_compute_rate * hours

    compute_usd = driver_compute_usd + worker_compute_usd
    total_usd = dbu_usd + compute_usd

    # Pricing method (most conservative)
    method = driver_method if driver_method == "fallback" else worker_method

    # Human-readable note
    worker_spec = parse_instance_type(worker_node_type)
    driver_spec = parse_instance_type(driver_node_type)
    photon_label = " Photon" if photon_enabled else ""
    region_label = region or "us-east-1"
    region_mult = _get_region_multiplier(region)
    sku = f"Jobs Compute{photon_label}"
    note = (
        f"{sku}, "
        f"{worker_node_type or '(unknown)'} ({worker_spec.vcpus} vCPU), "
        f"{worker_dbu_rate:.2f} DBU/hr/node, "
        f"PAYGO, {region_label} On-Demand"
    )

    # Worker count summary for breakdown display
    if autoscale_cost:
        worker_counts = [int(e.get("worker_count", 0)) for e in autoscale_cost]
        worker_count_label = f"{min(worker_counts)}→{max(worker_counts)}"
    else:
        wc = min_workers if min_workers > 0 else max_workers
        worker_count_label = str(wc)

    return {
        "estimated_total_dbu": round(total_dbu, 3),
        "estimated_dbu_per_hour": round(dbu_per_hour, 3),
        "driver_dbu": round(driver_dbu, 3),
        "worker_dbu": round(worker_dbu, 3),
        "estimated_total_usd": round(total_usd, 3),
        "estimated_dbu_usd": round(dbu_usd, 3),
        "estimated_compute_usd": round(compute_usd, 3),
        "photon_multiplier": photon_mult,
        "pricing_method": method,
        "pricing_note": note,
        "region": region,
        # Breakdown fields for Section G display
        "sku": sku,
        "worker_node_type": worker_node_type or "(unknown)",
        "driver_node_type": driver_node_type or "(unknown)",
        "worker_vcpus": worker_spec.vcpus,
        "driver_vcpus": driver_spec.vcpus,
        "worker_count_label": worker_count_label,
        "duration_min": round(duration_min, 1),
        "dbu_unit_price": dbu_unit_price,
        "worker_dbu_rate": round(worker_dbu_rate, 4),
        "driver_dbu_rate": round(driver_dbu_rate, 4),
        "worker_compute_rate": round(worker_compute_rate, 4),
        "driver_compute_rate": round(driver_compute_rate, 4),
        "region_multiplier": region_mult,
        # Pre-formatted display string for LLM/report use
        "cost_display": (
            f"推定総コスト ${total_usd:.3f}　"
            f"内訳(DBU:${dbu_usd:.3f} + クラウドコンピュート:${compute_usd:.3f})"
        ),
        "cost_display_en": (
            f"Estimated Total ${total_usd:.3f}　"
            f"Breakdown(DBU:${dbu_usd:.3f} + Cloud Compute:${compute_usd:.3f})"
        ),
    }


# ---------------------------------------------------------------------------
# Cluster right-sizing recommendations
# ---------------------------------------------------------------------------

# Same family: size up / down
_SIZE_UP: dict[str, str] = {
    "xlarge": "2xlarge",
    "2xlarge": "4xlarge",
    "4xlarge": "8xlarge",
    "8xlarge": "16xlarge",
    "large": "xlarge",
}
_SIZE_DOWN: dict[str, str] = {
    "2xlarge": "xlarge",
    "4xlarge": "2xlarge",
    "8xlarge": "4xlarge",
    "16xlarge": "8xlarge",
    "xlarge": "large",
}

# Family migration: general/compute/storage → memory optimized
_FAMILY_MEMORY_UPGRADE: dict[str, str] = {
    "m5": "r5",
    "m5d": "r5d",
    "m6i": "r6i",
    "c5": "r5",
    "c6i": "r6i",
    "i3": "r5",
    "i3en": "r5d",
}


def _suggest_instance(current: str, direction: str) -> str:
    """Suggest an alternative instance type for the given direction.

    Returns current if no suggestion is possible.
    """
    spec = parse_instance_type(current)
    if spec.cloud != "aws":
        return current  # Azure/GCP: no family migration table yet

    m = re.match(r"^([a-z]\w*?)\.(\w+)$", current, re.IGNORECASE)
    if not m:
        return current
    family, size = m.group(1).lower(), m.group(2).lower()

    if direction == "UP":
        # Try memory family first, then size up
        new_family = _FAMILY_MEMORY_UPGRADE.get(family)
        if new_family:
            return f"{new_family}.{size}"
        new_size = _SIZE_UP.get(size)
        if new_size:
            return f"{family}.{new_size}"
    elif direction == "DOWN":
        new_size = _SIZE_DOWN.get(size)
        if new_size:
            return f"{family}.{new_size}"
    elif direction == "CONSOLIDATE":
        # Suggest fewer, larger nodes: size up
        new_size = _SIZE_UP.get(size)
        if new_size:
            return f"{family}.{new_size}"

    return current


def _node_cost_per_hr(instance_type: str, region: str) -> float:
    """Total cost (DBU + compute) per node per hour."""
    dbu_rate, _ = dbu_rate_per_hour(instance_type)
    compute_rate, _ = compute_price_per_hour(instance_type, region)
    dbu_usd = dbu_rate * _DBU_PRICE_USD
    return dbu_usd + compute_rate


def generate_sizing_recommendations(
    executor_summary: dict[str, Any],
    app_summary: dict[str, Any],
    bottleneck_summary: list[dict[str, Any]],
    autoscale_cost: list[dict[str, Any]],
    scaling_event_counts: dict[str, int],
    region: str = "",
    lang: str = "ja",
) -> list[SizingRecommendation]:
    """Generate cluster sizing recommendations from existing Gold table data.

    Returns an empty list if the cluster appears well-sized.
    """
    if not executor_summary and not app_summary:
        return []

    worker_type = app_summary.get("worker_node_type", "")
    if not worker_type:
        return []

    ja = lang == "ja"

    avg_cpu = float(executor_summary.get("avg_cpu_efficiency_pct") or 0)
    avg_gc = float(executor_summary.get("avg_gc_pct") or 0)
    exec_count = int(executor_summary.get("executor_count") or 0)
    underutil = int(executor_summary.get("underutilized_count") or 0)
    spill_mb = float(executor_summary.get("total_disk_spill_mb") or 0)
    max_workers = int(app_summary.get("max_workers") or 0)
    min_workers = int(app_summary.get("min_workers") or 0)

    # Bottleneck counts by type
    bn_counts: dict[str, int] = {}
    for b in bottleneck_summary:
        bt = b.get("bottleneck_type", "")
        bn_counts[bt] = int(b.get("count", 0))

    current_cost = _node_cost_per_hr(worker_type, region)

    # Direction labels
    _DIR_LABEL = {
        "UP": "スケールアップ" if ja else "Scale UP",
        "DOWN": "スケールダウン" if ja else "Scale DOWN",
        "CONSOLIDATE": "統合" if ja else "Consolidate",
        "HORIZONTAL": "水平拡張" if ja else "Horizontal",
        "SCALE_LIMIT": "スケール上限" if ja else "Scale Limit",
        "SPOT": "可用性変更" if ja else "Availability",
    }

    # Collect raw recommendations (before dedup)
    _SEV_RANK = {"HIGH": 3, "MEDIUM": 2, "INFO": 1}
    raw: list[
        tuple[str, str, str, str, str]
    ] = []  # (direction, signal, severity, rationale, recommended)

    # --- Rule 1: Disk spill → UP ---
    spill_stages = bn_counts.get("DISK_SPILL", 0)
    if spill_stages >= 3 or spill_mb > 1000:
        rec_inst = _suggest_instance(worker_type, "UP")
        rationale = (
            f"ディスクスピルが{spill_stages}ステージで発生（合計{spill_mb:.0f} MB）。ノードあたりのメモリを増やしてスピルを削減"
            if ja
            else f"{spill_stages} stages with disk spill ({spill_mb:.0f} MB total); increase memory per node to reduce spill"
        )
        raw.append(("UP", "DISK_SPILL", "HIGH", rationale, rec_inst))

    # --- Rule 2: High GC → UP ---
    if avg_gc > 15:
        rec_inst = _suggest_instance(worker_type, "UP")
        rationale = (
            f"平均GCオーバーヘッド {avg_gc:.1f}%（>15%）。ノードあたりのメモリを増やしてGC負荷を軽減"
            if ja
            else f"avg GC overhead {avg_gc:.1f}% (>15%); increase memory per node"
        )
        raw.append(("UP", "HIGH_GC", "HIGH", rationale, rec_inst))

    # --- Rule 3: Low CPU + many underutilized → DOWN ---
    underutil_ratio = underutil / exec_count if exec_count > 0 else 0
    if avg_cpu > 0 and avg_cpu < 30 and underutil_ratio > 0.5:
        rec_inst = _suggest_instance(worker_type, "DOWN")
        rationale = (
            f"平均CPU {avg_cpu:.1f}%、{underutil}/{exec_count} Executorが低稼働。ワーカー数またはインスタンスサイズを削減"
            if ja
            else f"avg CPU {avg_cpu:.1f}%, {underutil}/{exec_count} executors underutilized; reduce worker count or instance size"
        )
        raw.append(("DOWN", "LOW_CPU", "HIGH", rationale, rec_inst))

    # --- Rule 4: Heavy shuffle + many small nodes → CONSOLIDATE ---
    shuffle_stages = bn_counts.get("HEAVY_SHUFFLE", 0)
    spec = parse_instance_type(worker_type)
    if shuffle_stages >= 3 and exec_count >= 8 and spec.vcpus <= 4:
        rec_inst = _suggest_instance(worker_type, "CONSOLIDATE")
        rationale = (
            f"ヘビーシャッフルが{shuffle_stages}ステージで発生（{exec_count}台の小ノード、{spec.vcpus} vCPU）。"
            "少数の大ノードに統合してネットワークホップを削減"
            if ja
            else f"{shuffle_stages} stages with heavy shuffle on {exec_count} small nodes ({spec.vcpus} vCPU); "
            "consolidate to fewer larger nodes to reduce network hops"
        )
        raw.append(("CONSOLIDATE", "HEAVY_SHUFFLE", "MEDIUM", rationale, rec_inst))

    # --- Rule 5: Data skew → HORIZONTAL ---
    skew_stages = bn_counts.get("DATA_SKEW", 0)
    if skew_stages >= 3:
        rationale = (
            f"データスキューが{skew_stages}ステージで発生。ワーカーを追加し、AQE Skew Joinを有効化"
            if ja
            else f"{skew_stages} stages with data skew; add more workers and enable AQE skew join"
        )
        raw.append(("HORIZONTAL", "DATA_SKEW", "MEDIUM", rationale, worker_type))

    # --- Rule 6: Autoscale at max → raise max_workers ---
    if autoscale_cost and max_workers > min_workers:
        max_entry = max(autoscale_cost, key=lambda e: int(e.get("worker_count", 0)))
        pct_at_max = float(max_entry.get("pct_of_total", 0))
        if int(max_entry.get("worker_count", 0)) >= max_workers and pct_at_max > 70:
            rationale = (
                f"実行時間の{pct_at_max:.0f}%がmax_workers（{max_workers}）で稼働。max_workersの引き上げを検討"
                if ja
                else f"{pct_at_max:.0f}% of time at max workers ({max_workers}); consider raising max_workers to allow better scaling"
            )
            raw.append(("SCALE_LIMIT", "AUTOSCALE_AT_MAX", "MEDIUM", rationale, worker_type))

    # --- Rule 7: Autoscale at min → lower max_workers ---
    if autoscale_cost and max_workers > min_workers:
        min_entry = min(autoscale_cost, key=lambda e: int(e.get("worker_count", 0)))
        pct_at_min = float(min_entry.get("pct_of_total", 0))
        if int(min_entry.get("worker_count", 0)) <= min_workers and pct_at_min > 70:
            rationale = (
                f"実行時間の{pct_at_min:.0f}%がmin_workers（{min_workers}）で稼働。max_workersの引き下げを検討"
                if ja
                else f"{pct_at_min:.0f}% of time at min workers ({min_workers}); consider lowering max_workers to reduce idle capacity cost"
            )
            raw.append(("SCALE_LIMIT", "AUTOSCALE_AT_MIN", "MEDIUM", rationale, worker_type))

    # --- Rule 8: Spot preemption → ON_DEMAND advisory ---
    spot_events = int(scaling_event_counts.get("SPOT_PREEMPTION", 0))
    if spot_events > 3:
        rationale = (
            f"Spotプリエンプションが{spot_events}回発生。ON_DEMANDまたは混合可用性を検討"
            if ja
            else f"{spot_events} spot preemption events; consider ON_DEMAND or mixed availability"
        )
        raw.append(("SPOT", "SPOT_PREEMPTION", "INFO", rationale, worker_type))

    # --- Well-sized check: if avg_cpu > 80% and no issues, return empty ---
    if not raw and avg_cpu >= 80:
        return []

    if not raw:
        return []

    # Deduplicate by direction (keep highest severity, merge rationale)
    deduped: dict[str, tuple[str, str, str, str, str]] = {}
    for direction, signal, severity, rationale, rec_inst in raw:
        if direction in deduped:
            existing = deduped[direction]
            existing_rank = _SEV_RANK.get(existing[2], 0)
            new_rank = _SEV_RANK.get(severity, 0)
            if new_rank >= existing_rank:
                merged_signal = (
                    f"{existing[1]}+{signal}" if signal not in existing[1] else existing[1]
                )
                merged_rationale = f"{existing[3]}; {rationale}"
                deduped[direction] = (
                    direction,
                    merged_signal,
                    severity,
                    merged_rationale,
                    rec_inst,
                )
            else:
                merged_signal = (
                    f"{existing[1]}+{signal}" if signal not in existing[1] else existing[1]
                )
                merged_rationale = f"{existing[3]}; {rationale}"
                deduped[direction] = (
                    direction,
                    merged_signal,
                    existing[2],
                    merged_rationale,
                    existing[4],
                )
        else:
            deduped[direction] = (direction, signal, severity, rationale, rec_inst)

    # Build final recommendations with cost delta
    result: list[SizingRecommendation] = []
    for direction, signal, severity, rationale, rec_inst in deduped.values():
        rec_cost = _node_cost_per_hr(rec_inst, region) if rec_inst != worker_type else current_cost
        delta_pct = ((rec_cost - current_cost) / current_cost * 100) if current_cost > 0 else 0

        result.append(
            SizingRecommendation(
                signal=signal,
                severity=severity,
                direction=direction,
                current_instance=worker_type,
                recommended_instance=rec_inst,
                rationale=rationale,
                current_usd_per_hr=round(current_cost, 2),
                recommended_usd_per_hr=round(rec_cost, 2),
                cost_delta_pct=round(delta_pct, 1),
            )
        )

    # Sort by severity (HIGH first)
    result.sort(key=lambda r: _SEV_RANK.get(r.severity, 0), reverse=True)
    return result
