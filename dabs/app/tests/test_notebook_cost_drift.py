"""Drift guard: notebook cost logic must match dabs/app/core/dbu_pricing.py.

The Spark Perf ETL notebook inlines a minimal copy of dbu_pricing.py for
cost enrichment. This test extracts the constants from the notebook source
and asserts they equal the canonical values in the app module.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from core import dbu_pricing

_REPO_ROOT = Path(__file__).resolve().parents[3]
_NOTEBOOK_PATH = _REPO_ROOT / "dabs" / "notebooks" / "01_Spark Perf Pipeline PySpark.py"


def _extract_literal(source: str, name: str):
    """Extract a top-level assignment's literal value from notebook source."""
    pattern = rf"^{re.escape(name)}\s*=\s*(.+?)(?=\n[A-Za-z_#]|\Z)"
    match = re.search(pattern, source, re.MULTILINE | re.DOTALL)
    if not match:
        raise AssertionError(f"{name} not found in notebook source")
    raw = match.group(1).strip().rstrip(",")
    return ast.literal_eval(raw)


def _load_notebook_source() -> str:
    return _NOTEBOOK_PATH.read_text(encoding="utf-8")


def test_photon_multiplier_matches():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_PHOTON_MULTIPLIER") == dbu_pricing.PHOTON_MULTIPLIER


def test_dbu_price_constants_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_DBU_PRICE_USD") == dbu_pricing._DBU_PRICE_USD
    assert _extract_literal(src, "_COST_DBU_PRICE_USD_PHOTON") == dbu_pricing._DBU_PRICE_USD_PHOTON


def test_aws_size_vcpus_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_AWS_SIZE_VCPUS") == dbu_pricing._AWS_SIZE_VCPUS


def test_aws_family_category_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_AWS_FAMILY_CATEGORY") == dbu_pricing._AWS_FAMILY_CATEGORY


def test_azure_known_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_AZURE_KNOWN") == dbu_pricing._AZURE_KNOWN


def test_instance_pricing_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_INSTANCE_PRICING") == dbu_pricing._INSTANCE_PRICING


def test_dbu_per_vcpu_hour_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_DBU_PER_VCPU_HOUR") == dbu_pricing._DBU_PER_VCPU_HOUR


def test_usd_per_vcpu_hour_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_USD_PER_VCPU_HOUR") == dbu_pricing._USD_PER_VCPU_HOUR


def test_region_multiplier_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_REGION_MULTIPLIER") == dbu_pricing._REGION_MULTIPLIER


def test_fallback_vcpus_match():
    src = _load_notebook_source()
    assert _extract_literal(src, "_COST_FALLBACK_VCPUS") == dbu_pricing._FALLBACK_VCPUS
