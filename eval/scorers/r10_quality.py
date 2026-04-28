"""R10: Post-analysis quality evaluation add-on (Week 4 Day 5).

Aggregates the 6 deterministic Layer A signals into a single quality score
plus an explainable reasons[] list. LLM-judge Layer B is a placeholder for
Week 4+ (requires DATABRICKS_HOST/TOKEN to populate).

See: docs/eval/r10_quality_addon_design.md
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Layer A weights (must sum to 1.0). Locked at design time per W4 Day 4 doc.
LAYER_A_WEIGHTS: dict[str, float] = {
    "q3_composite":              0.30,
    "recall_strict":             0.20,
    "hallucination_clean":       0.20,
    "actionability_specific":    0.15,
    "schema_pass":               0.10,
    "canonical_parse_ok":        0.05,
}
assert abs(sum(LAYER_A_WEIGHTS.values()) - 1.0) < 1e-9

# Per-signal pass thresholds (used for reasons[] generation only — does NOT
# affect Layer A score directly, which uses a weighted average).
LAYER_A_THRESHOLDS: dict[str, float] = {
    "q3_composite":           0.80,
    "recall_strict":          0.50,
    "hallucination_clean":    0.85,
    "actionability_specific": 0.80,
    "schema_pass":            1.00,
    "canonical_parse_ok":     1.00,  # binary
}

# Verdict bands
PASS_THRESHOLD = 0.80
BORDERLINE_THRESHOLD = 0.60


@dataclass
class R10QualityScore:
    """Aggregated post-analysis quality score for one condition."""

    # Raw inputs (kept for transparency)
    schema_pass_ratio: float = 0.0  # 0..1 (cases passing R4 schema)
    actionability_specific: float = 0.0
    recall_strict: float = 0.0
    hallucination_clean: float = 1.0
    q3_composite: float = 1.0
    q3_finding_support: float = 1.0
    q3_metric_grounded: float = 1.0
    q3_ungrounded_numeric: float = 0.0  # lower-better
    canonical_parse_ok: float = 0.0  # 1 - parse_failure_rate

    # Computed
    layer_a_score: float = 0.0
    layer_a_reasons: list[str] = field(default_factory=list)

    # Layer B placeholder
    layer_b_score: float | None = None
    layer_b_reasons: list[str] = field(default_factory=list)

    # Aggregate
    overall_score: float = 0.0
    overall_verdict: str = "fail"
    overall_reasons: list[str] = field(default_factory=list)


def _verdict_for(score: float) -> str:
    if score >= PASS_THRESHOLD:
        return "pass"
    if score >= BORDERLINE_THRESHOLD:
        return "borderline"
    return "fail"


def _reason(metric: str, value: float, threshold: float) -> str:
    return f"{metric} {value * 100:.1f}% < target {threshold * 100:.0f}%"


def compute_layer_a(metrics: dict[str, float]) -> tuple[float, list[str]]:
    """Compute weighted Layer A score + reasons[].

    Args:
        metrics: dict with keys matching LAYER_A_WEIGHTS. Missing keys
            default to the worst plausible value (0 for up-is-better,
            1 for ungrounded-numeric — but ungrounded is consumed via
            (1 - q3_ungrounded_numeric) inside q3_composite, not here).

    Returns:
        (layer_a_score, reasons)
    """
    score = 0.0
    reasons: list[str] = []
    for metric, weight in LAYER_A_WEIGHTS.items():
        v = float(metrics.get(metric, 0.0) or 0.0)
        score += v * weight
        threshold = LAYER_A_THRESHOLDS[metric]
        if v < threshold:
            reasons.append(_reason(metric, v, threshold))
    return round(score, 4), reasons


def score_r10(
    *,
    schema_pass_ratio: float,
    actionability_specific: float,
    recall_strict: float,
    hallucination_clean: float,
    q3_composite: float,
    q3_finding_support: float,
    q3_metric_grounded: float,
    q3_ungrounded_numeric: float,
    canonical_parse_failure_rate: float,
    layer_b_score: float | None = None,
    layer_b_reasons: list[str] | None = None,
) -> R10QualityScore:
    """Compute a complete R10 quality score for one condition.

    All 0..1 ratios; ``q3_ungrounded_numeric`` is the lower-better signal
    captured raw (not flipped here — Q3 composite already incorporates it).

    Args:
        layer_b_score: optional 0..1 LLM-judge score. When None, the
            aggregate uses Layer A only.
    """
    canonical_parse_ok = 1.0 - max(0.0, min(1.0, canonical_parse_failure_rate))
    metrics = {
        "schema_pass":            schema_pass_ratio,
        "actionability_specific": actionability_specific,
        "recall_strict":          recall_strict,
        "hallucination_clean":    hallucination_clean,
        "q3_composite":           q3_composite,
        "canonical_parse_ok":     canonical_parse_ok,
    }
    layer_a_score, layer_a_reasons = compute_layer_a(metrics)

    if layer_b_score is None:
        overall_score = layer_a_score
    else:
        overall_score = round((layer_a_score + float(layer_b_score)) / 2, 4)
    overall_reasons = list(layer_a_reasons)
    if layer_b_reasons:
        overall_reasons.extend(f"[judge] {r}" for r in layer_b_reasons)
    overall_verdict = _verdict_for(overall_score)

    return R10QualityScore(
        schema_pass_ratio=schema_pass_ratio,
        actionability_specific=actionability_specific,
        recall_strict=recall_strict,
        hallucination_clean=hallucination_clean,
        q3_composite=q3_composite,
        q3_finding_support=q3_finding_support,
        q3_metric_grounded=q3_metric_grounded,
        q3_ungrounded_numeric=q3_ungrounded_numeric,
        canonical_parse_ok=canonical_parse_ok,
        layer_a_score=layer_a_score,
        layer_a_reasons=layer_a_reasons,
        layer_b_score=layer_b_score,
        layer_b_reasons=list(layer_b_reasons or []),
        overall_score=overall_score,
        overall_verdict=overall_verdict,
        overall_reasons=overall_reasons,
    )


def score_r10_from_condition_metrics(metrics: dict[str, float], parse_failure_rate: float) -> R10QualityScore:
    """Convenience wrapper that takes the ab_runner ``metrics_per_condition``
    layout directly (snake_case keys with the same names baseline_runner emits)."""
    return score_r10(
        schema_pass_ratio=float(metrics.get("schema_pass_rate", 1.0)),
        actionability_specific=float(metrics.get("actionability_specific_ratio", 0.0)),
        recall_strict=float(metrics.get("recall_strict_ratio", 0.0)),
        hallucination_clean=float(metrics.get("hallucination_score_avg", 0.0)),
        q3_composite=float(metrics.get("evidence_grounding_composite", 0.0)),
        q3_finding_support=float(metrics.get("finding_support_ratio", 0.0)),
        q3_metric_grounded=float(metrics.get("evidence_metric_grounding_ratio", 0.0)),
        q3_ungrounded_numeric=float(metrics.get("ungrounded_numeric_ratio", 0.0)),
        canonical_parse_failure_rate=float(parse_failure_rate),
    )


def to_dict(s: R10QualityScore) -> dict[str, Any]:
    return {
        "layer_a_score": s.layer_a_score,
        "layer_a_reasons": list(s.layer_a_reasons),
        "layer_b_score": s.layer_b_score,
        "layer_b_reasons": list(s.layer_b_reasons),
        "overall_score": s.overall_score,
        "overall_verdict": s.overall_verdict,
        "overall_reasons": list(s.overall_reasons),
        "inputs": {
            "schema_pass_ratio": s.schema_pass_ratio,
            "actionability_specific": s.actionability_specific,
            "recall_strict": s.recall_strict,
            "hallucination_clean": s.hallucination_clean,
            "q3_composite": s.q3_composite,
            "q3_finding_support": s.q3_finding_support,
            "q3_metric_grounded": s.q3_metric_grounded,
            "q3_ungrounded_numeric": s.q3_ungrounded_numeric,
            "canonical_parse_ok": s.canonical_parse_ok,
        },
    }
