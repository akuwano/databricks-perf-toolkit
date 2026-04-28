"""V6 R9 regression detector (Week 6 Day 2).

Compares two `goldens_runner` baseline JSON files and reports tier-graded
violations. Stage 1 of the V6 R5 acceptance gate.

See: docs/eval/regression_detector_design.md
"""

from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Tier thresholds (W6 Day 1 design)
# ---------------------------------------------------------------------------

# Tier 1: BLOCK on any breach. delta = current - baseline.
# All metrics here are "higher-is-better" except where flipped via sign.
TIER1_METRICS: dict[str, dict[str, Any]] = {
    "evidence_grounding_composite": {"max_drop": 0.03, "label": "Q3 composite"},
    "actionability_specific_ratio": {"max_drop": 0.03, "label": "Q4 actionability"},
    "failure_taxonomy_score":       {"max_drop": 0.03, "label": "Q5 failure taxonomy"},
    "recall_strict_ratio":          {"max_drop": 0.03, "label": "Recall (strict)"},
    "hallucination_score_avg":      {"max_drop": 0.03, "label": "Hallucination clean"},
}
# Schema is treated as a binary-ish 0..1 (1 case dropping is ~3.4% on 29 cases)
TIER1_SCHEMA_MAX_DROP = 0.01

# Tier 2: WARN
TIER2_METRICS: dict[str, dict[str, Any]] = {
    # higher-is-better
    "parse_success_rate":   {"max_drop": 0.05, "label": "Skeleton parse_success_rate"},
    # lower-is-better, so "delta > +threshold" is bad
    "ungrounded_numeric_ratio":     {"max_increase": 0.05, "label": "Q3 ungrounded_numeric"},
    "canonical_parse_failure_rate": {"max_increase": 0.05, "label": "Canonical parse failure"},
}

# Tier 3: INFO
TIER3_DRIFT_THRESHOLD = 0.10  # method share shift > 10pt → info


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class Violation:
    metric: str
    label: str
    current: float
    baseline: float
    delta: float
    tier: int
    direction: str  # "drop" | "increase"


@dataclass
class RegressionResult:
    current_run: str
    compared_against: str
    block_violations: list[dict[str, Any]] = field(default_factory=list)
    warn_violations: list[dict[str, Any]] = field(default_factory=list)
    info_violations: list[dict[str, Any]] = field(default_factory=list)
    skeleton_distribution_drift: dict[str, dict[str, float]] = field(default_factory=dict)
    verdict: str = "clean"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Loaders & aggregators
# ---------------------------------------------------------------------------


def _load(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _evaluated(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [r for r in records if "skipped_reason" not in r]


def _avg_field(records: list[dict[str, Any]], field_name: str) -> float:
    vals = [r.get(field_name) for r in _evaluated(records) if r.get(field_name) is not None]
    vals = [v for v in vals if isinstance(v, (int, float))]
    return sum(vals) / len(vals) if vals else 0.0


def _schema_pass_rate(records: list[dict[str, Any]]) -> float:
    ev = _evaluated(records)
    if not ev:
        return 1.0
    return sum(1 for r in ev if r.get("schema_valid") is True) / len(ev)


def _canonical_failure_rate(records: list[dict[str, Any]]) -> float:
    ev = _evaluated(records)
    if not ev:
        return 0.0
    fails = sum(
        1 for r in ev
        if r.get("canonical_source") in (None, "normalizer_fallback", "missing")
    )
    return fails / len(ev)


def _flatten(values: list[list]) -> list:
    out = []
    for v in values:
        out.extend(v or [])
    return out


def _skeleton_method_distribution(records: list[dict[str, Any]]) -> dict[str, float]:
    methods = _flatten([r.get("skeleton_methods") for r in _evaluated(records)])
    if not methods:
        return {}
    counts = Counter(methods)
    total = len(methods)
    return {k: round(v / total, 4) for k, v in counts.items()}


def _parse_success_rate(records: list[dict[str, Any]]) -> float:
    """skeleton method ∈ {fullsql, sqlglot, bypass, merge, view, insert} = success.
    head_tail / truncate = failure.

    V6.1: merge/view/insert added when V6_SQL_SKELETON_EXTENDED=1.
    """
    methods = _flatten([r.get("skeleton_methods") for r in _evaluated(records)])
    if not methods:
        return 1.0  # nothing to fail
    success_set = {"fullsql", "sqlglot", "bypass", "merge", "view", "insert"}
    succ = sum(1 for m in methods if m in success_set)
    return succ / len(methods)


def _compression_p50(records: list[dict[str, Any]]) -> float:
    ratios = _flatten([r.get("skeleton_compression_ratios") for r in _evaluated(records)])
    if not ratios:
        return 0.0
    return statistics.median(ratios)


# ---------------------------------------------------------------------------
# Comparison core
# ---------------------------------------------------------------------------


def _compare_metric(
    current: float,
    baseline: float,
    *,
    max_drop: float | None = None,
    max_increase: float | None = None,
) -> tuple[bool, str, float]:
    """Return (violated, direction, delta).

    Pass either max_drop (higher-is-better) or max_increase (lower-is-better).
    """
    delta = round(current - baseline, 4)
    if max_drop is not None and delta < -max_drop:
        return True, "drop", delta
    if max_increase is not None and delta > max_increase:
        return True, "increase", delta
    return False, "stable", delta


def detect_regression(
    current_path: Path,
    baseline_path: Path,
    *,
    current_run: str | None = None,
    compared_against: str | None = None,
) -> RegressionResult:
    """Run regression detection between two goldens_runner baseline JSONs."""
    current = _load(current_path)
    baseline = _load(baseline_path)

    cur_records = current.get("cases") or []
    base_records = baseline.get("cases") or []

    result = RegressionResult(
        current_run=current_run or current_path.stem,
        compared_against=compared_against or baseline_path.stem,
    )

    block: list[Violation] = []
    warn: list[Violation] = []
    info: list[Violation] = []

    # ----- Tier 1 -----
    for metric, cfg in TIER1_METRICS.items():
        cur = _avg_field(cur_records, metric)
        base = _avg_field(base_records, metric)
        violated, direction, delta = _compare_metric(cur, base, max_drop=cfg["max_drop"])
        if violated:
            block.append(Violation(
                metric=metric, label=cfg["label"],
                current=round(cur, 4), baseline=round(base, 4),
                delta=delta, tier=1, direction=direction,
            ))

    # schema (per-case ratio)
    cur_schema = _schema_pass_rate(cur_records)
    base_schema = _schema_pass_rate(base_records)
    violated, direction, delta = _compare_metric(
        cur_schema, base_schema, max_drop=TIER1_SCHEMA_MAX_DROP
    )
    if violated:
        block.append(Violation(
            metric="schema_pass", label="R4 schema pass rate",
            current=round(cur_schema, 4), baseline=round(base_schema, 4),
            delta=delta, tier=1, direction=direction,
        ))

    # ----- Tier 2 -----
    cur_parse = _parse_success_rate(cur_records)
    base_parse = _parse_success_rate(base_records)
    violated, direction, delta = _compare_metric(
        cur_parse, base_parse,
        max_drop=TIER2_METRICS["parse_success_rate"]["max_drop"],
    )
    if violated:
        warn.append(Violation(
            metric="parse_success_rate",
            label=TIER2_METRICS["parse_success_rate"]["label"],
            current=round(cur_parse, 4), baseline=round(base_parse, 4),
            delta=delta, tier=2, direction=direction,
        ))

    # ungrounded_numeric (lower-is-better)
    cur_un = _avg_field(cur_records, "ungrounded_numeric_ratio")
    base_un = _avg_field(base_records, "ungrounded_numeric_ratio")
    violated, direction, delta = _compare_metric(
        cur_un, base_un,
        max_increase=TIER2_METRICS["ungrounded_numeric_ratio"]["max_increase"],
    )
    if violated:
        warn.append(Violation(
            metric="ungrounded_numeric_ratio",
            label=TIER2_METRICS["ungrounded_numeric_ratio"]["label"],
            current=round(cur_un, 4), baseline=round(base_un, 4),
            delta=delta, tier=2, direction=direction,
        ))

    # canonical_parse_failure (lower-is-better)
    cur_pf = _canonical_failure_rate(cur_records)
    base_pf = _canonical_failure_rate(base_records)
    violated, direction, delta = _compare_metric(
        cur_pf, base_pf,
        max_increase=TIER2_METRICS["canonical_parse_failure_rate"]["max_increase"],
    )
    if violated:
        warn.append(Violation(
            metric="canonical_parse_failure_rate",
            label=TIER2_METRICS["canonical_parse_failure_rate"]["label"],
            current=round(cur_pf, 4), baseline=round(base_pf, 4),
            delta=delta, tier=2, direction=direction,
        ))

    # over-recommendation count (Codex W5 #5)
    cur_over = sum(
        (r.get("failure_counts") or {}).get("over_recommendation", 0)
        for r in _evaluated(cur_records)
    )
    base_over = sum(
        (r.get("failure_counts") or {}).get("over_recommendation", 0)
        for r in _evaluated(base_records)
    )
    if cur_over > base_over:
        warn.append(Violation(
            metric="over_recommendation_count",
            label="Q5 over-recommendation incidents",
            current=cur_over, baseline=base_over,
            delta=cur_over - base_over, tier=2, direction="increase",
        ))

    # ----- Tier 3 -----
    cur_dist = _skeleton_method_distribution(cur_records)
    base_dist = _skeleton_method_distribution(base_records)
    drift: dict[str, dict[str, float]] = {}
    for method in set(cur_dist) | set(base_dist):
        c = cur_dist.get(method, 0.0)
        b = base_dist.get(method, 0.0)
        d = round(c - b, 4)
        drift[method] = {"current": c, "baseline": b, "delta": d}
        if abs(d) >= TIER3_DRIFT_THRESHOLD:
            info.append(Violation(
                metric=f"skeleton_method:{method}",
                label=f"Skeleton method share ({method})",
                current=c, baseline=b, delta=d, tier=3,
                direction="increase" if d > 0 else "drop",
            ))
    result.skeleton_distribution_drift = drift

    result.block_violations = [asdict(v) for v in block]
    result.warn_violations = [asdict(v) for v in warn]
    result.info_violations = [asdict(v) for v in info]

    if block:
        result.verdict = "block"
    elif warn:
        result.verdict = "warn"
    else:
        result.verdict = "clean"
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _write_markdown(result: RegressionResult, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# {result.current_run} — Regression Detector",
        "",
        f"Compared against: {result.compared_against}",
        f"Verdict: **{result.verdict.upper()}**",
        "",
    ]
    for tier_name, items in [
        ("BLOCK (Tier 1)", result.block_violations),
        ("WARN (Tier 2)", result.warn_violations),
        ("INFO (Tier 3)", result.info_violations),
    ]:
        lines += ["", f"## {tier_name}", ""]
        if not items:
            lines.append("_(none)_")
            continue
        lines += [
            "| metric | current | baseline | delta |",
            "|--------|--------:|--------:|------:|",
        ]
        for v in items:
            cur = v["current"]
            base = v["baseline"]
            delta = v["delta"]
            cur_s = f"{cur:.2%}" if isinstance(cur, float) and cur <= 1.5 else str(cur)
            base_s = f"{base:.2%}" if isinstance(base, float) and base <= 1.5 else str(base)
            sign = "+" if isinstance(delta, (int, float)) and delta >= 0 else ""
            delta_s = f"{sign}{delta:.2%}" if isinstance(delta, float) else f"{sign}{delta}"
            lines.append(f"| {v['label']} ({v['metric']}) | {cur_s} | {base_s} | {delta_s} |")

    if result.skeleton_distribution_drift:
        lines += ["", "## Skeleton method distribution", "",
                  "| method | current | baseline | delta |",
                  "|--------|--------:|--------:|------:|"]
        for method, d in sorted(result.skeleton_distribution_drift.items()):
            lines.append(
                f"| {method} | {d['current']:.2%} | {d['baseline']:.2%} | "
                f"{'+' if d['delta'] >= 0 else ''}{d['delta']:.2%} |"
            )

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="regression_detector",
        description="V6 R9 regression detector — compare two goldens baselines.",
    )
    parser.add_argument("--current", required=True, help="path to current run baseline JSON")
    parser.add_argument("--baseline", required=True, help="path to compared-against baseline JSON")
    parser.add_argument("--out", default=None, help="output JSON path (defaults to eval/regression_summary/<current>.json)")
    parser.add_argument("--out-md", default=None, help="markdown path (default sibling .md)")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--exit-on", default="block", choices=["block", "warn", "never"],
                        help="exit 1 when verdict reaches this tier (default: block)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    current_path = Path(args.current).resolve()
    baseline_path = Path(args.baseline).resolve()
    if not current_path.exists():
        logger.error("current baseline not found: %s", current_path)
        return 2
    if not baseline_path.exists():
        logger.error("baseline not found: %s", baseline_path)
        return 2

    result = detect_regression(current_path, baseline_path)

    out_path = Path(args.out) if args.out else (REPO_ROOT / "eval" / "regression_summary" / f"{current_path.stem}.json")
    out_md = Path(args.out_md) if args.out_md else out_path.with_suffix(".md")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            **result.to_dict(),
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }, f, indent=2, ensure_ascii=False)
    _write_markdown(result, out_md)
    logger.info("regression summary: %s (verdict=%s)", out_path, result.verdict)

    if args.exit_on == "block" and result.verdict == "block":
        return 1
    if args.exit_on == "warn" and result.verdict in ("warn", "block"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
