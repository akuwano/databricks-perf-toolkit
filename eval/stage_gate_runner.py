"""V6 R5 stage gate CLI runner (Week 6 Day 5).

Wraps `stage_gate.run_stage_gate()` with a CLI suitable for CI:

  python -m eval.stage_gate_runner \\
    --current eval/baselines/v6_w6_baseline.json \\
    --baseline eval/baselines/v6_w5_baseline.json \\
    --on-hold-exit 0 \\
    --on-reject-exit 1
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from .stage_gate import DEFAULT_ACCEPTANCE, run_stage_gate, to_dict

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent


_VERDICT_EMOJI = {"adopt": "✅", "hold": "🟡", "reject": "❌"}


def _write_markdown(result_dict: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    measured = result_dict.get("measured", {})
    acc = result_dict.get("acceptance", {})

    def _pct(v):
        try:
            return f"{float(v) * 100:.2f}%"
        except (TypeError, ValueError):
            return str(v)

    lines = [
        f"# {result_dict['current_run']} — V6 Stage Gate",
        "",
        f"Compared to baseline: {result_dict.get('baseline_run', '(none)')}",
        f"Generated: {result_dict.get('generated_at', '')}",
        "",
        f"## Verdict: {_VERDICT_EMOJI.get(result_dict['overall_verdict'], '?')} **{result_dict['overall_verdict'].upper()}**",
        "",
        f"- Stage 1 (regression vs baseline): **{result_dict['stage1_verdict']}**",
        f"- Stage 2 (V6 absolute acceptance): **{result_dict['stage2_verdict']}**",
        "",
        "## Measured (Stage 2 inputs)",
        "",
        "| metric | measured | target |",
        "|--------|---------:|-------:|",
        f"| schema_pass | {_pct(measured.get('schema_pass', 0))} | {_pct(acc.get('schema_pass', 1))} |",
        f"| q3_composite | {_pct(measured.get('q3_composite', 0))} | {_pct(acc.get('q3_composite', 0.8))} |",
        f"| actionability_specific | {_pct(measured.get('actionability_specific', 0))} | {_pct(acc.get('actionability_specific', 0.8))} |",
        f"| failure_taxonomy | {_pct(measured.get('failure_taxonomy', 0))} | {_pct(acc.get('failure_taxonomy', 0.7))} |",
        f"| recall_strict | {_pct(measured.get('recall_strict', 0))} | {_pct(acc.get('recall_strict', 0.5))} |",
        f"| hallucination_clean | {_pct(measured.get('hallucination_clean', 0))} | {_pct(acc.get('hallucination_clean', 0.85))} |",
        f"| ungrounded_numeric_avg | {_pct(measured.get('ungrounded_numeric_avg', 0))} | ≤ {_pct(acc.get('ungrounded_numeric_max', 0.15))} |",
        f"| parse_success_rate | {_pct(measured.get('parse_success_rate', 0))} | {_pct(acc.get('parse_success_rate', 0.9))} |",
        f"| canonical_parse_failure | {_pct(measured.get('canonical_parse_failure', 0))} | ≤ {_pct(acc.get('canonical_parse_failure_max', 0.05))} |",
        f"| case_regressions | {int(measured.get('case_regressions', 0))} | ≤ {int(acc.get('case_regressions_max', 1))} |",
    ]

    if result_dict.get("stage1_violations"):
        lines += ["", "## Stage 1 violations (regression block tier)", ""]
        for v in result_dict["stage1_violations"]:
            lines.append(
                f"- {v.get('label', v.get('metric'))}: "
                f"current={_pct(v.get('current'))}, "
                f"baseline={_pct(v.get('baseline'))}, "
                f"delta={v.get('delta')}"
            )
    if result_dict.get("stage2_violations"):
        lines += ["", "## Stage 2 violations (V6 acceptance)", ""]
        for v in result_dict["stage2_violations"]:
            lines.append(
                f"- {v.get('metric')}: current={_pct(v.get('current'))}, "
                f"target {v.get('op', '>=')} {_pct(v.get('target'))}"
            )
    out_path.write_text("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="stage_gate_runner")
    parser.add_argument("--current", required=True, help="path to current baseline JSON")
    parser.add_argument("--baseline", default=None, help="path to compared baseline JSON")
    parser.add_argument(
        "--out", default=None, help="output JSON (default eval/stage_gate/<current>.json)"
    )
    parser.add_argument("--out-md", default=None)
    parser.add_argument(
        "--on-hold-exit", type=int, default=0,
        help="exit code when verdict=hold (default 0)",
    )
    parser.add_argument(
        "--on-reject-exit", type=int, default=1,
        help="exit code when verdict=reject (default 1)",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    current = Path(args.current).resolve()
    baseline = Path(args.baseline).resolve() if args.baseline else None
    if not current.exists():
        logger.error("current not found: %s", current)
        return 2

    result = run_stage_gate(current, baseline_path=baseline, acceptance=DEFAULT_ACCEPTANCE)

    payload = to_dict(result)
    payload["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    out_path = Path(args.out) if args.out else (REPO_ROOT / "eval" / "stage_gate" / f"{current.stem}.json")
    out_md = Path(args.out_md) if args.out_md else out_path.with_suffix(".md")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    _write_markdown(payload, out_md)

    logger.info(
        "stage gate verdict=%s (stage1=%s, stage2=%s)",
        result.overall_verdict, result.stage1_verdict, result.stage2_verdict,
    )

    if result.overall_verdict == "reject":
        return args.on_reject_exit
    if result.overall_verdict == "hold":
        return args.on_hold_exit
    return 0


if __name__ == "__main__":
    sys.exit(main())
