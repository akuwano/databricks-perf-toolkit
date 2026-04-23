"""CLI entry point for the evaluation framework."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="eval",
        description="Evaluate SQL recommendation quality from DBSQL Profiler Analysis Tool",
    )
    parser.add_argument(
        "profile",
        help="Path to profile JSON file or directory of JSONs",
    )
    parser.add_argument(
        "--model", default="databricks-claude-sonnet-4",
        help="Primary LLM model for analysis (default: databricks-claude-sonnet-4)",
    )
    parser.add_argument(
        "--judge-model", default="databricks-claude-sonnet-4",
        help="LLM model for judge scoring (default: same as --model)",
    )
    parser.add_argument(
        "--no-judge", action="store_true",
        help="Skip L3/L4 LLM-as-judge scoring (only run L1/L2)",
    )
    parser.add_argument(
        "--json", action="store_true", dest="json_output",
        help="Output full JSON report instead of console summary",
    )
    parser.add_argument(
        "--output", "-o", type=str, default=None,
        help="Write JSON report to file",
    )
    parser.add_argument(
        "--serverless", action="store_true",
        help="Force serverless mode for evaluation",
    )
    parser.add_argument(
        "--lang", default="en", choices=["en", "ja"],
        help="Language for analysis (default: en)",
    )
    parser.add_argument(
        "--host", default=None,
        help="Databricks host (or DATABRICKS_HOST env)",
    )
    parser.add_argument(
        "--token", default=None,
        help="Databricks token (or DATABRICKS_TOKEN env)",
    )
    parser.add_argument(
        "--diff-from", metavar="REF_OR_JSON", default=None,
        help="Compare against baseline: a JSON file path or git ref (tag/branch/SHA)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging",
    )
    args = parser.parse_args(argv)

    # Logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Reduce noisy library logs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)

    # Credentials
    host = args.host or os.environ.get("DATABRICKS_HOST", "")
    token = args.token or os.environ.get("DATABRICKS_TOKEN", "")

    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN required.", file=sys.stderr)
        print("Set via --host/--token or environment variables.", file=sys.stderr)
        sys.exit(1)

    # Collect profile paths
    profile_path = Path(args.profile)
    if profile_path.is_dir():
        paths = sorted(str(p) for p in profile_path.glob("*.json"))
        if not paths:
            print(f"ERROR: No .json files found in {profile_path}", file=sys.stderr)
            sys.exit(1)
    elif profile_path.is_file():
        paths = [str(profile_path)]
    else:
        print(f"ERROR: {profile_path} not found", file=sys.stderr)
        sys.exit(1)

    # Import after sys.path setup
    from core.usecases import LLMConfig, PipelineOptions

    from .report import to_console, to_json
    from .runner import evaluate_profiles

    llm_config = LLMConfig(
        primary_model=args.model,
        review_model=args.model,
        refine_model=args.model,
        databricks_host=host,
        databricks_token=token,
        lang=args.lang,
    )
    options = PipelineOptions(
        skip_llm=False,  # Always run LLM for evaluation
        lang=args.lang,
    )

    if args.diff_from:
        # Diff mode: compare current vs baseline git ref
        from .diff_report import diff_to_console, diff_to_json
        from .diff_runner import run_diff

        print(
            f"Diffing {len(paths)} profile(s): current vs {args.diff_from} (model={args.model})...",
            file=sys.stderr,
        )

        diff = run_diff(
            profile_paths=paths,
            git_ref=args.diff_from,
            llm_config=llm_config,
            options=options,
            judge_model=args.judge_model,
            skip_judge=args.no_judge,
        )

        if args.output:
            Path(args.output).write_text(diff_to_json(diff), encoding="utf-8")
            print(f"Diff report written to {args.output}", file=sys.stderr)
            print(diff_to_console(diff))
        elif args.json_output:
            print(diff_to_json(diff))
        else:
            print(diff_to_console(diff))
    else:
        # Normal eval mode
        print(f"Evaluating {len(paths)} profile(s) with model={args.model}...", file=sys.stderr)

        report = evaluate_profiles(
            paths,
            llm_config,
            options,
            judge_model=args.judge_model,
            skip_judge=args.no_judge,
        )

        if args.output:
            Path(args.output).write_text(to_json(report), encoding="utf-8")
            print(f"Report written to {args.output}", file=sys.stderr)
            print(to_console(report))
        elif args.json_output:
            print(to_json(report))
        else:
            print(to_console(report))
