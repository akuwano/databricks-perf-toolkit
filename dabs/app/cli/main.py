#!/usr/bin/env python3
"""
CLI entry point for Databricks SQL Query Profile Analyzer.
"""

import argparse
import json
import logging
import os
import sys

from core import (
    LLMConfig,
    PipelineOptions,
    load_tuning_knowledge,
    run_analysis_pipeline,
    set_language,
)


def _add_analysis_args(parser: argparse.ArgumentParser) -> None:
    """Add standard analysis arguments."""
    parser.add_argument(
        "profile",
        help="Path to query profile JSON file",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output file path (default: stdout)",
        default=None,
    )
    parser.add_argument(
        "--model",
        help="Primary LLM model for analysis (default: databricks-claude-opus-4-7)",
        default="databricks-claude-opus-4-7",
    )
    parser.add_argument(
        "--review-model",
        help="Secondary LLM model for reviewing the analysis (default: databricks-claude-opus-4-7)",
        default="databricks-claude-opus-4-7",
    )
    parser.add_argument(
        "--tuning-file",
        help="Path to dbsql_tuning.md file or directory containing it",
        default=None,
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip LLM analysis and generate report with metrics only",
    )
    parser.add_argument(
        "--no-review",
        action="store_true",
        help="Skip review by secondary LLM",
    )
    parser.add_argument(
        "--no-refine",
        action="store_true",
        help="Skip refinement step (output initial analysis and review separately)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show all analysis stages (initial, review, refined) in report",
    )
    parser.add_argument(
        "--refine-model",
        help="LLM model for refinement (default: same as --model)",
        default=None,
    )
    parser.add_argument(
        "--explain",
        help="Path to EXPLAIN EXTENDED output file for additional analysis",
        default=None,
    )
    parser.add_argument(
        "--lang",
        choices=["en", "ja"],
        help="Output language (default: en, can also be set via DBSQL_LANG env var)",
        default=None,
    )
    parser.add_argument(
        "--report-review",
        action="store_true",
        help="Add LLM review of the generated report",
    )
    parser.add_argument(
        "--report-review-model",
        help="LLM model for report review (default: same as --review-model)",
        default=None,
    )
    parser.add_argument(
        "--refine-report",
        action="store_true",
        help="Refine the report based on LLM review feedback (implies --report-review)",
    )
    parser.add_argument(
        "--refine-report-model",
        help="LLM model for report refinement (default: same as --refine-model)",
        default=None,
    )

    # v3: Comparison / tracking arguments
    parser.add_argument(
        "--experiment-id",
        help="Experiment or tuning campaign ID for grouping analyses",
        default=None,
    )
    parser.add_argument(
        "--variant",
        help="Variant label (e.g., baseline, optimized, candidate_a)",
        default=None,
    )
    # --baseline kept for backward compat but variant="baseline" is preferred
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="(Deprecated) Use --variant baseline instead",
    )
    parser.add_argument(
        "--tags",
        help='JSON tags for this analysis (e.g., \'{"env":"prod","team":"data"}\')',
        default=None,
    )
    parser.add_argument(
        "--compare-with",
        help="Analysis ID to compare against (runs comparison after analysis)",
        default=None,
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Persist analysis results to Delta tables",
    )


def _build_llm_config(args: argparse.Namespace) -> LLMConfig:
    databricks_host = os.environ.get("DATABRICKS_HOST", "")
    databricks_token = os.environ.get("DATABRICKS_TOKEN", "")
    return LLMConfig(
        primary_model=args.model,
        review_model=args.review_model,
        refine_model=args.refine_model or "",
        databricks_host=databricks_host,
        databricks_token=databricks_token,
        tuning_file=args.tuning_file,
        lang=args.lang or os.environ.get("DBSQL_LANG", "en"),
    )


def _build_options(args: argparse.Namespace, lang: str) -> PipelineOptions:
    explain_text = None
    if args.explain:
        with open(args.explain, encoding="utf-8") as f:
            explain_text = f.read()

    return PipelineOptions(
        skip_llm=args.no_llm,
        skip_review=args.no_review,
        skip_refine=args.no_refine,
        enable_report_review=args.report_review or args.refine_report,
        enable_report_refine=args.refine_report,
        verbose=args.verbose,
        explain_text=explain_text,
        lang=lang,
    )


def _make_stage_callback(args: argparse.Namespace, llm_config: LLMConfig):
    def on_stage(stage: str) -> None:
        stage_messages = {
            "metrics": "Analyzing query profile...",
            "explain": f"Parsing EXPLAIN file: {args.explain}...",
            "llm_initial": f"Sending to primary LLM ({args.model})...",
            "llm_review": f"Sending to review LLM ({args.review_model})...",
            "llm_refine": f"Sending to refine LLM ({llm_config.refine_model})...",
            "report": "Generating report...",
            "report_review": f"Reviewing report with LLM ({args.review_model})...",
            "report_refine": f"Refining report with LLM ({llm_config.refine_model})...",
        }
        msg = stage_messages.get(stage)
        if msg:
            print(msg, file=sys.stderr)

    return on_stage


def main():
    """Main entry point."""
    # Suppress noisy library warnings
    logging.getLogger("sqlglot").setLevel(logging.ERROR)

    parser = argparse.ArgumentParser(
        description="Analyze Databricks SQL query profile and generate performance report"
    )
    _add_analysis_args(parser)
    args = parser.parse_args()

    # Set language (priority: --lang > DBSQL_LANG env var > default 'en')
    lang = args.lang or os.environ.get("DBSQL_LANG", "en")
    if lang not in ("en", "ja"):
        lang = "en"
    set_language(lang)

    # Validate profile file exists
    if not os.path.exists(args.profile):
        print(f"Error: Profile file not found: {args.profile}", file=sys.stderr)
        sys.exit(1)

    # Validate explain file exists (if specified)
    if args.explain and not os.path.exists(args.explain):
        print(f"Error: Explain file not found: {args.explain}", file=sys.stderr)
        sys.exit(1)

    # Check tuning knowledge early to warn user
    tuning_knowledge = load_tuning_knowledge(args.tuning_file, lang=lang)
    if not tuning_knowledge:
        print(
            "Warning: dbsql_tuning.md not found, analysis will proceed without tuning guidelines",
            file=sys.stderr,
        )

    # Check environment variables for LLM
    databricks_host = os.environ.get("DATABRICKS_HOST", "")
    databricks_token = os.environ.get("DATABRICKS_TOKEN", "")

    if not args.no_llm and (not databricks_host or not databricks_token):
        print(
            "Warning: DATABRICKS_HOST and DATABRICKS_TOKEN not set, skipping LLM analysis",
            file=sys.stderr,
        )
        print("Set them with:", file=sys.stderr)
        print(
            '  export DATABRICKS_HOST="https://<workspace>.cloud.databricks.com"',
            file=sys.stderr,
        )
        print('  export DATABRICKS_TOKEN="<your-token>"', file=sys.stderr)

    # Load profile JSON
    with open(args.profile, encoding="utf-8") as f:
        data = json.load(f)

    # Build configuration
    llm_config = _build_llm_config(args)
    llm_config.lang = lang
    options = _build_options(args, lang)

    # Build AnalysisContext from CLI args
    from core.models import AnalysisContext

    tags = {}
    if args.tags:
        try:
            tags = json.loads(args.tags)
        except json.JSONDecodeError:
            print("Warning: --tags must be valid JSON, ignoring", file=sys.stderr)

    variant = args.variant or ""
    baseline_flag = args.baseline or variant.lower() == "baseline"

    analysis_context = AnalysisContext(
        experiment_id=args.experiment_id or "",
        variant=variant,
        baseline_flag=baseline_flag,
        tags=tags,
    )

    # Decide pipeline: persist or standard
    on_stage = _make_stage_callback(args, llm_config)

    if args.persist:
        from core.usecases import run_analysis_and_persist_pipeline
        from services.table_writer import TableWriter, TableWriterConfig

        writer_config = TableWriterConfig.from_env()
        writer_config.enabled = True
        writer = TableWriter(writer_config)

        print("Running analysis with Delta persistence...", file=sys.stderr)
        result = run_analysis_and_persist_pipeline(
            data,
            llm_config,
            options,
            writer=writer,
            analysis_context=analysis_context,
            on_stage=on_stage,
        )
    else:
        result = run_analysis_pipeline(data, llm_config, options, on_stage=on_stage)
        # Still attach context for potential comparison
        if args.experiment_id or args.variant:
            from core.fingerprint import generate_fingerprint, normalize_sql

            result.analysis.analysis_context = analysis_context
            sql_text = result.analysis.query_metrics.query_text
            if sql_text:
                analysis_context.query_text_normalized = normalize_sql(sql_text)
                analysis_context.query_fingerprint = generate_fingerprint(sql_text)

    # Run comparison if requested
    if args.compare_with:
        _run_comparison(args, result, llm_config)

    # Output
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(result.report)
        print(f"Report written to: {args.output}", file=sys.stderr)
    else:
        print(result.report)


def _run_comparison(args: argparse.Namespace, result, llm_config: LLMConfig) -> None:
    """Load a previous analysis and run comparison."""
    from core.comparison_reporter import generate_comparison_report
    from core.models import ComparisonRequest
    from core.usecases import run_comparison_pipeline
    from services.table_reader import TableReader
    from services.table_writer import TableWriterConfig

    reader_config = TableWriterConfig.from_env()
    reader = TableReader(reader_config)

    print(f"Loading baseline analysis: {args.compare_with}...", file=sys.stderr)
    baseline = reader.get_analysis_by_id(args.compare_with)

    if baseline is None:
        print(
            f"Error: Could not load analysis '{args.compare_with}' from Delta tables. "
            f"Ensure PROFILER_CATALOG, PROFILER_SCHEMA, and PROFILER_WAREHOUSE_HTTP_PATH are set.",
            file=sys.stderr,
        )
        return

    request = ComparisonRequest(
        baseline_analysis_id=args.compare_with,
        candidate_analysis_id="current",
        request_source="cli",
    )

    print("Comparing analyses...", file=sys.stderr)
    comparison = run_comparison_pipeline(baseline, result.analysis, request)

    # Persist comparison if --persist
    if args.persist:
        from services.table_writer import TableWriter, TableWriterConfig

        writer_config = TableWriterConfig.from_env()
        writer_config.enabled = True
        writer = TableWriter(writer_config)
        writer.write_comparison_result(comparison)
        print(f"Comparison persisted: {comparison.comparison_id}", file=sys.stderr)

    # Generate and append comparison report
    comparison_report = generate_comparison_report(comparison)
    result.report += "\n\n---\n\n" + comparison_report
    print("Comparison report appended.", file=sys.stderr)


if __name__ == "__main__":
    main()
