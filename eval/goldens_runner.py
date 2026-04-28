"""V6 Goldens runner: evaluate a goldens manifest end-to-end.

Reads `eval/goldens/manifest.yaml` + per-case yaml files, runs the analysis
pipeline on each referenced profile, and applies all scorers (L1-L4 +
actionability + hallucination + recall) to produce a baseline report.

Output:
- eval/baselines/<name>.json: full per-case + per-card scores
- eval/reports/<name>.md: human-readable summary

Usage:
    PYTHONPATH=dabs/app:. uv run python -m eval.goldens_runner \\
        --manifest eval/goldens/manifest.yaml \\
        --baseline-name v6_week1_baseline \\
        --skip-judge          # skip LLM judge (mechanical scorers only)
        --skip-llm            # skip analysis LLM (use rule-based only)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_yaml(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_global_forbidden(manifest_dir: Path) -> list[dict]:
    """Load globally forbidden claims (Codex (b) recommendation).

    Truly-global forbidden claims live in `global_forbidden.yaml` and are
    merged into every case's `forbidden_claims`. Cases can opt out by
    listing ids under `disable_global_forbidden:`.

    Returns empty list if the file is missing — keeps the runner working
    on older checkouts without the file.
    """
    gf_path = manifest_dir / "global_forbidden.yaml"
    if not gf_path.exists():
        return []
    raw = _load_yaml(gf_path)
    return list(raw.get("global_forbidden_claims") or [])


def _merge_global_forbidden(case: dict, global_forbidden: list[dict]) -> list[dict]:
    """Union case.forbidden_claims with global_forbidden, honoring opt-outs.

    Conflict resolution: case-level entries with the same id take
    precedence (case description wins). Codex flagged truly-global vs
    case-local distinction — `federation`, `spark.sql.adaptive`, etc.
    must NOT live here, so cases that legitimately mention those keep
    them as case-local.
    """
    disabled_ids = set(case.get("disable_global_forbidden") or [])
    case_claims = list(case.get("forbidden_claims") or [])
    case_ids = {c.get("id") for c in case_claims if isinstance(c, dict)}

    merged = list(case_claims)
    for g in global_forbidden:
        if not isinstance(g, dict):
            continue
        gid = g.get("id")
        if not gid or gid in disabled_ids or gid in case_ids:
            continue
        merged.append(g)
    return merged


def _load_manifest(manifest_path: Path) -> tuple[dict, list[dict]]:
    """Return (manifest, [case_dict, ...]) where case_dict has merged metadata."""
    manifest = _load_yaml(manifest_path)
    cases_dir = manifest_path.parent
    global_forbidden = _load_global_forbidden(cases_dir)
    cases = []
    for entry in manifest.get("cases", []):
        case_file = cases_dir / entry["file"]
        if not case_file.exists():
            logger.warning("case file missing: %s", case_file)
            continue
        case = _load_yaml(case_file)
        case["_manifest_entry"] = entry  # keep tags/priority
        # Merge truly-global forbidden claims (HC legacy syntax, TEMP VIEW
        # for CTE materialization, etc.) — case can opt out via
        # `disable_global_forbidden: [id...]`.
        case["forbidden_claims"] = _merge_global_forbidden(case, global_forbidden)
        cases.append(case)
    return manifest, cases


def _resolve_profile(case: dict) -> Path | None:
    """Resolve relative profile_path against repo root.

    Accepts file path or directory; for directories, picks the first
    *.json. Returns None if no usable profile is available.
    """
    raw = case.get("profile_path", "")
    if not raw:
        return None
    p = (REPO_ROOT / raw).resolve()
    if not p.exists():
        logger.warning("profile not found for case %s: %s", case.get("case_id"), p)
        return None
    if p.is_dir():
        candidates = sorted(p.glob("*.json"))
        if not candidates:
            logger.warning("no .json in dir for case %s: %s", case.get("case_id"), p)
            return None
        return candidates[0]
    return p


def _evaluate_one_case(case: dict, args: argparse.Namespace) -> dict:
    """Evaluate one golden case. Returns dict suitable for JSON serialization."""
    case_id = case.get("case_id", "<unknown>")
    profile = _resolve_profile(case)

    base_record: dict[str, Any] = {
        "case_id": case_id,
        "profile_path": case.get("profile_path", ""),
        "tags": case["_manifest_entry"].get("tags", []),
        "priority": case["_manifest_entry"].get("priority", ""),
        "workload_type": case.get("workload_type", ""),
        "expected_severity": case.get("expected_severity", ""),
        "must_cover_count": len(case.get("must_cover_issues", [])),
        "forbidden_count": len(case.get("forbidden_claims", [])),
    }

    if profile is None:
        return {**base_record, "skipped_reason": "profile_not_found"}

    # Lazy import: avoid importing core/ unless we actually need it
    try:
        from eval.runner import evaluate_profile  # noqa: WPS433
        from eval.scorers.actionability import (
            aggregate_actionability,
            score_canonical_report_actions,
        )
        from eval.scorers.hallucination import (
            aggregate_hallucination,
            score_canonical_report_hallucination,
        )
        from eval.scorers.evidence_grounding import score_evidence_grounding
        from eval.scorers.failure_taxonomy import score_failure_taxonomy
        from eval.scorers.r4_schema import score_schema
        from eval.scorers.recall import score_canonical_recall, score_recall
        from eval.scorers.rule_echo_in_llm import score_rule_echo
        from eval.scorers.invariants import score_invariants
        from eval.profile_evidence import collect_profile_evidence

        from core.usecases import LLMConfig, PipelineOptions, run_analysis_pipeline
        from core.v6_schema import build_canonical_report
    except ImportError as e:
        return {**base_record, "skipped_reason": f"import_error: {e}"}

    llm_config = LLMConfig(
        databricks_host=args.host,
        databricks_token=args.token,
        primary_model=args.model,
        review_model=args.model,
        refine_model=args.model,
        lang=args.lang,
    )

    options = PipelineOptions(
        skip_llm=args.skip_llm,
        skip_review=True,
        skip_refine=True,
        lang=args.lang,
    )

    try:
        qr = evaluate_profile(
            str(profile),
            llm_config,
            options,
            judge_model=args.judge_model,
            skip_judge=args.skip_judge,
        )
    except Exception as e:  # broad — Week 1 baseline must keep going
        logger.exception("evaluate_profile failed for %s", case_id)
        return {**base_record, "skipped_reason": f"pipeline_error: {e}"}

    # Apply V6 mechanical scorers using golden case metadata
    forbidden = case.get("forbidden_claims", [])
    must_cover = case.get("must_cover_issues", [])

    # Build canonical Report from raw analysis (single pipeline run reused
    # for both schema validation and Action-direct actionability scoring).
    schema_record: dict[str, Any] = {"schema_valid": None}
    canonical: dict[str, Any] = {}
    canonical_text = ""
    alias_hits: dict | None = None  # v6.7.0: filled in the try block below
    try:
        with open(profile, encoding="utf-8") as f:
            data = json.load(f)
        result = run_analysis_pipeline(data, llm_config, options)
        # W3.5 #1 + W4 Day 2: prefer LLM-direct canonical when flag is on
        # and extraction succeeded; fall back to the normalizer adapter.
        # `canonical_source` is recorded so the A/B runner can compute
        # canonical_parse_failure_rate without inferring (Codex W3.5 #5).
        llm_direct = getattr(result, "canonical_report_llm_direct", None)
        canonical_source = "llm_direct" if llm_direct else "normalizer_fallback"
        canonical = llm_direct or build_canonical_report(
            result.analysis,
            llm_text=result.llm_analysis or "",
            language=args.lang,
        )
        # v6.7.0 telemetry: alias_hits is populated by usecases.py when
        # the LLM-direct canonical path ran through enrich_llm_canonical.
        # It is None for the normalizer_fallback / build_canonical_report
        # branch (those don't use the alias map).
        alias_hits = getattr(result, "canonical_alias_hits", None)
        canonical_text = json.dumps(canonical, ensure_ascii=False)
        schema_score = score_schema(canonical)
        schema_record = {
            "schema_valid": schema_score.valid,
            "schema_error_count": schema_score.error_count,
            "schema_findings_count": schema_score.findings_count,
            "schema_actions_count": schema_score.actions_count,
            "schema_appendix_count": schema_score.appendix_count,
            "schema_sampled_issue_ids": schema_score.sampled_issue_ids,
            "schema_top_errors": schema_score.by_path[:3],
        }
    except Exception as e:  # nosec - baseline must continue
        schema_record = {"schema_valid": None, "schema_error_count": -1, "schema_setup_error": str(e)[:200]}

    # Actionability: score canonical Action[] directly (W2.5 #2). W6 Day 3
    # (Codex W5 #3): also compute strict-mode score using profile-known
    # identifiers so the markdown can show lenient vs strict separately.
    from eval.scorers.actionability import (
        score_canonical_report_actions_dual,
    )
    try:
        from core.v6_schema.normalizer import _build_known_metric_names  # noqa
        # Use NodeMetrics table names + scanned tables as known identifiers
        known_idents: set[str] = set()
        for ts in (result.analysis.top_scanned_tables or []):
            if getattr(ts, "table_name", ""):
                known_idents.add(ts.table_name.lower())
        # Fall back to metric names (covers config keys like spark.sql.*)
        known_idents |= _build_known_metric_names(result.analysis)
    except Exception:
        known_idents = set()
    dual = score_canonical_report_actions_dual(
        canonical, profile_known_identifiers=known_idents
    )
    actionability_scores = dual["lenient"]
    actionability_scores_strict = dual["strict"]

    # Hallucination (W2.5 #4 expanded): combine forbidden_claims +
    # ungrounded numeric claims + grounded ratio into a single score that
    # operates over the canonical Report directly.
    if canonical:
        hallucination_scores = [
            score_canonical_report_hallucination(canonical, forbidden_claims=forbidden)
        ]
    else:
        hallucination_scores = []

    # Recall: lenient (text + Finding.issue_id) and strict (issue_id only)
    report_text = (qr.llm_analysis_excerpt or "") + " " + canonical_text
    recall = score_recall(
        report_text,
        qr.card_results,
        must_cover,
        canonical_report=canonical or None,
    )
    strict_recall = score_canonical_recall(canonical or {}, must_cover)

    # Q3 evidence grounding (W3 Day 5)
    if canonical:
        # Build profile-known metric set from analysis if available; fall
        # back to common metric vocabulary when we don't have it here.
        try:
            from core.v6_schema.normalizer import _build_known_metric_names  # noqa
            known_metrics = _build_known_metric_names(result.analysis)  # type: ignore[name-defined]
        except Exception:
            known_metrics = set()
        eg = score_evidence_grounding(canonical, profile_known_metrics=known_metrics)
        eg_record = {
            "evidence_metric_grounding_ratio": eg.metric_grounding_ratio,
            "ungrounded_numeric_ratio": eg.ungrounded_numeric_ratio,
            "valid_source_ratio": eg.valid_source_ratio,
            "valid_knowledge_section_ratio": eg.valid_knowledge_section_ratio,
            "finding_support_ratio": eg.finding_support_ratio,
            "evidence_grounding_composite": eg.composite_score,
        }
    else:
        eg_record = {}

    # Q5 failure taxonomy (W5 Day 5)
    if canonical:
        ft = score_failure_taxonomy(
            canonical,
            must_cover_issues=must_cover,
            # suppression_expected: forbidden_claims that name an issue_id
            # to be suppressed. We extract such ids from forbidden entries
            # whose `id` matches a registered issue_id.
            suppression_expected=[
                claim.get("id") for claim in forbidden if isinstance(claim, dict)
            ],
        )
        ft_record = {
            "failure_taxonomy_score": ft.score,
            "failure_counts": ft.counts,
        }
    else:
        ft_record = {}

    # L1 (2026-04-26): rule_echo_in_llm — detect silent LLM drop of
    # rule-emitted Findings. Combines llm_analysis_excerpt + canonical
    # summary headline as the haystack so the scorer sees the same text
    # the human reads first. When --skip-llm is set the narrative comes
    # entirely from the rule pipeline; in that case we pass empty
    # narrative so the scorer marks the case no_op (rule_echo is only
    # meaningful when an LLM actually wrote prose).
    if canonical:
        if args.skip_llm:
            llm_narrative = ""
        else:
            narrative_parts = [qr.llm_analysis_excerpt or ""]
            summary = canonical.get("summary") or {}
            narrative_parts.append(str(summary.get("headline", "")))
            llm_narrative = " ".join(narrative_parts)
        rule_echo = score_rule_echo(canonical, llm_narrative)
        rule_echo_record = {
            "rule_echo_score": rule_echo.score,
            "rule_echo_total": rule_echo.rule_finding_count,
            "rule_echo_missed": rule_echo.missed_issue_ids,
            "rule_echo_no_op": rule_echo.no_op,
        }

        # L2 (2026-04-26): profile-signature invariants — for every
        # signature derived from the profile, require that some remedy
        # family member is mentioned in canonical Actions or narrative.
        try:
            evidence = collect_profile_evidence(result.analysis)
        except Exception:  # nosec
            evidence = None
        if evidence is not None:
            inv = score_invariants(
                evidence,
                canonical=canonical,
                llm_narrative=llm_narrative,
            )
            invariants_record = {
                "invariants_score": inv.score,
                "invariants_fired": inv.fired_invariants,
                "invariants_satisfied": inv.satisfied_invariants,
                "invariants_violations": [v.invariant_id for v in inv.violations],
                "invariants_no_op": inv.no_op,
            }
        else:
            invariants_record = {}
    else:
        rule_echo_record = {}
        invariants_record = {}

    # W6 Day 2: skeleton distribution per case (Codex W5 #4)
    # Aggregate every action's fix_sql_skeleton metadata so the regression
    # detector can compare distributions across runs.
    skeleton_methods: list[str] = []
    skeleton_compressions: list[float] = []
    if canonical:
        for f in (canonical.get("findings") or []) + (canonical.get("appendix_excluded_findings") or []):
            for a in (f.get("actions") or []):
                method = a.get("fix_sql_skeleton_method")
                if method:
                    skeleton_methods.append(method)
                orig = a.get("fix_sql_chars_original") or 0
                shrunk = a.get("fix_sql_chars_in_prompt") or 0
                if orig > 0:
                    skeleton_compressions.append(round(shrunk / orig, 4))
    skel_record = {
        "skeleton_methods": skeleton_methods,
        "skeleton_compression_ratios": skeleton_compressions,
    }

    return {
        **base_record,
        "num_action_cards": qr.num_action_cards,
        "l1_syntax_pass_rate": qr.l1_syntax_pass_rate,
        "l1_serverless_pass_rate": qr.l1_serverless_pass_rate,
        "l2_avg_grounding": qr.l2_avg_grounding,
        "l3_avg_diagnosis": qr.l3_avg_diagnosis,
        "l4_avg_relevance": qr.l4_avg_relevance,
        "l4_avg_feasibility": qr.l4_avg_feasibility,
        "actionability_specific_ratio": aggregate_actionability(actionability_scores),
        "actionability_strict_ratio": aggregate_actionability(actionability_scores_strict),
        "hallucination_score_avg": aggregate_hallucination(hallucination_scores),
        "recall_ratio": recall.recall_ratio,
        "recall_missed": recall.missed_issues,
        "recall_strict_ratio": strict_recall.recall_ratio,
        "recall_strict_missed": strict_recall.missed_issues,
        "expected_recall_min": case.get("expected_recall_min", 0.0),
        "expected_l3_min": case.get("expected_l3_min", 0),
        "pipeline_error": qr.pipeline_error,
        "canonical_source": canonical_source if canonical else "missing",
        # v6.7.0: alias-hit counts (None when LLM-direct path didn't run).
        "alias_hits": alias_hits,
        # V6.2 Tier 1: canonical Report dict travels with the record so
        # ``_write_baseline_json`` can split it into the sidecar. Layer B
        # then re-attaches via ``_load_baseline_with_canonical`` and
        # judges the actual content rather than a placeholder.
        "canonical_report": canonical or None,
        **schema_record,
        **eg_record,
        **ft_record,
        **skel_record,
        **rule_echo_record,
        **invariants_record,
    }


# V6.2 Tier 1: canonical Report sidecar persistence (Codex 2026-04-26).
#
# Each baseline JSON is paired with a sidecar
# ``<basename>_canonical_reports.json`` that holds the per-case
# canonical Report dicts keyed by case_id. The main baseline keeps only
# ``canonical_report_ref`` + ``has_canonical_report`` so summary diffs
# stay readable; Layer B re-attaches the canonical via
# ``_load_baseline_with_canonical`` before invoking the LLM judge.
#
# Sidecar format:
#   {
#     "schema_version": 1,
#     "cases": { "<case_id>": <canonical_report_dict>, ... }
#   }


_CANONICAL_SIDECAR_SCHEMA_VERSION = 1


def _sidecar_path_for_baseline(baseline_path: Path) -> Path:
    """Return the sidecar JSON path for a given baseline JSON."""
    stem = baseline_path.stem  # strips a single ``.json`` extension
    return baseline_path.parent / f"{stem}_canonical_reports.json"


def _split_canonical_reports(
    records: list[dict],
) -> tuple[list[dict], dict[str, dict]]:
    """Pop ``canonical_report`` off each record and collect into a map.

    Records are mutated in-place to carry only the lightweight ref +
    flag — see module docstring for shape. Records that lack a
    canonical (skipped or pipeline error) get
    ``has_canonical_report=False`` so consumers can distinguish "no
    canonical produced" from "lookup miss".

    Duplicate case_ids: the first canonical for a given case_id wins
    and any subsequent occurrences are logged as warnings but NOT
    overwritten. Codex 2026-04-26 flagged the silent-overwrite path
    as a corruption risk worth catching at write time.
    """
    canonical_map: dict[str, dict] = {}
    out: list[dict] = []
    for rec in records:
        new_rec = dict(rec)
        canonical = new_rec.pop("canonical_report", None)
        case_id = new_rec.get("case_id") or ""
        if canonical and case_id:
            if case_id in canonical_map:
                logger.warning(
                    "Duplicate case_id %r encountered while writing baseline; "
                    "keeping the first canonical and dropping subsequent ones",
                    case_id,
                )
            else:
                canonical_map[case_id] = canonical
            new_rec["canonical_report_ref"] = case_id
            new_rec["has_canonical_report"] = True
        else:
            new_rec["has_canonical_report"] = False
        out.append(new_rec)
    return out, canonical_map


def _write_baseline_json(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    main_records, canonical_map = _split_canonical_reports(records)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "num_cases": len(main_records),
                "cases": main_records,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )

    # Sidecar — sort_keys for byte-stable diffs (Codex 2026-04-26).
    # No timestamp in the sidecar so unchanged input → unchanged bytes.
    sidecar_path = _sidecar_path_for_baseline(path)
    with open(sidecar_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "schema_version": _CANONICAL_SIDECAR_SCHEMA_VERSION,
                "cases": canonical_map,
            },
            f,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )


def _load_baseline_with_canonical(path: Path) -> dict:
    """Load a baseline JSON and re-attach canonical Reports from the
    sidecar onto each case record.

    Returns the baseline payload extended with:
      - ``cases[i].canonical_report`` — dict or None
      - ``sidecar_present`` — bool
      - ``sidecar_schema_version`` — int or None

    Old baselines without a sidecar yield ``sidecar_present=False`` and
    every case's ``canonical_report`` is None. Layer B uses this signal
    to record an explicit skip reason instead of silently no-op'ing.
    """
    with open(path, encoding="utf-8") as f:
        payload = json.load(f)

    sidecar_path = _sidecar_path_for_baseline(path)
    sidecar_present = sidecar_path.exists()
    canonical_map: dict[str, dict] = {}
    sidecar_schema_version: int | None = None
    if sidecar_present:
        with open(sidecar_path, encoding="utf-8") as f:
            sidecar_payload = json.load(f)
        canonical_map = sidecar_payload.get("cases") or {}
        sidecar_schema_version = sidecar_payload.get("schema_version")

    for case in payload.get("cases", []):
        ref = case.get("canonical_report_ref") or case.get("case_id")
        case["canonical_report"] = canonical_map.get(ref) if ref else None

    payload["sidecar_present"] = sidecar_present
    payload["sidecar_schema_version"] = sidecar_schema_version
    return payload


def _write_report_md(records: list[dict], path: Path, name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    evaluated = [r for r in records if "skipped_reason" not in r]
    skipped = [r for r in records if "skipped_reason" in r]

    def _avg(field: str) -> float:
        vals = [r.get(field, 0.0) or 0.0 for r in evaluated]
        return sum(vals) / len(vals) if vals else 0.0

    def _avg_skip_no_op(records: list[dict], field: str, no_op_field: str) -> float:
        """Average ``field`` across records where ``no_op_field`` is False.

        Used by rule_echo where cases without enforceable Findings should
        not pollute the mean (vacuous 1.0 hides real-world drops).
        """
        vals = [
            r.get(field, 0.0) or 0.0 for r in records
            if not r.get(no_op_field, False)
        ]
        return sum(vals) / len(vals) if vals else 1.0

    schema_pass_rate = (
        sum(1 for r in evaluated if r.get("schema_valid") is True) / len(evaluated)
        if evaluated else 1.0
    )

    lines = [
        f"# {name} — V6 Quality Baseline",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        f"Cases: {len(records)} ({len(evaluated)} evaluated, {len(skipped)} skipped)",
        "",
        "## Summary",
        "",
        "| Metric | Avg |",
        "|--------|----:|",
        f"| L1 syntax pass rate | {_avg('l1_syntax_pass_rate'):.2%} |",
        f"| L2 evidence grounding | {_avg('l2_avg_grounding'):.2%} |",
        f"| L3 diagnosis (1-5) | {_avg('l3_avg_diagnosis'):.2f} |",
        f"| L4 fix relevance (1-5) | {_avg('l4_avg_relevance'):.2f} |",
        f"| Actionability specific (lenient) | {_avg('actionability_specific_ratio'):.2%} |",
        f"| Actionability specific (strict, profile-grounded) | {_avg('actionability_strict_ratio'):.2%} |",
        f"| Hallucination clean score | {_avg('hallucination_score_avg'):.2f} |",
        f"| Critical issue recall (lenient) | {_avg('recall_ratio'):.2%} |",
        f"| Critical issue recall (strict, canonical-only) | {_avg('recall_strict_ratio'):.2%} |",
        f"| **R4 schema pass rate** | {schema_pass_rate:.2%} |",
        f"| **Q3 evidence grounding (composite)** | {_avg('evidence_grounding_composite'):.2%} |",
        f"| Q3 — metric grounded ratio | {_avg('evidence_metric_grounding_ratio'):.2%} |",
        f"| Q3 — valid source taxonomy | {_avg('valid_source_ratio'):.2%} |",
        f"| Q3 — valid knowledge_id ratio | {_avg('valid_knowledge_section_ratio'):.2%} |",
        f"| Q3 — finding has grounded support | {_avg('finding_support_ratio'):.2%} |",
        f"| Q3 — ungrounded numeric ratio (lower better) | {_avg('ungrounded_numeric_ratio'):.2%} |",
        f"| **Q5 failure taxonomy (avg, higher better)** | {_avg('failure_taxonomy_score'):.2%} |",
        f"| **L1 rule_echo_in_llm (avg, higher better)** | {_avg_skip_no_op(evaluated, 'rule_echo_score', 'rule_echo_no_op'):.2%} |",
        f"| **L2 invariants (avg, higher better)** | {_avg_skip_no_op(evaluated, 'invariants_score', 'invariants_no_op'):.2%} |",
        "",
        "## Per-case Results",
        "",
        "| case_id | priority | schema | recall | actionability | l3_diag | l4_rel | issues |",
        "|---------|----------|:------:|-------:|--------------:|--------:|-------:|--------|",
    ]
    for r in records:
        if "skipped_reason" in r:
            lines.append(
                f"| {r['case_id']} | {r.get('priority','')} | — | — | — | — | — | "
                f"_skipped: {r['skipped_reason']}_ |"
            )
            continue
        sv = r.get("schema_valid")
        schema_cell = "✅" if sv is True else ("❌" if sv is False else "—")
        lines.append(
            f"| {r['case_id']} | {r.get('priority','')} | "
            f"{schema_cell} | "
            f"{r.get('recall_ratio', 0):.2%} | "
            f"{r.get('actionability_specific_ratio', 0):.2%} | "
            f"{r.get('l3_avg_diagnosis', 0):.2f} | "
            f"{r.get('l4_avg_relevance', 0):.2f} | "
            f"{', '.join(r.get('recall_missed', []) or []) or '-'} |"
        )

    # Schema violation details (top 5 cases by error count)
    schema_violations = sorted(
        [r for r in evaluated if r.get("schema_valid") is False],
        key=lambda x: -(x.get("schema_error_count") or 0),
    )[:5]
    if schema_violations:
        lines += ["", "## Schema Violations (top 5)", ""]
        for r in schema_violations:
            lines.append(
                f"### {r['case_id']} — {r.get('schema_error_count', 0)} errors"
            )
            for path, msg in r.get("schema_top_errors", []) or []:
                lines.append(f"- `{path}`: {msg}")
            lines.append("")

    if skipped:
        lines += ["", "## Skipped Cases", ""]
        for r in skipped:
            lines.append(f"- **{r['case_id']}**: {r['skipped_reason']}")

    lines += [
        "",
        "## Notes",
        "",
        "- Mechanical scorers (L1, actionability, hallucination, recall) run without LLM keys.",
        "- L2/L3/L4 require DATABRICKS_HOST/TOKEN unless `--skip-judge` is passed.",
        "- skipped_reason=`profile_not_found` means the referenced JSON in `json/` is unavailable in this checkout.",
        "",
    ]

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="goldens_runner",
        description="V6 quality baseline runner over goldens manifest.",
    )
    parser.add_argument("--manifest", default="eval/goldens/manifest.yaml")
    parser.add_argument("--baseline-name", default="v6_baseline")
    parser.add_argument("--out-dir", default="eval/baselines")
    parser.add_argument("--report-dir", default="eval/reports")
    parser.add_argument("--model", default=os.environ.get("EVAL_MODEL", "databricks-claude-sonnet-4-6"))
    parser.add_argument("--judge-model", default=os.environ.get("EVAL_JUDGE_MODEL", "databricks-claude-sonnet-4-6"))
    parser.add_argument("--host", default=os.environ.get("DATABRICKS_HOST", ""))
    parser.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN", ""))
    parser.add_argument("--lang", default="ja", choices=["en", "ja"])
    parser.add_argument("--skip-judge", action="store_true")
    parser.add_argument("--skip-llm", action="store_true",
                        help="skip analysis LLM (rule-based only); useful for sanity check")
    parser.add_argument("--limit", type=int, default=None,
                        help="evaluate at most N cases (debug)")
    parser.add_argument("--tag", default=None,
                        help="evaluate only cases with this tag")
    # W2.5 #10: CI gate. exit 1 when any threshold is breached.
    parser.add_argument("--gate-schema-pass", type=float, default=None,
                        help="fail if R4 schema pass rate < this value (0..1)")
    parser.add_argument("--gate-recall-strict", type=float, default=None,
                        help="fail if strict critical-recall < this value (0..1)")
    parser.add_argument("--gate-actionability", type=float, default=None,
                        help="fail if actionability specific ratio < this value (0..1)")
    parser.add_argument("--gate-hallucination", type=float, default=None,
                        help="fail if avg hallucination clean score < this value (0..1)")
    parser.add_argument("--gate-evidence-grounding", type=float, default=None,
                        help="fail if avg Q3 evidence grounding composite < this value (0..1)")
    # W3.5 #3: per-signal Q3 gates
    parser.add_argument("--gate-finding-support", type=float, default=None,
                        help="fail if avg Q3 finding_support_ratio < this value (0..1)")
    parser.add_argument("--gate-metric-grounded", type=float, default=None,
                        help="fail if avg Q3 metric_grounding_ratio < this value (0..1)")
    parser.add_argument("--gate-ungrounded-numeric-max", type=float, default=None,
                        help="fail if avg Q3 ungrounded_numeric_ratio > this value (0..1)")
    parser.add_argument("--gate-valid-source", type=float, default=None,
                        help="fail if avg Q3 valid_source_ratio < this value (0..1)")
    parser.add_argument("--gate-valid-knowledge-id", type=float, default=None,
                        help="fail if avg Q3 valid_knowledge_section_ratio < this value (0..1)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)

    manifest_path = (REPO_ROOT / args.manifest).resolve()
    manifest, cases = _load_manifest(manifest_path)
    logger.info("loaded %d cases from %s", len(cases), manifest_path)

    if args.tag:
        cases = [c for c in cases if args.tag in c["_manifest_entry"].get("tags", [])]
        logger.info("filtered to %d cases by tag=%s", len(cases), args.tag)
    if args.limit is not None:
        cases = cases[: args.limit]
        logger.info("limited to first %d", len(cases))

    records = []
    for i, case in enumerate(cases, 1):
        logger.info("[%d/%d] %s", i, len(cases), case.get("case_id"))
        records.append(_evaluate_one_case(case, args))

    out_json = REPO_ROOT / args.out_dir / f"{args.baseline_name}.json"
    out_md = REPO_ROOT / args.report_dir / f"{args.baseline_name}.md"
    _write_baseline_json(records, out_json)
    _write_report_md(records, out_md, args.baseline_name)

    logger.info("baseline written: %s", out_json)
    logger.info("report written:   %s", out_md)

    skipped = sum(1 for r in records if "skipped_reason" in r)
    logger.info("done. evaluated=%d skipped=%d", len(records) - skipped, skipped)

    # ----- W2.5 #10: CI gate -----
    evaluated = [r for r in records if "skipped_reason" not in r]
    if not evaluated:
        return 0
    gate_violations: list[str] = []

    def _avg(field: str) -> float:
        vals = [r.get(field, 0.0) or 0.0 for r in evaluated]
        return sum(vals) / len(vals) if vals else 0.0

    schema_pass = (
        sum(1 for r in evaluated if r.get("schema_valid") is True) / len(evaluated)
    )
    recall_strict_avg = _avg("recall_strict_ratio")
    actionability_avg = _avg("actionability_specific_ratio")
    hallucination_avg = _avg("hallucination_score_avg")

    if args.gate_schema_pass is not None and schema_pass < args.gate_schema_pass:
        gate_violations.append(
            f"R4 schema pass rate {schema_pass:.2%} < gate {args.gate_schema_pass:.2%}"
        )
    if args.gate_recall_strict is not None and recall_strict_avg < args.gate_recall_strict:
        gate_violations.append(
            f"Critical recall (strict) {recall_strict_avg:.2%} < gate {args.gate_recall_strict:.2%}"
        )
    if args.gate_actionability is not None and actionability_avg < args.gate_actionability:
        gate_violations.append(
            f"Actionability {actionability_avg:.2%} < gate {args.gate_actionability:.2%}"
        )
    if args.gate_hallucination is not None and hallucination_avg < args.gate_hallucination:
        gate_violations.append(
            f"Hallucination clean score {hallucination_avg:.2f} < gate {args.gate_hallucination:.2f}"
        )
    eg_avg = _avg("evidence_grounding_composite")
    if args.gate_evidence_grounding is not None and eg_avg < args.gate_evidence_grounding:
        gate_violations.append(
            f"Evidence grounding composite {eg_avg:.2%} < gate {args.gate_evidence_grounding:.2%}"
        )

    # W3.5 #3 per-signal Q3 gates
    fs_avg = _avg("finding_support_ratio")
    if args.gate_finding_support is not None and fs_avg < args.gate_finding_support:
        gate_violations.append(
            f"Q3 finding_support {fs_avg:.2%} < gate {args.gate_finding_support:.2%}"
        )
    mg_avg = _avg("evidence_metric_grounding_ratio")
    if args.gate_metric_grounded is not None and mg_avg < args.gate_metric_grounded:
        gate_violations.append(
            f"Q3 metric_grounded {mg_avg:.2%} < gate {args.gate_metric_grounded:.2%}"
        )
    un_avg = _avg("ungrounded_numeric_ratio")
    if args.gate_ungrounded_numeric_max is not None and un_avg > args.gate_ungrounded_numeric_max:
        gate_violations.append(
            f"Q3 ungrounded_numeric {un_avg:.2%} > gate {args.gate_ungrounded_numeric_max:.2%}"
        )
    vs_avg = _avg("valid_source_ratio")
    if args.gate_valid_source is not None and vs_avg < args.gate_valid_source:
        gate_violations.append(
            f"Q3 valid_source {vs_avg:.2%} < gate {args.gate_valid_source:.2%}"
        )
    vk_avg = _avg("valid_knowledge_section_ratio")
    if args.gate_valid_knowledge_id is not None and vk_avg < args.gate_valid_knowledge_id:
        gate_violations.append(
            f"Q3 valid_knowledge_id {vk_avg:.2%} < gate {args.gate_valid_knowledge_id:.2%}"
        )

    if gate_violations:
        logger.error("CI gate FAILED:")
        for v in gate_violations:
            logger.error("  - %s", v)
        return 1
    if any(g is not None for g in [
        args.gate_schema_pass,
        args.gate_recall_strict,
        args.gate_actionability,
        args.gate_hallucination,
        args.gate_evidence_grounding,
        args.gate_finding_support,
        args.gate_metric_grounded,
        args.gate_ungrounded_numeric_max,
        args.gate_valid_source,
        args.gate_valid_knowledge_id,
    ]):
        logger.info("CI gate passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
