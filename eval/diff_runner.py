"""Diff runner: compare eval results between git refs using worktrees."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from core.usecases import LLMConfig, PipelineOptions

from .diff_judge import DiffReport, DiffVerdict, judge_diff
from .models import EvalReport, QueryEvalResult, CardEvalResult, L1Score, L2Score, L3Score, L4Score
from .runner import evaluate_profiles

logger = logging.getLogger(__name__)


def run_diff(
    profile_paths: list[str],
    git_ref: str,
    llm_config: LLMConfig,
    options: PipelineOptions,
    judge_model: str = "databricks-claude-sonnet-4-6",
    skip_judge: bool = False,
    repo_root: Path | None = None,
) -> DiffReport:
    """Run eval on baseline and current code, then LLM-judge the diff.

    git_ref can be:
    - A path to a JSON file (previously saved eval result)
    - A git ref (tag/branch/SHA) — uses worktree to run baseline eval

    Steps:
    1. Load or generate baseline results
    2. Run eval in-process with current code
    3. Pair results by profile and judge each pair
    """
    if repo_root is None:
        repo_root = Path(__file__).resolve().parent.parent

    # Ensure absolute paths for profiles
    abs_paths = [str(Path(p).resolve()) for p in profile_paths]

    # Step 1: Get baseline results
    baseline_path = Path(git_ref)
    if baseline_path.is_file() and baseline_path.suffix == ".json":
        # Load from saved JSON
        logger.info("Loading baseline from %s...", git_ref)
        baseline = _report_from_json(baseline_path.read_text(encoding="utf-8"))
    else:
        # Run eval in worktree at git ref
        logger.info("Running baseline eval at %s...", git_ref)
        baseline = _run_baseline_in_worktree(
            abs_paths, git_ref, llm_config, options, judge_model, skip_judge, repo_root,
        )

    # Step 3: Run current eval in-process
    logger.info("Running current eval...")
    current = evaluate_profiles(
        abs_paths, llm_config, options,
        judge_model=judge_model, skip_judge=skip_judge,
    )

    # Step 4: Pair and judge
    verdicts = _pair_and_judge(
        baseline, current,
        llm_config.databricks_host, llm_config.databricks_token,
        judge_model, skip_judge,
    )

    # Build summary
    counts = {"improved": 0, "regressed": 0, "unchanged": 0, "error": 0}
    for v in verdicts:
        counts[v.verdict] = counts.get(v.verdict, 0) + 1
    summary_parts = []
    if counts["improved"]:
        summary_parts.append(f"{counts['improved']} improved")
    if counts["regressed"]:
        summary_parts.append(f"{counts['regressed']} regressed")
    if counts["unchanged"]:
        summary_parts.append(f"{counts['unchanged']} unchanged")
    if counts["error"]:
        summary_parts.append(f"{counts['error']} error(s)")

    return DiffReport(
        timestamp=datetime.now(timezone.utc).isoformat(),
        git_ref=git_ref,
        num_profiles=len(abs_paths),
        verdicts=verdicts,
        summary=", ".join(summary_parts),
        config={
            "primary_model": llm_config.primary_model,
            "judge_model": judge_model,
            "baseline_ref": git_ref,
        },
    )


# Inline script that runs ONLY the analysis pipeline (core/) in the worktree.
# No eval/ imports — just core.usecases.run_analysis_pipeline → ActionCards as JSON.
# This avoids import compatibility issues between old core/ and current eval/.
_PIPELINE_ONLY_SCRIPT = """\
import sys, json, os
sys.path.insert(0, {worktree_app!r})
os.environ.setdefault("DATABRICKS_HOST", {host!r})
os.environ.setdefault("DATABRICKS_TOKEN", {token!r})

from core.usecases import LLMConfig, PipelineOptions, run_analysis_pipeline

profiles = json.loads(sys.stdin.read())
results = []
for path in profiles:
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        llm_config = LLMConfig(
            primary_model={model!r},
            review_model={model!r},
            refine_model={model!r},
            databricks_host={host!r},
            databricks_token={token!r},
            lang={lang!r},
        )
        options = PipelineOptions(skip_llm=False, lang={lang!r})
        result = run_analysis_pipeline(data, llm_config, options)
        a = result.analysis
        cards = []
        for c in a.action_cards:
            cards.append({{
                "problem": c.problem,
                "evidence": c.evidence,
                "likely_cause": c.likely_cause,
                "fix": c.fix,
                "fix_sql": c.fix_sql,
                "expected_impact": c.expected_impact,
                "effort": c.effort,
                "priority_score": c.priority_score,
            }})
        qm = a.query_metrics
        bi = a.bottleneck_indicators
        results.append({{
            "profile_path": path,
            "query_id": qm.query_id or "",
            "query_sql": qm.query_text or "",
            "is_serverless": getattr(qm, "query_typename", "") == "LakehouseSqlQuery",
            "action_cards": cards,
            "metrics": {{
                "total_time_ms": qm.total_time_ms,
                "execution_time_ms": qm.execution_time_ms,
                "read_bytes": qm.read_bytes,
                "cache_hit_ratio": bi.cache_hit_ratio,
                "photon_ratio": bi.photon_ratio,
                "shuffle_impact_ratio": bi.shuffle_impact_ratio,
                "spill_bytes": bi.spill_bytes,
            }},
            "error": "",
        }})
    except Exception as e:
        results.append({{"profile_path": path, "error": str(e), "action_cards": []}})

print(json.dumps(results))
"""


def _run_baseline_in_worktree(
    profile_paths: list[str],
    git_ref: str,
    llm_config: LLMConfig,
    options: PipelineOptions,
    judge_model: str,
    skip_judge: bool,
    repo_root: Path,
) -> EvalReport:
    """Create a worktree at git_ref, run analysis pipeline only, then score with current eval/.

    The worktree subprocess runs ONLY core/ (analysis pipeline) and outputs
    ActionCards as JSON. The current eval/ scorers then evaluate those cards.
    This avoids import compatibility issues between old and new core/ versions.
    """
    tmp_dir = tempfile.TemporaryDirectory(prefix="eval_baseline_")
    worktree_dir = tmp_dir.name

    try:
        # Reject option-like refs to prevent git option injection
        if git_ref.startswith("-"):
            raise ValueError(f"Invalid git ref (starts with '-'): {git_ref}")

        # Create worktree
        try:
            subprocess.run(
                ["git", "worktree", "add", worktree_dir, "--", git_ref],
                cwd=str(repo_root), check=True, capture_output=True, text=True,
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"git worktree add failed for ref '{git_ref}': {e.stderr[:200]}"
            ) from e

        worktree_app = str(Path(worktree_dir) / "dabs" / "app")
        logger.info("Baseline worktree: %s (core/ from %s)", worktree_dir, git_ref)

        script = _PIPELINE_ONLY_SCRIPT.format(
            worktree_app=worktree_app,
            model=llm_config.primary_model,
            host=llm_config.databricks_host,
            token=llm_config.databricks_token,
            lang=options.lang,
        )

        # Run pipeline-only script with constrained env (avoid leaking parent env)
        import os
        clean_env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": os.environ.get("HOME", ""),
            "LANG": os.environ.get("LANG", "en_US.UTF-8"),
            "DATABRICKS_HOST": llm_config.databricks_host,
            "DATABRICKS_TOKEN": llm_config.databricks_token,
            # SSL certs needed for HTTPS calls
            "SSL_CERT_FILE": os.environ.get("SSL_CERT_FILE", ""),
            "REQUESTS_CA_BUNDLE": os.environ.get("REQUESTS_CA_BUNDLE", ""),
            "CURL_CA_BUNDLE": os.environ.get("CURL_CA_BUNDLE", ""),
        }
        # Remove empty values
        clean_env = {k: v for k, v in clean_env.items() if v}

        result = subprocess.run(
            [sys.executable, "-c", script],
            input=json.dumps(profile_paths),
            capture_output=True, text=True, timeout=1200,
            env=clean_env,
        )

        if result.returncode != 0:
            logger.error("Baseline pipeline failed (exit %d): %s", result.returncode, result.stderr[:500])
            return EvalReport(
                timestamp=datetime.now(timezone.utc).isoformat(),
                config={"error": f"Baseline pipeline failed: {result.stderr[:200]}"},
            )

        # Score the pipeline output with current eval scorers
        return _score_pipeline_output(
            result.stdout, llm_config, judge_model, skip_judge,
        )

    except subprocess.TimeoutExpired as te:
        stderr_tail = (te.stderr or "")[-500:] if te.stderr else ""
        stdout_tail = (te.stdout or "")[-200:] if te.stdout else ""
        logger.error("Baseline eval timed out. stderr: %s", stderr_tail)
        if stdout_tail:
            logger.error("Baseline stdout tail: %s", stdout_tail)
        return EvalReport(config={"error": f"Baseline eval timed out: {stderr_tail[:200]}"})
    except (RuntimeError, ValueError, OSError, json.JSONDecodeError) as e:
        logger.error("Worktree eval failed: %s", e)
        return EvalReport(config={"error": str(e)})
    finally:
        try:
            subprocess.run(
                ["git", "worktree", "remove", "--force", worktree_dir],
                cwd=str(repo_root), capture_output=True, text=True, timeout=30,
            )
        except Exception:
            logger.warning("Failed to remove worktree %s", worktree_dir)
        finally:
            tmp_dir.cleanup()


def _score_pipeline_output(
    json_output: str,
    llm_config: LLMConfig,
    judge_model: str,
    skip_judge: bool,
) -> EvalReport:
    """Score ActionCards from worktree pipeline output using current eval scorers."""
    from .scorers.l1_syntax import score_l1
    from .scorers.l2_evidence import score_l2
    from .scorers.l3l4_judge import score_l3l4, build_profile_summary
    from .runner import _aggregate_query_result, _aggregate_report

    try:
        pipeline_results = json.loads(json_output)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse pipeline output: %s", e)
        return EvalReport(config={"error": f"JSON parse error: {e}"})

    query_results: list[QueryEvalResult] = []

    for pr in pipeline_results:
        profile_path = pr.get("profile_path", "")

        if pr.get("error"):
            query_results.append(QueryEvalResult(
                profile_path=profile_path,
                pipeline_error=pr["error"],
            ))
            continue

        is_serverless = pr.get("is_serverless", False)
        query_sql = pr.get("query_sql", "")

        # Build a simple profile summary from metrics
        metrics = pr.get("metrics", {})
        summary_lines = []
        if metrics.get("total_time_ms"):
            summary_lines.append(f"Total time: {metrics['total_time_ms']}ms")
        if metrics.get("cache_hit_ratio"):
            summary_lines.append(f"Cache hit ratio: {metrics['cache_hit_ratio']:.1%}")
        if metrics.get("photon_ratio"):
            summary_lines.append(f"Photon utilization: {metrics['photon_ratio']:.1%}")
        if metrics.get("spill_bytes"):
            gb = metrics["spill_bytes"] / (1024**3)
            summary_lines.append(f"Spill bytes: {gb:.2f} GB")
        profile_summary = "\n".join(summary_lines) or "No metrics available"

        # Create lightweight card objects for scoring
        card_results: list[CardEvalResult] = []
        for i, card_d in enumerate(pr.get("action_cards", [])):
            # Use a SimpleNamespace as a duck-typed ActionCard
            from types import SimpleNamespace
            card = SimpleNamespace(
                problem=card_d.get("problem", ""),
                evidence=card_d.get("evidence", []),
                likely_cause=card_d.get("likely_cause", ""),
                fix=card_d.get("fix", ""),
                fix_sql=card_d.get("fix_sql", ""),
                expected_impact=card_d.get("expected_impact", ""),
                effort=card_d.get("effort", ""),
                priority_score=card_d.get("priority_score", 0),
            )

            l1 = score_l1(card, is_serverless=is_serverless)
            l1.card_index = i

            # L2 needs profile_data and analysis — mark as unavailable for worktree baseline
            # Use grounding_ratio=-1.0 as sentinel to indicate "not evaluated"
            l2 = L2Score(card_index=i, evidence_count=len(card.evidence),
                         grounded_count=0, grounding_ratio=-1.0)

            l3, l4 = None, None
            if not skip_judge and llm_config.is_available:
                l3, l4 = score_l3l4(
                    card, profile_summary, query_sql,
                    llm_config.databricks_host, llm_config.databricks_token,
                    judge_model=judge_model,
                )
                l3.card_index = i
                l4.card_index = i

            card_results.append(CardEvalResult(
                card_index=i,
                problem=card.problem,
                expected_impact=card.expected_impact,
                effort=card.effort,
                l1=l1, l2=l2, l3=l3, l4=l4,
            ))

        qr = _aggregate_query_result(
            query_id=pr.get("query_id", ""),
            profile_path=profile_path,
            card_results=card_results,
            primary_model=llm_config.primary_model,
            llm_text="",
        )
        query_results.append(qr)

    return _aggregate_report(query_results, llm_config, judge_model)


def _pair_and_judge(
    baseline: EvalReport,
    current: EvalReport,
    host: str,
    token: str,
    judge_model: str,
    skip_judge: bool,
) -> list[DiffVerdict]:
    """Pair baseline/current results by profile and judge each pair."""
    # Index baseline by resolved path, falling back to basename for cross-machine compatibility
    baseline_map: dict[str, QueryEvalResult] = {}
    baseline_by_name: dict[str, QueryEvalResult] = {}
    for qr in baseline.query_results:
        baseline_map[str(Path(qr.profile_path).resolve())] = qr
        baseline_by_name[Path(qr.profile_path).name] = qr

    verdicts: list[DiffVerdict] = []
    for qr in current.query_results:
        resolved = str(Path(qr.profile_path).resolve())
        base_qr = baseline_map.get(resolved) or baseline_by_name.get(Path(qr.profile_path).name)

        if base_qr is None:
            # Check if baseline had a global error (e.g., worktree timeout)
            has_baseline_error = baseline.config.get("error", "")
            verdicts.append(DiffVerdict(
                profile_path=qr.profile_path,
                verdict="error" if has_baseline_error else "unchanged",
                reasoning=f"Baseline error: {has_baseline_error}" if has_baseline_error else "No baseline result for this profile",
                current_card_count=qr.num_action_cards,
            ))
            continue

        if qr.pipeline_error or base_qr.pipeline_error:
            verdicts.append(DiffVerdict(
                profile_path=qr.profile_path,
                verdict="error",
                reasoning=f"Pipeline error — baseline: {base_qr.pipeline_error or 'ok'}, current: {qr.pipeline_error or 'ok'}",
                baseline_card_count=base_qr.num_action_cards,
                current_card_count=qr.num_action_cards,
            ))
            continue

        if skip_judge:
            # Numeric-only comparison without LLM
            verdicts.append(_numeric_verdict(base_qr, qr))
        else:
            logger.info("Judging diff for %s...", key)
            verdict = judge_diff(base_qr, qr, host, token, judge_model)
            verdicts.append(verdict)

    return verdicts


def _numeric_verdict(baseline: QueryEvalResult, current: QueryEvalResult) -> DiffVerdict:
    """Simple numeric comparison when --no-judge is set."""
    improvements = 0
    regressions = 0

    pairs = [
        (current.l1_syntax_pass_rate, baseline.l1_syntax_pass_rate),
    ]
    # Only compare L2 if both sides were actually evaluated (not sentinel -1.0)
    if baseline.l2_avg_grounding >= 0 and current.l2_avg_grounding >= 0:
        pairs.append((current.l2_avg_grounding, baseline.l2_avg_grounding))

    for curr_val, base_val in pairs:
        if curr_val > base_val + 0.05:
            improvements += 1
        elif curr_val < base_val - 0.05:
            regressions += 1

    if improvements > regressions:
        verdict = "improved"
    elif regressions > improvements:
        verdict = "regressed"
    else:
        verdict = "unchanged"

    return DiffVerdict(
        profile_path=current.profile_path,
        verdict=verdict,
        reasoning=(
            f"L1: {baseline.l1_syntax_pass_rate:.0%}→{current.l1_syntax_pass_rate:.0%}"
            + (f", L2: {baseline.l2_avg_grounding:.0%}→{current.l2_avg_grounding:.0%}"
               if baseline.l2_avg_grounding >= 0 and current.l2_avg_grounding >= 0
               else ", L2: n/a (baseline not evaluated)")
        ),
        baseline_card_count=baseline.num_action_cards,
        current_card_count=current.num_action_cards,
    )


def _report_from_json(json_str: str) -> EvalReport:
    """Deserialize EvalReport from JSON string."""
    try:
        d = json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse baseline JSON: %s", e)
        return EvalReport(config={"error": f"JSON parse error: {e}"})

    query_results = []
    for qr_d in d.get("query_results", []):
        card_results = []
        for cr_d in qr_d.get("card_results", []):
            l1_d = cr_d.get("l1", {})
            l2_d = cr_d.get("l2", {})
            l3_d = cr_d.get("l3")
            l4_d = cr_d.get("l4")

            card_results.append(CardEvalResult(
                card_index=cr_d.get("card_index", 0),
                problem=cr_d.get("problem", ""),
                expected_impact=cr_d.get("expected_impact", ""),
                effort=cr_d.get("effort", ""),
                l1=L1Score(
                    card_index=l1_d.get("card_index", 0),
                    has_fix_sql=l1_d.get("has_fix_sql", False),
                    parses_ok=l1_d.get("parses_ok", True),
                    parse_error=l1_d.get("parse_error", ""),
                    serverless_compliant=l1_d.get("serverless_compliant", True),
                    unsupported_configs=l1_d.get("unsupported_configs", []),
                ),
                l2=L2Score(
                    card_index=l2_d.get("card_index", 0),
                    evidence_count=l2_d.get("evidence_count", 0),
                    grounded_count=l2_d.get("grounded_count", 0),
                    ungrounded_evidence=l2_d.get("ungrounded_evidence", []),
                    grounding_ratio=l2_d.get("grounding_ratio", 1.0),
                ),
                l3=L3Score(
                    card_index=l3_d.get("card_index", 0),
                    diagnosis_score=l3_d.get("diagnosis_score", 0),
                    evidence_quality=l3_d.get("evidence_quality", 0),
                    reasoning=l3_d.get("reasoning", ""),
                ) if l3_d else None,
                l4=L4Score(
                    card_index=l4_d.get("card_index", 0),
                    fix_relevance=l4_d.get("fix_relevance", 0),
                    fix_feasibility=l4_d.get("fix_feasibility", 0),
                    expected_improvement=l4_d.get("expected_improvement", 0),
                    reasoning=l4_d.get("reasoning", ""),
                ) if l4_d else None,
            ))

        query_results.append(QueryEvalResult(
            query_id=qr_d.get("query_id", ""),
            profile_path=qr_d.get("profile_path", ""),
            num_action_cards=qr_d.get("num_action_cards", 0),
            card_results=card_results,
            l1_syntax_pass_rate=qr_d.get("l1_syntax_pass_rate", 0.0),
            l1_serverless_pass_rate=qr_d.get("l1_serverless_pass_rate", 0.0),
            l2_avg_grounding=qr_d.get("l2_avg_grounding", 0.0),
            l3_avg_diagnosis=qr_d.get("l3_avg_diagnosis", 0.0),
            l3_avg_evidence_quality=qr_d.get("l3_avg_evidence_quality", 0.0),
            l4_avg_relevance=qr_d.get("l4_avg_relevance", 0.0),
            l4_avg_feasibility=qr_d.get("l4_avg_feasibility", 0.0),
            l4_avg_improvement=qr_d.get("l4_avg_improvement", 0.0),
            pipeline_error=qr_d.get("pipeline_error", ""),
            primary_model=qr_d.get("primary_model", ""),
            llm_analysis_excerpt=qr_d.get("llm_analysis_excerpt", ""),
        ))

    return EvalReport(
        timestamp=d.get("timestamp", ""),
        num_queries=d.get("num_queries", 0),
        query_results=query_results,
        overall_l1_syntax=d.get("overall_l1_syntax", 0.0),
        overall_l1_serverless=d.get("overall_l1_serverless", 0.0),
        overall_l2_grounding=d.get("overall_l2_grounding", 0.0),
        overall_l3_diagnosis=d.get("overall_l3_diagnosis", 0.0),
        overall_l4_relevance=d.get("overall_l4_relevance", 0.0),
        overall_l4_feasibility=d.get("overall_l4_feasibility", 0.0),
        config=d.get("config", {}),
    )
