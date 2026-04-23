"""When file/byte pruning is 0% the fact pack must state "full table scan"
evidence-based, and the constraints block must forbid hedging with
"SQL truncated so we cannot confirm".

Regression: real report said "SQLが切り詰められているため確定不可" even
though filter_rate=0.0% and bytes_pruning_ratio=0.0% were both in the
Fact Pack — that's metric-based evidence, not SQL-text-dependent.
"""

import pytest
from core.llm_prompts.prompts import (
    _build_fact_pack_summary,
    _constraints_block,
    create_refine_prompt,
    create_review_prompt,
    create_structured_analysis_prompt,
)
from core.models import BottleneckIndicators, ProfileAnalysis, QueryMetrics


@pytest.fixture(autouse=True)
def _restore_locale():
    from core.i18n import get_language, set_language

    prev = get_language()
    yield
    set_language(prev)


def _full_scan_analysis() -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics(
        query_text="INSERT INTO t SELECT * FROM big_table",
        read_bytes=138 * 1024**3,
        read_files_count=7328,
        pruned_files_count=0,
        pruned_bytes=0,
    )
    a.bottleneck_indicators = BottleneckIndicators(
        filter_rate=0.0,
        bytes_pruning_ratio=0.0,
    )
    return a


class TestScanCoverageEvidence:
    def test_fact_pack_summary_has_scan_coverage_block(self):
        summary = _build_fact_pack_summary(_full_scan_analysis(), "en")
        assert "scan_coverage" in summary, (
            "Fact Pack must include a scan_coverage block when pruning is 0%"
        )

    def test_fact_pack_summary_marks_full_scan(self):
        summary = _build_fact_pack_summary(_full_scan_analysis(), "en")
        lower = summary.lower()
        # Strict full-scan (both pruning ratios exactly 0%) must use the
        # "full-table scan confirmed" verdict, not the weaker "near-full".
        assert "full-table scan confirmed" in lower
        assert "near-full" not in lower

    def test_fact_pack_uses_near_full_verdict_for_sub_one_percent(self):
        """0.5% pruning must be reported as near-full scan, not strict
        full-table confirmed — prevents false-positive full-scan claims."""
        a = _full_scan_analysis()
        # File pruning at 0.5%: 37 of 7365 files pruned
        a.query_metrics.read_files_count = 7328
        a.query_metrics.pruned_files_count = 37
        a.bottleneck_indicators.bytes_pruning_ratio = 0.005
        summary = _build_fact_pack_summary(a, "en")
        lower = summary.lower()
        assert "scan_coverage" in lower
        assert "near-full scan" in lower
        # Must NOT claim strict full-table scan confirmed
        assert "full-table scan confirmed" not in lower

    def test_fact_pack_omits_scan_coverage_when_pruning_present(self):
        a = _full_scan_analysis()
        a.query_metrics.pruned_files_count = 5000  # most files pruned
        a.bottleneck_indicators.bytes_pruning_ratio = 0.8
        a.bottleneck_indicators.filter_rate = 0.7
        summary = _build_fact_pack_summary(a, "en")
        # Either no scan_coverage key at all, or not the "full-table scan" hint
        assert "full-table scan" not in summary.lower() and "full table scan" not in summary.lower()


class TestConstraintsNoHedgeOnFullScan:
    @pytest.mark.parametrize("lang", ["ja", "en"])
    def test_constraints_block_forbids_sql_truncated_hedge(self, lang):
        block = _constraints_block(lang)
        lower = block.lower()
        # Must explicitly mention: if pruning=0% then "full scan" is evidence
        assert "pruning" in lower or "プルーニング" in block
        # And forbid the "SQL truncated → unconfirmed" pattern
        if lang == "ja":
            assert "切り詰め" in block or "トランケ" in block or "確定" in block
        else:
            assert "truncat" in lower  # "truncate" / "truncated"


class TestFullScanEvidenceReachesAllStages:
    def _prompts(self, a):
        return {
            "stage1": create_structured_analysis_prompt(a, lang="en"),
            "stage2": create_review_prompt(a, "prior", "primary", "en"),
            "stage3": create_refine_prompt("init", "rev", "p", "r", "en", analysis=a),
        }

    def test_full_scan_marker_visible_in_every_stage(self):
        prompts = self._prompts(_full_scan_analysis())
        for name, prompt in prompts.items():
            lower = prompt.lower()
            assert "scan_coverage" in lower or "full" in lower, (
                f"Full-scan evidence missing from {name}"
            )
