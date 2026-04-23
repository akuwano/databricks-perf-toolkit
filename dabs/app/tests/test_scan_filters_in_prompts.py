"""Scan-node FILTERS metadata (the predicates already pushed to the
scan) must reach every LLM stage prompt.

Regression: a user's shared report showed
``(ce.MYCLOUD_STARTMONTH = 12BD)`` and ``(ce.MYCLOUD_STARTYEAR = 2025BD)``
on the source scan in the profile JSON, yet the LLM action plan
recommended "apply date/account filter before JOIN" — contradicting
the already-present filters. The structured Stage 1/2/3 prompts did
not surface per-scan-node filter conditions (only the legacy
``create_analysis_prompt`` did), so the LLM was blind to them.
"""

from __future__ import annotations

import pytest
from core.llm_prompts.prompts import (
    create_refine_prompt,
    create_review_prompt,
    create_structured_analysis_prompt,
)
from core.models import (
    NodeMetrics,
    ProfileAnalysis,
    QueryMetrics,
    TableScanMetrics,
)


@pytest.fixture(autouse=True)
def _restore_locale():
    from core.i18n import get_language, set_language

    prev = get_language()
    yield
    set_language(prev)


def _analysis_with_scan_filters() -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics(
        query_text="INSERT INTO t SELECT * FROM mycloudcur_incremental_2xl_sf ce ...",
        total_time_ms=60_000,
    )
    a.node_metrics = [
        NodeMetrics(
            node_id="s1",
            node_name="Scan ck_db_ws.default.mycloudcur_incremental_2xl_sf",
            duration_ms=300_000,
            filter_conditions=[
                "(ce.MYCLOUD_STARTMONTH = 12BD)",
                "(ce.MYCLOUD_STARTYEAR = 2025BD)",
            ],
        ),
        NodeMetrics(
            node_id="s2",
            node_name="Scan ck_db_ws.default.other_table",
            duration_ms=20_000,
            filter_conditions=["(ot.active = true)"],
        ),
    ]
    a.top_scanned_tables = [
        TableScanMetrics(
            table_name="ck_db_ws.default.mycloudcur_incremental_2xl_sf",
            bytes_read=100 * 1024**3,
            rows_scanned=1_000_000_000,
        ),
        TableScanMetrics(
            table_name="ck_db_ws.default.other_table",
            bytes_read=10 * 1024**3,
            rows_scanned=100_000_000,
        ),
    ]
    return a


class TestScanFiltersReachEveryStage:
    def _prompts(self, a: ProfileAnalysis) -> dict[str, str]:
        return {
            "stage1": create_structured_analysis_prompt(a, lang="en"),
            "stage2": create_review_prompt(a, "prior analysis", "primary", "en"),
            "stage3": create_refine_prompt("init", "rev", "p", "r", "en", analysis=a),
        }

    def test_pushed_filter_literal_visible_in_every_stage(self):
        """The literal predicate ``MYCLOUD_STARTMONTH = 12BD`` must appear
        verbatim in every stage prompt."""
        prompts = self._prompts(_analysis_with_scan_filters())
        for name, prompt in prompts.items():
            assert "MYCLOUD_STARTMONTH" in prompt, (
                f"Scan filter column missing from {name} — LLM will not know "
                f"the predicate is already pushed down"
            )
            assert "MYCLOUD_STARTYEAR" in prompt, f"Second filter column missing from {name}"

    def test_filter_attributed_to_correct_table(self):
        """Each filter must be co-located with its table name so the LLM
        doesn't misattribute filters across tables."""
        prompts = self._prompts(_analysis_with_scan_filters())
        for name, prompt in prompts.items():
            # Extract a window around the filter mention and verify the
            # table name appears within the same section.
            idx = prompt.find("MYCLOUD_STARTMONTH")
            assert idx >= 0
            # Look back 500 chars and forward 200 chars for the table name.
            window = prompt[max(0, idx - 500) : idx + 200]
            assert "mycloudcur_incremental_2xl_sf" in window, (
                f"Table name missing from filter context in {name}"
            )

    def test_empty_filter_conditions_not_rendered_as_noise(self):
        """A scan node with no filter conditions must not produce a
        ``filters: None`` / ``filters: []`` / ``(no filters)`` artifact."""
        a = _analysis_with_scan_filters()
        a.node_metrics[1].filter_conditions = []  # strip filters from s2
        prompts = self._prompts(a)
        for name, prompt in prompts.items():
            assert "filters: None" not in prompt, f"{name} renders 'filters: None' artifact"
            assert "filters: []" not in prompt, f"{name} renders empty-list artifact"


class TestScanFiltersOmitSectionWhenEmpty:
    """When no scan has any filter conditions, the scan-filters
    subsection must NOT appear at all — it would just cost tokens."""

    def test_no_scan_filters_omits_subsection(self):
        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(query_text="SELECT * FROM t", total_time_ms=1000)
        a.node_metrics = [
            NodeMetrics(
                node_id="s1",
                node_name="Scan ck_db_ws.default.t",
                duration_ms=1000,
                filter_conditions=[],
            ),
        ]
        a.top_scanned_tables = [TableScanMetrics(table_name="ck_db_ws.default.t")]
        prompt = create_structured_analysis_prompt(a, lang="en")
        # The header should not appear when there are no filters anywhere.
        assert "Scan Filter" not in prompt and "Pushed Filter" not in prompt
