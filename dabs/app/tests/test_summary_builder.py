"""Tests for core.summary_builder — Slack summary generation."""

from core.models import BottleneckIndicators, QueryMetrics
from core.summary_builder import build_slack_summary


class TestBuildSlackSummary:
    """Tests for build_slack_summary()."""

    def _make_metrics(self, **overrides) -> QueryMetrics:
        defaults = dict(
            query_id="b137430a-739d-4152-96de-c1cef0c1b7c5",
            status="FINISHED",
            total_time_ms=12340,
            read_bytes=1_073_741_824,
            spill_to_disk_bytes=1_288_490_188,
        )
        defaults.update(overrides)
        return QueryMetrics(**defaults)

    def _make_indicators(self, **overrides) -> BottleneckIndicators:
        defaults = dict(
            cache_hit_ratio=0.852,
            photon_ratio=0.921,
            spill_bytes=1_288_490_188,
            critical_issues=["Data skew detected", "High spill"],
            warnings=["Moderate shuffle", "Low filter rate", "Remote reads"],
        )
        defaults.update(overrides)
        return BottleneckIndicators(**defaults)

    def test_basic_format(self):
        result = build_slack_summary(
            analysis_id="abc-123",
            query_metrics=self._make_metrics(),
            bottleneck_indicators=self._make_indicators(),
            action_count=5,
            base_url="https://myapp.databricksapps.com",
        )
        assert "abc-123" in result
        assert "12.3" in result  # total_time_ms → seconds
        assert "85.2%" in result  # cache_hit_ratio
        assert "92.1%" in result  # photon_ratio
        assert "2 critical" in result
        assert "3 warning" in result
        assert "5 action" in result
        assert "https://myapp.databricksapps.com/shared/abc-123" in result

    def test_query_id_short(self):
        """query_id is truncated to first 12 chars."""
        result = build_slack_summary(
            analysis_id="x",
            query_metrics=self._make_metrics(query_id="abcdefghijklmnop"),
            bottleneck_indicators=self._make_indicators(),
            action_count=0,
        )
        assert "abcdefghijkl" in result
        assert "abcdefghijklmnop" not in result

    def test_zero_values(self):
        result = build_slack_summary(
            analysis_id="zero",
            query_metrics=self._make_metrics(total_time_ms=0, spill_to_disk_bytes=0),
            bottleneck_indicators=self._make_indicators(
                cache_hit_ratio=0.0,
                photon_ratio=0.0,
                spill_bytes=0,
                critical_issues=[],
                warnings=[],
            ),
            action_count=0,
        )
        assert "0 critical" in result
        assert "0 warning" in result
        assert "0 action" in result

    def test_no_base_url(self):
        """When base_url is empty, link line is omitted."""
        result = build_slack_summary(
            analysis_id="no-url",
            query_metrics=self._make_metrics(),
            bottleneck_indicators=self._make_indicators(),
            action_count=1,
            base_url="",
        )
        assert "/shared/" not in result

    def test_returns_string(self):
        result = build_slack_summary(
            analysis_id="type-check",
            query_metrics=self._make_metrics(),
            bottleneck_indicators=self._make_indicators(),
            action_count=3,
        )
        assert isinstance(result, str)
        assert len(result) > 0

    def test_warehouse_info(self):
        """Warehouse name/size is included when provided."""
        result = build_slack_summary(
            analysis_id="wh",
            query_metrics=self._make_metrics(),
            bottleneck_indicators=self._make_indicators(),
            action_count=1,
            warehouse_name="Shared WH",
            warehouse_size="Medium",
        )
        assert "Shared WH" in result
        assert "Medium" in result

    def test_large_spill_gb(self):
        """Spill is formatted in GB."""
        result = build_slack_summary(
            analysis_id="spill",
            query_metrics=self._make_metrics(),
            bottleneck_indicators=self._make_indicators(spill_bytes=5_368_709_120),
            action_count=0,
        )
        assert "5.0 GB" in result

    def test_top_action_included(self):
        """Top action text is shown when provided."""
        result = build_slack_summary(
            analysis_id="action",
            query_metrics=self._make_metrics(),
            bottleneck_indicators=self._make_indicators(),
            action_count=3,
            top_action="Add BROADCAST hint for small table joins",
        )
        assert "Add BROADCAST hint" in result
