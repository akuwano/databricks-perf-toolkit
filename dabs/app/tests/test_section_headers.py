"""Regression tests for include_header parameter in report section generators.

Covers 3 functions in reporters/sections.py that had a latent NameError bug:
they referenced `include_header` in the body without declaring it in the
signature, so any call would raise at runtime. PR #49 added the kwarg; these
tests lock in the behavior.
"""

from core.explain_parser import ExplainExtended, ExplainSection, PlanKind
from core.models import (
    BottleneckIndicators,
    CloudStorageMetrics,
    QueryMetrics,
)
from core.reporters.sections import (
    generate_cloud_storage_section,
    generate_explain_section,
    generate_io_metrics_section,
)


class TestGenerateExplainSectionHeader:
    def _explain(self) -> ExplainExtended:
        return ExplainExtended(
            sections=[ExplainSection(name=PlanKind.PHYSICAL.value)],
        )

    def test_default_includes_header(self):
        result = generate_explain_section(self._explain())
        assert "Execution Plan Analysis (EXPLAIN)" in result

    def test_include_header_false_omits_header(self):
        result = generate_explain_section(self._explain(), include_header=False)
        assert "Execution Plan Analysis (EXPLAIN)" not in result

    def test_called_without_kwarg_does_not_raise(self):
        # Regression: body used `include_header` without declaring it → NameError
        generate_explain_section(self._explain())


class TestGenerateIoMetricsSectionHeader:
    def _inputs(self) -> tuple[QueryMetrics, BottleneckIndicators]:
        return QueryMetrics(read_bytes=1024), BottleneckIndicators()

    def test_default_includes_header(self):
        qm, bi = self._inputs()
        result = generate_io_metrics_section(qm, bi)
        assert "I/O Metrics" in result

    def test_include_header_false_omits_header(self):
        qm, bi = self._inputs()
        result = generate_io_metrics_section(qm, bi, include_header=False)
        assert "I/O Metrics" not in result

    def test_called_without_kwarg_does_not_raise(self):
        qm, bi = self._inputs()
        generate_io_metrics_section(qm, bi)


class TestGenerateCloudStorageSectionHeader:
    def _bi_with_metrics(self) -> BottleneckIndicators:
        return BottleneckIndicators(
            cloud_storage_metrics=CloudStorageMetrics(
                total_request_count=100,
                avg_request_duration_ms=50.0,
            )
        )

    def test_empty_returns_empty_string(self):
        # Guard clause: no requests → empty output, regardless of include_header
        result = generate_cloud_storage_section(BottleneckIndicators())
        assert result == ""

    def test_default_includes_header(self):
        result = generate_cloud_storage_section(self._bi_with_metrics())
        assert "Cloud Storage Performance" in result

    def test_include_header_false_omits_header(self):
        result = generate_cloud_storage_section(self._bi_with_metrics(), include_header=False)
        assert "Cloud Storage Performance" not in result

    def test_called_without_kwarg_does_not_raise(self):
        generate_cloud_storage_section(self._bi_with_metrics())
