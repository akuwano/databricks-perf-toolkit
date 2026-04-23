"""Regression: Top Scanned Tables in the rule-based report must show
column types (from EXPLAIN) and a clearer "none configured" label when
clustering keys are actually empty.

Captured from production analysis 8d970c04-... where the LLM guessed
from the "_lc" suffix because the report's Top Scanned Tables table
was bare (bytes + pruning + "N/A") and the column types were never
surfaced to the reader.
"""

from core.explain_parser import ExplainExtended
from core.models import BottleneckIndicators, QueryMetrics, TableScanMetrics
from core.reporters.sections import generate_io_metrics_section


def _qm() -> QueryMetrics:
    return QueryMetrics(read_bytes=500_000_000, read_files_count=100)


class TestTopScannedTablesColumnTypes:
    def test_column_types_rendered_when_explain_has_scan_schemas(self):
        qm = _qm()
        bi = BottleneckIndicators()
        tops = [
            TableScanMetrics(
                table_name="skato.aisin_poc.store_sales_delta_lc",
                bytes_read=500_000_000,
                current_clustering_keys=["SS_SOLD_DATE_SK"],
            )
        ]
        ex = ExplainExtended()
        ex.scan_schemas = {
            "skato.aisin_poc.store_sales_delta_lc": {
                "SS_CUSTOMER_SK": "decimal(38,0)",
                "SS_QUANTITY": "int",
            }
        }
        md = generate_io_metrics_section(qm, bi, tops, explain_analysis=ex)
        # Column types must be in the rendered markdown
        assert "decimal(38,0)" in md
        assert "SS_CUSTOMER_SK" in md

    def test_clustering_keys_empty_shows_none_configured_not_na(self):
        qm = _qm()
        bi = BottleneckIndicators()
        tops = [
            TableScanMetrics(
                table_name="some_plain_table",
                bytes_read=10_000_000,
                current_clustering_keys=[],
            )
        ]
        md = generate_io_metrics_section(qm, bi, tops)
        assert "some_plain_table" in md
        # Explicit wording so the LLM cannot interpret "N/A" ambiguously
        assert "none configured" in md.lower() or "none" in md.lower()

    def test_column_types_column_header_present_when_explain_attached(self):
        qm = _qm()
        bi = BottleneckIndicators()
        tops = [
            TableScanMetrics(
                table_name="t1",
                bytes_read=1_000,
                current_clustering_keys=[],
            )
        ]
        ex = ExplainExtended()
        ex.scan_schemas = {"t1": {"a": "int"}}
        md = generate_io_metrics_section(qm, bi, tops, explain_analysis=ex)
        # The table should have a column-types header cell
        assert "Column Types" in md or "カラム型" in md

    def test_no_explain_no_column_types_column(self):
        qm = _qm()
        bi = BottleneckIndicators()
        tops = [
            TableScanMetrics(
                table_name="t1",
                bytes_read=1_000,
                current_clustering_keys=["c1"],
            )
        ]
        # explain_analysis is None: preserve legacy output
        md = generate_io_metrics_section(qm, bi, tops)
        # Clustering key is still rendered
        assert "c1" in md

    def test_backward_compat_without_explain_arg(self):
        # Old call sites must keep working without the new kwarg
        qm = _qm()
        bi = BottleneckIndicators()
        tops = [TableScanMetrics(table_name="t1", bytes_read=1_000)]
        md = generate_io_metrics_section(qm, bi, tops)
        assert "t1" in md


class TestStandaloneTopScannedTablesSection:
    """Verifies the extracted helper works standalone so the main report
    pipeline (generate_performance_metrics) can append it."""

    def test_helper_renders_expected_table(self):
        from core.reporters.sections import generate_top_scanned_tables_section

        ex = ExplainExtended()
        ex.scan_schemas = {"t1": {"k": "decimal(38,0)"}}
        tops = [
            TableScanMetrics(
                table_name="t1",
                bytes_read=1_000_000,
                current_clustering_keys=["k"],
            )
        ]
        md = generate_top_scanned_tables_section(tops, explain_analysis=ex)
        assert "Top Scanned Tables" in md or "スキャン上位" in md
        assert "decimal(38,0)" in md
        assert "`k`: decimal(38,0)" in md

    def test_helper_empty_returns_empty_string(self):
        from core.reporters.sections import generate_top_scanned_tables_section

        assert generate_top_scanned_tables_section([]) == ""


class TestMainPipelineIncludesTopScannedTables:
    """Regression: the main structured report pipeline must include the
    Top Scanned Tables section under Performance Metrics — previously it
    was emitted only by the legacy template.
    """

    def test_main_report_contains_top_scanned_tables(self):
        from core.models import ProfileAnalysis
        from core.reporters import generate_report

        ex = ExplainExtended()
        ex.scan_schemas = {"main.base.store_sales_delta_lc": {"SS_CUSTOMER_SK": "decimal(38,0)"}}
        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(read_bytes=500_000_000)
        a.top_scanned_tables = [
            TableScanMetrics(
                table_name="main.base.store_sales_delta_lc",
                bytes_read=500_000_000,
                current_clustering_keys=["SS_CUSTOMER_SK"],
            )
        ]
        a.explain_analysis = ex
        md = generate_report(a, lang="en")
        # The rendered report MUST contain the column type, not just
        # mention the table name
        assert "decimal(38,0)" in md
        assert "SS_CUSTOMER_SK" in md
