"""AQE / AOS Runtime Optimization Events must live under section 7
"AQE Shuffle Health", not under the "D. Data Flow Details" appendix.

User-requested move: the events describe AQE/AOS interventions on
shuffle plans and belong next to the shuffle-health diagnosis, not
buried in the operator-level data flow appendix.
"""

import pytest
from core.models import ProfileAnalysis, QueryMetrics, ShuffleMetrics
from core.reporters import generate_report


@pytest.fixture(autouse=True)
def _restore_locale():
    """generate_report(lang='ja'/'en') mutates thread-local i18n state.
    Save + restore so subsequent tests in the suite don't inherit ja."""
    from core.i18n import get_language, set_language

    prev = get_language()
    yield
    set_language(prev)


def _analysis_with_aqe_event() -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics(read_bytes=1_000_000_000, total_time_ms=60_000)
    a.shuffle_metrics = [
        ShuffleMetrics(
            node_id="484",
            partition_count=112,
            sink_bytes_written=10 * 1024**3,
            peak_memory_bytes=1024**3,
            aqe_self_repartition_count=1,
            aqe_original_num_partitions=112,
            aqe_intended_num_partitions=3436,
        )
    ]
    return a


class TestAqeAosEventsNotInDataFlowDetails:
    """The Data Flow Details appendix (D. データフロー詳細) must NO LONGER
    contain the AQE/AOS runtime-optimization events."""

    def test_data_flow_appendix_does_not_contain_aqe_events_subheader(self):
        a = _analysis_with_aqe_event()
        # Add minimal data_flow so the appendix renders
        from core.models import DataFlowEntry

        a.data_flow = [
            DataFlowEntry(
                node_id="1",
                operation="Scan table",
                output_rows=1000,
                duration_ms=100,
                peak_memory_bytes=1024 * 1024,
            )
        ]
        md = generate_report(a, lang="en")
        # Locate the Data Flow Details appendix and snippet out its content
        # up to the next top-level heading
        dd_idx = md.find("## D. Data Flow Details")
        if dd_idx < 0:
            dd_idx = md.find("D. データフロー詳細")
        assert dd_idx >= 0, "Data Flow Details appendix must exist"
        # Next '## ' heading after dd_idx
        next_top = md.find("\n## ", dd_idx + 3)
        section = md[dd_idx : next_top if next_top > 0 else len(md)]
        assert "AQE / AOS Runtime Optimization" not in section, (
            "AQE/AOS events must no longer render inside Data Flow Details appendix"
        )
        assert "AQE / AOS 実行時最適化イベント" not in section


class TestAqeAosEventsUnderSection7:
    """Section 7 "AQE Shuffle Health" is the correct home for these events
    because they describe AQE interventions on shuffle plans."""

    def test_section7_contains_aqe_events_when_present(self):
        a = _analysis_with_aqe_event()
        md = generate_report(a, lang="en")
        # Section 7 heading
        s7 = md.find("## 7. AQE Shuffle Health")
        assert s7 >= 0, "Section 7 must render"
        next_top = md.find("\n## ", s7 + 3)
        section = md[s7 : next_top if next_top > 0 else len(md)]
        # AQE/AOS events subsection MUST appear here
        assert "AQE / AOS Runtime Optimization Events" in section, (
            "AQE/AOS events subheader must appear under section 7"
        )
        # Concrete node id from the fixture must also appear
        assert "#484" in section
        assert "AQE auto-repartition" in section
        assert "112" in section and "3436" in section

    def test_section7_skips_aqe_events_when_no_events(self):
        """If no shuffle has AQE/AOS event data, the events subsection must
        not render — section 7 may still render for the shuffle-health table."""
        from core.models import ShuffleMetrics

        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics()
        a.shuffle_metrics = [
            ShuffleMetrics(node_id="1", partition_count=200, sink_bytes_written=1_000_000)
        ]
        md = generate_report(a, lang="en")
        assert "AQE / AOS Runtime Optimization Events" not in md

    def test_section7_ja_label_present(self):
        a = _analysis_with_aqe_event()
        md = generate_report(a, lang="ja")
        s7_ja = md.find("## 7. AQE Shuffle健全性")
        assert s7_ja >= 0, "Section 7 (JA) must render"
        next_top = md.find("\n## ", s7_ja + 3)
        section = md[s7_ja : next_top if next_top > 0 else len(md)]
        assert "AQE / AOS 実行時最適化イベント" in section
