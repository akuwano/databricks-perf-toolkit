"""AQE/AOS runtime-optimization events helper.

The helper previously lived under ``generate_data_flow_section`` but was
moved to section 7 "AQE Shuffle Health" (see ``test_aqe_aos_events_section7``
for end-to-end coverage). These tests still exercise the rendering
logic at the helper level so it can be maintained in isolation.
"""

import pytest
from core.models import ShuffleMetrics
from core.reporters.dataflow import format_aqe_aos_events


@pytest.fixture(autouse=True)
def _force_en_locale():
    """Pin the locale to English for string assertions inside this module,
    and restore the prior locale on teardown so other tests are unaffected."""
    from core.i18n import get_language, set_language

    prev = get_language()
    set_language("en")
    yield
    set_language(prev)


class TestAqeAosEventsHelper:
    def test_no_section_when_no_shuffles(self):
        assert format_aqe_aos_events([]) == ""

    def test_no_section_when_shuffles_have_no_events(self):
        sm = ShuffleMetrics(node_id="32", partition_count=100, sink_bytes_written=1_000_000)
        assert format_aqe_aos_events([sm]) == ""

    def test_aqe_self_repartition_rendered(self):
        sm = ShuffleMetrics(
            node_id="32",
            aqe_self_repartition_count=1,
            aqe_original_num_partitions=112,
            aqe_intended_num_partitions=3436,
        )
        section = format_aqe_aos_events([sm])
        assert "AQE / AOS Runtime Optimization Events" in section
        assert "#32" in section
        assert "AQE auto-repartition" in section
        assert "112" in section and "3436" in section

    def test_aqe_skew_split_rendered(self):
        sm = ShuffleMetrics(node_id="85", aqe_skewed_partitions=12)
        section = format_aqe_aos_events([sm])
        assert "⚖️" in section or "skew-join split" in section
        assert "12" in section

    def test_aos_repartition_rendered(self):
        sm = ShuffleMetrics(
            node_id="99",
            aos_coordinated_repartition_count=1,
            aos_old_num_partitions=400,
            aos_new_num_partitions=50,
        )
        section = format_aqe_aos_events([sm])
        assert "AOS" in section
        assert "400" in section and "50" in section

    def test_multiple_events_in_same_shuffle(self):
        sm = ShuffleMetrics(
            node_id="41",
            aqe_self_repartition_count=1,
            aqe_original_num_partitions=200,
            aqe_intended_num_partitions=1000,
            aqe_skewed_partitions=3,
        )
        section = format_aqe_aos_events([sm])
        assert "AQE auto-repartition" in section
        assert "skew-join split" in section or "⚖️" in section

    def test_aqe_cancel_and_remat_rendered(self):
        sm = ShuffleMetrics(
            node_id="7",
            aqe_cancellation_count=2,
            aqe_triggered_on_materialized_count=3,
        )
        section = format_aqe_aos_events([sm])
        assert "re-plan" in section or "🔁" in section
        assert "2" in section and "3" in section

    def test_events_never_render_inside_data_flow_section(self):
        """Regression: generate_data_flow_section must NOT render AQE/AOS
        events anymore (they moved to section 7)."""
        from core.models import DataFlowEntry
        from core.reporters.dataflow import generate_data_flow_section

        sm = ShuffleMetrics(
            node_id="32",
            aqe_self_repartition_count=1,
            aqe_original_num_partitions=112,
            aqe_intended_num_partitions=3436,
        )
        entry = DataFlowEntry(
            node_id="1",
            operation="Scan table",
            output_rows=1000,
            duration_ms=100,
            peak_memory_bytes=1024 * 1024,
        )
        section = generate_data_flow_section([entry], shuffle_metrics=[sm])
        # Events must not render via this function
        assert "AQE / AOS Runtime Optimization Events" not in section
        assert "AQE / AOS 実行時最適化イベント" not in section
