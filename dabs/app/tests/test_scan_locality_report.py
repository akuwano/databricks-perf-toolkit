"""Tests for scan locality per-node report generation."""

from core.models import NodeMetrics
from core.reporters import generate_scan_locality_section


class TestGenerateScanLocalitySection:
    def test_empty_nodes(self):
        result = generate_scan_locality_section([])
        assert result == ""

    def test_no_scan_nodes(self):
        nodes = [NodeMetrics(node_name="Shuffle", local_scan_tasks=0, non_local_scan_tasks=0)]
        result = generate_scan_locality_section(nodes)
        assert result == ""

    def test_renders_table_with_scan_nodes(self):
        nodes = [
            NodeMetrics(
                node_id="1",
                node_name="Scan table_a",
                local_scan_tasks=10,
                non_local_scan_tasks=0,
                cache_hits_size=1000,
                cache_misses_size=0,
            ),
            NodeMetrics(
                node_id="2",
                node_name="Scan table_b",
                local_scan_tasks=5,
                non_local_scan_tasks=15,
                cache_hits_size=100,
                cache_misses_size=900,
            ),
        ]
        result = generate_scan_locality_section(nodes)
        assert "table_a" in result
        assert "table_b" in result
        assert "0.0%" in result  # table_a rescheduled
        assert "75.0%" in result  # table_b rescheduled (15/20)
        assert "100.0%" in result  # table_a cache hit
        assert "10.0%" in result  # table_b cache hit (100/1000)

    def test_highlights_cold_node_pattern(self):
        nodes = [
            NodeMetrics(
                node_id="1",
                node_name="Scan lineitem",
                local_scan_tasks=5,
                non_local_scan_tasks=20,
                cache_hits_size=100,
                cache_misses_size=5000,
            ),
        ]
        result = generate_scan_locality_section(nodes)
        assert "cold" in result.lower() or "Cold" in result or "COLD" in result
