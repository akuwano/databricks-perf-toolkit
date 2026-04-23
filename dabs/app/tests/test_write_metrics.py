"""Tests for write metrics extraction and bottleneck detection."""

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.extractors import extract_query_metrics
from core.models import QueryMetrics


def _make_qm(**overrides) -> QueryMetrics:
    defaults = dict(
        query_id="test-1",
        status="FINISHED",
        query_text="INSERT INTO t SELECT * FROM s",
        total_time_ms=10000,
        execution_time_ms=9000,
        read_bytes=1_000_000_000,
        read_cache_bytes=800_000_000,
        photon_total_time_ms=7000,
        task_total_time_ms=9000,
        read_files_count=100,
        pruned_files_count=900,
    )
    defaults.update(overrides)
    return QueryMetrics(**defaults)


class TestWriteRemoteRowsExtraction:
    """writeRemoteRows should be extracted into QueryMetrics."""

    def test_extracts_write_remote_rows(self):
        data = {"metrics": {"writeRemoteRows": 9259}}
        qm = extract_query_metrics(data)
        assert qm.write_remote_rows == 9259

    def test_defaults_to_zero(self):
        data = {"metrics": {}}
        qm = extract_query_metrics(data)
        assert qm.write_remote_rows == 0

    def test_not_in_extra_metrics(self):
        data = {"metrics": {"writeRemoteRows": 100}}
        qm = extract_query_metrics(data)
        assert "writeRemoteRows" not in qm.extra_metrics


class TestWriteMetricsBottleneck:
    """Large write operations should trigger signals."""

    def test_large_write_signal(self):
        """Writing >1GB should emit a large_write_volume signal."""
        qm = _make_qm(
            write_remote_bytes=5_000_000_000,  # 5GB
            write_remote_files=500,
            write_remote_rows=10_000_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "large_write_volume" in signal_ids

    def test_no_signal_for_small_write(self):
        """Writing <1GB should not emit signal."""
        qm = _make_qm(
            write_remote_bytes=100_000_000,  # 100MB
            write_remote_files=10,
            write_remote_rows=1000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "large_write_volume" not in signal_ids

    def test_write_amplification_signal(self):
        """Write bytes >> read bytes indicates write amplification."""
        qm = _make_qm(
            read_bytes=1_000_000_000,  # 1GB read
            write_remote_bytes=3_000_000_000,  # 3GB write = 3x amplification
            write_remote_rows=10_000_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "write_amplification" in signal_ids

    def test_no_amplification_for_normal_ratio(self):
        """Write bytes < read bytes is normal, no signal."""
        qm = _make_qm(
            read_bytes=1_000_000_000,  # 1GB read
            write_remote_bytes=500_000_000,  # 0.5GB write
            write_remote_rows=1_000_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "write_amplification" not in signal_ids

    def test_small_file_write_signal(self):
        """Many small files written should emit a signal."""
        qm = _make_qm(
            write_remote_bytes=1_000_000_000,  # 1GB
            write_remote_files=1000,  # avg 1MB per file — small files
            write_remote_rows=10_000_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "small_write_files" in signal_ids

    def test_no_small_file_signal_for_normal_size(self):
        """Normal file sizes should not emit signal."""
        qm = _make_qm(
            write_remote_bytes=1_000_000_000,  # 1GB
            write_remote_files=5,  # avg 200MB per file — normal
            write_remote_rows=10_000_000,
        )
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "small_write_files" not in signal_ids
