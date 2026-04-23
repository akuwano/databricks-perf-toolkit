"""Tests for shuffle I/O volume metrics extraction and bottleneck detection."""

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.models import NodeMetrics, QueryMetrics


def _make_qm(**overrides) -> QueryMetrics:
    defaults = dict(
        query_id="test-1",
        status="FINISHED",
        query_text="SELECT 1",
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


def _make_shuffle_node(
    bytes_written: int = 0,
    remote_bytes_read: int = 0,
    local_bytes_read: int = 0,
    fetch_wait_ms: int = 0,
    node_name: str = "ShuffleExchange",
) -> NodeMetrics:
    extra = {}
    if bytes_written:
        extra["Sink - Num bytes written"] = bytes_written
    if remote_bytes_read:
        extra["Source - Remote bytes read"] = remote_bytes_read
    if local_bytes_read:
        extra["Source - Local bytes read"] = local_bytes_read
    if fetch_wait_ms:
        extra["Source - Fetch wait time"] = fetch_wait_ms
    return NodeMetrics(
        node_name=node_name,
        node_tag="SHUFFLE",
        extra_metrics=extra,
    )


class TestShuffleIOVolumeExtraction:
    """Shuffle bytes written and remote bytes read should be aggregated."""

    def test_single_node(self):
        qm = _make_qm()
        nodes = [_make_shuffle_node(bytes_written=500_000_000, remote_bytes_read=400_000_000)]
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        assert bi.shuffle_bytes_written_total == 500_000_000
        assert bi.shuffle_remote_bytes_read_total == 400_000_000

    def test_multiple_nodes_aggregated(self):
        qm = _make_qm()
        nodes = [
            _make_shuffle_node(bytes_written=200_000_000, remote_bytes_read=150_000_000),
            _make_shuffle_node(bytes_written=300_000_000, remote_bytes_read=250_000_000),
        ]
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        assert bi.shuffle_bytes_written_total == 500_000_000
        assert bi.shuffle_remote_bytes_read_total == 400_000_000

    def test_zero_when_no_shuffle(self):
        qm = _make_qm()
        bi = calculate_bottleneck_indicators(qm, [], [], [])
        assert bi.shuffle_bytes_written_total == 0
        assert bi.shuffle_remote_bytes_read_total == 0

    def test_local_bytes_also_tracked(self):
        qm = _make_qm()
        nodes = [_make_shuffle_node(local_bytes_read=100_000_000)]
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        assert bi.shuffle_local_bytes_read_total == 100_000_000


class TestShuffleIOVolumeBottleneck:
    """High shuffle data volume should trigger alerts."""

    def test_high_shuffle_write_alert(self):
        qm = _make_qm(read_bytes=1_000_000_000)
        # Shuffle writes > 50% of read bytes
        nodes = [_make_shuffle_node(bytes_written=800_000_000)]
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        shuffle_alerts = [a for a in bi.alerts if a.metric_name == "shuffle_bytes_written"]
        assert len(shuffle_alerts) >= 1

    def test_no_alert_for_small_shuffle(self):
        qm = _make_qm(read_bytes=1_000_000_000)
        nodes = [_make_shuffle_node(bytes_written=10_000_000)]  # 1% of read
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        shuffle_alerts = [a for a in bi.alerts if a.metric_name == "shuffle_bytes_written"]
        assert len(shuffle_alerts) == 0

    def test_signal_emitted_for_high_shuffle_volume(self):
        qm = _make_qm(read_bytes=1_000_000_000)
        nodes = [_make_shuffle_node(bytes_written=600_000_000, remote_bytes_read=500_000_000)]
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "high_shuffle_data_volume" in signal_ids

    def test_shuffle_locality_signal(self):
        """When most shuffle reads are remote, emit a locality signal."""
        qm = _make_qm()
        nodes = [
            _make_shuffle_node(
                remote_bytes_read=900_000_000,
                local_bytes_read=100_000_000,
            )
        ]
        bi = calculate_bottleneck_indicators(qm, nodes, [], [])
        signal_ids = [s.signal_id for s in bi.detected_signals]
        assert "low_shuffle_locality" in signal_ids
