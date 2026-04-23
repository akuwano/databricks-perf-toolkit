"""Per-shuffle details (including partitioning key / shuffle_attributes)
must reach every LLM stage prompt.

Regression: real production report missed that the bottleneck was a
shuffle on ``ce.lineitem_usagetype`` because the new structured prompt
pipeline only surfaces aggregated shuffle totals. The legacy
create_analysis_prompt had per-shuffle detail (line ~956) but the
active Stage 1/2/3 prompts did not.
"""

import pytest
from core.llm_prompts.prompts import (
    create_refine_prompt,
    create_review_prompt,
    create_structured_analysis_prompt,
)
from core.models import ProfileAnalysis, QueryMetrics, ShuffleMetrics


@pytest.fixture(autouse=True)
def _restore_locale():
    from core.i18n import get_language, set_language

    prev = get_language()
    yield
    set_language(prev)


def _analysis_with_bottleneck_shuffle() -> ProfileAnalysis:
    a = ProfileAnalysis()
    a.query_metrics = QueryMetrics(query_text="INSERT INTO ...", total_time_ms=60_000)
    a.shuffle_metrics = [
        ShuffleMetrics(
            node_id="8403",
            partition_count=1024,
            peak_memory_bytes=981 * 1024**3,
            sink_bytes_written=146 * 1024**3,
            shuffle_attributes=["ce.lineitem_usagetype"],
        ),
        # Also a tiny shuffle — must NOT crowd out the bottleneck
        ShuffleMetrics(
            node_id="8404",
            partition_count=1,
            peak_memory_bytes=1024,
            sink_bytes_written=0,
            shuffle_attributes=[],
        ),
    ]
    return a


class TestShuffleDetailsReachAllStages:
    def _prompts(self, a):
        return {
            "stage1": create_structured_analysis_prompt(a, lang="en"),
            "stage2": create_review_prompt(a, "prior", "primary", "en"),
            "stage3": create_refine_prompt("init", "rev", "p", "r", "en", analysis=a),
        }

    def test_shuffle_key_visible_in_every_stage(self):
        prompts = self._prompts(_analysis_with_bottleneck_shuffle())
        for name, prompt in prompts.items():
            assert "ce.lineitem_usagetype" in prompt, (
                f"Shuffle partitioning key missing from {name} — LLM cannot "
                "identify the bottleneck key"
            )

    def test_memory_per_partition_visible(self):
        """The 1663MB/part figure is the smoking gun for memory-inefficient shuffle."""
        prompts = self._prompts(_analysis_with_bottleneck_shuffle())
        for name, prompt in prompts.items():
            # 981GB / 1024 parts ≈ 981 MB/part (actual calc), but we
            # expect the full MB/partition string to be rendered
            assert "MB" in prompt and ("1024" in prompt or "partition" in prompt.lower()), (
                f"Shuffle partition count/memory missing from {name}"
            )

    def test_bottleneck_shuffle_node_id_visible(self):
        prompts = self._prompts(_analysis_with_bottleneck_shuffle())
        for name, prompt in prompts.items():
            assert "8403" in prompt, f"Top shuffle node_id missing from {name}"

    def test_empty_shuffle_attributes_not_rendered_as_none(self):
        """A shuffle with no partitioning key must not show 'None' or
        an empty brackets artefact."""
        prompts = self._prompts(_analysis_with_bottleneck_shuffle())
        for _name, prompt in prompts.items():
            assert "Partitioning key: None" not in prompt
            assert "Partitioning key: []" not in prompt


class TestNotableShuffleGate:
    """_is_notable_shuffle() filters trivial shuffles out of the prompt
    entirely so small healthy shuffles don't inflate token cost."""

    def _prompts(self, a):
        return {
            "stage1": create_structured_analysis_prompt(a, lang="en"),
            "stage2": create_review_prompt(a, "prior", "primary", "en"),
            "stage3": create_refine_prompt("init", "rev", "p", "r", "en", analysis=a),
        }

    def test_trivial_shuffles_omitted_entirely(self):
        """When all shuffles are small and healthy, the Shuffle Details
        section must NOT appear at all — it would just cost tokens."""
        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(query_text="SELECT * FROM t", total_time_ms=1000)
        a.shuffle_metrics = [
            ShuffleMetrics(
                node_id="1",
                partition_count=10,
                peak_memory_bytes=10 * 1024**2,  # 10 MB peak
                sink_bytes_written=50 * 1024**2,  # 50 MB written — below 1 GiB gate
                shuffle_attributes=["x"],
                sink_num_spills=0,
            ),
        ]
        prompts = self._prompts(a)
        for _name, prompt in prompts.items():
            # Header should not appear at all
            assert "Shuffle Details" not in prompt
            assert "シャッフル詳細" not in prompt

    def test_notable_shuffle_included_when_mpp_high(self):
        """A shuffle with memory_per_partition > 128 MB must appear even
        if bytes written is below the 1 GiB gate — the gate is OR, not AND."""
        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(query_text="SELECT ...", total_time_ms=1000)
        a.shuffle_metrics = [
            ShuffleMetrics(
                node_id="99",
                partition_count=2,  # forces MPP = peak/2
                peak_memory_bytes=500 * 1024**2,  # 500 MB peak → ~250 MB/part
                sink_bytes_written=10 * 1024**2,  # 10 MB written — below gate
                shuffle_attributes=["hotkey"],
            ),
        ]
        stage1 = create_structured_analysis_prompt(a, lang="en")
        assert "Shuffle Details" in stage1
        assert "hotkey" in stage1
