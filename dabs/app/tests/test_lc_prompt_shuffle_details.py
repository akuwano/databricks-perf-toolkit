"""Liquid Clustering recommendation prompt must include Shuffle Details
so the LC LLM can recognize runtime shuffle keys as LC candidates.

Regression: a user's report surfaced ``ce.lineitem_usagetype`` as the
dominant shuffle partitioning key (581MB/part, >1GB written) and the
Stage 1/2/3 LLMs correctly recommended REPARTITION on it, but the
separate ``recommend_clustering_with_llm`` call never saw the shuffle
details and skipped it as an LC candidate. Clustering on the shuffle
key reduces shuffle volume for that aggregation, so the LC LLM should
at minimum see the key and decide whether to include it.
"""

from __future__ import annotations

import pytest
from core.llm_prompts.prompts import create_clustering_prompt
from core.models import ShuffleMetrics


@pytest.fixture(autouse=True)
def _restore_locale():
    from core.i18n import get_language, set_language

    prev = get_language()
    yield
    set_language(prev)


def _notable_shuffle() -> ShuffleMetrics:
    """A GiB-scale memory-inefficient shuffle on ``ce.lineitem_usagetype``."""
    return ShuffleMetrics(
        node_id="8403",
        partition_count=1024,
        peak_memory_bytes=981 * 1024**3,
        sink_bytes_written=146 * 1024**3,
        shuffle_attributes=["ce.lineitem_usagetype"],
    )


class TestShuffleDetailsInClusteringPrompt:
    def test_shuffle_partitioning_key_rendered_in_user_prompt(self):
        """The literal shuffle key must reach the LC LLM prompt."""
        _system, user = create_clustering_prompt(
            query_sql="SELECT ce.lineitem_usagetype, SUM(x) FROM ce GROUP BY 1",
            target_table="ck_db_ws.default.ce",
            candidate_columns=[
                {"column": "lineitem_usagetype", "context": "group_by", "operator": "-"}
            ],
            top_scanned_tables=[{"table_name": "ck_db_ws.default.ce", "bytes_read": 100 * 1024**3}],
            filter_rate=0.0,
            read_files_count=1000,
            pruned_files_count=0,
            shuffle_metrics=[_notable_shuffle()],
            lang="en",
        )
        assert "lineitem_usagetype" in user, (
            "Shuffle partitioning key must appear in the LC LLM user prompt"
        )
        # And it should be under the Shuffle Details heading so LLM
        # knows it came from the runtime physical plan, not the SQL parse.
        assert "Shuffle Details" in user

    def test_system_prompt_mentions_shuffle_key_as_lc_candidate(self):
        """The LC system prompt must state the new criterion: dominant
        shuffle key on this table is an LC candidate."""
        system, _user = create_clustering_prompt(
            query_sql="SELECT 1",
            target_table="t",
            candidate_columns=[],
            top_scanned_tables=[],
            filter_rate=0.0,
            read_files_count=0,
            pruned_files_count=0,
            shuffle_metrics=[],
            lang="en",
        )
        lower = system.lower()
        assert "shuffle" in lower, "LC system prompt must mention shuffle keys as a criterion"

    def test_no_shuffle_metrics_omits_section(self):
        """When no notable shuffle exists, the Shuffle Details section
        must NOT appear in the user prompt — it would just cost tokens."""
        _system, user = create_clustering_prompt(
            query_sql="SELECT 1",
            target_table="t",
            candidate_columns=[],
            top_scanned_tables=[],
            filter_rate=0.0,
            read_files_count=0,
            pruned_files_count=0,
            shuffle_metrics=None,
            lang="en",
        )
        assert "Shuffle Details" not in user

    def test_ja_variant_renders_shuffle_details(self):
        """JA locale must render the same content (with JA heading)."""
        _system, user = create_clustering_prompt(
            query_sql="SELECT 1",
            target_table="t",
            candidate_columns=[],
            top_scanned_tables=[],
            filter_rate=0.0,
            read_files_count=0,
            pruned_files_count=0,
            shuffle_metrics=[_notable_shuffle()],
            lang="ja",
        )
        assert "lineitem_usagetype" in user
        # JA section header — use a flexible check for the shuffle label
        assert "シャッフル" in user or "Shuffle" in user

    def test_tiny_shuffle_not_rendered(self):
        """Trivial shuffles (no memory pressure, no spill, tiny write)
        must NOT inflate the LC prompt."""
        tiny = ShuffleMetrics(
            node_id="t",
            partition_count=8,
            peak_memory_bytes=1024,
            sink_bytes_written=0,
            shuffle_attributes=["some_key"],
        )
        _system, user = create_clustering_prompt(
            query_sql="SELECT 1",
            target_table="t",
            candidate_columns=[],
            top_scanned_tables=[],
            filter_rate=0.0,
            read_files_count=0,
            pruned_files_count=0,
            shuffle_metrics=[tiny],
            lang="en",
        )
        assert "some_key" not in user
