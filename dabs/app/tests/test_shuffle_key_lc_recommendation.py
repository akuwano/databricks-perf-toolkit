"""Make the "shuffle key → Liquid Clustering candidate" guidance reach
both the main analysis LLM and the recommendation generator.

Regression: a shared report had a CRITICAL shuffle-dominant alert on
``ce.lineitem_usagetype`` but no action proposed adding it to the
target table's Liquid Clustering keys. Four gaps compounded:

  (2) The ``lc_shuffle_key_candidate`` section_id was embedded inside
      §1's body — the parser only picks up the first section_id per
      H2 block, so the tag was never independently routable.
  (3) ``CATEGORY_TO_SECTION_IDS`` had no entry for
      ``shuffle``/``skew``/``memory``/``spill`` → ``lc_shuffle_key_candidate``.
  (4) ``recommend_clustering_with_llm`` only fired when
      ``filter_rate < 0.3 AND candidates ≥ 2``. Shuffle-dominant /
      compute-bound queries never triggered the LC LLM even when a
      GiB-scale shuffle key existed.
  (5) The rule-based recommendation path used only SQL-parsed filter
      columns; no ActionCard surfaced the shuffle key as an LC
      candidate.
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Fix #2 — ``lc_shuffle_key_candidate`` section is an independently
# routable H2 block (not embedded inside §1).
# ---------------------------------------------------------------------------


class TestFixTwoSectionIsIndependentlyRoutable:
    def test_parser_exposes_section_id_as_top_level_key(self):
        from core.llm_prompts.knowledge import parse_knowledge_sections

        md_path = Path(__file__).parent.parent / "core" / "knowledge" / "dbsql_tuning.md"
        sections = parse_knowledge_sections(md_path.read_text(encoding="utf-8"))
        assert "lc_shuffle_key_candidate" in sections, (
            "Expected 'lc_shuffle_key_candidate' to be a top-level section_id, "
            "not embedded inside another section's body"
        )

    def test_en_parser_also_exposes_section_id(self):
        from core.llm_prompts.knowledge import parse_knowledge_sections

        md_path = Path(__file__).parent.parent / "core" / "knowledge" / "dbsql_tuning_en.md"
        sections = parse_knowledge_sections(md_path.read_text(encoding="utf-8"))
        assert "lc_shuffle_key_candidate" in sections

    def test_section_body_preserves_shuffle_guidance(self):
        from core.llm_prompts.knowledge import parse_knowledge_sections

        md_path = Path(__file__).parent.parent / "core" / "knowledge" / "dbsql_tuning.md"
        sections = parse_knowledge_sections(md_path.read_text(encoding="utf-8"))
        body = sections.get("lc_shuffle_key_candidate", "")
        # Key phrases that must survive the migration
        assert "シャッフル" in body or "Shuffle" in body
        assert "クラスタリング" in body
        # The acceptable/unacceptable examples should still be there
        assert "採用" in body or "例" in body


# ---------------------------------------------------------------------------
# Fix #3 — shuffle/skew/memory/spill alerts route the shuffle-LC
# knowledge section into the LLM prompt.
# ---------------------------------------------------------------------------


class TestFixThreeCategoryRoutingIncludesShuffleLc:
    def test_shuffle_category_maps_to_lc_shuffle_key_candidate(self):
        from core.llm_prompts.knowledge import CATEGORY_TO_SECTION_IDS

        assert "lc_shuffle_key_candidate" in CATEGORY_TO_SECTION_IDS["shuffle"]

    def test_skew_category_maps_to_lc_shuffle_key_candidate(self):
        from core.llm_prompts.knowledge import CATEGORY_TO_SECTION_IDS

        assert "lc_shuffle_key_candidate" in CATEGORY_TO_SECTION_IDS["skew"]

    def test_memory_category_maps_to_lc_shuffle_key_candidate(self):
        from core.llm_prompts.knowledge import CATEGORY_TO_SECTION_IDS

        assert "lc_shuffle_key_candidate" in CATEGORY_TO_SECTION_IDS["memory"]

    def test_spill_category_maps_to_lc_shuffle_key_candidate(self):
        from core.llm_prompts.knowledge import CATEGORY_TO_SECTION_IDS

        assert "lc_shuffle_key_candidate" in CATEGORY_TO_SECTION_IDS["spill"]


# ---------------------------------------------------------------------------
# Fix #5 — new rule-based ActionCard surfaces "add shuffle key to LC"
# when a notable shuffle key belongs to a scanned table.
# ---------------------------------------------------------------------------


def _make_shuffle(
    node_id: str = "8403",
    peak_gb: float = 981,
    written_gb: float = 146,
    partitions: int = 1024,
    attrs: list[str] | None = None,
):
    from core.models import ShuffleMetrics

    return ShuffleMetrics(
        node_id=node_id,
        partition_count=partitions,
        peak_memory_bytes=int(peak_gb * 1024**3),
        sink_bytes_written=int(written_gb * 1024**3),
        shuffle_attributes=attrs if attrs is not None else ["ce.lineitem_usagetype"],
    )


def _make_scanned_table(name: str = "ck_db_ws.default.mycloudcur", bytes_read: int = 100 * 1024**3):
    from core.models import TableScanMetrics

    return TableScanMetrics(
        table_name=name,
        bytes_read=bytes_read,
        rows_scanned=100_000_000,
        files_read=1000,
        files_pruned=0,
    )


def _make_indicators(shuffle_impact: float = 0.45, filter_rate: float = 0.5):
    from core.models import BottleneckIndicators

    return BottleneckIndicators(
        shuffle_impact_ratio=shuffle_impact,
        filter_rate=filter_rate,
        scan_impact_ratio=0.15,
    )


def _make_qm():
    from core.models import QueryMetrics

    return QueryMetrics(
        total_time_ms=60_000,
        task_total_time_ms=60_000,
        read_bytes=100 * 1024**3,
        read_files_count=1000,
        pruned_files_count=0,
    )


class TestFixFiveShuffleKeyLcCard:
    def test_card_fires_when_shuffle_key_matches_scanned_table(self):
        """Notable shuffle on ``ce.lineitem_usagetype`` + scanned table
        ``...mycloudcur`` with that column must produce the new
        "add shuffle key to LC" ActionCard."""
        from core.analyzers.recommendations import generate_action_cards
        from core.models import ColumnReference, SQLAnalysis

        # SQLAnalysis must tell us that lineitem_usagetype belongs to
        # the scanned table (shuffle-key ↔ table matching path).
        sa = SQLAnalysis()
        sa.columns = [
            ColumnReference(
                column_name="lineitem_usagetype",
                context="group_by",
                table_name="ck_db_ws.default.mycloudcur",
                table_alias="ce",
            )
        ]
        cards = generate_action_cards(
            indicators=_make_indicators(shuffle_impact=0.45),
            hot_operators=[],
            query_metrics=_make_qm(),
            shuffle_metrics=[_make_shuffle(attrs=["ce.lineitem_usagetype"])],
            join_info=[],
            sql_analysis=sa,
            top_scanned_tables=[
                _make_scanned_table("ck_db_ws.default.mycloudcur", bytes_read=100 * 1024**3)
            ],
        )
        shuffle_lc_cards = [
            c
            for c in cards
            if "shuffle" in c.problem.lower()
            and ("liquid clustering" in c.problem.lower() or "クラスタリング" in c.problem)
        ]
        assert shuffle_lc_cards, (
            "Expected a 'shuffle key → Liquid Clustering' card; "
            f"got problems: {[c.problem for c in cards]}"
        )

    def test_card_does_not_fire_when_shuffle_is_tiny(self):
        """Trivial shuffle (≤1 MB written, no memory pressure) must NOT
        trigger the new card — the gate is GiB-scale OR memory-inefficient."""
        from core.analyzers.recommendations import generate_action_cards
        from core.models import SQLAnalysis

        tiny = _make_shuffle(peak_gb=0.001, written_gb=0.001, partitions=8)
        cards = generate_action_cards(
            indicators=_make_indicators(shuffle_impact=0.01),
            hot_operators=[],
            query_metrics=_make_qm(),
            shuffle_metrics=[tiny],
            join_info=[],
            sql_analysis=SQLAnalysis(),
            top_scanned_tables=[_make_scanned_table()],
        )
        shuffle_lc_cards = [
            c
            for c in cards
            if "shuffle" in c.problem.lower()
            and ("liquid clustering" in c.problem.lower() or "クラスタリング" in c.problem)
        ]
        assert not shuffle_lc_cards

    def test_card_does_not_fire_when_shuffle_key_not_on_scanned_table(self):
        """Shuffle key on a column that doesn't belong to any scanned
        table (e.g., a synthetic column) must not trigger the card."""
        from core.analyzers.recommendations import generate_action_cards
        from core.models import SQLAnalysis

        cards = generate_action_cards(
            indicators=_make_indicators(),
            hot_operators=[],
            query_metrics=_make_qm(),
            shuffle_metrics=[_make_shuffle(attrs=["synthetic_partition_id"])],
            join_info=[],
            sql_analysis=SQLAnalysis(),  # empty — no column info
            top_scanned_tables=[_make_scanned_table()],
        )
        shuffle_lc_cards = [
            c
            for c in cards
            if "shuffle" in c.problem.lower()
            and ("liquid clustering" in c.problem.lower() or "クラスタリング" in c.problem)
        ]
        assert not shuffle_lc_cards

    def test_card_is_registered_in_registry(self):
        """Phase 2b (v5.16.19): preservation markers are removed — the
        card's presence is guaranteed by its CardDef registration in
        ``CARDS``. Verify the registry contains the ``shuffle_lc`` entry."""
        from core.analyzers.recommendations_registry import migrated_card_ids

        assert "shuffle_lc" in migrated_card_ids()
