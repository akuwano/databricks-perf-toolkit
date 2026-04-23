"""Cardinality classification (name / type / bounds / stats priority).

Regression: a user's table clustered on ``MYCLOUD_STARTMONTH`` and
``MYCLOUD_STARTYEAR`` failed to trigger the Hierarchical Clustering
card because the original name heuristic only matched underscore-
separated forms (``_month`` / ``_year``). v5.16.17 adds:

  A — name heuristic extended to suffix matches (``startmonth``,
      ``endyear``, etc.)
  C — column type heuristic (DATE/TIMESTAMP/BOOLEAN → low)
  D — exact distinct count from EXPLAIN per-column stats (highest
      confidence)
"""

from __future__ import annotations

from core.extractors import (
    _name_heuristic_cardinality,
    _stats_cardinality_class,
    _type_heuristic_cardinality,
    estimate_clustering_key_cardinality,
)

# ---------------------------------------------------------------------------
# A — name heuristic suffix extension
# ---------------------------------------------------------------------------


class TestNameHeuristicSuffix:
    def test_mycloud_startmonth_is_low(self):
        assert _name_heuristic_cardinality("MYCLOUD_STARTMONTH") == "low"

    def test_mycloud_startyear_is_low(self):
        assert _name_heuristic_cardinality("MYCLOUD_STARTYEAR") == "low"

    def test_billmonth_is_low(self):
        assert _name_heuristic_cardinality("BILL_MONTH") == "low"
        assert _name_heuristic_cardinality("billmonth") == "low"

    def test_endyear_is_low(self):
        assert _name_heuristic_cardinality("end_year") == "low"
        assert _name_heuristic_cardinality("endyear") == "low"

    def test_existing_underscore_forms_still_low(self):
        """Regression: underscore-prefixed forms must keep returning low."""
        assert _name_heuristic_cardinality("order_date") == "low"
        assert _name_heuristic_cardinality("user_dt") == "low"
        assert _name_heuristic_cardinality("sales_month") == "low"

    def test_date_prefix_still_low(self):
        assert _name_heuristic_cardinality("date_key") == "low"

    def test_id_suffix_is_high(self):
        assert _name_heuristic_cardinality("user_id") == "high"
        assert _name_heuristic_cardinality("event_sk") == "high"

    def test_false_positives_avoided(self):
        """The suffix extension must not misclassify unrelated column
        names that happen to end with similar strings."""
        # 'candidate' ends with 'date' but we don't use plain date suffix —
        # expected: unknown (not low).
        assert _name_heuristic_cardinality("candidate") == "unknown"
        # 'mammoth' / 'linear' / 'delay' — none contain month/year/day.
        assert _name_heuristic_cardinality("mammoth") == "unknown"
        assert _name_heuristic_cardinality("linear") == "unknown"
        assert _name_heuristic_cardinality("delay") == "unknown"
        # 'labour' ends with 'our' — make sure the ``hour`` suffix rule
        # does not misclassify it (``hour`` requires an underscore or
        # exact match).
        assert _name_heuristic_cardinality("labour") == "unknown"

    def test_quarter_week_hour_suffix_low(self):
        """Additional date tokens must classify as low via suffix match."""
        # Underscore-prefixed — always low.
        assert _name_heuristic_cardinality("fiscal_quarter") == "low"
        assert _name_heuristic_cardinality("event_week") == "low"
        assert _name_heuristic_cardinality("load_hour") == "low"
        # Concatenated forms — also low for quarter / week.
        assert _name_heuristic_cardinality("STARTQUARTER") == "low"
        assert _name_heuristic_cardinality("endweek") == "low"
        # Standalone ``hour`` — low (explicit match).
        assert _name_heuristic_cardinality("hour") == "low"


# ---------------------------------------------------------------------------
# C — type heuristic
# ---------------------------------------------------------------------------


class TestTypeHeuristic:
    def test_date_type_low(self):
        assert _type_heuristic_cardinality("date") == "low"
        assert _type_heuristic_cardinality("DATE") == "low"

    def test_timestamp_low(self):
        assert _type_heuristic_cardinality("timestamp") == "low"
        assert _type_heuristic_cardinality("TIMESTAMP_NTZ") == "low"

    def test_boolean_low(self):
        assert _type_heuristic_cardinality("boolean") == "low"
        assert _type_heuristic_cardinality("bool") == "low"
        assert _type_heuristic_cardinality("tinyint") == "low"

    def test_other_types_unknown(self):
        assert _type_heuristic_cardinality("int") == "unknown"
        assert _type_heuristic_cardinality("bigint") == "unknown"
        assert _type_heuristic_cardinality("decimal(38,0)") == "unknown"
        assert _type_heuristic_cardinality("string") == "unknown"

    def test_none_or_empty(self):
        assert _type_heuristic_cardinality(None) == "unknown"
        assert _type_heuristic_cardinality("") == "unknown"


# ---------------------------------------------------------------------------
# D — stats-based classification
# ---------------------------------------------------------------------------


class TestStatsClassification:
    def test_small_distinct_count_low(self):
        assert _stats_cardinality_class(12, rows_scanned=1_000_000) == "low"

    def test_large_distinct_count_high(self):
        assert _stats_cardinality_class(500_000, rows_scanned=1_000_000) == "high"

    def test_mid_band_with_high_ratio(self):
        # 50,000 distinct on 100,000 rows = 50% — high
        assert _stats_cardinality_class(50_000, rows_scanned=100_000) == "high"

    def test_mid_band_low_ratio_unknown(self):
        # 50,000 distinct on 10M rows = 0.5% — unknown
        assert _stats_cardinality_class(50_000, rows_scanned=10_000_000) == "unknown"

    def test_none_unknown(self):
        assert _stats_cardinality_class(None, rows_scanned=1000) == "unknown"


# ---------------------------------------------------------------------------
# Integrated estimator — priority ordering
# ---------------------------------------------------------------------------


class TestEstimatorPriority:
    def test_stats_beat_everything(self):
        """Exact distinct count = 12 should return low even if name suggests high."""
        result = estimate_clustering_key_cardinality(
            "user_id",  # name suggests high
            min_v=None,
            max_v=None,
            rows_scanned=1_000_000,
            col_type="bigint",
            distinct_count=12,
        )
        assert result == "low"

    def test_type_over_name_when_no_stats(self):
        """DATE type should return low even if name is 'event_key'."""
        result = estimate_clustering_key_cardinality(
            "event_key",  # name suggests high
            min_v=None,
            max_v=None,
            rows_scanned=1_000_000,
            col_type="DATE",
            distinct_count=None,
        )
        assert result == "low"

    def test_name_when_no_stats_or_type(self):
        """MYCLOUD_STARTMONTH should return low via name heuristic alone."""
        result = estimate_clustering_key_cardinality(
            "MYCLOUD_STARTMONTH",
            min_v=None,
            max_v=None,
            rows_scanned=1_000_000,
            col_type=None,
            distinct_count=None,
        )
        assert result == "low"

    def test_bounds_still_authoritative(self):
        """Bounds-derived classification takes precedence over type/name."""
        # min=1, max=12 → span=12 → low regardless of other signals
        result = estimate_clustering_key_cardinality(
            "anything",
            min_v="1",
            max_v="12",
            rows_scanned=1_000_000,
            col_type="bigint",
            distinct_count=None,
        )
        assert result == "low"

    def test_unknown_when_all_signals_miss(self):
        result = estimate_clustering_key_cardinality(
            "misc_flag",
            min_v=None,
            max_v=None,
            rows_scanned=0,
            col_type=None,
            distinct_count=None,
        )
        assert result == "unknown"

    def test_high_stats_override_type_low(self):
        """A column typed TIMESTAMP with a large distinct count must NOT
        be classified low just because the type hint says so. Stats win."""
        result = estimate_clustering_key_cardinality(
            "event_ts",
            min_v=None,
            max_v=None,
            rows_scanned=1_000_000,
            col_type="TIMESTAMP",
            distinct_count=500_000,
        )
        assert result == "high"

    def test_high_stats_override_name_low(self):
        """Name-based low (``_month`` suffix) must be overridden when the
        actual distinct count turns out to be high — e.g. a misnamed
        column or a derived column holding second-granularity values."""
        result = estimate_clustering_key_cardinality(
            "event_month",  # name suggests low
            min_v=None,
            max_v=None,
            rows_scanned=1_000_000,
            col_type=None,
            distinct_count=500_000,
        )
        assert result == "high"


# ---------------------------------------------------------------------------
# EXPLAIN per-column stats parser — identifier shapes
# ---------------------------------------------------------------------------


class TestExplainColumnStatParser:
    """The regex must accept bare identifiers, backtick-quoted names, and
    dotted qualified names (``schema.table.col``) since Spark/Databricks
    emit all three forms at different times. The leaf name is used as the
    key so downstream lookups by bare column name still work."""

    def _run(self, stats_fragment: str):
        from core.explain_parser import (
            ExplainNode,
            PlanKind,
            extract_scan_column_stats,
        )

        node_name = "Scan parquet default.sales"
        line = f"{node_name} {stats_fragment}"
        node = ExplainNode(
            raw_line=line,
            indent=0,
            kind=PlanKind.PHYSICAL,
            node_name=node_name,
        )
        return extract_scan_column_stats([node])

    def test_bare_identifier(self):
        out = self._run("col1:ColumnStat(distinctCount=Some(12))")
        assert out.get("default.sales", {}).get("col1")
        assert out["default.sales"]["col1"].distinct_count == 12

    def test_backtick_quoted_identifier(self):
        out = self._run("`My Col`:ColumnStat(distinctCount=Some(42))")
        # Backticks stripped for lookup.
        assert out.get("default.sales", {}).get("My Col")
        assert out["default.sales"]["My Col"].distinct_count == 42

    def test_dotted_qualified_identifier(self):
        out = self._run("schema.sales.col_x:ColumnStat(distinctCount=Some(7))")
        # Only the leaf survives as the lookup key.
        assert out.get("default.sales", {}).get("col_x")
        assert out["default.sales"]["col_x"].distinct_count == 7
