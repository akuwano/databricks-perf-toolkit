"""Tests for the post-LLM fix_sql safety net (Path B).

Codex 2026-04-26 review: when the LLM produces an ActionCard whose
``fix`` text describes a SQL action (CLUSTER BY etc.) but leaves
``fix_sql`` empty, fill it ONLY when the binding is unambiguous from
structured analyzer evidence. Per Codex, "fill when ambiguous" is
worse than "leave empty" — a wrong table/column would be a real
customer-facing error, while an empty fix_sql is just an annoyance
the prompt contract (Iter 1) already addresses.

Allowlist (Iter 3 scope):
  - CLUSTER BY recommendations on a unique top-scanned table with
    ``recommended_clustering_keys`` populated.

Outside allowlist → leave fix_sql empty + record internal reason.
"""

from __future__ import annotations

from core.fix_sql_filler import fill_missing_fix_sql
from core.models import ActionCard, TableScanMetrics


def _make_card(fix: str = "", fix_sql: str = "", problem: str = "") -> ActionCard:
    return ActionCard(problem=problem, fix=fix, fix_sql=fix_sql)


def _make_table(
    name: str = "main.base.store_sales",
    recommended_keys: list[str] | None = None,
    current_keys: list[str] | None = None,
) -> TableScanMetrics:
    return TableScanMetrics(
        table_name=name,
        recommended_clustering_keys=recommended_keys or [],
        current_clustering_keys=current_keys or [],
    )


# ---------------------------------------------------------------------------
# No-op cases
# ---------------------------------------------------------------------------


class TestNoOpCases:
    def test_already_has_fix_sql_unchanged(self):
        """If the LLM already produced fix_sql, never overwrite it."""
        card = _make_card(
            fix="Use CLUSTER BY",
            fix_sql="ALTER TABLE foo CLUSTER BY (a);",
        )
        tables = [_make_table(recommended_keys=["b", "c"])]
        out = fill_missing_fix_sql(card, top_scanned_tables=tables)
        assert out.fix_sql == "ALTER TABLE foo CLUSTER BY (a);"

    def test_fix_does_not_mention_sql_action_unchanged(self):
        card = _make_card(fix="Resize the warehouse to Large.")
        tables = [_make_table(recommended_keys=["b", "c"])]
        out = fill_missing_fix_sql(card, top_scanned_tables=tables)
        assert out.fix_sql == ""

    def test_no_top_scanned_tables_unchanged(self):
        card = _make_card(fix="Add CLUSTER BY on the hot column.")
        out = fill_missing_fix_sql(card, top_scanned_tables=[])
        assert out.fix_sql == ""

    def test_no_recommended_keys_anywhere_unchanged(self):
        """Without structured key evidence, binding is ambiguous —
        Codex: leave empty + record reason."""
        card = _make_card(fix="Add CLUSTER BY on the hot column.")
        tables = [_make_table(recommended_keys=[])]
        out = fill_missing_fix_sql(card, top_scanned_tables=tables)
        assert out.fix_sql == ""

    def test_multiple_tables_with_recommendations_unchanged(self):
        """Two candidates → ambiguous → must not pick one."""
        card = _make_card(fix="Apply CLUSTER BY.")
        tables = [
            _make_table(name="main.base.store_sales", recommended_keys=["a"]),
            _make_table(name="main.base.web_sales", recommended_keys=["b"]),
        ]
        out = fill_missing_fix_sql(card, top_scanned_tables=tables)
        assert out.fix_sql == ""


# ---------------------------------------------------------------------------
# Happy path — unique CLUSTER BY binding
# ---------------------------------------------------------------------------


class TestClusterByFill:
    def test_single_table_with_recommended_keys_fills_alter_table(self):
        card = _make_card(
            fix=(
                "Use Liquid Clustering with CLUSTER BY syntax, putting "
                "ss_sold_date_sk first and ss_customer_sk second."
            )
        )
        tables = [
            _make_table(
                name="main.base.store_sales",
                recommended_keys=["ss_sold_date_sk", "ss_customer_sk"],
            )
        ]
        out = fill_missing_fix_sql(card, top_scanned_tables=tables)
        assert (
            out.fix_sql
            == "ALTER TABLE main.base.store_sales CLUSTER BY "
            "(ss_sold_date_sk, ss_customer_sk);"
        )

    def test_only_first_table_has_keys_fills_unambiguously(self):
        """One table has recs, the other doesn't → the one with recs
        is the unique target."""
        card = _make_card(fix="Add CLUSTER BY on the hot column.")
        tables = [
            _make_table(name="main.base.store_sales", recommended_keys=["x"]),
            _make_table(name="main.base.dim_date", recommended_keys=[]),
        ]
        out = fill_missing_fix_sql(card, top_scanned_tables=tables)
        assert "ALTER TABLE main.base.store_sales CLUSTER BY (x);" == out.fix_sql

    def test_returns_new_object_not_mutating_input(self):
        """Defensive: callers may rely on the original card being
        unchanged (immutability simplifies pipelines)."""
        card = _make_card(fix="CLUSTER BY hint", fix_sql="")
        tables = [_make_table(recommended_keys=["a"])]
        out = fill_missing_fix_sql(card, top_scanned_tables=tables)
        assert card.fix_sql == ""  # original untouched
        assert out.fix_sql != ""  # new card has fill


# ---------------------------------------------------------------------------
# Diagnostics — internal reason exposed for eval / logging
# ---------------------------------------------------------------------------


class TestDiagnostics:
    def test_skip_reason_recorded_when_ambiguous_table(self):
        """Codex: 'fix_sql omitted: missing unambiguous table/column
        binding' — make this introspectable."""
        from core.fix_sql_filler import explain_skip_reason

        card = _make_card(fix="Apply CLUSTER BY.")
        tables = [
            _make_table(name="t1", recommended_keys=["a"]),
            _make_table(name="t2", recommended_keys=["b"]),
        ]
        reason = explain_skip_reason(card, top_scanned_tables=tables)
        assert reason
        assert "ambiguous" in reason.lower() or "multiple" in reason.lower()

    def test_skip_reason_empty_when_no_action_required(self):
        """No SQL keyword in fix → no fix_sql expected → no skip
        reason needed."""
        from core.fix_sql_filler import explain_skip_reason

        card = _make_card(fix="Resize the warehouse.")
        tables = [_make_table(recommended_keys=["a"])]
        reason = explain_skip_reason(card, top_scanned_tables=tables)
        assert reason == ""
