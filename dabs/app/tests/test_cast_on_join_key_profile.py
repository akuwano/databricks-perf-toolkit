"""Detect implicit CAST on JOIN key from profile JSON alone.

Regression: a user's Databricks SQL query profile showed
``on CAST(ce.lineitem_usageaccountid AS BIGINT) and a.account_id``
in a Left Outer Join node. The CAST wrapper blocks predicate pushdown
and inflates hash-table memory, but the existing detector in
``analyzers/explain_analysis.py`` only fires when EXPLAIN EXTENDED is
attached. This module adds a parallel profile-only detector based on
``NodeMetrics.join_keys_left/right`` so the alert fires even without
EXPLAIN.
"""

from __future__ import annotations

from core.analyzers.bottleneck import calculate_bottleneck_indicators
from core.constants import Severity
from core.models import NodeMetrics, QueryMetrics


def _join_node(
    left_keys: list[str],
    right_keys: list[str],
    *,
    node_id: str = "7",
    node_name: str = "Left Outer Join",
) -> NodeMetrics:
    return NodeMetrics(
        node_id=node_id,
        node_name=node_name,
        duration_ms=60_000,
        join_keys_left=left_keys,
        join_keys_right=right_keys,
    )


def _qm() -> QueryMetrics:
    return QueryMetrics(total_time_ms=60_000, task_total_time_ms=60_000)


class TestProfileCastDetection:
    def test_cast_on_left_key_fires_critical_alert(self):
        """The user's reported case: CAST on the left join key.
        Severity is CRITICAL because implicit CAST defeats DFP /
        runtime filters and can dramatically inflate hash-table
        memory — a severe performance cliff even though correctness
        is preserved."""
        nodes = [
            _join_node(
                left_keys=["CAST(ce.lineitem_usageaccountid AS BIGINT)"],
                right_keys=["a.account_id"],
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        cast_alerts = [a for a in bi.alerts if a.metric_name == "implicit_cast_on_join_key"]
        assert cast_alerts, "Expected CAST-on-join-key alert when profile shows CAST in LEFT_KEYS"
        assert cast_alerts[0].severity == Severity.CRITICAL
        assert bi.implicit_cast_on_join_key is True

    def test_cast_on_right_key_also_detected(self):
        nodes = [
            _join_node(
                left_keys=["ce.account_id"],
                right_keys=["CAST(a.account_id AS BIGINT)"],
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        cast_alerts = [a for a in bi.alerts if a.metric_name == "implicit_cast_on_join_key"]
        assert cast_alerts
        assert bi.implicit_cast_on_join_key is True

    def test_no_cast_no_alert(self):
        """Plain column refs on both sides must NOT trigger the alert."""
        nodes = [
            _join_node(
                left_keys=["ce.lineitem_usageaccountid"],
                right_keys=["a.account_id"],
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        cast_alerts = [a for a in bi.alerts if a.metric_name == "implicit_cast_on_join_key"]
        assert not cast_alerts
        assert bi.implicit_cast_on_join_key is False

    def test_non_join_node_ignored(self):
        """CAST inside a Scan/Aggregate expression must NOT be flagged as
        a JOIN-key cast (the metric is specifically about JOIN keys)."""
        # Scan node happens to store CAST in its filter_conditions, but
        # it has no join_keys_left/right — must not trigger the alert.
        scan = NodeMetrics(
            node_id="3",
            node_name="Scan databricks_poc.public.ticket",
            duration_ms=100_000,
            filter_conditions=["CAST(col AS DATE) = '2025-01-01'"],
        )
        bi = calculate_bottleneck_indicators(_qm(), [scan], [], [])
        cast_alerts = [a for a in bi.alerts if a.metric_name == "implicit_cast_on_join_key"]
        assert not cast_alerts

    def test_multiple_casts_summarized(self):
        """When multiple CAST sites exist, the alert lists up to 3
        examples with a ``+N more`` suffix if more exist."""
        nodes = [
            _join_node(
                left_keys=[
                    "CAST(t1.a AS BIGINT)",
                    "CAST(t1.b AS STRING)",
                    "CAST(t1.c AS DOUBLE)",
                    "CAST(t1.d AS DATE)",
                ],
                right_keys=["t2.a", "t2.b", "t2.c", "t2.d"],
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        cast_alerts = [a for a in bi.alerts if a.metric_name == "implicit_cast_on_join_key"]
        assert cast_alerts
        msg = cast_alerts[0].message
        # Should mention at least one column verbatim
        assert "t1.a" in msg or "t1.b" in msg or "t1.c" in msg
        # Should indicate there's more beyond the top 3
        assert "+1 more" in msg or "+" in msg

    def test_cast_in_comment_or_string_literal_still_flagged(self):
        """Known limitation: the regex does not lex SQL, so a ``CAST(``
        substring inside a comment or string literal inside a join key
        expression would still trigger the alert. This test pins that
        behavior so future edits that introduce full tokenization
        deliberately update it instead of regressing silently.

        In practice profile JSON join key strings are structured
        expressions (not raw SQL with /* comments */) so the false
        positive is theoretical, but the contract is documented here."""
        nodes = [
            _join_node(
                # Hypothetical join key where the string "CAST(" appears
                # inside what would be a quoted literal. Profile JSON
                # does not deliver this shape in practice, but the regex
                # cannot distinguish it from a genuine CAST expression.
                left_keys=["'... CAST(...) ...' = t1.x"],
                right_keys=["t2.x"],
            )
        ]
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        cast_alerts = [a for a in bi.alerts if a.metric_name == "implicit_cast_on_join_key"]
        # Current contract: regex-based, flagged. If a future PR
        # introduces proper tokenization this assertion should flip.
        assert cast_alerts, "Regex-based detector flags CAST(... inside literals"

    def test_numeric_widening_vs_string_numeric_both_flagged(self):
        """Both numeric-widening (BIGINT ← INT) and type-family mixing
        (BIGINT ← STRING) produce implicit CAST; both must be flagged.
        The detector does not currently distinguish them, and that is
        intentional — the DFP / hash-inflation cost applies to both."""
        nodes_numeric = [
            _join_node(
                left_keys=["CAST(t1.id AS BIGINT)"],
                right_keys=["t2.id"],
                node_id="n1",
            )
        ]
        nodes_string = [
            _join_node(
                left_keys=["CAST(t1.id AS STRING)"],
                right_keys=["t2.id"],
                node_id="n2",
            )
        ]
        bi_num = calculate_bottleneck_indicators(_qm(), nodes_numeric, [], [])
        bi_str = calculate_bottleneck_indicators(_qm(), nodes_string, [], [])
        assert any(a.metric_name == "implicit_cast_on_join_key" for a in bi_num.alerts)
        assert any(a.metric_name == "implicit_cast_on_join_key" for a in bi_str.alerts)


class TestProfileAndExplainCoexistence:
    """When EXPLAIN is also attached, the two detectors must not
    double-fire the same alert."""

    def test_explain_detector_skips_when_profile_already_fired(self):
        """``enhance_bottleneck_with_explain`` should see that
        ``implicit_cast_on_join_key`` is already True and not add a
        duplicate CRITICAL alert."""
        from core.analyzers.explain_analysis import enhance_bottleneck_with_explain
        from core.explain_parser import ExplainExtended

        nodes = [
            _join_node(
                left_keys=["CAST(ce.account_id AS BIGINT)"],
                right_keys=["a.account_id"],
            )
        ]
        # Step 1: profile detector fires
        bi = calculate_bottleneck_indicators(_qm(), nodes, [], [])
        profile_alert_count = sum(
            1 for a in bi.alerts if a.metric_name == "implicit_cast_on_join_key"
        )
        assert profile_alert_count == 1

        # Step 2: EXPLAIN with a join cast site is attached later
        # (simulated — the actual ExplainExtended has an implicit_cast_sites
        # list). We construct one manually here.
        from core.explain_parser import ImplicitCastSite

        explain = ExplainExtended()
        explain.implicit_cast_sites = [
            ImplicitCastSite(
                column_ref="ce.account_id",
                to_type="BIGINT",
                context="join",
            )
        ]
        bi = enhance_bottleneck_with_explain(bi, explain)
        # Total count must still be 1 (EXPLAIN layer did not double-fire).
        total_count = sum(1 for a in bi.alerts if a.metric_name == "implicit_cast_on_join_key")
        assert total_count == 1, (
            f"Expected at most 1 CAST-on-join alert when both profile and "
            f"EXPLAIN detect the same issue; got {total_count}"
        )
