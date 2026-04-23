"""Top Alerts must be severity-sorted, numbered, and each Top-5 action
must cite which alert number(s) it addresses.

Regression: a real shared report listed alerts in detection order
(HIGH → CRITICAL → HIGH → HIGH → CRITICAL) and Top-5 actions without
any reference to which alert each action solves. Users could not map
actions to alerts, making the report hard to act on.
"""

from __future__ import annotations

import re

import pytest
from core.constants import Severity
from core.models import ActionCard, Alert

# ---------------------------------------------------------------------------
# Module under test (not yet implemented — Red phase).
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _restore_locale():
    from core.i18n import get_language, set_language

    prev = get_language()
    set_language("ja")
    yield
    set_language(prev)


def _alert(sev: Severity, category: str, message: str, metric: str = "") -> Alert:
    return Alert(severity=sev, category=category, message=message, metric_name=metric)


# ---------------------------------------------------------------------------
# (1) sort_alerts_by_severity — CRITICAL → HIGH → MEDIUM → INFO, stable
# ---------------------------------------------------------------------------


class TestSortAlertsBySeverity:
    def test_critical_comes_before_high(self):
        from core.reporters.alert_crossref import sort_alerts_by_severity

        alerts = [
            _alert(Severity.HIGH, "io", "remote read"),
            _alert(Severity.CRITICAL, "spill", "disk spill"),
            _alert(Severity.HIGH, "io", "pruning"),
            _alert(Severity.CRITICAL, "shuffle", "shuffle dom"),
            _alert(Severity.MEDIUM, "photon", "low photon"),
        ]
        sorted_alerts = sort_alerts_by_severity(alerts)
        severities = [a.severity for a in sorted_alerts]
        assert severities == [
            Severity.CRITICAL,
            Severity.CRITICAL,
            Severity.HIGH,
            Severity.HIGH,
            Severity.MEDIUM,
        ]

    def test_stable_within_same_severity(self):
        from core.reporters.alert_crossref import sort_alerts_by_severity

        alerts = [
            _alert(Severity.HIGH, "io", "first"),
            _alert(Severity.HIGH, "io", "second"),
            _alert(Severity.HIGH, "io", "third"),
        ]
        out = sort_alerts_by_severity(alerts)
        assert [a.message for a in out] == ["first", "second", "third"]


# ---------------------------------------------------------------------------
# (2) match_card_to_alert_numbers — return 1-indexed list of matched alerts
# ---------------------------------------------------------------------------


def _alerts_default() -> list[Alert]:
    """Ordered: #1 spill, #2 shuffle, #3 remote_read, #4 file pruning, #5 byte pruning."""
    return [
        _alert(Severity.CRITICAL, "spill", "大量のディスクスピルが発生しています"),
        _alert(Severity.CRITICAL, "shuffle", "Shuffle操作が全体時間の41.6%を占めています"),
        _alert(Severity.HIGH, "io", "リモート読み取り率が非常に高いです", "remote_read_ratio"),
        _alert(Severity.HIGH, "io", "ファイルプルーニング効率が低いです", "filter_rate"),
        _alert(Severity.HIGH, "io", "バイトプルーニング効率が低いです", "bytes_pruning_ratio"),
    ]


class TestMatchCardToAlertNumbers:
    def test_shuffle_card_matches_shuffle_alert(self):
        from core.reporters.alert_crossref import match_card_to_alert_numbers

        card = ActionCard(
            problem="Shuffleパーティション数の増加（REPARTITION）",
            fix="REPARTITION...",
            root_cause_group="shuffle_overhead",
        )
        nums = match_card_to_alert_numbers(card, _alerts_default())
        # Should match alert #2 (shuffle). Spill also a common secondary but
        # the primary should at minimum include #2.
        assert 2 in nums

    def test_pruning_card_matches_both_pruning_alerts(self):
        from core.reporters.alert_crossref import match_card_to_alert_numbers

        card = ActionCard(
            problem="Liquid Clusteringキーの変更",
            fix="ALTER TABLE ... CLUSTER BY ...",
            root_cause_group="scan_efficiency",
        )
        nums = match_card_to_alert_numbers(card, _alerts_default())
        assert 4 in nums and 5 in nums

    def test_where_filter_card_addresses_pruning_and_spill(self):
        """INSERT SELECT + WHERE filter reduces scanned bytes (pruning) and
        also reduces downstream volume (spill mitigation). Match both."""
        from core.reporters.alert_crossref import match_card_to_alert_numbers

        card = ActionCard(
            problem="INSERT SELECTにWHERE句による期間フィルタを追加",
            fix="WHERE ts BETWEEN '2025-05-01' AND '2025-11-01'",
            root_cause_group="scan_efficiency",
        )
        nums = match_card_to_alert_numbers(card, _alerts_default())
        assert 4 in nums and 5 in nums

    def test_spill_card_matches_spill_alert(self):
        from core.reporters.alert_crossref import match_card_to_alert_numbers

        card = ActionCard(
            problem="I/O delay due to disk spill",
            fix="increase memory / REPARTITION",
            root_cause_group="spill_memory_pressure",
        )
        nums = match_card_to_alert_numbers(card, _alerts_default())
        assert nums == [1]

    def test_remote_read_matched_via_metric_name(self):
        from core.reporters.alert_crossref import match_card_to_alert_numbers

        card = ActionCard(
            problem="キャッシュ効果の確認（再実行）",
            fix="re-run to benefit from disk cache",
            root_cause_group="cache_utilization",
        )
        nums = match_card_to_alert_numbers(card, _alerts_default())
        assert 3 in nums

    def test_unrelated_card_returns_empty(self):
        from core.reporters.alert_crossref import match_card_to_alert_numbers

        card = ActionCard(
            problem="ソーステーブルのフォーマット・圧縮コーデック確認",
            fix="check compression codec",
            root_cause_group="sql_pattern",
        )
        nums = match_card_to_alert_numbers(card, _alerts_default())
        # No direct alert → empty list (caller renders as "全般").
        assert nums == []


# ---------------------------------------------------------------------------
# (3) Report rendering — Top Alerts shows numbered list sorted by severity
# ---------------------------------------------------------------------------


class TestTopAlertsSection:
    def test_top_alerts_sorted_and_numbered(self):
        """Integration: Top Alerts section renders `**#1**` prefix and
        places CRITICAL alerts before HIGH."""
        from core.models import (
            BottleneckIndicators,
            ProfileAnalysis,
            QueryMetrics,
        )
        from core.reporters import generate_report

        a = ProfileAnalysis()
        a.query_metrics = QueryMetrics(query_text="INSERT INTO t SELECT ...", total_time_ms=60000)
        a.bottleneck_indicators = BottleneckIndicators()
        a.bottleneck_indicators.alerts = _alerts_default()

        md = generate_report(a, lang="ja")
        # Extract Top Alerts section
        m = re.search(r"## 2\. [^\n]+\n(.+?)(?=##|$)", md, re.DOTALL)
        assert m, "Top Alerts section missing"
        section = m.group(1)
        # CRITICAL appears before HIGH
        spill_pos = section.find("ディスクスピル")
        shuffle_pos = section.find("Shuffle操作")
        remote_pos = section.find("リモート読み取り")
        assert spill_pos >= 0 and shuffle_pos >= 0 and remote_pos >= 0
        assert max(spill_pos, shuffle_pos) < remote_pos, (
            f"CRITICAL alerts must render before HIGH — "
            f"spill@{spill_pos}, shuffle@{shuffle_pos}, remote@{remote_pos}"
        )
        # Numbered prefix present for each
        assert "**#1**" in section
        assert "**#5**" in section


# ---------------------------------------------------------------------------
# (4) Top-5 recommendations include `→ アラート #N` tag
# ---------------------------------------------------------------------------


class TestTop5AlertReferences:
    def test_each_action_shows_alert_reference(self):
        from core.reporters.summary import generate_top5_recommendations_section

        alerts = _alerts_default()
        cards = [
            ActionCard(
                problem="Shuffleパーティション数の増加",
                fix="REPARTITION(...)",
                expected_impact="high",
                effort="low",
                root_cause_group="shuffle_overhead",
            ),
            ActionCard(
                problem="Liquid Clusteringキーの変更",
                fix="ALTER TABLE ...",
                expected_impact="high",
                effort="medium",
                root_cause_group="scan_efficiency",
            ),
            ActionCard(
                problem="ソーステーブルのフォーマット確認",
                fix="check",
                expected_impact="low",
                effort="low",
                root_cause_group="sql_pattern",
            ),
        ]
        md = generate_top5_recommendations_section(cards, alerts=alerts)
        # Shuffle action references #2
        assert re.search(r"Shuffleパーティション数の増加.+アラート.+#2", md, re.DOTALL)
        # Clustering action references #4 and #5
        assert re.search(r"Liquid Clusteringキー.+#4.+#5", md, re.DOTALL) or re.search(
            r"Liquid Clusteringキー.+#5.+#4", md, re.DOTALL
        )
        # Unmatched card renders "全般"
        assert re.search(r"フォーマット.+全般", md, re.DOTALL)


class TestTop5OrderingFollowsAlertSeverity:
    def test_critical_addressing_card_before_high_addressing_card(self):
        """When both cards address alerts, the one whose highest-severity
        alert is CRITICAL must come before the one whose highest is HIGH."""
        from core.reporters.summary import generate_top5_recommendations_section

        alerts = _alerts_default()  # #1,#2 CRITICAL; #3,#4,#5 HIGH
        cards = [
            # HIGH-addressing (pruning)
            ActionCard(
                problem="Liquid Clusteringキー変更",
                fix="ALTER ...",
                expected_impact="medium",
                effort="medium",
                root_cause_group="scan_efficiency",
            ),
            # CRITICAL-addressing (shuffle)
            ActionCard(
                problem="REPARTITION増加",
                fix="repartition",
                expected_impact="medium",
                effort="low",
                root_cause_group="shuffle_overhead",
            ),
        ]
        md = generate_top5_recommendations_section(cards, alerts=alerts)
        repartition_pos = md.find("REPARTITION増加")
        clustering_pos = md.find("Liquid Clusteringキー変更")
        assert repartition_pos >= 0 and clustering_pos >= 0
        assert repartition_pos < clustering_pos, (
            f"CRITICAL-addressing action must come before HIGH — "
            f"REPARTITION@{repartition_pos}, Clustering@{clustering_pos}"
        )
