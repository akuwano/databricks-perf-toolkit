"""Shared helpers for analyzer submodules."""

from ..constants import Severity
from ..models import Alert, BottleneckIndicators

# Conflicting recommendation pairs - if one exists, the other should be flagged
CONFLICTING_RECOMMENDATIONS = {
    # Scale up vs scale down
    "cluster:scale_up": ["cluster:scale_down"],
    "cluster:scale_down": ["cluster:scale_up"],
    # Partition increase vs decrease
    "partition:increase": ["partition:decrease"],
    "partition:decrease": ["partition:increase"],
    # Memory increase vs I/O optimization first
    "memory:increase": ["io:optimize_first"],
}


def _add_alert(
    indicators: "BottleneckIndicators",
    severity: Severity,
    category: str,
    message: str,
    metric_name: str = "",
    current_value: str = "",
    threshold: str = "",
    recommendation: str = "",
    is_actionable: bool = True,
) -> Alert:
    """Add a structured alert to indicators with conflict detection.

    Also adds to legacy warnings/critical_issues for backward compatibility.
    """
    alert = Alert(
        severity=severity,
        category=category,
        message=message,
        metric_name=metric_name,
        current_value=current_value,
        threshold=threshold,
        recommendation=recommendation,
        is_actionable=is_actionable,
    )

    # Check for conflicts with existing alerts
    alert_id = alert.alert_id
    for existing in indicators.alerts:
        existing_id = existing.alert_id
        if existing_id in CONFLICTING_RECOMMENDATIONS:
            if alert_id in CONFLICTING_RECOMMENDATIONS[existing_id]:
                alert.conflicts_with.append(existing_id)
                existing.conflicts_with.append(alert_id)

    indicators.alerts.append(alert)

    # Backward compatibility - also add to string-based lists
    if severity == Severity.CRITICAL:
        indicators.critical_issues.append(message)
    elif severity in (Severity.HIGH, Severity.MEDIUM):
        indicators.warnings.append(message)

    if recommendation:
        indicators.recommendations.append(recommendation)

    return alert
