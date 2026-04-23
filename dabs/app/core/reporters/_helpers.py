"""Shared helper functions for report generation."""

import re as _re

from ..constants import Severity
from ..i18n import gettext as _


def _strip_first_h2(text: str) -> str:
    """Remove the first ## heading line from markdown text."""
    return _re.sub(r"^##\s+.*\n+", "", text.lstrip(), count=1)


def _strip_all_h2(text: str) -> str:
    """Remove ALL ## heading lines from markdown text (for Appendix sub-content)."""
    return _re.sub(r"^##\s+.*\n+", "", text, flags=_re.MULTILINE)


def _get_complexity_label(score: int) -> str:
    """Convert complexity score to human-readable label."""
    if score <= 3:
        return _("Low")
    elif score <= 7:
        return _("Medium")
    elif score <= 12:
        return _("High")
    else:
        return _("Very High")


def _extract_operator_short_name(node_name: str, bottleneck_type: str | None) -> str:
    """Extract a meaningful short name from operator node_name.

    For Scan operators, extracts the table name.
    For other operators, applies smart truncation.

    Args:
        node_name: Full operator name (e.g., "Scan tpcds.tpcds_sf10000_delta_lc:store_sales")
        bottleneck_type: Type of bottleneck (scan, join, shuffle, etc.)

    Returns:
        Short display name for the operator
    """
    if not node_name:
        return "-"

    # For Scan operators, extract table name
    if bottleneck_type == "scan" or node_name.lower().startswith(("scan ", "photonscan ")):
        # Pattern: "Scan catalog.schema.table:alias" or "PhotonScan ..."
        parts = node_name.split(" ", 1)
        if len(parts) > 1:
            table_part = parts[1]
            # Remove alias after colon if present
            if ":" in table_part:
                table_part = table_part.split(":")[0]
            # Return "Scan: table_name" format
            op_type = parts[0]
            # Truncate table name if still too long
            if len(table_part) > 40:
                table_part = table_part[:37] + "..."
            return f"{op_type}: {table_part}"

    # For Join operators, try to extract join type
    if bottleneck_type == "join" or "join" in node_name.lower():
        # Keep join type visible
        if len(node_name) > 45:
            return node_name[:42] + "..."
        return node_name

    # Default: smart truncation (keep prefix + suffix for context)
    if len(node_name) > 45:
        return node_name[:42] + "..."

    return node_name


def _severity_to_label(severity: Severity | str) -> str:
    """Convert severity value to display label."""
    labels = {
        "ok": _("Good"),
        "info": _("Info"),
        "medium": _("Warning"),
        "high": _("Alert"),
        "critical": _("Critical"),
    }
    key = severity.value if isinstance(severity, Severity) else severity
    return labels.get(key, str(key))


def _severity_to_icon(severity: Severity | str) -> str:
    """Convert severity value to icon."""
    icons = {
        "ok": "✓",
        "info": "ⓘ",
        "medium": "△",
        "high": "▲",
        "critical": "●",
    }
    key = severity.value if isinstance(severity, Severity) else severity
    return icons.get(key, "")
