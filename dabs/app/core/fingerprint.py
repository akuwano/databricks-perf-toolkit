"""SQL fingerprint generation for query identity matching.

Normalizes SQL text and produces a stable hash so that semantically
equivalent queries (differing only in literal values, whitespace, or
comments) share the same fingerprint.
"""

from __future__ import annotations

import hashlib
import re

FINGERPRINT_VERSION = "v1"


def normalize_sql(sql: str) -> str:
    """Normalize SQL for fingerprinting.

    Steps:
        1. Strip leading/trailing whitespace
        2. Remove SQL comments (single-line and block)
        3. Collapse whitespace to single spaces
        4. Lower-case keywords (full lower-case for simplicity)
        5. Replace numeric literals with placeholder ``?``
        6. Replace string literals with placeholder ``?``
    """
    if not sql:
        return ""

    text = sql.strip()

    # Remove block comments
    text = re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)
    # Remove single-line comments
    text = re.sub(r"--[^\n]*", " ", text)

    # Replace string literals (single-quoted) with placeholder
    text = re.sub(r"'(?:[^'\\]|\\.)*'", "?", text)
    # Replace numeric literals (integers and decimals)
    text = re.sub(r"\b\d+(?:\.\d+)?\b", "?", text)

    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()

    # Lower-case everything
    text = text.lower()

    return text


def generate_fingerprint(sql: str) -> str:
    """Generate a SHA-256 fingerprint from normalized SQL.

    Returns:
        Hex-encoded SHA-256 hash of the normalized SQL text.
        Empty string if the input is empty.
    """
    normalized = normalize_sql(sql)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
