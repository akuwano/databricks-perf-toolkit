"""User identity helper for Databricks Apps (L5 feedback loop, 2026-04-26).

Codex insisted that L5 feedback must NOT trust browser-provided
``user_email``. Databricks Apps forwards user identity in trusted
headers — we read those server-side and ignore any client-supplied
identity. If the headers are absent (e.g., local dev without an
authentication proxy), the helper returns the empty string and callers
treat the feedback as anonymous rather than rejecting it.

Header reference (Databricks Apps platform, 2026):
- ``X-Forwarded-Email``: user email (preferred)
- ``X-Forwarded-Preferred-Username``: human-friendly login name
- ``X-Forwarded-User``: subject id (least useful for triage)
"""

from __future__ import annotations

from typing import Iterable

# Order is precedence: first non-empty wins. Email is preferred because
# it's the most useful key for the monthly triage workflow.
_USER_HEADERS: tuple[str, ...] = (
    "X-Forwarded-Email",
    "X-Forwarded-Preferred-Username",
    "X-Forwarded-User",
)


def _first_present(headers: Iterable[tuple[str, str]], names: Iterable[str]) -> str:
    name_set = {n.lower() for n in names}
    for k, v in headers:
        if k.lower() in name_set and (v or "").strip():
            return v.strip()
    return ""


def get_user_email_from_headers(headers) -> str:
    """Extract the user identity from trusted forward headers.

    Args:
        headers: anything iterable of ``(name, value)`` pairs — typically
            ``flask.request.headers``. Case-insensitive.

    Returns:
        The first non-empty value of the precedence order, or empty
        string when none are present (anonymous feedback).
    """
    if headers is None:
        return ""
    try:
        items = list(headers.items())  # Flask Headers
    except AttributeError:
        items = list(headers)
    return _first_present(items, _USER_HEADERS)
