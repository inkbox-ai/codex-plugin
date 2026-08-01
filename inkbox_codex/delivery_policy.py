"""Hosted SMS tool-failure classification from sanitized result metadata."""

from __future__ import annotations

from typing import Any

_TERMINAL_CODES = frozenset({
    "recipient_not_opted_in",
    "recipient_opted_out",
    "recipient_blocked",
    "invalid_phone_number",
    "carrier_rejected",
    "sender_sms_pending",
    "sender_sms_assignment_failed",
    "sender_not_registered",
    "sender_registration_required",
    "messaging_profile_disabled",
    "toll_free_sms_unsupported",
    "unauthorized",
    "forbidden",
    "insufficient_authority",
    "carrier_rate_limit",
    "carrier_unavailable",
    "carrier_temporarily_unavailable",
    "inkbox_duplicate_body",
    "inkbox_carrier_backoff",
})
_RECOVERABLE_CODES = frozenset({
    "sms_too_long",
    "message_too_long",
    "message_blocked_spam_filter",
    "content_flagged_as_spam",
    "content_rejected_by_carrier",
    "content_blocked_by_policy",
})
_COMMIT_AMBIGUOUS_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_TERMINAL_MARKERS = (
    "opted out",
    "opt-out",
    "not opted in",
    "not authorized",
    "unauthorized",
    "forbidden",
    "invalid number",
    "invalid phone",
    "unreachable",
    "unknown subscriber",
    "cannot receive",
    "unsafe",
    "harmful",
    "abusive",
    "harassment",
    "threatening",
    "illegal content",
    "duplicate_body",
    "carrier_backoff",
    "carrier_unavailable",
    "carrier temporarily unavailable",
    "rate limit",
    "timed out",
    "timeout",
)
_RECOVERABLE_MARKERS = (
    "invalid argument",
    "invalid params",
    "required property",
    "missing required",
    "schema",
    "format",
    "e.164",
    "maximum is",
    "too long",
    "must be",
    "specify exactly one",
    "message_blocked_spam_filter",
    "content_flagged_as_spam",
    "content_rejected_by_carrier",
    "content_blocked_by_policy",
)


def sms_tool_failure_kind(
    *,
    error_code: Any = None,
    rule: Any = None,
    status_code: Any = None,
    message: Any = None,
) -> str:
    """Return recoverable, terminal, or unknown without retaining raw detail."""
    code = str(error_code or "").strip().lower()
    detail = " ".join(
        str(value or "").strip().lower()
        for value in (code, rule, message)
        if value
    )
    try:
        status = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        status = None

    if (
        code in _TERMINAL_CODES
        or status in _COMMIT_AMBIGUOUS_STATUSES
        or any(marker in detail for marker in _TERMINAL_MARKERS)
    ):
        return "terminal"
    if (
        code in _RECOVERABLE_CODES
        or any(marker in detail for marker in _RECOVERABLE_MARKERS)
    ):
        return "recoverable"
    if status in {401, 403}:
        return "terminal"
    return "unknown"
