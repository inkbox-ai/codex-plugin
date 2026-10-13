"""Failure-only, bounded SMS transport evidence without message content."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta


_LIMIT = 100
_STATUSES = {
    "queued", "sent", "delivered", "delivery_failed", "delivery_unconfirmed",
    "sending_failed",
}


def _value(value) -> str:
    return str(getattr(value, "value", value) or "").lower()


def _status(message) -> str:
    value = _value(getattr(message, "delivery_status", None))
    return value if value in _STATUSES else "unknown"


def _digits(value) -> str:
    return re.sub(r"\D", "", value or "")


def _matches(message, direction: str, phone: str, token: str | None) -> bool:
    if _value(getattr(message, "direction", None)) != direction:
        return False
    peers = [getattr(message, "remote_phone_number", None)]
    if direction == "outbound":
        peers.extend(
            getattr(recipient, "recipient_phone_number", None)
            for recipient in (getattr(message, "recipients", None) or [])
        )
    if not _digits(phone) or _digits(phone) not in {_digits(peer) for peer in peers}:
        return False
    return token is None or token.lower() in (getattr(message, "text", None) or "").lower()


def _read(operation):
    try:
        return operation()
    except Exception:
        return None


@dataclass(repr=False)
class SmsTrace:
    """Keep correlation data in memory; read transport state only on failure.

    Counts describe at most one recent page, not proof of missing delivery.
    Without a response token, response rows are time-window candidates only.
    """

    driver: object
    agent: object
    driver_number_id: str
    agent_number_id: str
    driver_phone: str
    agent_phone: str
    response_token: str | None = None
    expect_response: bool = True
    since: datetime = field(default_factory=lambda: datetime.now(UTC) - timedelta(seconds=10))
    _receipt: object | None = field(default=None, init=False)
    _request_token: str | None = field(default=None, init=False)
    _accepted: bool = field(default=False, init=False)

    def sent(self, receipt, token: str) -> None:
        self._receipt = receipt
        self._request_token = token
        self._accepted = True

    def summary(self) -> str:
        # Diagnostics must not replace the original assertion if a read fails.
        try:
            evidence = self._evidence()
        except Exception:
            evidence = {"diagnostics_available": False}
        return "sms_transport=" + json.dumps(evidence, sort_keys=True)

    def _evidence(self) -> dict:
        result = {"history_limit": _LIMIT}
        agent_rows = _read(lambda: self.agent.texts.list(
            self.agent_number_id, limit=_LIMIT, start_datetime=self.since.isoformat(),
        ))
        result["agent_history_available"] = agent_rows is not None
        if agent_rows is not None:
            result["agent_history_limit_reached"] = len(agent_rows) >= _LIMIT
        if self._accepted:
            result["request_accepted"] = True
            receipt_id = getattr(self._receipt, "id", None)
            current = _read(lambda: self.driver.texts.get(
                self.driver_number_id, receipt_id,
            )) if receipt_id is not None else None
            result["request_status_available"] = current is not None
            result["request_initial_status"] = _status(self._receipt)
            result["request_current_status"] = _status(current)
            if agent_rows is not None:
                result["request_inbound_matches"] = sum(
                    _matches(row, "inbound", self.driver_phone, self._request_token)
                    for row in agent_rows[:_LIMIT]
                )
        if self.expect_response:
            driver_rows = _read(lambda: self.driver.texts.list(
                self.driver_number_id, limit=_LIMIT, start_datetime=self.since.isoformat(),
            ))
            result["driver_history_available"] = driver_rows is not None
            if driver_rows is not None:
                result["driver_history_limit_reached"] = len(driver_rows) >= _LIMIT
            result["response_token_correlated"] = self.response_token is not None
            if agent_rows is not None:
                outbound = [row for row in agent_rows[:_LIMIT] if _matches(
                    row, "outbound", self.driver_phone, self.response_token,
                )]
                result["response_outbound_candidates"] = len(outbound)
                result["response_delivery_status_counts"] = {
                    status: sum(_status(row) == status for row in outbound)
                    for status in sorted({_status(row) for row in outbound})
                }
            if driver_rows is not None:
                result["response_inbound_candidates"] = sum(
                    _matches(row, "inbound", self.agent_phone, self.response_token)
                    for row in driver_rows[:_LIMIT]
                )
        return result
