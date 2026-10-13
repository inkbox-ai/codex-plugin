"""Transport diagnostics must separate evidence and never print message data."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from tests.live.sms_diagnostics import SmsTrace


DRIVER = "+12025550110"
AGENT = "+12025550111"


def _message(direction, peer, text, status=None, **kwargs):
    return SimpleNamespace(
        direction=direction, remote_phone_number=peer, text=text,
        delivery_status=status, **kwargs,
    )


class _Texts:
    def __init__(self, rows=(), receipt=None, fail=False):
        self.rows, self.receipt, self.fail = rows, receipt, fail
        self.reads = []

    def list(self, number_id, **kwargs):
        self.reads.append(("list", number_id, kwargs))
        if self.fail:
            raise RuntimeError("sensitive exception detail")
        return self.rows

    def get(self, number_id, message_id):
        self.reads.append(("get", number_id, message_id))
        if self.fail:
            raise RuntimeError("sensitive exception detail")
        return self.receipt


def _trace(driver=None, agent=None, **kwargs):
    return SmsTrace(
        SimpleNamespace(texts=driver or _Texts()),
        SimpleNamespace(texts=agent or _Texts()),
        "driver-number", "agent-number", DRIVER, AGENT,
        since=datetime(2026, 1, 1, tzinfo=UTC), **kwargs,
    )


def _evidence(trace):
    return json.loads(trace.summary().removeprefix("sms_transport="))


def test_accepted_and_delivered_request_does_not_claim_inbound_or_response():
    driver = _Texts(receipt=SimpleNamespace(delivery_status="delivered"))
    agent = _Texts()
    trace = _trace(driver, agent)
    trace.sent(SimpleNamespace(id="request-id", delivery_status="queued"), "request-ref")
    assert driver.reads == agent.reads == []

    result = _evidence(trace)
    assert result["request_accepted"] is True
    assert result["request_initial_status"] == "queued"
    assert result["request_current_status"] == "delivered"
    assert result["request_inbound_matches"] == 0
    assert result["response_outbound_candidates"] == 0
    assert result["response_inbound_candidates"] == 0
    assert result["response_token_correlated"] is False
    assert len(driver.reads) + len(agent.reads) == 3
    for texts in (driver, agent):
        for method, _, options in texts.reads:
            if method == "list":
                assert options == {"limit": 100, "start_datetime": "2026-01-01T00:00:00+00:00"}


def test_sent_response_and_missing_inbound_are_distinct_and_token_correlated():
    driver = _Texts(receipt=SimpleNamespace(delivery_status="delivered"))
    agent = _Texts([
        _message("inbound", DRIVER, "request-ref"),
        _message("inbound", "+12025550199", "request-ref"),
        _message("outbound", None, "reply-ref", SimpleNamespace(value="delivered"),
                 recipients=[SimpleNamespace(recipient_phone_number=DRIVER)]),
        _message("outbound", DRIVER, "unrelated reply", "delivered"),
    ])
    trace = _trace(driver, agent, response_token="reply-ref")
    trace.sent(SimpleNamespace(id="request-id", delivery_status="queued"), "request-ref")

    result = _evidence(trace)
    assert result["request_inbound_matches"] == 1
    assert result["response_outbound_candidates"] == 1
    assert result["response_delivery_status_counts"] == {"delivered": 1}
    assert result["response_inbound_candidates"] == 0
    assert result["response_token_correlated"] is True


def test_email_to_sms_diagnostic_matches_both_sides_without_inventing_sms_request():
    driver = _Texts([
        _message("inbound", AGENT, "reply-ref"),
        _message("inbound", DRIVER, "reply-ref"),
        _message("inbound", AGENT, "another reply"),
        _message("outbound", AGENT, "reply-ref"),
    ])
    trace = _trace(driver, _Texts([
        _message("outbound", DRIVER, "reply-ref", "sent"),
    ]), response_token="reply-ref")

    result = _evidence(trace)
    assert "request_accepted" not in result
    assert result["response_inbound_candidates"] == 1
    assert result["response_outbound_candidates"] == 1
    assert len(driver.reads) == 1


def test_unavailable_reads_do_not_look_like_zero_observations_or_leak_errors():
    trace = _trace(_Texts(fail=True), _Texts(fail=True))
    trace.sent(SimpleNamespace(id="secret-id", delivery_status="private status"), "private ref")

    result = _evidence(trace)
    assert result["agent_history_available"] is False
    assert result["driver_history_available"] is False
    assert result["request_status_available"] is False
    assert result["request_current_status"] == result["request_initial_status"] == "unknown"
    assert "request_inbound_matches" not in result
    assert "response_outbound_candidates" not in result
    assert "response_inbound_candidates" not in result
    assert not any(value in json.dumps(result) for value in (
        "sensitive", "secret-id", "private", DRIVER, AGENT,
    ))


def test_unknown_delivery_values_and_message_content_never_reach_output():
    trace = _trace(agent=_Texts([
        _message("outbound", DRIVER, "confidential body", "untrusted status"),
    ]))
    result = _evidence(trace)
    assert result["response_delivery_status_counts"] == {"unknown": 1}
    output = trace.summary() + repr(trace)
    assert all(value not in output for value in (
        "confidential", "untrusted", DRIVER, AGENT, "driver-number", "agent-number",
    ))


def test_known_blocked_delivery_is_distinct_from_unknown_without_details():
    trace = _trace(agent=_Texts([
        _message("outbound", DRIVER, "private body", "blocked_spam_filter"),
    ]))
    result = _evidence(trace)
    assert result["response_delivery_status_counts"] == {"blocked": 1}
    assert "blocked_spam_filter" not in trace.summary()


def test_call_trigger_diagnostic_is_bounded_and_does_not_query_sms_responses():
    rows = [_message("inbound", DRIVER, "different reference") for _ in range(100)]
    rows.append(_message("inbound", DRIVER, "request-ref"))
    driver = _Texts(receipt=SimpleNamespace(delivery_status="sent"))
    trace = _trace(driver, _Texts(rows), expect_response=False)
    trace.sent(SimpleNamespace(id="request-id"), "request-ref")

    result = _evidence(trace)
    assert result["agent_history_limit_reached"] is True
    assert result["request_inbound_matches"] == 0
    assert "response_outbound_candidates" not in result
    assert [read[0] for read in driver.reads] == ["get"]


def test_sms_success_does_not_run_diagnostics_and_timeout_still_fails(monkeypatch):
    from tests.live import test_sms as sms

    diagnostics = []
    def diagnostic():
        diagnostics.append(True)
        return "transport-evidence"

    monkeypatch.setattr(sms, "_inbound_from_aut", lambda _: [SimpleNamespace(id="reply", text="OK")])
    assert sms._wait_new_inbound({}, set(), 1, "probe", diagnostic) == "ok"
    assert diagnostics == []
    with pytest.raises(pytest.fail.Exception, match="transport-evidence"):
        sms._wait_new_inbound({}, set(), 0, "probe", diagnostic)
    assert diagnostics == [True]


def test_voice_timeout_includes_trigger_diagnostics_without_changing_failure():
    from tests.live import test_voice as voice

    with pytest.raises(pytest.fail.Exception, match="stable call pair.*transport-evidence"):
        voice._wait_for_fresh_call_pair(
            lambda: [], lambda: [], set(), set(), not_before=datetime.now(UTC),
            deadline=0, label="test", diagnostic=lambda: "transport-evidence",
        )
