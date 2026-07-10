"""Outbound delivery-failure feedback loop (the capped retry budget).

Covers both failure surfaces:
  - synchronous send rejections (carrier spam filter, opt-out, too-long) →
    the session records the failure and re-queues a recovery turn;
  - asynchronous delivery-failure webhooks (text.delivery_failed,
    imessage.delivery_failed, message.bounced / message.failed) → the gateway
    wakes the session via run_consult.

And the budget mechanics: at most OUTBOUND_FAILURE_MAX_ATTEMPTS sends per
logical reply, shared across both surfaces, reset on inbound / delivered /
TTL, replay-deduped webhooks.
"""

import asyncio
import json
import time
import types

import pytest

from inkbox_codex import gateway
from inkbox_codex import sessions as sessions_mod
from inkbox_codex.config import BridgeConfig
from inkbox_codex.sessions import ContactSession, _Turn

MAX = gateway.OUTBOUND_FAILURE_MAX_ATTEMPTS


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    """aiohttp isn't installed in tests; stub the json_response the handlers use."""
    def json_response(payload):
        return types.SimpleNamespace(text=json.dumps(payload), payload=payload)
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(json_response=json_response))


@pytest.fixture(autouse=True)
def _isolated_state_dir(tmp_path, monkeypatch):
    # Keep any session-state writes off the real home dir.
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))


# ── Fakes ────────────────────────────────────────────────────────────────


class _FakeSession:
    """Captures the prompt run_consult was called with."""

    def __init__(self):
        self.consulted = []

    async def run_consult(self, prompt):
        self.consulted.append(prompt)
        return ""


class _FakeSessions:
    def __init__(self):
        self.by_id = {}

    def get(self, chat_id):
        return self.by_id.setdefault(chat_id, _FakeSession())


def _gw():
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False))
    gw.sessions = _FakeSessions()
    return gw


async def _drain():
    # Let the background _run_failure_turn task finish.
    await asyncio.sleep(0)
    await asyncio.sleep(0)


def _dispatch(gw, envelope, event_type):
    async def go():
        if event_type in ("text.delivery_failed", "text.delivery_unconfirmed"):
            r = await gw._on_text_delivery_failed(envelope, event_type)
        elif event_type == "imessage.delivery_failed":
            r = await gw._on_imessage_delivery_failed(envelope)
        elif event_type == "text.delivered":
            r = gw._on_text_delivered(envelope)
        elif event_type == "imessage.delivered":
            r = gw._on_imessage_delivered(envelope)
        else:
            r = await gw._on_mail_delivery_failed(envelope, event_type)
        await _drain()
        return r
    return asyncio.run(go())


def _sms_fail(text_id="m1", conversation_id="conv-1", remote="+15551234567",
              text="build passed", reason="Message filtered by carrier"):
    return {"data": {"text_message": {
        "id": text_id, "remote_phone_number": remote, "conversation_id": conversation_id,
        "text": text, "error_detail": reason,
    }, "contacts": [{"id": "contact-9"}]}}


# ── Synchronous send rejections (session path) ──────────────────────────────


def _wired_session(gw, mode="sms", meta=None):
    """A ContactSession whose send rejections feed the gateway's shared budget."""
    async def send_fn(*_a, **_k):  # replaced per-test via session._reply
        return None

    session = ContactSession(
        chat_id="contact-9",
        cfg=BridgeConfig(project_dir="/tmp"),
        send_fn=send_fn,
        mcp_server_config={},
        identity_info={"handle": "t", "email": "", "phone": ""},
        on_send_failure=gw._note_sync_send_failure,
    )
    session.mode = mode
    session.reply_meta = meta or {"conversation_id": "conv-1", "to": "+15551234567"}
    return session


class _Blocked(Exception):
    detail = {
        "error": "message_blocked_spam_filter",
        "rule": "markdown_artifacts",
        "message": "Markdown formatting reads as bot traffic in SMS.",
    }


def test_sync_rejection_wakes_with_rule_and_attempt():
    async def scenario():
        gw = _gw()
        session = _wired_session(gw)

        async def boom(_text):
            raise _Blocked()
        session._reply = boom

        await session._deliver_reply(_Turn(text="orig"), "**Jane Doe** is on file.")

        assert session._queue.qsize() == 1
        recovery = session._queue.get_nowait()
        assert recovery.recovery is True
        assert "attempt 1/%d" % MAX in recovery.text
        assert "stage send_rejected" in recovery.text
        assert "Markdown formatting reads as bot traffic in SMS." in recovery.text
        assert "**Jane Doe** is on file." in recovery.text
        assert "[SILENT]" in recovery.text

    asyncio.run(scenario())


def test_sync_retry_budget_caps_total_sends():
    async def scenario():
        gw = _gw()
        session = _wired_session(gw)

        async def boom(_text):
            raise _Blocked()
        session._reply = boom

        # Each fresh (non-recovery) reply that is rejected records one failure.
        for _ in range(MAX + 1):
            await session._deliver_reply(_Turn(text="orig"), "blocked body")

        # Failures 1 and 2 queue a recovery turn; failure 3 goes quiet.
        queued = [session._queue.get_nowait() for _ in range(session._queue.qsize())]
        assert len(queued) == MAX - 1
        assert "attempt 1/%d" % MAX in queued[0].text
        assert "attempt 2/%d" % MAX in queued[1].text

    asyncio.run(scenario())


def test_sync_too_long_reason_flows_through():
    async def scenario():
        gw = _gw()
        session = _wired_session(gw)
        reason = gateway._message_too_long_reason("SMS", "x" * 2000, gateway.SMS_MAX_LENGTH)

        async def boom(_text):
            raise ValueError(reason)
        session._reply = boom

        await session._deliver_reply(_Turn(text="orig"), "y" * 2000)

        recovery = session._queue.get_nowait()
        assert "maximum is %d" % gateway.SMS_MAX_LENGTH in recovery.text

    asyncio.run(scenario())


def test_successful_send_does_not_wake_or_count():
    async def scenario():
        gw = _gw()
        session = _wired_session(gw)

        async def ok(_text):
            return None
        session._reply = ok

        await session._deliver_reply(_Turn(text="orig"), "all good")

        assert session._queue.empty()
        assert gw._outbound_failure_state == {}

    asyncio.run(scenario())


# ── Asynchronous delivery-failure webhooks (gateway path) ───────────────────


def test_carrier_delivery_failed_wakes_agent():
    gw = _gw()
    _dispatch(gw, _sms_fail(), "text.delivery_failed")

    session = gw.sessions.by_id["contact-9"]
    assert len(session.consulted) == 1
    prompt = session.consulted[0]
    assert "SMS" in prompt and "+15551234567" in prompt
    assert "attempt 1/%d" % MAX in prompt
    assert "Message filtered by carrier" in prompt
    assert "build passed" in prompt


def test_carrier_delivery_failed_replay_is_deduped():
    gw = _gw()
    envelope = _sms_fail()
    first = _dispatch(gw, envelope, "text.delivery_failed")
    second = _dispatch(gw, envelope, "text.delivery_failed")

    assert json.loads(second.text)["deduped"] is True
    assert len(gw.sessions.by_id["contact-9"].consulted) == 1


def test_webhook_budget_caps_total_sends():
    gw = _gw()
    # Distinct message ids (so dedup doesn't fire) but the same conversation +
    # remote, so they share one budget.
    for i in range(MAX + 1):
        _dispatch(gw, _sms_fail(text_id=f"m{i}"), "text.delivery_failed")

    # Failures 1 and 2 wake; failure 3 is capped and never runs a turn.
    prompts = gw.sessions.by_id["contact-9"].consulted
    assert len(prompts) == MAX - 1
    assert "attempt 1/%d" % MAX in prompts[0]
    assert "attempt 2/%d" % MAX in prompts[1]


def test_imessage_delivery_failed_wakes_agent():
    gw = _gw()
    envelope = {"data": {"message": {
        "id": "i1", "remote_number": "+15551112222", "conversation_id": "imsg-1",
        "content": "on it", "error_reason": "recipient_unavailable", "status": "error",
    }}}
    _dispatch(gw, envelope, "imessage.delivery_failed")

    # No contacts, but a conversation → keyed by the imessage thread.
    session = gw.sessions.by_id["imessage:imsg-1"]
    assert "iMessage" in session.consulted[0]
    assert "recipient_unavailable" in session.consulted[0]
    assert "on it" in session.consulted[0]


def test_mail_bounce_wakes_and_failed_is_deduped():
    gw = _gw()
    envelope = {"data": {"message": {
        "id": "e1", "to_addresses": ["bob@example.com"], "subject": "Re: pricing",
        "thread_id": "thread-1",
    }}}
    first = _dispatch(gw, envelope, "message.bounced")
    failed = {"data": {"message": dict(envelope["data"]["message"])}}
    second = _dispatch(gw, failed, "message.failed")

    assert json.loads(second.text)["deduped"] is True
    # No contacts, but a thread → keyed by the email thread.
    session = gw.sessions.by_id["email:thread-1"]
    assert len(session.consulted) == 1
    assert "email" in session.consulted[0]
    assert "bounced" in session.consulted[0]


def test_mail_failure_events_are_subscribed():
    assert "message.bounced" in gateway.MAIL_EVENTS
    assert "message.failed" in gateway.MAIL_EVENTS
    assert "message.received" in gateway.MAIL_EVENTS


def test_text_lifecycle_events_are_subscribed():
    # Delivered/failed must be subscribed for their handlers to ever fire.
    for evt in ("text.received", "text.delivered", "text.delivery_failed"):
        assert evt in gateway.TEXT_EVENTS
    for evt in ("imessage.delivered", "imessage.delivery_failed"):
        assert evt in gateway.IMESSAGE_EVENTS


# ── Budget mechanics across surfaces ────────────────────────────────────────


def test_sync_and_webhook_failures_share_one_budget():
    async def scenario():
        gw = _gw()
        session = _wired_session(gw)

        async def boom(_text):
            raise _Blocked()
        session._reply = boom

        # Failure 1 (sync) — keyed by conv-1 + +15551234567.
        await session._deliver_reply(_Turn(text="orig"), "blocked body")
        recovery = session._queue.get_nowait()
        assert "attempt 1/%d" % MAX in recovery.text

        # Failure 2 (webhook) on the SAME conversation + remote must read 2/MAX.
        await gw._on_text_delivery_failed(_sms_fail(text_id="w1"), "text.delivery_failed")
        await _drain()
        assert "attempt 2/%d" % MAX in gw.sessions.by_id["contact-9"].consulted[0]

    asyncio.run(scenario())


def test_delivered_receipt_resets_budget():
    gw = _gw()
    _dispatch(gw, _sms_fail(text_id="m1"), "text.delivery_failed")  # attempt 1
    _dispatch(gw, _sms_fail(text_id="m2"), "text.delivery_failed")  # attempt 2
    # A delivered receipt on the same conversation clears the budget.
    _dispatch(gw, {"data": {"text_message": {
        "direction": "outbound", "remote_phone_number": "+15551234567",
        "conversation_id": "conv-1",
    }}}, "text.delivered")
    _dispatch(gw, _sms_fail(text_id="m3"), "text.delivery_failed")  # fresh attempt 1

    prompts = gw.sessions.by_id["contact-9"].consulted
    assert len(prompts) == 3
    assert "attempt 1/%d" % MAX in prompts[2]


def test_inbound_reset_clears_budget():
    # The received handlers call _clear_outbound_failures with the same routing
    # facts, so a fresh inbound wipes the recorded budget.
    gw = _gw()
    keys = gateway._outbound_failure_keys("sms", "conv-1", "+15551234567", chat_id="contact-9")
    gw._record_outbound_failure(keys)
    assert gw._outbound_failure_state != {}

    gw._clear_outbound_failures("sms", "conv-1", "+15551234567", chat_id="contact-9")
    assert gw._outbound_failure_state == {}


def test_budget_expires_after_ttl():
    gw = _gw()
    _dispatch(gw, _sms_fail(text_id="m1"), "text.delivery_failed")  # attempt 1
    # Age every counter entry past the TTL.
    for entry in gw._outbound_failure_state.values():
        entry["at"] = time.time() - gateway.OUTBOUND_FAILURE_STATE_TTL_SECONDS - 1
    _dispatch(gw, _sms_fail(text_id="m2"), "text.delivery_failed")  # fresh attempt 1

    prompts = gw.sessions.by_id["contact-9"].consulted
    assert len(prompts) == 2
    assert "attempt 1/%d" % MAX in prompts[1]
