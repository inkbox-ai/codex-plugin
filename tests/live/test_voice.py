"""Live voice-call suite — real phone calls, real model, transcript-verified.

Three scenarios, each run against a gateway booted in the matching speech mode (the
workflow sets that up and selects the scenario via VOICE_SCENARIO):

  * inbound_inkbox   — the driver calls the agent; the agent answers with Inkbox
                       STT/TTS and holds a turn.
  * outbound_realtime — the driver texts "call me"; the agent places a call back,
                       powered by the realtime API, and holds a turn.
  * outbound_hosted — the driver texts "call me"; Inkbox Voice AI runs the call,
                      then Codex executes one post-call commitment.

A companion driver process (voice_driver.py) bridges the driver's side of the call
over an Inkbox tunnel and speaks one line. We then read the stored call transcript
and assert both parties spoke — proving the agent reached the caller out loud.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import UTC, datetime

import pytest

# This suite owns the call transport and media path; natural-language routing is
# exercised separately by test_cross_channel.py. Name the required action here so
# model phrasing variance cannot prevent the telephony smoke test from starting.
# A unique reference is still required because two identical no-reply SMS sends
# to the same number trip the server's duplicate_body rule (422).
_CALL_ME_REQUEST = (
    "Use the inkbox_place_call tool now to call my phone number from this SMS. "
    "Do not reply by text."
)


def _call_me_text() -> str:
    """An explicit call action with a fresh body for every send."""
    return f"{_CALL_ME_REQUEST} (ref {uuid.uuid4().hex[:6]})"


REMOTE_KEY = os.environ.get("REMOTE_INKBOX_API_KEY")
AUT_KEY = os.environ.get("CODEX_INKBOX_API_KEY")
BASE_URL = os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai")
REAL = os.environ.get("LIVE_REAL_MODEL") == "1"
SCENARIO = os.environ.get("VOICE_SCENARIO", "")
STATE_FILE = os.environ.get("VOICE_DRIVER_STATE", "/tmp/voice_driver_state.json")
GATEWAY_LOG = os.environ.get("GATEWAY_LOG", "/tmp/gateway.log")
HOSTED_POST_CALL_MARKER = os.environ.get("HOSTED_POST_CALL_MARKER", "")
TIMEOUT_S = float(os.environ.get("LIVE_VOICE_TIMEOUT", "220"))
POLL_EVERY_S = 6.0
HOSTED_POST_CALL_SETTLEMENT_S = 90.0
HOSTED_DUPLICATE_GRACE_S = 2 * POLL_EVERY_S
HOSTED_SCENARIO_TIMEOUT_S = (
    TIMEOUT_S
    + HOSTED_POST_CALL_SETTLEMENT_S
    + HOSTED_DUPLICATE_GRACE_S
    + POLL_EVERY_S
)

pytestmark = pytest.mark.skipif(
    not (REMOTE_KEY and AUT_KEY and REAL),
    reason="voice suite: needs both keys + LIVE_REAL_MODEL=1",
)


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _enum_value(value) -> str:
    return str(getattr(value, "value", value))


def _voicemail_detection_value(call) -> str:
    return _enum_value(getattr(call, "voicemail_detection", ""))


def _spoken_tokens(value: str | None) -> list[str]:
    return re.findall(r"[a-z0-9]+", (value or "").casefold())


def _voice_marker_key(value: str) -> str:
    """Normalize punctuation/case that may change across TTS/PSTN/STT."""
    return "".join(_spoken_tokens(value))


def _record_created_at(record):
    """Return an aware server timestamp from an SDK record."""
    value = getattr(record, "created_at", None)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _sms_target_numbers(message) -> set[str]:
    """All authoritative targets represented by an outbound SMS row."""
    values = [getattr(message, "remote_phone_number", "") or ""]
    values.extend(
        getattr(recipient, "recipient_phone_number", "") or ""
        for recipient in (getattr(message, "recipients", None) or [])
    )
    return {digits for value in values if (digits := _digits(value))}


def _has_after_call_sms_intent(value: str | None) -> bool:
    tokens = _spoken_tokens(value)
    token_set = set(tokens)
    joined = " ".join(tokens)
    after_call = "after" in token_set and bool(
        token_set & {"call", "hangup", "hang", "hung"}
    )
    send = bool(token_set & {"send", "text"})
    sms = bool(token_set & {"sms", "text", "message"}) or "s m s" in joined
    return after_call and send and sms


def _has_sms_action_intent(value: str | None) -> bool:
    """Recognize a persisted send-SMS action without requiring prose wording."""
    tokens = _spoken_tokens(value)
    token_set = set(tokens)
    joined = " ".join(tokens)
    send = bool(token_set & {"send", "text"})
    sms = bool(token_set & {"sms", "text", "message"}) or "s m s" in joined
    return send and sms


def _matching_post_call_action(call, marker):
    """Return the open current-marker SMS action persisted for a hosted call."""
    marker_key = _voice_marker_key(marker)
    for item in getattr(call, "post_call_action_items", None) or []:
        if isinstance(item, dict):
            status = item.get("status", "")
            action = item.get("action", "")
            details = item.get("details", "")
        else:
            status = getattr(item, "status", "")
            action = getattr(item, "action", "")
            details = getattr(item, "details", "")
        value = f"{action} {details}"
        if (
            str(status).casefold() == "open"
            and marker_key in _voice_marker_key(value)
            and _has_sms_action_intent(value)
        ):
            return item
    return None


def _client(key):
    from inkbox import Inkbox

    return Inkbox(api_key=key, base_url=BASE_URL)


def _driver_state() -> dict:
    with open(STATE_FILE) as fh:
        return json.load(fh)


def _aut_phone(aut) -> str:
    nums = aut.phone_numbers.list()
    assert nums, "AUT identity has no phone number"
    return nums[0].number


def _ensure_driver_allowed(aut, driver_number: str) -> None:
    """Allow the live driver through identity-level phone contact rules."""
    handle = aut.mailboxes.list()[0].email_address.split("@", 1)[0]
    rules = aut.phone_identity_contact_rules.list(handle)
    for rule in rules:
        if (
            getattr(rule, "match_target", "") == driver_number
            and str(getattr(rule, "action", "")).lower().endswith("allow")
            and str(getattr(rule, "status", "active")).lower().endswith("active")
        ):
            return
    aut.phone_identity_contact_rules.create(
        handle,
        action="allow",
        match_type="exact_number",
        match_target=driver_number,
    )


def _segments(remote, number_id, call_id):
    """Transcript segments for a call, split by who spoke."""
    # Identity-centered transcript read (SDK 0.4.15+); number_id is vestigial.
    segs = remote.calls.transcripts(call_id)
    rem = [s for s in segs if (getattr(s, "party", "") or "").lower() == "remote" and (s.text or "").strip()]
    loc = [s for s in segs if (getattr(s, "party", "") or "").lower() == "local" and (s.text or "").strip()]
    return segs, rem, loc


# A call can end normally and still never carry a conversation - answering-machine
# detection hanging up on the driver ends it `completed`, hangup_reason=voicemail.
# Transcript rows can still land during teardown, so allow a short grace period
# before giving up rather than polling a finished call for the full timeout.
TERMINAL_FAILURE_STATUSES = {"canceled", "failed"}
ENDED_STATUSES = {"completed"}
ENDED_GRACE_S = float(os.environ.get("LIVE_VOICE_ENDED_GRACE", "15"))


def _call_state(remote, call_id) -> tuple[str, str]:
    """Compact current call state for progress and terminal-failure output."""
    call = remote.calls.get(call_id)
    status = (getattr(call, "status", "") or "").lower()
    fields = (
        f"status={status!r}",
        f"reason={getattr(call, 'reason', None)!r}",
        f"hangup_reason={getattr(call, 'hangup_reason', None)!r}",
        f"started_at={getattr(call, 'started_at', None)!r}",
        f"ended_at={getattr(call, 'ended_at', None)!r}",
    )
    return status, " ".join(fields)


def _wait_for_two_way_call(remote, number_id, call_id, *, deadline=None):
    """Block until the call transcript shows BOTH the agent and the driver spoke."""
    if deadline is None:
        deadline = time.monotonic() + TIMEOUT_S
    last = ""
    ended_at = None
    while time.monotonic() < deadline:
        transcript_state = ""
        try:
            _all, rem, loc = _segments(remote, number_id, call_id)
        except Exception as exc:  # transcripts may 404 until the call is set up
            rem, loc = [], []
            transcript_state = f"transcripts not ready: {exc!r}"
        if not transcript_state and rem and loc:
            agent_said = " | ".join(s.text.strip() for s in rem)
            return agent_said  # the agent reached the caller out loud, in a two-way call
        try:
            status, state = _call_state(remote, call_id)
        except Exception as exc:
            state = f"call state unavailable: {exc!r}"
            status = ""
        progress = transcript_state or f"segments so far: remote={len(rem)} local={len(loc)}"
        last = f"{progress}; {state}"
        if status in TERMINAL_FAILURE_STATUSES:
            pytest.fail(f"call ended before a two-way conversation ({last})")
        if status in ENDED_STATUSES:
            if ended_at is None:
                ended_at = time.monotonic()
            elif time.monotonic() - ended_at > ENDED_GRACE_S:
                pytest.fail(f"call ended without a two-way conversation ({last})")
        time.sleep(POLL_EVERY_S)
    pytest.fail(f"agent never held a two-way call within {TIMEOUT_S:.0f}s ({last})")


def _wait_for_persisted_hosted_request(
    remote,
    number_id,
    call_id,
    aut,
    aut_call_id,
    marker,
    *,
    deadline,
):
    """Wait for both caller intent and the AUT's durable open action item."""
    marker_key = _voice_marker_key(marker)
    assert marker_key
    transcript_ready = False
    action_ready = False
    last_transcript = ""
    last_actions = ""
    while time.monotonic() < deadline:
        try:
            _all, _rem, loc = _segments(remote, number_id, call_id)
            text = " ".join(segment.text.strip() for segment in loc)
            transcript_ready = (
                marker_key in _voice_marker_key(text)
                and _has_after_call_sms_intent(text)
            )
            last_transcript = repr(text)
        except Exception as exc:  # transcripts may 404 until the call is set up
            last_transcript = f"not ready: {exc!r}"
        try:
            aut_call = aut.calls.get(aut_call_id)
            action_ready = _matching_post_call_action(aut_call, marker) is not None
            last_actions = repr(getattr(aut_call, "post_call_action_items", None))
        except Exception as exc:
            last_actions = f"not ready: {exc!r}"
        if transcript_ready and action_ready:
            return
        time.sleep(POLL_EVERY_S)
    pytest.fail(
        "hosted call did not persist both current caller intent and its open "
        "post-call SMS action before the shared deadline "
        f"(transcript_ready={transcript_ready}, action_ready={action_ready}, "
        f"local_transcript={last_transcript}, action_items={last_actions})"
    )


def _gateway_log_text() -> str:
    try:
        with open(GATEWAY_LOG) as fh:
            return fh.read()
    except OSError:
        return ""


def _aut_speech_mode(aut, direction, driver_number):
    """(use_inkbox_tts, use_inkbox_stt) of the agent's most recent answered call
    in `direction` with the driver. Tells Inkbox STT/TTS (True/True) from realtime
    (False/False), so each leg can prove it ran the speech path it claims."""
    tail = _digits(driver_number)[-10:]
    answered = [c for c in aut.calls.list(limit=10)
                if (getattr(c, "direction", "") or "").lower() == direction
                and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail
                and c.use_inkbox_tts is not None]
    assert answered, f"no answered {direction} agent call with the driver found"
    c = answered[0]  # newest first
    return c.use_inkbox_tts, c.use_inkbox_stt


def _hangup_call(client, call_id) -> None:
    """End a live test call through the control API, tolerating an ended race."""
    if not call_id:
        return
    try:
        client.calls.hangup(call_id)
        return
    except Exception as hangup_error:
        deadline = time.monotonic() + 10
        status = "unknown"
        while time.monotonic() < deadline:
            try:
                status = (getattr(client.calls.get(call_id), "status", "") or "").lower()
            except Exception:
                status = "unknown"
            if status in {"completed", "canceled", "failed"}:
                return
            time.sleep(0.5)
        raise RuntimeError(
            f"failed to hang up live test call {call_id}; status={status!r}"
        ) from hangup_error


@pytest.mark.skipif(SCENARIO != "inbound_inkbox", reason="inbound Inkbox STT/TTS leg only")
def test_inbound_call_inkbox_tts_stt():
    """Driver calls the agent; the agent answers via Inkbox STT/TTS and replies."""
    st = _driver_state()
    remote, aut = _client(REMOTE_KEY), _client(AUT_KEY)
    aut_phone = _aut_phone(aut)

    # Server-side contact rules run before the plugin or its local allow-all
    # setting. Whitelisted smoke identities therefore need the driver allowed
    # explicitly or the call is rejected before either media WS connects.
    _ensure_driver_allowed(aut, st["number"])

    # Place the call to the agent, handing Inkbox the driver's own media WS.
    call = remote.calls.place(
        from_number=st["number"],
        to_number=aut_phone,
        client_websocket_url=st["ws_url"],
        voicemail_detection="disabled",
    )
    try:
        agent_said = _wait_for_two_way_call(remote, st["number_id"], call.id)
        assert agent_said, "agent produced no speech on the inbound call"
        persisted = remote.calls.get(call.id)
        assert _voicemail_detection_value(persisted) == "disabled"

        tts, stt = _aut_speech_mode(aut, "inbound", st["number"])
        assert tts and stt, f"inbound call should run Inkbox STT/TTS, got tts={tts} stt={stt}"
    finally:
        _hangup_call(remote, call.id)


@pytest.mark.skipif(SCENARIO != "outbound_realtime", reason="outbound realtime leg only")
def test_outbound_call_realtime():
    """Driver texts 'call me'; the agent places a realtime-powered call and replies."""
    st = _driver_state()
    remote, aut = _client(REMOTE_KEY), _client(AUT_KEY)
    aut_phone = _aut_phone(aut)
    tail = _digits(aut_phone)[-10:]

    def _inbound_from_aut():
        return [c for c in remote.calls.list(limit=30)
                if (getattr(c, "direction", "") or "").lower() == "inbound"
                and _digits(getattr(c, "remote_phone_number", "") or "")[-10:] == tail]

    before = {c.id for c in _inbound_from_aut()}
    remote.texts.send(st["number_id"], to=aut_phone, text=_call_me_text())

    call_id = None
    try:
        # Wait for the agent to dial back, then verify the call transcript.
        deadline = time.monotonic() + TIMEOUT_S
        while time.monotonic() < deadline:
            fresh = [c for c in _inbound_from_aut() if c.id not in before]
            if fresh:
                call_id = fresh[0].id
                break
            time.sleep(POLL_EVERY_S)
        assert call_id, f"agent never placed a call back within {TIMEOUT_S:.0f}s"
        persisted = remote.calls.get(call_id)
        assert _voicemail_detection_value(persisted) == "disabled"

        agent_said = _wait_for_two_way_call(remote, st["number_id"], call_id)
        assert agent_said, "agent produced no speech on the outbound call"

        tts, stt = _aut_speech_mode(aut, "outbound", st["number"])
        assert tts is False and stt is False, \
            f"outbound call must be powered by the realtime API (Inkbox speech off), got tts={tts} stt={stt}"
    finally:
        _hangup_call(remote, call_id)


@pytest.mark.skipif(SCENARIO != "outbound_hosted", reason="outbound Voice AI leg only")
def test_outbound_call_voice_ai_and_post_call_completion():
    """Voice AI calls back, then Codex executes exactly one post-call SMS."""
    st = _driver_state()
    remote, aut = _client(REMOTE_KEY), _client(AUT_KEY)
    aut_numbers = aut.phone_numbers.list()
    assert aut_numbers, "AUT identity has no phone number"
    aut_phone = aut_numbers[0].number
    aut_number_id = aut_numbers[0].id
    aut_tail = _digits(aut_phone)[-10:]
    driver_number = _digits(st["number"])
    driver_tail = driver_number[-10:]

    def driver_calls():
        return [
            call for call in remote.calls.list(limit=30)
            if (getattr(call, "direction", "") or "").lower() == "inbound"
            and _digits(getattr(call, "remote_phone_number", "") or "")[-10:] == aut_tail
        ]

    def aut_calls():
        return [
            call for call in aut.calls.list(limit=30)
            if (getattr(call, "direction", "") or "").lower() == "outbound"
            and _digits(getattr(call, "remote_phone_number", "") or "")[-10:] == driver_tail
        ]

    def aut_outbound_sms():
        return [
            message for message in aut.texts.list(aut_number_id, limit=200)
            if (getattr(message, "direction", "") or "").lower() == "outbound"
            and driver_number in _sms_target_numbers(message)
        ]

    assert HOSTED_POST_CALL_MARKER
    baseline_driver_calls = driver_calls()
    baseline_aut_calls = aut_calls()
    before_driver = {call.id for call in baseline_driver_calls}
    before_aut = {call.id for call in baseline_aut_calls}
    driver_call_watermark = max(
        (
            created_at
            for call in baseline_driver_calls
            if (created_at := _record_created_at(call)) is not None
        ),
        default=datetime.min.replace(tzinfo=UTC),
    )
    aut_call_watermark = max(
        (
            created_at
            for call in baseline_aut_calls
            if (created_at := _record_created_at(call)) is not None
        ),
        default=datetime.min.replace(tzinfo=UTC),
    )
    baseline_sms = aut_outbound_sms()
    before_sms = {message.id for message in baseline_sms}
    baseline_times = [
        created_at for message in baseline_sms
        if (created_at := _record_created_at(message)) is not None
    ]
    sms_watermark = max(baseline_times, default=datetime.min.replace(tzinfo=UTC))
    identity_handle = aut.mailboxes.list()[0].email_address.split("@", 1)[0]
    hosted_config = aut.get_identity(identity_handle).get_hosted_agent_config()
    expected_authority = _enum_value(
        getattr(hosted_config, "authority_mode", "contact_scoped")
    ) or "contact_scoped"
    scenario_deadline = time.monotonic() + HOSTED_SCENARIO_TIMEOUT_S
    pre_hangup_deadline = (
        scenario_deadline
        - HOSTED_POST_CALL_SETTLEMENT_S
        - HOSTED_DUPLICATE_GRACE_S
        - POLL_EVERY_S
    )
    remote.texts.send(st["number_id"], to=aut_phone, text=_call_me_text())

    driver_call_id = None
    aut_call = None
    try:
        while time.monotonic() < pre_hangup_deadline:
            fresh_driver = [
                call
                for call in driver_calls()
                if call.id not in before_driver
                and (created_at := _record_created_at(call)) is not None
                and created_at >= driver_call_watermark
            ]
            fresh_aut = [
                call
                for call in aut_calls()
                if call.id not in before_aut
                and (created_at := _record_created_at(call)) is not None
                and created_at >= aut_call_watermark
            ]
            if fresh_driver:
                driver_call_id = max(
                    fresh_driver,
                    key=_record_created_at,
                ).id
            if fresh_aut:
                aut_call = max(fresh_aut, key=_record_created_at)
            if driver_call_id and aut_call is not None:
                break
            time.sleep(POLL_EVERY_S)
        assert driver_call_id and aut_call is not None
        assert str(getattr(getattr(aut_call, "mode", ""), "value", getattr(aut_call, "mode", ""))) == "hosted_agent"
        assert _voicemail_detection_value(aut_call) == "disabled"
        assert getattr(aut_call, "reason", None)
        assert _enum_value(
            getattr(aut_call, "hosted_agent_authority_mode", "")
        ) == expected_authority
        driver_call = remote.calls.get(driver_call_id)
        driver_created_at = _record_created_at(driver_call)
        aut_created_at = _record_created_at(aut_call)
        assert driver_created_at is not None and aut_created_at is not None
        assert abs((driver_created_at - aut_created_at).total_seconds()) <= 60, (
            "fresh driver and AUT records are not the same hosted call: "
            f"driver_created_at={driver_created_at!r} "
            f"aut_created_at={aut_created_at!r}"
        )
        _wait_for_two_way_call(
            remote,
            st["number_id"],
            driver_call_id,
            deadline=pre_hangup_deadline,
        )
        # Greeting/filler turns do not prove that the request became durable.
        # Require both the caller transcript and the AUT's matching open action
        # item before ending the call, under the same placement deadline.
        _wait_for_persisted_hosted_request(
            remote,
            st["number_id"],
            driver_call_id,
            aut,
            aut_call.id,
            HOSTED_POST_CALL_MARKER,
            deadline=pre_hangup_deadline,
        )
    finally:
        _hangup_call(remote, driver_call_id)

    # The plugin contract ends when the exact-target SMS tool returns the
    # synchronous API-accepted result. Carrier delivery is asynchronous, and
    # recipient-side agent keys can intentionally hide contact-rule-blocked
    # inbox rows. The reconciliation log is emitted only after the captured
    # tool result passes the one-call, exact-target and `sent: true` checks.
    completion = f"hosted post-call reconciliation completed: {aut_call.id}"
    settlement_deadline = (
        scenario_deadline - HOSTED_DUPLICATE_GRACE_S - POLL_EVERY_S
    )
    marker_sms = []
    while time.monotonic() < settlement_deadline:
        gateway_log = _gateway_log_text()
        marker_sms = [
            message for message in aut_outbound_sms()
            if message.id not in before_sms
            and (created_at := _record_created_at(message)) is not None
            and created_at >= sms_watermark
            and _voice_marker_key(HOSTED_POST_CALL_MARKER)
            in _voice_marker_key(getattr(message, "text", "") or "")
        ]
        if completion in gateway_log and marker_sms:
            break
        time.sleep(3)
    gateway_log = _gateway_log_text()
    assert completion in gateway_log, (
        "Codex did not settle the Voice AI post-call commitment"
    )
    # Approval-elicitation logs are an implementation detail: a pre-approved
    # MCP tool may execute without emitting one. The sender-side message rows
    # are the authoritative record of the accepted side effect.
    assert time.monotonic() + HOSTED_DUPLICATE_GRACE_S <= scenario_deadline, (
        "hosted settlement left no room for the duplicate-detection grace window"
    )
    time.sleep(HOSTED_DUPLICATE_GRACE_S)
    marker_sms = [
        message for message in aut_outbound_sms()
        if message.id not in before_sms
        and (created_at := _record_created_at(message)) is not None
        and created_at >= sms_watermark
        and _voice_marker_key(HOSTED_POST_CALL_MARKER)
        in _voice_marker_key(getattr(message, "text", "") or "")
    ]
    current_candidates = [
        {
            "id": getattr(message, "id", None),
            "created_at": getattr(message, "created_at", None),
            "targets": sorted(_sms_target_numbers(message)),
            "text": getattr(message, "text", ""),
        }
        for message in aut_outbound_sms()
        if message.id not in before_sms
        and (created_at := _record_created_at(message)) is not None
        and created_at >= sms_watermark
    ]
    assert len(marker_sms) == 1, (
        "post-call processing did not produce exactly one current-marker SMS "
        "to the authoritative caller: "
        f"candidates={current_candidates!r}"
    )
