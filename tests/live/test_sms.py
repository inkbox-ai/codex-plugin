"""Live SMS suite — the same questions as the email suite, over real SMS.

SMS differs from email: agent-to-agent SMS skips the START opt-in (the server
bypasses it for inter-agent traffic), and outbound SMS is subject to carrier +
spam filtering — so prompts ask for SHORT replies and avoid spammy content.

  * mock leg → reachability (deterministic ``REPLY_OK`` from the mock model).
  * real leg → intelligence: basic, own identity, sender, tools.

Skipped unless both keys are set. Replies are matched by *new* inbound message id
from the AUT's number (robust to clock skew).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
import urllib.request
import uuid
from pathlib import Path

import pytest

REMOTE_KEY = os.environ.get("REMOTE_INKBOX_API_KEY")
AUT_KEY = os.environ.get("CODEX_INKBOX_API_KEY")
BASE_URL = os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai")
REAL = os.environ.get("LIVE_REAL_MODEL") == "1"
TIMEOUT_S = float(os.environ.get("LIVE_SMS_TIMEOUT", "180"))
POLL_EVERY_S = 6.0
# "hit an error" is the bridge's canned failed-turn reply.
ERROR_MARKERS = ("non-retryable error", "missing authentication", "http 401", "http 403", "traceback",
                 "hit an error")
# Delivery-failure retry tests: the loop adds a full extra agent turn
# (wake → rewrite → resend), so they get a longer budget than one Q/A.
RETRY_TIMEOUT_S = TIMEOUT_S + 120
# The gateway's stdout log (the workflow points GATEWAY_LOG at it) — the
# authoritative source for the plugin's delivery-failure wake-up lines.
GATEWAY_LOG = os.environ.get("GATEWAY_LOG", "")
# The AUT's org signing key — same value the gateway verifies webhooks with —
# lets the test forge a valid delivery-failure webhook. Exported under a
# dedicated name so it can't accidentally un-skip another suite's gate.
SIGNING_KEY = os.environ.get("AUT_INKBOX_SIGNING_KEY", "")
# The gateway's LOCAL webhook listener (behind the tunnel), default bridge port.
AUT_WEBHOOK_URL = os.environ.get("AUT_WEBHOOK_URL", "http://127.0.0.1:8767/webhook")

pytestmark = pytest.mark.skipif(
    not (REMOTE_KEY and AUT_KEY),
    reason="live SMS suite: needs REMOTE_INKBOX_API_KEY + CODEX_INKBOX_API_KEY",
)
real_only = pytest.mark.skipif(not REAL, reason="intelligence runs in the real-model leg")
mock_only = pytest.mark.skipif(REAL, reason="reachability runs in the mock-model leg")


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _client(key):
    from inkbox import Inkbox

    return Inkbox(api_key=key, base_url=BASE_URL)


def _phone(client):
    nums = client.phone_numbers.list()
    assert nums, "identity has no phone number"
    return nums[0].number, str(nums[0].id)


def _plugin_tool_names() -> list[str]:
    """Tool names the bridge registers, scraped from the tool source — tracks
    the code without a hand-kept list."""
    root = Path(__file__).resolve().parents[2]
    src = (root / "inkbox_codex" / "tools.py").read_text()
    return sorted(set(re.findall(r'"(inkbox_[a-z0-9_]+)"', src)))


@pytest.fixture(scope="module")
def sms():
    remote = _client(REMOTE_KEY)
    aut = _client(AUT_KEY)
    aut_phone, _aut_pid = _phone(aut)
    _remote_phone, remote_pid = _phone(remote)
    # No opt-in/START needed: the server bypasses the missing-opt-in gate for
    # inter-agent traffic (the recipient is an Inkbox-managed number). Only an
    # explicit STOP/opt-out would block.
    return {"remote": remote, "aut": aut, "aut_phone": aut_phone, "remote_pid": remote_pid}


def _inbound_from_aut(sms):
    """List the remote's inbound messages that came from the AUT's number."""
    remote, aut_phone, pid = sms["remote"], sms["aut_phone"], sms["remote_pid"]
    tail = _digits(aut_phone)[-10:]
    out = []
    for m in remote.texts.list(pid, limit=30):
        if (getattr(m, "direction", "") or "").lower() == "inbound" \
                and _digits(getattr(m, "remote_phone_number", "") or "")[-10:] == tail:
            out.append(m)
    return out


def _settle_inbound(sms) -> set:
    """Drain to a quiet state; return the settled inbound id-set.

    The agent sometimes emits a trailing *second* SMS for the PREVIOUS question
    (a duplicate "OK", or a masked + unmasked identity pair) that lands a few
    seconds late. Matching on "any new inbound id after I sent" would let that
    leftover leak into the next question's match, so we poll until the id-set
    stops growing — folding any in-flight trailing reply into the baseline.
    """
    before = {m.id for m in _inbound_from_aut(sms)}
    quiet_deadline = time.monotonic() + 2 * POLL_EVERY_S
    while time.monotonic() < quiet_deadline:
        time.sleep(POLL_EVERY_S)
        now_ids = {m.id for m in _inbound_from_aut(sms)}
        if now_ids == before:
            break
        before = now_ids
    return before


def _wait_new_inbound(sms, before: set, timeout_s: float, context: str) -> str:
    """Poll for the first inbound not in ``before``; return its body lowercased."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for m in _inbound_from_aut(sms):
            if m.id not in before:
                body = getattr(m, "text", "") or ""
                bad = [x for x in ERROR_MARKERS if x in body.lower()]
                assert not bad, f"SMS reply is an error, not a real answer: {bad}\n{body[:200]}"
                return body.lower()
        time.sleep(POLL_EVERY_S)
    pytest.fail(f"no SMS reply within {timeout_s:.0f}s to: {context}")


def _diversify(text: str) -> str:
    """Append a unique ref so no two driver sends share a body.

    Questions the agent answers reset the conversation cadence, but prompts that
    get no SMS reply (call requests, a spam-blocked reply that never lands)
    accumulate — two identical no-reply sends to the same number trip the
    server's duplicate_body rule (422) and 500-block the harness. The ref is
    inert to every reply-content assertion (they check the agent's answer, not
    the echoed question).
    """
    return f"{text} (ref {uuid.uuid4().hex[:6]})"


def _ask_sms(sms, text: str, timeout_s: float = TIMEOUT_S) -> str:
    """Text the agent; return the reply body (lowercased), matched by new message id."""
    remote, aut_phone, pid = sms["remote"], sms["aut_phone"], sms["remote_pid"]
    before = _settle_inbound(sms)
    remote.texts.send(pid, to=aut_phone, text=_diversify(text))
    return _wait_new_inbound(sms, before, timeout_s, repr(text))


def _ask_sms_besteffort(sms, text: str, timeout_s: float) -> str | None:
    """Text the agent; return the reply body lowercased, or None on timeout.

    Unlike ``_ask_sms`` this never fails the test on no-reply — used where a
    reply is a best-effort signal, not the assertion.
    """
    remote, aut_phone, pid = sms["remote"], sms["aut_phone"], sms["remote_pid"]
    before = _settle_inbound(sms)
    remote.texts.send(pid, to=aut_phone, text=_diversify(text))
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for m in _inbound_from_aut(sms):
            if m.id not in before:
                return (getattr(m, "text", "") or "").lower()
        time.sleep(POLL_EVERY_S)
    return None


def _gateway_log_size() -> int:
    if not GATEWAY_LOG or not os.path.exists(GATEWAY_LOG):
        return 0
    return os.path.getsize(GATEWAY_LOG)


def _gateway_log_since(offset: int) -> str:
    """Return gateway log text past ``offset`` ('' when the log isn't wired)."""
    if not GATEWAY_LOG or not os.path.exists(GATEWAY_LOG):
        return ""
    with open(GATEWAY_LOG, encoding="utf-8", errors="replace") as fh:
        fh.seek(offset)
        return fh.read()


@mock_only
def test_sms_reachability(sms):
    body = _ask_sms(sms, "ping")
    assert "reply_ok" in body, f"mock reachability: missing REPLY_OK marker\n{body[:200]}"


@real_only
def test_sms_basic_reply(sms):
    body = _ask_sms(sms, "Please reply OK to confirm you got this text.")
    assert len(body.strip()) > 0, "empty reply"


@real_only
def test_sms_reports_own_identity(sms):
    aut_email = sms["aut"].mailboxes.list()[0].email_address
    body = _ask_sms(sms, "Reply with just your Inkbox email address and phone number — short.")
    assert aut_email in body, f"reply missing email {aut_email!r}\n{body[:200]}"


@real_only
def test_sms_reports_sender_details(sms):
    aut, remote = sms["aut"], sms["remote"]
    remote_email = remote.mailboxes.list()[0].email_address
    matches = aut.contacts.lookup(email=remote_email)
    if not matches:
        pytest.skip("no contact card for the sender to report")
    name = (getattr(matches[0], "preferred_name", None) or getattr(matches[0], "given_name", None) or "")
    body = _ask_sms(sms, "Who am I to you? Tell me what you have on file about me.")
    if name:
        assert name.lower() in body, f"reply missing sender name {name!r}\n{body[:200]}"


@real_only
def test_sms_aware_of_inkbox_tools(sms):
    tool_names = _plugin_tool_names()
    body = _ask_sms(sms, "Name three of your Inkbox tools (exact names).")
    hits = [t for t in tool_names if t.lower() in body]
    assert len(hits) >= 2, f"agent named only {hits} of its tools\n{body[:300]}"


# ── Outbound delivery-failure retry loop ────────────────────────────────
#
# Ordering matters: the carrier-failure test runs FIRST. The spam-block test
# deliberately creates blocked_spam_filter rows on the AUT's number, and
# listing a conversation containing such rows crashes SDK clients whose
# SmsDeliveryStatus enum predates that status — the carrier test's
# conversation-id lookup must run against a clean history.
#
# Loop evidence comes from the gateway log (the plugin's wake-up lines), which
# is authoritative and deterministic. Codex wakes the session via run_consult:
# the agent's follow-up is NOT auto-sent (unlike a normal inbound reply), so it
# must act through its Inkbox tools — whether it does is model behaviour, hence
# a best-effort signal, not the assertion.


def _sign_inkbox_webhook(payload: bytes, request_id: str, timestamp: str, secret: str) -> str:
    """Forge the Inkbox webhook signature: HMAC-SHA256 over id.ts.body."""
    key = secret.removeprefix("whsec_")
    message = f"{request_id}.{timestamp}.".encode() + payload
    return "sha256=" + hmac.new(key.encode(), message, hashlib.sha256).hexdigest()


def _inject_inkbox_webhook(envelope: dict) -> int:
    """POST a signed Inkbox-style webhook to the gateway's local listener."""
    payload = json.dumps(envelope).encode()
    request_id = str(uuid.uuid4())
    timestamp = str(int(time.time()))
    req = urllib.request.Request(
        AUT_WEBHOOK_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Inkbox-Request-Id": request_id,
            "X-Inkbox-Timestamp": timestamp,
            "X-Inkbox-Signature": _sign_inkbox_webhook(
                payload, request_id, timestamp, SIGNING_KEY,
            ),
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.status


def _assert_wake_logged(log_offset: int, stage: str) -> None:
    """Require the retry loop's gateway-log fingerprint past ``log_offset``."""
    log = _gateway_log_since(log_offset)
    assert "Woke agent about failed outbound sms" in log, (
        "no delivery-failure wake-up in the gateway log — retry loop did not run"
    )
    assert f"stage={stage}" in log


@real_only
@pytest.mark.skipif(not SIGNING_KEY, reason="needs AUT_INKBOX_SIGNING_KEY to sign the fake webhook")
def test_sms_retry_after_carrier_delivery_failure(sms):
    """Inject a fake carrier delivery-failure webhook; expect the loop to fire.

    Simulates the async failure surface: the send was accepted, then the
    carrier flagged it (error 40002) and the server reported it via a
    ``text.delivery_failed`` webhook. The webhook is forged with the AUT's own
    signing key and posted to the gateway's local webhook listener — exactly
    how a real delivery would arrive through the tunnel. The plugin must wake
    the session (authoritative: the gateway log). A real follow-up SMS is a
    best-effort bonus: Codex's wake is a run_consult turn whose reply is not
    auto-sent, so the agent has to reach the human via a tool, which is model
    behaviour rather than a property of the loop.
    """
    aut, remote = sms["aut"], sms["remote"]
    aut_phone = sms["aut_phone"]
    remote_phone, _remote_pid = _phone(remote)
    _aut_number, aut_pid = _phone(aut)

    # Prime the conversation so the agent has live routing state and the wake-up
    # lands in an existing session.
    _ask_sms(sms, "Please reply OK to confirm you got this text.")

    # The AUT-side conversation id for this thread, read from its own API.
    # Best-effort: a history row the installed SDK cannot hydrate (e.g. a
    # delivery status newer than its enum) must not kill the test — the injected
    # failure also routes fine by remote number alone.
    conversation_id = ""
    remote_tail = _digits(remote_phone)[-10:]
    try:
        for m in aut.texts.list(aut_pid, limit=30):
            if _digits(getattr(m, "remote_phone_number", "") or "")[-10:] == remote_tail:
                conversation_id = str(getattr(m, "conversation_id", "") or "")
                if conversation_id:
                    break
    except Exception as exc:
        print(f"note: conversation-id lookup failed ({exc!r}); injecting without one")

    log_offset = _gateway_log_size()
    before = _settle_inbound(sms)

    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    envelope = {
        "id": f"evt_{uuid.uuid4()}",
        "event_type": "text.delivery_failed",
        "timestamp": now,
        "data": {
            "text_message": {
                "id": str(uuid.uuid4()),
                "direction": "outbound",
                "local_phone_number": aut_phone,
                "remote_phone_number": remote_phone,
                "conversation_id": conversation_id or None,
                "text": "Quick update: everything is on track for tomorrow.",
                "type": "sms",
                "media": None,
                "is_read": True,
                "delivery_status": "delivery_failed",
                "error_code": "40002",
                "error_detail": (
                    "The message was flagged by a SPAM filter and was not "
                    "delivered. This is a temporary condition."
                ),
                "created_at": now,
                "updated_at": now,
            },
            "contacts": [],
            "agent_identities": [],
            "recipient_phone_number": None,
        },
    }
    status = _inject_inkbox_webhook(envelope)
    assert status == 200, f"gateway rejected the forged delivery-failure webhook: {status}"

    # Authoritative: the loop fired for the carrier failure.
    assert GATEWAY_LOG, "GATEWAY_LOG must be wired for this test to observe the loop"
    # Give the background wake task a moment to log.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and "Woke agent about failed outbound sms" not in _gateway_log_since(log_offset):
        time.sleep(2)
    _assert_wake_logged(log_offset, "delivery_failed")

    # A real, delivered follow-up SMS is a best-effort bonus (see docstring):
    # the wake-up turn carries the resend ask, so just watch for a new inbound.
    body = _wait_or_none(sms, before, RETRY_TIMEOUT_S)
    print(f"note: follow-up {'delivered' if body and body.strip() else 'not received (acceptable)'}")


def _wait_or_none(sms, before: set, timeout_s: float):
    """Best-effort: return the first new inbound body, or None on timeout."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for m in _inbound_from_aut(sms):
            if m.id not in before:
                return (getattr(m, "text", "") or "").lower()
        time.sleep(POLL_EVERY_S)
    return None


@real_only
def test_sms_retry_after_internal_spam_block(sms):
    """Bait the server's outbound content filter; assert the block reaches the agent.

    Asking for three emojis makes the agent's reply trip the server's one-emoji
    SMS budget (a synchronous 422 block — no carrier send). When the agent
    answers with a normal reply, the bridge routes it through send_to_contact,
    the send is rejected, and the plugin's delivery-failure loop wakes the
    session (gateway log: "Woke agent about failed outbound sms", stage
    send_rejected). That is the loop engaging.

    Deliberately does NOT require a compliant follow-up SMS: the ask is
    inherently unsatisfiable (three emojis vs a one-emoji budget), so whether a
    real model abandons the emojis and sends plain text, or stops after
    exhausting its send budget, is model behaviour — not a property of the retry
    loop. Loop mechanics (cap, budget, recovery) are covered deterministically
    by tests/test_delivery_failure_retry.py.

    NOTE: the *ask* itself must contain no emojis — the remote driver's outbound
    rides the same filter, and an emoji-laden question would be blocked before
    the AUT ever saw it. (Codex MCP tools run in a separate process, so a
    tool-path send leaves no gateway-log trace; the observable path is the
    normal-reply send_rejected wake, which is also the path a session agent
    takes when replying to an inbound it is already handling.)
    """
    log_offset = _gateway_log_size()

    # Best-effort: a reply may or may not come (the request can't be satisfied
    # as literally asked). Its arrival is a bonus signal, not the assertion.
    body = _ask_sms_besteffort(
        sms,
        "Fun formatting test: reply with ONE short message that contains at "
        "least three different emojis of your choice. Just send it, no questions.",
        timeout_s=RETRY_TIMEOUT_S,
    )
    print(f"note: compliant follow-up {'delivered' if body and body.strip() else 'not received (acceptable)'}")

    # Accept either legitimate real-model outcome: the attempted emoji reply
    # was blocked and woke the retry loop, or the model abandoned the unsafe
    # formatting request and sent a deliverable fallback directly. The loop
    # mechanics themselves are covered deterministically by unit tests.
    assert GATEWAY_LOG, "GATEWAY_LOG must be wired for this test to observe the loop"
    log = _gateway_log_since(log_offset)
    block_surfaced = (
        "Woke agent about failed outbound sms" in log and "stage=send_rejected" in log
    )
    delivered_fallback = bool(body and body.strip())
    assert block_surfaced or delivered_fallback, (
        "neither a delivery-failure wake-up nor a safe fallback reply was observed"
    )
