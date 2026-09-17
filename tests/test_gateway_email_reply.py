"""Automatic email replies: threading, copied recipients, blocked fallback.

The reply to an inbound email threads onto it and keeps the people the sender
copied. When contact rules block a copied recipient the bridge retries once as
a sender-only reply; a second failure goes to the normal failure handling.
``INKBOX_EMAIL_REPLY_ALL`` decides when copied recipients are kept: ``trusted``
(the default - only when inbound mail is allowed contacts only), ``always``,
or ``never``.
A sender-only reply is still threaded.
"""

import asyncio
import json
import types

import pytest

from inkbox_codex import gateway
from inkbox_codex.config import BridgeConfig

AGENT = "agent@inkboxmail.example"
OWNER = "owner@example.com"
RFC_ID = "<CAF123@mail.example.com>"


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    """aiohttp isn't installed in tests; stub the json_response the handlers use."""
    def json_response(payload):
        return types.SimpleNamespace(text=json.dumps(payload), payload=payload)
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(json_response=json_response))


class _Blocked(Exception):
    status_code = 403
    detail = {"error": "recipient_blocked", "message": "Recipient is blocked by a contact rule."}


class _FakeIdentity:
    """Records mail sends; raises the queued errors first, one per call."""

    def __init__(self, errors=(), **attrs):
        self.calls = []
        self._errors = list(errors)
        # Optional contact-rule mode attributes, as the SDK exposes them.
        for name, value in attrs.items():
            setattr(self, name, value)

    def _record(self, name, args, kwargs):
        self.calls.append((name, args, kwargs))
        if self._errors:
            error = self._errors.pop(0)
            if error is not None:
                raise error

    def reply_all_email(self, *args, **kwargs):
        self._record("reply_all_email", args, kwargs)

    def send_email(self, *args, **kwargs):
        self._record("send_email", args, kwargs)


def _allowed_only(errors=()):
    """An identity whose inbound mail mode only admits allowed contacts."""
    return _FakeIdentity(errors, mail_inbound_filter_mode="whitelist")


class _FakeSession:
    def __init__(self):
        self.inbound = []

    async def handle_inbound(self, text, mode, meta):
        self.inbound.append((text, mode, meta))


class _FakeSessions:
    def __init__(self):
        self.by_id = {}

    def get(self, chat_id):
        return self.by_id.setdefault(chat_id, _FakeSession())


def _gw(identity=None, **cfg):
    gw = gateway.InkboxGateway(
        BridgeConfig(require_signature=False, allow_all_users=True, identity="agent", **cfg)
    )
    gw.sessions = _FakeSessions()
    gw._self_addresses = {AGENT}
    gw._inkbox = types.SimpleNamespace(get_identity=lambda _handle: identity)
    return gw


def _mail_envelope(to=None, cc=None, **message):
    body = {
        "id": "11111111-1111-1111-1111-111111111111",
        "message_id": RFC_ID,
        "from_address": OWNER,
        "subject": "Plans",
        "body_text": "Can you confirm?",
        "thread_id": "thread-1",
        **message,
    }
    if to is not None:
        body["to_addresses"] = to
    if cc is not None:
        body["cc_addresses"] = cc
    return {"data": {"contacts": [], "message": body}}


def _inbound_meta(envelope, **cfg):
    gw = _gw(**cfg)
    # Contact lookups are out of scope here.
    gw._inkbox = None
    asyncio.run(gw._on_mail_received(envelope))
    (session,) = gw.sessions.by_id.values()
    ((_, mode, meta),) = session.inbound
    assert mode == "email"
    return meta


def _send(gw, meta, content="Confirmed."):
    asyncio.run(gw.send_to_contact("chat-1", content, "email", meta))


# ── What the inbound handler stashes ─────────────────────────────────────────


def test_inbound_meta_carries_thread_ids_and_copied_recipients():
    meta = _inbound_meta(_mail_envelope(
        to=[AGENT.upper(), "Pat <pat@example.com>", "sam@example.com"],
        cc=["PAT@example.com", OWNER.title(), "lee@example.com", "", "sam@example.com"],
    ))
    assert meta["to"] == OWNER
    assert meta["message_id"] == "11111111-1111-1111-1111-111111111111"
    assert meta["rfc_message_id"] == RFC_ID
    # Own address and the sender removed; deduped case-insensitively, in order.
    assert meta["reply_cc"] == ["pat@example.com", "sam@example.com", "lee@example.com"]


@pytest.mark.parametrize("fields", [
    {},
    {"to": [AGENT], "cc": None},
    {"to": "not-a-list", "cc": {"a": 1}},
])
def test_inbound_without_copied_recipients_has_empty_reply_cc(fields):
    meta = _inbound_meta(_mail_envelope(**fields))
    assert meta["reply_cc"] == []


def test_entries_that_are_not_addresses_are_not_copied_recipients(caplog):
    meta = _inbound_meta(_mail_envelope(
        to=["undisclosed-recipients:;", "not an address", AGENT],
        cc=["Pat <pat@example.com>"],
    ))
    assert meta["reply_cc"] == ["pat@example.com"]

    # With nobody real copied there is nothing to keep - and nothing to log.
    meta = _inbound_meta(_mail_envelope(to=["undisclosed-recipients:;"]))
    assert meta["reply_cc"] == []
    identity = _FakeIdentity(mail_inbound_filter_mode="blacklist")
    with caplog.at_level("INFO"):
        _send(_gw(identity), meta)
    assert [name for name, _, _ in identity.calls] == ["send_email"]
    assert not [r for r in caplog.records if "were not kept" in r.getMessage()]


def test_inbound_without_ids_keeps_them_unset():
    meta = _inbound_meta(_mail_envelope(id=None, message_id=None))
    assert meta["message_id"] is None
    assert meta["rfc_message_id"] is None


# ── How the reply goes out ───────────────────────────────────────────────────


def _meta(**overrides):
    meta = {
        "to": OWNER,
        "subject": "Plans",
        "message_id": "msg-uuid",
        "rfc_message_id": RFC_ID,
        "reply_cc": ["pat@example.com"],
    }
    meta.update(overrides)
    return meta


def test_reply_keeps_copied_recipients_with_a_threaded_reply_all():
    identity = _allowed_only()
    _send(_gw(identity), _meta())
    assert identity.calls == [
        ("reply_all_email", ("msg-uuid",), {"subject": "Re: Plans", "body_text": "Confirmed."}),
    ]


def test_reply_without_copied_recipients_is_threaded_to_the_sender():
    identity = _FakeIdentity()
    _send(_gw(identity), _meta(reply_cc=[]))
    assert identity.calls == [
        ("send_email", (), {
            "to": [OWNER], "subject": "Re: Plans", "body_text": "Confirmed.",
            "in_reply_to_message_id": RFC_ID,
        }),
    ]


def test_never_replies_to_the_sender_only_still_threaded():
    identity = _FakeIdentity(mail_inbound_filter_mode="whitelist")
    _send(_gw(identity, email_reply_all="never"), _meta())
    assert [name for name, _, _ in identity.calls] == ["send_email"]
    assert identity.calls[0][2]["to"] == [OWNER]
    assert identity.calls[0][2]["in_reply_to_message_id"] == RFC_ID


class _Mode:
    """Stands in for an SDK enum member."""

    def __init__(self, value):
        self.value = value


@pytest.mark.parametrize("attrs", [
    {"mail_inbound_filter_mode": "whitelist"},
    {"mail_inbound_filter_mode": _Mode("whitelist")},
    # An SDK without the directional attribute: the single mail mode decides.
    {"mail_filter_mode": _Mode("whitelist")},
])
def test_trusted_keeps_copied_recipients_when_mail_is_allowed_contacts_only(attrs):
    identity = _FakeIdentity(**attrs)
    _send(_gw(identity), _meta())
    assert [name for name, _, _ in identity.calls] == ["reply_all_email"]


def test_trusted_on_open_mail_is_sender_only_even_for_a_known_contact():
    # Contacts are also created for anyone who writes in, so a matched contact
    # says nothing about whether the owner allowed the sender.
    envelope = _mail_envelope(to=[AGENT], cc=["pat@example.com"])
    envelope["data"]["contacts"] = [{"bucket": "from", "id": "c-1", "address": OWNER}]
    meta = _inbound_meta(envelope)
    assert meta["reply_cc"] == ["pat@example.com"]

    identity = _FakeIdentity(mail_inbound_filter_mode="blacklist")
    _send(_gw(identity), meta)
    assert [name for name, _, _ in identity.calls] == ["send_email"]
    assert identity.calls[0][2]["to"] == [OWNER]


@pytest.mark.parametrize("attrs", [
    {"mail_inbound_filter_mode": "blacklist"},
    # The directional attribute wins over the legacy one.
    {"mail_inbound_filter_mode": "blacklist", "mail_filter_mode": "whitelist"},
    {"mail_filter_mode": _Mode("blacklist")},
    # Nothing readable on the identity => not trusted.
    {},
    {"mail_inbound_filter_mode": None, "mail_filter_mode": None},
])
def test_trusted_drops_copied_recipients_on_open_mail(attrs, caplog):
    identity = _FakeIdentity(**attrs)
    with caplog.at_level("INFO"):
        _send(_gw(identity), _meta())
    assert identity.calls == [
        ("send_email", (), {
            "to": [OWNER], "subject": "Re: Plans", "body_text": "Confirmed.",
            "in_reply_to_message_id": RFC_ID,
        }),
    ]
    kept = [r.getMessage() for r in caplog.records if "were not kept" in r.getMessage()]
    assert len(kept) == 1


def test_unreadable_identity_mode_counts_as_not_trusted():
    class _Raises(_FakeIdentity):
        @property
        def mail_inbound_filter_mode(self):
            raise RuntimeError("no mode")

    identity = _Raises()
    _send(_gw(identity), _meta())
    assert [name for name, _, _ in identity.calls] == ["send_email"]


def test_always_keeps_copied_recipients_for_any_sender():
    identity = _FakeIdentity(mail_inbound_filter_mode="blacklist")
    _send(_gw(identity, email_reply_all="always"), _meta())
    assert [name for name, _, _ in identity.calls] == ["reply_all_email"]


def test_trusted_without_copied_recipients_logs_nothing(caplog):
    identity = _FakeIdentity()
    with caplog.at_level("INFO"):
        _send(_gw(identity), _meta(reply_cc=[]))
    assert [name for name, _, _ in identity.calls] == ["send_email"]
    assert not [r for r in caplog.records if "were not kept" in r.getMessage()]


def test_meta_from_before_this_field_existed_sends_exactly_as_before():
    identity = _FakeIdentity()
    _send(_gw(identity), {"to": OWNER, "subject": "Re: Plans"})
    assert identity.calls == [
        ("send_email", (), {"to": [OWNER], "subject": "Re: Plans", "body_text": "Confirmed."}),
    ]


def test_too_many_copied_recipients_gets_a_sender_only_reply():
    identity = _allowed_only()
    copied = [f"p{index}@example.com" for index in range(gateway.EMAIL_REPLY_ALL_MAX_COPIED + 1)]
    _send(_gw(identity), _meta(reply_cc=copied))
    assert [name for name, _, _ in identity.calls] == ["send_email"]


def test_blocked_copied_recipient_retries_once_to_the_sender_only(caplog):
    identity = _allowed_only(errors=[_Blocked()])
    with caplog.at_level("WARNING"):
        _send(_gw(identity), _meta())
    assert [name for name, _, _ in identity.calls] == ["reply_all_email", "send_email"]
    assert identity.calls[1][2] == {
        "to": [OWNER], "subject": "Re: Plans", "body_text": "Confirmed.",
        "in_reply_to_message_id": RFC_ID,
    }
    dropped = [r.getMessage() for r in caplog.records if "copied recipients were dropped" in r.getMessage()]
    assert len(dropped) == 1
    assert "supervised" in dropped[0]


def test_blocked_sender_only_retry_raises_into_normal_failure_handling():
    identity = _allowed_only(errors=[_Blocked(), _Blocked()])
    with pytest.raises(_Blocked):
        _send(_gw(identity), _meta())
    # Exactly one retry - never a loop.
    assert [name for name, _, _ in identity.calls] == ["reply_all_email", "send_email"]


def test_other_reply_all_errors_are_not_retried():
    identity = _allowed_only(errors=[RuntimeError("mailbox over quota")])
    with pytest.raises(RuntimeError):
        _send(_gw(identity), _meta())
    assert [name for name, _, _ in identity.calls] == ["reply_all_email"]


# ── Idempotency (only when the installed SDK takes a key) ────────────────────


class _KeyedIdentity(_FakeIdentity):
    """An SDK whose mail sends accept ``idempotency_key``."""

    def reply_all_email(self, message_id, *, subject=None, body_text=None, idempotency_key=None):
        self._record("reply_all_email", (message_id,), {"idempotency_key": idempotency_key})

    def send_email(self, *, to, subject, body_text=None, in_reply_to_message_id=None, idempotency_key=None):
        self._record("send_email", (), {"idempotency_key": idempotency_key})


def _keys(identity):
    return [kwargs["idempotency_key"] for _, _, kwargs in identity.calls]


def test_sdk_without_an_idempotency_parameter_is_called_without_one():
    # The default fake takes **kwargs only, like an SDK that has no such
    # parameter: nothing extra may be passed (asserted by the exact-call tests
    # above); spot-check both paths here.
    identity = _allowed_only(errors=[_Blocked()])
    _send(_gw(identity), _meta())
    assert all("idempotency_key" not in kwargs for _, _, kwargs in identity.calls)


def test_same_reply_reuses_its_key_and_a_different_reply_does_not():
    identity = _KeyedIdentity(mail_inbound_filter_mode="whitelist")
    gw = _gw(identity)
    _send(gw, _meta(), "Confirmed.")
    _send(gw, _meta(), "Confirmed.")
    _send(gw, _meta(), "Confirmed, see you at 8.")
    _send(gw, _meta(message_id="other-uuid"), "Confirmed.")
    first, again, reworded, other_message = _keys(identity)
    assert first and first == again
    assert len({first, reworded, other_message}) == 3


def test_sender_only_retry_gets_its_own_key():
    identity = _KeyedIdentity(errors=[_Blocked()], mail_inbound_filter_mode="whitelist")
    _send(_gw(identity), _meta())
    assert [name for name, _, _ in identity.calls] == ["reply_all_email", "send_email"]
    reply_all_key, sender_key = _keys(identity)
    assert reply_all_key and sender_key and reply_all_key != sender_key

    # The same sender-only reply sent directly dedupes against that retry.
    direct = _KeyedIdentity()
    _send(_gw(direct), _meta(reply_cc=[]))
    assert _keys(direct) == [sender_key]


def test_no_key_without_an_inbound_message_id():
    identity = _KeyedIdentity()
    _send(_gw(identity), {"to": OWNER, "subject": "Plans"})
    assert _keys(identity) == [None]


# ── Each reply uses its own message's thread and audience ────────────────────


def _queued_email_session(gw):
    """A real session wired to the gateway, with its worker held busy so
    inbound emails queue instead of running."""
    from inkbox_codex.sessions import ContactSession

    session = ContactSession(
        chat_id="contact-1",
        cfg=BridgeConfig(project_dir="/tmp"),
        send_fn=gw.send_to_contact,
        mcp_server_config={},
        identity_info={"handle": "t", "email": AGENT, "phone": ""},
    )
    session._worker = asyncio.create_task(asyncio.sleep(10))
    return session


PRIVATE = {"to": OWNER, "subject": "Private", "message_id": "uuid-b",
           "rfc_message_id": "<b@mail.example.com>", "reply_cc": []}
SHARED = {"to": OWNER, "subject": "Shared", "message_id": "uuid-a",
          "rfc_message_id": "<a@mail.example.com>",
          "reply_cc": ["bob@example.com", "carol@example.com"]}


def test_interleaved_emails_each_reply_to_their_own_message():
    async def scenario():
        identity = _allowed_only()
        session = _queued_email_session(_gw(identity))
        # A private 1:1 email, then one with people copied, before either runs.
        await session.handle_inbound("private question", "email", dict(PRIVATE))
        await session.handle_inbound("shared question", "email", dict(SHARED))
        private_turn = session._queue.get_nowait()
        shared_turn = session._queue.get_nowait()
        assert session.reply_meta["message_id"] == "uuid-a"

        await session._deliver_reply(private_turn, "private answer")
        await session._deliver_reply(shared_turn, "shared answer")
        session._worker.cancel()
        return identity.calls

    private_call, shared_call = asyncio.run(scenario())
    # The private answer stays private, threaded onto its own message.
    assert private_call == ("send_email", (), {
        "to": [OWNER], "subject": "Re: Private", "body_text": "private answer",
        "in_reply_to_message_id": "<b@mail.example.com>",
    })
    assert shared_call == (
        "reply_all_email", ("uuid-a",), {"subject": "Re: Shared", "body_text": "shared answer"},
    )


def test_turns_without_their_own_email_never_inherit_a_reply_all():
    from inkbox_codex.sessions import _Turn

    async def scenario():
        identity = _allowed_only()
        session = _queued_email_session(_gw(identity))
        await session.handle_inbound("shared question", "email", dict(SHARED))
        # A recovery turn from before that email, and a plain bridge notice.
        await session._deliver_reply(_Turn(text="[delivery failed] ...", recovery=True), "retrying")
        await session._reply("Sorry - I hit an error.")
        session._worker.cancel()
        return identity.calls

    calls = asyncio.run(scenario())
    assert [name for name, _, _ in calls] == ["send_email", "send_email"]
    for _, _, kwargs in calls:
        assert kwargs["to"] == [OWNER]
        assert "in_reply_to_message_id" not in kwargs


def test_recovery_turn_keeps_the_snapshot_of_the_reply_it_resends():
    async def scenario():
        identity = _allowed_only(errors=[RuntimeError("temporary failure")])
        gw = _gw(identity)
        session = _queued_email_session(gw)
        session.on_send_failure = gw._note_sync_send_failure
        await session.handle_inbound("shared question", "email", dict(SHARED))
        shared_turn = session._queue.get_nowait()
        await session.handle_inbound("private question", "email", dict(PRIVATE))
        session._queue.get_nowait()

        await session._deliver_reply(shared_turn, "shared answer")
        recovery = session._queue.get_nowait()
        session._worker.cancel()
        return recovery

    recovery = asyncio.run(scenario())
    assert recovery.recovery is True
    assert recovery.mode == "email"
    assert recovery.reply_meta["message_id"] == "uuid-a"
