"""Automatic email replies: threading, copied recipients, blocked fallback.

The reply to an inbound email threads onto it and keeps the people the sender
copied. When contact rules block a copied recipient the bridge retries once as
a sender-only reply; a second failure goes to the normal failure handling.
``INKBOX_EMAIL_REPLY_ALL`` decides when copied recipients are kept: ``trusted``
(the default - only for an allowed or saved sender), ``always``, or ``never``.
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


def test_inbound_marks_a_sender_the_webhook_resolved_to_a_saved_contact():
    assert _inbound_meta(_mail_envelope())["sender_is_contact"] is False

    envelope = _mail_envelope()
    envelope["data"]["contacts"] = [{"bucket": "from", "id": "c-1", "address": OWNER}]
    assert _inbound_meta(envelope)["sender_is_contact"] is True

    # A contact for someone else on the message says nothing about the sender.
    envelope["data"]["contacts"] = [{"bucket": "cc", "id": "c-2", "address": "pat@example.com"}]
    assert _inbound_meta(envelope)["sender_is_contact"] is False


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
        "sender_is_contact": True,
    }
    meta.update(overrides)
    return meta


def test_reply_keeps_copied_recipients_with_a_threaded_reply_all():
    identity = _FakeIdentity()
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
    _send(_gw(identity), _meta(sender_is_contact=False))
    assert [name for name, _, _ in identity.calls] == ["reply_all_email"]


def test_trusted_keeps_copied_recipients_for_a_saved_contact_on_open_mail():
    identity = _FakeIdentity(mail_inbound_filter_mode="blacklist")
    _send(_gw(identity), _meta(sender_is_contact=True))
    assert [name for name, _, _ in identity.calls] == ["reply_all_email"]


@pytest.mark.parametrize("attrs", [
    {"mail_inbound_filter_mode": "blacklist"},
    # The directional attribute wins over the legacy one.
    {"mail_inbound_filter_mode": "blacklist", "mail_filter_mode": "whitelist"},
    {"mail_filter_mode": _Mode("blacklist")},
    # Nothing readable on the identity => not trusted.
    {},
    {"mail_inbound_filter_mode": None, "mail_filter_mode": None},
])
def test_trusted_drops_copied_recipients_for_an_unknown_sender_on_open_mail(attrs, caplog):
    identity = _FakeIdentity(**attrs)
    with caplog.at_level("INFO"):
        _send(_gw(identity), _meta(sender_is_contact=False))
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
    _send(_gw(identity), _meta(sender_is_contact=False))
    assert [name for name, _, _ in identity.calls] == ["send_email"]


def test_always_keeps_copied_recipients_for_any_sender():
    identity = _FakeIdentity(mail_inbound_filter_mode="blacklist")
    _send(_gw(identity, email_reply_all="always"), _meta(sender_is_contact=False))
    assert [name for name, _, _ in identity.calls] == ["reply_all_email"]


def test_trusted_without_copied_recipients_logs_nothing(caplog):
    identity = _FakeIdentity()
    with caplog.at_level("INFO"):
        _send(_gw(identity), _meta(reply_cc=[], sender_is_contact=False))
    assert [name for name, _, _ in identity.calls] == ["send_email"]
    assert not [r for r in caplog.records if "were not kept" in r.getMessage()]


def test_meta_from_before_this_field_existed_sends_exactly_as_before():
    identity = _FakeIdentity()
    _send(_gw(identity), {"to": OWNER, "subject": "Re: Plans"})
    assert identity.calls == [
        ("send_email", (), {"to": [OWNER], "subject": "Re: Plans", "body_text": "Confirmed."}),
    ]


def test_too_many_copied_recipients_gets_a_sender_only_reply():
    identity = _FakeIdentity()
    copied = [f"p{index}@example.com" for index in range(gateway.EMAIL_REPLY_ALL_MAX_COPIED + 1)]
    _send(_gw(identity), _meta(reply_cc=copied))
    assert [name for name, _, _ in identity.calls] == ["send_email"]


def test_blocked_copied_recipient_retries_once_to_the_sender_only(caplog):
    identity = _FakeIdentity(errors=[_Blocked()])
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
    identity = _FakeIdentity(errors=[_Blocked(), _Blocked()])
    with pytest.raises(_Blocked):
        _send(_gw(identity), _meta())
    # Exactly one retry - never a loop.
    assert [name for name, _, _ in identity.calls] == ["reply_all_email", "send_email"]


def test_other_reply_all_errors_are_not_retried():
    identity = _FakeIdentity(errors=[RuntimeError("mailbox over quota")])
    with pytest.raises(RuntimeError):
        _send(_gw(identity), _meta())
    assert [name for name, _, _ in identity.calls] == ["reply_all_email"]
