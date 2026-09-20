"""Real reply-all delivery to the sender and a separate, controlled CC inbox.

The gateway must already be running. All three credentials must identify
dedicated test identities; the two recipient mailboxes must not auto-reply.
No reply is sent directly by this test: the running gateway must produce it.
"""

from __future__ import annotations

import os
import time
import uuid
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr

import pytest

from tests.live.test_email_reply import ERROR_MARKERS


REMOTE_KEY = os.environ.get("REMOTE_INKBOX_API_KEY")
AUT_KEY = os.environ.get("CODEX_INKBOX_API_KEY")
CC_KEY = os.environ.get("REPLY_ALL_INKBOX_API_KEY")
BASE_URL = os.environ.get("INKBOX_BASE_URL", "https://inkbox.ai")
TIMEOUT_S = float(os.environ.get("LIVE_EMAIL_TIMEOUT", "150"))
POLL_EVERY_S = 3.0

pytestmark = [
    pytest.mark.skipif(
        not (REMOTE_KEY and AUT_KEY), reason="requires a running live gateway and test identities",
    ),
    pytest.mark.no_sms_reset,
]


def _address(value):
    return parseaddr(value or "")[1].lower()


def _mailbox(client):
    principal = client.whoami()
    assert principal.auth_subtype == "api_key.agent_scoped.claimed", "use a claimed test identity"
    boxes = client.mailboxes.list()
    assert len(boxes) == 1, "test credentials must resolve to exactly one mailbox"
    return boxes[0].email_address


def _find_message(client, mailbox, *, since, sender, nonce, direction):
    for message in client.messages.list(mailbox, direction=direction, start_datetime=since):
        if _address(message.from_address) == _address(sender) and nonce in (message.subject or ""):
            return client.messages.get(mailbox, message.id)
    return None


def _assert_reply(reply, *, sender, recipient, cc, original_wire_id, nonce):
    assert _address(reply.from_address) == _address(sender)
    assert [_address(value) for value in reply.to_addresses] == [_address(recipient)]
    assert [_address(value) for value in reply.cc_addresses or []] == [_address(cc)]
    assert not getattr(reply, "bcc_addresses", None), "reply must not expose BCC recipients"
    assert str(reply.in_reply_to or "").strip("<>") == original_wire_id.strip("<>")
    references = reply.references or []
    if isinstance(references, str):
        references = references.split()
    assert original_wire_id.strip("<>") in [str(value).strip("<>") for value in references]
    body = (reply.body_text or "").lower()
    assert f"reply_ok {nonce}" in body, "expected the running agent's response to this request"
    assert not any(marker in body for marker in ERROR_MARKERS), "received an error fallback"


@pytest.mark.parametrize("copied_in", ["cc", "to"])
def test_live_reply_all_preserves_audience_and_thread(copied_in):
    from inkbox import Inkbox
    from inkbox.mail.types import MessageDirection

    assert CC_KEY, "configure REPLY_ALL_INKBOX_API_KEY for the dedicated CC test inbox"
    with ExitStack() as stack:
        remote, aut, copied = [
            stack.enter_context(Inkbox(api_key=key, base_url=BASE_URL))
            for key in (REMOTE_KEY, AUT_KEY, CC_KEY)
        ]
        remote_email, aut_email, cc_email = [_mailbox(client) for client in (remote, aut, copied)]
        assert len({_address(value) for value in (remote_email, aut_email, cc_email)}) == 3
        since = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        nonce = f"smoke-{uuid.uuid4().hex}"
        sent = remote.messages.send(
            remote_email,
            to=[aut_email, cc_email] if copied_in == "to" else [aut_email],
            cc=[cc_email] if copied_in == "cc" else None,
            subject=f"[{nonce}] Reply-all delivery check",
            body_text=(
                f"Live reply-all check. Reply with exactly REPLY_OK {nonce}. "
                "Use your normal email reply, not a separate send tool."
            ),
        )
        assert sent.message_id and sent.thread_id, "the original must have wire and thread IDs"
        expected = {
            "original_aut": (aut, aut_email, remote_email, MessageDirection.INBOUND),
            "original_cc": (copied, cc_email, remote_email, MessageDirection.INBOUND),
            "reply_aut": (aut, aut_email, aut_email, MessageDirection.OUTBOUND),
            "reply_remote": (remote, remote_email, aut_email, MessageDirection.INBOUND),
            "reply_cc": (copied, cc_email, aut_email, MessageDirection.INBOUND),
        }
        found = {}
        deadline = time.monotonic() + TIMEOUT_S
        while time.monotonic() < deadline and len(found) != len(expected):
            for name, (client, mailbox, sender, direction) in expected.items():
                if name not in found:
                    message = _find_message(
                        client, mailbox, since=since, sender=sender, nonce=nonce, direction=direction,
                    )
                    if message is not None:
                        found[name] = message
            if len(found) != len(expected):
                time.sleep(POLL_EVERY_S)
        assert found.keys() == expected.keys(), f"missing real email deliveries: {sorted(expected.keys() - found.keys())}"

        # A sent row can be observed before delivery completes. Refresh the
        # copies once both inboxes have received them before comparing headers.
        for name, (client, mailbox, _, _) in expected.items():
            found[name] = client.messages.get(mailbox, found[name].id)

        for name in ("reply_aut", "reply_remote", "reply_cc"):
            _assert_reply(
                found[name], sender=aut_email, recipient=remote_email, cc=cc_email,
                original_wire_id=sent.message_id, nonce=nonce,
            )
        # Thread UUIDs are mailbox-local: compare each reply to its own original.
        assert str(found["reply_aut"].thread_id) == str(found["original_aut"].thread_id)
        assert str(found["reply_remote"].thread_id) == str(sent.thread_id)
        assert str(found["reply_cc"].thread_id) == str(found["original_cc"].thread_id)
        # Both inbox copies must be the same outbound email, not separate sends.
        assert len({found[name].message_id for name in ("reply_aut", "reply_remote", "reply_cc")}) == 1
