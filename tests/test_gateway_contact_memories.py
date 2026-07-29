import asyncio
import types

import pytest

from inkbox_codex import gateway
from inkbox_codex.config import BridgeConfig
from inkbox_codex.prompts import frame_inbound


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    monkeypatch.setattr(
        gateway,
        "web",
        types.SimpleNamespace(json_response=lambda payload: types.SimpleNamespace(payload=payload)),
    )


class _Session:
    def __init__(self):
        self.turns = []

    async def handle_inbound(self, text, mode, meta):
        self.turns.append((text, mode, meta))


class _Sessions:
    def __init__(self):
        self.session = _Session()

    def get(self, _chat_id):
        return self.session


class _Contacts:
    def __init__(self, resolved=None):
        self.resolved = resolved
        self.lookups = []

    def lookup(self, **kwargs):
        self.lookups.append(kwargs)
        return [self.resolved] if self.resolved else []


def _gateway(enabled=True, resolved=None):
    instance = gateway.InkboxGateway(
        BridgeConfig(
            require_signature=False,
            allow_all_users=True,
            contact_memories_enabled=enabled,
        )
    )
    instance.sessions = _Sessions()
    instance._inkbox = types.SimpleNamespace(contacts=_Contacts(resolved))
    return instance


def test_mail_uses_only_matching_from_contact_memories():
    instance = _gateway(resolved={
        "id": "contact-ada",
        "preferred_name": "Ada Full",
        "emails": [{"value": "ada@example.com"}],
        "notes": "Full contact note",
    })
    envelope = {
        "data": {
            "message": {
                "id": "mail-1",
                "from_address": "Ada <ADA@example.com>",
                "body": "Hello",
            },
            "contacts": [
                {
                    "id": "contact-to",
                    "bucket": "to",
                    "address": "ada@example.com",
                    "memories": ["Wrong bucket"],
                },
                {
                    "id": "contact-ada",
                    "bucket": "from",
                    "address": "ada@example.com",
                    "memories": ["First", " First ", "", "Second"],
                },
            ],
        }
    }

    asyncio.run(instance._on_mail_received(envelope))

    text, mode, meta = instance.sessions.session.turns[0]
    framed = frame_inbound(mode, meta, text)
    assert meta["contact"] == {
        "id": "contact-ada",
        "name": "Ada Full",
        "emails": ["ada@example.com"],
        "phones": [],
        "company": None,
        "job_title": None,
        "notes": "Full contact note",
    }
    assert meta["contact_memories"] == ["First", "Second"]
    assert '"Wrong bucket"' not in framed


def test_sms_matches_resolved_contact_id_without_merging_group_contacts():
    instance = _gateway(resolved={
        "id": "contact-sender",
        "preferred_name": "Resolved Sender",
        "phones": [{"value": "+15551234567"}],
        "notes": "Authoritative note",
    })
    envelope = {
        "data": {
            "text_message": {
                "id": "text-1",
                "direction": "inbound",
                "sender_phone_number": "+15551234567",
                "conversation_id": "group-1",
                "participants": ["+15551234567", "+15557654321"],
                "text": "What do you think?",
            },
            "contacts": [
                {"id": "contact-other", "memories": ["Other memory"]},
                {"id": "contact-sender", "memories": ["Sender memory"]},
            ],
        }
    }

    asyncio.run(instance._on_text_received(envelope))

    text, mode, meta = instance.sessions.session.turns[0]
    framed = frame_inbound(mode, meta, text)
    assert meta["contact"]["name"] == "Resolved Sender"
    assert meta["contact"]["notes"] == "Authoritative note"
    assert meta["contact_memories"] == ["Sender memory"]
    assert framed.count("[inkbox:contact_memories]") == 1
    assert '"Other memory"' not in framed


def test_ambiguous_contacts_and_disabled_flag_suppress_memories():
    contacts = [
        {"id": "one", "memories": ["One"]},
        {"id": "two", "memories": ["Two"]},
    ]
    assert gateway.InkboxGateway._matched_payload_contact(contacts) is None

    instance = _gateway(enabled=False)
    assert instance._webhook_contact_memories({"memories": ["Hidden"]}) == []


def test_reaction_preframing_receives_one_memory_block():
    instance = _gateway(resolved={
        "id": "contact-1",
        "preferred_name": "Resolved Reactor",
        "phones": [{"value": "+15551234567"}],
    })
    envelope = {
        "data": {
            "reaction": {
                "id": "reaction-1",
                "direction": "inbound",
                "remote_number": "+15551234567",
                "conversation_id": "imessage-1",
                "target_message_id": "message-1",
                "reaction": "question",
            },
            "contacts": [{"id": "contact-1", "memories": ["Likes concise answers"]}],
        }
    }

    asyncio.run(instance._on_imessage_reaction_received(envelope))

    text, mode, meta = instance.sessions.session.turns[0]
    framed = frame_inbound(mode, meta, text)
    assert framed.startswith("[inkbox:imessage_reaction")
    assert "contact_name='Resolved Reactor'" in framed
    assert framed.count("[inkbox:contact_memories]") == 1
    assert framed.index("[/inkbox:contact_memories]") < framed.index("reacted with")
