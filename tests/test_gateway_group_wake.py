"""``INKBOX_GROUP_WAKE``: how group SMS / iMessage messages start a turn.

``judgement`` (default) is today's behavior: every allowed sender's group
message starts a turn and the model answers or returns ``[SILENT]``.
``mention`` starts a turn only when the message mentions the agent; other
group messages are acked, held in a bounded in-memory buffer per
conversation, and rendered ahead of the next mentioning turn's context.
"""

import asyncio
import itertools
import json
import time
import types

import pytest

from inkbox_codex import gateway, prompts
from inkbox_codex.config import BridgeConfig
from inkbox_codex.prompts import mentions_agent

WAKER = "+15551234567"
OTHER = "+15557654321"
HANDLE = "graham"


@pytest.fixture(autouse=True)
def fake_web(monkeypatch):
    """aiohttp isn't installed in tests; stub the json_response the handlers use."""
    def json_response(payload):
        return types.SimpleNamespace(text=json.dumps(payload), payload=payload)
    monkeypatch.setattr(gateway, "web", types.SimpleNamespace(json_response=json_response))


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


def _gw(**cfg):
    cfg.setdefault("allow_all_users", True)
    cfg.setdefault("identity", HANDLE)
    gw = gateway.InkboxGateway(BridgeConfig(require_signature=False, **cfg))
    gw.sessions = _FakeSessions()
    return gw


def _turns(gw):
    return [turn for session in gw.sessions.by_id.values() for turn in session.inbound]


_ids = itertools.count(1)


def _sms_envelope(text, *, sender=WAKER, group=True, context=None, conversation_id="conv-1"):
    data = {
        "contacts": [],
        "text_message": {
            "id": f"txt-{next(_ids)}", "direction": "inbound",
            "local_phone_number": "+15550000001",
            "remote_phone_number": sender, "sender_phone_number": sender,
            "conversation_id": conversation_id, "text": text,
        },
    }
    if group:
        data["text_message"]["participants"] = [WAKER, OTHER, "+15550009999"]
    if context is not None:
        data["context_messages"] = context
    return {"data": data}


def _imessage_envelope(text, *, sender=WAKER, group=True, context=None, conversation_id="imconv-1"):
    data = {
        "contacts": [],
        "message": {
            "id": f"im-{next(_ids)}", "direction": "inbound",
            "conversation_id": conversation_id, "remote_number": sender, "content": text,
        },
    }
    if group:
        data["message"]["participants"] = [WAKER, OTHER, "+15550009999"]
    if context is not None:
        data["context_messages"] = context
    return {"data": data}


def _sms_item(text, sender=OTHER):
    return {"id": f"ctx-{next(_ids)}", "sender_phone_number": sender, "text": text,
            "media": None, "created_at": "2026-01-01T00:00:00Z"}


def _imessage_item(text, sender=OTHER):
    return {"id": f"ctx-{next(_ids)}", "sender_number": sender, "content": text,
            "media": None, "created_at": "2026-01-01T00:00:00Z"}


def _run_sms(gw, envelope):
    return asyncio.run(gw._on_text_received(envelope))


def _run_imessage(gw, envelope):
    return asyncio.run(gw._on_imessage_received(envelope))


CASES = [
    pytest.param(_run_sms, _sms_envelope, _sms_item, "sms", id="sms"),
    pytest.param(_run_imessage, _imessage_envelope, _imessage_item, "imessage", id="imessage"),
]


# ── The mention matcher ──────────────────────────────────────────────────────


@pytest.mark.parametrize("text", [
    "hey @graham can you check the build?",
    "@Graham, yes",
    "thanks @GRAHAM.",
    "(@graham) what do you think?!",
    "@graham",
    "@graham’s idea works",
    "ünïcode before @graham",
    "line one\n@graham line two",
])
def test_mention_matches_the_handle_as_a_whole_token(text):
    assert mentions_agent(text, HANDLE) is True


@pytest.mark.parametrize("text", [
    "",
    None,
    "@",
    "@gra",
    "@grahamson",
    "@graham-bot",
    "graham, are you there?",
    "@@graham",
    "x@graham",
    "me@graham.com",
    "mail me at bob@graham.example.org",
    "https://x.example/@graham",
    "www.x.example/@graham ok",
    "@graham.com is down",
])
def test_mention_ignores_partial_handles_addresses_and_links(text):
    assert mentions_agent(text, HANDLE) is False


def test_mention_accepts_extra_tokens_the_same_way():
    extra = ["@ai", "aigraham", " ", ""]
    assert mentions_agent("@ai please", HANDLE, extra) is True
    assert mentions_agent("AIGraham!", HANDLE, extra) is True
    assert mentions_agent("email@ai.co", HANDLE, extra) is False
    assert mentions_agent("aigraham.com", HANDLE, extra) is False
    assert mentions_agent("@aigraham", HANDLE, extra) is False
    assert mentions_agent("hello", HANDLE, extra) is False
    # No handle and no tokens => nothing can match.
    assert mentions_agent("@graham", "", []) is False


# ── Gateway behavior ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(("run", "envelope", "item", "mode"), CASES)
def test_judgement_mode_is_unchanged(run, envelope, item, mode):
    # Explicit: the default starts a turn for a group message with no mention.
    gw = _gw()
    assert gw.cfg.group_wake == "judgement"
    response = run(gw, envelope("just chatting", context=[item("earlier")]))
    assert response.payload == {"ok": True}
    (text, _, meta), = _turns(gw)
    assert meta["conversation_kind"] == "group"
    assert text.endswith("\njust chatting")
    assert gw._group_chatter == {}


@pytest.mark.parametrize(("run", "envelope", "item", "mode"), CASES)
def test_judgement_mode_prompt_is_identical_to_a_gateway_without_the_setting(run, envelope, item, mode):
    turns = []
    for cfg in ({}, {"group_wake": "judgement"}):
        gw = _gw(**cfg)
        run(gw, envelope("just chatting", context=[{**item("earlier"), "id": "ctx-same"}]))
        turns.append(_turns(gw)[0][0])
    assert turns[0] == turns[1]
    assert "[inkbox:group_context]" in turns[0]
    assert prompts.GROUP_CONTEXT_ALLOWED_NOTE not in turns[0]


@pytest.mark.parametrize(("run", "envelope", "item", "mode"), CASES)
def test_mention_mode_holds_a_group_message_that_does_not_mention_the_agent(run, envelope, item, mode):
    gw = _gw(group_wake="mention")
    response = run(gw, envelope("lunch at noon?", context=[item("morning all")]))
    assert response.payload == {"ok": True, "ignored": "group-no-mention"}
    assert _turns(gw) == []
    key = f"{mode}:{'conv-1' if mode == 'sms' else 'imconv-1'}"
    (_updated_at, held), = [gw._group_chatter[key]]
    # The webhook's context first, then the sender's own message, oldest first.
    assert [prompts._group_context_item_field(i, "content", "text") for i in held] == [
        "morning all", "lunch at noon?",
    ]


@pytest.mark.parametrize(("run", "envelope", "item", "mode"), CASES)
def test_mention_mode_flushes_held_chatter_ahead_of_the_turn_context(run, envelope, item, mode):
    gw = _gw(group_wake="mention")
    shared = item("shared with both")
    run(gw, envelope("first quiet message", context=[item("before first")]))
    run(gw, envelope("second quiet message", context=[shared]))
    run(gw, envelope("ok @graham, what time?", context=[shared, item("right before")]))

    (text, _, meta), = _turns(gw)
    assert meta["conversation_kind"] == "group"
    block = text[text.index("[inkbox:group_context]"):text.index("[/inkbox:group_context]")]
    assert prompts.GROUP_CONTEXT_ALLOWED_NOTE in block
    order = [
        f'{OTHER}: "before first"',
        f'{WAKER}: "first quiet message"',
        f'{OTHER}: "shared with both"',
        f'{WAKER}: "second quiet message"',
        f'{OTHER}: "right before"',
    ]
    positions = [block.index(line) for line in order]
    assert positions == sorted(positions)
    assert block.count('"shared with both"') == 1
    assert text.endswith("\nok @graham, what time?")
    # Flushed: the next mention starts clean.
    assert gw._group_chatter == {}


def test_mention_mode_keeps_conversations_apart():
    gw = _gw(group_wake="mention")
    _run_sms(gw, _sms_envelope("only in one", conversation_id="conv-1"))
    _run_sms(gw, _sms_envelope("only in two", conversation_id="conv-2"))
    _run_sms(gw, _sms_envelope("@graham here", conversation_id="conv-1"))
    (text, _, _), = _turns(gw)
    assert '"only in one"' in text
    assert '"only in two"' not in text
    assert set(gw._group_chatter) == {"sms:conv-2"}


def test_mention_mode_extra_tokens_wake_the_agent():
    gw = _gw(group_wake="mention", group_wake_mentions=["@ai"])
    _run_sms(gw, _sms_envelope("@ai what's up"))
    assert len(_turns(gw)) == 1


def test_held_chatter_is_bounded_and_expires(monkeypatch):
    gw = _gw(group_wake="mention")
    for index in range(gateway.GROUP_CHATTER_MAX_ITEMS + 5):
        _run_sms(gw, _sms_envelope(f"chatter {index}"))
    _updated_at, held = gw._group_chatter["sms:conv-1"]
    assert len(held) == gateway.GROUP_CHATTER_MAX_ITEMS
    assert held[0]["text"] == "chatter 5"
    assert held[-1]["text"] == f"chatter {gateway.GROUP_CHATTER_MAX_ITEMS + 4}"

    # Past the TTL the held chatter is gone, so a mention sees no block.
    now = time.time()
    monkeypatch.setattr(gateway.time, "time", lambda: now + gateway.GROUP_CHATTER_TTL_SECONDS + 1)
    _run_sms(gw, _sms_envelope("@graham anything?"))
    (text, _, _), = _turns(gw)
    assert "[inkbox:group_context]" not in text
    assert gw._group_chatter == {}


def test_held_chatter_conversation_count_is_bounded():
    gw = _gw(group_wake="mention")
    for index in range(gateway.GROUP_CHATTER_MAX_CONVERSATIONS + 3):
        _run_sms(gw, _sms_envelope("hi", conversation_id=f"conv-{index}"))
    assert len(gw._group_chatter) == gateway.GROUP_CHATTER_MAX_CONVERSATIONS


def test_mention_mode_rendering_of_a_flushed_block_is_capped():
    gw = _gw(group_wake="mention")
    for index in range(gateway.GROUP_CHATTER_MAX_ITEMS):
        _run_sms(gw, _sms_envelope("x" * prompts.GROUP_CONTEXT_MAX_TEXT_CHARS + f" {index}"))
    _run_sms(gw, _sms_envelope("@graham summary?"))
    (text, _, _), = _turns(gw)
    block = text[text.index("[inkbox:group_context]"):text.index("[/inkbox:group_context]")]
    assert "[earlier context messages omitted]" in block
    assert len(block) <= prompts.GROUP_CONTEXT_MAX_BLOCK_CHARS + 1200


def test_held_chatter_without_a_conversation_id_is_dropped_not_kept():
    gw = _gw(group_wake="mention")
    envelope = _sms_envelope("no id here")
    envelope["data"]["text_message"]["conversation_id"] = ""
    response = _run_sms(gw, envelope)
    assert response.payload == {"ok": True, "ignored": "group-no-mention"}
    assert gw._group_chatter == {}


@pytest.mark.parametrize(("run", "envelope", "item", "mode"), CASES)
def test_mention_mode_leaves_direct_messages_alone(run, envelope, item, mode):
    gw = _gw(group_wake="mention")
    response = run(gw, envelope("no mention here", group=False))
    assert response.payload == {"ok": True}
    (text, _, meta), = _turns(gw)
    assert meta["conversation_kind"] == "direct"
    assert gw._group_chatter == {}


def test_mention_mode_leaves_email_alone():
    gw = _gw(group_wake="mention")
    gw._inkbox = None
    asyncio.run(gw._on_mail_received({"data": {"contacts": [], "message": {
        "id": "mail-1", "from_address": "owner@example.com", "subject": "No mention",
        "body_text": "Just an email.", "thread_id": "t-1",
    }}}))
    (text, mode, _), = _turns(gw)
    assert mode == "email"


def test_mention_mode_allowlist_still_gates_the_sender_first():
    gw = _gw(group_wake="mention", allow_all_users=False, allowed_users=[OTHER])
    response = _run_sms(gw, _sms_envelope("@graham hi", sender=WAKER))
    assert response.payload == {"ok": True, "ignored": "sender-not-allowed"}
    assert gw._group_chatter == {}
