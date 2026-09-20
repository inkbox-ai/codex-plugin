import asyncio

import pytest

from inkbox_codex.escalation import PendingInteraction
from inkbox_codex.prompts import mentions_agent
from tests.test_sessions import make_session


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))


@pytest.mark.parametrize("text", [
    "@agent what do you think?", "hello @Atlas-bot!", "(@ATLAS-BOT)",
    "@agent.", "@atlas-bot’s suggestion", "line one\n@agent, help",
])
def test_explicit_mentions(text):
    assert mentions_agent(text, "atlas-bot")


@pytest.mark.parametrize("text", [
    "hello", "@", "@atlas", "@atlas-bot-extra", "@agents", "@@agent",
    "someone@agent.example", "https://example.com/@agent", "www.example.com/@agent",
    "@atlas-bot.example", "agent please help", "other@atlas-bot",
    "someone+@agent", "+" * 100000,
])
def test_non_mentions(text):
    assert not mentions_agent(text, "atlas-bot")


class Client:
    thread_id = "thread-group"

    def __init__(self):
        self.events = []
        self.interrupts = 0

    async def append_context(self, messages):
        self.events.extend(("context", text) for text in messages)

    async def run(self, text):
        self.events.append(("run", text))
        return "Answer"

    async def interrupt(self):
        self.interrupts += 1


def meta(text, *, sender="alice", group=True):
    return {
        "conversation_id": "group-one",
        "conversation_kind": "group" if group else "direct",
        "sender": sender,
        "raw_text": text,
    }


@pytest.mark.parametrize("channel", ["sms", "imessage"])
def test_only_new_message_mentions_wake_and_background_does_not_reply(channel):
    async def scenario():
        sent, typing = [], []
        session = make_session(sent, typing)
        session.cfg.group_reply_mode = "mention"
        session.identity_info["handle"] = "atlas-bot"
        client = session._client = Client()
        for raw in ["Saturday works", "@atlas-bot summarize", "Thanks"]:
            await session.handle_inbound(
                "[inkbox:group context mentions @agent]\n" + raw, channel, meta(raw)
            )
            await session._worker
        assert [event[0] for event in client.events] == ["context", "run", "context"]
        assert len(sent) == 1
        assert client.interrupts == 0
        assert len(typing) <= 1
    asyncio.run(scenario())


@pytest.mark.parametrize("reply_mode,group", [("auto", True), ("mention", False)])
def test_auto_mode_and_direct_messages_still_start_turns(reply_mode, group):
    async def scenario():
        sent = []
        session = make_session(sent)
        session.cfg.group_reply_mode = reply_mode
        client = session._client = Client()
        await session.handle_inbound("Hello", "sms", meta("Hello", group=group))
        await session._worker
        assert [event[0] for event in client.events] == ["run"]
        assert len(sent) == 1
    asyncio.run(scenario())


def test_group_reaction_is_background_even_when_label_contains_mention():
    async def scenario():
        sent, typing = [], []
        session = make_session(sent, typing)
        session.cfg.group_reply_mode = "mention"
        client = session._client = Client()
        reaction_meta = meta("")
        reaction_meta["reaction"] = "question"
        await session.handle_inbound("Reaction on @agent's message", "imessage", reaction_meta)
        await session._worker
        assert [event[0] for event in client.events] == ["context"]
        assert not sent and not typing
    asyncio.run(scenario())


def test_background_waits_for_active_reply_without_changing_its_target():
    async def scenario():
        sent = []
        session = make_session(sent)
        session.cfg.group_reply_mode = "mention"
        started, finish = asyncio.Event(), asyncio.Event()

        class SlowClient(Client):
            async def run(self, text):
                self.events.append(("run", text))
                started.set()
                await finish.wait()
                return "Answer"

        client = session._client = SlowClient()
        await session.handle_inbound("@agent help", "sms", meta("@agent help"))
        await started.wait()
        await session.handle_inbound("Other chatter", "sms", meta("Other chatter", sender="bob"))
        assert [event[0] for event in client.events] == ["run"]
        assert client.interrupts == 0
        assert session.reply_meta["sender"] == "alice"
        finish.set()
        await session._worker
        assert [event[0] for event in client.events] == ["run", "context"]
        assert len(sent) == 1 and sent[0][3]["sender"] == "alice"
    asyncio.run(scenario())


def test_failed_context_append_is_retried_before_the_next_model_turn():
    async def scenario():
        sent = []
        session = make_session(sent)
        session.cfg.group_reply_mode = "mention"

        class RetryClient(Client):
            attempts = 0

            async def append_context(self, messages):
                self.attempts += 1
                if self.attempts == 1:
                    raise RuntimeError("temporarily unavailable")
                await super().append_context(messages)

        client = session._client = RetryClient()
        await session.handle_inbound("Background", "sms", meta("Background"))
        await session._worker
        assert not sent and session._pending_context
        await session.handle_inbound("@agent summarize", "sms", meta("@agent summarize"))
        await session._worker
        assert [event[0] for event in client.events] == ["context", "run"]
        assert not session._pending_context
    asyncio.run(scenario())


def test_group_commands_use_raw_text_without_requiring_mention(monkeypatch):
    async def scenario():
        session = make_session([])
        session.cfg.group_reply_mode = "mention"
        called = []

        async def stop():
            called.append("stop")

        monkeypatch.setattr(session, "_stop_turn", stop)
        await session.handle_inbound("[inkbox:group_sms]\nPolicy\n/stop", "sms", meta("/stop"))
        assert called == ["stop"]
        assert session._worker is None
    asyncio.run(scenario())


def test_only_valid_permission_answer_from_prompted_sender_consumes_pending():
    async def scenario():
        session = make_session([])
        session.cfg.group_reply_mode = "mention"
        session.reply_meta = meta("@agent help")
        session._client = Client()
        future = asyncio.get_running_loop().create_future()
        session.pending = PendingInteraction(kind="permission", prompt_text="Allow?", future=future)
        for text, sender in [("YES", "bob"), ("ordinary chatter", "alice")]:
            await session.handle_inbound("[inkbox:group_sms]\n" + text, "sms", meta(text, sender=sender))
            assert not future.done()
        await session.handle_inbound("[inkbox:group_sms]\nYES", "sms", meta("YES"))
        assert future.result() == "YES"
        await session._worker
    asyncio.run(scenario())
