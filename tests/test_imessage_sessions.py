import asyncio
from copy import deepcopy

import pytest

from inkbox_codex import sessions as sessions_mod
from inkbox_codex.codex_client import CodexAppServerError, CodexTurnResult
from inkbox_codex.config import BridgeConfig
from inkbox_codex.escalation import PendingInteraction
from inkbox_codex.prompts import build_channel_prompt
from inkbox_codex.sessions import ContactSession, _Turn


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))


class Client:
    thread_id = "existing-conversation"

    def __init__(self, reply="Done."):
        self.calls = []
        self.context = []
        self.interrupts = 0
        self.reply = reply
        self.gate = None
        self.started = asyncio.Event()

    async def run(self, text):
        self.calls.append(text)
        self.started.set()
        if self.gate is not None:
            await self.gate.wait()
        return self.reply

    async def append_context(self, messages):
        self.context.extend(messages)

    async def run_detailed(self, text):
        return CodexTurnResult(text=await self.run(text), mcp_tool_calls=())

    async def interrupt(self):
        self.interrupts += 1
        if self.gate is not None:
            self.gate.set()

    async def disconnect(self):
        if self.gate is not None:
            self.gate.set()


def make_session(*, enabled=True, hook=None, send=None):
    sent, states = [], []

    async def send_fn(chat_id, text, mode, meta):
        sent.append((text, mode, deepcopy(meta)))
        if send is not None:
            await send(chat_id, text, mode, meta)

    async def state_fn(chat_id, state, meta, text):
        states.append((state, deepcopy(meta), text))
        return await hook(chat_id, state, meta, text) if hook else None

    cfg = BridgeConfig(project_dir="/tmp", permission_timeout_s=1)
    cfg.imessage_threaded_replies = enabled
    session = ContactSession(
        "contact-example", cfg, send_fn, {}, {"handle": "example-agent"},
        imessage_turn_fn=state_fn,
    )
    session._client = Client()
    return session, sent, states


def source(number, **extra):
    return {
        "message_id": f"message-{number}",
        "imessage_event_id": f"event-{number}",
        "conversation_id": "conversation-example",
        "sender": "+15555550123",
        "thread_id": f"standalone-thread-{number}",
        **extra,
    }


async def finish_burst(session):
    session._flush_imessage_burst()
    if session._worker is not None:
        await session._worker


def test_top_level_fragments_with_distinct_native_ids_share_one_turn():
    async def scenario():
        session, sent, states = make_session()
        original = source(1)
        await session.handle_inbound("Find dinner", "imessage", original)
        await session.handle_inbound("Friday", "imessage", source(2))
        await session.handle_inbound("six people", "imessage", source(3))
        original["conversation_id"] = "different-conversation"
        assert not session._client.calls
        await finish_burst(session)
        assert len(session._client.calls) == 1
        assert session._client.thread_id == "existing-conversation"
        assert session._client.calls[0].endswith("Find dinner\n\nFriday\n\nsix people")
        assert sent[0][2]["conversation_id"] == "conversation-example"
        assert sent[0][2]["imessage_reply_target"] == "message-1"
        assert [row["id"] for row in sent[0][2]["imessage_sources"]] == [
            "message-1", "message-2", "message-3",
        ]
        assert sent[0][2]["imessage_event_ids"] == ["event-1", "event-2", "event-3"]
        assert [state for state, _, _ in states] == [
            "admitted", "admitted", "admitted", "started", "result", "done",
        ]
    asyncio.run(scenario())


@pytest.mark.parametrize("different", [
    {"sender": "+15555550124"},
    {"conversation_id": "other-conversation"},
    {"sender_access": "sponsored"},
    {"companion_scope_id": "different-scope"},
    {"reply_to_message_id": "earlier-message", "thread_root_message_id": "earlier-root"},
])
def test_incompatible_inputs_keep_separate_routes(different):
    async def scenario():
        session, sent, _ = make_session()
        await session.handle_inbound("First request", "imessage", source(1))
        await session.handle_inbound("Second request", "imessage", source(2, **different))
        await finish_burst(session)
        assert len(session._client.calls) == 2
        assert [entry[2]["message_id"] for entry in sent] == ["message-1", "message-2"]
        assert all(len(entry[2]["imessage_sources"]) == 1 for entry in sent)
    asyncio.run(scenario())


def test_native_reply_targets_current_source_without_forking_session():
    async def scenario():
        session, sent, _ = make_session()
        await session.handle_inbound("Make it shorter", "imessage", source(
            1, reply_to_message_id="previous-answer", thread_root_message_id="original-root",
        ))
        await finish_burst(session)
        assert sent[0][2]["imessage_reply_target"] == "message-1"
        assert '"reply_to_message_id": "previous-answer"' in session._client.calls[0]
        assert session._client.thread_id == "existing-conversation"
    asyncio.run(scenario())


@pytest.mark.parametrize("root, expected", [("earlier-root", "message-1"), ("message-1", None)])
def test_visible_root_identifies_reply_when_immediate_parent_is_unavailable(root, expected):
    async def scenario():
        session, sent, _ = make_session()
        await session.handle_inbound("More on this", "imessage", source(1, thread_root_message_id=root))
        await finish_burst(session)
        assert sent[0][2].get("imessage_reply_target") == expected
    asyncio.run(scenario())


def test_known_descendant_without_parent_does_not_merge_with_top_level():
    async def scenario():
        session, sent, _ = make_session()
        await session.handle_inbound("New request", "imessage", source(1, thread_root_message_id="message-1"))
        await session.handle_inbound("An earlier topic", "imessage", source(2, thread_root_message_id="earlier-root"))
        await finish_burst(session)
        assert len(session._client.calls) == 2
        assert sent[1][2]["imessage_reply_target"] == "message-2"
    asyncio.run(scenario())


def test_simple_message_remains_untargeted_and_quiet_timer_flushes(monkeypatch):
    monkeypatch.setattr(sessions_mod, "IMESSAGE_BURST_QUIET_SECONDS", 0.001)

    async def scenario():
        session, sent, _ = make_session()
        await session.handle_inbound("Hello", "imessage", source(1))
        assert not session._client.calls
        await asyncio.wait_for(session._imessage_burst_task, timeout=1)
        await session._worker
        assert len(sent) == 1
        assert not sent[0][2].get("imessage_reply_target")
    asyncio.run(scenario())


def test_continuous_burst_has_a_maximum_deadline(monkeypatch):
    async def scenario():
        session, _, _ = make_session()
        loop = asyncio.get_running_loop()
        real_clock = loop.time
        now = [100.0]
        deadlines = []

        async def park_timer(deadline):
            deadlines.append(deadline)
            await asyncio.Event().wait()

        monkeypatch.setattr(session, "_wait_imessage_burst", park_timer)
        monkeypatch.setattr(loop, "time", lambda: now[0])
        try:
            for index, instant in enumerate((100.0, 100.6, 101.2, 101.8)):
                now[0] = instant
                await session.handle_inbound("More details", "imessage", source(index))
                await asyncio.sleep(0)
            assert deadlines == pytest.approx([100.75, 101.35, 101.95, 102.0])
        finally:
            monkeypatch.setattr(loop, "time", real_clock)
        await finish_burst(session)
        assert len(session._client.calls) == 1
    asyncio.run(scenario())


def test_busy_correction_queues_without_interrupting_original_work():
    async def scenario():
        session, sent, _ = make_session()
        client = session._client
        client.gate = asyncio.Event()
        await session.handle_inbound("Search Friday", "imessage", source(1))
        session._flush_imessage_burst()
        await client.started.wait()
        await session.handle_inbound("Actually Saturday", "imessage", source(2))
        session._flush_imessage_burst()
        assert client.interrupts == 0
        assert session._queue.qsize() == 1
        client.gate.set()
        await session._worker
        assert len(client.calls) == 2
        assert client.calls[1].endswith("Actually Saturday")
        assert [entry[2]["message_id"] for entry in sent] == ["message-1", "message-2"]
    asyncio.run(scenario())


def test_unmentioned_group_context_is_not_collected_as_a_request():
    async def scenario():
        session, sent, _ = make_session()
        session.cfg.group_reply_mode = "mention"
        await session.handle_inbound("Background", "imessage", source(1, conversation_kind="group"))
        await session.handle_inbound("@example-agent help", "imessage", source(2, conversation_kind="group"))
        await finish_burst(session)
        assert len(session._client.calls) == 1
        assert len(session._client.context) == 1
        assert session._client.context[0].endswith("Background")
        assert [row["id"] for row in sent[0][2]["imessage_sources"]] == ["message-2"]
    asyncio.run(scenario())


def test_approval_answer_bypasses_buffer_but_new_request_does_not_cancel():
    async def scenario():
        session, _, states = make_session()
        future = asyncio.get_running_loop().create_future()
        session.pending = PendingInteraction(kind="permission", prompt_text="Allow?", future=future)
        await session.handle_inbound("Also check tomorrow", "imessage", source(1))
        assert not future.done()
        assert session._imessage_burst is not None
        await session.handle_inbound("yes", "imessage", source(2))
        assert future.result() == "yes"
        assert [row["id"] for row in session._imessage_burst.reply_meta["imessage_sources"]] == ["message-1"]
        assert any(state == "done" and meta["message_id"] == "message-2" for state, meta, _ in states)
        assert [meta["message_id"] for state, meta, _ in states if state == "consumed"] == ["message-2"]
        await finish_burst(session)
    asyncio.run(scenario())


def test_invalid_poll_answer_is_not_checkpointed_as_consumed():
    async def scenario():
        session, sent, states = make_session()
        future = asyncio.get_running_loop().create_future()
        session.pending = PendingInteraction(
            kind="poll", prompt_text="Choose one", future=future,
            validate_reply=lambda text: text == "1",
        )
        await session.handle_inbound("unsupported answer", "imessage", source(1))
        assert not future.done()
        assert len(sent) == 1
        assert [state for state, _, _ in states] == ["admitted", "done"]
    asyncio.run(scenario())


def test_control_consumption_is_checkpointed_before_effect():
    async def scenario():
        order = []

        async def hook(_chat, state, _meta, _text):
            order.append(state)

        async def send(*_):
            order.append("sent")

        session, _, _ = make_session(hook=hook, send=send)
        session.on_clear = lambda _chat: order.append("cleared")
        await session.handle_inbound("/clear", "imessage", source(1))
        assert order.index("consumed") < order.index("cleared") < order.index("done")
    asyncio.run(scenario())


def test_stop_drops_buffer_and_queue_but_preserves_voice_capture():
    async def scenario():
        session, sent, states = make_session()
        client = session._client
        voice = _Turn(text="Voice work", future=asyncio.get_running_loop().create_future())
        session._current_turn = voice
        session._turn_active = True
        session._worker = asyncio.create_task(asyncio.Event().wait())
        await session.handle_inbound("First", "imessage", source(1))
        session._flush_imessage_burst()
        await session.handle_inbound("Second", "imessage", source(2))
        await session.handle_inbound("/stop", "imessage", source(3))
        assert session._imessage_burst is None
        assert session._queue.empty()
        assert client.interrupts == 0
        assert not voice.future.done()
        assert sent[-1][0] == "Stopped."
        assert {meta["message_id"] for state, meta, _ in states if state == "cancelled"} == {
            "message-1", "message-2",
        }
        session._worker.cancel()
        await asyncio.gather(session._worker, return_exceptions=True)
    asyncio.run(scenario())


def test_stop_can_cancel_an_idle_resume_question():
    async def scenario():
        session, sent, _ = make_session()
        session.mode, session.reply_meta = "imessage", source(1)
        question = asyncio.create_task(session._escalate("resume", "Choose a conversation"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert session.pending is not None
        await session.handle_inbound("/stop", "imessage", source(2))
        assert await question is None
        assert sent[-1][0] == "Stopped."
    asyncio.run(scenario())


@pytest.mark.parametrize("enabled, mode", [(False, "imessage"), (True, "sms")])
def test_disabled_and_other_channel_keep_existing_interrupt_behavior(enabled, mode):
    async def scenario():
        session, _, states = make_session(enabled=enabled)
        session._turn_active = True
        session._current_turn = _Turn(text="Earlier work")
        session._worker = asyncio.create_task(asyncio.Event().wait())
        await session.handle_inbound("New input", mode, source(1))
        assert session._client.interrupts == 1
        assert session._queue.qsize() == 1
        assert not states
        session._worker.cancel()
        await asyncio.gather(session._worker, return_exceptions=True)
    asyncio.run(scenario())


def test_duplicate_admission_never_reaches_model():
    async def reject(*_):
        return False

    async def scenario():
        session, sent, _ = make_session(hook=reject)
        await session.handle_inbound("Duplicate", "imessage", source(1))
        assert session._imessage_burst is None
        assert not session._client.calls
        assert not sent
    asyncio.run(scenario())


def test_exact_tool_delivery_suppresses_only_correlated_final():
    async def hook(_chat, state, _meta, text):
        return True if state == "result" and text == "Already delivered." else None

    async def scenario():
        session, sent, _ = make_session(hook=hook)
        session._client.reply = "Already delivered."
        await session.handle_inbound("First", "imessage", source(1))
        await finish_burst(session)
        assert sent[0][0] == "[SILENT]"
        session._client.reply = "A different answer."
        await session.handle_inbound("Second", "imessage", source(2))
        await finish_burst(session)
        assert sent[1][0] == "A different answer."
    asyncio.run(scenario())


def test_only_automatic_final_carries_final_delivery_marker():
    async def scenario():
        session, sent, states = make_session()

        async def run(_text):
            await session._reply("Approval question", turn=session._current_turn)
            return "The answer."

        session._client.run = run
        await session.handle_inbound("Work", "imessage", source(1))
        await finish_burst(session)
        assert not sent[0][2].get("imessage_final_reply")
        assert sent[1][2]["imessage_final_reply"] is True
        assert all(not meta.get("imessage_final_reply") for _, meta, _ in states)
    asyncio.run(scenario())


def test_uncertain_send_never_enqueues_recovery_or_another_notice():
    async def fail(*_):
        raise TimeoutError("send outcome unknown")

    async def scenario():
        session, sent, states = make_session(send=fail)
        await session.handle_inbound("Work", "imessage", source(1, reply_to_message_id="earlier"))
        await finish_burst(session)
        assert len(sent) == 1
        assert len(session._client.calls) == 1
        assert session._queue.empty()
        assert states[-1][0] == "uncertain"
        assert sent[0][2]["imessage_reply_target"] == "message-1"
    asyncio.run(scenario())


def test_companion_keeps_its_own_checkpoint_and_completion():
    async def scenario():
        session, sent, states = make_session()
        session.cfg.group_reply_mode = "all"
        checkpoints = []

        async def checkpoint():
            checkpoints.append("submitted")

        meta = source(1, companion=True, sender_access="direct", source_message_id="message-1")
        reply = await session.submit_companion("Current input", "imessage", meta, before_submit=checkpoint)
        await session._worker
        assert reply == "Done."
        assert checkpoints == ["submitted"]
        assert not sent
        assert [state for state, _, _ in states] == ["started", "result", "done"]
        assert session._imessage_burst is None
    asyncio.run(scenario())


def test_delivery_notice_adds_context_without_model_or_receipt_hooks():
    async def scenario():
        session, sent, states = make_session()
        await session.append_delivery_notice("A previous message failed.", "imessage", source(1))
        await session._worker
        assert not session._client.calls
        assert session._client.context[0].endswith("A previous message failed.")
        assert not sent
        assert not states
    asyncio.run(scenario())


@pytest.mark.parametrize("method", ["run_consult", "run_consult_detailed"])
def test_later_voice_consult_runs_after_already_buffered_imessage(method):
    async def scenario():
        session, sent, _ = make_session()
        await session.handle_inbound("Earlier text", "imessage", source(1))
        await getattr(session, method)("Later call work")
        assert len(session._client.calls) == 2
        assert session._client.calls[0].endswith("Earlier text")
        assert session._client.calls[1] == "Later call work"
        assert len(sent) == 1
    asyncio.run(scenario())


def test_reaction_does_not_join_text_burst_or_create_a_receipt():
    async def scenario():
        session, sent, states = make_session()
        await session.handle_inbound("A request", "imessage", source(1))
        await session.handle_inbound("Liked a message", "imessage", source(2, reaction=True))
        await session._worker
        assert len(session._client.calls) == 2
        assert len(sent[0][2]["imessage_sources"]) == 1
        assert "imessage_sources" not in sent[1][2]
        assert "imessage_reply_target" not in sent[1][2]
        assert all(meta["message_id"] == "message-1" for _, meta, _ in states)
    asyncio.run(scenario())


def test_media_flushes_preceding_text_without_merging_source_ids():
    async def scenario():
        session, sent, _ = make_session()
        await session.handle_inbound("A request", "imessage", source(1))
        await session.handle_inbound("An attachment", "imessage", source(2, attachments=[{"id": "file-example"}]))
        await session._worker
        assert len(session._client.calls) == 2
        assert [len(entry[2]["imessage_sources"]) for entry in sent] == [1, 1]
        assert session._imessage_burst is None
    asyncio.run(scenario())


def test_incidental_client_close_does_not_discard_buffered_input():
    async def scenario():
        session, sent, _ = make_session()
        await session.handle_inbound("Waiting input", "imessage", source(1))
        await session.close()
        assert session._imessage_burst is not None
        session._client = Client()
        await finish_burst(session)
        assert len(sent) == 1
    asyncio.run(scenario())


def test_shutdown_retains_unsubmitted_receipts_without_late_execution():
    async def scenario():
        session, sent, states = make_session()
        client = session._client
        await session.handle_inbound("Waiting input", "imessage", source(1))
        await session.close(shutting_down=True)
        await asyncio.sleep(0)
        assert session._imessage_burst is None
        assert not client.calls
        assert not sent
        assert [state for state, _, _ in states] == ["admitted"]
    asyncio.run(scenario())


def test_shutdown_does_not_complete_active_companion_receipt_as_success():
    async def scenario():
        session, sent, states = make_session()
        session.cfg.group_reply_mode = "all"
        client = session._client
        client.gate = asyncio.Event()

        async def checkpoint():
            pass

        task = asyncio.create_task(session.submit_companion(
            "A request", "imessage", source(1, companion=True, sender_access="direct"),
            before_submit=checkpoint,
        ))
        await client.started.wait()
        await session.close(shutting_down=True)
        with pytest.raises(CodexAppServerError, match="stopped before"):
            await task
        await session._worker
        assert states[-1][0] == "uncertain"
        assert not sent
    asyncio.run(scenario())


def test_threaded_prompt_is_opt_in_and_retains_automatic_delivery_default():
    default = build_channel_prompt("/tmp")
    enabled = build_channel_prompt("/tmp", imessage_threaded_replies=True)
    assert "Source-aware iMessage replies" not in default
    assert "Source-aware iMessage replies" in enabled
    assert "ordinary final reply is sent automatically" in enabled
    assert "return\nexactly [SILENT]" in enabled
