"""Slack admission, durable context, and exact reply destinations."""

import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_codex.companion import CompanionError, Event
from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway
from inkbox_codex.sessions import ContactSession, SessionManager
from tests.test_companion import SDK, drained
from tests.test_group_reply_mode import Client


def fixture():
    return json.loads((Path(__file__).parent / "fixtures/companion/slack.json").read_text())


def source_id(envelope):
    suffix = envelope["data"]["message_ts"].split(".")[-1]
    return f"40000000-0000-4000-8000-{int(suffix):012}"


def prepared(envelope):
    result = deepcopy(envelope)
    result["_codex_slack_source"] = {
        "id": source_id(envelope),
        "author": f"THOME:{envelope['data']['actor_id']}",
        "thread_ts": envelope["data"].get("thread_ts"),
        "bot_user_id": "UBOT",
    }
    return result


def live(original, *, sequence=2, access="direct", actor="UALICE", mentioned=True, text=None):
    result = deepcopy(original)
    result["id"] = f"evt_slack_source_{200 + sequence}"
    result["companion"] = {
        key: value for key, value in result["companion"].items()
        if key not in {"history", "history_complete", "history_next_cursor", "reply_context"}
    }
    result["companion"].update(phase="live", sequence=sequence)
    data = result["data"]
    data.update(actor_id=actor, sender_access=access, message_ts=f"1767268999.{200 + sequence:06}")
    data["message_kinds"] = ["channel", "thread"] + (["mention"] if mentioned else [])
    data["event"].update(user=actor, ts=data["message_ts"], text=text or (
        "<@UBOT> Please check the updated draft." if mentioned else "@agent <@UOTHER> a background update."
    ))
    return result


def gateway(monkeypatch, tmp_path, envelope=None, *, response_mode="safe", reply_mode="mention", normalize=True):
    from inkbox_codex import slack_companion

    envelope = envelope or fixture()
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    if normalize:
        monkeypatch.setattr(slack_companion, "prepare_envelope", lambda client, identity, payload: prepared(payload))
        monkeypatch.setattr(slack_companion, "require_sdk_support", lambda: None)
    cfg = BridgeConfig(identity="test-agent", slack_enabled=True,
                       companion_response_mode=response_mode, group_reply_mode=reply_mode)
    gw = InkboxGateway(cfg)
    gw._identity = NS(id=envelope["data"]["identity_id"])
    sdk = SDK(envelope)
    # Snapshot scope is channel-wide; the webhook keeps its message's route.
    sdk.c["reply_context"]["thread_ts"] = None
    slack = Mock()
    slack.send_message.return_value = NS(id="action-test", status="sent")
    gw._inkbox = NS(companion=sdk, slack=slack)
    gw.sessions = SessionManager(cfg, gw.send_to_contact, {}, {"handle": "test-agent"})

    async def ensure_client(session):
        if session._client is None:
            session._client = Client()
        return session._client

    monkeypatch.setattr(ContactSession, "_ensure_client", ensure_client)
    return gw, sdk, slack


def session_for(gw, envelope):
    receiver = gw._companion()
    return gw.sessions.get(Event.parse(prepared(envelope)).session_key(receiver.namespace))


@pytest.mark.parametrize("response_mode,access,mentioned,wakes", [
    ("safe", "direct", True, True),
    ("safe", "sponsored", True, False),
    ("safe", "direct", False, False),
    ("relaxed", "sponsored", True, True),
    ("relaxed", "sponsored", False, False),
])
def test_live_access_and_native_mentions_gate_model_not_context(tmp_path, monkeypatch, response_mode, access, mentioned, wakes):
    async def scenario():
        initial = fixture()
        gw, _, slack = gateway(monkeypatch, tmp_path, initial, response_mode=response_mode)
        receiver = gw._companion()
        try:
            await receiver.accept(initial)
            await drained(receiver)
            session = session_for(gw, initial)
            session._client.events.clear()
            slack.send_message.reset_mock()
            incoming = live(initial, access=access, mentioned=mentioned)
            assert await receiver.accept(incoming)
            await drained(receiver)
            assert [kind for kind, _ in session._client.events] == ["run" if wakes else "context"]
            assert f"sender_access={access}" in session._client.events[0][1]
            assert session._client.interrupts == 0
            assert slack.send_message.call_count == int(wakes)
            assert not await receiver.accept(incoming)
            await drained(receiver)
            assert len(session._client.events) == 1
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("thread", [None, "1767268800.000100"])
def test_initialization_uses_native_reply_scope_and_original_sponsor(tmp_path, monkeypatch, thread):
    async def scenario():
        initial = fixture()
        initial["data"]["thread_ts"] = thread
        initial["companion"]["reply_context"]["thread_ts"] = thread
        gw, _, slack = gateway(monkeypatch, tmp_path, initial)
        receiver = gw._companion()
        try:
            await receiver.accept(initial)
            await drained(receiver)
            session = session_for(gw, initial)
            prompt = session._client.events[0][1]
            for entry in initial["companion"]["history"]:
                assert prompt.count(entry["text"]) == 1
            call = slack.send_message.call_args
            assert call.args == (initial["data"]["connection_id"],)
            assert call.kwargs["conversation_id"] == "CEXAMPLE"
            assert call.kwargs["thread_ts"] == thread
            followup = live(initial, actor="UBOB", text="<@UBOT> /stop")
            await receiver.accept(followup)
            await drained(receiver)
            assert session._client.interrupts == 0
            assert [kind for kind, _ in session._client.events] == ["run", "run"]
            assert receiver.inbox.activation(Event.parse(prepared(initial)))[1] == "THOME:UALICE"
            assert slack.send_message.call_count == 2
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("snapshot_thread", [None, "1767268700.000100"])
def test_channel_scope_keeps_shared_context_and_each_sources_reply_thread(tmp_path, monkeypatch, snapshot_thread):
    async def scenario():
        initial = fixture()
        gw, sdk, slack = gateway(monkeypatch, tmp_path, initial)
        sdk.c["reply_context"]["thread_ts"] = snapshot_thread
        receiver = gw._companion()
        try:
            await receiver.accept(initial)
            await drained(receiver)
            session = session_for(gw, initial)
            assert session._client is not None
            for sequence, thread in ((2, "1767268801.000100"), (3, None)):
                current = live(initial, sequence=sequence)
                current["data"]["thread_ts"] = thread
                current["data"]["event"]["thread_ts"] = thread
                await receiver.accept(current)
                await drained(receiver)
                assert session_for(gw, current) is session
            assert sdk.loads == 1
            assert len(session._client.events) == 3
            assert [call.kwargs["thread_ts"] for call in slack.send_message.call_args_list] == [
                initial["data"]["thread_ts"], "1767268801.000100", None,
            ]
            assert receiver.inbox.db.execute("SELECT COUNT(*) FROM events WHERE state='done'").fetchone()[0] == 3
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_legacy_thread_binding_accepts_another_thread_but_not_another_channel(tmp_path, monkeypatch):
    async def scenario():
        initial = fixture()
        gw, _, _ = gateway(monkeypatch, tmp_path, initial)
        receiver = gw._companion()
        event = Event.parse(prepared(initial))
        try:
            receiver.inbox.accept(event)
            legacy = json.dumps([event.conversation, initial["data"]["connection_id"],
                                 initial["data"]["conversation_id"], initial["data"]["thread_ts"]])
            with receiver.inbox.db:
                receiver.inbox.db.execute("UPDATE scope_bindings SET conversation=?", (legacy,))
            await receiver.close()
            gw, _, _ = gateway(monkeypatch, tmp_path, initial)
            receiver = gw._companion()
            other = live(initial)
            other["data"]["thread_ts"] = "1767268801.000100"
            assert receiver.inbox.accept(Event.parse(prepared(other)))
            wrong = live(initial, sequence=3)
            wrong["data"]["conversation_id"] = "COTHER"
            with pytest.raises(CompanionError, match="changed its conversation"):
                receiver.inbox.accept(Event.parse(prepared(wrong)))
            binding = json.loads(receiver.inbox.db.execute("SELECT conversation FROM scope_bindings").fetchone()[0])
            assert binding == [event.conversation, initial["data"]["connection_id"], "CEXAMPLE"]
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_saved_reply_cannot_move_to_another_thread_in_the_same_channel(tmp_path, monkeypatch):
    async def scenario():
        initial = fixture()
        gw, _, _ = gateway(monkeypatch, tmp_path, initial)
        receiver = gw._companion()
        try:
            meta = receiver.meta(Event.parse(prepared(initial)))
            meta["thread_ts"] = "1767268801.000100"
            with pytest.raises(CompanionError, match="thread"):
                receiver.check_reply_route(meta)
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_unmentioned_channel_message_is_context_for_the_next_thread(tmp_path, monkeypatch):
    async def scenario():
        initial = fixture()
        gw, _, slack = gateway(monkeypatch, tmp_path, initial)
        receiver = gw._companion()
        try:
            await receiver.accept(initial)
            await drained(receiver)
            quiet = live(initial, actor="UBOB", access="sponsored", mentioned=False)
            quiet["event_type"] = "slack.channel_message_received"
            quiet["data"].update(thread_ts=None, message_kinds=["channel"])
            quiet["data"]["event"].pop("thread_ts", None)
            await receiver.accept(quiet)
            await drained(receiver)
            current = live(initial, sequence=3)
            current["data"]["thread_ts"] = "1767268801.000100"
            current["data"]["event"]["thread_ts"] = current["data"]["thread_ts"]
            await receiver.accept(current)
            await drained(receiver)
            events = session_for(gw, initial)._client.events
            assert [kind for kind, _ in events] == ["run", "context", "run"]
            assert quiet["data"]["event"]["text"] in events[1][1]
            assert slack.send_message.call_count == 2
            assert slack.send_message.call_args.kwargs["thread_ts"] == current["data"]["thread_ts"]
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("phase", ["initialization", "live"])
def test_late_first_delivery_does_not_replay_old_trigger_as_a_request(tmp_path, monkeypatch, phase):
    async def scenario():
        original = fixture()
        current = live(original, access="sponsored", actor="UBOB")
        current["companion"]["phase"] = phase
        gw, sdk, slack = gateway(monkeypatch, tmp_path, original)
        receiver = gw._companion()
        try:
            await receiver.accept(current)
            await drained(receiver)
            session = session_for(gw, current)
            assert all(kind == "context" for kind, _ in session._client.events)
            text = "\n".join(text for _, text in session._client.events)
            for entry in original["companion"]["history"]:
                assert text.count(entry["text"]) == 1
            assert text.count(current["data"]["event"]["text"]) == 1
            assert sdk.loads == 1
            slack.send_message.assert_not_called()
            assert not await receiver.accept(current)
            await drained(receiver)
            assert sdk.loads == 1
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_send_timeout_checkpoints_uncertainty_and_never_replays_model(tmp_path, monkeypatch):
    async def scenario():
        initial = fixture()
        gw, _, slack = gateway(monkeypatch, tmp_path, initial)
        receiver = gw._companion()
        seen = []
        database = receiver.inbox.db.execute("PRAGMA database_list").fetchone()[2]

        def timeout(*args, **kwargs):
            with sqlite3.connect(database) as db:
                seen.append(db.execute("SELECT state FROM events").fetchone()[0])
            raise TimeoutError("delivery acknowledgment unavailable")

        slack.send_message.side_effect = timeout
        try:
            await receiver.accept(initial)
            await drained(receiver)
            assert seen == ["sending"]
            assert receiver.inbox.db.execute("SELECT state FROM events").fetchone()[0] == "uncertain"
            assert len(session_for(gw, initial)._client.events) == 1
            assert slack.send_message.call_count == 1
            assert receiver.inbox.db.execute("SELECT COUNT(*) FROM replies").fetchone()[0] == 1
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_distinct_activation_and_thread_do_not_share_context(tmp_path, monkeypatch):
    async def scenario():
        first = fixture()
        second = deepcopy(first)
        second["id"] = "evt_other_thread"
        second["companion"].update(scope_id="40000000-0000-4000-8000-000000000021",
                                    activation_id="40000000-0000-4000-8000-000000000022")
        second["data"]["thread_ts"] = "1767268801.000100"
        second["companion"]["reply_context"]["thread_ts"] = second["data"]["thread_ts"]
        gw, sdk, slack = gateway(monkeypatch, tmp_path, first)
        receiver = gw._companion()
        try:
            await receiver.accept(first)
            await drained(receiver)
            first_session = session_for(gw, first)
            sdk.c = deepcopy(second["companion"])
            await receiver.accept(second)
            await drained(receiver)
            second_session = session_for(gw, second)
            assert first_session is not second_session
            assert len(first_session._client.events) == len(second_session._client.events) == 1
            assert [call.kwargs["thread_ts"] for call in slack.send_message.call_args_list] == [
                first["data"]["thread_ts"], second["data"]["thread_ts"],
            ]
        finally:
            await receiver.close()
    asyncio.run(scenario())


def test_signed_http_retry_admits_one_durable_turn(tmp_path, monkeypatch):
    from aiohttp import ClientSession, web
    import time
    from inkbox import verify_webhook
    from inkbox_codex.webhook_providers import inkbox as verifier
    from tests.test_webhook_providers import _sign

    monkeypatch.setattr(verifier, "verify_webhook", verify_webhook)

    async def scenario():
        initial = fixture()
        gw, _, slack = gateway(monkeypatch, tmp_path, initial)
        gw.cfg.signing_key = "whsec_test"
        app = web.Application()
        app.router.add_post("/webhook", gw._handle_webhook)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            async with ClientSession() as http:
                body = json.dumps(initial).encode()
                for request_id in ("first-delivery", "transport-retry"):
                    headers = _sign(body, "whsec_test", request_id=request_id, timestamp=str(int(time.time())))
                    async with http.post(f"http://127.0.0.1:{port}/webhook", data=body, headers=headers) as response:
                        assert response.status == 200, await response.text()
            await drained(gw._companion())
            assert slack.send_message.call_count == 1
            assert len(session_for(gw, initial)._client.events) == 1
        finally:
            await runner.cleanup()
            if gw._companion_receiver is not None:
                await gw._companion_receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("disabled", [True, False])
def test_disabled_or_other_identity_webhooks_do_not_admit_context(tmp_path, monkeypatch, disabled):
    from tests.test_gateway_dedup import _FakeRequest

    async def scenario():
        initial = fixture()
        gw, sdk, slack = gateway(monkeypatch, tmp_path, initial)
        if disabled:
            gw.cfg.slack_enabled = False
        else:
            initial["data"]["identity_id"] = "40000000-0000-4000-8000-000000000099"
        monkeypatch.setattr("inkbox_codex.gateway.match_provider", lambda _: NS(name="inkbox", verify=lambda **_: True))
        try:
            await gw._handle_webhook(_FakeRequest(json.dumps(initial).encode()))
            if gw._companion_receiver is not None:
                await drained(gw._companion_receiver)
            assert not gw.sessions.sessions
            assert sdk.loads == 0
            slack.send_message.assert_not_called()
        finally:
            if gw._companion_receiver is not None:
                await gw._companion_receiver.close()
    asyncio.run(scenario())


def test_disabled_restart_keeps_pending_slack_receipt_without_running_it(tmp_path, monkeypatch):
    async def scenario():
        initial = fixture()
        gw, _, _ = gateway(monkeypatch, tmp_path, initial)
        first = gw._companion()
        first.inbox.accept(Event.parse(prepared(initial)))
        await first.close()
        restarted, sdk, slack = gateway(monkeypatch, tmp_path, initial)
        restarted.cfg.slack_enabled = False
        receiver = restarted._companion()
        try:
            receiver.recover()
            await drained(receiver)
            assert sdk.loads == 0
            assert not restarted.sessions.sessions
            slack.send_message.assert_not_called()
            assert receiver.inbox.db.execute("SELECT state FROM events").fetchone()[0] == "pending"
            restarted.cfg.slack_enabled = True
            receiver.recover()
            await drained(receiver)
            assert slack.send_message.call_count == 1
            assert receiver.inbox.db.execute("SELECT state FROM events").fetchone()[0] == "done"
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("actor,access,same_activation,accepts", [
    ("UALICE", "direct", True, True),
    ("UBOB", "direct", True, False),
    ("UALICE", "sponsored", True, False),
    ("UALICE", "direct", False, False),
])
def test_native_mention_approval_requires_prompted_actor_and_activation(tmp_path, monkeypatch, actor, access, same_activation, accepts):
    from inkbox_codex.escalation import PendingInteraction
    from inkbox_codex.sessions import _Turn

    async def scenario():
        initial = fixture()
        gw, _, _ = gateway(monkeypatch, tmp_path, initial)
        receiver = gw._companion()
        try:
            await receiver.accept(initial)
            await drained(receiver)
            session = session_for(gw, initial)
            owner = Event.parse(prepared(live(initial)))
            session._current_turn = _Turn(text="work", reply_mode="slack", reply_meta=receiver.meta(owner))
            future = asyncio.get_running_loop().create_future()
            session.pending = PendingInteraction(kind="permission", future=future, prompt_text="Allow?")
            answer = live(initial, sequence=3, actor=actor, access=access, text="<@UBOT> yes")
            if not same_activation:
                answer["companion"]["activation_id"] = "40000000-0000-4000-8000-000000000029"
            assert session.companion_answer(answer["data"]["event"]["text"], receiver.meta(Event.parse(prepared(answer)))) is accepts
            assert future.done() is accepts
            if accepts:
                assert future.result() == "yes"
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("command", ["yes", "/stop", "/reset"])
def test_same_actor_in_another_thread_cannot_control_active_work(tmp_path, monkeypatch, command):
    from inkbox_codex.escalation import PendingInteraction
    from inkbox_codex.sessions import _Turn

    async def scenario():
        initial = fixture()
        gw, _, _ = gateway(monkeypatch, tmp_path, initial)
        receiver = gw._companion()
        try:
            session = session_for(gw, initial)
            session._current_turn = _Turn(text="work", reply_mode="slack", reply_meta=receiver.meta(Event.parse(prepared(initial))))
            future = asyncio.get_running_loop().create_future()
            session.pending = PendingInteraction(kind="permission", future=future, prompt_text="Allow?")
            answer = live(initial, text=f"<@UBOT> {command}")
            answer["data"]["thread_ts"] = "1767268801.000100"
            meta = receiver.meta(Event.parse(prepared(answer)))
            assert not await session.companion_interaction(answer["data"]["event"]["text"], meta)
            assert not future.done()
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("change,stops", [(None, True), ("actor", False), ("thread", False), ("workspace", False)])
def test_native_stop_targets_active_companion_actor_and_exact_route(tmp_path, monkeypatch, change, stops):
    async def scenario():
        initial = fixture()
        gw, _, slack = gateway(monkeypatch, tmp_path, initial)
        gw.cfg.allowed_users = ["THOME:UALICE"]
        receiver = gw._companion()
        started, finish = asyncio.Event(), asyncio.Event()

        class WaitingClient(Client):
            async def run(self, text):
                self.events.append(("run", text))
                started.set()
                await finish.wait()
                return "Answer"

            async def interrupt(self):
                self.interrupts += 1
                finish.set()

        session = session_for(gw, initial)
        client = session._client = WaitingClient()
        try:
            await receiver.accept(initial)
            await asyncio.wait_for(started.wait(), 3)
            stop = deepcopy(initial)
            stop.pop("companion")
            stop["id"] = "evt_native_stop"
            stop["event_type"] = "slack.session_stopped"
            stop["data"]["event"] = {"type": "agent_session_stopped"}
            stop["data"]["sender_access"] = None  # Native controls are not policy-labelled content.
            if change == "actor":
                stop["data"]["actor_id"] = "UBOB"
            elif change == "thread":
                stop["data"]["thread_ts"] = "1767268801.000100"
            elif change == "workspace":
                stop["data"]["workspace_id"] = "TOTHER"
            await gw._on_slack_received(stop)
            assert client.interrupts == int(stops)
            finish.set()
            await drained(receiver)
            if stops:
                slack.send_message.assert_not_called()
        finally:
            finish.set()
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("access,failure", [("sponsored", False), ("direct", False), ("direct", True)])
def test_activity_starts_only_for_work_and_finishes_after_delivery(tmp_path, monkeypatch, access, failure):
    async def scenario():
        initial = fixture()
        initial["data"]["sender_access"] = access
        gw, _, slack = gateway(monkeypatch, tmp_path, initial)
        observed = []

        async def activity(_chat, _mode, _meta, state):
            observed.append(state)

        def send(*args, **kwargs):
            observed.append("send")
            if failure:
                raise TimeoutError("send outcome unavailable")
            return NS(id="action-test", status="sent")

        gw.sessions.turn_activity_fn = activity
        slack.send_message.side_effect = send
        receiver = gw._companion()
        try:
            await receiver.accept(initial)
            await drained(receiver)
            assert observed == ([] if access == "sponsored" else ["accepted", "send", "failed" if failure else "completed"])
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("thread", [None, "1767268800.000100"])
@pytest.mark.parametrize("restart_activity", [False, True])
@pytest.mark.parametrize("stage", ["before-model", "before-send"])
def test_retryable_preflight_finishes_actual_slack_activity(tmp_path, monkeypatch, thread, restart_activity, stage):
    from inkbox_codex.slack_activity import SlackActivity

    async def scenario():
        initial = fixture()
        initial["data"]["thread_ts"] = thread
        gw, sdk, slack = gateway(monkeypatch, tmp_path, initial)
        for method in ("set_processing_status", "add_reaction", "remove_reaction"):
            getattr(slack, method).return_value = NS(status="succeeded")
        tracker = SlackActivity(slack, tmp_path / "activity.json")
        gw.sessions.turn_activity_fn = tracker.notify
        authorize = sdk.activation_messages
        calls = 0

        def transient_preflight(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == (2 if stage == "before-model" else 3):
                raise ConnectionError("temporary authorization lookup failure")
            return authorize(*args, **kwargs)

        sdk.activation_messages = transient_preflight
        receiver = gw._companion()
        try:
            await receiver.accept(initial)
            await drained(receiver)
            await tracker.flush()
            assert receiver.inbox.db.execute("SELECT state FROM events").fetchone()[0] == (
                "pending" if stage == "before-model" else "reply_pending")
            slack.send_message.assert_not_called()
            if thread:
                assert [call.args[3] for call in slack.set_processing_status.call_args_list] == ["processing"]
                slack.add_reaction.assert_not_called()
            else:
                assert [call.args[3] for call in slack.add_reaction.call_args_list] == ["eyes"]
                assert not any(call.args[3] == "eyes" for call in slack.remove_reaction.call_args_list)
            if restart_activity:
                await tracker.close()
                tracker = SlackActivity(slack, tmp_path / "activity.json")
                await tracker.recover()
                await tracker.flush()
                session_for(gw, initial).turn_activity_fn = tracker.notify
            receiver.schedule(initial["companion"]["scope_id"])
            await drained(receiver)
            await tracker.flush()
            assert receiver.inbox.db.execute("SELECT state FROM events").fetchone()[0] == "done"
            assert slack.send_message.call_count == 1
            assert len(session_for(gw, initial)._client.events) == 1
            assert not tracker._active
            assert json.loads(tracker.state_path.read_text()) == {}
            if thread:
                assert slack.set_processing_status.call_args.args[3] == "active"
                slack.add_reaction.assert_not_called()
            else:
                assert slack.remove_reaction.call_args.args[3] == "eyes"
                assert all(call.args[3] != "x" for call in slack.add_reaction.call_args_list)
        finally:
            await receiver.close()
            await tracker.close()
    asyncio.run(scenario())


def test_graceful_receiver_shutdown_clears_inline_activity_without_failure(tmp_path, monkeypatch):
    from inkbox_codex.slack_activity import SlackActivity

    async def scenario():
        initial = fixture()
        initial["data"]["thread_ts"] = None
        gw, _, slack = gateway(monkeypatch, tmp_path, initial)
        slack.add_reaction.return_value = slack.remove_reaction.return_value = NS(status="succeeded")
        tracker = SlackActivity(slack, tmp_path / "activity.json")
        gw.sessions.turn_activity_fn = tracker.notify
        receiver = gw._companion()
        started = asyncio.Event()

        class BlockingClient(Client):
            async def run(self, text):
                started.set()
                await asyncio.Event().wait()

        session_for(gw, initial)._client = BlockingClient()
        try:
            await receiver.accept(initial)
            await asyncio.wait_for(started.wait(), 3)
            await tracker.flush()
            assert slack.add_reaction.call_args.args[3] == "eyes"
        finally:
            await receiver.close()
            await tracker.flush()
            await tracker.close()
        assert [call.args[3] for call in slack.add_reaction.call_args_list] == ["eyes"]
        assert slack.remove_reaction.call_args.args[3] == "eyes"
        assert json.loads(tracker.state_path.read_text()) == {}
    asyncio.run(scenario())


def test_restart_after_late_initialization_snapshot_only_submits_current_source(tmp_path, monkeypatch):
    async def scenario():
        initial = fixture()
        current = live(initial, actor="UBOB", access="sponsored")
        current["companion"]["phase"] = "initialization"
        gw, _, _ = gateway(monkeypatch, tmp_path, initial)
        first = gw._companion()
        submit = first.submit

        async def interrupted(event, text, meta, **kwargs):
            await submit(event, text, meta, **kwargs)
            if kwargs.get("trigger"):
                raise ConnectionError("restart after snapshot checkpoint")

        monkeypatch.setattr(first, "submit", interrupted)
        await first.accept(current)
        await drained(first)
        assert first.inbox.db.execute("SELECT state FROM events").fetchone()[0] == "pending"
        assert len(session_for(gw, current)._client.events) == 1
        await first.close()
        restarted, sdk, slack = gateway(monkeypatch, tmp_path, initial)
        receiver = restarted._companion()
        try:
            receiver.recover()
            await drained(receiver)
            assert sdk.loads == 0
            events = session_for(restarted, current)._client.events
            assert len(events) == 1 and events[0][0] == "context"
            assert current["data"]["event"]["text"] in events[0][1]
            assert initial["companion"]["history"][-1]["text"] not in events[0][1]
            assert receiver.inbox.db.execute("SELECT state FROM events").fetchone()[0] == "done"
            slack.send_message.assert_not_called()
        finally:
            await receiver.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("stage", ["before-model", "before-send"])
@pytest.mark.parametrize("change", ["revoked", "replaced"])
def test_activation_is_revalidated_before_model_and_saved_reply_send(tmp_path, monkeypatch, stage, change):
    async def scenario():
        initial = fixture()
        gw, sdk, slack = gateway(monkeypatch, tmp_path, initial)
        receiver = gw._companion()

        def revoke():
            if change == "revoked":
                sdk.denied = True
            else:
                sdk.c["activation_id"] = "40000000-0000-4000-8000-000000000098"

        try:
            await receiver.accept(initial)
            await drained(receiver)
            session = session_for(gw, initial)
            session._client.events.clear()
            slack.send_message.reset_mock()
            if stage == "before-model":
                revoke()
            else:
                class RevokingClient(Client):
                    async def run(self, text):
                        reply = await super().run(text)
                        revoke()
                        return reply
                session._client = RevokingClient()
            current = live(initial)
            await receiver.accept(current)
            await drained(receiver)
            slack.send_message.assert_not_called()
            assert len(session._client.events) == (0 if stage == "before-model" else 1)
            state = receiver.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (current["id"],)).fetchone()[0]
            assert state == ("pending" if stage == "before-model" else "reply_pending")
        finally:
            await receiver.close()
    asyncio.run(scenario())
