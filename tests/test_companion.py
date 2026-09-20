"""Companion conformance fixtures, version 1."""

import asyncio
import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from inkbox_codex import gateway, sessions
from inkbox_codex.codex_client import CodexAppServerClient
from inkbox_codex.companion import CompanionInbox, metadata
from inkbox_codex.config import BridgeConfig


def uid(number):
    return str(UUID(int=number))


def event(channel="mail", phase="initialization", source=3, sequence=1, scope=10, activation=20):
    routing = {
        "scope_id": uid(scope),
        "conversation_id": uid(30),
        "channel": channel,
        "phase": phase,
        "sequence": sequence,
    }
    if phase != "ordinary":
        routing["activation_id"] = uid(activation)
    sender = "owner@example.com" if channel == "mail" else "+15555550100"
    message = {
        "id": uid(source),
        "direction": "inbound",
        "body_text": "A live message",
        "text": "A live message",
        "content": "A live message",
    }
    if channel == "mail":
        message.update(
            from_address=sender,
            thread_id=uid(30),
            to_addresses=["agent@example.com", "guest@example.com"],
            cc_addresses=[sender],
        )
    else:
        message.update(
            conversation_id=uid(30),
            sender_phone_number=sender if channel == "phone" else None,
            sender_number=sender if channel == "imessage" else None,
            remote_number=None,
        )
    event_type = {
        "mail": "message.received",
        "phone": "text.received",
        "imessage": "imessage.received",
    }[channel]
    return {
        "event_type": event_type,
        "companion": routing,
        "data": {
            "text_message" if channel == "phone" else "message": message,
            "contacts": [{"id": uid(99), "memories": ["PRIVATE MEMORY"]}],
        },
    }


def snapshot(envelope):
    routing = envelope["companion"]
    channel = routing["channel"]
    sponsor = "owner@example.com" if channel == "mail" else "+15555550100"
    entries = [
        {
            "id": uid(1),
            "author": "guest@example.com",
            "occurred_at": "2026-01-01T00:00:00Z",
            "text": "/clear",
            "historical": True,
            "is_trigger": False,
            "attachments": [],
        },
        {
            "id": uid(2),
            "author": "another@example.com",
            "occurred_at": "2026-01-01T00:01:00Z",
            "text": "YES",
            "historical": True,
            "is_trigger": False,
            "attachments": [{"id": uid(90), "filename": "agenda.txt"}],
        },
        {
            "id": uid(3),
            "author": sponsor,
            "occurred_at": "2026-01-01T00:02:00Z",
            "text": "Please join",
            "historical": False,
            "is_trigger": True,
            "attachments": [],
        },
    ]
    reply = {"channel": channel, "conversation_id": routing["conversation_id"]}
    if channel == "mail":
        reply.update(reply_to_message_id=uid(3), to=[sponsor], cc=["guest@example.com"])
    return {
        **{k: routing[k] for k in ("scope_id", "activation_id", "conversation_id", "channel")},
        "entries": entries,
        "text": json.dumps(entries),
        "reply_context": reply,
        "notices": [
            {"code": "available_history", "message": "Only retained history is available."}
        ],
    }


class Request:
    def __init__(self, envelope, request_id="request-1"):
        self.body = json.dumps(envelope).encode()
        self.headers = {"X-Inkbox-Signature": "test", "X-Inkbox-Request-Id": request_id}
        self.url = "https://agent.example/webhook"

    async def read(self):
        return self.body


@pytest.fixture
def harness(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.setattr(
        gateway,
        "match_provider",
        lambda _headers: SimpleNamespace(name="inkbox", verify=lambda **_: True),
    )
    inputs, replies, clients, checkpoints = [], [], [], []
    controls = SimpleNamespace(release=None, fail=False, connect_hook=None)
    gw = gateway.InkboxGateway(
        BridgeConfig(
            identity="agent",
            project_dir=str(tmp_path),
            allowed_users=["owner@example.com", "+15555550100"],
            codex_turn_timeout_s=2,
            permission_timeout_s=1,
        )
    )
    gw._identity = SimpleNamespace(id=uid(80))
    holder = SimpleNamespace(value=snapshot(event()), calls=0, fail=None)

    def load(_handle, _activation, *, max_bytes):
        holder.calls += 1
        assert max_bytes == gw.cfg.companion_max_bytes
        if holder.fail:
            raise holder.fail
        return copy.deepcopy(holder.value)

    gw._inkbox = SimpleNamespace(
        whoami=lambda: SimpleNamespace(auth_subtype="api_key.agent_scoped.claimed"),
        companion=SimpleNamespace(load_initialization=load),
    )

    class Client(CodexAppServerClient):
        async def connect(self, resume_thread_id=None):
            if controls.connect_hook:
                controls.connect_hook()
            self.thread_id = resume_thread_id or f"thread-{len(clients)}"
            clients.append(self)
            return self.thread_id

        async def _request(self, method, params):
            assert method == "turn/start"
            inputs.append(params["input"])
            checkpoints.append(
                [
                    dict(row)
                    for row in gw._companion.db.execute(
                        "SELECT * FROM jobs WHERE status='submitting'"
                    )
                ]
            )
            if controls.fail:
                raise TimeoutError("Host acknowledgement lost")
            turn_id = f"turn-{len(inputs)}"

            async def complete():
                await asyncio.sleep(0)
                if controls.release is not None:
                    await controls.release.wait()
                self._handle_notification(
                    {
                        "method": "item/completed",
                        "params": {
                            "turnId": turn_id,
                            "item": {"type": "agentMessage", "phase": "final", "text": "Reply"},
                        },
                    }
                )
                self._handle_notification(
                    {
                        "method": "turn/completed",
                        "params": {
                            "turn": {"id": turn_id, "status": "completed"},
                        },
                    }
                )

            asyncio.create_task(complete())
            return {"turn": {"id": turn_id}}

    monkeypatch.setattr(sessions, "CodexAppServerClient", Client)

    async def send(chat, text, mode, meta):
        replies.append((chat, text, mode, copy.deepcopy(meta)))

    gw.send_to_contact = send
    gw._fetch_mail_body = lambda message: message.get("body_text", "")
    gw._resolve_contact_full = AsyncMock(side_effect=AssertionError("Contact routing must not run"))
    gw.sessions = sessions.SessionManager(gw.cfg, send, {}, {"handle": "agent"})
    return SimpleNamespace(
        gw=gw,
        inputs=inputs,
        replies=replies,
        controls=controls,
        snapshot=holder,
        clients=clients,
        checkpoints=checkpoints,
    )


async def settled(gw):
    await asyncio.wait_for(asyncio.gather(*gw._companion.tasks.values()), 3)


async def until(predicate):
    async def wait():
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(wait(), 2)


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_one_real_app_server_submission_with_history_and_duplicate_recovery(harness, channel):
    async def run():
        h = harness
        envelope = event(channel)
        h.snapshot.value = snapshot(envelope)
        assert (await h.gw._handle_webhook(Request(envelope))).status == 200
        assert h.gw._companion.db.execute("SELECT status FROM jobs").fetchone()[0] == "pending"
        await settled(h.gw)
        await h.gw._handle_webhook(Request(envelope, "retry"))
        await settled(h.gw)
        assert len(h.inputs) == 1
        assert len(h.inputs[0]) == 1
        text = h.inputs[0][0]["text"]
        for value in ("/clear", "YES", "Please join", "agenda.txt", "available_history"):
            assert value in text
        assert "PRIVATE MEMORY" not in text
        assert h.checkpoints[0][0]["thread_id"] == "thread-0"
        assert h.replies[0][3]["conversation_id"] == uid(30)
        assert h.replies[0][0].startswith("companion:")
        await h.gw._companion.close()
        h.gw._companion = CompanionInbox(h.gw)
        h.gw._companion.recover()
        await h.gw._handle_webhook(Request(envelope, "restart-retry"))
        await settled(h.gw)
        assert len(h.inputs) == 1
        row = dict(h.gw._companion.db.execute("SELECT * FROM jobs").fetchone())
        assert row["status"] == "completed" and row["turn_id"] == "turn-1"
        await h.gw._companion.close()

    asyncio.run(run())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_live_waits_for_initialization_and_keeps_immutable_reply(harness, channel):
    async def run():
        h = harness
        envelope = event(channel)
        h.snapshot.value = snapshot(envelope)
        h.controls.release = asyncio.Event()
        await h.gw._handle_webhook(Request(envelope))
        await until(lambda: len(h.inputs) == 1)
        live = event(channel, "live", 4, 2)
        reply = copy.deepcopy(h.snapshot.value["reply_context"])
        if channel == "mail":
            reply["reply_to_message_id"] = uid(4)
        live["companion"]["reply_context"] = reply
        await h.gw._handle_webhook(Request(live, "live"))
        await asyncio.sleep(0.02)
        assert len(h.inputs) == 1
        assert (
            h.gw._companion.db.execute(
                "SELECT count(*) FROM jobs WHERE status='pending'"
            ).fetchone()[0]
            == 1
        )
        h.controls.release.set()
        await settled(h.gw)
        assert len(h.inputs) == 2 and len(h.replies) == 2
        assert h.replies[0][3]["reply_context"] == h.snapshot.value["reply_context"]
        assert h.replies[1][3]["reply_context"] == reply
        await h.gw._companion.close()

    asyncio.run(run())


def test_hydration_retry_recovers_durable_initializer_and_live(harness):
    async def run():
        h = harness
        h.snapshot.fail = OSError("Temporary network failure")
        await h.gw._handle_webhook(Request(event()))
        await h.gw._handle_webhook(Request(event(phase="live", source=4, sequence=2), "live"))
        await until(lambda: h.snapshot.calls > 0)
        await h.gw._companion.close()
        assert h.inputs == []
        h.snapshot.fail = None
        h.gw._companion = CompanionInbox(h.gw)
        h.gw._companion.recover()
        await settled(h.gw)
        assert len(h.inputs) == 2
        await h.gw._companion.close()

    asyncio.run(run())


def test_uncertain_host_acceptance_pauses_without_resubmitting(harness):
    async def run():
        h = harness
        h.controls.fail = True
        await h.gw._handle_webhook(Request(event()))
        await settled(h.gw)
        assert len(h.inputs) == 1 and h.replies == []
        assert h.gw._companion.db.execute("SELECT status FROM jobs").fetchone()[0] == "paused"
        await h.gw._companion.close()
        h.controls.fail = False
        h.gw._companion = CompanionInbox(h.gw)
        await h.gw._handle_webhook(Request(event(), "retry"))
        await h.gw._handle_webhook(Request(event(phase="live", source=4, sequence=2), "live"))
        await settled(h.gw)
        assert len(h.inputs) == 1
        await h.gw._companion.close()

    asyncio.run(run())


def test_crash_after_host_acceptance_pauses_live_on_restart(harness):
    async def run():
        h = harness
        h.controls.release = asyncio.Event()
        await h.gw._handle_webhook(Request(event()))
        await until(lambda: h.inputs)
        await until(
            lambda: (
                h.gw._companion.db.execute("SELECT status FROM jobs").fetchone()[0] == "submitted"
            )
        )
        await h.gw._companion.close()
        h.gw._companion = CompanionInbox(h.gw)
        h.gw._companion.recover()
        await settled(h.gw)
        assert len(h.inputs) == 1
        assert (
            h.gw._companion.db.execute("SELECT error FROM jobs").fetchone()[0]
            == "uncertain-submission"
        )
        h.controls.release.set()
        await h.gw._companion.close()

    asyncio.run(run())


@pytest.mark.parametrize("failure", ["size", "revoked", "sponsor", "trigger", "scope", "principal"])
def test_invalid_initialization_never_submits_partial_context(harness, failure):
    async def run():
        h = harness
        if failure == "size":
            h.gw.cfg.companion_max_bytes = 10
        elif failure == "revoked":
            h.snapshot.fail = PermissionError("Revoked")
        elif failure == "sponsor":
            h.gw.cfg.allowed_users = ["different@example.com"]
        elif failure == "trigger":
            h.snapshot.value["entries"].pop()
        elif failure == "scope":
            h.snapshot.value["scope_id"] = uid(11)
        else:
            h.gw._inkbox.whoami = lambda: {"auth_subtype": "api_key.admin_scoped"}
        await h.gw._handle_webhook(Request(event()))
        await settled(h.gw)
        assert h.inputs == []
        assert h.gw._companion.db.execute("SELECT status FROM jobs").fetchone()[0] == "paused"
        await h.gw._companion.close()

    asyncio.run(run())


def test_local_revocation_at_session_boundary_prevents_submission(harness):
    async def run():
        h = harness
        h.controls.connect_hook = lambda: setattr(
            h.gw.cfg, "allowed_users", ["different@example.com"]
        )
        await h.gw._handle_webhook(Request(event()))
        await settled(h.gw)
        assert h.inputs == []
        await h.gw._companion.close()

    asyncio.run(run())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_ordinary_scope_has_no_activation_or_history_and_is_isolated(harness, channel):
    async def run():
        h = harness
        ordinary = event(channel, phase="ordinary", source=5)
        h.snapshot.value = snapshot(event(channel))
        await h.gw._handle_webhook(Request(ordinary))
        await settled(h.gw)
        assert h.snapshot.calls == 0
        first = h.replies[0][0]
        await h.gw._handle_webhook(Request(event(channel), "init"))
        await settled(h.gw)
        assert first != h.replies[1][0]
        assert "/clear" not in h.inputs[0][0]["text"]
        await h.gw._companion.close()

    asyncio.run(run())


def test_new_scope_and_activation_have_separate_host_threads(harness):
    async def run():
        h = harness
        for index, (scope, activation) in enumerate(((10, 20), (10, 21), (11, 22))):
            envelope = event(scope=scope, activation=activation)
            h.snapshot.value = snapshot(envelope)
            await h.gw._handle_webhook(Request(envelope, f"request-{index}"))
            await settled(h.gw)
        assert len(h.inputs) == 3
        assert len({reply[0] for reply in h.replies}) == 3
        assert len(h.clients) == 3
        await h.gw._companion.close()

    asyncio.run(run())


def test_live_first_hydrates_before_its_own_turn(harness):
    async def run():
        h = harness
        await h.gw._handle_webhook(Request(event(phase="live", source=4, sequence=2)))
        await settled(h.gw)
        await h.gw._handle_webhook(Request(event(), "late-init"))
        await settled(h.gw)
        assert len(h.inputs) == 2
        assert "Please join" in h.inputs[0][0]["text"]
        assert "A live message" in h.inputs[1][0]["text"]
        await h.gw._companion.close()

    asyncio.run(run())


def test_only_live_sponsor_can_answer_pending_approval(harness):
    async def run():
        h = harness
        h.controls.release = asyncio.Event()
        await h.gw._handle_webhook(Request(event()))
        await until(lambda: h.inputs)
        session = next(iter(h.gw.sessions.sessions.values()))
        pending = asyncio.create_task(session._escalate("permission", "Approve this action?"))
        await until(lambda: session.pending is not None)
        bystander = event(phase="live", source=4, sequence=2)
        bystander["data"]["message"].update(from_address="guest@example.com", body_text="YES")
        await h.gw._handle_webhook(Request(bystander, "guest"))
        assert not session.pending.future.done()
        sponsor = event(phase="live", source=5, sequence=3)
        sponsor["data"]["message"]["body_text"] = "YES"
        await h.gw._handle_webhook(Request(sponsor, "sponsor"))
        assert await pending == "YES"
        h.controls.release.set()
        await settled(h.gw)
        assert len(h.inputs) == 2
        await h.gw._companion.close()

    asyncio.run(run())


def test_unverified_companion_cannot_use_signature_opt_out(harness, monkeypatch):
    async def run():
        h = harness
        h.gw.cfg.require_signature = False
        monkeypatch.setattr(
            gateway,
            "match_provider",
            lambda _headers: SimpleNamespace(name="inkbox", verify=lambda **_: False),
        )
        response = await h.gw._handle_webhook(Request(event()))
        assert response.status == 401
        assert h.gw._companion is None and h.inputs == []

    asyncio.run(run())


def test_ordinary_metadata_cannot_include_history():
    envelope = event(phase="ordinary")
    envelope["companion"]["history"] = []
    with pytest.raises(ValueError, match="activation context"):
        metadata(envelope)


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_actual_gateway_delivery_uses_canonical_group_binding(harness, channel):
    async def run():
        h = harness
        from inkbox_codex.companion import reply_meta

        envelope = event(channel)
        context = snapshot(envelope)["reply_context"]
        calls = []
        identity = SimpleNamespace(
            reply_all_email=lambda *args, **kwargs: calls.append(("mail", args, kwargs)),
            send_text=lambda **kwargs: calls.append(("phone", (), kwargs)),
            send_imessage=lambda **kwargs: calls.append(("imessage", (), kwargs)),
        )
        h.gw._inkbox.get_identity = lambda _: identity
        meta = reply_meta(context, envelope["companion"])
        await gateway.InkboxGateway.send_to_contact(
            h.gw, "companion:test", "Reply", meta["mode"], meta
        )
        assert len(calls) == 1
        if channel == "mail":
            assert calls[0][1] == (uid(3),)
            assert "to" not in calls[0][2]
        else:
            assert calls[0][2]["conversation_id"] == uid(30)
            assert "to" not in calls[0][2]

    asyncio.run(run())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_sdk_pagination_and_revalidation_submit_one_host_input(harness, channel):
    sdk = pytest.importorskip("inkbox.companion")

    async def run():
        h = harness
        envelope = event(channel)
        fixture = snapshot(envelope)
        cursors = []

        class Transport:
            def get(self, path, *, params):
                assert path.endswith(f"/{uid(20)}/messages")
                cursor = params.get("cursor")
                cursors.append(cursor)
                return {
                    **{
                        key: fixture[key]
                        for key in (
                            "scope_id",
                            "activation_id",
                            "conversation_id",
                            "channel",
                            "reply_context",
                        )
                    },
                    "items": fixture["entries"][:2] if cursor is None else fixture["entries"][1:],
                    "history_complete": cursor is not None,
                    "next_cursor": "page-two" if cursor is None else None,
                    "notices": [
                        {
                            "code": "available_history",
                            "level": "info",
                            "message": "Retained history only.",
                        }
                    ],
                }

        h.gw._inkbox.companion = sdk.CompanionResource(Transport())
        await h.gw._handle_webhook(Request(envelope))
        await settled(h.gw)
        assert len(h.inputs) == 1
        assert "page-two" in cursors
        text = h.inputs[0][0]["text"]
        assert text.count('"is_trigger":true') == 1
        assert text.count('"id":"' + uid(2) + '"') == 1
        assert text.index("/clear") < text.index("YES") < text.index("Please join")
        assert "agenda.txt" in text and "available_history" in text
        await h.gw._companion.close()

    asyncio.run(run())


def test_history_revocation_during_host_connect_discards_ready_input(harness):
    async def run():
        h = harness

        def revoke_entry():
            h.snapshot.value["entries"].pop(0)
            h.snapshot.value["text"] = json.dumps(h.snapshot.value["entries"])

        h.controls.connect_hook = revoke_entry
        await h.gw._handle_webhook(Request(event()))
        await settled(h.gw)
        assert h.inputs == []
        row = h.gw._companion.db.execute("SELECT status,context FROM jobs").fetchone()
        assert tuple(row) == ("paused", None)
        await h.gw._companion.close()

    asyncio.run(run())


def test_restart_uses_durable_thread_when_session_map_is_missing(harness):
    async def run():
        h = harness
        await h.gw._handle_webhook(Request(event()))
        await settled(h.gw)
        thread_id = h.clients[0].thread_id
        await h.gw._companion.close()
        h.gw.sessions.sessions.clear()
        h.gw.sessions._session_ids.clear()
        h.gw._companion = CompanionInbox(h.gw)
        await h.gw._handle_webhook(Request(event(phase="live", source=4, sequence=2), "live"))
        await settled(h.gw)
        assert h.clients[-1].thread_id == thread_id
        assert len(h.inputs) == 2
        await h.gw._companion.close()

    asyncio.run(run())


def test_recovery_requires_review_for_uncertain_outcome(harness):
    from inkbox_codex.companion import inspect_jobs, resolve_job

    async def run():
        h = harness
        h.controls.fail = True
        await h.gw._handle_webhook(Request(event()))
        await settled(h.gw)
        jobs = inspect_jobs()
        assert len(jobs) == 1 and "payload" not in jobs[0]
        with pytest.raises(ValueError, match="Stop the gateway"):
            resolve_job(jobs[0]["id"], "not-submitted")
        await h.gw._companion.close()
        with pytest.raises(ValueError, match="Review the Codex thread"):
            resolve_job(jobs[0]["id"], "retry")
        resolve_job(jobs[0]["id"], "not-submitted")
        h.controls.fail = False
        h.gw._companion = CompanionInbox(h.gw)
        h.gw._companion.recover()
        await settled(h.gw)
        assert len(h.inputs) == 2
        assert inspect_jobs()[0]["status"] == "completed"
        await h.gw._companion.close()

    asyncio.run(run())


def test_pre_submission_failure_can_be_retried_after_limit_change(harness):
    from inkbox_codex.companion import inspect_jobs, resolve_job

    async def run():
        h = harness
        h.gw.cfg.companion_max_bytes = 10
        await h.gw._handle_webhook(Request(event()))
        await settled(h.gw)
        await h.gw._companion.close()
        resolve_job(inspect_jobs()[0]["id"], "retry")
        h.gw.cfg.companion_max_bytes = 262144
        h.gw._companion = CompanionInbox(h.gw)
        h.gw._companion.recover()
        await settled(h.gw)
        assert len(h.inputs) == 1
        await h.gw._companion.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "channel,failure",
    [("mail", None), ("phone", None), ("imessage", None)]
    + [("mail", failure) for failure in ("bystander", "revoked", "scope", "local")],
)
def test_reviewed_completed_initializer_restores_sponsor_approvals(harness, channel, failure):
    from inkbox_codex.companion import resolve_job

    async def run():
        h = harness
        h.snapshot.value = snapshot(event(channel))
        h.controls.fail = True
        await h.gw._handle_webhook(Request(event(channel)))
        await settled(h.gw)
        initial = dict(h.gw._companion.db.execute("SELECT * FROM jobs").fetchone())
        assert initial["status"] == "paused" and initial["context"] is None
        await h.gw._companion.close()
        resolve_job(initial["id"], "completed")
        h.gw.sessions.sessions.clear()
        h.gw.sessions._session_ids.clear()
        h.gw._companion = CompanionInbox(h.gw)
        h.controls.fail = False
        h.controls.release = asyncio.Event()
        await h.gw._handle_webhook(Request(event(channel, "live", 4, 2), "live"))
        await until(lambda: len(h.inputs) == 2)
        session = next(iter(h.gw.sessions.sessions.values()))
        assert session._client.thread_id == initial["thread_id"]
        approval = asyncio.create_task(session._escalate("permission", "Approve this action?"))
        await until(lambda: session.pending is not None)
        calls = h.snapshot.calls
        answer = event(channel, "live", 5, 3)
        message = answer["data"]["text_message" if channel == "phone" else "message"]
        message.update(body_text="YES", text="YES", content="YES")
        if failure == "bystander":
            message["from_address"] = "guest@example.com"
            answer["companion"]["sponsor"] = "guest@example.com"
            answer["companion"]["context"] = {"sponsor": "guest@example.com"}
        elif failure == "revoked":
            h.snapshot.fail = PermissionError("Revoked")
        elif failure == "scope":
            h.snapshot.value["scope_id"] = uid(11)
        elif failure == "local":
            h.gw.cfg.allowed_users = ["different@example.com"]
        response = await h.gw._handle_webhook(Request(answer, "answer"))
        assert response.status == 200
        assert h.snapshot.calls > calls
        if failure:
            assert not session.pending.future.done()
            assert (
                h.gw._companion.db.execute(
                    "SELECT status FROM jobs WHERE kind='live' AND sequence=3"
                ).fetchone()[0]
                == "pending"
            )
            assert len(h.inputs) == 2
            approval.cancel()
            await asyncio.gather(approval, return_exceptions=True)
            await h.gw._companion.close()
            h.controls.release.set()
            return
        assert await approval == "YES"
        h.controls.release.set()
        await settled(h.gw)
        assert len(h.inputs) == 2
        assert h.gw._companion._job(initial["id"])["status"] == "completed"
        await h.gw._companion.close()

    asyncio.run(run())


def test_acknowledged_turn_cannot_be_resubmitted_by_recovery(harness):
    from inkbox_codex.companion import inspect_jobs, resolve_job

    async def run():
        h = harness
        h.controls.release = asyncio.Event()
        await h.gw._handle_webhook(Request(event()))
        await until(lambda: inspect_jobs()[0]["status"] == "submitted")
        initial = inspect_jobs()[0]
        assert initial["turn_id"] == "turn-1"
        await h.gw._companion.close()
        for outcome in ("retry", "not-submitted"):
            with pytest.raises(ValueError, match="Review|acknowledged"):
                resolve_job(initial["id"], outcome)
        h.gw._companion = CompanionInbox(h.gw)
        h.gw._companion.recover()
        await h.gw._handle_webhook(Request(event(), "duplicate"))
        await h.gw._handle_webhook(Request(event(phase="live", source=4, sequence=2), "live"))
        await settled(h.gw)
        assert len(h.inputs) == 1
        assert inspect_jobs()[0]["turn_id"] == initial["turn_id"]
        await h.gw._companion.close()
        for outcome in ("retry", "not-submitted"):
            with pytest.raises(ValueError, match="Review|acknowledged"):
                resolve_job(initial["id"], outcome)
        resolve_job(initial["id"], "completed")
        h.gw._companion = CompanionInbox(h.gw)
        h.controls.release.set()
        h.gw._companion.recover()
        await settled(h.gw)
        assert len(h.inputs) == 2
        assert h.gw._companion._job(initial["id"])["turn_id"] == initial["turn_id"]
        await h.gw._companion.close()

    asyncio.run(run())


def test_cleanup_rejects_request_resuming_after_body_read(harness):
    async def run():
        h = harness
        entered, resume = asyncio.Event(), asyncio.Event()

        class SlowRequest(Request):
            async def read(self):
                entered.set()
                await resume.wait()
                return self.body

        request = asyncio.create_task(h.gw._handle_webhook(SlowRequest(event())))
        await entered.wait()
        await h.gw._cleanup()
        resume.set()
        assert (await request).status == 503
        assert h.gw._companion is None and not CompanionInbox.exists(h.gw)
        assert not h.gw._recent_request_ids and not h.inputs

    asyncio.run(run())


def test_closed_inbox_rejects_work_after_owner_lock_is_released(harness):
    from inkbox_codex.companion import CompanionClosedError

    async def run():
        h = harness
        inbox = CompanionInbox(h.gw)
        await inbox.close()
        owner = CompanionInbox(h.gw)
        try:
            with pytest.raises(CompanionClosedError):
                await inbox.accept(event(), metadata(event()))
            with pytest.raises(CompanionClosedError):
                inbox.recover()
            with pytest.raises(CompanionClosedError):
                inbox._schedule("companion:test")
            with pytest.raises(CompanionClosedError):
                inbox.record_delivery_failure("message.bounced", {})
            with pytest.raises(CompanionClosedError):
                inbox._update(uid(1), status="completed")
            await inbox.close()
            assert owner.db.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0
            assert not inbox.tasks and not h.inputs
        finally:
            await owner.close()

    asyncio.run(run())


def test_concurrent_duplicate_ack_follows_durable_live_acceptance(harness, monkeypatch):
    from inkbox_codex.companion import inspect_jobs

    async def run():
        h = harness
        h.controls.release = asyncio.Event()
        await h.gw._handle_webhook(Request(event()))
        await until(lambda: h.inputs)
        session = next(iter(h.gw.sessions.sessions.values()))
        approval = asyncio.create_task(session._escalate("permission", "Approve this action?"))
        await until(lambda: session.pending is not None)
        entered, resume = asyncio.Event(), asyncio.Event()
        original = asyncio.to_thread

        async def hold(func, *args, **kwargs):
            result = await original(func, *args, **kwargs)
            if func is h.gw._inkbox.companion.load_initialization:
                entered.set()
                await resume.wait()
            return result

        monkeypatch.setattr(asyncio, "to_thread", hold)
        answer = event(phase="live", source=4, sequence=2)
        answer["data"]["message"]["body_text"] = "YES"
        first = asyncio.create_task(h.gw._handle_webhook(Request(answer, "answer")))
        await entered.wait()
        assert [job["status"] for job in inspect_jobs() if job["kind"] == "live"] == ["pending"]
        duplicate = await h.gw._handle_webhook(Request(answer, "answer"))
        assert duplicate.status == 200 and json.loads(duplicate.text)["deduped"]
        assert not first.done()
        resume.set()
        assert (await first).status == 200
        assert await approval == "YES"
        h.controls.release.set()
        await settled(h.gw)
        assert len(h.inputs) == 1
        await h.gw._companion.close()

    asyncio.run(run())


@pytest.mark.parametrize("suspension", ["validation", "mail_body"])
def test_cleanup_rejects_suspended_approval_and_late_requests(harness, monkeypatch, suspension):
    async def run():
        h = harness
        h.controls.release = asyncio.Event()
        await h.gw._handle_webhook(Request(event()))
        await until(lambda: h.inputs)
        inbox = h.gw._companion
        session = next(iter(h.gw.sessions.sessions.values()))
        approval = asyncio.create_task(session._escalate("permission", "Approve this action?"))
        await until(lambda: session.pending is not None)
        interaction = session.pending
        entered, resume = asyncio.Event(), asyncio.Event()
        original = asyncio.to_thread
        target = (
            h.gw._inkbox.companion.load_initialization
            if suspension == "validation"
            else h.gw._fetch_mail_body
        )

        async def hold(func, *args, **kwargs):
            result = await original(func, *args, **kwargs)
            if func is target:
                entered.set()
                await resume.wait()
            return result

        monkeypatch.setattr(asyncio, "to_thread", hold)
        answer = event(phase="live", source=4, sequence=2)
        answer["data"]["message"]["body_text"] = "YES"
        request = asyncio.create_task(h.gw._handle_webhook(Request(answer, "answer")))
        await entered.wait()
        await h.gw._cleanup()
        owner = CompanionInbox(h.gw)
        before = [dict(row) for row in owner.db.execute("SELECT * FROM jobs")]
        resume.set()
        try:
            assert (await request).status == 503
            assert not interaction.future.done()
            assert "answer" not in h.gw._recent_request_ids
            assert all(task.done() for task in inbox.tasks.values())
            assert (await h.gw._handle_webhook(Request(event(), "late"))).status == 503
            assert h.gw._companion is None
            assert [dict(row) for row in owner.db.execute("SELECT * FROM jobs")] == before
            assert len(h.inputs) == 1
        finally:
            approval.cancel()
            await asyncio.gather(approval, return_exceptions=True)
            h.controls.release.set()
            await owner.close()

    asyncio.run(run())


def test_forged_external_event_does_not_enter_companion(harness, monkeypatch):
    async def run():
        h = harness
        monkeypatch.setattr(
            gateway,
            "match_provider",
            lambda _: SimpleNamespace(name="mock", verify=lambda **_: True),
        )
        h.gw._on_external_event = AsyncMock(return_value=gateway.web.json_response({"ok": True}))
        await h.gw._handle_webhook(Request(event()))
        assert h.gw._companion is None and h.inputs == []
        h.gw._on_external_event.assert_awaited_once()

    asyncio.run(run())


def test_persistence_failure_cannot_acknowledge_webhook(harness, monkeypatch):
    async def run():
        h = harness
        h.gw._companion = CompanionInbox(h.gw)
        h.gw._companion.db.execute("PRAGMA query_only=ON")
        import sqlite3

        with pytest.raises(sqlite3.OperationalError):
            await h.gw._handle_webhook(Request(event()))
        assert h.inputs == []
        assert "request-1" not in h.gw._recent_request_ids
        await h.gw._companion.close()

    asyncio.run(run())


def test_startup_does_not_reserve_inbox_before_sessions_exist(harness):
    async def run():
        h = harness
        h.gw.sessions = None
        response = await h.gw._handle_webhook(Request(event()))
        assert response.status == 503
        assert h.gw._companion is None
        assert not CompanionInbox.exists(h.gw)
        assert "request-1" not in h.gw._recent_request_ids

    asyncio.run(run())


def test_session_connection_failure_retries_before_submission(harness):
    async def run():
        h = harness

        def fail_connect():
            raise OSError("Host temporarily unavailable")

        h.controls.connect_hook = fail_connect
        await h.gw._handle_webhook(Request(event()))
        await until(
            lambda: (
                h.gw._companion.db.execute("SELECT error FROM jobs").fetchone()[0]
                == "pre-submit-retry"
            )
        )
        assert h.inputs == []
        await h.gw._companion.close()
        h.controls.connect_hook = None
        h.gw._companion = CompanionInbox(h.gw)
        h.gw._companion.recover()
        await settled(h.gw)
        assert len(h.inputs) == 1
        await h.gw._companion.close()

    asyncio.run(run())


def test_expired_approval_does_not_drop_live_sponsor_message(harness, monkeypatch):
    async def run():
        h = harness
        h.controls.release = asyncio.Event()
        await h.gw._handle_webhook(Request(event()))
        await until(lambda: h.inputs)
        session = next(iter(h.gw.sessions.sessions.values()))
        approval = asyncio.create_task(session._escalate("permission", "Approve this action?"))
        await until(lambda: session.pending is not None)
        original = asyncio.to_thread

        async def expire_while_loading(func, *args, **kwargs):
            if func is h.gw._fetch_mail_body and session.pending is not None:
                session.pending.future.set_result(None)
                session.pending = None
            return await original(func, *args, **kwargs)

        monkeypatch.setattr(asyncio, "to_thread", expire_while_loading)
        await h.gw._handle_webhook(Request(event(phase="live", source=4, sequence=2), "live"))
        assert await approval is None
        assert (
            h.gw._companion.db.execute(
                "SELECT count(*) FROM jobs WHERE status='pending'"
            ).fetchone()[0]
            == 1
        )
        h.controls.release.set()
        await settled(h.gw)
        assert len(h.inputs) == 2
        await h.gw._companion.close()

    asyncio.run(run())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_group_delivery_failure_never_wakes_a_private_session(harness, channel):
    async def run():
        h = harness
        envelope = event(channel)
        h.snapshot.value = snapshot(envelope)
        await h.gw._handle_webhook(Request(envelope))
        await settled(h.gw)
        failed = event(channel, source=8)
        failed.pop("companion")
        failed["event_type"] = {
            "mail": "message.bounced",
            "phone": "text.delivery_failed",
            "imessage": "imessage.delivery_failed",
        }[channel]
        await h.gw._handle_webhook(Request(failed, "failure"))
        assert len(h.inputs) == 1
        assert len(h.gw.sessions.sessions) == 1
        assert (
            h.gw._companion.db.execute("SELECT error FROM jobs").fetchone()[0] == "delivery-failed"
        )
        await h.gw._companion.close()
        h.gw._companion = CompanionInbox(h.gw)
        await h.gw._handle_webhook(Request(failed, "repeated-failure"))
        assert len(h.inputs) == 1
        await h.gw._companion.close()

    asyncio.run(run())


@pytest.mark.parametrize("channel", ["mail", "phone", "imessage"])
def test_versioned_wire_fixture_through_sdk_and_native_queue(harness, channel):
    sdk = pytest.importorskip("inkbox.companion")

    async def run():
        h = harness
        fixture = json.loads((Path(__file__).parent / "fixtures" / "companion-v1.json").read_text())
        assert fixture["version"] == 1
        pages = fixture["pages"]
        sponsor = "sponsor@example.com" if channel == "mail" else "+15555550100"
        for page in pages:
            page["channel"] = channel
            page["reply_context"]["channel"] = channel
            if channel != "mail":
                page["reply_context"] = {
                    "channel": channel,
                    "conversation_id": page["conversation_id"],
                }
                for entry in page["items"]:
                    if entry["is_trigger"]:
                        entry["author"] = sponsor
        h.gw.cfg.identity = fixture["handle"]
        h.gw.cfg.allowed_users = [sponsor]
        envelope = event(channel)
        envelope["companion"].update(
            {
                key: pages[0][key]
                for key in ("scope_id", "activation_id", "conversation_id", "channel")
            }
        )
        message = envelope["data"]["text_message" if channel == "phone" else "message"]
        message["id"] = pages[-1]["items"][-1]["id"]
        message["thread_id" if channel == "mail" else "conversation_id"] = pages[0][
            "conversation_id"
        ]
        if channel == "mail":
            message["from_address"] = sponsor
        cursors = []

        class Transport:
            def get(self, path, *, params):
                assert f"/identities/{fixture['handle']}/companion/" in path
                cursors.append(params.get("cursor"))
                return copy.deepcopy(pages[1 if params.get("cursor") else 0])

        h.gw._inkbox.companion = sdk.CompanionResource(Transport())
        await h.gw._handle_webhook(Request(envelope))
        await settled(h.gw)
        assert len(h.inputs) == 1
        text = h.inputs[0][0]["text"]
        assert text.count('"is_trigger":true') == 1
        assert text.count('"id":"55555555-5555-4555-8555-555555555555"') == 1
        assert "/clear" in text and "YES" in text and "café" in text
        assert "future_history_notice" in text and "future_level" in text
        assert "source_message_id" in text
        assert "opaque-page-2" in cursors
        assert h.replies[0][3]["reply_context"]["conversation_id"] == pages[0]["conversation_id"]
        await h.gw._companion.close()

    asyncio.run(run())
