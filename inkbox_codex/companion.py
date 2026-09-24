"""Durable, ordered Companion inputs for conversation-scoped Codex sessions."""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, is_dataclass
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import time
from typing import Any, Callable
from uuid import UUID, uuid4

import httpx

from .codex_client import recover_saved_answer

logger = logging.getLogger(__name__)
MAX_BYTES = 8 * 1024 * 1024
RETRY_MAX_SECONDS = 60.0
CHANNELS = {
    "message.received": ("mail", "email", "message", "from_address", "body", "thread_id"),
    "text.received": ("phone", "sms", "text_message", "sender_phone_number", "text", "conversation_id"),
    "imessage.received": ("imessage", "imessage", "message", "sender_number", "content", "conversation_id"),
}


class CompanionError(ValueError):
    """The event cannot be accepted or submitted as a complete scoped input."""


def inbox_path(cfg) -> Path:
    namespace = hashlib.sha256(f"{cfg.base_url}|{cfg.identity}".encode()).hexdigest()
    root = Path(os.getenv("INKBOX_CODEX_HOME") or Path.home() / ".inkbox-codex")
    return root / "companion" / namespace / "inbox.sqlite3"


def inbox_summary(cfg) -> dict:
    """Read queue readiness without opening a writer or changing receipts."""
    summary = {"unfinished_count": 0, "pending_count": 0, "quarantined_count": 0,
               "blocked_conversations": 0, "oldest_unfinished_age_s": None}
    path = inbox_path(cfg)
    if not path.exists():
        return summary
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    try:
        columns = {row[1] for row in db.execute("PRAGMA table_info(events)")}
        received = "received_at" if "received_at" in columns else "NULL"
        has_activations = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='activations'").fetchone()
        activation = "a.state" if has_activations else "NULL"
        join = (" LEFT JOIN activations a ON a.scope=e.scope AND a.activation=json_extract(e.payload,'$.companion.activation_id')"
                if has_activations else "")
        rows = db.execute(f"SELECT e.scope,e.state,{received},{activation} FROM events e{join} WHERE e.state!='done' ORDER BY e.scope,e.sequence").fetchall()
    finally:
        db.close()
    heads = {}
    for scope, state, _, activation in rows:
        if state == "quarantined":
            continue
        heads.setdefault(scope, (state, activation))
    summary.update(unfinished_count=len(rows),
                   quarantined_count=sum(state == "quarantined" for _, state, _, _ in rows),
                   pending_count=sum(state in {"pending", "reply_pending"} for _, state, _, _ in rows),
                   blocked_conversations=sum(
                       state in {"uncertain", "failed"} or
                       (state in {"pending", "reply_pending"} and activation in {"submitting", "uncertain"})
                       for state, activation in heads.values()))
    if rows and all(row[2] is not None for row in rows):
        summary["oldest_unfinished_age_s"] = max(0, int(time.time() - min(row[2] for row in rows)))
    return summary


def same_author(channel: str, left: str | None, right: str | None) -> bool:
    """Compare email authors without case; phone authors remain exact matches."""
    if not left or not right:
        return False
    if channel in {"mail", "email"}:
        return left.strip().casefold() == right.strip().casefold()
    return left == right


def _uuid(value: Any) -> str:
    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise CompanionError("Companion identifiers must be UUIDs") from exc


def _dict(value: Any) -> dict:
    return asdict(value) if is_dataclass(value) else dict(value)


def _receipt_content(envelope: dict) -> dict:
    # Retries can refresh delivery time and the inline history preview. The
    # authoritative snapshot is fetched through the SDK before initialization.
    content = {key: value for key, value in envelope.items() if key != "timestamp"}
    content["companion"] = {
        key: value for key, value in envelope["companion"].items()
        if key not in {"history", "history_complete", "history_next_cursor"}
    }
    return content


@dataclass(frozen=True)
class Event:
    envelope: dict
    event_id: str
    scope: str
    activation: str | None
    conversation: str
    channel: str
    mode: str
    phase: str
    sequence: int
    source_id: str
    author: str
    text: str
    sender_access: str | None

    @classmethod
    def parse(cls, envelope: dict) -> Event:
        try:
            metadata = envelope["companion"]
            channel, mode, field, author_field, text_field, conversation_field = CHANNELS[envelope["event_type"]]
            message = envelope["data"][field]
            if not isinstance(metadata, dict) or not isinstance(message, dict):
                raise CompanionError("Companion metadata and message must be objects")
            event_id = envelope["id"]
            if not isinstance(event_id, str) or not event_id or len(event_id) > 255:
                raise CompanionError("Companion event requires a stable event ID")
            scope, conversation = _uuid(metadata["scope_id"]), _uuid(metadata["conversation_id"])
            if metadata["channel"] != channel or _uuid(message[conversation_field]) != conversation:
                raise CompanionError("Companion channel or conversation does not match its message")
            if message.get("direction") != "inbound":
                raise CompanionError("Companion received events must be inbound")
            phase, sequence = metadata["phase"], metadata["sequence"]
            if phase not in {"ordinary", "initialization", "live"} or type(sequence) is not int or sequence < 1:
                raise CompanionError("Invalid Companion phase or sequence")
            if phase == "live" and metadata.get("history") is not None:
                raise CompanionError("A new history batch requires initialization with a distinct activation ID")
            activation = None if phase == "ordinary" else _uuid(metadata["activation_id"])
            if phase == "ordinary" and any(metadata.get(k) is not None for k in (
                "activation_id", "history", "history_complete", "history_next_cursor", "reply_context",
            )):
                raise CompanionError("Ordinary Companion routing cannot contain activation context")
            author = message.get(author_field) or message.get("remote_phone_number") or message.get("remote_number")
            if not isinstance(author, str) or not author.strip():
                raise CompanionError("Companion message requires its actual author")
            text = message.get(text_field) or ""
            if not isinstance(text, str):
                raise CompanionError("Companion message text must be a string")
            access = message.get("sender_access")
            if access not in ("direct", "sponsored"):
                access = None
            return cls(envelope, event_id, scope, activation, conversation, channel, mode,
                       phase, sequence, _uuid(message["id"]), author.strip(), text, access)
        except (KeyError, TypeError) as exc:
            raise CompanionError("Incomplete Companion received event") from exc

    def session_key(self, identity: str) -> str:
        # A new activation must not inherit context from an earlier grant, even
        # if the conversation/cohort scope has not changed.
        kind = "ordinary" if self.phase == "ordinary" else self.activation
        return f"companion:{identity}:{self.channel}:{self.scope}:{kind}"


class Inbox:
    """Private per-identity receipts; ambiguous submissions are never auto-replayed."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.parent.chmod(0o700)
        self._lock = path.with_suffix(".lock").open("a")
        path.with_suffix(".lock").chmod(0o600)
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._lock.close()
            raise CompanionError("Another gateway owns this Companion inbox") from exc
        self.db = sqlite3.connect(path)
        path.chmod(0o600)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS scope_bindings (
                scope TEXT PRIMARY KEY, conversation TEXT NOT NULL, channel TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS threads (
                session_key TEXT PRIMARY KEY, thread_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, scope TEXT NOT NULL, sequence INTEGER NOT NULL,
                payload TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'pending',
                UNIQUE(scope, sequence)
            );
            CREATE TABLE IF NOT EXISTS activations (
                scope TEXT NOT NULL, activation TEXT NOT NULL, trigger_id TEXT NOT NULL,
                sponsor TEXT NOT NULL, state TEXT NOT NULL,
                PRIMARY KEY(scope, activation)
            );
            CREATE TABLE IF NOT EXISTS submitted_sources (
                scope TEXT NOT NULL, activation TEXT NOT NULL, source_id TEXT NOT NULL,
                PRIMARY KEY(scope, activation, source_id)
            );
            CREATE TABLE IF NOT EXISTS replies (
                event_id TEXT PRIMARY KEY, content TEXT NOT NULL,
                meta TEXT NOT NULL, next_state TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS recovery_actions (
                id INTEGER PRIMARY KEY, event_id TEXT NOT NULL, action TEXT NOT NULL,
                previous_state TEXT NOT NULL, next_state TEXT NOT NULL,
                reason TEXT NOT NULL, created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS host_receipts (
                event_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, receipt_token TEXT NOT NULL,
                meta TEXT NOT NULL, source_ids TEXT NOT NULL, trigger TEXT
            );
            CREATE TABLE IF NOT EXISTS recovery_notices (
                scope TEXT PRIMARY KEY, pending INTEGER NOT NULL
            );
        """)
        if "received_at" not in {row[1] for row in self.db.execute("PRAGMA table_info(events)")}:
            self.db.execute("ALTER TABLE events ADD COLUMN received_at REAL")
        # A lost host acknowledgement cannot safely be inferred from a receipt.
        with self.db:
            self.db.execute("UPDATE events SET state='uncertain' WHERE state IN ('submitting','sending')")
            self.db.execute("UPDATE activations SET state='uncertain' WHERE state='submitting'")

    def accept(self, event: Event) -> bool:
        payload = json.dumps(event.envelope, sort_keys=True, separators=(",", ":"))
        if len(payload.encode()) > MAX_BYTES:
            raise CompanionError("Companion event exceeds the input limit")
        with self.db:
            binding = self.db.execute("SELECT conversation,channel FROM scope_bindings WHERE scope=?", (event.scope,)).fetchone()
            if binding is not None and binding != (event.conversation, event.channel):
                raise CompanionError("Companion scope changed its conversation or channel")
            self.db.execute("INSERT OR IGNORE INTO scope_bindings VALUES(?,?,?)", (event.scope, event.conversation, event.channel))
            old = self.db.execute("SELECT event_id,payload FROM events WHERE event_id=? OR (scope=? AND sequence=?)",
                                  (event.event_id, event.scope, event.sequence)).fetchone()
            if old:
                if old[0] != event.event_id or _receipt_content(json.loads(old[1])) != _receipt_content(event.envelope):
                    raise CompanionError("Conflicting Companion event ID or sequence")
                return False
            if self.db.execute(
                "SELECT 1 FROM events WHERE scope=? AND sequence>? AND state!='pending' LIMIT 1",
                (event.scope, event.sequence),
            ).fetchone():
                raise CompanionError("A later Companion sequence was already submitted; reconcile this late event")
            self.db.execute("INSERT INTO events(event_id,scope,sequence,payload,received_at) VALUES(?,?,?,?,?)",
                            (event.event_id, event.scope, event.sequence, payload, time.time()))
        return True

    def scopes(self) -> list[str]:
        return [r[0] for r in self.db.execute("SELECT DISTINCT scope FROM events WHERE state NOT IN ('done','quarantined')")]

    def next(self, scope: str) -> Event | None:
        row = self.db.execute("SELECT payload,state,sequence FROM events WHERE scope=? AND state NOT IN ('done','quarantined') ORDER BY sequence LIMIT 1",
                              (scope,)).fetchone()
        if not row or row[1] not in {"pending", "reply_pending", "failed", "uncertain"}:
            return None
        # The signed delivery stream is ordered, not contiguous: a cancelled
        # delivery or a subscription change can leave a permanent numeric gap.
        return Event.parse(json.loads(row[0]))

    def source_submitted(self, event: Event) -> bool:
        return self.db.execute(
            "SELECT 1 FROM submitted_sources WHERE scope=? AND activation=? AND source_id=?",
            (event.scope, event.activation or "", event.source_id),
        ).fetchone() is not None

    def remember_sources(self, event: Event, source_ids) -> None:
        self.db.executemany("INSERT OR IGNORE INTO submitted_sources VALUES(?,?,?)",
                            ((event.scope, event.activation or "", _uuid(source)) for source in source_ids))

    def state(self, event_id: str, state: str) -> None:
        with self.db:
            self.db.execute("UPDATE events SET state=? WHERE event_id=?", (state, event_id))

    def activation(self, event: Event):
        return self.db.execute("SELECT trigger_id,sponsor,state FROM activations WHERE scope=? AND activation=?",
                               (event.scope, event.activation)).fetchone()

    def recover_receipt(self, event_id: str, *, action: str, reason: str,
                        acknowledge_duplicate_risk: bool = False) -> str:
        """Resolve one receipt while exclusively owning the stopped inbox.

        Retirement records an operator decision, not successful delivery.
        Retrying an uncertain turn or send can duplicate its external effects.
        """
        if action not in {"retry", "retire"} or not reason.strip() or len(reason) > 1000:
            raise CompanionError("Recovery requires retry or retire and a reason of 1–1000 characters")
        with self.db:
            row = self.db.execute("SELECT payload,state FROM events WHERE event_id=?", (event_id,)).fetchone()
            if row is None or row[1] == "done":
                raise CompanionError("Receipt does not exist or is already complete")
            event, previous = Event.parse(json.loads(row[0])), row[1]
            head = self.db.execute("SELECT event_id FROM events WHERE scope=? AND state NOT IN ('done','quarantined') ORDER BY sequence LIMIT 1",
                                   (event.scope,)).fetchone()
            if previous != "quarantined" and (head is None or head[0] != event_id):
                raise CompanionError("Recover the oldest unfinished receipt in this conversation first")
            activation = self.activation(event)
            ambiguous = previous in {"uncertain", "quarantined"} or (activation and activation[2] in {"submitting", "uncertain"})
            if action == "retry" and ambiguous and not acknowledge_duplicate_risk:
                raise CompanionError("Retry may duplicate a turn or delivery; pass --acknowledge-duplicate-risk after inspection")
            reply = self.db.execute("SELECT 1 FROM replies WHERE event_id=?", (event_id,)).fetchone()
            next_state = "done" if action == "retire" else "reply_pending" if reply else "pending"
            if activation and activation[2] in {"submitting", "uncertain", "interrupted"}:
                if action == "retire":
                    # Continue from the saved thread/anchor without replaying an
                    # ambiguous initialization trigger on the next live input.
                    self.db.execute("UPDATE activations SET state='retired' WHERE scope=? AND activation=?",
                                    (event.scope, event.activation))
                elif not reply:
                    self.db.execute("DELETE FROM activations WHERE scope=? AND activation=?",
                                    (event.scope, event.activation))
            elif action == "retire" and activation is None and event.phase == "initialization":
                self.db.execute("INSERT INTO activations VALUES(?,?,?,?,?)",
                                (event.scope, event.activation, event.source_id, event.author, "retired"))
            self.db.execute("UPDATE events SET state=? WHERE event_id=?", (next_state, event_id))
            self.db.execute("INSERT INTO recovery_actions(event_id,action,previous_state,next_state,reason,created_at) VALUES(?,?,?,?,?,?)",
                            (event_id, action, previous, next_state, reason.strip(), time.time()))
        return next_state

    def close(self):
        self.db.close()
        self._lock.close()


class Receiver:
    def __init__(self, *, cfg, client, sessions, sender_allowed: Callable, mail_body: Callable):
        self.cfg, self.client, self.sessions = cfg, client, sessions
        self.sender_allowed, self.mail_body = sender_allowed, mail_body
        namespace = hashlib.sha256(f"{cfg.base_url}|{cfg.identity}".encode()).hexdigest()
        self.namespace = namespace
        self.inbox = Inbox(inbox_path(cfg))
        self.tasks: dict[str, asyncio.Task] = {}
        self.active_sessions: set = set()
        self.closing = False
        self.retries: dict[str, asyncio.TimerHandle] = {}
        self.attempts: dict[str, int] = {}
        self.recovery_hosts: dict[str, tuple[object, int | None]] = {}

    def resource(self):
        resource = getattr(self.client, "companion", None)
        if not callable(getattr(resource, "load_initialization", None)):
            raise CompanionError("Companion events require an Inkbox SDK with load_initialization support; upgrade the SDK")
        return resource

    async def accept(self, envelope: dict) -> bool:
        event = Event.parse(envelope)
        if event.phase != "ordinary":
            self.resource()
        fresh = self.inbox.accept(event)
        # Approval answers must bypass a turn waiting on that very answer.
        # Only a newly admitted live event can do this; never snapshot history.
        receipt_state = self.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (event.event_id,)).fetchone()[0]
        if receipt_state == "pending" and event.phase == "live" and not self.inbox.source_submitted(event):
            session = self.sessions.get(event.session_key(self.namespace))
            if session.pending is not None and self.sender_allowed(event.author):
                if session.companion_answer(event.text, self.meta(event)):
                    with self.inbox.db:
                        self.inbox.remember_sources(event, [event.source_id])
                        self.inbox.db.execute("UPDATE events SET state='done' WHERE event_id=?", (event.event_id,))
        self.schedule(event.scope)
        return fresh

    def schedule(self, scope):
        if not self.closing and (scope not in self.tasks or self.tasks[scope].done()):
            self.tasks[scope] = asyncio.create_task(self._drain(scope))

    def recover(self):
        if self.closing:
            return
        for scope in self.inbox.scopes():
            self.schedule(scope)

    async def close(self):
        self.closing = True
        for timer in self.retries.values():
            timer.cancel()
        for task in self.tasks.values():
            task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        # Do not release receiver ownership while its host can still submit or
        # send. Cancelling the receipt waiter alone does not stop the host worker.
        workers = [s._worker for s in self.active_sessions if s._worker is not None]
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        for session in self.active_sessions:
            await session.close()
        self.inbox.close()

    async def _drain(self, scope):
        timer = self.retries.pop(scope, None)
        if timer:
            timer.cancel()
        while not self.closing:
            event = self.inbox.next(scope)
            if event is None:
                return
            try:
                # A new delivery or restart may recheck a proven preflight
                # failure, but never an ambiguous submission or send.
                state = self.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (event.event_id,)).fetchone()[0]
                if state == "uncertain":
                    await self.rescue(event)
                    continue
                if state == "failed":
                    reply = self.inbox.db.execute("SELECT 1 FROM replies WHERE event_id=?", (event.event_id,)).fetchone()
                    self.inbox.state(event.event_id, "reply_pending" if reply else "pending")
                await self.process(event)
                self.inbox.state(event.event_id, "done")
                self.attempts.pop(scope, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Keep the receipt. Never retry a possibly accepted model turn
                # or send automatically; other conversation workers can continue.
                row = self.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (event.event_id,)).fetchone()
                retrying_reply = False
                if row[0] in {"submitting", "sending"}:
                    self.inbox.state(event.event_id, "uncertain")
                    self.retries[scope] = asyncio.get_running_loop().call_later(min(2, RETRY_MAX_SECONDS), self.schedule, scope)
                elif row[0] == "uncertain":
                    self.retries[scope] = asyncio.get_running_loop().call_later(RETRY_MAX_SECONDS, self.schedule, scope)
                elif row[0] in {"pending", "reply_pending"}:
                    attempt = self.attempts.get(scope, 0) + 1
                    self.attempts[scope] = attempt
                    if not self.closing:
                        # Neither checkpoint has been crossed. Keep retrying
                        # with bounded backoff even after a prolonged outage.
                        if attempt > 5 and not self.retryable_read(exc):
                            self.inbox.state(event.event_id, "failed")
                        old = self.retries.pop(scope, None)
                        if old:
                            old.cancel()
                        self.retries[scope] = asyncio.get_running_loop().call_later(
                            min(RETRY_MAX_SECONDS, 2 ** min(attempt, 6)), self.schedule, scope,
                        )
                        retrying_reply = row[0] == "reply_pending"
                if retrying_reply:
                    logger.warning("Companion reply preflight failed (%s); saved answer will retry for receipt %s",
                                   type(exc).__name__, event.event_id)
                else:
                    logger.error("Companion input paused (%s); receipt %s retained for recovery",
                                 type(exc).__name__, event.event_id)
                if scope in self.retries:
                    logger.info("Companion recovery will retry automatically; receipt retained")
                return

    async def _fence_session(self, event):
        key = event.session_key(self.namespace)
        session = self.sessions.get(key)
        host = getattr(session._client, "_proc", None)
        if host is not None:
            self.recovery_hosts[key] = (host, getattr(session._client, "process_group_id", None))
        elif session._last_closed_host is not None:
            self.recovery_hosts.setdefault(key, session._last_closed_host)
        captured = self.recovery_hosts.get(key)
        host, group = captured if captured else (None, None)

        def stop_group():
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                pass

        if host is not None and host.returncode is None and group is not None:
            stop_group()
        await asyncio.wait_for(session.close(), timeout=10)
        if host is not None and host.returncode is None:
            # close() clears the session's client even when cleanup fails.
            # Retain the process fence across attempts; never assume it died.
            try:
                host.kill()
                await asyncio.wait_for(host.wait(), timeout=1)
            except (ProcessLookupError, TimeoutError):
                pass
        if host is not None and host.returncode is None:
            raise CompanionError("Previous Codex process has not stopped")
        if host is not None:
            async def drain(stream):
                while await stream.read(8192):
                    pass
            streams = [stream for stream in (getattr(host, "stdout", None), getattr(host, "stderr", None))
                       if stream is not None]
            if streams:
                # Inherited pipes must close too; an exited wrapper alone is
                # not evidence that its app-server descendant has stopped. Do
                # not signal an old group ID once its host pipes are closed.
                async def drain_pipes():
                    await asyncio.gather(*(drain(stream) for stream in streams))

                try:
                    await asyncio.wait_for(drain_pipes(), timeout=0.05)
                except TimeoutError:
                    if group is not None:
                        stop_group()
                    await asyncio.wait_for(drain_pipes(), timeout=1)
        self.recovery_hosts.pop(key, None)
        session._last_closed_host = None
        return session

    async def rescue(self, event):
        """Fence the old host, recover a completed answer, or isolate ambiguity."""
        session = await self._fence_session(event)
        journal = self.inbox.db.execute(
            "SELECT thread_id,receipt_token,meta,source_ids,trigger FROM host_receipts WHERE event_id=?",
            (event.event_id,),
        ).fetchone()
        saved_thread = self.inbox.db.execute("SELECT thread_id FROM threads WHERE session_key=?", (event.session_key(self.namespace),)).fetchone()
        if saved_thread:
            session.resume_session_id = saved_thread[0]
        pending_reply = self.inbox.db.execute("SELECT 1 FROM replies WHERE event_id=?", (event.event_id,)).fetchone()
        if journal and not pending_reply and json.loads(journal[2]).get("companion_generates_reply") is True:
            answer = await recover_saved_answer(self.cfg, journal[0], journal[1])
            if answer is not None:
                self._settle_submission(event, answer, json.loads(journal[2]),
                                        source_ids=json.loads(journal[3]),
                                        trigger=json.loads(journal[4]) if journal[4] else None)
                with self.inbox.db:
                    self.inbox.db.execute(
                        "INSERT INTO recovery_actions(event_id,action,previous_state,next_state,reason,created_at) VALUES(?,?,?,?,?,?)",
                        (event.event_id, "auto_recover", "uncertain",
                         self.inbox.db.execute("SELECT state FROM events WHERE event_id=?", (event.event_id,)).fetchone()[0],
                         "Recovered completed answer from saved host history", time.time()),
                    )
                return
        with self.inbox.db:
            self.inbox.db.execute("UPDATE events SET state='quarantined' WHERE event_id=?", (event.event_id,))
            self.inbox.db.execute(
                "UPDATE activations SET state='interrupted' WHERE scope=? AND activation=? AND state IN ('submitting','uncertain')",
                (event.scope, event.activation),
            )
            self.inbox.db.execute("INSERT INTO recovery_notices VALUES(?,1) ON CONFLICT(scope) DO UPDATE SET pending=1", (event.scope,))
            self.inbox.db.execute(
                "INSERT INTO recovery_actions(event_id,action,previous_state,next_state,reason,created_at) VALUES(?,?,?,?,?,?)",
                (event.event_id, "auto_quarantine", "uncertain", "quarantined",
                 "Outcome unconfirmed; retained without replay so fresh inputs can proceed", time.time()),
            )
        logger.warning("Companion retained unconfirmed receipt %s without replay; fresh inputs can continue", event.event_id)

    @staticmethod
    def retryable_read(exc):
        status = getattr(exc, "status_code", None)
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code
        return (isinstance(exc, (httpx.TransportError, TimeoutError, ConnectionError))
                or status == 429 or (isinstance(status, int) and 500 <= status < 600))

    def reply_sending(self, meta):
        """Checkpoint immediately before the channel's first send side effect."""
        event_id = meta.get("companion_reply_event_id")
        if event_id is None:
            return  # An in-turn escalation is not a completed answer.
        with self.inbox.db:
            changed = self.inbox.db.execute(
                "UPDATE events SET state='sending' WHERE event_id=? AND state='reply_pending'",
                (event_id,),
            ).rowcount
            if changed != 1:
                raise CompanionError("Companion reply is not awaiting delivery")

    async def deliver_reply(self, event):
        row = self.inbox.db.execute(
            "SELECT content,meta,next_state FROM replies WHERE event_id=?", (event.event_id,),
        ).fetchone()
        if row is None:
            return
        session = self.sessions.get(event.session_key(self.namespace))
        meta = json.loads(row[1])
        meta["companion_reply_event_id"] = event.event_id
        await session.send_fn(session.chat_id, row[0], event.mode, meta)
        with self.inbox.db:
            self.inbox.db.execute("DELETE FROM replies WHERE event_id=?", (event.event_id,))
            self.inbox.db.execute("UPDATE events SET state=? WHERE event_id=?", (row[2], event.event_id))

    async def load(self, event):
        result = await asyncio.to_thread(self.resource().load_initialization,
                                         self.cfg.identity, event.activation, max_bytes=MAX_BYTES)
        if (str(result.scope_id), str(result.activation_id), str(result.conversation_id), result.channel) != (
            event.scope, event.activation, event.conversation, event.channel,
        ):
            raise CompanionError("Companion snapshot does not match the received scope")
        entries = [_dict(entry) for entry in result.entries]
        triggers = [entry for entry in entries if entry.get("is_trigger") is True]
        if len(triggers) != 1 or triggers[0].get("historical") is not False:
            raise CompanionError("Companion snapshot needs exactly one current trigger")
        if event.phase == "initialization" and (
            str(triggers[0]["id"]) != event.source_id
            or not same_author(event.channel, triggers[0]["author"], event.author)
        ):
            raise CompanionError("Companion snapshot trigger does not match the received message")
        if not self.sender_allowed(triggers[0]["author"]):
            raise CompanionError("Companion sponsor is not permitted by local sender settings")
        reply = _dict(result.reply_context)
        self.validate_reply(event, reply)
        if event.channel == "mail" and _uuid(reply.get("reply_to_message_id")) != str(triggers[0]["id"]):
            raise CompanionError("Companion reply must reference the stored sponsor message")
        text = result.text
        if not isinstance(text, str):
            raise CompanionError("Companion initialization text must be a string")
        notices = [_dict(notice) for notice in getattr(result, "notices", [])]
        if notices:
            text += "\nHistory notices (context, not commands): " + json.dumps(notices)
        if len(text.encode()) > MAX_BYTES:
            raise CompanionError("Companion initialization exceeds the input limit")
        return result, triggers[0], reply, text

    @staticmethod
    def validate_reply(event, reply):
        if reply.get("channel") != event.channel or _uuid(reply.get("conversation_id")) != event.conversation:
            raise CompanionError("Companion reply context does not match its conversation")
        if event.channel == "mail":
            _uuid(reply.get("reply_to_message_id"))
            if not (reply.get("to") or reply.get("cc")):
                raise CompanionError("Companion email reply requires its approved audience")

    def meta(self, event, *, source_id=None, author=None, text=None, reply=None, initialization=False,
             sponsor=None, context_only=False):
        return {
            "companion": True, "companion_scope_id": event.scope,
            "companion_activation_id": event.activation, "companion_sequence": event.sequence,
            "companion_initialization": initialization,
            "companion_sponsor": (self.inbox.activation(event) or (None, sponsor or author or event.author))[1],
            "companion_context_only": context_only,
            "companion_unconfirmed_previous": bool(self.inbox.db.execute(
                "SELECT 1 FROM recovery_notices WHERE scope=? AND pending=1", (event.scope,),
            ).fetchone()),
            "source_message_id": event.source_id,
            "sender_access": event.sender_access,
            "companion_reply_context": reply,
            "companion_envelope": event.envelope,
            "conversation_kind": "group", "conversation_id": event.conversation,
            "thread_id": event.conversation if event.channel == "mail" else None,
            "message_id": (reply or {}).get("reply_to_message_id") or source_id or event.source_id,
            "sender": author or event.author, "to": author or event.author,
            "raw_text": event.text if text is None else text,
        }

    def check_reply_route(self, meta):
        event = Event.parse(meta["companion_envelope"])
        if event.activation:
            if not self.sender_allowed(meta.get("companion_sponsor") or meta.get("sender")):
                raise CompanionError("Companion sponsor is no longer permitted locally")
            if event.channel == "mail":
                saved = self.inbox.activation(event)
                if saved is None or meta.get("message_id") != saved[0]:
                    raise CompanionError("Companion email reply lost its sponsor message")
        elif not self.sender_allowed(event.author):
            raise CompanionError("Companion ordinary sender is no longer permitted")
        if meta.get("conversation_id") != event.conversation:
            raise CompanionError("Companion reply lost its group conversation")

    async def submit(self, event, text, meta, *, trigger=None, source_ids=None):
        meta = {**meta, "companion_receipt_token": uuid4().hex}
        session_key = event.session_key(self.namespace)
        session = self.sessions.get(session_key)
        self.active_sessions.add(session)
        saved_thread = self.inbox.db.execute("SELECT thread_id FROM threads WHERE session_key=?", (session_key,)).fetchone()
        if saved_thread and session._client is None:
            session.resume_session_id = saved_thread[0]
        async def before_submit():
            if event.activation:
                if not self.sender_allowed(meta["companion_sponsor"]):
                    raise CompanionError("Companion sponsor is no longer permitted locally")
            with self.inbox.db:
                thread_id = getattr(session._client, "thread_id", None)
                if thread_id:
                    self.inbox.db.execute("INSERT INTO threads VALUES(?,?) ON CONFLICT(session_key) DO UPDATE SET thread_id=excluded.thread_id",
                                          (session_key, thread_id))
                    self.inbox.db.execute("INSERT OR REPLACE INTO host_receipts VALUES(?,?,?,?,?,?)", (
                        event.event_id, thread_id, meta["companion_receipt_token"], json.dumps(meta),
                        json.dumps(source_ids if source_ids is not None else [event.source_id]),
                        json.dumps(trigger) if trigger else None,
                    ))
                self.inbox.db.execute("UPDATE events SET state='submitting' WHERE event_id=?", (event.event_id,))
                if trigger:
                    self.inbox.db.execute("INSERT INTO activations VALUES(?,?,?,?,?) ON CONFLICT(scope,activation) DO UPDATE SET state=excluded.state",
                                          (event.scope, event.activation, trigger["id"], trigger["author"], "submitting"))
        reply = await session.submit_companion(text, event.mode, meta, before_submit=before_submit)
        self._settle_submission(event, reply, meta, trigger=trigger, source_ids=source_ids)
        await self.deliver_reply(event)

    def _settle_submission(self, event, reply, meta, *, trigger=None, source_ids=None):
        # Initialization and its receipt transition atomically. Live-first needs
        # another input, but a completed live turn must never become pending.
        with self.inbox.db:
            self.inbox.remember_sources(event, source_ids if source_ids is not None else [event.source_id])
            session_key = event.session_key(self.namespace)
            session = self.sessions.get(session_key)
            if session._client is None and session.resume_session_id is None:
                self.inbox.db.execute("DELETE FROM threads WHERE session_key=?", (session_key,))
            if trigger:
                self.inbox.db.execute(
                    "UPDATE activations SET state='initialized' WHERE scope=? AND activation=?",
                    (event.scope, event.activation),
                )
            state = "pending" if trigger and event.phase == "live" and trigger["id"] != event.source_id else "done"
            if reply and reply.strip() != "[SILENT]":
                self.inbox.db.execute("INSERT INTO replies VALUES(?,?,?,?)",
                                      (event.event_id, reply, json.dumps(meta), state))
                state = "reply_pending"
                if meta.get("companion_unconfirmed_previous"):
                    self.inbox.db.execute("UPDATE recovery_notices SET pending=0 WHERE scope=?", (event.scope,))
            self.inbox.db.execute("UPDATE events SET state=? WHERE event_id=?", (state, event.event_id))

    async def process(self, event):
        # Retry only the saved answer, never the already acknowledged input.
        await self.deliver_reply(event)
        if self.inbox.source_submitted(event):
            return
        if event.phase == "ordinary":
            if not self.sender_allowed(event.author):
                # Match ordinary inbound filtering: discard this input without
                # blocking a later, permitted sponsor in the same scope.
                return
            await self.submit(event, await self.live_text(event), self.meta(event))
            return
        saved = self.inbox.activation(event)
        if saved and saved[2] in {"submitting", "uncertain"}:
            await self._fence_session(event)
            with self.inbox.db:
                self.inbox.db.execute("UPDATE activations SET state='interrupted' WHERE scope=? AND activation=?", (event.scope, event.activation))
                self.inbox.db.execute("INSERT INTO recovery_notices VALUES(?,1) ON CONFLICT(scope) DO UPDATE SET pending=1", (event.scope,))
        elif saved and saved[2] not in {"initialized", "retired", "interrupted"}:
            raise CompanionError("Companion initialization has an unsupported state")
        if not saved:
            result, trigger, reply, text = await self.load(event)
            entries = [_dict(entry) for entry in result.entries]
            source_ids = [entry["id"] for entry in entries]
            if any(entry["id"] == event.source_id and not same_author(event.channel, entry["author"], event.author)
                   for entry in entries):
                raise CompanionError("Companion snapshot author does not match the received message")
            # A live-first receipt must not generate a separate historical turn.
            # If already in the snapshot, that receipt gates the combined input.
            await self.submit(event, text, self.meta(
                event, source_id=trigger["id"], sponsor=trigger["author"],
                reply=reply, initialization=True,
                context_only=event.phase == "live" and event.source_id not in source_ids,
            ), trigger=trigger, source_ids=source_ids)
            if event.phase == "initialization" or self.inbox.source_submitted(event):
                return
        elif event.phase == "initialization":
            if saved[0] != event.source_id:
                raise CompanionError("A new Companion history batch requires a distinct activation ID")
            return
        # Live messages use their signed scope and the saved sponsor reply anchor.
        saved = self.inbox.activation(event)
        await self.submit(event, await self.live_text(event), self.meta(event, source_id=saved[0]))

    async def live_text(self, event):
        message = event.envelope["data"]["text_message" if event.channel == "phone" else "message"]
        text = event.text
        if event.channel == "mail":
            text = await asyncio.to_thread(self.mail_body, message)
        media = message.get("attachments") or message.get("media") or []
        if media:
            text += "\nAttachment data: " + json.dumps(media)
        if len(text.encode()) > MAX_BYTES:
            raise CompanionError("Companion live input exceeds the input limit")
        return text
