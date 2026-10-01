"""Best-effort, ordered Slack turn reactions with restart cleanup."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from pathlib import Path
from uuid import uuid4

logger = logging.getLogger(__name__)


class SlackActivity:
    def __init__(self, resource, state_path: Path):
        self.resource = resource
        self.state_path = state_path
        self._active: dict[str, set[str]] = {}
        self._failed: set[str] = set()
        self._records: dict[str, dict] = {}
        self._tails: dict[str, asyncio.Task] = {}
        self._closing = False
        self._supported = all(callable(getattr(resource, name, None))
                              for name in ("add_reaction", "remove_reaction"))

    def _persist(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.state_path.with_suffix(".tmp")
            temporary.write_text(json.dumps(self._records) + "\n")
            temporary.chmod(0o600)
            temporary.replace(self.state_path)
        except OSError:
            logger.warning("Slack activity state could not be saved")

    async def recover(self) -> None:
        if not self._supported:
            logger.warning("Slack activity reactions need an SDK with reaction support")
            return
        try:
            records = json.loads(self.state_path.read_text())
        except FileNotFoundError:
            return
        except (OSError, ValueError):
            logger.warning("Slack activity state could not be read")
            return
        if not isinstance(records, dict):
            return
        valid = {}
        for key, record in records.items():
            if (not isinstance(record, dict)
                    or not all(isinstance(record.get(field), str) and record[field]
                               for field in ("connection_id", "conversation_id", "message_ts", "token"))
                    or record.get("state") not in {"active", "completed", "failed", "cancelled"}):
                continue
            if record["state"] == "active":
                record = {**record, "state": "failed"}
            valid[key] = record
        self._records.update(valid)
        for key, record in valid.items():
            self._schedule(key, record)

    async def notify(self, _chat_id: str, mode: str, meta: dict, state: str) -> None:
        if mode != "slack" or self._closing or not self._supported:
            return
        anchor = meta.get("thread_ts") or meta.get("message_ts")
        fields = [meta.get("connection_id"), meta.get("conversation_id"), anchor]
        event_id = meta.get("source_event_id")
        if not all(isinstance(value, str) and value for value in [*fields, event_id]):
            return
        key = hashlib.sha256(json.dumps(fields).encode()).hexdigest()
        active = self._active.get(key, set())
        if state == "accepted":
            if event_id in active:
                return
            active.add(event_id)
            self._active[key] = active
            if len(active) > 1:
                return
            self._failed.discard(key)
            desired = "active"
        else:
            if event_id not in active:
                return
            active.remove(event_id)
            if state == "failed":
                self._failed.add(key)
            if active:
                return
            self._active.pop(key, None)
            desired = "failed" if key in self._failed else state
            self._failed.discard(key)
        self._schedule(key, dict(zip(("connection_id", "conversation_id", "message_ts"), fields),
                                 state=desired, token=uuid4().hex))

    def _schedule(self, key: str, record: dict) -> None:
        self._records[key] = record
        self._persist()
        previous = self._tails.get(key)
        task = asyncio.create_task(self._apply_after(previous, key, record))
        self._tails[key] = task

        def finished(done):
            if self._tails.get(key) is done:
                self._tails.pop(key, None)

        task.add_done_callback(finished)

    async def _apply_after(self, previous, key: str, record: dict) -> None:
        if previous is not None:
            await previous
        state = record["state"]
        changes = [("remove", "x"), ("add", "eyes")] if state == "active" else [
            ("remove", "eyes"), ("add" if state == "failed" else "remove", "x"),
        ]
        succeeded = True
        for verb, name in changes:
            operation_key = hashlib.sha256(
                f"{key}:{record['token']}:{state}:{verb}:{name}".encode()
            ).hexdigest()
            try:
                operation = await asyncio.to_thread(
                    getattr(self.resource, f"{verb}_reaction"),
                    record["connection_id"], record["conversation_id"], record["message_ts"], name,
                    idempotency_key=f"codex:activity:{operation_key}",
                )
                if operation.status != "succeeded":
                    succeeded = False
                    logger.warning("Slack activity reaction was not confirmed; the agent turn is unaffected")
            except Exception:
                succeeded = False
                logger.warning("Slack activity reaction failed; check connection permissions and API availability")
        if succeeded and state != "active" and self._records.get(key) is record:
            self._records.pop(key, None)
            self._persist()

    async def flush(self) -> None:
        while self._tails:
            await asyncio.gather(*list(self._tails.values()), return_exceptions=True)

    async def close(self) -> None:
        self._closing = True
        for key in self._active:
            record = self._records.get(key)
            if record is not None:
                self._schedule(key, {**record, "state": "cancelled", "token": uuid4().hex})
        self._active.clear()
        try:
            await asyncio.wait_for(self.flush(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("Slack activity cleanup is deferred until the next gateway start")
