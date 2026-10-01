"""Best-effort, ordered native Slack status with restart cleanup."""

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
        self._active: dict[str, dict[str, str]] = {}
        self._records: dict[str, dict] = {}
        self._tails: dict[str, asyncio.Task] = {}
        self._closing = False
        self._supported = callable(getattr(resource, "set_processing_status", None))

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
            logger.warning("Slack native status needs an SDK with processing-status support")
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
                               for field in ("connection_id", "conversation_id", "token"))):
                continue
            if isinstance(record.get("thread_ts"), str) and record["thread_ts"]:
                if record.get("state") not in {"processing", "suspended", "active"}:
                    continue
                if record["state"] != "active":
                    record = {**record, "state": "active", "token": uuid4().hex}
            elif isinstance(record.get("message_ts"), str) and record["message_ts"]:
                # Retire indicators left by the previous reaction-based implementation.
                record = {**record, "state": "completed"}
            else:
                continue
            valid[key] = record
        self._records.update(valid)
        for key, record in valid.items():
            self._schedule(key, record)

    async def notify(self, _chat_id: str, mode: str, meta: dict, state: str) -> None:
        if mode != "slack" or self._closing or not self._supported:
            return
        # A status on an unthreaded DM would open a thread we do not reply into.
        fields = [meta.get("connection_id"), meta.get("conversation_id"), meta.get("thread_ts")]
        event_id = meta.get("source_event_id")
        if not all(isinstance(value, str) and value for value in [*fields, event_id]):
            return
        key = hashlib.sha256(json.dumps(fields).encode()).hexdigest()
        active = self._active.get(key, {})
        if state == "accepted":
            if event_id in active:
                return
            active[event_id] = "processing"
        elif state in {"waiting", "resumed"}:
            if event_id not in active:
                return
            active[event_id] = "suspended" if state == "waiting" else "processing"
        elif state in {"completed", "failed", "cancelled"}:
            if event_id not in active:
                return
            active.pop(event_id)
        else:
            return
        if active:
            self._active[key] = active
            desired = "suspended" if "suspended" in active.values() else "processing"
        else:
            self._active.pop(key, None)
            desired = "active"
        if self._records.get(key, {}).get("state") == desired:
            return
        self._schedule(key, dict(zip(("connection_id", "conversation_id", "thread_ts"), fields),
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
        native = "thread_ts" in record
        changes = [("set_processing_status", record["thread_ts"], state)] if native else [
            ("remove_reaction", record["message_ts"], "eyes"),
            ("remove_reaction", record["message_ts"], "x"),
        ]
        succeeded = True
        for method, timestamp, value in changes:
            operation_key = hashlib.sha256(
                f"{key}:{record['token']}:{state}:{method}:{value}".encode()
            ).hexdigest()
            try:
                operation = await asyncio.to_thread(
                    getattr(self.resource, method),
                    record["connection_id"], record["conversation_id"], timestamp, value,
                    idempotency_key=f"codex:activity:{operation_key}",
                )
                if operation.status != "succeeded":
                    succeeded = False
                    code = getattr(operation, "error_code", None)
                    reason = code if code in {"feature_disabled", "missing_scope", "not_allowed_token_type",
                                              "channel_not_found", "thread_ts_required"} else "unconfirmed"
                    logger.warning("Slack native status/cleanup not confirmed (%s); the agent turn is unaffected", reason)
                elif native:
                    logger.info("Slack native status confirmed: %s", state)
            except Exception:
                succeeded = False
                logger.warning("Slack native status/cleanup failed; check connection permissions and API availability")
        if succeeded and state in {"active", "completed"} and self._records.get(key) is record:
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
                self._schedule(key, {**record, "state": "active", "token": uuid4().hex})
        self._active.clear()
        try:
            await asyncio.wait_for(self.flush(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("Slack activity cleanup is deferred until the next gateway start")
