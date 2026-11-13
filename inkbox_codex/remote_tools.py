"""Native catalog and operation identities for remote Inkbox tools."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path

from inkbox.remote_mcp import RemoteMcp, RemoteToolError

from . import terminal_reply
from .config import read_config
from .remote_compat import COMPATIBILITY_NAMES, compatibility_arguments
from .turn_authority import capture, validate


def native_identity(metadata):
    origin = (
        metadata.get("x-codex-turn-metadata") if isinstance(metadata, dict) else None
    )
    values = (
        [origin.get("thread_id"), origin.get("turn_id"), metadata.get("callId")]
        if isinstance(origin, dict)
        else []
    )
    if len(values) != 3 or not all(
        isinstance(value, str) and 1 <= len(value) <= 256 for value in values
    ):
        raise RemoteToolError(
            "This Codex version does not supply a stable native tool-call identity"
        )
    return values[0], hashlib.sha256(
        json.dumps(values, separators=(",", ":")).encode()
    ).hexdigest()


class RemoteTools:
    def __init__(self, client, identity, api_key):
        self.client, self.identity, self.api_key = client, identity, api_key
        self.remote = None
        self.cohort = None
        self.names = frozenset()
        self.root = (
            Path(os.getenv("INKBOX_CODEX_HOME") or Path.home() / ".inkbox-codex")
            / "remote_calls"
        )

    async def refresh(self, local):
        try:
            account = await asyncio.wait_for(asyncio.to_thread(self.client.whoami), 5)
        except Exception:  # noqa: BLE001
            raise RemoteToolError(
                "Remote tool discovery could not verify current authority"
            ) from None
        cohort = account.auth_subtype
        if cohort in {"api_key.admin_scoped", "api_key.agent_scoped.unclaimed"}:
            self.cohort = cohort
            self.remote = None
            self.names = frozenset()
            return local
        if cohort != "api_key.agent_scoped.claimed":
            raise RemoteToolError("This credential type does not support remote tools")
        try:
            identity = await asyncio.wait_for(
                asyncio.to_thread(self.client.get_identity, self.identity), 5
            )
        except Exception:  # noqa: BLE001
            raise RemoteToolError(
                "Remote tool discovery could not verify the configured identity"
            ) from None
        remote = RemoteMcp(
            api_key=self.api_key,
            base_url=self.client._base_url,
            identity=str(identity.id),
            journal_path=self.root / "catalog.sqlite",
        )
        catalog = await remote.refresh()
        names = {tool["name"] for tool in catalog}
        if terminal_reply.NAME in names:
            raise RemoteToolError("A remote tool conflicts with a local delivery tool")
        retained = []
        for tool in local:
            name = tool["name"]
            if name in names:
                raise RemoteToolError("A remote tool conflicts with a local tool")
            replacement = COMPATIBILITY_NAMES.get(name)
            if replacement is not None:
                if replacement not in names:
                    continue
                tool = {
                    **tool,
                    "description": tool["description"]
                    + f" Compatibility name for {replacement}.",
                }
            retained.append(tool)
        self.names = frozenset(names)
        self.remote = remote
        self.cohort = cohort
        helpers = (
            [terminal_reply.DESCRIPTOR]
            if names & {"inkbox_email_reply", "inkbox_text_send"}
            else []
        )
        return catalog + retained + helpers

    def routes(self, name):
        return self.remote is not None and (
            name in self.names
            or name in COMPATIBILITY_NAMES
            or name == terminal_reply.NAME
        )

    async def call(self, name, arguments, metadata):
        thread_id, operation_id = native_identity(metadata)
        origin = capture(self.identity, read_config())
        if name == terminal_reply.NAME:
            if origin is None:
                raise RemoteToolError(
                    "Terminal replies require an admitted incoming message"
                )
            journal = self.root / (
                "reply-"
                + hashlib.sha256(origin["chat_id"].encode()).hexdigest()
                + ".sqlite"
            )
            return await terminal_reply.send(
                self.remote,
                self.client,
                self.identity,
                origin.get("reply_source"),
                arguments,
                journal,
                lambda: validate(origin),
            )
        journal_name = hashlib.sha256(thread_id.encode()).hexdigest() + ".sqlite"
        return await self.remote.call(
            COMPATIBILITY_NAMES.get(name, name),
            compatibility_arguments(name, arguments, operation_id),
            call_identity=operation_id,
            journal_path=self.root / journal_name,
            before_execute=lambda: validate(origin),
        )
