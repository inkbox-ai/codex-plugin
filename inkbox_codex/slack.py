"""Slack channel routing and the small SDK-backed tool surface."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any


SLACK_INCOMING_EVENTS = (
    "slack.dm_received",
    "slack.group_dm_received",
    "slack.channel_message_received",
    "slack.mention_received",
    "slack.thread_reply_received",
)
SLACK_ATTENTION_EVENTS = tuple(
    event for event in SLACK_INCOMING_EVENTS if event != "slack.channel_message_received"
)


def slack_resource(client: Any) -> Any:
    resource = getattr(client, "slack", None)
    if resource is None:
        raise RuntimeError("Slack requires an Inkbox SDK with Slack support")
    return resource


def reconcile_subscription(client: Any, identity_id: Any, url: str) -> None:
    slack_resource(client)
    subscriptions = client.webhooks.subscriptions
    events = list(SLACK_ATTENTION_EVENTS)
    for sub in subscriptions.list(agent_identity_id=identity_id):
        if sub.url == url and set(sub.event_types) == set(events):
            if sub.status != "active":
                raise RuntimeError("The Slack subscription is paused; resume it before starting")
            return
    # A test receiver must not replace another receiver or another channel.
    subscriptions.create(
        agent_identity_id=identity_id, url=url, event_types=events,
    )


def inbound_message(envelope: dict, identity_id: str) -> tuple[str, str, dict] | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or data.get("identity_id") != identity_id:
        return None
    event = data.get("event")
    if not isinstance(event, dict) or not envelope.get("id"):
        return None
    fields = ("connection_id", "workspace_id", "conversation_id", "actor_id", "message_ts")
    if any(not isinstance(data.get(key), str) or not data[key] for key in fields):
        return None
    # Only human messages wake the agent; bot echoes must not form reply loops.
    if event.get("bot_id") or event.get("app_id") or event.get("subtype") == "bot_message":
        return None
    # The selected event type may represent only one of a message's categories.
    kinds = data.get("message_kinds") or []
    if not isinstance(kinds, list):
        return None
    thread_ts = data.get("thread_ts")
    if thread_ts is not None and not isinstance(thread_ts, str):
        return None
    direct = "dm" in kinds
    root = thread_ts or (None if direct else data["message_ts"])
    # Timestamps are opaque strings: converting them to floats loses precision.
    chat_id = "slack:" + ":".join(
        [identity_id, data["connection_id"], data["conversation_id"], root or "dm"]
    )
    raw = event.get("text") or ""
    if not isinstance(raw, str):
        return None
    body = raw
    files = event.get("files")
    if isinstance(files, list) and files:
        body += "\nAttachment references (not downloaded): " + json.dumps([
            {key: file[key] for key in ("id", "name", "mimetype", "size") if key in file}
            for file in files if isinstance(file, dict)
        ])
    if not body.strip():
        return None
    meta = {
        **{key: data[key] for key in fields},
        "thread_ts": root,
        "sender": f"{data['workspace_id']}:{data['actor_id']}",
        "conversation_kind": "direct" if direct else "group",
        "raw_text": raw,
        "source_event_id": envelope["id"],
        "slack_mentioned": "mention" in kinds,
        "slack_addressed": bool(set(kinds) & {"dm", "group_dm", "mention"}),
    }
    # Sender context never changes workspace/thread isolation or approval ownership.
    sender_context = {}
    if isinstance(data.get("contact_id"), str):
        sender_context["contact_id"] = data["contact_id"]
    actor = data.get("actor_profile")
    if isinstance(actor, dict) and actor.get("id") == data["actor_id"]:
        profile = actor.get("profile")
        profile = profile if isinstance(profile, dict) else {}
        sender_context.update({
            key: profile[key] for key in ("display_name", "real_name", "email", "phone", "title")
            if isinstance(profile.get(key), str) and profile[key]
        })
    if sender_context:
        meta["slack_sender_context"] = sender_context
    return chat_id, body, meta


def send_reply(client: Any, meta: dict, text: str) -> Any:
    coordinates = [meta["connection_id"], meta["conversation_id"], meta.get("thread_ts")]
    key = hashlib.sha256(json.dumps(
        [meta["source_event_id"], coordinates, text], separators=(",", ":")
    ).encode()).hexdigest()
    action = slack_resource(client).send_message(
        meta["connection_id"], conversation_id=meta["conversation_id"],
        thread_ts=meta.get("thread_ts"), text=text, idempotency_key=f"codex:{key}",
    )
    if action.status != "sent":
        raise RuntimeError(
            f"Slack action {action.id} has status {action.status}; inspect it with "
            "inkbox_slack_get_action before deciding whether to send again"
        )
    return action


def _tool(name: str, description: str, properties: dict, required: list[str]) -> dict:
    return {
        "name": "inkbox_slack_" + name, "description": description,
        "inputSchema": {"type": "object", "properties": properties,
                        "required": required, "additionalProperties": False},
    }


_STRING = {"type": "string", "minLength": 1}
_CONNECTION = {"connection_id": _STRING}
_CONVERSATION = {**_CONNECTION, "conversation_id": _STRING}
_PAGE = {"cursor": _STRING, "limit": {"type": "integer", "minimum": 1, "maximum": 100}}
SLACK_TOOLS = [
    _tool("list_connections", "List this identity's Slack workspace connections.", {}, []),
    _tool("list_conversations", "List accessible Slack conversations; follow next_cursor.",
          {**_CONNECTION, **_PAGE}, ["connection_id"]),
    _tool("list_messages", "Read a Slack conversation or thread; follow next_cursor.",
          {**_CONVERSATION, **_PAGE, "thread_ts": _STRING}, ["connection_id", "conversation_id"]),
    _tool("search", "Search retained Slack text across connected workspaces. Not complete workspace "
          "history or file contents. Follow next_cursor even on an empty page.",
          {**_CONVERSATION, **_PAGE, "q": _STRING}, ["q"]),
    _tool("send_message", "Send a Slack message only when explicitly requested. Ordinary replies "
          "are automatic. Reuse idempotency_key for retries of the exact same message. "
          "For sending/unknown outcomes inspect get_action; never blindly resend.",
          {**_CONVERSATION, "text": {"type": "string", "minLength": 1, "maxLength": 40000},
           "thread_ts": _STRING, "idempotency_key": {
               "type": "string", "pattern": "^[A-Za-z0-9._:-]{1,128}$"}},
          ["connection_id", "conversation_id", "text", "idempotency_key"]),
    _tool("get_action", "Inspect a Slack send outcome by action ID; unknown is not proof of failure.",
          {**_CONNECTION, "action_id": _STRING}, ["connection_id", "action_id"]),
]


def run_tool(client: Any, identity_handle: str, name: str, args: dict) -> Any:
    spec = next((tool for tool in SLACK_TOOLS if tool["name"] == name), None)
    if spec is None:
        raise ValueError(f"Unknown Slack tool: {name}")
    schema = spec["inputSchema"]
    if set(args) - set(schema["properties"]) or any(key not in args for key in schema["required"]):
        raise ValueError("Invalid Slack tool arguments")
    for key, value in args.items():
        if key == "limit":
            if type(value) is not int or not 1 <= value <= 100:
                raise ValueError("limit must be an integer from 1 to 100")
        elif not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} must be a nonempty string")
    resource = slack_resource(client)
    identity = client.get_identity(identity_handle)
    if name == "inkbox_slack_list_connections":
        return resource.list_connections(identity.id)
    # Even an organization-scoped credential must stay on the configured identity.
    connection = args.get("connection_id")
    if connection is not None:
        owned = resource.list_connections(identity.id).connections
        if not any(str(item.id) == connection for item in owned):
            raise ValueError("Slack connection does not belong to this identity")
    if name == "inkbox_slack_search":
        return resource.search_messages(identity_id=identity.id, **args)
    if name == "inkbox_slack_send_message":
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", args["idempotency_key"]):
            raise ValueError("Invalid Slack idempotency_key")
        if len(args["text"]) > 40000:
            raise ValueError("Slack text must not exceed 40000 characters")
    return getattr(resource, name.removeprefix("inkbox_slack_"))(**args)
