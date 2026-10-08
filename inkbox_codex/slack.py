"""Slack channel routing and the small SDK-backed tool surface."""

from __future__ import annotations

import hashlib
import base64
import os
import stat
from pathlib import Path
import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

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
SLACK_STOP_EVENT = "slack.session_stopped"
SLACK_SUBSCRIPTION_EVENTS = (*SLACK_INCOMING_EVENTS, SLACK_STOP_EVENT)
SLACK_MAX_TEXT_LENGTH = 12000
SLACK_MAX_UPLOAD_BYTES = 10 * 1024 * 1024


def file_payload(file_path, *, local_root=None, filename=None, title=None, initial_comment=None):
    """Read a bounded regular local file; URLs and special files are not uploads."""
    path = Path(file_path).expanduser()
    if not path.is_absolute():
        path = Path(local_root or Path.cwd()) / path
    filename = path.name if filename is None else filename
    if (not isinstance(filename, str) or not 1 <= len(filename) <= 255 or filename in {".", ".."}
            or any(ord(c) < 32 or ord(c) == 127 or c in "/\\" for c in filename)):
        raise ValueError("Provide a plain filename without paths or control characters")
    for key, value, maximum in (("title", title, 255), ("initial_comment", initial_comment, SLACK_MAX_TEXT_LENGTH)):
        if value is not None and (not isinstance(value, str) or len(value) > maximum or "\x00" in value):
            raise ValueError(f"Invalid Slack {key}")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0))
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= SLACK_MAX_UPLOAD_BYTES:
            raise ValueError("Slack file must be a regular file containing 1 byte–10 MiB")
        content = stream.read(SLACK_MAX_UPLOAD_BYTES + 1)
    if not 1 <= len(content) <= SLACK_MAX_UPLOAD_BYTES:
        raise ValueError("Slack file must contain 1 byte–10 MiB")
    return {"filename": filename, "content_base64": base64.b64encode(content).decode("ascii"),
            **({"title": title} if title is not None else {}),
            **({"initial_comment": initial_comment} if initial_comment is not None else {})}


def operation_summary(operation):
    """Expose inspection coordinates and outcomes without file bytes or local paths."""
    return {key: str(value) if key in {"id", "connection_id"} else value
            for key in ("id", "connection_id", "operation", "status", "conversation_id", "thread_ts",
                        "message_ts", "file_id", "error_code", "retry_after")
            if (value := getattr(operation, key, None)) is not None}




def slack_resource(client: Any) -> Any:
    resource = getattr(client, "slack", None)
    if resource is None:
        raise RuntimeError("Slack requires an Inkbox SDK with Slack support")
    return resource


def reconcile_subscription(client: Any, identity_id: Any, url: str) -> None:
    slack_resource(client)
    subscriptions = client.webhooks.subscriptions
    events = list(SLACK_SUBSCRIPTION_EVENTS)
    covered = set()
    message_subscriptions = []
    for sub in subscriptions.list(agent_identity_id=identity_id, scope="identity", url=url):
        overlap = set(sub.event_types) & set(events)
        if sub.url == url and overlap:
            if sub.status != "active":
                raise RuntimeError("The Slack subscription is paused; resume it before starting")
            covered.update(overlap)
            if overlap.intersection(SLACK_INCOMING_EVENTS):
                message_subscriptions.append(sub)
    missing = [event for event in events if event not in covered]
    if not missing:
        return
    if message_subscriptions:
        # Extend the existing receiver: a second channel-only subscription
        # would deliver mentions again under another event category.
        sub = max(message_subscriptions, key=lambda item: len(set(item.event_types).intersection(SLACK_INCOMING_EVENTS)))
        subscriptions.update(sub.id, event_types=list(dict.fromkeys([*sub.event_types, *missing])), scope="identity")
        return
    # A test receiver must not replace another receiver or another channel.
    subscriptions.create(
        agent_identity_id=identity_id, url=url, event_types=missing,
    )


def inbound_message(envelope: dict, identity_id: str) -> tuple[str, str, dict] | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or data.get("identity_id") != identity_id:
        return None
    # Older deliveries omit the label. An explicit denial/unknown label must
    # never become an ordinary request, and ride-alongs need their scoped grant.
    if "sender_access" in data and (
        data["sender_access"] not in ("direct", "sponsored")
        or (data["sender_access"] == "sponsored" and not envelope.get("companion"))
    ):
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
    # Keep ordinary DMs in the main conversation; native mentions can start a thread.
    root = thread_ts or (None if direct and "mention" not in kinds else data["message_ts"])
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
        "identity_id": identity_id,
        "thread_ts": root,
        "sender": f"{data['workspace_id']}:{data['actor_id']}",
        "conversation_kind": "direct" if direct else "group",
        "raw_text": raw,
        "source_event_id": envelope["id"],
        "slack_mentioned": "mention" in kinds,
        "slack_addressed": bool(set(kinds) & {"dm", "group_dm", "mention"}),
    }
    if "sender_access" in data:
        meta["sender_access"] = data["sender_access"]
    # Sender context never changes workspace/thread isolation or approval ownership.
    sender_context = {}
    if isinstance(data.get("contact_id"), str):
        sender_context["contact_id"] = data["contact_id"]
    actor = data.get("actor_profile")
    if isinstance(actor, dict) and actor.get("id") == data["actor_id"]:
        if isinstance(actor.get("team_id"), str) and actor["team_id"]:
            meta["recipient_team_id"] = actor["team_id"]
        profile = actor.get("profile")
        profile = profile if isinstance(profile, dict) else {}
        sender_context.update({
            key: profile[key] for key in ("display_name", "real_name", "email", "phone", "title")
            if isinstance(profile.get(key), str) and profile[key]
        })
    if sender_context:
        meta["slack_sender_context"] = sender_context
    return chat_id, body, meta


def inbound_stop(envelope: dict, identity_id: str) -> tuple[str, str, dict] | None:
    data = envelope.get("data")
    if not isinstance(data, dict) or envelope.get("event_type") != SLACK_STOP_EVENT:
        return None
    event = data.get("event")
    if not isinstance(event, dict) or event.get("type") != "agent_session_stopped":
        return None
    thread = data.get("thread_ts")
    if not isinstance(thread, str) or not thread:
        return None
    # Native controls carry null access, not a contact-rule admission label.
    # Their authority comes from the active request's exact actor and route.
    control = {key: value for key, value in data.items() if key != "sender_access"}
    incoming = inbound_message({**envelope, "data": {
        **control, "message_ts": thread, "message_kinds": ["thread"],
        "event": {"type": "message", "text": "/stop"},
    }}, identity_id)
    if incoming is not None:
        incoming[2]["slack_native_stop"] = True
    return incoming


def validate_connection(resource: Any, identity_id: str, meta: dict) -> str | None:
    """Fail closed on stale, ambiguous, disconnected or cross-identity routes."""
    def field(item, key):
        return item.get(key) if isinstance(item, dict) else getattr(item, key, None)
    connections = field(resource.list_connections(identity_id), "connections")
    if not isinstance(connections, list):
        raise PermissionError("Slack connection ownership could not be verified")
    matches = [item for item in connections if str(field(item, "id")) == meta.get("connection_id")]
    if (len(matches) != 1 or str(field(matches[0], "identity_id")) != identity_id
            or not meta.get("workspace_id") or field(matches[0], "workspace_id") != meta["workspace_id"]
            or field(matches[0], "status") != "connected"):
        raise PermissionError("Slack connection is not uniquely active for this identity and workspace")
    bot = field(matches[0], "bot_user_id")
    return bot if isinstance(bot, str) and bot else None


def send_reply(client: Any, meta: dict, text: str) -> Any:
    if not text or len(text) > SLACK_MAX_TEXT_LENGTH or "\x00" in text:
        raise ValueError("Slack text must be 1–12000 characters without NUL characters")
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
          {**_CONVERSATION, "text": {"type": "string", "minLength": 1, "maxLength": SLACK_MAX_TEXT_LENGTH},
           "thread_ts": _STRING, "idempotency_key": {
               "type": "string", "pattern": "^[A-Za-z0-9._:-]{1,128}$"}},
          ["connection_id", "conversation_id", "text", "idempotency_key"]),
    _tool("upload_file", "Upload a local file to the original active Slack request. Copy source_event_id and destination from the inbound metadata. Reuse the same idempotency_key for identical content. Unknown outcomes must not be resent with a new key.",
          {**_CONVERSATION, "thread_ts": _STRING, "source_event_id": _STRING, "file_path": _STRING,
           "filename": _STRING, "title": _STRING, "initial_comment": _STRING,
           "idempotency_key": {"type": "string", "pattern": "^[A-Za-z0-9._:-]{1,128}$"}},
          ["connection_id", "conversation_id", "source_event_id", "file_path", "idempotency_key"]),
    _tool("get_operation", "Inspect a Slack attachment operation. Supply exactly one operation_id or returned idempotency_key; by-key inspection requires an SDK exposing get_operation_by_key. Unknown is not proof of failure.",
          {**_CONNECTION, "operation_id": _STRING, "idempotency_key": _STRING}, ["connection_id"]),
    _tool("get_action", "Inspect a Slack send outcome by action ID; unknown is not proof of failure.",
          {**_CONNECTION, "action_id": _STRING}, ["connection_id", "action_id"]),
]


def run_tool(client: Any, identity_handle: str, name: str, args: dict, *, upload_context=None) -> Any:
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
        elif "\x00" in value:
            raise ValueError(f"{key} must not contain NUL characters")
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
    if name == "inkbox_slack_upload_file":
        if upload_context is None:
            raise ValueError("A source-bound active Slack request is required for uploads")
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", args["idempotency_key"]):
            raise ValueError("Invalid Slack idempotency_key")
        payload = file_payload(args["file_path"], local_root=upload_context["project_dir"],
            **{key: args[key] for key in ("filename", "title", "initial_comment") if key in args})
        from .slack_turns import upload
        return upload(client, upload_context, args, payload)
    if name == "inkbox_slack_get_operation":
        operation_id, key = args.get("operation_id"), args.get("idempotency_key")
        if bool(operation_id) == bool(key):
            raise ValueError("Supply exactly one operation_id or idempotency_key")
        if key:
            if not callable(getattr(type(resource), "get_operation_by_key", None)):
                raise ValueError("This SDK cannot inspect an operation without its ID; upgrade to an SDK with get_operation_by_key. Keep the receipt and do not resend the upload.")
            result = operation_summary(resource.get_operation_by_key(connection, idempotency_key=key))
        else:
            result = operation_summary(resource.get_operation(connection, operation_id))
        from .config import read_config
        from .slack_turns import reconcile
        try:
            reconcile(identity_handle, read_config(), connection, result, operation_id=operation_id, idempotency_key=key)
        except OSError:
            logger.warning("Slack attachment inspection succeeded but its local receipt was not saved")
        return result
    if name == "inkbox_slack_search":
        return resource.search_messages(identity_id=identity.id, **args)
    if name == "inkbox_slack_send_message":
        if not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", args["idempotency_key"]):
            raise ValueError("Invalid Slack idempotency_key")
        if len(args["text"]) > SLACK_MAX_TEXT_LENGTH:
            raise ValueError("Slack text must not exceed 12000 characters")
    return getattr(resource, name.removeprefix("inkbox_slack_"))(**args)
