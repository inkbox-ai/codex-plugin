"""Bounded observation of retained native conversations without executing turns."""

import asyncio
import json

from .managed_chat import safe_text
from .managed_conversations import page
from .schedules import ScheduleStore, schedule_path


def bounded(events):
    result, size = [], 0
    for event in events:
        length = len(json.dumps(event).encode())
        if result and (size + length > 40000 or len(result) >= 32):
            break
        result.append(event)
        size += length
    return result


async def observe(gateway, command):
    if set(command) != {"action", "after", "source_id", "before"}:
        raise ValueError("Invalid observation request")
    manager = gateway.sessions
    sources = {
        f"contact:{native}": (key, native)
        for key, native in manager._session_ids.items()
        if not key.startswith("chat:")
        and key in manager.sessions
        and getattr(manager.sessions[key], "_client", None) is not None
        and manager.sessions[key]._client.is_alive
    }
    with ScheduleStore(schedule_path(gateway.cfg)) as store:
        for job in store.list_jobs(include_deleted=True):
            sources[f"schedule:{job['id']}"] = (job["name"], job["id"])
        source = command["source_id"]
        if source is None:
            source = next(
                (
                    key
                    for key in sorted(sources)
                    if command["after"] is None or key > command["after"]
                ),
                None,
            )
        if source is None:
            return {"source_id": None, "complete": True}
        if command["source_id"] is None:
            return {"source_id": source, "selected": True}
        if source not in sources:
            return {"source_id": source, "missing": True}
        label, native = sources[source]
        checkpoint = command["before"] if isinstance(command["before"], dict) else {}
        position = checkpoint.get(
            "position",
            command["before"] if isinstance(command["before"], str) else None,
        )
        if source.startswith("schedule:"):
            rows = store.db.execute(
                "SELECT id,prompt,result,status,rowid FROM runs WHERE job_id=? AND (? IS NULL OR rowid>?) ORDER BY rowid LIMIT 16",
                (native, position, position),
            ).fetchall()
            events = []
            for row in rows:
                events.extend(
                    [
                        {
                            "source_id": row["id"] + ":user",
                            "type": "message.completed",
                            "data": {
                                "id": row["id"] + ":user",
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": safe_text(
                                            str(row["prompt"] or "")[:4096]
                                        ),
                                    }
                                ],
                            },
                        },
                        {
                            "source_id": row["id"] + ":assistant",
                            "type": "message.completed",
                            "data": {
                                "id": row["id"] + ":assistant",
                                "role": "assistant",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": safe_text(
                                            str(row["result"] or "")[:4096]
                                        ),
                                    }
                                ],
                            },
                        },
                    ]
                )
            selected = bounded(events)
            taken = len(selected) // 2
            if len(selected) % 2:
                selected.pop()
            return {
                "source_id": source,
                "origin": "schedule",
                "title": safe_text(label[:128]),
                "events": selected,
                "next_before": str(rows[taken - 1]["rowid"])
                if taken and (taken < len(rows) or taken == 16)
                else None,
                "checkpoint": {
                    "position": str(rows[max(0, taken - 2)]["rowid"] - 1)
                    if taken
                    else position
                },
                "history_completeness": "partial",
            }
    client = manager.sessions[label]._client
    version = f"{client.observation_id}:{getattr(client, '_observation_version', 0)}"
    if checkpoint.get("version") == version:
        return {"source_id": source, "unchanged": True, "checkpoint": checkpoint}
    result = await asyncio.wait_for(
        client._request("thread/read", {"threadId": native, "includeTurns": True}), 5
    )
    thread = result.get("thread", {})
    if thread.get("id") != native:
        raise ValueError("Conversation unavailable")
    try:
        history = page(thread, position, forward=True)
    except ValueError:
        history = page(thread, None, forward=True)
    events = [
        {
            "source_id": message["id"],
            "type": "message.completed",
            "data": {
                "id": message["id"],
                "role": message["role"],
                "content": [{"type": "text", "text": safe_text(message["text"])}],
            },
        }
        for message in history["messages"]
    ]
    selected = {message["id"] for message in history["messages"]}
    budget = len(json.dumps(events).encode())
    for turn in thread.get("turns", []):
        if not any(str(item.get("id")) in selected for item in turn.get("items", [])):
            continue
        for item in turn.get("items", []):
            kind = item.get("type")
            titles = {
                "commandExecution": "Run command",
                "fileChange": "Change files",
                "mcpToolCall": "Call tool",
                "webSearch": "Search",
                "collabAgentToolCall": "Agent task",
            }
            if kind not in titles or not item.get("id"):
                continue
            state = (
                "failed"
                if item.get("error") or item.get("status") == "failed"
                else "completed"
                if turn.get("status") == "completed"
                or item.get("status") == "completed"
                else "running"
            )
            event = {
                "source_id": item["id"],
                "type": "tool.updated",
                "data": {"id": item["id"], "title": titles[kind], "status": state},
            }
            budget += len(json.dumps(event).encode())
            if budget > 50000 or len(events) >= 64:
                break
            events.append(event)
    return {
        "source_id": source,
        "origin": "communication",
        "title": safe_text(label[:128]),
        "events": events,
        "next_before": history["next_before"],
        "checkpoint": {
            "position": history["messages"][-2]["id"]
            if len(history["messages"]) > 1
            else position,
            "version": version,
        },
        "history_completeness": "partial",
    }
