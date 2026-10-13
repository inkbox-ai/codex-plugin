"""Local scheduled-job tools shared by the CLI and MCP interface."""

from __future__ import annotations

import os
from typing import Any

from .config import read_config
from .schedules import ScheduleStore, schedule_path


def _schema(properties: dict, required: tuple[str, ...] = ()) -> dict:
    return {"type": "object", "properties": properties, "required": list(required), "additionalProperties": False}


SCHEDULE_SCHEMA = {
    "oneOf": [
        _schema({"kind": {"const": "once", "type": "string"}, "at": {"type": "string", "description": "ISO 8601 timestamp with UTC offset."}}, ("kind", "at")),
        _schema({"kind": {"const": "interval", "type": "string"}, "seconds": {"type": "integer", "minimum": 60}}, ("kind", "seconds")),
        _schema({"kind": {"const": "cron", "type": "string"}, "expression": {"type": "string", "description": "Five-field cron expression."}, "timezone": {"type": "string", "description": "IANA timezone; defaults to UTC."}}, ("kind", "expression")),
    ],
}
_FIELDS = {
    "name": {"type": "string", "minLength": 1, "maxLength": 200},
    "prompt": {"type": "string", "minLength": 1, "maxLength": 64000, "description": "Complete task instructions, including explicit channel and recipient for any requested delivery. Each job has its own session."},
    "schedule": SCHEDULE_SCHEMA,
    "catch_up": {"type": "boolean", "description": "Coalesce missed executions after downtime into one run. Default false; skipped runs remain in history."},
    "enabled": {"type": "boolean", "description": "Whether future scheduled executions are enabled. Default true."},
}
_ID = {"job_id": {"type": "string", "minLength": 1}}
SCHEDULE_TOOLS = [
    {"name": "inkbox_schedule_add", "description": "Save a persistent local scheduled job with its own prompt and session. Runs while the gateway is online; no messages are sent automatically. Only claim success after this tool returns the saved job.", "inputSchema": _schema(_FIELDS, ("name", "prompt", "schedule"))},
    {"name": "inkbox_schedule_list", "description": "List scheduled jobs belonging to this conversation, including next due times; use history to inspect missed executions.", "inputSchema": _schema({})},
    {"name": "inkbox_schedule_update", "description": "Edit an existing job; enabled=false pauses it and catch_up controls automatic catch-up after downtime. Omitted fields are unchanged.", "inputSchema": _schema({**_ID, **_FIELDS}, ("job_id",))},
    {"name": "inkbox_schedule_delete", "description": "Delete a scheduled job and cancel future executions. Does not undo work already started.", "inputSchema": _schema(_ID, ("job_id",))},
    {"name": "inkbox_schedule_run_now", "description": "Queue one immediate execution of a job. This does not mean execution completed. The gateway must be running and scheduling enabled.", "inputSchema": _schema(_ID, ("job_id",))},
    {"name": "inkbox_schedule_history", "description": "Read execution history, missed counts, results and errors for this conversation's jobs. Times are Unix seconds.", "inputSchema": _schema({**_ID, "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50}})},
    {"name": "inkbox_schedule_catch_up", "description": "Queue one catch-up run covering tracked missed executions for a job, even when automatic catch-up is off. Does not replay every missed tick.", "inputSchema": _schema(_ID, ("job_id",))},
]
SCHEDULE_TOOL_NAMES = frozenset(tool["name"] for tool in SCHEDULE_TOOLS)
_READ_ONLY = {"inkbox_schedule_list", "inkbox_schedule_history"}


def schedule_owner() -> str | None:
    """Use the bridge-assigned conversation, never a caller-supplied owner."""
    return (os.getenv("INKBOX_CODEX_CHAT_ID") or "").strip() or None


def check_schedule_mutation() -> None:
    if os.getenv("INKBOX_SCHEDULE_JOB_ID"):
        raise ValueError("Scheduled jobs cannot create or change schedules; use the original conversation or local CLI.")


def dispatch_schedule_tool(name: str, args: dict[str, Any], *, identity: str = "") -> Any:
    """Execute a local tool; validation errors are rendered by the caller."""
    if name not in SCHEDULE_TOOL_NAMES:
        raise ValueError("Unknown scheduled-job tool")
    spec = next(tool for tool in SCHEDULE_TOOLS if tool["name"] == name)
    schema = spec["inputSchema"]
    if not isinstance(args, dict) or set(args) - set(schema["properties"]):
        raise ValueError("Unexpected scheduled-job arguments")
    if any(key not in args for key in schema["required"]):
        raise ValueError("Missing required scheduled-job arguments")
    if "job_id" in args and (not isinstance(args["job_id"], str) or not args["job_id"].strip()):
        raise ValueError("job_id must be a nonempty string")
    if "limit" in args and (isinstance(args["limit"], bool) or not isinstance(args["limit"], int) or not 1 <= args["limit"] <= 200):
        raise ValueError("limit must be an integer between 1 and 200")
    if name not in _READ_ONLY:
        check_schedule_mutation()
    cfg = read_config({"identity": identity} if identity else None)
    if not cfg.identity:
        raise ValueError("Set INKBOX_IDENTITY before managing scheduled jobs.")
    owner = schedule_owner()
    with ScheduleStore(schedule_path(cfg)) as store:
        current_job = (os.getenv("INKBOX_SCHEDULE_JOB_ID") or "").strip()
        if current_job:
            if name == "inkbox_schedule_list":
                return [store.get(current_job)]
            if args.get("job_id", current_job) != current_job:
                raise ValueError("Scheduled jobs can inspect only their own execution history")
            return store.runs(job_id=current_job, limit=args.get("limit", 50))
        if name == "inkbox_schedule_add":
            return store.create(owner=owner or "", **args)
        if name == "inkbox_schedule_list":
            return store.list_jobs(owner=owner)
        if name == "inkbox_schedule_update":
            changes = {key: value for key, value in args.items() if key != "job_id"}
            if not changes:
                raise ValueError("Provide at least one field to update")
            return store.update(args["job_id"], owner=owner, **changes)
        if name == "inkbox_schedule_delete":
            store.delete(args["job_id"], owner=owner)
            return {"deleted": True, "job_id": args["job_id"]}
        if name == "inkbox_schedule_run_now":
            return store.enqueue(args["job_id"], owner=owner)
        if name == "inkbox_schedule_catch_up":
            return store.catch_up(args["job_id"], owner=owner)
        return store.runs(owner=owner, **args)


def schedule_status(cfg: Any, *, now: float | None = None, stale_after_s: float = 120) -> dict[str, Any]:
    """Read aggregate readiness without creating or changing scheduler state.

    Suitable for unauthenticated readiness responses: neither job identifiers,
    prompts, results, process identifiers nor raw stored error text are returned.
    """
    import sqlite3
    import time

    status: dict[str, Any] = {
        "jobs": 0,
        "enabled_jobs": 0,
        "enabled": True,
        "missed_pending": 0,
        "uncertain": 0,
        "orphan_hosts": 0,
        "heartbeat_age_s": None,
        "heartbeat_stale": False,
        "needs_gateway": False,
        "healthy": True,
        "attention_required": False,
        "last_error": None,
    }
    if not cfg.identity:
        return status
    db = None
    try:
        path = schedule_path(cfg)
        if not path.exists():
            return status
        db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1, isolation_level=None)
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        setting = db.execute("SELECT enabled,heartbeat,last_error FROM settings WHERE id=1").fetchone()
        if setting is None:
            raise sqlite3.DatabaseError("Missing scheduler settings")
        status["enabled"] = bool(setting[0])
        status["jobs"], status["enabled_jobs"] = db.execute(
            "SELECT COUNT(*),COALESCE(SUM(enabled),0) FROM jobs WHERE deleted=0"
        ).fetchone()
        status["missed_pending"] = db.execute(
            "SELECT COALESCE(SUM(r.count),0) FROM runs r JOIN jobs j ON j.id=r.job_id "
            "WHERE j.deleted=0 AND r.status='missed' AND r.source!='paused' AND r.catchup_run_id IS NULL"
        ).fetchone()[0]
        status["uncertain"] = db.execute("SELECT COUNT(*) FROM runs WHERE status='uncertain'").fetchone()[0]
        if setting[1] is not None:
            status["heartbeat_age_s"] = max(0.0, (time.time() if now is None else now) - setting[1])
        status["heartbeat_stale"] = bool(
            status["enabled"] and status["enabled_jobs"] and
            (status["heartbeat_age_s"] is None or status["heartbeat_age_s"] > stale_after_s)
        )
        status["orphan_hosts"] = db.execute(
            "SELECT COUNT(*) FROM runs WHERE host_pid IS NOT NULL "
            "AND (status NOT IN ('starting','running') OR ?)",
            (status["heartbeat_stale"],),
        ).fetchone()[0]
        status["needs_gateway"] = status["heartbeat_stale"]
        if setting[2]:
            status["last_error"] = "Scheduler reported an error; inspect local scheduler status."
        status["healthy"] = not (status["needs_gateway"] or status["orphan_hosts"] or (
            status["enabled"] and status["enabled_jobs"] and status["last_error"]
        ))
        status["attention_required"] = bool(status["uncertain"] or status["orphan_hosts"] or not status["healthy"])
    except (OSError, sqlite3.Error, ValueError, TypeError):
        status.update(healthy=False, attention_required=True, last_error="Could not read scheduled-job state.")
    finally:
        if db is not None:
            db.close()
    return status


def schedule_status_detail(status: dict[str, Any]) -> str:
    """Render the safe aggregate status for doctor or a text health response."""
    parts = [f"{status['jobs']} jobs ({status['enabled_jobs']} enabled)",
             "scheduler enabled" if status["enabled"] else "scheduler intentionally paused",
             f"{status['missed_pending']} missed occurrences awaiting catch-up"]
    if status["needs_gateway"]:
        parts.append("heartbeat missing or stale; start or inspect the gateway")
    elif status["heartbeat_age_s"] is not None:
        parts.append(f"heartbeat {int(status['heartbeat_age_s'])}s ago")
    if status["uncertain"]:
        parts.append(f"warning: {status['uncertain']} uncertain executions; inspect before recovery")
    if status["orphan_hosts"]:
        parts.append(f"{status['orphan_hosts']} execution processes awaiting cleanup")
    if status["last_error"]:
        parts.append(status["last_error"])
    return "; ".join(parts)
