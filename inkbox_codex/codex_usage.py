"""Fetch Codex rate-limit and token usage through ``codex app-server``."""

from __future__ import annotations

import json
import select
import shutil
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Optional


def _request_account_usage(codex_bin: str = "codex", timeout: float = 10.0) -> dict:
    """Call app-server account usage endpoints and return their raw payloads."""
    if not shutil.which(codex_bin):
        raise RuntimeError("codex CLI is not on PATH")

    proc = subprocess.Popen(
        [codex_bin, "app-server"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None and proc.stdout is not None
    try:
        messages = [
            {
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "inkbox_codex_usage",
                        "title": "Inkbox Codex Usage",
                        "version": "0.1.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
            },
            {"method": "initialized", "params": {}},
            {"id": 2, "method": "account/rateLimits/read", "params": {}},
            {"id": 3, "method": "account/usage/read", "params": {}},
        ]
        for message in messages:
            proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
        proc.stdin.flush()

        results: dict[int, Any] = {}
        deadline = time.monotonic() + timeout
        while 2 not in results or 3 not in results:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for Codex usage")
            ready, _, _ = select.select([proc.stdout], [], [], remaining)
            if not ready:
                raise TimeoutError("timed out waiting for Codex usage")
            raw = proc.stdout.readline()
            if not raw:
                raise RuntimeError("codex app-server exited before returning usage")
            message = json.loads(raw)
            message_id = message.get("id")
            if message_id in (1, 2, 3):
                if message.get("error"):
                    error = message["error"]
                    raise RuntimeError(str(error.get("message") or error))
                if message_id in (2, 3):
                    results[int(message_id)] = message.get("result") or {}
        return {"rate_limits": results[2], "usage": results[3]}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # App-server schemas use Unix milliseconds for rate-limit resets.
        if value > 10_000_000_000:
            value = value / 1000
        return datetime.fromtimestamp(value, tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _fmt_reset(value: Any, *, now: Optional[datetime] = None) -> str:
    dt = _parse_dt(value)
    if dt is None:
        return ""
    now = now or datetime.now(timezone.utc)
    seconds = (dt - now).total_seconds()
    if seconds <= 0:
        return "now"
    hours, minutes = int(seconds // 3600), int((seconds % 3600) // 60)
    if hours >= 24:
        return f"in {hours // 24}d {hours % 24}h"
    if hours >= 1:
        return f"in {hours}h {minutes}m"
    return f"in {minutes}m"


def _window_label(name: str, window: dict) -> str:
    mins = window.get("windowDurationMins")
    if isinstance(mins, int) and mins:
        if mins % (60 * 24) == 0:
            return f"{name} ({mins // (60 * 24)}d)"
        if mins % 60 == 0:
            return f"{name} ({mins // 60}h)"
        return f"{name} ({mins}m)"
    return name


def _format_rate_limit(snapshot: dict, *, now: Optional[datetime] = None) -> list[str]:
    lines: list[str] = []
    name = str(snapshot.get("limitName") or snapshot.get("limitId") or "Codex")
    for key, fallback in (("primary", "Primary"), ("secondary", "Secondary")):
        window = snapshot.get(key)
        if not isinstance(window, dict) or window.get("usedPercent") is None:
            continue
        reset = _fmt_reset(window.get("resetsAt"), now=now)
        label = _window_label(name if key == "primary" else fallback, window)
        lines.append(
            f"{label}: {int(window['usedPercent'])}% used"
            + (f", resets {reset}" if reset else "")
        )
    credits = snapshot.get("credits")
    if isinstance(credits, dict) and not credits.get("unlimited") and credits.get("balance"):
        lines.append(f"Credits: {credits['balance']}")
    return lines


def _format_tokens(usage: dict) -> list[str]:
    summary = usage.get("summary") if isinstance(usage, dict) else None
    if not isinstance(summary, dict):
        return []
    lines = []
    if summary.get("lifetimeTokens") is not None:
        lines.append(f"Lifetime tokens: {int(summary['lifetimeTokens']):,}")
    if summary.get("currentStreakDays") is not None:
        lines.append(f"Current streak: {int(summary['currentStreakDays'])}d")
    return lines


def format_usage(data: dict, *, now: Optional[datetime] = None) -> str:
    """Format app-server rate-limit and token usage data for a text reply."""
    lines: list[str] = []
    rate_limits = data.get("rate_limits") or {}
    by_id = rate_limits.get("rateLimitsByLimitId")
    if isinstance(by_id, dict) and by_id:
        for snapshot in by_id.values():
            if isinstance(snapshot, dict):
                lines.extend(_format_rate_limit(snapshot, now=now))
    elif isinstance(rate_limits.get("rateLimits"), dict):
        lines.extend(_format_rate_limit(rate_limits["rateLimits"], now=now))
    lines.extend(_format_tokens(data.get("usage") or {}))
    return "\n".join(lines) if lines else "No Codex usage windows reported."


def fetch_usage() -> dict:
    return _request_account_usage()


def usage_report() -> str:
    """Fetch and format Codex usage, returning a text-ready summary."""
    try:
        data = fetch_usage()
    except Exception as exc:
        return f"Couldn't fetch Codex usage: {exc}"
    return "Codex usage:\n" + format_usage(data)
