"""Readiness checks for the bridge (`inkbox-codex doctor`)."""

from __future__ import annotations

import os
import shutil
import socket
from typing import List, Tuple

try:
    from .config import VoiceStack, inkbox_client_kwargs, read_config
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import VoiceStack, inkbox_client_kwargs, read_config


def _webhook_port_available(host: str, port: int) -> bool:
    """Probe whether ``host:port`` can be bound right now.

    Used to catch a port collision (e.g. a second Inkbox bridge such as
    claude-code-plugin defaulting to the same port) before the gateway
    itself fails to start.

    Returns:
        bool: True if the port is free to bind.
    """
    bind_host = host or "0.0.0.0"
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind((bind_host, port))
        return True
    except OSError:
        return False


def run_doctor() -> List[Tuple[str, bool, str]]:
    """Run every readiness check.

    Returns:
        List[Tuple[str, bool, str]]: (check name, passed, detail) rows.
    """
    # Read the same .env the bridge does before judging anything missing —
    # otherwise doctor reports "missing" for credentials that are on disk and
    # working, purely because this process did not inherit them.
    try:
        from .daemon import _maybe_load_env_file, running_pid
    except ImportError:  # pragma: no cover - direct local import/test fallback
        from daemon import _maybe_load_env_file, running_pid
    _maybe_load_env_file()

    cfg = read_config()
    checks: List[Tuple[str, bool, str]] = []

    checks.append(("INKBOX_API_KEY", bool(cfg.api_key), "set" if cfg.api_key else "missing"))
    if running_pid() is not None:
        # This bridge's own background gateway already holds the port —
        # that's expected, not a collision.
        checks.append(("webhook port", True, f"{cfg.host}:{cfg.port} (bridge already running)"))
    else:
        port_free = _webhook_port_available(cfg.host, cfg.port)
        checks.append((
            "webhook port",
            port_free,
            f"{cfg.host}:{cfg.port} free" if port_free else (
                f"{cfg.host}:{cfg.port} already in use — another process (e.g. a "
                "second Inkbox bridge such as claude-code-plugin) may be running "
                "there; set INKBOX_BRIDGE_PORT to a free port"
            ),
        ))
    checks.append(("INKBOX_IDENTITY", bool(cfg.identity), cfg.identity or "missing"))
    checks.append((
        "INKBOX_SIGNING_KEY",
        bool(cfg.signing_key) or not cfg.require_signature,
        "set" if cfg.signing_key else "missing (required for signed inbound webhooks)",
    ))
    checks.append((
        "phone voice stack",
        not bool(cfg.voice_stack_invalid_value),
        cfg.voice_stack.value if not cfg.voice_stack_invalid_value else f"invalid: {cfg.voice_stack_invalid_value}",
    ))
    if cfg.voice_stack is VoiceStack.OPENAI_REALTIME:
        checks.append((
            "OpenAI Realtime API key",
            cfg.realtime.enabled and bool(cfg.realtime.api_key),
            "set" if cfg.realtime.api_key else "missing (required by openai_realtime)",
        ))
    checks.append((
        "voicemail detection",
        cfg.voicemail_detection in {"enabled", "disabled"},
        cfg.voicemail_detection,
    ))
    if cfg.voice_stack is VoiceStack.INKBOX_VOICE_AI:
        checks.append((
            "Voice AI authority config",
            cfg.voice_ai_authority_mode in {"contact_scoped", "yolo"},
            cfg.voice_ai_authority_mode,
        ))

    try:
        import inkbox  # noqa: F401
        checks.append(("inkbox SDK", True, "installed"))
    except ImportError:
        checks.append(("inkbox SDK", False, "pip install 'inkbox>=0.5.9,<1.0.0'"))

    try:
        import aiohttp  # noqa: F401
        checks.append(("aiohttp", True, "installed"))
    except ImportError:
        checks.append(("aiohttp", False, "pip install 'aiohttp>=3.9'"))

    codex_bin = shutil.which("codex")
    checks.append((
        "codex CLI",
        bool(codex_bin),
        codex_bin or "not on PATH — install Codex first",
    ))

    codex_home = os.getenv("CODEX_HOME") or os.path.join(os.path.expanduser("~"), ".codex")
    has_api_key = bool(os.getenv("OPENAI_API_KEY") or os.getenv("CODEX_API_KEY") or os.getenv("CODEX_ACCESS_TOKEN"))
    has_login = os.path.exists(os.path.join(codex_home, "auth.json"))
    checks.append((
        "Codex auth",
        has_api_key or has_login,
        "API key/token set" if has_api_key else "subscription login found" if has_login else "run codex login or set OPENAI_API_KEY/CODEX_API_KEY",
    ))

    project_dir = cfg.project_dir
    checks.append((
        "project dir",
        bool(project_dir) and os.path.isdir(project_dir),
        project_dir or "unset (defaults to cwd)",
    ))

    if cfg.api_key and cfg.identity:
        try:
            from inkbox import Inkbox

            identity = Inkbox(**inkbox_client_kwargs(cfg.api_key, cfg.base_url)).get_identity(cfg.identity)
        except Exception as exc:
            checks.append(("identity reachable", False, str(exc)))
        else:
            mailbox = getattr(identity, "mailbox", None)
            phone = getattr(identity, "phone_number", None)
            detail = ", ".join(filter(None, [
                getattr(mailbox, "email_address", None),
                getattr(phone, "number", None),
                "imessage" if getattr(identity, "imessage_enabled", False) else None,
            ])) or "no channels provisioned"
            checks.append(("identity reachable", True, detail))
            if getattr(identity, "phone_number", None) is not None or getattr(identity, "imessage_enabled", False):
                expected_action = (
                    "hosted_agent"
                    if cfg.voice_stack is VoiceStack.INKBOX_VOICE_AI
                    else "auto_accept"
                )
                try:
                    incoming = identity.get_incoming_call_action()
                    actual_action = str(getattr(getattr(incoming, "incoming_call_action", ""), "value", getattr(incoming, "incoming_call_action", "")))
                except Exception as exc:
                    checks.append(("incoming call action", False, str(exc)))
                else:
                    checks.append((
                        "incoming call action",
                        actual_action == expected_action,
                        f"{actual_action or 'unset'} (expected {expected_action})",
                    ))
                if cfg.voice_stack is VoiceStack.INKBOX_VOICE_AI:
                    try:
                        hosted = identity.get_hosted_agent_config()
                        actual_authority = str(getattr(getattr(hosted, "authority_mode", ""), "value", getattr(hosted, "authority_mode", "")))
                    except Exception as exc:
                        checks.append(("Voice AI authority", False, str(exc)))
                    else:
                        checks.append((
                            "Voice AI authority",
                            actual_authority == cfg.voice_ai_authority_mode,
                            f"{actual_authority or 'unset'} (expected {cfg.voice_ai_authority_mode})",
                        ))

    return checks


def print_doctor() -> int:
    """Print check results.

    Returns:
        int: Process exit code — 0 when everything passed, 1 otherwise.
    """
    rows = run_doctor()
    failed = 0
    for name, ok, detail in rows:
        mark = "✓" if ok else "✗"
        print(f" {mark} {name:<20} {detail}")
        failed += 0 if ok else 1
    return 0 if failed == 0 else 1
