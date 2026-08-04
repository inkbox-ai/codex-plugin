"""Non-interactive bootstrap for an existing Inkbox Codex identity."""

from __future__ import annotations

import time
from typing import Any

from . import daemon
from .config import INKBOX_BASE_URL_DEFAULT, VoiceStack, inkbox_client_kwargs
from .setup_wizard import _enum_value, _env, _load_inkbox_symbols, _save


def _handle(value: str) -> str:
    return value.strip().removeprefix("@").strip()


def _identity_for_key(client: Any, expected: str) -> Any:
    handles = {_handle(str(getattr(item, "agent_handle", ""))) for item in client.list_identities()}
    if expected not in handles:
        raise ValueError("The API key is not scoped to the requested identity.")
    return client.get_identity(expected)


def _resolve_credentials(api_key: str, expected: str, base_url: str, symbols: dict[str, Any], actions: list[str]) -> tuple[str, Any]:
    Inkbox = symbols["Inkbox"]
    client = Inkbox(**inkbox_client_kwargs(api_key, base_url))
    info = client.whoami()
    if _enum_value(getattr(info, "auth_type", "")) != "api_key":
        raise ValueError("Bootstrap requires an Inkbox API key.")
    subtype = _enum_value(getattr(info, "auth_subtype", ""))
    claimed = _enum_value(symbols["AGENT_CLAIMED"])
    if subtype == claimed:
        return api_key, _identity_for_key(client, expected)
    if subtype == _enum_value(symbols["AGENT_UNCLAIMED"]):
        raise ValueError("The API key is not attached to a claimed identity yet.")
    if subtype != _enum_value(symbols["ADMIN_SCOPED"]):
        raise ValueError("Use an agent-scoped or admin-scoped Inkbox API key.")
    saved_key = _env("INKBOX_API_KEY").strip()
    if saved_key and _handle(_env("INKBOX_IDENTITY")) == expected:
        try:
            saved_client = Inkbox(**inkbox_client_kwargs(saved_key, base_url))
            if _enum_value(getattr(saved_client.whoami(), "auth_subtype", "")) == claimed:
                actions.append("reused_saved_agent_key")
                return saved_key, _identity_for_key(saved_client, expected)
        except Exception:
            pass
    identity = client.get_identity(expected)
    created = client.api_keys.create(
        label=f"Codex gateway - {expected}",
        description="Agent-scoped key created by the Codex Inkbox bootstrap.",
        scoped_identity_id=identity.id,
    )
    scoped_key = str(getattr(created, "api_key", "") or "")
    if not scoped_key:
        raise RuntimeError("Inkbox did not return the new agent-scoped API key.")
    actions.append("minted_agent_scoped_key")
    scoped_client = Inkbox(**inkbox_client_kwargs(scoped_key, base_url))
    return scoped_key, scoped_client.get_identity(expected)


def _voice_instructions(identity: Any, client: Any) -> str:
    handle = _handle(str(getattr(identity, "agent_handle", "")))
    mailbox = getattr(identity, "mailbox", None)
    channels = []
    email = getattr(identity, "email_address", None) or getattr(mailbox, "email_address", None)
    phone = getattr(getattr(identity, "phone_number", None), "number", None)
    tunnel = getattr(getattr(identity, "tunnel", None), "public_host", None)
    dedicated = getattr(getattr(identity, "imessage_number", None), "number", None)
    if email:
        channels.append(f"Email: {email}.")
    if phone:
        channels.append(f"VoIP phone: {phone}.")
    if tunnel:
        channels.append(f"Public address: https://{tunnel}.")
    if dedicated:
        channels.append(f"Dedicated iMessage line: {dedicated}.")
    elif bool(getattr(identity, "imessage_enabled", False)):
        try:
            triage = client.imessages.get_triage_number()
            number = str(getattr(triage, "number", "") or "")
            command = str(getattr(triage, "connect_command", "") or f"connect @{handle}")
            if number:
                channels.append(f"Shared iMessage: text '{command}' to {number}.")
        except Exception:
            channels.append("Shared iMessage is enabled; use the current Inkbox connection instructions.")
    configured = " ".join(channels) or "No direct communication channel is currently configured."
    return f"You are the hosted voice interface for Inkbox agent @{handle}. Help callers connect using only these configured channels. {configured}"


def _configure_voice(identity: Any, client: Any, instructions: str | None) -> None:
    hosted = identity.get_hosted_agent_config()
    desired = instructions if instructions is not None else (getattr(hosted, "instructions", None) or _voice_instructions(identity, client))
    if len(desired) > 8000:
        raise ValueError("Voice AI instructions must be 8,000 characters or fewer.")
    if getattr(hosted, "instructions", None) != desired:
        identity.set_hosted_agent_config(voice=getattr(hosted, "voice", None), model=getattr(hosted, "model", None), instructions=desired)
    incoming = identity.get_incoming_call_action()
    if _enum_value(getattr(incoming, "incoming_call_action", "")) != "hosted_agent" or getattr(incoming, "client_websocket_url", None) is not None or getattr(incoming, "incoming_call_webhook_url", None) is not None:
        identity.set_incoming_call_action(incoming_call_action="hosted_agent", client_websocket_url=None, incoming_call_webhook_url=None)
    _save("INKBOX_VOICE_STACK", VoiceStack.INKBOX_VOICE_AI.value)
    _save("INKBOX_VOICE_AI_AUTHORITY_MODE", _enum_value(getattr(hosted, "authority_mode", "contact_scoped")))
    _save("INKBOX_REALTIME_ENABLED", "false")


def _configure_signing(identity: Any, client: Any, rotate: bool, same_identity: bool, actions: list[str]) -> str | None:
    status_reader = getattr(identity, "get_signing_key_status", None) or getattr(client, "get_signing_key_status")
    configured = bool(getattr(status_reader(), "configured", False))
    if _env("INKBOX_SIGNING_KEY").strip() and same_identity and configured and not rotate:
        _save("INKBOX_REQUIRE_SIGNATURE", "true")
        actions.append("reused_local_signing_key")
        return None
    if configured and not rotate:
        return "A signing key already exists but is unavailable in this Codex profile. Set INKBOX_SIGNING_KEY or rerun with --rotate-signing-key."
    creator = getattr(identity, "create_signing_key", None) or getattr(client, "create_signing_key")
    created = creator()
    key = str(getattr(created, "signing_key", "") or "")
    if not key:
        raise RuntimeError("Inkbox did not return the new signing key.")
    _save("INKBOX_SIGNING_KEY", key)
    _save("INKBOX_REQUIRE_SIGNATURE", "true")
    actions.append("rotated_signing_key" if configured else "created_signing_key")
    return None


def _start_gateway(actions: list[str]) -> bool:
    was_running = daemon.running_pid() is not None
    code = daemon.restart() if was_running else daemon.start()
    actions.append("restarted_gateway" if was_running else "started_gateway_process")
    if code != 0:
        return False
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if daemon.running_pid():
            return True
        time.sleep(0.25)
    return False


def bootstrap(*, identity_handle: str, api_key: str, base_url: str = INKBOX_BASE_URL_DEFAULT, project_dir: str = "", voice_ai: bool = False, voice_ai_instructions: str | None = None, rotate_signing_key: bool = False, start_gateway: bool = False) -> dict[str, Any]:
    handle = _handle(identity_handle)
    if not handle:
        return {"status": "error", "error": "identity is required"}
    if not api_key.strip():
        return {"status": "error", "error": "API key is required"}
    actions: list[str] = []
    secrets = [api_key.strip()]
    try:
        previous = _handle(_env("INKBOX_IDENTITY"))
        symbols = _load_inkbox_symbols()
        scoped_key, identity = _resolve_credentials(api_key.strip(), handle, base_url, symbols, actions)
        secrets.append(scoped_key)
        client = symbols["Inkbox"](**inkbox_client_kwargs(scoped_key, base_url))
        _save("INKBOX_API_KEY", scoped_key)
        _save("INKBOX_IDENTITY", handle)
        if base_url:
            _save("INKBOX_BASE_URL", base_url)
        if project_dir:
            _save("CODEX_PROJECT_DIR", project_dir)
        _save("INKBOX_ALLOW_ALL_USERS", "true")
        _save("INKBOX_CODEX_AUTO_APPROVE_INKBOX_TOOLS", "true")
        actions.append("saved_codex_configuration")
        if voice_ai:
            _configure_voice(identity, client, voice_ai_instructions)
            actions.append("configured_voice_ai")
        blocker = _configure_signing(identity, client, rotate_signing_key, not previous or previous == handle, actions)
        if blocker:
            return {"status": "requires_human", "identity": handle, "actions": actions, "human_actions": [blocker]}
        running = False
        if start_gateway:
            running = _start_gateway(actions)
            if not running:
                return {"status": "error", "identity": handle, "actions": actions, "error": "Codex gateway did not become ready. Check ~/.inkbox-codex/gateway.log."}
        return {"status": "configured", "identity": handle, "actions": actions, "gateway_running": running}
    except Exception as exc:
        message = str(exc)
        for secret in secrets:
            if secret:
                message = message.replace(secret, "[redacted]")
        return {"status": "error", "identity": handle, "actions": actions, "error": message}
