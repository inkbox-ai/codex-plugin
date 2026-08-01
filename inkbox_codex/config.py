"""Shared Inkbox Codex bridge configuration helpers."""

from __future__ import annotations

import importlib.metadata
import hashlib
import os
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

from .realtime import (
    DEFAULT_MODEL as REALTIME_DEFAULT_MODEL,
    DEFAULT_VOICE as REALTIME_DEFAULT_VOICE,
    RealtimeConfig,
)

# Empty means "do not override"; the Inkbox SDK owns its API default.
INKBOX_BASE_URL_DEFAULT = ""
INKBOX_WS_PATH = "/phone/media/ws"

USER_AGENT_NAME = "inkbox-codex"
DISTRIBUTION_NAME = "codex-plugin"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8767
DEFAULT_WEBHOOK_PATH = "/webhook"


class VoiceStack(str, Enum):
    """Supported phone-call voice stacks."""

    INKBOX_VOICE_AI = "inkbox_voice_ai"
    OPENAI_REALTIME = "openai_realtime"
    INKBOX_TTS_STT = "inkbox_tts_stt"


def call_contexts_dir() -> Path:
    """Directory where ``inkbox_place_call`` stashes per-call context.

    The tool process writes purpose/opening details here and the gateway reads
    them when Inkbox connects back to the call-media WebSocket.

    Returns:
        Path: The created ``<home>/call_contexts`` directory.
    """
    root = Path(os.getenv("INKBOX_CODEX_HOME") or (Path.home() / ".inkbox-codex"))
    path = root / "call_contexts"
    path.mkdir(parents=True, exist_ok=True)
    return path


def channel_hints_path() -> Path:
    """File where the gateway records each session's last inbound channel.

    The gateway writes ``{chat_id: {"mode": ..., "at": ...}}`` on every inbound
    turn; the tool process reads it so an outbound call can follow the
    conversation's current channel.

    Returns:
        Path: ``<home>/channel_hints.json`` (parent directory created).
    """
    root = Path(os.getenv("INKBOX_CODEX_HOME") or (Path.home() / ".inkbox-codex"))
    root.mkdir(parents=True, exist_ok=True)
    return root / "channel_hints.json"


def a2a_turn_context_path(chat_id: str) -> Path:
    """Return the trusted cross-process context file for one Codex session."""
    root = Path(os.getenv("INKBOX_CODEX_HOME") or (Path.home() / ".inkbox-codex"))
    path = root / "a2a_turn_contexts"
    path.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(chat_id.encode()).hexdigest()
    return path / f"{digest}.json"


def hosted_sms_turn_context_path(chat_id: str) -> Path:
    """Return the private cross-process context for one hosted SMS turn."""
    root = Path(os.getenv("INKBOX_CODEX_HOME") or (Path.home() / ".inkbox-codex"))
    path = root / "hosted_sms_turn_contexts"
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)
    digest = hashlib.sha256(chat_id.encode()).hexdigest()
    return path / f"{digest}.json"


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _csv_env(name: str) -> List[str]:
    raw = os.getenv(name) or ""
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass
class BridgeConfig:
    api_key: str = ""
    identity: str = ""
    signing_key: str = ""
    base_url: str = INKBOX_BASE_URL_DEFAULT
    public_url: str = ""
    tunnel_name: str = ""
    home_channel: str = ""
    allowed_users: List[str] = field(default_factory=list)
    allow_all_users: bool = False
    require_signature: bool = True
    # Wake the agent on unrecognised (external) webhooks. Off by default;
    # registered third-party providers bypass it once their secret is set.
    external_events_enabled: bool = False
    contact_memories_enabled: bool = True
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    # Codex side
    project_dir: str = ""
    codex_model: str = ""
    codex_bin: str = "codex"
    codex_sandbox: str = "workspace-write"
    codex_approval_policy: str = "on-request"
    auto_approve_inkbox_tools: bool = False
    permission_timeout_s: float = 600.0
    codex_turn_timeout_s: float = 1800.0
    codex_interrupt_timeout_s: float = 10.0
    voice_stack: VoiceStack = VoiceStack.INKBOX_TTS_STT
    voice_stack_invalid_value: str = ""
    voice_ai_authority_mode: str = "contact_scoped"
    voicemail_detection: str = "enabled"
    # OpenAI Realtime voice (off unless the wizard validated a key)
    realtime: RealtimeConfig = field(default_factory=RealtimeConfig)


def inkbox_base_url_kwargs(base_url: str | None = None) -> Dict[str, str]:
    normalized = str(base_url or "").strip()
    return {"base_url": normalized} if normalized else {}


@lru_cache(maxsize=1)
def plugin_user_agent() -> str:
    """Identifies this plugin ahead of the SDK's own ``User-Agent`` token."""
    try:
        version = importlib.metadata.version(DISTRIBUTION_NAME)
    except importlib.metadata.PackageNotFoundError:
        version = "unknown"
    return f"{USER_AGENT_NAME}/{version}"


def inkbox_client_kwargs(api_key: str, base_url: str | None = None) -> Dict[str, str]:
    return {
        "api_key": api_key,
        "user_agent_prefix": plugin_user_agent(),
        **inkbox_base_url_kwargs(base_url),
    }


def resolve_voice_stack(
    value: Any,
    *,
    realtime_enabled: Any = None,
    realtime_api_key: str = "",
) -> tuple[VoiceStack, str]:
    """Resolve the canonical stack while preserving legacy Realtime behavior."""
    normalized = str(value or "").strip().lower()
    if normalized:
        try:
            return VoiceStack(normalized), ""
        except ValueError:
            return VoiceStack.INKBOX_TTS_STT, normalized

    enabled = str(realtime_enabled or "").strip().lower() in {
        "auto", "1", "true", "yes", "on",
    }
    if enabled and realtime_api_key:
        return VoiceStack.OPENAI_REALTIME, ""
    return VoiceStack.INKBOX_TTS_STT, ""


def _read_realtime_config(voice_stack: VoiceStack) -> RealtimeConfig:
    """Build the Realtime voice config from the env.

    The API key falls back to OPENAI_API_KEY so an operator who already
    exports one doesn't have to re-enter it. The resolved canonical voice
    stack decides whether Realtime is enabled; legacy enablement is folded
    into that resolution before this function runs.

    Returns:
        RealtimeConfig: Resolved settings; ``enabled`` False leaves calls on
        the Inkbox STT/TTS path.
    """
    api_key = str(os.getenv("INKBOX_REALTIME_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    return RealtimeConfig(
        enabled=voice_stack is VoiceStack.OPENAI_REALTIME and bool(api_key),
        api_key=api_key,
        model=str(os.getenv("INKBOX_REALTIME_MODEL") or REALTIME_DEFAULT_MODEL).strip(),
        voice=str(os.getenv("INKBOX_REALTIME_VOICE") or REALTIME_DEFAULT_VOICE).strip(),
        fallback_to_inkbox_stt_tts=env_flag("INKBOX_REALTIME_FALLBACK_TO_INKBOX_STT_TTS", True),
    )


def read_config(extra: Dict[str, Any] | None = None) -> BridgeConfig:
    extra = extra or {}
    realtime_api_key = str(
        os.getenv("INKBOX_REALTIME_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
    ).strip()
    realtime_enabled = os.getenv("INKBOX_REALTIME_ENABLED")
    voice_stack, invalid_voice_stack = resolve_voice_stack(
        extra.get("voice_stack") or os.getenv("INKBOX_VOICE_STACK"),
        realtime_enabled=realtime_enabled,
        realtime_api_key=realtime_api_key,
    )
    return BridgeConfig(
        api_key=str(extra.get("api_key") or os.getenv("INKBOX_API_KEY") or "").strip(),
        identity=str(extra.get("identity") or os.getenv("INKBOX_IDENTITY") or "").strip(),
        signing_key=str(extra.get("signing_key") or os.getenv("INKBOX_SIGNING_KEY") or "").strip(),
        base_url=str(extra.get("base_url") or os.getenv("INKBOX_BASE_URL") or INKBOX_BASE_URL_DEFAULT).strip(),
        public_url=str(extra.get("public_url") or os.getenv("INKBOX_PUBLIC_URL") or "").strip(),
        tunnel_name=str(extra.get("tunnel_name") or os.getenv("INKBOX_TUNNEL_NAME") or "").strip(),
        home_channel=str(os.getenv("INKBOX_HOME_CHANNEL") or extra.get("home_channel") or "").strip(),
        allowed_users=_csv_env("INKBOX_ALLOWED_USERS"),
        allow_all_users=env_flag("INKBOX_ALLOW_ALL_USERS", False),
        require_signature=env_flag("INKBOX_REQUIRE_SIGNATURE", True),
        external_events_enabled=env_flag("INKBOX_EXTERNAL_EVENTS_ENABLED", False),
        contact_memories_enabled=env_flag("INKBOX_CONTACT_MEMORIES_ENABLED", True),
        host=str(os.getenv("INKBOX_BRIDGE_HOST") or DEFAULT_HOST).strip(),
        port=int(os.getenv("INKBOX_BRIDGE_PORT") or DEFAULT_PORT),
        project_dir=str(
            os.getenv("CODEX_PROJECT_DIR")
            or os.getenv("CLAUDE_PROJECT_DIR")
            or extra.get("project_dir")
            or os.getcwd()
        ).strip(),
        codex_model=str(os.getenv("CODEX_MODEL") or os.getenv("CLAUDE_MODEL") or extra.get("codex_model") or "").strip(),
        codex_bin=str(os.getenv("CODEX_BIN") or extra.get("codex_bin") or "codex").strip(),
        codex_sandbox=str(os.getenv("CODEX_SANDBOX") or extra.get("codex_sandbox") or "workspace-write").strip(),
        codex_approval_policy=str(
            os.getenv("CODEX_APPROVAL_POLICY")
            or extra.get("codex_approval_policy")
            or "on-request"
        ).strip(),
        auto_approve_inkbox_tools=env_flag("INKBOX_CODEX_AUTO_APPROVE_INKBOX_TOOLS", False),
        permission_timeout_s=float(os.getenv("INKBOX_PERMISSION_TIMEOUT_S") or 600.0),
        codex_turn_timeout_s=float(os.getenv("CODEX_TURN_TIMEOUT_S") or 1800.0),
        codex_interrupt_timeout_s=float(os.getenv("CODEX_INTERRUPT_TIMEOUT_S") or 10.0),
        voice_stack=voice_stack,
        voice_stack_invalid_value=invalid_voice_stack,
        voice_ai_authority_mode=str(
            extra.get("voice_ai_authority_mode")
            or os.getenv("INKBOX_VOICE_AI_AUTHORITY_MODE")
            or "contact_scoped"
        ).strip().lower(),
        voicemail_detection=str(
            extra.get("voicemail_detection")
            or os.getenv("INKBOX_VOICEMAIL_DETECTION")
            or "enabled"
        ).strip().lower(),
        realtime=_read_realtime_config(voice_stack),
    )
