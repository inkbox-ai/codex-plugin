"""Shared Inkbox Codex bridge configuration helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

from .realtime import (
    DEFAULT_MODEL as REALTIME_DEFAULT_MODEL,
    DEFAULT_VOICE as REALTIME_DEFAULT_VOICE,
    RealtimeConfig,
)

INKBOX_BASE_URL_DEFAULT = "https://inkbox.ai"
INKBOX_WS_PATH = "/phone/media/ws"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8767
DEFAULT_WEBHOOK_PATH = "/webhook"


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
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    # Codex side
    project_dir: str = ""
    codex_model: str = ""
    codex_bin: str = "codex"
    codex_sandbox: str = "workspace-write"
    codex_approval_policy: str = "on-request"
    permission_timeout_s: float = 600.0
    # OpenAI Realtime voice (off unless the wizard validated a key)
    realtime: RealtimeConfig = field(default_factory=RealtimeConfig)


def _read_realtime_config() -> RealtimeConfig:
    """Build the Realtime voice config from the env.

    The API key falls back to OPENAI_API_KEY so an operator who already
    exports one doesn't have to re-enter it. Realtime stays disabled unless
    INKBOX_REALTIME_ENABLED is truthy.

    Returns:
        RealtimeConfig: Resolved settings; ``enabled`` False leaves calls on
        the Inkbox STT/TTS path.
    """
    api_key = str(os.getenv("INKBOX_REALTIME_API_KEY") or os.getenv("OPENAI_API_KEY") or "").strip()
    return RealtimeConfig(
        enabled=env_flag("INKBOX_REALTIME_ENABLED", False) and bool(api_key),
        api_key=api_key,
        model=str(os.getenv("INKBOX_REALTIME_MODEL") or REALTIME_DEFAULT_MODEL).strip(),
        voice=str(os.getenv("INKBOX_REALTIME_VOICE") or REALTIME_DEFAULT_VOICE).strip(),
        fallback_to_inkbox_stt_tts=env_flag("INKBOX_REALTIME_FALLBACK_TO_INKBOX_STT_TTS", True),
    )


def read_config(extra: Dict[str, Any] | None = None) -> BridgeConfig:
    extra = extra or {}
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
        permission_timeout_s=float(os.getenv("INKBOX_PERMISSION_TIMEOUT_S") or 600.0),
        realtime=_read_realtime_config(),
    )
