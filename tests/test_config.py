from inkbox_codex.config import VoiceStack, read_config


def test_read_config_defaults(monkeypatch):
    for var in (
        "INKBOX_API_KEY", "INKBOX_IDENTITY", "INKBOX_ALLOW_ALL_USERS",
        "INKBOX_ALLOWED_USERS", "CODEX_BIN", "CODEX_SANDBOX",
        "CODEX_APPROVAL_POLICY", "INKBOX_CODEX_AUTO_APPROVE_INKBOX_TOOLS",
        "INKBOX_BASE_URL", "CODEX_TURN_TIMEOUT_S", "CODEX_INTERRUPT_TIMEOUT_S",
        "INKBOX_CONTACT_MEMORIES_ENABLED",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = read_config()
    assert cfg.base_url == ""
    assert cfg.require_signature is True
    assert cfg.codex_bin == "codex"
    assert cfg.codex_sandbox == "workspace-write"
    assert cfg.codex_approval_policy == "on-request"
    assert cfg.auto_approve_inkbox_tools is False
    assert cfg.codex_turn_timeout_s == 1800.0
    assert cfg.codex_interrupt_timeout_s == 10.0
    assert cfg.contact_memories_enabled is True


def test_read_config_env(monkeypatch):
    monkeypatch.setenv("INKBOX_API_KEY", "ApiKey_test")
    monkeypatch.setenv("INKBOX_IDENTITY", "code-agent")
    monkeypatch.setenv("INKBOX_BASE_URL", "https://proxy.example")
    monkeypatch.setenv("INKBOX_ALLOWED_USERS", "+15551234567, me@example.com")
    monkeypatch.setenv("CODEX_BIN", "/opt/codex")
    monkeypatch.setenv("CODEX_SANDBOX", "read-only")
    monkeypatch.setenv("CODEX_APPROVAL_POLICY", "never")
    monkeypatch.setenv("INKBOX_CODEX_AUTO_APPROVE_INKBOX_TOOLS", "true")
    monkeypatch.setenv("CODEX_TURN_TIMEOUT_S", "42")
    monkeypatch.setenv("CODEX_INTERRUPT_TIMEOUT_S", "3")
    cfg = read_config()
    assert cfg.api_key == "ApiKey_test"
    assert cfg.base_url == "https://proxy.example"
    assert cfg.allowed_users == ["+15551234567", "me@example.com"]
    assert cfg.codex_bin == "/opt/codex"
    assert cfg.codex_sandbox == "read-only"
    assert cfg.codex_approval_policy == "never"
    assert cfg.auto_approve_inkbox_tools is True
    assert cfg.codex_turn_timeout_s == 42.0
    assert cfg.codex_interrupt_timeout_s == 3.0


def test_contact_memories_can_be_disabled(monkeypatch):
    monkeypatch.setenv("INKBOX_CONTACT_MEMORIES_ENABLED", "false")
    assert read_config().contact_memories_enabled is False


def _clear_realtime_env(monkeypatch):
    for var in (
        "INKBOX_REALTIME_ENABLED", "INKBOX_REALTIME_API_KEY", "OPENAI_API_KEY",
        "INKBOX_REALTIME_MODEL", "INKBOX_REALTIME_VOICE",
        "INKBOX_REALTIME_FALLBACK_TO_INKBOX_STT_TTS",
    ):
        monkeypatch.delenv(var, raising=False)


def test_realtime_disabled_by_default(monkeypatch):
    _clear_realtime_env(monkeypatch)
    assert read_config().realtime.enabled is False


def test_openai_key_alone_does_not_enable_legacy_realtime(monkeypatch):
    _clear_realtime_env(monkeypatch)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
    cfg = read_config()
    assert cfg.voice_stack is VoiceStack.INKBOX_TTS_STT
    assert cfg.realtime.enabled is False


def test_realtime_needs_both_flag_and_key(monkeypatch):
    # Flag on but no key → still disabled (gateway would have nothing to dial).
    _clear_realtime_env(monkeypatch)
    monkeypatch.setenv("INKBOX_REALTIME_ENABLED", "true")
    assert read_config().realtime.enabled is False

    # Flag on + key → enabled.
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-rt")
    cfg = read_config()
    assert cfg.realtime.enabled is True
    assert cfg.realtime.api_key == "sk-rt"


def test_realtime_key_falls_back_to_openai_env(monkeypatch):
    _clear_realtime_env(monkeypatch)
    monkeypatch.setenv("INKBOX_REALTIME_ENABLED", "1")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-openai")
    cfg = read_config()
    assert cfg.realtime.enabled is True
    assert cfg.realtime.api_key == "sk-openai"


def test_explicit_voice_stack_is_canonical(monkeypatch):
    monkeypatch.setenv("INKBOX_VOICE_STACK", "inkbox_voice_ai")
    monkeypatch.setenv("INKBOX_REALTIME_ENABLED", "true")
    monkeypatch.setenv("INKBOX_REALTIME_API_KEY", "sk-realtime")
    monkeypatch.setenv("INKBOX_VOICE_AI_AUTHORITY_MODE", "yolo")
    monkeypatch.setenv("INKBOX_VOICEMAIL_DETECTION", "disabled")

    cfg = read_config()

    assert cfg.voice_stack is VoiceStack.INKBOX_VOICE_AI
    assert cfg.realtime.enabled is False
    assert cfg.voice_ai_authority_mode == "yolo"
    assert cfg.voicemail_detection == "disabled"


def test_invalid_voice_stack_fails_closed_to_tts(monkeypatch):
    monkeypatch.setenv("INKBOX_VOICE_STACK", "made_up")
    cfg = read_config()
    assert cfg.voice_stack is VoiceStack.INKBOX_TTS_STT
    assert cfg.voice_stack_invalid_value == "made_up"
