from inkbox_codex.config import read_config


def test_read_config_defaults(monkeypatch):
    for var in (
        "INKBOX_API_KEY", "INKBOX_IDENTITY", "INKBOX_ALLOW_ALL_USERS",
        "INKBOX_ALLOWED_USERS", "CODEX_BIN", "CODEX_SANDBOX",
        "CODEX_APPROVAL_POLICY",
    ):
        monkeypatch.delenv(var, raising=False)
    cfg = read_config()
    assert cfg.base_url == "https://inkbox.ai"
    assert cfg.require_signature is True
    assert cfg.codex_bin == "codex"
    assert cfg.codex_sandbox == "workspace-write"
    assert cfg.codex_approval_policy == "on-request"


def test_read_config_env(monkeypatch):
    monkeypatch.setenv("INKBOX_API_KEY", "ApiKey_test")
    monkeypatch.setenv("INKBOX_IDENTITY", "code-agent")
    monkeypatch.setenv("INKBOX_ALLOWED_USERS", "+15551234567, me@example.com")
    monkeypatch.setenv("CODEX_BIN", "/opt/codex")
    monkeypatch.setenv("CODEX_SANDBOX", "read-only")
    monkeypatch.setenv("CODEX_APPROVAL_POLICY", "never")
    cfg = read_config()
    assert cfg.api_key == "ApiKey_test"
    assert cfg.allowed_users == ["+15551234567", "me@example.com"]
    assert cfg.codex_bin == "/opt/codex"
    assert cfg.codex_sandbox == "read-only"
    assert cfg.codex_approval_policy == "never"


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
