"""Doctor keeps identity reachability separate from remote voice config."""

from __future__ import annotations

import sys
import types

from inkbox_codex import daemon, doctor
from inkbox_codex.config import BridgeConfig, RealtimeConfig, VoiceStack


def test_voice_config_probe_failures_do_not_mark_identity_unreachable(
    monkeypatch, tmp_path,
):
    identity = types.SimpleNamespace(
        mailbox=types.SimpleNamespace(email_address="agent@example.com"),
        phone_number=types.SimpleNamespace(number="+15551234567"),
        imessage_enabled=False,
        get_incoming_call_action=lambda: (_ for _ in ()).throw(
            RuntimeError("incoming config unavailable")
        ),
        get_hosted_agent_config=lambda: (_ for _ in ()).throw(
            RuntimeError("authority config unavailable")
        ),
    )
    client = types.SimpleNamespace(get_identity=lambda _handle: identity)
    inkbox_module = types.ModuleType("inkbox")
    inkbox_module.Inkbox = lambda **_kwargs: client
    monkeypatch.setitem(sys.modules, "inkbox", inkbox_module)
    monkeypatch.setattr(daemon, "_maybe_load_env_file", lambda: None)
    monkeypatch.setattr(doctor.shutil, "which", lambda _name: "/usr/bin/codex")
    monkeypatch.setattr(
        doctor,
        "read_config",
        lambda: BridgeConfig(
            api_key="ApiKey_agent",
            identity="agent",
            signing_key="whsec_test",
            project_dir=str(tmp_path),
            voice_stack=VoiceStack.INKBOX_VOICE_AI,
            voice_ai_authority_mode="yolo",
        ),
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-only")

    checks = doctor.run_doctor()
    by_name = {name: (ok, detail) for name, ok, detail in checks}
    reachable = [row for row in checks if row[0] == "identity reachable"]

    assert reachable == [("identity reachable", True, "agent@example.com, +15551234567")]
    assert by_name["incoming call action"] == (False, "incoming config unavailable")
    assert by_name["Voice AI authority"] == (False, "authority config unavailable")


def test_realtime_stack_reports_missing_api_key(monkeypatch, tmp_path):
    monkeypatch.setattr(daemon, "_maybe_load_env_file", lambda: None)
    monkeypatch.setattr(doctor.shutil, "which", lambda _name: "/usr/bin/codex")
    monkeypatch.setattr(
        doctor,
        "read_config",
        lambda: BridgeConfig(
            identity="agent",
            signing_key="whsec_test",
            project_dir=str(tmp_path),
            voice_stack=VoiceStack.OPENAI_REALTIME,
            realtime=RealtimeConfig(enabled=False, api_key=""),
        ),
    )
    monkeypatch.setenv("OPENAI_API_KEY", "test-only")

    by_name = {
        name: (ok, detail)
        for name, ok, detail in doctor.run_doctor()
    }

    assert by_name["OpenAI Realtime API key"] == (
        False,
        "missing (required by openai_realtime)",
    )
