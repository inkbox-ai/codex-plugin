from inkbox_codex import gateway


def test_codex_health_reports_api_key(monkeypatch):
    monkeypatch.setattr(gateway.shutil, "which", lambda name: "/usr/bin/codex")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    assert "API key" in gateway._codex_health()


def test_codex_health_reports_subscription(tmp_path, monkeypatch):
    monkeypatch.setattr(gateway.shutil, "which", lambda name: "/usr/bin/codex")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    home = tmp_path
    (home / ".codex").mkdir()
    (home / ".codex" / "auth.json").write_text("{}")
    monkeypatch.setattr(gateway.Path, "home", classmethod(lambda cls: home))
    assert "subscription" in gateway._codex_health()


def test_codex_health_reports_missing_auth(tmp_path, monkeypatch):
    monkeypatch.setattr(gateway.shutil, "which", lambda name: "/usr/bin/codex")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(gateway.Path, "home", classmethod(lambda cls: tmp_path))
    assert "NOT authenticated" in gateway._codex_health()


def test_codex_health_reports_missing_cli(monkeypatch):
    monkeypatch.setattr(gateway.shutil, "which", lambda name: None)
    assert "CLI missing" in gateway._codex_health()
