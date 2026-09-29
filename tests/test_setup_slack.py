"""Slack onboarding, preparation, installation, and credential boundaries."""

from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from inkbox_codex import setup_wizard as wizard


def snapshot(status="ready", connections=()):
    return NS(setup=NS(status=status), connections=list(connections), installation_available=True)


def connection(id="workspace-1", status="connected"):
    return NS(id=id, status=status, workspace_name="Example workspace")


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setenv("INKBOX_CODEX_ENV_FILE", str(tmp_path / ".env"))
    monkeypatch.delenv("INKBOX_SLACK_ENABLED", raising=False)
    monkeypatch.setattr(wizard, "_TRANSIENT_ADMIN_CLIENT", None)
    identity = NS(id="identity-1", agent_handle="agent", slack_enabled=True, update=Mock())
    resource = Mock()
    resource.list_connections.return_value = snapshot()
    resource.create_invitation.return_value = NS(
        invitation_url="https://example.com/install#token=one-time",
        expires_at="2030-01-01T00:00:00Z",
    )
    info = NS(auth_subtype="api_key.agent_scoped.claimed", scope="agent_identity:identity-1",
              organization_id="org-1")

    class Client:
        def __init__(self, who):
            self.who = who
            self.slack = resource
            self.closed = False

        def whoami(self):
            return self.who

        def get_identity(self, handle):
            assert handle == "agent"
            return identity

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.closed = True

    runtime = Client(info)
    admin = Client(NS(auth_subtype="api_key.admin_scoped", organization_id="org-1"))
    clients = {"runtime-key": runtime, "admin-key": admin}
    factory = Mock(side_effect=lambda **kw: clients[kw["api_key"]])
    answers = iter([True, True])
    monkeypatch.setattr(wizard, "prompt_yes_no", lambda *a: next(answers))
    monkeypatch.setattr(wizard, "prompt", lambda *a, **kw: "admin-key")
    now = [0.0]
    monkeypatch.setattr(wizard.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(wizard.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    return NS(identity=identity, resource=resource, runtime=runtime, admin=admin, factory=factory,
              env=tmp_path / ".env", now=now,
              run=lambda: wizard._configure_slack("runtime-key", "https://example.com", "agent", factory))


def test_install_prepares_then_links_then_polls_real_connection(setup, capsys):
    setup.resource.list_connections.side_effect = [
        snapshot("not_started"), snapshot("pending"), snapshot(),
        snapshot(), snapshot(connections=[connection()]),
    ]
    assert setup.run() is True
    assert setup.identity.update.call_count == 0
    assert setup.resource.start_setup.call_args.args == ("identity-1",)
    setup.resource.create_invitation.assert_called_once_with("identity-1")
    assert setup.now[0] == 8
    out = capsys.readouterr().out
    assert "https://example.com/install#token=one-time" in out
    assert "Slack connected: Example workspace" in out
    assert "admin-key" not in out and "runtime-key" not in out
    assert setup.env.read_text() == "INKBOX_SLACK_ENABLED=true\n"
    assert setup.runtime.closed and setup.admin.closed


def test_decline_only_disables_local_bridge(setup, monkeypatch):
    monkeypatch.setattr(wizard, "prompt_yes_no", lambda *a: False)
    assert setup.run() is False
    setup.factory.assert_not_called()
    assert setup.env.read_text() == "INKBOX_SLACK_ENABLED=false\n"


def test_enable_later_does_not_create_an_invitation(setup, monkeypatch):
    setup.identity.slack_enabled = False
    answers = iter([True, False])
    monkeypatch.setattr(wizard, "prompt_yes_no", lambda *a: next(answers))
    assert setup.run() is True
    setup.identity.update.assert_called_once_with(slack_enabled=True)
    setup.resource.create_invitation.assert_not_called()
    assert "true" in setup.env.read_text()


def test_existing_connection_needs_no_admin_and_preserves_saved_default(setup, monkeypatch, capsys):
    monkeypatch.setenv("INKBOX_SLACK_ENABLED", "true")
    setup.resource.list_connections.return_value = snapshot(connections=[connection()])
    questions = []

    def answer(question, default):
        questions.append((question, default))
        return default

    monkeypatch.setattr(wizard, "prompt_yes_no", answer)
    monkeypatch.setattr(wizard, "prompt", lambda *a, **kw: pytest.fail("unexpected credential prompt"))
    assert setup.run() is True
    assert [default for _, default in questions] == [True, False]
    setup.resource.start_setup.assert_not_called()
    assert "Already connected" in capsys.readouterr().out


def test_connect_another_workspace_does_not_count_old_connection_as_success(setup):
    old = connection()
    setup.resource.list_connections.side_effect = [
        snapshot(connections=[old]), snapshot(connections=[old]),
        snapshot(connections=[old]), snapshot(connections=[old, connection("workspace-2")]),
    ]
    assert setup.run() is True
    assert setup.resource.list_connections.call_count == 4
    assert setup.now[0] == 4


def test_reauthorization_counts_as_connected_when_status_changes(setup, capsys):
    setup.resource.list_connections.side_effect = [
        snapshot(connections=[connection(status="reauthorization_required")]),
        snapshot(), snapshot(connections=[connection()]),
    ]
    assert setup.run() is True
    assert "Slack connected" in capsys.readouterr().out


@pytest.mark.parametrize("status", ["failed", "unavailable"])
def test_failed_preparation_never_creates_link_or_claims_connection(setup, capsys, status):
    setup.resource.list_connections.return_value = snapshot(status)
    assert setup.run() is True
    setup.resource.create_invitation.assert_not_called()
    out = capsys.readouterr().out
    assert "needs attention" in out and "Slack connected" not in out


@pytest.mark.parametrize("phase", ["preparation", "connection"])
def test_timeout_is_bounded_and_preserves_enabled_config(setup, phase, capsys):
    setup.resource.list_connections.return_value = snapshot("pending" if phase == "preparation" else "ready")
    assert setup.run() is True
    assert setup.now[0] == 300
    assert "Still waiting" in capsys.readouterr().out
    assert "true" in setup.env.read_text()


def test_interrupt_wait_keeps_other_setup_steps_available(setup, monkeypatch, capsys):
    setup.resource.list_connections.return_value = snapshot("pending")
    monkeypatch.setattr(wizard.time, "sleep", Mock(side_effect=KeyboardInterrupt))
    assert setup.run() is True
    assert "waiting skipped" in capsys.readouterr().out
    setup.resource.create_invitation.assert_not_called()


@pytest.mark.parametrize("wrong", ["organization", "scope", "identity"])
def test_admin_must_match_identity_organization_and_scope(setup, wrong, capsys):
    setup.identity.slack_enabled = False
    if wrong == "organization":
        setup.admin.who.organization_id = "different-org"
    elif wrong == "scope":
        setup.admin.who.auth_subtype = "api_key.agent_scoped.claimed"
    else:
        setup.admin.get_identity = lambda handle: NS(id="different-identity")
    assert setup.run() is False
    setup.identity.update.assert_not_called()
    setup.resource.start_setup.assert_not_called()
    assert not setup.env.exists()
    assert "Use an admin-scoped key" in capsys.readouterr().out


def test_mismatched_runtime_key_cannot_configure_another_identity(setup):
    setup.runtime.who.scope = "agent_identity:other"
    assert setup.run() is False
    setup.resource.list_connections.assert_not_called()
    setup.identity.update.assert_not_called()


def test_unclaimed_identity_gets_actionable_claim_instruction(setup, capsys):
    setup.runtime.who.auth_subtype = "api_key.agent_scoped.unclaimed"
    assert setup.run() is False
    assert "Claim it" in capsys.readouterr().out
    setup.resource.start_setup.assert_not_called()


def test_skip_admin_leaves_disabled_identity_untouched(setup, monkeypatch):
    setup.identity.slack_enabled = False
    monkeypatch.setattr(wizard, "prompt", lambda *a, **kw: "")
    assert setup.run() is False
    setup.identity.update.assert_not_called()
    assert not setup.env.exists()


def test_admin_from_current_setup_is_reused_but_not_closed_or_persisted(setup, monkeypatch):
    monkeypatch.setattr(wizard, "_TRANSIENT_ADMIN_CLIENT", setup.admin)
    monkeypatch.setattr(wizard, "prompt", lambda *a, **kw: pytest.fail("unexpected key prompt"))
    setup.resource.list_connections.side_effect = [snapshot(), snapshot(), snapshot(connections=[connection()])]
    assert setup.run() is True
    assert not setup.admin.closed
    assert setup.env.read_text() == "INKBOX_SLACK_ENABLED=true\n"


def test_old_sdk_is_optional_and_does_not_break_setup(setup, capsys):
    setup.runtime.slack = NS(list_connections=Mock())
    assert setup.run() is False
    assert "requires an Inkbox SDK" in capsys.readouterr().out
    assert not setup.env.exists()


def test_missing_server_setup_status_does_not_claim_success(setup, capsys):
    setup.resource.list_connections.return_value.setup = None
    assert setup.run() is True
    setup.resource.create_invitation.assert_not_called()
    assert "not supported by this API/SDK" in capsys.readouterr().out


@pytest.mark.parametrize("pending", [False, True])
def test_invitation_retry_only_for_explicit_setup_pending(setup, capsys, pending):
    error = RuntimeError("credential=do-not-print")
    error.status_code = 409 if pending else 503
    error.detail = {"code": "slack_setup_pending"} if pending else None
    invitation = setup.resource.create_invitation.return_value
    setup.resource.create_invitation.side_effect = [error, invitation]
    setup.resource.list_connections.side_effect = [
        snapshot(), snapshot(), snapshot(), snapshot(connections=[connection()]),
    ]
    assert setup.run() is True
    assert setup.resource.create_invitation.call_count == (2 if pending else 1)
    out = capsys.readouterr().out
    assert "credential=do-not-print" not in out
    assert ("Slack connected" in out) is pending


def test_missing_invitation_link_does_not_poll_or_claim_connection(setup, capsys):
    setup.resource.create_invitation.return_value.invitation_url = None
    assert setup.run() is True
    assert "No Slack invitation" in capsys.readouterr().out
    assert setup.resource.list_connections.call_count == 2


def test_onboarding_through_slack_sdk_wire_contract(setup, monkeypatch, capsys):
    """Run the actual SDK parsers and request builders when Slack setup is available."""
    import json

    import httpx
    from inkbox import Inkbox

    try:
        from inkbox.slack import SlackResource
    except ImportError:
        pytest.skip("installed SDK predates Slack")
    if not hasattr(SlackResource, "start_setup"):
        pytest.skip("installed SDK predates Slack app preparation")

    identity_id = "00000000-0000-4000-8000-000000000001"
    invitation_id = "00000000-0000-4000-8000-000000000002"
    connection_id = "00000000-0000-4000-8000-000000000003"
    state = {"enabled": False, "polls": 0, "invited": False}
    requests = []

    def handle(request):
        key = request.headers["X-API-Key"]
        body = json.loads(request.content) if request.content else None
        path = request.url.path
        requests.append((request.method, path, key, body))
        if path == "/api/whoami":
            return httpx.Response(200, json={
                "auth_type": "api_key", "organization_id": "org-1",
                "auth_subtype": "api_key.admin_scoped" if key == "admin-key" else "api_key.agent_scoped.claimed",
                "scope": "organization" if key == "admin-key" else f"agent_identity:{identity_id}",
            })
        if path == "/api/v1/identities/agent":
            if request.method == "PATCH":
                assert key == "admin-key" and body == {"slack_enabled": True}
                state["enabled"] = True
            return httpx.Response(200, json={
                "id": identity_id, "organization_id": "org-1", "agent_handle": "agent",
                "slack_enabled": state["enabled"],
                "created_at": "2030-01-01T00:00:00Z", "updated_at": "2030-01-01T00:00:00Z",
            })
        if path == "/api/v1/slack/applications/setup":
            assert key == "admin-key" and body == {"identity_id": identity_id}
            return httpx.Response(202, json={"status": "pending"})
        if path == "/api/v1/slack/connections":
            assert request.url.params["identity_id"] == identity_id
            assert key == "runtime-key"
            state["polls"] += 1
            connections = []
            if state["invited"]:
                connections.append({
                    "id": connection_id, "identity_id": identity_id, "workspace_id": "T_EXAMPLE",
                    "workspace_name": "Example workspace", "bot_user_id": "U_AGENT",
                    "status": "connected", "scopes": [], "created_at": "2030-01-01T00:00:00Z",
                })
            return httpx.Response(200, json={
                "connections": connections, "installation_available": True,
                "setup": {"status": "pending" if state["polls"] < 3 else "ready"},
            })
        if path == "/api/v1/slack/invitations":
            assert key == "admin-key" and body == {"identity_id": identity_id, "expires_in_seconds": 86400}
            state["invited"] = True
            return httpx.Response(201, json={
                "id": invitation_id, "identity_id": identity_id, "status": "pending",
                "expires_at": "2030-01-02T00:00:00Z", "invitation_url": "https://example.com/install#token=example",
            })
        raise AssertionError(f"Unexpected request: {request.method} {path}")

    monkeypatch.setattr(httpx, "HTTPTransport", lambda **kw: httpx.MockTransport(handle))
    assert wizard._configure_slack("runtime-key", "https://example.com", "agent", Inkbox) is True
    assert state["enabled"] and state["invited"]
    assert setup.env.read_text() == "INKBOX_SLACK_ENABLED=true\n"
    assert sum(path.endswith("/invitations") for _, path, _, _ in requests) == 1
    assert "Slack connected: Example workspace" in capsys.readouterr().out
