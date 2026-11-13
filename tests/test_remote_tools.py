import asyncio
from types import SimpleNamespace

import pytest

from inkbox_codex import remote_tools


def tool(name="example_dynamic", description="Initial"):
    return {"name": name, "description": description, "inputSchema": {"type": "object"}}


@pytest.fixture
def router(tmp_path, monkeypatch):
    monkeypatch.setenv("INKBOX_CODEX_HOME", str(tmp_path))
    monkeypatch.delenv("INKBOX_CODEX_HOST_EPOCH", raising=False)
    client = SimpleNamespace(
        _base_url="https://example.test",
        whoami=lambda: SimpleNamespace(auth_subtype="api_key.agent_scoped.claimed"),
        get_identity=lambda _: SimpleNamespace(id="identity-example"),
    )
    return remote_tools.RemoteTools(client, "agent", "synthetic-key")


def test_claimed_catalog_changes_without_package_updates(router, monkeypatch):
    catalog = [tool()]

    async def refresh(self):
        self._catalog = tuple(catalog)
        return list(catalog)

    monkeypatch.setattr(remote_tools.RemoteMcp, "refresh", refresh)

    async def scenario():
        assert (await router.refresh([]))[0]["description"] == "Initial"
        catalog[:] = [tool("added", "Changed")]
        assert (await router.refresh([]))[0]["name"] == "added"
        assert router.routes("added") and not router.routes("example_dynamic")
        catalog[:] = []
        assert await router.refresh([]) == []

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "cohort", ["api_key.admin_scoped", "api_key.agent_scoped.unclaimed"]
)
def test_supported_legacy_cohorts_keep_local_capabilities(router, monkeypatch, cohort):
    router.client.whoami = lambda: SimpleNamespace(auth_subtype=cohort)

    async def unexpected(*args):
        pytest.fail("Legacy credentials must not be sent to remote MCP")

    monkeypatch.setattr(remote_tools.RemoteMcp, "refresh", unexpected)
    local = [tool("local_example")]
    assert asyncio.run(router.refresh(local)) == local
    assert not router.routes("local_example")


def test_unknown_authority_does_not_fall_back(router):
    router.client.whoami = lambda: SimpleNamespace(auth_subtype="unknown")
    with pytest.raises(remote_tools.RemoteToolError, match="credential type"):
        asyncio.run(router.refresh([tool()]))
    router.client.whoami = lambda: (_ for _ in ()).throw(
        RuntimeError("private failure")
    )
    with pytest.raises(
        remote_tools.RemoteToolError, match="current authority"
    ) as error:
        asyncio.run(router.refresh([tool()]))
    assert "private failure" not in str(error.value)


def test_collision_keeps_previous_generation(router, monkeypatch):
    catalog = [tool()]

    async def refresh(self):
        return list(catalog)

    monkeypatch.setattr(remote_tools.RemoteMcp, "refresh", refresh)

    async def scenario():
        await router.refresh([])
        previous = router.remote
        catalog[:] = [tool("local_example")]
        with pytest.raises(remote_tools.RemoteToolError, match="conflicts"):
            await router.refresh([tool("local_example")])
        assert router.remote is previous
        assert router.routes("example_dynamic")

    asyncio.run(scenario())


def test_legacy_contact_calls_use_same_remote_handler_and_native_identity(
    router, monkeypatch
):
    calls = []

    async def refresh(self):
        return [tool("inkbox_contacts_list")]

    async def call(self, name, arguments, **kwargs):
        kwargs["before_execute"]()
        calls.append((name, arguments, kwargs))
        return {"content": [], "structuredContent": {"contacts": []}}

    monkeypatch.setattr(remote_tools.RemoteMcp, "refresh", refresh)
    monkeypatch.setattr(remote_tools.RemoteMcp, "call", call)
    metadata = {
        "x-codex-turn-metadata": {"thread_id": "thread", "turn_id": "turn"},
        "callId": "native-call",
    }

    async def scenario():
        catalog = await router.refresh([tool("inkbox_list_contacts")])
        assert len(catalog) == 2
        result = await router.call("inkbox_list_contacts", {"q": "Example"}, metadata)
        assert result["structuredContent"] == {"contacts": []}
        await router.call("inkbox_list_contacts", {"q": "Example"}, metadata)
        assert calls[0][0] == "inkbox_contacts_list"
        assert calls[0][1] == {"q": "Example", "limit": 25}
        assert calls[0][2]["call_identity"] == calls[1][2]["call_identity"]
        assert calls[0][2]["journal_path"] == calls[1][2]["journal_path"]
        changed = {**metadata, "callId": "intentional-new-call"}
        await router.call("inkbox_list_contacts", {"q": "Example"}, changed)
        assert calls[-1][2]["call_identity"] != calls[0][2]["call_identity"]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {},
        {"callId": "call"},
        {"x-codex-turn-metadata": {"thread_id": "t", "turn_id": "u"}},
    ],
)
def test_no_fabricated_native_operation_identity(metadata):
    with pytest.raises(remote_tools.RemoteToolError, match="stable native"):
        remote_tools.native_identity(metadata)


def test_contact_compatibility_arguments_preserve_omitted_fields():
    assert remote_tools.compatibility_arguments(
        "inkbox_create_contact",
        {"given_name": "Example", "phones": ["+15550000001"]},
        "operation",
    ) == {
        "contact": {
            "given_name": "Example",
            "phones": [
                {"value_e164": "+15550000001", "label": None, "is_primary": True}
            ],
        },
        "idempotency_key": "operation",
    }
    assert remote_tools.compatibility_arguments(
        "inkbox_update_contact", {"contact_id": "contact", "emails": []}, "operation"
    ) == {
        "contact_id": "contact",
        "patch": {"emails": []},
        "idempotency_key": "operation",
    }


def test_native_adapter_rejects_invalid_remote_arguments_before_submission(
    router, monkeypatch
):
    descriptor = tool()
    descriptor["inputSchema"] = {
        "type": "object",
        "properties": {"text": {"type": "string", "minLength": 2}},
        "required": ["text"],
        "additionalProperties": False,
    }

    async def refresh(self):
        self._catalog = (descriptor,)
        return [descriptor]

    async def request(*args, **kwargs):
        pytest.fail("Invalid arguments must not reach the transport")

    monkeypatch.setattr(remote_tools.RemoteMcp, "refresh", refresh)
    monkeypatch.setattr(remote_tools.RemoteMcp, "request", request)

    async def scenario():
        await router.refresh([])
        with pytest.raises(remote_tools.RemoteToolError, match="remote schema"):
            await router.call(
                "example_dynamic",
                {"text": "x"},
                {
                    "x-codex-turn-metadata": {"thread_id": "thread", "turn_id": "turn"},
                    "callId": "invalid-native-call",
                },
            )

    asyncio.run(scenario())
