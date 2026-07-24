"""Startup reconciliation: inbound-call config must be identity-scoped (one row
covers the dedicated number AND the shared iMessage line), with the
number-scoped update only as a legacy-SDK fallback."""

import types

from inkbox_codex import gateway as gateway_mod
from inkbox_codex.config import BridgeConfig
from inkbox_codex.gateway import InkboxGateway


class _FakeSubscriptions:
    def __init__(self):
        self.created = []

    def list(self, **_kwargs):
        return []

    def create(self, **kwargs):
        self.created.append(kwargs)
        return None

    def delete(self, _sub_id):
        return None


class _UnsupportedA2ASubscriptions(_FakeSubscriptions):
    def create(self, **kwargs):
        if any(event.startswith("a2a.") for event in kwargs["event_types"]):
            error = RuntimeError("unsupported A2A events")
            error.status_code = 422
            error.detail = "a2a.task.created is not a valid event type"
            raise error
        return super().create(**kwargs)


class _FakePhoneNumbers:
    def __init__(self):
        self.updates = []

    def update(self, phone_id, **kwargs):
        self.updates.append((phone_id, kwargs))


class _FakeInkbox:
    def __init__(self, identity, subscriptions=None):
        self._identity = identity
        self.webhooks = types.SimpleNamespace(
            subscriptions=subscriptions or _FakeSubscriptions()
        )
        self.phone_numbers = _FakePhoneNumbers()

    def get_identity(self, _handle):
        return self._identity


class _Identity:
    """Modern identity: exposes the identity-scoped incoming-call setter."""

    def __init__(self, *, phone=True, imessage=False):
        self.id = "identity-1"
        self.agent_handle = "codex-agent"
        self.mailbox = None
        self.phone_number = (
            types.SimpleNamespace(id="phone-1", number="+15550000000") if phone else None
        )
        self.imessage_enabled = imessage
        self.incoming_call_configs = []

    def set_incoming_call_action(self, **kwargs):
        self.incoming_call_configs.append(kwargs)


def _legacy_identity(**kwargs):
    # Old-SDK identity: no ``set_incoming_call_action`` attribute at all.
    identity = _Identity(**kwargs)
    legacy = types.SimpleNamespace(
        id=identity.id,
        agent_handle=identity.agent_handle,
        mailbox=None,
        phone_number=identity.phone_number,
        imessage_enabled=identity.imessage_enabled,
    )
    return legacy


def _patched_gateway(identity, subscriptions=None):
    gw = InkboxGateway(BridgeConfig(identity="codex-agent", allow_all_users=True))
    gw._inkbox = _FakeInkbox(identity, subscriptions)
    gw._public_url = "https://agent.inkboxwire.com"
    gw._public_host = "agent.inkboxwire.com"
    gw._patch_identity_objects()
    return gw


def test_incoming_call_config_is_identity_scoped():
    identity = _Identity(phone=True, imessage=False)
    gw = _patched_gateway(identity)

    assert identity.incoming_call_configs == [{
        "incoming_call_action": "auto_accept",
        "client_websocket_url": "wss://agent.inkboxwire.com/phone/media/ws",
        "incoming_call_webhook_url": "https://agent.inkboxwire.com/webhook",
    }]
    # The number-scoped legacy write must not also fire.
    assert gw._inkbox.phone_numbers.updates == []


def test_incoming_call_config_registers_for_imessage_only_identity():
    # No dedicated number at all — the shared iMessage line alone can receive
    # calls, so the identity-scoped row must still be written.
    identity = _Identity(phone=False, imessage=True)
    _patched_gateway(identity)

    assert len(identity.incoming_call_configs) == 1
    assert identity.incoming_call_configs[0]["incoming_call_action"] == "auto_accept"


def test_incoming_call_config_skipped_when_no_line_can_ring():
    identity = _Identity(phone=False, imessage=False)
    gw = _patched_gateway(identity)

    assert identity.incoming_call_configs == []
    assert gw._inkbox.phone_numbers.updates == []


def test_legacy_sdk_falls_back_to_number_scoped_update():
    identity = _legacy_identity(phone=True, imessage=False)
    gw = _patched_gateway(identity)

    assert not hasattr(identity, "set_incoming_call_action")
    phone_id, kwargs = gw._inkbox.phone_numbers.updates[0]
    assert phone_id == "phone-1"
    assert kwargs["incoming_call_action"] == "auto_accept"
    assert kwargs["client_websocket_url"] == "wss://agent.inkboxwire.com/phone/media/ws"


def test_legacy_sdk_without_number_cannot_configure_and_skips():
    # Legacy shim is number-scoped; an iMessage-only identity has nothing to
    # hang it on — must not crash, must not write anything.
    identity = _legacy_identity(phone=False, imessage=True)
    gw = _patched_gateway(identity)

    assert gw._inkbox.phone_numbers.updates == []


def test_a2a_subscription_falls_back_to_imessage_on_older_api():
    subscriptions = _UnsupportedA2ASubscriptions()
    _patched_gateway(
        _Identity(phone=False, imessage=True),
        subscriptions=subscriptions,
    )

    assert subscriptions.created[-1]["event_types"] == gateway_mod.IMESSAGE_EVENTS


def test_a2a_only_subscription_is_skipped_on_older_api():
    subscriptions = _UnsupportedA2ASubscriptions()
    _patched_gateway(
        _Identity(phone=False, imessage=False),
        subscriptions=subscriptions,
    )

    assert subscriptions.created == []
