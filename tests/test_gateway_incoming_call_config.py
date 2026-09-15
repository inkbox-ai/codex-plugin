"""Startup reconciliation: inbound-call config must be identity-scoped (one row
covers the dedicated number AND the shared iMessage line), with the
number-scoped update only as a legacy-SDK fallback."""

import types

from inkbox_codex import gateway as gateway_mod
from inkbox_codex.config import BridgeConfig, VoiceStack
from inkbox_codex.gateway import InkboxGateway


class _FakeSubscriptions:
    def __init__(self, existing=()):
        self.created = []
        self.existing = list(existing)
        self.deleted = []

    def list(self, **_kwargs):
        return list(self.existing)

    def create(self, **kwargs):
        assert kwargs["agent_identity_id"] == "identity-1"
        self.created.append(kwargs)
        return None

    def delete(self, sub_id):
        self.deleted.append(sub_id)


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


def _patched_gateway(identity, subscriptions=None, voice_stack=VoiceStack.INKBOX_TTS_STT):
    gw = InkboxGateway(BridgeConfig(
        identity="codex-agent", allow_all_users=True, voice_stack=voice_stack,
    ))
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
        "incoming_call_webhook_url": None,
    }]
    # The number-scoped legacy write must not also fire.
    assert gw._inkbox.phone_numbers.updates == []


def test_voice_ai_reconciles_hosted_incoming_action_and_completion_subscription():
    identity = _Identity(phone=True, imessage=True)
    subscriptions = _FakeSubscriptions()
    _patched_gateway(
        identity,
        subscriptions=subscriptions,
        voice_stack=VoiceStack.INKBOX_VOICE_AI,
    )

    assert identity.incoming_call_configs == [{
        "incoming_call_action": "hosted_agent",
        "client_websocket_url": None,
        "incoming_call_webhook_url": None,
    }]
    assert len(subscriptions.created) == 1
    assert "call.ended" in subscriptions.created[0]["event_types"]


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


def test_one_mixed_subscription_is_created_without_channels():
    subscriptions = _FakeSubscriptions()
    _patched_gateway(_Identity(phone=False, imessage=False), subscriptions=subscriptions)
    assert len(subscriptions.created) == 1
    assert subscriptions.created[0]["agent_identity_id"] == "identity-1"
    assert set(subscriptions.created[0]["event_types"]) == set(
        gateway_mod.MAIL_EVENTS + gateway_mod.TEXT_EVENTS + gateway_mod.IMESSAGE_EVENTS + gateway_mod.CALL_EVENTS + gateway_mod.A2A_EVENTS
    )
    assert subscriptions.deleted == []


def test_persists_first_created_signing_key_without_rotating_existing_key(monkeypatch):
    from unittest.mock import Mock
    from inkbox_codex import setup_wizard

    save = Mock()
    monkeypatch.setattr(setup_wizard, "_save", save)
    subscriptions = _FakeSubscriptions()
    subscriptions.create = Mock(return_value=types.SimpleNamespace(signing_key="synthetic-first-key"))
    gw = _patched_gateway(_Identity(), subscriptions=subscriptions)
    assert gw.cfg.signing_key == "synthetic-first-key"
    save.assert_called_once_with("INKBOX_SIGNING_KEY", "synthetic-first-key")
    gw._patch_identity_objects()
    assert save.call_count == 1
