"""Deterministic checks for the hosted-call live-test evidence helpers."""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_voice_module():
    path = Path(__file__).parent / "live" / "test_voice.py"
    spec = importlib.util.spec_from_file_location("codex_live_voice_helpers", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


voice = _load_voice_module()


def test_workflow_requires_action_first_exact_body_and_readback():
    workflow = (Path(__file__).parent.parent / ".github/workflows/live-voice.yml").read_text()

    assert (
        "After we hang up, send me one SMS with this exact three-word body: "
        "$HOSTED_MARKER. Record that post-call SMS action now. Once the action tool "
        "succeeds, read the exact three words back to me. Do not send the SMS during "
        "the call." in workflow
    )


def test_spoken_marker_normalizes_punctuation_and_case():
    assert voice._voice_marker_key("Victor-Echo, JULIET!") == "victorechojuliet"


def test_after_call_sms_intent_requires_after_call_language():
    assert voice._has_after_call_sms_intent(
        "After we hang up, send an S.M.S. containing Victor Echo."
    )
    assert not voice._has_after_call_sms_intent(
        "Send an SMS containing Victor Echo during this call."
    )


def test_sms_targets_include_recipient_rows():
    message = SimpleNamespace(
        remote_phone_number=None,
        recipients=[SimpleNamespace(recipient_phone_number="+1 (516) 555-0101")],
    )
    assert voice._sms_target_numbers(message) == {"15165550101"}


def test_record_timestamp_accepts_datetime_and_iso_z():
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    assert voice._record_created_at(SimpleNamespace(created_at=stamp)) == stamp
    assert voice._record_created_at(SimpleNamespace(created_at="2026-08-01T12:00:00Z")) == stamp


def test_voicemail_detection_value_accepts_sdk_enum_or_wire_string():
    enum_like = SimpleNamespace(value="disabled")
    assert (
        voice._voicemail_detection_value(SimpleNamespace(voicemail_detection=enum_like))
        == "disabled"
    )
    assert (
        voice._voicemail_detection_value(SimpleNamespace(voicemail_detection="disabled"))
        == "disabled"
    )


def test_two_way_proof_returns_aut_local_speech(monkeypatch):
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)
    calls = SimpleNamespace(
        transcripts=lambda _call_id: [
            SimpleNamespace(party="remote", text="driver request"),
            SimpleNamespace(party="local", text="agent reply"),
        ],
        get=lambda _call_id: SimpleNamespace(status="answered"),
    )

    assert (
        voice._wait_for_two_way_call(
            SimpleNamespace(calls=calls),
            "unused",
            "aut-call",
            deadline=voice.time.monotonic() + 1,
        )
        == "agent reply"
    )


def test_driver_proof_requires_driver_local_speech(monkeypatch):
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)
    calls = SimpleNamespace(
        transcripts=lambda _call_id: [
            SimpleNamespace(party="local", text="driver request"),
            SimpleNamespace(party="remote", text="agent reply"),
        ],
        get=lambda _call_id: SimpleNamespace(status="answered"),
    )

    assert (
        voice._wait_for_driver_local_speech(
            SimpleNamespace(calls=calls),
            "unused",
            "driver-call",
            deadline=voice.time.monotonic() + 1,
        )
        == "driver request"
    )


def test_call_pair_correlation_keeps_driver_and_aut_ownership(monkeypatch):
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = SimpleNamespace(id="driver-1", created_at=stamp, voicemail_detection="enabled")
    aut = SimpleNamespace(id="aut-1", created_at=stamp, voicemail_detection="disabled")

    assert voice._wait_for_fresh_call_pair(
        lambda: [driver],
        lambda: [aut],
        set(),
        set(),
        not_before=stamp,
        deadline=voice.time.monotonic() + 1,
        label="test",
    ) == (driver, aut)


def test_call_pair_duplicate_diagnostic_names_owner_without_ids(monkeypatch):
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = SimpleNamespace(id="driver-1", created_at=stamp)
    aut = [
        SimpleNamespace(id="aut-1", created_at=stamp),
        SimpleNamespace(id="aut-2", created_at=stamp),
    ]

    with pytest.raises(
        AssertionError,
        match=r"duplicate AUT call records .*matching_count=2",
    ):
        voice._wait_for_fresh_call_pair(
            lambda: [driver],
            lambda: aut,
            set(),
            set(),
            not_before=stamp,
            deadline=voice.time.monotonic() + 1,
            label="test",
        )


def test_call_pair_ignores_delayed_old_row_after_snapshot(monkeypatch):
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)
    request_time = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    old = SimpleNamespace(id="late-old", created_at=request_time - voice.timedelta(minutes=5))
    driver = SimpleNamespace(id="driver-current", created_at=request_time)
    aut = SimpleNamespace(id="aut-current", created_at=request_time)

    assert voice._wait_for_fresh_call_pair(
        lambda: [old, driver],
        lambda: [aut],
        set(),
        set(),
        not_before=request_time,
        deadline=voice.time.monotonic() + 1,
        label="test",
    ) == (driver, aut)


def test_cleanup_ends_every_post_baseline_call():
    hung_up = []
    client = SimpleNamespace(calls=SimpleNamespace(hangup=hung_up.append))
    old = SimpleNamespace(id="old")
    new_a = SimpleNamespace(id="new-a")
    new_b = SimpleNamespace(id="new-b")

    voice._hangup_fresh_calls(client, lambda: [old, new_a, new_b], {"old"})

    assert hung_up == ["new-a", "new-b"]


def test_pretest_sweep_ends_only_active_matching_calls(monkeypatch):
    monkeypatch.setattr(voice.time, "sleep", lambda _seconds: None)
    hung_up = []
    calls = SimpleNamespace(
        hangup=hung_up.append,
        get=lambda call_id: SimpleNamespace(id=call_id, status="completed"),
    )
    client = SimpleNamespace(calls=calls)
    active = SimpleNamespace(id="active", status="answered")
    terminal = SimpleNamespace(id="terminal", status="completed")

    voice._sweep_matching_calls(client, lambda: [active, terminal])

    assert hung_up == ["active"]


def test_matching_post_call_action_requires_open_current_marker_sms():
    marker = "victor echo juliet"
    matching = {
        "status": "open",
        "action": "send_sms",
        "details": "After the call, send Victor-Echo, Juliet to the caller.",
    }
    assert (
        voice._matching_post_call_action(SimpleNamespace(post_call_action_items=[matching]), marker)
        is matching
    )

    for item in (
        {**matching, "status": "canceled"},
        {**matching, "details": "Send a different marker."},
        {**matching, "action": "create_note", "details": marker},
    ):
        assert (
            voice._matching_post_call_action(SimpleNamespace(post_call_action_items=[item]), marker)
            is None
        )


def test_action_gate_diagnostic_is_bounded_and_content_redacted():
    secret = "customer-secret-" * 10_000
    call = SimpleNamespace(
        post_call_action_items=[
            {
                "status": "open",
                "action": "send_sms",
                "details": f"Send Victor Echo Juliet {secret}",
            },
            *[{"status": "closed", "action": secret, "details": secret} for _ in range(15)],
        ]
    )

    diagnostic = voice._post_call_action_diagnostic(
        call,
        "Victor Echo Juliet",
    )

    assert diagnostic == {
        "item_count": 16,
        "inspected_count": 10,
        "open_count": 1,
        "marker_count": 1,
        "sms_count": 1,
        "matching_action": True,
    }
    assert "customer-secret" not in repr(diagnostic)
