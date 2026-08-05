"""Deterministic checks for the hosted-call live-test evidence helpers."""

from __future__ import annotations

import time

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


def test_voice_driver_allows_realtime_response_latency():
    driver = (Path(__file__).parent / "live/voice_driver.py").read_text()
    assert 'VOICE_DRIVER_LISTEN", "30"' in driver


def test_workflow_requires_exact_action_body_and_corrective_readback():
    workflow = (Path(__file__).parent.parent / ".github/workflows/live-voice.yml").read_text()

    assert (
        "After we hang up, send me one SMS containing exactly $HOSTED_MARKER. Before "
        "we hang up, record one post-call action whose action title and details are both "
        "exactly Send SMS $HOSTED_MARKER. Read the five-word SMS body back after recording "
        "it. Do not send the SMS during the call."
        in workflow
    )
    assert (
        "Verify the post-call action you just recorded. If its action title or details are "
        "not exactly Send SMS $HOSTED_MARKER, edit that same action now so both match "
        "exactly. Then read back exactly $HOSTED_MARKER."
        in workflow
    )
    assert "list the actions" not in workflow.lower()
    assert "export VOICE_DRIVER_FOLLOWUP_AFTER=45" in workflow
    assert "banana elephant pineapple" in workflow
    assert "alpha bravo charlie" not in workflow


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
    assert voice._record_created_at(
        SimpleNamespace(created_at="2026-08-01T12:00:00Z")
    ) == stamp


def test_voicemail_detection_value_accepts_sdk_enum_or_wire_string():
    enum_like = SimpleNamespace(value="disabled")
    assert voice._voicemail_detection_value(
        SimpleNamespace(voicemail_detection=enum_like)
    ) == "disabled"
    assert voice._voicemail_detection_value(
        SimpleNamespace(voicemail_detection="disabled")
    ) == "disabled"


def test_call_pair_correlation_keeps_driver_and_aut_ownership():
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = SimpleNamespace(
        id="driver-1", created_at=stamp, voicemail_detection="enabled"
    )
    aut = SimpleNamespace(
        id="aut-1", created_at=stamp, voicemail_detection="disabled"
    )

    assert voice._correlate_fresh_call_pair(
        [driver],
        [aut],
        before_driver=set(),
        before_aut=set(),
        driver_watermark=stamp,
        aut_watermark=stamp,
    ) == (driver, aut)


def test_call_pair_duplicate_diagnostic_names_owner_and_ids():
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    driver = SimpleNamespace(id="driver-1", created_at=stamp)
    aut = [
        SimpleNamespace(id="aut-1", created_at=stamp),
        SimpleNamespace(id="aut-2", created_at=stamp),
    ]

    with pytest.raises(
        AssertionError,
        match=r"phase=call_pairing .*aut-1.*aut-2.*duplicate AUT legs",
    ):
        voice._correlate_fresh_call_pair(
            [driver],
            aut,
            before_driver=set(),
            before_aut=set(),
            driver_watermark=stamp,
            aut_watermark=stamp,
        )


def test_matching_post_call_action_requires_open_current_marker_sms():
    marker = "victor echo juliet"
    matching = {
        "status": "open",
        "action": "send_sms",
        "details": "After the call, send Victor-Echo, Juliet to the caller.",
    }
    assert voice._matching_post_call_action(
        SimpleNamespace(post_call_action_items=[matching]), marker
    ) is matching

    for item in (
        {**matching, "status": "canceled"},
        {**matching, "details": "Send a different marker."},
        {**matching, "action": "create_note", "details": marker},
    ):
        assert voice._matching_post_call_action(
            SimpleNamespace(post_call_action_items=[item]), marker
        ) is None


def test_action_gate_diagnostic_is_bounded_and_content_redacted():
    secret = "customer-secret-" * 10_000
    call = SimpleNamespace(
        post_call_action_items=[
            {
                "status": "open",
                "action": "send_sms",
                "details": f"Send Victor Echo Juliet {secret}",
            },
            *[
                {"status": "closed", "action": secret, "details": secret}
                for _ in range(15)
            ],
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


def test_paired_agent_leg_is_queried_through_aut_identity():
    stamp = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    pair_id = "33333333-3333-3333-3333-333333333333"
    driver_call = SimpleNamespace(
        id="driver", paired_call_id=pair_id, created_at=stamp,
    )
    aut_call = SimpleNamespace(
        id="aut", paired_call_id=pair_id, direction="outbound", created_at=stamp,
    )
    driver = SimpleNamespace(calls=SimpleNamespace(get=lambda _call_id: driver_call))

    class Calls:
        kwargs = None

        def list(self, **kwargs):
            self.kwargs = kwargs
            return [aut_call]

    calls = Calls()
    result = voice._wait_for_agent_leg(
        driver,
        SimpleNamespace(calls=calls),
        "driver",
        lambda: [],
        direction="outbound",
        deadline=time.monotonic() + 1,
    )

    assert result is aut_call
    assert calls.kwargs == {"limit": 2, "paired_call_id": pair_id}


def test_owned_leg_transcript_proof_uses_local_speech_correctly():
    class Calls:
        def __init__(self, segments):
            self.segments = segments

        def transcripts(self, _call_id):
            return self.segments

        def get(self, _call_id):
            return SimpleNamespace(status="answered")

    driver = SimpleNamespace(calls=Calls([
        SimpleNamespace(party="local", text="scripted driver line"),
    ]))
    aut = SimpleNamespace(calls=Calls([
        SimpleNamespace(party="remote", text="caller line"),
        SimpleNamespace(party="local", text="agent reply"),
    ]))
    deadline = time.monotonic() + 1

    assert voice._wait_for_driver_local_speech(
        driver, "unused", "driver", deadline=deadline
    ) == "scripted driver line"
    assert voice._wait_for_two_way_call(
        aut, "unused", "aut", deadline=deadline
    ) == "agent reply"
