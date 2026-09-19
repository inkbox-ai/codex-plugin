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


def test_workflow_uses_one_explicit_hosted_action_utterance_after_quiet():
    workflow = (Path(__file__).parent.parent / ".github/workflows/live-voice.yml").read_text()

    assert (
        "After we hang up, send me one SMS containing exactly: $HOSTED_MARKER. "
        "Please repeat those words back to me." in workflow
    )
    assert "export VOICE_DRIVER_SPEAK_AFTER=8" in workflow
    # The driver re-asks while the agent is idle and stops once it says the
    # marker back, so the marker has to reach the driver.
    assert 'export VOICE_DRIVER_ANSWER_CONTAINS="$HOSTED_MARKER"' in workflow


def test_call_request_is_fresh_without_internal_tool_instructions():
    first, second = voice._call_me_text(), voice._call_me_text()
    assert first != second
    assert "call me" in first.lower()
    assert "inkbox_" not in first
    assert "post-call action" not in first


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


def test_hosted_request_gate_requires_aut_transcript(monkeypatch):
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)
    marker = "victor echo juliet"
    request = f"After this call ends, send one SMS containing {marker}."
    driver = SimpleNamespace(
        calls=SimpleNamespace(
            transcripts=lambda _call_id: [SimpleNamespace(party="local", text=request)]
        )
    )
    aut = SimpleNamespace(
        calls=SimpleNamespace(
            transcripts=lambda _call_id: [],
            get=lambda _call_id: SimpleNamespace(
                post_call_action_items=[{
                    "status": "open",
                    "action": "send_sms",
                    "details": f"Send {marker} to the caller.",
                }]
            ),
        )
    )

    with pytest.raises(pytest.fail.Exception, match="aut_transcript_ready=False"):
        voice._wait_for_persisted_hosted_request(
            driver,
            "unused",
            "driver-call",
            aut,
            "aut-call",
            marker,
            deadline=voice.time.monotonic() + 0.01,
        )


def test_hosted_request_gate_accepts_both_transcripts_and_action(monkeypatch):
    monkeypatch.setattr(voice, "POLL_EVERY_S", 0)
    marker = "victor echo juliet"
    request = f"After this call ends, send one SMS containing {marker}."
    driver = SimpleNamespace(
        calls=SimpleNamespace(
            transcripts=lambda _call_id: [SimpleNamespace(party="local", text=request)]
        )
    )
    aut = SimpleNamespace(
        calls=SimpleNamespace(
            transcripts=lambda _call_id: [SimpleNamespace(party="remote", text=request)],
            get=lambda _call_id: SimpleNamespace(
                post_call_action_items=[{
                    "status": "open",
                    "action": "send_sms",
                    "details": f"Send {marker} to the caller.",
                }]
            ),
        )
    )

    assert voice._wait_for_persisted_hosted_request(
        driver,
        "unused",
        "driver-call",
        aut,
        "aut-call",
        marker,
        deadline=voice.time.monotonic() + 1,
    ) is None


@pytest.mark.parametrize("defect", ["prose", "extra_marker", "extra_other", "early", "missing_created", "missing_ended", "wrong_target", "extra_target"])
def test_post_call_sms_rejects_false_success(defect):
    from datetime import timedelta

    ended = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    message = SimpleNamespace(id="new", text="Alpha Bravo Charlie", created_at=ended,
                              remote_phone_number="+15551112222", recipients=[])
    call = SimpleNamespace(ended_at=ended)
    messages = [message]
    if defect == "prose":
        message.text = "Your words: Alpha Bravo Charlie"
    elif defect in {"extra_marker", "extra_other"}:
        messages.append(SimpleNamespace(id="extra", text=message.text if defect == "extra_marker" else "Done",
                                        created_at=ended, remote_phone_number="+15551112222", recipients=[]))
    elif defect == "early":
        message.created_at = ended - timedelta(microseconds=1)
    elif defect == "missing_created":
        message.created_at = None
    elif defect == "missing_ended":
        call.ended_at = None
    elif defect == "wrong_target":
        message.remote_phone_number = "+15553334444"
    else:
        message.recipients = [SimpleNamespace(recipient_phone_number="+15553334444")]
    with pytest.raises(AssertionError):
        voice._assert_post_call_sms(messages, set(), "Alpha Bravo Charlie", call, "+15551112222")


def test_post_call_sms_accepts_one_exact_body_after_persisted_end_and_ignores_baseline():
    ended = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
    old = SimpleNamespace(id="old", text="Unrelated old message")
    new = SimpleNamespace(id="new", text="Alpha, Bravo Charlie.", created_at=ended.isoformat(),
                          remote_phone_number="+15551112222", recipients=[])
    voice._assert_post_call_sms([old, new], {"old"}, "Alpha Bravo Charlie",
                                SimpleNamespace(ended_at=ended), "+15551112222")


@pytest.mark.parametrize("local_text,passes", [("Alpha Bravo Charlie", True), ("Hello", False), ("Alpha Bravo", False)])
@pytest.mark.parametrize("party", ["local", "remote"])
def test_readback_requires_complete_speech_from_correct_party(monkeypatch, local_text, passes, party):
    now = [0.0]
    monkeypatch.setattr(voice.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(voice.time, "sleep", lambda delay: now.__setitem__(0, now[0] + delay))
    client = SimpleNamespace(calls=SimpleNamespace(transcripts=lambda _: [
        SimpleNamespace(party="local" if party == "remote" else "remote", text="Alpha Bravo Charlie"),
        SimpleNamespace(party=party, text=local_text),
    ]))
    if passes:
        voice._wait_for_hosted_readback(client, "call", "Alpha Bravo Charlie", deadline=10, party=party)
    else:
        with pytest.raises(pytest.fail.Exception):
            voice._wait_for_hosted_readback(client, "call", "Alpha Bravo Charlie", deadline=10, party=party)


def test_sms_window_exhausts_actual_sdk_pages_and_retains_all_targets(monkeypatch):
    from uuid import UUID
    from inkbox import Inkbox

    client = Inkbox(api_key="offline")
    bound = "2026-08-01T12:00:00+00:00"
    def row(index):
        return dict(id=str(UUID(int=index)), direction="outbound", local_phone_number="+15551112222",
                    remote_phone_number="+15553334444" if index == 201 else "+15555556666",
                    text="Alpha Bravo Charlie" if index == 201 else "Other", type="sms", is_read=True,
                    created_at=bound, updated_at=bound)
    rows = [row(index) for index in range(1, 202)]
    requests = []
    def get(path, *, params):
        requests.append(dict(params))
        assert params["start_datetime"] == bound
        return rows[params["offset"]:params["offset"] + params["limit"]]
    monkeypatch.setattr(client.texts._http, "get", get)
    baseline = voice._outbound_sms_since(client, "number", bound)
    before = {message.id for message in baseline}
    rows.insert(0, row(202))
    current = voice._outbound_sms_since(client, "number", bound)
    assert len(baseline) == 201 and len(current) == 202
    assert {message.id for message in current} - before == {UUID(int=202)}
    assert current[-1].remote_phone_number == "+15553334444"
    assert [request["offset"] for request in requests] == [0, 200, 0, 200]
    assert all(request["start_datetime"] == bound for request in requests)
