from inkbox_codex.prompts import build_channel_prompt, frame_inbound, strip_markdown


def test_frame_inbound_tags_channel_and_sender():
    assert frame_inbound("imessage", {"sender": "+15551234567"}, "hi").startswith(
        "[inkbox:imessage from=+15551234567 | contact=unknown_in_inkbox]"
    )
    assert frame_inbound("sms", {"sender": "+15551234567"}, "yo").startswith(
        "[inkbox:sms from=+15551234567 | contact=unknown_in_inkbox]"
    )
    # Email carries its subject into the tag.
    framed = frame_inbound("email", {"sender": "a@b.com", "subject": "Deploy?"}, "body")
    assert framed.startswith("[inkbox:email from=a@b.com subject='Deploy?'")
    # Voice has no sender tag but flags speech.
    assert frame_inbound("voice", {}, "what's up").startswith("[inkbox:voice_call")
    outbound_voice = frame_inbound(
        "voice",
        {
            "outbound_purpose": "talk about soccer and the World Cup",
            "outbound_context": "Dima asked by iMessage for this call.",
            "outbound_scheduled_by": "Dima",
        },
        "why are you calling?",
    )
    assert "Outbound call reason: talk about soccer and the World Cup" in outbound_voice
    assert "Outbound call scheduled by: Dima" in outbound_voice
    assert "Outbound call background: Dima asked by iMessage for this call." in outbound_voice
    assert outbound_voice.startswith("[inkbox:voice_call")
    # The body always survives intact.
    assert frame_inbound("imessage", {"sender": "x"}, "the message").endswith("the message")


def test_frame_inbound_includes_contact_marker():
    framed = frame_inbound(
        "imessage",
        {
            "sender": "+15167251294",
            "conversation_id": "imconv-1",
            "contact": {
                "id": "contact-dima",
                "name": "Dima",
                "company": "Inkbox",
                "emails": ["dima@inkbox.ai"],
                "phones": ["+15167251294"],
                "job_title": "ignored",
                "notes": "ignored",
            },
        },
        "hi",
    )
    assert framed.startswith(
        "[inkbox:imessage from=+15167251294 conversation_id=imconv-1 | "
        "contact_id=contact-dima contact_name='Dima' contact_company='Inkbox'"
    )
    assert "contact_emails=['dima@inkbox.ai']" in framed
    assert "contact_phones=['+15167251294']" in framed
    assert "job_title" not in framed
    assert "notes" not in framed


def test_channel_prompt_mentions_identity_and_dir():
    text = build_channel_prompt(
        project_dir="/srv/app",
        identity_handle="dev-agent",
        email_address="dev-agent@inkbox.ai",
        phone_number="+15551234567",
    )
    assert "/srv/app" in text
    assert "dev-agent@inkbox.ai" in text
    assert "jargon" in text.lower()
    assert "AskUserQuestion" in text
    assert "NEVER call an Inkbox send tool to answer the current inbound message" in text
    assert "include the literal requested value" in text
    assert "You MUST call inkbox_place_call in that same turn" in text
    assert "Do not merely text that" in text
    assert "replace the call with an SMS reply" in text
    assert "Codex can read and write Inkbox contacts" in text
    assert "inkbox_create_contact" in text
    assert "inkbox_update_contact" in text
    assert "inkbox_delete_contact" in text
    assert "vCard export/import" in text


def test_strip_markdown():
    raw = "**Done!** Ran `npm test`:\n```\nall green\n```\nSee [docs](https://x.y)."
    flat = strip_markdown(raw)
    assert "**" not in flat
    assert "`" not in flat
    assert "docs (https://x.y)" in flat
