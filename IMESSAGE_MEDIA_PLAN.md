# Media over the bridge — plan (issue #3)

## Why media doesn't work today

All three inbound handlers in `gateway.py` (`_on_imessage_received`,
`_on_text_received`, `_on_mail_received`) read **only the text body**. Two
concrete bugs:

1. **Media is never extracted.** Nothing reads the attachment URLs off the
   webhook payload, so images/audio/files never reach Codex.
2. **Attachment-only messages are thrown away.** A photo sent with no caption
   has empty text, so it hits the `if not text: ignored "empty"` guard and is
   dropped before it's even queued.

So when you send me a picture, the gateway silently discards it — I never see
it.

## What the Inkbox SDK already gives us

- **Inbound iMessage:** `message.media` is a `list[IMessageMediaItem]`, each with
  a `.url` (and content type). SMS/MMS media and email attachments are
  analogous.
- **Outbound iMessage:** `identity.upload_imessage_media(content, filename,
  content_type)` → a reusable `media_url`, then `send_imessage(..., media_urls=[url])`.
- **Outbound email:** `send_email(attachments=[{filename, content, content_type}])`.

## How Codex ingests media

Codex is multimodal and its built-in Read tool natively reads images and
PDFs from local file paths. So the simplest, most robust approach is: download
each attachment to a scratch file, then reference the local path in the turn
text. Codex reads it with its own Read tool — no need to hand-build image
content blocks for the SDK.

## Plan — inbound (the issue)

1. **Stop dropping media-only messages.** Relax the empty guard so a message
   with text *or* media proceeds.
2. **Extract attachments** in each handler into a small list of
   `(url, content_type, filename)` — `message.media` for iMessage, MMS media for
   SMS, attachments for email.
3. **Download to a per-contact scratch dir** under the project
   (e.g. `<project>/.inkbox-codex-media/<chat>/<ts>-<name>`), with size/count
   caps and a content-type allowlist (image/*, audio/*, pdf); skip + warn on
   failure or unsupported types.
4. **Plumb attachments through** `handle_inbound(... attachments=[...])` and
   `frame_inbound`, so the turn tells Codex e.g.
   `[iMessage from +1… — 1 image attached: /path/to/file.jpg]` and it reads it.
5. **Clean up** scratch files after the turn (or a TTL sweep); never commit them
   (add the dir to `.gitignore`).

## Plan — outbound (smaller follow-on)

6. Let Codex send a file back: extend `inkbox_send_imessage` with an optional
   local-path/URL argument that uploads via `upload_media` then sends with
   `media_urls`. Same idea for email attachments.

## Open questions / decisions

- **Download auth:** are the media URLs public/signed, or do they need the API
  key? Verify; prefer an SDK download helper if one lands, else an authenticated
  GET.
- **Limits:** max size and count per message; what to tell the human when a type
  is unsupported or too big.
- **Privacy/cleanup:** attachments touch disk in the project dir — cap, TTL, and
  delete.

## Done when

Inbound attachments are downloaded and referenced into the session, and
attachment-only messages are no longer discarded. (Outbound media tracked as a
follow-on.)
