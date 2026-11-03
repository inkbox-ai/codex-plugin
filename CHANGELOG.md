# Changelog

## Unreleased

- Ordinary WhatsApp replies are plain messages; use explicit quotes selectively. Saved sends retain their original payload and request key.
- Captionless and consecutive WhatsApp images reach native image input; voice notes use the existing transcription credential when available.
- Use the published Inkbox SDK 0.7.26 or newer.

- WhatsApp now supports direct messages only. `to` accepts exactly one E.164 string; arrays, formatted numbers, and comma-separated recipients are rejected.
- Removed WhatsApp group creation, membership, routing, and group tool parameters. Unsupported inbound group messages and reactions do not start agent turns or replies.
- Retained group send intents cannot be converted to direct messages. Their original request keys remain available for read-only receipt lookup.
- Direct-message quoted replies, threads, reactions, media, and idempotent recovery remain supported. Other channels' group support is unchanged.
