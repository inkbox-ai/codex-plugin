# Changelog

## Unreleased

- WhatsApp now supports direct messages only. `to` accepts exactly one E.164 string; arrays, formatted numbers, and comma-separated recipients are rejected.
- Removed WhatsApp group creation, membership, routing, and group tool parameters. Unsupported inbound group messages and reactions do not start agent turns or replies.
- Retained group send intents cannot be converted to direct messages. Their original request keys remain available for read-only receipt lookup.
- Direct-message quoted replies, threads, reactions, media, and idempotent recovery remain supported. Other channels' group support is unchanged.
