# Changelog

## Unreleased

- Add 14 agent-scoped tools for email reads/replies/forwarding/read state, individual SMS/iMessage reads, iMessage reactions, call inspection/activity/hangup, and send-key lookup.
- Register WhatsApp tools only for identities with WhatsApp configured; sending restrictions remain enforced when called.
- Expand email recipients and body formats, SMS groups, and history pagination/date filters. Fix iMessage history to use the SDK message-list method.
- Email/SMS/iMessage send results now return `accepted`, actual status, and recovery keys instead of `sent: true`. SDK notices wrap results as `{data, notices}` when present.
- Preserve speaker phone numbers and connection history in post-call context.

- Ordinary WhatsApp replies are plain messages; use explicit quotes selectively. Saved sends retain their original payload and request key.
- Captionless and consecutive WhatsApp images reach native image input; voice notes use the existing transcription credential when available.
- Require the published Inkbox SDK >=0.7.27,<1.0.0 in plugin 0.2.26.

- WhatsApp now supports direct messages only. `to` accepts exactly one E.164 string; arrays, formatted numbers, and comma-separated recipients are rejected.
- Removed WhatsApp group creation, membership, routing, and group tool parameters. Unsupported inbound group messages and reactions do not start agent turns or replies.
- Retained group send intents cannot be converted to direct messages. Their original request keys remain available for read-only receipt lookup.
- Direct-message quoted replies, threads, reactions, media, and idempotent recovery remain supported. Other channels' group support is unchanged.
