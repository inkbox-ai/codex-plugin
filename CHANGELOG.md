# Changelog

## Unreleased

- Support WhatsApp Companion groups through the shared ordered inbox, complete scoped history, native mention gates, and Safe/Relaxed response modes. Revalidate the exact sponsorship before turns and sends without granting direct-message access.

- Recognize verified native WhatsApp @mentions in group mention mode without interpreting numeric text as identity evidence. Preserve structured mentions of other participants as context, without inventing unknown phone numbers. Render verified mention tokens in direct and group chats using name-only contact lookups, with phone or unresolved fallbacks while retaining original text.

- Reduce scheduled full-stack CI to once daily at noon Pacific Time, including daylight saving time changes.
- Pin CI host tests and live suites to Codex 0.163.0 instead of the moving alpha channel.
- Add 14 agent-scoped tools for email reads/replies/forwarding/read state, individual SMS/iMessage reads, iMessage reactions, call inspection/activity/hangup, and send-key lookup.
- Register WhatsApp tools only for identities with WhatsApp configured; sending restrictions remain enforced when called.
- Expand email recipients and body formats, SMS groups, and history pagination/date filters. Fix iMessage history to use the SDK message-list method.
- Email/SMS/iMessage send results now return `accepted`, actual status, and recovery keys instead of `sent: true`. SDK notices wrap results as `{data, notices}` when present.
- Preserve speaker phone numbers and connection history in post-call context.

- Ordinary WhatsApp replies are plain messages; use explicit quotes selectively. Saved sends retain their original payload and request key.
- Captionless and consecutive WhatsApp images reach native image input; voice notes use the existing transcription credential when available.
- Require the published Inkbox SDK >=0.7.29,<1.0.0.

- WhatsApp supports direct messages and existing inbound group conversations. `to` accepts exactly one E.164 string; arrays, formatted numbers, and comma-separated recipients are rejected.
- Group conversations have isolated sessions and support Automatic or Mention required mode, media, quoted replies, and reactions. Group names are display metadata; replies use conversation IDs. Group creation and membership changes remain unsupported.
- Group routing uses the required SDK’s preserved group metadata.
- Incoming inline replies show the accessible quoted author, text, and attachment labels alongside the source and parent message IDs, including Companion context. Unavailable targets are explicit; quoted mentions never count as mentions in the current message.
- Retained group send intents cannot be converted to direct messages. Their original request keys remain available for read-only receipt lookup.
- Direct-message quoted replies, threads, reactions, media, and idempotent recovery remain supported. Other channels' group support is unchanged.
