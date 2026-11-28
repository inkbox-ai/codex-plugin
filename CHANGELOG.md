# Changelog

## 0.2.46 (unreleased)

- Fail an inbound A2A task with a generic reason when its Codex turn fails before any outcome was sent, instead of leaving the task working. Raw error details are not sent to the caller. The failure is retried briefly, and if it still cannot be sent the task stays eligible for catch-up when a self-hosted bridge restarts.
- Explain how to set `INKBOX_CODEX_VAULT_KEY` for an agent hosted by Inkbox: save it as a Secret in the identity's Env vars tab, since a key exported in a shell does not reach the agent.

## 0.2.45 (unreleased)

- Wait for Slack voice-note transcripts like SMS, iMessage, and WhatsApp, and use them instead of transcribing Slack or WhatsApp audio locally with the realtime voice credential.
- Wake the agent once for a Slack message delivered under several event categories.

## 0.2.44 (unreleased)

- Add `INKBOX_DELIVERY_MODE=local`: no tunnel or webhook registration, a loopback-only listener, and 2 MiB request bodies with unchanged signature verification.
- Handle `inbound.received` events, including hosted deliveries: each event is stored durably before it is acknowledged, deduplicated by `delivery_id` (retries run once, replays run again), and runs as an agent turn or a script from a local queue that resumes after a restart. Pending events, running scripts and script retry waits count as activity; pending failure reports do not. Scripts run up to 5 times (retries after 10 s, 30 s, 2 min, and 10 min; 120 s timeout each); output is read continuously, keeping up to 64 KiB of stdout and the last 4 KiB of stderr. An agent turn runs at most once: a failed or interrupted turn is reported, never repeated, and a failed turn on script output does not run the script again. Failures are reported to Inkbox with the `delivery_id`; the report is retried with backoff (at most an hour apart) while the bridge runs and again on every start until it is recorded. A 2xx, a 409 (stale delivery) or any other 4xx except 408 and 429 ends the retries. A valid Inkbox signature is always required. Each delivery's body and request metadata are written to `~/.inkbox/inbound/events/<delivery_id>/` (`body` and `request.json`, private to the user, deleted after 7 days by a sweep at start and every hour), so a replay never overwrites files from an earlier turn, and the agent message points at that directory; a UTF-8 body (or script output) up to 4 KiB is also shown inline, a larger or binary one only by reference, always under the untrusted-data label. The message shows only the bare media type of the content type (`unknown` if malformed); the full value stays in `request.json`. If the files cannot be written, the body is inlined instead, truncated at 64 KiB. A script run cut off by a restart counts toward its 5 runs. The event finishes when the agent turn ends, and a failed turn is reported.
- Inbound events from different sources never share a conversation: each source's turns run in a session keyed by its id, so sources with the same name stay apart and a renamed source keeps its history.
- At most 100 inbound events (64 MiB) wait locally; a new delivery beyond that is refused with a retryable 503 before it is acknowledged, while a retry of a delivery already stored still gets its receipt. At most 4 inbound scripts run at once.
- The 30-minute inbound turn limit counts from the turn's start, not its wait behind earlier turns, and an expired turn is interrupted instead of running on after it is reported failed.
- Require Inkbox SDK 0.7.41; setup installs and checks for it.

## 0.2.43 (unreleased)

- Wait up to 60 seconds for incoming voice-note transcripts, then read the message once before continuing with its original attachments.
- Register transcription-completion notifications automatically and label generated voice-note text distinctly from typed messages.
- Retain pending voice-note receipts and their original deadline across gateway restarts without waking twice for late completion notifications.

## 0.2.42

- Retain managed inbound work when final reply delivery cannot be confirmed.

## 0.2.41

### Changed
- Require Inkbox SDK 0.7.39 for durable messaging and remote tool handling.

## 0.2.40

- Make managed diagnostics use a recent runtime health report instead of terminal signing-key or ordinary webhook settings. Keep standalone checks and report unavailable runtime evidence.

## 0.2.39

- Accept empty MCP continuation input requests through the pinned SDK revision.

- Keep contact and terminal-reply MCP arguments within the advertised schema while retaining durable operation identity and retry receipts.

## 0.2.38

- Retry incomplete live-test call cleanup and verify calls have ended between tests.

## 0.2.37

- Deduplicate incoming message and call-completion retries by event ID even when each delivery has a new request ID.
- Ignore the standard Inkbox email footer when handling approval and cancellation replies.

## 0.2.36

- Reuse valid tunnel certificates across reconnects and renew mismatched or expired state with Inkbox SDK 0.7.38 or newer.
- Bound tunnel startup and reconcile cancelled attempts before reconnecting, including late listener cleanup.

## 0.2.35

- Report durable managed message and chat completion timestamps so supervisors can count inactivity after work finishes. Status reads do not extend the idle window.

## 0.2.34

- Resolve call recipients from explicit numbers or matching contacts; ask when the recipient is unknown.
- Preserve the attempted recipient and provide lookup guidance when a shared-line call has no matching connection.

## 0.2.33

- Run saved scheduled tasks without repeated tool-approval prompts, with approvals scoped to each execution.
- Record failed scheduled tool calls and requests for missing input as failures instead of successful runs.

## 0.2.32

- Show MCP tool approval requests in direct chat and honor one-time approval, denial, and cancellation.

## 0.2.31

- Forward an optional model selection to the native chat turn without changing the default for other conversations.

## 0.2.30

- Preserve cancellation and expired interactions across pending responses, bound oversized chat output, and recover confirmed completed work without replay.
- Report known model-access failures with actionable codes and avoid reopening dormant conversations during observation.
- Include native execution timing and output identities in schedule receipts.

## 0.2.29

- Add supervised direct chat with streamed text, tool activity, approvals, questions, cancellation, and downloadable file references.
- Retain control receipts across reconnects and expose bounded conversation observations without rerunning work.
- Add revisioned native schedule management and explicit timezone previews.

## 0.2.28

- Retry early schedule deliveries, enforce the start deadline when queued work begins, and synchronize changed schedules in bounded batches.

- Coalesce delayed recurring schedules, report definite dispatch rejections, and keep failed preflight checks from pinning unrelated work.
- Bound externally timed startup to 15 minutes unless native catch-up is enabled.
- Keep stopped uncertain work isolated from unrelated activity and configuration changes.
- Add durable, bounded webhook acceptance and retain uncertain outcomes without automatic replay.
- Preserve timely schedule admission across cold starts, serialize conversation admission, and separate completed receipts from pending capacity.
- Limit hosted startup work to authorized deliveries and support explicit quarantine without replay.
- Support externally timed native schedules, registration acknowledgements and cancellation without duplicate local execution. New cron jobs use the configured timezone, defaulting to UTC.
- Expose supervised activity, configuration draining and read-only conversation access with one owner per native thread.
- Apply hosted instructions and shared connected tools while preserving standalone behavior.

## Unreleased

- Mask generated webhook secrets in workflow output.

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
- Incoming inline replies show the accessible quoted author, text, and attachment labels alongside the source and parent message IDs, including Companion context. Unavailable targets are explicit; temporary quote lookup failures preserve the current message with a `temporarily_unavailable` marker and parent ID rather than retrying admission. Quoted mentions never count as mentions in the current message.
- Retained group send intents cannot be converted to direct messages. Their original request keys remain available for read-only receipt lookup.
- Direct-message quoted replies, threads, reactions, media, and idempotent recovery remain supported. Other channels' group support is unchanged.
