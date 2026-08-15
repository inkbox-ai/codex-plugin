# Live CI

These live Actions exercise the installed plugin against live Inkbox identities. Each component Action supports reusable and manual execution. Tests use current-run markers or pre-request snapshots so stale records cannot pass.

## Full stack e2e

Runs the component Actions in sequence for ready same-repository pull requests, manual dispatches, and successful canary runs on `main`. Its voice invocation includes the otherwise optional inbound scenario.

### `full-stack`

**Proves:** Every live suite passes as one required gate. **Flow:** 1. Run channels. 2. Run Agent2Agent. 3. Run voice. 4. Run external events. 5. Fail unless every suite succeeded.

## Live — Agent2Agent

Runs all five scenarios serially and requires both configured identities plus real model access.

### `inbound-single`

**Proves:** The plugin completes one inbound A2A task. **Flow:** 1. Send a tagged task. 2. Wait for completion. 3. Require the tag in task history.

### `inbound-multi`

**Proves:** An inbound task can request and consume follow-up input. **Flow:** 1. Send a tagged task. 2. Wait for `input-required`. 3. Reply in the same task. 4. Require both tags at completion.

### `inbound-progress`

**Proves:** A long-running inbound task acknowledges pickup, publishes ordered nonterminal progress on schedule, and completes with the expected result. **Flow:** 1. Send a two-minute calculation task. 2. Require acknowledgement within 30 seconds. 3. Require two progress messages about one minute apart. 4. Require the tagged final calculation.

### `outbound-single`

**Proves:** The agent delegates work without completing its outer task early. **Flow:** 1. Request delegation. 2. Find the tagged worker task. 3. Complete it remotely. 4. Require its result in the outer completion.

### `outbound-multi`

**Proves:** Delegation preserves a worker's input round trip. **Flow:** 1. Start a delegated task. 2. Receive its input request. 3. Reply through the agent. 4. Complete the worker. 5. Require its result in the outer task.

## Live — agent channels (email + SMS)

Both matrix legs collect `tests/live`; test gates select matching channel coverage and skip unrelated voice and external-event modules. The `mock` leg proves transport, the `real` leg proves reasoning, and contact mutation remains opt-in.

### `test_email_reachability`

**Proves:** Email reaches the gateway and produces a real reply. **Flow:** 1. Send a unique nonce in a new thread. 2. Wait for a fresh reply. 3. Reject error text. 4. Require the nonce and mock marker.

### `test_basic_reply` (email)

**Proves:** The real agent answers email. **Flow:** 1. Snapshot inbound mail. 2. Request a short acknowledgement. 3. Wait for a fresh matching reply. 4. Require non-empty content.

### `test_reports_own_identity` (email)

**Proves:** Email context exposes the agent's configured identity. **Flow:** 1. Read the authoritative handle, email, and phone. 2. Ask the agent for them. 3. Require those values in a fresh reply.

### `test_reports_sender_details` (email)

**Proves:** The agent can resolve the email sender's contact. **Flow:** 1. Establish the sender contact. 2. Ask for its details. 3. Require its available stored name, email, and phone.

### `test_aware_of_inkbox_tools` (email)

**Proves:** A majority of the registered contact tools are model-discoverable. **Flow:** 1. Read tool names from the plugin source. 2. Ask the agent to search for contact tools. 3. Require at least three known contact-tool names.

### `test_contact_crud_tool_use` (opt-in)

**Proves:** The agent can create and update a temporary contact. **Flow:** 1. Create a tagged contact through the agent. 2. Verify it through the API. 3. Update and verify it. 4. Remove the fixture during cleanup. This test is collected but skipped unless contact mutation is explicitly enabled.

### `test_sms_reachability`

**Proves:** SMS reaches the gateway and produces a deterministic reply. **Flow:** 1. Settle prior messages. 2. Send a unique request. 3. Wait for a fresh reply from the agent number. 4. Require the mock marker.

### `test_sms_basic_reply`

**Proves:** The real agent answers SMS. **Flow:** 1. Settle prior messages. 2. Send a tagged acknowledgement request. 3. Wait for the correlated reply. 4. Reject empty or error content.

### `test_sms_reports_own_identity`

**Proves:** SMS context exposes the agent's configured email. **Flow:** 1. Read the agent email. 2. Request its email and phone by SMS. 3. Require the email in the fresh reply.

### `test_sms_reports_sender_details`

**Proves:** The agent can use the SMS sender's contact card. **Flow:** 1. Resolve the sender contact. 2. Ask who the sender is. 3. Require its stored name when present. The test skips when no contact exists.

### `test_sms_aware_of_inkbox_tools`

**Proves:** The real agent can name registered Inkbox tools over SMS. **Flow:** 1. Read registered tool names from the plugin source. 2. Ask for three tool names. 3. Require at least two known names.

### `test_sms_retry_after_carrier_delivery_failure`

**Proves:** An asynchronous SMS failure wakes the agent for recovery. **Flow:** 1. Establish the conversation. 2. Submit an authenticated failure event. 3. Require the delivery-failure wake marker. 4. Observe any follow-up SMS as best-effort only.

### `test_sms_retry_after_internal_spam_block`

**Proves:** An unsafe-format request produces either a rejection wake or a deliverable fallback. **Flow:** 1. Request a response likely to be rejected. 2. Inspect only new gateway output and SMS. 3. Require either the rejection wake or a non-empty fallback. Deterministic unit tests own the retry policy.

### `test_email_request_gets_sms_response`

**Proves:** An email can request an SMS response. **Flow:** 1. Snapshot inbound SMS IDs. 2. Email a unique token. 3. Wait for a fresh SMS from the agent. 4. Require the token.

### `test_sms_request_gets_email_response`

**Proves:** An SMS can request an email response. **Flow:** 1. Snapshot inbound email IDs. 2. Text a unique token. 3. Wait for fresh mail from the agent. 4. Require the token in its subject or body.

### `test_email_request_gets_call`

**Proves:** An email can request a compliant outbound call. **Flow:** 1. Snapshot both call histories. 2. Email the call request. 3. Require one fresh leg in each history with timestamps within 60 seconds. 4. Verify disabled voicemail detection on the agent-owned leg.

### `test_sms_request_gets_call`

**Proves:** An SMS can request a compliant outbound call. **Flow:** 1. Snapshot both call histories. 2. Text a unique call request. 3. Require one fresh leg in each history with timestamps within 60 seconds. 4. Verify disabled voicemail detection on the agent-owned leg.

## Live — voice calls (Voice AI + Realtime + Inkbox TTS/STT)

Requires both configured identities and real model access. Outbound scenarios run by default; inbound runs only when `include_inbound` is enabled, including from the full-stack Action.

### `test_inbound_call_inkbox_tts_stt`

**Proves:** Inbound client-media calling uses Inkbox speech services. **Flow:** 1. Place a call with voicemail detection disabled. 2. Require two-way speech. 3. Verify persisted call policy and speech mode. 4. Hang up.

### `test_outbound_call_realtime`

**Proves:** A message-triggered callback uses Realtime. **Flow:** 1. Snapshot both call histories. 2. Request a callback. 3. Require one fresh time-correlated leg in each history and two-way speech. 4. Verify Realtime flags and disabled voicemail detection. 5. Hang up.

### `test_outbound_call_voice_ai_and_post_call_completion`

**Proves:** Hosted calling reaches post-call reconciliation. **Flow:** 1. Snapshot both call histories and sender-side SMS rows. 2. Request a hosted call. 3. Select fresh records within 60 seconds and verify hosted mode, reason, saved authority, and disabled voicemail detection. 4. Require two-way speech, caller intent, and a matching open action before hangup. 5. Require completed reconciliation and exactly one fresh caller-targeted SMS containing the marker after case/punctuation normalization; unrelated fresh sends are outside this count.

## Live — external events (escalation → agent calls driver)

Requires live credentials, a real model, the relevant signing secrets, and gateway output for exact session correlation. These tests prove event admission and session startup; they do not require an event-triggered call.

### `test_signed_external_event_starts_agent_session`

**Proves:** An authenticated external event starts an agent session. **Flow:** 1. Create a unique event. 2. Submit it to the gateway. 3. Require acceptance. 4. Wait for the exact session-start marker.

### `test_forged_github_signature_is_rejected_before_agent_wakes`

**Proves:** Invalid event signatures fail before agent execution. **Flow:** 1. Submit a uniquely tagged event with an invalid signature. 2. Require rejection. 3. Require no matching gateway wake.

### `test_valid_github_signature_wakes_agent_session`

**Proves:** A valid repository event wakes the agent. **Flow:** 1. Sign a unique event. 2. Submit it to the gateway. 3. Require acceptance. 4. Wait for its exact session marker.
