# Remote Inkbox tools

Claimed identity credentials discover tools from the current Inkbox MCP catalog.
The complete input/output schemas, annotations and structured results travel with
the catalog. New remote tools do not require a plugin release. Calls use the
native session, turn and call identity, retain a durable continuation before
execution, and recheck current authorization when retrying. An expired uncertain
operation is not converted into a new send.

Admin and unclaimed credentials retain their supported local setup and tools.
An unknown credential type or failed authority check cannot authorize remote tools.
Existing configuration, unrelated tool connections and conversation history remain
in place.

## Capability ownership

| Capability | Owner and preserved behavior |
| --- | --- |
| Current catalog, including mail, conversations, contacts, notes, calls and available channel operations | Remote MCP; full schema validation and current identity authority. |
| Legacy contact lookup/list/get/create/update/delete names | Compatibility names delegated to the matching remote tool, including labeled addresses and stable mutation identity. |
| Channel send/reply helpers | Local delivery plumbing retains local-file staging, channel formatting, native source checks and delivery receipts. Canonical remote sends remain available for ordinary tool use. |
| Historical list/read helpers without an equivalent schema | Retained for their existing date filters, raw message access, conversation selectors or transcript behavior. They do not define the remote catalog. |
| Voice sessions and live-call controls | Local transport and host callbacks remain responsible for voice connections and native execution. |
| A2A callbacks and delegation | Local task context, progress, cancellation and caller routing remain responsible for delivery. Remote catalog tools are also available where supported. |
| Local secrets, local scheduling and media | Existing host/plugin implementations remain local. Remote discovery never moves local keys or files to a model-visible catalog. |

`inkbox_reply_current` declares a terminal text reply to the admitted email or SMS
message. Its destination comes from the original message; it accepts no destination
argument. Accepted or submitted-but-unconfirmed replies suppress the automatic
final text for that source. Progress and unrelated sends do not suppress it. An
uncertain helper call must be reconciled with the same text and operation; a
pre-execution rejection still allows a later reply. Other channel-specific reply
helpers retain their existing receipts and delivery behavior.

Call histories live under the plugin's private state directory, scoped to origin,
identity and session. Each session has a bounded history. Retire its history only
when the native session and any outstanding delivery have been retired; deleting
history for an active session discards retry identity.

## Codex behavior

The native stdio connection preserves tool-call metadata and structured results.
Gateway sessions refresh the catalog at an idle turn boundary after five minutes,
reconnecting the same native conversation. Standalone connections refresh when
Codex reconnects the MCP server. Native browser Ask sessions enforce approval for
all configured MCP servers without rewriting their saved connection settings;
explicit YOLO sessions retain their selected native policy. The native contract
suite qualifies Codex 0.163.0. Older hosts without stable metadata cannot execute
remote calls. `inkbox_get_email` and `inkbox_get_call` also delegate to their remote
read equivalents. Vault decryption/TOTP and durable schedules remain local.
