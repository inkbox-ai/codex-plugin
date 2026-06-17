<h1>Codex Inkbox Bridge</h1>

<img src="assets/codex_iphone_avatar.png" alt="Codex, now with a phone" width="200" align="left">

<p>
  <br><br>
  <b>Give your Codex agent its own Inkbox identity:</b><br>
  a mailbox, iMessage, a phone number for calls and SMS, and an internet address.<br>
  Step away from the keyboard and keep working with it from anywhere.
</p>

<p>
  <code>Email</code> · <code>Calls</code> · <code>SMS / MMS</code> · <code>iMessage</code> · <code>Tunnel</code>
</p>

<br clear="left">

---

## Prerequisites

- **Codex installed and logged in.** The bridge drives a real Codex session, so the `codex` CLI has to be on the machine and authenticated — install it ([developers.openai.com/codex](https://developers.openai.com/codex)), then either sign in with a ChatGPT/Codex login or set `OPENAI_API_KEY`. `inkbox-codex doctor` checks for it.
- **Python 3.10+.** The installer finds one and builds the bridge its own venv.
- **macOS or Linux.** Boot persistence uses a systemd user unit on Linux and a launchd agent on macOS.
- **An Inkbox agent** — nothing to set up in advance; the setup wizard self-signs up for you (or takes an existing API key).

## Get started — one command

This finds a Python 3.10+, installs the bridge in its own venv, puts `inkbox-codex` on your PATH, and runs the setup wizard:

```bash
curl -fsSL https://raw.githubusercontent.com/inkbox-ai/codex-plugin/main/install.sh | bash
```

That's the whole setup. The wizard creates a fresh Inkbox agent for you (or takes an existing API key), provisions a phone number, connects iMessage, mints a webhook signing key, picks the project directory Codex works in, and offers to **keep the bridge running on every boot**. When it finishes, text/email/call your agent and it answers from a real Codex session.

The one thing to have ready: be **logged into Codex** — a ChatGPT/Codex login (via the Codex app/CLI) or `OPENAI_API_KEY` set. The installer checks this and warns if it's missing.

Flags: `--start` (launch the background gateway when done), `--no-setup` (install only). From a local checkout, run `./install.sh`. Re-running is safe.

Check it any time:

```bash
inkbox-codex doctor    # config, codex CLI/auth, identity reachability
inkbox-codex status    # is the background gateway up? where are the logs?
```

## What it does

```
you (phone)  ── SMS / iMessage / email / call ──▶  Inkbox  ──▶  tunnel  ──▶  bridge
                                                                              │
                                                                              ▼
                                                                  Codex session
                                                                  (full tool access in
                                                                   your project dir)
```

- Text, iMessage, email, or **call** your agent's Inkbox number. Each remote party gets one Codex session spanning every channel — text it on the walk home, then email it details, same conversation.
- Codex runs with full tool access in `CODEX_PROJECT_DIR`. It reads, searches, and browses freely; anything risky (running commands, editing files) is **escalated to you as a text**:

  > Codex wants to run the command: npm test
  >
  > Reply 1 (or YES) to allow once, 2 (or ALWAYS) to allow this kind of action for the rest of the session, 3 (or NO) to block it.

- When Codex needs you to pick between options (the `AskUserQuestion` tool), you get a numbered poll on whatever channel you're on, and your reply is fed back as the answer.
- Each message you send is tagged with its channel, so Codex knows whether it's on SMS, iMessage, email, or a call.
- A channel prompt is appended to Codex's system prompt so replies fit a phone: plain text, no markdown, short, jargon kept to a minimum ("saved and published the change", not "pushed to origin/main").
- Codex also gets Inkbox tools (`inkbox_send_email`, `inkbox_send_sms`, `inkbox_send_imessage`, …) so it can proactively reach you — "email me the full report" works.

## Manual install

If you'd rather not run the installer (any Python 3.10+ environment):

```bash
pip install -e .

inkbox-codex setup    # interactive wizard — writes .env for you
set -a; source .env; set +a

inkbox-codex doctor
inkbox-codex run
```

`inkbox-codex setup` walks you through everything and writes `.env`: create a fresh Inkbox agent via self-signup (or bring an existing API key), pick or create the identity, attach the Codex avatar to the agent's contact card (auto for a new self-signup agent; offered for an existing one with no avatar), provision a phone number, wait for your `START` opt-in, optionally enable OpenAI Realtime voice (validating your key), connect iMessage, mint a webhook signing key, choose the project directory, and set up autostart. Rerun it anytime to reconfigure. Prefer to wire `.env` by hand? Copy `.env.example` to `.env` and fill in `INKBOX_API_KEY`, `INKBOX_IDENTITY`, `INKBOX_SIGNING_KEY`, and `CODEX_PROJECT_DIR` yourself.

On startup the bridge opens an Inkbox tunnel, wires mail/text/iMessage webhook subscriptions and the incoming-call channel to it, and routes everything into Codex sessions.

### Running it

```bash
inkbox-codex run        # foreground (Ctrl+C to stop) — good for first runs and debugging
```

Or run it as a background daemon (PID + log under `~/.inkbox-codex/`):

```bash
inkbox-codex start      # detach and run in the background
inkbox-codex status     # is it running? where are the logs?
inkbox-codex restart    # restart it
inkbox-codex stop       # graceful stop (SIGTERM, then SIGKILL after 5s)

tail -f ~/.inkbox-codex/gateway.log
```

`start` auto-loads `.env` from the current directory, so you don't have to `source` it first. `run` is the foreground version a service manager (systemd, Docker) should supervise; `start`/`stop` are the self-contained background option.

### Start on boot

The setup wizard offers to keep the bridge running for you — either just in the background for this session, or as a service that starts on every boot. On Linux it installs a **systemd user unit** (`~/.config/systemd/user/inkbox-codex.service`) and enables it; on macOS it installs a **launchd agent**. To keep a Linux service alive while you're logged out, enable lingering once:

```bash
sudo loginctl enable-linger "$USER"
systemctl --user status inkbox-codex   # restart | stop | status
```

### Uninstall

```bash
inkbox-codex uninstall           # stop it, remove the boot service + launcher; keep config
inkbox-codex uninstall --purge   # also delete ~/.inkbox-codex (config, logs, sessions)
```

This is local-only — webhook subscriptions on the Inkbox side are left as-is; remove them in the [Inkbox Console](https://inkbox.ai/console) if you want.

Then, from your phone:

1. Text `START` to the agent's number (first time only, carrier opt-in).
2. Text it something like *"clean up the TODOs in the auth module"*.
3. Approve the permission texts as they arrive. Get the result as a text.

## How escalation works

Codex never silently runs anything destructive. The bridge starts `codex app-server` and answers its approval requests over your active Inkbox channel:

- Commands, file changes, permission-profile changes, and request-user-input prompts block the agent mid-turn while the bridge texts you a one-line plain-language summary.
- Your **next message answers the escalation** instead of starting a new turn — reply `1`/`yes`, `2`/`always` (session-scoped grant), or `3`/`no`.
- Request-user-input prompts are formatted as numbered options; reply with the number or free text.
- No reply within `INKBOX_PERMISSION_TIMEOUT_S` (default 10 min) → the request is denied or answered empty and Codex carries on as best it can.

## Sessions

Sessions are keyed by Inkbox contact, so one person = one conversation across channels. Codex session ids are persisted in `~/.inkbox-codex/sessions.json` and resumed across bridge restarts — your conversation picks up where it left off. Replies go out on the channel you last used (call replies fall back to SMS if you hang up before Codex finishes).

**Typing indicator.** While Codex works on a turn, the bridge keeps a typing indicator alive on your iMessage thread (refreshed every few seconds, since it expires) so you can see it's busy. SMS, email, and voice have no typing indicator, so this is iMessage-only.

**Delivery failures.** Outbound messages can silently fail — a carrier filters an SMS, an iMessage is declined, an email bounces. Inkbox reports these asynchronously (`text.delivery_failed`/`text.delivery_unconfirmed`, `imessage.delivery_failed`, `message.bounced`/`message.failed`). The bridge catches them and wakes the affected contact's session to tell Codex *which* message didn't land and *why*, so it can retry or reach you another way (a different channel, or a call) using its Inkbox tools. The notice runs as a side-effect turn — Codex acts via tools rather than replying on the channel that just failed — and repeat webhooks for the same message are de-duplicated so it can't loop.

**Interrupt by texting again.** Messaging the agent again while it's mid-turn works like pressing Esc in Codex and typing a new message: the running turn is interrupted, its partial answer is dropped, and Codex picks up your new message instead. (A reply while it's waiting on a permission/poll still answers that escalation — interrupting only applies while it's actively working.)

**Control commands.** A handful of slash-commands steer the conversation itself and are handled by the bridge instead of being sent to Codex (works on any channel):

- `/clear` (or `/new`) — start a fresh conversation: forgets the resumed session, tears down the client, and clears session-scoped permission grants.
- `/stop` (or `/cancel`) — interrupt the current turn and drop anything queued, keeping your conversation context intact.
- `/resume` — texts you back a numbered list of recent Codex conversations (each with a short summary and timestamp); reply with a number to reopen that one. Like `/resume` in the Codex CLI.
- `/status` — reports what the bridge is doing for you right now (working, waiting on a reply, or idle) and whether you're in a fresh or ongoing conversation. Read-only; doesn't disturb a running turn.
- `/usage` — reports Codex rate-limit windows and token summary from app-server account endpoints.
- `/health` — reports bridge health: whether Inkbox is reachable (live identity check + which channels are live), the inbound tunnel is connected, and Codex is ready to run (CLI present, authenticated).

These match only when the whole message is exactly the command, so "please /clear the cache" is still a normal turn.

**Errors.** If a turn fails, you get a short plain-language heads-up ("I hit an error while working on that and had to stop") rather than silence.

## Voice

Calls have two modes, chosen per call:

- **OpenAI Realtime** (when configured): the bridge pre-opens an OpenAI Realtime session and accepts the call in raw-media mode, so a natural, low-latency voice handles the conversation. It runs the call itself and has these tools:
  - `consult_codex` — do real work *now* in the project; runs in the *same* contact-keyed session as your SMS/iMessage and its answer is spoken back.
  - `register_post_call_action` / `edit_post_call_action` / `delete_post_call_action` — queue, change, or cancel work to run *after* you hang up.
  - `hang_up_call` — two-step (say goodbye, then end the call).

  When the call ends, queued actions run in your session (and any plain "reflect on the call" follow-up if none were queued) — so "after we hang up, open a PR and text me" actually happens. Enable it in `inkbox-codex setup` (it validates your OpenAI key live) or via the `INKBOX_REALTIME_*` env vars below.
- **Inkbox STT/TTS** (default / fallback): Inkbox auto-accepts the call and opens a WebSocket to the bridge; finalized transcripts become turns in your same session and Codex's replies are spoken back. The bridge falls back to this automatically if Realtime is off or OpenAI can't be reached (unless `INKBOX_REALTIME_FALLBACK_TO_INKBOX_STT_TTS=false`).

## Media

**Inbound.** When someone sends an MMS image, an iMessage attachment, or an email with files, the gateway downloads them to `~/.inkbox-codex/media/` (override with `INKBOX_CODEX_MEDIA_DIR`) and appends the local paths to the message, so Codex can open them with its Read tool — including viewing images. Media-only messages (no text) still wake the agent.

**Outbound.** Codex sends media with a single tool call per channel — it just passes local file paths, and the tool handles any upload-then-send round trip internally:
- **Email** — `inkbox_send_email(..., attachment_paths=[...])` (base64 inline, ~25 MB total).
- **iMessage** — `inkbox_send_imessage(..., media_path=...)` (uploaded + sent, ≤10 MB).
- **SMS/MMS** — `inkbox_send_sms(..., media_paths=[...])` (uploaded + sent; `media_urls` also accepts already-hosted URLs).

## Config reference

| Env var | Required | Default | Description |
|---|---|---|---|
| `INKBOX_API_KEY` | yes | - | Agent-scoped Inkbox API key. |
| `INKBOX_IDENTITY` | yes | - | Inkbox agent identity handle. |
| `INKBOX_SIGNING_KEY` | inbound | - | Webhook HMAC secret for signed inbound events. |
| `CODEX_PROJECT_DIR` | yes | cwd | Directory Codex works in. |
| `CODEX_MODEL` | no | CLI default | Model override for bridged sessions. |
| `INKBOX_REQUIRE_SIGNATURE` | no | `true` | Refuse unsigned inbound webhooks unless `false`. |
| `INKBOX_BASE_URL` | no | `https://inkbox.ai` | Override the Inkbox API base URL. |
| `INKBOX_PUBLIC_URL` | no | - | Public bridge URL. Omit to use an Inkbox tunnel. |
| `INKBOX_TUNNEL_NAME` | no | identity handle | Tunnel name override. |
| `INKBOX_ALLOWED_USERS` | no | - | Local allowlist (emails / E.164 numbers). Usually leave empty and use Inkbox contact rules. |
| `INKBOX_ALLOW_ALL_USERS` | no | `false` | Allow all senders admitted by Inkbox contact rules. |
| `INKBOX_BRIDGE_PORT` | no | `8767` | Local webhook server port. |
| `INKBOX_PERMISSION_TIMEOUT_S` | no | `600` | Seconds to wait for a permission/poll reply. |
| `CODEX_BIN` | no | `codex` | Codex CLI executable to run. |
| `CODEX_SANDBOX` | no | `workspace-write` | App-server thread sandbox (`read-only`, `workspace-write`, `danger-full-access`). |
| `CODEX_APPROVAL_POLICY` | no | `on-request` | Codex approval policy for bridged turns. |
| `INKBOX_REALTIME_ENABLED` | no | `false` | Use OpenAI Realtime for calls. Needs a key; off → Inkbox STT/TTS. |
| `INKBOX_REALTIME_API_KEY` | realtime | `OPENAI_API_KEY` | OpenAI key with `/v1/realtime` access. |
| `INKBOX_REALTIME_MODEL` | no | `gpt-realtime-2` | Realtime model id. |
| `INKBOX_REALTIME_VOICE` | no | `cedar` | Realtime voice name. |
| `INKBOX_REALTIME_FALLBACK_TO_INKBOX_STT_TTS` | no | `true` | Fall back to Inkbox STT/TTS if OpenAI connect fails. |

## Tools exposed to Codex

The agent reaches you (or third parties) through an in-process MCP server:

- `inkbox_whoami` — its own identity: handle, mailbox, phone, iMessage status.
- `inkbox_send_email` — send email; attach local files with `attachment_paths`.
- `inkbox_send_sms` — send SMS/MMS; attach local files with `media_paths` (or hosted `media_urls`).
- `inkbox_send_imessage` — send into an iMessage conversation; attach a local file with `media_path`.
- `inkbox_list_text_conversations` · `inkbox_get_text_conversation` — browse SMS threads and history.
- `inkbox_list_imessage_conversations` · `inkbox_get_imessage_conversation` — browse iMessage threads and history (find the `conversation_id` to send into).
- `inkbox_lookup_contact` · `inkbox_list_contacts` · `inkbox_get_contact` — resolve and read address-book contacts (reverse-lookup by email/phone, free-text search, or full record by id).
- `inkbox_create_contact` · `inkbox_update_contact` · `inkbox_export_contact_vcard` — save, edit, and export contacts (vCard 4.0). Reads and writes are filtered server-side to what this identity may see.

On a live call, the OpenAI Realtime voice agent additionally gets `consult_codex`, `register_post_call_action` / `edit_post_call_action` / `delete_post_call_action`, and `hang_up_call` — see [Voice](#voice).

## Smoke test

1. `inkbox-codex doctor` — everything green.
2. Text `START`, then text the agent; verify it replies in the same thread.
3. Ask it to do something requiring a command (e.g. "run the tests") and verify you get a permission text; reply `1` and verify the result comes back.
4. Ask it something open-ended enough to trigger a poll; reply with a number.
5. Email the agent; verify the reply lands as an email on the same thread.
6. Call the number, ask what it's working on, hang up mid-answer, and verify the tail arrives as a text.

## Development

```bash
python -m pytest
```

## Architecture notes

- **Tunnel-first inbound**: with a signing key, the gateway opens an Inkbox tunnel, reconciles mail/text/iMessage webhook subscriptions, and patches the phone number's incoming-call channel (`auto_accept` + call WebSocket) — same shape as hermes-agent-plugin.
- **Contact-keyed sessions**: webhook payloads carry resolved contacts; a single resolved contact id becomes the session key, otherwise the raw address/number does. One human, one session, every channel.
- **Escalation over the active channel**: a pending permission/poll captures the contact's next inbound message as its answer, on whichever text channel they're using.
- **Codex app-server**: each contact session owns one `codex app-server` subprocess, one Codex thread, app-server approval request handling over Inkbox, and a local stdio MCP server for the Inkbox tools.
