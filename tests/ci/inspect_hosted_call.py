"""Read-only, content-free inspection of a failed hosted call."""
import hashlib
import json
import os
import re
from inkbox import Inkbox


def words(value):
    return re.findall(r"[a-z0-9]+", str(value or "").lower())


marker = words(os.environ["EXPECTED_MARKER"])
vocabulary = set("call back caller sms text only now not do after before hang up follow request confirm speak instead reply phone following question cannot can will send won don t have able unable saved recorded registered done created complete completed wait until ask repeat words message later no yes please exactly".split())


def predicates(value):
    tokens = words(value)
    normalized = " ".join(tokens)
    return dict(word_count=len(tokens), marker=any(tokens[i:i+len(marker)] == marker for i in range(len(tokens))),
                semantic_tokens=[token for token in tokens if token in vocabulary][:90],
                cannot_send=bool(re.search(r"(?:can t|cannot|unable|won t).{0,50}(?:send|text|sms)", normalized)),
                saved=bool(re.search(r"\b(?:saved|recorded|registered|created)\b", normalized)))


try:
    for role, key_name, call_name in [("aut", "AUT_INKBOX_API_KEY", "AUT_CALL_ID"), ("driver", "REMOTE_INKBOX_API_KEY", "DRIVER_CALL_ID")]:
        client = Inkbox(api_key=os.environ[key_name], base_url=os.environ.get("INKBOX_BASE_URL") or "https://inkbox.ai")
        client.whoami()
        mailbox = client.mailboxes.list()[0]
        identity = client.get_identity(mailbox.email_address.split("@", 1)[0])
        fingerprint = hashlib.sha256(str(identity.id).encode()).hexdigest()[:16]
        call = client.calls.get(os.environ[call_name])
        transcripts = client.calls.transcripts(call.id)
        print(json.dumps(dict(role=role, identity_fingerprint=fingerprint, mode=str(getattr(call, "mode", "")),
                              authority=str(getattr(call, "hosted_agent_authority_mode", "")), reason=predicates(getattr(call, "reason", "")),
                              actions=len(getattr(call, "post_call_action_items", None) or []),
                              transcripts=[dict(local=str(row.party)=="local", remote=str(row.party)=="remote", **predicates(row.text)) for row in transcripts[:60]])))
        if role == "aut":
            config = identity.get_hosted_agent_config()
            page = client.calls.tool_invocations(call.id, limit=200)
            allowed = {"register_post_call_action", "edit_post_call_action", "delete_post_call_action", "send_sms", "send_imessage", "send_email", "hang_up_call", "refresh_counterparty_state"}
            print(json.dumps(dict(config=predicates(getattr(config, "instructions", "")), tool_count=len(page.items), has_more=page.has_more,
                                  tools=[dict(name=item.tool_name if item.tool_name in allowed else "other", status=str(item.status)) for item in page.items])))
except Exception as exc:
    print(json.dumps(dict(diagnostic_failed=True, error_type=type(exc).__name__)))
    raise SystemExit(1)
