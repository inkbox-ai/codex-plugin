"""Isolated, Slack-only foreground harness for a configured Inkbox identity."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import shlex

from .config import BridgeConfig, inkbox_client_kwargs
from .gateway import InkboxGateway, web
from .slack import slack_resource


def read_env_file(path: Path) -> dict[str, str]:
    """Read literal KEY=value entries without executing shell expansions."""
    values = {}
    for line in path.expanduser().read_text().splitlines():
        parts = shlex.split(line, comments=True)
        if parts and parts[0] == "export":
            parts = parts[1:]
        if parts and "=" in parts[0]:
            key, value = parts[0].split("=", 1)
            values[key] = value
    return values


class SlackHarness(InkboxGateway):
    """Reuse the gateway while leaving all non-Slack channel registrations alone."""

    def _patch_identity_objects(self) -> None:
        self._reconcile_slack()

    async def _catch_up_a2a_tasks(self) -> None:
        pass

    async def _recover_hosted_call_completions(self) -> None:
        pass

    async def _handle_webhook(self, request):
        try:
            envelope = json.loads(await request.read())
        except (ValueError, TypeError):
            return web.Response(status=400, text="invalid json")
        if not isinstance(envelope, dict):
            return web.Response(status=400, text="invalid json")
        if not str(envelope.get("event_type") or "").startswith("slack.") or envelope.get("companion"):
            return web.json_response({"ok": True, "ignored": "non-slack"})
        return await super()._handle_webhook(request)


def preflight(client, identity_handle: str):
    caller = client.whoami()
    identity = client.get_identity(identity_handle)
    if getattr(caller, "scope", None) != f"agent_identity:{identity.id}":
        raise ValueError("Use the API key scoped to the requested identity")
    connections = slack_resource(client).list_connections(identity.id)
    return identity, connections


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials-file", type=Path, required=True)
    parser.add_argument("--api-key-env", default="INKBOX_API_KEY")
    parser.add_argument("--signing-env-file", type=Path)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--project-dir", type=Path, default=Path.cwd())
    parser.add_argument("--port", type=int, default=8777)
    parser.add_argument("--public-url", default="")
    parser.add_argument("--allow-user", action="append", default=[], help="Workspace-qualified T_ID:U_ID")
    parser.add_argument("--run", action="store_true", help="Start the receiver and register its Slack subscription")
    args = parser.parse_args(argv)
    try:
        values = read_env_file(args.credentials_file)
        api_key = values.get(args.api_key_env)
        if not api_key:
            raise ValueError(f"Missing {args.api_key_env} in the selected credentials file")
        state = args.state_dir.expanduser().resolve()
        if state == (Path.home() / ".inkbox-codex").resolve():
            raise ValueError("Choose a separate harness state directory")
        profile = {"base_url": args.base_url.rstrip("/"), "identity": args.identity}
        profile_path = state / "slack-harness.json"
        if state.exists() and any(state.iterdir()):
            if not profile_path.exists() or json.loads(profile_path.read_text()) != profile:
                raise ValueError("The state directory belongs to a different or unknown profile")
        from inkbox import Inkbox

        client = Inkbox(**inkbox_client_kwargs(api_key, profile["base_url"]))
        identity, connections = preflight(client, args.identity)
        print(json.dumps({
            **profile, "identity_verified": True,
            "connections": [{"id": str(c.id), "workspace": c.workspace_name, "status": c.status}
                            for c in connections.connections],
        }, indent=2))
        if not args.run:
            return 0
        signing_values = read_env_file(args.signing_env_file) if args.signing_env_file else values
        signing_key = signing_values.get("INKBOX_SIGNING_KEY", "")
        if not signing_key:
            raise ValueError("Provide the existing identity's INKBOX_SIGNING_KEY; the harness never rotates it")
        for field, expected in (("INKBOX_BASE_URL", profile["base_url"]), ("INKBOX_IDENTITY", args.identity)):
            if signing_values.get(field, expected).rstrip("/") != expected:
                raise ValueError("The signing configuration belongs to a different identity or API URL")
        state.mkdir(mode=0o700, parents=True, exist_ok=True)
        state.chmod(0o700)
        profile_path.write_text(json.dumps(profile) + "\n")
        os.environ["INKBOX_CODEX_HOME"] = str(state)
        os.environ["INKBOX_SLACK_ENABLED"] = "1"
        cfg = BridgeConfig(
            api_key=api_key, identity=args.identity, signing_key=signing_key,
            base_url=profile["base_url"], project_dir=str(args.project_dir.resolve()),
            slack_enabled=True, port=args.port, host="127.0.0.1", public_url=args.public_url,
            tunnel_name=args.identity, allowed_users=args.allow_user,
        )
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

        async def run():
            gateway = SlackHarness(cfg)
            try:
                await gateway.run()
            finally:
                await gateway._cleanup()

        asyncio.run(run())
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        # Avoid echoing a request, payload, or credential in harness diagnostics.
        if isinstance(exc, ValueError):
            print(str(exc))
        else:
            print(f"Harness preflight/run failed: {type(exc).__name__} (HTTP {getattr(exc, 'status_code', 'n/a')}).")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
