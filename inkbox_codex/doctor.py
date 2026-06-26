"""Readiness checks for the bridge, in the spirit of `hermes inkbox doctor`."""

from __future__ import annotations

import os
import shutil
from typing import List, Tuple

try:
    from .config import read_config
except ImportError:  # pragma: no cover - direct local import/test fallback
    from config import read_config


def run_doctor() -> List[Tuple[str, bool, str]]:
    """Run every readiness check.

    Returns:
        List[Tuple[str, bool, str]]: (check name, passed, detail) rows.
    """
    cfg = read_config()
    checks: List[Tuple[str, bool, str]] = []

    checks.append(("INKBOX_API_KEY", bool(cfg.api_key), "set" if cfg.api_key else "missing"))
    checks.append(("INKBOX_IDENTITY", bool(cfg.identity), cfg.identity or "missing"))
    checks.append((
        "INKBOX_SIGNING_KEY",
        bool(cfg.signing_key) or not cfg.require_signature,
        "set" if cfg.signing_key else "missing (required for signed inbound webhooks)",
    ))

    try:
        import inkbox  # noqa: F401
        checks.append(("inkbox SDK", True, "installed"))
    except ImportError:
        checks.append(("inkbox SDK", False, "pip install 'inkbox>=0.4.10'"))

    try:
        import aiohttp  # noqa: F401
        checks.append(("aiohttp", True, "installed"))
    except ImportError:
        checks.append(("aiohttp", False, "pip install 'aiohttp>=3.9'"))

    codex_bin = shutil.which("codex")
    checks.append((
        "codex CLI",
        bool(codex_bin),
        codex_bin or "not on PATH — install Codex first",
    ))

    codex_home = os.getenv("CODEX_HOME") or os.path.join(os.path.expanduser("~"), ".codex")
    has_api_key = bool(os.getenv("OPENAI_API_KEY") or os.getenv("CODEX_API_KEY") or os.getenv("CODEX_ACCESS_TOKEN"))
    has_login = os.path.exists(os.path.join(codex_home, "auth.json"))
    checks.append((
        "Codex auth",
        has_api_key or has_login,
        "API key/token set" if has_api_key else "subscription login found" if has_login else "run codex login or set OPENAI_API_KEY/CODEX_API_KEY",
    ))

    project_dir = cfg.project_dir
    checks.append((
        "project dir",
        bool(project_dir) and os.path.isdir(project_dir),
        project_dir or "unset (defaults to cwd)",
    ))

    if cfg.api_key and cfg.identity:
        try:
            from inkbox import Inkbox

            identity = Inkbox(api_key=cfg.api_key, base_url=cfg.base_url).get_identity(cfg.identity)
            mailbox = getattr(identity, "mailbox", None)
            phone = getattr(identity, "phone_number", None)
            detail = ", ".join(filter(None, [
                getattr(mailbox, "email_address", None),
                getattr(phone, "number", None),
                "imessage" if getattr(identity, "imessage_enabled", False) else None,
            ])) or "no channels provisioned"
            checks.append(("identity reachable", True, detail))
        except Exception as exc:
            checks.append(("identity reachable", False, str(exc)))

    return checks


def print_doctor() -> int:
    """Print check results.

    Returns:
        int: Process exit code — 0 when everything passed, 1 otherwise.
    """
    rows = run_doctor()
    failed = 0
    for name, ok, detail in rows:
        mark = "✓" if ok else "✗"
        print(f" {mark} {name:<20} {detail}")
        failed += 0 if ok else 1
    return 0 if failed == 0 else 1
