"""Use the configured Codex install when its JavaScript interpreter is absent."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import platform
import shutil
import sys

logger = logging.getLogger(__name__)


def resolve_codex_launcher(configured: str) -> str:
    """Resolve only an installed launcher or its own matching native executable.

    Explicit native executables and working JavaScript launchers are unchanged.
    This never installs packages or searches unrelated Codex installations.
    """
    selected = os.path.expanduser(configured or 'codex')
    if shutil.which('node'):
        return selected
    executable = shutil.which(selected)
    if not executable:
        return selected
    try:
        launcher = Path(executable).resolve(strict=True)
        if launcher.name != 'codex.js' or launcher.parent.name != 'bin':
            return selected
        with launcher.open('rb') as source:
            if source.readline(256).strip() != b'#!/usr/bin/env node':
                return selected
        package = launcher.parent.parent
        metadata = json.loads((package / 'package.json').read_text())
        if metadata.get('name') != '@openai/codex' or not metadata.get('version'):
            return selected
        if metadata.get('bin', {}).get('codex') != 'bin/codex.js':
            return selected
        arch = {'x86_64': 'x64', 'amd64': 'x64', 'aarch64': 'arm64', 'arm64': 'arm64'}.get(platform.machine().lower())
        suffix = {'linux': 'unknown-linux-musl', 'darwin': 'apple-darwin', 'win32': 'pc-windows-msvc'}.get(sys.platform)
        if arch is None or suffix is None:
            return selected
        triple = f"{'x86_64' if arch == 'x64' else 'aarch64'}-{suffix}"
        target = f'codex-{sys.platform}-{arch}'
        candidates = []
        if f'@openai/{target}' in metadata.get('optionalDependencies', {}):
            for root in (package / 'node_modules' / '@openai' / target, package.parent / target):
                try:
                    native_metadata = json.loads((root / 'package.json').read_text())
                except (OSError, ValueError):
                    continue
                if (native_metadata.get('name') in {'@openai/codex', f'@openai/{target}'}
                        and native_metadata.get('version') in {metadata['version'], f"{metadata['version']}-{sys.platform}-{arch}"}):
                    candidates.append(root / 'vendor' / triple / 'bin')
        candidates.append(package / 'vendor' / triple / 'bin')
        for directory in candidates:
            native = directory / ('codex.exe' if sys.platform == 'win32' else 'codex')
            if native.is_file() and os.access(native, os.X_OK):
                logger.info('Codex launcher needs Node; using its installed native executable')
                return str(native)
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return selected
