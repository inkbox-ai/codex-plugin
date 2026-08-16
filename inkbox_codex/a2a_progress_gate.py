"""Cross-process fencing for A2A progress and explicit outcomes."""

from __future__ import annotations

import fcntl
import hashlib
import os
from pathlib import Path
from typing import IO


def _gate_paths(task_id: str) -> tuple[Path, Path]:
    root = Path(os.getenv("INKBOX_CODEX_HOME") or (Path.home() / ".inkbox-codex"))
    root = root / "a2a_progress_gates"
    root.mkdir(parents=True, exist_ok=True)
    root.chmod(0o700)
    digest = hashlib.sha256(task_id.encode()).hexdigest()
    return root / f"{digest}.lock", root / f"{digest}.fenced"


def acquire_a2a_progress_gate(task_id: str) -> IO[bytes]:
    """Acquire the stable task lock shared by the gateway and tool process."""
    lock_path, _ = _gate_paths(task_id)
    descriptor = lock_path.open("a+b")
    lock_path.chmod(0o600)
    fcntl.flock(descriptor.fileno(), fcntl.LOCK_EX)
    return descriptor


def try_acquire_a2a_progress_gate(task_id: str) -> IO[bytes] | None:
    """Acquire a task gate without blocking, or return ``None`` when busy."""
    lock_path, _ = _gate_paths(task_id)
    descriptor = lock_path.open("a+b")
    lock_path.chmod(0o600)
    try:
        fcntl.flock(descriptor.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        descriptor.close()
        return None
    return descriptor


def release_a2a_progress_gate(descriptor: IO[bytes]) -> None:
    """Release and close a task gate returned by ``acquire_a2a_progress_gate``."""
    try:
        fcntl.flock(descriptor.fileno(), fcntl.LOCK_UN)
    finally:
        descriptor.close()


def a2a_progress_is_fenced(task_id: str) -> bool:
    """Return whether an explicit outcome has fenced progress for this task."""
    _, fence_path = _gate_paths(task_id)
    return fence_path.exists()


def a2a_progress_fence_owner(task_id: str) -> str:
    """Return the message key that owns the durable fence, when available."""
    _, fence_path = _gate_paths(task_id)
    try:
        return fence_path.read_text().strip()
    except (FileNotFoundError, OSError):
        return ""


def fence_a2a_progress(task_id: str, message_id: str) -> None:
    """Persist a fence while the caller holds the task gate."""
    _, fence_path = _gate_paths(task_id)
    tmp = fence_path.with_suffix(".tmp")
    tmp.write_text(str(message_id or "") + "\n")
    tmp.chmod(0o600)
    os.replace(tmp, fence_path)
    fence_path.chmod(0o600)


def clear_a2a_progress_fence(task_id: str) -> None:
    """Clear a prior input-request fence for a genuine caller follow-up."""
    _, fence_path = _gate_paths(task_id)
    fence_path.unlink(missing_ok=True)
