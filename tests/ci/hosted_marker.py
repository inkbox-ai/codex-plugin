#!/usr/bin/env python3
"""Create a compact, speech-safe marker for one workflow attempt."""

from __future__ import annotations

import hashlib
import sys

WORDS = (
    "banana",
    "elephant",
    "pineapple",
    "alligator",
    "motorcycle",
    "umbrella",
    "dinosaur",
    "potato",
    "computer",
    "volcano",
    "airplane",
    "butterfly",
    "kangaroo",
    "octopus",
    "calendar",
    "chocolate",
    "hospital",
    "library",
    "sandwich",
    "telescope",
)


def marker_for(run_id: str, attempt: str) -> str:
    """Map the full run token to three distinct ordinary words."""
    digest = hashlib.sha256(f"{run_id}-{attempt}".encode()).digest()
    available = list(WORDS)
    chosen = []
    value = int.from_bytes(digest, "big")
    for _ in range(3):
        value, index = divmod(value, len(available))
        chosen.append(available.pop(index))
    return " ".join(chosen)


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: hosted_marker.py RUN_ID RUN_ATTEMPT")
    print(marker_for(sys.argv[1], sys.argv[2]))


if __name__ == "__main__":
    main()
