"""Compatibility import for the shared managed runtime protocol."""

import sys

from inkbox import managed_chat as _implementation

sys.modules[__name__] = _implementation
