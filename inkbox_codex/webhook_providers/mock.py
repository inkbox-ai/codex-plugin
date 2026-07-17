"""Shared-secret webhook events for manual and integration testing."""

from __future__ import annotations

import hmac
from typing import Mapping

from .base import WebhookProvider, register_provider

_HEADER = "X-Inkbox-Mock-Secret"


@register_provider
class MockProvider(WebhookProvider):
    """Authenticate test webhooks with a secret sent directly in a header.

    This provider intentionally does not sign the request body. It is useful
    for simple ``curl`` probes and test systems that can attach a shared secret
    but cannot calculate a webhook signature. Configure the expected value in
    ``INKBOX_WEBHOOK_SECRET_MOCK``.
    """

    name = "mock"
    provider_header = _HEADER

    def verify(
        self,
        *,
        body: bytes,
        headers: Mapping[str, str],
        url: str,
        secret: str,
    ) -> bool:
        if not secret:
            return False
        sent = next(
            (value for key, value in headers.items() if key.lower() == _HEADER.lower()),
            "",
        )
        return bool(sent) and hmac.compare_digest(sent, secret)
