"""Offline integration with the real snapshot loader (requires Companion SDK)."""
import asyncio
from copy import deepcopy
from types import SimpleNamespace as NS

import pytest

companion = pytest.importorskip('inkbox.companion', reason='Companion SDK not installed')
from tests.test_companion import fixture, harness, drained, isolated


class HTTP:
    def __init__(self, envelope, failure=None):
        self.c = deepcopy(envelope['companion'])
        self.calls = []
        self.failure = failure

    def get(self, path, params=None):
        params = params or {}
        self.calls.append((path, params))
        cursor = params.get('cursor')
        # Deliberately opaque: must survive unchanged, not parsed as an offset.
        assert cursor in (None, 'opaque/+==?cursor')
        first = cursor is None
        if self.failure == 'revoked' and len(self.calls) > 1:
            raise PermissionError('revoked between pages')
        c = self.c
        result = {k: c[k] for k in ('scope_id', 'activation_id', 'conversation_id', 'channel', 'reply_context')}
        result.update(items=deepcopy(c['history'][:2] if first else c['history'][1:]),
                      history_complete=not first, next_cursor='opaque/+==?cursor' if first else None)
        if self.failure == 'cursor': result.update(history_complete=False, next_cursor='opaque/+==?cursor')
        if self.failure == 'conflict' and not first: result['items'][0]['text'] = 'changed duplicate'
        if self.failure == 'scope' and not first: result['scope_id'] = result['conversation_id']
        if self.failure == 'no_trigger' and not first: result['items'][-1]['is_trigger'] = False
        if self.failure == 'missing_cursor' and first: result['next_cursor'] = None
        if self.failure == 'oversize': result['items'][0]['text'] = 'x' * (8 * 1024 * 1024)
        return result


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
def test_actual_sdk_paginates_deduplicates_revalidates_before_one_host_input(channel):
    async def scenario():
        e = fixture(channel)
        http = HTTP(e)
        # Trigger absent from the inline page; receiver must not rely on it.
        e['companion']['history'] = e['companion']['history'][:1]
        e['companion'].update(history_complete=False, history_next_cursor='inline-opaque')
        r, _, s, sent = harness(e)
        r.client = NS(companion=companion.CompanionResource(http))
        try:
            await r.accept(e)
            await drained(r)
            assert len(sent) == 1
            assert len(s._client.events) == 1
            text = s._client.events[0][1]
            for entry in fixture(channel)['companion']['history']:
                assert text.count(entry['text']) == 1
            assert [p.get('cursor') for _, p in http.calls] == [None, 'opaque/+==?cursor', None, None]
            assert all('/companion/activations/' in path for path, _ in http.calls)
        finally: await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('failure', ['revoked', 'cursor', 'conflict', 'scope', 'no_trigger', 'missing_cursor', 'oversize'])
def test_actual_sdk_incomplete_or_changed_snapshot_never_reaches_host(failure):
    async def scenario():
        e = fixture()
        r, _, s, sent = harness(e)
        r.client = NS(companion=companion.CompanionResource(HTTP(e, failure)))
        try:
            await r.accept(e)
            await drained(r)
            assert not s._client.events
            assert not sent
        finally: await r.close()
    asyncio.run(scenario())
