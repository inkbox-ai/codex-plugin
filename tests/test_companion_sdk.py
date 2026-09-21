"""Offline integration with the real snapshot loader (requires Companion SDK)."""
import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace as NS
from unittest.mock import patch
from uuid import UUID

import httpx
import pytest

companion = pytest.importorskip('inkbox.companion', reason='Companion SDK not installed')
from tests.test_companion import fixture, harness, drained, isolated
from tests.test_companion import live
from inkbox import Inkbox


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
            assert [p.get('cursor') for _, p in http.calls] == [None, 'opaque/+==?cursor', None]
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


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
def test_latest_sdk_wire_pages_normalize_ids_and_preserve_reply_scope(channel):
    async def scenario():
        original = fixture(channel)
        prefix = original['companion']['scope_id'].split('-')[0]
        e = json.loads(json.dumps(original).replace(prefix, 'aBcDeFaB'))
        pages = HTTP(e)
        def respond(request):
            assert request.method == 'GET'
            assert request.url.path == (
                '/api/v1/identities/test-agent/companion/activations/'
                + str(UUID(e['companion']['activation_id'])) + '/messages'
            )
            params = dict(request.url.params)
            assert int(params['limit']) in (1, 100)
            body = pages.get(request.url.path, params)
            return httpx.Response(200, json=body)
        with patch('inkbox._http.httpx.HTTPTransport', return_value=httpx.MockTransport(respond)):
            client = Inkbox(api_key='synthetic-key', base_url='https://api.example.com')
        with client:
            r, _, s, sent = harness(e)
            r.client = client
            try:
                await r.accept(e)
                await drained(r)
                assert len(s._client.events) == len(sent) == 1
                meta = sent[0][3]
                assert meta['conversation_id'] == str(UUID(e['companion']['conversation_id']))
                if channel == 'mail':
                    assert meta['message_id'] == str(UUID(e['companion']['reply_context']['reply_to_message_id']))
                    assert meta['companion_reply_context']['to'] == e['companion']['reply_context']['to']
                duplicate = live(e, 2)
                message = duplicate['data'].get('text_message') or duplicate['data']['message']
                message['id'] = str(UUID(e['companion']['history'][-1]['id'])).upper()
                await r.accept(duplicate)
                await drained(r)
                assert len(s._client.events) == len(sent) == 1
            finally:
                await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('channel', ['phone', 'imessage', 'mail'])
def test_attachment_only_trigger_is_quiet_context_not_a_historical_mention(channel):
    async def scenario():
        e = fixture(channel)
        message = e['data'].get('text_message') or e['data']['message']
        message[{'phone': 'text', 'imessage': 'content', 'mail': 'body'}[channel]] = ''
        e['companion']['history'][0]['text'] = '@agent historical request'
        trigger = e['companion']['history'][-1]
        trigger['text'] = ''
        trigger['attachments'] = [{'source_message_id': trigger['id'], 'index': 0,
                                   'content_type': 'image/png', 'size': 42}]
        r, _, s, sent = harness(e, reply_mode='mention')
        r.client = NS(companion=companion.CompanionResource(HTTP(e)))
        try:
            await r.accept(e)
            await drained(r)
            assert [kind for kind, _ in s._client.events] == ['context']
            assert 'image/png' in s._client.events[0][1]
            assert not sent
        finally:
            await r.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('reply_mode', ['auto', 'mention'])
def test_history_notices_reach_the_host_once_without_counting_as_mentions(reply_mode):
    async def scenario():
        e = fixture()
        class NoticeHTTP(HTTP):
            def get(self, path, params=None):
                page = super().get(path, params)
                page['notices'] = [{'code': 'history_limited', 'level': 'warning',
                                    'message': '@agent some earlier messages are unavailable.'}]
                return page
        r, _, s, sent = harness(e, reply_mode=reply_mode)
        r.client = NS(companion=companion.CompanionResource(NoticeHTTP(e)))
        try:
            await r.accept(e)
            await drained(r)
            assert len(s._client.events) == 1
            kind, text = s._client.events[0]
            assert text.count('@agent some earlier messages are unavailable.') == 1
            assert 'history_limited' in text
            assert kind == ('run' if reply_mode == 'auto' else 'context')
            assert len(sent) == (1 if reply_mode == 'auto' else 0)
        finally:
            await r.close()
    asyncio.run(scenario())


def test_later_allowed_sender_does_not_replace_original_activation_sponsor():
    async def scenario():
        e = fixture()
        sponsor = e['companion']['history'][-1]['author']
        other = '+12025550104'
        r, _, s, sent = harness(e, allowed=lambda author: author in {sponsor, other})
        http = HTTP(e)
        r.client = NS(companion=companion.CompanionResource(http))
        try:
            await r.accept(e)
            await drained(r)
            loads = len(http.calls)
            await r.accept(live(e, author=other))
            await drained(r)
            assert len(s._client.events) == len(sent) == 2
            assert sent[-1][3]['sender'] == other
            assert sent[-1][3]['companion_sponsor'] == sponsor
            assert len(http.calls) == loads
        finally:
            await r.close()
    asyncio.run(scenario())
