"""Read conversation history through its existing native owner."""
from __future__ import annotations

import asyncio
import json


def page(thread, before, *, forward=False):
    messages = []
    found = before is None
    for turn in thread.get('turns', []):
        for index, item in enumerate(turn.get('items', [])):
            kind = item.get('type')
            if kind not in {'userMessage', 'agentMessage'}:
                continue
            text = (item.get('text') if kind == 'agentMessage' else '\n'.join(
                part.get('text', '') for part in item.get('content', []) if part.get('type') == 'text'))
            if not isinstance(text, str):
                continue
            identifier = str(item.get('id') or f"{turn.get('id')}:{index}")
            if forward:
                if not found:
                    found = identifier == before
                    continue
            elif identifier == before:
                break
            messages.append({'id': identifier, 'role': 'assistant' if kind == 'agentMessage' else 'user',
                             'text': text[:8192], 'truncated': len(text) > 8192})
        else:
            continue
        break
    else:
        if before is not None and not (forward and found):
            raise ValueError('History cursor is no longer available')
    result, used = [], 0
    for message in messages if forward else reversed(messages):
        size = len(json.dumps(message).encode())
        if len(result) == 32 or used + size > 48000:
            break
        result.append(message)
        used += size
    if not forward:
        result.reverse()
    cursor = result[-1 if forward else 0]['id'] if result and len(result) < len(messages) else None
    return {'messages': result, 'next_before': cursor}


async def read(manager, data):
    if manager is None:
        raise RuntimeError('Conversations unavailable')
    if data == {'action': 'conversations'}:
        rows = sorted(manager._session_ids.items())
        return {'conversations': [{'id': native, 'label': key[:256]} for key, native in rows[:100]],
                'truncated': len(rows) > 100}
    if set(data) != {'action', 'session_id', 'before'} or data['action'] != 'conversation':
        raise ValueError('Invalid request')
    session_id, before = data['session_id'], data['before']
    if not isinstance(session_id, str) or (before is not None and (not isinstance(before, str) or len(before) > 256)):
        raise ValueError('Invalid request')
    key = next((key for key, native in manager._session_ids.items() if native == session_id), None)
    if key is None:
        raise ValueError('Conversation unavailable')
    # Resumption uses the same serialized owner as incoming and scheduled turns.
    client = await asyncio.wait_for(manager.get(key)._ensure_client(), 10)
    result = await asyncio.wait_for(client._request('thread/read', {'threadId': session_id, 'includeTurns': True}), 5)
    thread = result.get('thread', {})
    if thread.get('id') != session_id:
        raise RuntimeError('Conversation unavailable')
    return {'session_id': session_id, **page(thread, before)}
