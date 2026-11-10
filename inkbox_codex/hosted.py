"""Optional settings for an identity-hosted session."""
import os
from urllib.parse import quote, urlsplit


def instructions(base):
    prompt = os.environ.get('INKBOX_HOSTED_INSTRUCTIONS', '')
    if len(prompt.encode()) > 32768 or '\0' in prompt:
        raise ValueError('Invalid hosted instructions')
    return prompt + '\n\n' + base if prompt else base


def mcp_server(cfg):
    if os.environ.get('INKBOX_HOSTED_MODE') != '1':
        return None
    origin = cfg.base_url.rstrip('/')
    parsed = urlsplit(origin)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path:
        raise ValueError('Invalid hosted API origin')
    identity = quote(cfg.identity.lstrip('@'), safe='')
    if not identity:
        raise ValueError('Hosted identity is required')
    return {'url': f'{origin}/api/v1/identities/{identity}/hosting/mcp',
            'env_http_headers': {'X-Service-Token': 'INKBOX_API_KEY'},
            'startup_timeout_sec': 10, 'tool_timeout_sec': 60}
