import asyncio
import os
import random
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import httpx

SNIPPET = 'authorDisplayName,authorChannelId/value,textDisplay,publishedAt,updatedAt,likeCount'
THREAD_FIELDS = f'nextPageToken,items(id,snippet(totalReplyCount,topLevelComment(id,snippet({SNIPPET}))),replies/comments(id,snippet({SNIPPET})))'
REPLY_FIELDS = f'nextPageToken,items(id,snippet({SNIPPET}))'


class ApiError(Exception):
    def __init__(self, kind, message):
        self.kind = kind
        super().__init__(message)


class Credentials:
    """Private credential boundary; the scheduler is independent of credential count."""
    def __init__(self, key=None):
        self._key = key or os.environ.get('YOUTUBE_API_KEY', '')
        if not self._key:
            raise ValueError('Configure the private YOUTUBE_API_KEY environment variable.')

    def headers(self):
        return {'X-Goog-Api-Key': self._key}

    def redact(self, message):
        message = str(message).replace(self._key, '[REDACTED]')
        return re.sub(r'AIza[\w-]{20,}', '[REDACTED]', message)


class Api:
    def __init__(self, client, credentials, sleep=asyncio.sleep):
        self.client, self.credentials, self.sleep = client, credentials, sleep
        self.reply_slots = asyncio.Semaphore(4)
        self.blocked = None

    async def request(self, resource, params, stats, deadline):
        for attempt in range(3):
            if self.blocked:
                raise self.blocked
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Video deadline reached.')
            stats['apiRequestCount'] += 1
            response = None
            try:
                async with asyncio.timeout(remaining):
                    response = await self.client.get(f'https://www.googleapis.com/youtube/v3/{resource}',
                                                    params=params, headers=self.credentials.headers(),
                                                    timeout=min(20, remaining))
                if response.status_code < 400:
                    try:
                        return response.json()
                    except ValueError:
                        raise ApiError('invalid_response', 'API returned an invalid JSON response.') from None
                try:
                    body = response.json().get('error', {})
                except ValueError:
                    body = {}
                reason = next(iter(body.get('errors', [])), {}).get('reason', '')
                kind = {'commentsDisabled': 'comments_disabled', 'videoNotFound': 'unavailable',
                        'quotaExceeded': 'quota_exhausted', 'dailyLimitExceeded': 'quota_exhausted',
                        'keyInvalid': 'invalid_credentials', 'accessNotConfigured': 'invalid_credentials',
                        'ipRefererBlocked': 'invalid_credentials', 'forbidden': 'forbidden'}.get(reason)
                if response.status_code == 401 or (response.status_code == 400 and
                       ('API key' in body.get('message', '') or reason == 'badRequest')):
                    kind = 'invalid_credentials'
                if kind or not (response.status_code == 429 or 500 <= response.status_code <= 599):
                    error = ApiError(kind or 'api_error', self.credentials.redact(body.get('message', reason or 'API request failed.')))
                    if error.kind in ('quota_exhausted', 'invalid_credentials'):
                        self.blocked = error
                    raise error
            except (httpx.TransportError, httpx.TimeoutException):
                pass  # Never log a transport exception containing request headers.
            if attempt == 2:
                raise ApiError('network_error', 'API network or transient HTTP failure after three attempts.')
            stats['retryCount'] += 1
            delay = (0.4 * 2 ** attempt) + random.uniform(0, 0.25)
            if response is not None:
                try:
                    delay = max(delay, float(response.headers.get('Retry-After', 0)))
                except ValueError:
                    try:
                        until = parsedate_to_datetime(response.headers['Retry-After'])
                        delay = max(delay, (until - datetime.now(timezone.utc)).total_seconds())
                    except (ValueError, TypeError, KeyError):
                        pass
            if delay >= deadline - time.monotonic():
                raise TimeoutError('Video deadline reached during retry backoff.')
            await self.sleep(delay)

    async def replies(self, params, stats, deadline):
        async with self.reply_slots:
            return await self.request('comments', params, stats, deadline)
