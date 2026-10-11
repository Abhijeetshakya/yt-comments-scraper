from dataclasses import dataclass, asdict
from urllib.parse import urlparse, parse_qs
import hashlib
import json
import re
from .dates import resolve_cutoff


def video_id(url):
    parsed = urlparse(url)
    host = (parsed.hostname or '').lower()
    if parsed.scheme not in ('https', 'http'):
        raise ValueError('Use an HTTP or HTTPS YouTube video URL.')
    if host in ('youtu.be', 'www.youtu.be'):
        value = parsed.path.strip('/').split('/')[0]
    elif host in ('youtube.com', 'www.youtube.com', 'm.youtube.com', 'music.youtube.com'):
        parts = parsed.path.strip('/').split('/')
        value = parse_qs(parsed.query).get('v', [''])[0] if parsed.path == '/watch' else (
            parts[1] if len(parts) == 2 and parts[0] in ('shorts', 'live', 'embed') else '')
    else:
        value = ''
    if not re.fullmatch(r'[A-Za-z0-9_-]{11}', value):
        raise ValueError('A supported YouTube URL containing a video ID is required.')
    return value


def positive(value, name, default=None):
    value = default if value is None else value
    if value is not None and (type(value) is not int or value < 1):
        raise ValueError(f'{name} must be a positive integer.')
    return value


@dataclass(frozen=True)
class Video:
    id: str
    limit: int

    @property
    def url(self):
        return f'https://www.youtube.com/watch?v={self.id}'


@dataclass(frozen=True)
class Config:
    videos: tuple[Video, ...]
    sort: str = 'newest'
    include_replies: bool = True
    reply_cap: int | None = None
    total_cap: int | None = None
    details: tuple[str, ...] = ()
    fallback: bool = True
    concurrency: int = 3
    timeout: int = 180
    published_after: str | None = None
    cutoff: str | None = None

    @classmethod
    def parse(cls, data):
        entries = data.get('videos')
        if not isinstance(entries, list) or not entries:
            raise ValueError('videos must contain at least one URL and optional maxComments.')
        unique = {}
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get('url'), str):
                raise ValueError('Each video entry requires a URL.')
            vid = video_id(entry['url'])
            limit = positive(entry.get('maxComments'), 'maxComments', 100)
            if vid in unique and unique[vid] != limit:
                raise ValueError(f'Conflicting comment amounts for video {vid}.')
            unique[vid] = limit
        sort = data.get('sort', 'newest')
        if sort not in ('newest', 'top'):
            raise ValueError('sort must be newest or top.')
        published_after = data.get('publishedAfter')
        if resolve_cutoff(published_after) is not None:
            published_after = published_after.strip().lower()
            sort = 'newest'
        else:
            published_after = None
        details = data.get('publicDetails', [])
        if not isinstance(details, list) or any(x not in ('pinned', 'creatorHeart', 'nestedReplies') for x in details):
            raise ValueError('Unsupported publicDetails selection.')
        for name in ('includeReplies', 'enableFallback'):
            if name in data and type(data[name]) is not bool:
                raise ValueError(f'{name} must be a boolean.')
        concurrency = positive(data.get('maxConcurrency'), 'maxConcurrency', 3)
        if concurrency > 20:
            raise ValueError('maxConcurrency must not exceed 20.')
        return cls(tuple(Video(k, v) for k, v in unique.items()), sort,
                   data.get('includeReplies', True), positive(data.get('maxRepliesPerThread'), 'maxRepliesPerThread'),
                   positive(data.get('maxTotalComments'), 'maxTotalComments'), tuple(sorted(set(details))),
                   data.get('enableFallback', True), concurrency,
                   positive(data.get('videoTimeoutSeconds'), 'videoTimeoutSeconds', 180), published_after)

    @property
    def fingerprint(self):
        settings = asdict(self)
        settings.pop('cutoff')  # Relative ages resolve once per run, independently of restart time.
        return hashlib.sha256(json.dumps(settings, sort_keys=True).encode()).hexdigest()
