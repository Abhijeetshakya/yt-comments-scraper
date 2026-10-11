"""Isolated, version-pinned yt-dlp adapter. stdout is a privacy-safe JSONL protocol."""
import json
import re
import random
import sys
from datetime import datetime, timezone
from .dates import matches_cutoff
from yt_dlp import YoutubeDL
from yt_dlp.extractor.youtube import YoutubeIE


def send(kind, **values):
    print(json.dumps({'type': kind, **values}, ensure_ascii=False), flush=True)


class Logger:
    def __init__(self):
        self.zero = False
        self.incomplete = False
        self.requests = 0
        self.retries = 0

    def debug(self, message):
        if 'Downloading ~0 comments' in message:
            self.zero = True
        if 'Downloading' in message and ('JSON' in message or 'webpage' in message):
            self.requests += 1

    def info(self, message):
        self.debug(message)

    def warning(self, message):
        text = re.sub(r'AIza[\w-]{20,}', '[REDACTED]', message)
        if 'Retrying' in text:
            self.retries += 1
        if ('incomplete data' in text.lower() and ('giving up' in text.lower() or 'ignoring' in text.lower())) or 'comments looping' in text.lower():
            self.incomplete = True
        send('warning', message=text)

    def error(self, message):
        # Raw exception strings can contain URLs or credentials. Expose a generic error.
        send('warning', message='Public extraction reported an error.')


class StreamingYoutubeIE(YoutubeIE):
    def __init__(self, downloader=None, config=None):
        super().__init__(downloader)
        self.config = config or {}
        self.found_comments = False
        self.date_cutoff_reached = False
        self.missing_dates = 0

    @classmethod
    def ie_key(cls):
        return 'Youtube'

    def _real_extract(self, url):
        info = super()._real_extract(url)
        send('metadata', title=info.get('title'), channelId=info.get('channel_id'))
        return info

    def extract_comments(self, *args, **kwargs):
        generator = self._get_comments(*args, **kwargs)

        def consume():
            count = 0
            selected = {}
            reply_counts = {}
            try:
                for raw in generator:
                    self.found_comments = True
                    cutoff = self.config.get('publishedAfter')
                    if cutoff:
                        timestamp = raw.get('timestamp')
                        published = (datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
                                     if timestamp is not None else None)
                        if published is None:
                            self.missing_dates += 1
                        if not matches_cutoff(published, cutoff):
                            if (published is not None and raw.get('parent') in (None, 'root')
                                    and not raw.get('is_pinned')):
                                self.date_cutoff_reached = True
                                break
                            continue
                        if raw.get('parent') not in (None, 'root') and raw.get('parent') not in selected:
                            continue
                        parent = raw.get('parent')
                        root = selected[parent] if parent not in (None, 'root') else raw['id']
                        if parent not in (None, 'root'):
                            cap = self.config.get('replyCap')
                            if cap and reply_counts.get(root, 0) >= cap:
                                continue
                            reply_counts[root] = reply_counts.get(root, 0) + 1
                    # Never send author IDs, profile URLs or thumbnails across the boundary.
                    allowed = {k: raw.get(k) for k in ('id', 'text', 'timestamp', 'like_count', 'author',
                               'author_is_uploader', 'parent', 'reply_count', 'is_pinned', 'is_favorited')}
                    send('comment', comment=allowed)
                    selected[raw['id']] = root if cutoff else raw['id']
                    count += 1
                    if cutoff and count >= self.config['limit']:
                        break
            except self.CommentsDisabled:
                send('disabled')
            finally:
                generator.close()
            return {'comment_count': count}
        return consume


def main():
    cfg = json.loads(sys.stdin.readline())
    logger = Logger()
    replies = 'all' if cfg['includeReplies'] else '0'
    per_thread = str(cfg.get('replyCap') or 'all')
    args = {'comment_sort': [cfg['sort']],
            # Raw comments excluded by the cutoff must not consume the output budget.
            'max_comments': ['all' if cfg.get('publishedAfter') else str(cfg['limit']),
                             'all', replies, 'all' if cfg.get('publishedAfter') else per_thread,
                             'all' if cfg['includeReplies'] else '1'],
            'player_skip': ['configs', 'js'], 'skip': ['hls', 'dash', 'translated_subs']}
    options = {'skip_download': True, 'noplaylist': True, 'getcomments': True,
               'ignore_no_formats_error': True, 'socket_timeout': 15, 'retries': 2,
               'extractor_retries': 2, 'logger': logger, 'quiet': True,
               'retry_sleep_functions': {'extractor': lambda attempt: 0.4 * 2 ** (attempt - 1) + random.uniform(0, 0.25)},
               'extractor_args': {'youtube': args}}
    try:
        with YoutubeDL(options, auto_init=False) as ydl:
            extractor = StreamingYoutubeIE(config=cfg)
            ydl.add_info_extractor(extractor)
            ydl.extract_info(cfg['url'], download=False)
        send('complete', verifiedEmpty=logger.zero, incomplete=logger.incomplete,
             requests=logger.requests, retries=logger.retries, foundComments=extractor.found_comments,
             dateCutoffReached=extractor.date_cutoff_reached, missingPublicationDates=extractor.missing_dates)
    except Exception:
        send('failure', message='Public extraction failed; the comment section could not be verified.',
             requests=logger.requests, retries=logger.retries)


if __name__ == '__main__':
    main()
