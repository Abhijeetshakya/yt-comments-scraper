import asyncio
from datetime import datetime, timezone
import json

import httpx
import pytest

from src.config import Config
from src.dates import matches_cutoff, resolve_cutoff
from src.public_worker import StreamingYoutubeIE
from test_actor import Handler, IDS, MemoryStore, Public, raw, run

CUTOFF = '2025-01-01'
MIDNIGHT = '2025-01-01T00:00:00Z'
BEFORE = '2024-12-31T23:59:59Z'
AFTER = '2025-01-01T00:00:01Z'


def data(**kwargs):
    return {'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 10}],
            'publishedAfter': CUTOFF, **kwargs}


def comment(cid, published):
    item = raw(cid)
    item['snippet']['publishedAt'] = published
    # A recent edit must not make an older publication eligible.
    item['snippet']['updatedAt'] = '2026-10-11T00:00:00Z'
    return item


def thread(cid, published, replies=()):
    return {'id': cid, 'snippet': {'topLevelComment': comment(cid, published),
                                  'totalReplyCount': len(replies)},
            'replies': {'comments': [comment(rid, date) for rid, date in replies]}}


def test_dates_and_calendar_ages():
    now = datetime(2026, 10, 11, 18, 30, tzinfo=timezone.utc)
    assert resolve_cutoff('1 day', now) == '2026-10-11T00:00:00Z'
    assert resolve_cutoff('2 days', now) == '2026-10-10T00:00:00Z'
    assert resolve_cutoff(' 3 DAYS ', now) == '2026-10-09T00:00:00Z'
    assert resolve_cutoff('2024-02-29') == '2024-02-29T00:00:00Z'
    assert resolve_cutoff('') is None
    assert matches_cutoff(MIDNIGHT, MIDNIGHT)
    assert matches_cutoff('2024-12-31T19:00:00-05:00', MIDNIGHT)
    assert not matches_cutoff(BEFORE, MIDNIGHT)
    assert not matches_cutoff(None, MIDNIGHT)
    assert not matches_cutoff('2025-01-01T00:00:00', MIDNIGHT)


@pytest.mark.parametrize('value', ['2025-02-29', '11/10/2026', '0 days', '-1 day',
                                   '1.5 days', 'tomorrow', '100000000000 days', 1, True, []])
def test_bad_cutoff_rejected(value):
    with pytest.raises(ValueError, match='publishedAfter'):
        Config.parse(data(publishedAfter=value))


def test_sort_override_and_blank_filter():
    assert Config.parse(data(sort='top')).sort == 'newest'
    assert Config.parse(data(sort='top', publishedAfter='')).sort == 'top'
    assert Config.parse(data(sort='top', publishedAfter='  ')).published_after is None
    schema = json.loads(open('.actor/input_schema.json').read())
    assert schema['properties']['publishedAfter']['editor'] == 'textfield'
    assert 'publishedAfter' not in schema['required']


async def test_inclusive_roots_and_embedded_replies_skip_old_and_undated():
    normal = Handler()
    public = Public()

    async def handler(request):
        if request.url.path.endswith('commentThreads'):
            return httpx.Response(200, json={'items': [
                thread('boundary', MIDNIGHT, [('old-reply', BEFORE), ('new-reply', AFTER), ('unknown', None)]),
                thread('old-parent', BEFORE, [('recent-on-old-parent', AFTER)]),
                thread('new-parent', AFTER)]})
        return await normal(request)

    result, store, _ = await run(data(sort='top'), handler, public)
    assert [row['commentId'] for row in store.data] == ['boundary', 'new-reply', 'new-parent']
    assert store.data[0]['fetchedReplyCount'] == 1
    assert all(row['sort'] == 'newest' for row in store.data)
    assert result['videos'][0]['publishedAfter'] == MIDNIGHT
    assert result['videos'][0]['missingPublicationDates'] == 1
    assert public.calls == 0


async def test_stop_on_old_page_and_no_empty_fallback():
    normal = Handler()
    calls = []
    public = Public()

    async def handler(request):
        if request.url.path.endswith('commentThreads'):
            calls.append(dict(request.url.params))
            assert request.url.params['order'] == 'time'
            if len(calls) == 1:
                # A mixed page cannot stop pagination, even if its first root is older.
                return httpx.Response(200, json={'items': [thread('old', BEFORE), thread('new', MIDNIGHT)],
                                                'nextPageToken': '2'})
            return httpx.Response(200, json={'items': [thread('older', BEFORE)], 'nextPageToken': '3'})
        return await normal(request)

    result, store, _ = await run(data(includeReplies=False, sort='top'), handler, public)
    assert len(calls) == 2
    assert [row['commentId'] for row in store.data] == ['new']
    assert result['videos'][0]['dateCutoffReached'] is True
    assert public.calls == 0


async def test_existing_comments_outside_window_are_not_no_comments():
    public = Public()
    result, store, _ = await run(data(publishedAfter='2026-10-11'), Handler(), public)
    assert not store.data
    assert result['videos'][0]['status'] == 'no_matching_comments'
    assert result['videos'][0]['dateCutoffReached']
    assert public.calls == 0
    assert result['apiRequestCount'] == 2  # Metadata plus one older page.


async def test_filtered_reply_pages_do_not_consume_limit():
    normal = Handler(threads=1, replies=4)
    calls = []

    async def handler(request):
        response = await normal(request)
        body = response.json()
        if request.url.path.endswith('comments'):
            calls.append(dict(request.url.params))
            for item in body['items']:
                item['snippet']['publishedAt'] = BEFORE if item['id'].endswith(('r0', 'r1')) else AFTER
        return httpx.Response(200, json=body)

    result, store, _ = await run(data(videos=[{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 3}]), handler)
    assert len(calls) == 2
    assert [row['commentId'].rsplit('.', 1)[-1] for row in store.data[1:]] == ['r2', 'r3']
    assert len(store.data) == 3
    assert result['videos'][0]['videoLimitReached']
    assert store.data[0]['fetchedReplyCount'] == 2


async def test_concurrent_filtered_budget():
    result, store, handler = await run(data(videos=[{'url': f'https://youtu.be/{vid}'} for vid in IDS],
                                           maxTotalComments=17), Handler(delay=.005))
    assert len(store.data) == 17
    assert handler.peak == 3
    assert all(matches_cutoff(row['publishedAt'], MIDNIGHT) for row in store.data)


async def test_relative_cutoff_stays_fixed_on_resume(monkeypatch):
    import src.engine
    original = src.engine.resolve_cutoff
    monkeypatch.setattr(src.engine, 'resolve_cutoff', lambda value: original(value,
        datetime(2025, 1, 1, 20, tzinfo=timezone.utc)))
    store = MemoryStore()
    cfg = data(publishedAfter='1 day')
    await run(cfg, Handler(), store=store)
    monkeypatch.setattr(src.engine, 'resolve_cutoff', lambda value: original(value,
        datetime(2025, 1, 2, 20, tzinfo=timezone.utc)))
    result, store, handler = await run(cfg, Handler(), store=store)
    assert result['videos'][0]['publishedAfter'] == MIDNIGHT
    assert len(store.data) == 10
    assert not handler.requests
    with pytest.raises(ValueError, match='input differs'):
        await run(data(publishedAfter='2 days'), store=store)


async def test_public_fallback_dates_and_unknown_dates():
    timestamp = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp())
    events = [{'type': 'comment', 'comment': {'id': 'p', 'parent': 'root', 'timestamp': timestamp}},
              {'type': 'comment', 'comment': {'id': 'old', 'parent': 'p', 'timestamp': timestamp - 1}},
              {'type': 'comment', 'comment': {'id': 'new', 'parent': 'p', 'timestamp': timestamp + 1}},
              {'type': 'comment', 'comment': {'id': 'undated', 'parent': 'p'}},
              {'type': 'complete'}]
    result, store, _ = await run(data(), Handler(empty=True), Public(events))
    assert [r['commentId'] for r in store.data] == ['p', 'new']
    assert store.data[0]['publishedAtPrecision'] == 'estimated'
    assert store.data[0]['fetchedReplyCount'] == 1
    assert result['videos'][0]['missingPublicationDates'] == 1
    assert result['videos'][0]['status'] == 'completed'


@pytest.mark.parametrize('events,status', [
    ([{'type': 'complete', 'foundComments': True, 'dateCutoffReached': True}], 'no_matching_comments'),
    ([{'type': 'complete', 'verifiedEmpty': True}], 'no_comments'),
    ([{'type': 'failure', 'message': 'DNS failure'}], 'failed'),
])
async def test_filtered_public_verification_status(events, status):
    result, store, _ = await run(data(), Handler(empty=True), Public(events))
    assert not store.data
    assert result['videos'][0]['status'] == status


async def test_enrichment_uses_api_date_even_when_public_estimate_is_older():
    cid = IDS[0] + '-c0'
    timestamp = int(datetime(2024, 12, 31, tzinfo=timezone.utc).timestamp())

    class Enrichment(Public):
        async def stream(self, video, config):
            assert config.cutoff is None
            async for event in super().stream(video, config):
                yield event

    events = [{'type': 'comment', 'comment': {'id': cid, 'parent': 'root', 'timestamp': timestamp,
                                             'is_pinned': True}}, {'type': 'complete'}]
    _, store, _ = await run(data(publicDetails=['pinned']), Handler(), Enrichment(events))
    assert store.data[0]['publishedAt'] == '2025-01-01T01:02:03Z'
    assert store.data[0]['isPinned'] is True


def test_public_worker_filtered_limit_reply_cap_and_boundary(monkeypatch):
    sent = []
    monkeypatch.setattr('src.public_worker.send', lambda kind, **values: sent.append({'type': kind, **values}))
    timestamp = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp())
    records = [
        {'id': 'pin', 'parent': 'root', 'timestamp': timestamp - 1, 'is_pinned': True},
        {'id': 'p', 'parent': 'root', 'timestamp': timestamp},
        {'id': 'old', 'parent': 'p', 'timestamp': timestamp - 1},
        {'id': 'r', 'parent': 'p', 'timestamp': timestamp + 1},
        {'id': 'capped', 'parent': 'r', 'timestamp': timestamp + 1},
        {'id': 'unknown', 'parent': 'root'},
        {'id': 'q', 'parent': 'root', 'timestamp': timestamp + 1},
        {'id': 'boundary', 'parent': 'root', 'timestamp': timestamp - 1},
        {'id': 'not-reached', 'parent': 'root', 'timestamp': timestamp - 10},
    ]
    consumed = []

    def comments():
        for row in records:
            consumed.append(row['id'])
            yield row

    extractor = StreamingYoutubeIE(config={'publishedAfter': MIDNIGHT, 'limit': 100, 'replyCap': 1})
    monkeypatch.setattr(extractor, '_get_comments', lambda *args, **kwargs: comments())
    result = extractor.extract_comments()()
    assert [event['comment']['id'] for event in sent] == ['p', 'r', 'q']
    assert result['comment_count'] == 3
    assert extractor.date_cutoff_reached and extractor.found_comments
    assert extractor.missing_dates == 1
    assert 'not-reached' not in consumed
    sent.clear()
    extractor.config['limit'] = 2
    extractor.extract_comments()()
    assert [event['comment']['id'] for event in sent] == ['p', 'r']
