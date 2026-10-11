import asyncio
import copy
import csv
import io
import inspect
import json
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit
import httpx
import pytest
from apify_client import ApifyClientAsync
from src.api import Api, ApiError, Credentials, THREAD_FIELDS, REPLY_FIELDS
from src.config import Config, video_id
from src.engine import Engine
from src.records import api_row, public_row, FIELDS

IDS = ['8of5w7RgcTc', 'L2udL0v3hkg', 'wp43OdtAAkM', 'MrmPDUvKyLs']


def test_output_dataset_links_are_supported_by_apify_items_client():
    # Console loads dataset output links through an items client, not its export API.
    schema = json.loads(Path('.actor/output_schema.json').read_text())
    items_parameters = inspect.signature(ApifyClientAsync().dataset('example').list_items)
    dataset_outputs = 0
    for output in schema['properties'].values():
        if '{{links.apiDefaultDatasetUrl}}' not in output['template']:
            continue
        dataset_outputs += 1
        url = output['template'].replace('{{links.apiDefaultDatasetUrl}}',
                                         'https://api.apify.com/v2/datasets/example')
        assert urlsplit(url).path.endswith('/items')
        items_parameters.bind(**dict(parse_qsl(urlsplit(url).query)))
    assert dataset_outputs > 0


class MemoryStore:
    def __init__(self):
        self.state = None
        self.data = []
        self.result = None
        self.batches = []

    async def load(self):
        return copy.deepcopy(self.state)

    async def checkpoint(self, state):
        self.state = copy.deepcopy(state)

    async def existing(self):
        for row in copy.deepcopy(self.data):
            yield row

    async def push(self, rows):
        self.data.extend(copy.deepcopy(rows))
        self.batches.append(len(rows))

    async def output(self, result):
        self.result = copy.deepcopy(result)


class Public:
    def __init__(self, events=None, delay=0):
        self.events = events or [{'type': 'complete', 'verifiedEmpty': True}]
        self.calls = 0
        self.delay = delay

    async def stream(self, video, config):
        self.calls += 1
        for event in self.events:
            yield event
        if self.delay:
            await asyncio.sleep(self.delay)


def raw(cid, creator=False):
    return {'id': cid, 'snippet': {'textDisplay': 'Full body\n🙂', 'authorDisplayName': 'Author',
        'authorChannelId': {'value': 'OWNER' if creator else 'INTERNAL'},
        'publishedAt': '2025-01-01T01:02:03Z', 'updatedAt': '2025-02-01T01:02:03Z', 'likeCount': 1234}}


class Handler:
    def __init__(self, threads=1000, replies=2, embedded=False, error=None, empty=False, delay=0):
        self.threads, self.reply_count, self.embedded = threads, replies, embedded
        self.error, self.empty, self.delay = error, empty, delay
        self.requests = []
        self.active = self.peak = self.reply_active = self.reply_peak = 0
        self.events = []

    async def __call__(self, request):
        resource = request.url.path.rsplit('/', 1)[-1]
        p = dict(request.url.params)
        self.requests.append((resource, p, dict(request.headers)))
        if resource == 'videos':
            return httpx.Response(200, json={'items': [{'id': vid, 'snippet': {'title': 'Video', 'channelId': 'OWNER'},
                         'statistics': {'commentCount': str(self.threads * (self.reply_count + 1))}}
                         for vid in p['id'].split(',')]})
        if self.error:
            return httpx.Response(403, json={'error': {'message': self.error, 'errors': [{'reason': self.error}]}})
        self.active += resource == 'commentThreads'
        self.reply_active += resource == 'comments'
        self.peak = max(self.peak, self.active)
        self.reply_peak = max(self.reply_peak, self.reply_active)
        self.events.append(('start', p.get('videoId'), time.monotonic()))
        try:
            await asyncio.sleep(self.delay)
            start = int(p.get('pageToken', 0))
            amount = int(p['maxResults'])
            if resource == 'comments':
                items = [raw(f"{p['parentId']}.r{i}", True) for i in range(start, min(start + amount, self.reply_count))]
                end = start + len(items)
                response = {'items': items}
                if end < self.reply_count:
                    response['nextPageToken'] = str(end)
            else:
                total = 0 if self.empty else self.threads
                items = []
                for i in range(start, min(start + amount, total)):
                    cid = f"{p['videoId']}-c{i}"
                    thread = {'id': cid, 'snippet': {'totalReplyCount': self.reply_count,
                              'topLevelComment': raw(cid)}}
                    if self.embedded:
                        thread['replies'] = {'comments': [raw(f'{cid}.r{j}', True) for j in range(self.reply_count)]}
                    items.append(thread)
                end = start + len(items)
                response = {'items': items}
                if end < total:
                    response['nextPageToken'] = str(end)
            return httpx.Response(200, json=response)
        finally:
            self.active -= resource == 'commentThreads'
            self.reply_active -= resource == 'comments'
            self.events.append(('end', p.get('videoId'), time.monotonic()))


async def run(data=None, handler=None, public=None, store=None):
    cfg = Config.parse(data or {'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 10}]})
    handler = handler or Handler()
    store = store or MemoryStore()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        api = Api(client, Credentials('private-test-secret'), sleep=lambda _: asyncio.sleep(0))
        engine = Engine(cfg, api, public or Public(), store)
        result = await engine.run()
    return result, store, handler


@pytest.mark.parametrize('amount', [10, 100, 1500, None])
async def test_amounts_and_privacy(amount):
    entry = {'url': f'https://youtu.be/{IDS[0]}'}
    if amount is not None:
        entry['maxComments'] = amount
    result, store, handler = await run({'videos': [entry]})
    assert len(store.data) == (amount or 100)
    assert len({row['commentId'] for row in store.data}) == len(store.data)
    assert max(store.batches) <= 100
    assert len(store.state['emittedIds']) == len(store.data)
    assert store.state['records'] == {}
    assert result['totalFetched'] == len(store.data)
    forbidden = ['authorChannelId', 'authorChannelUrl', 'authorThumbnailUrl', 'avatarUrl', 'INTERNAL']
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(store.data)
    for representation in (json.dumps(store.data), buf.getvalue(), json.dumps(result), json.dumps(store.state),
                           Path('.actor/dataset_schema.json').read_text()):
        assert all(word not in representation for word in forbidden)
    assert all(set(row) == set(FIELDS) for row in store.data)
    assert store.data[0]['publishedAt'] == '2025-01-01T01:02:03Z'
    assert store.data[0]['updatedAt'] == '2025-02-01T01:02:03Z'
    assert store.data[0]['likeCount'] == 1234
    assert store.data[0]['publishedAtPrecision'] == 'exact'
    assert store.data[1]['isCreatorReply'] is True
    assert store.data[1]['parentId'] == store.data[0]['commentId']
    assert all('authorChannelUrl' not in p.get('fields', '') and 'authorProfileImageUrl' not in p.get('fields', '')
               for _, p, _ in handler.requests)


@pytest.mark.parametrize('sort,order', [('newest', 'time'), ('top', 'relevance')])
@pytest.mark.parametrize('include,cap', [(False, None), (True, 1), (True, None)])
async def test_sort_and_replies(sort, order, include, cap):
    data = {'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 10}],
            'sort': sort, 'includeReplies': include}
    if cap:
        data['maxRepliesPerThread'] = cap
    result, store, handler = await run(data, Handler(embedded=True))
    assert len(store.data) == 10
    assert not any(resource == 'comments' for resource, _, _ in handler.requests)
    assert all(p['order'] == order for resource, p, _ in handler.requests if resource == 'commentThreads')
    assert all(row['sort'] == sort for row in store.data)
    if not include:
        assert all(not row['isReply'] for row in store.data)
    elif cap:
        assert all((row['fetchedReplyCount'] or 0) <= cap for row in store.data)
        assert result['videos'][0]['repliesLimited']


async def test_shared_budget_and_workers():
    data = {'videos': [{'url': f'https://youtu.be/{vid}', 'maxComments': 100} for vid in IDS],
            'maxTotalComments': 175}
    result, store, handler = await run(data, Handler(delay=.005))
    assert len(store.data) == 175
    assert handler.peak == 3
    assert handler.reply_peak <= 4
    assert len({h['x-goog-api-key'] for _, _, h in handler.requests}) == 1
    assert all('key' not in p for _, p, _ in handler.requests)
    assert result['totalFetched'] == 175


async def test_immediate_reassignment():
    data = {'videos': [{'url': f'https://youtu.be/{vid}', 'maxComments': 10} for vid in IDS], 'includeReplies': False}
    handler = Handler(delay=.02)
    await run(data, handler)
    starts = [vid for event, vid, _ in handler.events if event == 'start']
    assert starts[:3] == IDS[:3]
    assert starts[3] == IDS[3]
    end = next(t for event, vid, t in handler.events if event == 'end' and vid == IDS[0])
    start = next(t for event, vid, t in handler.events if event == 'start' and vid == IDS[3])
    assert start - end < .02


@pytest.mark.parametrize('reason,status', [('commentsDisabled', 'comments_disabled'), ('videoNotFound', 'unavailable'),
                                         ('keyInvalid', 'failed'), ('quotaExceeded', 'failed')])
async def test_api_errors_do_not_fallback(reason, status):
    public = Public()
    result, store, _ = await run(handler=Handler(error=reason), public=public)
    assert result['videos'][0]['status'] == status
    assert public.calls == 0
    assert not store.data


def public_events(incomplete=False):
    return [{'type': 'comment', 'comment': {'id': 'p', 'text': 'Public', 'parent': 'root',
              'timestamp': 1700000000, 'author': 'Public Author', 'is_pinned': True}},
            {'type': 'comment', 'comment': {'id': 'r1', 'text': 'Reply', 'parent': 'p', 'author_is_uploader': True}},
            {'type': 'comment', 'comment': {'id': 'r2', 'text': 'Nested', 'parent': 'r1'}},
            {'type': 'complete', 'incomplete': incomplete}]


@pytest.mark.parametrize('incomplete', [False, True])
async def test_empty_api_public_fallback_same_session(incomplete):
    public = Public(public_events(incomplete))
    result, store, handler = await run(handler=Handler(empty=True), public=public)
    assert public.calls == 1
    assert sum(resource == 'commentThreads' for resource, _, _ in handler.requests) == 2
    assert len(store.data) == 3
    assert result['videos'][0]['status'] == ('partial' if incomplete else 'completed')
    assert store.data[0]['fetchedReplyCount'] == 2
    assert store.data[0]['updatedAt'] is None
    assert store.data[0]['publishedAtPrecision'] == 'estimated'
    assert store.data[2]['parentId'] == 'r1'
    assert store.data[2]['threadRootId'] == 'p'


@pytest.mark.parametrize('events,status', [([{'type': 'complete', 'verifiedEmpty': True}], 'no_comments'),
    ([{'type': 'complete'}], 'failed'), ([{'type': 'failure', 'message': 'DNS failure'}], 'failed'),
    ([{'type': 'disabled'}, {'type': 'complete'}], 'comments_disabled')])
async def test_public_empty_verification(events, status):
    result, _, _ = await run(handler=Handler(empty=True), public=Public(events))
    assert result['videos'][0]['status'] == status


async def test_empty_fallback_disabled():
    result, _, _ = await run({'videos': [{'url': f'https://youtu.be/{IDS[0]}'}], 'enableFallback': False},
                             Handler(empty=True))
    assert result['videos'][0]['status'] == 'failed'
    assert result['videos'][0]['errorKind'] == 'empty_unverified'


async def test_partial_public_deadline_retains_records():
    events = public_events()[:-1]
    result, store, _ = await run({'videos': [{'url': f'https://youtu.be/{IDS[0]}'}], 'videoTimeoutSeconds': 1},
                                 Handler(empty=True), Public(events, delay=3))
    assert result['videos'][0]['status'] == 'partial'
    assert len(store.data) == 3
    assert store.data[0]['fetchedReplyCount'] == 2


async def test_enrichment_preserves_api_fields_and_joins_without_duplicates():
    cid = IDS[0] + '-c0'
    events = [{'type': 'comment', 'comment': {'id': cid, 'text': 'Different public text',
               'timestamp': 0, 'like_count': 1, 'is_pinned': True, 'is_favorited': True}},
              {'type': 'complete'}]
    result, store, _ = await run({'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 10}],
                     'publicDetails': ['pinned', 'creatorHeart', 'nestedReplies']}, public=Public(events))
    assert len(store.data) == 10
    assert store.data[0]['text'] == 'Full body\n🙂'
    assert store.data[0]['likeCount'] == 1234
    assert store.data[0]['publishedAt'] == '2025-01-01T01:02:03Z'
    assert store.data[0]['isPinned'] is True
    assert result['videos'][0]['publicDetailsStatus'] == 'partial'
    assert result['videos'][0]['status'] == 'completed'


async def test_failed_enrichment_preserves_api_results():
    result, store, _ = await run({'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 10}],
                        'publicDetails': ['creatorHeart']}, public=Public([{'type': 'failure', 'message': 'Failed'}]))
    assert len(store.data) == 10
    assert result['videos'][0]['status'] == 'completed'
    assert result['videos'][0]['publicDetailsStatus'] == 'failed'


async def test_dns_and_transient_retries():
    attempts = 0
    normal = Handler()

    async def handler(request):
        nonlocal attempts
        if request.url.path.endswith('commentThreads'):
            attempts += 1
            if attempts == 1:
                raise httpx.ConnectError('DNS failure', request=request)
            if attempts == 2:
                return httpx.Response(503, json={'error': {'message': 'Temporarily unavailable'}})
        return await normal(request)
    result, store, _ = await run(handler=handler)
    assert len(store.data) == 10
    assert result['retryCount'] == 2


async def test_retry_exhaustion_and_redaction():
    def handler(request):
        raise httpx.ConnectError('Secret private-test-secret', request=request)
    result, _, _ = await run(handler=handler)
    assert result['videos'][0]['status'] == 'failed'
    assert result['retryCount'] == 2
    assert 'private-test-secret' not in json.dumps(result)
    assert Credentials('private-test-secret').redact('private-test-secret') == '[REDACTED]'


async def test_unavailable_metadata():
    async def handler(request):
        return httpx.Response(200, json={'items': []})
    result, store, _ = await run(handler=handler)
    assert result['videos'][0]['status'] == 'unavailable'
    assert not store.data


async def test_resume_after_ambiguous_dataset_write():
    class CrashStore(MemoryStore):
        crashed = False

        async def push(self, rows):
            await super().push(rows)
            if not self.crashed:
                self.crashed = True
                raise RuntimeError('Crash after a successful dataset write')
    store = CrashStore()
    data = {'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 150}], 'includeReplies': False}
    with pytest.raises(RuntimeError):
        await run(data, store=store)
    result, store, _ = await run(data, store=store)
    assert result['totalFetched'] == 150
    assert len(store.data) == 150
    assert len({r['commentId'] for r in store.data}) == 150
    _, store, handler = await run(data, store=store)
    assert len(store.data) == 150
    assert not handler.requests


def test_input_normalization_validation():
    assert video_id(f'https://www.youtube.com/watch?v={IDS[0]}&list=foo') == IDS[0]
    assert video_id(f'https://youtube.com/shorts/{IDS[0]}') == IDS[0]
    config = Config.parse({'videos': [{'url': f'https://youtu.be/{IDS[0]}'},
                          {'url': f'https://www.youtube.com/watch?v={IDS[0]}&si=abc', 'maxComments': 100}]})
    assert len(config.videos) == 1
    assert config.videos[0].limit == 100
    with pytest.raises(ValueError, match='Conflicting'):
        Config.parse({'videos': [{'url': f'https://youtu.be/{IDS[0]}'},
                       {'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 10}]})
    for value in (0, -1, '10', True, 1.5):
        with pytest.raises(ValueError):
            Config.parse({'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': value}]})
    for url in ('file:///etc/passwd', f'https://evil.com/watch?v={IDS[0]}', 'https://youtube.com/playlist?list=x'):
        with pytest.raises(ValueError):
            video_id(url)


def test_null_fields_and_creator_identity():
    video = Config.parse({'videos': [{'url': f'https://youtu.be/{IDS[0]}'}]}).videos[0]
    row = api_row({'id': 'id', 'snippet': {}}, video, {}, 'newest', root='parent')
    assert row['isCreatorReply'] is None
    assert row['publishedAt'] is None
    row = public_row({'id': 'id'}, video, {}, 'newest', {})
    assert row['isPinned'] is None and row['heartedByCreator'] is None


async def test_pending_reply_page_recovery():
    handler = Handler(threads=2, replies=150)
    interrupted = False

    async def crash_on_second_reply_page(request):
        nonlocal interrupted
        if request.url.path.endswith('/comments') and request.url.params.get('pageToken') == '100' and not interrupted:
            interrupted = True
            raise asyncio.CancelledError()
        return await handler(request)

    data = {'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 151}]}
    store = MemoryStore()
    with pytest.raises(asyncio.CancelledError):
        await run(data, handler=crash_on_second_reply_page, store=store)
    assert not store.state['videos'][IDS[0]]['finalized']
    assert len(next(iter(store.state['videos'][IDS[0]]['jobs'].values()))['rows']) == 101
    normal = Handler(threads=2, replies=150)
    result, store, _ = await run(data, handler=normal, store=store)
    reply_requests = [p for resource, p, _ in normal.requests if resource == 'comments']
    assert len(reply_requests) == 1
    assert reply_requests[0]['pageToken'] == '100'
    assert len(store.data) == 151
    assert store.data[0]['fetchedReplyCount'] == 150


async def test_deadline_retains_completed_reply_pages():
    handler = Handler(threads=2, replies=150)

    async def delayed_second_page(request):
        if request.url.path.endswith('/comments') and request.url.params.get('pageToken'):
            await asyncio.sleep(2)
        return await handler(request)
    result, store, _ = await run({'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 151}],
                                  'videoTimeoutSeconds': 1}, handler=delayed_second_page)
    assert result['videos'][0]['status'] == 'partial'
    assert len(store.data) == 101
    assert store.data[0]['fetchedReplyCount'] == 100


async def test_one_second_flush_before_next_page_finishes():
    handler = Handler(threads=10, replies=0)
    store = MemoryStore()
    saved_during_request = False

    async def slow_second_page(request):
        nonlocal saved_during_request
        if request.url.path.endswith('commentThreads') and request.url.params.get('pageToken'):
            await asyncio.sleep(1.3)
            saved_during_request = bool(store.data)
        response = await handler(request)
        if request.url.path.endswith('commentThreads') and not request.url.params.get('pageToken'):
            body = response.json()
            body['items'] = body['items'][:4]
            body['nextPageToken'] = '4'
            return httpx.Response(200, json=body)
        return response
    await run(handler=slow_second_page, store=store)
    assert saved_during_request
    assert len(store.data) == 10


@pytest.mark.parametrize('status', [429, 500, 501, 502, 503, 504])
async def test_http_retry_statuses_with_non_json_body(status):
    attempts = 0
    handler = Handler()

    async def transient(request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(status, text='<html>temporary error</html>', headers={'Retry-After': '0'})
        return await handler(request)
    result, store, _ = await run(handler=transient)
    assert len(store.data) == 10
    assert result['retryCount'] == 1


async def test_completed_public_limit_is_partial_when_incomplete():
    result, store, _ = await run({'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 3}]},
                                handler=Handler(empty=True), public=Public(public_events(True)))
    assert len(store.data) == 3
    assert result['videos'][0]['videoLimitReached']
    assert result['videos'][0]['status'] == 'partial'


async def test_video_failure_preserves_another_video():
    normal = Handler()

    async def handler(request):
        if request.url.params.get('videoId') == IDS[0]:
            return httpx.Response(403, json={'error': {'errors': [{'reason': 'commentsDisabled'}]}})
        return await normal(request)
    result, store, _ = await run({'videos': [{'url': f'https://youtu.be/{vid}', 'maxComments': 10}
                                             for vid in IDS[:2]]}, handler=handler)
    assert [v['status'] for v in result['videos']] == ['comments_disabled', 'completed']
    assert len(store.data) == 10


async def test_api_key_redaction_in_error_messages():
    async def handler(request):
        return httpx.Response(400, json={'error': {'message': 'API key private-test-secret invalid',
                                                 'errors': [{'reason': 'keyInvalid'}]}})
    result, store, _ = await run(handler=handler)
    assert 'private-test-secret' not in json.dumps(result)
    assert 'private-test-secret' not in json.dumps(store.state)


async def test_private_key_redaction_inside_content_and_metadata():
    normal = Handler()

    async def handler(request):
        response = await normal(request)
        body = response.json()
        if request.url.path.endswith('videos'):
            body['items'][0]['snippet']['title'] = 'private-test-secret'
        elif request.url.path.endswith('commentThreads'):
            body['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay'] = 'private-test-secret'
        return httpx.Response(200, json=body)
    result, store, _ = await run(handler=handler)
    assert store.data[0]['text'] == '[REDACTED]'
    assert all('private-test-secret' not in json.dumps(value) for value in (result, store.data, store.state))


def test_schema_and_build_privacy():
    forbidden = {'authorChannelId', 'authorChannelUrl', 'authorProfileImageUrl', 'authorThumbnailUrl', 'avatarUrl'}
    def keys(value):
        if isinstance(value, dict):
            return set(value) | set().union(*(keys(v) for v in value.values()))
        if isinstance(value, list):
            return set().union(*(keys(v) for v in value))
        return set()
    for path in Path('.actor').glob('*.json'):
        assert not keys(json.loads(path.read_text())) & forbidden
    assert Path('.dockerignore').read_text().startswith('*\n')
    assert 'YOUTUBE_API_KEY' not in Path('Dockerfile').read_text()


async def test_public_work_releases_api_slot_immediately():
    normal = Handler()
    public_started = asyncio.Event()
    fourth_started = asyncio.Event()

    async def handler(request):
        vid = request.url.params.get('videoId')
        if vid in IDS[:3]:
            return httpx.Response(200, json={'items': []})
        if vid == IDS[3]:
            fourth_started.set()
        return await normal(request)

    class SlowPublic:
        slots = asyncio.Semaphore(2)
        active = peak = 0

        async def stream(self, video, config):
            async with self.slots:
                self.active += 1
                self.peak = max(self.peak, self.active)
                public_started.set()
                await asyncio.wait_for(fourth_started.wait(), timeout=1)
                await asyncio.sleep(.02)
                self.active -= 1
                yield {'type': 'complete', 'verifiedEmpty': True}

    public = SlowPublic()
    data = {'videos': [{'url': f'https://youtu.be/{vid}', 'maxComments': 10} for vid in IDS]}
    result, store, _ = await run(data, handler=handler, public=public)
    assert public_started.is_set() and fourth_started.is_set()
    assert public.peak == 2
    assert len(store.data) == 10
    assert [s['status'] for s in result['videos']] == ['no_comments'] * 3 + ['completed']


@pytest.mark.parametrize('include,cap,expected', [(True, 1, 2), (False, None, 1)])
async def test_public_limits_are_enforced_for_nested_replies(include, cap, expected):
    data = {'videos': [{'url': f'https://youtu.be/{IDS[0]}'}], 'includeReplies': include}
    if cap:
        data['maxRepliesPerThread'] = cap
    result, store, _ = await run(data, handler=Handler(empty=True), public=Public(public_events()))
    assert len(store.data) == expected
    assert store.data[0]['fetchedReplyCount'] == expected - 1


async def test_nested_enrichment_preserves_known_root_when_parent_missing():
    parent = IDS[0] + '-c0'
    reply = parent + '.r0'
    events = [{'type': 'comment', 'comment': {'id': reply, 'parent': 'outside-selected-records'}},
              {'type': 'complete'}]
    result, store, _ = await run({'videos': [{'url': f'https://youtu.be/{IDS[0]}', 'maxComments': 3}],
                       'publicDetails': ['nestedReplies']}, public=Public(events))
    assert store.data[1]['parentId'] == 'outside-selected-records'
    assert store.data[1]['threadRootId'] == parent
    assert store.data[1]['parentRelationship'] == 'immediate'
