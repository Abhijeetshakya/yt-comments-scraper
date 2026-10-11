import asyncio
from collections import deque, Counter
import json
import time
from dataclasses import replace
from .dates import matches_cutoff, resolve_cutoff, publication_time
from .api import ApiError, THREAD_FIELDS, REPLY_FIELDS
from .records import api_row, public_row, recount, FIELDS


def stats(video):
    return {'videoId': video.id, 'videoUrl': video.url, 'status': 'pending',
            'requestedCommentLimit': video.limit, 'reportedCommentCount': None,
            'totalFetched': 0, 'topLevelFetched': 0, 'repliesFetched': 0,
            'elapsedSeconds': 0, 'apiRequestCount': 0, 'retryCount': 0,
            'publicRequestCount': 0, 'publicRetryCount': 0,
            'videoLimitReached': False, 'runLimitReached': False, 'repliesLimited': False,
            'publicDetailsStatus': 'not_requested', 'warnings': [], 'sourcesUsed': [], 'videoTitle': None}


class Engine:
    """Durable ordered extraction with shared budgets and an immutable dataset sink."""
    def __init__(self, config, api, public, store):
        self.config, self.api, self.public, self.store = config, api, public, store
        self.lock = asyncio.Lock()
        self.checkpoint_lock = asyncio.Lock()
        self.flush_lock = asyncio.Lock()
        self.api_video_slots = asyncio.Semaphore(config.concurrency)
        self.ready = deque()
        self.saved = set()
        self.first_saved_seconds = None
        self.started = time.monotonic()
        self.state = None
        self.last_checkpoint = 0.0
        self.last_snapshot = None
        self.sink_failed = False
        self.video_counts = Counter()
        self.root_reply_counts = Counter()

    async def checkpoint(self, force=False):
        async with self.checkpoint_lock:
            if not force and time.monotonic() - self.last_checkpoint < 1:
                return
            # Saved rows already live in the dataset and are reconciled on restart. Persist only
            # unsaved/staged bodies plus emitted IDs, keeping checkpoints small as output grows.
            snapshot = {**self.state, 'records': {key: row for key, row in self.state['records'].items()
                                                if key not in self.saved}, 'emittedIds': list(self.saved)}
            encoded = self.api.credentials.redact(json.dumps(snapshot))
            if encoded != self.last_snapshot:
                await self.store.checkpoint(json.loads(encoded))
                self.last_snapshot = encoded
            self.last_checkpoint = time.monotonic()

    async def initialize(self):
        state = await self.store.load()
        if state and state.get('fingerprint') != self.config.fingerprint:
            raise ValueError('Stored run input differs. Start a fresh run/storage directory for changed input.')
        self.state = state or {'fingerprint': self.config.fingerprint, 'records': {}, 'videos': {}}
        if 'publishedAfter' not in self.state:
            self.state['publishedAfter'] = resolve_cutoff(self.config.published_after)
        self.config = replace(self.config, cutoff=self.state['publishedAfter'])
        for video in self.config.videos:
            self.state['videos'].setdefault(video.id, {'summary': stats(video), 'meta': None,
                'pageToken': None, 'startedPages': False, 'pending': [], 'jobs': {},
                'apiDone': False, 'finalized': False, 'publicDone': False, 'needsPublic': False,
                'metadataSeconds': 0})
            self.state['videos'][video.id]['summary']['sort'] = self.config.sort
            self.state['videos'][video.id]['summary']['publishedAfter'] = self.config.cutoff
            self.state['videos'][video.id]['summary'].setdefault('dateCutoffReached', False)
            self.state['videos'][video.id]['summary'].setdefault('missingPublicationDates', 0)
        # Dataset is authoritative after an interrupted write/checkpoint sequence.
        async for row in self.store.existing():
            if row.get('videoId') not in self.state['videos'] or not row.get('commentId'):
                raise ValueError('Dataset contains records outside this run; use fresh storage.')
            if set(row) - set(FIELDS):
                raise ValueError('Dataset contains incompatible fields; use fresh storage.')
            safe = {field: row.get(field) for field in FIELDS}
            key = self.key(safe)
            self.saved.add(key)
            self.state['records'][key] = safe
        for row in self.state['records'].values():
            self.video_counts[row['videoId']] += 1
            if row['isReply']:
                self.root_reply_counts[row['videoId'] + ':' + row['threadRootId']] += 1
        for key, row in self.state['records'].items():
            vstate = self.state['videos'][row['videoId']]
            if key not in self.saved and (vstate['finalized'] or
                    (not self.config.details and row['source'] == 'youtube_api')):
                self.ready.append(key)
        await self.checkpoint()

    @staticmethod
    def key(row):
        return row['videoId'] + ':' + row['commentId']

    def rows(self, video):
        return [row for row in self.state['records'].values() if row['videoId'] == video.id]

    def remaining(self, video):
        remaining = video.limit - self.video_counts[video.id]
        if self.config.total_cap is not None:
            remaining = min(remaining, self.config.total_cap - len(self.state['records']))
        return max(0, remaining)

    async def accept_thread(self, video, rows, stage=False):
        async with self.lock:
            accepted = []
            for row in rows:
                if not self.date_matches(video, row.get('publishedAt')):
                    continue
                row = {key: self.api.credentials.redact(value) if isinstance(value, str) else value
                       for key, value in row.items()}
                key = self.key(row)
                if key in self.state['records']:
                    continue
                if not self.remaining(video):
                    break
                # A reply is only useful when its root is in this run's selected records.
                if row['isReply'] and video.id + ':' + row['threadRootId'] not in self.state['records']:
                    continue
                if row['isReply'] and not self.config.include_replies:
                    continue
                if row['isReply'] and self.config.reply_cap is not None:
                    existing_replies = self.root_reply_counts[video.id + ':' + row['threadRootId']]
                    if existing_replies >= self.config.reply_cap:
                        self.state['videos'][video.id]['summary']['repliesLimited'] = True
                        continue
                self.state['records'][key] = row
                self.video_counts[video.id] += 1
                if row['isReply']:
                    self.root_reply_counts[video.id + ':' + row['threadRootId']] += 1
                accepted.append(row)
            # Count only replies actually accepted by the shared budget.
            if rows and not rows[0]['isReply']:
                parent = self.state['records'].get(self.key(rows[0]))
                if parent and self.key(parent) not in self.saved:
                    parent['fetchedReplyCount'] = self.root_reply_counts[self.key(parent)]
            if not self.config.details and not stage:
                self.ready.extend(self.key(row) for row in accepted)
        await self.checkpoint()
        if len(self.ready) >= 100:
            await self.flush()

    def date_matches(self, video, published):
        if self.config.cutoff and publication_time(published) is None:
            summary = self.state['videos'][video.id]['summary']
            summary['missingPublicationDates'] += 1
            warning = 'Comments with unavailable publication dates were excluded by the date filter.'
            if warning not in summary['warnings']:
                summary['warnings'].append(warning)
        return matches_cutoff(published, self.config.cutoff)

    async def flush(self):
        async with self.flush_lock:
            while self.ready:
                keys = [self.ready.popleft() for _ in range(min(100, len(self.ready)))]
                keys = list(dict.fromkeys(key for key in keys if key not in self.saved))
                if not keys:
                    continue
                try:
                    await self.store.push([self.state['records'][key] for key in keys])
                except BaseException:
                    self.sink_failed = True
                    self.ready.extendleft(reversed(keys))
                    raise
                self.saved.update(keys)
                if self.first_saved_seconds is None:
                    self.first_saved_seconds = time.monotonic() - self.started
            await self.checkpoint()

    async def timer(self):
        while True:
            await asyncio.sleep(1)
            await self.flush()

    async def metadata(self):
        missing = [v for v in self.config.videos if self.state['videos'][v.id]['meta'] is None
                   and not self.state['videos'][v.id]['finalized']]
        for offset in range(0, len(missing), 50):
            group = missing[offset:offset + 50]
            summary = self.state['videos'][group[0].id]['summary']
            metadata_started = time.monotonic()
            try:
                response = await self.api.request('videos', {
                    'part': 'snippet,statistics', 'id': ','.join(v.id for v in group),
                    'fields': 'items(id,snippet(title,channelId),statistics/commentCount)'}, summary,
                    time.monotonic() + self.config.timeout)
                by_id = {item['id']: item for item in response.get('items', [])}
                for video in group:
                    vs = self.state['videos'][video.id]
                    item = by_id.get(video.id)
                    if item is None:
                        vs['summary'].update(status='unavailable', warnings=['Video metadata is unavailable.'])
                        vs['finalized'] = True
                    else:
                        vs['meta'] = {key: self.api.credentials.redact(value) if isinstance(value, str) else value
                                      for key, value in item.get('snippet', {}).items()}
                        vs['summary']['videoTitle'] = vs['meta'].get('title')
                        count = item.get('statistics', {}).get('commentCount')
                        vs['summary']['reportedCommentCount'] = int(count) if count is not None else None
            except (ApiError, TimeoutError) as error:
                for video in group:
                    vs = self.state['videos'][video.id]
                    vs['summary'].update(status='failed', errorKind=getattr(error, 'kind', 'timeout'),
                                         warnings=[str(error)])
                    vs['finalized'] = True
            for video in group:
                self.state['videos'][video.id]['metadataSeconds'] = time.monotonic() - metadata_started
            await self.checkpoint()

    async def reply_job(self, video, job, summary, deadline):
        if job['done']:
            return
        while len(job['rows']) - 1 < job['desired']:
            params = {'part': 'snippet', 'parentId': job['rows'][0]['commentId'],
                      'maxResults': min(100, job['desired'] - len(job['rows']) + 1),
                      'textFormat': 'plainText', 'fields': REPLY_FIELDS}
            if job['token']:
                params['pageToken'] = job['token']
            page = await self.api.replies(params, summary, deadline)
            seen = {r['commentId'] for r in job['rows']}
            job['rows'].extend(api_row(raw, video, self.state['videos'][video.id]['meta'], self.config.sort,
                                      root=job['rows'][0]['commentId']) for raw in page.get('items', [])
                               if raw['id'] not in seen and self.date_matches(video, raw['snippet'].get('publishedAt')))
            token = page.get('nextPageToken')
            if token and token == job['token']:
                raise ApiError('incomplete', 'Reply pagination repeated a continuation token.')
            job['token'] = token
            job['done'] = not token or len(job['rows']) - 1 >= job['desired']
            await self.checkpoint(force=True)
            if job['done']:
                return
        job['done'] = True

    async def pending_threads(self, video, vs, deadline):
        summary = vs['summary']
        while vs['pending'] and self.remaining(video):
            # Plan a small ordered batch using the combined remaining amount.
            available = self.remaining(video)
            batch = []
            for thread in vs['pending']:
                raw = thread['snippet']['topLevelComment']
                root = raw['id']
                if root not in vs['jobs']:
                    total = thread['snippet'].get('totalReplyCount', 0)
                    desired = min(total, max(0, available - 1)) if self.config.include_replies else 0
                    if self.config.reply_cap is not None:
                        desired = min(desired, self.config.reply_cap)
                        if self.config.include_replies and total > self.config.reply_cap:
                            summary['repliesLimited'] = True
                    embedded = thread.get('replies', {}).get('comments', [])
                    rows = [api_row(raw, video, vs['meta'], self.config.sort, reply_count=total)]
                    complete = len(embedded) >= total
                    if complete:
                        eligible = [reply for reply in embedded
                                    if self.date_matches(video, reply['snippet'].get('publishedAt'))]
                        rows += [api_row(reply, video, vs['meta'], self.config.sort, root=root)
                                 for reply in eligible[:desired]]
                    vs['jobs'][root] = {'rows': rows, 'desired': desired, 'token': None,
                                        'done': complete or desired == 0}
                job = vs['jobs'][root]
                batch.append((root, job))
                available -= 1 + job['desired']
                if available <= 0 or len(batch) >= 4:
                    break
            await self.checkpoint()
            errors = await asyncio.gather(*(self.reply_job(video, job, summary, deadline) for _, job in batch),
                                          return_exceptions=True)
            for error in errors:
                if isinstance(error, asyncio.CancelledError):
                    raise error
            for root, job in batch:
                await self.accept_thread(video, job['rows'])
                vs['jobs'].pop(root, None)
                vs['pending'].pop(0)
            await self.checkpoint()
            for error in errors:
                if isinstance(error, BaseException):
                    raise error

    async def api_video(self, video, vs, deadline):
        summary = vs['summary']
        while self.remaining(video) and not vs['apiDone']:
            await self.pending_threads(video, vs, deadline)
            if not self.remaining(video):
                return
            if vs['startedPages'] and not vs['pageToken']:
                vs['apiDone'] = True
                return
            params = {'part': 'snippet,replies' if self.config.include_replies else 'snippet',
                      'videoId': video.id, 'order': 'time' if self.config.sort == 'newest' else 'relevance',
                      'textFormat': 'plainText', 'maxResults': min(100, self.remaining(video)),
                      'fields': THREAD_FIELDS}
            if vs['pageToken']:
                params['pageToken'] = vs['pageToken']
            page = await self.api.request('commentThreads', params, summary, deadline)
            if not page.get('items') and not vs['startedPages']:
                summary['retryCount'] += 1
                page = await self.api.request('commentThreads', params, summary, deadline)
                if not page.get('items'):
                    if not self.config.fallback:
                        raise ApiError('empty_unverified', 'API returned no comments; public verification is disabled.')
                    vs['needsPublic'] = True
                    vs['apiDone'] = True
                    await self.checkpoint(force=True)
                    return
            threads = page.get('items', [])
            vs['pending'] = [thread for thread in threads if self.date_matches(
                video, thread['snippet']['topLevelComment']['snippet'].get('publishedAt'))]
            # Stop only on a whole page of dated, older roots. Never treat filtered-out
            # comments as an empty API response, and don't stop on a mixed page.
            if self.config.cutoff and threads and all(
                (published := publication_time(thread['snippet']['topLevelComment']['snippet'].get('publishedAt')))
                is not None and published < publication_time(self.config.cutoff) for thread in threads):
                vs['apiDone'] = True
                summary['dateCutoffReached'] = True
            # Drop transient author IDs as soon as comments are projected. Checkpoints contain only safe rows.
            # Pending pages remain internal; sanitize them by projecting into jobs before checkpointing below.
            for thread in vs['pending']:
                raw = thread['snippet']['topLevelComment']
                s = raw['snippet']
                creator = s.get('authorChannelId', {}).get('value')
                s['_creatorMatch'] = creator == vs['meta'].get('channelId') if creator else None
                for reply in thread.get('replies', {}).get('comments', []):
                    rs = reply['snippet']
                    rid = rs.get('authorChannelId', {}).get('value')
                    rs['_creatorMatch'] = rid == vs['meta'].get('channelId') if rid else None
                    rs.pop('authorChannelId', None)
                s.pop('authorChannelId', None)
            old_token = vs['pageToken']
            vs['pageToken'] = page.get('nextPageToken')
            if old_token and old_token == vs['pageToken']:
                raise ApiError('incomplete', 'Thread pagination repeated a continuation token.')
            vs['startedPages'] = True
            await self.checkpoint(force=True)

    async def public_video(self, video, vs, deadline, enrich):
        summary = vs['summary']
        roots, collected = {}, []
        complete = verified_empty = incomplete = disabled = failed = False
        found_comments = False
        if enrich:
            summary['publicDetailsStatus'] = 'running'
        # API publication dates remain authoritative during enrichment, even if the
        # public estimate straddles the cutoff.
        stream = self.public.stream(video, replace(self.config, cutoff=None) if enrich else self.config)
        try:
            async with asyncio.timeout(max(0, deadline - time.monotonic())):
                async for event in stream:
                    kind = event['type']
                    if kind == 'metadata':
                        for key in ('title', 'channelId'):
                            if not vs['meta'].get(key):
                                vs['meta'][key] = event.get(key)
                    elif kind == 'comment':
                        found_comments = True
                        row = public_row(event['comment'], video, vs['meta'], self.config.sort, roots)
                        if enrich:
                            original = self.state['records'].get(self.key(row))
                            if original:
                                changed = False
                                for option, field in (('pinned', 'isPinned'), ('creatorHeart', 'heartedByCreator')):
                                    if option in self.config.details and row[field] is not None:
                                        original[field] = row[field]
                                        changed = True
                                if 'nestedReplies' in self.config.details and original['isReply'] and row['parentId']:
                                    original.update(parentId=row['parentId'], parentRelationship='immediate')
                                    changed = True
                                if changed or ('nestedReplies' in self.config.details and not original['isReply']):
                                    original['publicDetailsSource'] = 'yt_dlp'
                        else:
                            # Stage public rows until thread counts are known. Checkpoint every streamed record.
                            collected.append(row)
                            await self.accept_thread(video, [row], stage=True)
                        await self.checkpoint()
                    elif kind == 'warning':
                        summary['warnings'].append(self.api.credentials.redact(event['message']))
                    elif kind == 'disabled':
                        disabled = True
                    elif kind == 'complete':
                        complete = True
                        verified_empty = event.get('verifiedEmpty', False)
                        incomplete = event.get('incomplete', False)
                        found_comments = found_comments or event.get('foundComments', False)
                        summary['dateCutoffReached'] = summary['dateCutoffReached'] or event.get('dateCutoffReached', False)
                        summary['missingPublicationDates'] += event.get('missingPublicationDates', 0)
                        if event.get('missingPublicationDates'):
                            summary['warnings'].append('Comments with unavailable publication dates were excluded by the date filter.')
                        summary['publicRequestCount'] += event.get('requests', 0)
                        summary['publicRetryCount'] += event.get('retries', 0)
                    elif kind == 'failure':
                        failed = True
                        summary['warnings'].append(event['message'])
                        summary['publicRequestCount'] += event.get('requests', 0)
                        summary['publicRetryCount'] += event.get('retries', 0)
        except TimeoutError:
            failed = True
            summary['warnings'].append('Public extraction reached the video deadline; streamed records were retained.')
        finally:
            await stream.aclose()
        if enrich:
            def established(row):
                if 'pinned' in self.config.details and row['isPinned'] is None:
                    return False
                if 'creatorHeart' in self.config.details and row['heartedByCreator'] is None:
                    return False
                if 'nestedReplies' in self.config.details and row['isReply'] and row['parentRelationship'] != 'immediate':
                    return False
                return row['publicDetailsSource'] == 'yt_dlp'
            matches = [r for r in self.rows(video) if established(r)]
            summary['publicDetailsStatus'] = ('failed' if not matches and failed else
                'partial' if failed or incomplete or len(matches) < len(self.rows(video)) else 'completed')
            if summary['publicDetailsStatus'] != 'completed':
                summary['warnings'].append('Some selected public details could not be established.')
        else:
            recount(self.rows(video))
            if collected or self.rows(video):
                summary['status'] = 'partial' if failed or incomplete or not complete else 'completed'
            elif disabled:
                summary['status'] = 'comments_disabled'
            elif self.config.cutoff and found_comments and complete and not failed and not incomplete:
                summary['status'] = 'no_matching_comments'
            elif complete and verified_empty and not failed and not incomplete:
                summary['status'] = 'no_comments'
            else:
                raise ApiError('verification_failed', 'Public verification did not establish whether comments exist.')
        vs['publicDone'] = True

    async def video(self, video):
        vs = self.state['videos'][video.id]
        if vs['finalized']:
            return
        summary = vs['summary']
        await self.api_video_slots.acquire()
        start = time.monotonic()
        deadline = start + max(0, self.config.timeout - vs.get('metadataSeconds', 0))
        summary['status'] = 'running'
        interrupted = False
        try:
            try:
                if not vs['apiDone']:
                    await self.api_video(video, vs, deadline)
            finally:
                # Public work has independent slots. Immediately give the API slot to the next video.
                self.api_video_slots.release()
            if vs.get('needsPublic') and not vs['publicDone']:
                await self.public_video(video, vs, deadline, enrich=False)
            if summary['status'] == 'running':
                summary['status'] = ('no_matching_comments' if self.config.cutoff and vs['apiDone']
                                     and not self.rows(video) else 'completed')
            if self.config.details and self.rows(video) and not vs['publicDone']:
                await self.public_video(video, vs, deadline, enrich=True)
        except asyncio.CancelledError:
            interrupted = True
            summary['status'] = 'pending'
            raise
        except (ApiError, TimeoutError) as error:
            summary['status'] = 'partial' if self.rows(video) else getattr(error, 'kind', 'failed')
            if summary['status'] not in ('partial', 'comments_disabled', 'unavailable'):
                summary['status'] = 'failed'
            summary['errorKind'] = getattr(error, 'kind', 'timeout')
            summary['warnings'].append(str(error) or 'Video deadline reached; completed records were retained.')
        finally:
            summary['elapsedSeconds'] += time.monotonic() - start
            if interrupted or self.sink_failed:
                await self.checkpoint(force=True)
                if interrupted:
                    raise asyncio.CancelledError
                raise RuntimeError('Dataset write failed. Resume with the same storage to reconcile output.')
            rows = self.rows(video)
            recount(rows)
            summary.update(totalFetched=len(rows), topLevelFetched=sum(not r['isReply'] for r in rows),
                           repliesFetched=sum(r['isReply'] for r in rows), sort=self.config.sort,
                           sourcesUsed=sorted({r['source'] for r in rows}), videoLimitReached=len(rows) >= video.limit,
                           runLimitReached=self.config.total_cap is not None and len(self.state['records']) >= self.config.total_cap)
            vs['finalized'] = True
            self.ready.extend(self.key(row) for row in rows if self.key(row) not in self.saved)
            await self.checkpoint(force=True)
            await self.flush()

    async def run(self):
        await self.initialize()
        metadata_started = time.monotonic()
        await self.metadata()
        metadata_seconds = time.monotonic() - metadata_started
        timer = asyncio.create_task(self.timer())
        workers = [asyncio.create_task(self.video(video)) for video in self.config.videos]
        try:
            # Fail fast on sink failures as well as worker failures; don't silently lose periodic writes.
            group = asyncio.gather(*workers)
            done, _ = await asyncio.wait((group, timer), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                await task
        finally:
            timer.cancel()
            for task in workers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(timer, *workers, return_exceptions=True)
        await self.flush()
        await self.checkpoint(force=True)
        summaries = [self.state['videos'][v.id]['summary'] for v in self.config.videos]
        result = {'videos': summaries, 'totalFetched': len(self.state['records']),
                  'elapsedSeconds': time.monotonic() - self.started,
                  'timeToFirstSavedResultsSeconds': self.first_saved_seconds,
                  'metadataSeconds': metadata_seconds,
                  'apiRequestCount': sum(s['apiRequestCount'] for s in summaries),
                  'retryCount': sum(s['retryCount'] for s in summaries)}
        if hasattr(self.store, 'metrics'):
            result['storageMetrics'] = self.store.metrics.copy()
        await self.store.output(result)
        return result
