import asyncio
import json
import os
import sys


class PublicExtractor:
    def __init__(self):
        self.slots = asyncio.Semaphore(2)

    async def stream(self, video, config):
        async with self.slots:
            env = {k: v for k, v in os.environ.items() if k not in
                   ('YOUTUBE_API_KEY', 'YOUTUBE_API_KEYS', 'APIFY_TOKEN')}
            process = await asyncio.create_subprocess_exec(
                sys.executable, '-m', 'src.public_worker', stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL, env=env,
                limit=2 ** 22)
            payload = {'url': video.url, 'limit': video.limit, 'sort': 'new' if config.sort == 'newest' else 'top',
                       'includeReplies': config.include_replies, 'replyCap': config.reply_cap,
                       'publishedAfter': config.cutoff}
            process.stdin.write((json.dumps(payload) + '\n').encode())
            await process.stdin.drain()
            process.stdin.close()
            completed = False
            try:
                async for line in process.stdout:
                    event = json.loads(line)
                    if event['type'] in ('complete', 'failure'):
                        completed = True
                    yield event
                await process.wait()
                if not completed:
                    yield {'type': 'failure', 'message': 'Public worker exited without verification.'}
            finally:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    await process.wait()
