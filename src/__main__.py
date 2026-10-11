import asyncio
import httpx
from apify import Actor
from apify.events import Event
from .api import Api, Credentials
from .config import Config
from .engine import Engine
from .public import PublicExtractor
from .storage import ActorStore


async def main():
    async with Actor:
        config = Config.parse(await Actor.get_input() or {})
        credentials = Credentials()
        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=32, max_keepalive_connections=24),
                                    follow_redirects=False) as client:
            engine = Engine(config, Api(client, credentials), PublicExtractor(), ActorStore())

            async def persist(_event):
                if engine.state is not None:
                    await engine.checkpoint(force=True)

            Actor.on(Event.PERSIST_STATE, persist)
            Actor.on(Event.MIGRATING, persist)
            result = await engine.run()
            Actor.log.info('Collected %s comments across %s videos.', result['totalFetched'], len(config.videos))


if __name__ == '__main__':
    asyncio.run(main())
