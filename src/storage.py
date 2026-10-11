from apify import Actor
import time


class ActorStore:
    def __init__(self):
        self.metrics = {'checkpointWrites': 0, 'checkpointSeconds': 0.0,
                        'datasetBatches': 0, 'datasetWriteSeconds': 0.0}

    async def load(self):
        return await Actor.get_value('CHECKPOINT')

    async def checkpoint(self, state):
        started = time.monotonic()
        await Actor.set_value('CHECKPOINT', state)
        self.metrics['checkpointWrites'] += 1
        self.metrics['checkpointSeconds'] += time.monotonic() - started

    async def existing(self):
        dataset = await Actor.open_dataset()
        async for row in dataset.iterate_items():
            yield row

    async def push(self, rows):
        started = time.monotonic()
        await Actor.push_data(rows)
        self.metrics['datasetBatches'] += 1
        self.metrics['datasetWriteSeconds'] += time.monotonic() - started

    async def output(self, result):
        await Actor.set_value('OUTPUT', result)
