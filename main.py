from concurrent.futures import ThreadPoolExecutor
from task_processor import TaskProcessor
from storage.storage import Storage
from api.api import API
import asyncio
import uvloop


class Main:

    def __init__(self):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.storage = Storage()
        self.api = API(self.storage, self.executor)
        self.task_processor = TaskProcessor(self.storage)
        self.api.set_execute_and_callback(self.task_processor.execute, self.task_processor.callback)

    async def init(self):
        await self.storage.init_connections()
        for _ in self.storage.launch_queue():
            self.executor.submit(self.task_processor.execute).add_done_callback(self.task_processor.callback)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.init())
        self.api.run()
