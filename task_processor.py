from datetime import datetime
import asyncio
import random
import time


class TaskProcessor:

    def __init__(self, storage):
        self.storage = storage
        self.loop = asyncio.get_event_loop()

    def execute(self):
        time_to_execute = random.randint(0, 10)
        task_id = self.storage.get_task_id()
        date = datetime.now()
        self.loop.create_task(self.storage.psql_update(fields=("status", "start_time", "time_to_execute"),
                                 values=("Run", date, time_to_execute),
                                 searching_param=task_id))
        time.sleep(time_to_execute)
        return task_id

    def callback(self, future):
        task_id = future.result()
        self.loop.create_task(self.async_callback(task_id))

    async def async_callback(self, task_id):
        await self.storage.psql_update(fields=("status",), values=("Completed",),
                                 searching_param=task_id)

        result = await self.storage.psql_select(searching_param=task_id)
        await self.storage.psql_insert(fields=("id", "create_time", "start_time", "exec_time"),
                                 values=(result[0], *result[2:]))
