from datetime import datetime
import random
import time


class TaskProcessor:

    def __init__(self, storage):
        self.storage = storage

    def execute(self):
        time_to_execute = random.randint(0, 10)
        task_id = self.storage.get_task_id()
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.storage.psql_update(fields=("status", "start_time", "time_to_execute"),
                                 values=("Run", date, time_to_execute),
                                 searching_param=task_id)
        time.sleep(time_to_execute)
        return task_id

    def callback(self, future):
        task_id = future.result()
        self.storage.psql_update(fields=("status",), values=("Completed",),
                                 searching_param=task_id)

        result = self.storage.psql_select(searching_param=task_id)
        self.storage.psql_insert(table="completed_tasks",
                                 fields=("id", "create_time", "start_time", "exec_time"),
                                 values=(result[0], *result[2:]))
