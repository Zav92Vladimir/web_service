from task_processor import TaskProcessor
from storage.storage import Storage
from api.api import API


if __name__ == "__main__":
    storage = Storage()
    api = API(storage)
    task_processor = TaskProcessor(storage)
    api.set_execute_and_callback(task_processor.execute, task_processor.callback)
    storage.launch_queue(task_processor.execute, task_processor.callback)
    api.run()
