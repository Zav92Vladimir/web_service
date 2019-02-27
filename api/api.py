from datetime import datetime
from aiohttp import web


class API:

    def __init__(self, storage, executor):
        self.storage = storage
        self.app = web.Application()
        self.register_shutdown()
        self.executor = executor
        self.app.add_routes([
            web.post("/tasks", self.create_task),
            web.get(r"/tasks/{task_id:[1-9]\d*}", self.get_task_status)
        ])

    def run(self):
        web.run_app(self.app, port=2000)

    def register_shutdown(self):
        self.app.on_shutdown.append(self.storage.closing)

    async def create_task(self, request):
        date = datetime.now()  # .strftime("%Y-%m-%d %H:%M:%S")
        auto_id = await self.storage.psql_insert(fields=("status", "create_time"),
                                           values=("IN Queue", date), return_required=True)
        created_url = "{0}/{1}".format(request.url, auto_id)
        self.storage.push_task_id(auto_id)
        self.executor.submit(self.execute).add_done_callback(self.callback)
        return web.json_response({"task_id": auto_id},
                                 status=201, headers={"Location": created_url})

    async def get_task_status(self, request):
        task_id = request.match_info["task_id"]
        result = self.storage.check_cache(task_id)
        cached = bool(result)
        if not result:
            result = await self.storage.psql_select(int(task_id))
        if not result:
            raise web.HTTPNotFound()
        response = {"status": result[1], "create_time": result[2],
                    "start_time": result[3], "time_to_execute": result[4]}
        if response["status"] == "Completed" and not cached:
            self.storage.caching(task_id, result)
        return web.json_response(response)

    def set_execute_and_callback(self, execute, callback):
        self.execute = execute
        self.callback = callback
