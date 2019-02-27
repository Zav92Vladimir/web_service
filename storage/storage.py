from datetime import datetime
import asyncpg
import redis
import json


class Storage:

    async def init_connections(self):
        self.init_redis()
        await self.init_psql()

    def init_redis(self):
        self.redis_conn = redis.Redis("redis", 6379)

    async def init_psql(self):

        self.pool = await asyncpg.create_pool(database='tasks', user='test_user', host='psgrsql')
        await self.create_tables()

    async def closing(self, app):
        await self.pool.close()

    async def create_tables(self):

        queries = (
            """
            CREATE TABLE IF NOT EXISTS task_tracking (
                    id SERIAL PRIMARY KEY,
                    status VARCHAR(10),
                    create_time TIMESTAMP,
                    start_time TIMESTAMP,
                    time_to_execute INTEGER
            )
            """,
        )

        async with self.pool.acquire() as psql_conn:
            for query in queries:
                await psql_conn.execute(query)

    def launch_queue(self):
        for _ in range(self.redis_conn.llen("queue")):
            yield _

    def push_task_id(self, value):
        self.redis_conn.rpush("queue", value)

    def get_task_id(self):
        return int(self.redis_conn.lpop("queue"))

    def check_cache(self, key):
        cache = self.redis_conn.get(key)
        return json.loads(cache.decode()) if cache else cache

    def caching(self, key, value):
        self.redis_conn.setex(key, 60*60*24, json.dumps(value))

    async def psql_insert(self, fields, values, return_required=False):
        async with self.pool.acquire() as psql_conn:
            value_placeholders = ", ".join(["${0}".format(it) for it in range(1, len(values) + 1)])
            _fields = ", ".join([it for it in fields])
            return_ = "RETURNING id" if return_required else ""
            sql_string = "INSERT INTO task_tracking ({0}) VALUES ({1}) {2}".format(_fields, value_placeholders, return_)
            return (await psql_conn.fetchrow(sql_string, *tuple(values)))[0] if return_required else None

    async def psql_select(self, searching_param):
        async with self.pool.acquire() as psql_conn:
            result = await psql_conn.fetchrow("SELECT * FROM task_tracking WHERE id = $1", *(searching_param,))
            if result:
                result = [it.strftime("%Y-%m-%d %H:%M:%S") if isinstance(it, datetime) else it for it in result]
            return result

    async def psql_update(self, fields, values, searching_param):
        async with self.pool.acquire() as psql_conn:
            set_query = ", ".join(["{0} = '{1}'".format(f, v) for f, v in zip(fields, values)])
            sql_string = "UPDATE task_tracking SET {0} WHERE id = {1}".format(set_query, "$1")
            await psql_conn.execute(sql_string, *(searching_param, ))
