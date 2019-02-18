from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import psycopg2
import redis
import json
import time


class Storage:

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.init_redis()
        self.init_psql()

    def init_redis(self):
        time.sleep(5)  # need to wait until db starts inside a container
        self.redis_conn = redis.Redis("redis", 6379)

    def init_psql(self):
        time.sleep(5)  # need to wait until db starts inside a container
        self.psql_conn = psycopg2.connect(dbname='tasks', user='test_user', host='psgrsql')
        self.cursor = self.psql_conn.cursor()
        self.create_tables()

    async def closing(self, app):
        self.cursor.close()
        self.psql_conn.close()

    def create_tables(self):

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
            """
            CREATE TABLE IF NOT EXISTS completed_tasks (
                    id INTEGER PRIMARY KEY,
                    create_time TIMESTAMP,
                    start_time TIMESTAMP,
                    exec_time INTEGER
            )
            """
        )

        for query in queries:
            self.cursor.execute(query)

        self.psql_conn.commit()

    def launch_queue(self, execute, callback):
        for _ in range(self.redis_conn.llen("queue")):
            self.executor.submit(execute).add_done_callback(callback)

    def push_task_id(self, value):
        self.redis_conn.rpush("queue", value)

    def get_task_id(self):
        return int(self.redis_conn.lpop("queue"))

    def check_cache(self, key):
        cache = self.redis_conn.get(key)
        return json.loads(cache.decode()) if cache else cache

    def caching(self, key, value):
        self.redis_conn.setex(key, 60*60*24, json.dumps(value))

    def psql_insert(self, table, fields, values, return_required=False):
        value_placeholders = ", ".join(["%s" for _ in values])
        _fields = ", ".join([it for it in fields])
        return_ = "RETURNING id" if return_required else ""
        sql_string = "INSERT INTO {0} ({1}) VALUES ({2}) {3}".format(table, _fields, value_placeholders, return_)
        self.cursor.execute(sql_string, tuple(values))
        self.psql_conn.commit()
        return self.cursor.fetchone()[0] if return_required else None

    def psql_select(self, searching_param):
        self.cursor.execute("SELECT * FROM task_tracking WHERE id = %s", (searching_param,))
        self.psql_conn.commit()
        result = self.cursor.fetchone()
        if result:
            result = [it.strftime("%Y-%m-%d %H:%M:%S") if isinstance(it, datetime) else it for it in result]
        return result

    def psql_update(self, fields, values, searching_param):
        set_query = ", ".join(["{0} = '{1}'".format(f, v) for f, v in zip(fields, values)])
        sql_string = "UPDATE task_tracking SET {0} WHERE id = {1}".format(set_query, "%s")
        self.cursor.execute(sql_string, (searching_param, ))
        self.psql_conn.commit()
