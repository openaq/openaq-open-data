import asyncpg
import time
import logging

from config import settings
from buildpg import render
from pandas import DataFrame
import orjson

logger = logging.getLogger(__name__)

class DB:
    pg_pool = None
    response_format = 'Record' ## json, dataframe
    query_time = 0

    def __init__(self, response_format: str = 'Record'):
        self.response_format = response_format

    async def pool(self):
        if self.pg_pool is None:
            self.pg_pool = await asyncpg.create_pool(
                settings.DATABASE_READ_URL,
                command_timeout=14,
                max_inactive_connection_lifetime=15,
                min_size=1,
                max_size=10,
            )
        return self.pg_pool


    async def rows(self, query, kwargs):
        start = time.time()
        pool = await self.pool()
        rquery, args = render(query, **kwargs)
        logger.debug(f"Running query: {rquery}, {args}")
        async with pool.acquire() as con:
            try:
                stm = await con.prepare(rquery)
                fields = [a.name for a in stm.get_attributes()]
                data = await stm.fetch(*args)
            except Exception as e:
                logger.warning(f"{e}");
                raise ValueError(f"{e}")

        if self.response_format == 'dataframe':
            data = DataFrame(data, columns=fields)

        dur = time.time() - start
        self.query_time += dur
        logger.info(
            "seconds: %0.4f, results: %s",
            dur,
            len(data),
        )
        return data

    async def row(self, query, kwargs):
        start = time.time()
        pool = await self.pool()
        rquery, args = render(query, **kwargs)
        logger.debug(f"Running query: {rquery}, {args}")
        async with pool.acquire() as con:
            try:
                stm = await con.prepare(rquery)
                fields = [a.name for a in stm.get_attributes()]
                data = await stm.fetchrow(*args)
            except Exception as e:
                logger.warning(f"{e}");
                raise ValueError(f"{e}")

        if self.response_format == 'dataframe':
            data = DataFrame(data, columns=fields)

        dur = time.time() - start
        self.query_time += dur
        logger.info(
            "seconds: %0.4f, results: %s",
            dur,
            len(data),
        )
        return data


    async def value(self, query, kwargs):
        start = time.time()
        pool = await self.pool()
        rquery, args = render(query, **kwargs)
        logger.debug(f"Running query: {rquery}, {args}")
        async with pool.acquire() as con:
            try:
                stm = await con.prepare(rquery)
                data = await stm.fetchval(*args)
            except Exception as e:
                logger.warning(f"{e}");
                raise ValueError(f"{e}")

        if self.response_format == 'json':
            data = orjson.loads(data)

        dur = time.time() - start
        self.query_time += dur
        logger.info(
            "seconds: %0.4f, results: %s",
            dur,
            len(data),
        )
        return data
