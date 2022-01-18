import asyncpg
import time
import logging

#from config import settings
from open_data_export.config import settings
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
                settings.DATABASE_WRITE_URL,
                command_timeout=200,
                max_inactive_connection_lifetime=15,
                min_size=1,
                max_size=10,
            )
        return self.pg_pool


    async def __query(
            self,
            query: str,
            params: dict,
            method: str = 'rows',
    ):
        start = time.time()
        pool = await self.pool()
        rquery, args = render(query, **params)
        logger.debug(f"Running query: {rquery}, {args}")
        async with pool.acquire() as con:
            try:
                stm = await con.prepare(rquery)
                fields = [a.name for a in stm.get_attributes()]
                if method == 'row':
                    data = await stm.fetchrow(*args)
                elif method == 'value':
                    data = await stm.fetchval(*args)
                else:
                    data = await stm.fetch(*args)
                n = len(data)
                dur = time.time() - start
                self.query_time += dur
                logger.info("query rows: seconds: %0.4f, results: %s", dur, n)
                return data, fields, n
            except Exception as e:
                logger.warning(f"Query error: {e}");
                raise ValueError(f"{e}") from None
            finally:
                logger.warning("releasing connection")
                await pool.release(con)

    async def rows(
            self,
            query: str,
            response_format: str = 'default',
            **kwargs
    ):
        data, fields, n = await self.__query(query, params = kwargs, method='rows')
        if response_format == 'DataFrame' or self.response_format == 'DataFrame':
            data = DataFrame(data, columns=fields)
        return data

    async def row(
            self,
            query: str,
            response_format: str = 'default',
            **kwargs
    ):
        data, fields, n = await self.__query(query, params = kwargs, method='row')
        if response_format == 'DataFrame' or self.response_format == 'DataFrame':
            print(fields)
            data = DataFrame([data], columns=fields)
        return data

    async def value(
            self,
            query: str,
            response_format: str = 'default',
            **kwargs
    ):
        data, fields, n = await self.__query(query, params = kwargs, method='value')
        if response_format == 'json' or self.response_format == 'json':
            data = orjson.loads(data)
        return data
