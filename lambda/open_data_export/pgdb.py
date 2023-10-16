import psycopg
import time
import logging

from open_data_export.config import settings
from buildpg import render
from pandas import DataFrame
from psycopg.rows import dict_row
import orjson
import re
from io import BytesIO


logger = logging.getLogger('db')


class DB:
    response_format = 'Record'
    query_time = 0

    def __init__(self, response_format: str = 'Record'):
        self.response_format = response_format

    def get_connection(self, write: bool = True, **kwargs):
        if write:
            cstring = settings.DATABASE_WRITE_URL
        else:
            cstring = settings.DATABASE_READ_URL
        conn = psycopg.connect(cstring, **kwargs)
        return conn

    def __query(
            self,
            query: str,
            params: dict,
            method: str = 'rows',
    ):
        start = time.time()
        rquery, args = render(query, **params)
        # psycopg3 needs the placeholders to be either
        # %s or %(name)s
        # and since render will rearrange the arguments
        # we can get away with this
        rquery = re.sub(r'\$[0-9]+', '%s', rquery)
        logger.debug(f"Running query: {rquery}, {args}")
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(rquery, args)
                    logger.debug(f"executed query")
                    if method == 'row':
                        data = cur.fetchone()
                    elif method == 'value':
                        data = cur.fetchone()
                        data = data[0]
                    else:
                        data = cur.fetchall()
                    fields = [desc[0] for desc in cur.description]
                    n = cur.rowcount
                    dur = time.time() - start
                    self.query_time += dur
                    logger.debug("query: seconds: %0.4f, results: %s", dur, n)
                    return data, fields, n, round(dur*1000)
                except Exception as e:
                    logger.warning(f"Query error: {e}")
                    raise ValueError(f"{e}") from None
                finally:
                    conn.commit()

    def rows(
            self,
            query: str,
            response_format: str = 'default',
            **kwargs
    ):
        data, fields, n, time_ms = self.__query(
            query,
            params=kwargs, method='rows'
        )
        if response_format == 'DataFrame' or self.response_format == 'DataFrame':
            data = DataFrame(data, columns=fields)
        return data, time_ms

    def row(
            self,
            query: str,
            response_format: str = 'default',
            **kwargs
    ):
        data, fields, n, time_ms = self.__query(
            query,
            params=kwargs, method='row'
        )
        if response_format == 'DataFrame' or self.response_format == 'DataFrame':
            print(fields)
            data = DataFrame([data], columns=fields)
        return data, time_ms

    def value(
            self,
            query: str,
            response_format: str = 'default',
            **kwargs
    ):
        data, fields, n, time_ms = self.__query(query, params = kwargs, method='value')
        if response_format == 'json' or self.response_format == 'json':
            data = orjson.loads(data)
        return data, time_ms

    def stream(
            self,
            query: str,
            chunk_size: int = 200,
            **kwargs
    ):
        rquery, args = render(query, **kwargs)
        # psycopg3 needs the placeholders to be either
        # %s or %(name)s
        # and since render will rearrange the arguments
        # we can get away with this
        rquery = re.sub(r'\$[0-9]+', '%s', rquery)
        logger.debug(f"Running query: {rquery}, {args}")
        with self.get_connection(row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(rquery, args)
                fields = [desc[0] for desc in cur.description]
                rows = True
                while rows:
                    rows = cur.fetchmany(chunk_size)
                    if rows:
                        df = DataFrame(rows, columns=fields)
                        yield df
