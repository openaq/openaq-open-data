import logging
import os
from db import DB
import asyncio
import time
import pyarrow
from pyarrow import csv
import pyarrow.parquet as pq
from pandas import DataFrame
from datetime import datetime
from config import settings
from io import StringIO, BytesIO
import boto3

logging.basicConfig(
    format = '[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level = settings.LOG_LEVEL,
)

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")


def get_station_days():
    """get the entire set of station/days."""

    sql = """
    SELECT sn.sensor_nodes_id
    , (m.datetime-'1sec'::interval)::date::text as day
    , COUNT(m.value) as n
    FROM measurements m
    JOIN sensors s ON (m.sensors_id = s.sensors_id)
    JOIN measurands p ON (s.measurands_id = p.measurands_id)
    JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
    JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
    LEFT JOIN versions v ON (s.sensors_id = v.sensors_id)
    WHERE s.sensors_id NOT IN (SELECT sensors_id FROM stale_versions)
    GROUP BY sn.sensor_nodes_id, (m.datetime-'1sec'::interval)::date
    LIMIT 2
    """
    db = DB()
    return db.rows(sql, {});

def get_modified_station_days():
    """get the set of station/days that need to be updated."""
    sql = """
    -- Query needed
    -- UPDATE open_data_audit_log
    -- SET queued_on = now()
    -- WHERE modified_on > exported_on
    -- RETURNING sensor_nodes_id, date, modified_on
    """
    db = DB()
    return db.rows(sql, {});


## def get_station_data():

def get_measurement_data(
        sensor_nodes_id: int,
        day: str,
):
    """Pull all measurement data for one site and day"""
    where = {
        'sensor_nodes_id': sensor_nodes_id,
        'day': datetime.fromisoformat(day),
    }

    #AND (m.datetime - '1sec'::interval)::date = :day
    sql = """
    SELECT sn.site_name
    , s.source_id as sensor
    , m.datetime::text
    , p.measurand||'-'||p.units as measurand
    , p.units
    , m.value
    FROM measurements m
    JOIN sensors s ON (m.sensors_id = s.sensors_id)
    JOIN measurands p ON (s.measurands_id = p.measurands_id)
    JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
    JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
    LEFT JOIN versions v ON (s.sensors_id = v.sensors_id)
    WHERE sn.sensor_nodes_id = :sensor_nodes_id
    AND (m.datetime - '1sec'::interval)::date = :day
    AND s.sensors_id NOT IN (SELECT sensors_id FROM stale_versions)
    """
    # sql2 = """
    # WITH raw AS (
    # SELECT sn.site_name
    # , s.source_id as sensor
    # , m.datetime::text
    # , p.measurand||'-'||p.units as measurand
    # , p.units
    # , m.value
    # FROM measurements m
    # JOIN sensors s ON (m.sensors_id = s.sensors_id)
    # JOIN measurands p ON (s.measurands_id = p.measurands_id)
    # JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
    # JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
    # LEFT JOIN versions v ON (s.sensors_id = v.sensors_id)
    # WHERE sn.sensor_nodes_id = :sensor_nodes_id
    # AND (m.datetime - '1sec'::interval)::date = :day
    # AND s.sensors_id NOT IN (SELECT sensors_id FROM stale_versions)
    # ), measurands AS (
    # SELECT json_agg(DISTINCT measurand) as measurands
    # FROM raw
    # ), aggregated AS (
    # SELECT site_name
    # , datetime
    # , json_object_agg(measurand, value) as values
    # FROM raw
    # GROUP BY site_name, datetime
    # ), aggregated2 AS (
    # SELECT json_object_agg(datetime, values) as data
    # FROM aggregated)
    # SELECT json_build_object('data', a.data, 'measurands', m.measurands)::text
    # FROM aggregated2 a, measurands m;
    # """
    # db = DB('json')
    # rows = db.value(sql2, where);
    db = DB()
    rows = db.rows(sql, where);
    return rows;


# this is handy but does not leave the data in a format that is easy write to a file
# def pivot(df: DataFrame):
#     try:
#         return df.pivot(values='value', index='datetime', columns='measurand'),
#     except Exception as e:
#         logger.error(f"{e}")
#         ## error is most likely associated with the versioning
#         logger.debug(pivot_table(df, values='value', aggfunc=len, index='datetime', columns='measurand'))

def pivot(obj: dict):
    """Create a wide format dataframe from either records or a json/dict object from the database"""
    ## first get data organized by datetime
    ## and a list of all the measurands
    ## method depends on format of the data
    if isinstance(obj, dict):
        logger.debug('data provided in dict format')
        data = obj['data']
        measurands = obj['measurands']
        if data is None:
            logger.warning('No data provided')
            return DataFrame()
    else:
        logger.debug('data provided in long/record format')
        data = {}
        measurands = []
        for row in obj:
            #print(row)
            station = row['site_name']
            datetime = row['datetime']
            measurand = row['measurand']
            value = row['value']
            if datetime not in data.keys():
                data[datetime] = { 'station': station }
            if measurand not in data[datetime].keys():
                data[datetime][measurand] = value
            if measurand not in measurands:
                measurands.append(measurand)
    # Now loop through the dates and the measurands to build the dataframe
    df = { 'datetime': [], 'station': [] };
    for datetime in data.keys():
        row = data[datetime]
        df['datetime'].append(datetime)
        df['station'].append(row['station'])
        for measurand in measurands:
            if measurand not in df.keys():
                df[measurand] = []
            df[measurand].append(row[measurand])
    #df = DataFrame(df, index=df['datetime'])
    ## https://arrow.apache.org/docs/python/parquet.html
    ## suggests dropping the index unless its really needed
    df = DataFrame(df)
    return df



def convert(df):
    """Convert the dataframe to another format for writing"""
    ## https://arrow.apache.org/docs/python/parquet.html
    ## suggests dropping the index unless its really needed
    tbl = pyarrow.Table.from_pandas(df, preserve_index=False)
    return tbl

def write_file(tbl, filepath: str = 'example'):
    """write the results in the given format"""
    #if not isinstance(tbl, pyarrow.lib.Table):
    #    tbl = convert(tbl)
    if settings.WRITE_FILE_FORMAT == 'csv':
        logger.debug('writing file to csv format')
        out = StringIO()
        ext = 'csv'
        mode = 'w'
        tbl.to_csv(out)
    else:
        logger.debug('writing file to parquet format')
        out = BytesIO()
        ext = 'parquet'
        mode = 'wb'
        tbl.to_parquet(out, index=False)
        #pq.write_table(tbl, )

    if settings.OPEN_DATA_BUCKET is not None and settings.OPEN_DATA_BUCKET != '':
        logger.debug(f"writing file to bucket: {settings.OPEN_DATA_BUCKET}")
        s3.put_object(
            Bucket=settings.OPEN_DATA_BUCKET,
            Key=f"{filepath}.{ext}",
            Body=out.getvalue()
        )
    else:
        logger.debug(f"writing file to local file")
        txt = open(f"{filepath}.{ext}", mode)
        txt.write(out.getvalue())
        txt.close()


start = time.time()
days = asyncio.run(get_station_days())

for d in days:
    # get the data
    day = d['day']
    node = d['sensor_nodes_id']
    try:
        rows = asyncio.run(get_measurement_data(
            sensor_nodes_id = node,
            day = day,
        ))
        # transform the data
        df = pivot(rows)
        ## tbl = convert(df)
        # write the data
        write_file(df, f"data/sn-{node}-{day}")
    except Exception as e:
        logger.warning(f"Error processing {node}-{day}: {e}");



logger.warning(
    "seconds: %0.4f",
    time.time() - start,
)


#print(data)
#json = orjson.loads(rows)
# print(rows)
#df = pivot(rows)
# df = DataFrame({'one': [-1, numpy.nan, 2.5],
#                 'two': ['foo', 'bar', 'baz'],
#                 'three': [True, False, True]},
#                index=list('abc'))


#print(type(df))
#print(df[0])
#write_file(df)
