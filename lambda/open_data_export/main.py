import logging
import os

from open_data_export.pgdb import DB
from open_data_export.config import settings

# import asyncio
import time
# import pyarrow
# from pyarrow import csv
# import pyarrow.parquet as pq
from pandas import DataFrame
from datetime import datetime
from io import StringIO, BytesIO
from typing import Union
import boto3
import json

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level=settings.LOG_LEVEL.upper(),
    force=True,
)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

s3 = boto3.client("s3")
db = None


def get_database():
    global db
    if db is None:
        db = DB()
    return db


def ping(event, context):
    """
    Test environmental variables and database connection
    """
    ctime = "failed"
    total = "failed"
    exported = "failed"
    db = get_database()
    try:
        ctime = db.value('SELECT now()::text as now')
        total, exported = db.row('SELECT COUNT(1) as total, SUM((exported_on IS NOT NULL)::int) as exported FROM open_data_export_logs')
        return f"{exported} of {total} rows as of {ctime}"
    except Exception as e:
        logger.warning(f"something failed. {e}")
    finally:
        logger.info(f"""
        HOST: {settings.DATABASE_HOST}, EVENT: {event}, LOCATION: {settings.WRITE_FILE_LOCATION} TIME: {ctime} TOTAL: {total}, EXPORTED: {exported}
        """)


def reset_queue():
    """
    Initialize or reset the export log/queue. Update the queue
    """
    sql = """
    SELECT * FROM reset_export_logs();
    """
    db = get_database()
    return db.rows(sql, response_format='DataFrame')


def object_exists(Bucket: str, Key: str):
    """Check to see if a given key exists in a bucket"""
    exists = False
    try:
        s3.head_object(Bucket=Bucket, Key=Key)
        exists = True
    except Exception:
        pass
    logger.debug(f'{exists} - {Bucket}/{Key}')
    return exists


def move_object(from_location, to_location):
    try:

        s3.copy_object(
            Bucket=to_location['Bucket'],
            Key=to_location['Key'],
            CopySource={
                'Bucket': from_location['Bucket'],
                'Key': from_location['Key'],
            },
        )

        s3.delete_object(
           Bucket=from_location['Bucket'],
           Key=from_location['Key'],
        )

        logger.debug(f"Moved: {to_location['Bucket']}/{to_location['Key']}")
        return True

    except s3.exceptions.NoSuchKey:
        new_exists = object_exists(
            Bucket=to_location['Bucket'],
            Key=to_location['Key'],
        )
        if not new_exists:
            raise


def move_objects_handler(event, context=None):
    start = time.time()
    db = get_database()
    limit = event['limit']
    # determine the extension type
    if settings.WRITE_FILE_FORMAT == 'csv':
        ext = 'csv'
    elif settings.WRITE_FILE_FORMAT == 'csv.gz':
        ext = 'csv.gz'
    elif settings.WRITE_FILE_FORMAT == 'parquet':
        ext = 'parquet'
    elif settings.WRITE_FILE_FORMAT == 'json':
        raise Exception("We are not supporting JSON yet")
    else:
        raise Exception(f"We are not supporting {settings.WRITE_FILE_FORMAT}")

    days = db.rows(
        f"""
        WITH days AS (
        SELECT
          l.day
        , l.sensor_nodes_id
        , lower(COALESCE(sn.country, 'no-country')) as country
        , p.export_prefix
        , l.open_data_export_logs_id
        , FORMAT('records/{ext}/provider=%%s/country=%%s/locationid=%%s/year=%%s/month=%%s/location-%%s-%%s.{ext}'
          , p.export_prefix
          , lower(COALESCE(sn.country, 'no-country'))
          , l.sensor_nodes_id
          , to_char(l.day, 'YYYY')
          , to_char(l.day, 'MM')
          , l.sensor_nodes_id
          , to_char(l.day, 'YYYYMMDD')
        ) as key
        FROM open_data_export_logs l
        JOIN sensor_nodes sn ON (l.sensor_nodes_id = sn.sensor_nodes_id)
        JOIN providers p ON (sn.source_name = p.source_name)
        WHERE exported_on IS NOT NULL
        AND l.metadata->>'bucket' IS NULL
        LIMIT {limit})
        UPDATE open_data_export_logs
        SET metadata = jsonb_build_object(
           'Bucket', '{settings.OPEN_DATA_BUCKET}'
           , 'Key', key
        )
        FROM days
        WHERE days.open_data_export_logs_id = open_data_export_logs.open_data_export_logs_id
        RETURNING days.*;
        """)

    for row in days:

        node = row[1]
        country = row[2]
        day = row[0]
        key = row[5]

        yr = day.strftime('%Y')
        mn = day.strftime('%m')
        dy = day.strftime('%d')

        old_key = f"""records/{settings.WRITE_FILE_FORMAT}/country={country}/locationid={node}/year={yr}/month={mn}/location-{node}-{yr}{mn}{dy}.{ext}"""

        try:
            move_object(
                from_location={
                    "Bucket": settings.OPEN_DATA_BUCKET,
                    "Key": old_key,
                },
                to_location={
                    "Bucket": settings.OPEN_DATA_BUCKET,
                    "Key": key,
                }
            )
        except Exception as e:
            logger.warning(f"{e}")
            submit_error(day, node, f"{e}")

    sec = time.time() - start
    logger.info(f'Moved {limit} files in {sec} seconds')


def update_export_log(
        day: str,
        node: int,
        n: int,
        sec: int,
        bucket: str,
        key: str
):
    """
    Mark the location/day as exported
    """
    if isinstance(day, str):
        day = datetime.fromisoformat(day).date()
    sql = """
    UPDATE public.open_data_export_logs
    SET exported_on = now()
    , records = :n
    , metadata = jsonb_build_object(
      'Bucket', (:bucket)::text
    , 'Key', (:key)::text
    , 'sec', (:sec)::numeric
    )
    WHERE day = :day
    AND sensor_nodes_id = :node
    RETURNING open_data_export_logs_id
    """
    db = get_database()
    return db.rows(sql, day=day, node=node,
                   n=n, sec=sec, bucket=bucket, key=key)


def submit_error(day: str, node: int, error: str):
    """
    Mark the location/day with an error message
    """
    if isinstance(day, str):
        day = datetime.fromisoformat(day).date()
    sql = """
    UPDATE open_data_export_logs
    SET metadata = :error
    WHERE day = :day AND sensor_nodes_id = :node
    RETURNING open_data_export_logs_id
    """
    error = {"error": True, "message": error}
    db = get_database()
    return db.rows(sql, day=day, node=node, error=json.dumps(error))


def get_all_location_days():
    """
    get the entire set of location/days
    """
    sql = f"""
    SELECT sn.sensor_nodes_id
    , (m.datetime-'1sec'::interval)::date as day
    , COUNT(m.value) as n
    FROM measurements m
    JOIN sensors s ON (m.sensors_id = s.sensors_id)
    JOIN measurands p ON (s.measurands_id = p.measurands_id)
    JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
    JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
    LEFT JOIN versions v ON (s.sensors_id = v.sensors_id)
    WHERE s.sensors_id NOT IN (SELECT sensors_id FROM stale_versions)
    GROUP BY sn.sensor_nodes_id, (m.datetime-'1sec'::interval)::date
    LIMIT {settings.LIMIT}
    """
    db = get_database()
    return db.rows(sql, {})


def get_pending_location_days():
    """
    get the set of location/days that need to be updated
    """
    sql = f"""
    SELECT * FROM get_pending({settings.LIMIT})
    """
    db = get_database()
    return db.rows(sql)


def get_measurement_data(
        sensor_nodes_id: int,
        day: Union[str, datetime.date],
):
    """
    Pull all measurement data for one site and day.
    Data is organized by sensor_node and the sensor_systems_id
    and units is appended to the measurand to ensure that
    there will be no duplicate columns when we convert to long format
    """
    if isinstance(day, str):
        day = datetime.fromisoformat(day).date()

    where = {
        'day1': day,
        'day2': day,
        'sensor_nodes_id': f"{sensor_nodes_id}",
    }

    # AND (m.datetime - '1sec'::interval)::date = :day
    # , p.measurand||'-'||ss.sensor_systems_id||'-'||p.units as measurand
    sql = """
    SELECT sensor_nodes_id as location_id
    , sensors_id
    , measurands_id
    , location
    , country
    , ismobile
    , sensor
    , datetime_str as datetime
    , measurand
    , units
    , value
    , lon
    , lat
    , provider
    FROM measurement_data_export
    WHERE sensor_nodes_id = :sensor_nodes_id
    AND datetime > timezone(tz, (:day1)::timestamp)
    AND datetime <= timezone(tz, :day2 + '1day'::interval)
    """
    db = get_database()
    logger.debug(
        f'Getting measurement data for {sensor_nodes_id} for {day}'
    )
    rows = db.rows(sql, **where, response_format='DataFrame')
    return rows


def reshape(rows: Union[DataFrame, dict], fields: list = []):
    """
    Create a wide format dataframe from either records or a json/dict object
    from the database
    """
    if len(rows) > 0:
        rows = rows[fields]
    return rows


def write_file(tbl, filepath: str = 'example'):
    """
    write the results in the given format
    """
    if settings.WRITE_FILE_FORMAT == 'csv':
        logger.debug('writing file to csv format')
        out = StringIO()
        ext = 'csv'
        mode = 'w'
        tbl.to_csv(out, index=False)
    elif settings.WRITE_FILE_FORMAT == 'csv.gz':
        logger.debug('writing file to csv.gz format')
        out = BytesIO()
        ext = 'csv.gz'
        mode = 'wb'
        tbl.to_csv(out, index=False, compression="gzip")
    elif settings.WRITE_FILE_FORMAT == 'parquet':
        logger.debug('writing file to parquet format')
        out = BytesIO()
        ext = 'parquet'
        mode = 'wb'
        tbl.to_parquet(out, index=False)
    elif settings.WRITE_FILE_FORMAT == 'json':
        raise Exception("We are not supporting JSON yet")
    else:
        raise Exception(f"We are not supporting {settings.WRITE_FILE_FORMAT}")

    if (
        settings.WRITE_FILE_LOCATION == 's3'
        and settings.OPEN_DATA_BUCKET is not None
        and settings.OPEN_DATA_BUCKET != ''
    ):
        logger.debug(
            f"write_file: {settings.OPEN_DATA_BUCKET}/{filepath}.{ext}"
        )
        s3.put_object(
            Bucket=settings.OPEN_DATA_BUCKET,
            Key=f"{filepath}.{ext}",
            ACL='public-read',
            Body=out.getvalue()
         )
    elif settings.WRITE_FILE_LOCATION == 'local':
        filepath = os.path.join(settings.LOCAL_SAVE_DIRECTORY, filepath)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        logger.debug(f"writing file to local file in {filepath}")
        txt = open(f"{filepath}.{ext}", mode)
        txt.write(out.getvalue())
        txt.close()
    else:
        raise Exception(
            f"{settings.WRITE_FILE_LOCATION} is not a valid location"
        )


def export_data(day, node):
    start = time.time()
    rows = get_measurement_data(
        sensor_nodes_id=node,
        day=day,
    )
    if len(rows) > 0:
        country = rows['country'][0]
        provider = rows['provider'][0]
        df = reshape(
            rows,
            fields=[
                "location_id",
                "sensors_id",
                "location",
                "datetime",
                "lat",
                "lon",
                "measurand",
                "units",
                "value"
            ]
        )
        yr = day.strftime('%Y')
        mn = day.strftime('%m')
        dy = day.strftime('%d')
        bucket = settings.OPEN_DATA_BUCKET
        filepath = f"""records/{settings.WRITE_FILE_FORMAT}/provider={provider}/country={country}/locationid={node}/year={yr}/month={mn}/location-{node}-{yr}{mn}{dy}"""
        write_file(df, filepath)
        sec = time.time() - start
        update_export_log(day, node, len(rows), sec, bucket, filepath)
        logger.info(
            "export_data: location: %s, day: %s; %s rows; %0.4f seconds",
            node, f"{yr}-{mn}-{dy}", len(rows), sec
        )
    else:
        error = "query returned 0 records"
        logger.info(
            "export_data: location: %s, day: %s; error: %s",
            node, f"{yr}-{mn}-{dy}", error
        )
        submit_error(day, node, error)


def export_pending(event={}, context={}):
    """
    Only export the location/days that are marked for export. Location days
    will be limited to the value in the LIMIT environmental parameter
    """
    if 'source' not in event.keys():
        event['source'] = 'not set'

    if 'method' in event.keys():
        if event['method'] == 'ping':
            return ping(event, context)

    start = time.time()
    days = get_pending_location_days()
    for d in days:
        try:
            export_data(d[1], d[0])
        except Exception as e:
            logger.warning(f"Error processing {d[0]}-{d[1]}: {e}")

    logger.info(
        "export_pending: %s; seconds: %0.4f; source: %s",
        len(days),
        time.time() - start,
        event['source'],
    )
    return len(days)


def export_all():
    """
    Export all location/days in the database. This will reset the
    export log and then run the `export_pending` method
    """
    reset_queue()
    return export_pending()


if __name__ == '__main__':
    # rsp = asyncio.run(export_all())
    # rsp = asyncio.run(reset_queue())
    # rsp = asyncio.run(get_pending_location_days())
    # rsp = asyncio.run(export_pending())
    # rsp = asyncio.run(update_export_log('2021-08-08', 1))
    # print(rsp)
    print(f"total query time: {db.query_time}")
