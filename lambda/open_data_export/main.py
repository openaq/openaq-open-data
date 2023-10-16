import logging
import os
import csv
import gzip

from open_data_export.pgdb import DB
from open_data_export.config import settings
from smart_open import open

# import asyncio
import time
# import pyarrow
# from pyarrow import csv
# import pyarrow.parquet as pq
from pandas import DataFrame
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from typing import Union
import boto3

logger = logging.getLogger('main')

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level=settings.LOG_LEVEL.upper(),
    force=True,
)

logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

db = None
# Iterate the version number when when a change is made
# version number must be an integer
FILE_FORMAT_VERSION = 1


def get_database():
    global db
    if db is None:
        db = DB()
    return db


def put_metric(
        namespace,
        metricname,
        value,
        units: str = None,
        attributes: dict = None,
):
    try:
        dimensions = [
            {
                'Name': 'Environment',
                'Value': 'openaq',
            },
        ]
        if attributes is not None:
            for key in attributes.keys():
                dimensions.append({
                    'Name': key,
                    'Value': str(attributes[key]),
                })

        cloudwatch.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    'MetricName': metricname,
                    'Dimensions': dimensions,
                    'Unit': units,
                    'Value': value,
                    'StorageResolution': 1,
                },
            ],
        )

    except Exception as e:
        logger.warn(f'Could not submit custom metric: {namespace}/{metricname}: {e}')


def ping(event, context):
    """
    Test environmental variables and database connection
    """
    ctime = "failed"
    total = "failed"
    exported = "failed"
    logger.info('Pinging database')
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


def dump_measurements(
        day: Union[str, datetime.date]
):
    """
    Export the 24h (utc) data to the export bucket
    This file has a different format than the open data format
    and does not include any metadata other than station id
    """

    sql = """
    SELECT sensors_id
    , datetime
    , value
    , lat
    , lon
    , added_on
    FROM measurements
    WHERE datetime > :day::date
    AND datetime <= :nextday::date
    LIMIT 30000000
    """

    if isinstance(day, str):
        day = datetime.fromisoformat(day).date()

    bucket = "openaq-db-backups"
    folder = "testing"
    ext = "csv.gz"
    nextday = day + timedelta(days=1)
    formatted_start = day.strftime("%Y%m%d%H%M%S")
    formatted_end = nextday.strftime("%Y%m%d%H%M%S")
    version = "v0"
    filepath = f"{folder}/measurements_{version}_{formatted_start}_{formatted_end}"

    logger.debug(f"Writing to {bucket}/{filepath}")
    db = get_database()
    start = time.time()

    data = db.stream(
        sql,
        day=day,
        nextday=nextday,
        chunk_size=1000,
    )
    n = 0

    params = {"buffer_size": 5*1024 ** 2}
    with open(f"s3://{bucket}/{filepath}.{ext}", "wb", params) as fout:
        include_header = True
        for chunk in data:
            n += len(chunk)
            csv = chunk.to_csv(header=include_header)
            include_header = False
            fout.write(csv.encode('UTF-8'))

        # out = BytesIO()
        # row.to_csv(csv_buffer, index=False, mode="w", encoding="UTF-8")
        # row.to_csv(out, index=False, compression="gzip")
        # logger.debug(csv_buffer.read())

    logger.info(
        "dump_measurements (query): day: %s; %s rows; %0.4f seconds",
        day, n, time.time() - start
    )

    #download_file(bucket, f"{filepath}.{ext}")

    return filepath


def dump_metadata():
    """
    Export the public metadata to the export bucket
    """
    print('here')


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
        if (to_location['Bucket'] != from_location['Bucket'] or to_location['Key'] != from_location['Key']):
            s3.copy_object(
                Bucket=to_location['Bucket'],
                Key=to_location['Key'],
                ACL='public-read',
                CopySource={
                    'Bucket': from_location['Bucket'],
                    'Key': from_location['Key'],
                },
            )
            logger.debug(f"Copied: {to_location['Bucket']}/{to_location['Key']}")
            s3.delete_object(
               Bucket=from_location['Bucket'],
               Key=from_location['Key'],
            )
            logger.debug(f"Deleted: {from_location['Bucket']}/{from_location['Key']}")
        else:
            # if its the same lets just update the ACL policy
            s3.put_object_acl(
                Bucket=to_location['Bucket'],
                Key=to_location['Key'],
                ACL='public-read',
            )

        return True

    except s3.exceptions.NoSuchKey:
        new_exists = object_exists(
            Bucket=to_location['Bucket'],
            Key=to_location['Key'],
        )
        if not new_exists:
            raise
    except Exception as err:
        # its possible that we are trying to move a file to itself, possibly
        # to update its metadata. If it doesnt this will
        # protect from that error
        if 'without changing' in str(err):
            logger.debug(f"{str(err)}")
            return True
        raise


def move_objects_handler(event, context=None):
    start = time.time()
    db = get_database()
    limit = 5000
    where = "exported_on IS NOT NULL"
    # AND (l.metadata->>'move' IS NOT NULL AND (l.metadata->>'move')::boolean = true)
    args = {}

    if 'limit' in event.keys() and event['limit'] is not None:
        limit = event['limit']

    if 'day' in event.keys() and event['day'] is not None:
        args['day'] = datetime.fromisoformat(event['day']).date()
        where += " AND l.day = :day"

    if 'node' in event.keys() and event['node'] is not None:
        args['node'] = event['node']
        where += " AND l.sensor_nodes_id = :node"

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

    # where = " AND l.metadata->>'Bucket' IS NOT NULL"

    days = db.rows(
        f"""
        WITH days AS (
        -----------
        -- get a set of files to move
        -----------
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
        , l.metadata->>'Key' as current_key
        , l.metadata->>'error' as current_error
        , l.metadata->>'message' as current_error_message
        FROM open_data_export_logs l
        JOIN sensor_nodes sn ON (l.sensor_nodes_id = sn.sensor_nodes_id)
        JOIN providers p ON (sn.source_name = p.source_name)
        WHERE {where}
        LIMIT {limit})
        -----------
        -- Update those records and return data
        -----------
        UPDATE open_data_export_logs
        SET metadata = COALESCE(metadata::jsonb, '{{}}'::jsonb)
        ||jsonb_build_object(
           'Bucket', '{settings.OPEN_DATA_BUCKET}'
          ,'Key', key
          ,'move', false
          ,'error', false
          ,'message', 'no error'
        )
        FROM days
        WHERE days.open_data_export_logs_id =
              open_data_export_logs.open_data_export_logs_id
        -----------
        -- return the pre-update data
        -----------
        RETURNING days.*;
        """, **args)

    successes = 0
    for row in days:

        node = row[1]
        country = row[2]
        day = row[0]
        key = row[5]
        old_key = row[6]

        yr = day.strftime('%Y')
        mn = day.strftime('%m')
        dy = day.strftime('%d')

        if row[7] is not None:
            logger.debug(f"Previous error: {row[8]}")

        # if old_key is None:
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
            successes += 1
        except Exception as e:
            submit_error(day, node, f"{e}")

    sec = time.time() - start
    logger.info(f'Moved {successes} files (of {len(days)}) in {sec} seconds')


def update_hourly_data(ts):
    """
    Mark the location/day as exported
    """
    if isinstance(ts, str):
        day = datetime.fromisoformat(ts)
    sql = """
	SELECT update_hourly_data(:datetime::timestamptz)
    """
    db = get_database()
    n, time_ms = db.value(
        sql,
        datetime=ts
    )
    logger.info(f"Updated hourly data: {ts} ({n}) in {time_ms} ms")
    return n, time_ms


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
    , 'version', :version
    )
    WHERE day = :day
    AND sensor_nodes_id = :node
    RETURNING TRUE
    """
    db = get_database()
    return db.rows(
        sql,
        day=day,
        node=node,
        n=n,
        sec=sec,
        bucket=bucket,
        key=key,
        version=FILE_FORMAT_VERSION
    )


def submit_error(day: str, node: int, error: str):
    """
    Mark the location/day with an error message
    """
    if isinstance(day, str):
        day = datetime.fromisoformat(day).date()
    sql = """
    UPDATE open_data_export_logs
    SET metadata = (COALESCE(metadata::jsonb, '{}'::jsonb)||jsonb_build_object(
      'error', true
    , 'message', (:error)::text
    , 'at', current_timestamp::text
    ))::json
    WHERE day = :day AND sensor_nodes_id = :node
    RETURNING TRUE
    """
    logger.error(f"error: {node} on {day} - {error}")
    db = get_database()
    return db.rows(sql, day=day, node=node, error=error)


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
    logger.debug(f'get_pending_days: {settings.LIMIT}')
    sql = f"""
    SELECT * FROM get_pending({settings.LIMIT})
    """
    db = get_database()
    return db.rows(sql)


def get_outdated_location_days():
    """
    get the set of location/days that are old and need to be updated
    """
    sql = f"""
    SELECT * FROM outdated_location_days({FILE_FORMAT_VERSION}, {settings.LIMIT})
    """
    db = get_database()
    return db.rows(sql)


def get_measurement_data_n(
        sensor_nodes_id: int,
        day: Union[str, datetime.date],
):
    """
    Pull all measurement data for one site and day.
    Data is organized by sensor_node and the sensor_systems_id
    and units is appended to the measurand to ensure that
    there will be no duplicate columns when we convert to long format
    """
    # db = get_database()
    if isinstance(day, str):
        day = datetime.fromisoformat(day).date()

    # Start by getting the sensor node data
    db = get_database()

    sql = f"""
    WITH sensors AS (
      SELECT sensors_id
      , s.source_id as sensor
      , sn.site_name||'-'||sy.sensor_systems_id as location
      , s.measurands_id
      , p.measurand
      , p.units
      , sn.sensor_nodes_id
      , st_x(geom) as lon
      , st_y(geom) as lat
      , pr.export_prefix as provider
      , sn.ismobile
      , CASE WHEN sn.ismobile
        THEN 'mobile'
        ELSE COALESCE(LOWER(sn.country), 'no-country')
        END as country
		  , z.tzid as tz
      FROM sensors s
      JOIN measurands p ON (s.measurands_id = p.measurands_id)
      JOIN sensor_systems sy ON (s.sensor_systems_id = sy.sensor_systems_id)
      JOIN sensor_nodes sn ON (sy.sensor_nodes_id = sn.sensor_nodes_id)
      JOIN providers pr ON (sn.source_name = pr.source_name)
	    JOIN timezones z ON (sn.timezones_id = z.gid)
      WHERE sn.sensor_nodes_id = :sensor_nodes_id)
    SELECT s.sensor_nodes_id as location_id
    , s.location
    , s.sensors_id
    , s.measurands_id
    , format_timestamp(m.datetime, tz) as datetime
    , s.measurand as parameter
    , s.units
    , m.value
    , s.provider
    , s.country
    , CASE WHEN s.ismobile
      THEN m.lon
      ELSE s.lon
      END as lon
    , CASE WHEN s.ismobile
      THEN m.lat
      ELSE s.lat
      END as lat
      FROM public.measurements m
    JOIN sensors s ON (m.sensors_id = s.sensors_id)
    AND datetime > timezone(tz, (:day1)::timestamp)
    AND datetime <= timezone(tz, :day2 + '1day'::interval)
    """

    logger.debug(
        f'Getting measurement data for {sensor_nodes_id} for {day}'
    )

    rows, time_ms = db.rows(
        sql,
        day1=day,
        day2=day,
        sensor_nodes_id=sensor_nodes_id,
        response_format='DataFrame'
    )

    return rows


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
    , measurand as parameter
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
    rows, time_ms = db.rows(sql, **where, response_format='DataFrame')
    logger.info(
        "get_measurement_data: node: %s, day: %s, seconds: %0.4f, results: %s",
        sensor_nodes_id,
        day,
		time_ms,
        len(rows)
    )
    return rows


def reshape(rows: Union[DataFrame, dict], fields: list = []):
    """
    Create a wide format dataframe from either records or a json/dict object
    from the database
    """
    if len(rows) > 0:
        rows = rows[fields]
    return rows


def write_file(
        tbl,
        filepath: str = 'example',
        bucket: str = settings.OPEN_DATA_BUCKET,
        ext: str = settings.WRITE_FILE_FORMAT,
        location: str = settings.WRITE_FILE_LOCATION,
        public: bool = True
):
    """
    write the results in the given format
    """
    if ext == 'csv':
        out = StringIO()
        mode = 'w'
        tbl.to_csv(out, index=False, quoting=csv.QUOTE_NONNUMERIC)
    elif ext == 'csv.gz':
        out = BytesIO()
        mode = 'wb'
        tbl.to_csv(out, index=False, compression="gzip")
    elif ext == 'parquet':
        out = BytesIO()
        mode = 'wb'
        tbl.to_parquet(out, index=False)
    elif ext == 'json':
        raise Exception("We are not supporting JSON yet")
    else:
        raise Exception(f"We are not supporting {ext}")

    if (
        location == 's3'
        and bucket is not None
        and bucket != ''
    ):
        logger.debug(
            f"writing file to: {bucket}/{filepath}.{ext}"
        )
        s3.put_object(
            Bucket=bucket,
            Key=f"{filepath}.{ext}",
            ACL='public-read' if public else 'private',
            Body=out.getvalue()
        )
    elif location == 'local':
        filepath = os.path.join(settings.LOCAL_SAVE_DIRECTORY, filepath)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        logger.debug(f"writing file to local file in {filepath}.{ext}")
        txt = open(f"{filepath}.{ext}", mode)
        txt.write(out.getvalue())
        txt.close()
    else:
        raise Exception(
            f"{settings.WRITE_FILE_LOCATION} is not a valid location"
        )


def export_data(day, node):
    yr = day.strftime('%Y')
    mn = day.strftime('%m')
    dy = day.strftime('%d')

    try:
        start = time.time()
		# using the statement version and not the view
        rows = get_measurement_data_n(
            sensor_nodes_id=node,
            day=day,
        )

        get_ms = time.time() - start

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
                    "parameter",
                    "units",
                    "value"
                ]
            )
            bucket = settings.OPEN_DATA_BUCKET
            filepath = f"records/{settings.WRITE_FILE_FORMAT}/locationid={node}/year={yr}/month={mn}/location-{node}-{yr}{mn}{dy}"
            write_file(df, filepath)
        else:
            filepath = None
            bucket = None

        sec = time.time() - start
        start = time.time()
        res, update_ms = update_export_log(day, node, len(rows), sec, bucket, filepath)

        logger.info(
            "export_data: location: %s, day: %s; %s rows; %0.2fs, %0.2fs, %0.2fs",
            node, f"{yr}-{mn}-{dy}", len(rows), get_ms, sec, update_ms
        )
        return len(rows), round(sec*1000)
    except Exception as e:
        submit_error(day, node, str(e))


def export_pending():
    """
    Only export the location/days that are marked for export. Location days
    will be limited to the value in the LIMIT environmental parameter
    """
    start = time.time()
    days, time_ms = get_pending_location_days()
    for d in days:
        try:
            logger.debug(f"{d[1]}/{d[0]}")
            export_data(day=d[1], node=d[0])
        except Exception as e:
            logger.warning(f"Error processing {d[0]}-{d[1]}: {e}")

    logger.info(
        "export_pending: %s; seconds: %0.4f;",
        len(days),
        time.time() - start,
    )

    return len(days)


def update_outdated_handler(event=None, context=None):
    """
    Run the updater repeatedly until there is not enough time left
    """
    time_spent = 0
    if context is not None:
        time_available = context.get_remaining_time_in_millis()/1000
    else:
        time_available = 80  # basically do it once

    time_left = time_available
    days = 0
    start = time.time()
    last_days = settings.LIMIT

    # Keep updating until we are out of time or days
    while (
            time_spent <= time_left
            and last_days == settings.LIMIT
           ):
        logger.info(
            "update_outdated_handler: time spent: %0.2f, time left: %0.2f",
            time_spent,
            time_left,
        )
        method_start = time.time()
        last_days = update_outdated(event, context)
        days += last_days
        time_spent = time.time() - method_start
        time_left = time_available - (time.time() - start)

    logger.info(
        "update_outdated_handler: days: %s, seconds: %0.2f, time spent: %0.2f, time left: %0.2f",
        days,
        time.time() - start,
        time_spent,
        time_left,
    )

    return days


def update_outdated(event={}, context={}):
    """
    Only export the location/days that are marked for export. Location days
    will be limited to the value in the LIMIT environmental parameter
    """
    if 'source' not in event.keys():
        event['source'] = 'not set'

    start = time.time()

    days = get_outdated_location_days()
    logger.info(
        "get_outdated: %s rows; seconds: %0.4f; source: %s",
        len(days),
        time.time() - start,
        event['source'],
    )

    for d in days:
        try:
            export_data(d[1], d[0])
        except Exception as e:
            logger.warning(f"Error processing {d[0]}-{d[1]}: {e}")

    logger.info(
        "update_outdated: %s; seconds: %0.4f; source: %s",
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


def download_file(bucket: str, key: str):
    obj = s3.get_object(
        Bucket=bucket,
        Key=key
    )
    body = obj['Body']
    if key.endswith(".gzd"):
        text = gzip.decompress(body.read()).decode('utf-8')
    else:
        text = body

    fpath = os.path.expanduser(f'~/Downloads/{bucket}/{key}')
    fpath = fpath.replace('.gzd', '')
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    with open(fpath, 'w') as f:
        f.write(text)
    return obj


def handler(event={}, context={}):
    """
    Only export the location/days that are marked for export. Location days
    will be limited to the value in the LIMIT environmental parameter
    """
    if 'source' not in event.keys():
        event['source'] = 'not set'

    if 'method' in event.keys():
        if event['method'] == 'ping':
            return ping(event, context)
        elif event['method'] == 'dump':
            return dump_measurements(**event['args'])
    else:
        return export_pending()


def test():
    days = [
        ['2022-08-10', 233590],
        ['2022-08-10', 233591],
        ['2022-08-10', 233592],
        ['2021-02-08', 66470],
    ]
    if (len(days) > 0):
        for d in days:
            day = d[0]
            node = d[1]
            get_measurement_data(node, day)
