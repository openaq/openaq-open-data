import boto3
import time

from config import settings

athena = boto3.client("athena")

DATABASE = 'openaq'
CATALOG = 'AwsDataCatalog'
DATA_BUCKET = settings.OPEN_DATA_BUCKET
RESULTS_BUCKET = settings.OPEN_DATA_BUCKET
OUTPUT_LOCATION = f"s3://{RESULTS_BUCKET}/query-results/"

# from https://danbernstein.netlify.app/post/2021-01-08-enhancing-aws-athena-with-python/
def wait(query_execution_id: str, timeout: int = 20):
    state = "WAITING"
    seconds = 0
    while (state not in ['FAILED','SUCCEEDED'] and seconds < timeout):
        response = athena.get_query_execution(
            QueryExecutionId = query_execution_id
        )
        state = response['QueryExecution']['Status']['State']
        if state == 'FAILED':
            return False
        elif state == 'SUCCEEDED':
            filepath = response['QueryExecution']['ResultConfiguration']['OutputLocation']
            print(filepath)
            return filepath
        seconds += 1
        print(f"Waiting for {query_execution_id}:{state} to finish: {seconds} secs")
        time.sleep(1)
    return False

def execute_query(query: str, timeout: int = 20) -> dict:
    #print(f"Writing {query} to {OUTPUT_LOCATION}")
    q = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE,
            'Catalog':  CATALOG,
        },
        ResultConfiguration={
            "OutputLocation": OUTPUT_LOCATION,
        }
    )
    filename = wait(q['QueryExecutionId'], timeout)
    print(f"Finished query, {q['QueryExecutionId']} results are in {filename}")
    return filename


def create_realtime_table():
    rst = execute_query(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `openaq_realtime`(
  `date` struct<utc:string,local:string> COMMENT 'from deserializer',
  `parameter` string COMMENT 'from deserializer',
  `location` string COMMENT 'from deserializer',
  `value` float COMMENT 'from deserializer',
  `unit` string COMMENT 'from deserializer',
  `city` string COMMENT 'from deserializer',
  `attribution` array<struct<name:string,url:string>> COMMENT 'from deserializer',
  `averagingperiod` struct<unit:string,value:float> COMMENT 'from deserializer',
  `coordinates` struct<latitude:float,longitude:float> COMMENT 'from deserializer',
  `country` string COMMENT 'from deserializer',
  `sourcename` string COMMENT 'from deserializer',
  `sourcetype` string COMMENT 'from deserializer',
  `mobile` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://openaq-fetches/realtime'
TBLPROPERTIES (
  'transient_lastDdlTime'='1504040724')
""")


def create_realtime_compressed_table():
    rst = execute_query(f"""
CREATE EXTERNAL TABLE `openaq_realtime_compressed`(
  `date` struct<utc:string,local:string> COMMENT 'from deserializer',
  `parameter` string COMMENT 'from deserializer',
  `location` string COMMENT 'from deserializer',
  `value` float COMMENT 'from deserializer',
  `unit` string COMMENT 'from deserializer',
  `city` string COMMENT 'from deserializer',
  `attribution` array<struct<name:string,url:string>> COMMENT 'from deserializer',
  `averagingperiod` struct<unit:string,value:float> COMMENT 'from deserializer',
  `coordinates` struct<latitude:float,longitude:float> COMMENT 'from deserializer',
  `country` string COMMENT 'from deserializer',
  `sourcename` string COMMENT 'from deserializer',
  `sourcetype` string COMMENT 'from deserializer',
  `mobile` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://openaq-fetches/realtime-gzipped'
TBLPROPERTIES (
  'transient_lastDdlTime'='1519395796')
""")

def create_lcs_table():
    rst = execute_query(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `lcs_measurements`(
    `location` string,
    `value` string,
    `epoch` string
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    LOCATION 's3://openaq-fetches/lcs-etl-pipeline/measures/clarity'
""")


def create_openaq_data_table():
    rst = execute_query(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `openaq_data`(
  `location` string COMMENT 'from deserializer',
  `city` string COMMENT 'from deserializer',
  `country` string COMMENT 'from deserializer',
  `utc` string COMMENT 'from deserializer',
  `local` string COMMENT 'from deserializer',
  `parameter` string COMMENT 'from deserializer',
  `value` string COMMENT 'from deserializer',
  `unit` string COMMENT 'from deserializer',
  `latitude` string COMMENT 'from deserializer',
  `longitude` string COMMENT 'from deserializer',
  `attribution` string COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'escapeChar'='\\',
  'quoteChar'='\"',
  'separatorChar'=',')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://openaq-data/'
TBLPROPERTIES (
  'transient_lastDdlTime'='1491597347')
""")


def create_database():
    rst = execute_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    rst = execute_query(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.measurements (
    `location` string,
    `datetime` string,
    `lat` double,
    `lon` double,
    `measurand` string,
    `value` double
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    LOCATION 's3://{DATA_BUCKET}/csv.gz/'
    TBLPROPERTIES (
    'skip.header.line.count' = '1'
    )
    """)
    #rst = execute_query(f"MSCK REPAIR TABLE {DATABASE}.measurements")

def create_tables():
    execute_query(f"DROP TABLE IF EXISTS {DATABASE}.measurements")
    execute_query(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.measurements (
    `station` string,
    `datetime` string,
    `lat` double,
    `lon` double,
    `measurand` string,
    `value` double
    ) PARTITIONED BY (country string, location int, year int, month int)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ","
    LINES TERMINATED BY "\n"
    LOCATION 's3://{DATA_BUCKET}/csv.gz/'
    TBLPROPERTIES (
    'skip.header.line.count' = '1'
    )
    """)
    execute_query(f"MSCK REPAIR TABLE {DATABASE}.measurements")

def drop_database():
    rst = execute_query(f"DROP TABLE IF EXISTS {DATABASE}.measurements")
    rst = execute_query(f"DROP TABLE IF EXISTS {DATABASE}.lcs_measurements")
    rst = execute_query(f"DROP TABLE IF EXISTS {DATABASE}.openaq_data")
    rst = execute_query(f"DROP TABLE IF EXISTS {DATABASE}.openaq_realtime")
    rst = execute_query(f"DROP TABLE IF EXISTS {DATABASE}.openaq_realtime_compressed")
    rst = execute_query(f"DROP TABLE IF EXISTS {DATABASE}.fetches_realtime")
    rst = execute_query(f"DROP DATABASE IF EXISTS {DATABASE}")
    return rst


#drop_database()
#create_database()
#create_tables()
#create_realtime_table()
#create_realtime_compressed_table()
#create_lcs_table()
#create_openaq_data_table()

#execute_query(f"SELECT COUNT(*) FROM {DATABASE}.openaq_realtime_compressed LIMIT 10")
#execute_query(f"SELECT COUNT(*) FROM {DATABASE}.openaq_realtime LIMIT 10")

#execute_query(f"SELECT COUNT(1) FROM {DATABASE}.openaq_data LIMIT 10", 2)
#execute_query(f"SELECT location, MIN(value) as min_value, MAX(value) as max_value, COUNT(value) as count_value FROM {DATABASE}.openaq_data GROUP BY location", 2)

execute_query(f"""
SELECT * FROM openaq_data LIMIT 100
""")


execute_query(f"""
SELECT station
, measurand
, MIN(value) as min_value
, MAX(value) as max_value
, AVG(value) as avg_value
, COUNT(1) as count_value
FROM {DATABASE}.measurements
WHERE month = 11
GROUP BY station, measurand
""")
