
import boto3
import re
from open_data_export.pgdb import DB
from open_data_export.config import settings
from datetime import datetime, timedelta
import pandas as pd
import random
import csv

s3 = boto3.client("s3")
bucket = 'openaq-logs'
# Number of files per datetime to pull down, limit = 1000
maxkeys = 5
# The min and max dates to search from
datetime_min = datetime.fromisoformat('2017-08-10 23:00')
datetime_max = datetime.now()
csv_file_path = 'sample.csv'
# log_directory = None
log_directory = 'logs/'

# Pattern to use to extract data from log
pattern = "\[(?P<date>.+)\] (?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) (?P<user>[\w:/\-\.@=]+) \w+ (?P<method>[\w\.]+) (?P<key>[\w\-/\.%]+) \"[\w\/\.\- &%=?]+\" [\w\- ]+ \"-\" \"(?P<agent>.+)\""


#######################
# SAMPLE RANDOM DATES #
#######################
# set of random dates to sample with
dates = []
for i in range(300):
    mn = int(datetime_min.strftime('%s'))
    mx = int(datetime_max.strftime('%s'))
    dt = datetime.fromtimestamp(
        mn + (mx - mn) * random.random()
    )
    dates.append(dt)


############################
# SAMPLE RANDOM FROM APRIL #
############################
# set of random dates to sample with
dates = []
datetime_min = datetime.fromisoformat('2022-04-01')
datetime_max = datetime.fromisoformat('2022-05-01')
for i in range(30):
    mn = int(datetime_min.strftime('%s'))
    mx = int(datetime_max.strftime('%s'))
    dt = datetime.fromtimestamp(
        mn + (mx - mn) * random.random()
    )
    dates.append(dt)

# OR
######################
# SAMPLE DATE SERIES #
######################
# Set of sequential dates to sample
# if you want to specify a certain hour to sample you
# can do that in the datetime_min
datetime_min = datetime.fromisoformat('2017-09-01 06:00')
# https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
# sample end of every other month
# dates = pd.date_range(datetime_min, end=datetime_max, freq="2M")
# sample middle of every month
# dates = pd.date_range(datetime_min, end=datetime_max, freq="SM")
# sample middle of every 6 months
# dates = pd.date_range(datetime_min, end=datetime_max, freq="12SM")
# Daily starting this past month
# dates = pd.date_range('2022-05-01 12:00', end=datetime_max, freq="D")

# Loop through all the ros
rows = []
with open(csv_file_path, 'w', newline='') as f:
    w = csv.DictWriter(
        f,
        fieldnames=['date', 'ip', 'user', 'method', 'key', 'agent', 'log'],
        quoting=csv.QUOTE_NONNUMERIC
    )

    w.writeheader()
    for date in dates:
        prefix = date.strftime("fetches%Y-%m-%d-%H")
        print(f"DATE: {prefix}")
        page = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=maxkeys,
        )

        if 'Contents' in page.keys():
            print(f"Found: {len(page['Contents'])} files")
            for obj in page['Contents']:
                key = obj['Key']
                log = s3.get_object(
                    Bucket=bucket,
                    Key=key,
                )
                content = (log['Body']).read().decode('utf-8')
                # Write the log to a file
                if log_directory is not None:
                    with open(f"{log_directory}{key}.txt", 'w') as lg:
                        lg.write(content)

                entries = content.split('\n')
                for entry in entries:
                    match = re.search(pattern, entry)
                    if match:
                        # print(f"IP: {match['ip']}")
                        row = match.groupdict()
                        row['log'] = key
                        rows.append(row)
                        w.writerow(row)
                    elif entry != '':
                        print(f"ENTRY: `{entry}`")

df = pd.DataFrame(rows)
print("USERS")
print(df['user'].value_counts())
print("IPS")
print(df['ip'].value_counts())
print("AGENTS")
print(df['agent'].value_counts())
print("METHODS")
print(df['method'].value_counts())
