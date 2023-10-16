
import boto3
from open_data_export.pgdb import DB
from open_data_export.config import settings
from datetime import datetime
import re
import pandas as pd

s3 = boto3.client("s3")

bucket = settings.DB_BACKUP_BUCKET
prefix = 'measurements/measurements_'

n = 0
i = 0
limit = 50
files = []
max_keys = 1000

page = s3.list_objects_v2(
    Bucket=bucket,
    Prefix=prefix,
    MaxKeys=max_keys,
)

while page is not None and i < limit:
    i += 1

    if 'Contents' in page.keys():
        print(f"Number of keys: {len(page['Contents'])}")
        for obj in page['Contents']:
            key = obj['Key']
            match = re.search("/measurements_v1_(\d{8})00_(\d{8})00.csv", key)
            if match:
                day = match.group(1)
                dt = datetime.strptime(day, '%Y%m%d').strftime('%Y-%m-%d')
                added = 1
                # get the file datetime
                files.append({
                    "day": dt,
                    "added": obj["LastModified"],
                    "path": f"s3://{bucket}/{key}",
                    "size": obj['Size']/1024
                })
                n += 1

    if 'NextContinuationToken' in page.keys():
        NextContinuationToken = page['NextContinuationToken']
        print(f"Checking for page {i}")
        page = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=max_keys,
            ContinuationToken=NextContinuationToken
        )
    else:
        page = None


print(f"{n} files in {i} pages")
pd.DataFrame(files).to_csv('exported_files_list.csv')
