
import boto3
import re
from datetime import datetime
from open_data_export.config import settings
from open_data_export.main import move_objects_handler


s3 = boto3.client("s3")

bucket = settings.OPEN_DATA_BUCKET
prefix = 'records/csv.gz/country='

res = s3.head_bucket(Bucket=bucket)

paginator = s3.get_paginator('list_objects')
pages = paginator.paginate(
    Bucket=bucket,
    Prefix=prefix,
    MaxKeys=100,
)

n = 0
size = 0
i = 0

#page = s3.list_objects(
#    Bucket=bucket,
#    Prefix=prefix,
#    MaxKeys=100,
#    )
#pages = [page]

for page in pages:
    i += 1
    sz = 0
    if 'Contents' in page.keys():
        for obj in page['Contents']:
            key = obj['Key']
            match = re.search("/location-(\d+)-(\d{8})", key)
            if match:
                node = match.group(1)
                day = match.group(2)
                print(f"------------------\nMoving: {key}")
                move_objects_handler({
                    "node": node,
                    "day": datetime.strptime(day, '%Y%m%d').strftime('%Y-%m-%d'),
                })
            n += 1
            sz += obj['Size']

    size += sz
    # if i % 5 == 0:
    print(f"page {i}: {sz/1024} kb and {n} objects for {size/1024/1024} mb (average: {(size/1024)/n} kb")
    if i > 450:
        break

print(f"{n} finished: moved objects for {size/1024/1024} mb")
