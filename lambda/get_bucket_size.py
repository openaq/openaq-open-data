
import boto3
from open_data_export.pgdb import DB
from open_data_export.config import settings


s3 = boto3.client("s3")

bucket = settings.OPEN_DATA_BUCKET
prefix = 'records/csv.gz/provider='

res = s3.head_bucket(Bucket=bucket)

paginator = s3.get_paginator('list_objects')
pages = paginator.paginate(
    Bucket=bucket,
    Prefix=prefix,
)

n = 0
size = 0
i = 0

page = s3.list_objects(
    Bucket=bucket,
    Prefix=prefix,
    MaxKeys=1000,
    )

pages = [page]

for page in pages:
    i += 1
    sz = 0
    if 'Contents' in page.keys():
        for obj in page['Contents']:
            n += 1
            sz += obj['Size']

    size += sz
    # if i % 5 == 0:
    print(f"page {i}: {sz/1024} kb and {n} objects for {size/1024/1024} mb (average: {(size/1024)/n} kb")
    if i > 450:
        break

print(f"{n} finished: objects for {size/1024/1024} mb")
