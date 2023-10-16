# from open_data_export.db import DB
# from open_data_export.config import settings
from open_data_export.config import settings
from open_data_export.main import handler
import argparse
from datetime import date
import boto3
import os
import gzip


s3 = boto3.client("s3")


parser = argparse.ArgumentParser(
    description="""
    Simple script to facilitate checking files
    """)

parser.add_argument(
    '--key',
    type=str,
    required=False,
    help='File to check'
)

parser.add_argument(
    '--prefix',
    type=str,
    required=False,
    help='Pattern to search for in the bucket'
)

parser.add_argument(
    '--acl',
    type=str,
    required=False,
    help='Update acl'
)

parser.add_argument(
    '--day',
    type=date.fromisoformat,
    required=False,
    help='Day to use for exporting'
)

args = parser.parse_args()


# acl = s3.get_bucket_acl(
#     Bucket=settings.OPEN_DATA_BUCKET,
# )
# for grant in acl['Grants']:
#     print(f"{grant['Grantee']['Type']}: {grant['Permission']}")

if args.day:
    event = {"method": "dump", "args": {"day": args.day}}
    handler(event, {})

if args.prefix:
    print(settings.OPEN_DATA_BUCKET)
    print(args.prefix)
    page = s3.list_objects_v2(
        Bucket=settings.OPEN_DATA_BUCKET,
        Prefix=args.prefix,
        MaxKeys=1000,
    )
    for obj in page['Contents']:
        key = obj['Key']
        print(f"************ Checking ACL ({key}) ***********")
        print(f"Last modified: {obj['LastModified']}")
        acl = s3.get_object_acl(
            Bucket=settings.OPEN_DATA_BUCKET,
            Key=key
        )
        for grant in acl['Grants']:
            print(f"{grant['Grantee']['Type']}: {grant['Permission']}")

        if args.acl:
            print("**************** Updating ACL *************")

            res = s3.put_object_acl(
                Bucket=settings.OPEN_DATA_BUCKET,
                Key=key,
                ACL='public-read'
            )
            print(res)

if args.key:
    try:
        print("************ Checking for key ***********")
        meta = s3.head_object(
            Bucket=settings.OPEN_DATA_BUCKET,
            Key=args.key
        )
        print(meta)
    except Exception as err:
        print(f"{err.response['Error']['Code']}: {err.response['Error']['Message']}")

    print("************ Checking ACL ***********")
    try:
        acl = s3.get_object_acl(
            Bucket=settings.OPEN_DATA_BUCKET,
            Key=args.key
        )
        for grant in acl['Grants']:
            print(f"{grant['Grantee']['Type']}: {grant['Permission']}")

    except Exception as err:
        print(f"Could not get ACL: {err.response['Error']['Message']}")

    print("************ Downloading ***********")
    try:
        obj = s3.get_object(
            Bucket=settings.OPEN_DATA_BUCKET,
            Key=args.key
        )
        body = obj['Body']
        if args.key.endswith(".gz"):
            text = gzip.decompress(body.read()).decode('utf-8')
        else:
            text = body

        fpath = os.path.expanduser(f'~/Downloads/{args.key}')
        fpath = fpath.replace('.gz', '')
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        with open(fpath, 'w') as f:
            f.write(text)
        print(f"Downloaded to: {fpath}")

    except Exception as err:
        print(f"Could not download: {err.response['Error']['Message']}")

    if args.acl:
        print("**************** Updating ACL *************")

        obj = s3.put_object_acl(
            Bucket=settings.OPEN_DATA_BUCKET,
            Key=args.key,
            ACL='public-read'
        )
        print(obj)
