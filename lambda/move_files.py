# from open_data_export.db import DB
# from open_data_export.config import settings
import logging
from open_data_export.config import settings
from open_data_export.main import move_objects_handler, get_database
import argparse
from datetime import datetime
import boto3
import json
from time import sleep

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s',
    level=settings.LOG_LEVEL.upper(),
    force=True,
)

parser = argparse.ArgumentParser(
    description="""
    Simple script to facilitate copying files
    """)
parser.add_argument('--day', type=str, required=False,
                    help='The day to export, in YYYY-MM-DD')
parser.add_argument('--node', type=int, required=False,
                    help='The node id to export')
parser.add_argument(
    '--limit',
    type=int,
    required=False,
    default=1,
    help="""
    How many to limit the move to,
    more for when you are not passing
    a day or node
    """
)
parser.add_argument(
    '--repeat',
    type=int,
    required=False,
    default=1,
    help="""
    How many times do you want to repeat the process
    """
)

parser.add_argument(
    '--delay',
    type=int,
    required=False,
    default=600,
    help="""
    How long should we sleep between repeats
    """
)

args = parser.parse_args()


client = boto3.client("lambda")

db = get_database()

sql = """
	  SELECT day::text
	, COUNT(1) as n
	FROM open_data_export_logs l
	WHERE COALESCE(key, l.metadata->>'Key') IS NOT NULL
	AND COALESCE(key, l.metadata->>'Key') ~* '/country'
	AND l.exported_on IS NOT NULL
	AND l.records > 0
	AND l.metadata->>'error' IS NULL
	GROUP BY day
	ORDER BY COUNT(1) DESC
	LIMIT :limit;
	  """

for i in range(args.repeat):
	# get new files to move
	rows, ms = db.rows(sql, limit = args.limit)
	files = 0
	for row in rows:

		day = row[0]
		files += row[1]

		logger.info(f'Queueing {day}/{row[1]}')
		params = {"method": "move", "args": {"day": day, "limit": files + 1000} }
		res = client.invoke(
			FunctionName='arn:aws:lambda:us-east-1:470049585876:function:openaq-move-production-openaqmoveproductionmoveobj-jOynf0aliMGq',
			InvocationType="Event",
			Payload=json.dumps(params),
			)

	logger.info(f"Queued a total of {files} files")
	if i < (args.repeat-1):
		logger.info(f"Sleeping for {args.delay} seconds - done {i+1} of {args.repeat}")
		sleep(args.delay)
