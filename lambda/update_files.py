# from open_data_export.db import DB
# from open_data_export.config import settings
import logging
from open_data_export.config import settings
from open_data_export.main import handler, get_database
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
	'--local',
	action="store_true",
	help='Should we run locally?'
	)

parser.add_argument(
    '--method',
    type=str,
    required=False,
    default='check',
    help="""
    The method to run
    """
)

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

parser.add_argument(
    '--lambdas',
    type=int,
    required=False,
    default=1,
    help="""
    How many lambdas should we invoke?
    """
)

args = parser.parse_args()

if args.local or (args.day is not None and args.node is not None):
	## run locally
	params = {"method": args.method, "args": {}}
	if args.day is not None:
		params['args']['day'] = args.day;

	if args.node is not None:
		params['args']['node'] = args.node;

	if args.limit is not None:
		params['args']['limit'] = args.limit;

	handler(params)

elif args.method == 'export':
	client = boto3.client("lambda")
	params = {"method": args.method, "args": {"limit": args.limit } }
	for i in range(args.lambdas):
		logger.info(f'Invoking export function {params}')
		res = client.invoke(
			FunctionName=settings.LAMBDA_FUNCTION_ARN,
			InvocationType="Event",
			Payload=json.dumps(params),
			)


else:
	client = boto3.client("lambda")
	db = get_database()
	sql = """
		  SELECT day::text
		  , COUNT(1) as n
		  FROM open_data_export_logs l
		  WHERE l.exported_on IS NOT NULL
		  AND l.records > 0
		  AND l.key IS NOT NULL
		  AND (has_error IS NULL OR NOT has_error)
		  AND (checked_on IS NULL OR checked_on	< current_date - 1)
		  GROUP BY day
		  --HAVING COUNT(1) > 5000
		  --ORDER BY COUNT(1) ASC
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
			params = {"method": args.method, "args": {"day": day, "limit": files + 1000} }
			res = client.invoke(
				FunctionName=settings.LAMBDA_FUNCTION_ARN,
				InvocationType="Event",
				Payload=json.dumps(params),
				)

		logger.info(f"Queued a total of {files} files for {args.method}")

		if i < (args.repeat-1):
			logger.info(f"Sleeping for {args.delay} seconds - done {i+1} of {args.repeat}")
			sleep(args.delay)
