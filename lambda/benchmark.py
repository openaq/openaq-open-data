import logging
import os
import sys
import argparse
from time import time
import re
from datetime import datetime


logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="""
Test benchmarks for exports and rollups
    """)

parser.add_argument(
	'--name',
	type=str,
	required=False,
	default="test",
	help='Name to use for the test'
	)
parser.add_argument(
	'--env',
	type=str,
	default='.env',
	required=False,
	help='The dot env file to use'
	)
parser.add_argument(
	'--debug',
	action="store_true",
	help='Output at DEBUG level'
	)
args = parser.parse_args()

if 'DOTENV' not in os.environ.keys() and args.env is not None:
    logger.info(args)
    os.environ['DOTENV'] = args.env

if args.debug:
    os.environ['LOG_LEVEL'] = 'DEBUG'

from open_data_export.config import settings
from open_data_export.main import export_data, update_hourly_data

exports = [
	{"node":61936, "day":"2023-07-15"},
	{"node":61941, "day":"2023-07-15"},
	{"node":61948, "day":"2023-07-15"},
	{"node":61949, "day":"2023-07-15"},
	{"node":61950, "day":"2023-07-15"},
	{"node":61952, "day":"2023-07-15"},
	{"node":61964, "day":"2023-07-15"},
	{"node":61965, "day":"2023-07-15"},
	{"node":61975, "day":"2023-07-15"},
	{"node":61982, "day":"2023-07-15"},
	]
rollups= [
	{"datetime":"2023-07-15 01:00:00"},
	{"datetime":"2023-06-15 02:00:00"},
	{"datetime":"2023-05-15 03:00:00"},
	{"datetime":"2023-04-15 04:00:00"},
	{"datetime":"2023-03-15 05:00:00"},
	{"datetime":"2023-02-15 06:00:00"},
	{"datetime":"2023-01-15 07:00:00"},
	{"datetime":"2022-12-15 08:00:00"},
	{"datetime":"2022-11-15 09:00:00"},
	{"datetime":"2022-10-15 10:00:00"},
	]

f = open(f"benchmark_export_output_{args.name}.csv", "w")
f.writelines("name,action,time_ms,count\n")

for export in exports:
	node = export["node"]
	day = datetime.fromisoformat(export["day"]).date()
	# start = time()
	n, time_ms = export_data(day, node)
	# time_ms = round((time() - start)*1000)
	f.writelines(f"'{node}-{day}','export-day',{time_ms},{n}\n")

for rollup in rollups:
	datetime = rollup["datetime"]
	n, time_ms = update_hourly_data(datetime)
	f.writelines(f"'{datetime}','update-hourly-data',{time_ms},{n}\n")


f.close()
