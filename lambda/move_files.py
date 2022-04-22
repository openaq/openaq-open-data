# from open_data_export.db import DB
# from open_data_export.config import settings
import logging
from open_data_export.config import settings
from open_data_export.main import move_objects_handler
import argparse
from datetime import datetime

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
    default=5,
    help="""
    How many to limit the move to,
    more for when you are not passing
    a day or node
    """
)

args = parser.parse_args()

print(vars(args))

move_objects_handler(vars(args))
