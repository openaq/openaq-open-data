import argparse
from os import environ

parser = argparse.ArgumentParser(
    description="""
    Simple script to check the update_outdated method
    """)
parser.add_argument('--limit', type=int, nargs='?',
                    default=2,
                    help='The number to update')
args = parser.parse_args()

environ['LIMIT'] = str(args.limit)

from open_data_export.main import update_outdated

update_outdated({'source': 'testing-cp'})
