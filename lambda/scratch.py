# from open_data_export.db import DB
# from open_data_export.config import settings
# from .db import DB
from os import environ

# if its already set than dont update it
if (
        'DOTENV' not in environ
        and 'ENV' not in environ
):
    environ['DOTENV'] = '.env.talloaks'

if 'WRITE_FILE_LOCATION' not in environ:
    environ['WRITE_FILE_LOCATION'] = 's3'

# from open_data_export.config import settings
from open_data_export.main import (
    ping,
    reset_queue,
    export_pending,
    export_all,
    get_measurement_data,
    get_pending_location_days,
    test,
    )

# this will either reset or create the queue
# reset_queue()

# this will just check to see if things are working
# ping({},{})

# get a list of pending days
# days = get_pending_location_days()


# get the measurement data for the first one
# data = get_measurement_data(
test()

# export_pending({"method": "ping"}, {})
