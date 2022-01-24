# Overview

The exporter works by monitoring a table that keeps track of the last time a location/day was updated and the when it was exported.

# Process
1. When new measurements are ingested the `open_data_export_log` is updated to reflect the new data. The update method is part of the ingest process and therefor can be found in the `openaq-api-v2/openaq_fastapi/openaq_fastapi/ingest/lcs_meas_ingest.sql` file.

2. An AWS Lambda function is run at a given rate (e.g. 5min, 1hour) to export any pending location/days. The method uses the `get_pending(limit)` function to pull down any pending updates and mark them as queued. Marking them ensures that another process does not come along and pull them down as well. This process also only pulls down location/days with a complete day of data based on their timezone. This spreads out the export process and reduces the number of times a file is touched.

3. Once the file is exported its export date is updated in the export log. If new data is added to this file at anytime the process will update the `modiifed_on` date and mark the file for export.

# Tuning
Lambda functions are not meant to run for very long and will timeout after a set amount of time, with the max runtime being 15 min. If the lambda function times out before getting to all of the queued location/dates those location/dates will be marked as queued but never exported and need to be reset [^improvements]. Given this you want to make sure that the lambda does not pull down more than it can handle in a given period. To do this there are a few parameters you can adjust.
* LIMIT: The limit setting puts a hard limit on how many records are pulled down per process. Estimate how long the average process takes and let the limit as needed. For example, if it takes 1.25 sec to export one location/day you would not want to set the limit higher than 720. Make sure you give yourself some buffer as well.
* Schedule: Another way to increase the rate would be to schedule the function to be run more often. It will take about 20min for every 1000 stations given the 1.25 sec/location/day rate.
* Timeout: finally you could increase the timeout as needed

# Settings
```shell
# The log level to use, must be capitalized
LOG_LEVEL=INFO
# The number of location/days to pull down at once
LIMIT=500
# Where to export the files to, could be s3 or local
WRITE_FILE_LOCATION=s3
# The format to export the data as, could be csv, csv.gz or parquet
WRITE_FILE_FORMAT=csv
# The bucket to export to when using the s3 write method
OPEN_DATA_BUCKET=openaq-open-data-testing
# The directory to export to when using the local method
LOCAL_SAVE_DIRECTORY=data
# Database parameters
DATABASE_READ_USER=postgres
DATABASE_READ_PASSWORD=postgres
DATABASE_WRITE_USER=postgres
DATABASE_WRITE_PASSWORD=postgres
DATABASE_HOST=172.17.0.2
DATABASE_PORT=5432
DATABASE_DB=postgres
```

# Installing
You will need to install a few different parts to get this working.
1. Update your database to include the export module (see `tables/exports.sql` and `idempotent/exports_views.sql` in the `openaq-db` repository). There is a [patch](schema/schema.sql) file to help with this but you may need to adjust the paths.
2. Update the `openaq-api-v2` ingest process to include the new `lcs_meas_ingest.sql` file.
3. Install the function and event using the AWS CDK process (see [README](cdk/README.md))


[^improvements]: It might make sense to update the `get_pending` query to also pull down any abandoned queued location days, though this would only happen if the process times out.
