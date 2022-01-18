# OpenAQ Open Data Export Utility

A `scratch.py` file is provided to help with testing the code during development.

The top of the file sets the `.env` file to use for the testing. You can override these values by providing them in the command when calling the script. For example
```shell
# load the base .env file
OPEN_DATA_DOT_ENV=.env python3 lambda/scratch.py
# use the base file but export to s3 bucket
OPEN_DATA_DOT_ENV=.env WRITE_FILE_LOCATION=s3 python3 lambda/scratch.py

```
