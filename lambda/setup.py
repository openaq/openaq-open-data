from setuptools import setup

setup(
    name="OpenAQ-open-data",
    version="0.0.1",
    author="OpenAQ",
    author_email="info@openaq.org",
    packages=[
        "open_data_export",
    ],
    url="http://openaq.org/",
    license="LICENSE.txt",
    description="Utility to export files to open data for OpenAQ",
    long_description=open("README.md").read(),
    install_requires=[
        "pydantic[dotenv]",
        "psycopg[binary]",
        #"asyncpg",
        "buildpg",
        "pyarrow",
        "pandas",
        "orjson",
        #"boto3"
    ],
    extras_require={}
)
