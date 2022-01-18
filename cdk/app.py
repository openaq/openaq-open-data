#!/usr/bin/env python3
import os

# For consistency with TypeScript code, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
import aws_cdk
from aws_cdk import (
    aws_lambda as _lambda,
    aws_events as _events,
    aws_logs as _logs,
    aws_s3 as _s3,
    aws_events_targets as _targets,
    Stack,
    Environment,
    Duration,
)

from aws_cdk.aws_events_targets import EcsTask

import docker
from dotenv import load_dotenv
import pathlib
from pathlib import Path
from constructs import Construct

from dotenv.main import dotenv_values


client = docker.from_env()

code_dir = pathlib.Path(__file__).parent.absolute()

#load_dotenv(env_file)

print("Building image")
client.images.build(
    path="..",
    dockerfile="Dockerfile",
    tag="openaq-open-data",
    nocache=False,
)

print("Packaging image")
client.containers.run(
    image="openaq-open-data",
    command="/bin/sh -c 'cp /tmp/package.zip /local/package.zip'",
    remove=True,
    volumes={str(code_dir): {"bind": "/local/", "mode": "rw"}},
    user=0,
)

class OpenDataStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        environment: dict = {},
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        package = _lambda.Code.from_asset(
            str(pathlib.Path.joinpath(code_dir, "package.zip"))
        )

        exportPendingLambda = _lambda.Function(
            self,
            f"{id}-export-pending-lambda",
            runtime = _lambda.Runtime.PYTHON_3_8,
            code = package,
            handler = 'open_data_export.main.export_pending',
            environment = environment,
            memory_size = 1512,
            log_retention = _logs.RetentionDays.ONE_WEEK,
            timeout = Duration.seconds(900),
        )

        rule = _events.Rule(
            self,
            f"{id}-event-rule",
            schedule = _events.Schedule.cron(minute="0/5"),
            targets = [
                _targets.LambdaFunction(exportPendingLambda),
            ],
        )

        bucket = _s3.Bucket.from_bucket_name(
            self,
            "{id}-openaq-open-data-exports",
            environment['OPEN_DATA_BUCKET'],
        )

        bucket.grant_put(exportPendingLambda)



app = aws_cdk.App()

## env variables come from .env.talloaks
talloaks = OpenDataStack(
    app,
    "talloaks-open-data",
    environment = dotenv_values(Path.joinpath(code_dir.parent.absolute(), ".env.talloaks")),
)

cac = OpenDataStack(
    app,
    "cac-open-data",
    environment = dotenv_values(Path.joinpath(code_dir.parent.absolute(), ".env.cac")),
)

app.synth()
