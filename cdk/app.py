#!/usr/bin/env python3
import pathlib

# import docker
import aws_cdk
from aws_cdk import (
	Environment,
    Tags,
)
from os import environ

from settings import settings

# Stacks
from stacks import (
    ExporterStack,
)

code_dir = pathlib.Path(__file__).parent.absolute()
env = Environment(account=environ["CDK_DEFAULT_ACCOUNT"], region=environ["CDK_DEFAULT_REGION"])

app = aws_cdk.App()


move = ExporterStack(
    app,
    f"openaq-exporter-{settings.ENV}",
    package_directory=code_dir,
    env_name=settings.ENV,
	lambda_role_arn=settings.OPEN_DATA_ROLE_ARN,
    ingest_lambda_timeout=settings.INGEST_LAMBDA_TIMEOUT,
    ingest_lambda_memory_size=512,
	vpc_id=settings.VPC_ID,
    env_variables=settings.ENV_VARIABLES,
	env=env,
)

Tags.of(move).add("project", settings.PROJECT)
Tags.of(move).add("product", "export")
Tags.of(move).add("env", settings.ENV)

app.synth()
