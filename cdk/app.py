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
    ExportStack,
	MoveStack,
)

code_dir = pathlib.Path(__file__).parent.absolute()
env = Environment(account=environ["CDK_DEFAULT_ACCOUNT"], region=environ["CDK_DEFAULT_REGION"])

app = aws_cdk.App()

# export = ExportStack(
#     app,
#     f"openaq-export-{settings.ENV}",
#     package_directory=code_dir,
#     env_name=settings.ENV,
#     ingest_lambda_timeout=settings.INGEST_LAMBDA_TIMEOUT,
#     ingest_lambda_memory_size=settings.INGEST_LAMBDA_MEMORY_SIZE,
# 	vpc_id=settings.VPC_ID,
#     env_variables=settings.ENV_VARIABLES,
# 	env=env,
# )

# Tags.of(export).add("project", settings.PROJECT)
# Tags.of(export).add("product", "export")
# Tags.of(export).add("env", settings.ENV)

move = MoveStack(
    app,
    f"openaq-move-{settings.ENV}",
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
Tags.of(move).add("product", "move")
Tags.of(move).add("env", settings.ENV)

app.synth()
