#!/usr/bin/env python3
from pathlib import Path
from dotenv.main import dotenv_values
import pathlib

import docker
import aws_cdk
from aws_cdk import (
    Tags,
)

# Stacks
from stacks import (
    ExportStack,
    MoveStack,
)


client = docker.from_env()
code_dir = pathlib.Path(__file__).parent.absolute()


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


app = aws_cdk.App()

# env variables come from .env.talloaks
talloaks = ExportStack(
    app,
    "talloaks-open-data",
    package_directory=code_dir,
    env_variables=dotenv_values(
        Path.joinpath(code_dir.parent.absolute(), ".env.talloaks")
    ),
)

cac = ExportStack(
    app,
    "cac-open-data",
    package_directory=code_dir,
    env_variables=dotenv_values(
        Path.joinpath(code_dir.parent.absolute(), ".env.cac")
    ),
)

openaq = ExportStack(
    app,
    "openaq-open-data",
    package_directory=code_dir,
    env_variables=dotenv_values(
        Path.joinpath(code_dir.parent.absolute(), ".env.openaq")
    ),
)

move = MoveStack(
    app,
    "openaq-move-objects",
    package_directory=code_dir,
    env_variables=dotenv_values(
        Path.joinpath(code_dir.parent.absolute(), ".env.openaq")
    ),
)
Tags.of(move).add("Project", 'openaq')


app.synth()
