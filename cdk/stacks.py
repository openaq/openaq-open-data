from aws_cdk import (
    aws_lambda as _lambda,
    aws_events as _events,
    aws_logs as _logs,
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_events_targets as _targets,
    aws_ec2 as _ec2,
    Stack,
    Duration,
)
import pathlib

from constructs import Construct

from typing import Dict

from utils import (
    stringify_settings,
    create_dependencies_layer,
)


class ExporterStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        package_directory: str,
        env_name: str,
        ingest_lambda_timeout: int,
        ingest_lambda_memory_size: int,
        env_variables: dict = {},
        lambda_role_arn: str = None,
        vpc_id: str | None = None,
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        if vpc_id not in [None, 'null']:
            vpc = _ec2.Vpc.from_lookup(
                self,
                f"{id}-exporter-vpc",
                vpc_id=vpc_id,
            )
        else:
            vpc = None

        lambda_role = None
        if lambda_role_arn is not None:
            lambda_role = _iam.Role.from_role_arn(
                self,
                f"{id}-role",
                role_arn=lambda_role_arn
            )

        fn = _lambda.Function(
            self,
            f"{id}-exporter-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                path='../lambda',
                exclude=[
                    'venv',
                    '__pycache__',
                    '.pytest_cache',
                    '.hypothesis',
                    'tests',
                    '.build',
                    'cdk',
                    '*.pyc',
                    '*.md',
                    '.env*',
                    '.gitignore',
                    'logs',
                    'move_files.py',
                    'move_country_files.py',
                ],
            ),
            vpc=vpc,
            handler='open_data_export.main.handler',
            role=lambda_role,
            environment=stringify_settings(env_variables),
            memory_size=ingest_lambda_memory_size,
            timeout=Duration.seconds(ingest_lambda_timeout),
            layers=[
                create_dependencies_layer(
                    self,
                    f"{env_name}",
                    'exporter'
                ),
            ],
            log_retention=_logs.RetentionDays.ONE_WEEK,
        )

        _events.Rule(
            self,
            f"{id}-export-event-rule",
            schedule=_events.Schedule.cron(minute="0/10"),
            targets=[
                _targets.LambdaFunction(fn),
            ],
        )

        bucket = _s3.Bucket.from_bucket_name(
            self,
            "{id}-openaq-open-data-exports",
            env_variables['OPEN_DATA_BUCKET'],
        )
        # print(rule)
        bucket.grant_put(fn)
        bucket.grant_delete(fn)
        bucket.grant_put_acl(fn)

        fn.add_to_role_policy(
            _iam.PolicyStatement(
                actions=["s3:GetObjectAcl"],
                effect=_iam.Effect.ALLOW,
                resources=[
                    f"{bucket.bucket_arn}/*"
        ]
        )
        )
