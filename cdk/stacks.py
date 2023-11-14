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


class ExportStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        package_directory: str,
        env_name: str,
        ingest_lambda_timeout: int,
        ingest_lambda_memory_size: int,
        env_variables: dict = {},
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

        sg = _ec2.SecurityGroup(
            self,
            f"{id}-exporter-ssh-sg",
            vpc=vpc,
            allow_all_outbound=True,
        )


        exportPendingLambda = _lambda.Function(
            self,
            f"{id}-export-pending-lambda",
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
            environment=stringify_settings(env_variables),
            memory_size=ingest_lambda_memory_size,
            timeout=Duration.seconds(ingest_lambda_timeout),
            layers=[
                create_dependencies_layer(
                    self,
                    f"{env_name}",
                    'update'
                ),
            ],
            log_retention=_logs.RetentionDays.ONE_WEEK
        )

        _events.Rule(
            self,
            f"{id}-event-rule",
            schedule=_events.Schedule.cron(minute="0/10"),
            targets=[
                _targets.LambdaFunction(exportPendingLambda),
            ],
        )

        bucket = _s3.Bucket.from_bucket_name(
            self,
            "{id}-openaq-exports-open-data-bucket",
            env_variables['OPEN_DATA_BUCKET'],
        )

        bucket.grant_put(exportPendingLambda)
        bucket.grant_put_acl(exportPendingLambda)

        if env_variables['DB_BACKUP_BUCKET'] is not None:
            db_backup_bucket = _s3.Bucket.from_bucket_name(
                self,
                "{id}-openaq-exports-db-backup-bucket",
                env_variables['DB_BACKUP_BUCKET'],
            )
            db_backup_bucket.grant_put(exportPendingLambda)
            db_backup_bucket.grant_put_acl(exportPendingLambda)



class UpdateStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        bucket: str,
        env_name: str,
        lambda_env: Dict,
        lambda_timeout: int = 900,
        lambda_memory_size: int = 512,
        rate_minutes: int = 5,
        lambda_role_arn: str = None,
        # package_directory: str,
        # env_variables: dict = {},
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        lambda_role = None
        if lambda_role_arn is not None:
            print(f'looking up role: {lambda_role_arn}')
            lambda_role = _iam.Role.from_role_arn(
                self,
                f"{id}-role",
                role_arn=lambda_role_arn
            )

        updateOutdatedLambda = _lambda.Function(
            self,
            f"{id}-update-outdated-lambda",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                path='../lambda',
                exclude=[
                    'venv',
                    '__pycache__',
                    'pytest_cache',
                ],
            ),
            handler='open_data_export.main.update_outdated_handler',
            memory_size=lambda_memory_size,
            timeout=Duration.seconds(lambda_timeout),
            environment=stringify_settings(lambda_env),
            layers=[
                create_dependencies_layer(
                    self,
                    f"{env_name}",
                    'update'  # just use the same layer for now
                ),
            ],
            log_retention=_logs.RetentionDays.ONE_WEEK,
            role=lambda_role
        )

        rule = _events.Rule(
            self,
            f"{id}-update-outdated-event-rule",
            schedule=_events.Schedule.cron(
                minute=f"0/{rate_minutes}"
            ),
            targets=[
                _targets.LambdaFunction(updateOutdatedLambda),
            ],
        )

        bucket = _s3.Bucket.from_bucket_name(
            self,
            "{id}-openaq-open-data-exports",
            bucket,
        )

        print(rule)
        bucket.grant_put(updateOutdatedLambda)
        bucket.grant_put_acl(updateOutdatedLambda)


class MoveStack(Stack):
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

        moveLambda = _lambda.Function(
            self,
            f"{id}-move-objects-lambda",
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
                    'move'
                ),
            ],
            log_retention=_logs.RetentionDays.ONE_WEEK,
        )
        # _events.Rule(
        #     self,
        #     f"{id}-move-objects-rule",
        #     schedule=_events.Schedule.cron(minute="0/1"),
        #     targets=[
        #         _targets.LambdaFunction(moveLambda),
        #     ],
        # )
        bucket = _s3.Bucket.from_bucket_name(
            self,
            "{id}-openaq-open-data-exports",
            env_variables['OPEN_DATA_BUCKET'],
        )
        # print(rule)
        bucket.grant_put(moveLambda)
        bucket.grant_delete(moveLambda)
        bucket.grant_put_acl(moveLambda)

        moveLambda.add_to_role_policy(
            _iam.PolicyStatement(
                actions=["s3:GetObjectAcl"],
				effect=_iam.Effect.ALLOW,
                resources=[
					f"{bucket.bucket_arn}/*"
		]
		)
		)
