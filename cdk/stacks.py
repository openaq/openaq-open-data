from aws_cdk import (
    aws_lambda as _lambda,
    aws_events as _events,
    aws_logs as _logs,
    aws_s3 as _s3,
    aws_events_targets as _targets,
    Stack,
    Duration,
)
import pathlib
from constructs import Construct


class ExportStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        package_directory: str,
        env_variables: dict = {},
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        package = _lambda.Code.from_asset(
            str(pathlib.Path.joinpath(package_directory, "package.zip"))
        )

        exportPendingLambda = _lambda.Function(
            self,
            f"{id}-export-pending-lambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=package,
            handler='open_data_export.main.export_pending',
            environment=env_variables,
            memory_size=512,
            log_retention=_logs.RetentionDays.ONE_WEEK,
            timeout=Duration.seconds(900),
        )

        rule = _events.Rule(
            self,
            f"{id}-event-rule",
            schedule=_events.Schedule.cron(minute="0/1"),
            targets=[
                _targets.LambdaFunction(exportPendingLambda),
            ],
        )

        bucket = _s3.Bucket.from_bucket_name(
            self,
            "{id}-openaq-open-data-exports",
            env_variables['OPEN_DATA_BUCKET'],
        )

        print(rule)
        bucket.grant_put(exportPendingLambda)
        bucket.grant_put_acl(exportPendingLambda)


class UpdateStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        package_directory: str,
        env_variables: dict = {},
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        package = _lambda.Code.from_asset(
            str(pathlib.Path.joinpath(package_directory, "package.zip"))
        )

        updateOutdatedLambda = _lambda.Function(
            self,
            f"{id}-update-outdated-lambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=package,
            handler='open_data_export.main.update_outdated',
            environment=env_variables,
            memory_size=512,
            log_retention=_logs.RetentionDays.ONE_WEEK,
            timeout=Duration.seconds(900),
        )

        rule = _events.Rule(
            self,
            f"{id}-update-outdated-event-rule",
            schedule=_events.Schedule.cron(minute="0/5"),
            targets=[
                _targets.LambdaFunction(updateOutdatedLambda),
            ],
        )

        bucket = _s3.Bucket.from_bucket_name(
            self,
            "{id}-openaq-open-data-exports",
            env_variables['OPEN_DATA_BUCKET'],
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
        env_variables: dict = {},
        **kwargs,
    ) -> None:
        """Define stack."""
        super().__init__(scope, id, **kwargs)

        package = _lambda.Code.from_asset(
            str(pathlib.Path.joinpath(package_directory, "package.zip"))
        )

        moveLambda = _lambda.Function(
            self,
            f"{id}-move-objects-lambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            code=package,
            handler='open_data_export.main.move_objects_handler',
            environment=env_variables,
            memory_size=512,
            log_retention=_logs.RetentionDays.ONE_WEEK,
            timeout=Duration.seconds(900),
        )

        _events.Rule(
            self,
            f"{id}-move-objects-rule",
            schedule=_events.Schedule.cron(minute="0/1"),
            targets=[
                _targets.LambdaFunction(moveLambda),
            ],
        )

        bucket = _s3.Bucket.from_bucket_name(
            self,
            "{id}-openaq-open-data-exports",
            env_variables['OPEN_DATA_BUCKET'],
        )

        # print(rule)
        bucket.grant_put(moveLambda)
        bucket.grant_delete(moveLambda)
        bucket.grant_put_acl(moveLambda)
