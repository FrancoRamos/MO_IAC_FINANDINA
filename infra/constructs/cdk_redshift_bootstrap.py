# infra/constructs/cdk_redshift_bootstrap.py

from aws_cdk import (
    Duration,
    CustomResource,
    aws_iam as iam,
    aws_lambda as _lambda,
    custom_resources as cr,
    aws_logs as logs,
)
from constructs import Construct
from dataclasses import dataclass
from typing import Optional
import os
from ..utils.naming import create_name


@dataclass
class RedshiftBootstrapProps:
    workgroup_name: str
    database: str
    external_schema_name: str
    glue_database_name: str
    role_arn: str
    drop_on_delete: Optional[bool] = False
    lambda_code_dir: Optional[str] = "../scripts/lambda/redshift/src"
    enabled: Optional[bool] = None
    secret_arn: Optional[str] = None  # Optional secret for DB credentials


class RedshiftBootstrap(Construct):
    def __init__(self, scope: Construct, id: str, props: RedshiftBootstrapProps) -> None:
        super().__init__(scope, id)

        workgroup_name = props.workgroup_name
        database = props.database
        external_schema_name = props.external_schema_name
        glue_database_name = props.glue_database_name
        role_arn = props.role_arn
        drop_on_delete = props.drop_on_delete or False
        lambda_code_dir = props.lambda_code_dir or "../scripts/lambda/redshift/src"
        enabled = props.enabled
        secret_arn = props.secret_arn

        if not enabled:
            # Skip creation (used for first deploy without DB user)
            return

        handler = _lambda.Function(
            self,
            "BootstrapFn",
            function_name=create_name("lambda", "redshift-bootstrap"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.minutes(5),
            memory_size=512,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(os.path.join(os.path.dirname(__file__), lambda_code_dir)),
            log_retention=logs.RetentionDays.ONE_WEEK,
            description="Creates a Redshift external schema pointing to Glue Data Catalog via Data API",
        )

        # === Permissions for Redshift Data API ===
        handler.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "redshift-data:ExecuteStatement",
                    "redshift-data:DescribeStatement",
                    "redshift-data:GetStatementResult",
                    "redshift-data:ListSchemas",
                    "redshift-data:ListDatabases",
                ],
                resources=["*"],
            )
        )

        # === Permissions for Redshift Serverless credentials ===
        handler.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "redshift-serverless:GetCredentials",
                    "redshift-serverless:GetWorkgroup",
                    "redshift-serverless:GetNamespace",
                ],
                resources=["*"],
            )
        )

        # Optional: permission to read IAM Role if verified in code
        handler.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:GetRole"],
                resources=[role_arn],
            )
        )

        # Optional: permission to read Secret if provided
        if secret_arn:
            handler.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[secret_arn],
                )
            )

        provider = cr.Provider(
            self,
            "BootstrapProvider",
            on_event_handler=handler,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        CustomResource(
            self,
            "RedshiftExternalSchemaBootstrap",
            service_token=provider.service_token,
            properties={
                "workgroupName": workgroup_name,
                "database": database,
                "externalSchemaName": external_schema_name,
                "glueDatabaseName": glue_database_name,
                "roleArn": role_arn,
                "dropOnDelete": drop_on_delete,
                "secretArn": secret_arn,
            },
        )
