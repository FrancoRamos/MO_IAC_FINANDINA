from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Duration,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_kms as kms,
)
import os
from .....utils.naming import create_name


@dataclass
class EtlTfAdlsCetFive9ConstructProps:
    environment: str
    region: str
    account: str
    raw_bucket: s3.Bucket
    scripts_bucket: s3.Bucket
    raw_database: str
    datalake_lib_layer_arn: str
    lambda_execution_role: iam.Role
    job_role: iam.Role
    kms_key: kms.Key
    redshift_database: str
    redshift_secret_arn: str
    redshift_workgroup_name: str


class EtlTfAdlsCetFive9Construct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlTfAdlsCetFive9ConstructProps) -> None:
        super().__init__(scope, id)

        datalake_lib_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "DatalakeLibLayer",
            props.datalake_lib_layer_arn
        )

        lambda_lookup1 = _lambda.Function(
            self,
            "lambdaLookUp1",
            function_name=create_name("lambda", "fuentes-adls-cet-five9-lambdalookup1"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
                    "../etl_glue_jobs/FUENTES/FIVE9/ADLS_CET_FIVE9/lambda/lambdaLookUp1/src"
                )
            ),
            role=props.lambda_execution_role,
            environment={
                "REDSHIFT_WORKGROUP": props.redshift_workgroup_name,
                "REDSHIFT_DATABASE": props.redshift_database,
                "REDSHIFT_SECRET_ARN": props.redshift_secret_arn,
            },
            layers=[datalake_lib_layer],
            memory_size=512,
            timeout=Duration.minutes(10),
        )

        self.lambda_lookup1 = lambda_lookup1