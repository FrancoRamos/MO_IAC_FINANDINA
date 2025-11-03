from dataclasses import dataclass
from aws_cdk import (
    Duration,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_glue as glue,
    aws_sns as sns,
)
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_dynamodb import Table
from aws_cdk.aws_lambda import LayerVersion, IFunction
from aws_cdk.aws_kms import Key
from aws_cdk.aws_iam import Role
from constructs import Construct
import os
from ....utils.naming import create_name


# --- Props ---
@dataclass
class EtlTfGeneralSDDiarioConstructProps:
    environment: str
    scripts_bucket: Bucket
    lambda_execution_role: Role
    job_role: Role
    kms_key: Key


# --- Construct ---
class EtlTfGeneralSDDiarioConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlTfGeneralSDDiarioConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        scripts_bucket = props.scripts_bucket
        lambda_execution_role = props.lambda_execution_role
        job_role = props.job_role
        kms_key = props.kms_key


        self.job_raw_name = create_name("glue", "raw-sd")
        # --- Glue Jobs ---
        glue.CfnJob(
            self,
            "GlueJobRawSD",
            name=self.job_raw_name,
            role=job_role.role_arn,
            command={
                "name": "glueetl",
                "pythonVersion": "3",
                "scriptLocation": f"s3://{scripts_bucket.bucket_name}/general_SD_diario/etl_raw_sd.py",
            },
            execution_property={"maxConcurrentRuns": 3},
            default_arguments={
            },
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=10,
            max_retries=0,
            timeout=60,
        )


        # --- Lambdas ---
        self.autom_lambda = _lambda.Function(
            self,
            "RawSDLambda",
            function_name=create_name("lambda", "raw-sd"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "../../scripts/lambda/raw_sd/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
