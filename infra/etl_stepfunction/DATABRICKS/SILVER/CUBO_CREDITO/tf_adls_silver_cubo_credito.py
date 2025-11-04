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
from .....utils.naming import create_name

@dataclass
class EtlTfAdlsSilverCuboCreditoConstructProps:
    environment: str
    scripts_bucket: Bucket
    job_role: Role


class EtlTfAdlsSilverCuboCreditoConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlTfAdlsSilverCuboCreditoConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        scripts_bucket = props.scripts_bucket
        job_role = props.job_role
        
        self.job_silver_cubocredito_name = create_name("glue", "silver-cubocredito-traCCDC")
        self.job_silver_reportecubo_name = create_name("glue", "silver-reportecubo")
        # --- Glue Jobs ---
        glue.CfnJob(
            self,
            "GlueJobSilverCuboCredito",
            name=self.job_silver_cubocredito_name,
            role=job_role.role_arn,
            command={
                "name": "glueetl",
                "pythonVersion": "3",
                "scriptLocation": f"s3://{scripts_bucket.bucket_name}/DATABRICKS/SILVER/CUBO_CREDITO/etl_silver_cubocredito.py",
            },
            execution_property={"maxConcurrentRuns": 3},
            default_arguments={
                "--extra-py-files": f"s3://{scripts_bucket.bucket_name}/lib/lib_function.py"
            },
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=0,
            timeout=60,
        )

        # --- Glue Jobs ---
        glue.CfnJob(
            self,
            "GlueJobSilverReporteCubo",
            name=self.job_silver_reportecubo_name,
            role=job_role.role_arn,
            command={
                "name": "glueetl",
                "pythonVersion": "3",
                "scriptLocation": f"s3://{scripts_bucket.bucket_name}/DATABRICKS/SILVER/CUBO_CREDITO/etl_silver_reportecubo.py",
            },
            execution_property={"maxConcurrentRuns": 3},
            default_arguments={
                "--extra-py-files": f"s3://{scripts_bucket.bucket_name}/lib/lib_function.py"
            },
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=0,
            timeout=60,
        )
