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
class EtlTfAdlsBronzeSfcConstructProps:
    environment: str
    scripts_bucket: Bucket
    job_role: Role
    raw_bucket_name: str
    raw_database: str

class EtlTfAdlsBronzeSfcConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlTfAdlsBronzeSfcConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        scripts_bucket = props.scripts_bucket
        job_role = props.job_role
        raw_bucket_name = props.raw_bucket_name
        raw_database = props.raw_database
        
        self.job_bronze_sfc_name = create_name("glue", "bronze-sfc")
        # --- Glue Jobs ---
        glue.CfnJob(
            self,
            "GlueJobRawSD",
            name=self.job_bronze_sfc_name,
            role=job_role.role_arn,
            command={
                "name": "glueetl",
                "pythonVersion": "3",
                "scriptLocation": f"s3://{scripts_bucket.bucket_name}/DATABRICKS/BRONZE/SFC/etl_bronze_sfc.py",
            },
            execution_property={"maxConcurrentRuns": 3},
            default_arguments={
                "--JOB_NAME": self.job_bronze_sfc_name,
                "--raw_bucket": raw_bucket_name,
                "--raw_db": raw_database,
            },
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=0,
            timeout=60,
        )
