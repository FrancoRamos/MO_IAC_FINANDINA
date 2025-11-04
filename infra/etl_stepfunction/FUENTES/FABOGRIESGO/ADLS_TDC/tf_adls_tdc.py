from aws_cdk import (
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    aws_glue as glue,
    aws_sns as sns,
)
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_dynamodb import Table
from aws_cdk.aws_kms import Key
from aws_cdk.aws_iam import Role
from constructs import Construct
import os

from .....utils.naming import create_name
from dataclasses import dataclass

@dataclass
class EtlTfAdlsFaboTdcConstructProps:
    environment: str
    scripts_bucket: Bucket
    job_role: Role


class EtlTfAdlsFaboTdcConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlTfAdlsFaboTdcConstructProps) -> None:
        super().__init__(scope, id)

        scripts_bucket = props.scripts_bucket
        job_role = props.job_role

        # job_tdc_name = create_name("glue", "adls-tdc")
        # glue.CfnJob(
        #     self,
        #     "GlueAdlsTDC",
        #     name=job_tdc_name,
        #     role=job_role.role_arn,
        #     command=glue.CfnJob.JobCommandProperty(
        #         name="glueetl",
        #         python_version="3",
        #         script_location=f"s3://{scripts_bucket.bucket_name}/FUENTES/FABOGRIESGO/ADLS_TDC/etl_tdc.py",
        #     ),
        #     execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=3),
        #     default_arguments={
        #         "--additional-python-modules": "delta-spark==3.3.0",
        #     },
        #     glue_version="5.0",
        #     worker_type="G.1X",
        #     number_of_workers=10,
        #     max_retries=0,
        #     timeout=60,
        # )
        
        # self.job_tdc_name = job_tdc_name
