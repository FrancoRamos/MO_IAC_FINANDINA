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


# --- Props ---
@dataclass
class EtlTfCargaSFCConstructProps:
    environment: str
    scripts_bucket: Bucket
    lambda_execution_role: Role
    job_role: Role
    kms_key: Key


# --- Construct ---
class EtlTfCargaSFCConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlTfCargaSFCConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        scripts_bucket = props.scripts_bucket
        lambda_execution_role = props.lambda_execution_role
        job_role = props.job_role
        kms_key = props.kms_key


        self.job_cargasfc_name = create_name("glue", "carga-sfc")
        # --- Glue Jobs ---   falta las configuraciones
        glue.CfnJob(
            self,
            "GlueStartJobRun",
            name=self.job_cargasfc_name,
            role=job_role.role_arn,
            command={
                "name": "glueetl",
                "pythonVersion": "3",
                "scriptLocation": f"s3://{scripts_bucket.bucket_name}/AUTOMATIZACION/SFC/etl_cargasfc.py",  ## ok nombre
            },
            execution_property={"maxConcurrentRuns": 3},
            default_arguments={
            },
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            max_retries=0,
            timeout=60,
        )


        # --- Lambdas ---
        self.cargasfcdim_lambda = _lambda.Function(
            self,
            "LambdauspLoad_Dim",
            function_name=create_name("lambda", "cargadim-sfc"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "../../scripts/lambda/carga_sfc/src")  
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.cargasfcfacts_lambda = _lambda.Function(
            self,
            "LambdauspLoad_Facts",
            function_name=create_name("lambda", "cargafacts-sfc"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "../../scripts/lambda/carga_sfc/src2")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
