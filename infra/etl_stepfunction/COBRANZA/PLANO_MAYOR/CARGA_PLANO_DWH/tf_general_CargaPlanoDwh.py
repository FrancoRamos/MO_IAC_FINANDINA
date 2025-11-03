from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Duration,
    aws_iam as iam,
    aws_s3_assets as s3assets,
    aws_glue as glue,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_lambda as _lambda,
)
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_iam import Role
from .....utils.naming import create_name
import os

@dataclass
class EtlTfCargaPlanoDWHConstructProps:
    environment: str
    project: str
    scripts_bucket: Bucket
    job_role: iam.IRole
    lambda_execution_role: Role


class EtlTfCargaPlanoDWHConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlTfCargaPlanoDWHConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        project = props.project
        scripts_bucket = props.scripts_bucket
        job_role = props.job_role
        lambda_execution_role = props.lambda_execution_role

        # 2) Rol IAM para Glue
        glue_role = job_role or iam.Role(
            self,
            "PlanoMayorGlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        # if isinstance(glue_role, iam.Role):
        #     glue_role.add_managed_policy(
        #         iam.ManagedPolicy.from_aws_managed_policy_name(
        #             "service-role/AWSGlueServiceRole"
        #         )
        #     )
        #     glue_role.add_to_policy(
        #         iam.PolicyStatement(
        #             actions=[
        #                 "logs:CreateLogGroup",
        #                 "logs:CreateLogStream",
        #                 "logs:PutLogEvents",
        #             ],
        #             resources=["*"],
        #         )
        #     )

        # glue_role.add_managed_policy(
        #     iam.ManagedPolicy.from_aws_managed_policy_name(
        #         "service-role/AWSGlueServiceRole"
        #     )
        # )

        # 3) Glue Job
        job_carga_plano_dwh_name = create_name("glue", "carga-plano-dwh")
        job = glue.CfnJob(
            self,
            "GlueJobCargaPlanoDWH",
            name=job_carga_plano_dwh_name,
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/COBRANZAS/PLANO_MAYOR/CARGA_PLANO_DWH/carga_plano_dwh.py",
            ),
            glue_version="4.0",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            timeout=60,  # minutos
            max_retries=0,
            number_of_workers=2,
            worker_type="G.1X",
            default_arguments={
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--fecha": "20250101",  # valores dummy, se sobreescriben desde Step Functions
                "--tipoPlano": "-1",
            },
            tags={
                "Environment": environment,
                "Project": project,
                "project": "cobranza",
                "pipeline": "plano-mayor",
            },
        )

        self.job_carga_plano_dwh_name = job_carga_plano_dwh_name
        
        
        # --- Lambdas ---
        self.carga_plano_dwh_lambda = _lambda.Function(
            self,
            "CargaPlanoDWHLambda",
            function_name=create_name("lambda", "carga-plano-dwh"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "../../scripts/lambda/carga_plano_dwh/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )