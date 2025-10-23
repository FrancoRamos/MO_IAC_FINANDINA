# tf_general_CargaPlanoDwh.py
from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    RemovalPolicy,
    aws_iam as iam,
    aws_s3_assets as s3assets,
    aws_glue as glue,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
)


@dataclass
class PlanoMayorTfProps:
    script_path: str
    alert_emails: list[str]
    job_name: str
    job_role: iam.IRole | None = None


class PlanoMayorTf(Construct):
    """
    Crea:
      - Asset S3 con el script .py
      - IAM Role para Glue
      - Glue Job
      - SNS Topic con subscripciones de email
    """

    def __init__(self, scope: Construct, id: str, props: PlanoMayorTfProps) -> None:
        super().__init__(scope, id)

        # 1) Asset: subir el script .py a S3 (lo maneja CDK)
        script_asset = s3assets.Asset(self, "PlanoMayorScript", path=props.script_path)

        # 2) Rol IAM para Glue
        glue_role = props.job_role or iam.Role(
            self,
            "PlanoMayorGlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )

        # Permisos básicos: leer script asset en S3 y escribir logs
        script_asset.bucket.grant_read(glue_role)

        if isinstance(glue_role, iam.Role):
            glue_role.add_managed_policy(
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            )
            glue_role.add_to_policy(
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    resources=["*"],
                )
            )

        glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        # 3) Glue Job
        job = glue.CfnJob(
            self,
            "GlueJobPlanoMayor",
            name=props.job_name,
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script_asset.s3_object_url,
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
                "project": "cobranza",
                "pipeline": "plano-mayor",
            },
        )

        job.node.add_dependency(script_asset)

        self.job_name = props.job_name

        # 4) SNS para fallas
        self.failure_topic = sns.Topic(
            self,
            "PlanoMayorFailureTopic",
            display_name="PlanoMayor pipeline failures",
        )
        self.failure_topic.apply_removal_policy(RemovalPolicy.DESTROY)  # demo; en prod usa RETAIN

        for email in props.alert_emails:
            self.failure_topic.add_subscription(subs.EmailSubscription(email))
