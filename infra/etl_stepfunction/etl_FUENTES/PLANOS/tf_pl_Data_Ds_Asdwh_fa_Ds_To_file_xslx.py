# pl_xlsx_tf.py
from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_logs as logs,
)


@dataclass
class TfProps:
    """
    Props para la Stack de exportación XLSX (PlXlsxTf).
    """
    export_prefix: str                     # p.ej. 'finandina/planos/dwh/taximetro/'
    redshift_secret_arn: str               # ARN del secret Redshift
    notification_email: str | None = None  # opcional


class PlXlsxTf(Construct):
    """
    Stack que crea el bucket de exportación, tópico SNS y rol Step Functions
    para ejecutar Redshift Data API y manejar fallos.
    """

    def __init__(self, scope: Construct, id: str, *, props: TfProps) -> None:
        super().__init__(scope, id)

        # ----- Bucket de exportación -----
        self.export_bucket = s3.Bucket(
            self,
            "ExportBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    enabled=True,
                    expiration=Duration.days(90),
                    prefix=props.export_prefix,
                )
            ],
        )

        # ----- SNS Topic para fallos -----
        self.topic = sns.Topic(
            self,
            "FailureTopic",
            display_name="ADF-xlsx Failure Notifications",
        )

        if props.notification_email:
            self.topic.add_subscription(
                subs.EmailSubscription(props.notification_email)
            )

        # ----- Rol Step Functions (Data API + SNS) -----
        self.sfn_role = iam.Role(
            self,
            "PlXlsxSfRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )

        self.sfn_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "redshift-data:ExecuteStatement",
                    "redshift-data:DescribeStatement",
                    "redshift-data:GetStatementResult",
                    "redshift-data:CancelStatement",
                ],
                resources=["*"],
            )
        )

        self.sfn_role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[props.redshift_secret_arn],  # permiso al secret
            )
        )

        # Permisos cruzados
        self.topic.grant_publish(self.sfn_role)
        self.export_bucket.grant_read_write(self.sfn_role)

        # ----- Política TLS obligatoria en S3 -----
        self.export_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="DenyInsecureTransport",
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                actions=["s3:*"],
                resources=[
                    self.export_bucket.bucket_arn,
                    f"{self.export_bucket.bucket_arn}/*",
                ],
                conditions={"Bool": {"aws:SecureTransport": False}},
            )
        )

        # ----- Logs Step Functions -----
        logs.LogGroup(
            self,
            "SfnLogs",
            retention=logs.RetentionDays.ONE_MONTH,
        )
