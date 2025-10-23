from constructs import Construct
from aws_cdk import (
    Aws,
    aws_kms as kms,
    aws_iam as iam,
    RemovalPolicy,
    Stack,
    ArnFormat,
)
from dataclasses import dataclass

from infra.utils.naming import create_name


@dataclass
class SecurityConstructProps:
    context_env: dict


class SecurityConstruct(Construct):
    """Creación de recursos de seguridad: KMS Key, Lake Formation Role y Lambda Execution Role."""

    def __init__(self, scope: Construct, id: str, *, props: SecurityConstructProps) -> None:
        super().__init__(scope, id)

        context_env = props.context_env
        region = context_env.region
        account_id = context_env.accountId
        environment = context_env.environment

        stack = Stack.of(self)

        # === KMS Key ===
        kms_key_policy = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    sid="Allow administration of the key",
                    effect=iam.Effect.ALLOW,
                    principals=[iam.AccountRootPrincipal()],
                    actions=["kms:*"],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    sid="Allow logs access",
                    effect=iam.Effect.ALLOW,
                    principals=[iam.ServicePrincipal("logs.amazonaws.com")],
                    actions=[
                        "kms:CreateGrant",
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:Encrypt",
                        "kms:GenerateDataKey*",
                        "kms:ReEncrypt*",
                    ],
                    resources=["*"],
                ),
            ]
        )

        self.kmsKey = kms.Key(
            self,
            "rKMSKey",
            description="SDLF Foundations KMS Key",
            enable_key_rotation=True,
            policy=kms_key_policy,
            removal_policy=RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
        )

        self.kmsKey.add_alias(f"alias/{create_name('kms', 'key')}")

        # === Lake Formation Data Access Role ===
        lakeformation_policy = iam.Policy(
            self,
            "LakeFormationLogAccess",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    resources=[
                        stack.format_arn(
                            service="logs",
                            resource="log-group",
                            resource_name="/aws-lakeformation-acceleration/*",
                            arn_format=ArnFormat.COLON_RESOURCE_NAME,
                        ),
                        stack.format_arn(
                            service="logs",
                            resource="log-group",
                            resource_name="/aws-lakeformation-acceleration/*:log-stream:*",
                            arn_format=ArnFormat.COLON_RESOURCE_NAME,
                        ),
                    ],
                ),
            ],
        )

        self.lakeFormationRole = iam.Role(
            self,
            "rLakeFormationDataAccessRole",
            role_name=create_name("iam", "lakeformation-data-access"),
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("lakeformation.amazonaws.com"),
                iam.ServicePrincipal("glue.amazonaws.com"),
            ),
        )

        self.lakeFormationRole.attach_inline_policy(lakeformation_policy)

        # === Lambda Execution Role ===
        self.lambdaExecutionRole = iam.Role(
            self,
            "rLambdaExecutionRole",
            role_name=create_name("iam", "lambda-execution"),
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        # Permiso básico para logs
        self.lambdaExecutionRole.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/*"],
            )
        )
