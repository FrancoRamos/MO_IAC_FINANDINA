# lib/analytics/analytics_consumption_stack.py
from aws_cdk import (
    Stack,
    CfnOutput,
    Fn,
    aws_ec2 as ec2,
    aws_kms as kms,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct
from dataclasses import dataclass
from typing import Optional
from infra.constructs.cdk_redshift_serverless import RedshiftServerlessConstruct
from infra.constructs.cdk_redshift_bootstrap import RedshiftBootstrap
from ...utils.naming import create_name


@dataclass
class AnalyticsConsumptionStackProps:
    context_env: dict
    analytics_bucket: s3.IBucket
    data_kms_key: Optional[kms.IKey] = None


class AnalyticsConsumptionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, props: AnalyticsConsumptionStackProps, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        context_env = props.context_env
        analytics_bucket = props.analytics_bucket
        data_kms_key = props.data_kms_key

        # 1) Rebuild VPC from DataMigrationStack exports
        vpc = ec2.Vpc.from_vpc_attributes(
            self,
            "ImportedVpc",
            vpc_id=Fn.import_value(create_name("DataMigration", "VpcId")),
            private_subnet_ids=Fn.split(",", Fn.import_value(create_name("DataMigration", "PrivateSubnetIds"))),
            private_subnet_route_table_ids=Fn.split(",", Fn.import_value(create_name("DataMigration", "PrivateRouteTableIds"))),
            availability_zones=Fn.get_azs(self.region),
        )

        # 2) Dedicated Security Group for Redshift Serverless
        redshift_sg = ec2.SecurityGroup(
            self,
            "RedshiftSg",
            vpc=vpc,
            description="SG for Redshift Serverless workgroup",
            allow_all_outbound=True,
        )
        # Optional ingress example:
        # redshift_sg.add_ingress_rule(ec2.Peer.security_group_id("<source-sg>"), ec2.Port.tcp(5439), "JDBC from app/bastion")

        ns_name = create_name("rs", "namespace")
        wg_name = create_name("rs", "workgroup")
        db_name = f"{context_env.project}_{context_env.environment}".lower().replace(r"[^a-z0-9]", "_")

        # 3) Create Redshift Serverless using private subnets with egress
        redshift = RedshiftServerlessConstruct(
            self,
            "RedshiftServerless",
            vpc=vpc,
            subnets={"subnetType": ec2.SubnetType.PRIVATE_WITH_EGRESS},
            security_group=redshift_sg,
            namespace_name=ns_name,
            workgroup_name=wg_name,
            db_name=db_name,
            buckets_for_spectrum=[analytics_bucket],
            kms_key=data_kms_key,
            base_capacity_rpus=32,
        )

        CfnOutput(self, "WorkgroupName", value=wg_name)
        CfnOutput(self, "NamespaceName", value=ns_name)
        CfnOutput(self, "RedshiftDefaultRoleArn", value=redshift.role_for_spectrum.role_arn)

        # === Placeholders ===
        glue_database_name = create_name("glue", "analytics").replace("-", "_").lower()
        external_schema_name = create_name("rs", "spectrum").replace("-", "_").lower()
        # db_user = "admin"

        # Reminder output
        CfnOutput(
            self,
            "ReminderCreateDbUser",
            value=(
                f"Después del primer deploy: crea el usuario DB 'admin' en Redshift Serverless y concédele permisos. "
                f"Ejecuta: CREATE USER admin; GRANT CREATE ON DATABASE {db_name} TO admin;"
            ),
            description="Recordatorio para crear el usuario DB requerido por el bootstrap",
        )

        redshift_admin_secret = secretsmanager.Secret(
            self,
            "RedshiftAdminSecret",
            secret_name=create_name("secrets", "redshift-admin"),
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "admin"}',
                generate_string_key="password",
                exclude_characters='"@/\\',
            ),
        )

        # Bootstrap for External Schema — disabled on first deploy to avoid failure
        RedshiftBootstrap(
            self,
            "BootstrapSpectrum",
            workgroup_name=wg_name,
            database=db_name,
            external_schema_name=external_schema_name,
            glue_database_name=glue_database_name,
            role_arn=redshift.role_for_spectrum.role_arn,
            drop_on_delete=False,
            enabled=False,  # First deploy: set False. After user creation, change to True and redeploy.
            secret_arn=redshift_admin_secret.secret_arn,
        )
