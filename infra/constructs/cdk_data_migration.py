from dataclasses import dataclass
from typing import Optional
from constructs import Construct
from aws_cdk import (
    aws_dms as dms,
    aws_s3 as s3,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_ec2 as ec2,
    aws_lambda as _lambda,
    custom_resources as cr,
    aws_glue as glue,
    Stack,
    RemovalPolicy,
    CfnDeletionPolicy,
    Duration,
    CustomResource,
    SecretValue
)
from ..utils.naming import create_name
import os

# --- Props dataclass ---
@dataclass
class DataMigrationConstructProps:
    raw_bucket: s3.IBucket
    vpc: ec2.IVpc
    subnet_group: dms.CfnReplicationSubnetGroup
    security_group: ec2.ISecurityGroup
    data_sync_ami_id: Optional[str] = None
    data_sync_security_group: Optional[ec2.ISecurityGroup] = None
    dms_iam_mode: str = "create"  # 'create' | 'useExisting'
    dms_vpc_role_name: str = "dms-vpc-role"
    dms_logs_role_name: str = "dms-cloudwatch-logs-role"
    create_sac_secret: bool = False


# --- Construct ---
class DataMigrationConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: DataMigrationConstructProps) -> None:
        super().__init__(scope, id)

        raw_bucket = props.raw_bucket
        vpc = props.vpc
        subnet_group = props.subnet_group
        security_group = props.security_group
        dms_iam_mode = props.dms_iam_mode
        dms_vpc_role_name = props.dms_vpc_role_name
        dms_logs_role_name = props.dms_logs_role_name
        create_sac_secret = props.create_sac_secret

        created_vpc_role_cfn: Optional[iam.CfnRole] = None
        created_logs_role_cfn: Optional[iam.CfnRole] = None

        # --- DMS IAM Roles ---
        if dms_iam_mode == "create":
            dms_vpc_role = iam.Role(
                self,
                "DmsVpcRole",
                role_name=dms_vpc_role_name,
                assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AmazonDMSVPCManagementRole"
                    )
                ],
            )
            created_vpc_role_cfn = dms_vpc_role.node.default_child

            dms_logs_role = iam.Role(
                self,
                "DmsCloudWatchLogsRole",
                role_name=dms_logs_role_name,
                assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
                managed_policies=[
                    iam.ManagedPolicy.from_aws_managed_policy_name(
                        "service-role/AmazonDMSCloudWatchLogsRole"
                    )
                ],
            )
            created_logs_role_cfn = dms_logs_role.node.default_child
        else:
            iam.Role.from_role_name(self, "DmsVpcRoleImported", dms_vpc_role_name)
            iam.Role.from_role_name(self, "DmsCloudWatchLogsRoleImported", dms_logs_role_name)

        # --- IAM Role for S3 Access ---
        dms_s3_role = iam.Role(
            self,
            "DmsS3AccessRole",
            role_name=create_name("iam", "dms-s3-access-role"),
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
            inline_policies={
                "s3Access": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["s3:ListBucket", "s3:GetBucketLocation"],
                            resources=[raw_bucket.bucket_arn],
                        ),
                        iam.PolicyStatement(
                            actions=["s3:PutObject", "s3:DeleteObject", "s3:GetObject"],
                            resources=[f"{raw_bucket.bucket_arn}/*"],
                        ),
                    ]
                )
            },
        )

        # --- SAC_FINANDINA Secret ---
        sac_secret_name = "dms/sqlserver/SAC_FINANDINA"
        if create_sac_secret:
            sac_sql_secret = secretsmanager.Secret(
                self,
                "SacSqlSecret",
                secret_name=sac_secret_name,
                description="Credenciales SQL Server para DMS - SAC_FINANDINA",
                removal_policy=RemovalPolicy.RETAIN,
                secret_object_value={
                    "engine": SecretValue.unsafe_plain_text("sqlserver"),
                    "host": SecretValue.unsafe_plain_text("SACINTERNAL"),
                    "port": SecretValue.unsafe_plain_text("1433"),
                    "dbname": SecretValue.unsafe_plain_text("SAC_FINANDINA"),
                    "username": SecretValue.unsafe_plain_text("username"),
                    "password": SecretValue.unsafe_plain_text("password"),
                },
            )
        else:
            sac_sql_secret = secretsmanager.Secret.from_secret_name_v2(
                self, "SacSqlSecretImported", sac_secret_name
            )

        self.sac_sql_secret = sac_sql_secret
        self.sac_sql_secret_arn = sac_sql_secret.secret_arn

        # --- DMS Role to read Secret ---
        dms_sm_role = iam.Role(
            self,
            "DmsSMAccessRole",
            role_name=create_name("iam", "dms-dm-access-role"),
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
            inline_policies={
                "SNAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
                            resources=[self.sac_sql_secret_arn],
                        )
                    ]
                )
            },
        )

        # --- Generic DB2 Secret ---
        self.db2_secret = secretsmanager.Secret(
            self,
            "DmsDb2Secret",
            secret_name=create_name("secrets", "dms-db2"),
            description="DB2 database credentials for DMS source endpoint",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"db2admin","engine":"db2","host":"db2-server.company.com","port":50000,"dbname":"TESTDB2"}',
                generate_string_key="password",
                exclude_characters='"@/\\;:\'<>',
            ),
        )

        # --- Generic SQL Server Secret ---
        self.sql_server_secret = secretsmanager.Secret(
            self,
            "DmsSqlServerSecret",
            secret_name=create_name("secrets", "dms-sqlserver"),
            description="SQL Server database credentials for DMS source endpoint",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"sqladmin","serverName":"sqlserver.company.com","port":1433,"dbname":"TestDatabase"}',
                generate_string_key="password",
                exclude_characters='"@/\\;:\'<>',
            ),
        )

        # --- Replication Instance ---
        self.replication_instance = dms.CfnReplicationInstance(
            self,
            "ReplicationInstance",
            replication_instance_identifier=create_name("dms", "ri"),
            replication_instance_class="dms.t3.medium",
            allocated_storage=50,
            publicly_accessible=False,
            replication_subnet_group_identifier=subnet_group.replication_subnet_group_identifier,
            vpc_security_group_ids=[security_group.security_group_id],
        )

        self.replication_instance.add_dependency(subnet_group)
        if created_vpc_role_cfn:
            self.replication_instance.add_dependency(created_vpc_role_cfn)
        if created_logs_role_cfn:
            self.replication_instance.add_dependency(created_logs_role_cfn)

        # --- Endpoints ---
        self.db2_endpoint = dms.CfnEndpoint(
            self,
            "Db2Endpoint",
            endpoint_identifier=create_name("dms", "ep-db2-src"),
            endpoint_type="source",
            engine_name="db2",
            server_name=self.db2_secret.secret_value_from_json("host").unsafe_unwrap(),
            port=50000,
            username=self.db2_secret.secret_value_from_json("username").unsafe_unwrap(),
            password=self.db2_secret.secret_value_from_json("password").unsafe_unwrap(),
            database_name=self.db2_secret.secret_value_from_json("dbname").unsafe_unwrap(),
        )

        self.sql_server_endpoint = dms.CfnEndpoint(
            self,
            "SqlServerEndpoint",
            endpoint_identifier=create_name("dms", "ep-sqlserver-src"),
            endpoint_type="source",
            engine_name="sqlserver",
            server_name=self.sql_server_secret.secret_value_from_json("serverName").unsafe_unwrap(),
            port=1433,
            username=self.sql_server_secret.secret_value_from_json("username").unsafe_unwrap(),
            password=self.sql_server_secret.secret_value_from_json("password").unsafe_unwrap(),
            database_name=self.sql_server_secret.secret_value_from_json("dbname").unsafe_unwrap(),
        )

        self.sql_server_endpoint_finandina = dms.CfnEndpoint(
            self,
            "SqlServerEndpointFinandina",
            endpoint_identifier=create_name("dms", "ep-sqlserver-finandina-src"),
            endpoint_type="source",
            engine_name="sqlserver",
            server_name=sac_sql_secret.secret_value_from_json("host").unsafe_unwrap(),
            port=1433,
            username=sac_sql_secret.secret_value_from_json("username").unsafe_unwrap(),
            password=sac_sql_secret.secret_value_from_json("password").unsafe_unwrap(),
            database_name=sac_sql_secret.secret_value_from_json("dbname").unsafe_unwrap(),
        )

        self.s3_target_endpoint = dms.CfnEndpoint(
            self,
            "S3TargetEndpoint",
            endpoint_identifier=create_name("dms", "ep-s3-target"),
            endpoint_type="target",
            engine_name="s3",
            s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                bucket_name=raw_bucket.bucket_name,
                bucket_folder="dms-ingestion/",
                service_access_role_arn=dms_s3_role.role_arn,
                data_format="parquet",
                compression_type="GZIP",
            ),
        )

        # --- Replication Tasks ---
        dms.CfnReplicationTask(
            self,
            "Db2ToS3Task",
            replication_task_identifier=create_name("dms", "task-db2-s3"),
            replication_instance_arn=self.replication_instance.ref,
            source_endpoint_arn=self.db2_endpoint.ref,
            target_endpoint_arn=self.s3_target_endpoint.ref,
            migration_type="full-load",
            table_mappings='{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"include-db2","object-locator":{"schema-name":"DB2ADMIN","table-name":"%"},"rule-action":"include"},{"rule-type":"transformation","rule-id":"2","rule-name":"prefix-db2","rule-target":"table","rule-action":"add-prefix","object-locator":{"schema-name":"DB2ADMIN","table-name":"%"},"value":"db2_"}]}',
        )

        dms.CfnReplicationTask(
            self,
            "SqlServerToS3Task",
            replication_task_identifier=create_name("dms", "task-sqlserver-s3"),
            replication_instance_arn=self.replication_instance.ref,
            source_endpoint_arn=self.sql_server_endpoint.ref,
            target_endpoint_arn=self.s3_target_endpoint.ref,
            migration_type="full-load",
            table_mappings='{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"include-sqlserver","object-locator":{"schema-name":"dbo","table-name":"%"},"rule-action":"include"},{"rule-type":"transformation","rule-id":"2","rule-name":"prefix-sqlserver","rule-target":"table","rule-action":"add-prefix","object-locator":{"schema-name":"dbo","table-name":"%"},"value":"sqlserver_"}]}',
        )

        dms.CfnReplicationTask(
            self,
            "SqlServerFinandinaToS3Task",
            replication_task_identifier=create_name("dms", "task-sqlserver-finandina-s3"),
            replication_instance_arn=self.replication_instance.ref,
            source_endpoint_arn=self.sql_server_endpoint_finandina.ref,
            target_endpoint_arn=self.s3_target_endpoint.ref,
            migration_type="full-load",
            table_mappings='{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"include-dbo","object-locator":{"schema-name":"dbo","table-name":"%"},"rule-action":"include"}]}',
        )

        # --- Outputs ---
        self.replication_instance_arn = self.replication_instance.ref
        self.target_endpoint_arn = self.s3_target_endpoint.ref
        self.db2_endpoint_arn = self.db2_endpoint.ref
        self.sql_server_endpoint_arn = self.sql_server_endpoint.ref
        self.db2_secret_arn = self.db2_secret.secret_arn
        self.sql_server_secret_arn = self.sql_server_secret.secret_arn
