from dataclasses import dataclass
from typing import Optional
from aws_cdk import (
    Stack,
    StackProps,
    CfnOutput,
)
from constructs import Construct
from aws_cdk import aws_s3 as s3

from infra.constructs.cdk_network import NetworkConstruct
from infra.constructs.cdk_data_migration import DataMigrationConstruct
from ...utils.naming import create_name


# --- Props dataclass ---
@dataclass
class DataMigrationStackProps(StackProps):
    raw_bucket: s3.IBucket


# --- Stack ---
class DataMigrationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, props: DataMigrationStackProps, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        raw_bucket = props.raw_bucket

        # 1. Red (VPC, Subnets, SecurityGroup, DMS SubnetGroup)
        network = NetworkConstruct(
            self,
            "Network",
            include_dms_subnet_group=True,
        )

        # Security Group para DataSync
        data_sync_sg = network.create_data_sync_security_group()

        # 2. Migración de datos (DMS, DataSync, IAM, Secrets, Endpoints, Tasks)
        data_migration = DataMigrationConstruct(
            self,
            "DataMigration",
            raw_bucket=raw_bucket,
            vpc=network.vpc,
            subnet_group=network.dms_subnet_group,
            security_group=network.dms_security_group,
            data_sync_ami_id="ami-0fcd2673fce5dd85d",
            data_sync_security_group=data_sync_sg,
            dms_iam_mode="create",
        )

        # 3. Outputs
        CfnOutput(
            self,
            "RawBucketName",
            value=raw_bucket.bucket_name,
            description="Raw bucket used by DMS",
        )

        CfnOutput(
            self,
            "VpcId",
            value=network.vpc.vpc_id,
            export_name=create_name("DataMigration", "VpcId"),
            description="VPC ID used for data migration",
        )

        CfnOutput(
            self,
            "PrivateSubnetIdsExport",
            value=",".join([s.subnet_id for s in network.vpc.private_subnets]),
            export_name=create_name("DataMigration", "PrivateSubnetIds"),
            description="Private subnet IDs for workloads (Redshift Serverless WG)",
        )

        CfnOutput(
            self,
            "PrivateRouteTableIdsExport",
            value=",".join([s.route_table.route_table_id for s in network.vpc.private_subnets]),
            export_name=create_name("DataMigration", "PrivateRouteTableIds"),
            description="Route table IDs for those private subnets",
        )

        CfnOutput(
            self,
            "DmsSecurityGroupId",
            value=network.dms_security_group.security_group_id,
            description="Security Group ID for DMS",
        )

        CfnOutput(
            self,
            "DmsInstanceArn",
            value=data_migration.replication_instance_arn,
            description="DMS Replication Instance ARN",
        )

        CfnOutput(
            self,
            "DmsTargetEndpointArn",
            value=data_migration.target_endpoint_arn,
            description="DMS Target Endpoint ARN (S3)",
        )

        CfnOutput(
            self,
            "DmsDb2SourceEndpointArn",
            value=data_migration.db2_endpoint_arn,
            description="DMS DB2 Source Endpoint ARN",
        )

        CfnOutput(
            self,
            "DmsSqlServerSourceEndpointArn",
            value=data_migration.sql_server_endpoint_arn,
            description="DMS SQL Server Source Endpoint ARN",
        )

        CfnOutput(
            self,
            "Db2SecretArn",
            value=data_migration.db2_secret_arn,
            description="DB2 credentials secret ARN",
        )

        CfnOutput(
            self,
            "SqlServerSecretArn",
            value=data_migration.sql_server_secret_arn,
            description="SQL Server credentials secret ARN",
        )
