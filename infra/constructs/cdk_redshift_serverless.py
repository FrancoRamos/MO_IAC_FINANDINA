# infra/constructs/cdk_redshift_serverless.py
from aws_cdk import (
    aws_redshiftserverless as redshiftserverless,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_kms as kms,
    aws_s3 as s3,
    RemovalPolicy,
    Tags,
)
from constructs import Construct
from dataclasses import dataclass
from typing import List, Optional
from aws_cdk.aws_iam import PolicyStatement
from ..utils.naming import create_name


@dataclass
class RedshiftServerlessProps:
    vpc: ec2.IVpc
    subnets: Optional[ec2.SubnetSelection] = None
    security_group: Optional[ec2.ISecurityGroup] = None
    namespace_name: str = ""
    workgroup_name: str = ""
    db_name: str = ""
    buckets_for_spectrum: Optional[List[s3.IBucket]] = None
    kms_key: Optional[kms.IKey] = None
    base_capacity_rpus: Optional[int] = 32
    enable_log_exports: Optional[bool] = False
    removal_policy: Optional[RemovalPolicy] = RemovalPolicy.DESTROY


class RedshiftServerlessConstruct(Construct):
    def __init__(self, scope: Construct, id: str, props: RedshiftServerlessProps) -> None:
        super().__init__(scope, id)

        vpc = props.vpc
        subnets = props.subnets or ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
        security_group = props.security_group
        namespace_name = props.namespace_name
        workgroup_name = props.workgroup_name
        db_name = props.db_name
        buckets_for_spectrum = props.buckets_for_spectrum or []
        kms_key = props.kms_key
        base_capacity_rpus = props.base_capacity_rpus or 32
        enable_log_exports = props.enable_log_exports or False
        removal_policy = props.removal_policy or RemovalPolicy.DESTROY

        # === Security Group ===
        self.security_group = (
            security_group
            or ec2.SecurityGroup(
                self,
                "RedshiftServerlessSg",
                vpc=vpc,
                description="Security Group for Redshift Serverless workgroup",
                allow_all_outbound=True,
                security_group_name=create_name("sg", "redshift-serverless"),
            )
        )
        Tags.of(self.security_group).add("Name", create_name("sg", "redshift"))

        # === IAM Role (Glue/Lake Formation/S3) ===
        self.role_for_spectrum = iam.Role(
            self,
            "RedshiftDefaultRole",
            role_name=create_name("iam", "redshift-default-role"),
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            description="Default IAM Role for Redshift Serverless to access S3/Glue/Lake Formation",
        )

        # Glue Data Catalog permissions
        self.role_for_spectrum.add_to_policy(
            PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:SearchTables",
                ],
                resources=["*"],
            )
        )

        # Lake Formation access
        self.role_for_spectrum.add_to_policy(
            PolicyStatement(
                actions=["lakeformation:GetDataAccess"],
                resources=["*"],
            )
        )

        # S3 read-only access for declared buckets
        for b in buckets_for_spectrum:
            b.grant_read(self.role_for_spectrum)
            self.role_for_spectrum.add_to_policy(
                PolicyStatement(
                    actions=["s3:ListBucket"],
                    resources=[b.bucket_arn],
                )
            )

        # === Namespace ===
        self.namespace = redshiftserverless.CfnNamespace(
            self,
            "Namespace",
            namespace_name=namespace_name,
            db_name=db_name,
            default_iam_role_arn=self.role_for_spectrum.role_arn,
            iam_roles=[self.role_for_spectrum.role_arn],
            kms_key_id=kms_key.key_arn if kms_key else None,
            log_exports=["userlog", "connectionlog", "useractivitylog"] if enable_log_exports else None,
        )
        self.namespace.apply_removal_policy(removal_policy)

        # === Workgroup (compute) ===
        subnet_ids = vpc.select_subnets(subnets).subnet_ids

        self.workgroup = redshiftserverless.CfnWorkgroup(
            self,
            "Workgroup",
            workgroup_name=workgroup_name,
            namespace_name=namespace_name,
            base_capacity=base_capacity_rpus,
            enhanced_vpc_routing=True,
            subnet_ids=subnet_ids,
            security_group_ids=[self.security_group.security_group_id],
        )
        self.workgroup.apply_removal_policy(removal_policy)
        self.workgroup.add_dependency(self.namespace)
