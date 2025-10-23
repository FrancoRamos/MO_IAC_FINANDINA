# constructs_lib/cdk_network.py

from constructs import Construct
from aws_cdk import (
    aws_ec2 as ec2,
    Tags,
)
from ..utils.naming import create_name
from dataclasses import dataclass


@dataclass
class NetworkConstructProps:
    """
    :param vpc_cidr: CIDR principal para la VPC. Default: 10.0.0.0/16
    """
    vpc_cidr: str = "10.0.0.0/16"

class NetworkConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: NetworkConstructProps = None) -> None:
        super().__init__(scope, id)

        vpc_cidr = props.vpc_cidr if props else "10.0.0.0/16"

        # VPC con subredes públicas y privadas
        self.vpc = ec2.Vpc(
            self,
            "AnalyticsVpc",
            vpc_name=create_name("vpc", "analytics"),
            cidr=vpc_cidr,
            max_azs=3,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name=create_name("subnet", "public"),
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name=create_name("subnet", "private"),
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # Subnets
        self.private_subnets = self.vpc.private_subnets
        self.public_subnets = self.vpc.public_subnets

        # Security Group para Redshift
        self.redshift_sg = ec2.SecurityGroup(
            self,
            "RedshiftSecurityGroup",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Security group for Redshift Serverless",
            security_group_name=create_name("sg", "redshift"),
        )

        self.redshift_sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(self.vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(5439),
            description="Allow Redshift connections from VPC",
        )
