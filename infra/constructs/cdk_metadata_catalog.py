# lib/constructs/cdk_metadata_catalog.py
from constructs import Construct
from aws_cdk import (
    aws_dynamodb as ddb,
    RemovalPolicy,
)
from aws_cdk.aws_kms import Key
from dataclasses import dataclass

from ..utils.naming import create_name


@dataclass
class MetadataCatalogConstructProps:
    kms_key: Key
    removal_policy: RemovalPolicy = RemovalPolicy.DESTROY


class MetadataCatalogConstruct(Construct):
    def __init__(self, scope: Construct, id: str, props: MetadataCatalogConstructProps) -> None:
        super().__init__(scope, id)

        kms_key = props.kms_key
        removal_policy = props.removal_policy

        # === Metadata Table (main metadata catalog) ===
        self.metadata_table = ddb.Table(
            self,
            "DynamoObjectMetadata",
            table_name=create_name("ddb", "metadata"),
            partition_key=ddb.Attribute(
                name="excId", type=ddb.AttributeType.STRING
            ),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            encryption=ddb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=kms_key,
            stream=ddb.StreamViewType.NEW_AND_OLD_IMAGES,
            point_in_time_recovery=True,
            removal_policy=removal_policy,
        )

        # --- Global Secondary Indexes ---
        self.metadata_table.add_global_secondary_index(
            index_name=create_name("gsi", "automatization-processDateStart"),
            partition_key=ddb.Attribute(
                name="automatization", type=ddb.AttributeType.STRING
            ),
            sort_key=ddb.Attribute(
                name="processDateStart", type=ddb.AttributeType.STRING
            ),
        )

        self.metadata_table.add_global_secondary_index(
            index_name=create_name("gsi", "status-processDateStart"),
            partition_key=ddb.Attribute(
                name="status", type=ddb.AttributeType.STRING
            ),
            sort_key=ddb.Attribute(
                name="processDateStart", type=ddb.AttributeType.STRING
            ),
        )

        # === Dependencies Table ===
        self.dependencies_table = ddb.Table(
            self,
            "DependenciesTable",
            table_name=create_name("ddb", "dependencies"),
            partition_key=ddb.Attribute(
                name="automatization", type=ddb.AttributeType.STRING
            ),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            encryption=ddb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=kms_key,
            point_in_time_recovery=True,
            removal_policy=removal_policy,
        )

        # === Tracking Table ===
        self.tracking_table = ddb.Table(
            self,
            "TrackingTable",
            table_name=create_name("ddb", "tracking"),
            partition_key=ddb.Attribute(
                name="excId", type=ddb.AttributeType.STRING
            ),
            sort_key=ddb.Attribute(
                name="eventTimestamp", type=ddb.AttributeType.STRING
            ),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            encryption=ddb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=kms_key,
            point_in_time_recovery=True,
            removal_policy=removal_policy,
        )
