from aws_cdk import (
    Stack,
    CfnOutput,
    aws_lambda as _lambda,
)
from constructs import Construct
from dataclasses import dataclass

from infra.constructs.cdk_security import SecurityConstruct, SecurityConstructProps
from infra.constructs.cdk_storage import StorageConstruct, StorageConstructProps
from infra.constructs.cdk_metadata_catalog import MetadataCatalogConstruct, MetadataCatalogConstructProps
from infra.constructs.cdk_data_ingestion import DataIngestionConstruct, DataIngestionConstructProps
from infra.constructs.cdk_data_governance import DataGovernanceConstruct, DataGovernanceConstructProps


@dataclass
class FoundationsStackProps:
    context_env: dict


class FoundationsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, *, props: FoundationsStackProps, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        context_env = props.context_env
        account_id = context_env.accountId
        region = context_env.region
        environment = context_env.environment

        # --- Security Construct (KMS, IAM)
        self.security = SecurityConstruct(
            self,
            "Security",
            props=SecurityConstructProps(context_env=context_env)
        )

        # --- Storage Construct (S3 Buckets)
        self.storage = StorageConstruct(
            self,
            "Storage",
            props=StorageConstructProps(
                context_env=context_env,
                kms_key=self.security.kmsKey,
                lake_formation_role=self.security.lakeFormationRole,
            ),
        )

        self.storage.raw_bucket.grant_read(self.security.lambdaExecutionRole)
        self.storage.master_bucket.grant_read_write(self.security.lambdaExecutionRole)
        self.storage.athena_bucket.grant_read_write(self.security.lambdaExecutionRole)

        # --- Metadata Catalog Construct (DynamoDB)
        self.metadata_catalog = MetadataCatalogConstruct(
            self,
            "MetadataCatalog",
            props=MetadataCatalogConstructProps(
                kms_key=self.security.kmsKey,
            ),
        )

        # --- Data Ingestion Construct (Lambda + SQS + EventBridge)
        ingestion = DataIngestionConstruct(
            self,
            "DataIngestion",
            props=DataIngestionConstructProps(
                context_env=context_env,
                kms_key=self.security.kmsKey,
                lambda_execution_role=self.security.lambdaExecutionRole,
                metadata_table=self.metadata_catalog.metadata_table,
                raw_bucket=self.storage.raw_bucket,
                landing_bucket=self.storage.landing_bucket,
                analytics_bucket=self.storage.analytics_bucket,
            ),
        )

        self.datalake_layer: _lambda.LayerVersion = ingestion.datalake_layer

        # --- Data Governance Construct (Glue / SSM)
        self.governance = DataGovernanceConstruct(
            self,
            "DataGovernance",
            props=DataGovernanceConstructProps(
                context_env=context_env,
            ),
        )

        self.rawDatabaseName: str = self.governance.raw_database_name
        self.masterDatabaseName: str = self.governance.master_database_name
        self.analyticsDatabaseName: str = self.governance.analytics_database_name

        # --- Outputs
        CfnOutput(
            self,
            "oS3ArtifactsBucket",
            description="Name of the domain's Artifacts S3 bucket",
            value=self.storage.artifacts_bucket.bucket_name,
        )

        CfnOutput(
            self,
            "oChildAccountId",
            description="Child AWS account ID",
            value=account_id,
        )
