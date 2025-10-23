# lib/constructs/cdk_storage.py
from constructs import Construct
from aws_cdk import (
    aws_s3 as s3,
    aws_lakeformation as lakeformation,
    Duration,
    RemovalPolicy,
)
from aws_cdk.aws_kms import Key
from aws_cdk.aws_iam import Role
from dataclasses import dataclass

from ..utils.naming import create_name
from ..utils.environments import environments as Environment

REMOVAL_POLICY =RemovalPolicy.DESTROY # RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE

@dataclass
class StorageConstructProps:
    context_env: dict
    kms_key: Key
    lake_formation_role: Role


class StorageConstruct(Construct):
    def __init__(self, scope: Construct, id: str, props: StorageConstructProps) -> None:
        super().__init__(scope, id)

        context_env = props.context_env
        kms_key = props.kms_key
        lake_formation_role = props.lake_formation_role

        region = context_env.region
        account_id = context_env.accountId
        owner_account = context_env.ownerAccount
        environment = context_env.environment

        # === Access Logs Bucket ===
        self.access_logs_bucket = s3.Bucket(
            self,
            "S3AccessLogsBucket",
            bucket_name=create_name("s3", f"access-logs-{account_id}"),
            encryption=s3.BucketEncryption.KMS,
            encryption_key=kms_key,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="InfrequentAccess",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(60),
                        )
                    ],
                ),
                s3.LifecycleRule(
                    id="DeepArchive",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.DEEP_ARCHIVE,
                            transition_after=Duration.days(60),
                        )
                    ],
                ),
            ],
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=REMOVAL_POLICY,
        )

        # === Artifacts Bucket ===
        self.artifacts_bucket = s3.Bucket(
            self,
            "ArtifactsBucket",
            bucket_name=create_name("s3", f"artifacts-{account_id}"),
            encryption=s3.BucketEncryption.KMS,
            encryption_key=kms_key,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix=create_name("s3", "artifacts-logs"),
            bucket_key_enabled=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=REMOVAL_POLICY,
        )

        # === Data Buckets (raw, landing, master, analytics) ===
        self.raw_bucket = self._create_data_bucket("raw", kms_key, lake_formation_role, account_id)
        self.landing_bucket = self._create_data_bucket("landing", kms_key, lake_formation_role, account_id)
        self.master_bucket = self._create_data_bucket("master", kms_key, lake_formation_role, account_id)
        self.analytics_bucket = self._create_data_bucket("analytics", kms_key, lake_formation_role, account_id)

        # === Scripts Bucket ===
        self.scripts_bucket = s3.Bucket(
            self,
            "ScriptsBucket",
            bucket_name=create_name("s3", f"scripts-{account_id}"),
            encryption=s3.BucketEncryption.KMS,
            encryption_key=kms_key,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix=create_name("s3", "scripts-logs"),
            event_bridge_enabled=False,
            bucket_key_enabled=True,
            removal_policy=REMOVAL_POLICY,
        )

        # === Athena Bucket ===
        self.athena_bucket = s3.Bucket(
            self,
            "AthenaBucket",
            bucket_name=create_name("s3", f"athena-{account_id}"),
            encryption=s3.BucketEncryption.KMS,
            encryption_key=kms_key,
            versioned=True,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix=create_name("s3", "athena-logs"),
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            event_bridge_enabled=True,
            bucket_key_enabled=True,
            removal_policy=REMOVAL_POLICY,
        )

    # === Private helper for LF-enabled Data Buckets ===
    def _create_data_bucket(self, layer: str, kms_key: Key, lake_formation_role: Role, account_id: str) -> s3.Bucket:
        bucket = s3.Bucket(
            self,
            f"{layer.capitalize()}Bucket",
            bucket_name=create_name("s3", f"{layer}-{account_id}"),
            encryption=s3.BucketEncryption.KMS,
            encryption_key=kms_key,
            versioned=True,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix=create_name("s3", f"{layer}-logs"),
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            event_bridge_enabled=True,
            bucket_key_enabled=True,
            removal_policy=REMOVAL_POLICY,
        )

        lakeformation.CfnResource(
            self,
            f"LakeFormation{layer.capitalize()}Bucket",
            resource_arn=f"{bucket.bucket_arn}/",
            use_service_linked_role=False,
            role_arn=lake_formation_role.role_arn,
        )

        return bucket
