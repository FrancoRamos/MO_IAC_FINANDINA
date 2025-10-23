from dataclasses import dataclass
from aws_cdk import (
    Duration,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_logs as logs,
    aws_glue as glue,
    aws_sns as sns,
)
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_dynamodb import Table
from aws_cdk.aws_lambda import LayerVersion, IFunction
from aws_cdk.aws_kms import Key
from aws_cdk.aws_iam import Role
from constructs import Construct

from ...utils.naming import create_name


# --- Props ---
@dataclass
class EtlTfGeneralCETDiarioConstructProps:
    environment: str
    raw_bucket: Bucket
    master_bucket: Bucket
    scripts_bucket: Bucket
    raw_database: str
    master_database: str
    datalake_lib_layer_arn: str
    lambda_execution_role: Role
    job_role: Role
    kms_key: Key


# --- Construct ---
class EtlTfGeneralCETDiarioConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlTfGeneralCETDiarioConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        raw_bucket = props.raw_bucket
        master_bucket = props.master_bucket
        scripts_bucket = props.scripts_bucket
        raw_database = props.raw_database
        master_database = props.master_database
        datalake_lib_layer_arn = props.datalake_lib_layer_arn
        lambda_execution_role = props.lambda_execution_role
        job_role = props.job_role
        kms_key = props.kms_key

        # --- Lambda Layer ---
        datalake_lib_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "DatalakeLibLayer",
            datalake_lib_layer_arn,
        )

        # --- SNS Topic ---
        sns_topic = sns.Topic(
            self,
            "TransformSnsTopic",
            topic_name=create_name("sns", "CET-notify"),
        )

        sns_topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                principals=[lambda_execution_role],
                resources=[sns_topic.topic_arn],
            )
        )

        # --- Glue Jobs ---
        glue.CfnJob(
            self,
            "GlueJobRawCET",
            name=create_name("glue", "raw-cet"),
            role=job_role.role_arn,
            command={
                "name": "glueetl",
                "pythonVersion": "3",
                "scriptLocation": f"s3://{scripts_bucket.bucket_name}/general_CET_diario/sm-cet/etl_raw_cet.py",
            },
            execution_property={"maxConcurrentRuns": 3},
            default_arguments={
                "--enable-glue-datacatalog": "",
                "--target_database_name": master_database,
                "--target_bucket": master_bucket.bucket_name,
                "--source_database_name": raw_database,
                "--source_bucket": raw_bucket.bucket_name,
            },
            glue_version="4.0",
            worker_type="G.2X",
            number_of_workers=2,
            max_retries=1,
            timeout=60,
        )

        glue.CfnJob(
            self,
            "GlueJobRawToMasterTraCET",
            name=create_name("glue", "raw-to-master-tra-cet"),
            role=job_role.role_arn,
            command={
                "name": "glueetl",
                "pythonVersion": "3",
                "scriptLocation": f"s3://{scripts_bucket.bucket_name}/general_CET_diario/sm-cet/etl_raw_to_master_tra_cet.py",
            },
            execution_property={"maxConcurrentRuns": 3},
            default_arguments={
                "--enable-glue-datacatalog": "",
                "--target_database_name": master_database,
                "--target_bucket": master_bucket.bucket_name,
                "--source_database_name": raw_database,
                "--source_bucket": raw_bucket.bucket_name,
            },
            glue_version="4.0",
            worker_type="G.2X",
            number_of_workers=2,
            max_retries=1,
            timeout=60,
        )

        glue.CfnJob(
            self,
            "GlueJobMasterToAnalyticsLoadCET",
            name=create_name("glue", "master-to-analytics-load-cet"),
            role=job_role.role_arn,
            command={
                "name": "glueetl",
                "pythonVersion": "3",
                "scriptLocation": f"s3://{scripts_bucket.bucket_name}/general_CET_diario/sm-cet/etl_master_to_analytics_load_cet.py",
            },
            execution_property={"maxConcurrentRuns": 3},
            default_arguments={
                "--enable-glue-datacatalog": "",
                "--target_database_name": master_database,
                "--target_bucket": master_bucket.bucket_name,
                "--source_database_name": raw_database,
                "--source_bucket": raw_bucket.bucket_name,
            },
            glue_version="4.0",
            worker_type="G.2X",
            number_of_workers=2,
            max_retries=1,
            timeout=60,
        )

        # --- Job Names ---
        self.job_raw_cet = f"datalake-gluejob-{environment}-raw-cet"
        self.job_raw_to_master_tra_cet = f"datalake-gluejob-{environment}-raw-to-master-tra-cet"
        self.job_master_to_analytics_load_cet = f"datalake-gluejob-{environment}-master-to-analytics-load-cet"
