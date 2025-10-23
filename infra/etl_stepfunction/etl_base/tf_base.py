# infra/etl_transform/etl_tf_base_construct.py
from aws_cdk import (
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
    aws_glue as glue,
    aws_sns as sns,
)
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_dynamodb import Table
from aws_cdk.aws_kms import Key
from aws_cdk.aws_iam import Role
from constructs import Construct
import os

from ...utils.naming import create_name
from dataclasses import dataclass

@dataclass
class EtlTfBaseConstructProps:
    environment: str
    region: str
    account: str
    raw_bucket: Bucket
    master_bucket: Bucket
    analytics_bucket: Bucket
    scripts_bucket: Bucket
    tracking_table: Table
    raw_database: str
    master_database: str
    analytics_database: str
    datalake_lib_layer_arn: str
    lambda_execution_role: Role
    job_role: Role
    kms_key: Key


class EtlTfBaseConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlTfBaseConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        region = props.region
        account = props.account
        raw_bucket = props.raw_bucket
        master_bucket = props.master_bucket
        analytics_bucket = props.analytics_bucket
        scripts_bucket = props.scripts_bucket
        tracking_table = props.tracking_table
        raw_database = props.raw_database
        master_database = props.master_database
        analytics_database = props.analytics_database
        datalake_lib_layer_arn = props.datalake_lib_layer_arn
        lambda_execution_role = props.lambda_execution_role
        job_role = props.job_role
        kms_key = props.kms_key

        # --- Layer from ARN ---
        datalake_lib_layer = _lambda.LayerVersion.from_layer_version_arn(
            self,
            "DatalakeLibLayer",
            datalake_lib_layer_arn,
        )

        # --- SNS Topic ---
        sns_topic = sns.Topic(
            self,
            "TransformSnsTopic",
            topic_name=create_name("sns", "autom01-notify"),
        )

        sns_topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                principals=[lambda_execution_role],
                resources=[sns_topic.topic_arn],
            )
        )

        # --- Lambdas ---
        preupdate_lambda = _lambda.Function(
            self,
            "PreUpdateLambda",
            function_name=create_name("lambda", "autom01-preupdate"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../../scripts/lambda/preupdate/src")
            ),
            role=lambda_execution_role,
            environment={
                "trakingTable": tracking_table.table_name,
                "rawBucket": raw_bucket.bucket_name,
                "trackingDynamoDB": tracking_table.table_name,
                "rawDatabase": raw_database,
                "masterBucket": master_bucket.bucket_name,
                "masterDatabase": master_database,
            },
            layers=[datalake_lib_layer],
            memory_size=512,
            timeout=Duration.seconds(300),
        )

        postupdate_lambda = _lambda.Function(
            self,
            "PostUpdateLambda",
            function_name=create_name("lambda", "autom01-postupdate"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../../scripts/lambda/postupdate/src")
            ),
            role=lambda_execution_role,
            environment={
                "region": region,
                "account": account,
            },
            layers=[datalake_lib_layer],
            memory_size=256,
            timeout=Duration.seconds(60),
        )

        error_lambda = _lambda.Function(
            self,
            "ErrorLambda",
            function_name=create_name("lambda", "autom01-error"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../../scripts/lambda/error/src")
            ),
            role=lambda_execution_role,
            environment={
                "topic": sns_topic.topic_arn,
            },
            layers=[datalake_lib_layer],
            memory_size=256,
            timeout=Duration.seconds(60),
        )

        # --- Glue Jobs ---
        glue.CfnJob(
            self,
            "GlueJobRawToMaster",
            name=create_name("glue", "raw-to-master-autom01"),
            role=job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/base/etl_raw_to_master_autom01.py",
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=3),
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
            "GlueJobMasterToAnalytics",
            name=create_name("glue", "master-to-analytics-autom01"),
            role=job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/base/etl_master_to_analytics_autom01.py",
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=3),
            default_arguments={
                "--enable-glue-datacatalog": "",
                "--target_database_name": analytics_database,
                "--target_bucket": analytics_bucket.bucket_name,
                "--source_database_name": master_database,
                "--source_bucket": master_bucket.bucket_name,
            },
            glue_version="4.0",
            worker_type="G.2X",
            number_of_workers=2,
            max_retries=1,
            timeout=60,
        )

        # --- Exposed attributes ---
        self.preupdate_lambda = preupdate_lambda
        self.postupdate_lambda = postupdate_lambda
        self.error_lambda = error_lambda
        self.job_raw_to_master_name = f"datalake-gluejob-{environment}-raw-to-master-autom01"
        self.job_master_to_analytics_name = f"datalake-gluejob-{environment}-master-to-analytics-autom01"
