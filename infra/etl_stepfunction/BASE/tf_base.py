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
    redshift_secret_arn: str
    redshift_workgroup_name: str
    redshift_role: str


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
        redshift_secret_arn = props.redshift_secret_arn
        redshift_workgroup_name = props.redshift_workgroup_name
        redshift_role = props.redshift_role

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

        sns_failure_topic = sns.Topic(
            self,
            "FailureSnsTopic",
            topic_name=create_name("sns", "failure-topic"),
        )

        sns_failure_topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                principals=[lambda_execution_role],
                resources=[sns_failure_topic.topic_arn],
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
            timeout=Duration.seconds(300),
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
            timeout=Duration.seconds(300),
        )

        sql_runner_lambda = _lambda.Function(
            self,
            "SqlRunnerLambda",
            function_name=create_name("lambda", "sql-runner"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../../scripts/lambda/sql_runner/src")
            ),
            role=lambda_execution_role,
            environment={
            },
            layers=[datalake_lib_layer],
            memory_size=256,
            timeout=Duration.seconds(300),
        )

        read_metrics_lambda = _lambda.Function(
            self,
            "ReadMetricsLambda",
            function_name=create_name("lambda", "read-metrics"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../../scripts/lambda/read_metrics/src")
            ),
            role=lambda_execution_role,
            environment={
            },
            layers=[datalake_lib_layer],
            memory_size=256,
            timeout=Duration.seconds(300),
        )

        get_active_tables_lambda = _lambda.Function(
            self,
            "GetActiveTablesLambda",
            function_name=create_name("lambda", "get-active-tables"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../../scripts/lambda/get_active_tables/src")
            ),
            role=lambda_execution_role,
            environment={
            },
            layers=[datalake_lib_layer],
            memory_size=256,
            timeout=Duration.seconds(300),
        )

        get_origin_params_lambda = _lambda.Function(
            self,
            "GetOriginParamsLambda",
            function_name=create_name("lambda", "get-origin-params"),
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../../scripts/lambda/get_origin_params/src")
            ),
            role=lambda_execution_role,
            environment={
            },
            layers=[datalake_lib_layer],
            memory_size=256,
            timeout=Duration.seconds(300),
        )

        # --- Glue Jobs ---
        job_raw_to_master_name = create_name("glue", "raw-to-master-autom01")
        glue.CfnJob(
            self,
            "GlueJobRawToMaster",
            name=job_raw_to_master_name,
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

        job_master_to_analytics_name = create_name("glue", "master-to-analytics-autom01")
        glue.CfnJob(
            self,
            "GlueJobMasterToAnalytics",
            name=job_master_to_analytics_name,
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

        job_extract_copy_name = create_name("glue", "extract-copy")
        glue.CfnJob(
            self,
            "GlueExtractCopy",
            name=job_extract_copy_name,
            role=job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/base/etl_extract_copy.py",
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=3),
            default_arguments={
                "REDSHIFT_IAM_ROLE": redshift_role,
                "REDSHIFT_REGION": region,
                "REDSHIFT_SECRET_ARN": redshift_secret_arn,
                "REDSHIFT_WORKGROUP": redshift_workgroup_name
            },
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=20,
            max_retries=0,
            timeout=60,
        )

        job_tdc_name = create_name("glue", "tdc")
        glue.CfnJob(
            self,
            "GlueTdc",
            name=job_tdc_name,
            role=job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/base/etl_tdc.py",
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=3),
            default_arguments={
            },
            glue_version="5.0",
            worker_type="G.1X",
            number_of_workers=10,
            max_retries=0,
            timeout=60,
        )

        # --- Exposed attributes ---
        self.sns_failure_topic = sns_failure_topic
        self.preupdate_lambda = preupdate_lambda
        self.postupdate_lambda = postupdate_lambda
        self.error_lambda = error_lambda
        self.sql_runner_lambda = sql_runner_lambda
        self.read_metrics_lambda = read_metrics_lambda
        self.get_active_tables_lambda = get_active_tables_lambda
        self.get_origin_params_lambda = get_origin_params_lambda
        self.job_raw_to_master_name = job_raw_to_master_name
        self.job_master_to_analytics_name = job_master_to_analytics_name
        self.job_extract_copy_name = job_extract_copy_name
        self.job_tdc_name = job_tdc_name
