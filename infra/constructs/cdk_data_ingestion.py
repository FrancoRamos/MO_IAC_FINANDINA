# infra/constructs/data_ingestion_construct.py
import os
from constructs import Construct
from aws_cdk import (
    Aws,
    Duration,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda_event_sources as lambda_sources,
    aws_logs as logs,
    aws_iam as iam,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
)
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_dynamodb import Table
from aws_cdk.aws_kms import Key
from aws_cdk.aws_iam import Role
from dataclasses import dataclass

from ..utils.environments import environments as Environment
from ..utils.naming import create_name


# === Props ===
@dataclass
class DataIngestionConstructProps:
    context_env: dict
    kms_key: Key
    lambda_execution_role: Role
    metadata_table: Table
    raw_bucket: Bucket
    landing_bucket: Bucket
    analytics_bucket: Bucket


# === Construct ===
class DataIngestionConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: DataIngestionConstructProps):
        super().__init__(scope, id)

        context_env = props.context_env
        kms_key = props.kms_key
        lambda_execution_role = props.lambda_execution_role
        metadata_table = props.metadata_table
        raw_bucket = props.raw_bucket
        landing_bucket = props.landing_bucket
        analytics_bucket = props.analytics_bucket

        region = context_env.region
        account_id = context_env.accountId
        environment = context_env.environment

        # --- IAM log permissions ---
        lambda_execution_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=[
                    f"arn:aws:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/*"
                ],
            )
        )

        # --- Queues ---
        dlq = sqs.Queue(
            self,
            "DeadLetterQueueCatalog",
            queue_name=create_name("sqs", "catalog-dlq"),
            removal_policy=RemovalPolicy.DESTROY,
            retention_period=Duration.days(14),
            encryption_master_key=kms_key,
        )

        raw_queue = sqs.Queue(
            self,
            "QueueRawCatalog",
            queue_name=create_name("sqs", "catalog-raw"),
            dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=1),
            encryption_master_key=kms_key,
            visibility_timeout=Duration.seconds(120),
        )

        landing_queue = sqs.Queue(
            self,
            "QueueLandingCatalog",
            queue_name=create_name("sqs", "catalog-landing"),
            dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=1),
            encryption_master_key=kms_key,
            visibility_timeout=Duration.seconds(120),
        )

        # --- EventBridge Rules ---
        self.setup_eventbridge_rules(
            raw_bucket, landing_bucket, raw_queue, landing_queue
        )

        # --- Queue policies ---
        for q in [raw_queue, landing_queue]:
            q.add_to_resource_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    principals=[iam.ServicePrincipal("events.amazonaws.com")],
                    actions=["SQS:SendMessage"],
                    resources=[q.queue_arn],
                )
            )

        # --- Lambda Layers ---
        self.datalake_layer = _lambda.LayerVersion(
            self,
            "DatalakeLibraryLayer",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../lambda-layers")
            ),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Layer with datalake_library code for all SDLF Lambdas",
            layer_version_name=create_name("lambda", "datalake-layer"),
        )

        wr_arn = f"arn:aws:lambda:{region}:336392948345:layer:AWSSDKPandas-Python312:18"
        aws_sdk_pandas = _lambda.LayerVersion.from_layer_version_arn(
            self, "AwsSdkPandasPy312", wr_arn
        )

        ijson_layer = _lambda.LayerVersion(
            self,
            "IjsonLayer",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../layers/ijson-layer.zip")
            ),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="ijson for Python 3.12 (AL2023)",
        )

        # --- Lambda: catalog-raw ---
        catalog_lambda_raw = _lambda.Function(
            self,
            "LambdaCatalogRaw",
            function_name=create_name("lambda", "catalog-raw"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(__file__), "../scripts/lambda/catalog/src")
            ),
            layers=[self.datalake_layer],
            environment={"OBJECTMETADATA_TABLE": metadata_table.table_name},
            role=lambda_execution_role,
            timeout=Duration.seconds(60),
            memory_size=256,
            environment_encryption=kms_key,
        )
        catalog_lambda_raw.add_event_source(
            lambda_sources.SqsEventSource(raw_queue, batch_size=10)
        )

        # --- Lambda: catalog-landing ---
        catalog_lambda_landing = _lambda.Function(
            self,
            "LambdaCatalogLanding",
            function_name=create_name("lambda", "catalog-landing"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(
                    os.path.dirname(__file__), "../scripts/lambda/catalog-redrive/src"
                )
            ),
            layers=[self.datalake_layer, aws_sdk_pandas, ijson_layer],
            environment={
                "QUEUE": landing_queue.queue_name,
                "DLQ": dlq.queue_name,
            },
            role=lambda_execution_role,
            timeout=Duration.seconds(90),
            memory_size=256,
            environment_encryption=kms_key,
        )
        catalog_lambda_landing.add_event_source(
            lambda_sources.SqsEventSource(landing_queue, batch_size=10)
        )

        # --- Log Groups ---
        # logs.LogGroup(
        #     self,
        #     "LambdaCatalogRawLogGroup",
        #     log_group_name=f"/aws/lambda/{catalog_lambda_raw.function_name}",
        #     removal_policy=RemovalPolicy.DESTROY,
        #     retention=logs.RetentionDays.ONE_MONTH,
        #     encryption_key=kms_key,
        # )

        # logs.LogGroup(
        #     self,
        #     "LambdaCatalogRedriveLogGroup",
        #     log_group_name=f"/aws/lambda/{catalog_lambda_landing.function_name}",
        #     removal_policy=RemovalPolicy.DESTROY,
        #     retention=logs.RetentionDays.ONE_MONTH,
        #     encryption_key=kms_key,
        # )

        # --- Scheduled Step Function Trigger ---
        self.setup_eventbridge_scheduler(context_env)

    # === Private Methods ===

    def setup_eventbridge_rules(
        self,
        raw_bucket: Bucket,
        landing_bucket: Bucket,
        raw_queue: sqs.Queue,
        landing_queue: sqs.Queue,
    ):
        # Raw bucket rules
        raw_patterns = [
            {"id": "RawParquet", "func": "raw-parquet", "pattern": "onpremise/core/*.parquet"},
            {"id": "RawCsv", "func": "raw-csv", "pattern": "onpremise/core/*.csv"},
        ]
        for cfg in raw_patterns:
            events.Rule(
                self,
                cfg["id"],
                rule_name=create_name("events", f"catalog-{cfg['func']}"),
                event_pattern=events.EventPattern(
                    source=["aws.s3"],
                    detail_type=["Object Created"],
                    detail={
                        "bucket": {"name": [raw_bucket.bucket_name]},
                        "object": {"key": [{"wildcard": cfg["pattern"]}]},
                    },
                ),
                targets=[targets.SqsQueue(raw_queue)],
            )

        # Landing bucket rules
        landing_patterns = [
            {"id": "LandingBusXml", "func": "landing-bus-xml", "pattern": "onpremise/bus/*.xml"},
            {"id": "LandingExtXml", "func": "landing-ext-xml", "pattern": "onpremise/externos/*.xml"},
            {"id": "LandingExtJson", "func": "landing-ext-json", "pattern": "onpremise/externos/*.json"},
        ]
        for cfg in landing_patterns:
            events.Rule(
                self,
                cfg["id"],
                rule_name=create_name("events", f"catalog-{cfg['func']}"),
                event_pattern=events.EventPattern(
                    source=["aws.s3"],
                    detail_type=["Object Created"],
                    detail={
                        "bucket": {"name": [landing_bucket.bucket_name]},
                        "object": {"key": [{"wildcard": cfg["pattern"]}]},
                    },
                ),
                targets=[targets.SqsQueue(landing_queue)],
            )

    def setup_eventbridge_scheduler(self, context_env):
        region = context_env.region
        account_id = context_env.accountId
        environment = context_env.environment
        state_machine_name = f"dl-general-cet-diario-{environment}-sfn-{region}"
        state_machine_arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{state_machine_name}"

        state_machine = sfn.StateMachine.from_state_machine_arn(
            self, "ScheduledStateMachine", state_machine_arn
        )

        events.Rule(
            self,
            "DailyTrigger",
            rule_name=create_name("events", "dl-daily-trigger-sm-CET"),
            description="Triggers the Step Function daily at 05:30 AM",
            schedule=events.Schedule.cron(minute="30", hour="5"),
            targets=[targets.SfnStateMachine(state_machine)],
        )
