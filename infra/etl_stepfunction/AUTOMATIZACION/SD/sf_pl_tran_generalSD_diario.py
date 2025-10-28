from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_iam as iam,
)
from aws_cdk.aws_lambda import IFunction
from aws_cdk.aws_s3 import Bucket

from ....utils.naming import create_name


@dataclass
class EtlSfGenetalSDDiarioConstructProps:
    environment: str
    region: str
    autom_lambda: IFunction
    job_load_sd: str
    raw_bucket: Bucket
    master_bucket: Bucket
    raw_database: str
    master_database: str
    sns_topic_email_addresses: list[str]


class EtlSfGenetalSDDiarioConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlSfGenetalSDDiarioConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        region = props.region
        autom_lambda = props.autom_lambda
        job_load_sd = props.job_load_sd
        raw_bucket = props.raw_bucket
        master_bucket = props.master_bucket
        raw_database = props.raw_database
        master_database = props.master_database
        sns_topic_email_addresses = props.sns_topic_email_addresses


        # 1) RunGlueLoadSD (GlueStartJobRun) - arguments read from $.config.*
        run_glue_load_sd = tasks.GlueStartJobRun(
            self,
            "RunGlueLoadSD",
            glue_job_name=job_load_sd,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--bronze_db.$": "$.config.bronze_db", #  raw_database, #
                "--s3_gold_root.$": "$.config.s3_gold_root",
                "--redshift_write_mode.$": "$.config.redshift_write_mode",
                "--redshift_tmp_dir.$": "$.config.redshift_tmp_dir",
                "--redshift_db.$": "$.config.redshift_db",
                "--redshift_workgroup.$": "$.config.redshift_workgroup",
                "--redshift_secret_arn.$": "$.config.redshift_secret_arn",
                "--redshift_iam_role_arn.$": "$.config.redshift_iam_role_arn",
            }),
            result_path="$.glue",
        )
        # Retry (IntervalSeconds: 30, MaxAttempts: 2, BackoffRate: 1)
        run_glue_load_sd.add_retry(max_attempts=2, interval=Duration.seconds(30), backoff_rate=1)

        # 2) NotifyGoldFail (SNS Publish) - topic passed dynamically in input ($.config.snsTopicArn)
        notify_gold_fail = tasks.CallAwsService(
            self,
            "NotifyGoldFail",
            service="sns",
            action="publish",
            parameters={
                "TopicArn.$": "$.config.snsTopicArn",
                "Message.$": "States.Format('Pipeline: {} | Paso: load_synapse_SD (Glue) | Error: {} | ExecId: {}', $$.StateMachine.Name, $.error.Cause, $$.Execution.Id)",
                "Subject": "Falla GOLD - Pl_Tran_GeneralSD_Diario",
            },
            iam_resources=["*"],  # using wildcard because TopicArn is dynamic; tighten this if possible
        )

        # 3) ExecUspAutomatizacion (LambdaInvoke) - call the lambda to execute the stored proc
        exec_usp_automatizacion = tasks.LambdaInvoke(
            self,
            "ExecUspAutomatizacion",
            lambda_function=autom_lambda,
            payload=sfn.TaskInput.from_object({
                "database.$": "$.config.redshift_db",
                "workgroup.$": "$.config.redshift_workgroup",
                "secretArn.$": "$.config.redshift_secret_arn",
                "sql": "CALL sd.uspLoad_Automatizacion();",
                "withResult": False,
                "statementName": "uspLoad_Automatizacion",
            }),
            result_path="$.sp",
        )
        exec_usp_automatizacion.add_retry(max_attempts=2, interval=Duration.seconds(30), backoff_rate=1)

        # 4) NotifyDimFail (SNS Publish) - called if ExecUspAutomatizacion fails
        notify_dim_fail = tasks.CallAwsService(
            self,
            "NotifyDimFail",
            service="sns",
            action="publish",
            parameters={
                "TopicArn.$": "$.config.snsTopicArn",
                "Message.$": "States.Format('Pipeline: {} | Paso: uspLoad_Automatizacion | Error: {} | ExecId: {}', $$.StateMachine.Name, $.error.Cause, $$.Execution.Id)",
                "Subject": "Falla DIM - Pl_Tran_GeneralSD_Diario",
            },
            iam_resources=["*"],
        )

        # --- Chain & Catch/Next wiring ---
        # Glue catch -> NotifyGoldFail (which ends)
        run_glue_load_sd.add_catch(
            handler=notify_gold_fail.next(sfn.Fail(self, "GlueJobFailed")),
            errors=["States.ALL"],
            result_path="$.error",
        )

        # Lambda catch -> NotifyDimFail (which ends)
        exec_usp_automatizacion.add_catch(
            handler=notify_dim_fail.next(sfn.Fail(self, "StoredProcFailed")),
            errors=["States.ALL"],
            result_path="$.error",
        )

        # Successful flow: RunGlueLoadSD -> ExecUspAutomatizacion -> Success
        main_flow = sfn.Chain.start(run_glue_load_sd).next(exec_usp_automatizacion)

        success_state = sfn.Succeed(self, "Success")

        # The top-level chain ends with success (the failure paths already route to notifications + Fail)
        definition = main_flow.next(success_state)

        # --- Logs ---
        log_group = logs.LogGroup(
            self,
            "StateMachineExecutionLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        # --- State Machine ---
        self.state_machine = sfn.StateMachine(
            self,
            "Pl_Tran_GeneralSD_Diario_StateMachine",
            state_machine_name=create_name('sfn', 'pl-tran-general-sd-diario'),
            definition=definition,
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.FATAL,
                include_execution_data=True,
            ),
        )

        # --- Permissions for the state machine role ---
        # Allow starting Glue jobs (GlueStartJobRun)
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=["*"],  # tighten if you can construct specific Glue job ARNs
            )
        )

        # Allow SNS Publish (CallAwsService uses IAM; give region-wide topic publish)
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=[f"arn:aws:sns:{region}:*:*"],
            )
        )

        # Optional: create a local SNS topic with email subscriptions if you want a topic managed by CDK as well.
        if sns_topic_email_addresses:
            self.failure_topic = sns.Topic(
                self,
                "LocalStepFunctionFailureTopic",
                topic_name=create_name('sns-topic', 'SD-failure'),
            )
            for email in sns_topic_email_addresses:
                self.failure_topic.add_subscription(subs.EmailSubscription(email))

            # grant publish to the state machine role for the local topic too
            self.state_machine.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["sns:Publish"],
                    resources=[self.failure_topic.topic_arn],
                )
            )