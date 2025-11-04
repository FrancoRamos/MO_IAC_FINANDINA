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
from aws_cdk.aws_sns import ITopic
from ....utils.naming import create_name


@dataclass
class EtlSfCargaSFCConstructProps:
    environment: str
    region: str
    dim_lambda: IFunction
    facts_lambda: IFunction
    job_load_sd: str
    raw_bucket: Bucket
    master_bucket: Bucket
    raw_database: str
    master_database: str
    failure_topic: ITopic


class EtlSfCargaSFCConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlSfCargaSFCConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        region = props.region
        dim_lambda = props.dim_lambda
        facts_lambda = props.facts_lambda
        job_load_sd = props.job_load_sd
        raw_bucket = props.raw_bucket
        master_bucket = props.master_bucket
        raw_database = props.raw_database
        master_database = props.master_database
        failure_topic = props.failure_topic


        # 1) RunGlueLoadSD (GlueStartJobRun) - arguments read from $.config.*
        run_glue_load_sd = tasks.GlueStartJobRun(
            self,
            "RunGlueLoadSD",
            glue_job_name=job_load_sd,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path="$.glue",
        )
        # Retry (IntervalSeconds: 30, MaxAttempts: 2, BackoffRate: 1)
        run_glue_load_sd.add_retry(max_attempts=2, interval=Duration.seconds(30), backoff_rate=1)

        # 2) NotifyGoldFail (SNS Publish) - topic passed dynamically in input ($.config.snsTopicArn)
        # no hay sns

        # 3) ExecUspAutomatizacion (LambdaInvoke) - call the lambda to execute the stored proc
        exec_usp_Load_Dim = tasks.LambdaInvoke(
            self,
            "uspLoad_Dim",
            lambda_function=dim_lambda,
            payload=sfn.TaskInput.from_object({
                "{% $states.input %}",
            }),
            result_path="$.sp",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
        )
        exec_usp_Load_Dim.add_retry(max_attempts=3, interval=Duration.seconds(30), backoff_rate=1)
        
        exec_usp_Load_Facts = tasks.LambdaInvoke(
            self,
            "uspLoad_Facts",
            lambda_function=facts_lambda,
            payload=sfn.TaskInput.from_object({
                "{% $states.input %}",
            }),
            result_path="$.sp",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
        )
        exec_usp_Load_Facts.add_retry(max_attempts=3, interval=Duration.seconds(30), backoff_rate=1)

        # # 4) NotifyDimFail (SNS Publish) - called if ExecUspAutomatizacion fails
        # notify_dim_fail = tasks.CallAwsService(
        #     self,
        #     "NotifyDimFail",
        #     service="sns",
        #     action="publish",
        #     parameters={
        #         "TopicArn.$": "$.config.snsTopicArn",
        #         "Message.$": "States.Format('Pipeline: {} | Paso: uspLoad_Automatizacion | Error: {} | ExecId: {}', $$.StateMachine.Name, $.error.Cause, $$.Execution.Id)",
        #         "Subject": "Falla DIM - Pl_Tran_GeneralSD_Diario",
        #     },
        #     iam_resources=["*"],
        # )

        # # --- Chain & Catch/Next wiring ---
        # # Glue catch -> NotifyGoldFail (which ends)
        # run_glue_load_sd.add_catch(
        #     handler=notify_gold_fail,
        #     errors=["States.ALL"],
        #     result_path="$.error",
        # )

        # # Lambda catch -> NotifyDimFail (which ends)
        # exec_usp_automatizacion.add_catch(
        #     handler=notify_dim_fail,
        #     errors=["States.ALL"],
        #     result_path="$.error",
        # )

        # Successful flow: RunGlueLoadSD -> ExecUspAutomatizacion -> Success
        # main_flow = sfn.Chain.start(run_glue_load_sd).next(exec_usp_automatizacion)

        # success_state = sfn.Succeed(self, "Success")

        # # The top-level chain ends with success (the failure paths already route to notifications + Fail)
        # definition = main_flow.next(success_state)

        # # --- Logs ---
        # log_group = logs.LogGroup(
        #     self,
        #     "StateMachineExecutionLogGroup",
        #     retention=logs.RetentionDays.ONE_WEEK,
        # )

        # --- State Machine ---
        self.state_machine = sfn.StateMachine(
            self,
            "Pl_Tran_CargaSFC_StateMachine_",
            state_machine_name=create_name('sfn', 'pl-tran-carga-sfc'),
        )

        # --- Permissions for the state machine role ---
        # Allow starting Glue jobs (GlueStartJobRun)
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=["*"],
            )
        )

        # grant publish to the state machine role for the local topic too
        # self.state_machine.add_to_role_policy(
        #     iam.PolicyStatement(
        #         actions=["sns:Publish"],
        #         resources=[failure_topic.topic_arn],
        #     )
        # )