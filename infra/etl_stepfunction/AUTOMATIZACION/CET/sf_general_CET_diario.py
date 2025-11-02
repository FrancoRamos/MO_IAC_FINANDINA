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
from aws_cdk.aws_sns import ITopic
from ....utils.naming import create_name


@dataclass
class EtlSfGeneralCETDiarioConstructProps:
    environment: str
    region: str
    pre_update_lambda: IFunction
    post_update_lambda: IFunction
    error_lambda: IFunction
    job_raw_cet: str
    job_raw_to_master_tra_cet: str
    job_master_to_analytics_load_cet: str
    bucket_result_rma01: str
    bucket_result_maa01: str
    failure_topic: ITopic


class EtlSfGeneralCETDiarioConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlSfGeneralCETDiarioConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        region = props.region
        pre_update_lambda = props.pre_update_lambda
        post_update_lambda = props.post_update_lambda
        error_lambda = props.error_lambda
        job_raw_cet = props.job_raw_cet
        job_raw_to_master_tra_cet = props.job_raw_to_master_tra_cet
        job_master_to_analytics_load_cet = props.job_master_to_analytics_load_cet
        bucket_result_rma01 = props.bucket_result_rma01
        bucket_result_maa01 = props.bucket_result_maa01
        failure_topic = props.failure_topic

        # --- Step Function Tasks ---
        pre_update_task = tasks.LambdaInvoke(
            self,
            "Pre-update Comprehensive Catalogue",
            lambda_function=pre_update_lambda,
            result_path="$.EvaluateRequestOutput",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
        )

        post_update_task1 = tasks.LambdaInvoke(
            self,
            "Post-update Comprehensive Catalogue 1",
            lambda_function=post_update_lambda,
            result_path="$.EvaluateRequestOutput",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
        )

        notify_failure_task = tasks.LambdaInvoke(
            self,
            "Notify Failure",
            lambda_function=error_lambda,
            payload=sfn.TaskInput.from_object({
                "Input.$": "$",
                "ErrorInfo.$": "$.ErrorInfo",
            }),
            result_path="$.statusCode",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
        )

        run_job_task_raw_cet = tasks.GlueStartJobRun(
            self,
            "Job Raw CET",
            glue_job_name=job_raw_cet,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--ENVIRONMENT": environment,
                # "--TABLE.$": "$.EvaluateRequestOutput.table",
                # "--BUCKET_TARGET.$": "$.EvaluateRequestOutput.bucketTarget",
                # "--BUCKET_RESULT": bucket_result_rma01,
                # "--NAMEPARTITION.$": "$.EvaluateRequestOutput.namePartition",
                # "--VALUEPARTITION.$": "$.EvaluateRequestOutput.valuePartition",
                # "--COLUMNS_INFO.$": "$.EvaluateRequestOutput.columns_info",
                # "--TYPE_LOAD.$": "$.EvaluateRequestOutput.typeload",
                # "--DATABASE_ORIGIN.$": "$.EvaluateRequestOutput.databaseOrigin",
                # "--DATABASE_TARGET.$": "$.EvaluateRequestOutput.databaseTarget",
            }),
            result_path="$.RunJobTransformRMA01",
        )
        run_job_task_raw_cet.add_retry(max_attempts=1, interval=Duration.seconds(60), backoff_rate=1)

        run_job_task_raw_to_master_tra_cet = tasks.GlueStartJobRun(
            self,
            "Job Raw to Master Tra_CET",
            glue_job_name=job_raw_to_master_tra_cet,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--ENVIRONMENT": environment,
                # "--TABLE.$": "$.EvaluateRequestOutput.table",
                # "--BUCKET_TARGET.$": "$.EvaluateRequestOutput.bucketTarget",
                # "--BUCKET_RESULT": bucket_result_maa01,
                # "--NAMEPARTITION.$": "$.EvaluateRequestOutput.namePartition",
                # "--VALUEPARTITION.$": "$.EvaluateRequestOutput.valuePartition",
                # "--COLUMNS_INFO.$": "$.EvaluateRequestOutput.columns_info",
                # "--TYPE_LOAD.$": "$.EvaluateRequestOutput.typeload",
                # "--DATABASE_ORIGIN.$": "$.EvaluateRequestOutput.databaseOrigin",
                # "--DATABASE_TARGET.$": "$.EvaluateRequestOutput.databaseTarget",
            }),
            result_path="$.RunJobTransformMAA01",
        )
        run_job_task_raw_to_master_tra_cet.add_retry(max_attempts=1, interval=Duration.seconds(60), backoff_rate=1)

        run_job_task_master_to_analytics_load_cet = tasks.GlueStartJobRun(
            self,
            "Job Master to Analytics Load_CET",
            glue_job_name=job_master_to_analytics_load_cet,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--ENVIRONMENT": environment,
                # "--TABLE.$": "$.EvaluateRequestOutput.table",
                # "--BUCKET_TARGET.$": "$.EvaluateRequestOutput.bucketTarget",
                # "--BUCKET_RESULT": bucket_result_maa01,
                # "--NAMEPARTITION.$": "$.EvaluateRequestOutput.namePartition",
                # "--VALUEPARTITION.$": "$.EvaluateRequestOutput.valuePartition",
                # "--COLUMNS_INFO.$": "$.EvaluateRequestOutput.columns_info",
                # "--TYPE_LOAD.$": "$.EvaluateRequestOutput.typeload",
                # "--DATABASE_ORIGIN.$": "$.EvaluateRequestOutput.databaseOrigin",
                # "--DATABASE_TARGET.$": "$.EvaluateRequestOutput.databaseTarget",
            }),
            result_path="$.RunJobTransformMAA01",
        )
        run_job_task_master_to_analytics_load_cet.add_retry(max_attempts=1, interval=Duration.seconds(60), backoff_rate=1)

        sns_publish_task = tasks.SnsPublish(
            self,
            "Send Failure Notification",
            topic=failure_topic,
            message=sfn.TaskInput.from_object({
                "Subject": f"[ALERTA] Fallo en el flujo de Step Function: {id}",
                "Message.$": "States.Format('El flujo de Step Function ha fallado. Detalles del error: {}', $.ErrorInfo)",
            }),
        )

        # --- Step Function Definition ---
        main_flow = (
            sfn.Chain.start(run_job_task_raw_cet)
            .next(run_job_task_raw_to_master_tra_cet)
            .next(run_job_task_master_to_analytics_load_cet)
            .next(post_update_task1)
        )


        success_state = sfn.Succeed(self, "Success")

        parallel_branch = (
            sfn.Parallel(self, "Try")
            .branch(main_flow)
            .add_catch(
                notify_failure_task.next(sns_publish_task).next(sfn.Fail(self, "Failed")),
                errors=["States.ALL"],
                result_path="$.ErrorInfo",
            )
            .next(success_state)
        )

        log_group = logs.LogGroup(
            self,
            "StateMachineExecutionLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        self.state_machine = sfn.StateMachine(
            self,
            "StepFunctionGeneralCET_Diario",
            state_machine_name=create_name('sfn', 'general-cet-diario'),
            definition=parallel_branch,
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.FATAL,
                include_execution_data=True,
            ),
        )

        # --- Permissions for State Machine Role ---
        # Allow starting Glue jobs
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=["glue:StartJobRun"],
                resources=["*"],
            )
        )

        # Allow SNS Publish: specifically grant on the provided topic ARN
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=[failure_topic.topic_arn],
            )
        )