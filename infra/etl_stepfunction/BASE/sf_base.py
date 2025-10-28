# infra/etl_stepfunction/etl_base/sf_base.py
from aws_cdk import (
    Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
)
from aws_cdk.aws_lambda import IFunction
from constructs import Construct
from ...utils.naming import create_name
from dataclasses import dataclass

@dataclass
class EtlSfBaseConstructProps:
    environment: str
    region: str
    pre_update_lambda: IFunction
    post_update_lambda: IFunction
    error_lambda: IFunction
    job_raw_to_master_name: str
    job_master_to_analytics_name: str
    bucket_result_rma01: str
    bucket_result_maa01: str


class EtlSfBaseConstruct(Construct):
    def __init__(self, scope: Construct, id: str, *, props: EtlSfBaseConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        region = props.region
        pre_update_lambda = props.pre_update_lambda
        post_update_lambda = props.post_update_lambda
        error_lambda = props.error_lambda
        job_raw_to_master_name = props.job_raw_to_master_name
        job_master_to_analytics_name = props.job_master_to_analytics_name
        bucket_result_rma01 = props.bucket_result_rma01
        bucket_result_maa01 = props.bucket_result_maa01

        # --- Lambda Tasks ---
        pre_update_task = tasks.LambdaInvoke(
            self,
            "Pre-update Comprehensive Catalogue",
            lambda_function=pre_update_lambda,
            result_path="$.EvaluateRequestOutput",
        )

        pre_update_task2 = tasks.LambdaInvoke(
            self,
            "Pre-update Comprehensive Catalogue 2",
            lambda_function=pre_update_lambda,
            result_path="$.EvaluateRequestOutput",
        )

        # --- Glue Job: Raw → Master ---
        run_job_task_rma01 = tasks.GlueStartJobRun(
            self,
            "Run Job Raw to Master Automation01",
            glue_job_name=job_raw_to_master_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object(
                {
                    "--ENVIRONMENT": environment,
                    "--TABLE.$": "$.EvaluateRequestOutput.table",
                    "--BUCKET_TARGET.$": "$.EvaluateRequestOutput.bucketTarget",
                    "--BUCKET_RESULT": bucket_result_rma01,
                    "--NAMEPARTITION.$": "$.EvaluateRequestOutput.namePartition",
                    "--VALUEPARTITION.$": "$.EvaluateRequestOutput.valuePartition",
                    "--COLUMNS_INFO.$": "$.EvaluateRequestOutput.columns_info",
                    "--TYPE_LOAD.$": "$.EvaluateRequestOutput.typeload",
                    "--DATABASE_ORIGIN.$": "$.EvaluateRequestOutput.databaseOrigin",
                    "--DATABASE_TARGET.$": "$.EvaluateRequestOutput.databaseTarget",
                }
            ),
            result_path="$.RunJobTransformRMA01",
        )
        run_job_task_rma01.add_retry(max_attempts=1, interval=Duration.seconds(60), backoff_rate=1)

        # --- Glue Job: Master → Analytics ---
        run_job_task_maa01 = tasks.GlueStartJobRun(
            self,
            "Run Job Master to Analytics Automation01",
            glue_job_name=job_master_to_analytics_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object(
                {
                    "--ENVIRONMENT": environment,
                    "--TABLE.$": "$.EvaluateRequestOutput.table",
                    "--BUCKET_TARGET.$": "$.EvaluateRequestOutput.bucketTarget",
                    "--BUCKET_RESULT": bucket_result_maa01,
                    "--NAMEPARTITION.$": "$.EvaluateRequestOutput.namePartition",
                    "--VALUEPARTITION.$": "$.EvaluateRequestOutput.valuePartition",
                    "--COLUMNS_INFO.$": "$.EvaluateRequestOutput.columns_info",
                    "--TYPE_LOAD.$": "$.EvaluateRequestOutput.typeload",
                    "--DATABASE_ORIGIN.$": "$.EvaluateRequestOutput.databaseOrigin",
                    "--DATABASE_TARGET.$": "$.EvaluateRequestOutput.databaseTarget",
                }
            ),
            result_path="$.RunJobTransformMAA01",
        )
        run_job_task_maa01.add_retry(max_attempts=1, interval=Duration.seconds(60), backoff_rate=1)

        # --- Post Update Lambdas ---
        post_update_task1 = tasks.LambdaInvoke(
            self,
            "Post-update Comprehensive Catalogue 1",
            lambda_function=post_update_lambda,
            result_path="$.EvaluateRequestOutput",
        )

        post_update_task2 = tasks.LambdaInvoke(
            self,
            "Post-update Comprehensive Catalogue 2",
            lambda_function=post_update_lambda,
            result_path="$.EvaluateRequestOutput",
        )

        # --- Error Handling Lambda ---
        notify_failure_task = tasks.LambdaInvoke(
            self,
            "Notify Failure",
            lambda_function=error_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "Input.$": "$",
                    "ErrorInfo.$": "$.ErrorInfo",
                }
            ),
            result_path="$.statusCode",
        )

        # --- Main Flow ---
        main_flow = (
            sfn.Chain.start(pre_update_task)
            .next(run_job_task_rma01)
            .next(post_update_task1)
            .next(pre_update_task2)
            .next(run_job_task_maa01)
            .next(post_update_task2)
        )

        success_state = sfn.Succeed(self, "Success")

        # --- Parallel Branch with Catch ---
        parallel_branch = sfn.Parallel(self, "Try").branch(main_flow)
        parallel_branch.add_catch(
            notify_failure_task.next(sfn.Fail(self, "Failed")),
            errors=["States.ALL"],
            result_path="$.ErrorInfo",
        )
        parallel_branch.next(success_state)

        # --- Logging ---
        log_group = logs.LogGroup(
            self,
            "StateMachineExecutionLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        # --- State Machine ---
        self.state_machine = sfn.StateMachine(
            self,
            "StepFunctionAutom01",
            state_machine_name=create_name('sfn', 'autom01'),
            definition=parallel_branch,
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.FATAL,
                include_execution_data=True,
            ),
        )
