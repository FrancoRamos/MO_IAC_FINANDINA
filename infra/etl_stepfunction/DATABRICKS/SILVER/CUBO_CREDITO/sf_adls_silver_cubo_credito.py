from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Stack,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_sns as sns,
    aws_iam as iam,
)
from aws_cdk import Duration
from aws_cdk import aws_lambda as _lambda
from .....utils.naming import create_name


@dataclass
class EtlSfAdlsSilverCuboCreditoConstructProps:
    environment: str
    error_fn: _lambda.IFunction
    glue_silver_cubocredito_name: str
    glue_silver_reportecubo_name: str
    failure_topic: sns.ITopic


class EtlSfAdlsSilverCuboCreditoConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlSfAdlsSilverCuboCreditoConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        error_fn = props.error_fn
        glue_silver_cubocredito_name = props.glue_silver_cubocredito_name
        glue_silver_reportecubo_name = props.glue_silver_reportecubo_name
        failure_topic = props.failure_topic
        
        
        # 1) Pass state that adds executionId and original input -> payloadWithExecutionId
        pass_state = sfn.Pass(
            self,
            "Pass",
            parameters={
                "originalInput.$": "$",
                "executionId.$": "$$.Execution.Id",
            },
            result_path="$.payloadWithExecutionId",
        )

        # 2) Branch Glue cubocredito_traCCDC -> Glue reporteCubo (both sync)
        glue_cubocredito = tasks.GlueStartJobRun(
            self,
            "Glue cubocredito_traCCDC",
            glue_job_name=glue_silver_cubocredito_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--ENVIRONMENT": environment,
                "--Payload.$": "States.JsonToString($)",
            }),
            result_path=None,
        )

        glue_reportecubo = tasks.GlueStartJobRun(
            self,
            "Glue reporteCubo",
            glue_job_name=glue_silver_reportecubo_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--ENVIRONMENT": environment,
                "--Payload.$": "States.JsonToString($)",
            }),
            result_path=None,
        )

        branch_chain = sfn.Chain.start(glue_cubocredito).next(glue_reportecubo)

        # 3) Parallel state (single branch as per console)
        parallel = sfn.Parallel(self, "Parallel", result_path=None)
        parallel.branch(branch_chain)

        # Add catch on Parallel to notify failure (States.ALL -> Notify Failure)
        # 4) Notify Failure -> Lambda invoke (with retry) -> Send Failure Notification (SNS publish) -> Fail
        notify_failure_lambda = tasks.LambdaInvoke(
            self,
            "Notify Failure",
            lambda_function=error_fn,
            payload=sfn.TaskInput.from_object({
                "Input.$": "$",
                "ErrorInfo.$": "$.ErrorInfo",
            }),
            result_path="$.statusCode",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # Retry on the Lambda invoke (same as console)
        notify_failure_lambda.add_retry(
            errors=[
                "Lambda.ClientExecutionTimeoutException",
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
            ],
            interval=Duration.seconds(2),
            max_attempts=6,
            backoff_rate=2,
        )

        sfn_name = create_name('sfn', 'adls-silver-cubo-credito')

        # 5) Send Failure Notification -> SNS publish (use CallAwsService to allow Message.$ structure)
        send_failure_notification = tasks.SnsPublish(
            self,
            "Send Failure Notification",
            topic=failure_topic,
            message=sfn.TaskInput.from_object({
                "Subject": f"[ALERTA] Fallo en el flujo de Step Function: {sfn_name}",
                "Message.$": "States.Format('El flujo de Step Function ha fallado. Detalles del error: {}', $.ErrorInfo)"
            }),
            result_path=None,
        )

        # 6) Failed state
        failed_state = sfn.Fail(self, "Failed")

        # Wire notify lambda -> send notification -> failed
        notify_failure_lambda.next(send_failure_notification)
        send_failure_notification.next(failed_state)

        # Now add the Parallel catch that points to notify_failure_lambda
        parallel.add_catch(
            handler=notify_failure_lambda,
            errors=["States.ALL"],
            result_path="$.ErrorInfo",
        )

        # 7) Success state
        success_state = sfn.Succeed(self, "Success")

        # Main flow: Pass -> Parallel -> Success
        definition = sfn.Chain.start(pass_state).next(parallel).next(success_state)

        # --- Logs (optional, similar to previous example) ---
        log_group = logs.LogGroup(
            self,
            "SilverCuboCreditoLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        # --- State Machine ---
        self.state_machine = sfn.StateMachine(
            self,
            "SilverCuboCredito_StateMachine",
            state_machine_name=sfn_name,
            definition=definition,
            timeout=Duration.hours(2),
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

        # Allow SNS Publish on the failure topic
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=[failure_topic.topic_arn],
            )
        )