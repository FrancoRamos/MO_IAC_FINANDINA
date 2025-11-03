from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_sns as sns,
    aws_iam as iam,
)
from aws_cdk import aws_lambda as _lambda
from .....utils.naming import create_name

@dataclass
class EtlSfAdlsBronzeSfcConstructProps:
    environment: str
    job_bronze_sfc_name: str
    failure_topic: sns.ITopic


class EtlSfAdlsBronzeSfcConstruct(Construct):
    def __init__(self, scope: Construct, id: str, props: EtlSfAdlsBronzeSfcConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        job_bronze_sfc_name = props.job_bronze_sfc_name
        failure_topic = props.failure_topic
        
        # --- Step: Pass (add executionId)
        pass_state = sfn.Pass(
            self,
            "Pass",
            parameters={
                "originalInput.$": "$",
                "executionId.$": "$$.Execution.Id"
            },
            result_path="$.payloadWithExecutionId",
        )

        # --- Step: Glue StartJobRun
        glue_start_job = tasks.GlueStartJobRun(
            self,
            "StartGlueBronzeSFC",
            glue_job_name=job_bronze_sfc_name,
            arguments=sfn.TaskInput.from_object({
                "--ENVIRONMENT": environment,
                "--Payload.$": "States.JsonToString($)"
            }),
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        sfn_name = create_name('sfn', 'adls-bronze-sfc')

        # --- Step: SNS Publish (notification)
        sns_publish = tasks.SnsPublish(
            self,
            "SNS Publish",
            topic=failure_topic,
            message=sfn.TaskInput.from_object({
                "Subject": f"[ALERTA] Fallo en el flujo de Step Function: {sfn_name}",
                "Message.$": "States.Format('El flujo de Step Function ha fallado. Detalles del error: {}', $.ErrorInfo)"
            }),
        )

        # --- Chain definition
        definition = (
            pass_state
            .next(glue_start_job)
            .next(sns_publish)
        )

        # --- Logs (optional) ---
        log_group = logs.LogGroup(
            self,
            "BronzeSFCLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        # --- State Machine definition
        self.state_machine = sfn.StateMachine(
            self,
            "BronzeSFC_StateMachine",
            state_machine_name= sfn_name,
            definition=definition,
            timeout=Duration.seconds(7200),
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