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
class EtlSfCargaPlanoDWHConstructProps:
    environment: str
    job_carga_plano_dwh_name: str
    carga_plano_dwh_lambda: _lambda.IFunction
    failure_topic: sns.ITopic


class EtlSfCargaPlanoDWHConstruct(Construct):
    def __init__(self, scope: Construct, id: str, props: EtlSfCargaPlanoDWHConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        job_name = props.job_carga_plano_dwh_name
        carga_plano_lambda = props.carga_plano_dwh_lambda
        failure_topic = props.failure_topic

        # 1) Start Glue Job (sync)
        start_glue_plano = tasks.GlueStartJobRun(
            self,
            "StartGluePlanoMayor",
            glue_job_name=job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # .sync in console -> RUN_JOB
            arguments=sfn.TaskInput.from_object({
                "--fecha.$": "$.fecha",
                "--tipoPlano.$": "$.tipoPlano",
            }),
            result_path="$.glue",
        )

        # Retry per console: IntervalSeconds 10, MaxAttempts 3, BackoffRate 2
        start_glue_plano.add_retry(
            errors=["Glue.AWSGlueException", "Glue.ThrottlingException", "States.TaskFailed"],
            interval=Duration.seconds(10),
            max_attempts=3,
            backoff_rate=2,
        )

        # 2) Exec Stored Procs -> LambdaInvoke
        sql_list = [
            "CALL cobranza.uspTL_DimProducto();",
            "CALL cobranza.uspTL_DimGeografia();",
            "CALL aux.uspTL_DimMeta();",
            "CALL aux.uspTL_DimCaracteristica();",
            "CALL aux.uspTL_DimGrupoCaracteristica();",
            "CALL cobranza.uspTL_DimAsesor();",
            "CALL cobranza.uspTL_DimCoordinador();",
            "CALL cobranza.uspTL_DimCodeudor();",
            "CALL cobranza.uspTL_DimCliente();",
            "CALL cobranza.uspTL_DimMoraArrastreSeg();",
            "CALL cobranza.uspTL_FactPlano();"
        ]

        exec_stored_procs = tasks.LambdaInvoke(
            self,
            "ExecStoredProcs",
            lambda_function=carga_plano_lambda,
            payload=sfn.TaskInput.from_object({
                "workgroup.$": "$.redshift.workgroup",
                "database.$": "$.redshift.database",
                "secretArn.$": "$.redshift.secretArn",
                "sqlList": sql_list,
            }),
            result_path="$.sp",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
        )

        # Retry per console: IntervalSeconds 10, MaxAttempts 2, BackoffRate 2
        exec_stored_procs.add_retry(
            errors=[
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
                "States.TaskFailed",
            ],
            interval=Duration.seconds(10),
            max_attempts=2,
            backoff_rate=2,
        )

        # 3) NotifyOnError -> Publish to SNS (uses CallAwsService so we can use Message.$ / States.Format)
        notify_on_error = tasks.CallAwsService(
            self,
            "NotifyOnError",
            service="sns",
            action="publish",
            parameters={
                # Use explicit TopicArn from the provided failure_topic
                "TopicArn": failure_topic.topic_arn,
                "Subject": "Falla en pipeline RunCargaPlanoMayor",
                # dynamic message using States.Format and JsonToString($.error)
                "Message.$": "States.Format('Fallo en RunCargaPlanoMayor. Error: {}', States.JsonToString($.error))",
            },
            iam_resources=["*"],  # wildcard because CallAwsService sometimes needs flexible resources; tighten if desired
            result_path=None,
        )

        # --- Catches wiring ---
        failed_state = sfn.Fail(self, "Failed")
        # Link NotifyOnError -> Failed (only once)
        notify_on_error.next(failed_state)
        
        # Glue catch -> NotifyOnError -> Fail
        start_glue_plano.add_catch(
            handler=notify_on_error,
            errors=["States.ALL"],
            result_path="$.error",
        )

        # Lambda catch -> NotifyOnError -> Fail
        exec_stored_procs.add_catch(
            handler=notify_on_error,
            errors=["States.ALL"],
            result_path="$.error",
        )

        # --- Chain ---
        success_state = sfn.Succeed(self, "Success")

        main_flow = sfn.Chain.start(start_glue_plano).next(exec_stored_procs).next(success_state)

        # If NotifyOnError runs it already points to a Fail via the catches above.
        definition = main_flow

        # --- Logs (optional) ---
        log_group = logs.LogGroup(
            self,
            "PlReasoningLogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
        )

        # --- State Machine ---
        # TimeoutSeconds = 7200 from the console definition
        self.state_machine = sfn.StateMachine(
            self,
            "RunPlanoMayor_StateMachine",
            state_machine_name= create_name('sfn', 'run-carga-plano-dwh'),
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