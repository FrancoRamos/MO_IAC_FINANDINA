# sf_plano_mayor.py
from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Duration,
    RemovalPolicy,
    aws_logs as logs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
)
from ....utils.naming import create_name

@dataclass
class PlanoMayorSfProps:
    """
    Props para PlanoMayorSf:
      - job_name: nombre del Glue Job
      - failure_topic: SNS topic para notificar fallas
      - state_machine_timeout: opcional, default 2h
    """

    job_name: str
    failure_topic: sns.ITopic
    state_machine_timeout: Duration | None = None


class PlanoMayorSf(Construct):
    """
    Capa de orquestación (Step Functions): define la State Machine.

    Por ahora, los pasos "SP" son stubs (Pass). 
    Cuando habiliten SPs se reemplaza cada Pass por CallAwsService (redshift-data) o LambdaInvoke.
    """

    def __init__(self, scope: Construct, id: str, props: PlanoMayorSfProps) -> None:
        super().__init__(scope, id)

        # 1) Paso Glue: ejecuta el Job de Glue
        glue_start = tasks.GlueStartJobRun(
            self,
            "load_planoMayor",
            glue_job_name=props.job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,  # espera a que termine
            timeout=Duration.hours(1),
            arguments=sfn.TaskInput.from_object(
                {
                    "--fecha": sfn.JsonPath.string_at("$.fecha"),
                    "--tipoPlano": sfn.JsonPath.string_at("$.tipoPlano"),
                }
            ),
        )

        # 2) Stub de SPs (por ahora: Pass)
        def sp(name: str) -> sfn.Pass:
            return sfn.Pass(self, name)
            # podrías agregar result o resultPath como en TS si lo deseas

        chain = (
            sfn.Chain.start(glue_start)
            .next(sp("uspTL_DimProducto"))
            .next(sp("uspTL_DimGeografia"))
            .next(sp("uspTL_DimMeta"))
            .next(sp("uspTL_DimCaracteristica"))
            .next(sp("uspTL_DimGrupoCaracteristica"))
            .next(sp("uspTL_DimAsesor"))
            .next(sp("uspTL_DimCoordinador"))
            .next(sp("uspTL_DimCodeudor"))
            .next(sp("uspTL_DimCliente"))
            .next(sp("uspTL_DimMoraArrastreSeg"))
            .next(sp("uspTL_FactPlano"))
        )

        # 3) Logs y manejo de errores
        log_group = logs.LogGroup(
            self,
            "PlanoMayorSfnLogs",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,  # demo; en prod: RETAIN
        )

        notify_on_error = tasks.SnsPublish(
            self,
            "NotifyOnError",
            topic=props.failure_topic,
            subject="Falla en pipeline RunPlanoMayor",
            message=sfn.TaskInput.from_json_path_at("$.error"),
        )

        main = sfn.Parallel(self, "MainFlow")
        main.branch(chain)

        main.add_catch(
            notify_on_error.next(sfn.Fail(self, "Failed")),
            result_path="$.error",
        )

        definition = main

        self.state_machine = sfn.StateMachine(
            self,
            "RunPlanoMayor",
            state_machine_name=create_name('sfn', 'cobranza-carga-plano-dwh'),
            definition=definition,
            timeout=props.state_machine_timeout or Duration.hours(2),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            tracing_enabled=True,
        )
