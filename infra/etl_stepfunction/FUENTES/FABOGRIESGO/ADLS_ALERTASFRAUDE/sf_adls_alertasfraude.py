from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Stack,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_logs as logs,
    aws_sns as sns,
)
from aws_cdk import Duration
from aws_cdk import aws_lambda as _lambda
from .....utils.naming import create_name


@dataclass
class EtlSfAdlsFaboAlertasFraudeConstructProps:
    environment: str
    # Lambda functions
    get_active_tables_fn: _lambda.IFunction  
    get_origin_params_fn: _lambda.IFunction  
    sql_runner_fn: _lambda.IFunction        
    read_metrics_fn: _lambda.IFunction      

    # Glue job names (strings)
    glue_extractcopy_job_name: str
    glue_tdc_job_name: str

    # SNS topic to publish failures
    failure_topic: sns.ITopic

    # runtime knobs
    max_map_concurrency: int = 10
    map_items_path: str = "$.active.sfn_view"
    exec_start_path: str = "$.execStart"


class EtlSfAdlsFaboAlertasFraudeConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlSfAdlsFaboAlertasFraudeConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment

        # ----- Log Group for the State Machine -----
        log_group = logs.LogGroup(
            self,
            "SfAlertasFraudeLogs",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # ----- Task: GetActiveTables (Lambda invoke) -----
        get_active_tables = tasks.LambdaInvoke(
            self,
            "GetActiveTables",
            lambda_function=props.get_active_tables_fn,
            payload_response_only=False,
            result_selector={
                "count.$": "$.Payload.count",
                "sfn_view.$": "$.Payload.sfn_view",
                "tables.$": "$.Payload.tables",
            },
            result_path="$.active",
            payload=sfn.TaskInput.from_object({"Payload.$": "$"}),
        )

        # Add retries and catch to mirror console JSON
        get_active_tables.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=3,
            errors=[
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
            ],
        )
        # Global failure -> Publish to SNS
        publish_global_fail = sfn.CustomState(
            self,
            "NotifyGlobalFail",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:sns:publish",
                "Parameters": {
                    "TopicArn": props.failure_topic.topic_arn,
                    "Subject": "Pipeline FALLÓ al iniciar",
                    "Message.$": "States.Format('Fallo al obtener tablas activas. ExecId={}', $$.Execution.Id)",
                },
                "End": True,
            },
        )

        get_active_tables.add_catch(publish_global_fail, result_path="$.error")

        # ----- Choice: HasTables -----
        has_tables = sfn.Choice(self, "HasTables")
        end_no_tables = sfn.Succeed(self, "EndNoTables")

        # ----- Map State: IngestMap -----
        ingest_map = sfn.Map(
            self,
            "IngestMap",
            items_path=props.map_items_path,
            max_concurrency=props.max_map_concurrency,
            parameters={
                "table.$": "$$.Map.Item.Value",
                "execStart.$": "$$.Execution.StartTime",
            },
            result_path=sfn.JsonPath.DISCARD,
        )

        # --- Iterator states inside Map ---
        # GetOriginParams (lambda)
        get_origin_params = tasks.LambdaInvoke(
            self,
            "GetOriginParams",
            lambda_function=props.get_origin_params_fn,
            payload_response_only=True,
            result_path="$.origin",
            payload=sfn.TaskInput.from_object({"idConfigOrigen.$": "$.table.idConfigOrigen"}),
        )

        # Choice: IsIncremental (depends on nombreCampoPivot == '-')
        is_incremental_choice = sfn.Choice(self, "IsIncremental")

        # AuditStartFull -> SQL runner to insert execution (full)
        audit_start_full = tasks.LambdaInvoke(
            self,
            "AuditStartFull",
            lambda_function=props.sql_runner_fn,
            payload_response_only=True,
            result_path="$.auditStart",
            payload=sfn.TaskInput.from_object({
                "action": "call",
                "sql": "call dbo.usp_ins_ejecucion_light_gen2(:archivoDestino,:estado,:fechaFin,:fechaInicio,:fechaInsertUpdate,:nombreTabla,:origen,:runID,:tipoCarga,:valorPivot,:sistemaFuente);",
                "params": {
                    "archivoDestino": "-",
                    "estado": 2,
                    "fechaFin": None,
                    "fechaInicio.$": "$.execStart",
                    "fechaInsertUpdate.$": "$.execStart",
                    "nombreTabla.$": "$.table.nombreTabla",
                    "origen": "sqlServer",
                    "runID.$": "$$.Execution.Id",
                    "tipoCarga": 0,
                    "valorPivot": "-",
                    "sistemaFuente.$": "$.table.nombreCarpetaDL",
                },
            }),
        )

        # GlueCopyFull
        glue_copy_full = tasks.GlueStartJobRun(
            self,
            "GlueCopyFull",
            glue_job_name=props.glue_extractcopy_job_name,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN if False else sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--mode": "full",
                "--sql_query.$": "$.table.query",
                "--pivot_type": "none",
                "--output_prefix.$": "States.Format('s3://{}/{}/{}/', $.table.nombreCarpetaDL, $.table.nombreTabla, '')",
                "--metrics_path.$": "States.Format('s3://finandina-dev-temp/metrics/{}/{}/', $.table.nombreCarpetaDL, $.table.nombreTabla)",
                "--archivo_nombre.$": "States.Format('{}_' , States.ArrayGetItem(States.StringSplit($.execStart,'.'),0))",
            }),
            result_path="$.glue",
        )

        # Add retry behavior to GlueCopyFull (mirror JSON)
        glue_copy_full.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(10),
            max_attempts=3,
            errors=["Glue.AWSGlueException", "Glue.ThrottlingException", "States.TaskFailed"],
        )

        # AuditStartIncr (similar to full but tipoCarga=1)
        audit_start_incr = tasks.LambdaInvoke(
            self,
            "AuditStartIncr",
            lambda_function=props.sql_runner_fn,
            payload_response_only=True,
            result_path="$.auditStart",
            payload=sfn.TaskInput.from_object({
                "action": "call",
                "sql": "call dbo.usp_ins_ejecucion_light_gen2(:archivoDestino,:estado,:fechaFin,:fechaInsertUpdate,:nombreTabla,:origen,:runID,:tipoCarga,:valorPivot,:sistemaFuente);",
                "params": {
                    "archivoDestino": "-",
                    "estado": 2,
                    "fechaFin": None,
                    "fechaInicio.$": "$.execStart",
                    "fechaInsertUpdate.$": "$.execStart",
                    "nombreTabla.$": "$.table.nombreTabla",
                    "origen": "sqlServer",
                    "runID.$": "$$.Execution.Id",
                    "tipoCarga": 1,
                    "valorPivot": "-",
                    "sistemaFuente.$": "$.table.nombreCarpetaDL",
                },
            }),
        )

        # BuildPivotAuditSQL (Pass state building SQL)
        build_pivot_audit_sql = sfn.Pass(
            self,
            "BuildPivotAuditSQL",
            result_path="$.pivot",
            parameters={
                "sql": sfn.JsonPath.format(
                    "SELECT valorPivot FROM dbo.controlPipelineLight_gen2 WHERE nombreTabla = '{}' AND estado = 1 ORDER BY fechaInsertUpdate DESC LIMIT 1",
                    sfn.JsonPath.string_at("$.table.nombreTabla"),
                )
            },
        )

        # GetPivotAudit (call sql_runner_fn with select_one)
        get_pivot_audit = tasks.LambdaInvoke(
            self,
            "GetPivotAudit",
            lambda_function=props.sql_runner_fn,
            payload_response_only=True,
            result_path="$.pivotAudit",
            payload=sfn.TaskInput.from_object({
                "action": "select_one",
                "sql.$": "$.pivot.sql",
            }),
        )

        get_pivot_audit.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=3,
            errors=["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
        )

        # GlueCopyIncr
        glue_copy_incr = tasks.GlueStartJobRun(
            self,
            "GlueCopyIncr",
            glue_job_name=props.glue_extractcopy_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--mode": "incr",
                "--sql_query.$": "$.table.query",
                "--pivot_from.$": "$.pivotAudit.Payload.value",
                "--pivot_type.$": "$.table.tipoCampoPivot",
                "--pivot_column.$": "$.table.nombreCampoPivot",
                "--output_prefix.$": "States.Format('s3://{}/{}/{}/', $.table.nombreCarpetaDL, $.table.nombreTabla, '')",
                "--metrics_path.$": "States.Format('s3://finandina-dev-temp/metrics/{}/{}/', $.table.nombreCarpetaDL, $.table.nombreTabla)",
                "--archivo_nombre.$": "States.Format('{}_' , States.ArrayGetItem(States.StringSplit($.execStart,'.'),0))",
            }),
            result_path="$.glue",
        )

        glue_copy_incr.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(10),
            max_attempts=3,
            errors=["Glue.AWSGlueException", "Glue.ThrottlingException", "States.TaskFailed"],
        )

        # ReadMetrics Full (lambda)
        read_metrics = tasks.LambdaInvoke(
            self,
            "ReadMetrics",
            lambda_function=props.read_metrics_fn,
            payload_response_only=True,
            result_path="$.metrics",
            payload=sfn.TaskInput.from_object({
                "metrics_path.$": sfn.JsonPath.format(
                    "s3://finandina-dev-temp/metrics/{}/{}/",
                    sfn.JsonPath.string_at("$.table.nombreCarpetaDL"),
                    sfn.JsonPath.string_at("$.table.nombreTabla"),
                )
            }),
        )

        read_metrics.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=5,
            errors=["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
        )

        # AuditSuccess (sql runner to update execution success)
        audit_success = tasks.LambdaInvoke(
            self,
            "AuditSuccess",
            lambda_function=props.sql_runner_fn,
            payload_response_only=True,
            result_path="$.auditEnd",
            payload=sfn.TaskInput.from_object({
                "action": "call",
                "sql": "call dbo.usp_upd_ejecucion_light_gen2(:archivoDestino,:estado,:fechaFin,:fechaInsertUpdate,:runID,:tablaNombre,:valorPivot,:cantidadRegistros,:sistemaFuente,:cantidadRegistrosTotales);",
                "params": {
                    "archivoDestino.$": "States.Format('/{}/{}/{}', $.table.nombreCarpetaDL, $.table.nombreTabla, $.metrics.Payload.archivo_nombre)",
                    "estado": 1,
                    "fechaFin.$": "$.execStart",
                    "fechaInsertUpdate.$": "$.execStart",
                    "runID.$": "$$.Execution.Id",
                    "tablaNombre.$": "$.table.nombreTabla",
                    "valorPivot.$": "$.metrics.Payload.pivot_to",
                    "cantidadRegistros.$": "$.metrics.Payload.rows_copied",
                    "sistemaFuente.$": "$.table.nombreCarpetaDL",
                    "cantidadRegistrosTotales.$": "$.metrics.Payload.total_source",
                },
            }),
        )

        # AuditFail parallel: UpdFail and NotifyFail
        upd_fail = tasks.LambdaInvoke(
            self,
            "UpdFail",
            lambda_function=props.sql_runner_fn,
            payload_response_only=True,
            payload=sfn.TaskInput.from_object({
                "FunctionName": props.sql_runner_fn.function_name if hasattr(props.sql_runner_fn, 'function_name') else "",
                "Payload": {
                    "action": "call",
                    "sql": "call dbo.usp_upd_ejecucion_light_gen2(:archivoDestino,:estado,:fechaFin,:fechaInsertUpdate,:runID,:tablaNombre,:valorPivot,:cantidadRegistros,:sistemaFuente,:cantidadRegistrosTotales);",
                    "params": {
                        "archivoDestino": "-",
                        "estado": 0,
                        "fechaFin.$": "$.execStart",
                        "fechaInsertUpdate.$": "$.execStart",
                        "runID.$": "$$.Execution.Id",
                        "tablaNombre.$": "$.table.nombreTabla",
                        "valorPivot": "-",
                        "cantidadRegistros": 0,
                        "sistemaFuente.$": "$.table.nombreCarpetaDL",
                        "cantidadRegistrosTotales": 0,
                    },
                },
            }),
            result_path=sfn.JsonPath.DISCARD,
        )

        notify_fail = sfn.CustomState(
            self,
            "NotifyFail",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:sns:publish",
                "Parameters": {
                    "TopicArn": props.failure_topic.topic_arn,
                    "Subject": "finandina-dev Ingesta FALLIDA",
                    "Message.$": "States.Format('Tabla {} falló. ExecId={}', $.table.nombreTabla, $$.Execution.Id)",
                },
                "End": True,
            },
        )

        audit_fail_parallel = sfn.Parallel(self, "AuditFail")
        audit_fail_parallel.branch(sfn.Chain.start(upd_fail))
        audit_fail_parallel.branch(sfn.Chain.start(notify_fail))

        # Add catches
        read_metrics.add_catch(audit_fail_parallel, errors=["States.ALL"], result_path=sfn.JsonPath.DISCARD)
        audit_success.add_catch(audit_fail_parallel, errors=["States.ALL"], result_path=sfn.JsonPath.DISCARD)

        # ----- Wire the map iterator -----
        full_chain = (
            sfn.Chain.start(audit_start_full)
            .next(glue_copy_full)
        )

        incr_chain = (
            sfn.Chain.start(audit_start_incr)
            .next(build_pivot_audit_sql)
            .next(get_pivot_audit)
            .next(glue_copy_incr)
        )

        # The IsIncremental choice branches
        is_incremental_choice = sfn.Choice(self, "IsIncrementalChoice")
        is_incremental_choice.when(
            sfn.Condition.string_equals("$.table.nombreCampoPivot", "-"),
            full_chain
        )
        is_incremental_choice.otherwise(incr_chain)

        # Después de cualquiera de las dos ramas, ejecutamos ReadMetrics -> AuditSuccess
        is_incremental_choice.afterwards().next(read_metrics).next(audit_success)

        # Assemble iterator: start at GetOriginParams -> IsIncrementalChoice
        iterator_chain = sfn.Chain.start(get_origin_params).next(is_incremental_choice)

        ingest_map.iterator(iterator_chain)

        # ----- RunTDC -----
        run_tdc = tasks.GlueStartJobRun(
            self,
            "RunTDC",
            glue_job_name=props.glue_tdc_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # Catch for RunTDC -> NotifyTDCFail
        notify_tdc_fail = sfn.CustomState(
            self,
            "NotifyTDCFail",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:sns:publish",
                "Parameters": {
                    "TopicArn": props.failure_topic.topic_arn,
                    "Subject": "TDC (Glue) FALLÓ",
                    "Message.$": "States.Format('Glue TDC falló. ExecId={}', $$.Execution.Id)",
                },
                "End": True,
            },
        )

        run_tdc.add_catch(notify_tdc_fail, result_path=sfn.JsonPath.DISCARD)

        # ----- Top-level chain -----
        has_tables.when(
            sfn.Condition.number_equals("$.active.count", 0), 
            end_no_tables
        )
        has_tables.otherwise(ingest_map.next(run_tdc))
        definition = sfn.Chain.start(get_active_tables).next(has_tables)

        # ----- State Machine -----
        self.state_machine = sfn.StateMachine(
            self,
            "TdcIngestStateMachine",
            state_machine_name=create_name('sfn', 'fuentes-adls-alertas-fraude'),
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            logs=sfn.LogOptions(destination=log_group, level=sfn.LogLevel.ALL, include_execution_data=True),
        )
