from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    aws_logs as logs,
    aws_sns as sns,
    aws_lambda as _lambda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration,
    aws_iam as iam,
)
from .....utils.naming import create_name 

@dataclass
class EtlSfAdlsFaboTdcConstructProps:
    environment: str
    get_active_tables_fn: _lambda.IFunction
    get_origin_params_fn: _lambda.IFunction
    sql_runner_fn: _lambda.IFunction
    read_metrics_fn: _lambda.IFunction
    glue_extractcopy_job_name: str
    glue_tdc_job_name: str
    failure_topic: sns.ITopic
    raw_bucket_name: str


class EtlSfAdlsFaboTdcConstruct(Construct):
    def __init__(self, scope: Construct, id: str, props: EtlSfAdlsFaboTdcConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        get_active_tables_fn = props.get_active_tables_fn
        get_origin_params_fn = props.get_origin_params_fn
        sql_runner_fn = props.sql_runner_fn
        read_metrics_fn = props.read_metrics_fn
        glue_extractcopy_job_name = props.glue_extractcopy_job_name
        glue_tdc_job_name = props.glue_tdc_job_name
        failure_topic = props.failure_topic
        raw_bucket_name = props.raw_bucket_name

        # Log group
        log_group = logs.LogGroup(
            self,
            "SfTdcLogs",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # ----- GetActiveTables (Lambda invoke) -----
        get_active_tables = tasks.LambdaInvoke(
            self,
            "GetActiveTables",
            lambda_function=get_active_tables_fn,
            # payload_response_only=False,
            result_selector={
                "count.$": "$.Payload.count",
                "sfn_view.$": "$.Payload.sfn_view",
                "tables.$": "$.Payload.tables",
            },
            result_path="$.active",
            # payload=sfn.TaskInput.from_object({"Payload.$": "$"}),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # retries and catch (mirror JSON)
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

        # NotifyGlobalFail (SNS publish) used as catch target
        notify_global_fail = sfn.CustomState(
            self,
            "NotifyGlobalFail",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:sns:publish",
                "Parameters": {
                    "TopicArn": failure_topic.topic_arn,
                    "Subject": "Pipeline FALLÓ al iniciar",
                    "Message.$": "States.Format('Fallo al obtener tablas activas. ExecId={}', $$.Execution.Id)",
                },
                "End": True,
            },
        )

        get_active_tables.add_catch(notify_global_fail)

        # ----- Choice HasTables -----
        has_tables = sfn.Choice(self, "HasTables")
        end_no_tables = sfn.Succeed(self, "EndNoTables")

        # ----- Map: IngestMap -----
        ingest_map = sfn.Map(
            self,
            "IngestMap",
            items_path= "$.active.sfn_view",
            max_concurrency=10,
            parameters={
                "table.$": "$$.Map.Item.Value",
                "execStart.$": "$$.Execution.StartTime",
            },
            result_path=sfn.JsonPath.DISCARD,
        )

        # ---- Iterator states inside Map ----
        # GetOriginParams
        get_origin_params = tasks.LambdaInvoke(
            self,
            "GetOriginParams",
            lambda_function=get_origin_params_fn,
            # payload_response_only=True,
            result_path="$.origin",
            payload=sfn.TaskInput.from_object({"idConfigOrigen.$": "$.table.idConfigOrigen"}),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # IsIncremental choice (nombreCampoPivot == "-")
        is_incremental_choice = sfn.Choice(self, "IsIncremental")

        # AuditStartIncr (call insert exec with tipoCarga=1)
        audit_start_incr = tasks.LambdaInvoke(
            self,
            "AuditStartIncr",
            lambda_function=sql_runner_fn,
            # payload_response_only=True,
            result_path="$.auditStart",
            payload=sfn.TaskInput.from_object({
                "action": "call",
                "sql": "call dbo.usp_ins_ejecucion_light_gen2(:origen::varchar,:nombreTabla::varchar,:tipoCarga::int,:valorPivot::varchar,:estado::int,:fechaInicio::timestamp,:fechaFin::timestamp,:archivoDestino::varchar,:fechaInsertUpdate::timestamp,:runID::varchar,:sistemaFuente::varchar);",
                "params": {
                    "origen": "sqlServer",
                    "nombreTabla.$": "$.table.nombreTabla",
                    "tipoCarga": "1",
                    "valorPivot": "-",
                    "estado": "2",
                    "fechaInicio.$": "$.execStart",
                    "fechaFin": "1970-01-01 00:00:00+00",
                    "archivoDestino": "-",
                    "fechaInsertUpdate.$": "$.execStart",
                    "runID.$": "$$.Execution.Id",
                    "sistemaFuente.$": "$.table.nombreCarpetaDL",
                },
            }),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # AuditStartFull (tipoCarga=0)
        audit_start_full = tasks.LambdaInvoke(
            self,
            "AuditStartFull",
            lambda_function=sql_runner_fn,
            # payload_response_only=True,
            result_path="$.auditStart",
            payload=sfn.TaskInput.from_object({
                "action": "call",
                "sql": "call dbo.usp_ins_ejecucion_light_gen2(:origen::varchar,:nombreTabla::varchar,:tipoCarga::int,:valorPivot::varchar,:estado::int,:fechaInicio::timestamp,:fechaFin::timestamp,:archivoDestino::varchar,:fechaInsertUpdate::timestamp,:runID::varchar,:sistemaFuente::varchar);",
                "params": {
                    "origen": "sqlServer",
                    "nombreTabla.$": "$.table.nombreTabla",
                    "tipoCarga": "0",
                    "valorPivot": "-",
                    "estado": "2",
                    "fechaInicio.$": "$.execStart",
                    "fechaFin": "1970-01-01 00:00:00+00",
                    "archivoDestino": "-",
                    "fechaInsertUpdate.$": "$.execStart",
                    "runID.$": "$$.Execution.Id",
                    "sistemaFuente.$": "$.table.nombreCarpetaDL",
                },
            }),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # GlueCopyFull (sync)
        glue_copy_full = tasks.GlueStartJobRun(
            self,
            "GlueCopyFull",
            glue_job_name=glue_extractcopy_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--mode": "full",
                "--sql_query.$": "$.table.query",
                "--pivot_type": "none",
                "--output_prefix.$": f"States.Format('s3://{raw_bucket_name}/data/{{}}/{{}}/{{}}/{{}}/', $.table.nombreInstancia, $.table.nombreBaseDatos, $.table.nombreCarpetaDL, $.table.nombreTabla)",
                "--metrics_path.$": f"States.Format('s3://{raw_bucket_name}/temp/metrics/data/{{}}/{{}}/{{}}/{{}}/', $.table.nombreInstancia, $.table.nombreBaseDatos, $.table.nombreCarpetaDL, $.table.nombreTabla)",
                "--archivo_nombre.$": "States.Format('{}.parquet', States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($.execStart, '.'), 0), 'Z'), 0))",
            }),
            result_path="$.glue",
        )

        glue_copy_full.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(10),
            max_attempts=3,
            errors=["Glue.AWSGlueException", "Glue.ThrottlingException", "States.TaskFailed"],
        )

        # GlueCopyFull catch -> AuditFail (store last_error)
        # We'll define audit_fail_parallel later and then add the catch

        # GetPivotAudit (note: JSON had a hardcoded SQL example; keep as-is)
        get_pivot_audit = tasks.LambdaInvoke(
            self,
            "GetPivotAudit",
            lambda_function=sql_runner_fn,
            # payload_response_only=True,
            result_path="$.pivotAudit",
            payload=sfn.TaskInput.from_object({
                "action": "select_one",
                "sql": "SELECT valorPivot FROM dbo.controlPipelineLight_gen2 WHERE nombreTabla = 'dbo.XYZ' AND estado = 1 ORDER BY fechaInsertUpdate DESC LIMIT 1",
            }),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        get_pivot_audit.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=3,
            errors=["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
        )

        # GlueCopyIncr (sync)
        glue_copy_incr = tasks.GlueStartJobRun(
            self,
            "GlueCopyIncr",
            glue_job_name=glue_extractcopy_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                "--mode": "incr",
                "--sql_query.$": "$.table.query",
                "--pivot_from.$": "$.pivotAudit.Payload.value",
                "--pivot_type.$": "$.table.tipoCampoPivot",
                "--pivot_column.$": "$.table.nombreCampoPivot",
                "--output_prefix.$": f"States.Format('s3://{raw_bucket_name}/data/{{}}/{{}}/{{}}/{{}}/', $.table.nombreInstancia, $.table.nombreBaseDatos, $.table.nombreCarpetaDL, $.table.nombreTabla)",
                "--metrics_path.$": f"States.Format('s3://{raw_bucket_name}/temp/metrics/data/{{}}/{{}}/{{}}/{{}}/', $.table.nombreInstancia, $.table.nombreBaseDatos, $.table.nombreCarpetaDL, $.table.nombreTabla)",
                "--archivo_nombre.$": "States.Format('{}.parquet', States.ArrayGetItem(States.StringSplit(States.ArrayGetItem(States.StringSplit($.execStart, '.'), 0), 'Z'), 0))",
            }),
            result_path="$.glue",
        )

        glue_copy_incr.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(10),
            max_attempts=3,
            errors=["Glue.AWSGlueException", "Glue.ThrottlingException", "States.TaskFailed"],
        )

        # ReadMetrics
        read_metrics = tasks.LambdaInvoke(
            self,
            "ReadMetrics",
            lambda_function=read_metrics_fn,
            # payload_response_only=True,
            result_path="$.metrics",
            payload=sfn.TaskInput.from_object({
                "metrics_path.$": f"States.Format('s3://{raw_bucket_name}/temp/metrics/data/{{}}/{{}}/{{}}/{{}}/', $.table.nombreInstancia, $.table.nombreBaseDatos, $.table.nombreCarpetaDL, $.table.nombreTabla)"
            }),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        read_metrics.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(2),
            max_attempts=5,
            errors=["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException"],
        )

        # AuditSuccess (call to update exec success)
        audit_success = tasks.LambdaInvoke(
            self,
            "AuditSuccess",
            lambda_function=sql_runner_fn,
            # payload_response_only=True,
            result_path="$.auditEnd",
            payload=sfn.TaskInput.from_object({
                "action": "call",
                "sql": "CALL dbo.usp_upd_ejecucion_light_gen2(:tablaNombre::varchar,:valorPivot::varchar,:estado::int,:fechaFin::timestamp,:archivoDestino::varchar,:fechaInsertUpdate::timestamp,:runID::varchar,:cantidadRegistros::int,:sistemaFuente::varchar,:cantidadRegistrosTotales::int);",
                "params": {
                    "archivoDestino.$": "States.Format('/data/{}/{}/{}/{}/{}', $.table.nombreInstancia, $.table.nombreBaseDatos, $.table.nombreCarpetaDL, $.table.nombreTabla, $.metrics.Payload.archivo_nombre)",
                    "estado": 1,
                    "fechaFin.$": "$$.State.EnteredTime",
                    "fechaInsertUpdate.$": "$$.State.EnteredTime",
                    "runID.$": "$$.Execution.Id",
                    "tablaNombre.$": "$.table.nombreTabla",
                    "valorPivot.$": "$.metrics.Payload.pivot_to",
                    "cantidadRegistros.$": "$.metrics.Payload.rows_copied",
                    "sistemaFuente.$": "$.table.nombreCarpetaDL",
                    "cantidadRegistrosTotales.$": "$.metrics.Payload.total_source",
                },
            }),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # AuditFail parallel (UpdFail + NotifyFail)
        # UpdFail - lambda call to update exec as failed
        upd_fail = tasks.LambdaInvoke(
            self,
            "UpdFail",
            lambda_function=sql_runner_fn,
            # payload_response_only=True,
            payload=sfn.TaskInput.from_object({
                "action": "call",
                "sql": "CALL dbo.usp_upd_ejecucion_light_gen2(:tablaNombre::varchar,:valorPivot::varchar,:estado::int,:fechaFin::timestamp,:archivoDestino::varchar,:fechaInsertUpdate::timestamp,:runID::varchar,:cantidadRegistros::int,:sistemaFuente::varchar,:cantidadRegistrosTotales::int);",
                "params": {
                    "tablaNombre.$": "$.table.nombreTabla",
                    "valorPivot": "-",
                    "estado": 0,
                    "fechaFin.$": "$$.State.EnteredTime",
                    "archivoDestino": "-",
                    "fechaInsertUpdate.$": "$$.State.EnteredTime",
                    "runID.$": "$$.Execution.Name",
                    "cantidadRegistros": 0,
                    "sistemaFuente.$": "$.table.nombreCarpetaDL",
                    "cantidadRegistrosTotales": 0,
                },
            }),
            result_path=sfn.JsonPath.DISCARD,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # NotifyFail (SNS publish)
        notify_fail = sfn.CustomState(
            self,
            "NotifyFail",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:sns:publish",
                "Parameters": {
                    "TopicArn": failure_topic.topic_arn,
                    "Subject": "finandina-dev Ingesta FALLIDA",
                    "Message.$": "States.Format('Tabla {} falló. ExecId={}', $.table.nombreTabla, $$.Execution.Id)",
                },
                "End": True,
            },
        )

        audit_fail_parallel = sfn.Parallel(self, "AuditFail")
        audit_fail_parallel.branch(sfn.Chain.start(upd_fail))
        audit_fail_parallel.branch(sfn.Chain.start(notify_fail))

        # Now add catches that should route to audit_fail_parallel with result path as in JSON
        glue_copy_full.add_catch(audit_fail_parallel, errors=["States.ALL"], result_path="$.last_error")
        glue_copy_incr.add_catch(audit_fail_parallel, errors=["States.ALL"], result_path="$.last_error")
        read_metrics.add_catch(audit_fail_parallel, errors=["States.ALL"], result_path="$.last_error")
        
        # audit_success already had a catch placeholder; replace it by wiring proper catch
        audit_success.add_catch(audit_fail_parallel, errors=["States.ALL"], result_path="$.last_error")

        # ----- Wire the iterator chains -----
        full_chain = sfn.Chain.start(audit_start_full).next(glue_copy_full)
        incr_chain = sfn.Chain.start(audit_start_incr).next(get_pivot_audit).next(glue_copy_incr)

        # Choice: nombreCampoPivot == "-"
        is_incremental_choice.when(
            sfn.Condition.string_equals("$.table.nombreCampoPivot", "-"),
            full_chain
        )
        is_incremental_choice.otherwise(incr_chain)

        # After either branch, go to ReadMetrics -> AuditSuccess
        # Use afterwards() to attach the following states
        is_incremental_choice.afterwards().next(read_metrics).next(audit_success)

        # Assemble iterator: GetOriginParams -> IsIncremental
        iterator_chain = sfn.Chain.start(get_origin_params).next(is_incremental_choice)

        ingest_map.iterator(iterator_chain)

        # ----- RunTdc (Glue job) -----
        run_tdc = tasks.GlueStartJobRun(
            self,
            "RunTdc",
            glue_job_name=glue_tdc_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # NotifyTdcFail
        notify_finandina_fail = sfn.CustomState(
            self,
            "NotifyTdcFail",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:sns:publish",
                "Parameters": {
                    "TopicArn": failure_topic.topic_arn,
                    "Subject": "TDC (Glue) FALLÓ",
                    "Message.$": "States.Format('Glue TDC falló. ExecId={}', $$.Execution.Id)",
                },
                "End": True,
            },
        )

        run_tdc.add_catch(notify_finandina_fail, result_path=sfn.JsonPath.DISCARD)

        # ----- Top-level wiring -----
        has_tables.when(
            sfn.Condition.number_equals("$.active.count", 0),
            end_no_tables
        )
        has_tables.otherwise(ingest_map.next(run_tdc))

        definition = sfn.Chain.start(get_active_tables).next(has_tables)

        # ----- State Machine -----
        self.state_machine = sfn.StateMachine(
            self,
            "TdcStateMachine",
            state_machine_name=create_name('sfn', 'fuentes-adls-tdc'),
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            logs=sfn.LogOptions(destination=log_group, level=sfn.LogLevel.ALL, include_execution_data=True),
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