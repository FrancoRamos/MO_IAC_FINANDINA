# pl_xlsx_sf.py
from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    aws_iam as iam,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)
from .....utils.naming import create_name


@dataclass
class SfProps:
    """
    Props para la Stack de Step Functions (PlXlsxSf).
    """
    sfn_role: iam.IRole                     # rol creado en el stack tf-
    export_bucket_name: str                 # bucket de salida
    export_prefix: str                      # 'finandina/planos/dwh/taximetro/'
    redshift_database: str                  # p.ej. 'asdwh'
    redshift_secret_arn: str                # ARN del secret con credenciales Redshift
    failure_topic: sns.ITopic         # topic del stack tf-
    redshift_workgroup_name: str | None = None
    cluster_identifier: str | None = None
    redshift_unload_role_arn: str | None = None


class PlXlsxSf(Construct):
    """
    Stack que define la Step Function para ejecutar un UNLOAD de Redshift
    hacia S3 y manejar notificaciones de fallo.
    """

    def __init__(self, scope: Construct, id: str, *, props: SfProps) -> None:
        super().__init__(scope, id)

        # Validación: exactamente uno de los dos
        if bool(props.redshift_workgroup_name) == bool(props.cluster_identifier):
            raise ValueError(
                "Debe especificar exactamente uno: redshift_workgroup_name (serverless) "
                "o cluster_identifier (provisioned)."
            )

        # ----- SQL y parámetros dinámicos -----
        s3_target = f"s3://{props.export_bucket_name}/{props.export_prefix}resultadoTaximetro.txt"
        iam_role_clause = (
            f" IAM_ROLE '{props.redshift_unload_role_arn}'"
            if props.redshift_unload_role_arn
            else ""
        )

        sql = f"""
            UNLOAD ($$
                SELECT *
                FROM dw.FactConsultaCentral
                WHERE fechaConsulta >= DATEADD(month, -4, DATE_TRUNC('month', CURRENT_DATE))
            $$)
            TO '{s3_target}'
            WITH
            FORMAT AS CSV
            DELIMITER AS ';'
            HEADER
            PARALLEL OFF
            ALLOWOVERWRITE
            {iam_role_clause}
            ;
        """

        exec_params = {
            "Database": props.redshift_database,
            "SecretArn": props.redshift_secret_arn,
            "Sql": sql,
            "WithEvent": True,
        }
        if props.redshift_workgroup_name:
            exec_params["WorkgroupName"] = props.redshift_workgroup_name
        if props.cluster_identifier:
            exec_params["ClusterIdentifier"] = props.cluster_identifier

        # ----- Task principal -----
        exec_task = tasks.CallAwsService(
            self,
            "UNLOAD_to_S3_txt",
            service="redshiftdata",
            action="executeStatement",
            parameters=exec_params,
            iam_resources=["*"],
            result_selector={"StatementId.$": "$.Id"},
            result_path="$.unload",
        )

        # ----- Notificación de error -----
        capture = sfn.Pass(
            self,
            "CaptureError",
            result_path="$.Error",
            parameters={"Error.$": "$"},
        )
        notify = tasks.CallAwsService(
            self,
            "NotifyFailure",
            service="sns",
            action="publish",
            parameters={
                "TopicArn": props.failure_topic.topic_arn,
                "Subject": "Fallo en pl_Data_Ds_Asdwh_fa_Ds_To_file_xlsx",
                "Message": sfn.JsonPath.string_at("$.Error"),
            },
            iam_resources=[props.failure_topic.topic_arn],
        )
        failure_chain = capture.next(notify)

        # ----- Describe + Poll -----
        describe = tasks.CallAwsService(
            self,
            "DescribeStatement",
            service="redshiftdata",
            action="describeStatement",
            parameters={"Id": sfn.JsonPath.string_at("$.unload.StatementId")},
            iam_resources=["*"],
            result_path="$.desc",
        )
        describe_with_catch = describe.add_catch(failure_chain, result_path="$.error")

        wait = sfn.Wait(
            self,
            "Wait5s",
            time=sfn.WaitTime.duration(Duration.seconds(5)),
        )

        # Choice basado en Status
        is_done = (
            sfn.Choice(self, "IsDone")
            .when(
                sfn.Condition.string_equals("$.desc.Status", "FINISHED"),
                sfn.Succeed(self, "Finished"),
            )
            .when(
                sfn.Condition.or_(
                    sfn.Condition.string_equals("$.desc.Status", "FAILED"),
                    sfn.Condition.string_equals("$.desc.Status", "ABORTED"),
                ),
                failure_chain,
            )
            .otherwise(wait.next(describe_with_catch))
        )

        # Catch también en el executeStatement
        exec_with_catch = exec_task.add_catch(failure_chain, result_path="$.error")

        # ----- Cadena final -----
        definition = (
            sfn.Chain.start(exec_with_catch)
            .next(describe_with_catch)
            .next(is_done)
        )

        # ----- State Machine -----
        self.state_machine = sfn.StateMachine(
            self,
            "SM_pl_Data_Ds_Asdwh_fa_Ds_To_file_xlsx",
            state_machine_name=create_name('sfn', 'pl-Data-Ds-Asdwh-fa-Ds-To-file-xlsx'),
            role=props.sfn_role,
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(2),
        )
