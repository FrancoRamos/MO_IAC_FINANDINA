# pl_txt_sf.py
from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_logs as logs,
    aws_iam as iam,
    aws_sns as sns,
    aws_stepfunctions as sfn,
)
from ....utils.naming import create_name

@dataclass
class PlTxtSfProps:
    """
    Props para la orquestación vía Redshift Data API.
    """

    environment: str
    cluster_identifier: str | None = None
    workgroup_name: str | None = None
    database: str = ""
    db_user_secret_arn: str = ""
    s3_prefix_uri: str = ""
    procedure_fqn: str | None = None  # default: sd.sp_export_base_experian
    delimiter: str | None = None      # default: ";"
    add_quotes: bool | None = None    # default: True
    failure_topic: sns.ITopic | None = None


class PlTxtSf(Construct):
    """
    Define la Step Function que orquesta la ejecución del Stored Procedure
    Redshift (Data API) para generar archivos planos en S3.
    """

    def __init__(self, scope: Construct, id: str, props: PlTxtSfProps) -> None:
        super().__init__(scope, id)

        # ----- Validaciones -----
        if not props.cluster_identifier and not props.workgroup_name:
            raise ValueError("Debes especificar cluster_identifier o workgroup_name.")
        if props.cluster_identifier and props.workgroup_name:
            raise ValueError("Usa solo uno: cluster_identifier o workgroup_name, no ambos.")

        region = Stack.of(self).region
        account = Stack.of(self).account

        # ----- Rol IAM para Step Functions -----
        role = iam.Role(
            self,
            "SfRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            description="Permite ejecutar Redshift Data API y publicar en SNS",
        )

        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "redshift-data:ExecuteStatement",
                    "redshift-data:DescribeStatement",
                    "redshift-data:CancelStatement",
                ],
                resources=["*"],  # Data API suele requerir "*"
            )
        )

        role.add_to_policy(
            iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                resources=[props.db_user_secret_arn],
            )
        )

        if props.failure_topic:
            props.failure_topic.grant_publish(role)

        # ----- Defaults -----
        proc = props.procedure_fqn or "sd.sp_export_base_experian"
        delimiter = props.delimiter or ";"
        add_quotes = props.add_quotes if props.add_quotes is not None else True

        # ----- Estados personalizados -----
        publish_api_fail = sfn.CustomState(
            self,
            "PublishApiFailureSNS",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:sns:publish",
                "Parameters": {
                    "TopicArn": props.failure_topic.topic_arn if props.failure_topic else "",
                    "Subject": "Fallo API en pl_Data_Ds_Asdwh_fa_Ds_To_file_txt (Data API)",
                    "Message": sfn.JsonPath.format(
                        "Execution: {0}\nState: {1}\nError: {2}",
                        sfn.JsonPath.string_at("$$.Execution.Id"),
                        sfn.JsonPath.string_at("$$.State.Name"),
                        sfn.JsonPath.string_at("$.error"),
                    ),
                },
                "End": True,
            },
        )

        publish_fail_logical = sfn.CustomState(
            self,
            "PublishFailureSNS",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:sns:publish",
                "Parameters": {
                    "TopicArn": props.failure_topic.topic_arn if props.failure_topic else "",
                    "Subject": "Fallo en pl_Data_Ds_Asdwh_fa_Ds_To_file_txt (SP/UNLOAD)",
                    "Message": sfn.JsonPath.format(
                        "Pipeline: {0}\nExecution: {1}\nStatus: {2}\nError: {3}",
                        "pl_Data_Ds_Asdwh_fa_Ds_To_file_txt",
                        sfn.JsonPath.string_at("$$.Execution.Id"),
                        sfn.JsonPath.string_at("$.desc.Status"),
                        sfn.JsonPath.string_at("$.desc.Error"),
                    ),
                },
                "End": True,
            },
        )

        # ----- Parámetros para ExecuteStatement -----
        exec_params = {
            "Database": props.database,
            "SecretArn": props.db_user_secret_arn,
            "Sql": sfn.JsonPath.format(
                "CALL {}('{}','{}','{}');",
                proc,
                props.s3_prefix_uri,
                delimiter,
                "true" if add_quotes else "false",
            ),
        }
        if props.cluster_identifier:
            exec_params["ClusterIdentifier"] = props.cluster_identifier
        if props.workgroup_name:
            exec_params["WorkgroupName"] = props.workgroup_name

        # ----- Paso 1: ExecuteStatement -----
        exec_state = sfn.CustomState(
            self,
            "CallStoredProcedure",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement",
                "Parameters": exec_params,
                "ResultSelector": {"StatementId": sfn.JsonPath.string_at("$.Id")},
                "ResultPath": "$.exec",
            },
        )
        exec_with_catch = exec_state.add_catch(publish_api_fail, result_path="$.error")

        # ----- Paso 2: Espera -----
        wait = sfn.Wait(
            self,
            "Wait",
            time=sfn.WaitTime.duration(Duration.seconds(5)),
        )

        # ----- Paso 3: DescribeStatement -----
        describe_state = sfn.CustomState(
            self,
            "DescribeStatement",
            state_json={
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:redshiftdata:describeStatement",
                "Parameters": {"Id": sfn.JsonPath.string_at("$.exec.StatementId")},
                "ResultPath": "$.desc",
            },
        )
        describe_with_catch = describe_state.add_catch(publish_api_fail, result_path="$.error")

        # ----- Paso 4: Choice (CheckStatus) -----
        check = (
            sfn.Choice(self, "CheckStatus")
            .when(
                sfn.Condition.string_equals("$.desc.Status", "FINISHED"),
                sfn.Succeed(self, "OK"),
            )
            .when(
                sfn.Condition.or_(
                    sfn.Condition.string_equals("$.desc.Status", "FAILED"),
                    sfn.Condition.string_equals("$.desc.Status", "ABORTED"),
                ),
                publish_fail_logical,
            )
            .otherwise(wait)
        )

        # ----- Logs -----
        log_group = logs.LogGroup(
            self,
            "SfLogs",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ----- Cadena final -----
        definition = (
            sfn.Chain.start(exec_with_catch)
            .next(wait)
            .next(describe_with_catch)
            .next(check)
        )

        # ----- State Machine -----
        self.state_machine = sfn.StateMachine(
            self,
            "PlStateMachineSp",
            state_machine_name=create_name('sfn', 'pl-Data-Ds-Asdwh-fa-Ds-To-file-txt'),
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            timeout=Duration.hours(1),
            role=role,
        )
