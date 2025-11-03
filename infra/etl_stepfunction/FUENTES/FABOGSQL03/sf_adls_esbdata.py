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
from ....utils.naming import create_name


@dataclass
class EtlSfAdlsFabogsqlEsbdataConstructProps:
    environment: str
    lookup_fn: _lambda.IFunction  
    auditoria_inicio_fn: _lambda.IFunction  
    copia_integra_fn: _lambda.IFunction        
    conteo_registros_fn: _lambda.IFunction    
    auditoria_ok_fn: _lambda.IFunction 
    auditoria_ko_fn: _lambda.IFunction 
    failure_topic: sns.ITopic
    raw_bucket_name: str


class EtlSfAdlsFabogsqlEsbdataConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlSfAdlsFabogsqlEsbdataConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        lookup_fn = props.lookup_fn
        auditoria_inicio_fn = props.auditoria_inicio_fn
        copia_integra_fn = props.copia_integra_fn
        conteo_registros_fn = props.conteo_registros_fn
        auditoria_ok_fn = props.auditoria_ok_fn
        auditoria_ko_fn = props.auditoria_ko_fn
        failure_topic = props.failure_topic
        raw_bucket_name = props.raw_bucket_name
        
        # ----- Log Group for the State Machine -----
        log_group = logs.LogGroup(
            self,
            "SfEsbDataLogs",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # ----- Task: Lookup_Fuentes (Lambda invoke) -----
        lookup_fuentes = tasks.LambdaInvoke(
            self,
            "Lookup_Fuentes",
            lambda_function=lookup_fn,
            output_path="$.Payload",
            payload=sfn.TaskInput.from_object({}),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # ----- Choice: HayMetadatos? -----
        hay_metadatos_choice = sfn.Choice(self, "HayMetadatos?")

        # ----- Pass: Set_Query_From_Input -----
        set_query_from_input = sfn.Pass(
            self,
            "Set_Query_From_Input",
            parameters={
                "query.$": "$.unloadQuery"
            },
            result_path="$.effective",
        )

        # ----- Pass: Set_Query_From_Metadata -----
        set_query_from_metadata = sfn.Pass(
            self,
            "Set_Query_From_Metadata",
            parameters={
                "query.$": "$.metadata.query"
            },
            result_path="$.effective",
        )

        # ----- Task: Auditoria_Inicio (lambda) -----
        auditoria_inicio = tasks.LambdaInvoke(
            self,
            "Auditoria_Inicio",
            lambda_function=auditoria_inicio_fn,
            result_path="$.auditoriaInicio",
            payload=sfn.TaskInput.from_object({
                "run_id.$": "$$.Execution.Id",
                "origen": "redshift",
                "metadata.$": "$.metadata",
                "valor_pivot": "-"
            }),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # ----- Task: Copia_Integra_Sin_Pivot (lambda) -----
        copia_integra = tasks.LambdaInvoke(
            self,
            "Copia_Integra_Sin_Pivot",
            lambda_function=copia_integra_fn,
            payload=sfn.TaskInput.from_object({
                "unload_query.$": "$.effective.query",
                "metadata.$": "$.metadata"
            }),
            result_path="$.copy",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # Add retry behavior for copia_integra (matches ASL)
        copia_integra.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(15),
            max_attempts=3,
            errors=[
                "States.TaskFailed",
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
            ],
        )

        # ----- Task: Conteo_Registros (lambda) -----
        conteo_registros = tasks.LambdaInvoke(
            self,
            "Conteo_Registros",
            lambda_function=conteo_registros_fn,
            payload=sfn.TaskInput.from_object({
                "metadata.$": "$.metadata",
                "effective.$": "$.effective"
            }),
            result_path="$.conteo",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # ----- Task: Auditoria_OK (lambda) -----
        auditoria_ok = tasks.LambdaInvoke(
            self,
            "Auditoria_OK",
            lambda_function=auditoria_ok_fn,
            payload=sfn.TaskInput.from_object({
                "tabla_nombre.$": "$.metadata.nombreTabla",
                "valor_pivot": "-",
                "archivo_destino.$": "$.copy.Payload.s3_output",
                "run_id.$": "$$.Execution.Id",
                "cantidad_registros.$": "$.conteo.Payload.cantidad",
                "sistema_fuente.$": "$.metadata.nombreCarpetaDL",
                "cantidad_registros_totales.$": "$.conteo.Payload.cantidad"
            }),
            result_path="$.auditoriaOk",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # ----- Task: Auditoria_KO (lambda) -----
        auditoria_ko = tasks.LambdaInvoke(
            self,
            "Auditoria_KO",
            lambda_function=auditoria_ko_fn,
            payload=sfn.TaskInput.from_object({
                "tabla_nombre.$": "$.metadata.nombreTabla",
                "run_id.$": "$$.Execution.Id",
                "sistema_fuente.$": "$.metadata.nombreCarpetaDL",
                "archivo_destino.$": "$.copy.Payload.s3_output",
                "error.$": "$.errorCopia"
            }),
            result_path="$.auditoriaKo",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )

        # ----- Terminal Succeed state for "SinTrabajo" -----
        sin_trabajo = sfn.Succeed(self, "SinTrabajo")

        # ----- Wire choice "Resolver_Query_Efectiva" -----
        resolver_query_choice = sfn.Choice(self, "Resolver_Query_Efectiva")
        resolver_query_choice.when(
            sfn.Condition.and_(
                sfn.Condition.is_present("$.unloadQuery"),
                sfn.Condition.is_not_null("$.unloadQuery"),
            ),
            set_query_from_input
        )
        resolver_query_choice.otherwise(set_query_from_metadata)

        # ----- Build the iterator chain / main chain pieces -----
        # Hook up copia_integra catch to auditoria_ko
        copia_integra.add_catch(auditoria_ko, result_path="$.errorCopia")

        # Define the HayMetadatos? choice to check $.ok boolean and route accordingly
        hay_metadatos_choice.when(
            sfn.Condition.boolean_equals("$.ok", True),
            resolver_query_choice
        )
        hay_metadatos_choice.otherwise(sin_trabajo)

        # We will create a chain that starts at lookup and continues to hay_metadatos_choice
        definition = sfn.Chain.start(lookup_fuentes).next(hay_metadatos_choice)

        # Now tie those pass states into the main_chain by making them continue to auditoria_inicio
        set_query_from_input.next(auditoria_inicio)
        set_query_from_metadata.next(auditoria_inicio)

        # auditoria_inicio -> copia_integra -> conteo_registros -> auditoria_ok
        auditoria_inicio.next(copia_integra)
        copia_integra.next(conteo_registros)
        conteo_registros.next(auditoria_ok)


        # ----- State Machine -----
        self.state_machine = sfn.StateMachine(
            self,
            "EsbDataStateMachine",
            state_machine_name=create_name('sfn', 'fuentes-adls-esbdata'),
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            logs=sfn.LogOptions(destination=log_group, level=sfn.LogLevel.ALL, include_execution_data=True),
        )

        # # ----- Permissions for State Machine Role -----
        # # Allow invoking the Lambdas used in this state machine
        # lambda_functions = [
        #     lookup_fn,
        #     auditoria_inicio_fn,
        #     copia_integra_fn,
        #     conteo_registros_fn,
        #     auditoria_ok_fn,
        #     auditoria_ko_fn,
        # ]
        # for fn in lambda_functions:
        #     # Grant permission by ARN to the role (InvokeFunction)
        #     self.state_machine.add_to_role_policy(
        #         iam.PolicyStatement(
        #             actions=["lambda:InvokeFunction"],
        #             resources=[fn.function_arn],
        #         )
        #     )
