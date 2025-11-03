from dataclasses import dataclass
from constructs import Construct
from aws_cdk import (
    Duration,
    aws_iam as iam,
    aws_s3_assets as s3assets,
    aws_glue as glue,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_lambda as _lambda,
)
from aws_cdk.aws_s3 import Bucket
from aws_cdk.aws_iam import Role
from ....utils.naming import create_name
import os

@dataclass
class EtlTfAdlsFabogsqlEsbdataConstructProps:
    environment: str
    project: str
    scripts_bucket: Bucket
    lambda_execution_role: Role


class EtlTfAdlsFabogsqlEsbdataConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlTfAdlsFabogsqlEsbdataConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        project = props.project
        scripts_bucket = props.scripts_bucket
        lambda_execution_role = props.lambda_execution_role
        
        
        # --- Lambdas ---
        common_redshift_layer = _lambda.LayerVersion(
            self,
            "IjsonLayer",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "../../layers/common-redshift-layer.zip")
            ),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description="Layer para los lambda del ETL pl_Data_Ds_Sql_ESBDATA2_To_Ds_ADLS_esbdata",
        )
        
        self.lookup_lambda = _lambda.Function(
            self,
            "LookUpFuentesLambda",
            function_name=create_name("lambda", "look-up-fuentes"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "../../scripts/lambda/look_up_fuentes/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[common_redshift_layer],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.auditoria_inicio_lambda = _lambda.Function(
            self,
            "AusitoriaInicioLambda",
            function_name=create_name("lambda", "auditoria-inicio"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "../../scripts/lambda/auditoria_inicio/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[common_redshift_layer],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.copia_integra_lambda = _lambda.Function(
            self,
            "CopiaIntegraLambda",
            function_name=create_name("lambda", "copia-integra"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "../../scripts/lambda/copia_integra/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[common_redshift_layer],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.conteo_registros_lambda = _lambda.Function(
            self,
            "ConteoRegistrosLambda",
            function_name=create_name("lambda", "conteo-registros"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "../../scripts/lambda/conteo_registros/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[common_redshift_layer],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.auditoria_ok_lambda = _lambda.Function(
            self,
            "AuditoriaOKLambda",
            function_name=create_name("lambda", "auditoria-ok"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "../../scripts/lambda/auditoria_ok/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[common_redshift_layer],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.auditoria_ko_lambda = _lambda.Function(
            self,
            "AuditoriaKOLambda",
            function_name=create_name("lambda", "auditoria-ko"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "../../scripts/lambda/auditoria_ko/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[common_redshift_layer],
            memory_size=512,
            timeout=Duration.seconds(300),
        )