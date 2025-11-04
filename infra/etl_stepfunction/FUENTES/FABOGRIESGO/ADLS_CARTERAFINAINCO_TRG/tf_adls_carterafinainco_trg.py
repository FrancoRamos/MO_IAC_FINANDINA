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
from .....utils.naming import create_name
import os

@dataclass
class EtlTfAdlsCarteraFinaincoConstructProps:
    environment: str
    scripts_bucket: Bucket
    lambda_execution_role: Role


class EtlTfAdlsCarteraFinaincoConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlTfAdlsCarteraFinaincoConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        scripts_bucket = props.scripts_bucket
        lambda_execution_role = props.lambda_execution_role
        
        
        # --- Lambdas ---
        self.insertaud_lambda = _lambda.Function(
            self,
            "LookInsertAudCarterafinaincoTrgLambda",
            function_name=create_name("lambda", "look-insertaud-finainco-trg"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "../../scripts/lambda/look_insertaud_carterafinainco_trg/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.incredata_lambda = _lambda.Function(
            self,
            "LookIncreDataCarterafinaincoTrgLambda",
            function_name=create_name("lambda", "look-incredata-finainco-trg"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "../../scripts/lambda/look_incredata_carterafinainco_trg/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.fulldata_lambda = _lambda.Function(
            self,
            "FullDataCarterafinaincoTrgLambda",
            function_name=create_name("lambda", "fulldata-finainco-trg"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "../../scripts/lambda/fulldata_carterafinainco_trg/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.success_lambda = _lambda.Function(
            self,
            "SuccessInsertaudCarterafinaincoTrgLambda",
            function_name=create_name("lambda", "success-insertaud-finainco-trg"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "../../scripts/lambda/success_insertaud_carterafinainco_trg/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )
        
        self.daily_lambda = _lambda.Function(
            self,
            "DailyInsertaudCarterafinaincoTrgLambda",
            function_name=create_name("lambda", "daily-insertaud-finainco-trg"),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset(
                os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "../../scripts/lambda/daily_insertaud_carterafinainco_trg/src")
            ),
            role=lambda_execution_role,
            environment={},
            layers=[],
            memory_size=512,
            timeout=Duration.seconds(300),
        )