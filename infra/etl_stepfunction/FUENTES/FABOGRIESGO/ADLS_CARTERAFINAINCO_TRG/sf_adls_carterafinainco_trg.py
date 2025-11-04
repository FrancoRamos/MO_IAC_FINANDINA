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
from .....utils.naming import create_name


@dataclass
class EtlSfAdlsCarteraFinaincoConstructProps:
    environment: str
    insertaud_fn: _lambda.IFunction  
    incredata_fn: _lambda.IFunction  
    fulldata_fn: _lambda.IFunction        
    success_fn: _lambda.IFunction    
    daily_fn: _lambda.IFunction 
    failure_topic: sns.ITopic
    raw_bucket_name: str


class EtlSfAdlsCarteraFinaincoConstruct(Construct):

    def __init__(self, scope: Construct, id: str, props: EtlSfAdlsCarteraFinaincoConstructProps) -> None:
        super().__init__(scope, id)

        environment = props.environment
        insertaud_fn = props.insertaud_fn
        incredata_fn = props.incredata_fn
        fulldata_fn = props.fulldata_fn
        success_fn = props.success_fn
        daily_fn = props.daily_fn
        failure_topic = props.failure_topic
        raw_bucket_name = props.raw_bucket_name
        
        
        # Log group
        log_group = logs.LogGroup(
            self,
            "SfCarteraFinaincoTrgLogs",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # Pass state "Pass" that attaches executionId and original input into $.payloadWithExecutionId
        pass_with_exec = sfn.Pass(
            self,
            "Pass",
            parameters={
                "originalInput.$": "$",
                "executionId.$": "$$.Execution.Id"
            },
            result_path="$.payloadWithExecutionId",
        )

        # Lambda Invoke (first)
        lambda_invoke = tasks.LambdaInvoke(
            self,
            "Lambda Invoke",
            lambda_function=insertaud_fn,
            # payload=sfn.TaskInput.from_object({}),
            result_path="$.lambdaOutputValue",
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            retry_on_service_exceptions=False,
        )
        # Retry as per console
        lambda_invoke.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(1),
            max_attempts=3,
            errors=[
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
                "Lambda.TooManyRequestsException",
            ],
        )

        # IncrementalFlow task
        incremental_flow = tasks.LambdaInvoke(
            self,
            "IncrementalFlow",
            lambda_function=incredata_fn,
            # payload=sfn.TaskInput.from_object({"Payload.$": "$"}),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            # result_path="$.incrementalResult",
            retry_on_service_exceptions=False,
        )

        # FullLoadFlow task
        full_load_flow = tasks.LambdaInvoke(
            self,
            "FullLoadFlow",
            lambda_function=fulldata_fn,
            # payload=sfn.TaskInput.from_object({"Payload.$": "$"}),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            # result_path="$.fullResult",
            retry_on_service_exceptions=False,
        )

        # HandleSuccess Pass
        handle_success = sfn.Pass(
            self,
            "HandleSuccess",
            result=sfn.Result.from_string("Flujo Incremental/Full Load terminado con éxito."),
        )

        # LambdaInvoke_OnSuccess task
        lambda_invoke_on_success = tasks.LambdaInvoke(
            self,
            "LambdaInvoke_OnSuccess",
            lambda_function=success_fn,
            # payload=sfn.TaskInput.from_object({"Payload.$": "$"}),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            # result_path="$.onSuccessResult",
            retry_on_service_exceptions=False,
        )

        # HandleFailure Pass
        handle_failure = sfn.Pass(
            self,
            "HandleFailure",
            result=sfn.Result.from_string("Flujo Incremental/Full Load falló."),
        )

        # Parallel_Fail - two branches: SNS Publish and LambdaInvoke_OnFailure
        parallel_fail = sfn.Parallel(self, "Parallel_Fail")

        # Branch 1: SNS Publish
        sns_publish = tasks.SnsPublish(
            self,
            "SNS Publish",
            topic=failure_topic,
            message=sfn.TaskInput.from_json_path_at("$"),#sfn.TaskInput.from_object({"Message.$": "$"}),
            # result_path="$.snsPublishResult",
        )
        branch1 = sfn.Chain.start(sns_publish)

        # Branch 2: LambdaInvoke_OnFailure (daily_fn) with retry
        lambda_invoke_on_failure = tasks.LambdaInvoke(
            self,
            "LambdaInvoke_OnFailure",
            lambda_function=daily_fn,
            # payload=sfn.TaskInput.from_object({"Payload.$": "$"}),
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            # result_path="$.onFailureResult",
            retry_on_service_exceptions=False,
        )
        lambda_invoke_on_failure.add_retry(
            backoff_rate=2,
            interval=Duration.seconds(1),
            max_attempts=3,
            errors=[
                "Lambda.ServiceException",
                "Lambda.AWSLambdaException",
                "Lambda.SdkClientException",
                "Lambda.TooManyRequestsException",
            ],
        )
        branch2 = sfn.Chain.start(lambda_invoke_on_failure)

        parallel_fail.branch(branch1)
        parallel_fail.branch(branch2)

        # HandleFailure chain: Pass -> Parallel (console: Parallel has End true)
        failure_chain = sfn.Chain.start(handle_failure).next(parallel_fail)

        # Wire catches to go to the failure chain (HandleFailure -> Parallel_Fail)
        incremental_flow.add_catch(failure_chain.start_state, errors=["States.ALL"])
        full_load_flow.add_catch(failure_chain.start_state, errors=["States.ALL"])

        # Incremental and Full flows end in success lambda
        success_chain = sfn.Chain.start(handle_success).next(lambda_invoke_on_success)
        incremental_chain = sfn.Chain.start(incremental_flow).next(success_chain)
        full_chain = sfn.Chain.start(full_load_flow).next(success_chain)

        # Now wire the main flow:
        # Pass -> Lambda Invoke -> Choice(CheckIncremental) -> either IncrementalFlow Chain or FullLoadFlow Chain
        definition = (
            sfn.Chain.start(pass_with_exec)
            .next(lambda_invoke)
            .next(
                sfn.Choice(self, "CheckIncremental")  # recreate choice properly with correct branches
                .when(sfn.Condition.string_equals("$.lambdaOutputValue", "-"), incremental_chain)
                .otherwise(full_chain)
            )
        )

        # Create state machine
        self.state_machine = sfn.StateMachine(
            self,
            "CarteraFinaincoTrgStateMachine",
            state_machine_name=create_name('sfn', 'adls-carterafinainco-trg'),
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            logs=sfn.LogOptions(destination=log_group, level=sfn.LogLevel.ALL, include_execution_data=True),
        )

        # Grant invoke permissions to the state machine role for each Lambda used
        # lambda_functions = [
        #     insertaud_fn,
        #     incredata_fn,
        #     fulldata_fn,
        #     success_fn,
        #     daily_fn,
        # ]
        # for fn in lambda_functions:
        #     self.state_machine.add_to_role_policy(
        #         iam.PolicyStatement(
        #             actions=["lambda:InvokeFunction"],
        #             resources=[fn.function_arn],
        #         )
        #     )

        # Grant publish permission for SNS topic
        self.state_machine.add_to_role_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=[failure_topic.topic_arn],
            )
        )