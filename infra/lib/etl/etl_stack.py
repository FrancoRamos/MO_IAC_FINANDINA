# infra/stacks/etl_stack.py
from aws_cdk import Stack
from constructs import Construct
from dataclasses import dataclass

from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3_deployment as s3_deployment, aws_sns_subscriptions as subs
from aws_cdk import aws_sns as sns
from aws_cdk import aws_iam as iam
from aws_cdk import RemovalPolicy
import os

from infra.constructs.cdk_storage import StorageConstruct
from infra.constructs.cdk_metadata_catalog import MetadataCatalogConstruct
from infra.constructs.cdk_security import SecurityConstruct

from infra.etl_stepfunction.BASE import EtlTfBaseConstruct, EtlTfBaseConstructProps
from infra.etl_stepfunction.BASE import EtlSfBaseConstruct, EtlSfBaseConstructProps
from infra.etl_stepfunction.AUTOMATIZACION.CET import EtlTfGeneralCETDiarioConstruct, EtlTfGeneralCETDiarioConstructProps
from infra.etl_stepfunction.AUTOMATIZACION.CET import EtlSfGeneralCETDiarioConstruct, EtlSfGeneralCETDiarioConstructProps

from infra.etl_stepfunction.COBRANZA.PLANO_MAYOR.CARGA_PLANO_DWH import EtlTfCargaPlanoDWHConstruct, EtlTfCargaPlanoDWHConstructProps
from infra.etl_stepfunction.COBRANZA.PLANO_MAYOR.CARGA_PLANO_DWH import EtlSfCargaPlanoDWHConstruct, EtlSfCargaPlanoDWHConstructProps

from infra.etl_stepfunction.FUENTES.SD import PlTxtSf, PlTxtSfProps

from infra.etl_stepfunction.FUENTES.PLANOS.DS_TO_FILE_XLSX import PlXlsxTf, TfProps
from infra.etl_stepfunction.FUENTES.PLANOS.DS_TO_FILE_XLSX import PlXlsxSf, SfProps

from infra.etl_stepfunction.FUENTES.FIVE9.ADLS_CET_FIVE9 import EtlTfAdlsCetFive9Construct, EtlTfAdlsCetFive9ConstructProps
from infra.etl_stepfunction.FUENTES.FIVE9.ADLS_CET_FIVE9 import EtlSfAdlsCetFive9Construct, EtlSfAdlsCetFive9ConstructProps

from infra.etl_stepfunction.AUTOMATIZACION.SD import EtlTfGeneralSDDiarioConstruct, EtlTfGeneralSDDiarioConstructProps
from infra.etl_stepfunction.AUTOMATIZACION.SD import EtlSfGeneralSDDiarioConstruct, EtlSfGenetalSDDiarioConstructProps

from infra.etl_stepfunction.FUENTES.FABOGRIESGO.ADLS_ALERTASFRAUDE import EtlSfAdlsFaboAlertasFraudeConstruct, EtlSfAdlsFaboAlertasFraudeConstructProps

from infra.etl_stepfunction.FUENTES.FABOGRIESGO.ADLS_AGIL import EtlSfAdlsFaboAgilConstruct, EtlSfAdlsFaboAgilConstructProps
from infra.etl_stepfunction.FUENTES.FABOGRIESGO.ADLS_AGIL import EtlTfAdlsFaboAgilConstruct, EtlTfAdlsFaboAgilConstructProps

from infra.etl_stepfunction.FUENTES.FABOGSQLCLU.ADLS_GESTIONCLIENTE import EtlTfAdlsFaboGestionClienteConstruct, EtlTfAdlsFaboGestionClienteConstructProps
from infra.etl_stepfunction.FUENTES.FABOGSQLCLU.ADLS_GESTIONCLIENTE import EtlSfAdlsFaboGestionClienteConstruct, EtlSfAdlsFaboGestionClienteConstructProps

from infra.etl_stepfunction.FUENTES.FABOGRIESGO.ADLS_INSUMOS_AS400 import EtlSfAdlsFaboInsumosAs400Construct, EtlSfAdlsFaboInsumosAs400ConstructProps 
from infra.etl_stepfunction.FUENTES.FABOGRIESGO.ADLS_INSUMOS_AS400 import EtlTfAdlsFaboInsumosAs400Construct, EtlTfAdlsFaboInsumosAs400ConstructProps 

from infra.etl_stepfunction.FUENTES.FABOGRIESGO.ADLS_VEHICULOS_BIC import EtlSfAdlsFaboVehiculosBic, EtlSfAdlsFaboVehiculosBicProps

from ...utils.naming import create_name

@dataclass
class EtlStackProps:
    context_env: dict
    storage: StorageConstruct
    metadata_catalog: MetadataCatalogConstruct
    security: SecurityConstruct
    raw_database_name: str
    master_database_name: str
    analytics_database_name: str
    datalake_lib_layer: _lambda.LayerVersion  # ARN del layer de la librería de datalake
    redshift_jdbc_url: str | None             # opcional
    redshift_secret_arn: str                  # Secret con credenciales Redshift


class EtlStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, props: EtlStackProps, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        context_env = props.context_env
        storage = props.storage
        metadata_catalog = props.metadata_catalog
        security = props.security

        # Deploy local Glue ETL job scripts to S3 bucket
        s3_deployment.BucketDeployment(
            self,
            "UploadGlueJobs",
            sources=[s3_deployment.Source.asset(os.path.join(os.path.dirname(os.path.dirname(__file__)), "../etl_glue_jobs"))],
            destination_bucket=storage.scripts_bucket,
        )  


        # --- Notification emails ---
        email_addresses = [
            # "ocastro@morrisopazo.com",
            # "mmolina@morrisopazo.com",
            # TODO
            "jtorres@morrisopazo.com"
        ]
        # --- SNS Topic + Subscriptions ---
        self.failure_topic = sns.Topic(
            self,
            "StepFunctionFailureTopic",
            topic_name=create_name('sns-topic', 'failure'),
        )
        self.failure_topic.apply_removal_policy(RemovalPolicy.DESTROY) #TODO
        
        self.failure_topic.add_to_resource_policy(
            iam.PolicyStatement(
                sid="__default_statement_ID",
                effect=iam.Effect.ALLOW,
                principals=[iam.ArnPrincipal("*")],
                actions=[
                    "SNS:GetTopicAttributes",
                    "SNS:SetTopicAttributes",
                    "SNS:AddPermission",
                    "SNS:RemovePermission",
                    "SNS:DeleteTopic",
                    "SNS:Subscribe",
                    "SNS:ListSubscriptionsByTopic",
                    "SNS:Publish"
                ],
                resources=[self.failure_topic.topic_arn],
                conditions={
                    "StringEquals": {
                        "AWS:SourceOwner": context_env.accountId
                    }
                }
            )
        )
        
        for email in email_addresses:
            self.failure_topic.add_subscription(subs.EmailSubscription(email))


        # --- Etl Base Transform ---
        etl_tf_base_props = EtlTfBaseConstructProps(
            environment=context_env.environment,
            region=context_env.region,
            account=context_env.accountId,
            raw_bucket=storage.raw_bucket,
            master_bucket=storage.master_bucket,
            analytics_bucket=storage.analytics_bucket,
            scripts_bucket=storage.scripts_bucket,
            tracking_table=metadata_catalog.tracking_table,
            raw_database=props.raw_database_name,
            master_database=props.master_database_name,
            analytics_database=props.analytics_database_name,
            datalake_lib_layer_arn=props.datalake_lib_layer.layer_version_arn,
            lambda_execution_role=security.lambdaExecutionRole,
            job_role=security.lakeFormationRole,
            kms_key=security.kmsKey,
            redshift_secret_arn=props.redshift_secret_arn,
            redshift_workgroup_name="dl-workgroup-dev-rs",
            redshift_role = "arn:aws:iam::637423369807:role/redshift-finandina-role",
        )

        etl_tf_base = EtlTfBaseConstruct(
            self,
            "EtlTransformBase",
            props=etl_tf_base_props,
        )

        # --- Etl Base Step Function ---
        etl_sf_base_props = EtlSfBaseConstructProps(
            environment=context_env.environment,
            region=context_env.region,
            pre_update_lambda=etl_tf_base.preupdate_lambda,
            post_update_lambda=etl_tf_base.postupdate_lambda,
            error_lambda=etl_tf_base.error_lambda,
            job_raw_to_master_name=etl_tf_base.job_raw_to_master_name,
            job_master_to_analytics_name=etl_tf_base.job_master_to_analytics_name,
            bucket_result_rma01=storage.master_bucket.bucket_name,
            bucket_result_maa01=storage.analytics_bucket.bucket_name,
        )

        etl_sf_base = EtlSfBaseConstruct(
            self,
            "EtlStepFunctionsBase",
            props=etl_sf_base_props,
        )


        # --- Five9 Fuente Transform ---
        etl_tf_five9_props = EtlTfAdlsCetFive9ConstructProps(
            environment=context_env.environment,
            region=context_env.region,
            account=context_env.accountId,
            raw_bucket=storage.raw_bucket,
            scripts_bucket=storage.scripts_bucket,
            raw_database=props.raw_database_name,
            datalake_lib_layer_arn=props.datalake_lib_layer.layer_version_arn,
            lambda_execution_role=security.lambdaExecutionRole,
            job_role=security.lakeFormationRole,
            kms_key=security.kmsKey,
            redshift_database="dl_dev",
            redshift_secret_arn=props.redshift_secret_arn,
            redshift_workgroup_name="dl-workgroup-dev-rs",
        )

        etl_tf_five9 = EtlTfAdlsCetFive9Construct(
            self,
            "EtlTfAdlsCetFive9",
            props=etl_tf_five9_props,
        )

        # --- Five9 Fuente Step Function ---
        etl_sf_adls_cet_five9_props = EtlSfAdlsCetFive9ConstructProps(
            environment=context_env.environment,
            region=context_env.region,
            account=context_env.accountId,
            lookup_redshift_fn=etl_tf_five9.lambda_lookup1,
        )

        etl_sf_adls_cet_five9 = EtlSfAdlsCetFive9Construct(
            self,
            "EtlSfAdlsCetFive9",
            props=etl_sf_adls_cet_five9_props,
        )


        # --- Fuente Experian (SD) Step Function ---
        sd_txt_sf_props = PlTxtSfProps(
                environment=context_env.environment,
                workgroup_name="dl-workgroup-dev-rs",
                database="dl_dev",
                db_user_secret_arn=props.redshift_secret_arn,
                s3_prefix_uri=f"s3://{storage.master_bucket.bucket_name}/Finandina/Planos/Experian/Input/BaseExperian_",
                failure_topic=self.failure_topic,
            )

        sd_txt_sf = PlTxtSf(
            self,
            "SdBaseExperianSF",
            props=sd_txt_sf_props
        )


        # --- Exportar XLSX Transform ---
        export_prefix = "Finandina/Planos/DWH/Taximetro/"

        pl_xlsx_tf_props = TfProps(
                export_prefix=export_prefix,
                redshift_secret_arn=props.redshift_secret_arn,
                notification_email="mmolina@morrisopazo.com",
            )

        pl_xlsx_tf = PlXlsxTf(
            self,
            "PlAsdwhXlsxTF",
            props=pl_xlsx_tf_props
        )

        # --- Exportar XLSX Step Function ---
        pl_xlsx_sf_props = SfProps(
                sfn_role=pl_xlsx_tf.sfn_role,
                export_bucket_name=pl_xlsx_tf.export_bucket.bucket_name,
                export_prefix=export_prefix,
                redshift_database="dl_dev",
                redshift_secret_arn=props.redshift_secret_arn,
                redshift_workgroup_name="dl-workgroup-dev-rs",
                failure_topic=self.failure_topic,
            )

        pl_xlsx_sf = PlXlsxSf(
            self,
            "PlAsdwhXlsxSF",
            props=pl_xlsx_sf_props
        )

        # ======================================================================================
        # ======================================================================================

        # --- CET Diario Transform ---
        etl_tf_cet_diario_props = EtlTfGeneralCETDiarioConstructProps(
            environment=context_env.environment,
            raw_bucket=storage.raw_bucket,
            master_bucket=storage.master_bucket,
            scripts_bucket=storage.scripts_bucket,
            raw_database=props.raw_database_name,
            master_database=props.master_database_name,
            datalake_lib_layer_arn=props.datalake_lib_layer.layer_version_arn,
            lambda_execution_role=security.lambdaExecutionRole,
            job_role=security.lakeFormationRole,
            kms_key=security.kmsKey,
        )

        etl_tf_cet_diario = EtlTfGeneralCETDiarioConstruct(
            self,
            "EtlTfGenetalCETDiario",
            props=etl_tf_cet_diario_props,
        )

        # --- CET Diario Step Function ---
        etl_sf_general_cet_diario_props = EtlSfGeneralCETDiarioConstructProps(
            environment=context_env.environment,
            region=context_env.region,
            pre_update_lambda=etl_tf_base.preupdate_lambda,
            post_update_lambda=etl_tf_base.postupdate_lambda,
            error_lambda=etl_tf_base.error_lambda,
            bucket_result_rma01=storage.master_bucket.bucket_name,
            bucket_result_maa01=storage.analytics_bucket.bucket_name,
            job_raw_cet=etl_tf_cet_diario.job_raw_cet,
            job_raw_to_master_tra_cet=etl_tf_cet_diario.job_raw_to_master_tra_cet,
            job_master_to_analytics_load_cet=etl_tf_cet_diario.job_master_to_analytics_load_cet,
            failure_topic=self.failure_topic,
        )

        etl_sf_general_cet_diario = EtlSfGeneralCETDiarioConstruct(
            self,
            "EtlSfGeneralCETDiario",
            props=etl_sf_general_cet_diario_props,
        )


        # --- Cobranza Carga Plano DWH Transform ---
        plano_mayor_tf_props = EtlTfCargaPlanoDWHConstructProps(
            environment=context_env.environment,
            project=context_env.project,
            scripts_bucket=storage.scripts_bucket,
            job_role=security.lakeFormationRole,
            lambda_execution_role=security.lambdaExecutionRole,
        )

        plano_mayor_tf = EtlTfCargaPlanoDWHConstruct(
            self,
            "CobranzaPlanoMayorTF",
            props=plano_mayor_tf_props,
        )

        # --- Cobranza Carga Plano DWH Step Function ---
        plano_mayor_sf_props = EtlSfCargaPlanoDWHConstructProps(
            environment=context_env.environment,
            job_carga_plano_dwh_name=plano_mayor_tf.job_carga_plano_dwh_name,
            carga_plano_dwh_lambda= plano_mayor_tf.carga_plano_dwh_lambda,
            failure_topic=self.failure_topic,
        )

        plano_mayor_sf = EtlSfCargaPlanoDWHConstruct(
            self,
            "CobranzaPlanoMayorSF",
            props=plano_mayor_sf_props,
        )


        # --- Automatizacion SD Diario Transform ---
        etl_tf_sd_diario_props = EtlTfGeneralSDDiarioConstructProps(
            environment=context_env.environment,
            scripts_bucket=storage.scripts_bucket,
            lambda_execution_role=security.lambdaExecutionRole,
            job_role=security.lakeFormationRole,
            kms_key=security.kmsKey,
        )

        etl_tf_sd_diario = EtlTfGeneralSDDiarioConstruct(
            self,
            "EtlTfGeneralSDDiario",
            props=etl_tf_sd_diario_props,
        )
        
        # --- Automatizacion SD Diario Step Function ---
        etl_sf_sd_diario_props = EtlSfGenetalSDDiarioConstructProps(
            environment=context_env.environment,
            region=context_env.region,
            autom_lambda=etl_tf_sd_diario.autom_lambda,
            job_load_sd=etl_tf_sd_diario.job_raw_name,
            raw_bucket=storage.raw_bucket,
            master_bucket=storage.master_bucket,
            raw_database=props.raw_database_name,
            master_database=props.master_database_name,
            failure_topic=self.failure_topic,
        )

        etl_sf_sd_diario = EtlSfGeneralSDDiarioConstruct(
            self,
            "EtlSfGeneralSDDiario",
            props=etl_sf_sd_diario_props,
        )
        
        
        # --- Fuentes Faboriesgo Alertas Fraude Step Function ---
        etl_sf_fabo_alertasfraude_props = EtlSfAdlsFaboAlertasFraudeConstructProps(
            environment=context_env.environment,
            get_active_tables_fn=etl_tf_base.get_active_tables_lambda,
            get_origin_params_fn=etl_tf_base.get_origin_params_lambda,
            sql_runner_fn=etl_tf_base.sql_runner_lambda,
            read_metrics_fn=etl_tf_base.read_metrics_lambda,
            glue_extractcopy_job_name=etl_tf_base.job_extract_copy_name,
            glue_tdc_job_name=etl_tf_base.job_tdc_name,
            failure_topic=etl_tf_base.sns_failure_topic,
            raw_bucket_name=storage.raw_bucket.bucket_name,
        )

        etl_sf_fabo_alertasfraude = EtlSfAdlsFaboAlertasFraudeConstruct(
            self,
            "EtlSfFaboAlertasFraude",
            props=etl_sf_fabo_alertasfraude_props,
        )
        

        # --- Fuentes Faboriesgo ADSL Agil Transform ---
        etl_tf_fabo_agil_props = EtlTfAdlsFaboAgilConstructProps(
            environment=context_env.environment,
            scripts_bucket=storage.scripts_bucket,
            job_role=security.lakeFormationRole,
        )
        
        etl_tf_fabo_agil = EtlTfAdlsFaboAgilConstruct(
            self,
            "EtlTfFaboAgil",
            props=etl_tf_fabo_agil_props,
        )
        
        # --- Fuentes Faboriesgo ADSL Agil Step Function ---
        etl_sf_fabo_agil_props = EtlSfAdlsFaboAgilConstructProps(
            environment=context_env.environment,
            get_active_tables_fn=etl_tf_base.get_active_tables_lambda,
            get_origin_params_fn=etl_tf_base.get_origin_params_lambda,
            sql_runner_fn=etl_tf_base.sql_runner_lambda,
            read_metrics_fn=etl_tf_base.read_metrics_lambda,
            glue_extractcopy_job_name=etl_tf_base.job_extract_copy_name,
            glue_bpmpro_job_name=etl_tf_fabo_agil.job_bpmpro_name,
            failure_topic=etl_tf_base.sns_failure_topic,
            raw_bucket_name=storage.raw_bucket.bucket_name,
        )

        etl_sf_fabo_agil = EtlSfAdlsFaboAgilConstruct(
            self,
            "EtlSfFaboAgil",
            props=etl_sf_fabo_agil_props,
        )
        
        # --- Fuentes FaboGSQLCLU ADSL Gestion Cliente Transform ---
        etl_tf_fabo_gestion_cliente_props = EtlTfAdlsFaboGestionClienteConstructProps(
            environment=context_env.environment,
            scripts_bucket=storage.scripts_bucket,
            job_role=security.lakeFormationRole,
        )
        
        etl_tf_fabo_gestion_cliente = EtlTfAdlsFaboGestionClienteConstruct(
            self,
            "EtlTfFaboGestionCliente",
            props=etl_tf_fabo_gestion_cliente_props,
        )
        
        # --- Fuentes FaboGSQLCLU ADSL Gestion Cliente Step Function ---
        etl_sf_fabo_agil_props = EtlSfAdlsFaboGestionClienteConstructProps(
            environment=context_env.environment,
            get_active_tables_fn=etl_tf_base.get_active_tables_lambda,
            get_origin_params_fn=etl_tf_base.get_origin_params_lambda,
            sql_runner_fn=etl_tf_base.sql_runner_lambda,
            read_metrics_fn=etl_tf_base.read_metrics_lambda,
            glue_extractcopy_job_name=etl_tf_base.job_extract_copy_name,
            glue_gestioncliente_job_name=etl_tf_fabo_gestion_cliente.job_gestioncliente_name,
            failure_topic=etl_tf_base.sns_failure_topic,
            raw_bucket_name=storage.raw_bucket.bucket_name,
        )

        etl_sf_fabo_agil = EtlSfAdlsFaboGestionClienteConstruct(
            self,
            "EtlSfFaboGestionCliente",
            props=etl_sf_fabo_agil_props,
        )
        
        # --- Fuentes Faboriesgo ADSL Insumos AS400 Transform ---
        etl_tf_fabo_insumos_as400_props = EtlTfAdlsFaboInsumosAs400ConstructProps(
            environment=context_env.environment,
            scripts_bucket=storage.scripts_bucket,
            job_role=security.lakeFormationRole,
        )
        
        etl_tf_fabo_insumos_as400 = EtlTfAdlsFaboInsumosAs400Construct(
            self,
            "EtlTfFaboInsumosAS400",
            props=etl_tf_fabo_insumos_as400_props,
        )
        
        # --- Fuentes Faboriesgo ADSL Insumos AS400 Step Function ---
        etl_sf_fabo_insumos_as400_props = EtlSfAdlsFaboInsumosAs400ConstructProps(
            environment=context_env.environment,
            get_active_tables_fn=etl_tf_base.get_active_tables_lambda,
            get_origin_params_fn=etl_tf_base.get_origin_params_lambda,
            sql_runner_fn=etl_tf_base.sql_runner_lambda,
            read_metrics_fn=etl_tf_base.read_metrics_lambda,
            glue_extractcopy_job_name=etl_tf_base.job_extract_copy_name,
            glue_insumos_as400_job_name=etl_tf_fabo_insumos_as400.job_insumos_as400_name,
            failure_topic=etl_tf_base.sns_failure_topic,
            raw_bucket_name=storage.raw_bucket.bucket_name,
        )

        etl_sf_fabo_insumos_as400 = EtlSfAdlsFaboInsumosAs400Construct(
            self,
            "EtlSfFaboInsumosAS400",
            props=etl_sf_fabo_insumos_as400_props,
        )
        
        
        # --- Fuentes Faboriesgo ADSL Vehiculos BIC Step Function ---
        etl_sf_fabo_vehiculos_bic_props = EtlSfAdlsFaboVehiculosBicProps(
            environment=context_env.environment,
            get_active_tables_fn=etl_tf_base.get_active_tables_lambda,
            get_origin_params_fn=etl_tf_base.get_origin_params_lambda,
            sql_runner_fn=etl_tf_base.sql_runner_lambda,
            read_metrics_fn=etl_tf_base.read_metrics_lambda,
            glue_extractcopy_job_name=etl_tf_base.job_extract_copy_name,
            failure_topic=etl_tf_base.sns_failure_topic,
            raw_bucket_name=storage.raw_bucket.bucket_name,
        )

        etl_sf_fabo_vehiculos_bic = EtlSfAdlsFaboVehiculosBic(
            self,
            "EtlSfFaboVehiculosBic",
            props=etl_sf_fabo_vehiculos_bic_props,
        )

        
