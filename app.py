#!/usr/bin/env python3
import aws_cdk as cdk
from aws_cdk import aws_secretsmanager as secretsmanager

from infra.utils.environments import environments
from infra.lib.foundations import FoundationsStack, FoundationsStackProps
from infra.lib.etl import EtlStack, EtlStackProps
from infra.lib.data_migration import DataMigrationStack, DataMigrationStackProps
from infra.lib.analytics_consumption import AnalyticsConsumptionStack, AnalyticsConsumptionStackProps
from infra.utils.naming import create_name


app = cdk.App()

# Context configuration (required)
# config = app.node.try_get_context("config")
# if not config:
#     raise ValueError("Context variable missing on CDK command. Pass in as '-c config=XXX'")
config = 'dev'
context_env = environments[config]
region = context_env.region
environment = context_env.environment
account_id = context_env.accountId
project = context_env.project

# === FOUNDATIONS STACK ===
stack_foundations = FoundationsStack(
    app,
    create_name("stack", "foundations"),
    env=cdk.Environment(account=account_id, region=region),
    props=FoundationsStackProps(context_env=context_env),
)

# Import Redshift secret
redshift_secret = secretsmanager.Secret.from_secret_name_v2(
    stack_foundations,
    "RedshiftSecretImported",
    "dl-redshift-admin-dev-secrets"  # tu secreto
)
redshift_secret_arn = redshift_secret.secret_arn

# === ETL STACK ===
stack_etl = EtlStack(
    app,
    create_name("stack", "etl"),
    env=cdk.Environment(account=account_id, region=region),
    props=EtlStackProps(
        context_env=context_env,
        storage=stack_foundations.storage,
        metadata_catalog=stack_foundations.metadata_catalog,
        security=stack_foundations.security,
        raw_database_name=stack_foundations.governance.raw_database_name,
        master_database_name=stack_foundations.governance.master_database_name,
        analytics_database_name=stack_foundations.governance.analytics_database_name,
        datalake_lib_layer=stack_foundations.datalake_layer,
        redshift_jdbc_url=None,
        redshift_secret_arn=redshift_secret_arn,
    ),
)

# # === DATA MIGRATION STACK ===
# stack_data_migration = DataMigrationStack(
#     app,
#     create_name("stack", "data-migration"),
#     env=cdk.Environment(account=account_id, region=region),
#     props=DataMigrationStackProps(
#         raw_bucket=stack_foundations.storage.raw_bucket
#     ),
# )

# # === ANALYTICS CONSUMPTION STACK ===
# stack_analytics = AnalyticsConsumptionStack(
#     app,
#     create_name("stack", "analytics"),
#     env=cdk.Environment(account=account_id, region=region),
#     props=AnalyticsConsumptionStackProps(
#         context_env=context_env,
#         analytics_bucket=stack_foundations.storage.analytics_bucket,
#         data_kms_key=stack_foundations.security.kms_key,
#     ),
# )

# # === TAGS ===
# for stack in [stack_foundations, stack_etl, stack_data_migration, stack_analytics]:
#     cdk.Tags.of(stack).add("Project", project)
#     cdk.Tags.of(stack).add("Environment", environment)

# # === DEPENDENCIES ===
# stack_analytics.add_dependency(stack_data_migration)

# === SYNTHESIZE APP ===
app.synth()
