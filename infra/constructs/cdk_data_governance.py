# infra/constructs/data_governance_construct.py
from constructs import Construct
from aws_cdk import (
    aws_ssm as ssm,
    aws_lakeformation as lakeformation,
    aws_glue as glue,
)
from dataclasses import dataclass

from ..utils.environments import environments as Environment
from ..utils.naming import create_name


@dataclass
class DataGovernanceConstructProps:
    context_env: dict


class DataGovernanceConstruct(Construct):

    def __init__(self, scope: Construct, construct_id: str, props: DataGovernanceConstructProps) -> None:
        super().__init__(scope, construct_id)

        context_env = props.context_env
        account_id = context_env.accountId
        region = context_env.region

        # Helper: Glue requires snake_case without hyphens
        def to_glue_id(svc: str, func: str) -> str:
            """Create valid Glue identifiers in snake_case."""
            return create_name(svc, func).replace("-", "_").lower()

        # --- Glue database names ---
        self.raw_database_name = to_glue_id("glue", "raw")
        self.master_database_name = to_glue_id("glue", "master")
        self.analytics_database_name = to_glue_id("glue", "analytics")

        # --- Create Glue databases (L1 resources) ---
        glue.CfnDatabase(
            self,
            "RawDatabase",
            catalog_id=account_id,
            database_input={"name": self.raw_database_name},
        )

        glue.CfnDatabase(
            self,
            "MasterDatabase",
            catalog_id=account_id,
            database_input={"name": self.master_database_name},
        )

        glue.CfnDatabase(
            self,
            "AnalyticsDatabase",
            catalog_id=account_id,
            database_input={"name": self.analytics_database_name},
        )

        # --- Register parameters in SSM for readability and stability ---
        # self._register_param(
        #     "raw-db-name", "Name of the raw Glue database", self.raw_database_name
        # )
        # self._register_param(
        #     "master-db-name", "Name of the master Glue database", self.master_database_name
        # )
        # self._register_param(
        #     "analytics-db-name",
        #     "Name of the analytics Glue database",
        #     self.analytics_database_name,
        # )

        cdk_exec_role_arn = f"arn:aws:iam::{account_id}:role/cdk-hnb659fds-cfn-exec-role-{account_id}-{region}"

        # Activar si se desea definir permisos de administración de Lake Formation
        lakeformation.CfnDataLakeSettings(
            self,
            "DataLakeSettings",
            admins=[
                {
                    "dataLakePrincipalIdentifier": cdk_exec_role_arn,
                }
            ],
            create_database_default_permissions=[],
            create_table_default_permissions=[],
            mutation_type="APPEND",
        )

    # --- Private helper to register SSM parameters ---
    def _register_param(self, name: str, description: str, value: str) -> None:
        ssm_base_path = f"/{create_name('ssm', 'governance')}"
        id_safe = f"{''.join(ch for ch in name if ch.isalnum())}Ssm"

        ssm.StringParameter(
            self,
            id_safe,
            parameter_name=f"{ssm_base_path}/{name}",
            description=description,
            string_value=value,
        )
