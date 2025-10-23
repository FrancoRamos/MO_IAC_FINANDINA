# scripts/redshift/bootstrap_external_schema.py
import json
import time
import boto3

redshift_data = boto3.client("redshift-data")

POLL_INTERVAL_SEC = 2
POLL_TIMEOUT_SEC = 300  # 5 minutes

def _poll_statement(statement_id: str):
    start = time.time()
    while True:
        resp = redshift_data.describe_statement(Id=statement_id)
        status = resp.get("Status")
        if status == "FINISHED":
            return resp
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Statement {statement_id} failed: {resp.get('Error')}")
        if time.time() - start > POLL_TIMEOUT_SEC:
            raise TimeoutError(f"Timed out waiting for statement {statement_id}")
        time.sleep(POLL_INTERVAL_SEC)

def _exec_sql(sql: str, workgroup: str, database: str, secret_arn: str | None):
    params = {
        "WorkgroupName": workgroup,
        "Database": database,
        "Sql": sql,
    }
    # Usa credenciales del Secret si se pasó (usuario/password DB)
    if secret_arn:
        params["SecretArn"] = secret_arn

    resp = redshift_data.execute_statement(**params)
    return _poll_statement(resp["Id"])

def _create_external_schema(external_schema: str, glue_db: str, role_arn: str) -> str:
    # Opción 1: SIN "CREATE EXTERNAL DATABASE IF NOT EXISTS"
    # Asume que el Glue Database YA existe en Glue/Lake Formation
    return (
        f'CREATE EXTERNAL SCHEMA IF NOT EXISTS "{external_schema}" '
        f"FROM DATA CATALOG DATABASE '{glue_db}' IAM_ROLE '{role_arn}';"
    )

def _drop_external_schema(external_schema: str) -> str:
    return f'DROP SCHEMA IF EXISTS "{external_schema}" CASCADE;'

def lambda_handler(event, _context):
    request_type = event.get("RequestType")
    props = event.get("ResourceProperties", {})

    workgroup = props["workgroupName"]
    database = props["database"]
    external_schema = props["externalSchemaName"]
    glue_db = props["glueDatabaseName"]
    role_arn = props["roleArn"]
    drop_on_delete = str(props.get("dropOnDelete", "false")).lower() == "true"
    secret_arn = props.get("secretArn")  # puede ser None

    physical_id = f"rs-bootstrap-{external_schema}"

    try:
        if request_type == "Delete":
            if drop_on_delete:
                _exec_sql(_drop_external_schema(external_schema), workgroup, database, secret_arn)
            return {"PhysicalResourceId": physical_id}

        # Create / Update
        sql = _create_external_schema(external_schema, glue_db, role_arn)
        _exec_sql(sql, workgroup, database, secret_arn)

        return {
            "PhysicalResourceId": physical_id,
            "Data": {"ExternalSchema": external_schema, "GlueDatabase": glue_db},
        }

    except Exception as e:
        raise RuntimeError(json.dumps({
            "error": str(e),
            "requestType": request_type,
            "physicalId": physical_id,
        }))
