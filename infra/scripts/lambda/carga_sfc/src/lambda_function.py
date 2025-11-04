import os
import time
import json
import boto3

# ======== Config (env vars con defaults sensatos) ========
rs = boto3.client('redshift-data')

SECRET_ARN = os.environ.get(
    'REDSHIFT_SECRET_ARN',
    'arn:aws:secretsmanager:us-east-1:637423369807:secret:redshift/finandina/dev-CkFGPc'
)
WORKGROUP = os.environ.get('REDSHIFT_WORKGROUP', 'dl-workgroup-dev-rs')
DATABASE  = os.environ.get('REDSHIFT_DATABASE', 'dl_dev')

POLL_INTERVAL_SEC = float(os.environ.get('POLL_INTERVAL', '1.0'))
POLL_TIMEOUT_SEC  = int(os.environ.get('POLL_TIMEOUT_SEC', '120'))

PROC_FQN = os.environ.get('REDSHIFT_PROC_FQN', '[sd].[uspLoad_Automatizacion]') 

def _exec_sql(sql: str):
    """Ejecuta un statement en Redshift Data API y espera a que termine."""
    args = {
        "WorkgroupName": WORKGROUP,
        "SecretArn": SECRET_ARN,
        "Sql": sql,
    }
    if DATABASE:
        args["Database"] = DATABASE

    sid = rs.execute_statement(**args)["Id"]

    waited = 0.0
    while True:
        d = rs.describe_statement(Id=sid)
        st = d["Status"]

        if st in ("FAILED", "ABORTED"):
            # Propaga el mensaje detallado de Redshift Data API si existe
            raise RuntimeError(d.get("Error") or f"{sid} {st}")

        if st == "FINISHED":
            return {
                "ok": True,
                "statementId": sid,
                "hasResultSet": d.get("HasResultSet", False),
                "rowCount": d.get("ResultRows", 0),
            }

        time.sleep(POLL_INTERVAL_SEC)
        waited += POLL_INTERVAL_SEC
        if waited > POLL_TIMEOUT_SEC:
            raise TimeoutError(f"Timeout esperando resultado del statement {sid}")

def run_automatizacion():
    """Llama únicamente al proc [sd].[uspLoad_Automatizacion]."""
    # Si tu proc no recibe parámetros:
    sql = f"CALL {PROC_FQN}();"
    return _exec_sql(sql)

def lambda_handler(event, context):
    # Ejecuta siempre y solo el SP; el 'event' se ignora a propósito
    res = run_automatizacion()
    # Log simple y respuesta JSON
    print("SP ejecutado:", json.dumps(res, ensure_ascii=False))
    return {
        "message": f"Ejecutado {PROC_FQN}",
        "result": res
    }
