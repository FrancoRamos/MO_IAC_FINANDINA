import os
import time
import json
import boto3

# Cliente Redshift Data API
rs = boto3.client('redshift-data')

# Variables de entorno con valores por defecto
SECRET_ARN = os.environ.get(
    'REDSHIFT_SECRET_ARN',
    'arn:aws:secretsmanager:us-east-1:637423369807:secret:redshift/finandina/dev-CkFGPc'
)
WORKGROUP = os.environ.get('REDSHIFT_WORKGROUP', 'dl-workgroup-dev-rs')
DATABASE  = os.environ.get('REDSHIFT_DATABASE', 'dl_dev')

POLL_INTERVAL_SEC = float(os.environ.get('POLL_INTERVAL', '1.0'))
POLL_TIMEOUT_SEC  = int(os.environ.get('POLL_TIMEOUT_SEC', '120'))


def _fix_iso_ts(v: str | None):
    """'2025-10-03T17:19:26.038Z' -> '2025-10-03 17:19:26.038'"""
    if not v:
        return v
    return v.replace('T', ' ').replace('Z', '')

def _exec(sql, params=None):
    """Ejecuta una sentencia SQL en Redshift Serverless vía Data API."""
    args = {
        "WorkgroupName": WORKGROUP,
        "SecretArn": SECRET_ARN,
        "Sql": sql
    }
    if DATABASE:
        args["Database"] = DATABASE

    if params:
        args["Parameters"] = [
            {"name": str(k), "value": "" if v is None else str(v)}
            for k, v in params.items()
        ]

    sid = rs.execute_statement(**args)["Id"]

    waited = 0.0
    while True:
        d = rs.describe_statement(Id=sid)
        st = d["Status"]

        if st in ("FAILED", "ABORTED"):
            # Propaga el mensaje de error del Data API
            raise RuntimeError(d.get("Error") or f"{sid} {st}")

        if st == "FINISHED":
            if d.get("HasResultSet"):
                return rs.get_statement_result(Id=sid)
            return {"ok": True, "StatementId": sid}

        time.sleep(POLL_INTERVAL_SEC)
        waited += POLL_INTERVAL_SEC
        if waited > POLL_TIMEOUT_SEC:
            raise TimeoutError(f"Timeout esperando resultado del statement {sid}")

def lambda_handler(event, context):
    print("EVENT:", json.dumps(event, ensure_ascii=False))
    """Lambda handler para ejecutar consultas en Redshift."""
    action = event.get("action", "select_one")

    if action == "call":
        sql = event["sql"]
        params = event.get("params") or {}
        # Normaliza posibles timestamps ISO para que casteen a ::timestamp sin problemas
        for k in ("fechaFin", "fechaInsertUpdate", "fechaInicio"):
            v = params.get(k)
            if isinstance(v, str):
                params[k] = _fix_iso_ts(v)
        return _exec(sql, params)

    elif action == "select_one":
        if "sql" in event:
            res = _exec(event["sql"])
        else:
            tbl = event["tableName"]
            q = f"SELECT valorPivot FROM dbo.controlPipelineLight_gen2 WHERE nombreTabla = '{tbl}' AND estado = 1 ORDER BY fechaInsertUpdate DESC LIMIT 1"
            res = _exec(q)

        # Extrae valor si hay resultado
        if "Records" in res and res["Records"]:
            cell = res["Records"][0][0]
            return {"value": next(iter(cell.values()))}
        return {"value": None}

    elif action in ("unload", "copy", "select"):
        return _exec(event["sql"])
        
    else:
        raise ValueError(f"Unknown action {action}")
