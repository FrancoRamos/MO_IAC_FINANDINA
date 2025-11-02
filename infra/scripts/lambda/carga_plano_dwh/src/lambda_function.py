import json, time, os
import boto3

redshift = boto3.client("redshift-data")

# 🔒 Configuración fija
WORKGROUP = "dl-workgroup-dev-rs"  # esto no viene en el secret
SECRET_ARN = "arn:aws:secretsmanager:us-east-1:637423369807:secret:redshift/finandina/dev-CkFGPc"

def exec_and_wait(sql):
    resp = redshift.execute_statement(
        WorkgroupName=WORKGROUP,
        SecretArn=SECRET_ARN,
        Sql=sql
    )
    stmt_id = resp["Id"]

    while True:
        d = redshift.describe_statement(Id=stmt_id)
        status = d["Status"]

        if status in ("FINISHED", "FAILED", "ABORTED"):
            if status != "FINISHED":
                raise RuntimeError(f"SQL failed: {sql} | {d.get('Error')}")
            return {"sql": sql, "duration": d.get("Duration", -1)}
        time.sleep(2)

def lambda_handler(event, context):
    sql_list = event["sqlList"]
    results = [exec_and_wait(sql) for sql in sql_list]
    return {"ok": True, "executed": len(results), "details": results}
