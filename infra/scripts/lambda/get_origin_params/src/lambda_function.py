import os, time, json, boto3

rs = boto3.client('redshift-data')

SECRET_ARN = os.environ.get('REDSHIFT_SECRET_ARN', 'arn:aws:secretsmanager:us-east-1:637423369807:secret:redshift/finandina/dev-CkFGPc')
WORKGROUP  = os.environ.get('REDSHIFT_WORKGROUP',  'dl-workgroup-dev-rs')
DATABASE   = os.environ.get('REDSHIFT_DATABASE',   'dl_dev')

POLL_INTERVAL = float(os.environ.get('POLL_INTERVAL', '0.7'))
POLL_TIMEOUT  = int(os.environ.get('POLL_TIMEOUT_SEC', '60'))

def exec_sql(sql: str, params: dict | None = None):
    args = {
        "WorkgroupName": WORKGROUP,
        "SecretArn": SECRET_ARN,
        "Sql": sql
    }
    if DATABASE:
        args["Database"] = DATABASE

    # ← CAMBIO CLAVE: usar 'Parameters' (no 'SqlParameters')
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
            raise RuntimeError(d.get("Error") or f"{sid} {st}")
        if st == "FINISHED":
            return rs.get_statement_result(Id=sid) if d.get("HasResultSet") else {"Records": [], "ColumnMetadata": []}
        time.sleep(POLL_INTERVAL)
        waited += POLL_INTERVAL
        if waited > POLL_TIMEOUT:
            raise TimeoutError(f"Timeout esperando resultado del statement {sid}")

def get_cell(cell_dict):
    """Convierte una celda del Data API a su valor Python."""
    if not isinstance(cell_dict, dict) or not cell_dict:
        return None
    # Prioriza null explícito
    if "isNull" in cell_dict and cell_dict["isNull"]:
        return None
    # Toma el primer valor presente (stringValue, longValue, doubleValue, booleanValue)
    for key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
        if key in cell_dict:
            return cell_dict[key]
    # Fallback: primer valor cualquiera
    return next(iter(cell_dict.values()))

def _extract_id_conf(event):
    if "idConfigOrigen" in event:
        return event["idConfigOrigen"]
    if isinstance(event.get("table"), dict) and "idConfigOrigen" in event["table"]:
        return event["table"]["idConfigOrigen"]
    v = event.get("idConfigOrigen") or (event.get("table") or {}).get("idConfigOrigen")
    if isinstance(v, dict) and v:
        return next(iter(v.values()))
    raise ValueError("Falta idConfigOrigen en el evento de entrada")

def lambda_handler(event, context):
    raw_id = _extract_id_conf(event)
    try:
        id_conf = int(raw_id)
    except Exception as e:
        raise ValueError(f"idConfigOrigen inválido: {raw_id}") from e

    sql = """
    SELECT IdConfOrigen, nombreOrigen, fechainsertupdate, nombreesquema, baseorigen, pathkey, pathorigen
    FROM dbo.configuracionOrigen
    WHERE IdConfOrigen = :idc
    """
    res = exec_sql(sql, params={"idc": id_conf})

    if not res.get("Records"):
        return {}

    r = res["Records"][0]
    # Cada r[i] es un dict con stringValue/longValue/etc.
    out = {
        "IdConfOrigen":    get_cell(r[0]),
        "nombreOrigen":    get_cell(r[1]),
        "fechaInsertUpdate": get_cell(r[2]),
        "nombreEsquema":   get_cell(r[3]),
        "baseOrigen":      get_cell(r[4]),
        "pathKey":         get_cell(r[5]),
        "pathOrigen":      get_cell(r[6]),
    }

    print(json.dumps({"input": event, "output": out}, ensure_ascii=False))
    return out
