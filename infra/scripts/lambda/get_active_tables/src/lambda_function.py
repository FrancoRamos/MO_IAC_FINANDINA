import os
import time
import json
import boto3
import re

rs = boto3.client('redshift-data')

# Puedes dejarlo hardcodeado o moverlo a env vars
SECRET_ARN = os.environ.get('REDSHIFT_SECRET_ARN',
                            "arn:aws:secretsmanager:us-east-1:637423369807:secret:redshift/finandina/dev-CkFGPc")
WORKGROUP  = os.environ.get('REDSHIFT_WORKGROUP', "dl-workgroup-dev-rs")
DATABASE   = os.environ.get('REDSHIFT_DATABASE',  "dl_dev")

POLL_INTERVAL    = float(os.environ.get('POLL_INTERVAL', '0.7'))
POLL_TIMEOUT_SEC = int(os.environ.get('POLL_TIMEOUT_SEC', '60'))

def exec_sql(sql: str):
    args = {
        "WorkgroupName": WORKGROUP,
        "SecretArn": SECRET_ARN,
        "Sql": sql
    }
    if DATABASE:
        args["Database"] = DATABASE

    sid = rs.execute_statement(**args)["Id"]
    waited = 0.0
    while True:
        d = rs.describe_statement(Id=sid)
        st = d["Status"]
        if st in ("FAILED", "ABORTED"):
            raise RuntimeError(d.get("Error") or f"Statement {sid} {st}")
        if st == "FINISHED":
            if d.get("HasResultSet"):
                return rs.get_statement_result(Id=sid)
            return {"Records": [], "ColumnMetadata": []}
        time.sleep(POLL_INTERVAL)
        waited += POLL_INTERVAL
        if waited > POLL_TIMEOUT_SEC:
            raise TimeoutError(f"Timeout esperando resultado del statement {sid}")

def _norm(name: str) -> str:
    """Normaliza nombres de columna a una forma estable (snake-like, lower)."""
    s = re.sub(r'[^0-9A-Za-z]+', '_', name).strip('_').lower()
    return s

def _cell(rec, i):
    """Extrae valor de la celda i de un registro del Data API."""
    c = rec[i]
    if c is None:
        return None
    return next(iter(c.values()), None)

def lambda_handler(event, context):
    """
    Entrada (ejemplos):
      {}
      {"idConfigOrigen": 16}
      {"idConfigOrigen": 3}

    Salida:
      {
        "count": N,
        "tables": [ {todas las columnas originales}, ... ],
        "sfn_view": [
          {
            nombreInstancia, direccionInstancia, nombreBaseDatos,
            nombreTabla, query, nombreCampoPivot, tipoCampoPivot,
            nombreCarpetaDL, idConfigOrigen
          }, ...
        ]
      }
    """
    id_conf = int(event.get("idConfigOrigen", 7))

    # Trae TODAS las columnas para ese origen y solo activas
    sql = f"""
    SELECT *
    FROM dbo.ConfigADFMetadataDriven
    WHERE idConfigOrigen = {id_conf} AND indEstadoTrigger = 1
    """
    res = exec_sql(sql)

    # Índices por nombre normalizado
    col_meta = res.get("ColumnMetadata", [])
    idx_map = { _norm(m["name"]): i for i, m in enumerate(col_meta) }

    records = res.get("Records", [])
    tables_full = []
    for rec in records:
        row = {}
        for _, i in idx_map.items():
            original_name = col_meta[i]["name"]
            row[original_name] = _cell(rec, i)
        tables_full.append(row)

    # Helper tolerante a variaciones de nombre
    def get_by_any(row, *candidates):
        for c in candidates:
            if c in row:
                return row[c]
            norm_candidates = [_norm(k) for k in row.keys()]
            if _norm(c) in norm_candidates:
                k2 = list(row.keys())[norm_candidates.index(_norm(c))]
                return row[k2]
        return None

    wanted = {
        "nombreInstancia":    ("nombreinstancia", "nombreInstancia"),
        "direccionInstancia": ("direccioninstancia", "direccionInstancia"),
        "nombreBaseDatos":    ("nombrebasedatos", "nombreBaseDatos"),
        "nombreTabla":        ("nombretabla", "nombreTabla"),
        "nombreCarpetaDL":    ("nombrecarpetadl", "nombreCarpetaDL"),
        "idConfigOrigen":     ("idconfigorigen", "idConfigOrigen"),
    }

    # --------- Vista combinada para Step Functions (UNIÓN de ambos bloques) ----------
    sfn_subset = []
    for row in tables_full:
        sfn_subset.append({
            # Campos de contexto (instancia, base, etc.)
            "nombreInstancia":    get_by_any(row, "nombreinstancia", "nombreInstancia"),
            "direccionInstancia": get_by_any(row, "direccioninstancia", "direccionInstancia"),
            "nombreBaseDatos":    get_by_any(row, "nombrebasedatos", "nombreBaseDatos"),

            # Campos de ejecución técnica
            "nombreTabla":        get_by_any(row, "nombretabla", "nombreTabla"),
            "query":              get_by_any(row, "query"),
            "nombreCampoPivot":   get_by_any(row, "nombrecampopivot", "nombreCampoPivot"),
            "tipoCampoPivot":     get_by_any(row, "tipocampopivot", "tipoCampoPivot"),
            "nombreCarpetaDL":    get_by_any(row, "nombrecarpetadl", "nombreCarpetaDL"),
            "idConfigOrigen":     get_by_any(row, "idconfigorigen", "idConfigOrigen"),
        })

    # --------- WHITELIST de tablas (opcional, para salida más limpia) ----------
    tables_min = []
    for row in tables_full:
        tables_min.append({
            "nombreInstancia":    get_by_any(row, "nombreinstancia", "nombreInstancia"),
            "direccionInstancia": get_by_any(row, "direccioninstancia", "direccionInstancia"),
            "nombreBaseDatos":    get_by_any(row, "nombrebasedatos", "nombreBaseDatos"),
            "nombreTabla":        get_by_any(row, "nombretabla", "nombreTabla"),
            "nombreCarpetaDL":    get_by_any(row, "nombrecarpetadl", "nombreCarpetaDL"),
            "idConfigOrigen":     get_by_any(row, "idconfigorigen", "idConfigOrigen"),
        })

    # --------- Salida final ----------
    out = {
        "count": len(tables_min),
        "tables": sfn_subset,
        "sfn_view": sfn_subset   # ← unión completa
    }
    print(json.dumps(out, ensure_ascii=False))
    return out

