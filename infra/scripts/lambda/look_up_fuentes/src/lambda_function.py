# lambda_lookup_fuentes.py
import os, json
from common_redshift import exec_statement, fetch_one_value

SCHEMA = os.getenv("CONTROL_SCHEMA", "dbo")            # esquema donde guardaste metadatos
TABLE_MD = os.getenv("TABLE_METADATA", "configadfmetadatadriven")  # tabla de metadatos

# Cambios clave: ID por event, un solo SELECT, validaciones
def lambda_handler(event, context):
    id_cfg = int(event.get("id_config_origen", 2))  # default 34 si no llega
    sql = f"""
        SELECT
          query,
          nombretabla,
          nombrecarpetadl,
          COALESCE(tipocampopivot, '-') AS tipocampopivot
        FROM {SCHEMA}.{TABLE_MD}
        WHERE idconfigorigen = {id_cfg}
          AND indestadotrigger = 1
        LIMIT 1;
    """
    res = exec_statement(sql)
    if not res["records"]:
        return {"ok": False, "reason": "Sin metadatos o trigger inactivo", "metadata": None}

    row = res["records"][0]
    def cell(i):
        c = row[i]; return c.get("stringValue") or c.get("longValue") or c.get("doubleValue") or c.get("booleanValue")

    metadata = {
        "query": cell(0),
        "nombreTabla": (cell(1) or "").replace("[","").replace("]",""),
        "nombreCarpetaDL": cell(2),
        "tipoCampoPivot": cell(3)
    }
    if not metadata["query"]:
        return {"ok": False, "reason": "Query vacía", "metadata": None}
    return {"ok": True, "metadata": metadata}

