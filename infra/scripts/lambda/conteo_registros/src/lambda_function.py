# lambda_conteo_registros.py (versión tolerante)
import json
from common_redshift import exec_statement, fetch_one_value

def _count_from_object(obj: str) -> int:
    sql = f"SELECT COUNT(*) FROM {obj};"
    res = exec_statement(sql)
    return int(fetch_one_value(res['records'], 0))

def _count_from_query(query: str) -> int:
    sql = f"SELECT COUNT(*) FROM ({query}) t;"
    res = exec_statement(sql)
    return int(fetch_one_value(res['records'], 0))

def lambda_handler(event, context):
    # 1) Preferir from_object si viene
    obj = event.get("from_object")
    if obj:
        return {"ok": True, "cantidad": _count_from_object(obj)}

    # 2) Si no, usar query directa si viene
    q = event.get("query")
    if q:
        return {"ok": True, "cantidad": _count_from_query(q)}

    # 3) Si viene payload del paso anterior
    md = event.get("metadata") or {}
    eff = event.get("effective") or {}
    if md.get("nombreTabla"):
        return {"ok": True, "cantidad": _count_from_object(md["nombreTabla"])}
    if eff.get("query"):
        return {"ok": True, "cantidad": _count_from_query(eff["query"])}

    raise ValueError("No se proporcionó 'from_object' ni 'query' ni 'metadata/effective'.")
