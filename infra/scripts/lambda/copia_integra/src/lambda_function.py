import os, json
from datetime import datetime, timezone, timedelta
from common_redshift import exec_statement

# Rol que Redshift usará para escribir en S3
IAM_ROLE_ARN = os.getenv(
    "REDSHIFT_S3_ROLE_ARN",
    "arn:aws:iam::637423369807:role/redshift-finandina-role"
)

def _require(field_name: str, value):
    if value is None or (isinstance(value, str) and value.strip() == ""):
        raise ValueError(f"Parametro requerido ausente: '{field_name}'")
    return value

def _pick_query(event: dict) -> str:
    """
    Prioridades:
      1) event.get("effective", {}).get("query")
      2) event.get("metadata", {}).get("query")
      3) event.get("unload_query")
    """
    eff = (event.get("effective") or {}).get("query")
    if eff: return eff
    mdq = (event.get("metadata") or {}).get("query")
    if mdq: return mdq
    uq = event.get("unload_query")
    if uq: return uq
    raise ValueError("No se encontró query. Envía 'effective.query' o 'metadata.query' o 'unload_query'.")

def _adf_style_s3_output(event: dict) -> str:
    """
    Emula ADF:
      Path  = /<carpetaDL>/<tabla>/
      Nombre= <yyyyMMddHHmm>_<tabla>.parquet
      Resultado: s3://{bucket}/{prefix}/{carpeta}/{tabla}/{timestamp}_{tabla}.parquet/
    """
    bucket = os.getenv("S3_OUTPUT_BUCKET") or "dl-raw-dev-s3"
    if not bucket:
        raise ValueError("Falta bucket destino: define env S3_OUTPUT_BUCKET o envía 'bucket' en el evento.")
    prefix = (os.getenv("S3_OUTPUT_PREFIX") or "bronze").strip("/")

    md = event.get("metadata", {})
    carpeta = md.get("nombreCarpetaDL") or "unknown"
    tabla   = (md.get("nombreTabla") or "unknown").replace("[","").replace("]","")

    # fecha/hora estilo ADF (zona Lima UTC-5)
    pet = timezone(timedelta(hours=-5))
    ts  = datetime.now(pet).strftime("%Y%m%d%H%M")

    return f"s3://{bucket}/{prefix}/{carpeta}/{tabla}/{ts}_{tabla}.parquet/"

def lambda_handler(event, context):
    if not IAM_ROLE_ARN:
        raise ValueError("Falta REDSHIFT_S3_ROLE_ARN en variables de entorno.")

    # 1) Query a descargar
    query = _pick_query(event)

    # 2) S3 de salida (si no viene, se calcula estilo ADF)
    s3_out = (event.get("s3_output") or _adf_style_s3_output(event)).rstrip("/") + "/"

    # 3) Opciones UNLOAD (por defecto Parquet)
    options = (event.get("unload_options") or "FORMAT AS PARQUET").strip()

    # 4) Construcción y ejecución UNLOAD
    sql = f"""
        UNLOAD ($$
{query}
        $$)
        TO '{s3_out}'
        {options}
        IAM_ROLE '{IAM_ROLE_ARN}';
    """
    res = exec_statement(sql)

    return {
        "ok": True,
        "s3_output": s3_out,
        "sql": sql,
        "rs": res
    }
