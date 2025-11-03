# lambda_auditoria_inicio.py
import os, json
from datetime import datetime, timezone, timedelta
from common_redshift import call_sp_positional

SCHEMA_SP = os.getenv("SP_SCHEMA", "dbo")  # En Redshift suele ser 'public' si no usas 'dbo'
SP_NAME   = os.getenv("SP_AUDITORIA_INICIO", "usp_ins_ejecucion_light_gen2")

def _tipo_carga_from(metadata_tipo: str) -> int:
    # En ADF: 0 si '-', 1 si cualquier otro valor
    if metadata_tipo is None:
        return 0
    return 0 if str(metadata_tipo).strip() == '-' else 1

def _now_lima_str() -> str:
    # SA Pacific (Lima), sin DST
    pet = timezone(timedelta(hours=-5))
    return datetime.now(pet).strftime("%Y-%m-%d %H:%M:%S")

def lambda_handler(event, context):
    # 1) run_id robusto
    run_id = event.get("run_id") or event.get("runId") or getattr(context, "aws_request_id", None)
    if not run_id:
        raise ValueError("run_id (o runId) es requerido")

    # 2) Normalizar payload
    metadata = event.get("metadata")
    if metadata:
        nombre_tabla   = metadata.get("nombreTabla")
        sistema_fuente = metadata.get("nombreCarpetaDL", "")
        tipo_carga     = _tipo_carga_from(metadata.get("tipoCampoPivot"))
        valor_pivot    = event.get("valor_pivot", "-")
        archivo_destino= event.get("archivo_destino") or event.get("s3Output") or "-"
        origen         = event.get("origen", "redshift")
    else:
        nombre_tabla   = event["nombre_tabla"]
        sistema_fuente = event.get("sistema_fuente", "")
        tipo_carga     = int(event.get("tipo_carga", 0))
        valor_pivot    = event.get("valor_pivot", "-")
        archivo_destino= event.get("archivo_destino", "-")
        origen         = event.get("origen", "redshift")

    if not nombre_tabla:
        raise ValueError("nombre_tabla (o metadata.nombreTabla) es requerido")

    # 3) Fechas sin zona (tu tabla es TIMESTAMP WITHOUT TIME ZONE)
    now_local = _now_lima_str()

    # 4) Orden de parámetros debe calzar con el SP en Redshift
    args = [
        origen,            # @origen           varchar(50)
        nombre_tabla,      # @nombreTabla      varchar(<=255)
        tipo_carga,        # @tipoCarga        int
        valor_pivot,       # @valorPivot       varchar(<=255)
        2,                 # @estado           int (2 = en proceso)
        now_local,         # @fechaInicio      timestamp
        None,              # @fechaFin         timestamp
        archivo_destino,   # @archivoDestino   varchar(<=1024 recomendado)
        now_local,         # @fechaInsertUpdate timestamp
        run_id,            # @runID            varchar(>=255 recomendado)
        sistema_fuente     # @sistemaFuente    varchar(<=100)
    ]

    spq = f"{SCHEMA_SP}.{SP_NAME}"
    out = call_sp_positional(spq, args)
    return {"ok": True, "called": spq, "args": args, "rs": out}
