# lambda_auditoria_ko.py
import os, json
from datetime import datetime, timezone
from common_redshift import call_sp_positional

SCHEMA_SP = os.getenv("SP_SCHEMA", "public")
SP_NAME   = os.getenv("SP_AUDITORIA_UPDATE", "usp_upd_ejecucion_light_gen2")

def lambda_handler(event, context):
    """
    event:
    {
      "tabla_nombre": "public.vw_message_log_hist",
      "run_id": "uuid",
      "sistema_fuente": "esbdata",
      "error": "mensaje opcional",
      "archivo_destino": "-"
    }
    """
    now_utc = datetime.now(timezone.utc).isoformat()
    args = [
        event["tabla_nombre"],   # @tablaNombre
        "-",                     # @valorPivot
        0,                       # @estado
        now_utc,                 # @fechaFin
        event.get("archivo_destino", "-"),  # @archivoDestino
        now_utc,                 # @fechaInsertUpdate
        event["run_id"],         # @runID
        0,                       # @cantidadRegistros
        event.get("sistema_fuente", ""),
        0                        # @cantidadRegistrosTotales
    ]
    spq = f"{SCHEMA_SP}.{SP_NAME}"
    out = call_sp_positional(spq, args)
    return {"ok": True, "called": spq, "args": args, "rs": out}
