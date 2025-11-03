# lambda_auditoria_ok.py
import os, json
from datetime import datetime, timezone
from common_redshift import call_sp_positional, to_int_safe

SCHEMA_SP = os.getenv("SP_SCHEMA", "dbo")
SP_NAME   = os.getenv("SP_AUDITORIA_UPDATE", "usp_upd_ejecucion_light_gen2")

def _from_paths(dct, *paths, default=None):
    """Devuelve el primer valor no None encontrado en una serie de rutas 'a.b.c'."""
    for p in paths:
        cur = dct
        try:
            for k in p.split("."):
                cur = cur[k]
        except Exception:
            cur = None
        if cur is not None:
            return cur
    return default

def lambda_handler(event, context):
    """
    Admite evento 'plano' y también con subestructuras del Step Function:
      - copy.Payload.s3_output
      - conteo.Payload.cantidad
      - metadata.nombreTabla / nombreCarpetaDL
    """

    now_utc = datetime.now(timezone.utc).isoformat()

    # ------- Normalizaciones / defaults seguros --------
    metadata = event.get("metadata", {}) or {}

    run_id = event.get("run_id") or event.get("runId")
    if not run_id:
        # último fallback: el Execution.Id a veces lo pasamos plano como run_id;
        # si no vino, intenta tomarlo de context (no siempre aplica)
        run_id = getattr(context, "aws_request_id", "run-unknown")

    tabla_nombre = (
        event.get("tabla_nombre")
        or metadata.get("nombreTabla")
    )
    if not tabla_nombre:
        raise ValueError("tabla_nombre (o metadata.nombreTabla) es requerido")

    valor_pivot = event.get("valor_pivot", "-")

    # s3 de salida: preferir lo que generó UNLOAD
    archivo_destino = (
        _from_paths(event, "copy.Payload.s3_output")  # preferido
        or event.get("archivo_destino")
        or "-"
    )

    # cantidades: usar conteo si existe
    cantidad_registros = to_int_safe(
        event.get("cantidad_registros",
                  _from_paths(event, "conteo.Payload.cantidad", default=0))
    )
    cantidad_totales = to_int_safe(
        event.get("cantidad_registros_totales",
                  _from_paths(event, "conteo.Payload.cantidad", default=0))
    )

    sistema_fuente = (
        event.get("sistema_fuente")
        or metadata.get("nombreCarpetaDL", "")
    )

    # -------- Llamado al SP --------
    args = [
        tabla_nombre,         # @tablaNombre varchar(100)
        valor_pivot,          # @valorPivot  varchar(100)
        1,                    # @estado (1 = OK)
        now_utc,              # @fechaFin
        archivo_destino,      # @archivoDestino
        now_utc,              # @fechaInsertUpdate
        run_id,               # @runID
        cantidad_registros,   # @cantidadRegistros
        sistema_fuente,       # @sistemaFuente
        cantidad_totales      # @cantidadRegistrosTotales
    ]

    spq = f"{SCHEMA_SP}.{SP_NAME}"
    out = call_sp_positional(spq, args)
    return {"ok": True, "called": spq, "args": args, "rs": out}
