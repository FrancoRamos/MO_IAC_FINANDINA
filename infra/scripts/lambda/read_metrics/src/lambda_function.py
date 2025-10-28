import json
import os
import re
from typing import Tuple, Dict, Any

import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Convierte s3://bucket/prefix -> (bucket, prefix/)
    Asegura que prefix termine con '/'
    """
    if not uri.startswith("s3://"):
        raise ValueError(f"metrics_path inválido (debe comenzar con s3://): {uri}")
    no_scheme = uri[5:]
    parts = no_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) == 2 else ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def _list_latest_metrics_obj(bucket: str, prefix: str) -> Dict[str, Any]:
    """
    Lista objetos bajo prefix y retorna el MÁS RECIENTE cuyo nombre termine en '_metrics.json'.
    Si no hay coincidencias, lanza error detallado.
    """
    paginator = s3.get_paginator("list_objects_v2")
    latest = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if key.endswith("_metrics.json"):
                if latest is None or obj["LastModified"] > latest["LastModified"]:
                    latest = obj

    if not latest:
        # Para diagnóstico, listamos lo que haya debajo (solo nombres)
        # Nota: evitamos retornar objetos muy largos
        sample_keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": 50}):
            for obj in page.get("Contents", []) or []:
                sample_keys.append(obj["Key"])
        raise FileNotFoundError(
            f"No se encontró *_metrics.json en s3://{bucket}/{prefix}. "
            f"Objetos de muestra: {sample_keys[:20]}"
        )
    return latest


def _read_json(bucket: str, key: str) -> Dict[str, Any]:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        body = resp["Body"].read()
        return json.loads(body.decode("utf-8"))
    except ClientError as e:
        raise RuntimeError(f"Error leyendo s3://{bucket}/{key}: {e}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"JSON inválido en s3://{bucket}/{key}: {e}")


def lambda_handler(event, context):
    """
    Espera:
      {
        "metrics_path": "s3://dl-raw-dev-s3/temp/metrics/<carpeta>/<tabla>/"
      }

    Devuelve (lo que Step Functions usa en $.metrics.Payload.*):
      {
        "rows_copied": <int>,
        "total_source": <int>,
        "pivot_to": <str>,
        "archivo_nombre": <str>,
        "s3_key": "<ruta completa al metrics usado>"
      }
    """
    metrics_path = event.get("metrics_path")
    if not metrics_path:
        raise ValueError("Falta 'metrics_path' en el event")

    bucket, prefix = _parse_s3_uri(metrics_path)
    latest_obj = _list_latest_metrics_obj(bucket, prefix)
    key = latest_obj["Key"]

    data = _read_json(bucket, key)

    # Validación y default razonable
    rows_copied = int(data.get("rows_copied", 0))
    total_source = int(data.get("total_source", 0))
    pivot_to = data.get("pivot_to", "-")
    archivo_nombre = data.get("archivo_nombre")
    if not archivo_nombre:
        # Mantén consistencia: tu Glue acostumbra a setearlo. Pero si faltara, mejor fallar explícito.
        raise KeyError(f"'archivo_nombre' no está presente en {key}")

    return {
        "rows_copied": rows_copied,
        "total_source": total_source,
        "pivot_to": str(pivot_to),
        "archivo_nombre": archivo_nombre,
        "s3_key": f"s3://{bucket}/{key}"
    }
