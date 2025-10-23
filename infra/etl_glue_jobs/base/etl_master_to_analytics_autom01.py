#!/usr/bin/env python3
import os
import sys
import gc
import boto3
from pyspark.sql import SparkSession, functions as F
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# 1) Inicializar Spark/Glue
sc = SparkContext.getOrCreate()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite","CORRECTED")

# 2) Leer parámetros (del CDK/Console o Step Functions)
args = getResolvedOptions(sys.argv, [
    'ENVIRONMENT',             # dev/prod
    'BUCKET_MASTER',           # ej. us-west-2-…-master
    'BUCKET_ANALYTICS',        # ej. us-west-2-…-analytics
    'TABLE',                   # nombre lógico de la tabla
    'NAME_PARTITION_MASTER',   # columna de partición en master
    'VALUE_PARTITION_MASTER',  # valor de partición (YYYY-MM-DD, etc.)
    'TYPE_LOAD'                # full o incremental
])
bucket_master     = args['BUCKET_MASTER']
bucket_analytics  = args['BUCKET_ANALYTICS']
table             = args['TABLE']
pr_name_master    = args['NAME_PARTITION_MASTER']
pr_value_master   = args['VALUE_PARTITION_MASTER']
load_type         = args['TYPE_LOAD'].lower()
db_analytics      = os.environ.get('GLUE_DB_ANALYTICS','finandina-account_devops_dev_analytics')  # catálogo analytics

# 3) Construir rutas
if load_type == 'full':
    path_in = f"s3://{bucket_master}/master/{table}/"
else:
    path_in = (
      f"s3://{bucket_master}/master/{table}/"
      f"{pr_name_master}={pr_value_master}/"
    )
if load_type == 'full':
    path_out = f"s3://{bucket_analytics}/analytics/{table}/"
else:
    path_out = (
      f"s3://{bucket_analytics}/analytics/{table}/"
      f"{pr_name_master}={pr_value_master}/"
    )

print(f"[master→analytics] Leyendo de:  {path_in}")
print(f"[master→analytics] Escribiendo en: {path_out}")

# 4) Leer datos
df = spark.read.parquet(path_in)

# 5) Normalizar esquema (opcional)
for c in df.columns:
    df = df.withColumnRenamed(c, c.lower())

# 6) Transformaciones de negocio ejemplo
#    Ajusta según tus necesidades reales:
#    - Agregados
#    - Joins con otras tablas
#    - Cálculos de métricas
df_agg = (
    df.groupBy("producto_id")
      .agg(
         F.max("saldo").alias("saldo_max"),
         F.count("*").alias("total_registros")
      )
)

# 7) Escribir resultados
df_agg.write.mode("overwrite").parquet(path_out)

# 8) Registrar partición en Glue Catalog
if load_type != 'full':
    athena = boto3.client('athena')
    tbl_analytics = f"analytics__{table}".lower()
    partition = f"{pr_name_master}='{pr_value_master}'"
    sql = (
        f"ALTER TABLE {db_analytics}.{tbl_analytics} "
        f"ADD IF NOT EXISTS PARTITION ({partition})"
    )
    print(f"[master→analytics] Añadiendo partición: {partition}")
    athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': db_analytics},
        ResultConfiguration={'OutputLocation': path_out}
    )

# 9) Limpieza
gc.collect()
spark.stop()
print("[master→analytics] Job COMPLETADO")
