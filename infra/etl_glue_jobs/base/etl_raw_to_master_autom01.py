# raw_to_master.py

import sys, gc, boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

# 1. Inicialización
sc = SparkContext.getOrCreate()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# 2. Parámetros (pásalos al crear el Job en CDK/Console)
args = getResolvedOptions(sys.argv, [
    'BUCKET_RAW',
    'BUCKET_MASTER',
    'TABLE',
    'NAME_PARTITION_RAW',
    'VALUE_PARTITION_RAW',
    'TYPE_LOAD'   # full o incremental
])
bucket_raw    = args['BUCKET_RAW']
bucket_master = args['BUCKET_MASTER']
table         = args['TABLE']
pr_name       = args['NAME_PARTITION_RAW']
pr_value      = args['VALUE_PARTITION_RAW']
load_type     = args['TYPE_LOAD'].lower()

# 3. Leer datos
if load_type == "full":
    path_in = f"s3://{bucket_raw}/raw/{table}/"
else:
    path_in = f"s3://{bucket_raw}/raw/{table}/{pr_name}={pr_value}/"

print(f"[raw→master] Leyendo {path_in}")
df = spark.read.parquet(path_in)

# 4. Normalizar esquema
for c in df.columns:
    df = df.withColumnRenamed(c, c.lower())

# 5. Metadatos de ingestión
df = df.withColumn("date_created", F.current_timestamp()) \
       .withColumn("date_updated", F.current_timestamp())

# 6. Escribir en master
if load_type == "full":
    path_out = f"s3://{bucket_master}/master/{table}/"
else:
    path_out = f"s3://{bucket_master}/master/{table}/{pr_name}={pr_value}/"

print(f"[raw→master] Escribiendo en {path_out}")
df.write.mode("overwrite").parquet(path_out)

# 7. Registrar partición en Glue Catalog (si es incremental)
if load_type != "full":
    athena = boto3.client("athena")
    db_master  = "finandina-account_devops_dev_master"
    tbl_master = f"nodomain__{table}".lower()
    partition  = f"{pr_name}='{pr_value}'"
    sql        = (
        f"ALTER TABLE {db_master}.{tbl_master} "
        f"ADD IF NOT EXISTS PARTITION ({partition})"
    )
    print(f"[raw→master] Añadiendo partición {partition}")
    athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': db_master},
        ResultConfiguration={'OutputLocation': path_out}
    )

# 8. Limpieza
gc.collect()
spark.stop()
print("[raw→master] Job COMPLETADO")
print("[raw→master] Job COMPLETADO - prueba de pipeline")
