import sys, os, datetime as dt
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bronze_db',
    's3_gold_root',
    'redshift_write_mode',         # 's3' (default) | 'redshift'
    'redshift_tmp_dir',
    'redshift_db',
    'redshift_workgroup',
    'redshift_secret_arn',
    'redshift_iam_role_arn'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bronze_db = args['bronze_db']
s3_gold_root = args['s3_gold_root'].rstrip('/')
write_mode = args['redshift_write_mode'].lower().strip()
today = dt.date.today().strftime('%Y-%m-%d')

# Utilidad: escribir a S3 como parquet particionado dt=YYYY-MM-DD
def save_to_s3_gold(df, dataset_name):
    out = f"{s3_gold_root}/{dataset_name}/dt={today}"
    (df
     .coalesce(1)
     .write
     .mode("overwrite")
     .option("compression", "snappy")
     .parquet(out))
    print(f"[OK] S3 gold: {out}")

# Opcional: escribir a Redshift stg.<tabla> con conector Glue (requiere connection o jdbc conf)
def load_redshift_staging_table(df, table_target, mode="overwrite"):
    # Convert to DynamicFrame si deseas usar writer Glue (no obligatorio en Glue 4.0 con spark write jdbc)
    # Aquí usamos JDBC + S3 temp (spark-redshift). Alternativamente, puedes usar COPY vía Lambda.
    jdbc_url = f"jdbc:redshift://{spark.conf.get('spark.redshift.host', '')}:5439/{args['redshift_db']}"
    # Si no configuras host por conf, toma desde secret (recomendado configurar en Connection y usar Glue writer).
    # Por simplicidad, muestra vía DataFrame->JDBC (requiere driver redshift en job, o usa Glue writer):
    df.write \
      .format("io.github.spark_redshift_community.spark.redshift") \
      .option("url", jdbc_url) \
      .option("dbtable", f"stg.{table_target}") \
      .option("tempdir", args['redshift_tmp_dir']) \
      .option("aws_iam_role", args['redshift_iam_role_arn']) \
      .mode(mode) \
      .save()
    print(f"[OK] Redshift stg.{table_target}")

# Registra las tablas del catálogo 'bronze' en Spark (si no están registradas)
# Glue crea bases de datos/tables: asegúrate de que 'bronze.<tabla>' existe en Data Catalog apuntando a tus S3.
spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")

# === BLOQUES DE CARGA (traducción del notebook load_synapse_SD.ipynb) ===
# Ejemplo 1 (del ipynb): bronze.finandinacartera_refinanciados -> Tmp_dwh_fc_refinanciados
df1 = spark.sql(f"""
SELECT *
FROM {bronze_db}.finandinacartera_refinanciados
""")

if write_mode == 'redshift':
    load_redshift_staging_table(df1, "Tmp_dwh_fc_refinanciados", "overwrite")
else:
    save_to_s3_gold(df1, "Tmp_dwh_fc_refinanciados")

# Ejemplo 2 (del ipynb): bronze.sign_calxalilgf -> Tmp_dwh_sign_interfaces_calxalilgf
df2 = spark.sql(f"""
SELECT CAST(CLNROIDE AS DECIMAL(18,0)) AS Cedula,
       CLFECALI AS Fecha,
       CAST(CLTIPNEG AS DECIMAL(18,0)) AS TipoAlivio
FROM {bronze_db}.sign_calxalilgf
WHERE CLTIPNEG IN (1,2,3,4,5)
""")

if write_mode == 'redshift':
    load_redshift_staging_table(df2, "Tmp_dwh_sign_interfaces_calxalilgf", "overwrite")
else:
    save_to_s3_gold(df2, "Tmp_dwh_sign_interfaces_calxalilgf")

# Ejemplo 3 (del ipynb): bronze.finandinacartera_011_baseofertassegmentaciondinamica -> Tmp_dwh_bro_fc_011_BaseOfertasSegmentacionDinamica
df3 = spark.sql(f"""
SELECT identificacion, clasificacion
FROM {bronze_db}.finandinacartera_011_baseofertassegmentaciondinamica
""")

if write_mode == 'redshift':
    load_redshift_staging_table(df3, "Tmp_dwh_bro_fc_011_BaseOfertasSegmentacionDinamica", "overwrite")
else:
    save_to_s3_gold(df3, "Tmp_dwh_bro_fc_011_BaseOfertasSegmentacionDinamica")

# ... Repite aquí los demás SELECT del notebook (p.ej. relacionados, 00fraudes, etc.) con el mismo patrón ...
# Referencia: tu ipynb contiene múltiples bloques SELECT sobre {bronze_db}.<tabla> → write a Tmp_* (overwrite)

job.commit()
