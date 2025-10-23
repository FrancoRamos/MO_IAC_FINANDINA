# glue/jobs/cet_plano_mayor.py  (fragmento limpio y coherente)
import sys 
import os, time, boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession, functions as F
from pyspark.context import SparkContext
from awsglue.context import GlueContext

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'fecha', 'tipoPlano',
    'REDSHIFT_WORKGROUP', 'REDSHIFT_DATABASE', 'REDSHIFT_IAM_ROLE',
    'STAGING_BASE', 'DRY_RUN',
    'RAW_DB', 'SILVER_DB'               # <-- NUEVO
])
RAW_DB   = args['RAW_DB']               # p.ej. dl_raw_dev_glue
SILVER_DB= args['SILVER_DB']            
FECHA = args['fecha']
TIPO  = args['tipoPlano']
FLAG  = 'N' if str(TIPO).upper() == 'DIARIO' else 'Y'

WG      = args['REDSHIFT_WORKGROUP']
DB      = args['REDSHIFT_DATABASE']
RS_ROLE = args['REDSHIFT_IAM_ROLE']
STAGING = args['STAGING_BASE'].rstrip('/') + '/'
DRY_RUN = args['DRY_RUN'].upper() == 'Y'

spark = (SparkSession.builder
         .appName(f"cet_plano_mayor_{FECHA}")
         .config("spark.sql.sources.partitionOverwriteMode","dynamic")
         .getOrCreate())

rs = boto3.client("redshift-data")

def s3_path(name: str) -> str:
    # p.ej. "Tmp_cobranza_Producto" -> s3://.../staging/cobranza/Tmp_cobranza_Producto/
    return f"{STAGING}{name}/"

def table_stg(name: str) -> str:
    # Mantén el esquema 'stg' y el nombre lógico igual al de Databricks
    # Si tu Redshift usa snake_case, transforma aquí.
    return f"public.{name}" #se debe cambiar a stg por ahora prueba public

def wait_statement(stmt_id: str, poll_sec: int = 2):
    # Espera a que Redshift termine (COPY/DDL no devuelven rows)
    while True:
        d = rs.describe_statement(Id=stmt_id)
        s = d["Status"]
        if s in ("FINISHED", "FAILED", "ABORTED"):
            if s != "FINISHED":
                raise RuntimeError(f"Redshift statement {s}: {d.get('Error', '')}")
            return
        time.sleep(poll_sec)

def load_dwh_staging_table(df, tableTarget: str, modeLoad: str = "overwrite"):
    """
    Equivalente a transversales.loadDwhStagingTable:
      - Escribe a S3 (parquet) en prefijo parametrizado.
      - Si DRY_RUN==False, TRUNCATE (si overwrite) + COPY a stg.<tableTarget>.
    """
    prefix = s3_path(tableTarget)
    df.repartition(4).write.mode("overwrite").parquet(prefix)

    if DRY_RUN:
        print(f"[DRY_RUN] Escrito S3: {prefix} (sin COPY)")
        return

    if modeLoad.lower() == "overwrite":
        stmt = rs.execute_statement(WorkgroupName=WG, Database=DB,
                                    Sql=f"TRUNCATE TABLE {table_stg(tableTarget)};")
        wait_statement(stmt["Id"])

    copy_sql = f"""
    COPY {table_stg(tableTarget)}
    FROM '{prefix}'
    IAM_ROLE '{RS_ROLE}'
    FORMAT AS PARQUET;
    """
    stmt = rs.execute_statement(WorkgroupName=WG, Database=DB, Sql=copy_sql)
    wait_statement(stmt["Id"])
    print(f"Carga exitosa a Redshift: {table_stg(tableTarget)} desde {prefix}")

# ==================== QUERIES ====================

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

spark.sql("SHOW DATABASES").show(truncate=False)
spark.sql("SHOW TABLES IN dl_raw_dev_glue").show(truncate=False)


##RPUEBAS BORAR EDESPUES LO DE ARRIBA

# 1) dwh_tipo_producto -> stg.Tmp_cobranza_Producto
df_prod = spark.sql(f"""
SELECT skTipoProductoCobranza, idTipoProductoCobranza, tipoProductoCobranza,
       tipoCartera, propiedad, idprodOrigen, prodOrigen
FROM {RAW_DB}.dwh_tipo_producto
""")
load_dwh_staging_table(df_prod, "Tmp_cobranza_Producto", modeLoad="overwrite")

# 2) sign_munidian + departamentoindicativo -> stg.Tmp_cobranza_Geografia
df_geo = spark.sql(f"""
SELECT
  mun.CMCODICBS AS codigoMunicipio,
  mun.CMNOMMUNI AS municipio,
  mun.CMNOMDEPA AS departamento,
  dep.indicativo AS indicativoDepartamento
FROM {RAW_DB}.sign_munidian AS mun
LEFT JOIN {SILVER_DB}.cobranza_departamentoindicativo AS dep
  ON translate(lower(trim(dep.departamento)), 'ñáéíóúàèìòù', 'naeiouaeiou')
   = translate(lower(trim(mun.CMNOMDEPA)), 'ñáéíóúàèìòù', 'naeiouaeiou')
""")
load_dwh_staging_table(df_geo, "Tmp_cobranza_Geografia", modeLoad="overwrite")

# 3) dwh_metas -> stg.Tmp_cobranza_Meta
df_meta = spark.sql(f"""
SELECT idMeta, moraArrastre, metaCaida, metaMejora
FROM {RAW_DB}.dwh_metas
""")
load_dwh_staging_table(df_meta, "Tmp_cobranza_Meta", modeLoad="overwrite")

# 4) dwh_caracteristica -> stg.Tmp_cobranza_Caracteristica
df_car = spark.sql(f"""
SELECT skCaracteristica, idGrupoCaracteristica, idCaracteristica,
       limiteInicial, limiteFinal, valor, caracteristica
FROM {RAW_DB}.dwh_caracteristica
""")
load_dwh_staging_table(df_car, "Tmp_cobranza_Caracteristica", modeLoad="overwrite")

# 5) dwh_grupocaracteristica -> stg.Tmp_cobranza_GrupoCaracteristica
df_grp = spark.sql(f"""
SELECT idGrupoCaracteristica, grupoCaracteristica
FROM {RAW_DB}.dwh_grupocaracteristica
""")
load_dwh_staging_table(df_grp, "Tmp_cobranza_GrupoCaracteristica", modeLoad="overwrite")

# 6) sil_plano_full (último plano por FECHA/FLAG) -> stg.sil_plano_full
df_plano = spark.sql(f"""
SELECT *
FROM {SILVER_DB}.cobranza_plano
WHERE CAST(CONCAT(fechaplano, horaplano) AS BIGINT) = (
  SELECT MAX(CAST(CONCAT(fechaplano, horaplano) AS BIGINT))
  FROM {SILVER_DB}.cobranza_plano
  WHERE fechaplano = CAST('{FECHA}' AS BIGINT)
    AND '{FLAG}' = FLAG_CIERRE_MES
)
AND '{FLAG}' = FLAG_CIERRE_MES
""")
load_dwh_staging_table(df_plano, "sil_plano_full", modeLoad="overwrite")
