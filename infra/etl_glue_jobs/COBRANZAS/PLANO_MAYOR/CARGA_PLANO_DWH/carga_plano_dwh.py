import sys, os, time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower, trim, translate, concat_ws
from pyspark.sql.types import LongType

# ================== CONFIG FIJA (no desde Step Functions) ==================
WORKGROUP   = "dl-workgroup-dev-rs"  
SECRET_ARN  = "arn:aws:secretsmanager:us-east-1:637423369807:secret:redshift/finandina/dev-CkFGPc"
REDSHIFT_DB = None  # si quieres forzar DB: "dl_dev"; si None, usa dbname del secret

# ================== ARGUMENTOS (desde Step Functions o Defaults) ===========
args = {sys.argv[i].lstrip("--"): sys.argv[i+1] for i in range(1, len(sys.argv)-1, 2)}
FECHA        = args.get("fecha")                 # 'yyyyMMdd' (string)
TIPO_PLANO   = args.get("tipoPlano", "-1")
MODE_LOAD    = args.get("modeLoad", "overwrite") # 'overwrite' | 'append'
S3_ROOT      = args.get("s3_root", "s3://pruebas-tablas-finandina/Finandina")  # <-- CAMBIA
S3_TMP       = args.get("s3_tmp",  "s3://pruebas-tablas-finandina/tmp/redshift/")  # staging COPY

# ================== SPARK INIT ============================================
spark = (SparkSession.builder
         .appName("plano_mayor_load_staging")
         .getOrCreate())
spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")

# ================== HELPERS ===============================================
redshift = boto3.client("redshift-data")

def exec_and_wait(sql):
    params = {"WorkgroupName": WORKGROUP, "SecretArn": SECRET_ARN, "Sql": sql}
    if REDSHIFT_DB:
        params["Database"] = REDSHIFT_DB
    resp = redshift.execute_statement(**params)
    sid  = resp["Id"]
    while True:
        d = redshift.describe_statement(Id=sid)
        st = d["Status"]
        if st in ("FINISHED", "FAILED", "ABORTED"):
            if st != "FINISHED":
                raise RuntimeError(f"[Redshift] SQL failed: {sql} | {d.get('Error')}")
            return d
        time.sleep(2)

def load_staging_to_redshift(df, table_target, mode_load):
    """
    Emula transversales.loadDwhStagingTable(df, 'stg.<table>', modeLoad)
    Escribe df a S3 tmp (parquet) y COPY a Redshift (stg.<table_target>)
    """
    table_full = f"stg.{table_target}"
    run_tag = f"table={table_target}/fecha={FECHA}/run_ts={int(time.time())}"
    tmp_out = os.path.join(S3_TMP.rstrip("/"), "stg", run_tag)

    (df
     .coalesce(1)  # opcional; según tamaño
     .write
     .mode("overwrite")
     .format("parquet")
     .option("compression", "snappy")
     .save(tmp_out))

    if mode_load.lower() == "overwrite":
        exec_and_wait(f"TRUNCATE TABLE {table_full};")

    copy_sql = f"COPY {table_full} FROM '{tmp_out}/' FORMAT AS PARQUET;"
    exec_and_wait(copy_sql)
    print(f"[OK] Cargado {table_full} (mode={mode_load}) desde {tmp_out}")

def path(*parts):
    return "/".join([p.strip("/") for p in parts])

def read_parquet(layer, name):
    p = f"{S3_ROOT}/{layer}/{name}/"
    print(f"[READ] {p}")
    return spark.read.format("parquet").option("mergeSchema", "true").load(p)

# ================== PARÁMETROS DEL LOAD ===================================
# flag: igual que en tu notebook
FLAG = "N" if str(TIPO_PLANO).upper() == "DIARIO" else "Y"
print(f"fecha={FECHA} tipoPlano={TIPO_PLANO} flag={FLAG}")

# ================== 1) Tmp_cobranza_Producto ==============================
# select skTipoProductoCobranza, idTipoProductoCobranza, tipoProductoCobranza, tipoCartera, propiedad, idprodOrigen, prodOrigen
dwh_tipo_producto = read_parquet("bronze", "dwh_tipo_producto")
df_prod = (dwh_tipo_producto
           .select("skTipoProductoCobranza","idTipoProductoCobranza","tipoProductoCobranza",
                   "tipoCartera","propiedad","idprodOrigen","prodOrigen"))
load_staging_to_redshift(df_prod, "Tmp_cobranza_Producto", "overwrite")

# ================== 2) Tmp_cobranza_Geografia =============================
# from bronze.sign_munidian mun
# left join silver.cobranza_departamentoindicativo dep
# on translate(lower(trim(dep.departamento)), 'ñáéíóúàèìòù', 'naeiouaeiou')
#  = translate(lower(trim(mun.CMNOMDEPA)), 'ñáéíóúàèìòù', 'naeiouaeiou')
mun = read_parquet("bronze", "sign_munidian")
dep = read_parquet("silver", "cobranza_departamentoindicativo")

def norm(scol):
    return translate(lower(trim(scol)), "ñáéíóúàèìòù", "naeiouaeiou")

dep_n = dep.withColumn("dep_norm", norm(col("departamento")))
mun_n = mun.withColumn("dep_norm", norm(col("CMNOMDEPA")))

df_geo = (mun_n.join(dep_n, on="dep_norm", how="left")
          .select(col("CMCODICBS").alias("codigoMunicipio"),
                  col("CMNOMMUNI").alias("municipio"),
                  col("CMNOMDEPA").alias("departamento"),
                  col("indicativo").alias("indicativoDepartamento")))
load_staging_to_redshift(df_geo, "Tmp_cobranza_Geografia", "overwrite")

# ================== 3) Tmp_cobranza_Meta ==================================
# select idMeta, moraArrastre, metaCaida, metaMejora from bronze.dwh_metas
dwh_metas = read_parquet("bronze", "dwh_metas")
df_meta = dwh_metas.select("idMeta","moraArrastre","metaCaida","metaMejora")
load_staging_to_redshift(df_meta, "Tmp_cobranza_Meta", "overwrite")

# ================== 4) Tmp_cobranza_Caracteristica ========================
# select skCaracteristica, idGrupoCaracteristica, idCaracteristica, limiteInicial, limiteFinal, valor, caracteristica
dwh_caracteristica = read_parquet("bronze", "dwh_caracteristica")
df_car = dwh_caracteristica.select("skCaracteristica","idGrupoCaracteristica","idCaracteristica",
                                   "limiteInicial","limiteFinal","valor","caracteristica")
load_staging_to_redshift(df_car, "Tmp_cobranza_Caracteristica", "overwrite")

# ================== 5) Tmp_cobranza_GrupoCaracteristica ===================
# select idGrupoCaracteristica, grupoCaracteristica from bronze.dwh_grupocaracteristica
dwh_grupo = read_parquet("bronze", "dwh_grupocaracteristica")
df_gru = dwh_grupo.select("idGrupoCaracteristica","grupoCaracteristica")
load_staging_to_redshift(df_gru, "Tmp_cobranza_GrupoCaracteristica", "overwrite")

# ================== 6) sil_plano_full =====================================
# refresh table silver.cobranza_plano  (en Glue no hace falta; si usas Glue Catalog podrías MSCK/refresh)
# Selección:
#   from silver.cobranza_plano
#   where CAST(CONCAT(fechaplano,horaplano) AS bigint) = MAX(...) para la misma fechaplano y FLAG_CIERRE_MES = flag
plano = read_parquet("silver", "cobranza_plano") \
        .select("fechaplano","horaplano","FLAG_CIERRE_MES", "*")

plano_f = (plano
           .filter( (col("fechaplano").cast("string") == FECHA) & (col("FLAG_CIERRE_MES") == FLAG) )
           .withColumn("concat_num", concat_ws("", col("fechaplano").cast("string"), col("horaplano").cast("string")).cast(LongType())) )

# max concat para ese día y flag
max_row = plano_f.agg({"concat_num":"max"}).collect()[0]
max_concat = max_row["max(concat_num)"] if max_row else None

if max_concat is None:
    # Si no hay plano para esa combinación, genera DF vacío con mismas columnas
    df_plano_sel = plano_f.limit(0)
else:
    df_plano_sel = plano_f.filter(col("concat_num") == lit(max_concat)).drop("concat_num")

load_staging_to_redshift(df_plano_sel, "sil_plano_full", "overwrite")

print("[DONE] load_synapse_planoMayor replicado en Glue → Redshift")
spark.stop()
