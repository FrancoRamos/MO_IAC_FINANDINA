# glue_version: 5.0
# python: 3.11
import re
import traceback
import sys
import os
import json
import time
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

# =========================
# Args y parámetros (JOB_NAME obligatorio)
# =========================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
print("[BOOT] sys.argv =", sys.argv)
print("[BOOT] ENV LOG_LEVEL =", os.environ.get("LOG_LEVEL"))

def get_opt_arg(name, default=''):
    flag = f'--{name}'
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return os.environ.get(name.upper(), default)

# ---- Redshift / Data API
SECRET_ARN    = get_opt_arg('secret_arn',   'arn:aws:secretsmanager:us-east-1:637423369807:secret:redshift/finandina/dev-CkFGPc')
WORKGROUP     = get_opt_arg('workgroup',    'dl-workgroup-dev-rs')
REDSHIFT_DB   = get_opt_arg('redshift_db',  'dl_dev')

# ---- S3 raw y warehouse Iceberg
RAW_BUCKET    = get_opt_arg('raw_bucket',     'dl-raw-dev-s3')
RAW_PREFIX    = get_opt_arg('raw_prefix',     '')  # <- ajusta a 'bronze/' si corresponde
WAREHOUSE_S3  = get_opt_arg('warehouse_s3',   's3://dl-master-dev-s3/warehouse/')

# ---- Auditoría / Config en Redshift
AUDIT_SCHEMA  = get_opt_arg('audit_schema',  'dbo')
CONTROL_TABLE = get_opt_arg('control_table', 'controlPipelineLight_gen2')
CONFIG_TABLE  = get_opt_arg('config_table',  'ConfigADFMetadataDriven')

# ---- Catálogo Glue (tabla de orquestación inicial)
CATALOG_DB    = get_opt_arg('catalog_db',    'dl_raw_dev_glue')
CATALOG_TBL   = get_opt_arg('catalog_tbl',   'load_raw_bronze_ice')

# ---- Filtros
INSTANCE_NAME = get_opt_arg('instance_name', 'FABOGREPORTS')
SOURCE_DBNAME = get_opt_arg('source_dbname', 'FinandinaBPMPro')
SOURCE_SYSTEM = get_opt_arg('source_system', 'agil')
PROJECT_NAME  = get_opt_arg('project',       'DWH')

# ---- Log level
LOG_LEVEL     = get_opt_arg('log_level', 'DEBUG').upper()  # DEBUG | INFO | WARN

def log(level, msg, **kv):
    levels = ['DEBUG','INFO','WARN','ERROR']
    if levels.index(level) >= levels.index(LOG_LEVEL):
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        extra = f" | {json.dumps(kv, ensure_ascii=False)}" if kv else ""
        print(f"[{ts}] [{level}] {msg}{extra}", flush=True)

log("INFO", "Parámetros efectivos", 
    SECRET_ARN=SECRET_ARN, WORKGROUP=WORKGROUP, REDSHIFT_DB=REDSHIFT_DB,
    RAW_BUCKET=RAW_BUCKET, RAW_PREFIX=RAW_PREFIX, WAREHOUSE_S3=WAREHOUSE_S3,
    AUDIT_SCHEMA=AUDIT_SCHEMA, CONTROL_TABLE=CONTROL_TABLE, CONFIG_TABLE=CONFIG_TABLE,
    CATALOG_DB=CATALOG_DB, CATALOG_TBL=CATALOG_TBL,
    INSTANCE_NAME=INSTANCE_NAME, SOURCE_DBNAME=SOURCE_DBNAME,
    SOURCE_SYSTEM=SOURCE_SYSTEM, PROJECT_NAME=PROJECT_NAME,
    LOG_LEVEL=LOG_LEVEL
)

# =========================
# Spark / GlueContext (ICEBERG)
# =========================
spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "glue_catalog")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE_S3)
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")
glueContext = GlueContext(sc)

# =========================
# Clientes AWS
# =========================
rsd = boto3.client('redshift-data') if SECRET_ARN and WORKGROUP and REDSHIFT_DB else None
s3  = boto3.client('s3')

# =========================
# Utils
# =========================
def exec_sql(sql: str):
    if rsd is None:
        raise RuntimeError("Cliente Redshift Data API no configurado (faltan SECRET_ARN/WORKGROUP/REDSHIFT_DB).")
    log("DEBUG", "Ejecutando SQL Redshift", sql=sql.strip())

    exec_id = rsd.execute_statement(
        WorkgroupName=WORKGROUP, SecretArn=SECRET_ARN, Database=REDSHIFT_DB, Sql=sql
    )['Id']

    # Poll
    while True:
        stat = rsd.describe_statement(Id=exec_id)
        if stat['Status'] in ('FINISHED', 'FAILED', 'ABORTED'):
            break
        time.sleep(0.8)

    if stat['Status'] != 'FINISHED':
        raise RuntimeError(f"SQL failed: {stat.get('Error')} | SQL: {sql}")

    result = {'records': [], 'rowcount': None}

    if stat.get('HasResultSet'):
        next_token = None
        cols = None
        total = 0
        while True:
            kw = {'Id': exec_id}
            if next_token:
                kw['NextToken'] = next_token
            page = rsd.get_statement_result(**kw)

            if cols is None:
                cols = [ (c.get('name') or '').lower() for c in page.get('ColumnMetadata', []) ]

            for row in page.get('Records', []):
                parsed = []
                for cell in row:
                    if 'stringValue' in cell:    parsed.append(cell['stringValue'])
                    elif 'longValue' in cell:    parsed.append(cell['longValue'])
                    elif 'doubleValue' in cell:  parsed.append(cell['doubleValue'])
                    elif 'booleanValue' in cell: parsed.append(cell['booleanValue'])
                    elif cell.get('isNull'):     parsed.append(None)
                    else:                        parsed.append(next(iter(cell.values())))
                result['records'].append(dict(zip(cols, parsed)))

            total += len(page.get('Records', []))
            next_token = page.get('NextToken')
            if not next_token:
                break

        log("DEBUG", "SQL resultset", rows=total)
    else:
        result['rowcount'] = stat.get('ResultRows')
        log("DEBUG", "SQL rowcount", rowcount=result['rowcount'])

    return result
    
def s3_join(*parts) -> str:
    return 's3://' + '/'.join([str(x).strip('/') for x in parts if str(x).strip('/') != ''])

def parse_s3_uri(uri: str):
    m = re.match(r'^s3://([^/]+)/(.*)$', uri)
    if not m: raise ValueError(f"URI S3 inválida: {uri}")
    return m.group(1), m.group(2)

def s3_exists_prefix(uri: str, expect_parquet=False, max_keys=5) -> bool:
    try:
        bkt, pfx = parse_s3_uri(uri)
        resp = s3.list_objects_v2(Bucket=bkt, Prefix=pfx, MaxKeys=max_keys)
        contents = resp.get('Contents', [])
        if not contents: 
            log("WARN", "S3 prefix vacío", uri=uri)
            return False
        if expect_parquet:
            any_parquet = any(obj['Key'].lower().endswith('.parquet') for obj in contents)
            if not any_parquet:
                log("WARN", "No se ven .parquet en el prefijo (muestra)", uri=uri, sample=[c['Key'] for c in contents])
            return any_parquet
        return True
    except Exception as e:
        log("ERROR", "Error listando S3", uri=uri, error=str(e))
        return False

def table_exists(db: str, tbl: str) -> bool:
    try:
        return spark.catalog.tableExists(f"glue_catalog.{db}.{tbl}")
    except Exception as e:
        log("ERROR", "Error consultando existencia de tabla", db=db, tbl=tbl, error=str(e))
        return False

def df_debug(name, df, n=5):
    try:
        cnt = df.count()
        log("INFO", f"[DF] {name} count", rows=cnt)
        if cnt:
            log("DEBUG", f"[DF] {name} schema", schema=str(df.printSchema()))
            log("DEBUG", f"[DF] {name} sample", sample=str(df.limit(n).toPandas().to_dict(orient='records')))
    except Exception as e:
        log("ERROR", f"Falla debug DF {name}", error=str(e))

# =========================
# Funciones de negocio (Glue + Iceberg)
# =========================
def activeTableIngestion_glue(instanceName: str, dbName: str,
                              audit_schema: str = AUDIT_SCHEMA,
                              config_table: str = CONFIG_TABLE):
    def _qlist(xs):
        if isinstance(xs, str): xs = [xs]
        xs = [str(x).replace("'", "''") for x in xs]
        return ",".join([f"'{x}'" for x in xs])
    sql = f"""
    SELECT
      nombreBaseDatos,
      lower(replace(replace(nombreTabla,'[',''),']','')) AS nombreTabla,
      indEstadoTrigger
    FROM {audit_schema}.{config_table}
    WHERE nombreBaseDatos IN ({_qlist(dbName)})
      AND nombreInstancia IN ({_qlist(instanceName)});
    """
    recs = exec_sql(sql).get('records', [])
    log("INFO", "Config activa leída", rows=len(recs))
    return spark.createDataFrame(recs) if recs else spark.createDataFrame([], "nombreBaseDatos string, nombreTabla string, indEstadoTrigger boolean")

def _sql_quote(s: str) -> str:
    return (s or "").replace("'", "''")

def fileToProcess_glue(tableName: str, sourceSystem: str,
                       audit_schema: str = AUDIT_SCHEMA,
                       control_table: str = CONTROL_TABLE):
    q = f"""
    SELECT
      archivoDestino AS archivodestino,
      idEjecucion   AS idejecucion
    FROM {audit_schema}.{control_table}
    WHERE nombreTabla   = '{_sql_quote(tableName)}'
      AND sistemaFuente = '{_sql_quote(sourceSystem)}'
      AND estado = 1
      AND (procesado IS DISTINCT FROM TRUE)
      AND (
          CASE
            WHEN TRIM(cantidadRegistros) = '' THEN 0
            WHEN TRANSLATE(TRIM(cantidadRegistros), '0123456789', '') = '' 
              THEN CAST(TRIM(cantidadRegistros) AS INT)
            ELSE 0
          END
        ) > 0;
    """
    recs = exec_sql(q).get('records', [])
    log("INFO", "Pendientes en control", table=tableName, rows=len(recs))
    return spark.createDataFrame(recs) if recs else spark.createDataFrame([], "archivodestino string, idejecucion long")


def loadTableStaging_full_iceberg(sourceTable: str, databaseName: str, targetTable: str, sourceSystem: str,
                                  audit_schema: str = AUDIT_SCHEMA,
                                  control_table: str = CONTROL_TABLE,
                                  raw_bucket: str = RAW_BUCKET,
                                  raw_prefix: str = RAW_PREFIX,
                                  mark_processed: bool = True):
    log("INFO", "[FULL] Preparando", sourceTable=sourceTable, databaseName=databaseName, targetTable=targetTable)
    q = f"""
    SELECT
      idEjecucion   AS idejecucion,
      archivoDestino AS archivodestino
    FROM {audit_schema}.{control_table}
    WHERE nombreTabla = '{sourceTable.replace("'", "''")}'
      AND sistemaFuente = '{sourceSystem.replace("'", "''")}'
      AND estado = 1
      AND COALESCE(cantidadRegistros,0) > 0
    ORDER BY fechaInsertUpdate DESC
    LIMIT 1;
    """
    rows = exec_sql(q).get("records", [])
    if not rows:
        log("WARN", "[FULL] No hay última ejecución exitosa", table=sourceTable)
        return False

    r = rows[0]
    id_ejec = int(r["idejecucion"])
    archivo_destino = (r["archivodestino"] or "").strip('/')
    s3_parquet_path = s3_join(raw_bucket, raw_prefix, archivo_destino)
    log("INFO", "[FULL] Resuelto src", archivo_destino=archivo_destino, s3_parquet_path=s3_parquet_path)
    if not s3_exists_prefix(s3_parquet_path, expect_parquet=True):
        log("WARN", "[FULL] Prefijo RAW no encontrado o sin parquet", path=s3_parquet_path)
        return False

    tgt_fq = f"glue_catalog.{databaseName}.{targetTable}"
    exists = table_exists(databaseName, targetTable)
    log("INFO", "[FULL] Operación", target=tgt_fq, table_exists=exists, source_path=s3_parquet_path)
    if not exists:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{databaseName}")
        spark.sql(f"""CREATE TABLE {tgt_fq} USING iceberg AS SELECT * FROM parquet.`{s3_parquet_path}`""")
        log("INFO", "[CREATE CTAS]", target=tgt_fq)
    else:
        spark.sql(f"""INSERT OVERWRITE {tgt_fq} SELECT * FROM parquet.`{s3_parquet_path}`""")
        log("INFO", "[OVERWRITE]", target=tgt_fq)

    if mark_processed:
        upd = f"UPDATE {audit_schema}.{control_table} SET procesado = 1 WHERE idEjecucion = {id_ejec};"
        exec_sql(upd)
        log("INFO", "[AUDIT] Marcado procesado", idEjecucion=id_ejec)
    return True

def upsertIcebergTable_glue(databaseName: str, targetTable: str, df_updates, pivot_col: str):
    tgt_fq = f"glue_catalog.{databaseName}.{targetTable}"
    if not pivot_col or pivot_col.strip() in ('-', ''):
        log("WARN", "[UPSERT] pivotColumn inválido, se omite", table=tgt_fq, pivotColumn=pivot_col)
        return False

    if df_updates is None:
        log("WARN", "[UPSERT] df_updates vacío", table=tgt_fq)
        return False

    if not table_exists(databaseName, targetTable):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS glue_catalog.{databaseName}")
        df_updates.writeTo(tgt_fq).using("iceberg").createOrReplace()
        log("INFO", "[CREATE TABLE] por UPSERT (bootstrap)", target=tgt_fq)
        return True

    cols = [f.name for f in spark.table(tgt_fq).schema.fields]
    if pivot_col not in cols:
        log("WARN", "[UPSERT] pivotColumn no existe en destino", table=tgt_fq, pivotColumn=pivot_col, columns=cols)
        return False

    tmp_view = f"updates_{databaseName}_{targetTable}_{int(time.time())}"
    df_updates.createOrReplaceTempView(tmp_view)
    set_clause = ", ".join([f"t.{c}=s.{c}" for c in cols if c != pivot_col])
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join([f"s.{c}" for c in cols])

    sql_merge = f"""
        MERGE INTO {tgt_fq} t
        USING (SELECT * FROM {tmp_view}) s
        ON t.{pivot_col} = s.{pivot_col}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    log("DEBUG", "[UPSERT] SQL MERGE", sql=sql_merge)
    spark.sql(sql_merge)
    log("INFO", "[MERGE OK]", target=tgt_fq, pivotColumn=pivot_col)
    return True

def updateAsProcessedFile_glue(idEjecucion: int,
                               audit_schema: str = AUDIT_SCHEMA,
                               control_table: str = CONTROL_TABLE):
    upd = f"UPDATE {audit_schema}.{control_table} SET procesado = 1 WHERE idEjecucion = {int(idEjecucion)};"
    exec_sql(upd)
    log("INFO", "[AUDIT] idEjecucion marcado como procesado", idEjecucion=int(idEjecucion))

# =========================
# Pre-chequeos Warehouse
# =========================
try:
    bkt, pfx = parse_s3_uri(WAREHOUSE_S3 if WAREHOUSE_S3.endswith('/') else WAREHOUSE_S3 + '/')
    s3.head_bucket(Bucket=bkt)
    log("INFO", "Warehouse OK (bucket existe)", bucket=bkt, prefix=pfx)
except Exception as e:
    log("ERROR", "Warehouse inválido o sin permisos", warehouse=WAREHOUSE_S3, error=str(e))

# =========================
# Flujo principal
# =========================
summary = {"full_ok":0, "full_skip":0, "inc_ok":0, "inc_skip":0}

try:
    # 1) Orquestación
    dfTableIngestionAll = (
        spark.table(f"{CATALOG_DB}.{CATALOG_TBL}")
          .filter(col("sourceSystem") == SOURCE_SYSTEM)
          .filter(col("project") == PROJECT_NAME)
          .withColumn("sourceTable", lower(col("sourceTable")))
    )
    df_debug("orquestacion_raw", dfTableIngestionAll)

    # 2) Config activa
    dfActiveTable = activeTableIngestion_glue(
        instanceName=INSTANCE_NAME,
        dbName=SOURCE_DBNAME,
        audit_schema=AUDIT_SCHEMA,
        config_table=CONFIG_TABLE
    )
    df_debug("config_activa", dfActiveTable)

    # 3) Join
    dfJoin = dfTableIngestionAll.join(
        dfActiveTable,
        dfTableIngestionAll["sourceTable"] == dfActiveTable["nombreTabla"],
        "inner"
    )
    df_debug("join_orquestacion_config", dfJoin)

    if dfJoin.rdd.isEmpty():
        log("WARN", "El join quedó vacío. Revisa filtros (sourceSystem/project/instance/base) y nombres normalizados.")
    else:
        # Muestra buckets de salida
        df_debug("destinos_unicos", dfJoin.select(lower(col("databasename")).alias("db"), col("targettable")).dropDuplicates())

    # =========================
    # FULL
    # =========================
    dfJoin = dfJoin.toDF(*[c.lower() for c in dfJoin.columns])
    
    dfFull = (
        dfJoin
          .filter((col("loadtype") == "full") & col("indestadotrigger"))
          .select("sourcetable","databasename","targettable","sourcesystem")
    )
    
    for r in dfFull.toLocalIterator():
        try:
            rd = r.asDict()  # {'sourcetable': ..., 'databasename': ...}
    
            ok = loadTableStaging_full_iceberg(
                sourceTable = rd["sourcetable"],
                databaseName= rd["databasename"],
                targetTable = rd["targettable"],
                sourceSystem= rd["sourcesystem"],
                audit_schema=AUDIT_SCHEMA,
                control_table=CONTROL_TABLE,
                raw_bucket=RAW_BUCKET,
                raw_prefix=RAW_PREFIX,
                mark_processed=False  # marcamos manual luego si todo ok
            )
    
            if ok:
                summary["full_ok"] += 1
            else:
                summary["full_skip"] += 1
    
        except Exception as e:
            summary["full_skip"] += 1
            # usa rd.get(...) por si falló antes de construir rd
            log("ERROR", "[FULL] Excepción",
                table=(rd.get("sourcetable") if 'rd' in locals() else None),
                error=str(e),
                stack=traceback.format_exc())

    # =========================
    # INCREMENTAL
    # =========================
    dfInc = (
        dfJoin
          .filter((col("loadType") == "upsert") & col("indEstadoTrigger"))
          .select("sourceTable","databaseName","targetTable","sourceSystem","pivotColumn")
    )
    df_debug("inc_lista", dfInc)

    if dfInc.rdd.isEmpty():
        log("INFO", "[INC] No hay tablas incrementales activas")

    for t in dfInc.toLocalIterator():
        source_table  = t["sourceTable"]
        source_system = t["sourceSystem"]
        target_db     = t["databaseName"]
        target_tbl    = t["targetTable"]
        pivot_col     = t["pivotColumn"]

        try:
            dfPend = fileToProcess_glue(
                tableName=source_table,
                sourceSystem=source_system,
                audit_schema=AUDIT_SCHEMA,
                control_table=CONTROL_TABLE
            )
            cntPend = dfPend.count()
            log("INFO", "[INC] Pendientes", table=source_table, count=cntPend)
            if cntPend == 0:
                summary["inc_skip"] += 1
                continue

            for r in dfPend.toLocalIterator():
                archivo_destino = (r["archivodestino"] or "").strip('/')
                id_ejec = int(r["idejecucion"])
                src_path = s3_join(RAW_BUCKET, RAW_PREFIX, archivo_destino)
                log("INFO", "[INC] Resuelto src", archivo_destino=archivo_destino, src_path=src_path, idEjec=id_ejec)
                if not s3_exists_prefix(src_path, expect_parquet=True):
                    log("WARN", "[INC] Prefijo RAW inexistente/sin parquet, se omite", table=source_table, path=src_path, idEjec=id_ejec)
                    summary["inc_skip"] += 1
                    continue

                dfp = spark.read.parquet(src_path)
                df_debug(f"inc_input_{source_table}", dfp, n=3)

                ok = upsertIcebergTable_glue(
                    databaseName=target_db,
                    targetTable=target_tbl,
                    df_updates=dfp,
                    pivot_col=pivot_col
                )
                if ok:
                    updateAsProcessedFile_glue(idEjecucion=id_ejec, audit_schema=AUDIT_SCHEMA, control_table=CONTROL_TABLE)
                    summary["inc_ok"] += 1
                else:
                    summary["inc_skip"] += 1

        except Exception as e:
            summary["inc_skip"] += 1
            log("ERROR", "[INC] Excepción", table=source_table, error=str(e), stack=traceback.format_exc())

except Exception as e:
    log("ERROR", "Falla en flujo principal", error=str(e), stack=traceback.format_exc())
    raise
finally:
    log("INFO", "RESUMEN",
        full_ok=summary["full_ok"], full_skip=summary["full_skip"],
        inc_ok=summary["inc_ok"], inc_skip=summary["inc_skip"])
    print("[FIN] Proceso FULL+INCR con Iceberg finalizado.")
