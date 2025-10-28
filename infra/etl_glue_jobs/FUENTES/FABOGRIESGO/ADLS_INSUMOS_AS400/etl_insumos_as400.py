# glue_version: 5.0
# python: 3.11 (Glue 5.0)
# Requisitos opcionales si usas Delta Lake:
#   - Additional Python modules: delta-spark==3.3.0
#   - Job parameters (o en código): 
#       spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
#       spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

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

# ---- S3 (bronze/raw y delta)
RAW_BUCKET    = get_opt_arg('raw_bucket',   'dl-raw-dev-s3')
RAW_PREFIX    = get_opt_arg('raw_prefix',   'bronze/')
DELTA_BUCKET  = get_opt_arg('delta_bucket', 'dl-silver-dev-s3')
DELTA_PREFIX  = get_opt_arg('delta_prefix', 'delta/')

# ---- Auditoría / Config en Redshift
AUDIT_SCHEMA  = get_opt_arg('audit_schema',  'dbo')
CONTROL_TABLE = get_opt_arg('control_table', 'controlPipelineLight_gen2')
CONFIG_TABLE  = get_opt_arg('config_table',  'ConfigADFMetadataDriven')

# ---- Catálogo Glue (tabla de orquestación inicial)
CATALOG_DB    = get_opt_arg('catalog_db',    'bronze')
CATALOG_TBL   = get_opt_arg('catalog_tbl',   'load_raw_bronze')

# ---- Filtros y equivalentes ADF
INSTANCE_NAME = get_opt_arg('instance_name', 'FABOGRIESGO')
SOURCE_DBNAME = get_opt_arg('source_dbname', 'insumosAS400')
SOURCE_SYSTEM = get_opt_arg('source_system', 'insumosAS400')
PROJECT_NAME  = get_opt_arg('project',       'DWH')

print("[INFO] Parámetros efectivos:")
print(json.dumps({
    "SECRET_ARN": SECRET_ARN,
    "WORKGROUP": WORKGROUP,
    "REDSHIFT_DB": REDSHIFT_DB,
    "RAW_BUCKET": RAW_BUCKET,
    "RAW_PREFIX": RAW_PREFIX,
    "DELTA_BUCKET": DELTA_BUCKET,
    "DELTA_PREFIX": DELTA_PREFIX,
    "AUDIT_SCHEMA": AUDIT_SCHEMA,
    "CONTROL_TABLE": CONTROL_TABLE,
    "CONFIG_TABLE": CONFIG_TABLE,
    "CATALOG_DB": CATALOG_DB,
    "CATALOG_TBL": CATALOG_TBL,
    "INSTANCE_NAME": INSTANCE_NAME,
    "SOURCE_DBNAME": SOURCE_DBNAME,
    "SOURCE_SYSTEM": SOURCE_SYSTEM,
    "PROJECT_NAME": PROJECT_NAME
}, indent=2))

# =========================
# Spark / GlueContext
# =========================
spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")
glueContext = GlueContext(sc)

# =========================
# Clientes AWS
# =========================
rsd = boto3.client('redshift-data')
s3  = boto3.client('s3')

# =========================
# Utils
# =========================
def exec_sql(sql: str):
    """
    Ejecuta SQL en Redshift (Data API Serverless) y retorna:
      - SELECT: {'records': [ {col: val, ...}, ... ]}
      - DDL/DML: {'rowcount': <int|None>}
    """
    exec_id = rsd.execute_statement(
        WorkgroupName=WORKGROUP,
        SecretArn=SECRET_ARN,
        Database=REDSHIFT_DB,
        Sql=sql
    )['Id']

    # Polling
    while True:
        stat = rsd.describe_statement(Id=exec_id)
        status = stat['Status']
        if status in ('FINISHED', 'FAILED', 'ABORTED'):
            break
        time.sleep(1.0)

    if stat['Status'] != 'FINISHED':
        raise RuntimeError(f"SQL failed: {stat.get('Error', 'Unknown error')}\nSQL:\n{sql}")

    result = {'records': [], 'rowcount': None}
    if stat.get('HasResultSet'):
        # Paginación de resultados
        next_token = None
        cols = None
        while True:
            kw = {'Id': exec_id}
            if next_token:
                kw['NextToken'] = next_token
            page = rsd.get_statement_result(**kw)
            if cols is None:
                cols = [c['name'] for c in page['ColumnMetadata']]
            for row in page['Records']:
                parsed = []
                for cell in row:
                    if 'stringValue' in cell:   parsed.append(cell['stringValue'])
                    elif 'longValue' in cell:   parsed.append(cell['longValue'])
                    elif 'doubleValue' in cell: parsed.append(cell['doubleValue'])
                    elif 'booleanValue' in cell:parsed.append(cell['booleanValue'])
                    elif cell.get('isNull'):    parsed.append(None)
                    else:                       parsed.append(next(iter(cell.values())))
                result['records'].append(dict(zip(cols, parsed)))
            next_token = page.get('NextToken')
            if not next_token:
                break
    else:
        # describe_statement expone "ResultRows" para filas devueltas/afectadas
        result['rowcount'] = stat.get('ResultRows')
    return result

def qt(name: str) -> str:
    return f"`{name}`"

def s3_join(*parts) -> str:
    return 's3://' + '/'.join([str(x).strip('/') for x in parts if str(x).strip('/') != ''])

# =========================
# Funciones de negocio (Glue)
# =========================
def activeTableIngestion_glue(instanceName: str, dbName: str,
                              audit_schema: str = AUDIT_SCHEMA,
                              config_table: str = CONFIG_TABLE):
    """
    Lee tablas activas desde Redshift (config metadata-driven).
    Retorna DF con columnas: nombreBaseDatos, nombreTabla (normalizada), indEstadoTrigger
    """
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
    res = exec_sql(sql).get('records', [])
    if not res:
        # esquema mínimo esperado
        return spark.createDataFrame([], "nombreBaseDatos string, nombreTabla string, indEstadoTrigger boolean")
    return spark.createDataFrame(res)

def fileToProcessDeltaLake_glue(tableName: str, sourceSystem: str,
                                audit_schema: str = AUDIT_SCHEMA,
                                control_table: str = CONTROL_TABLE):
    """
    Devuelve DF con archivos pendientes (estado=1, procesado=0, cantidadRegistros>0)
    columnas: archivoDestino, idEjecucion
    """
    tn = tableName.replace("'", "''")
    ss = sourceSystem.replace("'", "''")
    sql = f"""
        SELECT archivoDestino, idEjecucion
        FROM {audit_schema}.{control_table}
        WHERE nombreTabla   = '{tn}'
          AND sistemaFuente = '{ss}'
          AND estado = 1
          AND COALESCE(procesado,0) = 0
          AND COALESCE(cantidadRegistros,0) > 0;
    """
    res = exec_sql(sql).get('records', [])
    if not res:
        return spark.createDataFrame([], "archivoDestino string, idEjecucion long")
    return spark.createDataFrame(res)

def loadTableStaging_glue(sourceTable: str, databaseName: str, targetTable: str, sourceSystem: str,
                          audit_schema: str = AUDIT_SCHEMA,
                          control_table: str = CONTROL_TABLE,
                          raw_bucket: str = RAW_BUCKET,
                          raw_prefix: str = RAW_PREFIX,
                          mark_processed: bool = True):
    """
    Carga FULL:
      1) Busca la última ejecución exitosa en auditoría.
      2) Lee parquet desde s3://<raw_bucket>/<raw_prefix>/<archivoDestino>
      3) INSERT OVERWRITE sobre la tabla destino (Catálogo Glue)
      4) (opcional) marca como procesado
    """
    q = f"""
    SELECT idEjecucion, archivoDestino
    FROM {audit_schema}.{control_table}
    WHERE nombreTabla = '{sourceTable.replace("'", "''")}'
      AND sistemaFuente = '{sourceSystem.replace("'", "''")}'
      AND estado = 1
      AND COALESCE(cantidadRegistros,0) > 0
      AND fechaInsertUpdate = (
          SELECT MAX(fechaInsertUpdate)
          FROM {audit_schema}.{control_table}
          WHERE nombreTabla = '{sourceTable.replace("'", "''")}'
            AND sistemaFuente = '{sourceSystem.replace("'", "''")}'
            AND estado = 1
            AND COALESCE(cantidadRegistros,0) > 0
      )
    LIMIT 1;
    """
    rows = exec_sql(q).get("records", [])
    if not rows:
        raise RuntimeError(f"No se halló última ejecución exitosa para {sourceTable} / {sourceSystem}")

    r = rows[0]
    id_ejec = int(r["idEjecucion"])
    archivo_destino = (r["archivoDestino"] or "").strip('/')

    if not raw_bucket:
        raise ValueError("raw_bucket no puede ser None")

    s3_parquet_path = s3_join(raw_bucket, raw_prefix, archivo_destino)
    full_table = f"{qt(databaseName)}.{qt(targetTable)}"

    spark.sql(f"REFRESH TABLE {full_table}")
    spark.sql(f"INSERT OVERWRITE TABLE {full_table} SELECT * FROM parquet.`{s3_parquet_path}`")

    if mark_processed:
        upd = f"UPDATE {audit_schema}.{control_table} SET procesado = 1 WHERE idEjecucion = {id_ejec};"
        exec_sql(upd)

    print(f"[FULL OK] {databaseName}.{targetTable} <= {s3_parquet_path} (idEjecucion={id_ejec})")

# Delta Lake (requiere delta-spark instalado en el Job)
from delta.tables import DeltaTable

def upsertDeltaTable_glue(s3_delta_path: str, df, campoPivot: str):
    """
    UPSERT (MERGE) contra DeltaTable en S3 usando 'campoPivot' como clave.
    """
    delta_tbl = DeltaTable.forPath(spark, s3_delta_path)
    (delta_tbl.alias("t")
     .merge(df.alias("u"), f"t.{campoPivot} = u.{campoPivot}")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())

def updateAsProcessedFile_glue(idEjecucion: int,
                               audit_schema: str = AUDIT_SCHEMA,
                               control_table: str = CONTROL_TABLE):
    sql = f"UPDATE {audit_schema}.{control_table} SET procesado = 1 WHERE idEjecucion = {int(idEjecucion)};"
    exec_sql(sql)
    print(f"[AUDIT] idEjecucion {idEjecucion} marcado como procesado")

# =========================
# Flujo principal
# =========================

# 1) Lee definición desde Catálogo (bronze.load_raw_bronze)
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=CATALOG_DB,
    table_name=CATALOG_TBL
)
dfTableIngestionAll = (
    dyf.toDF()
      .filter(col("sourceSystem") == SOURCE_SYSTEM)
      .filter(col("project") == PROJECT_NAME)
      .withColumn("sourceTable", lower(col("sourceTable")))
)

# 2) Config activa (en Redshift)
dfActiveTable = activeTableIngestion_glue(
    instanceName=INSTANCE_NAME,
    dbName=SOURCE_DBNAME,
    audit_schema=AUDIT_SCHEMA,
    config_table=CONFIG_TABLE
)

# 3) Join por nombre de tabla normalizado
dfTableIngestionAll = dfTableIngestionAll.join(
    dfActiveTable,
    dfTableIngestionAll["sourceTable"] == dfActiveTable["nombreTabla"],
    "inner"
)

# 4) Refresh de tablas destino (únicas)
df_to_refresh = (
    dfTableIngestionAll
      .filter(col("loadType") != "disabled")
      .select(lower(col("databaseName")).alias("db"), col("targetTable").alias("tbl"))
      .dropDuplicates()
)

for r in df_to_refresh.toLocalIterator():
    full_name = f"{qt(r.db)}.{qt(r.tbl)}"
    spark.sql(f"REFRESH TABLE {full_name}")
    print(f"[REFRESH] {r.db}.{r.tbl}")

# =========================
# Carga FULL
# =========================
dfTableIngestionFull = (
    dfTableIngestionAll
      .filter((col("loadType") == "full") & col("indEstadoTrigger"))
      .select("sourceTable", "databaseName", "targetTable", "sourceSystem")
)

for r in dfTableIngestionFull.toLocalIterator():
    loadTableStaging_glue(
        sourceTable=r["sourceTable"],
        databaseName=r["databaseName"],
        targetTable=r["targetTable"],
        sourceSystem=r["sourceSystem"],
        audit_schema=AUDIT_SCHEMA,
        control_table=CONTROL_TABLE,
        raw_bucket=RAW_BUCKET,
        raw_prefix=RAW_PREFIX,
        mark_processed=True
    )
    print(f"[FULL DONE] {r['databaseName']}.{r['targetTable']}")

# =========================
# Carga INCREMENTAL (UPSERT → Delta)
# =========================
dfTableIngestionInc = (
    dfTableIngestionAll
      .filter((col("loadType") == "upsert") & col("indEstadoTrigger"))
      .select("sourceTable", "databaseName", "targetTable", "sourceSystem", "pivotColumn")
)

if dfTableIngestionInc.rdd.isEmpty():
    print("[INC] No hay tablas incrementales activas")

for t in dfTableIngestionInc.toLocalIterator():
    source_table  = t["sourceTable"]
    source_system = t["sourceSystem"]
    target_db     = t["databaseName"]
    target_tbl    = t["targetTable"]
    pivot_col     = t["pivotColumn"]

    dfSourceTable = fileToProcessDeltaLake_glue(
        tableName=source_table,
        sourceSystem=source_system,
        audit_schema=AUDIT_SCHEMA,
        control_table=CONTROL_TABLE
    )

    if dfSourceTable.rdd.isEmpty():
        print(f"[INC] No hay archivos pendientes para {source_table}")
        continue

    print(f"[INC] Inicio proceso: {source_table}")

    for r in dfSourceTable.toLocalIterator():
        archivo_destino = (r["archivoDestino"] or "").strip('/')
        id_ejec         = int(r["idEjecucion"])

        s3_parquet_path = s3_join(RAW_BUCKET, RAW_PREFIX, archivo_destino)
        dfp = spark.read.parquet(s3_parquet_path)

        delta_target_path = s3_join(DELTA_BUCKET, DELTA_PREFIX, target_db, target_tbl)

        upsertDeltaTable_glue(
            s3_delta_path=delta_target_path,
            df=dfp,
            campoPivot=pivot_col
        )

        updateAsProcessedFile_glue(
            idEjecucion=id_ejec,
            audit_schema=AUDIT_SCHEMA,
            control_table=CONTROL_TABLE
        )

        print(f"[INC OK] {target_db}.{target_tbl} <= {s3_parquet_path} (idEjecucion={id_ejec})")

print("[FIN] Proceso incremental/UPSERT finalizado.")
