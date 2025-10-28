import sys, json, os, time, re
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

#############################################
# ARGS (solo obligatorios)
#############################################
required_args = [
    'mode',                 # full | incr
    'sql_query',            # consulta base (subquery válida)
    'output_prefix',        # s3://bucket/carpeta/
    'metrics_path',         # s3://bucket/metrics/...
    'archivo_nombre',       # ej. tabla.parquet
    'pivot_type',           # none|datetime|int
]
args = getResolvedOptions(sys.argv, required_args)

# Opcionales: leerlos SIN exigirlos a Glue (si no vienen, no falla)
def get_opt_arg(name, default=''):
    flag = f'--{name}'
    if flag in sys.argv:
        i = sys.argv.index(flag)
        if i + 1 < len(sys.argv):
            return sys.argv[i + 1]
    return os.environ.get(name.upper(), default)

mode           = args['mode']          # full | incr
sql_query      = args['sql_query'].strip()
# por si vino con ';' al final (UNLOAD falla con ';')
sql_query      = re.sub(r';\s*$', '', sql_query)

output_prefix  = args['output_prefix'].rstrip('/') + '/'
metrics_path   = args['metrics_path'].rstrip('/') + '/'
archivo_nombre = args['archivo_nombre']
pivot_type     = args['pivot_type']    # none|datetime|int

pivot_from     = get_opt_arg('pivot_from', '')
pivot_column   = get_opt_arg('pivot_column', '')

# Redshift por ENV
rs_workgroup   = os.environ.get('REDSHIFT_WORKGROUP', 'dl-workgroup-dev-rs')
secret_arn     = os.environ.get('REDSHIFT_SECRET_ARN', 'arn:aws:secretsmanager:us-east-1:637423369807:secret:redshift/finandina/dev-CkFGPc')
rs_iam_role    = os.environ.get('REDSHIFT_IAM_ROLE', 'arn:aws:iam::637423369807:role/redshift-finandina-role')
unload_prefix = os.environ.get('UNLOAD_PREFIX', 's3://dl-raw-dev-s3/temp/unload/')


# Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# AWS clients
# Región explícita del workgroup Redshift Serverless
rs_region = os.environ.get('REDSHIFT_REGION', 'us-east-1')
rsd = boto3.client('redshift-data', region_name=rs_region)
secrets = boto3.client('secretsmanager', region_name=rs_region)
s3 = boto3.client('s3', region_name=os.environ.get('S3_REGION', rs_region))
secrets = boto3.client('secretsmanager', region_name=rs_region)

# Database desde el Secret (dbname)
sec_val = secrets.get_secret_value(SecretId=secret_arn)
sec_obj = json.loads(sec_val['SecretString'])
rs_database = sec_obj.get('dbname') or sec_obj.get('db') or 'dl_dev'  # fallback prudente

#############################################
# Helpers Redshift Data API
#############################################
def exec_rs(sql: str, with_results: bool=False):
    resp = rsd.execute_statement(
        WorkgroupName=rs_workgroup,
        Database=rs_database,
        SecretArn=secret_arn,
        Sql=sql
    )
    sid = resp['Id']

    # Poll
    while True:
        d = rsd.describe_statement(Id=sid)
        status = d['Status']
        if status in ('FINISHED', 'FAILED', 'ABORTED'):
            if status != 'FINISHED':
                raise RuntimeError(f"Redshift statement {status}: {d.get('Error', 'Unknown error')} (Id={sid})")
            break
        time.sleep(0.8)

    if not with_results:
        return {'Id': sid, 'Status': 'FINISHED'}

    # get results (pequeños)
    all_records = []
    next_token = None
    colmeta = None
    while True:
        kw = {'Id': sid}
        if next_token:
            kw['NextToken'] = next_token
        rr = rsd.get_statement_result(**kw)
        if colmeta is None:
            colmeta = rr.get('ColumnMetadata', [])
        all_records.extend(rr.get('Records', []))
        next_token = rr.get('NextToken')
        if not next_token:
            break
    return {'Id': sid, 'Status': 'FINISHED', 'Records': all_records, 'ColumnMetadata': colmeta or []}

def get_scalar(sql: str):
    out = exec_rs(sql, with_results=True)
    recs = out['Records']
    if not recs:
        return None
    cell = recs[0][0]  # primera fila, primera columna
    for _, v in cell.items():
        return v
    return None

#############################################
# Construcción de la consulta final (incremental)
#############################################
final_query = sql_query
pivot_to_val = "-"

if mode == 'incr' and pivot_type != 'none' and pivot_column:
    # proteger nombres problemáticos en UNLOAD interno
    safe_pivot_col = pivot_column.strip()

    if pivot_type == 'datetime':
        maxq = f"SELECT MAX({safe_pivot_col}) AS valorPivot FROM ({sql_query}) T"
    else:
        maxq = f"SELECT MAX(CAST({safe_pivot_col} AS BIGINT)) AS valorPivot FROM ({sql_query}) T"

    max_val = get_scalar(maxq)
    pivot_to_val = str(max_val) if max_val is not None else pivot_from

    if pivot_type == 'datetime':
        final_query = f"""
        SELECT * FROM ({sql_query}) X
        WHERE X.{safe_pivot_col} > CAST('{pivot_from}' AS TIMESTAMP)
          AND X.{safe_pivot_col} <= CAST('{pivot_to_val}' AS TIMESTAMP)
        """
    else:
        from_val = int(pivot_from or 0)
        to_val   = int(pivot_to_val or 0)
        final_query = f"""
        SELECT * FROM ({sql_query}) X
        WHERE CAST(X.{safe_pivot_col} AS BIGINT) >= {from_val}
          AND CAST(X.{safe_pivot_col} AS BIGINT) <= {to_val}
        """

#############################################
# Métrica total_source (sobre la consulta base)
#############################################
total_source_q = f"SELECT COUNT(1) AS c FROM ({sql_query}) Z"
total_source_raw = get_scalar(total_source_q)
total_source = int(total_source_raw or 0)

#############################################
# UNLOAD (CSV, PARALLEL OFF) a S3 temporal
#############################################
ts = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
run_tmp_prefix = f"{unload_prefix}run_{ts}/"     # ej. s3://bucket/tmp/unload/run_YYYY.../
unload_sql = f"""
UNLOAD ('{final_query.replace("'", "''")}')
TO '{run_tmp_prefix}'
IAM_ROLE '{rs_iam_role}'
FORMAT AS CSV
DELIMITER ','
HEADER
ALLOWOVERWRITE
PARALLEL OFF
EXTENSION 'csv';
"""



exec_rs(unload_sql, with_results=False)

#############################################
# Ubicar el único archivo CSV generado
#############################################
m = re.match(r's3://([^/]+)/(.+)', run_tmp_prefix)
if not m:
    raise RuntimeError(f"Prefijo UNLOAD inválido: {run_tmp_prefix}")
bucket_unload, prefix_unload = m.group(1), m.group(2)

resp = s3.list_objects_v2(Bucket=bucket_unload, Prefix=prefix_unload)
contents = resp.get('Contents', []) or []
csv_keys = [o['Key'] for o in contents if re.search(r'\.csv$', o['Key'])]
if not csv_keys:
    # fallback por si aparece .000
    csv_keys = [o['Key'] for o in contents if re.search(r'\.000$', o['Key'])]

if not csv_keys:
    raise RuntimeError(f"No se encontró archivo CSV en {run_tmp_prefix}. Objetos: {[o['Key'] for o in contents]}")

csv_key = csv_keys[0]
csv_uri = f"s3://{bucket_unload}/{csv_key}"

#############################################
# Leer CSV con Spark y escribir Parquet único con nombre exacto
#############################################
df = spark.read.csv(csv_uri, header=True, inferSchema=True)
rows = df.count()

target_path = output_prefix + archivo_nombre                # nombre exacto deseado
tmp_dir = output_prefix + "_tmp_write_" + ts + "/"
df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)

# Renombrar part-*.parquet → archivo_nombre (copy+delete)
m2 = re.match(r's3://([^/]+)/(.+)', tmp_dir)
if not m2:
    raise RuntimeError(f"tmp_dir inválido: {tmp_dir}")
bucket_out, prefix_out = m2.group(1), m2.group(2)

resp2 = s3.list_objects_v2(Bucket=bucket_out, Prefix=prefix_out)
objs2 = resp2.get('Contents', []) or []
part_key = next((o['Key'] for o in objs2 if o['Key'].endswith('.parquet')), None)

if not part_key:
    raise RuntimeError(f"No se encontró .parquet en {tmp_dir}")

dest = re.match(r's3://([^/]+)/(.+)', target_path)
if not dest:
    raise RuntimeError(f"target_path inválido: {target_path}")
dest_bucket, dest_key = dest.group(1), dest.group(2)

s3.copy_object(Bucket=dest_bucket, CopySource={'Bucket': bucket_out, 'Key': part_key}, Key=dest_key)

# limpia tmp parquet
for o in objs2:
    s3.delete_object(Bucket=bucket_out, Key=o['Key'])

# limpia tmp unload CSV
for o in contents:
    s3.delete_object(Bucket=bucket_unload, Key=o['Key'])

#############################################
# Escribir métricas
#############################################
metrics = {
    "rows_copied": rows,
    "total_source": total_source,
    "pivot_to": pivot_to_val if mode == 'incr' else "-",
    "archivo_nombre": archivo_nombre
}
mp = metrics_path + ts + "_metrics.json"
m_uri = re.match(r's3://([^/]+)/(.+)', mp)
if not m_uri:
    raise RuntimeError(f"metrics_path inválido: {mp}")
s3.put_object(Bucket=m_uri.group(1), Key=m_uri.group(2),
              Body=json.dumps(metrics, ensure_ascii=False).encode('utf-8'))
print(json.dumps(metrics, ensure_ascii=False))
