"""
Transform
"""
import sys
import gc
import boto3
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

# Inicializar Spark y Glue context
SPARK_CONTEXT = SparkContext.getOrCreate()
glue_context = GlueContext(SPARK_CONTEXT)
spark = glue_context.spark_session

# Configuración recomendada para evitar problemas de incompatibilidad de fechas con Parquet
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")

# Asignar variables
args = getResolvedOptions(sys.argv, ['ENVIRONMENT', 'SOURCE','DATABASE_ONPREMISE',
       'TABLE','BUCKET_RAW','BUCKET_TARGET','BUCKET_RESULT','NAME_PARTITION_RAW',
       'VALUE_PARTITION_RAW','NAME_PARTITION_MASTER','COLUMNS_INFO_MASTER','TYPE_LOAD',
       'DATABASE_ORIGIN','DATABASE_TARGET','APLICATION'])


table = args['TABLE']
source = args['SOURCE']
database_onpremise = args['DATABASE_ONPREMISE']
bucket_raw = args['BUCKET_RAW']
bucketTarget = args['BUCKET_TARGET']
bucketResult = args['BUCKET_RESULT']
name_partition_raw = args['NAME_PARTITION_RAW']
value_partition_raw = args['VALUE_PARTITION_RAW']
name_partition_master = args ['NAME_PARTITION_MASTER']
columnInfo = args['COLUMNS_INFO_MASTER']
type_load = args['TYPE_LOAD']
databaseOrigin = args['DATABASE_ORIGIN']
databaseTarget = args['DATABASE_TARGET']
aplication=args['APLICATION']

# Función para actualizar las particiones en Athena
def update_partitions_in_athena(database, table_, s3_location, partition):
    """funcion"""
    athena = boto3.client('athena')
    query = f"ALTER TABLE {database}.{table_} ADD IF NOT EXISTS PARTITION ({partition})"
    s3_location=f"s3://{s3_location}"
    athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_location}
    )


# Leer datos de la base de datos de origen usando DynamicFrame
table_name_raw = f"{aplication}__{table}".lower()
print("=========")
print(databaseOrigin)
print(table_name_raw)
print(name_partition_raw)
print(value_partition_raw)
print(name_partition_master)
if type_load=="full":
    path = f"s3://{bucket_raw}/view/{source}/{aplication}/{database_onpremise}/{table}/"
    df = spark.read.parquet(path)
else:
    path= f"s3://{bucket_raw}/view/{source}/{aplication}/{database_onpremise}/{table}/{name_partition_raw}={value_partition_raw}/"
    df = spark.read.parquet(path)
print("Leyendo de ",path)

#Actualizar date_created and date_updated
df=df.withColumn("date_updated",from_utc_timestamp(split(current_timestamp(),'\+')[0],'America/Lima'))
df=df.withColumn("date_created",from_utc_timestamp(split(current_timestamp(),'\+')[0],'America/Lima'))


if type_load=="full":
    output_path = f"s3://{bucketTarget}/view/nodomain/{table}/"
    df.write.mode("overwrite").parquet(output_path)
else:
    output_path = f"s3://{bucketTarget}/view/nodomain/{table}/{name_partition_master}={value_partition_raw}"
    df.write.mode("overwrite").parquet(output_path)
    # Ejecutar la función para actualizar las particiones
    table_name_master = f"nodomain__{table}".lower()
    partition_value = f"{name_partition_master}='{value_partition_raw}'"  # Valor de la partición
    update_partitions_in_athena(databaseTarget, table_name_master, bucketResult, partition_value)
print("Escribiendo en ", output_path)
gc.collect()
print('OK')
