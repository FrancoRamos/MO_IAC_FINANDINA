# Importing packages
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType, DoubleType, BooleanType,LongType
from pyspark.sql.functions import *
import os
import shutil
import unicodedata
import boto3
import time
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F

# Importar tu librería personalizada
import transversales_aws02 as transversales_lib


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
'source_bucketraw', 
'source_bucketmaster',
'source_database_raw',
'source_database_master',
'source_database_stage',
'target_bucket'
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Iceberg + Glue Catalog configuration
spark.conf.set('spark.sql.defaultCatalog', 'glue_catalog')
spark.conf.set('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
spark.conf.set('spark.sql.catalog.glue_catalog.warehouse', 's3://dl-master-dev-s3/')
spark.conf.set('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
spark.conf.set('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')



#job.init(args['JOB_NAME'], args)

print("Init glue job>>", args)

#Llamado a funciones Transversales
#import libify
#transversales = libify.importer(globals(), '/Repos/Data Engineering/include/function')
transversales = transversales_lib

#print("init dfTableIngestion::::")
#dfTableIngestion =spark.read.table("dl_raw_dev_glue.load_raw_bronze")
#dfTableIngestion =dfTableIngestion.filter(dfTableIngestion.sourcesystem=="FilesFive9")
#print(dfTableIngestion.show())


print("init df_init_sfc_entidad::::")
df_init_sfc_entidad = spark.sql("""
select 
  fkTipoEntidad, 
  entidadId,
  entidadDesc
from dl_master_dev_glue.sfc_entidad
""")

print(df_init_sfc_entidad.show())

#transversales_lib.loadDwhStagingTable(df_init,"Tmp_sfc_entidad","overwrite")
url = "jdbc:redshift:iam://dl-workgroup-dev-rs.637423369807.us-east-1.redshift-serverless.amazonaws.com:5439/dl_dev"
temp_dir = "s3://dl-raw-dev-s3/temp/info-load/info-stg-loadredshift/"
dbtable = "public.stg_tmp_sfc_entidad" #"stg.tmp_sfc_entidad" #target
aws_iam_role_redshift = 'arn:aws:iam::637423369807:role/redshift-finandina-role'
# Write the Spark DataFrame to Redshift
# io.github.spark_redshift.DefaultSource
### temporal sfc_entidad_temp
pre_query = """drop table if exists public.sfc_entidad_temp;
create table public.sfc_entidad_temp as select * from public.tmp_sfc_entidad02 where 1=2;"""
post_query = """begin;
insert into public.tmp_sfc_entidad02 select * from public.sfc_entidad_temp;
drop table public.sfc_entidad_temp; end;"""

### opt.01:
df_init_sfc_entidad.write \
    .format("jdbc") \
    .option("url", url) \
    .option("aws_iam_role", aws_iam_role_redshift) \
    .option("driver", "com.amazon.redshift.jdbc.Driver") \
    .option("dbtable", dbtable) \
    .option("tempdir", temp_dir) \
    .mode("overwrite") \
    .save()

### opt.02:
#df_init_sfc_entidad.write \
#    .format("io.github.spark_redshift_community.spark.redshift") \
#    .option("url", url) \
#    .option("aws_iam_role", aws_iam_role_redshift) \
#    .option("dbtable", dbtable) \
#    .option("tempdir", temp_dir) \
#    .mode("overwrite") \
#    .save()



print("init df_init_sfc_tipoentidad::::")
#df_init_sfc_tipoentidad = spark.sql("""
#select 
#  tipoEntidadId, 
#  tipoEntidadDesc
#from silver.sfc_tipoentidad
#""")

#print(df_init_sfc_tipoentidad.show())
  
#transversales_lib.loadDwhStagingTable(df,"Tmp_sfc_tipoEntidad","overwrite")




CONTROL_TABLE       = 'public.control_pipeline_light_gen2' # O el esquema y tabla que corresponda
REDSHIFT_WORKGROUP = 'dl-workgroup-dev-rs'#os.environ.get('REDSHIFT_WORKGROUP') # o REDSHIFT_CLUSTER_IDENTIFIER
REDSHIFT_DATABASE = 'dl_dev'#os.environ['REDSHIFT_DATABASE']
REDSHIFT_SECRET_ARN = 'arn:aws:secretsmanager:us-east-1:637423369807:secret:dl-redshift-admin-dev-secrets-mngnvn'#get_secret()#os.environ['REDSHIFT_SECRET_ARN']
REDSHIFT_IAM_ROLE_ARN = 'arn:aws:iam::637423369807:role/redshift-finandina-role'#os.environ['REDSHIFT_IAM_ROLE_ARN']
redshift_table_name = "controlpipelinelight_gen2" # Replace with your table name
print(f"Redshift table name: {redshift_table_name} :::: INIT:")

try:
    redshift_data_client = boto3.client('redshift-data', region_name='us-east-1')
    print("Iniciado cliente boto3 redshift-data::::>")
except Exception as e:
    print(f"Error inicializando el cliente de Redshift Data API: {e}")
    redshift_data_client = None


def fileToProcessDeltaLake(spark, tableName, sourceSystem):
    """
        Consulta la tabla de control en Redshift para obtener los archivos pendientes de procesar.        
        Args:
            spark (SparkSession): La sesión de Spark activa en Glue.
            tableName (str): El nombre de la tabla de origen a consultar.
            sourceSystem (str): El nombre del sistema de origen.            
        Returns:
            DataFrame: Un DataFrame de Spark con la lista de archivos pendientes.
    """
    print(f"Buscando archivos pendientes para la tabla '{tableName}' del sistema '{sourceSystem}'...")
    
    if not redshift_data_client:
        raise ConnectionError("El cliente de Redshift Data API no está inicializado.")

    sql_query = f"""
        SELECT archivoDestino, idEjecucion FROM {CONTROL_TABLE}
        WHERE nombreTabla = '{tableName}'
          AND sistemaFuente = '{sourceSystem}'
          AND estado = 1
          AND procesado = 0
          AND cantidadRegistros >0
    """
    
    try:
    
        # Parámetros de la ejecución
        exec_params = {
            'SecretArn': REDSHIFT_SECRET_ARN,
            'Database': REDSHIFT_DATABASE,
            'Sql': sp_sql,
            'StatementName': 'sp_insert_five9webservice_test01'
        }
        
        if REDSHIFT_WORKGROUP:
            exec_params['WorkgroupName'] = REDSHIFT_WORKGROUP

        print(f"exec_params SQL: {exec_params}")

        # Ejecutar la consulta
        #exec_response = client.execute_statement(**exec_params)
        exec_response = redshift_data_client.execute_statement(**exec_params)
        statement_id = exec_response['Id']
        
        
        # Ejecutar la consulta usando la API de Datos de Redshift
        #response = redshift_data_client.execute_statement(
        #    ClusterIdentifier=REDSHIFT_CLUSTER_ID,
        #    Database=REDSHIFT_DATABASE,
        #    DbUser=REDSHIFT_DB_USER,
        #    Sql=sql_query,
        #    
        #)
        
        #statement_id = response['Id']
        
        # Esperar y obtener el resultado
        desc = redshift_data_client.describe_statement(Id=statement_id)
        while desc['Status'] in ['SUBMITTED', 'PICKED', 'STARTED']:
            time.sleep(1)
            desc = redshift_data_client.describe_statement(Id=statement_id)

        if desc['Status'] == 'FINISHED':
            print("Leyendo datos de control directamente desde Redshift con el conector de Spark...")
            pending_files_df = (spark.read
                .format("redshift")
                .option("url", \
                f"jdbc:redshift://{REDSHIFT_CLUSTER_ID}.us-east-1.redshift.amazonaws.com:5439/{REDSHIFT_DATABASE}") ###*
                .option("user", REDSHIFT_DB_USER)
                .option("password", "uSerEtl01L#ibrarY") ### **change
                #"aws_iam_role": "arn:aws:iam::account id:role/rs-role-name"
                #.option("aws_iam_role", "<your-aws-role-arn>") \arn:aws:iam::637423369807:role/dl-lambda-execution-dev-iam
                .option("aws_iam_role", f"arn:aws:iam::{accid}:role/dl-etls-dev-role-redshiftlib") \
                .option("query", sql_query)
                .option("tempdir", "s3://dl-analytics-dev-s3/temp/redshift") # Necesario
                .load()
            )
            # url = "jdbc:redshift:iam://redshifthost:5439/database"
            url = "jdbc:redshift:iam://dl-workgroup-dev-rs.637423369807.us-east-1.redshift-serverless.amazonaws.com:5439/dl_dev"
            df = sql_context.read \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", url) \
                .option("dbtable", "tableName") \
                .option("tempdir", "s3://dl-raw-dev-s3/temp/results-redshift/") \
                .option("aws_iam_role", REDSHIFT_IAM_ROLE_ARN) \
                .load()
            
            
            return pending_files_df

        else:
            raise Exception(f"Error al ejecutar la consulta en Redshift: {desc['Error']}")
            
    except Exception as e:
        print(f"Error al consultar la tabla de control en Redshift: {e}")
        # Retorna un DataFrame vacío en caso de error para no detener el pipeline.
        return spark.createDataFrame([], schema="archivoDestino string, idEjecucion string") # Define tu esquema




print("FIN glue job:::>")
#job.commit()