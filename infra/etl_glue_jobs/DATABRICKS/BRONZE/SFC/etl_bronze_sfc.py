import sys, re
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
import datetime
from datetime import datetime
from requests_html import HTMLSession
import requests_html
import requests
import pytz
import pandas as pd
from urllib.parse import urlparse
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F

# Importar tu librería personalizada
import transversales_aws02 as transversales_lib



args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'FechaProceso',
    'warehouse_s3_uri',
    'raw_db',
    'master_db',
    'staging_db',
    'periodicidad'
])
#bucket_source = args['raw_bucket']
bucket_target = "dl-raw-dev-s3"#s3://dl-raw-dev-s3/
#bucket_target = args['raw_bucket']
#bucket_source = args['raw_bucket']

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Iceberg + Glue Catalog configuration
spark.conf.set('spark.sql.defaultCatalog', 'glue_catalog')
spark.conf.set('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
spark.conf.set('spark.sql.catalog.glue_catalog.warehouse', args['warehouse_s3_uri'])
spark.conf.set('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
spark.conf.set('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')

RAW_DB = args['raw_db']
MASTER_DB = args['master_db']
STAGING_DB = args['staging_db']
FECHA_PROCESO = args['FechaProceso']  # yyyy-MM-dd

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Optional: internal libs packaged as .whl/.zip and attached to Glue job
# from your_internal_lib.io import read_table, write_table, upsert_iceberg

# Preprocess SQL to remap schema names from Databricks (bronze/silver/staging) to Glue DBs
_schema_patterns = [
    (re.compile(r'\bbronze\.', re.IGNORECASE), f'{RAW_DB}.'),
    (re.compile(r'\bsilver\.', re.IGNORECASE), f'{MASTER_DB}.'),
    (re.compile(r'\bstaging\.', re.IGNORECASE), f'{STAGING_DB}.'),
]

def _remap_schemas(sql: str) -> str:
    out = sql
    for pat, repl in _schema_patterns:
        out = pat.sub(repl, out)
    # Simple parameter expansions commonly seen in notebooks
    out = out.replace('${FechaProceso}', FECHA_PROCESO)
    out = out.replace('${FechaReporte}', FECHA_PROCESO)
    return out

def run_sql(sql: str):
    sql2 = _remap_schemas(sql)
    print('\n=== Executing SQL ===\n' + sql2[:4000] + ('...' if len(sql2) > 4000 else ''))
    return spark.sql(sql2)

# Ensure DBs exist (idempotent)
#spark.sql(f"CREATE DATABASE IF NOT EXISTS {RAW_DB}")
#spark.sql(f"CREATE DATABASE IF NOT EXISTS {MASTER_DB}")
#spark.sql(f"CREATE DATABASE IF NOT EXISTS {STAGING_DB}")

# Example name remapping in comments:
#   bronze.some_table  -> {RAW_DB}.some_table
#   silver.some_table  -> {MASTER_DB}.some_table
#   staging.tmp_table  -> {STAGING_DB}.tmp_table

#\n# ---- Notebook Cell 0 (Python) ----
#Llamado a funciones Transversales
#import libify
#transversales = libify.importer(globals(), '/Repos/Data Engineering/include/function')
transversales = transversales_lib


#\n# ---- Notebook Cell 1 (Python) ----
#from requests_html import HTMLSession
#import requests_html
#import requests
#from datetime import datetime
#import pytz
#import pandas as pd
#import time
#import re



#\n# ---- Notebook Cell 2 (Python) ----
def getHtml(url):
  session = HTMLSession()
  response = session.get(url)
  container = response.html.xpath('//*[@id="form1"]/div[3]/ul/li/a')
  return container

def getHtmlEnt(url):
  session = HTMLSession()
  response = session.get(url)
  container = response.html.xpath('//*[@id="form1"]/div[3]/ul[1]/li')
  return container[0].html



#\n# ---- Notebook Cell 3 (Python) ----
#dbutils.widgets.text("periodicidad","","")
#dbutils.widgets.get("periodicidad")
#periodicidad = getArgument("periodicidad")
periodicidad = ''
print('periodicidad: ',periodicidad)



#\n# ---- Notebook Cell 4 (Python) ----
urlEntidades = 'https://www.superfinanciera.gov.co/inicio/industrias-supervisadas/entidades-vigiladas-por-la-superintendencia-financiera-de-colombia/lista-general-de-entidades-vigiladas-por-la-superintendencia-financiera-de-colombia-61694'
containerEnt = getHtmlEnt(urlEntidades)


time_America = pytz.timezone('America/Bogota')
date = datetime.now(time_America)
date = date.strftime('%Y%m%d')


file=(containerEnt.split('"')[9].split('/')[-1])
fileUrl = ("https://www.superfinanciera.gov.co"+containerEnt.split('"')[9])


dfEnt = pd.DataFrame({'topicDesc':"Informe Entidades",
                   'dateId':date,
                   'file': file,
                   'fileUrl':fileUrl,
                   'SuperUrl':'https://www.superfinanciera.gov.co/inicio/industrias-supervisadas/entidades-vigiladas-por-la-superintendencia-financiera-de-colombia/lista-general-de-entidades-vigiladas-por-la-superintendencia-financiera-de-colombia-61694'
                  }, index=[1])


urlDL=f"{bucket_target}/planos/superintendenciaFinanciera"
folder="informe-entidades/"
path_save=f"{urlDL}/{folder}"

downloaded_file=requests.get(fileUrl)
save_file= open(f"path_save"+str(dfEnt['dateId'][1])+"-"+str(dfEnt['file'][1]),'wb')
save_file.write(downloaded_file.content)
save_file.close()
time.sleep(2)   



#\n# ---- Notebook Cell 5 (Python) ----
dfTableIngestionSuper =spark.read.table("bronze.sfc_load_web_bronze")
dfTableIngestionSuper =dfTableIngestionSuper.filter(dfTableIngestionSuper.periodicity==periodicidad)
display(dfTableIngestionSuper)



#\n# ---- Notebook Cell 6 (Python) ----
for file in dfTableIngestionSuper.rdd.collect():
  
  print("\n\n===========================================\n"
        +"Inicio proceso Archivos: "
        +file["informationType"]
        +"\n")
  
  transversales.downloadFileSuperFinanciera(file["pathSuperIntendencia"],
                                            file["pathDataLake"],
                                            file["xpath"],
                                            file["numberFileDownload"],
                                            file["dateFormatSource"],
                                            file["dateFormatTarget"],
                                            file["indexDateStart"],
                                            file["indexDateEnd"],
                                            file["informationType"],
                                           file["periodicity"])

  print("\n"
        +" Proceso Finalizado Correctamente.\n"
        +"===========================================\n")



#\n# ---- Notebook Cell 7 (Python) ----
#import datetime
#import pytz
naiveDate = datetime.datetime(2021, 1, 21, 13, 21, 25)
utc = pytz.UTC
time_zone = pytz.timezone('America/Bogota')
localizedDate = utc.localize(naiveDate)

localizedDate.astimezone(time_zone).strftime('%b')



#\n# ---- Notebook Cell 8 (Python) ----
#from pyspark.sql.functions import lower, col

dfTableIngestionAll =spark.read.table("bronze.load_raw_bronze")
dfTableIngestionAll =dfTableIngestionAll.filter(dfTableIngestionAll.sourceSystem=="sfc").filter(dfTableIngestionAll.project=="SFC")
dfTableIngestionAll = dfTableIngestionAll.withColumn('sourceTable', lower(col('sourceTable')))

dfActiveTable=transversales.activeTableIngestion("'API'","'API'")

dfTableIngestionAll = dfTableIngestionAll.join(dfActiveTable,dfTableIngestionAll["sourceTable"] == dfActiveTable["nombreTabla"])

display(dfTableIngestionAll)



#\n# ---- Notebook Cell 9 (Python) ----
for table in dfTableIngestionAll.filter(dfTableIngestionAll.loadType!="disabled").rdd.collect():
  spark.sql('refresh table '+table["databaseName"]+'.'+table["targetTable"])
  print("Refesh Correcto: "+table["databaseName"]+'.'+table["targetTable"])
# \n# ---- Notebook Cell 10 (Python) ----
dfTableIngestionCl = dfTableIngestionAll.filter(dfTableIngestionAll.loadType=="ClosingDate").filter(dfTableIngestionAll.indEstadoTrigger)

display(dfTableIngestionCl)



#\n# ---- Notebook Cell 11 (Python) ----
#Se recorre dataFrame de Tablas Matriculadas dentro del proceso
for table in dfTableIngestionCl.rdd.collect():
   
  #Se obtiene las rutas de archivos a procesar de cada tabla fuente y su idEjecucion
  dfSourceTable = transversales.fileToProcessDeltaLake(table["sourceTable"],table["sourceSystem"])
  #Validacion si existe archivos pendientes
  if ~(dfSourceTable.rdd.isEmpty()):
    
    #Para crea un df por cada archivo retornado, se realiza Upsert y actualiza como procesado cada registro de archivo en la tabla de auditoria.
    print("\nInicio proceso tabla: "+table["sourceTable"])
    
    for dfSource in dfSourceTable.rdd.collect():
      transversales.loadTableByClosingDate(table["sourceTable"],table["databaseName"],table["targetTable"],table["pivotColumn"],dfSource["archivoDestino"])
      transversales.updateAsProcessedFile(dfSource["idEjecucion"])
      print(dfSource["archivoDestino"]+" OK.")
      
  else:
    print("No hay archivos pendiente de procesar para "+table["sourceTable"])


# ---- End of translated notebook ----
job.commit()
