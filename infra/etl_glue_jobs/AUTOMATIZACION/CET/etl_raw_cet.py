## script cet
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

# Importar tu librería personalizada
import transversales_aws as transversales_lib

# --- Inicialización de Glue y Spark ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# ... inicialización del job



dfTableIngestion =spark.read.table("raw.load_raw_bronze")
dfTableIngestion =dfTableIngestion.filter(dfTableIngestion.sourceSystem=="FilesFive9")
display(dfTableIngestion)



#Llamado a funciones Transversales
#import libify
#transversales = libify.importer(globals(), '/Repos/Data Engineering/include/function')
transversales = transversales_lib

#Se recorre dataFrame de Tablas Matriculadas dentro del proceso
for table in dfTableIngestion.rdd.collect():
  
  print("\nInicio proceso archivo: "+table["sourceTable"])

  #Se obtiene las rutas de archivos que estan pendientes a procesar de cada tabla fuente y su idEjecución
  dfSourceTable = transversales.fileToProcessDeltaLake(table["sourceTable"], table["sourceSystem"])  
  
  #Define las columnas de la tabla
  esquema_tabla = spark.sql(f"DESCRIBE {table['databaseName']}.{table['targetTable']}")
  orden_col = [row['col_name'] for row in esquema_tabla.collect() if row['col_name'] not in ('', '# col_name')] 

  #Validación si existe archivos pendientes
  if not(dfSourceTable.rdd.isEmpty()):
    
    #Se crea DataFrame vacio con las columnas de la tabla bronze
    columnas_y_tipos = [(row['col_name'], row['data_type']) for row in esquema_tabla.collect() if row['col_name'] not in ('', '# col_name')] 
    esquema_data_frame = StructType([StructField(col, StringType(), True) for col, _ in columnas_y_tipos])
    dfpunion = spark.createDataFrame([], esquema_data_frame)
    upper_column_name_list = list(map(lambda x: x.upper(), dfpunion.columns))
    dfpunion = dfpunion.toDF(*upper_column_name_list)

    print("Cantidad de Archivos: "+str(dfSourceTable.count()))   

    i=0   
    pathCsvTemp="planos/five9/tmp/csv/tmp"
    dbfs_path = 'dbfs:/mnt/fa-raw/planos/five9/tmp/csv' 

    #Creación de Ruta Temporal 
    try:
        if dbutils.fs.ls(dbfs_path):
            dbutils.fs.rm(dbfs_path, True)
    except Exception as e:
        print(f"La ruta {dbfs_path} no existe, se creará.")
    dbutils.fs.mkdirs(dbfs_path)
    
    #Consolidación CSV en un solo DataFrame.
    for dfSource in dfSourceTable.rdd.collect():
      
      i+= 1
      #Se lee archivo para poder reemplazar caracteres ("") que afectan el formato     
      txtFile= open("/dbfs/mnt/fa-"+dfSource["archivoDestino"],'r')
      csvText = ''.join([i for i in txtFile]).replace("\"\"", "")
      txtFile.close()
      
      csvFile = open("/dbfs/mnt/fa-raw/"+pathCsvTemp+str(i)+".csv","w")
      csvFile.writelines(csvText)
      csvFile.close()
      
      #Crear Dataframe a partir de lo que llega en los archivos .csv delimitado por ("\,")
      dfcsv = spark.read.options(header='True', delimiter='\\\\,',quote='"',multiLine='True', unescapedQuoteHandling='STOP_AT_CLOSING_QUOTE').csv("dbfs:/mnt/fa-raw/"+pathCsvTemp+str(i)+".csv")
      new_column_name_list= list(map(lambda x: x.replace(")", ""),  dfcsv.columns))
      new_column_name_list= list(map(lambda x: x.replace('(', ''),  new_column_name_list))
      new_column_name_list= list(map(lambda x: x.replace("-", ""),  new_column_name_list))
      new_column_name_list= list(map(lambda x: x.replace(" ", "_"), new_column_name_list))
      new_column_name_list= list(map(lambda x: x.replace(".", "_"), new_column_name_list))
      new_column_name_list= list(map(lambda x: x.upper(), new_column_name_list))
      new_column_name_list= list(map(lambda x: ''.join(c for c in unicodedata.normalize('NFKD', x) if not unicodedata.combining(c)), new_column_name_list))
      dfcsv = dfcsv.toDF(*new_column_name_list)

      columnas_faltantes = set(dfpunion.columns) - set(dfcsv.columns)

      for col in columnas_faltantes: 
        dfcsv = dfcsv.withColumn(col, lit(None).cast("string"))
      #fin If

      dfcsv = dfcsv.withColumn("FILENAME", lit("dbfs:/mnt/fa-"+dfSource["archivoDestino"]))
      dfcsv = dfcsv.select(*orden_col)
      #print("Esquema del DataFrame dfcsv:")
      #dfcsv.printSchema()
      dfpunion = dfpunion.union(dfcsv)
      
      #Validar los registros que se procesaron en output
      print(dfSource["archivoDestino"]+" --- "+str(dfcsv.count())+" --- "+str(dfpunion.count())+" ---> # "+str(i))
    #Fin For   
    
    #Cast Pivot Column and Date info
    dfFormat = dfpunion.withColumn("TIMESTAMP_MILLISECOND", to_timestamp(substring(dfpunion.TIMESTAMP_MILLISECOND,7,23) ,'d MMM yyyy HH:mm:ss.SSS'))\
                       .withColumn("DATE", to_date(dfpunion.DATE,'yyyy/MM/dd'))
    
    dfFormat = dfFormat.withColumn("TIMESTAMP_ID",date_format(dfFormat.TIMESTAMP_MILLISECOND,"yyyyMMddHHmmssSS").cast(DoubleType()))

    #Establece el orden de las columnas
    dfFormat = dfFormat.select(*orden_col)

    #Creacion de archivo .parquet 
    dfFormat.write.mode("overwrite").parquet("dbfs:/mnt/fa-raw/planos/five9/tmp/"+str(table["targetTable"]))

    #Carga a tabla Bronce 
    transversales.loadTableByClosingDate(table["sourceTable"],table["databaseName"],table["targetTable"],table["pivotColumn"],"/planos/five9/tmp/"+str(table["targetTable"]))

    print(table["sourceTable"]+" OK.")
    shutil.rmtree(r"/dbfs/mnt/fa-raw/planos/five9/tmp/csv")
    
    for dfSource in dfSourceTable.rdd.collect():      
      #Actualiza tabla de auditoria para que cambie su estado a PROCESADO(1)
      transversales.updateAsProcessedFile(dfSource["idEjecucion"])
    print("Procesado OK.")
    
    #fin If       
  else:
    print("No hay archivos pendiente de procesar para "+table["sourceTable"])