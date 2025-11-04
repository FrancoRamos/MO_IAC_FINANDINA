# transversales_aws.py
# Librería de funciones para procesos de ingesta en AWS Glue con Redshift e Iceberg.
import sys, gc
import boto3
import time
from pyspark.sql.functions import current_timestamp
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

# Parámetros (pásalos al crear el Job en CDK/Console)
#args = getResolvedOptions(sys.argv, [
#    'BUCKET_RAW',
#    'BUCKET_MASTER',
#    'TABLE',
#    'NAME_PARTITION_RAW',
#    'VALUE_PARTITION_RAW',
#    'TYPE_LOAD',   # full o incremental
#    'ACCOUNT_ID'
#])
#bucket_raw    = args['BUCKET_RAW']
#bucket_master = args['BUCKET_MASTER']
#table         = args['TABLE']
#pr_name       = args['NAME_PARTITION_RAW']
#pr_value      = args['VALUE_PARTITION_RAW']
#load_type     = args['TYPE_LOAD'].lower()
#accid         = args['ACCOUNT_ID']

# ==============================================================================
# --- SECCIÓN DE CONFIGURACIÓN ---
# Estos valores deben ser gestionados de forma segura, por ejemplo,
# a través de los parámetros del Job de Glue o AWS Secrets Manager.
# ==============================================================================

# Configuración para la tabla de control en Amazon Redshift
#REDSHIFT_CLUSTER_ID = 'tu-cluster-de-redshift'
#REDSHIFT_DATABASE   = 'tu-base-de-datos'
#REDSHIFT_DB_USER    = 'tu-usuario-de-bd' # Para producción, usar un secreto de Secrets Manager y un rol de IAM.
#CONTROL_TABLE       = 'public.control_pipeline_light_gen2' # O el esquema y tabla que corresponda
#AWS_REGION          = 'us-east-1' # La región de tu cluster de Redshift

# Inicializar el cliente de la API de Datos de Redshift
# Esta es la forma moderna y recomendada de interactuar con Redshift desde Glue/Lambda.
try:
    redshift_data_client = boto3.client('redshift-data', region_name=AWS_REGION)
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
        # Ejecutar la consulta usando la API de Datos de Redshift
        response = redshift_data_client.execute_statement(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID,
            Database=REDSHIFT_DATABASE,
            DbUser=REDSHIFT_DB_USER,
            Sql=sql_query,
            WithEvent=False # Para ejecución síncrona
        )
        
        statement_id = response['Id']
        
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
            return pending_files_df

        else:
            raise Exception(f"Error al ejecutar la consulta en Redshift: {desc['Error']}")
            
    except Exception as e:
        print(f"Error al consultar la tabla de control en Redshift: {e}")
        # Retorna un DataFrame vacío en caso de error para no detener el pipeline.
        return spark.createDataFrame([], schema="archivoDestino string, idEjecucion string") # Define tu esquema



def updateAsProcessedFile(idEjecucion):
    """
        Actualiza el estado de un archivo a 'procesado' en la tabla de control de Redshift.
        Args:
            idEjecucion (int): El ID de ejecución del archivo a actualizar.
    """
    print(f"Actualizando estado para idEjecucion: {idEjecucion}")
    
    if not redshift_data_client:
        raise ConnectionError("El cliente de Redshift Data API no está inicializado.")
    sql_update = f"""
        UPDATE {CONTROL_TABLE}
        SET procesado = 1
        WHERE idEjecucion = {idEjecucion};
    """
    try:
        redshift_data_client.execute_statement(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID,
            Database=REDSHIFT_DATABASE,
            DbUser=REDSHIFT_DB_USER,
            Sql=sql_update
        )
        print(f"Actualización exitosa para idEjecucion: {idEjecucion}")
    except Exception as e:
        print(f"Error al actualizar la tabla de control en Redshift para idEjecucion {idEjecucion}: {e}")



def loadTableByClosingDate(spark, databaseName, targetTable, pivotColumn, pathParquet):
    """
    Carga datos desde un archivo Parquet a una tabla Apache Iceberg de destino
    utilizando una operación MERGE INTO (upsert).

    Esta función reemplaza el patrón "DELETE + INSERT" por una operación MERGE
    atómica, que es más segura y eficiente.
    
    Args:
        spark (SparkSession): La sesión de Spark activa en Glue.
        databaseName (str): La base de datos en el Glue Data Catalog.
        targetTable (str): El nombre de la tabla Iceberg de destino.
        pivotColumn (str): La columna clave para la operación MERGE.
        pathParquet (str): La ruta S3 al archivo Parquet con los nuevos datos.
    """
    full_target_table = f"glue_catalog.{databaseName}.{targetTable}"
    print(f"Iniciando carga MERGE INTO para la tabla Iceberg: {full_target_table}")

    try:
        # 1. Leer los datos nuevos del archivo Parquet (los datos "fuente")
        new_data_df = spark.read.parquet(pathParquet)
        
        # 2. Crear una vista temporal para poder referenciarla en la consulta SQL
        temp_view_name = "source_view_for_merge"
        new_data_df.createOrReplaceTempView(temp_view_name)
        
        # 3. Construir dinámicamente la sentencia MERGE
        columns = new_data_df.columns
        
        # Esta cláusula "UPDATE SET" es el equivalente al DELETE + INSERT para registros existentes.
        # En lugar de borrar y reinsertar, actualiza directamente los valores.
        set_clause = ", ".join([f"T.{col} = S.{col}" for col in columns])
        
        # Esta cláusula "INSERT" maneja los registros que son completamente nuevos.
        insert_cols = ", ".join(columns)
        values_cols = ", ".join([f"S.{col}" for col in columns])

        merge_sql = f"""
            MERGE INTO {full_target_table} AS T   -- T es la tabla de Destino
            USING {temp_view_name} AS S          -- S son los nuevos datos (Fuente)
            ON T.{pivotColumn} = S.{pivotColumn} -- Condición de cruce (cómo saber si un registro ya existe)
            
            -- Para las filas que coinciden (el equivalente al DELETE + INSERT):
            WHEN MATCHED THEN
                UPDATE SET {set_clause} -- Actualiza la fila en la tabla de destino con los nuevos datos.
            
            -- Para las filas que están en los nuevos datos pero no en la tabla de destino:
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({values_cols}) -- Inserta la nueva fila.
        """
        
        print("Ejecutando la siguiente consulta MERGE para Iceberg:")
        print(merge_sql)
        
        # 4. Ejecutar la consulta MERGE como una única transacción atómica
        spark.sql(merge_sql)
        
        print(f"Carga MERGE completada exitosamente para la tabla {full_target_table}")
        
    except Exception as e:
        print(f"Error durante la operación MERGE INTO para la tabla Iceberg {full_target_table}: {e}")
        raise e



    
    full_target_table = f"glue_catalog.{databaseName}.{targetTable}"
    print(f"Iniciando carga MERGE INTO para la tabla Iceberg: {full_target_table}")

    try:
        new_data_df = spark.read.parquet(pathParquet)
        temp_view_name = "source_view_for_merge"
        new_data_df.createOrReplaceTempView(temp_view_name)
        
        columns = new_data_df.columns
        set_clause = ", ".join([f"T.{col} = S.{col}" for col in columns])
        insert_cols = ", ".join(columns)
        values_cols = ", ".join([f"S.{col}" for col in columns])
        merge_sql = f"""
            MERGE INTO {full_target_table} AS T
            USING {temp_view_name} AS S
            ON T.{pivotColumn} = S.{pivotColumn}
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({values_cols})
        """
        
        print("Ejecutando la siguiente consulta MERGE para Iceberg:")
        print(merge_sql)
        spark.sql(merge_sql)
        print(f"Carga MERGE completada exitosamente para la tabla {full_target_table}")
        
    except Exception as e:
        print(f"Error durante la operación MERGE INTO para la tabla Iceberg {full_target_table}: {e}")
        raise e