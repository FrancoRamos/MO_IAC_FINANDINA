import boto3
import json
import datetime
import logging
import os
from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration
from datalake_library.interfaces.dynamo_interface import DynamoInterface


logger = init_logger(__name__)
glue_client = boto3.client('glue')

bucketRaw = os.environ['rawBucket']
dataBaseRaw = os.environ["rawDatabase"]

bucketMaster = os.environ["masterBucket"]
dataBaseMaster = os.environ["masterDatabase"]


STATUS='PENDING'

def get_table_info(database, table, partitionvalue):
    # Obtener la respuesta de AWS Glue
    glue_response = glue_client.get_table(
        DatabaseName=database,
        Name=table)

    # Extraer la ubicación de la tabla
    table_location = glue_response['Table']['StorageDescriptor']['Location']
    table_bucket = table_location.split('/')[2]
    table_path = table_location.split(table_bucket + "/")[1]

    # Verificar si la tabla tiene particiones
    haspartition = bool(glue_response['Table']['PartitionKeys'])
    table_partition = ''

    if haspartition:
        table_partition = glue_response['Table']['PartitionKeys'][0]['Name']
        full_table_path = f"{table_path}/{table_partition}={partitionvalue}/"
    else:
        full_table_path = f"{table_path}/"

    # Crear un diccionario para almacenar los nombres de las columnas y sus tipos
    columns_info = {}
    for column in glue_response['Table']['StorageDescriptor']['Columns']:
        columns_info[column['Name']] = column['Type']

    return haspartition, table_partition, columns_info
    
def lambda_handler(event, context):
    
    logger.info('Initializing client')
    object_metadata = event
    try:
        timestamp = datetime.datetime.now().isoformat()
        process_date = timestamp.split('T')[0]
        peh_id=object_metadata["peh_id"]
        tableName = object_metadata['table']
        bucket = object_metadata['bucketRaw']
        namePartition = object_metadata['namePartition']
        valuePartition = object_metadata['valuePartition']
        print(dataBaseMaster, tableName, valuePartition)
        partitionFlag, namePartition,columns_info = get_table_info(dataBaseMaster, tableName, valuePartition)
        pipeline_execution = f"{tableName}-{timestamp}"
        data= {
            'peh_id': str(peh_id),
            'pipeline-execution': pipeline_execution,
            'table': tableName,
            'stage': 'stageB',
            'process_date': process_date,
            'timestamp': timestamp,
            'status': STATUS 
        }
        
        logger.info('Initializing DynamoDB config and Interface')
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)
        
        table=dynamo_interface._get_object_metadata_table()
        logger.info('Storing metadata to DynamoDB')
        dynamo_interface.put_item(table,data)
        
       
        primarykey=tableName
        tableDependencies=dynamo_interface._get_object_metadata_table_dependencies()
        responseDependencies=dynamo_interface.readDependencies(tableDependencies, primarykey)
        
        data['namePartition']=namePartition
        data['valuePartition']=valuePartition
        data['typeload']=responseDependencies["type_load"]
        data['databaseOrigin']=dataBaseRaw
        data['databaseTarget']=dataBaseMaster
        data['bucketTarget']=bucketMaster
        data['columns_info']=json.dumps(columns_info)
        return data
        
        
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        logger.error(f"Error occurred: {str(e)}")
        
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        }