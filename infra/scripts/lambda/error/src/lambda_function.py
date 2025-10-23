import boto3
import json
import datetime
import logging
import os
import uuid
from datalake_library.commons import init_logger
from datalake_library.configuration.resource_configs import DynamoConfiguration
from datalake_library.interfaces.dynamo_interface import DynamoInterface

logger = init_logger(__name__)
STATUS = 'Failed'

def lambda_handler(event, context):
    logger.info('Fetching event data from previous step')
    print(json.dumps(event))
    
    object_metadata=event["Input"]
    object_metadata_error=event["ErrorInfo"]
    table_name = object_metadata.get('table', None)
    
    sns_topic_arn = os.environ.get('topic')

    try:
        timestamp = datetime.datetime.now().isoformat()
        process_date = timestamp.split('T')[0]

        logger.info('Initializing DynamoDB config and Interface')
        dynamo_config = DynamoConfiguration()
        dynamo_interface = DynamoInterface(dynamo_config)

        table = dynamo_interface._get_object_metadata_table()
        logger.info('Storing metadata to DynamoDB')
        dynamo_interface.update_table_status(table, object_metadata['peh_id'], STATUS, process_date)
        
        if sns_topic_arn:
            error_message=f"Se genero un error al cargar : {table_name} - Motivo {object_metadata_error}"
            sns_client = boto3.client('sns')
            response=sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=error_message,
                Subject=f'Error en la ingesta de la tabla {table_name} - {process_date}'
            )
            print("sns_client",response)
            
        #return object_metadata
        return {
            'statusCode': 200,
            'body': {
                'error': None
            }
        }
    except Exception as e:
        logger.error("Fatal error", exc_info=True)
        error_message = f"Error occurred: {str(e)}"
        logger.error(error_message)
        
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        }