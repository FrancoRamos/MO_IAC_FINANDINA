import datetime as dt
import os

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from ..commons import init_logger

class DynamoInterface:
    def __init__(self, configuration, log_level=None, dynamodb_resource=None):
        self.log_level = log_level or os.getenv("LOG_LEVEL", "INFO")
        self._logger = init_logger(__name__, self.log_level)
        self.dynamodb_resource = dynamodb_resource or boto3.resource("dynamodb")

        self._config = configuration

        self.object_metadata_table = None

        self._get_object_metadata_table()
        

    def _get_object_metadata_table(self):
        
        self.object_metadata_table = self.dynamodb_resource.Table(self._config.object_metadata_table)
        return self.object_metadata_table
        
    
    def _get_object_metadata_table_dependencies(self):
        
        self.object_metadata_table_dependencies = self.dynamodb_resource.Table(self._config.object_metadata_tableDependencies)
        return self.object_metadata_table_dependencies


    def get_item(self, table, key):
        try:
            item = table.get_item(Key=key, ConsistentRead=True)["Item"]
        except ClientError:
            msg = "Error getting item from {} table".format(table)
            self._logger.exception(msg)
            raise
        return item

    def put_item(self, table, item):
        try:
            table.put_item(Item=item)
        except ClientError:
            msg = "Error putting item {} into {} table".format(item, table)
            self._logger.exception(msg)
            raise
    
    def update_table_status(self, table, key, status, process_date_end):
        try:
            table.update_item(
                Key={
                    'peh_id': str(key)
                },
                UpdateExpression="SET #status = :status_value, #date_end = :date_end_value",
                ExpressionAttributeNames={
                    "#status": "status",
                    "#date_end": "process_date_end"
                },
                ExpressionAttributeValues={
                    ":status_value": status,
                    ":date_end_value": process_date_end
                }
            )
        except ClientError:
            msg = "Error putting {} table".format(table)
            self._logger.exception(msg)
            raise
    
    
    def readDependencies(self, table, primarykey):
        try:
            # Realizar la consulta por la clave primaria
            respuesta = table.get_item(Key={'source': primarykey})
            print(respuesta)
            # Verificar si se obtuvo algún resultado
            data={}
            if 'Item' in respuesta:
                item = respuesta['Item']
                # Obtener el valor del campo landing_queue si no está vacío
                data["landing_strategy"]= item.get('landing_strategy', None)
                data["raw_queue"]= item.get('raw_queue', None)
                data["raw_stageA"]= item.get('raw_stageA', None)
                data["raw_strategy"]= item.get('raw_strategy', None)
                data["type_load"]= item.get('type_load', None)
                data["quality"]= item.get('quality', None)
                return data
            else:
                return None, None
    
        except ClientError as e:
            print(e.response['Error']['Message'])
            return None, None