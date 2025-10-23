import os

import boto3

from ..commons import init_logger
from .base_config import BaseConfig

class DynamoConfiguration(BaseConfig):
    def __init__(self, log_level=None, ssm_interface=None):
        """
        Complementary Dynamo config stores the parameters required to access dynamo tables
        :param log_level: level the class logger should log at
        :param ssm_interface: ssm interface, normally boto, to read parameters from parameter store
        """
        self.log_level = log_level or os.getenv("LOG_LEVEL", "INFO")
        self._logger = init_logger(__name__, self.log_level)
        self._ssm = ssm_interface or boto3.client("ssm")
        super().__init__(self.log_level, self._ssm)

        self._object_metadata_table = None
        
    @property
    def object_metadata_table(self):
        
        self._object_metadata_table = self._get_ssm_param("/lakehouse/dynamo/tracking")
        return self._object_metadata_table
        
    @property
    def object_metadata_tableDependencies(self):
        
        self._object_metadata_table = self._get_ssm_param("/lakehouse/dynamo/dependencies")
        return self._object_metadata_table
    
    

