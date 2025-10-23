"""
Quality
"""
import sys
import gc
from awsglue.context import GlueContext
from pyspark.context import SparkContext
#from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions

# Inicializando Spark y Glue context
SPARK_CONTEXT = SparkContext.getOrCreate()
glue_context = GlueContext(SPARK_CONTEXT)
spark = glue_context.spark_session

# Obtener las variables de entorno
args = getResolvedOptions(sys.argv, ['ENVIRONMENT', 'DATABASE_ORIGIN', 'DATABASE_TARGET',
                                      'TABLE', 'NAME_PARTITION_RAW', 'VALUE_PARTITION_RAW',
                                      'TYPE_LOAD'])
database_origin = args['DATABASE_ORIGIN']
database_target = args['DATABASE_TARGET']
table = args['TABLE']
name_partition = args['NAME_PARTITION_RAW']
value_partition = args['VALUE_PARTITION_RAW']
type_load = args['TYPE_LOAD']
gc.collect()
print('OK')
