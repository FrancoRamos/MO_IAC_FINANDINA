import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [
    'S3_OUTPUT_PREFIX',
    'OUTPUT_FILE_BASENAME',
    'REDSHIFT_SECRET_ARN',
    'REDSHIFT_JDBC_URL',
    'SQL_TEXT',
    'CSV_DELIMITER',
    'CSV_QUOTE_ALL',
    'CSV_HEADER'
])

S3_OUTPUT_PREFIX      = args['S3_OUTPUT_PREFIX'].rstrip('/')
OUTPUT_FILE_BASENAME  = args['OUTPUT_FILE_BASENAME']
REDSHIFT_SECRET_ARN   = args['REDSHIFT_SECRET_ARN']
REDSHIFT_JDBC_URL     = args['REDSHIFT_JDBC_URL']
SQL_TEXT              = args['SQL_TEXT']
CSV_DELIMITER         = args['CSV_DELIMITER']
CSV_QUOTE_ALL         = args['CSV_QUOTE_ALL'].lower() == 'true'
CSV_HEADER            = args['CSV_HEADER'].lower() == 'true'

spark = (SparkSession.builder.getOrCreate())

# Obtener credenciales de Secrets Manager
sm = boto3.client('secretsmanager')
sec = sm.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
secret_dict = json.loads(sec['SecretString'])
user = secret_dict.get('username') or secret_dict.get('user')
pwd  = secret_dict.get('password') or secret_dict.get('pwd')

# Leer desde Redshift (JDBC)
df = (spark.read.format("jdbc")
      .option("url", REDSHIFT_JDBC_URL)
      .option("user", user)
      .option("password", pwd)
      .option("driver", "com.amazon.redshift.jdbc.Driver")
      .option("query", SQL_TEXT)
      .load())

# Escribir a S3 como CSV con delimitador ";" y quoteAll
# NOTA: Glue/Spark escriben como carpeta con part-*. Cumple el requisito de no usar Lambda.
# Si en el futuro quieres un único nombre fijo, orquestalo fuera (no contemplado aquí por tu decisión).
tmp_output = f"{S3_OUTPUT_PREFIX}/{OUTPUT_FILE_BASENAME}_tmp"

(df.coalesce(1)
   .write.mode("overwrite")
   .option("delimiter", CSV_DELIMITER)
   .option("quote", "\"")
   .option("quoteAll", CSV_QUOTE_ALL)
   .option("header", CSV_HEADER)
   .csv(tmp_output))
