import os
import json
import boto3
import csv
from io import StringIO
from botocore.exceptions import ClientError
import time
#import pandas as pd


# Cliente de Redshift Data
client = boto3.client('redshift-data')


def wait_result(statement_id):
    """
    Función helper que espera a que una consulta termine.
    """
    while True:
        try:
            desc = client.describe_statement(Id=statement_id)
            status = desc['Status']
            
            if status == 'FINISHED':
                print(f"La consulta con ID {statement_id} ha terminado exitosamente.")
                return
            elif status == 'FAILED':
                error_message = desc.get('Error', 'Error desconocido')
                raise Exception(f"La consulta a Redshift falló: {error_message}")
            
            print(f"Estado de la consulta: {status}. Esperando 1.5 segundos...")
            time.sleep(1.5)
        
        except Exception as e:
            print(f"Error al describir el estado de la consulta: {e}")
            raise


def lambda_handler(event, context):
    s3_client = boto3.client('s3')

    print("event: ",event)
    
    # Redshift connection details (can be stored in environment variables or Secrets Manager)
    #redshift_cluster_identifier = os.environ.get('REDSHIFT_CLUSTER_IDENTIFIER')
    #redshift_database_name = os.environ.get('REDSHIFT_DATABASE_NAME')
    #redshift_secret_arn = os.environ.get('REDSHIFT_SECRET_ARN') # If using Secrets Manager

    # Variables de entorno configuradas
    REDSHIFT_WORKGROUP = 'dl-workgroup-dev-rs'#os.environ.get('REDSHIFT_WORKGROUP') # o REDSHIFT_CLUSTER_IDENTIFIER
    REDSHIFT_DATABASE = 'dl_dev'#os.environ['REDSHIFT_DATABASE']
    REDSHIFT_SECRET_ARN = 'arn:aws:secretsmanager:us-east-1:637423369807:secret:dl-redshift-admin-dev-secrets-mngnvn'#get_secret()#os.environ['REDSHIFT_SECRET_ARN']
    REDSHIFT_IAM_ROLE_ARN = 'arn:aws:iam::637423369807:role/redshift-finandina-role'#os.environ['REDSHIFT_IAM_ROLE_ARN']

    # Target Redshift table name
    #redshift_table_name = "controlpipelinelight_gen2" # Replace with your table name
    #redshift_table_name = "ConfigADFMetadataDriven "
    redshift_table_name = "dbo.usp_ins_ejecucion_light_gen2"
    print(f"Redshift table name: {redshift_table_name} :::: INIT:")
    # Cliente de Redshift Data
    client = boto3.client('redshift-data')

    

    #p_nombretabla = f'{folderName}.{reportName}'
    ##@concat( formatDateTime(pipeline().parameters.dateStart, 'yyyyMMdd'), '_', formatDateTime(pipeline().parameters.dateStart, 'HHmmss'), '_', formatDateTime(pipeline().parameters.dateEnd, 'HHmmss') ),
    #p_valorpivot = f'{dateStart_formatedD}_{dateStart_formatedH}_{dateEnd_formatedH}'
    #p_fechainicio = #datetime.now()
    #p_fechainsertupdate = #datetime.now()
    #p_runid = #pipeline().RunId
    #sqlExc
    sp_query = """CALL sfc.uspLoad_FactsAutomatizacion();"""

    print(f"Ejecutando la siguiente consulta SQL: {copy_command}")

    sp_sql = str(sp_query)
    
    print(f"Ejecutando la siguiente consulta SQL: {sp_sql}")
    
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

    try:
        # Ejecutar la consulta
        exec_response = client.execute_statement(**exec_params)
        statement_id = exec_response['Id']
        # Esperar a que la consulta termine
        wait_result(statement_id)
        # Obtener los resultados
        result_response = client.get_statement_result(Id=statement_id)
        print(f"result_response SQL client: {result_response}")
        # You can optionally poll the statement status using describe_statement()
        # and get_statement_result() if you need to confirm completion or retrieve results.
        return {
            'statusCode': 200,
            'body': json.dumps('Data loaded successfully into Redshift!')
        }
    except Exception as e:
        print(f"Error loading data: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error loading data: {str(e)}')
        }
    


def get_secret():

    secret_name = "dl-redshift-admin-dev-secrets"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']

    return secret