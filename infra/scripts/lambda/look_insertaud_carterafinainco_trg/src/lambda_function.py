### lambda sin response redshift
import os
import time
import boto3
import json

# Variables de entorno configuradas
#REDSHIFT_WORKGROUP = os.environ.get('REDSHIFT_WORKGROUP') # o REDSHIFT_CLUSTER_IDENTIFIER
#REDSHIFT_DATABASE = os.environ['REDSHIFT_DATABASE']
#REDSHIFT_SECRET_ARN = os.environ['REDSHIFT_SECRET_ARN']

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

def normalize_rows(records):
    """
    Normaliza los registros de resultados a un formato [{DateStar: "...", DateEnd: "..."}].
    """
    rows = []
    if records:
        for record in records:
            # Los registros de Redshift Data API son diccionarios de tipos de datos,
            # por ejemplo: [{'stringValue': '2023-01-01'}, {'stringValue': '2023-01-02'}]
            
            # Extrae el valor de la clave de tipo de dato (por ejemplo, 'stringValue')
            date_star_value = list(record[0].values())[0] if record[0] else None
            date_end_value = list(record[1].values())[0] if record[1] else None
            
            rows.append({
                "DateStar": str(date_star_value),
                "DateEnd": str(date_end_value)
            })
    return rows

def lambda_handler(event, context):
    """
    Función principal de Lambda.
    """
    print("try logic - adls_cet_five9-func-LookUp1-dev :::")
    # 1. Recibir parámetros del Step Function
    print("event: ",event)

    # Variables de entorno configuradas
    REDSHIFT_WORKGROUP = 'dl-workgroup-dev-rs'#os.environ.get('REDSHIFT_WORKGROUP') # o REDSHIFT_CLUSTER_IDENTIFIER
    REDSHIFT_DATABASE = 'dl_dev'#os.environ['REDSHIFT_DATABASE']
    REDSHIFT_SECRET_ARN = 'arn:aws:secretsmanager:us-east-1:637423369807:secret:dl-redshift-admin-dev-secrets-mngnvn'#get_secret()#os.environ['REDSHIFT_SECRET_ARN']
    REDSHIFT_IAM_ROLE_ARN = 'arn:aws:iam::637423369807:role/redshift-finandina-role'#os.environ['REDSHIFT_IAM_ROLE_ARN']

    try:

        #query_template = event.get('queryTemplate')
        #folder_name = event.get('folderName')
        #report_name = event.get('reportName')
        #minutes_to_interval = event.get('minutesToInterval')

        query_template = """ ---Query01
            select * 
            from dbo.configuracionOrigen 
            where idConfigOrigen = 37
            ---ORDER BY NEWID()
            ;
        """
        folder_name = 'Shared Reports'
        report_name = 'Trafico Dwh'
        minutes_to_interval = 180

        if not all([query_template, folder_name, report_name, minutes_to_interval]):
            raise ValueError("Faltan uno o más parámetros de entrada requeridos: queryTemplate, folderName, reportName, minutesToInterval.")

        # Reemplazar parámetros en la consulta SQL
        sql = str(query_template)
        #sql = sql.replace('?p1', str(folder_name))
        #sql = sql.replace('?p2', str(report_name))
        #sql = sql.replace('?p3', str(minutes_to_interval))
        
        print(f"Ejecutando la siguiente consulta SQL: {sql}")
        
        # Parámetros de la ejecución
        exec_params = {
            'SecretArn': REDSHIFT_SECRET_ARN,
            'Database': REDSHIFT_DATABASE,
            'Sql': sql
        }
        
        if REDSHIFT_WORKGROUP:
            exec_params['WorkgroupName'] = REDSHIFT_WORKGROUP
        
        # Ejecutar la consulta
        ####exec_response = client.execute_statement(**exec_params)
        exec_response = "client.execute_statement(**exec_params)"
        statement_id = exec_response['Id']
        
        # Esperar a que la consulta termine
        wait_result(statement_id)
        
        # Obtener los resultados
        result_response = client.get_statement_result(Id=statement_id)
        
        print("result_response: ", result_response)
        # Normalizar los resultados
        normalized_rows = normalize_rows(result_response.get('Records'))
        print("normalized_rows: ", normalized_rows)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'rows': normalized_rows
            })
        }

    except Exception as e:
        print(f"Ocurrió un error en la ejecución de la Lambda: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }