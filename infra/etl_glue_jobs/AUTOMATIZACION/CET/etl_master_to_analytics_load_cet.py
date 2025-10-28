#!/usr/bin/env python3
import os
import sys
import gc
import boto3
from pyspark.sql import SparkSession, functions as F
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# 1) Inicializar Spark/Glue
sc = SparkContext.getOrCreate()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite","CORRECTED")

# 2) Leer parámetros (del CDK/Console o Step Functions)
args = getResolvedOptions(sys.argv, [
    'ENVIRONMENT',             # dev/prod
    'BUCKET_MASTER',           # ej. us-west-2-…-master
    'BUCKET_ANALYTICS',        # ej. us-west-2-…-analytics
    'TABLE',                   # nombre lógico de la tabla
    'NAME_PARTITION_MASTER',   # columna de partición en master
    'VALUE_PARTITION_MASTER',  # valor de partición (YYYY-MM-DD, etc.)
    'TYPE_LOAD'                # full o incremental
])
bucket_master     = args['BUCKET_MASTER']
bucket_analytics  = args['BUCKET_ANALYTICS']
table             = args['TABLE']
pr_name_master    = args['NAME_PARTITION_MASTER']
pr_value_master   = args['VALUE_PARTITION_MASTER']
load_type         = args['TYPE_LOAD'].lower()
db_analytics      = os.environ.get('GLUE_DB_ANALYTICS','finandina-account_devops_dev_analytics')  # catálogo analytics

# 3) Construir rutas
if load_type == 'full':
    path_in = f"s3://{bucket_master}/master/{table}/"
else:
    path_in = (
      f"s3://{bucket_master}/master/{table}/"
      f"{pr_name_master}={pr_value_master}/"
    )
if load_type == 'full':
    path_out = f"s3://{bucket_analytics}/analytics/{table}/"
else:
    path_out = (
      f"s3://{bucket_analytics}/analytics/{table}/"
      f"{pr_name_master}={pr_value_master}/"
    )

print(f"[master→analytics] Leyendo de:  {path_in}")
print(f"[master→analytics] Escribiendo en: {path_out}")

# 4) Leer datos
df = spark.read.parquet(path_in)

# 5) Normalizar esquema (opcional)
for c in df.columns:
    df = df.withColumnRenamed(c, c.lower())

# 6) Transformaciones de negocio ejemplo
#    Ajusta según tus necesidades reales:
#    - Agregados
#    - Joins con otras tablas
#    - Cálculos de métricas
df_agg = (
    df.groupBy("producto_id")
      .agg(
         F.max("saldo").alias("saldo_max"),
         F.count("*").alias("total_registros")
      )
)

# 7) Escribir resultados
df_agg.write.mode("overwrite").parquet(path_out)

# 8) Registrar partición en Glue Catalog
if load_type != 'full':
    athena = boto3.client('athena')
    tbl_analytics = f"analytics__{table}".lower()
    partition = f"{pr_name_master}='{pr_value_master}'"
    sql = (
        f"ALTER TABLE {db_analytics}.{tbl_analytics} "
        f"ADD IF NOT EXISTS PARTITION ({partition})"
    )
    print(f"[master→analytics] Añadiendo partición: {partition}")
    athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': db_analytics},
        ResultConfiguration={'OutputLocation': path_out}
    )

# 9) Limpieza
gc.collect()




df= sqlContext.sql("""
                      SELECT ID_REGISTRO_LISTA,
                            NOMBRE_LISTA,
                            CAMPANA,
                            TIPO_CAMPANA,
                            REGISTROS_LISTAS,
                            REGISTROS_MARCADOS,
                            CONTACTADOS,
                            HORA_PRIMER_CONTACTO,
                            INTENTOS_PRIMER_CONTACTO,
                            MARCA_HORA_PRIMER_CONTACTO,
                            INTENTOS_MARCACION,
                            ULTIMA_DISPOSICION,
                            DISPOSICION_FINAL,
                            MARCA_HORA_DISPOSICION_FINAL,
                            ID_LLAMADA_ULTIMO_INTENTO,
                            ULTIMO_NUMERO_TELEFONO_INTENTO_LLAMAR,
                            MARCA_HORA_ULTIMA_LLAMADA,
                            DIA_ULTIMA_LLAMADA,
                            HORA_ULTIMA_LLAMADA,
                            MARCA_HORA_ULTIMO_NUMERO_SALTADO,
                            MOTIVO_ULTIMO_NUMERO_SALTADO,
                            ESTADO,
                            NUMERO_DESHABILITADO_PERFIL,
                            NO_CONTACTABLES,
                            NO_CONTACTABLE_NO_MARCAR,
                            NO_CONTACTABLE_DNC,
                            NO_CONTACTABLE_MAXIMO_INTENTOS,
                            FINAL,
                            NUEVO,
                            EN_CURSO,
                            BANDERA_ASAP,
                            TIEMPO_CONSIDERACION,
                            TIEMPO_CONSIDERACION_FECHA,
                            TIEMPO_CONSIDERACION_FECHA_HORA,
                            ID_CONTACTO,
                            FECHAHORA,
                            FECHA,
                            HORA,
                            TIMESTAMP_ID
                      FROM silver.cet_registrolista
                      WHERE fecha > date_add(current_date(), -7)
""");

transversales.loadDwhStagingTable(df,"Tmp_dw_RegistroListaCET","overwrite")


df= sqlContext.sql("""
                      SELECT ID_LLAMADA,
                            TIPO_LLAMADA,
                            CAMPANA,
                            TIPO_CAMPANA,
                            DISPOSICION,
                            GRUPOA_DISPOSICIONES,
                            GRUPOB_DISPOSICIONES,
                            GRUPOC_DISPOSICIONES,
                            RUTA_DISPOSICION,
                            SUBSTRING(TRIM(NOTAS),0,4000) AS NOTAS,
                            COMPETENCIA,
                            NOMBRE_LISTA,
                            --FECHAHORA,
                            MARCA_HORA_MILISEGUNDOS,
                            --ANO,
                            FECHA,
                            --FECHA_HORA,
                            --DIA_SEMANA,
                            --MES,
                            --DIA_MES,
                            --CUARTO_HORA,
                            --MEDIA_HORA,
                            HORA,
                            --HORA_DIA,
                            --INTERVALO_TIEMPO,
                            ID_SESION,
                            ID_SESION_MATRIZ,
                            DNIS,
                            CODIGO_AREA_DNIS,
                            ESTADO_DNIS,
                            CODIGO_PAIS_DNIS,
                            PAIS_DNIS,
                            ANI,
                            --CODIGO_AREA_ANI,
                            --ESTADO_ANI,
                            CODIGO_PAIS_ANI,
                            PAIS_ANI,
                            LLAMADAS,
                            CONTACTADOS,
                            CONEXION_INTERLOCUTOR_HUMANO,
                            SIN_CONTACTO_INTERLOCUTOR_HUMANO,
                            VELOCIDAD_RESPUESTA,
                            GRABACIONES,
                            NIVEL_SERVICIO,
                            RESULTADO_ENCUESTA_LLAMADA,
                            ABANDONADA,
                            INDICE_LLAMADAS_ABANDONADAS,
                            DESCONECTO_ESPERA,
                            AGOTO_TIEMPO_ESPERA_LLAMADAS_IVR,
                            DURACION_LLAMADA,
                            TIEMPO_LLAMADA,
                            TIEMPO_MARCACION,
                            TIEMPO_ESPERA,
                            TIEMPO_ESPERA_PROLONGADA,
                            TIEMPO_ESPERA_COLA,
                            TIEMPO_COLA_TOTAL,
                            TIEMPO_ESPERA_DEVOLUCION_LLAMADAS_COLA,
                            TIEMPO_ATENCION,
                            TIEMPO_TRABAJO_DESPUES_LLAMADA,
                            TIEMPO_IVR,
                            TIEMPO_PREVISUALIZACION,
                            TIEMPO_REPIQUE,
                            TIEMPO_FACTURACION_REDONDEO,
                            TIEMPO_MANUAL,
                            CONFERENCIAS,
                            HORA_CONFERENCIA,
                            HORA_CONSULTA,
                            TRANSFERENCIAS,
                            INDICE,
                            ESPERAS,
                            ESPERAS_PROLONGADAS,
                            HORA_ABANDONO_LLAMADAS,
                            DEVOLUCION_LLAMADAS_COLA_REGISTRADAS,
                            PROCESAMIENTO_DEVOLUCION_LLAMADAS_COLA,
                            COSTO,
                            DURACION_VIDEO,
                            DURACION_LLAMADA_TERCERO,
                            DURACION_LLAMADA_MENOS_TIEMPO_ESPERA_ESPERA_PROLONGADA,
                            VISTA_PREVIA_INTERRUMPIDA,
                            VISTA_PREVIA_INTERRUMPIDA_LLAMADA,
                            VISTA_PREVIA_INTERRUMPIDA_CORREO_VOZ_GRUPO_COMPETENCIAS,
                            CORREOS_VOZ,
                            CORREOS_VOZ_ATENDIDOS,
                            CORREOS_VOZ_TRANSFERIDOS,
                            CORREOS_VOZ_BORRADOS,
                            CORREOS_VOZ_RECHAZADOS,
                            LLAMADA_DEVUELTA_A_CORREOS_VOZ,
                            TIEMPO_ATENCION_CORREO_VOZ,

                            ENCUESTA_AGILIDAD,
                            ENCUESTA_NPS,
                            ENCUESTA_AMABILIDAD,
                            ENCUESTA_INFORMACION,
                            ENCUESTA_GUSTO,
                            ARBOL_PRODUCTO_TDC,
                            ARBOL_PRODUCTO_CDT,
                            ARBOL_PRODUCTO_CUENTAS_DE_AHORRO,
                            ARBOL_PRODUCTO_VEHICULO,
                            ARBOL_PRODUCTO_LIBRANZA,
                            ARBOL_PRODUCTO_CONSUMO,
                            ARBOL_PRODUCTO_SEGUROS,
                            ARBOL_INFORMACION_GENERAL,
                            ARBOL_COBRANZAS,
                            ARBOL_VENTAS,

                            ID_AGENTE,
                            ID_CONTACTO,
                            TIMESTAMP_ID
                      FROM silver.cet_contactabilidad
                      WHERE fecha > date_add(current_date(), -7)
""");
  
transversales.loadDwhStagingTable(df,"Tmp_dw_ContactabilidadCET","overwrite")


df= sqlContext.sql("""
                      SELECT MODULO,
                            SUBSTRING(TRIM(TIPO_MODULO),0,4000) AS TIPO_MODULO,
                            HORA_MODULO,
                            SUBSTRING(TRIM(RUTA_AL_MODULO),0,4000) AS RUTA_AL_MODULO,
                            MARCA_HORA_INICIO_MODULO,
                            LATENCIA_MODULO_CONSULTA,
                            AGOTAMIENTO_TIEMPO_ESPERA_MODULO_CONSULTA,
                            ERROR_MODULO_CONSULTA,
                            TERMINACIONES,
                            GRABACION,
                            TIEMPO_IVR_MODULO,
                            ENTRADAS_VOZ,
                            ENTRADAS_DTMF,
                            ENTRADA_USUARIO,
                            INTENTOS_ENTRADA,
                            TIEMPOS_ESPERA_ENTRADA_AGOTADOS,
                            TIEMPOS_ESPERA_SILENCIO_AGOTADOS,
                            ID_SESION_IVR,
                            FECHAHORA,
                            FECHA,
                            HORA,
                            TIMESTAMP_ID
                      FROM silver.cet_modulo
                      WHERE fecha > date_add(current_date(), -7)
""");
  
transversales.loadDwhStagingTable(df,"Tmp_dw_ModuloCET","overwrite")



df= sqlContext.sql("""
                      SELECT ID_SESION_IVR,
                            GUION_IVR,
                            IVR_VOZ,
                            IVR_VISUAL,
                            TIPO_MEDIOS,
                            TIEMPO_IVR,
                            SUBSTRING(TRIM(RUTA_IVR),0,4000) AS RUTA_IVR,
                            RUTA_AGENTE,
                            SUBSTRING(TRIM(RUTA_TRANSFERENCIA_AGENTE),0,4000) AS RUTA_TRANSFERENCIA_AGENTE,
                            SUBSTRING(TRIM(RUTA_TRANSFERENCIA_MAXIMA_AGENTE),0,4000) AS RUTA_TRANSFERENCIA_MAXIMA_AGENTE,
                            SUBSTRING(TRIM(RUTA_ABANDONO_LLAMADA),0,4000) AS RUTA_ABANDONO_LLAMADA,
                            RUTA_COMPETENCIA,
                            RUTA_SIN_COINCIDENCIA,
                            TIEMPO_IVR_PRIMER_MENSAJE,
                            TIEMPO_IVR_PRIMERA_COLA,
                            TIEMPO_IVR_ABANDONO_LLAMADA,
                            LLAMADAS_COMPLETADAS_IVR,
                            LLAMADAS_DESCONECTADAS_IVR,
                            LLAMADAS_ABANDONADAS_COLA,
                            LLAMADAS_TRANSFERIDAS_AGENTE,
                            ID_LLAMADA,
                            FECHAHORA,
                            FECHA,
                            HORA,
                            TIMESTAMP_ID
                      FROM silver.cet_respuestainteractiva
                      WHERE fecha > date_add(current_date(), -7)
""");
  
transversales.loadDwhStagingTable(df,"Tmp_dw_RespuestaInteractivaCET","overwrite")



df= sqlContext.sql("""
                      SELECT ID_CONTACTO,
                            ID,
                            ID_SOLICITUD,
                            FECHA_LLEGADA_LEAD,
                            HORA_LLEGADA_LEAD,
                            FLUJO_AGIL,
                            ID_ORIGINAL,
                            NOMBRE,
                            APELLIDO,
                            EMAIL,
                            EDAD_PROMEDIO,
                            ESTADO,
                            CIUDAD,
                            CALLE,
                            CODIGO_POSTAL,
                            INGRESOS,
                            RIESGO,
                            ACTIVIDAD_ECONOMICA,
                            CLASIFICACION,
                            CAMPANIA,
                            FUENTE,
                            LINK_FUENTE,
                            NOMBRE_EMPRESA,
                            NEGOCIO,
                            ANI1,
                            PRINCIPAL,
                            ALTERNATIVO1,
                            ALTERNATIVO2,
                            MARCA_HORA_CREACION_CONTACTO,
                            MARCA_HORA_MODIFICACION_CONTACTO,
                            FECHA_INICIO,
                            MES_LLEGADA,
                            FECHA_SIG_CUOTA,
                            VLR_CUOTA,
                            SALDO_CAPITAL,
                            SUBSTRING(TRIM(NUMERO_CARGUE),0,200) NUMERO_CARGUE,
                            OBLIGACION,
                            TIPO_PRODUCTO, 
                            CUPO_CONSUMO,
                            CUPO_TDC,
                            CUPO_TDC_CASATORO,
                            CUPO_TDC_BMW,
                            CUPO_TDC_NACIONAL,
                            CUPO_TDC_CENTRALMOTOR,
                            CUPO_AUTOEFECTIVO,
                            CUPO_RETANQUEO,
                            CUPO_MOTORYSA,
                            CUPO_BANNERROJO,
                            CUPO_PRECALENTAMIENTO,
                            BANNERVH,
                            TC_VENDER, 
                            PLAN_APLICA,
                            APLICA_REDIFERIDO,
                            TASA,
                            TASA_ACTUAL,
                            PASO_ACTUAL,
                            PLACA,
                            MODELO,
                            VALOR_CINCUENTA,
                            VALOR_MORA,
                            DIAS_MORA,
                            TEST,
                            VIGENCIA_BASE,
                            ULTIMA_DISPOSICION,
                            FECHAHORA,
                            FECHA,
                            HORA,
                            TIMESTAMP_ID
                      FROM silver.cet_contacto
                      WHERE fecha > date_add(current_date(), -7)
""");
  
transversales.loadDwhStagingTable(df,"Tmp_dw_ContactoCET","overwrite")



df= sqlContext.sql("""
                      SELECT ID_AGENTE,
                            AGENTE,
                            PRIMER_NOMBRE_AGENTE,
                            APELLIDO_AGENTE,
                            NOMBRE_AGENTE,
                            CORREO_ELECTRONICO_AGENTE,
                            EXTENSION,
                            FECHA_INICIO_AGENTE,
                            MES_INICIO_AGENTE,
                            ANO_INICIO_AGENTE,
                            GRUPO_AGENTES,
                            HABILITADO_VIDEO,
                            ESTADO,
                            ESTADOS_AGENTES,
                            CODIGO_RAZON,
                            HORA_CONEXION,
                            MARCA_HORA_CONEXION,
                            HORA_DESCONEXION,
                            MARCA_HORA_DESCONEXION,
                            DURACION_VIDEO,
                            DISPONIBLE_LLAMADAS,
                            DISPONIBILIDAD_MEDIOS,
                            DISPONIBILIDAD_COMPETENCIAS,
                            DISPONIBLE_CORREO_VOZ,
                            DISPONIBLE_TODAS,
                            TIEMPO_ESTADO_AGENTE,
                            TIEMPO_ESPERA,
                            TIEMPO_TIMBRE,
                            TIEMPO_MANUAL,
                            TIEMPO_UNA_LLAMADA,
                            TIEMPO_PAGADO,
                            TIEMPO_NO_PAGADO,
                            TIEMPO_ACW,
                            TIEMPO_LISTO,
                            TIEMPO_NO_LISTO,
                            TIEMPO_CORREO_VOZ,
                            TIEMPO_CORREO_VOZ_CURSO,
                            NO_DISPONIBLE_LLAMADAS,
                            NO_DISPONIBLE_CORREO_VOZ,
                            FECHAHORA,
                            FECHA,
                            HORA,
                            TIMESTAMP_ID
                      FROM silver.cet_seguimientoagente
                      WHERE fecha > date_add(current_date(), -7)
""");
  
transversales.loadDwhStagingTable(df,"Tmp_dw_SeguimientoAgenteCET","overwrite")



df= sqlContext.sql("""
                      SELECT SKILL,
                             FECHA,
                             CANTIDAD_LLAMADAS,
                             VELOCIDAD_RESPUESTA,
                             NIVEL_SERVICIO,
                             ABANDONADO,
                             TRANSFERIDO,
                             TIEMPO_MANEJO,
                             TIPO_LLAMADA,
                             COLA_DEVOLUCION_LLAMADAS_REGISTRADAS,
                             COLA_DEVOLUCION_LLAMADAS_PROCESADAS,
                             TIEMPO_DESPUES_LLAMADA,
                             ESPERA,
                             TIEMPO_ESPERA,
                             TIEMPO_IVR,
                             AGENTE,
                             ID_LLAMADA,
                             MES,
                             MEDIA_HORA,
                             HORA,
                             HORA_DEL_DIA,
                             TIEMPO_INTERVALO,
                             TIEMPO_LLAMADA,
                             CAMPANIA,
                             FECHAHORA,
                             TIMESTAMP_ID
                      FROM silver.cet_trafico
                      WHERE fecha > date_add(current_date(), -7)
""");
  
transversales.loadDwhStagingTable(df,"Tmp_dw_TraficoAgenteCET","overwrite")



spark.stop()
print("[master→analytics] Job COMPLETADO")