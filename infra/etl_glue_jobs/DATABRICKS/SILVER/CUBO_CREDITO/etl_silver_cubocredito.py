import sys, re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,ArrayType, DoubleType, BooleanType,LongType
from pyspark.sql.functions import *
import os
import shutil
import unicodedata
import boto3
import time
from urllib.parse import urlparse
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F

# Importar tu librería personalizada
import transversales_aws02 as transversales_lib

# Optional imports for internal libraries packaged as .whl/.zip and attached to Glue job
# from your_internal_lib.io import read_table, write_table

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'FechaReporte',
    'warehouse_s3_uri',
    'raw_db',
    'master_db',
    'staging_db'
])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Iceberg + Glue Catalog configuration
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args['warehouse_s3_uri'])
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

RAW_DB = args['raw_db']
MASTER_DB = args['master_db']
STAGING_DB = args['staging_db']
FECHA_REPORTE = args['FechaReporte']  # yyyy-MM-dd

# Ensure DBs exist (no-op if already exist)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {RAW_DB}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {MASTER_DB}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {STAGING_DB}")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def run(sql):
    # simple logger
    print("\n=== Executing SQL ===\n" + sql[:5000] + ("..." if len(sql)>5000 else ""))
    spark.sql(sql)

# ==== Translated SQL blocks in execution order ====

# Block 0
run(f"""--Se crea vista Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TEMP VIEW vw_tmp_inttec_historico  
 AS
  SELECT DISTINCT
  CONTRATO AS contrato
, ID_NUMERO AS idNumero
, to_date(Fecha_Emision,'yyyy-MM-dd') AS fechaEmision
, CAST(REPLACE(TRIM(CUPO),' ', '') AS DECIMAL(18,2)) AS cupo
, CAST(REPLACE(TRIM(SALDO_INTTEC),' ', '') AS DECIMAL(18,2)) AS saldoINTTEC
, SUBPRODUCTO AS subproducto
, to_date(Cierre,'yyyy-MM-dd') AS cierre
, DIAS_MORA AS diasMora
, ESTADO_PLASTICO AS estadoPlastico
, ESTADO_CREDITO AS estadoCredito
, TRIM(CANAL) AS canal
, TRIM(ORIGEN) AS origen
, TRIM(FUENTE_CANAL) AS fuenteCanal
  FROM {{RAW_DB}}.finandinacartera_inttec_historico
  WHERE cierre >= TRUNC(TO_DATE(CAST('{{FECHA_REPORTE}}' AS STRING), 'yyyy-MM-dd'), 'MM');""")

# Block 1
run(f"""SELECT * FROM {{RAW_DB}}.finandinacartera_inttec_historico""")

# Block 2
run(f"""--Se eliminan de Tabla Silver los registros que existan en la tabla bronze (Campopivot(cierre)) para luego ser insertadas:
DELETE FROM {{MASTER_DB}}.dwh_inttec_historico
WHERE cierre IN ( SELECT DISTINCT cierre
                        FROM vw_tmp_inttec_historico
                      );""")

# Block 3
run(f"""--Se insertan los nuevos registros por (Campopivot(cierre))
INSERT INTO {{MASTER_DB}}.dwh_inttec_historico 
(
	contrato
,	idNumero
,	fechaEmision
,	cupo
,	saldoINTTEC
,	subproducto
,	cierre
,	diasMora
,	estadoPlastico
,	estadoCredito
,	canal
,	origen
,	fuenteCanal
)
SELECT DISTINCT
	bih.contrato
,	bih.idNumero
,	bih.fechaEmision
,	bih.cupo
,	bih.saldoINTTEC
,	bih.subproducto
,	bih.cierre
,	bih.diasMora
,	bih.estadoPlastico
,	bih.estadoCredito
,	bih.canal
,	bih.origen
,	bih.fuenteCanal
FROM vw_tmp_inttec_historico AS bih
LEFT JOIN {{MASTER_DB}}.dwh_inttec_historico AS sih
  ON bih.cierre = sih.cierre
WHERE sih.cierre IS NULL;""")

# Block 4
run(f"""--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_facturacion_tdc_historico
 AS
  SELECT DISTINCT
  Contrato AS contrato
, TRIM(TipoCompra) AS tipoCompra
, CAST(REPLACE(TRIM(Facturacion),' ', '') AS DECIMAL(18,2)) AS facturacion
, Cantidad AS cantidad
, to_date(Fecha_compra,'yyyy-MM-dd') AS fechaCompra
, to_date(Fecha_Emision,'yyyy-MM-dd') AS fechaEmision
FROM {{RAW_DB}}.finandinacartera_facturacion_tdc_historico;""")

# Block 5
run(f"""--Inserción full en silver
DELETE FROM {{MASTER_DB}}.dwh_facturacion_tdc_historico;

INSERT INTO {{MASTER_DB}}.dwh_facturacion_tdc_historico
(
	contrato
,	tipoCompra
,	facturacion
,	cantidad
,	fechaCompra
,	fechaEmision
)
  SELECT DISTINCT
	bfth.contrato
,	bfth.tipoCompra
,	bfth.facturacion
,	bfth.cantidad
,	bfth.fechaCompra
,	bfth.fechaEmision
FROM staging.tmp_facturacion_tdc_historico AS bfth""")

# Block 6
run(f"""--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TEMP VIEW vw_tmp_endeot_historico
 AS
  SELECT DISTINCT
  ETNRCR
, ETNRID
, ETCLAS
, CAST(REPLACE(TRIM(ETVRSD),' ', '') AS DECIMAL(18,2)) AS ETVRSD
, CAST(REPLACE(TRIM(ETINAC),' ', '') AS DECIMAL(18,2)) AS ETINAC
, CAST(REPLACE(TRIM(ETCAR1),' ', '') AS DECIMAL(18,2)) AS ETCAR1
, CAST(REPLACE(TRIM(ETCAR2),' ', '') AS DECIMAL(18,2)) AS ETCAR2
, CAST(REPLACE(TRIM(ETSEGAUT),' ', '') AS DECIMAL(18,2)) AS ETSEGAUT
, CAST(REPLACE(TRIM(ETSEGVID),' ', '') AS DECIMAL(18,2)) AS ETSEGVID
, CAST(REPLACE(TRIM(ETCXCSOA),' ', '') AS DECIMAL(18,2)) AS ETCXCSOA
, CAST(REPLACE(TRIM(ETSEGDES),' ', '') AS DECIMAL(18,2)) AS ETSEGDES
, CAST(REPLACE(TRIM(ETSEGOTR),' ', '') AS DECIMAL(18,2)) AS ETSEGOTR
, to_date(ETFCIN,'yyyy-MM-dd') AS ETFCIN
, to_date(ETFCFI,'yyyy-MM-dd') AS ETFCFI
, ETTPCR
, ETDSMR
, CAST(REPLACE(TRIM(ETPRCOCA),' ', '') AS DECIMAL(18,2)) AS ETPRCOCA
, CAST(REPLACE(TRIM(ETPRAMHI),' ', '') AS DECIMAL(18,2)) AS ETPRAMHI
, cierre
FROM {{RAW_DB}}.finandinacartera_endeot_historico
WHERE CONCAT(cierre, '-01') >= TRUNC(TO_DATE(CAST('{{FECHA_REPORTE}}' AS STRING), 'yyyy-MM-dd'), 'MM');""")

# Block 7
run(f"""--Se eliminan de Tabla Silver los registros que existan en la tabla bronze (Campopivot(cierre)) para luego ser insertadas:
DELETE FROM {{MASTER_DB}}.dwh_endeot_historico
WHERE cierre IN ( SELECT DISTINCT cierre
                        FROM vw_tmp_endeot_historico
                      );""")

# Block 8
run(f"""--Se insertan los nuevos registros por (Campopivot(cierre))
INSERT INTO {{MASTER_DB}}.dwh_endeot_historico
(
  ETNRCR
,	ETNRID
,	ETCLAS
,	ETVRSD
,	ETINAC
,	ETCAR1
,	ETCAR2
,	ETSEGAUT
,	ETSEGVID
,	ETCXCSOA
,	ETSEGDES
,	ETSEGOTR
,	ETFCIN
,	ETFCFI
,	ETTPCR
,	ETDSMR
,	ETPRCOCA
,	ETPRAMHI
,	cierre
)
  SELECT DISTINCT
  beh.ETNRCR
,	beh.ETNRID
,	beh.ETCLAS
,	beh.ETVRSD
,	beh.ETINAC
,	beh.ETCAR1
,	beh.ETCAR2
,	beh.ETSEGAUT
,	beh.ETSEGVID
,	beh.ETCXCSOA
,	beh.ETSEGDES
,	beh.ETSEGOTR
,	beh.ETFCIN
,	beh.ETFCFI
,	beh.ETTPCR
,	beh.ETDSMR
,	beh.ETPRCOCA
,	beh.ETPRAMHI
,	beh.cierre
FROM vw_tmp_endeot_historico AS beh
LEFT JOIN {{MASTER_DB}}.dwh_endeot_historico AS seh
  ON beh.cierre = seh.cierre
WHERE seh.cierre IS NULL;""")

# Block 9
run(f"""--Se crea vista Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TEMP VIEW vw_tmp_ende_historico
 AS
  SELECT DISTINCT
	Obligacion AS obligacion 
,	Identificacion AS identificacion 
,	TRIM(Digito_Chequeo) AS digitoChequeo 
,	Tipo_Identificacion AS tipoIdentificacion 
,	UPPER(TRIM(Primer_Apellido)) AS primerApellido 
,	UPPER(TRIM(Segundo_Apellido)) AS segundoApellido 
,	UPPER(TRIM(Nombre)) AS nombre 
,	UPPER(TRIM(Cliente)) AS cliente 
,	TRIM(Codigo_CIIU) AS codigoCIIU 
,	Naturaleza_Juridica AS naturalezaJuridica 
,	TRIM(Referencia) AS referencia 
,	TRIM(Concurso_Acredores) AS concursoAcredores 
,	TRIM(Acuerdo_Informal) AS acuerdoInformal 
,	TRIM(Indicador_Reestructurado) AS indicadorReestructurado 
,	TRIM(Departamento) AS departamento 
,	TRIM(Sucursal) AS sucursal 
,	Tipo_Credito AS tipoCredito 
,	CAST(REPLACE(TRIM(Valor_Activo),' ', '') AS DECIMAL(18,2)) AS valorActivo 
,	CAST(REPLACE(TRIM(Valor_Credito),' ', '') AS DECIMAL(18,2)) AS valorCredito 
,	CAST(REPLACE(TRIM(Valor_Anticipo),' ', '') AS DECIMAL(18,2)) AS valorAnticipo 
,	CAST(REPLACE(TRIM(Opcion_Compra),' ', '') AS DECIMAL(18,2)) AS opcionCompra 
,	to_date(Fecha_Inicio,'yyyy-MM-dd') AS fechaInicio 
,	to_date(Fecha_Final,'yyyy-MM-dd') AS fechaFinal 
,	CAST(REPLACE(TRIM(TasaEfectiva_Inicial),' ', '') AS DECIMAL(18,4)) AS tasaEfectivaInicial 
,	CAST(REPLACE(TRIM(TasaEfectiva_Actual),' ', '') AS DECIMAL(18,4)) AS tasaEfectivaActual 
,	TRIM(Tipo_Garantia) AS tipoGarantia 
,	TRIM(Garantia) AS garantia 
,	CAST(REPLACE(TRIM(Valor_Garantias),' ', '') AS DECIMAL(18,2)) AS valorGarantias 
,	TRIM(FechaAvaluo_Garantia) AS fechaAvaluoGarantia 
,	CAST(REPLACE(TRIM(Clase_Bien),' ', '') AS DECIMAL(18,2)) AS claseBien 
,	CAST(REPLACE(TRIM(Prima_AmortizacionAcumulada),' ', '') AS DECIMAL(18,2)) AS primaAmortizacionAcumulada 
,	CAST(REPLACE(TRIM(Valor_SaldoCapital),' ', '') AS DECIMAL(18,2)) AS valorSaldoCapital 
,	to_date(Fecha_Inicio_Mora,'yyyy-MM-dd') AS fechaInicioMora 
,	Dias_Mora AS diasMora 
,	Clasificacion_Anterior AS clasificacionAnterior 
,	Clasificacion_Nueva AS clasificacionNueva 
,	TRIM(Subclase) AS subclase 
,	TRIM(Uso_Garantia) AS usoGarantia 
,	TRIM(Calificacion_Automatica) AS calificacionAutomatica 
,	TRIM(Calificacion_Manual) AS calificacionManual 
,	TRIM(Calificacion_homologa) AS calificacionHomologa 
,	CAST(REPLACE(TRIM(Valor_Capital),' ', '') AS DECIMAL(18,2)) AS valorCapital 
,	CAST(REPLACE(TRIM(Valor_Interes),' ', '') AS DECIMAL(18,2)) AS valorInteres 
,	CAST(REPLACE(TRIM(Valor_Seguro_Auto),' ', '') AS DECIMAL(18,2)) AS valorSeguroAuto 
,	CAST(REPLACE(TRIM(Seguro_Vida),' ', '') AS DECIMAL(18,2)) AS seguroVida 
,	CAST(REPLACE(TRIM(Seguro_Desembolso),' ', '') AS DECIMAL(18,2)) AS seguroDesembolso 
,	CAST(REPLACE(TRIM(ENSEGSOA),' ', '') AS DECIMAL(18,2)) AS ENSEGSOA 
,	CAST(REPLACE(TRIM(ValorOtroSeguro),' ', '') AS DECIMAL(18,2)) AS valorotroseguro 
,	CAST(REPLACE(TRIM(Cargo1),' ', '') AS DECIMAL(18,2)) AS cargo1 
,	CAST(REPLACE(TRIM(Cargo2),' ', '') AS DECIMAL(18,2)) AS cargo2 
,	CAST(REPLACE(TRIM(Cargo3),' ', '') AS DECIMAL(18,2)) AS cargo3 
,	CAST(REPLACE(TRIM(Interes_Mora),' ', '') AS DECIMAL(18,2)) AS interesMora 
,	CAST(REPLACE(TRIM(Capital_Vencido),' ', '') AS DECIMAL(18,2)) AS capitalVencido 
,	CAST(REPLACE(TRIM(Interes_Vencido),' ', '') AS DECIMAL(18,2)) AS interesVencido 
,	CAST(REPLACE(TRIM(Seguro_Auto_Vencido),' ', '') AS DECIMAL(18,2)) AS seguroAutoVencido 
,	CAST(REPLACE(TRIM(Seguro_Vida_Vencido),' ', '') AS DECIMAL(18,2)) AS seguroVidaVencido 
,	CAST(REPLACE(TRIM(Seguro_DesembolsoVencido),' ', '') AS DECIMAL(18,2)) AS seguroDesembolsoVencido 
,	CAST(REPLACE(TRIM(ENSOAVEN),' ', '') AS DECIMAL(18,2)) AS ENSOAVEN 
,	CAST(REPLACE(TRIM(OtrosSeguros_Vencidos),' ', '') AS DECIMAL(18,2)) AS otrosSegurosVencidos 
,	CAST(REPLACE(TRIM(Cargo1_Vencido),' ', '') AS DECIMAL(18,2)) AS cargo1Vencido 
,	CAST(REPLACE(TRIM(Cargo2_Vencido),' ', '') AS DECIMAL(18,2)) AS cargo2Vencido 
,	CAST(REPLACE(TRIM(Cargo3_Vencido),' ', '') AS DECIMAL(18,2)) AS cargo3Vencido 
,	CAST(REPLACE(TRIM(Interes_Causado),' ', '') AS DECIMAL(18,2)) AS interesCausado 
,	CAST(REPLACE(TRIM(Interes_NoAcumulado),' ', '') AS DECIMAL(18,5)) AS interesNoAcumulado 
,	CAST(REPLACE(TRIM(Capital_Causado),' ', '') AS DECIMAL(18,2)) AS capitalCausado 
,	CAST(REPLACE(TRIM(Capital_NoAcumulacion),' ', '') AS DECIMAL(18,2)) AS capitalNoAcumulacion 
,	CAST(REPLACE(TRIM(Causacion_Mes),' ', '') AS DECIMAL(18,2)) AS causacionMes 
,	CAST(REPLACE(TRIM(Causado_SeguroVida),' ', '') AS DECIMAL(18,2)) AS causadoSeguroVida 
,	CAST(REPLACE(TRIM(Pagado_SeguroVida),' ', '') AS DECIMAL(18,2)) AS pagadoSeguroVida 
,	CAST(REPLACE(TRIM(Causado_SeguroAuto),' ', '') AS DECIMAL(18,2)) AS causadoSeguroAuto 
,	CAST(REPLACE(TRIM(Pagado_SeguroAuto),' ', '') AS DECIMAL(18,2)) AS pagadoSeguroAuto 
,	CAST(REPLACE(TRIM(SeguroVida_Causado),' ', '') AS DECIMAL(18,2)) AS seguroVidaCausado 
,	CAST(REPLACE(TRIM(Desembolso_Pagado),' ', '') AS DECIMAL(18,2)) AS desembolsoPagado 
,	CAST(REPLACE(TRIM(ENCAUSOA),' ', '') AS DECIMAL(18,2)) AS ENCAUSOA 
,	CAST(REPLACE(TRIM(ENPAGSOA),' ', '') AS DECIMAL(18,2)) AS ENPAGSOA 
,	CAST(REPLACE(TRIM(ENCXCSOA),' ', '') AS DECIMAL(18,2)) AS ENCXCSOA 
,	CAST(REPLACE(TRIM(SeguroVida_CausadoOtro),' ', '') AS DECIMAL(18,2)) AS segurovidaCausadoOtro 
,	CAST(REPLACE(TRIM(SeguroVida_Pagado),' ', '') AS DECIMAL(18,2)) AS seguroVidaPagado 
,	CAST(REPLACE(TRIM(InteresMora_Pagado),' ', '') AS DECIMAL(18,2)) AS interesMoraPagado 
,	TRIM(Nivel_Riesgo) AS nivelRiesgo 
,	CAST(REPLACE(TRIM(Probabilidad_Impago),' ', '') AS DECIMAL(18,9)) AS probabilidadImpago 
,	TRIM(Calificacion_ModeloReferencia) AS calificacionModeloReferencia 
,	CAST(REPLACE(TRIM(ProbIncumplimiento_ModeloRef),' ', '') AS DECIMAL(18,7)) AS probIncumplimientoModeloRef 
,	CAST(REPLACE(TRIM(TasaDescuentoEA),' ', '') AS DECIMAL(18,7)) AS tasaDescuentoEA 
,	CAST(REPLACE(TRIM(FactorPDI),' ', '') AS DECIMAL(18,7)) AS factorPDI 
,	CAST(REPLACE(TRIM(ProvisionCapital),' ', '') AS DECIMAL(18,2)) AS provisionCapital 
,	CAST(REPLACE(TRIM(ProvisionOtros),' ', '') AS DECIMAL(18,2)) AS provisionOtros 
,	CAST(REPLACE(TRIM(ProvisionAnticipada),' ', '') AS DECIMAL(18,2)) AS provisionAnticipada 
,	CAST(REPLACE(TRIM(ProvisionAnticipadaOtros),' ', '') AS DECIMAL(18,2)) AS provisionAnticipadaOtros 
,	CAST(REPLACE(TRIM(Provision_Capital),' ', '') AS DECIMAL(18,2)) AS provisionCapital1 
,	CAST(REPLACE(TRIM(Provision_K1),' ', '') AS DECIMAL(18,2)) AS provisionK1 
,	CAST(REPLACE(TRIM(Provision_Interes),' ', '') AS DECIMAL(18,2)) AS provisionInteres 
,	CAST(REPLACE(TRIM(Provi_Seguro),' ', '') AS DECIMAL(18,2)) AS proviSeguro 
,	CAST(REPLACE(TRIM(Provi_Otros),' ', '') AS DECIMAL(18,2)) AS proviOtros 
,	CAST(REPLACE(TRIM(Provi_Paga_AnioCapital),' ', '') AS DECIMAL(18,2)) AS proviPagaAnioCapital 
,	CAST(REPLACE(TRIM(Provi_Paga_AnioInteres),' ', '') AS DECIMAL(18,2)) AS proviPagaAnioInteres 
,	CAST(REPLACE(TRIM(Provi_Paga_AnioSeguro),' ', '') AS DECIMAL(18,2)) AS proviPagaAnioSeguro 
,	CAST(REPLACE(TRIM(Provi_Paga_AnioOtro),' ', '') AS DECIMAL(18,2)) AS proviPagaAnioOtro 
,	CAST(REPLACE(TRIM(RecK1_PagaMes),' ', '') AS DECIMAL(18,2)) AS reck1PagaMes 
,	CAST(REPLACE(TRIM(Provi_Paga_MesCapital),' ', '') AS DECIMAL(18,2)) AS proviPagaMesCapital 
,	CAST(REPLACE(TRIM(Provi_Paga_MesInteres),' ', '') AS DECIMAL(18,2)) AS proviPagaMesInteres 
,	CAST(REPLACE(TRIM(Provi_Paga_MesSeguro),' ', '') AS DECIMAL(18,2)) AS proviPagaMesSeguro 
,	CAST(REPLACE(TRIM(Provi_Paga_MesOtro),' ', '') AS DECIMAL(18,2)) AS proviPagaMesOtro 
,	CAST(REPLACE(TRIM(Recuperado_Capital),' ', '') AS DECIMAL(18,2)) AS recuperadoCapital 
,	CAST(REPLACE(TRIM(Recuperado_Interes),' ', '') AS DECIMAL(18,2)) AS recuperadoInteres 
,	CAST(REPLACE(TRIM(Recuperado_Seguros),' ', '') AS DECIMAL(18,2)) AS recuperadoSeguros 
,	CAST(REPLACE(TRIM(Recuperado_Otros),' ', '') AS DECIMAL(18,2)) AS recuperadoOtros 
,	CAST(REPLACE(TRIM(Valor_Contingencia),' ', '') AS DECIMAL(18,2)) AS valorContingencia 
,	CAST(REPLACE(TRIM(Canon_Corriente),' ', '') AS DECIMAL(18,2)) AS canonCorriente 
,	CAST(REPLACE(TRIM(Canon_NoCorriente),' ', '') AS DECIMAL(18,2)) AS canonNoCorriente 
,	CAST(REPLACE(TRIM(Opcion_Corriente),' ', '') AS DECIMAL(18,2)) AS opcionCorriente 
,	CAST(REPLACE(TRIM(Opcion_NoCorriente),' ', '') AS DECIMAL(18,2)) AS opcionNoCorriente 
,	CAST(REPLACE(TRIM(Proyeccion_Capital_Siguientes3Meses),' ', '') AS DECIMAL(18,2)) AS proyeccionCapitalSig3Meses 
,	CAST(REPLACE(TRIM(Proyeccion_Interes_Siguientes3Meses),' ', '') AS DECIMAL(18,2)) AS proyeccionInteresSig3Meses 
,	CAST(REPLACE(TRIM(Capital_Pagado_Ultimos3Meses),' ', '') AS DECIMAL(18,2)) AS capPagadoUlt3Meses 
,	CAST(REPLACE(TRIM(Interes_Pagado_Ultimos_3Meses),' ', '') AS DECIMAL(18,2)) AS intPagadoUlt3Meses 
,	TRIM(MoraActualB) AS moraActualB 
,	TRIM(MoraActualC) AS moraActualC 
,	TRIM(MoraAlta_UltTrim) AS moraAltaUltTrim 
,	TRIM(MoraAlta_PenulTrim) AS moraAltaPenulTrim 
,	TRIM(MoraAlta_AntepeTrim) AS moraAltaAntepeTrim 
,	TRIM(ComportamientoAnualRegular) AS comportamientoAnualRegular 
,	TRIM(ComportamientoAnualMalo) AS comportamientoAnualMalo 
,	TRIM(MoraMaximaB) AS moraMaximaB 
,	TRIM(MoraMaximaC) AS moraMaximaC 
,	TRIM(MoraMaximaD) AS moraMaximaD 
,	TRIM(RangoMoraMaxUlti3Anios) AS rangoMoraMaxUlti3Anios 
,	TRIM(IndicadorGarantia) AS indicadorGarantia 
,	CAST(REPLACE(TRIM(ValorZ),' ', '') AS DECIMAL(18,6)) AS valorZ 
,	CAST(REPLACE(TRIM(Puntaje),' ', '') AS DECIMAL(18,6)) AS puntaje 
,	TRIM(Cierre) AS cierre 
,	TRIM(ENCALMD) AS ENCALMD 
,	CAST(REPLACE(TRIM(ENUSOFU1),' ', '') AS DECIMAL(18,2)) AS ENUSOFU1 
,	CAST(REPLACE(TRIM(ENUSOFU2),' ', '') AS DECIMAL(18,2)) AS ENUSOFU2 
,	CAST(REPLACE(TRIM(ENUSOFU3),' ', '') AS DECIMAL(18,2)) AS ENUSOFU3 
,	CAST(REPLACE(TRIM(ENUSOFU4),' ', '') AS DECIMAL(18,2)) AS ENUSOFU4 
,	CAST(REPLACE(TRIM(ENPROIMPM2),' ', '') AS DECIMAL(18,7)) AS ENPROIMPM2 
,	CAST(REPLACE(TRIM(ENPVK2),' ', '') AS DECIMAL(18,2)) AS ENPVK2 
,	CAST(REPLACE(TRIM(ENPVKSEG),' ', '') AS DECIMAL(18,2)) AS ENPVKSEG 
,	CAST(REPLACE(TRIM(ENUSOFU5),' ', '') AS DECIMAL(18,2)) AS ENUSOFU5 
,	CAST(REPLACE(TRIM(ENUSOFU6),' ', '') AS DECIMAL(18,2)) AS ENUSOFU6 
,	TRIM(ENUSOFU7) AS ENUSOFU7 
,	TRIM(ENUSOFU8) AS ENUSOFU8 
,	CAST(REPLACE(TRIM(ENPVINGE),' ', '') AS DECIMAL(18,2)) AS ENPVINGE 
FROM {{RAW_DB}}.finandinacartera_ende_historico
WHERE CONCAT(cierre, '-01') >= TRUNC(TO_DATE(CAST('{{FECHA_REPORTE}}' AS STRING), 'yyyy-MM-dd'), 'MM');""")

# Block 10
run(f"""--Se eliminan de Tabla Silver los registros que existan en la tabla bronze (Campopivot(cierre)) para luego ser insertadas:
DELETE FROM {{MASTER_DB}}.dwh_ende_historico
WHERE cierre IN ( SELECT DISTINCT cierre
            FROM vw_tmp_ende_historico
                      );""")

# Block 11
run(f"""--Se insertan los nuevos registros por (Campopivot(cierre))
INSERT INTO {{MASTER_DB}}.dwh_ende_historico
(
	obligacion
,	identificacion
,	digitoChequeo
,	tipoIdentificacion
,	primerApellido
,	segundoApellido
,	nombre
,	cliente
,	codigoCIIU
,	naturalezaJuridica
,	referencia
,	concursoAcredores
,	acuerdoInformal
,	indicadorReestructurado
,	departamento
,	sucursal
,	tipoCredito
,	valorActivo
,	valorCredito
,	valorAnticipo
,	opcionCompra
,	fechaInicio
,	fechaFinal
,	tasaEfectivaInicial
,	tasaEfectivaActual
,	tipoGarantia
,	garantia
,	valorGarantias
,	fechaAvaluoGarantia
,	claseBien
,	primaAmortizacionAcumulada
,	valorSaldoCapital
,	fechaInicioMora
,	diasMora
,	clasificacionAnterior
,	clasificacionNueva
,	subclase
,	usoGarantia
,	calificacionAutomatica
,	calificacionManual
,	calificacionHomologa
,	valorCapital
,	valorInteres
,	valorSeguroAuto
,	seguroVida
,	seguroDesembolso
,	ENSEGSOA
,	valorOtroSeguro
,	cargo1
,	cargo2
,	cargo3
,	interesMora
,	capitalVencido
,	interesVencido
,	seguroAutoVencido
,	seguroVidaVencido
,	seguroDesembolsoVencido
,	ENSOAVEN 
,	otrosSegurosVencidos
,	cargo1Vencido
,	cargo2Vencido
,	cargo3Vencido
,	interesCausado
,	interesNoAcumulado
,	capitalCausado
,	capitalNoAcumulacion
,	causacionMes
,	causadoSeguroVida
,	pagadoSeguroVida
,	causadoSeguroAuto
,	pagadoSeguroAuto
,	seguroVidaCausado
,	desembolsoPagado
,	ENCAUSOA
,	ENPAGSOA
,	ENCXCSOA
,	segurovidaCausadoOtro
,	seguroVidaPagado
,	interesMoraPagado
,	nivelRiesgo
,	probabilidadImpago
,	calificacionModeloReferencia
,	probIncumplimientoModeloRef
,	tasaDescuentoEA
,	factorPDI
,	provisionCapital
,	provisionOtros
,	provisionAnticipada
,	provisionAnticipadaOtros
,	provisionCapital1
,	provisionK1
,	provisionInteres
,	proviSeguro
,	proviOtros
,	proviPagaAnioCapital
,	proviPagaAnioInteres
,	proviPagaAnioSeguro
,	proviPagaAnioOtro
,	reck1PagaMes
,	proviPagaMesCapital
,	proviPagaMesInteres
,	proviPagaMesSeguro
,	proviPagaMesOtro
,	recuperadoCapital
,	recuperadoInteres
,	recuperadoSeguros
,	recuperadoOtros
,	valorContingencia
,	canonCorriente
,	canonNoCorriente
,	opcionCorriente
,	opcionNoCorriente
,	proyeccionCapitalSig3Meses
,	proyeccionInteresSig3Meses
,	capPagadoUlt3Meses
,	intPagadoUlt3Meses
,	moraActualB
,	moraActualC
,	moraAltaUltTrim
,	moraAltaPenulTrim
,	moraAltaAntepeTrim
,	comportamientoAnualRegular
,	comportamientoAnualMalo
,	moraMaximaB
,	moraMaximaC
,	moraMaximaD
,	rangoMoraMaxUlti3Anios
,	indicadorGarantia
,	valorZ
,	puntaje
,	cierre
,	ENCALMD
,	ENUSOFU1
,	ENUSOFU2
,	ENUSOFU3
,	ENUSOFU4
,	ENPROIMPM2
,	ENPVK2
,	ENPVKSEG
,	ENUSOFU5
,	ENUSOFU6
,	ENUSOFU7
,	ENUSOFU8
,	ENPVINGE
)
  SELECT DISTINCT
	beh.obligacion
,	beh.identificacion
,	beh.digitoChequeo
,	beh.tipoIdentificacion
,	beh.primerApellido
,	beh.segundoApellido
,	beh.nombre
,	beh.cliente
,	beh.codigoCIIU
,	beh.naturalezaJuridica
,	beh.referencia
,	beh.concursoAcredores
,	beh.acuerdoInformal
,	beh.indicadorReestructurado
,	beh.departamento
,	beh.sucursal
,	beh.tipoCredito
,	beh.valorActivo
,	beh.valorCredito
,	beh.valorAnticipo
,	beh.opcionCompra
,	beh.fechaInicio
,	beh.fechaFinal
,	beh.tasaEfectivaInicial
,	beh.tasaEfectivaActual
,	beh.tipoGarantia
,	beh.garantia
,	beh.valorGarantias
,	beh.fechaAvaluoGarantia
,	beh.claseBien
,	beh.primaAmortizacionAcumulada
,	beh.valorSaldoCapital
,	beh.fechaInicioMora
,	beh.diasMora
,	beh.clasificacionAnterior
,	beh.clasificacionNueva
,	beh.subclase
,	beh.usoGarantia
,	beh.calificacionAutomatica
,	beh.calificacionManual
,	beh.calificacionHomologa
,	beh.valorCapital
,	beh.valorInteres
,	beh.valorSeguroAuto
,	beh.seguroVida
,	beh.seguroDesembolso
,	beh.ENSEGSOA
,	beh.valorOtroSeguro
,	beh.cargo1
,	beh.cargo2
,	beh.cargo3
,	beh.interesMora
,	beh.capitalVencido
,	beh.interesVencido
,	beh.seguroAutoVencido
,	beh.seguroVidaVencido
,	beh.seguroDesembolsoVencido
,	beh.ENSOAVEN 
,	beh.otrosSegurosVencidos
,	beh.cargo1Vencido
,	beh.cargo2Vencido
,	beh.cargo3Vencido
,	beh.interesCausado
,	beh.interesNoAcumulado
,	beh.capitalCausado
,	beh.capitalNoAcumulacion
,	beh.causacionMes
,	beh.causadoSeguroVida
,	beh.pagadoSeguroVida
,	beh.causadoSeguroAuto
,	beh.pagadoSeguroAuto
,	beh.seguroVidaCausado
,	beh.desembolsoPagado
,	beh.ENCAUSOA
,	beh.ENPAGSOA
,	beh.ENCXCSOA
,	beh.segurovidaCausadoOtro
,	beh.seguroVidaPagado
,	beh.interesMoraPagado
,	beh.nivelRiesgo
,	beh.probabilidadImpago
,	beh.calificacionModeloReferencia
,	beh.probIncumplimientoModeloRef
,	beh.tasaDescuentoEA
,	beh.factorPDI
,	beh.provisionCapital
,	beh.provisionOtros
,	beh.provisionAnticipada
,	beh.provisionAnticipadaOtros
,	beh.provisionCapital1
,	beh.provisionK1
,	beh.provisionInteres
,	beh.proviSeguro
,	beh.proviOtros
,	beh.proviPagaAnioCapital
,	beh.proviPagaAnioInteres
,	beh.proviPagaAnioSeguro
,	beh.proviPagaAnioOtro
,	beh.reck1PagaMes
,	beh.proviPagaMesCapital
,	beh.proviPagaMesInteres
,	beh.proviPagaMesSeguro
,	beh.proviPagaMesOtro
,	beh.recuperadoCapital
,	beh.recuperadoInteres
,	beh.recuperadoSeguros
,	beh.recuperadoOtros
,	beh.valorContingencia
,	beh.canonCorriente
,	beh.canonNoCorriente
,	beh.opcionCorriente
,	beh.opcionNoCorriente
,	beh.proyeccionCapitalSig3Meses
,	beh.proyeccionInteresSig3Meses
,	beh.capPagadoUlt3Meses
,	beh.intPagadoUlt3Meses
,	beh.moraActualB
,	beh.moraActualC
,	beh.moraAltaUltTrim
,	beh.moraAltaPenulTrim
,	beh.moraAltaAntepeTrim
,	beh.comportamientoAnualRegular
,	beh.comportamientoAnualMalo
,	beh.moraMaximaB
,	beh.moraMaximaC
,	beh.moraMaximaD
,	beh.rangoMoraMaxUlti3Anios
,	beh.indicadorGarantia
,	beh.valorZ
,	beh.puntaje
,	beh.cierre
,	beh.ENCALMD
,	beh.ENUSOFU1
,	beh.ENUSOFU2
,	beh.ENUSOFU3
,	beh.ENUSOFU4
,	beh.ENPROIMPM2
,	beh.ENPVK2
,	beh.ENPVKSEG
,	beh.ENUSOFU5
,	beh.ENUSOFU6
,	beh.ENUSOFU7
,	beh.ENUSOFU8
,	beh.ENPVINGE
FROM vw_tmp_ende_historico AS beh
LEFT JOIN {{MASTER_DB}}.dwh_ende_historico AS seh
  ON beh.cierre = seh.cierre
WHERE seh.cierre IS NULL;""")

# Block 12
run(f"""--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_ind_segm
 AS
  SELECT DISTINCT
  TRIM(SOLICITUD) AS solicitud
, SG_Obligacion AS sgObligacion
, Identificacion AS identificacion
, to_date(Cosecha_Fecha,'yyyy-MM-dd') AS cosechaFecha
, Altura AS altura
, TRIM(Producto) AS producto
, ACIERTA_FINAL AS aciertaFinal
, TRIM(Riesgo_Final) AS riesgoFinal
, CAST(REPLACE(TRIM(MontoDesembolso),' ', '') AS DECIMAL(18,2)) AS montoDesembolso
, TRIM(Rango_de_Desembolso) AS rangoDeDesembolso
, TRIM(Porcentaje_Financiacion) AS porcentajeFinanciacion
, TRIM(RANGO_PLAZO) AS rangoPlazo
, INITCAP(TRIM(TipoPersona)) AS tipoPersona
, TRIM(Genero) AS genero
, TRIM(EDAD) AS edad
, TRIM(Fecha_Nacimiento) AS fechaNacimiento
, TRIM(ClaseDocumento) AS claseDocumento
, TRIM(DOCUMENTO) AS documento
, TRIM(Fecha_Expedicion) AS fechaExpedicion
, UPPER(TRIM(DEPARTAMENTO)) AS departamento
, UPPER(TRIM(CIUDAD)) AS ciudad
, UPPER(TRIM(ESTADO_CIVIL)) AS estadoCivil
, UPPER(TRIM(TIPOVIVIENDA)) AS tipoVivienda
, COD_TIP_VIV AS codTipViv
, TRIM(ACTIVIDAD_CLIENTE) AS actividadCliente
, CIIU_CLIENTE AS CIIUCliente
, TRIM(DESCRI_CIIU) AS descriCIIU
, TRIM(RANGO_INGRESOS) AS rangoIngresos
, UPPER(TRIM(EMPRESA)) AS empresa
, UPPER(TRIM(TIPO_CONTRATO)) AS tipoContrato
, TRIM(FASECOLDA) AS faseColda
, TRIM(CLASEVEH) AS claseVeh
, TRIM(ESTADOGARANTIACUBO) AS estadoGarantiaCubo
, TRIM(SERVICIOCUBO) AS servicioCubo
, TRIM(Anio_modelo) AS anioModelo
, TRIM(MARCA) AS marca
, UPPER(TRIM(CONCESIONARIOSFIN)) AS concesionariosFin
, TRIM(PRODUCTO_ENDE) AS productoEnde
, TRIM(CONV_LIBRANZ) AS convLibranz
, TRIM(NOMBRE_PLAN) AS nombrePlan
, NPAGOS AS nPagos
, TRIM(TIPO_PLAN) AS tipoPlan
, TRIM(MARCA_SD) AS marcaSd
, UPPER(TRIM(COMERCIAL)) AS comercial
, TRIM(CANAL_ACTUALIZADO) AS canalActualizado
, TRIM(ORIGEN_ACTUALIZADO) AS origenActualizado
, TRIM(FUENTE_CANAL_NUEVO_ACTUALIZADO) AS fuenteCanalNuevoActualizado
, TIPO_CREDITO AS tipoCredito
, to_date(COSECHA_FECHA_DIA,'yyyy-MM-dd') AS cosechaFechaDia 
, CAST(REPLACE(TRIM(SALDO),' ', '') AS DECIMAL(18,2)) AS saldo
, CAST(REPLACE(TRIM(SALDO_FORMULADO),' ', '') AS DECIMAL(18,2)) AS saldoFormulado
, TRIM(CODIGO_COMERCIAL) AS codigoComercial
, UPPER(TRIM(SP_FUENTE)) AS spFuente
, TRIM(IND_LEASING) AS indLeasing
, UPPER(TRIM(IND_GPS)) AS indGps
, TRIM(IND_SD_MANUAL) AS indSdManual
, TRIM(INTTEC) AS INTTEC
, TRIM(MARCA_DUPLI) AS marcaDupli
, TRIM(ZONA_FINANCIABLE) AS zonaFinanciable
, TRIM(GPS_NUEVA_CLAS) AS gpsNuevaClas
, TRIM(ZONA_REGION) AS zonaRegion
FROM {{RAW_DB}}.finandinacartera_ind_segm;""")

# Block 13
run(f"""--Se eliminan de Tabla Silver los registros que existan en la tabla bronze (Campopivot(cosechaFecha)) para luego ser insertadas:
DELETE FROM {{MASTER_DB}}.dwh_ind_segm
WHERE cosechaFecha IN ( SELECT DISTINCT cosechaFecha
                        FROM staging.tmp_ind_segm
                      );""")

# Block 14
run(f"""--Se insertan los nuevos registros por (Campopivot(cosechaFecha))
INSERT INTO {{MASTER_DB}}.dwh_ind_segm
(
	solicitud
,	SGObligacion
,	identificacion
,	cosechaFecha
,	altura
,	producto
,	aciertaFinal
,	riesgoFinal
,	montoDesembolso
,	rangoDeDesembolso
,	porcentajeFinanciacion
,	rangoPlazo
,	tipoPersona
,	genero
,	edad
,	fechaNacimiento
,	claseDocumento
,	documento
,	fechaExpedicion
,	departamento
,	ciudad
,	estadoCivil
,	tipoVivienda
,	codTipViv
,	actividadCliente
,	CIIUCliente
,	descriCIIU
,	rangoIngresos
,	empresa
,	tipoContrato
,	fasecolda
,	claseVeh
,	estadoGarantiaCubo
,	servicioCubo
,	anioModelo
,	marca
,	concesionariosFin
,	productoEnde
,	convLibranz
,	nombrePlan
,	nPagos
,	tipoPlan
,	marcaSd
,	comercial
,	canalActualizado
,	origenActualizado
,	fuenteCanalNuevoActualizado
,	tipoCredito
,	cosechaFechaDia
,	saldo
,	saldoFormulado
,	codigoComercial
,	sPFuente
,	indLeasing
,	indGps
,	indSdManual
,	INTTEC
,	marcaDupli
,	zonaFinanciable
,	gpsNuevaClas
,	zonaRegion
)
  SELECT DISTINCT
	bis.solicitud
,	bis.SGObligacion
,	bis.identificacion
,	bis.cosechaFecha
,	bis.altura
,	bis.producto
,	bis.aciertaFinal
,	bis.riesgoFinal
,	bis.montoDesembolso
,	bis.rangoDeDesembolso
,	bis.porcentajeFinanciacion
,	bis.rangoPlazo
,	bis.tipoPersona
,	bis.genero
,	bis.edad
,	bis.fechaNacimiento
,	bis.claseDocumento
,	bis.documento
,	bis.fechaExpedicion
,	bis.departamento
,	bis.ciudad
,	bis.estadoCivil
,	bis.tipoVivienda
,	bis.codTipViv
,	bis.actividadCliente
,	bis.CIIUCliente
,	bis.descriCIIU
,	bis.rangoIngresos
,	bis.empresa
,	bis.tipoContrato
,	bis.fasecolda
,	bis.claseVeh
,	bis.estadoGarantiaCubo
,	bis.servicioCubo
,	bis.anioModelo
,	bis.marca
,	bis.concesionariosFin
,	bis.productoEnde
,	bis.convLibranz
,	bis.nombrePlan
,	bis.nPagos
,	bis.tipoPlan
,	bis.marcaSd
,	bis.comercial
,	bis.canalActualizado
,	bis.origenActualizado
,	bis.fuenteCanalNuevoActualizado
,	bis.tipoCredito
,	bis.cosechaFechaDia
,	bis.saldo
,	bis.saldoFormulado
,	bis.codigoComercial
,	bis.sPFuente
,	bis.indLeasing
,	bis.indGps
,	bis.indSdManual
,	bis.INTTEC
,	bis.marcaDupli
,	bis.zonaFinanciable
,	bis.gpsNuevaClas
,	bis.zonaRegion
FROM staging.tmp_ind_segm AS bis
LEFT JOIN {{MASTER_DB}}.dwh_ind_segm AS sis
  ON bis.cosechaFecha = sis.cosechaFecha
WHERE sis.cosechaFecha IS NULL;""")

# Block 15
run(f"""--Se eliminan las tablas empleadas para la inserción de tablas full
DROP TABLE IF EXISTS staging.tmp_facturacion_tdc_historico;
DROP TABLE IF EXISTS staging.tmp_ind_segm;""")

job.commit()