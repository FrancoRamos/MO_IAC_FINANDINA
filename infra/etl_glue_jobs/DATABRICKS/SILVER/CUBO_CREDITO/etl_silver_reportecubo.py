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
spark.conf.set('spark.sql.defaultCatalog', 'glue_catalog')
spark.conf.set('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')
spark.conf.set('spark.sql.catalog.glue_catalog.warehouse', args['warehouse_s3_uri'])
spark.conf.set('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
spark.conf.set('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')

RAW_DB = args['raw_db']
MASTER_DB = args['master_db']
STAGING_DB = args['staging_db']
FECHA_REPORTE = args['FechaReporte']  # yyyy-MM-dd

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Optional: internal libs packaged as .whl/.zip and attached to Glue job
# from your_internal_lib.io import read_table, write_table

# Preprocess SQL text to remap schema names from Databricks (bronze/silver/staging) to Glue DBs
_schema_patterns = [
    (re.compile(r'\bbronze\.', re.IGNORECASE), f'{RAW_DB}.'),
    (re.compile(r'\bsilver\.', re.IGNORECASE), f'{MASTER_DB}.'),
    (re.compile(r'\bstaging\.', re.IGNORECASE), f'{STAGING_DB}.'),
]

def _remap_schemas(sql: str) -> str:
    out = sql
    for pat, repl in _schema_patterns:
        out = pat.sub(repl, out)
    # Simple parameter expansion commonly seen in notebooks
    out = out.replace('${FechaReporte}', FECHA_REPORTE)
    return out

def run_sql(sql: str):
    sql2 = _remap_schemas(sql)
    print('\n=== Executing SQL ===\n' + sql2[:4000] + ('...' if len(sql2) > 4000 else ''))
    return spark.sql(sql2)

# Ensure DBs exist (idempotent)
spark.sql(f"CREATE DATABASE IF NOT EXISTS {RAW_DB}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {MASTER_DB}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {STAGING_DB}")

# Example name remapping in comments:
#   bronze.some_table  -> {RAW_DB}.some_table
#   silver.some_table  -> {MASTER_DB}.some_table
#   staging.tmp_table  -> {STAGING_DB}.tmp_table



# ---- Notebook Cell 0 (Python) ----
#El widget contiene la fecha de reporte en formato yyyyMMdd llamada = FechaReporte
#dbutils.widgets.text("FechaReporte", "","")
#FechaReporte = dbutils.widgets.get("FechaReporte")
print(f"El valor de FechaReporte es: {FechaReporte}")


# ---- Notebook Cell 1 (SQL) ----
run_sql("""
-- Vista temporal para los registros a insertar
CREATE OR REPLACE TEMP VIEW vw_fechaAltura AS
SELECT 
    TRUNC(TO_DATE(CAST('${FechaReporte}' AS STRING), 'yyyy-MM-dd'), 'MM') AS mes,
    EXPLODE(SEQUENCE(1, 73)) AS altura;


-- Eliminar registros existentes que coincidan con los de vw_fechaAltura
DELETE FROM bronze.finandinacartera_jd_fechas
WHERE EXISTS (
    SELECT 1
    FROM vw_fechaAltura fa
    WHERE fa.mes = bronze.finandinacartera_jd_fechas.mes
      AND fa.altura = bronze.finandinacartera_jd_fechas.altura
);

-- Insertar nuevos registros
INSERT INTO bronze.finandinacartera_jd_fechas
SELECT
    mes,
    altura
FROM vw_fechaAltura
WHERE NOT EXISTS (
    SELECT 1
    FROM bronze.finandinacartera_jd_fechas f
    WHERE f.mes = vw_fechaAltura.mes
      AND f.altura = vw_fechaAltura.altura
);
""")


# ---- Notebook Cell 2 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_jd_fechas
 AS
  SELECT DISTINCT
	to_date(MES,'yyyy-MM-dd') AS mes
,	altura
FROM bronze.finandinacartera_jd_fechas;
""")


# ---- Notebook Cell 3 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_jd_fechas;

INSERT INTO silver.dwh_jd_fechas
(
    mes
  , altura
)
  SELECT DISTINCT
	bjf.mes
,	bjf.altura
FROM staging.tmp_jd_fechas AS bjf
""")


# ---- Notebook Cell 4 (SQL) ----
run_sql("""
INSERT INTO bronze.finandinacartera_jd_cosechas_over
SELECT 
     SG_Obligacion
   , C.altura
   , date_add(SG_VrInicial_Fecha_Desembolso, 1 - day(SG_VrInicial_Fecha_Desembolso)) AS cosecha
   , SG_VrInicial_VrDesembolso AS monto
FROM 
    silver.reporte_desembolsos_completa A
LEFT JOIN silver.dwh_007_cierresmensuales B 
	ON A.SG_Obligacion = B.Obligacion 
	AND B.Cierre >= DATE_FORMAT(ADD_MONTHS(TO_DATE(CAST('${FechaReporte}' AS STRING), 'yyyy-MM-dd'), -1), 'yyyyMM')
LEFT JOIN bronze.finandinacartera_jd_fechas C 
	ON date_add(SG_VrInicial_Fecha_Desembolso, 1 - day(SG_VrInicial_Fecha_Desembolso)) = C.mes
WHERE 
    SG_VrInicial_Fecha_Desembolso >= DATE_FORMAT(TRUNC(ADD_MONTHS(TO_DATE(CAST('${FechaReporte}' AS STRING), 'yyyy-MM-dd'), -1), 'MM'), 'yyyy-MM-dd')
    AND month(SG_VrInicial_Fecha_Desembolso) = month(TO_DATE(CONCAT(CAST(B.cierre AS STRING), '01'), 'yyyyMMdd'))
    AND year(SG_VrInicial_Fecha_Desembolso) = year(TO_DATE(CONCAT(CAST(B.cierre AS STRING), '01'), 'yyyyMMdd'))
    AND (estadoContable = 'A. Balance' OR estadoContable IS NULL)
GROUP BY 
    SG_Obligacion
  , C.Altura
  , date_add(SG_VrInicial_Fecha_Desembolso, 1 - day(SG_VrInicial_Fecha_Desembolso))
  , SG_VrInicial_VrDesembolso
ORDER BY 
    C.Altura, SG_Obligacion;
""")


# ---- Notebook Cell 5 (SQL) ----
run_sql("""
--Se crea vista Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TEMP VIEW vw_tmp_jd_cosechas_over
 AS
  SELECT DISTINCT
	SG_Obligacion AS SGObligacion
,	altura
,	to_date(Cosecha,'yyyy-MM-dd') AS cosecha
,	CAST(REPLACE(TRIM(Monto),' ', '') AS DECIMAL(18,2)) AS monto
FROM bronze.finandinacartera_jd_cosechas_over
WHERE cosecha >= TRUNC(TO_DATE(CAST('${FechaReporte}' AS STRING), 'yyyy-MM-dd'), 'MM');
""")


# ---- Notebook Cell 6 (SQL) ----
run_sql("""
--Se eliminan de Tabla Silver los registros que existan en la tabla bronze (Campopivot(cosecha)) para luego ser insertadas:
DELETE FROM silver.dwh_jd_cosechas_over
WHERE cosecha IN ( SELECT DISTINCT cosecha
            FROM vw_tmp_jd_cosechas_over
                      );
""")


# ---- Notebook Cell 7 (SQL) ----
run_sql("""
--Se insertan los nuevos registros por (Campopivot(cosecha))
INSERT INTO silver.dwh_jd_cosechas_over
(
	SGObligacion
,	altura
,	cosecha
,	monto
)
  SELECT DISTINCT
	bco.SGObligacion
,	bco.altura
,	bco.cosecha
,	bco.monto
FROM vw_tmp_jd_cosechas_over AS bco
LEFT JOIN silver.dwh_jd_cosechas_over AS sco
  ON bco.cosecha = sco.cosecha
WHERE sco.cosecha IS NULL;
""")


# ---- Notebook Cell 8 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_ende
 AS
  SELECT DISTINCT
  Obligacion AS obligacion
, Identificacion AS identificacion
, TRIM(Digito_Chequeo) AS digitoChequeo
, Tipo_Identificacion AS tipoIdentificacion
, UPPER(TRIM(Primer_Apellido)) AS primerApellido
, UPPER(TRIM(Segundo_Apellido)) AS segundoApellido
, UPPER(TRIM(Nombre)) AS nombre
, UPPER(TRIM(Cliente)) AS cliente
, Codigo_CIIU AS codigoCIIU
, Naturaleza_Juridica AS naturalezaJuridica
, Referencia AS referencia
, Concurso_Acredores AS concursoAcredores
, Acuerdo_Informal AS acuerdoInformal
, Indicador_Reestructurado AS indicadorReestructurado
, TRIM(Departamento) AS departamento
, Sucursal AS sucursal
, Tipo_Credito AS tipoCredito
, CAST(REPLACE(TRIM(Valor_Activo),' ', '') AS DECIMAL(18,2)) AS valorActivo
, CAST(REPLACE(TRIM(Valor_Credito),' ', '') AS DECIMAL(18,2)) AS valorCredito
, CAST(REPLACE(TRIM(Valor_Anticipo),' ', '') AS DECIMAL(18,2)) AS valorAnticipo
, CAST(REPLACE(TRIM(Opcion_Compra),' ', '') AS DECIMAL(18,2)) AS opcionCompra
, to_date(Fecha_Inicio,'yyyy-MM-dd') AS fechaInicio
, to_date(Fecha_Final,'yyyy-MM-dd') AS fechaFinal
, CAST(REPLACE(TRIM(TasaEfectiva_Inicial),' ', '') AS DECIMAL(18,4)) AS tasaEfectivaInicial
, CAST(REPLACE(TRIM(TasaEfectiva_Actual),' ', '') AS DECIMAL(18,4)) AS tasaEfectivaActual
, Tipo_Garantia AS tipoGarantia
, Garantia AS garantia
, CAST(REPLACE(TRIM(Valor_Garantias),' ', '') AS DECIMAL(18,2)) AS valorGarantias
, to_date(FechaAvaluo_Garantia,'yyyy-MM-dd') AS fechaAvaluoGarantia
, CAST(REPLACE(TRIM(Clase_Bien),' ', '') AS DECIMAL(18,2)) AS claseBien
, CAST(REPLACE(TRIM(Prima_AmortizacionAcumulada),' ', '') AS DECIMAL(18,2)) AS primaAmortizacionAcumulada
, CAST(REPLACE(TRIM(Valor_SaldoCapital),' ', '') AS DECIMAL(18,2)) AS valorSaldoCapital
, to_date(Fecha_Inicio_Mora,'yyyy-MM-dd') AS fechaInicioMora
, Dias_Mora AS diasMora
, Clasificacion_Anterior AS clasificacionAnterior
, Clasificacion_Nueva AS clasificacionNueva
, Subclase AS subclase
, TRIM(Uso_Garantia) AS usoGarantia
, TRIM(Calificacion_Automatica) AS calificacionAutomatica
, TRIM(Calificacion_Manual) AS calificacionManual
, TRIM(ENCALMD) AS ENCALMD
, TRIM(Calificacion_homologa) AS calificacionHomologa
, CAST(REPLACE(TRIM(Valor_Capital),' ', '') AS DECIMAL(18,2)) AS valorCapital
, CAST(REPLACE(TRIM(Valor_Interes),' ', '') AS DECIMAL(18,2)) AS valorInteres
, CAST(REPLACE(TRIM(Valor_Seguro_Auto),' ', '') AS DECIMAL(18,2)) AS valorSeguroAuto
, CAST(REPLACE(TRIM(Seguro_Vida),' ', '') AS DECIMAL(18,2)) AS seguroVida
, CAST(REPLACE(TRIM(Seguro_Desembolso),' ', '') AS DECIMAL(18,2)) AS seguroDesembolso
, CAST(REPLACE(TRIM(ENSEGSOA),' ', '') AS DECIMAL(18,2)) AS ENSEGSOA
, CAST(REPLACE(TRIM(ValorOtroSeguro),' ', '') AS DECIMAL(18,2)) AS valorOtroSeguro
, CAST(REPLACE(TRIM(Cargo1),' ', '') AS DECIMAL(18,2)) AS cargo1
, CAST(REPLACE(TRIM(Cargo2),' ', '') AS DECIMAL(18,2)) AS cargo2
, CAST(REPLACE(TRIM(Cargo3),' ', '') AS DECIMAL(18,2)) AS cargo3
, CAST(REPLACE(TRIM(ENUSOFU1),' ', '') AS DECIMAL(18,2)) AS ENUSOFU1
, CAST(REPLACE(TRIM(Interes_Mora),' ', '') AS DECIMAL(18,2)) AS interesMora
, CAST(REPLACE(TRIM(Capital_Vencido),' ', '') AS DECIMAL(18,2)) AS capitalVencido
, CAST(REPLACE(TRIM(Interes_Vencido),' ', '') AS DECIMAL(18,2)) AS interesVencido
, CAST(REPLACE(TRIM(Seguro_Auto_Vencido),' ', '') AS DECIMAL(18,2)) AS seguroAutoVencido
, CAST(REPLACE(TRIM(Seguro_Vida_Vencido),' ', '') AS DECIMAL(18,2)) AS seguroVidaVencido
, CAST(REPLACE(TRIM(Seguro_DesembolsoVencido),' ', '') AS DECIMAL(18,2)) AS seguroDesembolsoVencido
, CAST(REPLACE(TRIM(ENSOAVEN),' ', '') AS DECIMAL(18,2)) AS ENSOAVEN
, CAST(REPLACE(TRIM(OtrosSeguros_Vencidos),' ', '') AS DECIMAL(18,2)) AS otrosSegurosVencidos
, CAST(REPLACE(TRIM(Cargo1_Vencido),' ', '') AS DECIMAL(18,2)) AS cargo1Vencido
, CAST(REPLACE(TRIM(Cargo2_Vencido),' ', '') AS DECIMAL(18,2)) AS cargo2Vencido
, CAST(REPLACE(TRIM(Cargo3_Vencido),' ', '') AS DECIMAL(18,2)) AS cargo3Vencido
, CAST(REPLACE(TRIM(ENUSOFU2),' ', '') AS DECIMAL(18,2)) AS ENUSOFU2
, CAST(REPLACE(TRIM(Interes_Causado),' ', '') AS DECIMAL(18,2)) AS interesCausado
, CAST(REPLACE(TRIM(Interes_NoAcumulado),' ', '') AS DECIMAL(18,2)) AS interesNoAcumulado
, CAST(REPLACE(TRIM(Capital_Causado),' ', '') AS DECIMAL(18,2)) AS capitalCausado
, CAST(REPLACE(TRIM(Capital_NoAcumulacion),' ', '') AS DECIMAL(18,2)) AS capitalNoAcumulacion
, CAST(REPLACE(TRIM(Causacion_Mes),' ', '') AS DECIMAL(18,2)) AS causacionMes
, CAST(REPLACE(TRIM(Causado_SeguroVida),' ', '') AS DECIMAL(18,2)) AS causadoSeguroVida
, CAST(REPLACE(TRIM(Pagado_SeguroVida),' ', '') AS DECIMAL(18,2)) AS pagadoSeguroVida
, CAST(REPLACE(TRIM(Causado_SeguroAuto),' ', '') AS DECIMAL(18,2)) AS causadoSeguroAuto
, CAST(REPLACE(TRIM(Pagado_SeguroAuto),' ', '') AS DECIMAL(18,2)) AS pagadoSeguroAuto
, CAST(REPLACE(TRIM(SeguroVida_Causado),' ', '') AS DECIMAL(18,2)) AS seguroVidaCausado
, CAST(REPLACE(TRIM(Desembolso_Pagado),' ', '') AS DECIMAL(18,2)) AS desembolsoPagado
, CAST(REPLACE(TRIM(ENCAUSOA),' ', '') AS DECIMAL(18,2)) AS ENCAUSOA
, CAST(REPLACE(TRIM(ENPAGSOA),' ', '') AS DECIMAL(18,2)) AS ENPAGSOA
, CAST(REPLACE(TRIM(ENUSOFU3),' ', '') AS DECIMAL(18,2)) AS ENUSOFU3
, CAST(REPLACE(TRIM(ENCXCSOA),' ', '') AS DECIMAL(18,2)) AS ENCXCSOA
, CAST(REPLACE(TRIM(SeguroVida_CausadoOtro),' ', '') AS DECIMAL(18,2)) AS seguroVidaCausadoOtro
, CAST(REPLACE(TRIM(SeguroVida_Pagado),' ', '') AS DECIMAL(18,2)) AS seguroVidaPagado
, CAST(REPLACE(TRIM(InteresMora_Pagado),' ', '') AS DECIMAL(18,2)) AS interesMoraPagado
, CAST(REPLACE(TRIM(ENUSOFU4),' ', '') AS DECIMAL(18,2)) AS ENUSOFU4
, TRIM(Nivel_Riesgo) AS nivelRiesgo
, CAST(REPLACE(TRIM(Probabilidad_Impago),' ', '') AS DECIMAL(18,9)) AS probabilidadImpago
, TRIM(Calificacion_ModeloReferencia) AS calificacionModeloReferencia
, CAST(REPLACE(TRIM(ProbIncumplimiento_ModeloRef),' ', '') AS DECIMAL(18,7)) AS probIncumplimientoModeloRef
, CAST(REPLACE(TRIM(ENPROIMPM2),' ', '') AS DECIMAL(18,7)) AS ENPROIMPM2
, CAST(REPLACE(TRIM(TasaDescuentoEA),' ', '') AS DECIMAL(18,7)) AS tasaDescuentoEA 
, CAST(REPLACE(TRIM(FactorPDI),' ', '') AS DECIMAL(18,7)) AS factorPDI 
, CAST(REPLACE(TRIM(ProvisionCapital),' ', '') AS DECIMAL(18,2)) AS provisionCapital 
, CAST(REPLACE(TRIM(ProvisionOtros),' ', '') AS DECIMAL(18,2)) AS provisionOtros 
, CAST(REPLACE(TRIM(ProvisionAnticipada),' ', '') AS DECIMAL(18,2)) AS provisionAnticipada 
, CAST(REPLACE(TRIM(ProvisionAnticipadaOtros),' ', '') AS DECIMAL(18,2)) AS provisionAnticipadaOtros 
, CAST(REPLACE(TRIM(Provision_Capital),' ', '') AS DECIMAL(18,2)) AS provisionCapital1 
, CAST(REPLACE(TRIM(Provision_K1),' ', '') AS DECIMAL(18,2)) AS provisionK1 
, CAST(REPLACE(TRIM(ENPVK2),' ', '') AS DECIMAL(18,2)) AS ENPVK2 
, CAST(REPLACE(TRIM(ENPVKSEG),' ', '') AS DECIMAL(18,2)) AS ENPVKSEG 
, CAST(REPLACE(TRIM(Provision_Interes),' ', '') AS DECIMAL(18,2)) AS provisionInteres 
, CAST(REPLACE(TRIM(ENPVINGE),' ', '') AS DECIMAL(18,2)) AS ENPVINGE 
, CAST(REPLACE(TRIM(Provi_Seguro),' ', '') AS DECIMAL(18,2)) AS proviSeguro 
, CAST(REPLACE(TRIM(Provi_Otros),' ', '') AS DECIMAL(18,2)) AS proviOtros 
, CAST(REPLACE(TRIM(ENUSOFU5),' ', '') AS DECIMAL(18,2)) AS ENUSOFU5
, CAST(REPLACE(TRIM(Provi_Paga_AnioCapital),' ', '') AS DECIMAL(18,2)) AS proviPagaAnioCapital 
, CAST(REPLACE(TRIM(Provi_Paga_AnioInteres),' ', '') AS DECIMAL(18,2)) AS proviPagaAnioInteres 
, CAST(REPLACE(TRIM(Provi_Paga_AnioSeguro),' ', '') AS DECIMAL(18,2)) AS proviPagaAnioSeguro 
, CAST(REPLACE(TRIM(Provi_Paga_AnioOtro),' ', '') AS DECIMAL(18,2)) AS proviPagaAnioOtro 
, CAST(REPLACE(TRIM(RecK1_PagaMes),' ', '') AS DECIMAL(18,2)) AS reck1PagaMes 
, CAST(REPLACE(TRIM(Provi_Paga_MesCapital),' ', '') AS DECIMAL(18,2)) AS proviPagaMesCapital 
, CAST(REPLACE(TRIM(Provi_Paga_MesInteres),' ', '') AS DECIMAL(18,2)) AS proviPagaMesInteres 
, CAST(REPLACE(TRIM(Provi_Paga_MesSeguro),' ', '') AS DECIMAL(18,2)) AS proviPagaMesSeguro 
, CAST(REPLACE(TRIM(Provi_Paga_MesOtro),' ', '') AS DECIMAL(18,2)) AS proviPagaMesOtro 
, CAST(REPLACE(TRIM(Recuperado_Capital),' ', '') AS DECIMAL(18,2)) AS recuperadoCapital 
, CAST(REPLACE(TRIM(Recuperado_Interes),' ', '') AS DECIMAL(18,2)) AS recuperadoInteres 
, CAST(REPLACE(TRIM(Recuperado_Seguros),' ', '') AS DECIMAL(18,2)) AS recuperadoSeguros 
, CAST(REPLACE(TRIM(Recuperado_Otros),' ', '') AS DECIMAL(18,2)) AS recuperadoOtros 
, CAST(REPLACE(TRIM(ENUSOFU6),' ', '') AS DECIMAL(18,2)) AS ENUSOFU6 
, CAST(REPLACE(TRIM(Valor_Contingencia),' ', '') AS DECIMAL(18,2)) AS valorContingencia 
, CAST(REPLACE(TRIM(Canon_Corriente),' ', '') AS DECIMAL(18,2)) AS canonCorriente 
, CAST(REPLACE(TRIM(Canon_NoCorriente),' ', '') AS DECIMAL(18,2)) AS canonNoCorriente 
, CAST(REPLACE(TRIM(Opcion_Corriente),' ', '') AS DECIMAL(18,2)) AS opcionCorriente 
, CAST(REPLACE(TRIM(Opcion_NoCorriente),' ', '') AS DECIMAL(18,2)) AS opcionNoCorriente 
, CAST(REPLACE(TRIM(Proyeccion_Capital_Siguientes3Meses),' ', '') AS DECIMAL(18,2)) AS proyeccionCapitalSig3Meses 
, CAST(REPLACE(TRIM(Proyeccion_Interes_Siguientes3Meses),' ', '') AS DECIMAL(18,2)) AS proyeccionInteresSig3Meses 
, CAST(REPLACE(TRIM(Capital_Pagado_Ultimos3Meses),' ', '') AS DECIMAL(18,2)) AS capPagadoUlt3Meses 
, CAST(REPLACE(TRIM(Interes_Pagado_Ultimos_3Meses),' ', '') AS DECIMAL(18,2)) AS intPagadoUlt3Meses 
, TRIM(MoraActualB) AS moraActualB 
, TRIM(MoraActualC) AS moraActualC 
, TRIM(MoraAlta_UltTrim) AS moraAltaUltTrim 
, TRIM(MoraAlta_PenulTrim) AS moraAltaPenulTrim 
, TRIM(MoraAlta_AntepeTrim) AS moraAltaAntepeTrim 
, TRIM(ComportamientoAnualRegular) AS comportamientoAnualRegular 
, TRIM(ComportamientoAnualMalo) AS comportamientoAnualMalo 
, TRIM(MoraMaximaB) AS moraMaximaB 
, TRIM(MoraMaximaC) AS moraMaximaC 
, TRIM(MoraMaximaD) AS moraMaximaD 
, TRIM(RangoMoraMaxUlti3Anios) AS rangoMoraMaxUlti3Anios 
, TRIM(IndicadorGarantia) AS indicadorGarantia 
, CAST(REPLACE(TRIM(ValorZ),' ', '') AS DECIMAL(18,6)) AS valorZ 
, CAST(REPLACE(TRIM(Puntaje),' ', '') AS DECIMAL(18,6)) AS puntaje 
, TRIM(ENUSOFU7) AS ENUSOFU7 
, TRIM(ENUSOFU8) AS ENUSOFU8 
, TRIM(Cierre) AS cierre
FROM bronze.finandinacartera_ende;
""")


# ---- Notebook Cell 9 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_ende;

INSERT INTO silver.dwh_ende
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
,	ENCALMD
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
,	ENUSOFU1
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
,	ENUSOFU2
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
,	ENUSOFU3
,	ENCXCSOA
,	seguroVidaCausadoOtro
,	seguroVidaPagado
,	interesMoraPagado
,	ENUSOFU4
,	nivelRiesgo
,	probabilidadImpago
,	calificacionModeloReferencia
,	probIncumplimientoModeloRef
,	ENPROIMPM2
,	tasaDescuentoEA
,	factorPDI
,	provisionCapital
,	provisionOtros
,	provisionAnticipada
,	provisionAnticipadaOtros
,	provisionCapital1
,	provisionK1
,	ENPVK2
,	ENPVKSEG
,	provisionInteres
,	ENPVINGE
,	proviSeguro
,	proviOtros
,	ENUSOFU5
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
,	ENUSOFU6
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
,	ENUSOFU7
,	ENUSOFU8
,	cierre
)
  SELECT DISTINCT
	be.obligacion
,	be.identificacion
,	be.digitoChequeo
,	be.tipoIdentificacion
,	be.primerApellido
,	be.segundoApellido
,	be.nombre
,	be.cliente
,	be.codigoCIIU
,	be.naturalezaJuridica
,	be.referencia
,	be.concursoAcredores
,	be.acuerdoInformal
,	be.indicadorReestructurado
,	be.departamento
,	be.sucursal
,	be.tipoCredito
,	be.valorActivo
,	be.valorCredito
,	be.valorAnticipo
,	be.opcionCompra
,	be.fechaInicio
,	be.fechaFinal
,	be.tasaEfectivaInicial
,	be.tasaEfectivaActual
,	be.tipoGarantia
,	be.garantia
,	be.valorGarantias
,	be.fechaAvaluoGarantia
,	be.claseBien
,	be.primaAmortizacionAcumulada
,	be.valorSaldoCapital
,	be.fechaInicioMora
,	be.diasMora
,	be.clasificacionAnterior
,	be.clasificacionNueva
,	be.subclase
,	be.usoGarantia
,	be.calificacionAutomatica
,	be.calificacionManual
,	be.ENCALMD
,	be.calificacionHomologa
,	be.valorCapital
,	be.valorInteres
,	be.valorSeguroAuto
,	be.seguroVida
,	be.seguroDesembolso
,	be.ENSEGSOA
,	be.valorOtroSeguro
,	be.cargo1
,	be.cargo2
,	be.cargo3
,	be.ENUSOFU1
,	be.interesMora
,	be.capitalVencido
,	be.interesVencido
,	be.seguroAutoVencido
,	be.seguroVidaVencido
,	be.seguroDesembolsoVencido
,	be.ENSOAVEN
,	be.otrosSegurosVencidos
,	be.cargo1Vencido
,	be.cargo2Vencido
,	be.cargo3Vencido
,	be.ENUSOFU2
,	be.interesCausado
,	be.interesNoAcumulado
,	be.capitalCausado
,	be.capitalNoAcumulacion
,	be.causacionMes
,	be.causadoSeguroVida
,	be.pagadoSeguroVida
,	be.causadoSeguroAuto
,	be.pagadoSeguroAuto
,	be.seguroVidaCausado
,	be.desembolsoPagado
,	be.ENCAUSOA
,	be.ENPAGSOA
,	be.ENUSOFU3
,	be.ENCXCSOA
,	be.seguroVidaCausadoOtro
,	be.seguroVidaPagado
,	be.interesMoraPagado
,	be.ENUSOFU4
,	be.nivelRiesgo
,	be.probabilidadImpago
,	be.calificacionModeloReferencia
,	be.probIncumplimientoModeloRef
,	be.ENPROIMPM2
,	be.tasaDescuentoEA
,	be.factorPDI
,	be.provisionCapital
,	be.provisionOtros
,	be.provisionAnticipada
,	be.provisionAnticipadaOtros
,	be.provisionCapital1
,	be.provisionK1
,	be.ENPVK2
,	be.ENPVKSEG
,	be.provisionInteres
,	be.ENPVINGE
,	be.proviSeguro
,	be.proviOtros
,	be.ENUSOFU5
,	be.proviPagaAnioCapital
,	be.proviPagaAnioInteres
,	be.proviPagaAnioSeguro
,	be.proviPagaAnioOtro
,	be.reck1PagaMes
,	be.proviPagaMesCapital
,	be.proviPagaMesInteres
,	be.proviPagaMesSeguro
,	be.proviPagaMesOtro
,	be.recuperadoCapital
,	be.recuperadoInteres
,	be.recuperadoSeguros
,	be.recuperadoOtros
,	be.ENUSOFU6
,	be.valorContingencia
,	be.canonCorriente
,	be.canonNoCorriente
,	be.opcionCorriente
,	be.opcionNoCorriente
,	be.proyeccionCapitalSig3Meses
,	be.proyeccionInteresSig3Meses
,	be.capPagadoUlt3Meses
,	be.intPagadoUlt3Meses
,	be.moraActualB
,	be.moraActualC
,	be.moraAltaUltTrim
,	be.moraAltaPenulTrim
,	be.moraAltaAntepeTrim
,	be.comportamientoAnualRegular
,	be.comportamientoAnualMalo
,	be.moraMaximaB
,	be.moraMaximaC
,	be.moraMaximaD
,	be.rangoMoraMaxUlti3Anios
,	be.indicadorGarantia
,	be.valorZ
,	be.puntaje
,	be.ENUSOFU7
,	be.ENUSOFU8
,	be.cierre
FROM staging.tmp_ende AS be
""")


# ---- Notebook Cell 10 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_endeot
 AS
  SELECT DISTINCT
	ETNRCR
,	ETNRID
,	TRIM(ETDIGC) AS ETDIGC
,	ETTPID
,	UPPER(TRIM(ETPRAP)) AS ETPRAP
,	UPPER(TRIM(ETSEAP)) AS ETSEAP
,	UPPER(TRIM(ETNMCL)) AS ETNMCL
,	UPPER(TRIM(ETNMRZ)) AS ETNMRZ
,	ETCIIU
,	ETNTJR
,	ETREFA
,	ETACRE
,	ETACIN
,	ETREES
,	ETDPTO
,	ETCDSC
,	ETTPCR
,	CAST(REPLACE(TRIM(ETVRAC),' ', '') AS DECIMAL(18,2)) AS ETVRAC
,	CAST(REPLACE(TRIM(ETVRCR),' ', '') AS DECIMAL(18,2)) AS ETVRCR
,	CAST(REPLACE(TRIM(ETVRAN),' ', '') AS DECIMAL(18,2)) AS ETVRAN
,	CAST(REPLACE(TRIM(ETVROP),' ', '') AS DECIMAL(18,2)) AS ETVROP
,	to_date(ETFCIN,'yyyy-MM-dd') AS ETFCIN
,	to_date(ETFCFI,'yyyy-MM-dd') AS ETFCFI
,	CAST(REPLACE(TRIM(ETTEAI),' ', '') AS DECIMAL(18,4)) AS ETTEAI
,	CAST(REPLACE(TRIM(ETTEAA),' ', '') AS DECIMAL(18,4)) AS ETTEAA
,	ETTPGT
,	ETLIGT
,	CAST(REPLACE(TRIM(ETVRGT),' ', '') AS DECIMAL(18,2)) AS ETVRGT
,	to_date(ETFAVAGT,'yyyy-MM-dd') AS ETFAVAGT
,	CAST(REPLACE(TRIM(ETPRCOCA),' ', '') AS DECIMAL(18,2)) AS ETPRCOCA
,	CAST(REPLACE(TRIM(ETPRAMHI),' ', '') AS DECIMAL(18,2)) AS ETPRAMHI
,	CAST(REPLACE(TRIM(ETVRSD),' ', '') AS DECIMAL(18,2)) AS ETVRSD
,	to_date(ETFCMR,'yyyy-MM-dd') AS ETFCMR
,	ETDSMR
,	ETCLAS
,	ETCLASN
,	ETSUBCL
,	TRIM(ETUSOG) AS ETUSOG
,	TRIM(ETCALA) AS ETCALA
,	TRIM(ETCALM) AS ETCALM
,	TRIM(ETCALMD) AS ETCALMD
,	TRIM(ETCALH) AS ETCALH
,	CAST(REPLACE(TRIM(ETVRCP),' ', '') AS DECIMAL(18,2)) AS ETVRCP
,	CAST(REPLACE(TRIM(ETVRIN),' ', '') AS DECIMAL(18,2)) AS ETVRIN
,	CAST(REPLACE(TRIM(ETSEGAUT),' ', '') AS DECIMAL(18,2)) AS ETSEGAUT
,	CAST(REPLACE(TRIM(ETSEGVID),' ', '') AS DECIMAL(18,2)) AS ETSEGVID
,	CAST(REPLACE(TRIM(ETSEGDES),' ', '') AS DECIMAL(18,2)) AS ETSEGDES
,	CAST(REPLACE(TRIM(ETSEGSOA),' ', '') AS DECIMAL(18,2)) AS ETSEGSOA
,	CAST(REPLACE(TRIM(ETSEGOTR),' ', '') AS DECIMAL(18,2)) AS ETSEGOTR
,	CAST(REPLACE(TRIM(ETCAR1),' ', '') AS DECIMAL(18,2)) AS ETCAR1
,	CAST(REPLACE(TRIM(ETCAR2),' ', '') AS DECIMAL(18,2)) AS ETCAR2
,	CAST(REPLACE(TRIM(ETCAR3),' ', '') AS DECIMAL(18,2)) AS ETCAR3
,	CAST(REPLACE(TRIM(ETUSOFU1),' ', '') AS DECIMAL(18,2)) AS ETUSOFU1
,	CAST(REPLACE(TRIM(ETINMR),' ', '') AS DECIMAL(18,2)) AS ETINMR
,	CAST(REPLACE(TRIM(ETCPVD),' ', '') AS DECIMAL(18,2)) AS ETCPVD
,	CAST(REPLACE(TRIM(ETINVD),' ', '') AS DECIMAL(18,2)) AS ETINVD
,	CAST(REPLACE(TRIM(ETAUTVEN),' ', '') AS DECIMAL(18,2)) AS ETAUTVEN
,	CAST(REPLACE(TRIM(ETVIDVEN),' ', '') AS DECIMAL(18,2)) AS ETVIDVEN
,	CAST(REPLACE(TRIM(ETDESVEN),' ', '') AS DECIMAL(18,2)) AS ETDESVEN
,	CAST(REPLACE(TRIM(ETSOAVEN),' ', '') AS DECIMAL(18,2)) AS ETSOAVEN
,	CAST(REPLACE(TRIM(ETOTRVEN),' ', '') AS DECIMAL(18,2)) AS ETOTRVEN
,	CAST(REPLACE(TRIM(ETCGVD),' ', '') AS DECIMAL(18,2)) AS ETCGVD
,	CAST(REPLACE(TRIM(ETC2VD),' ', '') AS DECIMAL(18,2)) AS ETC2VD
,	CAST(REPLACE(TRIM(ETC3VD),' ', '') AS DECIMAL(18,2)) AS ETC3VD
,	CAST(REPLACE(TRIM(ETUSOFU2),' ', '') AS DECIMAL(18,2)) AS ETUSOFU2
,	CAST(REPLACE(TRIM(ETINAC),' ', '') AS DECIMAL(18,2)) AS ETINAC
,	CAST(REPLACE(TRIM(ETINNA),' ', '') AS DECIMAL(18,5)) AS ETINNA
,	CAST(REPLACE(TRIM(ETKACU),' ', '') AS DECIMAL(18,2)) AS ETKACU
,	CAST(REPLACE(TRIM(ETKNAC),' ', '') AS DECIMAL(18,2)) AS ETKNAC
,	CAST(REPLACE(TRIM(ETCAUMES),' ', '') AS DECIMAL(18,2)) AS ETCAUMES
,	CAST(REPLACE(TRIM(ETCAUVID),' ', '') AS DECIMAL(18,2)) AS ETCAUVID
,	CAST(REPLACE(TRIM(ETPAGVID),' ', '') AS DECIMAL(18,2)) AS ETPAGVID
,	CAST(REPLACE(TRIM(ETCAUAUT),' ', '') AS DECIMAL(18,2)) AS ETCAUAUT
,	CAST(REPLACE(TRIM(ETPAGAUT),' ', '') AS DECIMAL(18,2)) AS ETPAGAUT
,	CAST(REPLACE(TRIM(ETCAUDES),' ', '') AS DECIMAL(18,2)) AS ETCAUDES
,	CAST(REPLACE(TRIM(ETPAGDES),' ', '') AS DECIMAL(18,2)) AS ETPAGDES
,	CAST(REPLACE(TRIM(ETCAUSOA),' ', '') AS DECIMAL(18,2)) AS ETCAUSOA
,	CAST(REPLACE(TRIM(ETPAGSOA),' ', '') AS DECIMAL(18,2)) AS ETPAGSOA
,	CAST(REPLACE(TRIM(ETUSOFU3),' ', '') AS DECIMAL(18,2)) AS ETUSOFU3
,	CAST(REPLACE(TRIM(ETCXCSOA),' ', '') AS DECIMAL(18,2)) AS ETCXCSOA
,	CAST(REPLACE(TRIM(ETCAUOTR),' ', '') AS DECIMAL(18,2)) AS ETCAUOTR
,	CAST(REPLACE(TRIM(ETPAGOTR),' ', '') AS DECIMAL(18,2)) AS ETPAGOTR
,	CAST(REPLACE(TRIM(ETPAGIMO),' ', '') AS DECIMAL(18,2)) AS ETPAGIMO
,	CAST(REPLACE(TRIM(ETUSOFU4),' ', '') AS DECIMAL(18,2)) AS ETUSOFU4
,	TRIM(ETNIVRIE) AS ETNIVRIE
,	CAST(REPLACE(TRIM(ETPROIMP),' ', '') AS DECIMAL(18,9)) AS ETPROIMP
,	TRIM(ETCALMR) AS ETCALMR
,	CAST(REPLACE(TRIM(ETPROIMPMR),' ', '') AS DECIMAL(18,7)) AS ETPROIMPMR
,	CAST(REPLACE(TRIM(ETPROIMPM2),' ', '') AS DECIMAL(18,7)) AS ETPROIMPM2
,	CAST(REPLACE(TRIM(ETTEADCTO),' ', '') AS DECIMAL(18,7)) AS ETTEADCTO
,	CAST(REPLACE(TRIM(ETFACPDI),' ', '') AS DECIMAL(18,7)) AS ETFACPDI
,	CAST(REPLACE(TRIM(ETPVCPMR),' ', '') AS DECIMAL(18,2)) AS ETPVCPMR
,	CAST(REPLACE(TRIM(ETPVOTMR),' ', '') AS DECIMAL(18,2)) AS ETPVOTMR
,	CAST(REPLACE(TRIM(ETPVACPMR),' ', '') AS DECIMAL(18,2)) AS ETPVACPMR
,	CAST(REPLACE(TRIM(ETPVAOTMR),' ', '') AS DECIMAL(18,2)) AS ETPVAOTMR
,	CAST(REPLACE(TRIM(ETPVCP),' ', '') AS DECIMAL(18,2)) AS ETPVCP
,	CAST(REPLACE(TRIM(ETPVK1),' ', '') AS DECIMAL(18,2)) AS ETPVK1
,	CAST(REPLACE(TRIM(ETPVK2),' ', '') AS DECIMAL(18,2)) AS ETPVK2
,	CAST(REPLACE(TRIM(ETPVKSEG),' ', '') AS DECIMAL(18,2)) AS ETPVKSEG
,	CAST(REPLACE(TRIM(ETPVIN),' ', '') AS DECIMAL(18,2)) AS ETPVIN
,	CAST(REPLACE(TRIM(ETPVINGE),' ', '') AS DECIMAL(18,2)) AS ETPVINGE
,	CAST(REPLACE(TRIM(ETPVSG),' ', '') AS DECIMAL(18,2)) AS ETPVSG
,	CAST(REPLACE(TRIM(ETPVOT),' ', '') AS DECIMAL(18,2)) AS ETPVOT
,	CAST(REPLACE(TRIM(ETUSOFU5),' ', '') AS DECIMAL(18,2)) AS ETUSOFU5
,	CAST(REPLACE(TRIM(ETPPACP),' ', '') AS DECIMAL(18,2)) AS ETPPACP
,	CAST(REPLACE(TRIM(ETPPAIN),' ', '') AS DECIMAL(18,2)) AS ETPPAIN
,	CAST(REPLACE(TRIM(ETPPASG),' ', '') AS DECIMAL(18,2)) AS ETPPASG
,	CAST(REPLACE(TRIM(ETPPAOT),' ', '') AS DECIMAL(18,2)) AS ETPPAOT
,	CAST(REPLACE(TRIM(ETPPMK1),' ', '') AS DECIMAL(18,2)) AS ETPPMK1
,	CAST(REPLACE(TRIM(ETPPMCP),' ', '') AS DECIMAL(18,2)) AS ETPPMCP
,	CAST(REPLACE(TRIM(ETPPMIN),' ', '') AS DECIMAL(18,2)) AS ETPPMIN
,	CAST(REPLACE(TRIM(ETPPMSG),' ', '') AS DECIMAL(18,2)) AS ETPPMSG
,	CAST(REPLACE(TRIM(ETPPMOT),' ', '') AS DECIMAL(18,2)) AS ETPPMOT
,	CAST(REPLACE(TRIM(ETRPCP),' ', '') AS DECIMAL(18,2)) AS ETRPCP
,	CAST(REPLACE(TRIM(ETRPIN),' ', '') AS DECIMAL(18,2)) AS ETRPIN
,	CAST(REPLACE(TRIM(ETRPSG),' ', '') AS DECIMAL(18,2)) AS ETRPSG
,	CAST(REPLACE(TRIM(ETRPOT),' ', '') AS DECIMAL(18,2)) AS ETRPOT
,	CAST(REPLACE(TRIM(ETUSOFU6),' ', '') AS DECIMAL(18,2)) AS ETUSOFU6
,	CAST(REPLACE(TRIM(ETVRCT),' ', '') AS DECIMAL(18,2)) AS ETVRCT
,	CAST(REPLACE(TRIM(ETCANCOR),' ', '') AS DECIMAL(18,2)) AS ETCANCOR
,	CAST(REPLACE(TRIM(ETCNNOCO),' ', '') AS DECIMAL(18,2)) AS ETCNNOCO
,	CAST(REPLACE(TRIM(ETOPCCOR),' ', '') AS DECIMAL(18,2)) AS ETOPCCOR
,	CAST(REPLACE(TRIM(ETOPNOCO),' ', '') AS DECIMAL(18,2)) AS ETOPNOCO
,	CAST(REPLACE(TRIM(ETPKSGT3),' ', '') AS DECIMAL(18,2)) AS ETPKSGT3
,	CAST(REPLACE(TRIM(ETPISGT3),' ', '') AS DECIMAL(18,2)) AS ETPISGT3
,	CAST(REPLACE(TRIM(ETKPAU3M),' ', '') AS DECIMAL(18,2)) AS ETKPAU3M
,	CAST(REPLACE(TRIM(ETIPAU3M),' ', '') AS DECIMAL(18,2)) AS ETIPAU3M
,	ET_AMB AS ETAMB
,	ET_AMC AS ETAMC
,	ET_RAMUT AS ETRAMUT 
,	ET_RAMPT AS ETRAMPT
,	ET_RAMAT AS ETRAMAT
,	ET_CAR AS ETCAR
,	ET_CAM AS ETCAM
,	ET_MMB AS ETMMB
,	ET_MMC AS ETMMC
,	ET_MMD AS ETMMD
,	ET_RMM3 AS ETRMM3
,	ET_GNI AS ETGNI
,	CAST(REPLACE(TRIM(ET_Z),' ', '') AS DECIMAL(18,6)) AS ETZ
,	CAST(REPLACE(TRIM(ET_PTJE),' ', '') AS DECIMAL(18,6)) AS ETPTJE
,	ETUSOFU7
,	TRIM(ETUSOFU8) AS ETUSOFU8
FROM bronze.finandinacartera_endeot;
""")


# ---- Notebook Cell 11 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_endeot;

INSERT INTO silver.dwh_endeot
(
	ETNRCR
,	ETNRID
,	ETDIGC
,	ETTPID
,	ETPRAP
,	ETSEAP
,	ETNMCL
,	ETNMRZ
,	ETCIIU
,	ETNTJR
,	ETREFA
,	ETACRE
,	ETACIN
,	ETREES
,	ETDPTO
,	ETCDSC
,	ETTPCR
,	ETVRAC
,	ETVRCR
,	ETVRAN
,	ETVROP
,	ETFCIN
,	ETFCFI
,	ETTEAI
,	ETTEAA
,	ETTPGT
,	ETLIGT
,	ETVRGT
,	ETFAVAGT
,	ETPRCOCA
,	ETPRAMHI
,	ETVRSD
,	ETFCMR
,	ETDSMR
,	ETCLAS
,	ETCLASN
,	ETSUBCL
,	ETUSOG
,	ETCALA
,	ETCALM
,	ETCALMD
,	ETCALH
,	ETVRCP
,	ETVRIN
,	ETSEGAUT
,	ETSEGVID
,	ETSEGDES
,	ETSEGSOA
,	ETSEGOTR
,	ETCAR1
,	ETCAR2
,	ETCAR3
,	ETUSOFU1
,	ETINMR
,	ETCPVD
,	ETINVD
,	ETAUTVEN
,	ETVIDVEN
,	ETDESVEN
,	ETSOAVEN
,	ETOTRVEN
,	ETCGVD
,	ETC2VD
,	ETC3VD
,	ETUSOFU2
,	ETINAC
,	ETINNA
,	ETKACU
,	ETKNAC
,	ETCAUMES
,	ETCAUVID
,	ETPAGVID
,	ETCAUAUT
,	ETPAGAUT
,	ETCAUDES
,	ETPAGDES
,	ETCAUSOA
,	ETPAGSOA
,	ETUSOFU3
,	ETCXCSOA
,	ETCAUOTR
,	ETPAGOTR
,	ETPAGIMO
,	ETUSOFU4
,	ETNIVRIE
,	ETPROIMP
,	ETCALMR
,	ETPROIMPMR
,	ETPROIMPM2
,	ETTEADCTO
,	ETFACPDI
,	ETPVCPMR
,	ETPVOTMR
,	ETPVACPMR
,	ETPVAOTMR
,	ETPVCP
,	ETPVK1
,	ETPVK2
,	ETPVKSEG
,	ETPVIN
,	ETPVINGE
,	ETPVSG
,	ETPVOT
,	ETUSOFU5
,	ETPPACP
,	ETPPAIN
,	ETPPASG
,	ETPPAOT
,	ETPPMK1
,	ETPPMCP
,	ETPPMIN
,	ETPPMSG
,	ETPPMOT
,	ETRPCP
,	ETRPIN
,	ETRPSG
,	ETRPOT
,	ETUSOFU6
,	ETVRCT
,	ETCANCOR
,	ETCNNOCO
,	ETOPCCOR
,	ETOPNOCO
,	ETPKSGT3
,	ETPISGT3
,	ETKPAU3M
,	ETIPAU3M
,	ETAMB
,	ETAMC
,	ETRAMUT
,	ETRAMPT
,	ETRAMAT
,	ETCAR
,	ETCAM
,	ETMMB
,	ETMMC
,	ETMMD
,	ETRMM3
,	ETGNI
,	ETZ
,	ETPTJE
,	ETUSOFU7
,	ETUSOFU8
)
  SELECT DISTINCT
	beo.ETNRCR
,	beo.ETNRID
,	beo.ETDIGC
,	beo.ETTPID
,	beo.ETPRAP
,	beo.ETSEAP
,	beo.ETNMCL
,	beo.ETNMRZ
,	beo.ETCIIU
,	beo.ETNTJR
,	beo.ETREFA
,	beo.ETACRE
,	beo.ETACIN
,	beo.ETREES
,	beo.ETDPTO
,	beo.ETCDSC
,	beo.ETTPCR
,	beo.ETVRAC
,	beo.ETVRCR
,	beo.ETVRAN
,	beo.ETVROP
,	beo.ETFCIN
,	beo.ETFCFI
,	beo.ETTEAI
,	beo.ETTEAA
,	beo.ETTPGT
,	beo.ETLIGT
,	beo.ETVRGT
,	beo.ETFAVAGT
,	beo.ETPRCOCA
,	beo.ETPRAMHI
,	beo.ETVRSD
,	beo.ETFCMR
,	beo.ETDSMR
,	beo.ETCLAS
,	beo.ETCLASN
,	beo.ETSUBCL
,	beo.ETUSOG
,	beo.ETCALA
,	beo.ETCALM
,	beo.ETCALMD
,	beo.ETCALH
,	beo.ETVRCP
,	beo.ETVRIN
,	beo.ETSEGAUT
,	beo.ETSEGVID
,	beo.ETSEGDES
,	beo.ETSEGSOA
,	beo.ETSEGOTR
,	beo.ETCAR1
,	beo.ETCAR2
,	beo.ETCAR3
,	beo.ETUSOFU1
,	beo.ETINMR
,	beo.ETCPVD
,	beo.ETINVD
,	beo.ETAUTVEN
,	beo.ETVIDVEN
,	beo.ETDESVEN
,	beo.ETSOAVEN
,	beo.ETOTRVEN
,	beo.ETCGVD
,	beo.ETC2VD
,	beo.ETC3VD
,	beo.ETUSOFU2
,	beo.ETINAC
,	beo.ETINNA
,	beo.ETKACU
,	beo.ETKNAC
,	beo.ETCAUMES
,	beo.ETCAUVID
,	beo.ETPAGVID
,	beo.ETCAUAUT
,	beo.ETPAGAUT
,	beo.ETCAUDES
,	beo.ETPAGDES
,	beo.ETCAUSOA
,	beo.ETPAGSOA
,	beo.ETUSOFU3
,	beo.ETCXCSOA
,	beo.ETCAUOTR
,	beo.ETPAGOTR
,	beo.ETPAGIMO
,	beo.ETUSOFU4
,	beo.ETNIVRIE
,	beo.ETPROIMP
,	beo.ETCALMR
,	beo.ETPROIMPMR
,	beo.ETPROIMPM2
,	beo.ETTEADCTO
,	beo.ETFACPDI
,	beo.ETPVCPMR
,	beo.ETPVOTMR
,	beo.ETPVACPMR
,	beo.ETPVAOTMR
,	beo.ETPVCP
,	beo.ETPVK1
,	beo.ETPVK2
,	beo.ETPVKSEG
,	beo.ETPVIN
,	beo.ETPVINGE
,	beo.ETPVSG
,	beo.ETPVOT
,	beo.ETUSOFU5
,	beo.ETPPACP
,	beo.ETPPAIN
,	beo.ETPPASG
,	beo.ETPPAOT
,	beo.ETPPMK1
,	beo.ETPPMCP
,	beo.ETPPMIN
,	beo.ETPPMSG
,	beo.ETPPMOT
,	beo.ETRPCP
,	beo.ETRPIN
,	beo.ETRPSG
,	beo.ETRPOT
,	beo.ETUSOFU6
,	beo.ETVRCT
,	beo.ETCANCOR
,	beo.ETCNNOCO
,	beo.ETOPCCOR
,	beo.ETOPNOCO
,	beo.ETPKSGT3
,	beo.ETPISGT3
,	beo.ETKPAU3M
,	beo.ETIPAU3M
,	beo.ETAMB
,	beo.ETAMC
,	beo.ETRAMUT
,	beo.ETRAMPT
,	beo.ETRAMAT
,	beo.ETCAR
,	beo.ETCAM
,	beo.ETMMB
,	beo.ETMMC
,	beo.ETMMD
,	beo.ETRMM3
,	beo.ETGNI
,	beo.ETZ
,	beo.ETPTJE
,	beo.ETUSOFU7
,	beo.ETUSOFU8
FROM staging.tmp_endeot AS beo
""")


# ---- Notebook Cell 12 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_jd_marca_crediflex_072017_122019
 AS
  SELECT DISTINCT
  OPE AS ope
, ID_NUMERO AS idNumero
, TRIM(C026) AS C026
FROM bronze.finandinacartera_jd_marca_crediflex_072017_122019;
""")


# ---- Notebook Cell 13 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_jd_marca_crediflex_072017_122019;

INSERT INTO silver.dwh_jd_marca_crediflex_072017_122019
(
	ope
,	idNumero
,	C026
)
  SELECT DISTINCT
	bmcr.ope
,	bmcr.idNumero
,	bmcr.C026
FROM staging.tmp_jd_marca_crediflex_072017_122019 AS bmcr
""")


# ---- Notebook Cell 14 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_jd_base_retomas
 AS
  SELECT DISTINCT
	OBL AS obl
,	TRIM(IND) AS ind
FROM bronze.finandinacartera_jd_base_retomas;
""")


# ---- Notebook Cell 15 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_jd_base_retomas;

INSERT INTO silver.dwh_jd_base_retomas
(
	obl
,	ind
)
  SELECT DISTINCT
	bbr.obl
,	bbr.ind
FROM staging.tmp_jd_base_retomas AS bbr
""")


# ---- Notebook Cell 16 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_canaltdc
 AS
  SELECT DISTINCT
	CONTRATO AS contrato
,	TRIM(canalproductofinal) AS canalProductoFinal
FROM bronze.finandinacartera_canaltdc;
""")


# ---- Notebook Cell 17 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_canaltdc;

INSERT INTO silver.dwh_canaltdc
(
	contrato
,	canalProductoFinal
)
  SELECT DISTINCT
	bctdc.contrato
,	bctdc.canalProductoFinal
FROM staging.tmp_canaltdc AS bctdc
""")


# ---- Notebook Cell 18 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_concesionarios
 AS
  SELECT DISTINCT
	TRIM(SG_ConcesionarioLP) AS SGConcesionarioLP
,	TRIM(Concesionario) AS concesionario
FROM bronze.finandinacartera_concesionarios;
""")


# ---- Notebook Cell 19 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_concesionarios;

INSERT INTO silver.dwh_concesionarios
(
	SGConcesionarioLP
,	concesionario
)
  SELECT DISTINCT
	bc.SGConcesionarioLP
,	bc.concesionario
FROM staging.tmp_concesionarios AS bc
""")


# ---- Notebook Cell 20 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_tabla10
 AS
  SELECT DISTINCT
	CAST(REPLACE(TRIM(LNBAL),' ', '') AS DECIMAL(18,2)) AS LNBAL
,	LNBRCH
,	CAST(REPLACE(TRIM(LNCPAS),' ', '') AS DECIMAL(18,2)) AS LNCPAS
,	CAST(REPLACE(TRIM(LNCPTM),' ', '') AS DECIMAL(18,2)) AS LNCPTM
,	CAST(REPLACE(TRIM(LNDISB),' ', '') AS DECIMAL(18,2)) AS LNDISB
,	CAST(REPLACE(TRIM(LNDUF1),' ', '') AS DECIMAL(18,2)) AS LNDUF1
,	CAST(REPLACE(TRIM(LNDUF2),' ', '') AS DECIMAL(18,2)) AS LNDUF2
,	CAST(REPLACE(TRIM(LNDUF3),' ', '') AS DECIMAL(18,2)) AS LNDUF3
,	LNFDIC
,	CAST(REPLACE(TRIM(LNFRTE),' ', '') AS DECIMAL(18,6)) AS LNFRTE
,	CAST(REPLACE(TRIM(LNIDUE),' ', '') AS DECIMAL(18,2)) AS LNIDUE
,	CAST(REPLACE(TRIM(LNIPD),' ', '') AS DECIMAL(18,2)) AS LNIPD
,	LNIPDT
,	CAST(REPLACE(TRIM(LNLAMT),' ', '') AS DECIMAL(18,2)) AS LNLAMT
,	CAST(REPLACE(TRIM(LNLFD),' ', '') AS DECIMAL(18,2)) AS LNLFD
,	LNLIDT
,	CAST(REPLACE(TRIM(LNLINO),' ', '') AS DECIMAL(18,2)) AS LNLINO
,	CAST(REPLACE(TRIM(LNLPMD),' ', '') AS DECIMAL(18,2)) AS LNLPMD
,	CAST(REPLACE(TRIM(LNNONA),' ', '') AS DECIMAL(18,5)) AS LNNONA
,	LNNOTE
,	TRIM(LNOFF) AS LNOFF
,	LNPDLS
,	LNPMPD
,	CAST(REPLACE(TRIM(LNPRPD),' ', '') AS DECIMAL(18,2)) AS LNPRPD
,	TRIM(LNRISK) AS LNRISK
,	LNSCF
,	TRIM(LNSCP) AS LNSCP
,	UPPER(TRIM(LNSHRT)) AS LNSHRT
,	LNSMSA
,	LNTYPE
,	LNTERM
,	TRIM(LNUSR6) AS LNUSR6
,	LNNTDT
,	LNSCNR
,	LNSCPD
,	LNSCPM
,	LNSCTY
,	CAST(REPLACE(TRIM(LNSCAM),' ', '') AS DECIMAL(18,2)) AS LNSCAM
,	LNSCDT
,	LNNXMT
,	CAST(REPLACE(TRIM(LNNABL),' ', '') AS DECIMAL(18,2)) AS LNNABL
,	CAST(REPLACE(TRIM(LNLRTE),' ', '') AS DECIMAL(18,6)) AS LNLRTE
,	LNINBR
,	CAST(REPLACE(TRIM(LNACMD),' ', '') AS DECIMAL(18,2)) AS LNACMD
FROM bronze.finandinacartera_tabla10;
""")


# ---- Notebook Cell 21 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_tabla10;

INSERT INTO silver.dwh_tabla10
(
	LNBAL
,	LNBRCH
,	LNCPAS
,	LNCPTM
,	LNDISB
,	LNDUF1
,	LNDUF2
,	LNDUF3
,	LNFDIC
,	LNFRTE
,	LNIDUE
,	LNIPD
,	LNIPDT
,	LNLAMT
,	LNLFD
,	LNLIDT
,	LNLINO
,	LNLPMD
,	LNNONA
,	LNNOTE
,	LNOFF
,	LNPDLS
,	LNPMPD
,	LNPRPD
,	LNRISK
,	LNSCF
,	LNSCP
,	LNSHRT
,	LNSMSA
,	LNTYPE
,	LNTERM
,	LNUSR6
,	LNNTDT
,	LNSCNR
,	LNSCPD
,	LNSCPM
,	LNSCTY
,	LNSCAM
,	LNSCDT
,	LNNXMT
,	LNNABL
,	LNLRTE
,	LNINBR
,	LNACMD
)
  SELECT DISTINCT
	bt10.LNBAL
,	bt10.LNBRCH
,	bt10.LNCPAS
,	bt10.LNCPTM
,	bt10.LNDISB
,	bt10.LNDUF1
,	bt10.LNDUF2
,	bt10.LNDUF3
,	bt10.LNFDIC
,	bt10.LNFRTE
,	bt10.LNIDUE
,	bt10.LNIPD
,	bt10.LNIPDT
,	bt10.LNLAMT
,	bt10.LNLFD
,	bt10.LNLIDT
,	bt10.LNLINO
,	bt10.LNLPMD
,	bt10.LNNONA
,	bt10.LNNOTE
,	bt10.LNOFF
,	bt10.LNPDLS
,	bt10.LNPMPD
,	bt10.LNPRPD
,	bt10.LNRISK
,	bt10.LNSCF
,	bt10.LNSCP
,	bt10.LNSHRT
,	bt10.LNSMSA
,	bt10.LNTYPE
,	bt10.LNTERM
,	bt10.LNUSR6
,	bt10.LNNTDT
,	bt10.LNSCNR
,	bt10.LNSCPD
,	bt10.LNSCPM
,	bt10.LNSCTY
,	bt10.LNSCAM
,	bt10.LNSCDT
,	bt10.LNNXMT
,	bt10.LNNABL
,	bt10.LNLRTE
,	bt10.LNINBR
,	bt10.LNACMD
FROM staging.tmp_tabla10 AS bt10
""")


# ---- Notebook Cell 22 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_obli_funcyexfunc_abr2024
 AS
  SELECT DISTINCT
	Cedula AS cedula
,	No_Credito AS noCredito
,	TRIM(Nombre) AS nombre
FROM bronze.finandinacartera_obli_funcyexfunc_abr2024;
""")


# ---- Notebook Cell 23 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_obli_funcyexfunc_abr2024;

INSERT INTO silver.dwh_obli_funcyexfunc_abr2024
(
	cedula
,	noCredito
,	nombre
)
  SELECT DISTINCT
	bfexf.cedula
,	bfexf.noCredito
,	bfexf.nombre
FROM staging.tmp_obli_funcyexfunc_abr2024 AS bfexf
""")


# ---- Notebook Cell 24 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_corretaje_hasta_mar2024
 AS
  SELECT DISTINCT
  to_date(FECHA_DSM,'dd/MM/yyyy') AS fechaDsm
, N_IDENTIFICACION AS nIdentificacion
, TRIM(NOMBRE_CLIENTE) AS nombreCliente
, PRODUCTO AS producto
, OBLIGACION AS obligacion
, TRIM(COD_COMERCIAL) AS codComercial
, TRIM(VR_DESEMBOLSO) AS vrDesembolso
, ESTRATEGIA_SMSA AS estrategiaSMSA
, TRIM(TASA_INT_E_A) AS tasaIntEA
, PLAZO AS plazo
, TRIM(SALDO_CARTERA) AS saldoCartera
, DIAS_MORA AS diasMora
, TRIM(CONVENIO) AS convenio
, TRIM(OFICINA) AS oficina
, TRIM(Canal) AS canal
, TRIM(Tasa_Nom) AS tasaNom
, TRIM(Vr_Dsm_en_Millones) AS vrDsmEnMillones
, TRIM(Estado_de_credito) AS estadoDeCredito
, TRIM(Clasificacion_de_la_cartera) AS clasificacionCartera
, to_date(FECHA_INFORME,'dd/MM/yyyy') AS fechaInforme
, TRIM(Saldo_de_cartera) AS saldoDeCartera
, TRIM(SUBPRIME) AS subprime
FROM bronze.finandinacartera_corretaje_hasta_mar2024;
""")


# ---- Notebook Cell 25 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_corretaje_hasta_mar2024;

INSERT INTO silver.dwh_corretaje_hasta_mar2024
(
	fechaDsm
,	nIdentificacion
,	nombreCliente
,	producto
,	obligacion
,	codComercial
,	vrDesembolso
,	estrategiaSMSA
,	tasaIntEA
,	plazo
,	saldoCartera
,	diasMora
,	convenio
,	oficina
,	canal
,	tasaNom
,	vrDsmEnMillones
,	estadoDeCredito
,	clasificacionCartera
,	fechaInforme
,	saldoDeCartera
,	subprime
)
  SELECT DISTINCT
	bchm.fechaDsm
,	bchm.nIdentificacion
,	bchm.nombreCliente
,	bchm.producto
,	bchm.obligacion
,	bchm.codComercial
,	bchm.vrDesembolso
,	bchm.estrategiaSMSA
,	bchm.tasaIntEA
,	bchm.plazo
,	bchm.saldoCartera
,	bchm.diasMora
,	bchm.convenio
,	bchm.oficina
,	bchm.canal
,	bchm.tasaNom
,	bchm.vrDsmEnMillones
,	bchm.estadoDeCredito
,	bchm.clasificacionCartera
,	bchm.fechaInforme
,	bchm.saldoDeCartera
,	bchm.subprime
FROM staging.tmp_corretaje_hasta_mar2024 AS bchm
""")


# ---- Notebook Cell 26 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_revisaorigenlead
 AS
  SELECT DISTINCT
	to_date(MES,'yyyy-MM-dd') AS mes
,	TRIM(CUSSNR) AS CUSSNR
,	TRIM(DINROOBL) AS DINROOBL
,	LNBRCH
,	DIINDCRE
,	TRIM(DINOMCLI) AS DINOMCLI
,	TRIM(DIVLRDES) AS DIVLRDES
,	DIVLRCAN
,	NETO AS neto
,	DITASEFE
,	to_date(DIFECINI,'yyyy-MM-dd') AS DIFECINI
,	DIPLAZO
,	TRIM(DICONCE) AS DICONCE
,	TRIM(NOMCOM) AS NOMCOM
,	DIVLRGAR
,	TRIM(DIMARCA) AS DIMARCA
,	TRIM(DITIPO) AS DITIPO
,	TRIM(DIDESCR) AS DIDESCR
,	DIMODEL
,	DICOMFIN
,	DIOTRCOM
,	DIVLRGIR
,	DIPROBIM
,	DIPOLANU
,	DISEGAUT
,	COMISION AS comision
,	DITIPPOL
,	DIORINEG
,	LNTYPE
,	LNNABL
,	TSVLRSUB
,	DICLASIF
,	DICCVEND
,	DIINDREF
,	TRIM(DINOMVEN) AS DINOMVEN
,	TRIM(LNOFF) AS LNOFF
,	LNFACE
,	DiaHabil AS diaHabil
,	DiaHabilCorte AS diaHabilCorte
,	TRIM(Corte) AS corte
,	CorteMesPasado AS corteMesPasado
,	TRIM(Ciudad) AS ciudad
, TRIM(RegionalCiudad) AS regionalCiudad
, TRIM(ZonaComercial) AS zonaComercial
, TRIM(RegionalComercial) AS regionalComercial
, TRIM(CanalComercial) AS canalComercial
, TRIM(Producto) AS producto
, TRIM(Comercial) AS comercial
, Unidad AS unidad
, MesActual AS mesActual
, TRIM(Valor) AS valor
, UnidadPm AS unidadPm
, PlanMenor AS planMenor
, TRIM(PlanMayorYDctoCheques) AS planMayorYDctoCheques
, MaqAgricola AS maqAgricola
, CompraCartera AS compraCartera
, PlanMayorFacturas AS planMayorFacturas
, Detal AS detal
, Subvencion AS subvencion
, TRIM(MontoSubvencion) AS montoSubvencion
, Tasa AS tasa
, TasaFinal AS tasaFinal
, TRIM(ParaInt) AS paraInt
, TRIM(ParaIntFinal) AS paraIntFinal
, TRIM(ParaPlazo) AS paraPlazo
, TRIM(ParaPI) AS paraPI
, ParaCI AS paraCI
, Comisiona AS comisiona
, TRIM(ValorComision) AS valorComision
, TRIM(Clasificacion) AS clasificacion
, Credito AS credito
, Leasing AS leasing
, TRIM(NuevoUsado) AS nuevoUsado
, Cardif AS cardif
, VrCardif AS vrCardif
, DobleVida AS dobleVida
, VrDobleVida AS vrDobleVida
, PlanMenorYMaquinaria AS planMenorYMaquinaria
, CreditoDeLibranza AS creditoDeLibranza
, Cumplimiento AS cumplimiento
, TRIM(Concesionario) AS concesionario
, TRIM(REGIONAL) AS regional
, TRIM(NombreGestionDiaria_LP) AS nombreGestionDiariaLP
, RETANQUEO AS retanqueo
, NUEVO AS nuevo
, TRIM(SEMANA) AS semana
, TRIM(Menor80millones) AS menor80Millones
, TRIM(Origen) AS origen
, TRIM(CEDULA) AS cedula
, TRIM(PaginaSitio) AS paginaSitio
, TRIM(IdSolicitud) AS idSolicitud
, TRIM(Nombre) AS nombre
, TRIM(Placa) AS placa
, MontoComercialDeLaGarantia AS montoComercialDeLaGarantia
, TRIM(NombreBeneficiarioGiro) AS nombreBeneficiarioGiro
, TRIM(TipoIdentificacionGiro) AS tipoIdentificacionGiro
, TRIM(NumeroIdentificacionGiro) AS numeroIdentificacionGiro
, TRIM(Marca_vehiculo) AS marcaVehiculo
, Modelo AS modelo
, TRIM(Ciudad_Concesionario_right) AS ciudadConcesionarioRight
, TRIM(EtapaActualDeLaSolicitud) AS etapaActualDeLaSolicitud
, TRIM(EstadoDelNegocio) AS estadoDelNegocio
, EsAprobacionAutomatica AS esAprobacionAutomatica
, to_date(FechadeSolicitud,'yyyy-MM-dd') AS fechaDeSolicitud
, TRIM(CorreoElectronico) AS correoElectronico
, TRIM(TelefonoCelular) AS telefonoCelular
, TasaEfectiva AS tasaEfectiva
, TasaNominalMesVencida AS tasaNominalMesVencida
, Plazo AS plazo
, TRIM(PlacaCON) AS placaCon
, TRIM(NumeroChasis) AS numeroChasis
, TRIM(Marca_vehiculoCON) AS marcaVehiculoCon
, Monto_Desembolsado AS montoDesembolsado
, TRIM(fkFecha) AS fkFecha
, TRIM(sOrigendelNegocio) AS sOrigenDelNegocio
, TRIM(Ciudad_Concesionario) AS ciudadConcesionario
, to_date(Fecha_desembolso,'yyyy-MM-dd') AS fechaDesembolso
, montoDesembolsado AS montoDesembolsado1
, TRIM(OrigencompraConsolidado) AS origenCompraConsolidado
, TRIM(NumerOsolicitudConsolidado) AS numeroSolicitudConsolidado
, TRIM(MarcaConsolidada) AS marcaConsolidada
, TRIM(ConcesionarioConsolidado) AS concesionarioConsolidado
, TRIM(NuevoUsadoConsolidado) AS nuevoUsadoConsolidado
, TRIM(UsoVehiculoConsolidado) AS usoVehiculoConsolidado
, TRIM(RiesgoConsolidado) AS riesgoConsolidado
, TRIM(ConsolidadoClaseVehiculo) AS consolidadoClaseVehiculo
, TRIM(ConsolidadoPlan) AS consolidadoPlan
, TRIM(ConsolidadoAseguradora) AS consolidadoAseguradora
, TRIM(ConsolidadoLineaVerde) AS consolidadoLineaVerde
, TRIM(ConsolidadoPlaca) AS consolidadoPlaca
, TRIM(ConsolidadoReferencia1) AS consolidadoReferencia1
, TRIM(ConsolidadoEndozoSeguro) AS consolidadoEndozoSeguro
FROM bronze.segmentaciondinamica2_1_revisaorigenlead;
""")


# ---- Notebook Cell 27 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_revisaorigenlead;

INSERT INTO silver.dwh_revisaorigenlead
(
	mes
,	CUSSNR
,	DINROOBL
,	LNBRCH
,	DIINDCRE
,	DINOMCLI
,	DIVLRDES
,	DIVLRCAN
,	neto
,	DITASEFE
,	DIFECINI
,	DIPLAZO
,	DICONCE
,	NOMCOM
,	DIVLRGAR
,	DIMARCA
,	DITIPO
,	DIDESCR
,	DIMODEL
,	DICOMFIN
,	DIOTRCOM
,	DIVLRGIR
,	DIPROBIM
,	DIPOLANU
,	DISEGAUT
,	comision
,	DITIPPOL
,	DIORINEG
,	LNTYPE
,	LNNABL
,	TSVLRSUB
,	DICLASIF
,	DICCVEND
,	DIINDREF
,	DINOMVEN
,	LNOFF
,	LNFACE
,	diaHabil
,	diaHabilCorte
,	corte
,	corteMesPasado
,	ciudad
,	regionalCiudad
,	zonaComercial
,	regionalComercial
,	canalComercial
,	producto
,	comercial
,	unidad
,	mesActual
,	valor
,	unidadPm
,	planMenor
,	planMayorYDctoCheques
,	maqAgricola
,	compraCartera
,	planMayorFacturas
,	detal
,	subvencion
,	montoSubvencion
,	tasa
,	tasaFinal
,	paraInt
,	paraIntFinal
,	paraPlazo
,	paraPI
,	paraCI
,	comisiona
,	valorComision
,	clasificacion
,	credito
,	leasing
,	nuevoUsado
,	cardif
,	vrCardif
,	dobleVida
,	vrDobleVida
,	planMenorYMaquinaria
,	creditoDeLibranza
,	cumplimiento
,	concesionario
,	regional
,	nombreGestionDiariaLP
,	retanqueo
,	nuevo
,	semana
,	menor80Millones
,	origen
,	cedula
,	paginaSitio
,	idSolicitud
,	nombre
,	placa
,	montoComercialDeLaGarantia
,	nombreBeneficiarioGiro
,	tipoIdentificacionGiro
,	numeroIdentificacionGiro
,	marcaVehiculo
,	modelo
,	ciudadConcesionario
,	etapaActualDeLaSolicitud
,	estadoDelNegocio
,	esAprobacionAutomatica
,	fechaDeSolicitud
,	correoElectronico
,	telefonoCelular
,	tasaEfectiva
,	tasaNominalMesVencida
,	plazo
,	placaCon
,	numeroChasis
,	marcaVehiculoCon
,	montoDesembolsado
,	fkFecha
,	SOrigenDelNegocio
,	ciudadConcesionarioRight
,	fechaDesembolso
,	montoDesembolsado1
,	origenCompraConsolidado
,	numeroSolicitudConsolidado
,	marcaConsolidada
,	concesionarioConsolidado
,	nuevoUsadoConsolidado
,	usoVehiculoConsolidado
,	riesgoConsolidado
,	consolidadoClaseVehiculo
,	consolidadoPlan
,	consolidadoAseguradora
,	consolidadoLineaVerde
,	consolidadoPlaca
,	consolidadoReferencia1
,	consolidadoEndozoSeguro
)
  SELECT DISTINCT
	brol.mes
,	brol.CUSSNR
,	brol.DINROOBL
,	brol.LNBRCH
,	brol.DIINDCRE
,	brol.DINOMCLI
,	brol.DIVLRDES
,	brol.DIVLRCAN
,	brol.neto
,	brol.DITASEFE
,	brol.DIFECINI
,	brol.DIPLAZO
,	brol.DICONCE
,	brol.NOMCOM
,	brol.DIVLRGAR
,	brol.DIMARCA
,	brol.DITIPO
,	brol.DIDESCR
,	brol.DIMODEL
,	brol.DICOMFIN
,	brol.DIOTRCOM
,	brol.DIVLRGIR
,	brol.DIPROBIM
,	brol.DIPOLANU
,	brol.DISEGAUT
,	brol.comision
,	brol.DITIPPOL
,	brol.DIORINEG
,	brol.LNTYPE
,	brol.LNNABL
,	brol.TSVLRSUB
,	brol.DICLASIF
,	brol.DICCVEND
,	brol.DIINDREF
,	brol.DINOMVEN
,	brol.LNOFF
,	brol.LNFACE
,	brol.diaHabil
,	brol.diaHabilCorte
,	brol.corte
,	brol.corteMesPasado
,	brol.ciudad
,	brol.regionalCiudad
,	brol.zonaComercial
,	brol.regionalComercial
,	brol.canalComercial
,	brol.producto
,	brol.comercial
,	brol.unidad
,	brol.mesActual
,	brol.valor
,	brol.unidadPm
,	brol.planMenor
,	brol.planMayorYDctoCheques
,	brol.maqAgricola
,	brol.compraCartera
,	brol.planMayorFacturas
,	brol.detal
,	brol.subvencion
,	brol.montoSubvencion
,	brol.tasa
,	brol.tasaFinal
,	brol.paraInt
,	brol.paraIntFinal
,	brol.paraPlazo
,	brol.paraPI
,	brol.paraCI
,	brol.comisiona
,	brol.valorComision
,	brol.clasificacion
,	brol.credito
,	brol.leasing
,	brol.nuevoUsado
,	brol.cardif
,	brol.vrCardif
,	brol.dobleVida
,	brol.vrDobleVida
,	brol.planMenorYMaquinaria
,	brol.creditoDeLibranza
,	brol.cumplimiento
,	brol.concesionario
,	brol.regional
,	brol.nombreGestionDiariaLP
,	brol.retanqueo
,	brol.nuevo
,	brol.semana
,	brol.menor80Millones
,	brol.origen
,	brol.cedula
,	brol.paginaSitio
,	brol.idSolicitud
,	brol.nombre
,	brol.placa
,	brol.montoComercialDeLaGarantia
,	brol.nombreBeneficiarioGiro
,	brol.tipoIdentificacionGiro
,	brol.numeroIdentificacionGiro
,	brol.marcaVehiculo
,	brol.modelo
,	brol.ciudadConcesionario
,	brol.etapaActualDeLaSolicitud
,	brol.estadoDelNegocio
,	brol.esAprobacionAutomatica
,	brol.fechaDeSolicitud
,	brol.correoElectronico
,	brol.telefonoCelular
,	brol.tasaEfectiva
,	brol.tasaNominalMesVencida
,	brol.plazo
,	brol.placaCon
,	brol.numeroChasis
,	brol.marcaVehiculoCon
,	brol.montoDesembolsado
,	brol.fkFecha
,	brol.SOrigenDelNegocio
,	brol.ciudadConcesionarioRight
,	brol.fechaDesembolso
,	brol.montoDesembolsado1
,	brol.origenCompraConsolidado
,	brol.numeroSolicitudConsolidado
,	brol.marcaConsolidada
,	brol.concesionarioConsolidado
,	brol.nuevoUsadoConsolidado
,	brol.usoVehiculoConsolidado
,	brol.riesgoConsolidado
,	brol.consolidadoClaseVehiculo
,	brol.consolidadoPlan
,	brol.consolidadoAseguradora
,	brol.consolidadoLineaVerde
,	brol.consolidadoPlaca
,	brol.consolidadoReferencia1
,	brol.consolidadoEndozoSeguro

FROM staging.tmp_revisaorigenlead AS brol
""")


# ---- Notebook Cell 28 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_suprimelibranza
 AS
  SELECT DISTINCT
	OPE
,	TRIM(IND) AS ind
FROM bronze.finandinacartera_suprimelibranza;
""")


# ---- Notebook Cell 29 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_suprimelibranza;

INSERT INTO silver.dwh_suprimelibranza
(
	OPE
,	ind
)
  SELECT DISTINCT
	bsl.OPE
,	bsl.ind
FROM staging.tmp_suprimelibranza AS bsl
""")


# ---- Notebook Cell 30 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_jc_basesdmanual
AS
  SELECT DISTINCT
	OBLIGACION AS obligacion
,	IDENTIFICACION AS identificacion
,	TRIM(NOMBRE_CLIENTE) AS nombreCliente
,	date_format(FECHA_DESEMBOLSO, 'yyyy-MM-dd HH:mm:ss.SSS') AS fechaDesembolso
,	date_format(FECHA_VENTA, 'yyyy-MM-dd HH:mm:ss.SSS') AS fechaVenta
,	TRIM(LINEA_CREDITO) AS lineaCredito
,	CAST(REPLACE(TRIM(MONTO),' ', '') AS DECIMAL(18,2)) AS monto
,	FLUJO AS flujo
,	TRIM(ANALISTA) AS analista
,	CIERRE AS cierre
FROM bronze.finandinacartera_jc_basesdmanual;
""")


# ---- Notebook Cell 31 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_jc_basesdmanual;

INSERT INTO silver.dwh_jc_basesdmanual
(
	obligacion
,	identificacion
,	nombreCliente
,	fechaDesembolso
,	fechaVenta
,	lineaCredito
,	monto
,	flujo
,	analista
,	cierre
)
  SELECT DISTINCT
	bjcsd.obligacion
,	bjcsd.identificacion
,	bjcsd.nombreCliente
,	bjcsd.fechaDesembolso
,	bjcsd.fechaVenta
,	bjcsd.lineaCredito
,	bjcsd.monto
,	bjcsd.flujo
,	bjcsd.analista
,	bjcsd.cierre
FROM staging.tmp_jc_basesdmanual AS bjcsd
""")


# ---- Notebook Cell 32 (SQL) ----
run_sql("""
--Se crea Tabla Temporal con las trasformaciones aplicadas a la tabla bronze:
CREATE OR REPLACE TABLE staging.tmp_comovamos
 AS
  SELECT DISTINCT
	to_date(MES,'yyyy-MM-dd') AS mes
,	TRIM(CUSSNR) AS CUSSNR 
,	TRIM(DINROOBL) AS DINROOBL 
,	LNBRCH
,	DIINDCRE
,	TRIM(DINOMCLI) AS DINOMCLI 
,	TRIM(DIVLRDES) AS DIVLRDES 
,	DIVLRCAN
,	NETO AS neto
,	DITASEFE
,	to_date(DIFECINI,'yyyy-MM-dd') AS DIFECINI
,	DIPLAZO
,	TRIM(DICONCE) AS DICONCE 
,	TRIM(NOMCOM) AS NOMCOM 
,	DIVLRGAR
,	TRIM(DIMARCA) AS DIMARCA 
,	TRIM(DITIPO) AS DITIPO 
,	TRIM(DIDESCR) AS DIDESCR 
,	DIMODEL
,	DICOMFIN
,	DIOTRCOM
,	DIVLRGIR
,	DIPROBIM
,	DIPOLANU
,	DISEGAUT
,	COMISION AS comision
,	DITIPPOL
,	DIORINEG
,	LNTYPE
,	LNNABL
,	TSVLRSUB
,	DICLASIF
,	DICCVEND
,	DIINDREF
,	TRIM(DINOMVEN) AS DINOMVEN 
,	TRIM(LNOFF) AS LNOFF 
,	LNFACE
,	DiaHabil AS diaHabil
, DiaHabilCorte AS diaHabilCorte
, Corte AS corte
, CorteMesPasado AS corteMesPasado
, TRIM(Ciudad) AS ciudad
, TRIM(RegionalCiudad) AS regionalCiudad
, TRIM(ZonaComercial) AS zonaComercial
, TRIM(RegionalComercial) AS regionalComercial
, TRIM(CanalComercial) AS canalComercial
, TRIM(Producto) AS producto
, TRIM(Comercial) AS comercial
, Unidad AS unidad
, MesActual AS mesActual
, TRIM(Valor) AS valor
, UnidadPm AS unidadPm
, PlanMenor AS planMenor
, TRIM(PlanMayorYDctoCheques) AS planMayorYDctoCheques
, MaqAgricola AS maqAgricola
, CompraCartera AS compraCartera
, PlanMayorFacturas AS planMayorFacturas
, Detal AS detal
, Subvencion AS subvencion
, TRIM(MontoSubvencion) AS montoSubvencion
, Tasa AS tasa
, TasaFinal AS tasaFinal
, TRIM(ParaInt) AS paraInt
, TRIM(ParaIntFinal) AS paraIntFinal
, TRIM(ParaPlazo) AS paraPlazo
, TRIM(ParaPI) AS paraPI
, ParaCI AS paraCI
, Comisiona AS comisiona
, TRIM(ValorComision) AS valorComision
, TRIM(Clasificacion) AS clasificacion
, Credito AS credito
, Leasing AS leasing
, TRIM(NuevoUsado) AS nuevoUsado
, Cardif AS cardif
, VrCardif AS vrCardif
, DobleVida AS dobleVida
, VrDobleVida AS vrDobleVida
, PlanMenorYMaquinaria AS planMenorYMaquinaria
, CreditoDeLibranza AS creditoDeLibranza
, Cumplimiento AS cumplimiento
, TRIM(Concesionario) AS concesionario
, TRIM(REGIONAL) AS regional
, TRIM(NombreGestionDiaria_LP) AS nombreGestionDiariaLP
, TRIM(concesionario1) AS concesionario1
, RETANQUEO AS retanqueo
, NUEVO AS nuevo
, TRIM(SEMANA) AS semana
, TRIM(Menor80millones) AS menor80Millones
, TRIM(Origen) AS origen
, TRIM(PaginaSitio) AS paginaSitio
, TRIM(IdSolicitud) AS idSolicitud
, TRIM(CEDULA) AS cedula
, TRIM(Nombre) AS nombre
, TRIM(Placa) AS placa
, MontoComercialDeLaGarantia AS montoComercialDeLaGarantia
, TRIM(NombreBeneficiarioGiro) AS nombreBeneficiarioGiro
, TRIM(TipoIdentificacionGiro) AS tipoIdentificacionGiro
, TRIM(NumeroIdentificacionGiro) AS numeroIdentificacionGiro
, TRIM(Marca_vehiculo) AS marcaVehiculo
, Modelo AS modelo
, TRIM(Ciudad_Concesionario) AS ciudadConcesionario
, TRIM(EtapaActualDeLaSolicitud) AS etapaActualDeLaSolicitud
, TRIM(EstadoDelNegocio) AS estadoDelNegocio
, EsAprobacionAutomatica AS esAprobacionAutomatica
, to_date(FechadeSolicitud,'yyyy-MM-dd') AS fechaDeSolicitud
, TRIM(CorreoElectronico) AS correoElectronico
, TRIM(TelefonoCelular) AS telefonoCelular
, TasaEfectiva AS tasaEfectiva
, TasaNominalMesVencida AS tasaNominalMesVencida
, Plazo AS plazo
, TRIM(PlacaCON) AS placaCon
, TRIM(NumeroChasis) AS numeroChasis
, TRIM(Marca_vehiculoCON) AS marcaVehiculoCon
, Monto_Desembolsado AS montoDesembolsado
, TRIM(fkFecha) AS fkFecha
, TRIM(sOrigendelNegocio) AS sOrigenDelNegocio
, TRIM(Ciudad_Concesionario_right) AS ciudadConcesionarioRight
, to_date(Fecha_desembolso,'yyyy-MM-dd') AS fechaDesembolso
, montoDesembolsado AS montoDesembolsado1
, TRIM(OrigencompraConsolidado) AS origenCompraConsolidado
, TRIM(NumerOsolicitudConsolidado) AS numeroSolicitudConsolidado
, TRIM(MarcaConsolidada) AS marcaConsolidada
, TRIM(ConcesionarioConsolidado) AS concesionarioConsolidado
, TRIM(NuevoUsadoConsolidado) AS nuevoUsadoConsolidado
, TRIM(UsoVehiculoConsolidado) AS usoVehiculoConsolidado
, TRIM(RiesgoConsolidado) AS riesgoConsolidado
, TRIM(ConsolidadoClaseVehiculo) AS consolidadoClaseVehiculo
, TRIM(ConsolidadoPlan) AS consolidadoPlan
, TRIM(ConsolidadoAseguradora) AS consolidadoAseguradora
, TRIM(ConsolidadoLineaVerde) AS consolidadoLineaVerde
, TRIM(ConsolidadoPlaca) AS consolidadoPlaca
, TRIM(ConsolidadoReferencia1) AS consolidadoReferencia1
, TRIM(ConsolidadoEndozoSeguro) AS consolidadoEndozoSeguro
FROM bronze.carterafinainco_comovamos;
""")


# ---- Notebook Cell 33 (SQL) ----
run_sql("""
--Inserción full en silver
DELETE FROM silver.dwh_comovamos;

INSERT INTO silver.dwh_comovamos
(
	mes
,	CUSSNR
,	DINROOBL
,	LNBRCH
,	DIINDCRE
,	DINOMCLI
,	DIVLRDES
,	DIVLRCAN
,	neto
,	DITASEFE
,	DIFECINI
,	DIPLAZO
,	DICONCE
,	NOMCOM
,	DIVLRGAR
,	DIMARCA
,	DITIPO
,	DIDESCR
,	DIMODEL
,	DICOMFIN
,	DIOTRCOM
,	DIVLRGIR
,	DIPROBIM
,	DIPOLANU
,	DISEGAUT
,	comision
,	DITIPPOL
,	DIORINEG
,	LNTYPE
,	LNNABL
,	TSVLRSUB
,	DICLASIF
,	DICCVEND
,	DIINDREF
,	DINOMVEN
,	LNOFF
,	LNFACE
,	diaHabil
,	diaHabilCorte
,	corte
,	corteMesPasado
,	ciudad
,	regionalCiudad
,	zonaComercial
,	regionalComercial
,	canalComercial
,	producto
,	comercial
,	unidad
,	mesActual
,	valor
,	unidadPm
,	planMenor
,	planMayorYDctoCheques
,	maqAgricola
,	compraCartera
,	planMayorFacturas
,	detal
,	subvencion
,	montoSubvencion
,	tasa
,	tasaFinal
,	paraInt
,	paraIntFinal
,	paraPlazo
,	paraPI
,	paraCI
,	comisiona
,	valorComision
,	clasificacion
,	credito
,	leasing
,	nuevoUsado
,	cardif
,	vrCardif
,	dobleVida
,	vrDobleVida
,	planMenorYMaquinaria
,	creditoDeLibranza
,	cumplimiento
,	concesionario
,	regional
,	nombreGestionDiariaLP
,	concesionario1
,	retanqueo
,	nuevo
,	semana
,	menor80Millones
,	origen
,	paginaSitio
,	idSolicitud
,	cedula
,	nombre
,	placa
,	montoComercialDeLaGarantia
,	nombreBeneficiarioGiro
,	tipoIdentificacionGiro
,	numeroIdentificacionGiro
,	marcaVehiculo
,	modelo
,	ciudadConcesionario
,	etapaActualDeLaSolicitud
,	estadoDelNegocio
,	esAprobacionAutomatica
,	fechaDeSolicitud
,	correoElectronico
,	telefonoCelular
,	tasaEfectiva
,	tasaNominalMesVencida
,	plazo
,	placaCon
,	numeroChasis
,	marcaVehiculoCon
,	montoDesembolsado
,	fkFecha
,	SOrigenDelNegocio
,	ciudadConcesionarioRight
,	fechaDesembolso
,	montoDesembolsado1
,	origenCompraConsolidado
,	numeroSolicitudConsolidado
,	marcaConsolidada
,	concesionarioConsolidado
,	nuevoUsadoConsolidado
,	usoVehiculoConsolidado
,	riesgoConsolidado
,	consolidadoClaseVehiculo
,	consolidadoPlan
,	consolidadoAseguradora
,	consolidadoLineaVerde
,	consolidadoPlaca
,	consolidadoReferencia1
,	consolidadoEndozoSeguro
)
  SELECT DISTINCT
  	bcv.mes
,	bcv.CUSSNR
,	bcv.DINROOBL
,	bcv.LNBRCH
,	bcv.DIINDCRE
,	bcv.DINOMCLI
,	bcv.DIVLRDES
,	bcv.DIVLRCAN
,	bcv.neto
,	bcv.DITASEFE
,	bcv.DIFECINI
,	bcv.DIPLAZO
,	bcv.DICONCE
,	bcv.NOMCOM
,	bcv.DIVLRGAR
,	bcv.DIMARCA
,	bcv.DITIPO
,	bcv.DIDESCR
,	bcv.DIMODEL
,	bcv.DICOMFIN
,	bcv.DIOTRCOM
,	bcv.DIVLRGIR
,	bcv.DIPROBIM
,	bcv.DIPOLANU
,	bcv.DISEGAUT
,	bcv.comision
,	bcv.DITIPPOL
,	bcv.DIORINEG
,	bcv.LNTYPE
,	bcv.LNNABL
,	bcv.TSVLRSUB
,	bcv.DICLASIF
,	bcv.DICCVEND
,	bcv.DIINDREF
,	bcv.DINOMVEN
,	bcv.LNOFF
,	bcv.LNFACE
,	bcv.diaHabil
,	bcv.diaHabilCorte
,	bcv.corte
,	bcv.corteMesPasado
,	bcv.ciudad
,	bcv.regionalCiudad
,	bcv.zonaComercial
,	bcv.regionalComercial
,	bcv.canalComercial
,	bcv.producto
,	bcv.comercial
,	bcv.unidad
,	bcv.mesActual
,	bcv.valor
,	bcv.unidadPm
,	bcv.planMenor
,	bcv.planMayorYDctoCheques
,	bcv.maqAgricola
,	bcv.compraCartera
,	bcv.planMayorFacturas
,	bcv.detal
,	bcv.subvencion
,	bcv.montoSubvencion
,	bcv.tasa
,	bcv.tasaFinal
,	bcv.paraInt
,	bcv.paraIntFinal
,	bcv.paraPlazo
,	bcv.paraPI
,	bcv.paraCI
,	bcv.comisiona
,	bcv.valorComision
,	bcv.clasificacion
,	bcv.credito
,	bcv.leasing
,	bcv.nuevoUsado
,	bcv.cardif
,	bcv.vrCardif
,	bcv.dobleVida
,	bcv.vrDobleVida
,	bcv.planMenorYMaquinaria
,	bcv.creditoDeLibranza
,	bcv.cumplimiento
,	bcv.concesionario
,	bcv.regional
,	bcv.nombreGestionDiariaLP
,	bcv.concesionario1
,	bcv.retanqueo
,	bcv.nuevo
,	bcv.semana
,	bcv.menor80Millones
,	bcv.origen
,	bcv.paginaSitio
,	bcv.idSolicitud
,	bcv.cedula
,	bcv.nombre
,	bcv.placa
,	bcv.montoComercialDeLaGarantia
,	bcv.nombreBeneficiarioGiro
,	bcv.tipoIdentificacionGiro
,	bcv.numeroIdentificacionGiro
,	bcv.marcaVehiculo
,	bcv.modelo
,	bcv.ciudadConcesionario
,	bcv.etapaActualDeLaSolicitud
,	bcv.estadoDelNegocio
,	bcv.esAprobacionAutomatica
,	bcv.fechaDeSolicitud
,	bcv.correoElectronico
,	bcv.telefonoCelular
,	bcv.tasaEfectiva
,	bcv.tasaNominalMesVencida
,	bcv.plazo
,	bcv.placaCon
,	bcv.numeroChasis
,	bcv.marcaVehiculoCon
,	bcv.montoDesembolsado
,	bcv.fkFecha
,	bcv.SOrigenDelNegocio
,	bcv.ciudadConcesionarioRight
,	bcv.fechaDesembolso
,	bcv.montoDesembolsado1
,	bcv.origenCompraConsolidado
,	bcv.numeroSolicitudConsolidado
,	bcv.marcaConsolidada
,	bcv.concesionarioConsolidado
,	bcv.nuevoUsadoConsolidado
,	bcv.usoVehiculoConsolidado
,	bcv.riesgoConsolidado
,	bcv.consolidadoClaseVehiculo
,	bcv.consolidadoPlan
,	bcv.consolidadoAseguradora
,	bcv.consolidadoLineaVerde
,	bcv.consolidadoPlaca
,	bcv.consolidadoReferencia1
,	bcv.consolidadoEndozoSeguro
FROM staging.tmp_comovamos AS bcv
""")


# ---- Notebook Cell 34 (SQL) ----
run_sql("""
--Se eliminan las tablas empleadas para la inserción de tablas full
DROP TABLE IF EXISTS staging.tmp_jd_fechas;
DROP TABLE IF EXISTS staging.tmp_ende;
DROP TABLE IF EXISTS staging.tmp_endeot;
DROP TABLE IF EXISTS staging.tmp_jd_marca_crediflex_072017_122019;
DROP TABLE IF EXISTS staging.tmp_jd_base_retomas;
DROP TABLE IF EXISTS staging.tmp_canaltdc;
DROP TABLE IF EXISTS staging.tmp_concesionarios;
DROP TABLE IF EXISTS staging.tmp_tabla10;
DROP TABLE IF EXISTS staging.tmp_funcyexfunc_abr2024;
DROP TABLE IF EXISTS staging.tmp_corretaje_hasta_mar2024;
DROP TABLE IF EXISTS staging.tmp_revisaorigenlead;
DROP TABLE IF EXISTS staging.tmp_suprimelibranza;
DROP TABLE IF EXISTS staging.tmp_jc_basesdmanual;
DROP TABLE IF EXISTS staging.tmp_comovamos;
""")


# ---- End of translated notebook ----
job.commit()
