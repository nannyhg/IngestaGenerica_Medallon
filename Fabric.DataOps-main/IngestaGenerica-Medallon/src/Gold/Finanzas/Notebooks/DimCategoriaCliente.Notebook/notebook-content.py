# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc3e1420-ba50-4077-b00c-4faf82d02323",
# META       "default_lakehouse_name": "Gold",
# META       "default_lakehouse_workspace_id": "29561514-52fa-45ff-9941-9a53458b4a4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc3e1420-ba50-4077-b00c-4faf82d02323"
# META         },
# META         {
# META           "id": "6f04d354-fefb-4fde-9146-a60eb87d0884"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run DataOpsConfig

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run DataOpsLibrary

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Plantilla Carga Dimensión — SCD Tipo 1
# 
# Notebook genérico para cargar dimensiones en Gold usando el patrón Slowly Changing Dimension Tipo 1.
# 
# **Para adaptarlo a otra dimensión**, solo se deben modificar las celdas de **Configuración** y **DDL**.
# 
# El proceso:
# 1. Crea la tabla destino si no existe (con registros dummy).
# 2. Detecta registros **nuevos** (por Natural Key) y les asigna SK secuencial.
# 3. Detecta registros **existentes con cambios** y los actualiza vía MERGE.
# 4. Retorna un resumen JSON con conteos y estado (éxito/error), listo para logging.

# CELL ********************

from pyspark.sql.functions import (col, lit, coalesce, row_number,max as spark_max)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Log = DataOpsLog()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuración de la Dimensión
# > **Modificar esta celda** para cargar otra dimensión.

# CELL ********************

# ── Tabla destino
GoldTable    = "Gold.Finanzas.DimCategoriaCliente"
SkCol        = "SKDCategoriaCliente"
NaturalKey   = ["IdCategoriaCliente"]
TrackingCols = [
    "CategoriaCliente","CtrVentas","CtaVentas","CtrDescGral","CtaDescGral","CtrCostVent",
"CtaCostVent","CtrDescLin","CtaDescLin","CtrCostLin","CtaCostLin","CtrVendCom","CtaVendCom","CtrCobrCom",
"CtaCobrCom","CtrCxc","CtaCxc","CtrLxc","CtaLxc","CtrContado","CtaContado","CtrProntoPagCxc","CtaProntoPagCxc",
"CtrIntMoraCxc","CtaIntMoraCxc","CtrRecibosCxc","CtaRecibosCxc","CtrDebitoCxc","CtaDebitoCxc","CtrCreditoCxc",
"CtaCreditoCxc","CtrImpuesto1Cxc","CtaImpuesto1Cxc","CtrImpuesto2Cxc","CtaImpuesto2Cxc","CtrRubro1Cxc",
"CtaRubro1Cxc","CtrRubro2Cxc","CtaRubro2Cxc","CtrAnticipoCxc","CtaAnticipoCxc","CtrDescBonif","CtaDescBonif",
"CtrDevVentas","CtaDevVentas","CtrIntCorriente","CtaIntCorriente","CtrVentasExen","CtaVentasExen",
"CtrAjusteRedondeo","CtaAjusteRedondeo","CtrCobrDudosa","CtaCobrDudosa","CtrImpBolsaCxc","CtaImpBolsaCxc",
"Source"


]

# ── DDL de creación (solo si la tabla no existe) 
DDLQuery = """
    CREATE TABLE IF NOT EXISTS Gold.Finanzas.DimCategoriaCliente (
        SKDCategoriaCliente      BIGINT    COMMENT 'Clave subrogada (Surrogate Key).',
        IdCategoriaCliente       STRING    COMMENT 'Identificador compuesto de la dimensión.',
        CategoriaCliente         STRING    COMMENT 'Nombre o denominación asociada de la Categoría de cliente.',
  
        CtrVentas                STRING    COMMENT 'Centro contable asociado a ventas.',
        CtaVentas                STRING    COMMENT 'Cuenta contable asociada a ventas.',
        
        CtrDescGral              STRING    COMMENT 'Centro contable asociado a descuentos generales.',
        CtaDescGral              STRING    COMMENT 'Cuenta contable asociada a descuentos generales.',
        
        CtrCostVent              STRING    COMMENT 'Centro contable asociado a costos de ventas.',
        CtaCostVent              STRING    COMMENT 'Cuenta contable asociada a costos de ventas.',
        
        CtrDescLin               STRING    COMMENT 'Centro contable asociado a descuentos por línea.',
        CtaDescLin               STRING    COMMENT 'Cuenta contable asociada a descuentos por línea.',
        
        CtrCostLin               STRING    COMMENT 'Centro contable asociado a costos por línea.',
        CtaCostLin               STRING    COMMENT 'Cuenta contable asociada a costos por línea.',
        
        CtrVendCom               STRING    COMMENT 'Centro contable asociado a comisiones de vendedores.',
        CtaVendCom               STRING    COMMENT 'Cuenta contable asociada a comisiones de vendedores.',
        
        CtrCobrCom               STRING    COMMENT 'Centro contable asociado a comisiones de cobranza.',
        CtaCobrCom               STRING    COMMENT 'Cuenta contable asociada a comisiones de cobranza.',
        
        CtrCxc                   STRING    COMMENT 'Centro contable asociado a cuentas por cobrar.',
        CtaCxc                   STRING    COMMENT 'Cuenta contable asociada a cuentas por cobrar.',
        
        CtrLxc                   STRING    COMMENT 'Centro contable asociado a letras por cobrar.',
        CtaLxc                   STRING    COMMENT 'Cuenta contable asociada a letras por cobrar.',
        
        CtrContado               STRING    COMMENT 'Centro contable asociado a ventas de contado.',
        CtaContado               STRING    COMMENT 'Cuenta contable asociada a ventas de contado.',
        
        CtrProntoPagCxc          STRING    COMMENT 'Centro contable asociado a descuentos por pronto pago en CxC.',
        CtaProntoPagCxc          STRING    COMMENT 'Cuenta contable asociada a descuentos por pronto pago en CxC.',
        
        CtrIntMoraCxc            STRING    COMMENT 'Centro contable asociado a intereses por mora en CxC.',
        CtaIntMoraCxc            STRING    COMMENT 'Cuenta contable asociada a intereses por mora en CxC.',
        
        CtrRecibosCxc            STRING    COMMENT 'Centro contable asociado a recibos de cuentas por cobrar.',
        CtaRecibosCxc            STRING    COMMENT 'Cuenta contable asociada a recibos de cuentas por cobrar.',
        
        CtrDebitoCxc             STRING    COMMENT 'Centro contable asociado a débitos en cuentas por cobrar.',
        CtaDebitoCxc             STRING    COMMENT 'Cuenta contable asociada a débitos en cuentas por cobrar.',
        
        CtrCreditoCxc            STRING    COMMENT 'Centro contable asociado a créditos en cuentas por cobrar.',
        CtaCreditoCxc            STRING    COMMENT 'Cuenta contable asociada a créditos en cuentas por cobrar.',
        
        CtrImpuesto1Cxc          STRING    COMMENT 'Centro contable asociado al impuesto 1 en CxC.',
        CtaImpuesto1Cxc          STRING    COMMENT 'Cuenta contable asociada al impuesto 1 en CxC.',
        
        CtrImpuesto2Cxc          STRING    COMMENT 'Centro contable asociado al impuesto 2 en CxC.',
        CtaImpuesto2Cxc          STRING    COMMENT 'Cuenta contable asociada al impuesto 2 en CxC.',
        
        CtrRubro1Cxc             STRING    COMMENT 'Centro contable asociado al rubro 1 en CxC.',
        CtaRubro1Cxc             STRING    COMMENT 'Cuenta contable asociada al rubro 1 en CxC.',
        
        CtrRubro2Cxc             STRING    COMMENT 'Centro contable asociado al rubro 2 en CxC.',
        CtaRubro2Cxc             STRING    COMMENT 'Cuenta contable asociada al rubro 2 en CxC.',
        
        CtrAnticipoCxc           STRING    COMMENT 'Centro contable asociado a anticipos de cuentas por cobrar.',
        CtaAnticipoCxc           STRING    COMMENT 'Cuenta contable asociada a anticipos de cuentas por cobrar.',
        
        CtrDescBonif             STRING    COMMENT 'Centro contable asociado a descuentos y bonificaciones.',
        CtaDescBonif             STRING    COMMENT 'Cuenta contable asociada a descuentos y bonificaciones.',
        
        CtrDevVentas             STRING    COMMENT 'Centro contable asociado a devoluciones de ventas.',
        CtaDevVentas             STRING    COMMENT 'Cuenta contable asociada a devoluciones de ventas.',
        
        CtrIntCorriente          STRING    COMMENT 'Centro contable asociado a intereses corrientes.',
        CtaIntCorriente          STRING    COMMENT 'Cuenta contable asociada a intereses corrientes.',
        
        CtrVentasExen            STRING    COMMENT 'Centro contable asociado a ventas exentas.',
        CtaVentasExen            STRING    COMMENT 'Cuenta contable asociada a ventas exentas.',
        
        CtrAjusteRedondeo        STRING    COMMENT 'Centro contable asociado a ajustes por redondeo.',
        CtaAjusteRedondeo        STRING    COMMENT 'Cuenta contable asociada a ajustes por redondeo.',
        
        CtrCobrDudosa            STRING    COMMENT 'Centro contable asociado a cuentas de cobro dudoso.',
        CtaCobrDudosa            STRING    COMMENT 'Cuenta contable asociada a cuentas de cobro dudoso.',
        
        CtrImpBolsaCxc           STRING    COMMENT 'Centro contable asociado al impuesto de bolsa en CxC.',
        CtaImpBolsaCxc           STRING    COMMENT 'Cuenta contable asociada al impuesto de bolsa en CxC.',
        Source                   STRING    COMMENT 'Origen de extracción de datos (Raw).',
        FechaCreacion            TIMESTAMP COMMENT 'Fecha de inserción del registro.',
        FechaModificacion        TIMESTAMP COMMENT 'Fecha de última actualización.'
    )
    USING DELTA
    COMMENT 'Dimensión de Categoría de Cliente simplificada — Finanzas (Gold Layer)'
"""

# ── Registros dummy (se insertan al crear la tabla) 
DummyInsert = """
    INSERT INTO Gold.Finanzas.DimCategoriaCliente (
        SKDCategoriaCliente,
        IdCategoriaCliente,
        CategoriaCliente,
        CtrVentas, CtaVentas,
        CtrDescGral, CtaDescGral,
        CtrCostVent, CtaCostVent,
        CtrDescLin, CtaDescLin,
        CtrCostLin, CtaCostLin,
        CtrVendCom, CtaVendCom,
        CtrCobrCom, CtaCobrCom,
        CtrCxc, CtaCxc,
        CtrLxc, CtaLxc,
        CtrContado, CtaContado,
        CtrProntoPagCxc, CtaProntoPagCxc,
        CtrIntMoraCxc, CtaIntMoraCxc,
        CtrRecibosCxc, CtaRecibosCxc,
        CtrDebitoCxc, CtaDebitoCxc,
        CtrCreditoCxc, CtaCreditoCxc,
        CtrImpuesto1Cxc, CtaImpuesto1Cxc,
        CtrImpuesto2Cxc, CtaImpuesto2Cxc,
        CtrRubro1Cxc, CtaRubro1Cxc,
        CtrRubro2Cxc, CtaRubro2Cxc,
        CtrAnticipoCxc, CtaAnticipoCxc,
        CtrDescBonif, CtaDescBonif,
        CtrDevVentas, CtaDevVentas,
        CtrIntCorriente, CtaIntCorriente,
        CtrVentasExen, CtaVentasExen,
        CtrAjusteRedondeo, CtaAjusteRedondeo,
        CtrCobrDudosa, CtaCobrDudosa,
        CtrImpBolsaCxc, CtaImpBolsaCxc,
        Source,
        FechaCreacion
    )
    VALUES
    (
        -101, '-101', 'NO_APLICA',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A'
        ,'DIM_DUMMY',current_timestamp()
    ),
    (
        -201, '-201', 'NO_ENCONTRADO',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A'
        ,'DIM_DUMMY',current_timestamp()
    ),
    (
        -301, '-301', 'NO_DEFINIDO',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A','N/A',
        'N/A','N/A','N/A','N/A','N/A','N/A'
        ,'DIM_DUMMY',current_timestamp()
    );
"""

# ── Columnas calculadas (no modificar) 
AllCols = [SkCol] + NaturalKey + TrackingCols + ["FechaCreacion", "FechaModificacion"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Bloque con proceso de transformación necesario.  Puede hacer varias queries, procesos a nivel de dataframe etc,  pero finalmente se debe entregar el dataframe dfSource  que satisfaga la dimension en gold, es decir con los datos ya transformados y nombres de columnas iguales a la tabla de destino.

# CELL ********************

# ── Query fuente (Silver → Gold) 
def ReadSilverAndTransform():
    SourceQuery = """
        SELECT 
            -- Clave de negocio
            UPPER(TRIM(concat(CategoriaCliente,"-",Source))) AS IdCategoriaCliente,

            -- Atributo principal
            UPPER(TRIM(CategoriaCliente)) AS CategoriaCliente,

            UPPER(TRIM(CtrVentas)) AS CtrVentas,
            UPPER(TRIM(CtaVentas)) AS CtaVentas,

            UPPER(TRIM(CtrDescGral)) AS CtrDescGral,
            UPPER(TRIM(CtaDescGral)) AS CtaDescGral,

            UPPER(TRIM(CtrCostVent)) AS CtrCostVent,
            UPPER(TRIM(CtaCostVent)) AS CtaCostVent,

            UPPER(TRIM(CtrDescLin)) AS CtrDescLin,
            UPPER(TRIM(CtaDescLin)) AS CtaDescLin,

            UPPER(TRIM(CtrCostLin)) AS CtrCostLin,
            UPPER(TRIM(CtaCostLin)) AS CtaCostLin,

            UPPER(TRIM(CtrVendCom)) AS CtrVendCom,
            UPPER(TRIM(CtaVendCom)) AS CtaVendCom,

            UPPER(TRIM(CtrCobrCom)) AS CtrCobrCom,
            UPPER(TRIM(CtaCobrCom)) AS CtaCobrCom,

            UPPER(TRIM(CtrCxc)) AS CtrCxc,
            UPPER(TRIM(CtaCxc)) AS CtaCxc,

            UPPER(TRIM(CtrLxc)) AS CtrLxc,
            UPPER(TRIM(CtaLxc)) AS CtaLxc,

            UPPER(TRIM(CtrContado)) AS CtrContado,
            UPPER(TRIM(CtaContado)) AS CtaContado,

            UPPER(TRIM(CtrProntoPagCxc)) AS CtrProntoPagCxc,
            UPPER(TRIM(CtaProntoPagCxc)) AS CtaProntoPagCxc,

            UPPER(TRIM(CtrIntMoraCxc)) AS CtrIntMoraCxc,
            UPPER(TRIM(CtaIntMoraCxc)) AS CtaIntMoraCxc,

            UPPER(TRIM(CtrRecibosCxc)) AS CtrRecibosCxc,
            UPPER(TRIM(CtaRecibosCxc)) AS CtaRecibosCxc,

            UPPER(TRIM(CtrDebitoCxc)) AS CtrDebitoCxc,
            UPPER(TRIM(CtaDebitoCxc)) AS CtaDebitoCxc,

            UPPER(TRIM(CtrCreditoCxc)) AS CtrCreditoCxc,
            UPPER(TRIM(CtaCreditoCxc)) AS CtaCreditoCxc,

            UPPER(TRIM(CtrImpuesto1Cxc)) AS CtrImpuesto1Cxc,
            UPPER(TRIM(CtaImpuesto1Cxc)) AS CtaImpuesto1Cxc,

            UPPER(TRIM(CtrImpuesto2Cxc)) AS CtrImpuesto2Cxc,
            UPPER(TRIM(CtaImpuesto2Cxc)) AS CtaImpuesto2Cxc,

            UPPER(TRIM(CtrRubro1Cxc)) AS CtrRubro1Cxc,
            UPPER(TRIM(CtaRubro1Cxc)) AS CtaRubro1Cxc,

            UPPER(TRIM(CtrRubro2Cxc)) AS CtrRubro2Cxc,
            UPPER(TRIM(CtaRubro2Cxc)) AS CtaRubro2Cxc,

            UPPER(TRIM(CtrAnticipoCxc)) AS CtrAnticipoCxc,
            UPPER(TRIM(CtaAnticipoCxc)) AS CtaAnticipoCxc,

            UPPER(TRIM(CtrDescBonif)) AS CtrDescBonif,
            UPPER(TRIM(CtaDescBonif)) AS CtaDescBonif,

            UPPER(TRIM(CtrDevVentas)) AS CtrDevVentas,
            UPPER(TRIM(CtaDevVentas)) AS CtaDevVentas,

            UPPER(TRIM(CtrIntCorriente)) AS CtrIntCorriente,
            UPPER(TRIM(CtaIntCorriente)) AS CtaIntCorriente,

            UPPER(TRIM(CtrVentasExen)) AS CtrVentasExen,
            UPPER(TRIM(CtaVentasExen)) AS CtaVentasExen,

            UPPER(TRIM(CtrAjusteRedondeo)) AS CtrAjusteRedondeo,
            UPPER(TRIM(CtaAjusteRedondeo)) AS CtaAjusteRedondeo,

            UPPER(TRIM(CtrCobrDudosa)) AS CtrCobrDudosa,
            UPPER(TRIM(CtaCobrDudosa)) AS CtaCobrDudosa,

            UPPER(TRIM(CtrImpBolsaCxc)) AS CtrImpBolsaCxc,
            UPPER(TRIM(CtaImpBolsaCxc)) AS CtaImpBolsaCxc,

            UPPER(TRIM(Source)) AS Source,
            current_timestamp() AS FechaCreacion

        FROM Silver.Finanzas.CategoriaCliente;
    """

    dfSource = spark.sql(SourceQuery)

    
    return dfSource

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creación de tabla destino

# CELL ********************

if not spark.catalog.tableExists(GoldTable):
    spark.sql(DDLQuery)
    spark.sql(DummyInsert)
    print(f"Tabla {GoldTable} creada con registros dummy.")
else:
    print(f"Tabla {GoldTable} ya existe.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Proceso de Carga SCD Tipo 1
# 
# Lógica genérica:
# 1. Lee datos fuente y destino; materializa ambos **antes** de cualquier escritura.
# 2. Identifica registros nuevos (anti-join por Natural Key) y asigna SK secuencial.
# 3. Ejecuta MERGE para actualizar registros existentes que cambiaron.
# 4. Inserta registros nuevos ya materializados.
# 5. Genera resumen JSON con conteos y estado.

# CELL ********************

process_start = datetime.now()

result = {
    "Status":              "success",
    "TableName":               GoldTable,
    "SourceRecords":      0,
    "RecordsTargetBefore": 0,
    "RecordsInserted":    0,
    "RecordsUpdated":     0,
    "Message":       None,
    "EventTimestamp":          process_start.isoformat(),
    "EndTime":            None,
}

try:
    # ── 1. Persistir la fuente para evitar reevaluaciones lazy
    dfSource = ReadSilverAndTransform()
    result["SourceRecords"] = dfSource.cache().count()

    # ── 2. Snapshot del destino (materializado antes de cualquier escritura) ──
    deltaTarget = DeltaTable.forName(spark, GoldTable)
    dfTarget = deltaTarget.toDF().cache()
    result["RecordsTargetBefore"] = dfTarget.count()

    maxSk = (
        dfTarget
        .filter(col(SkCol) > 0)
        .agg(coalesce(spark_max(col(SkCol)), lit(0)).alias("maxSk"))
        .first()["maxSk"]
    )

    # ── 3. Identificar registros NUEVOS y asignar SK 
    #    left_anti: registros en fuente que NO existen en destino por NK
    dfNew = dfSource.join(
        dfTarget.select(*NaturalKey),
        on=NaturalKey,
        how="left_anti"
    )

    wSk = Window.orderBy(*NaturalKey)
    dfNew = (
        dfNew
        .withColumn(SkCol, row_number().over(wSk) + lit(maxSk))
        .withColumn("FechaCreacion", now)
        .withColumn("FechaModificacion", lit(None).cast("timestamp"))
        .select(*AllCols)
    )

    # MATERIALIZAR nuevos ANTES del merge para evitar re-evaluación lazy
    # que leería la tabla ya modificada y causaría huecos en los SK.
    dfNew = dfNew.cache()
    newCount = dfNew.count()

    # ── 4. MERGE: actualizar existentes que cambiaron 
    mergeCondition = " AND ".join([f"t.{k} = s.{k}" for k in NaturalKey])

    changeCondition = " OR ".join([
        f"coalesce(cast(t.{c} as string), '') <> coalesce(cast(s.{c} as string), '')"
        for c in TrackingCols
    ])

    updateSet = {c: col(f"s.{c}") for c in TrackingCols}
    updateSet["FechaModificacion"] = now

    deltaTarget.alias("t").merge(
        dfSource.alias("s"),
        mergeCondition
    ).whenMatchedUpdate(
        condition=changeCondition,
        set=updateSet
    ).execute()

    # Obtener conteo de actualizaciones desde el historial de Delta
    mergeMetrics = (
        deltaTarget.history(1)
        .select("operationMetrics")
        .first()["operationMetrics"]
    )
    result["records_updated"] = int(mergeMetrics.get("numTargetRowsUpdated", "0"))

    # ── 5. INSERT de registros nuevos (ya materializados) 
    if newCount > 0:
        dfNew.write.format("delta").mode("append").insertInto(GoldTable)

    result["RecordsInserted"] = newCount

except Exception as e:
    result["Status"] = "error"
    result["Message"] = str(e)

finally:
    process_end = datetime.now()
    result["EndTime"] = process_end.isoformat()
    result["ExecutionTimeSeconds"] = round(
        (process_end - process_start).total_seconds(), 2
    )

    # Liberar caché
    try:
        dfSource.unpersist()
        dfTarget.unpersist()
        dfNew.unpersist()
    except Exception:
        pass

    #Registrar Log
    Log.LogData(result=result,data_zone='Gold', execution_artifact='DimEmpresa' )

    # Salida para pipelines — permite capturar el resultado con notebook.exit()
    mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
