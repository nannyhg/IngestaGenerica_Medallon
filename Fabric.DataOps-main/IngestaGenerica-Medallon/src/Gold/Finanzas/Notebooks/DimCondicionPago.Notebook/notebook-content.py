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
GoldTable    = "Gold.Finanzas.DimCondicionPago"
SkCol        = "SKCondicionPago"
NaturalKey   = ["IdCondicionPago"]
TrackingCols = [
    "CondicionPago","Descripcion","DiasNeto","PagosParciales","DescuentoContado","Source"
]

# ── DDL de creación (solo si la tabla no existe) 
DDLQuery = """
    CREATE TABLE IF NOT EXISTS Gold.Finanzas.DimCondicionPago (
        SKCondicionPago               BIGINT    COMMENT 'Clave subrogada (Surrogate Key).',
        IdCondicionPago               STRING    COMMENT 'Identificador compuesto de la dimensión por Condición de pago y Source.',
        CondicionPago                 STRING    COMMENT 'Identificador de la Condición de pago en la fuente.',
        Descripcion                   STRING    COMMENT 'Descripción de la condición de pago.',
        DiasNeto                      STRING    COMMENT 'Días netos de la condición de pago.',
        PagosParciales                STRING    COMMENT 'Pagos parciales de la condición de pago.',
        DescuentoContado              STRING    COMMENT 'Descuento contado de la condición de pago.',
        Source                        STRING    COMMENT 'Origen de extracción de datos (Raw).',
        FechaCreacion                 TIMESTAMP COMMENT 'Fecha de inserción del registro.',
        FechaModificacion             TIMESTAMP COMMENT 'Fecha de última actualización.'
    )
    USING DELTA
    COMMENT 'Dimensión de Condición de Pago — Finanzas (Gold Layer)'
"""

# ── Registros dummy (se insertan al crear la tabla) 
DummyInsert = """
    INSERT INTO Gold.Finanzas.DimCondicionPago
        (SKCondicionPago, IdCondicionPago, CondicionPago, Descripcion, DiasNeto, PagosParciales, DescuentoContado, Source, FechaCreacion)
    VALUES
        (-101, '-101', 'NO_APLICA',     'No Aplica',            'N/A','N/A','N/A', 'DIM_DUMMY', current_timestamp()),
        (-201, '-201', 'NO_ENCONTRADO', 'No se encontró valor', 'N/A','N/A','N/A', 'DIM_DUMMY', current_timestamp()),
        (-301, '-301', 'NO_DEFINIDO',   'No Definido',          'N/A','N/A','N/A', 'DIM_DUMMY', current_timestamp())
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
            UPPER(TRIM(concat(CondicionPago, '-', Source))) AS IdCondicionPago,
            UPPER(TRIM(CondicionPago)) AS CondicionPago,
            UPPER(TRIM(Descripcion)) AS Descripcion,
            UPPER(TRIM(DiasNeto)) AS DiasNeto,
            UPPER(TRIM(PagosParciales)) AS PagosParciales,
            UPPER(TRIM(DescuentoContado)) AS DescuentoContado,
            UPPER(TRIM(Source)) AS Source
        FROM Silver.Finanzas.CondicionPago
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
    Log.LogData(result=result,data_zone='Gold', execution_artifact='DimPais' )

    # Salida para pipelines — permite capturar el resultado con notebook.exit()
    mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
