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

# # Plantilla Carga Dimensión — Tipo Tiempo

# CELL ********************

from pyspark.sql.functions import (col, lit, coalesce, row_number, max as spark_max)
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


# CELL ********************

# ── Parámetros de generación del calendario
StartDate = "2009-01-01"
EndDate   = "2030-12-31"

# ── Tabla destino
GoldTable    = "Gold.Transversales.DimTiempo"
SkCol        = "SKTiempo"
NaturalKey   = ["Fecha"]
TrackingCols = [
    "Año", "Mes", "Dia", "SemanaAño", "Trimestre",
    "NombreMesEN", "NombreDiaEN", "FechaISO",
    "NombreMes", "NombreDia", "NombreTrimestre",
    "MesAño", "FechaLarga", "AñoMesNombreMes"
]

DDLQuery = """
    CREATE TABLE IF NOT EXISTS Gold.Transversales.DimTiempo (
        SKTiempo          INT       COMMENT 'Clave subrogada (Surrogate Key).',
        Fecha             DATE      COMMENT 'Fecha calendario (Natural Key).',
        `Año`             INT       COMMENT 'Año de la fecha.',
        Mes               INT       COMMENT 'Número de mes (1-12).',
        Dia               INT       COMMENT 'Número de día del mes.',
        `SemanaAño`       INT       COMMENT 'Número de semana del año (ISO).',
        Trimestre         INT       COMMENT 'Trimestre del año (1-4).',
        NombreMesEN       STRING    COMMENT 'Nombre del mes en inglés.',
        NombreDiaEN       STRING    COMMENT 'Nombre del día de la semana en inglés.',
        FechaISO          STRING    COMMENT 'Fecha en formato ISO yyyy-MM-dd.',
        NombreMes         STRING    COMMENT 'Nombre del mes en español.',
        NombreDia         STRING    COMMENT 'Nombre del día de la semana en español.',
        NombreTrimestre   STRING    COMMENT 'Nombre del trimestre en español (ej: Primer Trimestre).',
        `MesAño`          STRING    COMMENT 'Concatenación Mes y Año (ej: Enero 2024).',
        FechaLarga        STRING    COMMENT 'Descripción larga de la fecha en español.',
        `AñoMesNombreMes` STRING    COMMENT 'Formato Año - MM - NombreMes para ordenamiento.',
        FechaCreacion     TIMESTAMP COMMENT 'Fecha de inserción del registro.',
        FechaModificacion TIMESTAMP COMMENT 'Fecha de última actualización.'
    )
    USING DELTA
    COMMENT 'Dimensión de Tiempo — Transversales (Gold Layer)'
"""

# ── Registros dummy (se insertan al crear la tabla)
DummyInsert = """
    INSERT INTO Gold.Transversales.DimTiempo
        (SKTiempo, Fecha, `Año`, Mes, Dia, `SemanaAño`, Trimestre,
         NombreMesEN, NombreDiaEN, FechaISO,
         NombreMes, NombreDia, NombreTrimestre, `MesAño`, FechaLarga, `AñoMesNombreMes`, FechaCreacion)
    VALUES
        (19000101, CAST('1900-01-01' AS DATE), 0, 0, 0, 0, 0, 'N/A', 'N/A', '1900-01-01', 'No Aplica',     'N/A', 'N/A', 'N/A', 'No Aplica',            'N/A', current_timestamp()),
        (19000102, CAST('1900-01-02' AS DATE), 0, 0, 0, 0, 0, 'N/A', 'N/A', '1900-01-02', 'No Encontrado', 'N/A', 'N/A', 'N/A', 'No se encontró valor', 'N/A', current_timestamp()),
        (19000103, CAST('1900-01-03' AS DATE), 0, 0, 0, 0, 0, 'N/A', 'N/A', '1900-01-03', 'No Definido',   'N/A', 'N/A', 'N/A', 'No Definido',          'N/A', current_timestamp())
"""

# ── Columnas calculadas (no modificar)
AllCols = [SkCol] + NaturalKey + TrackingCols + ["FechaCreacion", "FechaModificacion"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transformación y Generación de Atributos
#  A diferencia de otras dimensiones, DimTiempo no consume datos de la capa Silver.
#  Se genera programáticamente mediante una secuencia de fechas dentro de un rango definido.
#  
#  Lógica de Negocio:
#  1. Se genera una serie temporal de días entre StartDate y EndDate.
#  2. Se extraen atributos granulares (Año, Mes, Día, etc.) usando funciones nativas de Spark SQL.
#  3. Se aplica traducción manual para los nombres de meses y días al español.
#  4. El SK (Surrogate Key) se define con formato 'yyyyMMdd' como un entero para optimizar 
#     el rendimiento de los JOINS en el modelo estrella (Fact Tables).

# CELL ********************

def ReadSilverAndTransform():
    from pyspark.sql.functions import when, concat_ws, lpad, expr, col

    # ── 1. Generar secuencia de fechas
    dates_df = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{StartDate}'),
            to_date('{EndDate}'),
            interval 1 day
        )) AS Fecha
    """)

    # ── 2. Atributos base (valores numéricos y nombres en inglés)
    temp_df = dates_df.select(
        col("Fecha"),
        expr("year(Fecha)").alias("Año"),
        expr("month(Fecha)").alias("Mes"),
        expr("day(Fecha)").alias("Dia"),
        expr("weekofyear(Fecha)").alias("SemanaAño"),
        expr("quarter(Fecha)").alias("Trimestre"),
        expr("date_format(Fecha, 'MMMM')").alias("NombreMesEN"),
        expr("date_format(Fecha, 'EEEE')").alias("NombreDiaEN"),
        expr("date_format(Fecha, 'yyyy-MM-dd')").alias("FechaISO"),
    )

    # ── 3. Traducción al español y columnas calculadas
    dfSource = temp_df.select(
        "*",
        when(col("NombreMesEN") == "January",   "Enero")
        .when(col("NombreMesEN") == "February",  "Febrero")
        .when(col("NombreMesEN") == "March",     "Marzo")
        .when(col("NombreMesEN") == "April",     "Abril")
        .when(col("NombreMesEN") == "May",       "Mayo")
        .when(col("NombreMesEN") == "June",      "Junio")
        .when(col("NombreMesEN") == "July",      "Julio")
        .when(col("NombreMesEN") == "August",    "Agosto")
        .when(col("NombreMesEN") == "September", "Septiembre")
        .when(col("NombreMesEN") == "October",   "Octubre")
        .when(col("NombreMesEN") == "November",  "Noviembre")
        .when(col("NombreMesEN") == "December",  "Diciembre")
        .alias("NombreMes"),
        when(col("NombreDiaEN") == "Monday",    "Lunes")
        .when(col("NombreDiaEN") == "Tuesday",  "Martes")
        .when(col("NombreDiaEN") == "Wednesday", "Miércoles")
        .when(col("NombreDiaEN") == "Thursday", "Jueves")
        .when(col("NombreDiaEN") == "Friday",   "Viernes")
        .when(col("NombreDiaEN") == "Saturday", "Sábado")
        .when(col("NombreDiaEN") == "Sunday",   "Domingo")
        .alias("NombreDia"),
        concat_ws(" ",
            when(col("Trimestre") == 1, "Primer")
            .when(col("Trimestre") == 2, "Segundo")
            .when(col("Trimestre") == 3, "Tercer")
            .when(col("Trimestre") == 4, "Cuarto"),
            lit("Trimestre")
        ).alias("NombreTrimestre"),
        concat_ws(" ", col("NombreMes"), col("Año")).alias("MesAño"),
        concat_ws(" ",
            col("NombreDia"), lit(","), col("Dia"),
            lit("de"), col("NombreMes"), lit("de"), col("Año")
        ).alias("FechaLarga"),
        concat_ws(" - ",
            col("Año"),
            lpad(col("Mes").cast("string"), 2, "0"),
            col("NombreMes")
        ).alias("AñoMesNombreMes"),
    )

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

# CELL ********************

process_start = datetime.now()

result = {
    "Status":              "success",
    "TableName":           GoldTable,
    "SourceRecords":       0,
    "RecordsTargetBefore": 0,
    "RecordsInserted":     0,
    "RecordsUpdated":      0,
    "Message":             None,
    "EventTimestamp":      process_start.isoformat(),
    "EndTime":             None,
}

try:
    # ── 1. Persistir la fuente para evitar reevaluaciones lazy
    dfSource = ReadSilverAndTransform()
    result["SourceRecords"] = dfSource.cache().count()

    # ── 2. Snapshot del destino (materializado antes de cualquier escritura)
    deltaTarget = DeltaTable.forName(spark, GoldTable)
    dfTarget = deltaTarget.toDF().cache()
    result["RecordsTargetBefore"] = dfTarget.count()

    maxSk = (
        dfTarget
        .filter(col(SkCol) > 0)
        .agg(coalesce(spark_max(col(SkCol)), lit(0)).alias("maxSk"))
        .first()["maxSk"]
    )

    # ── 3. Identificar registros NUEVOS y asignar SK como YYYYMMDD
    from pyspark.sql.functions import date_format, current_timestamp

    dfNew = dfSource.join(
        dfTarget.select(*NaturalKey),
        on=NaturalKey,
        how="left_anti"
    )

    dfNew = (
        dfNew
        .withColumn(SkCol, date_format(col("Fecha"), "yyyyMMdd").cast("int")) 
        .withColumn("FechaCreacion", current_timestamp())
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
    f"coalesce(cast(t.`{c}` as string), '') <> coalesce(cast(s.`{c}` as string), '')"
    for c in TrackingCols
    ])

    updateSet = {f"`{c}`": col(f"s.`{c}`") for c in TrackingCols}
    updateSet["FechaModificacion"] = lit(datetime.now())

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

    # Registrar Log
    Log.LogData(result=result, data_zone='Gold', execution_artifact='DimTiempo')

    # Salida para pipelines — permite capturar el resultado con notebook.exit()
    mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
