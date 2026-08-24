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

# CELL ********************

Log = DataOpsLog()

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
       WITH ClienteBase AS
       (
       -- Calculamos una sola vez el Id y el nombre normalizado
       SELECT
              concat(TRIM(Cl.Cliente), '-', TRIM(Cl.Source))                              AS IdCliente,
              REPLACE(REPLACE(UPPER(TRIM(COALESCE(Cl.Nombre,''))), ',', ''), '.', '')     AS NombreNormalizado,
              Cl.*
       FROM  Silver.Finanzas.Cliente Cl
       ),
       Equivalencias AS
       (
       -- Buscamos la equivalencia (EQUALS o CONTAINS) y nos quedamos con la mejor
       SELECT
              Cb.IdCliente,
              Eq.TargetValue,
              ROW_NUMBER() OVER (
              PARTITION BY Cb.IdCliente
              ORDER BY CASE Eq.LookupType WHEN 'EQUALS' THEN 1 ELSE 2 END
              ) AS IdFila
       FROM  ClienteBase Cb
       JOIN  Bronze.BDDataOps.DataEquivalence Eq
              ON  Eq.TargetValue IS NOT NULL
              AND (
                     ( Eq.LookupType = 'EQUALS'   AND Cb.NombreNormalizado = TRIM(Eq.SourceValue) )
              OR ( Eq.LookupType = 'CONTAINS' AND Cb.NombreNormalizado LIKE concat('%', TRIM(Eq.SourceValue), '%') )
                     )
    ),
    ClienteTransformado AS
    (
    SELECT
        Cb.IdCliente,
        Cb.Cliente,
        Cb.Nombre,
        COALESCE(Eq.TargetValue, Cb.NombreNormalizado, '') AS NombreEstandar,
        Cb.DetalleDireccion,
        Cb.Alias,
        Cb.Contacto,
        Cb.Cargo,
        Cb.Direccion,
        Cb.DirEmbDefault,
        Cb.Telefono1,
        Cb.Telefono2,
        Cb.Fax,
        Cb.Contribuyente,
        Cb.FechaIngreso,
        Cb.Multimoneda,
        Cb.Moneda,
        Cb.LimiteCredito,
        Cb.ExcederLimite,
        Cb.TasaInteres,
        Cb.TasaInteresMora,
        Cb.CondicionPago,
        Cb.Pais,
        Cb.Zona,
        Cb.Ruta,
        Cb.Vendedor,
        Cb.Cobrador,
        Cb.Activo,
        Cat.Descripcion AS CategoriaCliente,
        Cb.ClaseAbc,
        Cb.EMail,
        Cb.Notas,
        Cb.DiasPromedAtraso,
        Cb.DiasDeCobro,
        Cb.EmailDocElectronico,
        Cb.EmailPedEdi,
        Cb.CodigoImpuesto,
        Cb.EmailDocElectronicoCopia,
        Cb.TipoImpuesto,
        Cb.Source
    FROM       ClienteBase Cb
    LEFT JOIN  Silver.Finanzas.CategoriaCliente Cat
        ON  Cat.CategoriaCliente = Cb.CategoriaCliente
        AND Cat.Source           = Cb.Source
    LEFT JOIN  Equivalencias Eq
        ON  Eq.IdCliente = Cb.IdCliente
        AND Eq.IdFila    = 1
    ),
    EquivalenciasClientePadrino AS
    (
    SELECT
        TRIM(SourceValue) AS SourceValue,
        TargetValue,
        ROW_NUMBER() OVER (
        PARTITION BY TRIM(SourceValue)
        ORDER BY Priority, IdEquivalence
        ) AS IdFila
    FROM  Bronze.BDDataOps.DataEquivalence
    WHERE MappingGroup = 'Cliente-Padrino'
      AND IsActive = 1
      AND LookupType = 'EQUALS'
       )
       SELECT
        Ct.*,
        EqCp.TargetValue AS Vertical,
        EqCp.TargetValue AS PadrinoRegional
    FROM       ClienteTransformado Ct
    LEFT JOIN  EquivalenciasClientePadrino EqCp
        ON  Ct.NombreEstandar = EqCp.SourceValue
        AND EqCp.IdFila       = 1
       """
       
       dfSource = spark.sql(SourceQuery)

       ComplementQuery = """
       SELECT concat(TRIM(Cl.Cliente), '-', TRIM(Cl.Source))   AS IdCliente,
              COALESCE(Imp.Descripcion, 'NO_DEFINIDO')          AS DescImpuesto,
              COALESCE(Cp.Descripcion,  'NO_DEFINIDO')          AS DescCondicionPago,
              COALESCE(Cat.Descripcion, 'NO_DEFINIDO')          AS DescCategoriaCliente,
              COALESCE(Mon.Nombre,      'NO_DEFINIDO')          AS NombreMoneda,
              COALESCE(Pa.Nombre,       'NO_DEFINIDO')          AS NombrePais,
              COALESCE(Ve.Nombre,       'NO_DEFINIDO')          AS NombreVendedor
       FROM   Silver.Finanzas.Cliente Cl
       LEFT JOIN Silver.Finanzas.Impuesto Imp
              ON  Cl.Source          = Imp.Source
             AND  Cl.CodigoImpuesto  = Imp.Impuesto
       LEFT JOIN Silver.Finanzas.CondicionPago Cp
              ON  Cl.Source          = Cp.Source
             AND  Cl.CondicionPago   = Cp.CondicionPago
       LEFT JOIN Silver.Finanzas.CategoriaCliente Cat
              ON  Cl.Source          = Cat.Source
             AND  Cl.CategoriaCliente = Cat.CategoriaCliente
       LEFT JOIN Silver.Finanzas.Moneda Mon
              ON  Cl.Source          = Mon.Source
             AND  Cl.Moneda          = Mon.Moneda
       LEFT JOIN Silver.Finanzas.Pais Pa
              ON  Cl.Source          = Pa.Source
             AND  Cl.Pais            = Pa.Pais
       LEFT JOIN Silver.Finanzas.Vendedor Ve
              ON  Cl.Source          = Ve.Source
             AND  Cl.Vendedor        = Ve.Vendedor
       """

       dfComplement = spark.sql(ComplementQuery)

       dfSource2 = dfSource.join(
              dfComplement.select(
                     "IdCliente",
                     "DescImpuesto",
                     "DescCondicionPago",
                     "DescCategoriaCliente",
                     "NombreMoneda",
                     "NombrePais",
                     "NombreVendedor",
              ),
              on="IdCliente",
              how="left"
       )
       return dfSource2

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
GoldTable    = "Gold.Finanzas.DimCliente"
SkCol        = "SKCliente"
NaturalKey   = ["IdCliente"]
TrackingCols = [
    "Cliente",
    "Nombre",
    "NombreEstandar",
    "DetalleDireccion",
    "Alias",
    "Contacto",
    "Cargo",
    "Direccion",
    "DirEmbDefault",
    "Telefono1",
    "Telefono2",
    "Fax",
    "Contribuyente",
    "FechaIngreso",
    "Multimoneda",
    "Moneda",
    "LimiteCredito",
    "ExcederLimite",
    "TasaInteres",
    "TasaInteresMora",
    "CondicionPago",
    "Pais",
    "Zona",
    "Ruta",
    "Vendedor",
    "Cobrador",
    "Activo",
    "CategoriaCliente",
    "ClaseAbc",
    "EMail",
    "Notas",
    "DiasPromedAtraso",
    "DiasDeCobro",
    "EmailDocElectronico",
    "EmailPedEdi",
    "CodigoImpuesto",
    "EmailDocElectronicoCopia",
    "TipoImpuesto",
    "Source",
    "Vertical",
    "PadrinoRegional",
    "DescImpuesto",
    "DescCondicionPago",
    "DescCategoriaCliente",
    "NombreMoneda",
    "NombrePais",
    "NombreVendedor",
]

# ── DDL de creación (solo si la tabla no existe) 
DDLQuery = """
    CREATE TABLE IF NOT EXISTS Gold.Finanzas.DimCliente (
        SKCliente               BIGINT      COMMENT 'Clave subrogada (Surrogate Key).',
        IdCliente               STRING      COMMENT 'Identificador compuesto del cliente: Cliente-Source.',
        Cliente                 STRING      COMMENT 'Código identificador único del cliente (PK origen).',
        Nombre                  STRING      COMMENT 'Nombre completo o razón social del cliente.',
        NombreEstandar          STRING      COMMENT 'Nombre normalizado/equivalente de negocio.',
        DetalleDireccion        DOUBLE      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Alias                   STRING      COMMENT 'Alias o nombre comercial del cliente.',
        Contacto                STRING      COMMENT 'Nombre de la persona de contacto principal.',
        Cargo                   STRING      COMMENT 'Cargo de la persona de contacto principal.',
        Direccion               STRING      COMMENT 'Dirección física principal del cliente.',
        DirEmbDefault           STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Telefono1               STRING      COMMENT 'Primer número de teléfono de contacto.',
        Telefono2               STRING      COMMENT 'Segundo número de teléfono de contacto.',
        Fax                     STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Contribuyente           STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        FechaIngreso            TIMESTAMP   COMMENT 'Fecha en la que el cliente fue registrado.',
        Multimoneda             STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Moneda                  STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        LimiteCredito           DOUBLE      COMMENT 'Monto máximo de crédito otorgado al cliente.',
        ExcederLimite           STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        TasaInteres             DOUBLE      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        TasaInteresMora         DOUBLE      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        CondicionPago           STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Pais                    STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Zona                    STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Ruta                    STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Vendedor                STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Cobrador                STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Activo                  STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        CategoriaCliente        STRING      COMMENT 'Categoría homologada del cliente.',
        ClaseAbc                STRING      COMMENT 'Clasificación ABC del cliente.',
        EMail                   STRING      COMMENT 'Correo electrónico principal del cliente.',
        Notas                   STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        DiasPromedAtraso        DOUBLE      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        DiasDeCobro             STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        EmailDocElectronico     STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        EmailPedEdi             STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        CodigoImpuesto          STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        EmailDocElectronicoCopia STRING     COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        TipoImpuesto            STRING      COMMENT 'Campo descriptivo o de configuración asociado al cliente.',
        Source                  STRING      COMMENT 'Origen de datos.',
        Vertical                STRING      COMMENT 'Vertical derivada por equivalencia Cliente-Padrino.',
        PadrinoRegional         STRING      COMMENT 'Padrino regional derivado por equivalencia Cliente-Padrino.',
        DescImpuesto            STRING      COMMENT 'Descripción del código de impuesto.',
        DescCondicionPago       STRING      COMMENT 'Descripción de la condición de pago.',
        DescCategoriaCliente    STRING      COMMENT 'Descripción de la categoría del cliente.',
        NombreMoneda            STRING      COMMENT 'Nombre de la moneda del cliente.',
        NombrePais              STRING      COMMENT 'Nombre del país del cliente.',
        NombreVendedor          STRING      COMMENT 'Nombre del vendedor asignado al cliente.',
        FechaCreacion           TIMESTAMP   COMMENT 'Fecha de inserción del registro.',
        FechaModificacion       TIMESTAMP   COMMENT 'Fecha de última actualización.'
    )
    USING DELTA
    COMMENT 'Dimensión de Clientes — Finanzas (Gold Layer)'
"""

# ── Registros dummy (se insertan al crear la tabla) 
DummyInsert = """
    INSERT INTO Gold.Finanzas.DimCliente
        (SKCliente, IdCliente, Cliente, Nombre, NombreEstandar, Activo, Source, Vertical, PadrinoRegional)
    VALUES
        (-101, '-101', 'NO_APLICA',     'No Aplica',            'NO_APLICA',     'N', 'DIM_DUMMY', 'NO_APLICA',     'NO_APLICA'),
        (-201, '-201', 'NO_ENCONTRADO', 'No se encontró valor', 'NO_ENCONTRADO', 'N', 'DIM_DUMMY', 'NO_ENCONTRADO', 'NO_ENCONTRADO'),
        (-301, '-301', 'NO_DEFINIDO',   'No Definido',          'NO_DEFINIDO',   'N', 'DIM_DUMMY', 'NO_DEFINIDO',   'NO_DEFINIDO')
"""

# ── Columnas calculadas (no modificar) 
AllCols = [SkCol] + NaturalKey + TrackingCols + ["FechaCreacion", "FechaModificacion"]

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
    dfNew = dfNew.withColumn(SkCol, row_number().over(wSk) + lit(maxSk))
    dfNew = dfNew.withColumn("FechaCreacion", now)

    select_cols = [col(c) for c in AllCols if c not in ["FechaCreacion", "FechaModificacion"]]
    select_cols.extend([
        coalesce(col("FechaCreacion"), now).alias("FechaCreacion"),
        lit(None).cast("timestamp").alias("FechaModificacion")
    ])
    dfNew = dfNew.select(*select_cols)

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
    Log.LogData(result=result,data_zone='Gold', execution_artifact='DimCliente' )

    # Salida para pipelines — permite capturar el resultado con notebook.exit()
    mssparkutils.notebook.exit(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
