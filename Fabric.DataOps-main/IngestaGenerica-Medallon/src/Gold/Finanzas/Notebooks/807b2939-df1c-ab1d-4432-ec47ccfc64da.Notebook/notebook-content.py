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

# CELL ********************

import re
import json
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel
from delta.tables import DeltaTable

Log = DataOpsLog()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CONFIGURACIÓN — ÚNICA SECCIÓN A MODIFICAR POR TABLA DE HECHOS
# ============================================================

# Nombre completo Gold: Catálogo.Esquema.Nombre de la tabla
GoldTable = "Gold.Finanzas.FactVenta"

# Clave de negocio: identifica unívocamente un registro.
# Ninguna columna de NaturalKey debe aparecer en TrackingCols ni SKCols.
NaturalKey = ["Documento", "Linea", "IdEmpresa"]

# Columnas de clave sustituta (SKs).
# Se incluyen en INSERT y UPDATE del MERGE para mantener el modelo estrella
# actualizado cuando se recargan las dimensiones.
SKCols = [
    "SKEmpresa",
    "SKArticulo",
    "SKCliente",
    "SKVendedor",
    "SKImpuesto",
    "SKTiempo",
    "SKClasificacion",
    "SKCentroCosto",
    "SKCuentaContable",
    "SKUsuario",
    "SKActividadComercial",
]

# Columnas medidas / atributos.
# NO incluir columnas de NaturalKey ni SKCols.
TrackingCols = [
    "TipoDoc",
    "Pedido",
    "FechaFactura",
    "MontoImpuesto",
    "MonedaFactura",
    "TipoCambio",
    "MontoCOL",
    "MontoDOL",
    "MontoDesctoGenCOL",
    "MontoDesctoGenDOL",
    "Estado",
    "CondicionPago",
    "Bodega",
    "CostoCOL",
    "OrdenCompra",
    "BaseImpuesto",
    "DocFiscal",
    "DescripcionLinea",
]

# Validación de integridad referencial por FK.
# Formato: (columna_fk_en_fact, tabla_dimension, pk_en_dimension)
FKValidations = [
    ("SKEmpresa",            "Gold.Finanzas.DimEmpresa",            "SKEmpresa"),
    ("SKArticulo",           "Gold.Finanzas.DimArticulo",           "SKArticulo"),
    ("SKCliente",            "Gold.Finanzas.DimCliente",            "SKCliente"),
    ("SKVendedor",           "Gold.Finanzas.DimVendedor",           "SKVendedor"),
    ("SKImpuesto",           "Gold.Finanzas.DimImpuesto",           "SKImpuesto"),
    ("SKClasificacion",      "Gold.Finanzas.DimClasificacion",      "SKClasificacion"),
    ("SKCentroCosto",        "Gold.Finanzas.DimCentroCosto",        "SKCentroCosto"),
    ("SKCuentaContable",     "Gold.Finanzas.DimCuentaContable",     "SKCuentaContable"),
    ("SKUsuario",            "Gold.Finanzas.DimUsuario",            "SKUsuario"),
    ("SKActividadComercial", "Gold.Finanzas.DimActividadComercial", "SKActividadComercial"),
    # DimTiempo se valida mediante SKTiempo (INT yyyyMMdd), no se incluye aquí
    # porque su PK es un entero derivado y la validación es implícita.
]

# Umbral de filas huérfanas por FK antes de marcar HasFKWarnings=True.
# 0 = cualquier huérfana activa la alerta (recomendado para prod).
FK_ORPHAN_THRESHOLD = 0

# SortCol debe ser timestamp de versión (Recorddate/Createdate),
# NO una columna de fecha de negocio (como Fecha).
# Usar None si Silver no tiene columna de modificación.
SortCol = "Recorddate"

# Umbral de filas modificadas para ejecutar OPTIMIZE automático post-MERGE.
# NOTA: Para cargas incrementales diarias (<1M filas), considerar
# complementar con un OPTIMIZE programado externo (pipeline semanal)
# independiente de este umbral.
OPTIMIZE_THRESHOLD = 1_000_000

# Columnas del CLUSTER BY declaradas en el DDL.
# Deben coincidir exactamente con la cláusula CLUSTER BY del DDLQuery.
ClusterByCols = ["IdEmpresa", "SKTiempo", "Documento"]

DDLQuery = """
CREATE TABLE IF NOT EXISTS {GoldTable} (
    SKEmpresa            INT         NOT NULL,
    SKArticulo           INT         NOT NULL,
    SKCliente            INT         NOT NULL,
    SKVendedor           INT         NOT NULL,
    SKImpuesto           INT         NOT NULL,
    SKTiempo             INT         NOT NULL,
    SKClasificacion      INT         NOT NULL,
    SKCentroCosto        INT         NOT NULL,
    SKCuentaContable     INT         NOT NULL,
    SKUsuario            INT         NOT NULL,
    SKActividadComercial INT         NOT NULL,
    Documento            STRING      NOT NULL,
    Linea                INT         NOT NULL,
    IdEmpresa            STRING      NOT NULL,
    TipoDoc              STRING,
    Pedido               STRING,
    FechaFactura         TIMESTAMP,
    MontoImpuesto        DECIMAL(18,4),
    MonedaFactura        STRING,
    TipoCambio           DECIMAL(18,4),
    MontoCOL             DECIMAL(18,4),
    MontoDOL             DECIMAL(18,4),
    MontoDesctoGenCOL    DECIMAL(18,4),
    MontoDesctoGenDOL    DECIMAL(18,4),
    Estado               STRING             COMMENT 'ACTIVA | ANULADA',
    CondicionPago        STRING             COMMENT 'Condición de pago del cliente',
    Bodega               STRING             COMMENT 'Bodega de despacho de la línea',
    CostoCOL             DECIMAL(18,4)      COMMENT 'Costo total en moneda local (para cálculo de margen)',
    OrdenCompra          STRING             COMMENT 'Orden de compra del cliente',
    BaseImpuesto         DECIMAL(18,4)      COMMENT 'Base imponible para el cálculo de IVA',
    DocFiscal            STRING             COMMENT 'Número oficial de resolución fiscal',
    DescripcionLinea     STRING,
    FechaCreacion        TIMESTAMP,
    FechaModificacion    TIMESTAMP
)
USING DELTA
CLUSTER BY (IdEmpresa, SKTiempo, Documento)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'FactVenta — Modelo Estrella con Rentabilidad y Auditoría'
"""

SourceQuery = """
WITH BASE AS (
    SELECT
        F.Source AS IdEmpresa,
        F.Factura AS Documento,
        FL.Linea,
        F.TipoDocumento,
        F.Moneda,
        F.TipoCambio,
        F.Fecha AS FechaOriginal,
        F.Pedido,
        F.DocFiscal,
        F.Anulada,
        F.CondicionPago,
        F.OrdenCompra,
        F.Usuario,
        F.ActividadComercial,
        FL.Articulo,
        F.Cliente,
        F.Vendedor,
        F.MonedaFactura,
        FL.CodigoImpuesto,
        FL.TipoLinea,
        FL.CentroCosto,
        FL.CuentaContable,
        FL.Bodega,
        FL.Descripcion AS DescripcionLinea,
        FL.TotalImpuesto1,
        FL.PrecioTotal,
        FL.DescTotGeneral,
        FL.CostoTotalLocal,
        FL.BaseImpuesto1,
        COALESCE(F.Recorddate, F.Createdate) AS Recorddate,
        CONCAT(TRIM(FL.Articulo), '-', F.Source) AS IdArticulo,
        CONCAT(TRIM(F.Cliente), '-', F.Source) AS IdCliente,
        CONCAT(TRIM(F.Vendedor), '-', F.Source) AS IdVendedor,
        CONCAT(TRIM(FL.CodigoImpuesto), '-', F.Source) AS IdImpuesto,
        CONCAT(TRIM(FL.TipoLinea), '-', F.Source) AS IdClasificacion,
        CONCAT(TRIM(FL.CentroCosto), '-', F.Source) AS IdCentroCosto,
        CONCAT(TRIM(FL.CuentaContable), '-', F.Source) AS IdCuentaContable,
        CONCAT(TRIM(F.ActividadComercial), '-', F.Source) AS IdActividadComercial
    FROM Silver.Finanzas.Factura F
    INNER JOIN Silver.Finanzas.FacturaLinea FL
        ON F.Factura = FL.Factura AND F.Source = FL.Source
    WHERE COALESCE(F.Recorddate, F.Createdate) >= '{cutoff}'
)

SELECT
    COALESCE(DE.SKEmpresa, -201) AS SKEmpresa,
    COALESCE(DA.SKArticulo, -201) AS SKArticulo,
    COALESCE(DC.SKCliente, -201) AS SKCliente,
    COALESCE(DV.SKVendedor, -201) AS SKVendedor,
    COALESCE(DI.SKImpuesto, -201) AS SKImpuesto,
    CAST(DATE_FORMAT(B.FechaOriginal, 'yyyyMMdd') AS INT) AS SKTiempo,
    COALESCE(DCL.SKClasificacion, -201) AS SKClasificacion,
    COALESCE(DCC.SKCentroCosto, -201) AS SKCentroCosto,
    COALESCE(DCT.SKCuentaContable, -201) AS SKCuentaContable,
    COALESCE(DU.SKUsuario, -201) AS SKUsuario,
    COALESCE(DAC.SKActividadComercial, -201) AS SKActividadComercial,
    B.Documento,
    B.Linea,
    B.IdEmpresa,
    CASE WHEN B.TipoDocumento = 'D' THEN 'NOTA CREDITO' ELSE 'FACTURA' END AS TipoDoc,
    B.Pedido,
    B.FechaOriginal AS FechaFactura,
    ROUND((CASE WHEN B.Moneda = 'L' THEN B.TotalImpuesto1 ELSE B.TotalImpuesto1 * B.TipoCambio END)
          * (CASE WHEN B.TipoDocumento = 'D' THEN -1 ELSE 1 END), 2) AS MontoImpuesto,
    B.MonedaFactura,          
    B.TipoCambio,
    ROUND((CASE WHEN B.Moneda = 'L' THEN B.PrecioTotal ELSE B.PrecioTotal * B.TipoCambio END)
          * (CASE WHEN B.TipoDocumento = 'D' THEN -1 ELSE 1 END), 4) AS MontoCOL,
    ROUND((CASE WHEN B.Moneda = 'D' THEN B.PrecioTotal ELSE B.PrecioTotal / B.TipoCambio END)
          * (CASE WHEN B.TipoDocumento = 'D' THEN -1 ELSE 1 END), 4) AS MontoDOL,
    ROUND((CASE WHEN B.Moneda = 'L' THEN B.DescTotGeneral ELSE B.DescTotGeneral * B.TipoCambio END)
          * (CASE WHEN B.TipoDocumento = 'D' THEN -1 ELSE 1 END), 4) AS MontoDesctoGenCOL,
    ROUND((CASE WHEN B.Moneda = 'D' THEN B.DescTotGeneral ELSE B.DescTotGeneral / B.TipoCambio END)
          * (CASE WHEN B.TipoDocumento = 'D' THEN -1 ELSE 1 END), 4) AS MontoDesctoGenDOL,
    CASE WHEN B.Anulada = 'S' THEN 'ANULADA' ELSE 'ACTIVA' END AS Estado,
    B.CondicionPago,
    B.Bodega,
    ROUND(B.CostoTotalLocal * (CASE WHEN B.TipoDocumento = 'D' THEN -1 ELSE 1 END), 4) AS CostoCOL,
    B.OrdenCompra,
    ROUND(B.BaseImpuesto1 * (CASE WHEN B.TipoDocumento = 'D' THEN -1 ELSE 1 END), 4) AS BaseImpuesto,
    B.DocFiscal,
    B.DescripcionLinea,
    B.Recorddate
FROM BASE B
LEFT JOIN Gold.Finanzas.DimEmpresa DE               ON B.IdEmpresa = DE.IdEmpresa
LEFT JOIN Gold.Finanzas.DimArticulo DA              ON B.IdArticulo = DA.IdArticulo
LEFT JOIN Gold.Finanzas.DimCliente DC               ON B.IdCliente = DC.IdCliente
LEFT JOIN Gold.Finanzas.DimVendedor DV              ON B.IdVendedor = DV.IdVendedor
LEFT JOIN Gold.Finanzas.DimImpuesto DI              ON B.IdImpuesto = DI.IdImpuesto
LEFT JOIN Gold.Finanzas.DimClasificacion DCL        ON B.IdClasificacion = DCL.IdClasificacion
LEFT JOIN Gold.Finanzas.DimCentroCosto DCC          ON B.IdCentroCosto = DCC.IdCentroCosto
LEFT JOIN Gold.Finanzas.DimCuentaContable DCT       ON B.IdCuentaContable = DCT.IdCuentaContable
LEFT JOIN Gold.Finanzas.DimUsuario DU               ON B.Usuario = DU.IdUsuario
LEFT JOIN Gold.Finanzas.DimActividadComercial DAC   ON B.IdActividadComercial = DAC.IdActividadComercial
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _validate_config() -> None:

    errors = []

    # Formato GoldTable
    parts = GoldTable.strip().split(".")
    if len(parts) != 3:
        errors.append(
            f"GoldTable debe tener formato Lakehouse.Area.Tabla — actual: {GoldTable!r}"
        )

    # Solapamientos entre listas de columnas
    all_reserved = set(NaturalKey) | set(SKCols)
    overlap_tracking = all_reserved & set(TrackingCols)
    if overlap_tracking:
        errors.append(
            f"Columnas en (NaturalKey | SKCols) Y TrackingCols — "
            f"eliminar de TrackingCols: {sorted(overlap_tracking)}"
        )
    overlap_sk_nk = set(NaturalKey) & set(SKCols)
    if overlap_sk_nk:
        errors.append(
            f"Columnas repetidas en NaturalKey Y SKCols: {sorted(overlap_sk_nk)}"
        )

    # Marcadores obligatorios
    if "{GoldTable}" not in DDLQuery:
        errors.append("DDLQuery no contiene el marcador {GoldTable}.")
    if "{cutoff}" not in SourceQuery:
        errors.append("SourceQuery no contiene el marcador {cutoff}.")

    # Parser DDL robusto: acepta cualquier sangría y verifica
    # tipos de dato Delta explícitos para evitar falsos positivos.
    _DELTA_TYPES = (
        r"(?:INT|BIGINT|SMALLINT|TINYINT|STRING|DOUBLE|FLOAT|"
        r"DECIMAL|TIMESTAMP|DATE|BOOLEAN|BINARY|LONG|ARRAY|MAP|STRUCT)"
    )
    ddl_col_names = set(
        re.findall(
            rf"^\s+(\w+)\s+{_DELTA_TYPES}",
            DDLQuery,
            re.MULTILINE | re.IGNORECASE,
        )
    )

    # SKCols presentes en DDL
    for sk in SKCols:
        if sk.upper() not in {c.upper() for c in ddl_col_names}:
            errors.append(
                f"SKCol '{sk}' declarada en SKCols pero no encontrada en DDLQuery."
            )

    # Columnas de auditoría obligatorias
    for required_audit_col in ["FechaCreacion", "FechaModificacion"]:
        if required_audit_col.upper() not in {c.upper() for c in ddl_col_names}:
            errors.append(
                f"DDLQuery no contiene la columna de auditoría requerida: '{required_audit_col}'."
            )

    # Validar CLUSTER BY y ClusterByCols contra el DDL
    if "CLUSTER BY" not in DDLQuery.upper():
        errors.append(
            "DDLQuery no contiene cláusula CLUSTER BY. "
            "Con Liquid Clustering es obligatoria — no usar PARTITIONED BY."
        )
    for lc_col in ClusterByCols:
        if lc_col.upper() not in {c.upper() for c in ddl_col_names}:
            errors.append(
                f"ClusterByCol '{lc_col}' declarada en ClusterByCols "
                f"pero no encontrada en DDLQuery."
            )

    # Estructura de FKValidations
    for i, fkv in enumerate(FKValidations):
        if not (isinstance(fkv, (list, tuple)) and len(fkv) == 3):
            errors.append(
                f"FKValidations[{i}] debe ser una tupla de 3 elementos "
                f"(fk_col, dim_table, dim_pk) — actual: {fkv!r}"
            )

    if errors:
        raise ValueError(
            "[Config] Errores encontrados:\n" +
            "\n".join(f"  • {e}" for e in errors)
        )

    print("[Config] ✓ Configuración válida.")
    print(f"[Config]   GoldTable              : {GoldTable}")
    print(f"[Config]   NaturalKey             : {NaturalKey}")
    print(f"[Config]   SKCols                 : {SKCols}")
    print(f"[Config]   TrackingCols           : {len(TrackingCols)} columnas")
    print(f"[Config]   FKValidations          : {len(FKValidations)} FKs configuradas")
    print(f"[Config]   SortCol                : {SortCol}")
    print(f"[Config]   ClusterByCols          : {ClusterByCols}")
    print(f"[Config]   FK_ORPHAN_THRESHOLD    : {FK_ORPHAN_THRESHOLD}")
    print(f"[Config]   WatermarkBufferDays    : {WATERMARK_BUFFER_DAYS}")
    print(f"[Config]   OptimizeThreshold      : {OPTIMIZE_THRESHOLD:,}")
    print(f"[Config]   BroadcastThreshold     : {BROADCAST_THRESHOLD_ROWS:,}")
    print(f"[Config]   VacuumEnabled          : {VACUUM_ENABLED} "
          f"(retain {VACUUM_RETENTION_HOURS}h)")


_validate_config()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _table_exists(full_name: str) -> bool:
    """
    Verifica si una tabla existe en Unity Catalog (tres partes).
    Usa spark.catalog.tableExists con fallback a SHOW TABLES para
    entornos donde el método nativo no está disponible.
    """
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        parts = full_name.strip().split(".")
        if len(parts) == 3:
            catalog, schema, table = parts
            rows = (
                spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`")
                .filter(F.lower(F.col("tableName")) == table.lower())
            )
            return rows.count() > 0
        elif len(parts) == 2:
            schema, table = parts
            rows = (
                spark.sql(f"SHOW TABLES IN `{schema}`")
                .filter(F.lower(F.col("tableName")) == table.lower())
            )
            return rows.count() > 0
        else:
            return spark.catalog.tableExists(full_name)


if not _table_exists(GoldTable):
    spark.sql(DDLQuery.format(GoldTable=GoldTable))
    print(f"[DDL] Tabla {GoldTable} creada.")
else:
    print(f"[DDL] Tabla {GoldTable} ya existe — se omite creación.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_CUTOFF_PATTERN = re.compile(
    r"^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])"
    r"(?: (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d)?$"
)


def ReadSilverAndTransform(override_cutoff: str = None):
    """
    Lee la fuente Silver, aplica dedup por SortCol y retorna dfSource.

    """
    # Calcular cutoff o delta de tiempo
    if override_cutoff:
        effective_cutoff = override_cutoff
        print(f"[Watermark] Override manual → cutoff = '{effective_cutoff}'")
    else:
        try:
            last_row = spark.sql(
                f"SELECT MAX(FechaModificacion) AS max_ts FROM {GoldTable}"
            ).collect()[0]

            last_ts = last_row["max_ts"]

            if last_ts is not None:
                effective_cutoff = (
                    last_ts - timedelta(days=WATERMARK_BUFFER_DAYS)
                ).strftime("%Y-%m-%d %H:%M:%S")
            else:
                effective_cutoff = "1970-01-01 00:00:00"

        except Exception as e:
            print(f"[WARN] No se pudo leer watermark → {e}")
            effective_cutoff = "1970-01-01 00:00:00"

        print(
            f"[Watermark] Auto → cutoff = '{effective_cutoff}' "
            f"(buffer={WATERMARK_BUFFER_DAYS}d)"
        )

    # Ejecuta scritp SourceQuery
    dfRaw = spark.sql(SourceQuery.format(cutoff=effective_cutoff))
    dfRaw.persist(StorageLevel.MEMORY_AND_DISK)

    # Valida NaturalKey no nulos
    from functools import reduce
    nk_null_filter = reduce(
        lambda acc, col_name: acc | F.col(col_name).isNull(),
        NaturalKey,
        F.lit(False),
    )
    cnt_null_nk = dfRaw.filter(nk_null_filter).count()

    if cnt_null_nk > 0:
        null_cols = ", ".join(NaturalKey)
        dfRaw.unpersist()
        raise ValueError(
            f"[ERROR] {cnt_null_nk:,} registros con NULL en PK ({null_cols}) "
            f"→ abortando para evitar corrupción en MERGE."
        )


    # Dedup por SortCol
    if SortCol and SortCol in dfRaw.columns:
        w = Window.partitionBy(*NaturalKey).orderBy(F.col(SortCol).desc())
        dfSource = (
            dfRaw
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        print(f"[Dedup] Window aplicado por SortCol='{SortCol}'.")
    else:
        dfSource = dfRaw
        if SortCol:
            print(f"[WARN] SortCol='{SortCol}' no encontrado en el DataFrame — dedup SQL usado.")
        else:
            print("[Dedup] SortCol=None — se confía en dedup SQL (BASE_DEDUP).")

    dfSource.persist(StorageLevel.MEMORY_AND_DISK)
    print("[Cache] dfSource persistido en MEMORY_AND_DISK.")

    return dfSource, dfRaw, cnt_null_nk, effective_cutoff


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ValidateFKs(dfSource) -> dict:
    """
    Valida FKs iterando FKValidations.
    Retorna {fk_col: count_huerfanas} para enriquecer el JSON de métricas.
    """
    fk_results = {}

    for fk_col, dim_table, dim_pk in FKValidations:
        try:
            dim_df = spark.table(dim_table).select(dim_pk)
            # Broadcast condicional por tamaño de la dimensión
            dim_count = dim_df.count()
            if dim_count <= BROADCAST_THRESHOLD_ROWS:
                huerfanas = (
                    dfSource
                    .join(F.broadcast(dim_df), dfSource[fk_col] == dim_df[dim_pk], "left_anti")
                    .count()
                )
                join_tipo = "broadcast"
            else:
                huerfanas = (
                    dfSource
                    .join(dim_df, dfSource[fk_col] == dim_df[dim_pk], "left_anti")
                    .count()
                )
                join_tipo = "sort-merge"

            fk_results[fk_col] = huerfanas
            nivel  = "[WARN]" if huerfanas > 0 else "[INFO]"
            status = f"{huerfanas:,} huérfanos" if huerfanas > 0 else "OK"
            print(
                f"{nivel} FK {fk_col:25s} → {dim_table} "
                f"[{join_tipo}, dim={dim_count:,}]: {status}"
            )

        except Exception as fk_err:
            fk_results[fk_col] = -1
            print(f"[INFO] FK '{fk_col}' omitida (dim no disponible): {fk_err}")

    return fk_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

process_start = datetime.now()

try:
    _raw_cutoff = dbutils.widgets.get("override_cutoff").strip()
except Exception:
    _raw_cutoff = ""

if _raw_cutoff:
    if not _CUTOFF_PATTERN.match(_raw_cutoff):
        raise ValueError(
            f"[Config] override_cutoff inválido: {_raw_cutoff!r}. "
            "Formato esperado: 'YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS'."
        )
    override_cutoff = _raw_cutoff
    print(f"[Config] override_cutoff recibido y validado: '{override_cutoff}'")
else:
    override_cutoff = None

# Estructura de resultado — retornada al pipeline padre
result = {
    "Status":               "success",
    "TableName":            GoldTable,
    "OverrideCutoff":       override_cutoff,
    "EffectiveCutoff":      None,
    "SourceRecords":        0,
    "NullNaturalKey":       0,
    "FKsHuerfanas":         {},
    "HasFKWarnings":        False,
    "RecordsTargetBefore":  0,
    "RecordsInserted":      0,
    "RecordsUpdated":       0,
    "OptimizeExecuted":     False,
    "VacuumExecuted":       False,
    "Message":              None,
    "EventTimestamp":       process_start.isoformat(),
    "EndTime":              None,
    "ExecutionTimeSeconds": None,
}

dfSource = None
dfRaw    = None

try:
    # Lee, persiste y transformar desde Silver
    dfSource, dfRaw, cnt_null_nk, effective_cutoff = ReadSilverAndTransform(override_cutoff)
    result["NullNaturalKey"]  = cnt_null_nk
    result["EffectiveCutoff"] = effective_cutoff

    agg_row = dfSource.agg(F.count("*").alias("total")).collect()[0]
    result["SourceRecords"] = agg_row["total"]

    if dfRaw is not None:

        dfRaw.unpersist()

        dfRaw = None

        # 1. Obtención del conteo de filas en Gold (Método optimizado)
        try:
            parts = GoldTable.split(".")
            spark_safe_name = f"{parts[0]}.`{parts[1]}`.`{parts[2]}`"
            
            # Fast Count: Lee el log de Delta sin escanear archivos
            result["RecordsTargetBefore"] = spark.table(spark_safe_name).count()
        except Exception:
            # Fallback por si la tabla no existe
            result["RecordsTargetBefore"] = 0

    print(
        f"[INFO] Source: {result['SourceRecords']:,} registros | "
        f"Gold filas actuales: {result['RecordsTargetBefore']:,}"
    )

    # MERGE SCD1 + FK validation
    if result["SourceRecords"] > 0:

        fk_results = ValidateFKs(dfSource)
        result["FKsHuerfanas"] = fk_results

        result["HasFKWarnings"] = any(
            cnt > FK_ORPHAN_THRESHOLD
            for cnt in fk_results.values()
            if isinstance(cnt, int) and cnt >= 0
        )
        if result["HasFKWarnings"]:
            print(
                f"[WARN] HasFKWarnings=True — al menos una FK supera el "
                f"umbral de {FK_ORPHAN_THRESHOLD} huérfanos. "
                f"Revisar FKsHuerfanas en el log de auditoría."
            )

        deltaTarget = DeltaTable.forName(spark, GoldTable)

        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in NaturalKey])

        # Hash-diff: solo actualiza filas donde alguna columna de negocio cambió.
        change_cols = SKCols + TrackingCols
        hash_src = F.sha2(
            F.concat_ws("|", *[F.col(f"s.{c}").cast("string") for c in change_cols]), 256
        )
        hash_tgt = F.sha2(
            F.concat_ws("|", *[F.col(f"t.{c}").cast("string") for c in change_cols]), 256
        )

        # SCD1: actualiza SKs + tracking cols + FechaModificacion
        update_dict = {c: f"s.{c}" for c in SKCols + TrackingCols}
        update_dict["FechaModificacion"] = "current_timestamp()"

        # INSERT: NK + SKs + tracking cols + fechas de auditoría
        insert_dict = {c: f"s.{c}" for c in NaturalKey + SKCols + TrackingCols}
        insert_dict["FechaCreacion"]     = "current_timestamp()"
        insert_dict["FechaModificacion"] = "current_timestamp()"

        print(f"[MERGE] Iniciando MERGE sobre {GoldTable}...")
        (
            deltaTarget.alias("t")
            .merge(dfSource.alias("s"), merge_condition)
            .whenMatchedUpdate(
                condition=(hash_src != hash_tgt),
                set=update_dict,
            )
            .whenNotMatchedInsert(values=insert_dict)
            .execute()
        )

        # Métricas de Delta post-MERGE
        metrics = (
            deltaTarget.history(1)
            .select("operationMetrics")
            .first()["operationMetrics"]
        )
        result["RecordsInserted"] = int(metrics.get("numTargetRowsInserted", "0"))
        result["RecordsUpdated"]  = int(metrics.get("numTargetRowsUpdated",  "0"))

        print(
            f"[MERGE] Insertados: {result['RecordsInserted']:,} | "
            f"Actualizados: {result['RecordsUpdated']:,}"
        )

        total_modified = result["RecordsInserted"] + result["RecordsUpdated"]

        # OPTIMIZE condicional (Liquid Clustering)
        # NOTA: Para cargas incrementales con total_modified < OPTIMIZE_THRESHOLD,
        # ejecutar OPTIMIZE desde un pipeline programado externo (ej. semanal).
        if total_modified >= OPTIMIZE_THRESHOLD:
            print(
                f"[OPTIMIZE] {total_modified:,} filas >= umbral "
                f"({OPTIMIZE_THRESHOLD:,}) → Ejecutando OPTIMIZE (Liquid Clustering)..."
            )
            spark.sql(f"OPTIMIZE {GoldTable}")
            result["OptimizeExecuted"] = True
            print("[OPTIMIZE] Completado.")

            # VACUUM condicional post-OPTIMIZE
            if VACUUM_ENABLED:
                print(f"[VACUUM] Ejecutando VACUUM RETAIN {VACUUM_RETENTION_HOURS} HOURS...")
                spark.sql(
                    f"VACUUM {GoldTable} RETAIN {VACUUM_RETENTION_HOURS} HOURS"
                )
                result["VacuumExecuted"] = True
                print("[VACUUM] Completado.")

        else:
            print(
                f"[OPTIMIZE] {total_modified:,} filas < umbral "
                f"({OPTIMIZE_THRESHOLD:,}) → omitido en este run."
            )

    else:
        result["Message"] = (
            "No se encontraron registros nuevos desde la última marca de agua."
        )
        print(f"[INFO] {result['Message']}")

except Exception as e:
    result["Status"]  = "error"
    result["Message"] = str(e)
    raise

finally:
    process_end = datetime.now()
    result["EndTime"]              = process_end.isoformat()
    result["ExecutionTimeSeconds"] = round(
        (process_end - process_start).total_seconds(), 2
    )

    # Libera caché de dfSource (añadido) y dfRaw (original)
    for _df_name, _df in [("dfSource", dfSource), ("dfRaw", dfRaw)]:
        if _df is not None:
            try:
                _df.unpersist()
                print(f"[Cache] {_df_name} liberado.")
            except Exception as ue:
                print(f"[WARN] No se pudo liberar caché de {_df_name}: {ue}")

    if result.get("Message"):
        result["Message"] = str(result["Message"])[:2000].replace("'", "''")

    result["FKsHuerfanas"] = json.dumps(result["FKsHuerfanas"])

    # Registro en tabla de auditoría
    Log.LogData(
        result=result,
        data_zone="Gold",
        execution_artifact=GoldTable.split(".")[-1],
    )

    print("\n" + "=" * 64)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    # notebook.exit() solo en path exitoso.
    # En caso de error, la excepción ya fue re-raised arriba.
    if result["Status"] == "success":
        mssparkutils.notebook.exit(json.dumps(result, ensure_ascii=False))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
