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
# META           "id": "693cb2c8-a6af-4bdb-a471-32bcdf88adbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Esquema Gold: Creación de Tablas
# 
# Este notebook está diseñado para crear de manera explícita todas las tablas necesarias dentro del esquema Gold. Aquí se definen las estructuras de datos finales, asegurando la correcta organización, documentación y gobernanza de la información para los procesos analíticos y de negocio.


# CELL ********************

spark.conf.set("spark.sql.caseSensitive", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Admin.Logs.ExecutionLog (
# MAGIC     EventTimestamp TIMESTAMP COMMENT 'Fecha y hora del registro del log',
# MAGIC     DataZone STRING COMMENT 'Capa de la arquitectura de datos: Bronze, Silver o Gold',
# MAGIC     TableName STRING COMMENT 'Nombre de la tabla afectada en el Lakehouse',
# MAGIC     Status STRING COMMENT 'Resultado de la operacion: Success, Failed o InProgress',
# MAGIC     WriteType STRING COMMENT 'Metodo de escritura: Merge, Append, Overwrite o Update',
# MAGIC     SourceRecords LONG COMMENT 'Numero de registros identificados en el origen',
# MAGIC     RecordsInserted LONG COMMENT 'Numero de registros nuevos creados en el destino',
# MAGIC     RecordsUpdated LONG COMMENT 'Numero de registros existentes modificados',
# MAGIC     TotalAffected LONG COMMENT 'Total de registros insertados y actualizados',
# MAGIC     ExecutionTimeSeconds DOUBLE COMMENT 'Tiempo total de ejecucion en segundos',
# MAGIC     ExecutionArtifact STRING COMMENT 'Nombre del Notebook o artefacto de Fabric que ejecuto la carga',
# MAGIC     Message STRING COMMENT 'Descripcion del error si el estado es Failed')
# MAGIC USING DELTA
# MAGIC PARTITIONED by(Year SMALLINT, Month SMALLINT);
# MAGIC COMMENT ON TABLE Admin.Logs.ExecutionLog IS 'Tabla centralizada para el control de auditoría, métricas de carga y monitoreo de procesos ETL en las capas Bronze, Silver y Gold.';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS Gold.Finanzas.DimArticulo (
# MAGIC     SKArticulo      BIGINT    COMMENT 'Clave subrogada (Surrogate Key).',
# MAGIC     IdArticulo      STRING    COMMENT 'Identificador compuesto del artículo.',
# MAGIC     Articulo        STRING    COMMENT 'Código único del artículo (PK origen).',
# MAGIC     Descripcion     STRING    COMMENT 'Nombre completo del artículo.',
# MAGIC     Activo          STRING    COMMENT 'Indicador activo (S/N).',
# MAGIC     UnidadAlmacen   STRING    COMMENT 'Unidad de almacenamiento.',
# MAGIC     UnidadEmpaque   STRING    COMMENT 'Unidad de empaque.',
# MAGIC     UnidadVenta     STRING    COMMENT 'Unidad de venta.',
# MAGIC     Grupo           STRING    COMMENT 'Grupo de cuentas contables.',
# MAGIC     Source          STRING    COMMENT 'Origen de datos.',
# MAGIC     FechaCreacion       TIMESTAMP COMMENT 'Fecha de inserción del registro.',
# MAGIC     FechaModificacion   TIMESTAMP COMMENT 'Fecha de última actualización.'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimensión de Artículos — Finanzas (Gold Layer)'
# MAGIC 
# MAGIC 
# MAGIC --Registros dummy (se insertan al crear la tabla) 
# MAGIC 
# MAGIC INSERT INTO Gold.Finanzas.DimArticulo
# MAGIC     (SKArticulo, IdArticulo, Articulo, Descripcion, Activo,
# MAGIC         UnidadAlmacen, UnidadEmpaque, UnidadVenta, Grupo, Source)
# MAGIC VALUES
# MAGIC     (-101, '-101', 'NO_APLICA',      'No Aplica',            'N', 'N/A', 'N/A', 'N/A', 'N/A', 'DIM_DUMMY'),
# MAGIC     (-201, '-201', 'NO_ENCONTRADO',  'No se encontró valor', 'N', 'N/A', 'N/A', 'N/A', 'N/A', 'DIM_DUMMY'),
# MAGIC     (-301, '-301', 'NO_DEFINIDO',    'No Definido',          'N', 'N/A', 'N/A', 'N/A', 'N/A', 'DIM_DUMMY')
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
