# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.functions import (col, lit, coalesce, row_number,current_timestamp, from_utc_timestamp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Evita WRITE_ANCIENT_DATETIME en tablas con fechas anteriores a 1900
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.avro.datetimeRebaseModeInWrite",    "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite",    "CORRECTED")
# Adaptive Query Execution: reajusta particiones y joins en runtime
spark.conf.set("spark.sql.adaptive.enabled",                    "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",           "true")
# Ajustar según capacidad del clúster: F32→400  F64→800  F128→1600
spark.conf.set("spark.sql.shuffle.partitions", "800")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "128MB")
# Hacer que Spark sea más sensible al sesgo (si una tarea es 3 veces más grande que la media, la divide)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.caseSensitive", "true")
TIMEZONE = "America/Costa_Rica"
now = from_utc_timestamp(current_timestamp(), TIMEZONE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Watermark buffer — días de retroceso sobre MAX(FechaModificacion)
# para cubrir registros retroactivos publicados en Silver con demora.
WATERMARK_BUFFER_DAYS = 2
BROADCAST_THRESHOLD_ROWS = 5_000_000
VACUUM_ENABLED            = True
VACUUM_RETENTION_HOURS    = 168   # 7 días
OPTIMIZE_WINDOW_DAYS = 90

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
