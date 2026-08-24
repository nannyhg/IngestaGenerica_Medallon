# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "693cb2c8-a6af-4bdb-a471-32bcdf88adbe",
# META       "default_lakehouse_name": "Admin",
# META       "default_lakehouse_workspace_id": "29561514-52fa-45ff-9941-9a53458b4a4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "693cb2c8-a6af-4bdb-a471-32bcdf88adbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import col, year, month, lit, to_timestamp
from pyspark.sql.types import LongType, ShortType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class DataOpsLog:
    """Clase para registrar logs de ejecución"""
    
    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.target_table = "Admin.Logs.ExecutionLog"
    
    def logExecutionDataContract(self, execution_df: DataFrame, data_zone: str, execution_artifact: str):
        """
        Registra logs de ejecución en la tabla
        
        Args:
            execution_df: DataFrame con datos de ejecución
            data_zone: Zona de datos (Bronze, Silver, Gold)
            execution_artifact: Nombre del notebook o artefacto
        """
        # Transformar DataFrame al formato destino
        log_df = execution_df.select(
            col("timestamp").alias("EventTimestamp"),
            lit(data_zone).alias("DataZone"),
            col("tableName").alias("TableName"),
            col("status").alias("Status"),
            col("writeType").alias("WriteType"),
            col("sourceRecords").cast(LongType()).alias("SourceRecords"),
            col("recordsInserted").cast(LongType()).alias("RecordsInserted"),
            col("recordsUpdated").cast(LongType()).alias("RecordsUpdated"),
            col("totalAffected").cast(LongType()).alias("TotalAffected"),
            col("executionTimeSeconds").alias("ExecutionTimeSeconds"),
            lit(execution_artifact).alias("ExecutionArtifact"),
            col("errorMessage").alias("Message")
        )
        
        # Agregar particiones
        log_df = log_df.withColumn("Year", year(col("EventTimestamp")).cast(ShortType())) \
                       .withColumn("Month", month(col("EventTimestamp")).cast(ShortType()))
        
        # Escribir a la tabla
        log_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("Year", "Month") \
            .saveAsTable(self.target_table)
    

    def LogData(self, result: dict, data_zone: str, execution_artifact: str):

        def sql_value(v):
            if v is None:
                return "NULL"
            if isinstance(v, str):
                safe_str = v.replace("'", "''")
                return f"'{safe_str}'"
            return str(v)

        event_ts = result.get("EventTimestamp")

        sql = f"""
        INSERT INTO Admin.Logs.ExecutionLog
        (EventTimestamp, DataZone, TableName, Status, WriteType,
        SourceRecords, RecordsInserted, RecordsUpdated, TotalAffected,
        ExecutionTimeSeconds, ExecutionArtifact, Message, Year, Month)
        VALUES (
            {sql_value(event_ts)},
            {sql_value(data_zone)},
            {sql_value(result.get("TableName"))},
            Upper({sql_value(result.get("Status"))}),
            {sql_value(result.get("WriteType"))},
            {sql_value(result.get("SourceRecords"))},
            {sql_value(result.get("RecordsInserted"))},
            {sql_value(result.get("RecordsUpdated"))},
            {sql_value(result.get("TotalAffected"))},
            {sql_value(result.get("ExecutionTimeSeconds"))},
            {sql_value(execution_artifact)},
            {sql_value(result.get("Message"))},
            year(timestamp({sql_value(event_ts)})),
            month(timestamp({sql_value(event_ts)}))
        )
        """

        spark.sql(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
