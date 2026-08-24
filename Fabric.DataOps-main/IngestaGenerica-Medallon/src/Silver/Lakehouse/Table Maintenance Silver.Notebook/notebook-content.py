# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6f04d354-fefb-4fde-9146-a60eb87d0884",
# META       "default_lakehouse_name": "Silver",
# META       "default_lakehouse_workspace_id": "29561514-52fa-45ff-9941-9a53458b4a4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "6f04d354-fefb-4fde-9146-a60eb87d0884"
# META         },
# META         {
# META           "id": "d1e4999b-635b-4ce8-92ce-c4ec528e7ea8"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Este notebook ejecuta el proceso de optimización y mantenimiento sobre las tablas Delta de la capa Silver
# que estén configuradas en la tabla Bronze.BDDataOps.DataContract. 
# 
# Para cada tabla aplica:
# - OPTIMIZE con ZORDER (sobre columnas PK) y VORDER
# - VACUUM con retención de 8 días
# 
# Las tablas no registradas en el DataContract no serán procesadas.

# CELL ********************

from pyspark.sql.functions import col, collect_list
from datetime import datetime
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

RetentionHours = 192  # 8 days
TopicCode      = "ExactusTransaccional" #Puede enviar caracteres comodin para hacer el filtro con like   % * _
ProjectCode    = "Finanzas"  
Zone           = "Silver"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfTables = spark.sql(f"""
    SELECT DISTINCT
        DC.DestinationZone,
        DC.DestinationSchema,
        DC.DestinationTable
    FROM Bronze.BDDataOps.DataContract DC
        INNER JOIN Bronze.BDDataOps.DataPipeline DP ON DC.DataPipelineCode = DP.Code
        INNER JOIN Bronze.BDDataOps.ProjectTopic PT ON DP.TopicCode = PT.Code
    WHERE PT.Code like '{TopicCode}'
        AND DC.DestinationZone = '{Zone}'
        AND PT.ProjectCode = '{ProjectCode}'
        AND DC.IsActive = 1
""")

# === PK COLUMNS PER TABLE (for ZORDER) ===
dfPkCols = spark.sql(f"""
    SELECT DISTINCT
        DC.DestinationSchema,
        DC.DestinationTable,
        DC.DestinationField
    FROM Bronze.BDDataOps.DataContract DC
    WHERE DC.AttributeType = 'PK'
        AND DC.DestinationZone = '{Zone}'
""").groupBy("DestinationSchema", "DestinationTable").agg(
    collect_list("DestinationField").alias("PkColumns")
)

dfTablesWithPk = dfTables.join(
    dfPkCols,
    on=["DestinationSchema", "DestinationTable"],
    how="left"
)


print(f"Tables to maintain: {dfTablesWithPk.count()}")
display(dfTablesWithPk)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

auditLog = []

for row in dfTablesWithPk.collect():
    tableName = f"{row.DestinationZone}.{row.DestinationSchema}.{row.DestinationTable}"
    pkCols = row.PkColumns if row.PkColumns else []
    startTime = time.time()
    status = "Success"
    operation = ""
    errorMsg = ""

    try:
        # Step 1: OPTIMIZE with ZORDER (on PK columns) + VORDER
        if pkCols:
            zorderExpr = ", ".join(pkCols)
            spark.sql(f"OPTIMIZE {tableName} ZORDER BY ({zorderExpr}) VORDER")
            operation = f"OPTIMIZE ZORDER BY ({zorderExpr}) VORDER"
        else:
            spark.sql(f"OPTIMIZE {tableName} VORDER")
            operation = "OPTIMIZE VORDER (no PK for ZORDER)"

        # Step 2: VACUUM with retention
        spark.sql(f"VACUUM {tableName} RETAIN {RetentionHours} HOURS")
        operation += f" | VACUUM RETAIN {RetentionHours}H"

    except Exception as e:
        status = "Failed"
        errorMsg = str(e)[:500]

    elapsedSec = round(time.time() - startTime, 2)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    auditLog.append((tableName, ", ".join(pkCols) if pkCols else "N/A", operation, status, errorMsg, elapsedSec, timestamp))
    print(f"[{status}] {tableName} ({elapsedSec}s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

auditColumns = ["Table", "PkColumns", "Operation", "Status", "Error", "ElapsedSeconds", "Timestamp"]
dfAudit = spark.createDataFrame(auditLog, auditColumns)
display(dfAudit)

totalTables = len(auditLog)
successCount = sum(1 for x in auditLog if x[3] == "Success")
failedCount = totalTables - successCount

print(f"\nMaintenance complete: {totalTables} tables | {successCount} success | {failedCount} failed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
