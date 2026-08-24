# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d1e4999b-635b-4ce8-92ce-c4ec528e7ea8",
# META       "default_lakehouse_name": "Bronze",
# META       "default_lakehouse_workspace_id": "29561514-52fa-45ff-9941-9a53458b4a4a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d1e4999b-635b-4ce8-92ce-c4ec528e7ea8"
# META         },
# META         {
# META           "id": "6f04d354-fefb-4fde-9146-a60eb87d0884"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

# =====================================================================================
# Bronze to Silver - Proceso Configurado por Metadata
# DESCRIPCIÓN: Proceso que migra datos desde la zona Bronze hacia Silver utilizando
#              configuración almacenada en la base de datos de DataOps
# =====================================================================================

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max, min as spark_min, lit, when, sha2, concat_ws, expr
from pyspark.sql.types import *
from delta.tables import DeltaTable
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import logging

# =====================================================================================
# CONFIGURACIÓN INICIAL
# =====================================================================================

# Configuración del logger para seguimiento del proceso
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.caseSensitive", "true")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # cambiar dataPipelineCode *

# PARAMETERS CELL ********************

# Parámetros de entrada - Configuración del ambiente y tópico - Se esperan desde el pipeline de llamado,  excepto el de debug que puede ser solo para ejecutar el notebook
environmentCode = "DEV"  # Valores válidos: DEV, TEST, PRD
topicCode = "ExactusMaestros"  # Código del tópico a procesar
dataPipelineCode = "*"  # Código específico o * para todos

# Parámetro de debug
debugMode = False  # True: salida detallada con queries y dataframes | False: salida resumida

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parámetros de entrada - Base de datos de configuración - Manejar manual desde el notebook no es necesario que se envien como parametros
configurationDatabaseZone = "Bronze"  # Zona donde se encuentra la BD de configuración
configurationDatabaseSchema = "BDDataOps"  # Esquema de la BD de configuración

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================================
# CLASE PRINCIPAL: ConfigurationDrivenDataProcessor
# =====================================================================================

class ConfigurationDrivenDataProcessor:
    """
    Clase principal que gestiona el proceso de migración de datos desde Bronze a Silver
    basándose en configuración almacenada en la base de datos DataOps
    """
    
    def __init__(self, 
                 environmentCode: str, 
                 topicCode: str, 
                 dataPipelineCode: str = "*",
                 configurationDatabaseZone: str = "Bronze",
                 configurationDatabaseSchema: str = "BDDataOps",
                 debugMode: bool = False):
        """
        Constructor de la clase
        
        Args:
            environmentCode: Código del ambiente (DEV, TEST, PRD)
            topicCode: Código del tópico a procesar
            dataPipelineCode: Código del pipeline específico o * para todos
            configurationDatabaseZone: Zona donde se encuentra la BD de configuración
            configurationDatabaseSchema: Esquema de la BD de configuración
            debugMode: Activa modo debug con salida detallada (True/False)
        """
        self.environmentCode = environmentCode
        self.topicCode = topicCode
        self.dataPipelineCode = dataPipelineCode
        self.configurationDatabaseZone = configurationDatabaseZone
        self.configurationDatabaseSchema = configurationDatabaseSchema
        self.debugMode = debugMode
        self.spark = SparkSession.builder.getOrCreate()
        
        # Inicialización del sistema de trazabilidad
        self.traceabilityLog = []
        self.processStartTime = datetime.now()
        
        # Validación del código de ambiente
        self._validateEnvironmentCode()
        
        self._logInfo(f"Procesador inicializado - Environment: {environmentCode}, Topic: {topicCode}, Pipeline: {dataPipelineCode}")
        self._logDebug(f"BD Configuración: {configurationDatabaseZone}.{configurationDatabaseSchema}")
        self._logDebug(f"Modo Debug: {'ACTIVADO' if debugMode else 'DESACTIVADO'}")
    
    def _validateEnvironmentCode(self):
        """
        Valida que el código de ambiente sea válido
        """
        validEnvironments = ["DEV", "TEST", "PRD"]
        if self.environmentCode not in validEnvironments:
            raise ValueError(f"EnvironmentCode debe ser uno de: {', '.join(validEnvironments)}")
    
    # =================================================================================
    # MÉTODOS DE LOGGING
    # =================================================================================
    
    def _logInfo(self, message: str):
        """
        Registra mensaje informativo (siempre se muestra)
        
        Args:
            message: Mensaje a registrar
        """
        logger.info(message)
    
    def _logDebug(self, message: str):
        """
        Registra mensaje de debug (solo si debugMode está activo)
        
        Args:
            message: Mensaje a registrar
        """
        if self.debugMode:
            logger.info(f"[DEBUG] {message}")
    
    def _showQuery(self, query: str, description: str = "Query"):
        """
        Muestra el query SQL si debugMode está activo
        
        Args:
            query: Query SQL a mostrar
            description: Descripción del query
        """
        if self.debugMode:
            logger.info(f"[DEBUG] {description}:")
            logger.info("-" * 80)
            logger.info(query)
            logger.info("-" * 80)
    
    def _showDataFrame(self, dataFrame: DataFrame, description: str = "DataFrame", rowCount: int = 10):
        """
        Muestra el contenido de un DataFrame si debugMode está activo
        
        Args:
            dataFrame: DataFrame a mostrar
            description: Descripción del DataFrame
            rowCount: Número de filas a mostrar
        """
        if self.debugMode:
            logger.info(f"[DEBUG] {description}:")
            logger.info(f"[DEBUG] Total de registros: {dataFrame.count()}")
            logger.info(f"[DEBUG] Esquema:")
            dataFrame.printSchema()
            logger.info(f"[DEBUG] Primeras {rowCount} filas:")
            dataFrame.show(rowCount, truncate=False)
    
    # =================================================================================
    # MÉTODOS DE TRAZABILIDAD
    # =================================================================================
    
    def registerTableProcessing(self, 
                                tableName: str, 
                                writeType: str,
                                sourceRecords: int,
                                recordsInserted: int = 0,
                                recordsUpdated: int = 0,
                                recordsDeleted: int = 0,
                                recordsMatched: int = 0,
                                recordsUnchanged: int = 0,
                                status: str = "SUCCESS",
                                errorMessage: str = None,
                                executionTimeSeconds: float = 0.0):
        """
        Registra la información de procesamiento de una tabla para trazabilidad
        
        Args:
            tableName: Nombre completo de la tabla procesada
            writeType: Tipo de escritura (Merge, Delta, Overwrite)
            sourceRecords: Cantidad de registros en origen
            recordsInserted: Cantidad de registros insertados
            recordsUpdated: Cantidad de registros actualizados (solo para Merge)
            recordsDeleted: Cantidad de registros eliminados (solo para Delta)
            recordsMatched: Cantidad de registros que hicieron match en merge
            recordsUnchanged: Cantidad de registros sin cambios en merge
            status: Estado del procesamiento (SUCCESS, ERROR)
            errorMessage: Mensaje de error si existe
            executionTimeSeconds: Tiempo de ejecución en segundos
        """
        traceRecord = {
            'timestamp': datetime.now(),
            'tableName': tableName,
            'writeType': writeType,
            'sourceRecords': sourceRecords,
            'recordsInserted': recordsInserted,
            'recordsUpdated': recordsUpdated,
            'recordsDeleted': recordsDeleted,
            'recordsMatched': recordsMatched,
            'recordsUnchanged': recordsUnchanged,
            'totalAffected': recordsInserted + recordsUpdated + recordsDeleted,
            'status': status,
            'errorMessage': errorMessage if errorMessage else '',
            'executionTimeSeconds': executionTimeSeconds
        }
        
        self.traceabilityLog.append(traceRecord)
        
        self._logDebug(f"Registro de trazabilidad agregado para tabla {tableName}")
    
    def showProcessingSummary(self):
        """
        Muestra un resumen consolidado de todas las tablas procesadas en formato tabla
        """
        self._logInfo("")
        self._logInfo("=" * 100)
        self._logInfo("RESUMEN DE PROCESAMIENTO - TRAZABILIDAD")
        self._logInfo("=" * 100)
        
        if not self.traceabilityLog:
            self._logInfo("No hay registros de trazabilidad para mostrar")
            return
        
        # Cálculo de tiempo total de proceso
        processEndTime = datetime.now()
        totalDuration = (processEndTime - self.processStartTime).total_seconds()
        
        # Información general del proceso
        self._logInfo(f"Ambiente: {self.environmentCode} | Tópico: {self.topicCode} | Pipeline: {self.dataPipelineCode}")
        self._logInfo(f"Inicio: {self.processStartTime.strftime('%Y-%m-%d %H:%M:%S')} | Fin: {processEndTime.strftime('%Y-%m-%d %H:%M:%S')} | Duración: {totalDuration:.2f} seg")
        
        # Contador de tablas por estado
        successCount = sum(1 for record in self.traceabilityLog if record['status'] == 'SUCCESS')
        errorCount = sum(1 for record in self.traceabilityLog if record['status'] == 'ERROR')
        
        self._logInfo(f"Tablas Procesadas: {len(self.traceabilityLog)} | Exitosas: {successCount} | Con Errores: {errorCount}")
        self._logInfo("=" * 100)
        
        # Conversión de los registros de trazabilidad a DataFrame
        traceabilityDataFrame = self._createTraceabilityDataFrame()
        
        # Mostrar tabla de trazabilidad detallada
        self._logInfo("\nDETALLE DE PROCESAMIENTO POR TABLA:")
        display(traceabilityDataFrame)
        
        # Crear y mostrar resumen consolidado por método
        summaryDataFrame = self._createSummaryDataFrame()
        
        self._logInfo("\nRESUMEN CONSOLIDADO POR MÉTODO:")
        display(summaryDataFrame)
        
        self._logInfo("=" * 100)
    
    def _createTraceabilityDataFrame(self) -> DataFrame:
        """
        Crea un DataFrame con el detalle completo de trazabilidad
        
        Returns:
            DataFrame con toda la información de trazabilidad
        """
        # Esquema del DataFrame de trazabilidad
        schema = StructType([
            StructField("timestamp", TimestampType(), False),
            StructField("tableName", StringType(), False),
            StructField("writeType", StringType(), False),
            StructField("status", StringType(), False),
            StructField("sourceRecords", IntegerType(), False),
            StructField("recordsInserted", IntegerType(), False),
            StructField("recordsUpdated", IntegerType(), False),
            StructField("recordsDeleted", IntegerType(), False),
            StructField("recordsMatched", IntegerType(), False),
            StructField("recordsUnchanged", IntegerType(), False),
            StructField("totalAffected", IntegerType(), False),
            StructField("executionTimeSeconds", DoubleType(), False),
            StructField("errorMessage", StringType(), True)
        ])
        
        # Conversión de lista de diccionarios a lista de tuplas para el DataFrame
        rows = [
            (
                record['timestamp'],
                record['tableName'],
                record['writeType'],
                record['status'],
                record['sourceRecords'],
                record['recordsInserted'],
                record['recordsUpdated'],
                record['recordsDeleted'],
                record['recordsMatched'],
                record['recordsUnchanged'],
                record['totalAffected'],
                record['executionTimeSeconds'],
                record['errorMessage'] if record['errorMessage'] else None
            )
            for record in self.traceabilityLog
        ]
        
        traceabilityDataFrame = self.spark.createDataFrame(rows, schema)
        
        return traceabilityDataFrame
    
    def _createSummaryDataFrame(self) -> DataFrame:
        """
        Crea un DataFrame con el resumen consolidado por método de escritura
        
        Returns:
            DataFrame con resumen por WriteType
        """
        # Agrupación por writeType y status
        summaryData = {}
        
        for record in self.traceabilityLog:
            if record['status'] == 'SUCCESS':
                writeType = record['writeType']
                
                if writeType not in summaryData:
                    summaryData[writeType] = {
                        'writeType': writeType,
                        'tablesProcessed': 0,
                        'totalSourceRecords': 0,
                        'totalInserted': 0,
                        'totalUpdated': 0,
                        'totalDeleted': 0,
                        'totalMatched': 0,
                        'totalUnchanged': 0,
                        'totalAffected': 0
                    }
                
                summaryData[writeType]['tablesProcessed'] += 1
                summaryData[writeType]['totalSourceRecords'] += record['sourceRecords']
                summaryData[writeType]['totalInserted'] += record['recordsInserted']
                summaryData[writeType]['totalUpdated'] += record['recordsUpdated']
                summaryData[writeType]['totalDeleted'] += record['recordsDeleted']
                summaryData[writeType]['totalMatched'] += record['recordsMatched']
                summaryData[writeType]['totalUnchanged'] += record['recordsUnchanged']
                summaryData[writeType]['totalAffected'] += record['totalAffected']
        
        # Esquema del DataFrame de resumen
        schema = StructType([
            StructField("writeType", StringType(), False),
            StructField("tablesProcessed", IntegerType(), False),
            StructField("totalSourceRecords", IntegerType(), False),
            StructField("totalInserted", IntegerType(), False),
            StructField("totalUpdated", IntegerType(), False),
            StructField("totalDeleted", IntegerType(), False),
            StructField("totalMatched", IntegerType(), False),
            StructField("totalUnchanged", IntegerType(), False),
            StructField("totalAffected", IntegerType(), False)
        ])
        
        # Conversión a lista de tuplas
        rows = [
            (
                summary['writeType'],
                summary['tablesProcessed'],
                summary['totalSourceRecords'],
                summary['totalInserted'],
                summary['totalUpdated'],
                summary['totalDeleted'],
                summary['totalMatched'],
                summary['totalUnchanged'],
                summary['totalAffected']
            )
            for summary in summaryData.values()
        ]
        
        summaryDataFrame = self.spark.createDataFrame(rows, schema) if rows else self.spark.createDataFrame([], schema)
        
        return summaryDataFrame
    
    # =================================================================================
    # MÉTODOS DE LECTURA DE CONFIGURACIÓN
    # =================================================================================
    
    def getConfigurationFromDatabase(self) -> DataFrame:
        """
        Obtiene la configuración desde las tablas del lakehouse usando consulta nativa
        
        Returns:
            DataFrame con la configuración de las tablas a procesar
        """
        self._logInfo("Obteniendo configuración desde lakehouse...")
        
        # Construcción de las rutas de las tablas de configuración
        dataContractTable = f"{self.configurationDatabaseZone}.{self.configurationDatabaseSchema}.DataContract"
        dataPipelineTable = f"{self.configurationDatabaseZone}.{self.configurationDatabaseSchema}.DataPipeline"
        connectionTable = f"{self.configurationDatabaseZone}.{self.configurationDatabaseSchema}.Connection"
        environmentTable = f"{self.configurationDatabaseZone}.{self.configurationDatabaseSchema}.Environment"
        
        # Construcción de la consulta SQL con filtros dinámicos
        pipelineFilter = "" if self.dataPipelineCode == "*" else f"AND DC.DataPipelineCode like '%{self.dataPipelineCode}'"
        
        query = f"""
        SELECT 
            DP.DestinationTableName AS SourceTableName,
            DC.*
        FROM {dataContractTable} DC
        INNER JOIN {dataPipelineTable} DP
            ON DC.DataPipelineCode = DP.Code
        INNER JOIN {connectionTable} CN
            ON DP.ConnectionCode = CN.Code
        INNER JOIN {environmentTable} EV
            ON CN.EnvironmentCode = EV.Code
        WHERE EV.Code = '{self.environmentCode}'
            AND DP.TopicCode = '{self.topicCode}'
            {pipelineFilter}
            AND DC.IsActive = 1
        ORDER BY DC.DataPipelineCode, DC.SortOrder
        """
        
        self._showQuery(query, "Consulta de Configuración")
        
        # Lectura de la configuración usando consulta SQL nativa
        configurationDataFrame = self.spark.sql(query)
        
        recordCount = configurationDataFrame.count()
        self._logInfo(f"Configuración obtenida: {recordCount} registros")
        self._showDataFrame(configurationDataFrame, "Configuración Obtenida", 20)
        
        return configurationDataFrame
    
    def groupConfigurationByTable(self, configurationDataFrame: DataFrame) -> Dict[str, List[Dict]]:
        """
        Agrupa la configuración por combinación de DataPipelineCode (origen) y tabla destino
        Cada DataPipelineCode representa un flujo único origen→destino
        
        Args:
            configurationDataFrame: DataFrame con toda la configuración
            
        Returns:
            Diccionario con configuración agrupada por pipeline origen→destino
        """
        self._logDebug("Agrupando configuración por pipeline origen-destino...")
        
        configurationByTable = {}
        
        # Conversión del DataFrame a lista de diccionarios para facilitar el procesamiento
        configurationRows = configurationDataFrame.collect()
        
        for row in configurationRows:
            # Clave compuesta: DataPipelineCode|ZonaDestino.EsquemaDestino.TablaDestino
            # Ejemplo: transit_tla_mayor|Silver.Finanzas.Mayor
            tableKey = f"{row.DataPipelineCode}|{row.DestinationZone}.{row.DestinationSchema}.{row.DestinationTable}"
            
            if tableKey not in configurationByTable:
                configurationByTable[tableKey] = []
            
            configurationByTable[tableKey].append(row.asDict())
        
        self._logInfo(f"Configuración agrupada en {len(configurationByTable)} flujo(s) pipeline→destino")
        
        if self.debugMode:
            for tableKey, config in configurationByTable.items():
                pipelineCode, destName = tableKey.split('|')
                # Obtener el nombre físico de la tabla origen del primer registro
                sourceTableName = config[0]['SourceTableName']
                self._logDebug(f"Flujo: [{pipelineCode}] {sourceTableName} → {destName} - {len(config)} columnas configuradas")
        
        return configurationByTable
    
    # =================================================================================
    # MÉTODOS DE GESTIÓN DE TABLAS
    # =================================================================================
    
    def checkIfTableExists(self, destinationZone: str, destinationSchema: str, destinationTable: str) -> bool:
        """
        Verifica si la tabla de destino existe en el lakehouse
        
        Args:
            destinationZone: Zona de destino (Silver)
            destinationSchema: Esquema de destino
            destinationTable: Nombre de la tabla de destino
            
        Returns:
            True si la tabla existe, False en caso contrario
        """
        tablePath = f"{destinationZone}.{destinationSchema}.{destinationTable}"
        
        try:
            self.spark.read.table(tablePath)
            self._logDebug(f"Tabla {tablePath} existe en el destino")
            return True
        except:
            self._logDebug(f"Tabla {tablePath} no existe en el destino")
            return False
    
    def createDestinationTable(self, tableConfiguration: List[Dict]):
        """
        Crea la tabla de destino con su estructura, particiones y comentarios
        
        Args:
            tableConfiguration: Lista con la configuración de columnas de la tabla
        """
        # Obtención de información básica de la tabla
        firstRow = tableConfiguration[0]
        destinationZone = firstRow['DestinationZone']
        destinationSchema = firstRow['DestinationSchema']
        destinationTable = firstRow['DestinationTable']
        tablePath = f"{destinationZone}.{destinationSchema}.{destinationTable}"
        
        self._logInfo(f"Creando tabla {tablePath}...")
        
        # Identificación de columnas de partición
        partitionColumns = self._getPartitionColumns(tableConfiguration)
        
        # Construcción de la sentencia DDL con comentarios incluidos
        ddlStatement = self._buildCreateTableDDL(
            tablePath, 
            tableConfiguration, 
            partitionColumns
        )
        
        self._showQuery(ddlStatement, "DDL de Creación de Tabla")
        
        # Ejecución del DDL
        self.spark.sql(ddlStatement)
        
        self._logInfo(f"Tabla {tablePath} creada exitosamente")
    
    def _getPartitionColumns(self, tableConfiguration: List[Dict]) -> List[str]:
        """
        Identifica las columnas de partición ordenadas por jerarquía
        
        Args:
            tableConfiguration: Configuración de columnas
            
        Returns:
            Lista ordenada de columnas de partición [Part1, Part2, Part3]
        """
        partitionColumns = []
        
        # Búsqueda de columnas Part1, Part2, Part3
        for partitionLevel in ['Part1', 'Part2', 'Part3']:
            for column in tableConfiguration:
                if column['AttributeType'] == partitionLevel:
                    partitionColumns.append(column['DestinationField'])
                    break
        
        if partitionColumns:
            self._logDebug(f"Columnas de partición identificadas: {', '.join(partitionColumns)}")
        
        return partitionColumns
    
    def _buildCreateTableDDL(self, tablePath: str, tableConfiguration: List[Dict], 
                            partitionColumns: List[str]) -> str:
        """
        Construye la sentencia DDL para crear la tabla con comentarios incluidos
        
        Args:
            tablePath: Ruta completa de la tabla
            tableConfiguration: Configuración de columnas
            partitionColumns: Lista de columnas de partición
            
        Returns:
            Sentencia DDL completa con comentarios
        """
        # Construcción de las definiciones de columnas con comentarios
        columnDefinitions = []
        for column in tableConfiguration:
            # Construcción de la definición básica de columna
            columnDef = f"`{column['DestinationField']}` {column['DestinationFieldDataType']}"
            
            # Agregar comentario si existe descripción
            if column.get('DestinationFieldDescription'):
                # Escape de comillas simples en la descripción
                description = column['DestinationFieldDescription'].replace("'", "''")
                columnDef += f" COMMENT '{description}'"
            
            columnDefinitions.append(columnDef)
        
        columnsClause = ",\n    ".join(columnDefinitions)
        
        # Construcción de la cláusula de partición
        partitionClause = ""
        if partitionColumns:
            partitionClause = f"\nPARTITIONED BY ({', '.join([f'`{col}`' for col in partitionColumns])})"
        
        # Sentencia DDL completa
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {tablePath} (
            {columnsClause}
        )
        USING DELTA
        {partitionClause}
        """
        
        return ddl
    def _consolidateTableConfiguration(self, allPipelineConfigs: List[List[Dict]], destinationTablePath: str) -> List[Dict]:
        """
        Consolida configuraciones de múltiples pipelines que van a la misma tabla destino
        Elimina duplicados por DestinationField y valida consistencia
        
        Reglas de consolidación:
        - Tipo de dato: Debe ser IGUAL en todos los pipelines (error si difiere)
        - Descripción: Se usa la más larga
        - Nullability: Más permisivo (NULLABLE si hay conflicto)
        - SortOrder: Primera aparición
        - AttributeType: Se confía ciegamente (no se valida)
        - Orden final: Columnas comunes primero, luego específicas
        
        Args:
            allPipelineConfigs: Lista de configuraciones (una por cada pipeline)
            destinationTablePath: Path de la tabla destino (para mensajes de error)
            
        Returns:
            Configuración consolidada ordenada y sin duplicados
            
        Raises:
            ValueError: Si hay conflictos de tipo de dato entre pipelines
        """
        self._logDebug(f"Consolidando configuraciones de {len(allPipelineConfigs)} pipeline(s) para {destinationTablePath}")
        
        # Diccionario para consolidar: {DestinationField: config_consolidada}
        consolidatedFields = {}
        
        # Contador de apariciones por campo para identificar comunes vs específicas
        fieldAppearanceCount = {}
        totalPipelines = len(allPipelineConfigs)
        
        # Primera pasada: consolidar campos y contar apariciones
        for pipelineConfig in allPipelineConfigs:
            for column in pipelineConfig:
                destinationField = column['DestinationField']
                
                # Contar apariciones
                if destinationField not in fieldAppearanceCount:
                    fieldAppearanceCount[destinationField] = 0
                fieldAppearanceCount[destinationField] += 1
                
                # Si es la primera vez que vemos este campo, agregarlo
                if destinationField not in consolidatedFields:
                    consolidatedFields[destinationField] = column.copy()
                    self._logDebug(f"Campo nuevo: {destinationField} - Tipo: {column['DestinationFieldDataType']}")
                else:
                    # Campo ya existe, aplicar reglas de consolidación
                    existing = consolidatedFields[destinationField]
                    
                    # REGLA 1: Validar que tipo de dato sea IGUAL (lanzar error si difiere)
                    if existing['DestinationFieldDataType'] != column['DestinationFieldDataType']:
                        raise ValueError(
                            f"Conflicto de tipo de dato para columna '{destinationField}' en tabla {destinationTablePath}:\n"
                            f"  Pipeline anterior: {column['DestinationFieldDataType']}\n"
                            f"  Pipeline actual: {existing['DestinationFieldDataType']}\n"
                            f"  Los tipos de dato deben ser idénticos en todos los pipelines."
                        )
                    
                    # REGLA 2: Descripción - usar la más larga
                    existingDesc = existing.get('DestinationFieldDescription', '')
                    currentDesc = column.get('DestinationFieldDescription', '')
                    if len(currentDesc) > len(existingDesc):
                        existing['DestinationFieldDescription'] = currentDesc
                        self._logDebug(f"Campo {destinationField}: Descripción actualizada (más larga)")
                    
                    # REGLA 3: Nullability - más permisivo (1 = NULLABLE prevalece)
                    if column['IsNullable'] == 1:
                        if existing['IsNullable'] == 0:
                            self._logDebug(f"Campo {destinationField}: Cambiado a NULLABLE (más permisivo)")
                        existing['IsNullable'] = 1
                    
                    # REGLA 4: SortOrder - mantener el de primera aparición (no hacer nada)
                    
                    # REGLA 5: AttributeType - confiar ciegamente, si el actual tiene valor y el anterior no, actualizar
                    if not existing.get('AttributeType') and column.get('AttributeType'):
                        existing['AttributeType'] = column['AttributeType']
                        self._logDebug(f"Campo {destinationField}: AttributeType actualizado a {column['AttributeType']}")
        
        # Segunda pasada: clasificar en comunes y específicas
        commonFields = []
        specificFields = []
        
        for destinationField, config in consolidatedFields.items():
            if fieldAppearanceCount[destinationField] == totalPipelines:
                # Campo común (aparece en todos los pipelines)
                commonFields.append(config)
            else:
                # Campo específico (aparece solo en algunos pipelines)
                specificFields.append(config)
        
        # REGLA 6: Ordenar - comunes primero (por SortOrder), luego específicas (por SortOrder)
        commonFields.sort(key=lambda x: x['SortOrder'])
        specificFields.sort(key=lambda x: x['SortOrder'])
        
        # Consolidar lista final
        consolidatedConfig = commonFields + specificFields
        
        self._logInfo(f"Configuración consolidada: {len(consolidatedConfig)} columna(s) total")
        self._logInfo(f"  - Comunes (en todos los pipelines): {len(commonFields)}")
        self._logInfo(f"  - Específicas (en algunos pipelines): {len(specificFields)}")
        
        if self.debugMode:
            self._logDebug("Columnas comunes:")
            for field in commonFields:
                self._logDebug(f"  - {field['DestinationField']} ({field['DestinationFieldDataType']})")
            self._logDebug("Columnas específicas:")
            for field in specificFields:
                self._logDebug(f"  - {field['DestinationField']} ({field['DestinationFieldDataType']}) - Aparece en {fieldAppearanceCount[field['DestinationField']]}/{totalPipelines} pipelines")
        
        return consolidatedConfig    
    # =================================================================================
    # MÉTODOS DE CAPTURA DE VERSIÓN Y MÉTRICAS
    # =================================================================================
    
    def getTableVersion(self, tablePath: str) -> int:
        """
        Obtiene la versión actual de una tabla Delta
        
        Args:
            tablePath: Ruta de la tabla Delta
            
        Returns:
            Número de versión de la tabla
        """
        try:
            deltaTable = DeltaTable.forName(self.spark, tablePath)
            historyDataFrame = deltaTable.history(1)
            
            if historyDataFrame.count() > 0:
                version = historyDataFrame.collect()[0]["version"]
                self._logDebug(f"Versión actual de {tablePath}: {version}")
                return version
            
            return 0
            
        except Exception as e:
            self._logDebug(f"No se pudo obtener versión de tabla {tablePath}: {str(e)}")
            return 0
    
    def captureOperationMetrics(self, tablePath: str, versionBefore: int, versionAfter: int) -> Dict[str, int]:
        """
        Captura las métricas de la operación Delta comparando versiones
        Si la versión no cambió, retorna métricas en cero
        
        Args:
            tablePath: Ruta de la tabla Delta
            versionBefore: Versión de la tabla antes de la operación
            versionAfter: Versión de la tabla después de la operación
            
        Returns:
            Diccionario con las métricas de la operación
        """
        # Si la versión no cambió, no hubo cambios
        if versionBefore == versionAfter:
            self._logDebug(f"Versión no cambió ({versionBefore}), no hubo cambios en la tabla")
            return {
                'recordsInserted': 0,
                'recordsUpdated': 0,
                'recordsDeleted': 0,
                'recordsCopied': 0,
                'recordsMatched': 0,
                'recordsUnchanged': 0
            }
        
        # Si la versión cambió, capturar métricas del historial
        try:
            deltaTable = DeltaTable.forName(self.spark, tablePath)
            
            # Obtener el historial de la última operación (la versión después)
            historyDataFrame = deltaTable.history(1)
            
            if historyDataFrame.count() > 0:
                lastOperation = historyDataFrame.collect()[0]
                
                # Extracción de las métricas de la operación
                operationMetrics = lastOperation["operationMetrics"]
                
                metrics = {
                    'recordsInserted': int(operationMetrics.get("numTargetRowsInserted", 0)),
                    'recordsUpdated': int(operationMetrics.get("numTargetRowsUpdated", 0)),
                    'recordsDeleted': int(operationMetrics.get("numTargetRowsDeleted", 0)),
                    'recordsCopied': int(operationMetrics.get("numTargetRowsCopied", 0)),
                    'recordsMatched': int(operationMetrics.get("numTargetRowsMatchedUpdated", 0)) + 
                                     int(operationMetrics.get("numTargetRowsMatchedDeleted", 0)),
                    'recordsUnchanged': int(operationMetrics.get("numTargetRowsNotMatched", 0))
                }
                
                self._logDebug(f"Métricas capturadas (versión {versionBefore} -> {versionAfter}): "
                             f"Insertados={metrics['recordsInserted']}, "
                             f"Actualizados={metrics['recordsUpdated']}, "
                             f"Eliminados={metrics['recordsDeleted']}")
                
                return metrics
            
        except Exception as e:
            self._logDebug(f"No se pudieron capturar métricas de la operación: {str(e)}")
        
        # Retornar métricas vacías si no se pueden obtener
        return {
            'recordsInserted': 0,
            'recordsUpdated': 0,
            'recordsDeleted': 0,
            'recordsCopied': 0,
            'recordsMatched': 0,
            'recordsUnchanged': 0
        }
    
    # =================================================================================
    # MÉTODOS DE LECTURA Y TRANSFORMACIÓN DE DATOS
    # =================================================================================
    
    def readSourceData(self, tableConfiguration: List[Dict]) -> DataFrame:
        """
        Lee los datos desde la tabla de origen en la zona Bronze
        
        Args:
            tableConfiguration: Configuración de la tabla
            
        Returns:
            DataFrame con los datos de origen
        """
        firstRow = tableConfiguration[0]
        sourceLayer = firstRow['SourceLayer']
        sourceSchema = firstRow['SourceSchema']
        dataPipelineCode = firstRow['SourceTableName']
        
        sourcePath = f"{sourceLayer}.{sourceSchema}.{dataPipelineCode}"
        
        self._logInfo(f"Leyendo datos desde {sourcePath}...")
        
        sourceDataFrame = self.spark.read.table(sourcePath)
        #  Agregar columna con nombre origen de la tabla
        recordCount = sourceDataFrame.count()
        self._logInfo(f"Registros leídos desde origen: {recordCount}")
        self._showDataFrame(sourceDataFrame, f"Datos de Origen - {sourcePath}", 5)
        
        return sourceDataFrame
    
    def transformSourceToDestination(self, sourceDataFrame: DataFrame, 
                                 tableConfiguration: List[Dict]) -> DataFrame:
        """
        Transforma los datos de origen aplicando el mapeo de columnas y tipos de datos
        Soporta tanto columnas simples como expresiones SQL en SourceColumn
        
        Args:
            sourceDataFrame: DataFrame con datos de origen
            tableConfiguration: Configuración del mapeo de columnas
            
        Returns:
            DataFrame transformado con estructura de destino
        """
        self._logInfo("Aplicando transformación de columnas...")
        
        # Construcción de la lista de selección de columnas
        selectExpressions = []
        
        for column in tableConfiguration:
            sourceColumn = column['SourceColumn']
            destinationField = column['DestinationField']
            destinationDataType = column['DestinationFieldDataType']
            
            # Usar expr() en lugar de col() para soportar tanto columnas como expresiones SQL
            # Esto permite: 'FECHA', 'trim(NOMBRE)', "date_format(FECHA, 'yyyyMM')", 'coalesce(VALOR, 0)', etc.
            selectExpressions.append(
                expr(sourceColumn).cast(destinationDataType).alias(destinationField)
            )
            
            # Detectar si es una expresión o columna simple para logging
            isExpression = '(' in sourceColumn or sourceColumn.strip() != sourceColumn
            logType = "Expresión" if isExpression else "Columna"
            
            self._logDebug(f"Mapeo {logType}: {sourceColumn} ({column['SourceDataType']}) -> {destinationField} ({destinationDataType})")
        
        transformedDataFrame = sourceDataFrame.select(selectExpressions)
        
        self._logInfo("Transformación aplicada exitosamente")
        self._showDataFrame(transformedDataFrame, "Datos Transformados", 5)
        
        return transformedDataFrame
    
    # =================================================================================
    # MÉTODOS DE ESCRITURA - MERGE
    # =================================================================================
    
    def processMergeWriteType(self, transformedDataFrame: DataFrame, 
                         tableConfiguration: List[Dict]):
        """
        Procesa la escritura de datos utilizando el método MERGE
        Actualiza registros existentes solo si hay cambios e inserta nuevos basándose en clave primaria
        
        Args:
            transformedDataFrame: DataFrame con datos transformados
            tableConfiguration: Configuración de la tabla
        """
        tableStartTime = datetime.now()
        
        firstRow = tableConfiguration[0]
        pipelineCode = firstRow['DataPipelineCode']
        sourceTableName = firstRow['SourceTableName']
        destinationZone = firstRow['DestinationZone']
        destinationSchema = firstRow['DestinationSchema']
        destinationTable = firstRow['DestinationTable']
        tablePath = f"{destinationZone}.{destinationSchema}.{destinationTable}"
        
        # Nombre descriptivo para trazabilidad: [pipeline] TablaOrigen → TablaDestino
        traceTableName = f"[{pipelineCode}] {sourceTableName} → {tablePath}"
        
        self._logInfo(f"Iniciando proceso MERGE para {tablePath}...")
        
        # Captura de métricas iniciales
        sourceRecords = transformedDataFrame.count()
        
        # Captura de versión antes del merge
        versionBefore = self.getTableVersion(tablePath)
        self._logDebug(f"Versión antes del MERGE: {versionBefore}")
        
        # Identificación de columnas de clave primaria
        primaryKeyColumns = self._getPrimaryKeyColumns(tableConfiguration)
        
        if not primaryKeyColumns:
            raise ValueError(f"WriteType MERGE requiere al menos una columna marcada como PK")
        
        self._logDebug(f"Columnas PK identificadas: {', '.join(primaryKeyColumns)}")
        
        # Identificación de columnas de partición para optimización
        partitionColumns = self._getPartitionColumns(tableConfiguration)
        
        # Construcción de la condición de merge basada en PK
        mergeCondition = self._buildMergeCondition(primaryKeyColumns)
        
        # Identificación de todas las columnas para actualización y comparación
        allColumns = [column['DestinationField'] for column in tableConfiguration]
        updateColumns = [col for col in allColumns if col not in primaryKeyColumns]
        
        # Construcción de la condición de cambio (al menos una columna debe ser diferente)
        changeCondition = self._buildChangeCondition(updateColumns)
        
        # Construcción del diccionario de actualización (excluye PKs)
        updateColumnsDict = {col: f"source.{col}" for col in updateColumns}
        
        # Construcción del diccionario de inserción
        insertColumnsDict = {col: f"source.{col}" for col in allColumns}
        
        self._logDebug(f"Condición de MERGE (PK): {mergeCondition}")
        self._logDebug(f"Condición de CAMBIO: {changeCondition}")
        
        # Carga de la tabla Delta de destino
        destinationDeltaTable = DeltaTable.forName(self.spark, tablePath)
        
        # Ejecución del MERGE
        destinationDeltaTable.alias("target") \
            .merge(
                transformedDataFrame.alias("source"),
                mergeCondition
            ) \
            .whenMatchedUpdate(
                condition=changeCondition,
                set=updateColumnsDict
            ) \
            .whenNotMatchedInsert(values=insertColumnsDict) \
            .execute()
        
        # Captura de versión después del merge
        versionAfter = self.getTableVersion(tablePath)
        self._logDebug(f"Versión después del MERGE: {versionAfter}")
        
        # Captura de métricas comparando versiones
        metrics = self.captureOperationMetrics(tablePath, versionBefore, versionAfter)
        
        recordsInserted = metrics['recordsInserted']
        recordsUpdated = metrics['recordsUpdated']
        recordsDeleted = metrics['recordsDeleted']
        recordsMatched = metrics['recordsMatched']
        recordsUnchanged = metrics['recordsUnchanged']
        
        self._logInfo(f"Registros insertados: {recordsInserted:,}")
        self._logInfo(f"Registros actualizados: {recordsUpdated:,}")
        self._logDebug(f"Registros que hicieron match: {recordsMatched:,}")
        self._logDebug(f"Registros sin cambios: {recordsUnchanged:,}")
        
        # Cálculo de tiempo de ejecución
        tableEndTime = datetime.now()
        executionTime = (tableEndTime - tableStartTime).total_seconds()
        
        # Registro de trazabilidad con nombre descriptivo del flujo completo
        self.registerTableProcessing(
            tableName=traceTableName,
            writeType="Merge",
            sourceRecords=sourceRecords,
            recordsInserted=recordsInserted,
            recordsUpdated=recordsUpdated,
            recordsDeleted=recordsDeleted,
            recordsMatched=recordsMatched,
            recordsUnchanged=recordsUnchanged,
            executionTimeSeconds=executionTime,
            status="SUCCESS"
        )
        
        self._logInfo(f"Proceso MERGE completado para {tablePath}")
    
    def _getPrimaryKeyColumns(self, tableConfiguration: List[Dict]) -> List[str]:
        """
        Identifica las columnas marcadas como clave primaria
        
        Args:
            tableConfiguration: Configuración de columnas
            
        Returns:
            Lista de nombres de columnas PK
        """
        primaryKeyColumns = []
        
        for column in tableConfiguration:
            if column['AttributeType'] in ('PK','Part1','Part2','Part3'):
                primaryKeyColumns.append(column['DestinationField'])
        return primaryKeyColumns
    
    def _buildMergeCondition(self, primaryKeyColumns: List[str]) -> str:
        """
        Construye la condición de merge basada en columnas PK
        
        Args:
            primaryKeyColumns: Lista de columnas de clave primaria
            
        Returns:
            Condición SQL para el merge
        """
        conditions = [f"target.`{col}` = source.`{col}`" for col in primaryKeyColumns]
        return " AND ".join(conditions)
    
    def _buildChangeCondition(self, updateColumns: List[str]) -> str:
        """
        Construye la condición que verifica si al menos una columna ha cambiado
        
        Args:
            updateColumns: Lista de columnas a comparar (excluye PKs)
            
        Returns:
            Condición SQL que verifica cambios en al menos una columna
        """
        # Construcción de comparaciones que incluyen manejo de NULLs
        changeConditions = []
        
        for col in updateColumns:
            # Condición que considera NULL != valor y valor != NULL como cambio
            condition = f"(target.`{col}` != source.`{col}` OR " \
                       f"(target.`{col}` IS NULL AND source.`{col}` IS NOT NULL) OR " \
                       f"(target.`{col}` IS NOT NULL AND source.`{col}` IS NULL))"
            changeConditions.append(condition)
        
        # Si hay cambios en al menos una columna, se actualiza
        return " OR ".join(changeConditions)
    
    # =================================================================================
    # MÉTODOS DE ESCRITURA - DELTA
    # =================================================================================
    
    def processDeltaWriteType(self, transformedDataFrame: DataFrame, 
                         tableConfiguration: List[Dict]):
        """
        Procesa la escritura de datos utilizando el método DELTA
        Borra registros en el rango (min-max) de la columna Delta e inserta los nuevos
        
        Args:
            transformedDataFrame: DataFrame con datos transformados
            tableConfiguration: Configuración de la tabla
        """
        tableStartTime = datetime.now()
        
        firstRow = tableConfiguration[0]
        pipelineCode = firstRow['DataPipelineCode']  # NUEVO
        sourceTableName = firstRow['SourceTableName']  # NUEVO
        destinationZone = firstRow['DestinationZone']
        destinationSchema = firstRow['DestinationSchema']
        destinationTable = firstRow['DestinationTable']
        tablePath = f"{destinationZone}.{destinationSchema}.{destinationTable}"
        
        # Nombre descriptivo para trazabilidad
        traceTableName = f"[{pipelineCode}] {sourceTableName} → {tablePath}"  # NUEVO
        
        self._logInfo(f"Iniciando proceso DELTA para {tablePath}...")
        
        # Captura de métricas iniciales
        sourceRecords = transformedDataFrame.count()
        
        # Identificación de columna Delta
        deltaColumn = self._getDeltaColumn(tableConfiguration)
        
        if not deltaColumn:
            raise ValueError(f"WriteType DELTA requiere al menos una columna marcada como Delta")
        
        self._logDebug(f"Columna DELTA identificada: {deltaColumn}")
        
        # Obtención del rango de valores (min y max) desde los datos de origen transformados
        minMaxValues = self._getMinMaxValuesFromSource(transformedDataFrame, deltaColumn)
        
        if minMaxValues is None:
            self._logInfo(f"No se encontraron valores para la columna {deltaColumn}, no se procesará")
            
            tableEndTime = datetime.now()
            executionTime = (tableEndTime - tableStartTime).total_seconds()
            
            self.registerTableProcessing(
                tableName=traceTableName,  # CAMBIO
                writeType="Delta",
                sourceRecords=0,
                executionTimeSeconds=executionTime,
                status="SUCCESS"
            )
            return
        
        minValue, maxValue = minMaxValues
        self._logInfo(f"Rango de valores - Min: {minValue}, Max: {maxValue}")
        
        # Captura de versión antes del DELETE
        versionBeforeDelete = self.getTableVersion(tablePath)
        self._logDebug(f"Versión antes del DELETE: {versionBeforeDelete}")
        
        # Obtener tabla Delta
        destinationDeltaTable = DeltaTable.forName(self.spark, tablePath)
        
        # Eliminación de registros en el rango especificado
        deleteCondition = f"`{deltaColumn}` >= '{minValue}' AND `{deltaColumn}` <= '{maxValue}'"
        self._logDebug(f"Condición de borrado: {deleteCondition}")
        
        # Ejecutar DELETE
        destinationDeltaTable.delete(deleteCondition)
        
        # Captura de versión después del DELETE
        versionAfterDelete = self.getTableVersion(tablePath)
        self._logDebug(f"Versión después del DELETE: {versionAfterDelete}")
        
        # Captura de métricas del DELETE
        deleteMetrics = self.captureOperationMetrics(tablePath, versionBeforeDelete, versionAfterDelete)
        recordsDeleted = deleteMetrics['recordsDeleted']
        
        self._logInfo(f"Registros eliminados: {recordsDeleted:,}")
        
        # Captura de versión antes del INSERT
        versionBeforeInsert = self.getTableVersion(tablePath)
        
        # Inserción de nuevos registros
        transformedDataFrame.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(tablePath)
        
        # Captura de versión después del INSERT
        versionAfterInsert = self.getTableVersion(tablePath)
        self._logDebug(f"Versión después del INSERT: {versionAfterInsert}")
        
        # Para INSERT, si la versión cambió, los registros insertados son los del source
        recordsInserted = sourceRecords if versionBeforeInsert != versionAfterInsert else 0
        
        self._logInfo(f"Registros insertados: {recordsInserted:,}")
        
        # Cálculo de tiempo de ejecución
        tableEndTime = datetime.now()
        executionTime = (tableEndTime - tableStartTime).total_seconds()
        
        # Registro de trazabilidad
        self.registerTableProcessing(
            tableName=traceTableName,  # CAMBIO
            writeType="Delta",
            sourceRecords=sourceRecords,
            recordsInserted=recordsInserted,
            recordsDeleted=recordsDeleted,
            executionTimeSeconds=executionTime,
            status="SUCCESS"
        )
        
        self._logInfo(f"Proceso DELTA completado para {tablePath}")
    
    def _getDeltaColumn(self, tableConfiguration: List[Dict]) -> Optional[str]:
        """
        Identifica la columna marcada como Delta
        
        Args:
            tableConfiguration: Configuración de columnas
            
        Returns:
            Nombre de la columna Delta o None
        """
        for column in tableConfiguration:
            if column['AttributeType'] == 'Delta':
                return column['DestinationField']
        
        return None
    
    def _getMinMaxValuesFromSource(self, dataFrame: DataFrame, columnName: str) -> Optional[Tuple]:
        """
        Obtiene los valores mínimo y máximo de una columna en el DataFrame
        
        Args:
            dataFrame: DataFrame de origen
            columnName: Nombre de la columna
            
        Returns:
            Tupla (minValue, maxValue) o None si no hay datos
        """
        result = dataFrame.agg(
            spark_min(col(columnName)).alias("min_value"),
            spark_max(col(columnName)).alias("max_value")
        ).collect()[0]
        
        minValue = result['min_value']
        maxValue = result['max_value']
        
        if minValue is None or maxValue is None:
            return None
        
        return (minValue, maxValue)
    
    # =================================================================================
    # MÉTODOS DE ESCRITURA - OVERWRITE
    # =================================================================================
    
    def processOverwriteWriteType(self, transformedDataFrame: DataFrame, 
                              tableConfiguration: List[Dict]):
        """
        Procesa la escritura de datos utilizando el método OVERWRITE
        Sobrescribe completamente la tabla de destino con los nuevos datos
        
        Args:
            transformedDataFrame: DataFrame con datos transformados
            tableConfiguration: Configuración de la tabla
        """
        tableStartTime = datetime.now()
        
        firstRow = tableConfiguration[0]
        pipelineCode = firstRow['DataPipelineCode']  # NUEVO
        sourceTableName = firstRow['SourceTableName']  # NUEVO
        destinationZone = firstRow['DestinationZone']
        destinationSchema = firstRow['DestinationSchema']
        destinationTable = firstRow['DestinationTable']
        tablePath = f"{destinationZone}.{destinationSchema}.{destinationTable}"
        
        # Nombre descriptivo para trazabilidad
        traceTableName = f"[{pipelineCode}] {sourceTableName} → {tablePath}"  # NUEVO
        
        self._logInfo(f"Iniciando proceso OVERWRITE para {tablePath}...")
        
        # Captura de métricas
        sourceRecords = transformedDataFrame.count()
        
        # Captura de versión antes del OVERWRITE
        versionBefore = self.getTableVersion(tablePath)
        self._logDebug(f"Versión antes del OVERWRITE: {versionBefore}")
        
        # Sobrescritura completa de la tabla
        transformedDataFrame.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(tablePath)
        
        # Captura de versión después del OVERWRITE
        versionAfter = self.getTableVersion(tablePath)
        self._logDebug(f"Versión después del OVERWRITE: {versionAfter}")
        
        # Para OVERWRITE, si la versión cambió, todos los registros del source fueron escritos
        recordsInserted = sourceRecords if versionBefore != versionAfter else 0
        
        self._logInfo(f"Registros escritos: {recordsInserted:,}")
        
        # Cálculo de tiempo de ejecución
        tableEndTime = datetime.now()
        executionTime = (tableEndTime - tableStartTime).total_seconds()
        
        # Registro de trazabilidad
        self.registerTableProcessing(
            tableName=traceTableName,  # CAMBIO
            writeType="Overwrite",
            sourceRecords=sourceRecords,
            recordsInserted=recordsInserted,
            executionTimeSeconds=executionTime,
            status="SUCCESS"
        )
        
        self._logInfo(f"Proceso OVERWRITE completado para {tablePath}")
    
    # =================================================================================
    # MÉTODO PRINCIPAL DE PROCESAMIENTO
    # =================================================================================
    
    def processDataPipeline(self):
        """
        Método principal que orquesta todo el proceso de migración de datos
        """
        try:
            self._logInfo("=" * 80)
            self._logInfo("INICIO DEL PROCESO DE MIGRACIÓN BRONZE TO SILVER")
            self._logInfo("=" * 80)
            
            # Paso 1: Obtener configuración desde lakehouse
            configurationDataFrame = self.getConfigurationFromDatabase()
            
            if configurationDataFrame.count() == 0:
                self._logInfo("No se encontró configuración para procesar")
                return
            
            # Paso 2: Agrupar configuración por pipeline origen→destino
            configurationByPipeline = self.groupConfigurationByTable(configurationDataFrame)
            
            # Paso 3: Agrupar pipelines por tabla de destino para consolidar configuraciones
            pipelinesByDestination = {}
            for pipelineKey, pipelineConfig in configurationByPipeline.items():
                # Extraer tabla de destino de la clave: "transit_tla_mayor|Silver.Finanzas.Mayor"
                _, destinationPath = pipelineKey.split('|')
                
                if destinationPath not in pipelinesByDestination:
                    pipelinesByDestination[destinationPath] = []
                
                pipelinesByDestination[destinationPath].append({
                    'pipelineKey': pipelineKey,
                    'config': pipelineConfig
                })
            
            # Paso 4: Crear tablas de destino con configuración consolidada (una sola vez por tabla)
            createdTables = {}
            
            for destinationPath, pipelines in pipelinesByDestination.items():
                self._logInfo("-" * 80)
                self._logInfo(f"Preparando tabla destino: {destinationPath}")
                self._logInfo(f"Pipelines que alimentan esta tabla: {len(pipelines)}")
                
                # Extraer información de destino del primer pipeline (todos apuntan a la misma tabla)
                firstPipelineConfig = pipelines[0]['config']
                firstRow = firstPipelineConfig[0]
                destinationZone = firstRow['DestinationZone']
                destinationSchema = firstRow['DestinationSchema']
                destinationTable = firstRow['DestinationTable']
                
                # Verificar si la tabla ya existe
                tableExists = self.checkIfTableExists(destinationZone, destinationSchema, destinationTable)
                
                if not tableExists:
                    # Consolidar configuraciones de TODOS los pipelines que van a esta tabla
                    allPipelineConfigs = [p['config'] for p in pipelines]
                    
                    self._logInfo(f"Consolidando configuraciones de {len(allPipelineConfigs)} pipeline(s)...")
                    consolidatedConfig = self._consolidateTableConfiguration(allPipelineConfigs, destinationPath)
                    
                    # Crear tabla con configuración consolidada
                    self._logInfo(f"Tabla {destinationPath} no existe, creándola con {len(consolidatedConfig)} columnas...")
                    self.createDestinationTable(consolidatedConfig)
                    
                    createdTables[destinationPath] = True
                else:
                    self._logInfo(f"Tabla {destinationPath} ya existe, omitiendo creación")
                    createdTables[destinationPath] = True
            
            # Paso 5: Procesar cada flujo pipeline→destino
            for pipelineKey, pipelineConfiguration in configurationByPipeline.items():
                try:
                    # Separar DataPipelineCode y tabla destino de la clave compuesta
                    # Ejemplo: "transit_tla_mayor|Silver.Finanzas.Mayor"
                    pipelineCode, destinationName = pipelineKey.split('|')
                    
                    firstRow = pipelineConfiguration[0]
                    sourceTableName = firstRow['SourceTableName']  # Nombre físico de la tabla origen
                    destinationZone = firstRow['DestinationZone']
                    destinationSchema = firstRow['DestinationSchema']
                    destinationTable = firstRow['DestinationTable']
                    writeType = firstRow['WriteType']
                    
                    self._logInfo("-" * 80)
                    self._logInfo(f"Procesando pipeline: [{pipelineCode}]")
                    self._logInfo(f"Origen: {sourceTableName} → Destino: {destinationName}")
                    self._logInfo(f"WriteType: {writeType}")
                    self._logInfo("-" * 80)
                    
                    # La tabla ya fue creada en el paso anterior, continuar con procesamiento
                    
                    # Paso 5.1: Leer datos desde tabla origen específica
                    sourceDataFrame = self.readSourceData(pipelineConfiguration)
                    
                    # Validar que haya datos para procesar
                    if sourceDataFrame.count() == 0:
                        self._logInfo(f"No hay datos en origen {sourceTableName}, omitiendo procesamiento")
                        continue
                    
                    # Paso 5.2: Transformar datos (solo las columnas configuradas para ESTE pipeline)
                    transformedDataFrame = self.transformSourceToDestination(sourceDataFrame, pipelineConfiguration)
                    
                    # Paso 5.3: Escribir datos según WriteType
                    if writeType == 'Merge':
                        self.processMergeWriteType(transformedDataFrame, pipelineConfiguration)
                    elif writeType == 'Delta':
                        self.processDeltaWriteType(transformedDataFrame, pipelineConfiguration)
                    elif writeType == 'Overwrite':
                        self.processOverwriteWriteType(transformedDataFrame, pipelineConfiguration)
                    else:
                        raise ValueError(f"WriteType no soportado: {writeType}")
                    
                    self._logInfo(f"✓ Pipeline [{pipelineCode}]: {sourceTableName} → {destinationName} procesado exitosamente")
                    
                except Exception as tableError:
                    errorMessage = str(tableError)
                    logger.error(f"✗ Error procesando pipeline {pipelineKey}: {errorMessage}")
                    
                    # Extraer información para trazabilidad
                    try:
                        pipelineCode, destinationName = pipelineKey.split('|')
                        sourceTableName = pipelineConfiguration[0]['SourceTableName'] if pipelineConfiguration else 'Unknown'
                        traceTableName = f"[{pipelineCode}] {sourceTableName} → {destinationName}"
                    except:
                        traceTableName = pipelineKey
                    
                    # Registro de trazabilidad para tabla con error
                    self.registerTableProcessing(
                        tableName=traceTableName,
                        writeType=firstRow.get('WriteType', 'Unknown') if pipelineConfiguration else 'Unknown',
                        sourceRecords=0,
                        status="ERROR",
                        errorMessage=errorMessage
                    )
                    
                    # Continuar con el siguiente pipeline en lugar de abortar todo el proceso
                    continue
            
            self._logInfo("=" * 80)
            self._logInfo("PROCESO COMPLETADO")
            self._logInfo("=" * 80)
            
            # Mostrar resumen de trazabilidad al finalizar
            self.showProcessingSummary()
            
        except Exception as e:
            logger.error(f"Error en el proceso principal: {str(e)}")
            self.showProcessingSummary()
            raise



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Instanciación y ejecución del procesador
processor = ConfigurationDrivenDataProcessor(
    environmentCode=environmentCode,
    topicCode=topicCode,
    dataPipelineCode=dataPipelineCode,
    configurationDatabaseZone=configurationDatabaseZone,
    configurationDatabaseSchema=configurationDatabaseSchema,
    debugMode=debugMode
)

# Ejecución del pipeline completo
processor.processDataPipeline()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
