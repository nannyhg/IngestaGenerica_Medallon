CREATE PROCEDURE [Config].[uspGetPipelineConfiguration]
    @ProjectCode VARCHAR(50),
    @ProjectTopicCode VARCHAR(50),
    @DataPipelineCode VARCHAR(50) = '*',  -- '*' significa todos los pipelines del topic
    @EnvironmentCode VARCHAR(50),
    @Debug BIT = 0,  -- 1 para mostrar logs detallados
    @IsDeltaActive INT = 1 -- 0 para cargas full
AS
BEGIN
    SET NOCOUNT ON;

    /*******************************************************************************
     * Procedimiento: uspGetPipelineConfiguration
     * Descripción: Obtiene la configuración de pipelines con tokens reemplazados
     * 
     * Parámetros:
     *   @ProjectCode      - Código del proyecto
     *   @ProjectTopicCode - Código del tópico del proyecto
     *   @DataPipelineCode - Código del pipeline ('*' para todos los pipelines del topic)
     *   @EnvironmentCode  - Código del ambiente (DEV, QA, PROD)
     *   @Debug            - 1 para logs detallados, 0 para logs mínimos
     *   @IsDeltaActive     - 1 para cargas delta, 0 para cargas full
     * 
     * Retorna:
     *   Configuración completa del pipeline con tokens reemplazados en 
     *   SourceExecutionCommand y SourceCondition
     * 
     * Ejemplo de uso:
     *   EXEC [Config].[uspGetPipelineConfiguration] 
     *        @ProjectCode = 'Finanzas',
     *        @ProjectTopicCode = 'ExactusMaestros',
     *        @DataPipelineCode = 'alprosa_cuenta_contable',
     *        @EnvironmentCode = 'DEV';
     * 
     *   -- Para todos los pipelines de un tópico:
     *   EXEC [Config].[uspGetPipelineConfiguration] 
     *        @ProjectCode = 'Finanzas',
     *        @ProjectTopicCode = 'ExactusMaestros',
     *        @DataPipelineCode = '*',
     *        @EnvironmentCode = 'DEV';
     *******************************************************************************/

    -- Validar que los parámetros obligatorios no sean nulos
    IF @ProjectCode IS NULL OR @ProjectTopicCode IS NULL OR @EnvironmentCode IS NULL
    BEGIN
        RAISERROR('Los parámetros @ProjectCode, @ProjectTopicCode y @EnvironmentCode son obligatorios.', 16, 1);
        RETURN;
    END

    -- Tabla temporal para almacenar la configuración base
    CREATE TABLE #PipelineConfig (
        ProjectName NVARCHAR(200),
        TopicName NVARCHAR(100),
        EnvironmentCode VARCHAR(50),
        PipelineCode VARCHAR(50),
        ConnectionCode VARCHAR(50),
        ConnectionType NVARCHAR(100),
        ConnectionObjectId NVARCHAR(100),
        ConnectionProperties NVARCHAR(MAX),
        SourcePhysicalName NVARCHAR(100),
        SourceSchemaName NVARCHAR(50),
        SourceCommandType NVARCHAR(50),
        SourceExecutionCommand NVARCHAR(MAX),
        SourceCondition NVARCHAR(MAX),
        DataPipelineSettings NVARCHAR(MAX),
        DestinationLayerName NVARCHAR(50),
        DestinationSchemaName NVARCHAR(100),
        DestinationTableName NVARCHAR(100)
    );

    -- Insertar configuración base con validaciones de activos
    INSERT INTO #PipelineConfig (
        ProjectName, TopicName, EnvironmentCode, PipelineCode, ConnectionCode, 
        ConnectionType, ConnectionObjectId, ConnectionProperties,
        SourcePhysicalName, SourceSchemaName, SourceCommandType,
        SourceExecutionCommand, SourceCondition, DataPipelineSettings,
        DestinationLayerName, DestinationSchemaName, DestinationTableName
    )
    SELECT 
        p.Name AS ProjectName,
        pt.Name AS TopicName,
        @EnvironmentCode AS EnvironmentCode,
        dp.Code AS PipelineCode,
        dp.ConnectionCode,
        c.ConnectionType,
        c.ConnectionObjectId,
        c.ConnectionProperties,
        dp.SourcePhysicalName,
        dp.SourceSchemaName,
        dp.SourceCommandType,
        dp.SourceExecutionCommand,
        dp.SourceCondition,
        dp.DataPipelineSettings,
        dp.DestinationLayerName,
        dp.DestinationSchemaName,
        dp.DestinationTableName
    FROM [Config].[Project] p
        INNER JOIN [Config].[ProjectTopic] pt ON p.Code = pt.ProjectCode
        INNER JOIN [Config].[DataPipeline] dp ON pt.Code = dp.TopicCode
        INNER JOIN [Config].[Connection] c ON dp.ConnectionCode = c.Code
    WHERE 
        p.Code = @ProjectCode
        AND pt.Code = @ProjectTopicCode
        AND (@DataPipelineCode = '*' OR dp.Code like @DataPipelineCode)
        AND c.EnvironmentCode = @EnvironmentCode
        -- Validar que estén activos
        AND p.IsActive = 1
        AND pt.IsActive = 1
        AND dp.IsActive = 1
        AND c.IsActive = 1;

    -- Verificar si se encontraron resultados
    IF NOT EXISTS (SELECT 1 FROM #PipelineConfig)
    BEGIN
        RAISERROR('No se encontró configuración activa para los parámetros especificados.', 16, 1);
        DROP TABLE #PipelineConfig;
        RETURN;
    END

    -- Declarar variables para el reemplazo de tokens
    DECLARE @CurrentPipelineCode VARCHAR(50);
    DECLARE @TokenName NVARCHAR(50);
    DECLARE @ParameterType NVARCHAR(20);
    DECLARE @ParameterValue NVARCHAR(MAX);
    DECLARE @CalculationSQL NVARCHAR(MAX);
    DECLARE @CalculatedValue NVARCHAR(MAX);
    DECLARE @SQL NVARCHAR(MAX);

    -- Cursor para cada pipeline en la configuración
    DECLARE pipeline_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT DISTINCT PipelineCode FROM #PipelineConfig;

    OPEN pipeline_cursor;
    FETCH NEXT FROM pipeline_cursor INTO @CurrentPipelineCode;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Cursor para cada parámetro del pipeline actual
        DECLARE param_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT TokenName, ParameterType, ParameterValue, CalculationSQL
            FROM [Config].[DataPipelineParameter]
            WHERE DataPipelineCode = @CurrentPipelineCode
            ORDER BY IdParameter;

        OPEN param_cursor;
        FETCH NEXT FROM param_cursor INTO @TokenName, @ParameterType, @ParameterValue, @CalculationSQL;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Determinar el valor del parámetro
            IF @ParameterType = 'Dynamic' AND @CalculationSQL IS NOT NULL
            BEGIN
                -- Ejecutar SQL dinámico para obtener el valor calculado
                BEGIN TRY
                    SET @SQL = N'SELECT @Result = CAST((' + @CalculationSQL + N') AS NVARCHAR(MAX))';
                    
                    EXEC sp_executesql 
                        @SQL, 
                        N'@Result NVARCHAR(MAX) OUTPUT', 
                        @Result = @CalculatedValue OUTPUT;
                    
                    SET @ParameterValue = @CalculatedValue;
                END TRY
                BEGIN CATCH
                    -- Si falla el cálculo, usar valor por defecto
                    IF @Debug = 1
                    BEGIN
                        PRINT 'ADVERTENCIA: Error calculando parámetro ' + @TokenName + ' para pipeline ' + @CurrentPipelineCode;
                        PRINT 'Error: ' + ERROR_MESSAGE();
                        PRINT 'SQL ejecutado: ' + @CalculationSQL;
                    END
                    
                    -- Usar valor por defecto para fechas
                    IF @TokenName LIKE '%Date%' OR @TokenName LIKE '%date%'
                    BEGIN
                        SET @ParameterValue = '1900-01-01';
                        IF @Debug = 1 PRINT 'Usando valor por defecto: ' + @ParameterValue;
                    END
                    ELSE
                    BEGIN
                        SET @ParameterValue = NULL;
                    END
                END CATCH
            END
            -- Para parámetros estáticos (Static), @ParameterValue ya viene del cursor

            -- Reemplazar el token en SourceExecutionCommand y SourceCondition
            -- (incluso si @ParameterValue es NULL para debug)
            IF @ParameterValue IS NOT NULL
            BEGIN
                UPDATE #PipelineConfig
                SET 
                    SourceExecutionCommand = REPLACE(SourceExecutionCommand, @TokenName, @ParameterValue),
                    SourceCondition = REPLACE(ISNULL(SourceCondition, ''), @TokenName, @ParameterValue)
                WHERE PipelineCode = @CurrentPipelineCode;
                
                -- Log del reemplazo exitoso
                IF @Debug = 1 PRINT 'Token reemplazado: ' + @TokenName + ' = ' + @ParameterValue + ' en pipeline ' + @CurrentPipelineCode;
            END
            ELSE
            BEGIN
                IF @Debug = 1 PRINT 'ADVERTENCIA: Token ' + @TokenName + ' no se pudo reemplazar (valor NULL) en pipeline ' + @CurrentPipelineCode;
            END

            FETCH NEXT FROM param_cursor INTO @TokenName, @ParameterType, @ParameterValue, @CalculationSQL;
        END

        CLOSE param_cursor;
        DEALLOCATE param_cursor;

        FETCH NEXT FROM pipeline_cursor INTO @CurrentPipelineCode;
    END

    CLOSE pipeline_cursor;
    DEALLOCATE pipeline_cursor;

    -- Retornar el resultado final con tokens reemplazados
    SELECT 
        ProjectName,
        TopicName,
        EnvironmentCode,
        PipelineCode,
        ConnectionCode,
        ConnectionType,
        ConnectionObjectId,
        ConnectionProperties,
        SourcePhysicalName,
        SourceSchemaName,
        SourceCommandType,
        CASE 
            WHEN SourceCondition IS NOT NULL AND LTRIM(RTRIM(SourceCondition)) <> '' AND @IsDeltaActive = 1
            THEN SourceExecutionCommand + ' WHERE ' + SourceCondition
            ELSE SourceExecutionCommand
        END AS SourceExecutionCommand,
        DataPipelineSettings,
        DestinationLayerName,
        DestinationSchemaName,
        DestinationTableName
    FROM #PipelineConfig
    ORDER BY PipelineCode;

    -- Limpiar tabla temporal
    DROP TABLE #PipelineConfig;

END;

GO

