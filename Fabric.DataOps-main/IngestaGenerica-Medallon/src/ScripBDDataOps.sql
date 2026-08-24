CREATE SCHEMA [Config]
GO
/****** Object:  Table [Config].[Connection]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[Connection](
	[Code] [varchar](50) NOT NULL,
	[Name] [nvarchar](100) NOT NULL,
	[EnvironmentCode] [varchar](50) NOT NULL,
	[ConnectionType] [nvarchar](100) NOT NULL,
	[KeyVaultSecretName] [nvarchar](100) NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](6) NOT NULL,
	[CreatedBy] [nvarchar](100) NOT NULL,
	[ConnectionObjectId] [nvarchar](50) NULL,
	[ConnectionProperties] [nvarchar](max) NULL,
PRIMARY KEY CLUSTERED 
(
	[Code] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [Config].[DataContract]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[DataContract](
	[IdContract] [bigint] IDENTITY(1,1) NOT NULL,
	[DataPipelineCode] [varchar](50) NOT NULL,
	[SourceColumn] [nvarchar](255) NULL,
	[SourceDataType] [nvarchar](100) NULL,
	[IsNullable] [bit] NOT NULL,
	[SortOrder] [int] NOT NULL,
	[DestinationZone] [nvarchar](255) NULL,
	[DestinationSchema] [nvarchar](100) NULL,
	[DestinationTable] [nvarchar](255) NULL,
	[DestinationField] [nvarchar](255) NULL,
	[DestinationFieldDataType] [nvarchar](100) NULL,
	[DestinationFieldDescription] [nvarchar](1000) NULL,
	[AttributeType] [nvarchar](100) NULL,
	[WriteType] [nvarchar](100) NULL,
	[BusinessRuleDescription] [nvarchar](2000) NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](3) NOT NULL,
	[CreatedBy] [nvarchar](128) NULL,
	[UpdatedDate] [datetime2](3) NULL,
	[UpdatedBy] [nvarchar](128) NULL,
	[SourceSchema] [varchar](200) NULL,
	[SourceLayer] [varchar](200) NULL,
PRIMARY KEY CLUSTERED 
(
	[IdContract] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Config].[DataEquivalence]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[DataEquivalence](
	[IdEquivalence] [bigint] IDENTITY(1,1) NOT NULL,
	[MappingGroup] [varchar](100) NOT NULL,
	[SourceValue] [nvarchar](500) NOT NULL,
	[TargetValue] [nvarchar](500) NOT NULL,
	[TargetValue2] [nvarchar](500) NULL,
	[LookupType] [varchar](50) NOT NULL,
	[Priority] [int] NOT NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](3) NOT NULL,
	[CreatedBy] [nvarchar](100) NULL,
PRIMARY KEY CLUSTERED 
(
	[IdEquivalence] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Config].[DataPipeline]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[DataPipeline](
	[Code] [varchar](50) NOT NULL,
	[Name] [nvarchar](100) NOT NULL,
	[Description] [nvarchar](500) NULL,
	[TopicCode] [varchar](50) NOT NULL,
	[ConnectionCode] [varchar](50) NOT NULL,
	[SourcePhysicalName] [nvarchar](100) NULL,
	[SourceSchemaName] [nvarchar](50) NULL,
	[SourceExecutionCommand] [nvarchar](max) NULL,
	[SourceCondition] [nvarchar](max) NULL,
	[Priority] [int] NOT NULL,
	[SourceTimeZone] [nvarchar](50) NULL,
	[DataPipelineSettings] [nvarchar](max) NULL,
	[RetryCount] [int] NULL,
	[SupportsDelta] [bit] NOT NULL,
	[DeltaMethod] [nvarchar](50) NULL,
	[SourceDeltaColumn] [nvarchar](100) NULL,
	[DestinationLayerName] [nvarchar](50) NOT NULL,
	[DestinationPath] [nvarchar](250) NULL,
	[DestinationFileName] [nvarchar](200) NULL,
	[DestinationSchemaName] [nvarchar](100) NULL,
	[DestinationTableName] [nvarchar](100) NULL,
	[DataValidityMinutes] [int] NULL,
	[DataQualityEnabled] [bit] NOT NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](6) NOT NULL,
	[CreatedBy] [nvarchar](100) NOT NULL,
	[SourceCommandType] [nvarchar](50) NULL,
 CONSTRAINT [PK_Source_Code] PRIMARY KEY CLUSTERED 
(
	[Code] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [Config].[DataPipelineParameter]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[DataPipelineParameter](
	[IdParameter] [int] IDENTITY(1,1) NOT NULL,
	[DataPipelineCode] [varchar](50) NOT NULL,
	[TokenName] [nvarchar](50) NOT NULL,
	[ParameterType] [nvarchar](20) NOT NULL,
	[ParameterValue] [nvarchar](max) NULL,
	[CalculationSQL] [nvarchar](max) NULL,
	[LastUpdated] [datetime2](7) NULL,
PRIMARY KEY CLUSTERED 
(
	[IdParameter] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [Config].[DataQualityRule]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[DataQualityRule](
	[IdRule] [int] IDENTITY(1,1) NOT NULL,
	[IdContract] [bigint] NOT NULL,
	[RuleName] [nvarchar](100) NOT NULL,
	[RuleType] [nvarchar](50) NOT NULL,
	[RuleExpression] [nvarchar](max) NOT NULL,
	[Severity] [nvarchar](20) NOT NULL,
	[ErrorMessage] [nvarchar](500) NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](3) NULL,
PRIMARY KEY CLUSTERED 
(
	[IdRule] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [Config].[Environment]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[Environment](
	[Code] [varchar](50) NOT NULL,
	[Name] [nvarchar](100) NOT NULL,
	[Description] [nvarchar](500) NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](6) NOT NULL,
	[CreatedBy] [nvarchar](100) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[Code] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Config].[Project]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[Project](
	[Code] [varchar](50) NOT NULL,
	[Name] [varchar](200) NOT NULL,
	[Description] [nvarchar](500) NULL,
	[FunctionalOwner] [nvarchar](100) NULL,
	[TechnicalOwner] [nvarchar](100) NULL,
	[TechnicalEmail] [nvarchar](200) NULL,
	[FunctionalEmail] [nvarchar](200) NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](6) NOT NULL,
	[CreatedBy] [nvarchar](100) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[Code] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Config].[ProjectTopic]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Config].[ProjectTopic](
	[Code] [varchar](50) NOT NULL,
	[Name] [nvarchar](100) NOT NULL,
	[ProjectCode] [varchar](50) NOT NULL,
	[Description] [nvarchar](500) NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](6) NOT NULL,
	[CreatedBy] [nvarchar](100) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[Code] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[BK_DataContract]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[BK_DataContract](
	[IdContract] [bigint] IDENTITY(1,1) NOT NULL,
	[DataPipelineCode] [varchar](50) NOT NULL,
	[SourceColumn] [nvarchar](255) NULL,
	[SourceDataType] [nvarchar](100) NULL,
	[IsNullable] [bit] NOT NULL,
	[SortOrder] [int] NOT NULL,
	[DestinationZone] [nvarchar](255) NULL,
	[DestinationSchema] [nvarchar](100) NULL,
	[DestinationTable] [nvarchar](255) NULL,
	[DestinationField] [nvarchar](255) NULL,
	[DestinationFieldDataType] [nvarchar](100) NULL,
	[DestinationFieldDescription] [nvarchar](1000) NULL,
	[AttributeType] [nvarchar](100) NULL,
	[WriteType] [nvarchar](100) NULL,
	[BusinessRuleDescription] [nvarchar](2000) NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](3) NOT NULL,
	[CreatedBy] [nvarchar](128) NULL,
	[UpdatedDate] [datetime2](3) NULL,
	[UpdatedBy] [nvarchar](128) NULL,
	[SourceSchema] [varchar](200) NULL,
	[SourceLayer] [varchar](200) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[Bk_DataEquivalence]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[Bk_DataEquivalence](
	[IdEquivalence] [bigint] IDENTITY(1,1) NOT NULL,
	[MappingGroup] [varchar](100) NOT NULL,
	[SourceValue] [nvarchar](500) NOT NULL,
	[TargetValue] [nvarchar](500) NOT NULL,
	[LookupType] [varchar](50) NOT NULL,
	[Priority] [int] NOT NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](3) NOT NULL,
	[CreatedBy] [nvarchar](100) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[BK_DataPipeline]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[BK_DataPipeline](
	[Code] [varchar](50) NOT NULL,
	[Name] [nvarchar](100) NOT NULL,
	[Description] [nvarchar](500) NULL,
	[TopicCode] [varchar](50) NOT NULL,
	[ConnectionCode] [varchar](50) NOT NULL,
	[SourcePhysicalName] [nvarchar](100) NULL,
	[SourceSchemaName] [nvarchar](50) NULL,
	[SourceExecutionCommand] [nvarchar](max) NULL,
	[SourceCondition] [nvarchar](max) NULL,
	[Priority] [int] NOT NULL,
	[SourceTimeZone] [nvarchar](50) NULL,
	[DataPipelineSettings] [nvarchar](max) NULL,
	[RetryCount] [int] NULL,
	[SupportsDelta] [bit] NOT NULL,
	[DeltaMethod] [nvarchar](50) NULL,
	[SourceDeltaColumn] [nvarchar](100) NULL,
	[DestinationLayerName] [nvarchar](50) NOT NULL,
	[DestinationPath] [nvarchar](250) NULL,
	[DestinationFileName] [nvarchar](200) NULL,
	[DestinationSchemaName] [nvarchar](100) NULL,
	[DestinationTableName] [nvarchar](100) NULL,
	[DataValidityMinutes] [int] NULL,
	[DataQualityEnabled] [bit] NOT NULL,
	[IsActive] [bit] NOT NULL,
	[CreatedDate] [datetime2](6) NOT NULL,
	[CreatedBy] [nvarchar](100) NOT NULL,
	[SourceCommandType] [nvarchar](50) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_DataEquivalence_Group]    Script Date: 5/11/2026 3:42:14 PM ******/
CREATE NONCLUSTERED INDEX [IX_DataEquivalence_Group] ON [Config].[DataEquivalence]
(
	[MappingGroup] ASC,
	[IsActive] ASC
)
INCLUDE([SourceValue],[TargetValue],[LookupType]) WITH (STATISTICS_NORECOMPUTE = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [UIX_DataEquivalence_ActiveMapping]    Script Date: 5/11/2026 3:42:14 PM ******/
CREATE UNIQUE NONCLUSTERED INDEX [UIX_DataEquivalence_ActiveMapping] ON [Config].[DataEquivalence]
(
	[MappingGroup] ASC,
	[SourceValue] ASC
)
WHERE ([IsActive]=(1))
WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
ALTER TABLE [Config].[DataContract] ADD  CONSTRAINT [DF_SourceToDestinationMapping_IsNullable]  DEFAULT ((0)) FOR [IsNullable]
GO
ALTER TABLE [Config].[DataContract] ADD  CONSTRAINT [DF_SourceToDestinationMapping_IsActive]  DEFAULT ((1)) FOR [IsActive]
GO
ALTER TABLE [Config].[DataContract] ADD  CONSTRAINT [DF_SourceToDestinationMapping_CreatedDate]  DEFAULT (sysutcdatetime()) FOR [CreatedDate]
GO
ALTER TABLE [Config].[DataEquivalence] ADD  DEFAULT ('EXACT') FOR [LookupType]
GO
ALTER TABLE [Config].[DataEquivalence] ADD  DEFAULT ((10)) FOR [Priority]
GO
ALTER TABLE [Config].[DataEquivalence] ADD  DEFAULT ((1)) FOR [IsActive]
GO
ALTER TABLE [Config].[DataEquivalence] ADD  DEFAULT (sysutcdatetime()) FOR [CreatedDate]
GO
ALTER TABLE [Config].[DataPipelineParameter] ADD  DEFAULT (sysutcdatetime()) FOR [LastUpdated]
GO
ALTER TABLE [Config].[DataQualityRule] ADD  CONSTRAINT [DF_DQ_IsActive]  DEFAULT ((1)) FOR [IsActive]
GO
ALTER TABLE [Config].[DataQualityRule] ADD  DEFAULT (sysutcdatetime()) FOR [CreatedDate]
GO
ALTER TABLE [Config].[Connection]  WITH CHECK ADD  CONSTRAINT [FK_Connection_Environment] FOREIGN KEY([EnvironmentCode])
REFERENCES [Config].[Environment] ([Code])
GO
ALTER TABLE [Config].[Connection] CHECK CONSTRAINT [FK_Connection_Environment]
GO
ALTER TABLE [Config].[DataContract]  WITH CHECK ADD  CONSTRAINT [FK_SourceToDestinationMapping_DataPipeline] FOREIGN KEY([DataPipelineCode])
REFERENCES [Config].[DataPipeline] ([Code])
GO
ALTER TABLE [Config].[DataContract] CHECK CONSTRAINT [FK_SourceToDestinationMapping_DataPipeline]
GO
ALTER TABLE [Config].[DataPipeline]  WITH CHECK ADD  CONSTRAINT [FK_Source_Connection] FOREIGN KEY([ConnectionCode])
REFERENCES [Config].[Connection] ([Code])
GO
ALTER TABLE [Config].[DataPipeline] CHECK CONSTRAINT [FK_Source_Connection]
GO
ALTER TABLE [Config].[DataPipeline]  WITH CHECK ADD  CONSTRAINT [FK_Source_Topic] FOREIGN KEY([TopicCode])
REFERENCES [Config].[ProjectTopic] ([Code])
GO
ALTER TABLE [Config].[DataPipeline] CHECK CONSTRAINT [FK_Source_Topic]
GO
ALTER TABLE [Config].[DataPipelineParameter]  WITH CHECK ADD  CONSTRAINT [FK_Parameter_DataPipeline] FOREIGN KEY([DataPipelineCode])
REFERENCES [Config].[DataPipeline] ([Code])
GO
ALTER TABLE [Config].[DataPipelineParameter] CHECK CONSTRAINT [FK_Parameter_DataPipeline]
GO
ALTER TABLE [Config].[DataQualityRule]  WITH CHECK ADD  CONSTRAINT [FK_DQ_DataContract] FOREIGN KEY([IdContract])
REFERENCES [Config].[DataContract] ([IdContract])
GO
ALTER TABLE [Config].[DataQualityRule] CHECK CONSTRAINT [FK_DQ_DataContract]
GO
ALTER TABLE [Config].[ProjectTopic]  WITH CHECK ADD  CONSTRAINT [FK_ProjectTopic_Project] FOREIGN KEY([ProjectCode])
REFERENCES [Config].[Project] ([Code])
GO
ALTER TABLE [Config].[ProjectTopic] CHECK CONSTRAINT [FK_ProjectTopic_Project]
GO
ALTER TABLE [Config].[Connection]  WITH CHECK ADD  CONSTRAINT [CK_Connection_Properties_JSON] CHECK  ((isjson([ConnectionProperties])>(0)))
GO
ALTER TABLE [Config].[Connection] CHECK CONSTRAINT [CK_Connection_Properties_JSON]
GO
ALTER TABLE [Config].[DataPipeline]  WITH CHECK ADD  CONSTRAINT [CK_DataPipeline_DataPipelineSettings_JSON] CHECK  ((isjson([DataPipelineSettings])>(0)))
GO
ALTER TABLE [Config].[DataPipeline] CHECK CONSTRAINT [CK_DataPipeline_DataPipelineSettings_JSON]
GO
/****** Object:  StoredProcedure [Config].[uspGetPipelineConfiguration]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
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
/****** Object:  StoredProcedure [Config].[uspGetPipelineConfigurationAPI]    Script Date: 5/11/2026 3:42:14 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [Config].[uspGetPipelineConfigurationAPI]
    @ProjectCode VARCHAR(50),
    @ProjectTopicCode VARCHAR(50),
    @DataPipelineCode VARCHAR(50) = '*',  -- '*' significa todos los pipelines del topic
    @EnvironmentCode VARCHAR(50),
    @Debug BIT = 0  -- 1 para mostrar logs detallados
AS
BEGIN
    SET NOCOUNT ON;

    /*******************************************************************************
     * Procedimiento: uspGetPipelineConfigurationAPI
     * Descripción: Obtiene la configuración de pipelines tipo API con tokens 
     *              reemplazados en SourceExecutionCommand.
     *              Filtra únicamente conexiones con ConnectionType = 'API'.
     * 
     * Parámetros:
     *   @ProjectCode      - Código del proyecto
     *   @ProjectTopicCode - Código del tópico del proyecto
     *   @DataPipelineCode - Código del pipeline ('*' para todos los pipelines del topic)
     *   @EnvironmentCode  - Código del ambiente (DEV, QA, PROD)
     *   @Debug            - 1 para mostrar logs detallados
     * 
     * Retorna:
     *   Configuración del pipeline API con tokens reemplazados en 
     *   SourceExecutionCommand
     * 
     * Ejemplo de uso:
     *   EXEC [Config].[uspGetPipelineConfigurationAPI] 
     *        @ProjectCode = 'TransporteInt',
     *        @ProjectTopicCode = 'Disatel-Gps',
     *        @DataPipelineCode = 'disatelgps_vehicles',
     *        @EnvironmentCode = 'DEV';
     * 
     *   -- Para todos los pipelines API de un tópico:
     *   EXEC [Config].[uspGetPipelineConfigurationAPI] 
     *        @ProjectCode = 'TransporteInt',
     *        @ProjectTopicCode = 'Disatel-Gps',
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
        SourceExecutionCommand NVARCHAR(MAX),
        DataPipelineSettings NVARCHAR(MAX),
        DestinationLayerName NVARCHAR(50),
        DestinationSchemaName NVARCHAR(100),
        DestinationTableName NVARCHAR(100)
    );

    -- Insertar configuración base con validaciones de activos
    INSERT INTO #PipelineConfig (
        ProjectName, TopicName, EnvironmentCode, PipelineCode, ConnectionCode, 
        ConnectionType, ConnectionObjectId, ConnectionProperties,
        SourceExecutionCommand, DataPipelineSettings,
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
        dp.SourceExecutionCommand,
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
        AND (@DataPipelineCode = '*' OR dp.Code LIKE @DataPipelineCode)
        AND c.EnvironmentCode = @EnvironmentCode
        AND c.ConnectionType = 'API'
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

            -- Reemplazar el token en SourceExecutionCommand
            IF @ParameterValue IS NOT NULL
            BEGIN
                UPDATE #PipelineConfig
                SET 
                    SourceExecutionCommand = REPLACE(SourceExecutionCommand, @TokenName, @ParameterValue)
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
        SourceExecutionCommand,
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
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Unique identifier for the connection definition.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection', @level2type=N'COLUMN',@level2name=N'Code'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Human-friendly connection name.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection', @level2type=N'COLUMN',@level2name=N'Name'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Foreign key to the environment where the connection is used.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection', @level2type=N'COLUMN',@level2name=N'EnvironmentCode'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Technical type of connection (e.g. JDBC, ODBC, SQLServer).' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection', @level2type=N'COLUMN',@level2name=N'ConnectionType'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Name of the secret in Key Vault that holds the connection string or credential reference.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection', @level2type=N'COLUMN',@level2name=N'KeyVaultSecretName'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag indicating if the connection is active.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection', @level2type=N'COLUMN',@level2name=N'IsActive'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Date and time when the connection record was created.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection', @level2type=N'COLUMN',@level2name=N'CreatedDate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'User who created the connection record.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection', @level2type=N'COLUMN',@level2name=N'CreatedBy'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Connection definitions and credentials reference (Key Vault).' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Connection'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Unique identifier code for the environment.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Environment', @level2type=N'COLUMN',@level2name=N'Code'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Human-friendly name for the environment.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Environment', @level2type=N'COLUMN',@level2name=N'Name'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Optional description of the environment.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Environment', @level2type=N'COLUMN',@level2name=N'Description'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag that indicates whether the environment is active.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Environment', @level2type=N'COLUMN',@level2name=N'IsActive'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Date and time when the environment record was created.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Environment', @level2type=N'COLUMN',@level2name=N'CreatedDate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Login or user that created the record.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Environment', @level2type=N'COLUMN',@level2name=N'CreatedBy'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Configuration table that stores environment definitions (e.g. development, production).' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Environment'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Unique project identifier code.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'Code'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Human-friendly project name.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'Name'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Optional project description.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'Description'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Name of the functional owner for the project.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'FunctionalOwner'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Name of the technical owner for the project.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'TechnicalOwner'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Contact email for the technical owner.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'TechnicalEmail'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Contact email for the functional owner.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'FunctionalEmail'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag indicating if the project is active.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'IsActive'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Date and time when the project record was created.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'CreatedDate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'User who created the project record.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project', @level2type=N'COLUMN',@level2name=N'CreatedBy'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Project catalog for configuration grouping.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'Project'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Unique identifier for the topic within a project.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'ProjectTopic', @level2type=N'COLUMN',@level2name=N'Code'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Human-friendly name for the topic.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'ProjectTopic', @level2type=N'COLUMN',@level2name=N'Name'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Foreign key to the project the topic belongs to.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'ProjectTopic', @level2type=N'COLUMN',@level2name=N'ProjectCode'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Optional description of the topic.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'ProjectTopic', @level2type=N'COLUMN',@level2name=N'Description'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Flag indicating whether the topic is active.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'ProjectTopic', @level2type=N'COLUMN',@level2name=N'IsActive'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Date and time when the topic record was created.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'ProjectTopic', @level2type=N'COLUMN',@level2name=N'CreatedDate'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'User who created the topic record.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'ProjectTopic', @level2type=N'COLUMN',@level2name=N'CreatedBy'
GO
EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'Topics or functional groups within a project.' , @level0type=N'SCHEMA',@level0name=N'Config', @level1type=N'TABLE',@level1name=N'ProjectTopic'
GO

