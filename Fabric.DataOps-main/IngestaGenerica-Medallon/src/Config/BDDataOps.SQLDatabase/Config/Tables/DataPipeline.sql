CREATE TABLE [Config].[DataPipeline] (
    [Code]                   VARCHAR (50)   NOT NULL,
    [Name]                   NVARCHAR (100) NOT NULL,
    [Description]            NVARCHAR (500) NULL,
    [TopicCode]              VARCHAR (50)   NOT NULL,
    [ConnectionCode]         VARCHAR (50)   NOT NULL,
    [SourcePhysicalName]     NVARCHAR (100) NULL,
    [SourceSchemaName]       NVARCHAR (50)  NULL,
    [SourceExecutionCommand] NVARCHAR (MAX) NULL,
    [SourceCondition]        NVARCHAR (MAX) NULL,
    [Priority]               INT            NOT NULL,
    [SourceTimeZone]         NVARCHAR (50)  NULL,
    [DataPipelineSettings]   NVARCHAR (MAX) NULL,
    [RetryCount]             INT            NULL,
    [SupportsDelta]          BIT            NOT NULL,
    [DeltaMethod]            NVARCHAR (50)  NULL,
    [SourceDeltaColumn]      NVARCHAR (100) NULL,
    [DestinationLayerName]   NVARCHAR (50)  NOT NULL,
    [DestinationPath]        NVARCHAR (250) NULL,
    [DestinationFileName]    NVARCHAR (200) NULL,
    [DestinationSchemaName]  NVARCHAR (100) NULL,
    [DestinationTableName]   NVARCHAR (100) NULL,
    [DataValidityMinutes]    INT            NULL,
    [DataQualityEnabled]     BIT            NOT NULL,
    [IsActive]               BIT            NOT NULL,
    [CreatedDate]            DATETIME2 (6)  NOT NULL,
    [CreatedBy]              NVARCHAR (100) NOT NULL,
    [SourceCommandType]      NVARCHAR (50)  NULL,
    CONSTRAINT [PK_Source_Code] PRIMARY KEY CLUSTERED ([Code] ASC),
    CONSTRAINT [CK_DataPipeline_DataPipelineSettings_JSON] CHECK (isjson([DataPipelineSettings])>(0)),
    CONSTRAINT [FK_Source_Connection] FOREIGN KEY ([ConnectionCode]) REFERENCES [Config].[Connection] ([Code]),
    CONSTRAINT [FK_Source_Topic] FOREIGN KEY ([TopicCode]) REFERENCES [Config].[ProjectTopic] ([Code])
);


GO

