CREATE TABLE [Config].[DataContract] (
    [IdContract]                  BIGINT          IDENTITY (1, 1) NOT NULL,
    [DataPipelineCode]            VARCHAR (50)    NOT NULL,
    [SourceColumn]                NVARCHAR (255)  NULL,
    [SourceDataType]              NVARCHAR (100)  NULL,
    [IsNullable]                  BIT             CONSTRAINT [DF_SourceToDestinationMapping_IsNullable] DEFAULT ((0)) NOT NULL,
    [SortOrder]                   INT             NOT NULL,
    [DestinationZone]             NVARCHAR (255)  NULL,
    [DestinationSchema]           NVARCHAR (100)  NULL,
    [DestinationTable]            NVARCHAR (255)  NULL,
    [DestinationField]            NVARCHAR (255)  NULL,
    [DestinationFieldDataType]    NVARCHAR (100)  NULL,
    [DestinationFieldDescription] NVARCHAR (1000) NULL,
    [AttributeType]               NVARCHAR (100)  NULL,
    [WriteType]                   NVARCHAR (100)  NULL,
    [BusinessRuleDescription]     NVARCHAR (2000) NULL,
    [IsActive]                    BIT             CONSTRAINT [DF_SourceToDestinationMapping_IsActive] DEFAULT ((1)) NOT NULL,
    [CreatedDate]                 DATETIME2 (3)   CONSTRAINT [DF_SourceToDestinationMapping_CreatedDate] DEFAULT (sysutcdatetime()) NOT NULL,
    [CreatedBy]                   NVARCHAR (128)  NULL,
    [UpdatedDate]                 DATETIME2 (3)   NULL,
    [UpdatedBy]                   NVARCHAR (128)  NULL,
    [SourceSchema]                VARCHAR (200)   NULL,
    [SourceLayer]                 VARCHAR (200)   NULL,
    PRIMARY KEY CLUSTERED ([IdContract] ASC),
    CONSTRAINT [FK_SourceToDestinationMapping_DataPipeline] FOREIGN KEY ([DataPipelineCode]) REFERENCES [Config].[DataPipeline] ([Code])
);


GO

