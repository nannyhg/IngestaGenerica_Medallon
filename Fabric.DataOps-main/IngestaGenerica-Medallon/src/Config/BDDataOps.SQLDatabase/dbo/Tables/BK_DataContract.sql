CREATE TABLE [dbo].[BK_DataContract] (
    [IdContract]                  BIGINT          IDENTITY (1, 1) NOT NULL,
    [DataPipelineCode]            VARCHAR (50)    NOT NULL,
    [SourceColumn]                NVARCHAR (255)  NULL,
    [SourceDataType]              NVARCHAR (100)  NULL,
    [IsNullable]                  BIT             NOT NULL,
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
    [IsActive]                    BIT             NOT NULL,
    [CreatedDate]                 DATETIME2 (3)   NOT NULL,
    [CreatedBy]                   NVARCHAR (128)  NULL,
    [UpdatedDate]                 DATETIME2 (3)   NULL,
    [UpdatedBy]                   NVARCHAR (128)  NULL,
    [SourceSchema]                VARCHAR (200)   NULL,
    [SourceLayer]                 VARCHAR (200)   NULL
);


GO

