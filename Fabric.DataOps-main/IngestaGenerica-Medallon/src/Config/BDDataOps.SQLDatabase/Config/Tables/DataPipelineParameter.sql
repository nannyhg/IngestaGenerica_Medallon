CREATE TABLE [Config].[DataPipelineParameter] (
    [IdParameter]      INT            IDENTITY (1, 1) NOT NULL,
    [DataPipelineCode] VARCHAR (50)   NOT NULL,
    [TokenName]        NVARCHAR (50)  NOT NULL,
    [ParameterType]    NVARCHAR (20)  NOT NULL,
    [ParameterValue]   NVARCHAR (MAX) NULL,
    [CalculationSQL]   NVARCHAR (MAX) NULL,
    [LastUpdated]      DATETIME2 (7)  DEFAULT (sysutcdatetime()) NULL,
    PRIMARY KEY CLUSTERED ([IdParameter] ASC),
    CONSTRAINT [FK_Parameter_DataPipeline] FOREIGN KEY ([DataPipelineCode]) REFERENCES [Config].[DataPipeline] ([Code])
);


GO

