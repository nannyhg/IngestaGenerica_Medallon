CREATE TABLE [Config].[DataQualityRule] (
    [IdRule]         INT            IDENTITY (1, 1) NOT NULL,
    [IdContract]     BIGINT         NOT NULL,
    [RuleName]       NVARCHAR (100) NOT NULL,
    [RuleType]       NVARCHAR (50)  NOT NULL,
    [RuleExpression] NVARCHAR (MAX) NOT NULL,
    [Severity]       NVARCHAR (20)  NOT NULL,
    [ErrorMessage]   NVARCHAR (500) NULL,
    [IsActive]       BIT            CONSTRAINT [DF_DQ_IsActive] DEFAULT ((1)) NOT NULL,
    [CreatedDate]    DATETIME2 (3)  DEFAULT (sysutcdatetime()) NULL,
    PRIMARY KEY CLUSTERED ([IdRule] ASC),
    CONSTRAINT [FK_DQ_DataContract] FOREIGN KEY ([IdContract]) REFERENCES [Config].[DataContract] ([IdContract])
);


GO

