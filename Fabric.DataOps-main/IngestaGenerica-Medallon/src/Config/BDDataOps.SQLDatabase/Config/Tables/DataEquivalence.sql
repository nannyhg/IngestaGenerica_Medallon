CREATE TABLE [Config].[DataEquivalence] (
    [IdEquivalence] BIGINT         IDENTITY (1, 1) NOT NULL,
    [MappingGroup]  VARCHAR (100)  NOT NULL,
    [SourceValue]   NVARCHAR (500) NOT NULL,
    [TargetValue]   NVARCHAR (500) NOT NULL,
    [TargetValue2]  NVARCHAR (500) NULL,
    [LookupType]    VARCHAR (50)   DEFAULT ('EXACT') NOT NULL,
    [Priority]      INT            DEFAULT ((10)) NOT NULL,
    [IsActive]      BIT            DEFAULT ((1)) NOT NULL,
    [CreatedDate]   DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    [CreatedBy]     NVARCHAR (100) NULL,
    PRIMARY KEY CLUSTERED ([IdEquivalence] ASC)
);


GO

CREATE NONCLUSTERED INDEX [IX_DataEquivalence_Group]
    ON [Config].[DataEquivalence]([MappingGroup] ASC, [IsActive] ASC)
    INCLUDE([SourceValue], [TargetValue], [LookupType]);


GO

CREATE UNIQUE NONCLUSTERED INDEX [UIX_DataEquivalence_ActiveMapping]
    ON [Config].[DataEquivalence]([MappingGroup] ASC, [SourceValue] ASC) WHERE ([IsActive]=(1));


GO

